"""Bot de Trading - Orquestador principal.

Responsabilidades exclusivas:
1. Leer datos del exchange
2. Calcular indicadores en logic/
3. Evaluar filtros cuantitativos
4. Ejecutar órdenes
5. Registrar logs y notificaciones

NO contiene lógica de negocio pura ni acceso directo a la API.
"""

import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

import pandas as pd

from config import AppConfig, load_config
from data import ExchangeClient
from logic import (
    EstrategiaMultivariable,
    ResultadoFiltros,
    calcular_adx,
    calcular_atr,
    calcular_ema,
    calcular_ganancia_con_stoploss,
    calcular_profit_factor,
    crear_registro_csv,
    evaluar_estrategia_multivariable,
    validar_filtros_cuantitativos,
)
from notifications import enviar_notificacion_telegram, notificar_operacion

# ---------------------------------------------------------------------------
# Configuración de logging
# ---------------------------------------------------------------------------

def setup_logging(log_level: str = "INFO") -> None:
    """Configura logging estructurado con rotación de archivos."""
    from logging.handlers import RotatingFileHandler

    log_format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))
    root_logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(
        "bot_trading.log",
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))
    root_logger.addHandler(file_handler)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Servidor de salud para Render
# ---------------------------------------------------------------------------

class HealthCheckHandler(BaseHTTPRequestHandler):
    """Handler para el endpoint de salud de Render."""

    def do_GET(self) -> None:
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot de Trading Activo")

    def log_message(self, format: str, *args: object) -> None:
        return


def start_health_server(port: int) -> None:
    """Inicia el servidor HTTP de salud en un hilo daemon."""
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    logger.info("Servidor HTTP de salud activo en puerto %d", port)
    server.serve_forever()


# ---------------------------------------------------------------------------
# Estado del bot (sin variables globales)
# ---------------------------------------------------------------------------

@dataclass
class BotState:
    """Estado mutable del bot de trading."""
    saldo_usdt: float = 10.00
    saldo_btc: float = 0.0
    precio_compra: float = 0.0
    atr_compra: float = 0.0
    ganancias_totales: float = 0.0
    perdidas_totales: float = 0.0
    breakeven_activado: bool = False
    break_even_notificado: bool = False
    stop_loss: float = 0.0
    hora_compra: Optional[str] = None
    min_order_usdt: float = 10.0


# ---------------------------------------------------------------------------
# Funciones auxiliares del orquestador
# ---------------------------------------------------------------------------

def fetch_market_min_order(client: ExchangeClient, symbol: str, default: float = 10.0) -> float:
    """Obtiene el mínimo de orden del exchange."""
    market_info = client.get_market_info(symbol)
    if market_info and "min_order_usdt" in market_info:
        min_order = market_info["min_order_usdt"]
        logger.info("Mínimo de orden del exchange: $%.2f USDT", min_order)
        return min_order
    logger.info("Usando mínimo de orden default: $%.2f USDT", default)
    return default


def evaluar_salida_posicion(
    state: BotState,
    precio_actual: float,
    symbol: str,
    client: ExchangeClient,
    config: AppConfig,
) -> bool:
    """Evalúa salida de posición por Stop-Loss / Break-Even.

    Returns:
        True si la posición se cerró, False si sigue abierta.
    """
    if state.saldo_btc <= 0:
        return False

    if state.breakeven_activado:
        state.stop_loss = state.precio_compra
    else:
        state.stop_loss = state.precio_compra - (state.atr_compra * config.strategy.atr_sl_multiplier)

    if not state.breakeven_activado and state.atr_compra > 0:
        ganancia_flotante_atr = (precio_actual - state.precio_compra) / state.atr_compra
        if ganancia_flotante_atr >= config.strategy.trailing_be_threshold_atr:
            state.breakeven_activado = True
            state.stop_loss = state.precio_compra
            if not state.break_even_notificado:
                state.break_even_notificado = True
                pnl_pct = ((precio_actual - state.precio_compra) / state.precio_compra) * 100
                mensaje = (
                    f"🚨 *TRAILING STOP BREAK-EVEN ACTIVADO*\n\n"
                    f"• Precio Entrada: ${state.precio_compra:.2f}\n"
                    f"• Precio Actual: ${precio_actual:.2f}\n"
                    f"• Ganancia: +{pnl_pct:.2f}% (+{ganancia_flotante_atr:.1f} ATR)\n"
                    f"• Stop-Loss movido al punto de entrada (${state.precio_compra:.2f})"
                )
                enviar_notificacion_telegram(mensaje, config.telegram)
                logger.info(
                    "Trailing Stop activado: SL movido a Break-Even ($%.2f)",
                    state.precio_compra,
                )

    if precio_actual <= state.stop_loss:
        tipo_salida = "BE" if state.breakeven_activado else "SL"
        logger.info(
            "Ejecutando venta por %s: $%.2f <= $%.2f",
            tipo_salida,
            precio_actual,
            state.stop_loss,
        )

        if not config.trading.simulation_mode:
            try:
                orden = client.create_market_sell_order(symbol, state.saldo_btc)
                logger.info("Orden de venta ejecutada: %s", orden.get("id", "N/A"))
            except Exception as e:
                logger.error("ERROR CRÍTICO ejecutando venta: %s", e)
                enviar_notificacion_telegram(
                    f"❌ ERROR CRÍTICO: No se pudo vender BTC: {e}",
                    config.telegram,
                )
                return False
        else:
            logger.info(
                "[SIMULACIÓN] Venta de %.6f BTC a $%.2f",
                state.saldo_btc,
                precio_actual,
            )

        state.saldo_usdt = state.saldo_btc * precio_actual
        ganancia_usdt, ganancia_pct = calcular_ganancia_con_stoploss(
            precio_actual, state.precio_compra, state.saldo_btc, state.atr_compra, tipo_salida
        )

        if ganancia_usdt >= 0:
            state.ganancias_totales += ganancia_usdt
        else:
            state.perdidas_totales += abs(ganancia_usdt)

        reg = crear_registro_csv(
            "VENTA",
            precio_actual,
            state.saldo_btc,
            state.saldo_usdt,
            ganancias=(ganancia_usdt, ganancia_pct),
        )
        _guardar_csv(reg, config.csv_file)

        trailing_msg = " (Break-Even)" if state.breakeven_activado else ""
        mensaje = (
            f"🔴 *VENTA{tipo_salida} EJECUTADA{trailing_msg}*\n\n"
            f"• *Precio Venta:* ${precio_actual:,.2f}\n"
            f"• *Precio Compra:* ${state.precio_compra:,.2f}\n"
            f"• *Stop-Loss:* ${state.stop_loss:,.2f}\n"
            f"• *Monto BTC:* {state.saldo_btc:.6f}\n"
            f"• *Total USDT:* ${state.saldo_usdt:,.2f}\n"
            f"• *Ganancia:* ${ganancia_usdt:,.2f} ({ganancia_pct:.2f}%)\n"
            f"• *Tipo:* {tipo_salida}\n"
            f"• *Trailing Stop:* {'✅ Break-Even' if state.breakeven_activado else '❌ Inactivo'}"
        )
        enviar_notificacion_telegram(mensaje, config.telegram)

        state.saldo_btc = 0.0
        state.breakeven_activado = False
        state.break_even_notificado = False
        state.stop_loss = 0.0

        return True

    distancia_sl = precio_actual - state.stop_loss
    logger.debug(
        "Stop-Loss vigente: $%.2f | Distancia: $%.2f (%.3f%%)",
        state.stop_loss,
        distancia_sl,
        (distancia_sl / state.stop_loss) * 100 if state.stop_loss > 0 else 0,
    )
    return False


def _guardar_csv(registro: dict, csv_file: str) -> None:
    """Guarda un registro en el archivo CSV."""
    df = pd.DataFrame([registro])
    header = not os.path.exists(csv_file)
    df.to_csv(csv_file, mode="a", header=header, index=False)
    logger.debug("Operación registrada en '%s'", csv_file)


# ---------------------------------------------------------------------------
# Bucle principal del bot
# ---------------------------------------------------------------------------

def run_bot() -> None:
    """Bucle principal del bot de trading."""
    config = load_config()
    state = BotState(saldo_usdt=config.trading.initial_balance_usdt)
    client = ExchangeClient(config)

    state.min_order_usdt = fetch_market_min_order(
        client, config.trading.symbol, config.trading.min_order_usdt
    )

    logger.info("Bot iniciado correctamente")
    logger.info(
        "Estrategia: Cruce EMA %d/%d + RSI + Volumen + ADX + MTF + Trailing Stop",
        config.strategy.ema_fast,
        config.strategy.ema_slow,
    )
    logger.info("Mínimo de orden: $%.2f USDT", state.min_order_usdt)

    running = True

    def shutdown_handler(signum: int, frame: object) -> None:
        nonlocal running
        logger.info("Señal %d recibida. Cerrando bot de forma segura...", signum)
        running = False

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    while running:
        try:
            df = client.fetch_ohlcv(
                config.trading.symbol,
                config.trading.timeframe,
                limit=config.trading.limit_ohlcv,
            )

            df_mtf = client.fetch_ohlcv(
                config.trading.symbol,
                config.trading.timeframe_mtf,
                limit=config.trading.limit_mtf,
            )

            df["ema_fast"] = calcular_ema(df["close"], period=config.strategy.ema_fast)
            df["ema_slow"] = calcular_ema(df["close"], period=config.strategy.ema_slow)
            df["rsi"] = calcular_rsi(df["close"], period=config.strategy.rsi_period)
            df["atr"] = calcular_atr(
                df["high"], df["low"], df["close"], period=config.strategy.atr_period
            )
            df["volume_usdt"] = df["volume"] * df["close"]
            df["volumen_promedio"] = df["volume"].rolling(
                window=config.strategy.volume_avg_window
            ).mean()
            df["volumen_promedio_usdt"] = df["volume_usdt"].rolling(
                window=config.strategy.volume_avg_window
            ).mean()
            df["adx"] = calcular_adx(
                df["high"], df["low"], df["close"], period=config.strategy.adx_period
            )

            current_price = float(df["close"].iloc[-1])
            current_ema_fast = float(df["ema_fast"].iloc[-1])
            current_ema_slow = float(df["ema_slow"].iloc[-1])
            current_rsi = float(df["rsi"].iloc[-1])
            current_volume_usdt = float(df["volume_usdt"].iloc[-2])
            current_vol_avg_usdt = float(df["volumen_promedio_usdt"].iloc[-1])
            current_atr = float(df["atr"].iloc[-1])
            current_adx = float(df["adx"].iloc[-1])

            prev_price = float(df["close"].iloc[-2])
            prev_ema_fast = float(df["ema_fast"].iloc[-2])
            prev_ema_slow = float(df["ema_slow"].iloc[-2])

            try:
                ticker = client.fetch_ticker(config.trading.symbol)
                precio_real = float(ticker["last"])
            except Exception as e:
                logger.warning("Error fetch_ticker, usando precio de vela: %s", e)
                precio_real = current_price

            if state.saldo_btc > 0:
                if evaluar_salida_posicion(
                    state, precio_real, config.trading.symbol, client, config
                ):
                    time.sleep(config.trading.loop_interval_seconds)
                    continue

            filtros = validar_filtros_cuantitativos(
                high=df["high"],
                low=df["low"],
                close=df["close"],
                df_mtf=df_mtf,
                atr_series=df["atr"],
                adx_threshold=config.strategy.adx_threshold,
            )

            logger.info("--- Estado del mercado ---")
            logger.info(
                "Precio: $%.2f (real: $%.2f) | EMA %d: $%.2f | EMA %d: $%.2f",
                current_price,
                precio_real,
                config.strategy.ema_fast,
                current_ema_fast,
                config.strategy.ema_slow,
                current_ema_slow,
            )
            logger.info(
                "RSI: %.1f | ATR: $%.2f | ADX: %.1f | Vol: $%.0f",
                current_rsi,
                current_atr,
                current_adx,
                current_volume_usdt,
            )
            logger.info(
                "Billetera: $%.2f USDT | %.4f BTC",
                state.saldo_usdt,
                state.saldo_btc,
            )
            logger.info(
                "Filtros: ADX=%s, MTF=%s, Horario=%s, Volatilidad=%s",
                filtros.adx_valido,
                filtros.ema_mtf_valido,
                filtros.horario_valido,
                filtros.volatilidad_valida,
            )
            logger.info(
                "Profit Factor: %.2f",
                calcular_profit_factor(state.ganancias_totales, state.perdidas_totales),
            )

            senial = evaluar_estrategia_multivariable(
                EstrategiaMultivariable(
                    precio_actual=current_price,
                    ema_9=current_ema_fast,
                    ema_21=current_ema_slow,
                    rsi=current_rsi,
                    volumen=current_volume_usdt,
                    volumen_promedio=current_vol_avg_usdt,
                    prev_precio=prev_price,
                    prev_ema_9=prev_ema_fast,
                    prev_ema_21=prev_ema_slow,
                )
            )

            if senial == "COMPRA" and state.saldo_usdt > 0:
                saldo_suficiente = state.saldo_usdt >= state.min_order_usdt

                logger.info("Evaluando filtros para COMPRA:")
                logger.info("  ADX > %.1f: %s", config.strategy.adx_threshold, filtros.adx_valido)
                logger.info("  MTF EMA 200: %s", filtros.ema_mtf_valido)
                logger.info("  Horario: %s", filtros.horario_valido)
                logger.info("  Volatilidad: %s", filtros.volatilidad_valida)
                logger.info(
                    "  Saldo mínimo: %s ($%.2f vs $%.2f)",
                    saldo_suficiente,
                    state.saldo_usdt,
                    state.min_order_usdt,
                )

                if not saldo_suficiente:
                    logger.warning(
                        "Saldo insuficiente para orden mínima ($%.2f USDT)",
                        state.min_order_usdt,
                    )

                if not saldo_suficiente or not filtros.todos_validos:
                    logger.info("COMPRA BLOQUEADA — filtros no cumplidos")
                    time.sleep(config.trading.loop_interval_seconds)
                    continue

                logger.info(
                    "SEÑAL DE COMPRA CONFIRMADA: Cruce EMA %d/%d, RSI=%.1f, Vol=$%.0f",
                    config.strategy.ema_fast,
                    config.strategy.ema_slow,
                    current_rsi,
                    current_volume_usdt,
                )

                monto_usdt = state.saldo_usdt
                state.saldo_btc = state.saldo_usdt / current_price
                state.precio_compra = current_price
                state.atr_compra = current_atr
                state.saldo_usdt = 0.0
                state.breakeven_activado = False
                state.break_even_notificado = False
                state.stop_loss = current_price - (current_atr * config.strategy.atr_sl_multiplier)
                state.hora_compra = time.strftime("%Y-%m-%d %H:%M:%S UTC")

                reg = crear_registro_csv("COMPRA", current_price, state.saldo_btc, monto_usdt)
                _guardar_csv(reg, config.csv_file)

                notificar_operacion(
                    "COMPRA",
                    current_price,
                    state.saldo_btc,
                    monto_usdt,
                    config=config.telegram,
                )

            elif senial == "VENTA" and state.saldo_btc > 0:
                logger.info(
                    "Señal VENTA detectada pero SL/BE ya evaluado. Posición mantenida."
                )
            else:
                if senial == "NEUTRAL":
                    logger.info("Mercado NEUTRAL - sin señal clara")
                elif senial == "COMPRA" and state.saldo_usdt <= 0:
                    logger.info("Señal COMPRA pero sin saldo USDT")
                else:
                    logger.info("Monitoreando mercado...")

            time.sleep(config.trading.loop_interval_seconds)

        except KeyboardInterrupt:
            logger.info("Interrupción de teclado recibida. Cerrando bot...")
            running = False
        except Exception as e:
            logger.error("Error en ejecución: %s", e, exc_info=True)
            time.sleep(config.trading.error_interval_seconds)

    logger.info("Bot cerrado de forma segura")


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

def main() -> None:
    """Punto de entrada principal del bot."""
    config = load_config()
    setup_logging(config.log_level)

    logger.info("Detectando entorno...")
    import os
    if "RENDER" in os.environ or "RENDER_EXTERNAL_URL" in os.environ:
        os.environ["USE_DEMO_ACCOUNT"] = "true"
        os.environ["MODO_SIMULACION"] = "true"
        logger.info("Modo demo activado (entorno Render)")

    server_thread = threading.Thread(
        target=start_health_server,
        args=(config.health_check_port,),
        daemon=True,
    )
    server_thread.start()

    logger.info("Iniciando bot de trading...")
    run_bot()


if __name__ == "__main__":
    main()
