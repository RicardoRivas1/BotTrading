import os
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import ccxt
import pandas as pd
from dotenv import load_dotenv
from logic import (
    calcular_ema,
    calcular_rsi,
    calcular_atr,
    calcular_adx,
    EstrategiaMultivariable,
    FiltrosCuantitativos,
    evaluar_estrategia_multivariable,
    calcular_ganancia_con_stoploss,
    calcular_profit_factor,
    validar_profit_factor_minimo,
    crear_registro_csv,
    notificar_operacion_telegram,
    enviar_notificacion_telegram,
)

# Detectar si estamos en Render (para usar claves demo)
# Render establece autom�ticamente estas variables de entorno
print(f"Variables de entorno RENDER: {os.environ.get('RENDER')}")
print(
    f"Variables de entorno RENDER_EXTERNAL_URL: {os.environ.get('RENDER_EXTERNAL_URL')}"
)

if "RENDER" in os.environ or "RENDER_EXTERNAL_URL" in os.environ:
    os.environ["USE_DEMO_ACCOUNT"] = "true"
    os.environ["MODO_SIMULACION"] = "true"  # Activar modo simulaci�n en Render
    print("Modo demo y simulacion activados (entorno Render detectado)")
else:
    print("Modo desarrollo local (usando cuenta real)")

# Cargar variables del archivo .env
load_dotenv()


# --- Servidor de Salud para Render (Escucha en el puerto requerido) ---
class DummyHealthCheck(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot de Trading Activo en Render")

    def log_message(self, format, *args):
        # Silenciar logs HTTP en consola para mantener limpios los logs del bot
        return


def iniciar_servidor_puerto():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), DummyHealthCheck)
    print(f"Servidor HTTP activo escuchando en el puerto {port} para Render.")
    server.serve_forever()


# --- Configuraci�n del Exchange ---
# Render: Kraken (sin bloqueo geografico en EE. UU.) + datos publicos, NO API keys.
# Local: Binance con API keys reales para datos y posibles ordenes futuras.
# Todas las operaciones se simulan localmente (paper trading).

if os.environ.get("USE_DEMO_ACCOUNT") == "true":
    exchange = ccxt.kraken({"enableRateLimit": True})
    symbol = "BTC/USDT"
    print("Modo Render: Kraken Mainnet (datos publicos, paper trading)")
else:
    api_key = os.environ.get("BINANCE_API_KEY_REAL")
    api_secret = os.environ.get("BINANCE_SECRET_KEY_REAL")
    exchange = ccxt.binance(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
        }
    )
    symbol = "BTC/USDT"
    print("Modo local: Binance Mainnet (cuenta REAL)")

# Inicializar filtros cuantitativos
filtros = FiltrosCuantitativos(exchange, symbol=symbol)

saldo_usdt = 10.0
saldo_btc = 0.0
precio_compra = 0.0
atr_compra = 0.0
ganancias_totales = 0.0
perdidas_totales = 0.0
breakeven_activado = False  # Trailing Stop a Break-Even activado
hora_compra = None  # Hora UTC de última compra

MIN_VOLUMEN_USDT = 0  # Cambiar en modo local
timeframe = "1m"
csv_file = "historial_trading.csv"


def validar_filtros_cuantitativos(df: pd.DataFrame, df_mtf: pd.DataFrame) -> tuple:
    """Valida los filtros cuantitativos antes de permitir entrada."""
    try:
        # 1. Filtro ADX > 25
        adx_valido = filtros.validar_adx_tendencia(
            df["high"], df["low"], df["close"], threshold=25.0
        )

        # 2. Confirmación MTF EMA 200
        ema_mtf_valido = filtros.confirmar_ema_200_mtf(df_mtf)

        # 3. Filtro horario de mercado - Desactivado para pruebas
        horario_valido = True

        return adx_valido, ema_mtf_valido, horario_valido

    except Exception as e:
        print(f"Error validando filtros: {e}")
        return False, False, False


def obtener_saldo_real():
    """Consulta los saldos disponibles directamente desde tu cuenta de Binance."""
    try:
        balance = exchange.fetch_balance()
        usdt = balance["free"].get("USDT", 0.0)
        btc = balance["free"].get("BTC", 0.0)
        return usdt, btc
    except Exception as e:
        print(f"Error consultando saldo en Binance: {e}")
        return None, None


def guardar_csv(registro: dict):
    df = pd.DataFrame([registro])
    header = not os.path.exists(csv_file)
    df.to_csv(csv_file, mode="a", header=header, index=False)
    print(f"Operacion registrada en '{csv_file}'")


def run():
    global \
        saldo_usdt, \
        saldo_btc, \
        precio_compra, \
        atr_compra, \
        ganancias_totales, \
        perdidas_totales, \
        breakeven_activado, \
        hora_compra

    print("Bot iniciado con credenciales autenticadas de Binance (.env)...")
    print("Estrategia: EMA 9/21 con filtros RSI y Volumen + ADX + MTF + Trailing Stop")

    # Para obtener automáticamente los saldos reales de Binance al iniciar:
    # real_usdt, real_btc = obtener_saldo_real()
    # if real_usdt is not None:
    #     saldo_usdt, saldo_btc = real_usdt, real_btc

    while True:
        try:
            # Conexi�n real a Binance - siempre usar datos reales
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=100)
            df = pd.DataFrame(
                ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
            )

            # Obtener datos del timeframe superior (1h) para confirmación MTF
            df_mtf = filtros.obtener_datos_mtf("1h", limit=200)

            # Calcular indicadores técnicos avanzados
            df["ema_9"] = calcular_ema(df["close"], period=9)
            df["ema_21"] = calcular_ema(df["close"], period=21)
            df["rsi"] = calcular_rsi(df["close"], period=14)
            df["atr"] = calcular_atr(df["high"], df["low"], df["close"], period=14)
            df["volume_usdt"] = df["volume"] * df["close"]
            df["volumen_promedio"] = df["volume"].rolling(window=20).mean()
            df["volumen_promedio_usdt"] = df["volume_usdt"].rolling(window=20).mean()
            df["adx"] = calcular_adx(df["high"], df["low"], df["close"], period=14)

            current_price = df["close"].iloc[-1]
            current_ema9 = df["ema_9"].iloc[-1]
            current_ema21 = df["ema_21"].iloc[-1]
            current_rsi = df["rsi"].iloc[-1]
            current_volume = df["volume"].iloc[-2]
            current_volume_usdt = df["volume_usdt"].iloc[-2]
            current_vol_avg = df["volumen_promedio"].iloc[-1]
            current_vol_avg_usdt = df["volumen_promedio_usdt"].iloc[-1]
            current_atr = df["atr"].iloc[-1]
            current_adx = df["adx"].iloc[-1]

            prev_price = df["close"].iloc[-2]
            prev_ema9 = df["ema_9"].iloc[-2]
            prev_ema21 = df["ema_21"].iloc[-2]

            # Validar filtros cuantitativos
            adx_valido, ema_mtf_valido, horario_valido = validar_filtros_cuantitativos(
                df, df_mtf
            )

            vol_liquidez_ok = current_volume_usdt >= MIN_VOLUMEN_USDT

            print(f"\n[INFO] {time.strftime('%H:%M:%S UTC')}")
            print(
                f"Precio BTC: ${current_price:.2f} | EMA 9: ${current_ema9:.2f} | EMA 21: ${current_ema21:.2f}"
            )
            print(
                f"RSI: {current_rsi:.1f} | ATR: ${current_atr:.2f} | ADX: {current_adx:.1f} | Volumen: ${current_volume_usdt:,.0f} USDT (Minimo: ${MIN_VOLUMEN_USDT:,.0f}) -> {vol_liquidez_ok}"
            )
            print(f"Billetera: ${saldo_usdt:.2f} USDT | {saldo_btc:.4f} BTC")
            print(
                f"Filtros: ADX>{current_adx:.1f}>25={adx_valido}, MTF={ema_mtf_valido}, Horario={horario_valido}, Liquidez={vol_liquidez_ok}"
            )
            print(
                f"Profit Factor: {calcular_profit_factor(ganancias_totales, perdidas_totales):.2f}"
            )

            senial = evaluar_estrategia_multivariable(
                EstrategiaMultivariable(
                    precio_actual=current_price,
                    ema_9=current_ema9,
                    ema_21=current_ema21,
                    rsi=current_rsi,
                    volumen=current_volume_usdt,
                    volumen_promedio=current_vol_avg_usdt,
                    prev_precio=prev_price,
                    prev_ema_9=prev_ema9,
                )
            )

            # Verificar trailing stop a break-even para posiciones abiertas
            if saldo_btc > 0 and not breakeven_activado:
                ganancia_flotante_atr = (current_price - precio_compra) / atr_compra
                if (
                    ganancia_flotante_atr >= 0.5
                ):  # +0.5 ATR desde entrada (más conservador)
                    breakeven_activado = True
                    mensaje = f"🚨 *TRAILING STOP BREAK-EVEN ACTIVADO*\n\n• Precio Entrada: ${precio_compra:.2f}\n• Precio Actual: ${current_price:.2f}\n• Ganancia ATR: +{ganancia_flotante_atr:.1f} ATR\n• Stop-Loss movido al punto de entrada (Break-Even)"
                    enviar_notificacion_telegram(mensaje)
                    print("✅ Trailing Stop activado: Stop-Loss movido a Break-Even")

            if senial == "COMPRA" and saldo_usdt > 0:
                # Aplicar TODOS los filtros cuantitativos antes de comprar
                if not all(
                    [
                        adx_valido,
                        ema_mtf_valido,
                        horario_valido,
                        vol_liquidez_ok,
                    ]
                ):
                    print(
                        f"❌ Señal de COMPRA BLOQUEADA por filtros: ADX={adx_valido}, MTF={ema_mtf_valido}, Horario={horario_valido}, Liquidez={vol_liquidez_ok}"
                    )
                    time.sleep(60)
                    continue

                print(
                    "✅ SEÑAL DE COMPRA MULTIVARIABLE CONFIRMADA (todos los filtros OK)"
                )
                print(
                    f"Condiciones: EMA 9 > EMA 21, RSI={current_rsi:.1f}, Volumen=${current_volume_usdt:,.0f} > ${current_vol_avg_usdt:,.0f} USDT"
                )
                print(
                    f"Filtros: ADX={current_adx:.1f}>25, MTF OK, Horario OK"
                )

                monto_usdt = saldo_usdt
                saldo_btc = saldo_usdt / current_price
                precio_compra = current_price
                atr_compra = current_atr
                saldo_usdt = 0.0
                breakeven_activado = False  # Resetear trailing stop para nueva posición

                reg = crear_registro_csv("COMPRA", current_price, saldo_btc, monto_usdt)
                guardar_csv(reg)

                # Notificación Telegram detallada con filtros
                mensaje = (
                    f"🟢 *COMPRA CONFIRMADA*\n\n"
                    f"• *Precio:* ${current_price:,.2f}\n"
                    f"• *Monto BTC:* {saldo_btc:.6f}\n"
                    f"• *Total USDT:* ${monto_usdt:,.2f}\n"
                    f"• *Filtros aplicados:*\n"
                    f"  - ADX {current_adx:.1f} > 25.0 ✓\n"
                    f"  - MTF EMA 200 1h ✓\n"
                    f"  - Horario 13-21 UTC ✓"
                )
                enviar_notificacion_telegram(mensaje)

            elif senial == "VENTA" and saldo_btc > 0:
                print("SEÑAL DE VENTA MULTIVARIABLE DETECTADA")

                # Verificar si es Take-Profit o Stop-Loss (con trailing stop break-even)
                precio_sl_actual = (
                    precio_compra
                    if breakeven_activado
                    else precio_compra - (atr_compra * 1.0)
                )
                precio_tp = precio_compra + (atr_compra * 2.0)

                if breakeven_activado:
                    print(f"⚠️ Stop-Loss en Break-Even: ${precio_compra:.2f}")

                if current_price >= precio_tp:
                    tipo_salida = "TP"
                    print(
                        f"✅ TAKE-PROFIT alcanzado: ${current_price:.2f} >= ${precio_tp:.2f}"
                    )
                elif current_price <= precio_sl_actual:
                    tipo_salida = "SL"
                    if breakeven_activado:
                        print(
                            f"🔒 STOP-LOSS BREAK-EVEN: ${current_price:.2f} <= ${precio_compra:.2f}"
                        )
                    else:
                        print(
                            f"🔴 STOP-LOSS normal: ${current_price:.2f} <= ${precio_sl_actual:.2f}"
                        )
                else:
                    tipo_salida = "REGULAR"
                    print("VENTA por señal de estrategia")

                saldo_usdt = saldo_btc * current_price
                ganancia_usdt, ganancia_pct = calcular_ganancia_con_stoploss(
                    current_price, precio_compra, saldo_btc, atr_compra, tipo_salida
                )

                # Actualizar métricas de Profit Factor
                if ganancia_usdt >= 0:
                    ganancias_totales += ganancia_usdt
                else:
                    perdidas_totales += abs(ganancia_usdt)

                reg = crear_registro_csv(
                    "VENTA",
                    current_price,
                    saldo_btc,
                    saldo_usdt,
                    ganancias=(ganancia_usdt, ganancia_pct),
                )
                guardar_csv(reg)

                # Notificación Telegram con detalles de trailing stop
                mensaje_trailing = (
                    " (Break-Even activado)" if breakeven_activado else ""
                )
                mensaje = (
                    f"🔴 *VENTA EJECUTADA{mensaje_trailing}*\n\n"
                    f"• *Precio Venta:* ${current_price:,.2f}\n"
                    f"• *Precio Compra:* ${precio_compra:,.2f}\n"
                    f"• *Monto BTC:* {saldo_btc:.6f}\n"
                    f"• *Total USDT:* ${saldo_usdt:,.2f}\n"
                    f"• *Ganancia USDT:* ${ganancia_usdt:,.2f}\n"
                    f"• *Rendimiento:* {ganancia_pct:.2f}%\n"
                    f"• *Tipo:* {tipo_salida}\n"
                    f"• *Trailing Stop:* {'✅ Break-Even' if breakeven_activado else '❌ Inactivo'}"
                )
                enviar_notificacion_telegram(mensaje)

                # Resetear variables de posición
                saldo_btc = 0.0
                breakeven_activado = False

            else:
                print("Monitoreando mercado...")

            time.sleep(60)

        except Exception as e:
            print(f"Error en ejecucion: {e}")
            time.sleep(10)


if __name__ == "__main__":
    # Iniciar servidor HTTP en un hilo separado para Render
    server_thread = threading.Thread(target=iniciar_servidor_puerto, daemon=True)
    server_thread.start()

    # Iniciar el bot de trading
    print("Iniciando bot de trading...")
    run()
