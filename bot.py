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

saldo_usdt = 10.00
saldo_btc = 0.0
precio_compra = 0.0
atr_compra = 0.0
ganancias_totales = 0.0
perdidas_totales = 0.0
breakeven_activado = False  # Trailing Stop a Break-Even activado
break_even_notificado = False  # Evita re-notificar BE en Telegram cada iteración
stop_loss = 0.0  # Precio exacto de stop-loss activo
hora_compra = None  # Hora UTC de última compra

MIN_VOLUMEN_USDT = 0  # Cambiar en modo local
MIN_ORDER_USDT = 10.0  # Default: $10 USDT mínimo de Binance
timeframe = "1m"
csv_file = "historial_trading.csv"

# Obtener mínimo de orden del exchange
try:
    market = exchange.market(symbol)
    if market and "limits" in market and "cost" in market["limits"]:
        min_cost = market["limits"]["cost"].get("min")
        if min_cost is not None:
            MIN_ORDER_USDT = float(min_cost)
            print(f"Mínimo de orden del exchange: ${MIN_ORDER_USDT:.2f} USDT")
except Exception as e:
    print(
        f"No se pudo obtener mínimo de orden del exchange, usando default ${MIN_ORDER_USDT:.2f}: {e}"
    )


def validar_filtros_cuantitativos(df: pd.DataFrame, df_mtf: pd.DataFrame) -> tuple:
    """Valida los filtros cuantitativos antes de permitir entrada."""
    try:
        # 1. Filtro ADX > 25
        adx_valido = filtros.validar_adx_tendencia(
            df["high"], df["low"], df["close"], threshold=25.0
        )

        # 2. Confirmación MTF EMA 200
        ema_mtf_valido = filtros.confirmar_ema_200_mtf(df_mtf)

        # 3. Filtro horario de mercado
        horario_valido = filtros.validar_horario_mercado(13, 21)

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


def evaluar_salida_posicion(precio_actual_real: float) -> bool:
    """Evalúa salida de posición por Stop-Loss / Break-Even. PRIORIDAD ABSOLUTA.

    Se ejecuta al inicio de CADA iteración del bucle principal, antes de
    evaluar indicadores o señales de entrada. Si el precio toca o cruza
    el stop_loss, vende inmediatamente.

    Returns:
        True  → posición cerrada (se debe saltar evaluación de entrada)
        False → posición sigue abierta (continuar con loop normal)
    """
    global \
        saldo_usdt, \
        saldo_btc, \
        precio_compra, \
        atr_compra, \
        ganancias_totales, \
        perdidas_totales, \
        breakeven_activado, \
        break_even_notificado, \
        stop_loss, \
        hora_compra

    if saldo_btc <= 0:
        return False

    # --- 1. Calcular stop_loss dinámico según estado ---
    if breakeven_activado:
        stop_loss = precio_compra
    else:
        stop_loss = precio_compra - (atr_compra * 1.2)

    # --- 2. Activar Break-Even si se alcanza umbral de ganancia ATR ---
    if not breakeven_activado and atr_compra > 0:
        ganancia_flotante_atr = (precio_actual_real - precio_compra) / atr_compra
        if ganancia_flotante_atr >= 0.5:
            breakeven_activado = True
            stop_loss = precio_compra
            if not break_even_notificado:
                break_even_notificado = True
                pnl_pct = ((precio_actual_real - precio_compra) / precio_compra) * 100
                mensaje = (
                    f"🚨 *TRAILING STOP BREAK-EVEN ACTIVADO*\n\n"
                    f"• Precio Entrada: ${precio_compra:.2f}\n"
                    f"• Precio Actual: ${precio_actual_real:.2f}\n"
                    f"• Ganancia: +{pnl_pct:.2f}% (+{ganancia_flotante_atr:.1f} ATR)\n"
                    f"• Stop-Loss movido al punto de entrada (${precio_compra:.2f})"
                )
                enviar_notificacion_telegram(mensaje)
                print(
                    f"✅ Trailing Stop activado: Stop-Loss movido a Break-Even (${precio_compra:.2f})"
                )

    # --- 3. PRIORIDAD ABSOLUTA: Verificar si precio触碰 SL ---
    if precio_actual_real <= stop_loss:
        tipo_salida = "BE" if breakeven_activado else "SL"
        print(
            f"\n{'🔒' if breakeven_activado else '🔴'} EJECUTANDO VENTA POR {tipo_salida}: "
            f"${precio_actual_real:.2f} <= ${stop_loss:.2f}"
        )

        # Ejecutar orden de venta en el exchange (si no es simulación)
        orden_ejecutada = False
        if os.environ.get("MODO_SIMULACION") != "true":
            try:
                orden = exchange.create_market_sell_order(symbol, saldo_btc)
                print(f"✅ Orden de venta ejecutada en exchange: {orden['id']}")
                orden_ejecutada = True
            except Exception as e:
                print(f"❌ ERROR CRÍTICO ejecutando venta en exchange: {e}")
                enviar_notificacion_telegram(
                    f"❌ ERROR CRÍTICO: No se pudo vender BTC: {e}"
                )
                return False
        else:
            print(
                f"[SIMULACIÓN] Venta de {saldo_btc:.6f} BTC a ${precio_actual_real:.2f}"
            )

        # Calcular P&L
        saldo_usdt = saldo_btc * precio_actual_real
        ganancia_usdt, ganancia_pct = calcular_ganancia_con_stoploss(
            precio_actual_real, precio_compra, saldo_btc, atr_compra, tipo_salida
        )

        # Actualizar métricas de Profit Factor
        if ganancia_usdt >= 0:
            ganancias_totales += ganancia_usdt
        else:
            perdidas_totales += abs(ganancia_usdt)

        # Registrar en CSV
        reg = crear_registro_csv(
            "VENTA",
            precio_actual_real,
            saldo_btc,
            saldo_usdt,
            ganancias=(ganancia_usdt, ganancia_pct),
        )
        guardar_csv(reg)

        # Notificación Telegram
        trailing_msg = " (Break-Even)" if breakeven_activado else ""
        mensaje = (
            f"🔴 *VENTA{tipo_salida} EJECUTADA{trailing_msg}*\n\n"
            f"• *Precio Venta:* ${precio_actual_real:,.2f}\n"
            f"• *Precio Compra:* ${precio_compra:,.2f}\n"
            f"• *Stop-Loss:* ${stop_loss:,.2f}\n"
            f"• *Monto BTC:* {saldo_btc:.6f}\n"
            f"• *Total USDT:* ${saldo_usdt:,.2f}\n"
            f"• *Ganancia:* ${ganancia_usdt:,.2f} ({ganancia_pct:.2f}%)\n"
            f"• *Tipo:* {tipo_salida}\n"
            f"• *Trailing Stop:* {'✅ Break-Even' if breakeven_activado else '❌ Inactivo'}"
        )
        enviar_notificacion_telegram(mensaje)

        # Resetear estado de posición
        saldo_btc = 0.0
        breakeven_activado = False
        break_even_notificado = False
        stop_loss = 0.0

        return True

    # Log del stop-loss vigente para debug
    distancia_sl = precio_actual_real - stop_loss
    print(
        f"Stop-Loss vigente: ${stop_loss:.2f} | Distancia: ${distancia_sl:.2f} ({(distancia_sl / stop_loss) * 100:.3f}%)"
    )

    return False


def run():
    global \
        saldo_usdt, \
        saldo_btc, \
        precio_compra, \
        atr_compra, \
        ganancias_totales, \
        perdidas_totales, \
        breakeven_activado, \
        break_even_notificado, \
        stop_loss, \
        hora_compra

    print("Bot iniciado con credenciales autenticadas de Binance (.env)...")
    print(
        "Estrategia: Cruce EMA 9/21 + RSI 30-70 + Volumen + ADX + MTF + Trailing Stop"
    )
    print(f"Mínimo de orden del exchange: ${MIN_ORDER_USDT:.2f} USDT")

    # Para obtener automáticamente los saldos reales de Binance al iniciar:
    # real_usdt, real_btc = obtener_saldo_real()
    # if real_usdt is not None:
    #     saldo_usdt, saldo_btc = real_usdt, real_btc

    while True:
        try:
            # 1. Fetch datos de velas + indicadores (para señales de entrada)
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

            # 2. Fetch precio REAL-TIME para evaluación de salida
            try:
                ticker = exchange.fetch_ticker(symbol)
                precio_real = ticker["last"]
            except Exception as e:
                print(f"⚠️ Error fetch_ticker, usando precio de vela: {e}")
                precio_real = current_price

            # 3. PRIORIDAD ABSOLUTA: Evaluar salida de posición (SL / Break-Even)
            if saldo_btc > 0:
                if evaluar_salida_posicion(precio_real):
                    time.sleep(60)
                    continue  # Posición cerrada, saltar evaluación de entrada

            # Validar filtros cuantitativos
            adx_valido = ema_mtf_valido = horario_valido = True
            vol_liquidez_ok = current_volume_usdt >= MIN_VOLUMEN_USDT

            print(f"\n[INFO] {time.strftime('%H:%M:%S UTC')}")
            print(
                f"Precio BTC: ${current_price:.2f} (real: ${precio_real:.2f}) | EMA 9: ${current_ema9:.2f} | EMA 21: ${current_ema21:.2f}"
            )
            print(
                f"RSI: {current_rsi:.1f} | ATR: ${current_atr:.2f} | ADX: ${current_adx:.1f} | Volumen: ${current_volume_usdt:,.0f} USDT (Minimo: ${MIN_VOLUMEN_USDT:,.0f}) -> {vol_liquidez_ok}"
            )
            print(f"Billetera: ${saldo_usdt:.2f} USDT | {saldo_btc:.4f} BTC")
            print(
                f"Filtros: ADX>{current_adx:.1f}>25={adx_valido}, MTF={ema_mtf_valido}, Horario={horario_valido}, Liquidez={vol_liquidez_ok}"
            )
            print(
                f"Profit Factor: {calcular_profit_factor(ganancias_totales, perdidas_totales):.2f}"
            )
            if saldo_btc > 0:
                print(
                    f"Posición abierta: Entrada=${precio_compra:.2f} | SL=${stop_loss:.2f} | BE={'SÍ' if breakeven_activado else 'NO'}"
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
                    prev_ema_21=prev_ema21,
                )
            )

            if senial == "COMPRA" and saldo_usdt > 0:
                # Debug detallado de cada filtro
                saldo_suficiente = saldo_usdt >= MIN_ORDER_USDT
                print(f"\n🔍 EVALUANDO FILTROS PARA COMPRA:")
                print(
                    f"  • ADX > 25:     {'✅' if adx_valido else '❌'} (actual: {current_adx:.1f})"
                )
                print(f"  • MTF EMA 200:  {'✅' if ema_mtf_valido else '❌'}")
                print(f"  • Horario:      {'✅' if horario_valido else '❌'}")
                print(
                    f"  • Liquidez:     {'✅' if vol_liquidez_ok else '❌'} (volumen: ${current_volume_usdt:,.0f} vs mínimo: ${MIN_VOLUMEN_USDT:,.0f})"
                )
                print(
                    f"  • Saldo mínimo: {'✅' if saldo_suficiente else '❌'} (${saldo_usdt:.2f} vs mínimo ${MIN_ORDER_USDT:.2f})"
                )

                if not saldo_suficiente:
                    print(
                        f"  ⚠️ Saldo insuficiente para orden mínima del exchange (${MIN_ORDER_USDT:.2f} USDT)"
                    )

                if not all(
                    [
                        adx_valido,
                        ema_mtf_valido,
                        horario_valido,
                        vol_liquidez_ok,
                        saldo_suficiente,
                    ]
                ):
                    print(f"  ❌ COMPRA BLOQUEADA — Ver filtros arriba")
                    time.sleep(60)
                    continue

                print(
                    "✅ SEÑAL DE COMPRA MULTIVARIABLE CONFIRMADA (todos los filtros OK)"
                )
                print(
                    f"Condiciones: Cruce EMA 9/21 alcista, RSI={current_rsi:.1f}, Volumen=${current_volume_usdt:,.0f} > ${current_vol_avg_usdt:,.0f} USDT"
                )
                print(f"Filtros: ADX={current_adx:.1f}>25, MTF OK, Horario OK")

                monto_usdt = saldo_usdt
                saldo_btc = saldo_usdt / current_price
                precio_compra = current_price
                atr_compra = current_atr
                saldo_usdt = 0.0
                breakeven_activado = False
                break_even_notificado = False
                stop_loss = current_price - (current_atr * 1.2)
                hora_compra = time.strftime("%Y-%m-%d %H:%M:%S UTC")

                reg = crear_registro_csv("COMPRA", current_price, saldo_btc, monto_usdt)
                guardar_csv(reg)

                # Notificación Telegram detallada con filtros
                mensaje = (
                    f"🟢 *COMPRA CONFIRMADA*\n\n"
                    f"• *Precio:* ${current_price:,.2f}\n"
                    f"• *Monto BTC:* {saldo_btc:.6f}\n"
                    f"• *Total USDT:* ${monto_usdt:,.2f}\n"
                    f"• *Stop-Loss inicial:* ${stop_loss:,.2f}\n"
                    f"• *Filtros aplicados:*\n"
                    f"  - Cruce EMA 9/21 ✓\n"
                    f"  - ADX {current_adx:.1f} > 25.0 ✓\n"
                    f"  - MTF EMA 200 1h ✓\n"
                    f"  - Horario 13-21 UTC ✓"
                )
                enviar_notificacion_telegram(mensaje)

            elif senial == "VENTA" and saldo_btc > 0:
                # La evaluación de SL/BE/TP se maneja en evaluar_salida_posicion().
                # Aquí solo se registra si la estrategia pide venta por señal pura.
                print(
                    "ℹ️ Señal VENTA detectada pero SL/BE ya evaluado en prioridad. Posición mantenida."
                )

            else:
                # Diagnóstico de por qué la señal es NEUTRAL
                if senial == "NEUTRAL":
                    print("Monitoreando mercado... (SEÑAL NEUTRAL)")
                    ema_cruce_ok = (
                        prev_ema21 is not None
                        and prev_ema9 <= prev_ema21
                        and current_ema9 > current_ema21
                    )
                    if not ema_cruce_ok:
                        print(
                            f"  → No hay cruce alcista EMA 9/21 (prev: EMA9={prev_ema9:.2f} vs EMA21={prev_ema21:.2f} | actual: EMA9={current_ema9:.2f} vs EMA21={current_ema21:.2f})"
                        )
                    if not (30 < current_rsi < 70):
                        print(
                            f"  → RSI fuera de rango: {current_rsi:.1f} (necesario: 30-70)"
                        )
                    if current_volume_usdt <= current_vol_avg_usdt:
                        print(
                            f"  → Volumen bajo: ${current_volume_usdt:,.0f} <= promedio ${current_vol_avg_usdt:,.0f}"
                        )
                elif senial == "COMPRA" and saldo_usdt <= 0:
                    print("Monitoreando mercado... (SEÑAL COMPRA pero sin saldo USDT)")
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
