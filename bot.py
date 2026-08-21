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
    EstrategiaMultivariable,
    evaluar_estrategia_multivariable,
    calcular_ganancia_con_stoploss,
    calcular_profit_factor,
    validar_profit_factor_minimo,
    crear_registro_csv,
    notificar_operacion_telegram,
)

# Detectar si estamos en Render (para usar claves demo)
# Render establece autom�ticamente estas variables de entorno
print(f"Variables de entorno RENDER: {os.environ.get('RENDER')}")
print(f"Variables de entorno RENDER_EXTERNAL_URL: {os.environ.get('RENDER_EXTERNAL_URL')}")

if "RENDER" in os.environ or "RENDER_EXTERNAL_URL" in os.environ:
    os.environ["USE_DEMO_ACCOUNT"] = "true"
    os.environ["MODO_SIMULACION"] = "true"  # Activar modo simulaci�n en Render
    print("✅ Modo demo y simulaci�n activados (entorno Render detectado)")
else:
    print("🔧 Modo desarrollo local (usando cuenta real)")

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


# --- Configuraci�n del Exchange Binance con credenciales de .env ---
# Usar claves demo en producci�n (Render) y claves reales en desarrollo local
if os.environ.get("USE_DEMO_ACCOUNT") == "true":
    # Estamos en Render - usar cuenta demo
    api_key = os.environ.get("BINANCE_API_KEY_DEMO")
    api_secret = os.environ.get("BINANCE_SECRET_KEY_DEMO")
    print("🔧 Usando cuenta DEMO de Binance")
else:
    # Estamos en desarrollo local - usar cuenta real
    api_key = os.environ.get("BINANCE_API_KEY_REAL")
    api_secret = os.environ.get("BINANCE_SECRET_KEY_REAL")
    print("🔧 Usando cuenta REAL de Binance")

exchange = ccxt.binance(
    {
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
        "options": {
            "defaultType": "spot",
        },
    }
)

# Si deseas usar Binance Testnet, define BINANCE_TESTNET=true en tu .env
# O usar testnet autom�ticamente en modo demo
if os.environ.get("BINANCE_TESTNET", "false").lower() == "true" or os.environ.get("USE_DEMO_ACCOUNT") == "true":
    exchange.set_sandbox_mode(True)
    print("🔧 Modo testnet/sandbox activado")

saldo_usdt = 1000.0
saldo_btc = 0.0
precio_compra = 0.0
atr_compra = 0.0
ganancias_totales = 0.0
perdidas_totales = 0.0

symbol = "BTC/USDT"
timeframe = "1m"
csv_file = "historial_trading.csv"


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
        perdidas_totales

    print("Bot iniciado con credenciales autenticadas de Binance (.env)...")
    print(
        "Estrategia: EMA 9/21 con filtros RSI y Volumen, Stop-Loss/Take-Profit dinamico con ATR"
    )

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

            # Calcular indicadores técnicos
            df["ema_9"] = calcular_ema(df["close"], period=9)
            df["ema_21"] = calcular_ema(df["close"], period=21)
            df["rsi"] = calcular_rsi(df["close"], period=14)
            df["atr"] = calcular_atr(df["high"], df["low"], df["close"], period=14)
            df["volumen_promedio"] = df["volume"].rolling(window=20).mean()

            current_price = df["close"].iloc[-1]
            current_ema9 = df["ema_9"].iloc[-1]
            current_ema21 = df["ema_21"].iloc[-1]
            current_rsi = df["rsi"].iloc[-1]
            current_volume = df["volume"].iloc[-1]
            current_vol_avg = df["volumen_promedio"].iloc[-1]
            current_atr = df["atr"].iloc[-1]

            prev_price = df["close"].iloc[-2]
            prev_ema9 = df["ema_9"].iloc[-2]
            prev_ema21 = df["ema_21"].iloc[-2]

            print(f"\n[INFO] {time.strftime('%H:%M:%S')}")
            print(
                f"Precio BTC: ${current_price:.2f} | EMA 9: ${current_ema9:.2f} | EMA 21: ${current_ema21:.2f}"
            )
            print(
                f"RSI: {current_rsi:.1f} | ATR: ${current_atr:.2f} | Volumen: {current_volume:.0f}"
            )
            print(f"Billetera: ${saldo_usdt:.2f} USDT | {saldo_btc:.4f} BTC")
            print(
                f"Profit Factor actual: {calcular_profit_factor(ganancias_totales, perdidas_totales):.2f}"
            )

            senial = evaluar_estrategia_multivariable(
                EstrategiaMultivariable(
                    precio_actual=current_price,
                    ema_9=current_ema9,
                    ema_21=current_ema21,
                    rsi=current_rsi,
                    volumen=current_volume,
                    volumen_promedio=current_vol_avg,
                    prev_precio=prev_price,
                    prev_ema_9=prev_ema9,
                )
            )

            if senial == "COMPRA" and saldo_usdt > 0:
                print("SEÑAL DE COMPRA MULTIVARIABLE DETECTADA")
                print(
                    f"Condiciones: EMA 9 > EMA 21, RSI={current_rsi:.1f}, Volumen={current_volume:.0f} > {current_vol_avg:.0f}"
                )

                monto_usdt = saldo_usdt
                saldo_btc = saldo_usdt / current_price
                precio_compra = current_price
                atr_compra = current_atr
                saldo_usdt = 0.0

                reg = crear_registro_csv("COMPRA", current_price, saldo_btc, monto_usdt)
                guardar_csv(reg)

                # Envío de notificación a Telegram
                notificar_operacion_telegram(
                    "COMPRA", current_price, saldo_btc, monto_usdt
                )

            elif senial == "VENTA" and saldo_btc > 0:
                print("SEÑAL DE VENTA MULTIVARIABLE DETECTADA")

                # Verificar si es Take-Profit o Stop-Loss
                precio_tp = precio_compra + (atr_compra * 2.0)
                precio_sl = precio_compra - (atr_compra * 1.0)

                if current_price >= precio_tp:
                    tipo_salida = "TP"
                    print(
                        f"TAKE-PROFIT alcanzado: ${current_price:.2f} >= ${precio_tp:.2f}"
                    )
                elif current_price <= precio_sl:
                    tipo_salida = "SL"
                    print(
                        f"STOP-LOSS activado: ${current_price:.2f} <= ${precio_sl:.2f}"
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

                # Envío de notificación a Telegram
                notificar_operacion_telegram(
                    "VENTA",
                    current_price,
                    saldo_btc,
                    saldo_usdt,
                    ganancias=(ganancia_usdt, ganancia_pct),
                )

                saldo_btc = 0.0

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
