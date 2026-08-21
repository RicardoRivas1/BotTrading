import os
import ccxt
from dotenv import load_dotenv

load_dotenv()


def probar_binance():
    # Usar claves demo en producci�n y claves reales en desarrollo local
    if os.environ.get("USE_DEMO_ACCOUNT") == "true":
        api_key = os.getenv("BINANCE_API_KEY_DEMO")
        secret_key = os.getenv("BINANCE_SECRET_KEY_DEMO")
        print("Usando cuenta DEMO de Binance")
    else:
        api_key = os.getenv("BINANCE_API_KEY_REAL")
        secret_key = os.getenv("BINANCE_SECRET_KEY_REAL")
        print("Usando cuenta REAL de Binance")

    if not api_key or not secret_key:
        print(
            "❌ Error: No se encontraron BINANCE_API_KEY o BINANCE_SECRET_KEY en el archivo .env"
        )
        return

    # Inicializar la API de Binance con CCXT
    exchange = ccxt.binance(
        {
            "apiKey": api_key,
            "secret": secret_key,
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
        }
    )

    print("--- Verificando conexión con Binance ---")

    try:
        # Prueba 1: Lectura de datos públicos (Precio de BTC)
        ticker = exchange.fetch_ticker("BTC/USDT")
        print(
            f"✅ Conexión pública exitosa. Precio actual BTC/USDT: ${ticker['last']:,.2f}"
        )

        # Prueba 2: Autenticación privada (Consulta de saldo de la cuenta)
        balance = exchange.fetch_balance()
        usdt_disponible = balance.get("USDT", {}).get("free", 0.0)
        btc_disponible = balance.get("BTC", {}).get("free", 0.0)

        print("✅ Autenticación de API Key correcta.")
        print(f"   • Saldo USDT disponible: ${usdt_disponible:.2f}")
        print(f"   • Saldo BTC disponible:  {btc_disponible:.6f} BTC")

    except ccxt.AuthenticationError:
        print(
            "❌ Error de autenticación: Revisa que la API Key y Secret Key en el .env sean exactamente las de Binance."
        )
    except ccxt.PermissionDenied as e:
        print(
            f"❌ Permiso denegado por Binance. Verifica la restricción de IP o si falta el permiso de lectura/spot: {e}"
        )
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado: {e}")


if __name__ == "__main__":
    probar_binance()
