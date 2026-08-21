import os
import requests
from dotenv import load_dotenv

# Carga variables desde un archivo .env si ejecutas de forma local
load_dotenv()


def probar_telegram():
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    print(f"TELEGRAM_TOKEN presente: {'Sí' if token else 'No'}")
    print(f"TELEGRAM_CHAT_ID presente: {'Sí' if chat_id else 'No'}")

    if not token or not chat_id:
        print("❌ Faltan las variables de entorno TELEGRAM_TOKEN o TELEGRAM_CHAT_ID.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": "🤖 *Prueba de Notificación*\n¡Las credenciales de Telegram funcionan correctamente en tu bot!",
        "parse_mode": "Markdown",
    }

    try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code == 200:
            print("✅ Mensaje enviado exitosamente. Revisa tu Telegram.")
        else:
            print(
                f"❌ Telegram respondió con error {response.status_code}: {response.text}"
            )
    except Exception as e:
        print(f"❌ Error de red al intentar conectar con Telegram: {e}")


if __name__ == "__main__":
    probar_telegram()
