"""Módulo de notificaciones Telegram.

Separado de la lógica de negocio para mantener separación de responsabilidades.
Maneja envío de mensajes con manejo de errores y logging.
"""

import logging
from typing import Optional

import requests

from config import TelegramConfig

logger = logging.getLogger(__name__)


def enviar_notificacion_telegram(mensaje: str, config: Optional[TelegramConfig] = None) -> bool:
    """Envía un mensaje a Telegram.

    Args:
        mensaje: Texto del mensaje a enviar.
        config: Configuración de Telegram. Si es None, se crea una nueva.

    Returns:
        True si el mensaje se envió correctamente, False en caso contrario.
    """
    if config is None:
        config = TelegramConfig()

    if not config.enabled:
        logger.debug("Telegram no configurado, omitiendo notificación")
        return False

    url = f"https://api.telegram.org/bot{config.token}/sendMessage"
    payload = {"chat_id": config.chat_id, "text": mensaje, "parse_mode": "Markdown"}

    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.debug("Notificación Telegram enviada exitosamente")
            return True
        else:
            logger.warning(
                "Telegram respondió con código %d: %s",
                response.status_code,
                response.text[:200],
            )
            return False
    except requests.exceptions.Timeout:
        logger.warning("Timeout al enviar notificación a Telegram")
        return False
    except requests.exceptions.ConnectionError:
        logger.warning("Error de conexión al enviar notificación a Telegram")
        return False
    except Exception as e:
        logger.error("Error inesperado al enviar notificación a Telegram: %s", e)
        return False


def notificar_operacion(
    tipo: str,
    precio: float,
    btc: float,
    usdt: float,
    ganancias: tuple = (0.0, 0.0),
    config: Optional[TelegramConfig] = None,
) -> bool:
    """Genera y envía la alerta visual a Telegram al ejecutar compra/venta.

    Args:
        tipo: "COMPRA" o "VENTA".
        precio: Precio de la operación.
        btc: Cantidad de BTC.
        usdt: Monto en USDT.
        ganancias: Tupla de (ganancia_usdt, ganancia_porcentaje).
        config: Configuración de Telegram.

    Returns:
        True si la notificación se envió correctamente.
    """
    ganancia_usdt, ganancia_pct = ganancias

    if tipo == "COMPRA":
        mensaje = (
            f"🟢 *ORDEN DE COMPRA EJECUTADA*\n\n"
            f"• *Precio BTC:* ${precio:,.2f}\n"
            f"• *Monto BTC:* {btc:.6f}\n"
            f"• *Total USDT:* ${usdt:,.2f}"
        )
    elif tipo == "VENTA":
        mensaje = (
            f"🔴 *ORDEN DE VENTA EJECUTADA*\n\n"
            f"• *Precio Venta:* ${precio:,.2f}\n"
            f"• *Monto BTC:* {btc:.6f}\n"
            f"• *Total USDT:* ${usdt:,.2f}\n"
            f"• *Ganancia USDT:* ${ganancia_usdt:,.2f}\n"
            f"• *Rendimiento:* {ganancia_pct:.2f}%"
        )
    else:
        logger.warning("Tipo de operación desconocido para notificación: %s", tipo)
        return False

    return enviar_notificacion_telegram(mensaje, config)
