"""Alertas de Telegram en formato HTML.

Envía mensajes de compra/venta/error usando la Bot API de Telegram. Las
peticiones HTTP se realizan con `aiohttp` de forma asíncrona, por lo que no
bloquean el event loop del bot.
"""

from __future__ import annotations

import asyncio
from typing import Optional

import aiohttp
from loguru import logger

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


class TelegramNotifier:
    """Cliente de notificaciones hacia un chat de Telegram."""

    def __init__(self, token: str, chat_id: str) -> None:
        self.token = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)

    async def send(self, html: str, parse_mode: str = "HTML") -> bool:
        """Envía un mensaje HTML al chat configurado."""
        if not self.enabled:
            logger.debug("Telegram no configurado, mensaje omitido.")
            return False

        url = TELEGRAM_API.format(token=self.token)
        payload = {
            "chat_id": self.chat_id,
            "text": html,
            "parse_mode": parse_mode,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error("Telegram error {}: {}", resp.status, body)
                        return False
                    return True
        except aiohttp.ClientError as exc:
            logger.error("Fallo de red enviando a Telegram: {}", exc)
            return False

    # ------------------------------------------------------- Mensajes útiles
    async def send_buy(self, mint: str, amount_sol: float, price: Optional[float] = None) -> bool:
        """Notifica una compra de token ejecutada."""
        price_txt = f"{price:,.10f}" if price else "N/D"
        html = (
            "<b>🟢 COMPRA EJECUTADA</b>\n\n"
            f"<b>Token:</b> <code>{mint}</code>\n"
            f"<b>Monto:</b> {amount_sol:.4f} SOL\n"
            f"<b>Precio:</b> {price_txt}"
        )
        return await self.send(html)

    async def send_sell(self, mint: str, amount_tokens: float, sol_received: float) -> bool:
        """Notifica una venta de token ejecutada."""
        html = (
            "<b>🔴 VENTA EJECUTADA</b>\n\n"
            f"<b>Token:</b> <code>{mint}</code>\n"
            f"<b>Cantidad:</b> {amount_tokens:,.6f}\n"
            f"<b>Recibido:</b> {sol_received:.4f} SOL"
        )
        return await self.send(html)

    async def send_error(self, message: str) -> bool:
        """Notifica un error no bloqueante."""
        html = f"<b>⚠️ ERROR</b>\n\n<code>{message}</code>"
        return await self.send(html)

    async def send_take_profit(self, mint: str, pnl_pct: float) -> bool:
        """Notifica un take-profit ejecutado."""
        html = (
            "<b>🟢 TAKE PROFIT EJECUTADO</b>\n\n"
            f"<b>Token:</b> <code>{mint}</code>\n"
            f"<b>Ganancia:</b> +{pnl_pct:.2f}%"
        )
        return await self.send(html)

    async def send_stop_loss(self, mint: str, pnl_pct: float) -> bool:
        """Notifica un stop-loss ejecutado."""
        html = (
            "<b>🔴 STOP LOSS EJECUTADO</b>\n\n"
            f"<b>Token:</b> <code>{mint}</code>\n"
            f"<b>Pérdida:</b> {pnl_pct:.2f}%"
        )
        return await self.send(html)

    async def send_trailing_stop(self, mint: str, pnl_pct: float) -> bool:
        """Notifica un trailing stop ejecutado (ganancia asegurada)."""
        html = (
            "<b>🛡️ TRAILING STOP EJECUTADO</b>\n\n"
            f"<b>Token:</b> <code>{mint}</code>\n"
            f"<b>Ganancia asegurada:</b> +{pnl_pct:.2f}%"
        )
        return await self.send(html)


    async def send_status(self, message: str) -> bool:
        """Envía un mensaje de estado/información genérico."""
        html = f"<b>ℹ️ ESTADO</b>\n\n{message}"
        return await self.send(html)

    async def start_heartbeat(self, interval_minutes: float = 30.0) -> None:
        """Envío periódico de heartbeat a Telegram.

        Cada `interval_minutes` (por defecto 30) notifica que el bot sigue
        activo y escuchando memecoins en tiempo real. Corre de forma
        concurrente con el bucle principal; usa `asyncio.sleep` para no
        bloquear el event loop.
        """
        interval_seconds = interval_minutes * 60.0
        logger.info("Heartbeat iniciado cada {:.0f} min.", interval_minutes)
        while True:
            await asyncio.sleep(interval_seconds)
            ok = await self.send_status("🟢 Bot activo | Escuchando memecoins en tiempo real")
            if not ok:
                logger.warning("Heartbeat no pudo enviarse a Telegram.")


def create_notifier(token: str, chat_id: str) -> TelegramNotifier:
    """Factory para instanciar notificador de Telegram."""
    return TelegramNotifier(token, chat_id)
