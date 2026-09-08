"""Alertas de Telegram en formato HTML.

Envía mensajes de compra/venta/error usando la Bot API de Telegram. Las
peticiones HTTP se realizan con `aiohttp` de forma asíncrona, por lo que no
bloquean el event loop del bot.
"""

from __future__ import annotations

import asyncio
import html
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
        """Envía un mensaje HTML al chat configurado.

        Si Telegram responde HTTP 400 (Bad Request: can't parse entities)
        mientras el mensaje se envía con `parse_mode="HTML"`, se reintenta
        inmediatamente en texto plano (`parse_mode=None`).
        """
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
            status, body = await self._post_text(url, payload)
        except aiohttp.ClientError as exc:
            logger.error("Fallo de red enviando a Telegram: {}", exc)
            return False

        if status == 200:
            return True

        # HTTP 400 habitualmente significa HTML inválido (p. ej. entidades sin
        # escapar): se reintenta en texto plano en lugar de descartar el aviso.
        if status == 400 and parse_mode == "HTML":
            logger.warning(
                "Telegram rechazó el HTML (400: {}). Reintentando en texto plano...",
                body,
            )
            payload["parse_mode"] = None
            try:
                status, body = await self._post_text(url, payload)
            except aiohttp.ClientError as exc:
                logger.error("Fallo de red enviando a Telegram: {}", exc)
                return False
            return status == 200

        logger.error("Telegram error {}: {}", status, body)
        return False

    @staticmethod
    async def _post_text(url: str, payload: dict[str, object]) -> tuple[int, str]:
        """Publica el payload en la Bot API y devuelve (status, body)."""
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                body = await resp.text()
                return resp.status, body

    # ------------------------------------------------------- Mensajes útiles
    async def send_buy(self, mint: str, amount_sol: float, price: Optional[float] = None) -> bool:
        """Notifica una compra de token ejecutada."""
        amount_txt = f"{amount_sol:.4f}"
        price_txt = f"{price:,.10f}" if price else "N/D"
        html_text = (
            "<b>🟢 COMPRA EJECUTADA</b>\n\n"
            f"<b>Token:</b> <code>{html.escape(str(mint))}</code>\n"
            f"<b>Monto:</b> {html.escape(amount_txt)} SOL\n"
            f"<b>Precio:</b> {html.escape(price_txt)}"
        )
        return await self.send(html_text)

    async def send_buy_notification(self, mint: str, amount_sol: float, price: Optional[float] = None) -> bool:
        """Alias de `send_buy` para compatibilidad con flujos de test."""
        return await self.send_buy(mint, amount_sol, price)

    async def send_sell(self, mint: str, amount_tokens: float, sol_received: float) -> bool:
        """Notifica una venta de token ejecutada."""
        amount_txt = f"{amount_tokens:,.6f}"
        sol_txt = f"{sol_received:.4f}"
        html_text = (
            "<b>🔴 VENTA EJECUTADA</b>\n\n"
            f"<b>Token:</b> <code>{html.escape(str(mint))}</code>\n"
            f"<b>Cantidad:</b> {html.escape(amount_txt)}\n"
            f"<b>Recibido:</b> {html.escape(sol_txt)} SOL"
        )
        return await self.send(html_text)

    async def send_error(self, message: str) -> bool:
        """Notifica un error no bloqueante."""
        safe_message = html.escape(str(message))
        html_text = f"<b>⚠️ ERROR</b>\n\n<code>{safe_message}</code>"
        return await self.send(html_text)

    async def send_take_profit(self, mint: str, pnl_pct: float) -> bool:
        """Notifica un take-profit ejecutado."""
        pnl_txt = f"+{pnl_pct:.2f}%"
        html_text = (
            "<b>🟢 TAKE PROFIT EJECUTADO</b>\n\n"
            f"<b>Token:</b> <code>{html.escape(str(mint))}</code>\n"
            f"<b>Ganancia:</b> {html.escape(pnl_txt)}"
        )
        return await self.send(html_text)

    async def send_stop_loss(self, mint: str, pnl_pct: float) -> bool:
        """Notifica un stop-loss ejecutado."""
        pnl_txt = f"{pnl_pct:.2f}%"
        html_text = (
            "<b>🔴 STOP LOSS EJECUTADO</b>\n\n"
            f"<b>Token:</b> <code>{html.escape(str(mint))}</code>\n"
            f"<b>Pérdida:</b> {html.escape(pnl_txt)}"
        )
        return await self.send(html_text)

    async def send_trailing_stop(self, mint: str, pnl_pct: float) -> bool:
        """Notifica un trailing stop ejecutado (ganancia asegurada)."""
        pnl_txt = f"+{pnl_pct:.2f}%"
        html_text = (
            "<b>🛡️ TRAILING STOP EJECUTADO</b>\n\n"
            f"<b>Token:</b> <code>{html.escape(str(mint))}</code>\n"
            f"<b>Ganancia asegurada:</b> {html.escape(pnl_txt)}"
        )
        return await self.send(html_text)


    async def send_status(self, message: str) -> bool:
        """Envía un mensaje de estado/información genérico."""
        safe_message = html.escape(str(message))
        html_text = f"<b>ℹ️ ESTADO</b>\n\n{safe_message}"
        return await self.send(html_text)

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
