"""Alertas de Telegram en formato HTML.

Envía mensajes de compra/venta/error usando la Bot API de Telegram. Las
peticiones HTTP se realizan con `aiohttp` de forma asíncrona, por lo que no
bloquean el event loop del bot.
"""

from __future__ import annotations

import asyncio
import html
import time
from typing import Any, Optional

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
        payload: dict[str, object] = {
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
    async def send_buy(
        self,
        mint: str,
        amount_sol: float,
        symbol: str = "N/A",
        score: Optional[float] = None,
        price: Optional[float] = None,
        price_unit: str = "SOL",
        dry_run: bool = True,
    ) -> bool:
        """Notifica una compra de token ejecutada.

        Muestra el ticker/símbolo del token en grande y enlaza a Pump.fun,
        DexScreener y Solscan. `symbol` y `mint` se escapan con `html.escape()`
        para evitar errores 400 de Telegram. `price` (con `price_unit` "SOL" o
        "USD") solo se muestra si está definido.
        """
        mode_txt = "DRY_RUN" if dry_run else "REAL"
        safe_symbol = html.escape(str(symbol).upper())
        safe_mint = html.escape(str(mint))
        amount_txt = html.escape(f"{amount_sol:.4f}")

        # Ticker/símbolo en grande y precio con formato coherente (SOL o USD).
        big_symbol = f"💎 ${safe_symbol}"
        if price and price > 0:
            if price_unit.upper() == "USD":
                price_txt = f"${price:.8f}"
            else:
                price_txt = f"{price:.9f} SOL"
        else:
            price_txt = "N/D"

        price_line = f"\n<b>Precio:</b> {html.escape(price_txt)}" if price and price > 0 else ""

        score_txt = html.escape(str(score)) if score is not None else "N/D"

        html_text = (
            f"🚀 <b>COMPRA EJECUTADA</b> [{mode_txt}]\n\n"
            f"{big_symbol}\n\n"
            f"<b>Token:</b> ${safe_symbol}\n"
            f"<code>{safe_mint}</code>\n"
            f"<b>Monto:</b> {amount_txt} SOL\n"
            f"<b>Score RugCheck:</b> {score_txt}"
            f"{price_line}\n\n"
            f'🔗 <a href="https://pump.fun/{safe_mint}">Pump.fun</a> | '
            f'<a href="https://dexscreener.com/solana/{safe_mint}">DexScreener</a> | '
            f'<a href="https://solscan.io/token/{safe_mint}">Solscan</a>'
        )
        return await self.send(html_text)

    async def send_buy_notification(
        self,
        mint: str,
        amount_sol: float,
        price: Optional[float] = None,
        symbol: str = "N/A",
        score: Optional[float] = None,
    ) -> bool:
        """Alias de `send_buy` para compatibilidad con flujos de test."""
        return await self.send_buy(mint, amount_sol, symbol=symbol, score=score, price=price)

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
            f"<b>🎯 TAKE PROFIT ({html.escape(pnl_txt)})</b>\n\n"
            f"<b>Token:</b> <code>{html.escape(str(mint))}</code>"
        )
        return await self.send(html_text)

    async def send_stop_loss(self, mint: str, pnl_pct: float) -> bool:
        """Notifica un stop-loss ejecutado."""
        pnl_txt = f"-{abs(pnl_pct):.2f}%"
        html_text = (
            f"<b>🛑 STOP LOSS ({html.escape(pnl_txt)})</b>\n\n"
            f"<b>Token:</b> <code>{html.escape(str(mint))}</code>"
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

    async def notify_position_progress(
        self,
        position: Any,
        *,
        max_hold_seconds: float = 0.0,
    ) -> bool:
        """Notifica el estado/progreso de una posición abierta en espera.

        Muestra el ticker y mint, el PnL% actual con emoji (📈 positivo,
        📉 negativo), el precio de entrada vs. el actual, el tiempo transcurrido
        frente al máximo de retención (⏱️) y el pico de PnL alcanzado.
        `position` puede ser un `TrackerPosition` o cualquier objeto con los
        atributos: symbol, mint, buy_price, current_price, latest_pnl_pct,
        highest_pnl_pct, created_at y opcionalmente max_hold_seconds.
        """
        safe_symbol = html.escape(str(getattr(position, "symbol", "N/A")).upper() or "N/A")
        safe_mint = html.escape(str(getattr(position, "mint", "") or ""))
        pnl_pct = float(getattr(position, "latest_pnl_pct", 0.0) or 0.0)
        highest_pnl = float(getattr(position, "highest_pnl_pct", 0.0) or 0.0)
        entry_price = float(getattr(position, "buy_price", 0.0) or 0.0)
        current_price = float(getattr(position, "current_price", 0.0) or 0.0)

        created_at = getattr(position, "created_at", None)
        elapsed = max(0.0, time.time() - float(created_at)) if created_at else 0.0
        max_hold = max_hold_seconds or float(getattr(position, "max_hold_seconds", 0.0) or 0.0)

        emoji = "📈" if pnl_pct >= 0 else "📉"
        entry_txt = html.escape(f"{entry_price:.10g}")
        current_txt = html.escape(f"{current_price:.10g}")
        elapsed_txt = html.escape(f"{int(elapsed)}s")
        max_hold_txt = html.escape(f"{int(max_hold)}s") if max_hold > 0 else "∞"
        highest_txt = html.escape(f"{highest_pnl:+.2f}%")

        html_text = (
            "💰 <b>PROGRESO DE POSICIÓN</b>\n\n"
            f"💎 ${safe_symbol}\n"
            f"<code>{safe_mint}</code>\n\n"
            f"{emoji} <b>PnL:</b> {html.escape(f'{pnl_pct:+.2f}%')}\n"
            f"<b>Entrada:</b> {entry_txt} SOL\n"
            f"<b>Actual:</b> {current_txt} SOL\n"
            f"<b>Máx:</b> {highest_txt}\n"
            f"⏱️ {elapsed_txt} / {max_hold_txt}"
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
