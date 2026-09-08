"""Motor de seguimiento y salida de posiciones compradas.

Consulta la cotización actual de cada token vía Jupiter v6, calcula el PnL
porcentual desde el precio de entrada y dispara la venta (take-profit o
stop-loss de emergencia con slippage alto) notificando a Telegram. Cuando
DRY_RUN=False, la venta se firma localmente con la PRIVATE_KEY y se envía a
la red de Solana.
"""

from __future__ import annotations

from typing import Any, Optional

from loguru import logger


class PositionTracker:
    """Supervisa las posiciones del executor y ejecuta las salidas TP/SL."""

    # Slippage alto para ventas de emergencia en stop-loss (20% = 2000 bps).
    EMERGENCY_SLIPPAGE_BPS = 2000

    def __init__(self, executor: Any, notifier: Any, config: Any) -> None:
        self.executor = executor
        self.notifier = notifier
        self.config = config

    async def check_position(self, token_mint: str) -> tuple[str, float]:
        """Evalúa una posición y ejecuta la salida si corresponde.

        Obtiene la cotización actual vía Jupiter v6 y calcula la variación
        porcentual desde el precio de entrada:

        - PnL >= TAKE_PROFIT_PCT: venta inmediata y notifica
          "🎯 TAKE PROFIT (+X%)".
        - PnL <= -STOP_LOSS_PCT: venta de emergencia con slippage alto y
          notifica "🛑 STOP LOSS (-X%)".

        Returns:
            Una tupla (motivo, pnl_pct) donde `motivo` es "TAKE_PROFIT",
            "STOP_LOSS" o "" (sin salida).
        """
        position = self.executor.positions.get(token_mint)
        if position is None:
            return "", 0.0

        try:
            current_price = await self.executor.get_token_price(token_mint)
        except Exception as exc:  # noqa: BLE001 - fallo de red no bloqueante
            logger.warning("No se pudo consultar precio de {}: {}", token_mint, exc)
            return "", 0.0

        pnl_pct = (
            (current_price - position.entry_price) / position.entry_price * 100
        ) if position.entry_price else 0.0

        take_profit_pct = float(self.config.trading.TAKE_PROFIT_PCT)
        stop_loss_pct = float(self.config.trading.STOP_LOSS_PCT)

        if take_profit_pct and pnl_pct >= take_profit_pct:
            logger.info("🎯 TAKE PROFIT (+{:.2f}%) para {}", pnl_pct, token_mint)
            await self._close(token_mint, "TAKE_PROFIT", pnl_pct)
            await self.notifier.send_take_profit(token_mint, pnl_pct)
            return "TAKE_PROFIT", pnl_pct

        if stop_loss_pct and pnl_pct <= -stop_loss_pct:
            logger.info("🛑 STOP LOSS (-{:.2f}%) para {}", abs(pnl_pct), token_mint)
            await self._close(
                token_mint, "STOP_LOSS", pnl_pct,
                slippage_bps=self.EMERGENCY_SLIPPAGE_BPS,
            )
            await self.notifier.send_stop_loss(token_mint, pnl_pct)
            return "STOP_LOSS", pnl_pct

        return "", 0.0

    async def _close(
        self,
        token_mint: str,
        reason: str,
        pnl_pct: float,
        slippage_bps: Optional[int] = None,
    ) -> None:
        """Delega la venta (real o simulada) en el executor."""
        await self.executor.close_position(
            token_mint,
            reason,
            pnl_pct,
            slippage_bps=slippage_bps,
        )