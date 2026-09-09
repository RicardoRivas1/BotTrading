"""Motor de seguimiento y salida de posiciones compradas.

Consulta la cotización actual de cada token vía Jupiter v6, calcula el PnL
porcentual desde el precio de entrada y dispara la venta (take-profit o
stop-loss de emergencia con slippage alto) notificando a Telegram. Cuando
DRY_RUN=False, la venta se firma localmente con la PRIVATE_KEY y se envía a
la red de Solana.

El tracker mantiene su propia memoria de posiciones activas (`add_position` /
`remove_position`) que alimenta el bucle `monitor_positions()` en segundo
plano. `get_global_tracker()` expone la instancia compartida para que los
flujos de compra (p. ej. `core/websocket.py`) registren posiciones y el
punto de entrada de la app la ponga a monitorear.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from loguru import logger

# Intervalo del bucle de monitoreo de posiciones activas (segundos).
CHECK_INTERVAL_SEC = 2.0


@dataclass
class TrackerPosition:
    """Posición activa registrada en la memoria del tracker."""

    mint: str
    symbol: str
    buy_price: float
    amount: float  # SOL invertidos en la compra.
    created_at: float = field(default_factory=time.time)
    last_log_time: float = field(default_factory=time.time)


class PositionTracker:
    """Supervisa las posiciones activas y ejecuta las salidas TP/SL."""

    # Slippage alto para ventas de emergencia en stop-loss (20% = 2000 bps).
    EMERGENCY_SLIPPAGE_BPS = 2000

    def __init__(self, executor: Any, notifier: Any, config: Any) -> None:
        self.executor = executor
        self.notifier = notifier
        self.config = config
        # Memoría global de posiciones activas (independiente del executor).
        self.positions: dict[str, TrackerPosition] = {}

    # ------------------------------------------------------------- Público
    def add_position(
        self,
        mint: str,
        symbol: str,
        buy_price: float,
        amount: float,
    ) -> None:
        """Registra una posición activa para que el monitor la vigile."""
        self.positions[mint] = TrackerPosition(
            mint=mint,
            symbol=symbol,
            buy_price=buy_price,
            amount=amount,
        )

    def remove_position(self, mint: str) -> bool:
        """Elimina la posición; devuelve True si existía."""
        return self.positions.pop(mint, None) is not None

    def get_position(self, mint: str) -> Optional[TrackerPosition]:
        """Devuelve la posición registrada (o None)."""
        return self.positions.get(mint)

    async def start_monitoring(self) -> None:
        """Bucle de monitoreo en segundo plano; lánzalo con `asyncio.create_task`."""
        await self.monitor_positions()

    async def monitor_positions(self) -> None:
        """Revisa todas las posiciones activas cada CHECK_INTERVAL_SEC.

        Para cada posición obtiene el precio actual, calcula el PnL% y dispara
        la venta cuando se alcanza TAKE_PROFIT_PCT o STOP_LOSS_PCT, removiendo
        la posición del tracker tras ejecutar la salida.
        """
        logger.info("Monitoreo de posiciones iniciado (cada {:.0f}s)", CHECK_INTERVAL_SEC)
        try:
            while True:
                await asyncio.sleep(CHECK_INTERVAL_SEC)
                for mint in list(self.positions.keys()):
                    try:
                        await self._evaluate(mint)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001 - nunca detener el monitor
                        logger.warning("Error monitoreando {}: {}", mint, exc)
        except asyncio.CancelledError:
            logger.info("Monitoreo de posiciones detenido.")
            raise

    # ------------------------------------------------------- Evaluación
    async def _evaluate(self, mint: str) -> None:
        """Consulta precio y dispara TP/SL para una posición."""
        from core.websocket import process_sell_and_notify

        pos = self.positions.get(mint)
        if pos is None:
            return

        now = time.time()

        try:
            current_price = await self._get_current_price(mint)
        except Exception as exc:  # noqa: BLE001
            current_price = 0.0
            logger.warning("No se pudo obtener precio de {} ({}): {}", mint, pos.symbol, exc)

        # --- DRY_RUN: venta simulada si no hay precio tras 3 minutos ---
        if current_price <= 0 and self.config.trading.DRY_RUN:
            elapsed = now - pos.created_at
            if elapsed > 180:
                reason = random.choice(["TAKE_PROFIT", "STOP_LOSS"])
                if reason == "TAKE_PROFIT":
                    pnl = 110.0
                else:
                    pnl = -35.0
                logger.info(
                    "[DRY_RUN] ⚡ Venta simulada de {} ({}) — sin precio tras {:.0f}s | PnL simulado: {:+.2f}%",
                    mint, pos.symbol, elapsed, pnl,
                )
                ok = await process_sell_and_notify(
                    pos.mint, pos.symbol, reason=reason, pnl=pnl
                )
                if ok:
                    self.remove_position(mint)
                return
            else:
                logger.debug(
                    "Sin precio para {} ({}); esperando antes de simular ({:.0f}s / 180s).",
                    mint, pos.symbol, elapsed,
                )
                return

        if current_price <= 0:
            return

        if not pos.buy_price or pos.buy_price <= 0:
            logger.warning(
                "Precio de entrada inválido para {} ({}); adoptando precio actual.",
                mint, pos.symbol,
            )
            pos.buy_price = current_price
            return

        pnl_pct = (current_price - pos.buy_price) / pos.buy_price * 100.0
        take_profit_pct = float(self.config.trading.TAKE_PROFIT_PCT)
        stop_loss_pct = float(self.config.trading.STOP_LOSS_PCT)

        # --- Log periódico cada 15-30 segundos ---
        if now - pos.last_log_time >= 20:
            logger.info(
                "📊 Monitoreando {}: PnL actual {:+.2f}% (Precio: {:.10g} SOL)",
                pos.symbol, pnl_pct, current_price,
            )
            pos.last_log_time = now

        if take_profit_pct and pnl_pct >= take_profit_pct:
            logger.info("🎯 TAKE PROFIT (+{:.2f}%) para {} ({})", pnl_pct, mint, pos.symbol)
            ok = await process_sell_and_notify(
                pos.mint, pos.symbol, reason="TAKE_PROFIT", pnl=pnl_pct
            )
            if ok:
                self.remove_position(mint)
        elif stop_loss_pct and pnl_pct <= -stop_loss_pct:
            logger.info("🛑 STOP LOSS ({:.2f}%) para {} ({})", pnl_pct, mint, pos.symbol)
            ok = await process_sell_and_notify(
                pos.mint, pos.symbol, reason="STOP_LOSS", pnl=pnl_pct
            )
            if ok:
                self.remove_position(mint)

    async def _get_current_price(self, mint: str) -> float:
        """Precio actual vía el ejecutor (Jupiter → DexScreener → Pump.fun).

        Devuelve 0.0 si ningún endpoint pudo proporcionar precio real, para que
        el monitoreo detecte la ausencia de precio y (en DRY_RUN) genere la
        venta simulada tras el tiempo de espera.
        """
        try:
            return await self.executor.get_token_price(mint)
        except Exception as exc:  # noqa: BLE001 - sin precio en ningún endpoint
            logger.warning(
                "Sin precio real para {} tras agotar fallbacks ({}).",
                mint, exc,
            )
            return 0.0

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

        # Si falta un precio de entrada válido, se re-consulta el precio base
        # para no ignorar la posición ni provocar una división por cero.
        if not position.entry_price or position.entry_price <= 0:
            try:
                position.entry_price = await self.executor.get_token_price(token_mint)
            except Exception as exc:  # noqa: BLE001 - fallo de red no bloqueante
                logger.warning("No se pudo re-consultar precio de entrada de {}: {}", token_mint, exc)
                return "", 0.0
            logger.info("Precio de entrada re-establecido para {}: {:.10g}", token_mint, position.entry_price)

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


# -- Memoria global compartida ------------------------------------------------
# Instancia única del tracker usada por los flujos de compra (websocket) y el
# punto de entrada (bot.py). bot.py puede inyectar la suya con set_global_tracker.
_TRACKER: Optional[PositionTracker] = None


def set_global_tracker(tracker: PositionTracker) -> None:
    """Establece la instancia compartida del tracker (llamado por bot.py)."""
    global _TRACKER
    _TRACKER = tracker


def get_global_tracker() -> PositionTracker:
    """Devuelve el tracker global, construyéndolo bajo demanda si es necesario."""
    global _TRACKER
    if _TRACKER is None:
        from config import load_config
        from core.execution import JupiterExecutor
        from core.notifier import TelegramNotifier

        cfg = load_config()
        _TRACKER = PositionTracker(
            executor=JupiterExecutor(
                private_key=cfg.solana.PRIVATE_KEY,
                rpc_url=cfg.solana.HELIUS_RPC_URL,
                slippage_bps=cfg.trading.SLIPPAGE_BPS,
                buy_amount_sol=cfg.trading.BUY_AMOUNT_SOL,
                take_profit_pct=cfg.trading.TAKE_PROFIT_PCT,
                stop_loss_pct=cfg.trading.STOP_LOSS_PCT,
                trailing_activation_pct=cfg.trading.TRAILING_STOP_ACTIVATION_PCT,
                trailing_distance_pct=cfg.trading.TRAILING_STOP_DISTANCE_PCT,
                dry_run=cfg.trading.DRY_RUN,
            ),
            notifier=TelegramNotifier(
                token=cfg.telegram.TELEGRAM_TOKEN,
                chat_id=cfg.telegram.TELEGRAM_CHAT_ID,
            ),
            config=cfg,
        )
    return _TRACKER