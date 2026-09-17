"""Estrategia de Dollar Cost Averaging (DCA) en Solana.

Ejecuta compras periodicas de un token con intervalo configurable.
Util para acumular posiciones a largo plazo sin intentar predecir el timing.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from loguru import logger

from core.engine.strategy import Strategy, StrategyState


@dataclass
class DCAPosition:
    """Posicion DCA activa."""

    token_mint: str
    symbol: str
    interval_seconds: float
    amount_sol: float
    total_invested_sol: float = 0.0
    total_buys: int = 0
    last_buy_at: float = 0.0
    created_at: float = field(default_factory=time.time)
    enabled: bool = True


class DCAStrategy(Strategy):
    """Estrategia de DCA periodico.

    Configuracion via .env:
        DCA_TOKENS=MINT1,MINT2,MINT3
        DCA_AMOUNT_SOL=0.005
        DCA_INTERVAL_SECONDS=3600
    """

    def __init__(
        self,
        executor: Any,
        notifier: Any,
        tracker: Any,
        config: Any,
    ) -> None:
        super().__init__(executor, notifier, tracker, config)
        self._positions: dict[str, DCAPosition] = {}
        self._monitor_task: asyncio.Task | None = None
        self._load_config()

    @property
    def name(self) -> str:
        return "DCA"

    def _load_config(self) -> None:
        """Carga configuracion de DCA desde variables de entorno."""
        import os

        tokens_str = os.getenv("DCA_TOKENS", "")
        amount_sol = float(os.getenv("DCA_AMOUNT_SOL", "0.005"))
        interval_seconds = float(os.getenv("DCA_INTERVAL_SECONDS", "3600"))

        if tokens_str:
            tokens = [t.strip() for t in tokens_str.split(",") if t.strip()]
            for mint in tokens:
                self._positions[mint] = DCAPosition(
                    token_mint=mint,
                    symbol=mint[:6].upper(),
                    interval_seconds=interval_seconds,
                    amount_sol=amount_sol,
                )
                logger.info(
                    "DCA: posicion configurada: {} | {} SOL cada {:.0f}s",
                    mint[:8] + "...", amount_sol, interval_seconds,
                )

    async def start(self) -> None:
        """Inicia el monitoreo de DCA."""
        self._set_state(StrategyState.RUNNING)
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info(
            "DCA: iniciado ({} posiciones configuradas)",
            len(self._positions),
        )

    async def stop(self) -> None:
        """Detiene el monitoreo."""
        self._set_state(StrategyState.STOPPED)
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("DCA: detenido")

    async def _monitor_loop(self) -> None:
        """Bucle principal: verifica intervalos y ejecuta compras."""
        try:
            while self.is_running:
                await asyncio.sleep(10)  # Check cada 10 segundos

                now = time.time()
                for mint, pos in self._positions.items():
                    if not pos.enabled:
                        continue

                    time_since_last = now - pos.last_buy_at if pos.last_buy_at > 0 else float("inf")
                    if time_since_last >= pos.interval_seconds:
                        await self._execute_dca_buy(pos)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("DCA: error en monitor loop: {}", exc)
            self._set_state(StrategyState.ERROR)

    async def _execute_dca_buy(self, pos: DCAPosition) -> None:
        """Ejecuta una compra DCA periodica."""
        logger.info(
            "DCA: ejecutando compra de {} | {} SOL",
            pos.token_mint[:8] + "...", pos.amount_sol,
        )

        try:
            # Override temporal del buy_amount
            original_amount = self.executor.buy_amount_sol
            self.executor.buy_amount_sol = pos.amount_sol
            try:
                sig = await self.executor.buy_token(pos.token_mint)
            finally:
                self.executor.buy_amount_sol = original_amount

            if sig is None:
                logger.warning(
                    "DCA: compra de {} omitida (sin liquidez)",
                    pos.token_mint[:8] + "...",
                )
                return

            # Actualizar estado
            pos.last_buy_at = time.time()
            pos.total_buys += 1
            pos.total_invested_sol += pos.amount_sol
            self._inc_stat("trades_executed")

            # Obtener symbol
            try:
                symbol = await self.executor.get_token_symbol(pos.token_mint)
                pos.symbol = symbol
            except Exception:
                symbol = pos.symbol

            # Registrar en tracker
            entry_price = 0.0
            position = self.executor.positions.get(pos.token_mint)
            if position and position.entry_price and position.entry_price > 0:
                entry_price = position.entry_price

            self.tracker.add_position(
                mint=pos.token_mint,
                symbol=symbol,
                buy_price=entry_price,
                amount=pos.amount_sol,
            )

            # Notificar
            await self.notifier.send_buy(
                pos.token_mint,
                pos.amount_sol,
                symbol=symbol,
                dry_run=self.config.trading.DRY_RUN,
            )

            logger.success(
                "DCA: compra #{} de {} | {} SOL | Total invertido: {:.4f} SOL",
                pos.total_buys, symbol,
                pos.amount_sol, pos.total_invested_sol,
            )

        except Exception as exc:
            self._inc_stat("trades_failed")
            logger.error("DCA: error en compra de {}: {}", pos.token_mint[:8], exc)
            await self.notifier.send_error(
                f"DCA: error comprando {pos.token_mint[:8]}...: {exc}"
            )

    # ----------------------------------------------------------- Public API

    def add_token(self, mint: str, amount_sol: float = 0.005, interval_seconds: float = 3600) -> None:
        """Agrega un token para DCA periodico."""
        if mint in self._positions:
            logger.warning("DCA: token {} ya esta en DCA", mint[:8])
            return

        self._positions[mint] = DCAPosition(
            token_mint=mint,
            symbol=mint[:6].upper(),
            interval_seconds=interval_seconds,
            amount_sol=amount_sol,
        )
        logger.info(
            "DCA: token agregado: {} | {} SOL cada {:.0f}s",
            mint[:8] + "...", amount_sol, interval_seconds,
        )

    def remove_token(self, mint: str) -> bool:
        """Remueve un token del DCA."""
        pos = self._positions.pop(mint, None)
        if pos:
            logger.info("DCA: token removido: {}", mint[:8])
            return True
        return False

    def get_stats(self) -> dict[str, Any]:
        """Estadisticas del DCA."""
        total_invested = sum(p.total_invested_sol for p in self._positions.values())
        total_buys = sum(p.total_buys for p in self._positions.values())
        return {
            **self.stats,
            "total_positions": len(self._positions),
            "total_invested_sol": total_invested,
            "total_buys": total_buys,
        }
