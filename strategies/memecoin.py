"""Estrategia de snipeo de memecoins en Solana.

Monitorea el feed de nuevos tokens via PumpPortal WebSocket, valida
seguridad via RugCheck y ejecuta compras automaticas.

Esta es la estrategia original del bot, ahora encapsulada como modulo
independiente dentro del framework.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from loguru import logger

from core.engine.strategy import Strategy, StrategyState
from core.websocket import (
    TokenWebSocket,
    check_liquidity,
    check_rugcheck,
    create_listener,
    resolve_symbol,
)


class MemecoinSniper(Strategy):
    """Estrategia de snipeo de memecoins nuevos.

    Escucha el WebSocket de PumpPortal para detectar tokens nuevos,
    valida seguridad con RugCheck y ejecuta compra si pasa los filtros.
    """

    def __init__(
        self,
        executor: Any,
        notifier: Any,
        tracker: Any,
        config: Any,
    ) -> None:
        super().__init__(executor, notifier, tracker, config)
        self.listener: TokenWebSocket = create_listener()
        self._process_task: asyncio.Task | None = None

    @property
    def name(self) -> str:
        return "MemecoinSniper"

    async def start(self) -> None:
        """Inicia el listener de nuevos tokens."""
        logger.info("MemecoinSniper: iniciando listener de nuevos tokens...")
        self._set_state(StrategyState.RUNNING)
        self._process_task = asyncio.create_task(self._listen_loop())

    async def stop(self) -> None:
        """Detiene el listener."""
        logger.info("MemecoinSniper: deteniendo...")
        self.listener.stop()
        if self._process_task and not self._process_task.done():
            self._process_task.cancel()
            try:
                await self._process_task
            except asyncio.CancelledError:
                pass
        self._set_state(StrategyState.STOPPED)

    async def _listen_loop(self) -> None:
        """Bucle principal: escucha eventos y los procesa."""
        try:
            # Lanzar el listener en background
            listener_task = asyncio.create_task(self.listener.run())

            async for event in self.listener.events():
                mint = (
                    event.get("mint")
                    or event.get("token", {}).get("mint")
                    or event.get("address")
                )
                if not mint:
                    continue

                ticker = (
                    event.get("symbol")
                    or event.get("ticker")
                    or event.get("token", {}).get("symbol")
                    or event.get("token", {}).get("ticker")
                    or "N/A"
                )

                self._inc_stat("signals_received")
                asyncio.create_task(self._process_token(mint, ticker))

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("MemecoinSniper: error en listen loop: {}", exc)
            self._set_state(StrategyState.ERROR)

    async def _process_token(self, mint: str, ticker: str = "N/A") -> None:
        """Flujo completo: validar -> comprar si es seguro."""
        logger.info("MemecoinSniper: procesando {} ({})", mint, ticker)

        # FORCE_TEST_BUY
        if os.getenv("FORCE_TEST_BUY", "False").lower() == "true":
            logger.info("MemecoinSniper: FORCE_TEST_BUY para {} ({})", mint, ticker)
            if not await check_liquidity(mint):
                os.environ["FORCE_TEST_BUY"] = "False"
                return
            try:
                await self._execute_buy(mint, ticker)
            except Exception as exc:
                logger.error("MemecoinSniper: error en force buy: {}", exc)
            os.environ["FORCE_TEST_BUY"] = "False"
            return

        # Validacion de seguridad
        try:
            score = await check_rugcheck(mint)
            max_score = float(os.getenv("RUGCHECK_MAX_SCORE", "10000"))
            logger.info("MemecoinSniper: score RugCheck para {}: {}", mint, score)

            if score == 0:
                logger.warning("MemecoinSniper: token con score 0, omitiendo {}", mint)
                return

            if score <= max_score:
                if not await check_liquidity(mint):
                    return
                logger.info(
                    "MemecoinSniper: token APROBADO (score {} <= {}) para {}",
                    score, max_score, mint,
                )
                await self._execute_buy(mint, ticker, score=score)
            else:
                logger.info(
                    "MemecoinSniper: token RECHAZADO (score {} > {}) para {}",
                    score, max_score, mint,
                )
        except Exception as exc:
            logger.error("MemecoinSniper: error evaluando {}: {}", mint, exc)

    async def _execute_buy(
        self, mint: str, symbol: str = "N/A", score: float | None = None
    ) -> None:
        """Ejecuta la compra y notifica."""
        from core.websocket import process_buy_and_notify

        await process_buy_and_notify(mint, symbol, score=score)
        self._inc_stat("trades_executed")
