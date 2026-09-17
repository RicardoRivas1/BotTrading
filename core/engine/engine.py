"""Motor de estrategias - orquesta el lifecycle de todas las estrategias activas.

El engine:
1. Registra estrategias al iniciar
2. Ejecuta start() en paralelo para todas
3. Distribuye eventos del mercado a las estrategias activas
4. Gestiona stop/pause/resume por nombre de estrategia
5. Expone estadisticas agregadas
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Optional

from loguru import logger

from core.engine.strategy import Strategy, StrategyState


class StrategyEngine:
    """Orquestador de estrategias de trading."""

    def __init__(self) -> None:
        self._strategies: dict[str, Strategy] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._event_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._running = False
        self._started_at = 0.0

    @property
    def strategies(self) -> dict[str, Strategy]:
        return dict(self._strategies)

    @property
    def running(self) -> bool:
        return self._running

    # ------------------------------------------------------------- Registro

    def register(self, strategy: Strategy) -> None:
        """Registra una estrategia en el engine."""
        name = strategy.name
        if name in self._strategies:
            logger.warning("Estrategia '{}' ya registrada; sobrescribiendo.", name)
        self._strategies[name] = strategy
        logger.info("Estrategia registrada: {}", name)

    def unregister(self, name: str) -> bool:
        """Remueve una estrategia del engine."""
        strategy = self._strategies.pop(name, None)
        if strategy is None:
            return False
        task = self._tasks.pop(name, None)
        if task and not task.done():
            task.cancel()
        logger.info("Estrategia removida: {}", name)
        return True

    # ------------------------------------------------------------- Lifecycle

    async def start_all(self) -> None:
        """Inicia todas las estrategias registradas en paralelo."""
        self._running = True
        self._started_at = time.time()

        for name, strategy in self._strategies.items():
            if not strategy.is_enabled:
                logger.info("Estrategia '{}' deshabilitada; saltando.", name)
                strategy._set_state(StrategyState.STOPPED)
                continue

            task = asyncio.create_task(self._run_strategy(strategy))
            self._tasks[name] = task

        logger.info(
            "Engine iniciado: {} estrategias activas",
            sum(1 for s in self._strategies.values() if s.is_enabled),
        )

    async def stop_all(self) -> None:
        """Detiene todas las estrategias ordenadamente."""
        self._running = False

        for name, strategy in self._strategies.items():
            try:
                await strategy.stop()
            except Exception as exc:
                logger.error("Error deteniendo '{}': {}", name, exc)

        for name, task in self._tasks.items():
            if not task.done():
                task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)

        self._tasks.clear()
        logger.info("Engine detenido.")

    async def _run_strategy(self, strategy: Strategy) -> None:
        """Ejecuta una estrategia en un task aislado."""
        name = strategy.name
        try:
            strategy._set_state(StrategyState.RUNNING)
            strategy._stats["started_at"] = time.time()
            await strategy.start()
        except asyncio.CancelledError:
            strategy._set_state(StrategyState.STOPPED)
            logger.info("Estrategia '{}' cancelada.", name)
        except Exception as exc:
            strategy._set_state(StrategyState.ERROR)
            logger.error("Estrategia '{}' fallo: {}", name, exc)
            strategy._inc_stat("trades_failed")

    # ------------------------------------------------------------- Eventos

    async def dispatch_event(self, event: dict[str, Any]) -> None:
        """Envia un evento a todas las estrategias activas."""
        for name, strategy in self._strategies.items():
            if strategy.is_running:
                try:
                    await strategy.on_event(event)
                except Exception as exc:
                    logger.warning(
                        "Error en estrategia '{}' procesando evento: {}",
                        name, exc,
                    )

    # ------------------------------------------------------------- Control individual

    async def start_strategy(self, name: str) -> bool:
        """Inicia una estrategia individual."""
        strategy = self._strategies.get(name)
        if not strategy:
            logger.warning("Estrategia '{}' no encontrada.", name)
            return False

        if strategy.is_running:
            return True

        task = asyncio.create_task(self._run_strategy(strategy))
        self._tasks[name] = task
        return True

    async def stop_strategy(self, name: str) -> bool:
        """Detiene una estrategia individual."""
        strategy = self._strategies.get(name)
        if not strategy:
            return False

        try:
            await strategy.stop()
        except Exception as exc:
            logger.error("Error deteniendo '{}': {}", name, exc)

        task = self._tasks.pop(name, None)
        if task and not task.done():
            task.cancel()
        return True

    async def pause_strategy(self, name: str) -> bool:
        strategy = self._strategies.get(name)
        if not strategy:
            return False
        await strategy.pause()
        return True

    async def resume_strategy(self, name: str) -> bool:
        strategy = self._strategies.get(name)
        if not strategy:
            return False
        await strategy.resume()
        return True

    # ------------------------------------------------------------- Stats

    def get_stats(self) -> dict[str, Any]:
        """Estadisticas agregadas de todas las estrategias."""
        return {
            "engine_running": self._running,
            "uptime_seconds": time.time() - self._started_at if self._started_at else 0,
            "total_strategies": len(self._strategies),
            "active_strategies": sum(
                1 for s in self._strategies.values() if s.is_running
            ),
            "strategies": {
                name: s.stats for name, s in self._strategies.items()
            },
        }
