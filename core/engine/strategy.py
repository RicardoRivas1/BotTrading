"""Clase base abstracta para estrategias de trading.

Toda estrategia debe heredar de Strategy e implementar los metodos
obligatorios (start, stop). Opcionalmente puede implementar
on_event para reaccionar a eventos del mercado en tiempo real.
"""

from __future__ import annotations

import abc
import enum
from dataclasses import dataclass
from typing import Any


class StrategyState(enum.Enum):
    """Estados posibles de una estrategia."""

    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class StrategyConfig:
    """Configuracion base para todas las estrategias."""

    enabled: bool = True
    name: str = ""
    max_positions: int = 3
    max_sol_per_trade: float = 0.01
    dry_run: bool = False


class Strategy(abc.ABC):
    """Interfaz base para estrategias de trading.

    Lifecycle:
        1. __init__(): inyectar dependencias (executor, notifier, config)
        2. start(): estrategia comienza a operar
        3. on_event(): recibe eventos del mercado (opcional)
        4. stop(): estrategia se detiene ordenadamente

    Las estrategias NO deben crear sus propias instancias de executor/notifier.
    Estos se inyectan desde el engine para mantener un solo punto de verdad.
    """

    def __init__(
        self,
        executor: Any,
        notifier: Any,
        tracker: Any,
        config: Any,
    ) -> None:
        self.executor = executor
        self.notifier = notifier
        self.tracker = tracker
        self.config = config
        self._state = StrategyState.IDLE
        self._stats: dict[str, Any] = {
            "signals_received": 0,
            "trades_executed": 0,
            "trades_failed": 0,
            "started_at": 0.0,
        }

    @property
    def state(self) -> StrategyState:
        return self._state

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def is_running(self) -> bool:
        return self._state == StrategyState.RUNNING

    @property
    def is_enabled(self) -> bool:
        """Override en subclases para definir si la estrategia esta habilitada."""
        return True

    @property
    def stats(self) -> dict[str, Any]:
        return {**self._stats, "state": self._state.value, "name": self.name}

    # ------------------------------------------------------------- Lifecycle

    @abc.abstractmethod
    async def start(self) -> None:
        """Inicia la estrategia. Llamado por el engine."""
        ...

    @abc.abstractmethod
    async def stop(self) -> None:
        """Detiene la estrategia ordenadamente. Llamado por el engine."""
        ...

    async def pause(self) -> None:
        """Pausa la estrategia (opcional)."""
        self._state = StrategyState.PAUSED

    async def resume(self) -> None:
        """Reanuda la estrategia pausada (opcional)."""
        if self._state == StrategyState.PAUSED:
            self._state = StrategyState.RUNNING

    # ------------------------------------------------------------- Eventos

    async def on_event(self, event: dict[str, Any]) -> None:
        """Recibe un evento del mercado. Override en subclases.

        Eventos posibles:
            - {"type": "new_token", "mint": "...", "symbol": "..."}
            - {"type": "price_update", "mint": "...", "price": 0.0}
            - {"type": "copy_trade", "wallet": "...", "action": "buy/sell", ...}
            - {"type": "arbitrage_opportunity", "buy_dex": "...", "sell_dex": "...", ...}
        """
        pass

    # ------------------------------------------------------------- Utilidades

    def _set_state(self, state: StrategyState) -> None:
        self._state = state

    def _inc_stat(self, key: str, amount: int = 1) -> None:
        self._stats[key] = self._stats.get(key, 0) + amount
