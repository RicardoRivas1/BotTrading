"""Motor de estrategias de trading.

Proporciona la base abstracta para estrategias y el engine que orquesta
su lifecycle (start, stop, pause, resume) dentro del bot.
"""

from __future__ import annotations

from core.engine.strategy import Strategy, StrategyState
from core.engine.engine import StrategyEngine

__all__ = ["Strategy", "StrategyState", "StrategyEngine"]
