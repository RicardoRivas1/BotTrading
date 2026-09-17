"""Framework modular de trading para Solana.

Estructura:
  core/           - Infra compartida (wallet, RPC, executor, notifier, tracker)
  core/engine/    - Motor de estrategias (base class, lifecycle, orchestration)
  strategies/     - Estrategias concretas (memecoin, copy_trading, arbitrage, dca)

Cada estrategia se registra en el engine y recibe eventos del mercado.
El engine gestiona lifecycle (start/stop) y coordina las estrategias activas.
"""

from __future__ import annotations
