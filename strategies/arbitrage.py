"""Estrategia de Arbitraje entre DEXs en Solana.

Detecta diferencias de precio entre DEXs (Jupiter, Raydium, Orca, Pump.fun)
y ejecuta compras en el DEX mas barato y ventas en el mas caro.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Optional

import aiohttp
from loguru import logger

from core.engine.strategy import Strategy, StrategyState

# DEXs monitoreados para arbitraje
DEX_SOURCES = {
    "jupiter": "https://lite-api.jup.ag/v6/quote",
    "raydium": "https://api.raydium.io/v2/sdk/liquidity/mainnet",
}

# Spread minimo para ejecutar arbitraje (porcentaje)
DEFAULT_MIN_SPREAD_PCT = 1.5


@dataclass
class ArbitrageOpportunity:
    """Oportunidad de arbitraje detectada."""

    token_mint: str
    buy_dex: str
    sell_dex: str
    buy_price_sol: float
    sell_price_sol: float
    spread_pct: float
    timestamp: float


class ArbitrageStrategy(Strategy):
    """Estrategia de arbitraje entre DEXs.

    Monitorea precios en multiples DEXs y ejecuta cuando detecta
    diferencias significativas de precio.
    """

    def __init__(
        self,
        executor: Any,
        notifier: Any,
        tracker: Any,
        config: Any,
    ) -> None:
        super().__init__(executor, notifier, tracker, config)
        self._watched_tokens: dict[str, dict[str, float]] = {}
        self._check_interval = 5.0  # segundos entre checks
        self._min_spread_pct = float(
            getattr(config.copy_trading, "MIN_SPREAD_PCT", DEFAULT_MIN_SPREAD_PCT)
        )
        self._monitor_task: asyncio.Task | None = None

    @property
    def name(self) -> str:
        return "Arbitrage"

    async def start(self) -> None:
        """Inicia el monitoreo de arbitraje."""
        self._set_state(StrategyState.RUNNING)
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info(
            "Arbitrage: iniciado (min spread: {}%, intervalo: {}s)",
            self._min_spread_pct, self._check_interval,
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
        logger.info("Arbitrage: detenido")

    async def _monitor_loop(self) -> None:
        """Bucle principal de monitoreo de arbitraje."""
        try:
            while self.is_running:
                await asyncio.sleep(self._check_interval)

                # Monitorear tokens que ya tenemos en posicion
                for mint in list(self.executor.positions.keys()):
                    await self._check_arbitrage(mint)

                # Monitorear tokens watchlist (configurable)
                for mint in list(self._watched_tokens.keys()):
                    await self._check_arbitrage(mint)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Arbitrage: error en monitor loop: {}", exc)
            self._set_state(StrategyState.ERROR)

    async def _check_arbitrage(self, mint: str) -> None:
        """Compara precios entre DEXs para un token."""
        try:
            prices = await self._get_prices_from_dexs(mint)
            if len(prices) < 2:
                return

            # Encontrar el DEX mas barato y el mas caro
            sorted_prices = sorted(prices.items(), key=lambda x: x[1])
            buy_dex, buy_price = sorted_prices[0]
            sell_dex, sell_price = sorted_prices[-1]

            if buy_price <= 0 or sell_price <= 0:
                return

            spread_pct = (sell_price - buy_price) / buy_price * 100

            if spread_pct >= self._min_spread_pct:
                opportunity = ArbitrageOpportunity(
                    token_mint=mint,
                    buy_dex=buy_dex,
                    sell_dex=sell_dex,
                    buy_price_sol=buy_price,
                    sell_price_sol=sell_price,
                    spread_pct=spread_pct,
                    timestamp=time.time(),
                )
                await self._execute_arbitrage(opportunity)

        except Exception as exc:
            logger.debug("Arbitrage: error check {}: {}", mint[:8], exc)

    async def _get_prices_from_dexs(self, mint: str) -> dict[str, float]:
        """Obtiene precios de un token en multiples DEXs."""
        prices = {}

        # Jupiter price
        try:
            price = await self._get_jupiter_price(mint)
            if price > 0:
                prices["jupiter"] = price
        except Exception:
            pass

        # DexScreener price (agrega multiples DEXs)
        try:
            dex_prices = await self._get_dexscreener_prices(mint)
            prices.update(dex_prices)
        except Exception:
            pass

        return prices

    async def _get_jupiter_price(self, mint: str) -> float:
        """Precio via Jupiter Quote API."""
        from core.execution import SOL_MINT

        url = "https://lite-api.jup.ag/v6/quote"
        params = {
            "inputMint": mint,
            "outputMint": SOL_MINT,
            "amount": "1000000",  # 1 token (6 decimales)
            "slippageBps": 50,
        }
        headers = {"User-Agent": "Mozilla/5.0"}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status != 200:
                    return 0.0
                data = await resp.json()
                out_amount = float(data.get("outAmount", 0))
                return out_amount / 1e9  # lamports to SOL

    async def _get_dexscreener_prices(self, mint: str) -> dict[str, float]:
        """Precios de DexScreener (agrega Raydium, Orca, etc.)."""
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        headers = {"User-Agent": "Mozilla/5.0"}
        prices = {}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    return prices
                data = await resp.json()

        pairs = data.get("pairs") or []
        for pair in pairs:
            dex_id = pair.get("dexId", "").lower()
            price_native = float(pair.get("priceNative", 0) or 0)
            if price_native > 0 and dex_id:
                prices[dex_id] = price_native

        return prices

    async def _execute_arbitrage(self, opportunity: ArbitrageOpportunity) -> None:
        """Ejecuta una operacion de arbitraje."""
        self._inc_stat("trades_executed")

        logger.info(
            "Arbitrage: oportunidad detectada! {} | {} ({:.4f} SOL) -> {} ({:.4f} SOL) | Spread: {:.2f}%",
            opportunity.token_mint[:8] + "...",
            opportunity.buy_dex, opportunity.buy_price_sol,
            opportunity.sell_dex, opportunity.sell_price_sol,
            opportunity.spread_pct,
        )

        # Notificar
        await self.notifier.send_status(
            f"Arbitraje detectado: {opportunity.token_mint[:8]}...\n"
            f"Comprar en {opportunity.buy_dex}: {opportunity.buy_price_sol:.6f} SOL\n"
            f"Vender en {opportunity.sell_dex}: {opportunity.sell_price_sol:.6f} SOL\n"
            f"Spread: {opportunity.spread_pct:.2f}%"
        )

        # TODO: Implementar ejecucion real de arbitraje
        # Requiere:
        # 1. Comprar en el DEX mas barato
        # 2. Vender en el DEX mas caro
        # 3. Calcular gas + fees vs profit
        # 4. Ejecutar atomicamente o con flash loan
        logger.info(
            "Arbitrage: {} | Comprar en {} -> Vender en {} | Spread: {:.2f}%",
            opportunity.token_mint[:8] + "...",
            opportunity.buy_dex, opportunity.sell_dex,
            opportunity.spread_pct,
        )

    def add_token_to_watch(self, mint: str) -> None:
        """Agrega un token a la watchlist de arbitraje."""
        self._watched_tokens[mint] = {}
        logger.info("Arbitrage: token agregado a watchlist: {}", mint[:8] + "...")

    def remove_token_from_watch(self, mint: str) -> None:
        """Remueve un token de la watchlist."""
        self._watched_tokens.pop(mint, None)
