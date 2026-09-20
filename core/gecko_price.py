"""GeckoTerminal API price fetcher.

Free API for Solana DEX data with fast indexing of new tokens.
Docs: https://docs.geckoterminal.com
"""

from __future__ import annotations

from typing import Optional

import aiohttp
from loguru import logger

_USER_AGENT = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
GECKO_API = "https://api.geckoterminal.com/api/v2"


async def get_price_from_geckoterminal(token_mint: str) -> Optional[float]:
    """Get token price in SOL from GeckoTerminal.

    Queries the most liquid Solana pool for the token and returns
    price in SOL (priceNative).
    """
    url = f"{GECKO_API}/networks/solana/tokens/{token_mint}"
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=_USER_AGENT) as resp:
                if resp.status != 200:
                    logger.debug("GeckoTerminal responded {} for {}", resp.status, token_mint)
                    return None
                data = await resp.json()

        # priceNative is SOL price
        price_native = data.get("data", {}).get("attributes", {}).get("price_native")
        if price_native is not None:
            try:
                return float(price_native)
            except (ValueError, TypeError):
                pass

        # Try pool-based approach
        return await _get_price_from_pool(token_mint, session)

    except Exception as exc:  # noqa: BLE001 - catch ALL errors to prevent cascading
        logger.debug("GeckoTerminal error for {}: {}", token_mint, exc)
        return None


async def _get_price_from_pool(token_mint: str, session: aiohttp.ClientSession) -> Optional[float]:
    """Fallback: find the most liquid pool and extract price."""
    url = f"{GECKO_API}/networks/solana/tokens/{token_mint}/pools"
    try:
        async with session.get(url, headers=_USER_AGENT) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()

        pools = data.get("data", [])
        if not pools:
            return None

        # Sort by reserve_in_usd (liquidity) — handle None values
        def _safe_liquidity(p: dict) -> float:
            try:
                val = p.get("attributes", {}).get("reserve_in_usd")
                return float(val) if val is not None else 0.0
            except (ValueError, TypeError):
                return 0.0

        pools.sort(key=_safe_liquidity, reverse=True)

        best = pools[0]
        price_native = (
            best.get("attributes", {}).get("base_token_price_native")
            or best.get("attributes", {}).get("quote_token_price_native")
        )
        if price_native:
            return float(price_native)

    except (aiohttp.ClientError, ValueError, TypeError) as exc:
        logger.debug("GeckoTerminal pool error for {}: {}", token_mint, exc)

    return None


async def get_symbol_from_geckoterminal(token_mint: str) -> Optional[str]:
    """Get token symbol from GeckoTerminal."""
    url = f"{GECKO_API}/networks/solana/tokens/{token_mint}"
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=_USER_AGENT) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()

        symbol = data.get("data", {}).get("attributes", {}).get("symbol")
        if symbol:
            return symbol.upper()

    except Exception as exc:
        logger.debug("GeckoTerminal symbol error for {}: {}", token_mint, exc)

    return None
