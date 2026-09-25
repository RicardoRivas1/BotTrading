"""Utility for reading Pump.fun bonding curve reserves directly from Solana RPC.

For newly launched tokens that haven't been indexed by DexScreener/Jupiter yet,
reading the bonding curve account on-chain provides the most accurate price.

The bonding curve program: 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwry6
PDA seeds: ["bonding-curve", mint_pubkey]
"""

from __future__ import annotations

from typing import Optional

from loguru import logger
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey

# Pump.fun bonding curve program (original, pre-AMM)
PUMP_FUN_PROGRAM = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwry6")

# Cache for resolved program addresses (some tokens use pump_amm)
_PROGRAM_CACHE: dict[str, Pubkey] = {}

# Un unico cliente RPC reutilizado. Antes se creaba un AsyncClient POR LLAMADA
# (y nunca se cerraba): el monitor del tracker pide precio a esta funcion en
# cada posicion y en cada ciclo, asi que se acumulaban handshakes TCP+TLS
# contra Helius, con el riesgo añadido de disparar sus rate limits y de hacer
# que el propio monitor se ralentizara.
_CLIENTS: dict[str, AsyncClient] = {}


def _get_client(rpc_url: str) -> AsyncClient:
    client = _CLIENTS.get(rpc_url)
    if client is None:
        client = _CLIENTS[rpc_url] = AsyncClient(rpc_url)
    return client


async def get_bonding_curve_price(
    rpc_url: str,
    token_mint: str,
) -> Optional[float]:
    """Read bonding curve reserves on-chain and compute price in SOL/token.

    Tries the original Pump.fun bonding curve program first, then falls back
    to pump_amm if the PDA doesn't exist.

    Returns price in SOL per token, or None if the account doesn't exist or
    can't be parsed.
    """
    try:
        client = _get_client(rpc_url)
        mint_pubkey = Pubkey.from_string(token_mint)
    except Exception as exc:
        logger.debug("Invalid mint pubkey {}: {}", token_mint, exc)
        return None

    # Try original bonding curve program first, then pump_amm
    programs_to_try = [
        PUMP_FUN_PROGRAM,
        Pubkey.from_string("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"),
    ]

    for program_id in programs_to_try:
        try:
            # Derive PDA: ["bonding-curve", mint]
            pda, _bump = Pubkey.find_program_address(
                [b"bonding-curve", bytes(mint_pubkey)],
                program_id,
            )

            resp = await client.get_account_info(pda)
            account_info = resp.value
            if account_info is None:
                continue

            data = account_info.data
            if len(data) < 24:
                continue

            # Parse bonding curve account layout (offsets in bytes):
            # 0..8:    Anchor discriminator (8 bytes, ignore)
            # 8..16:   virtual_sol_reserves (u64 LE, lamports)
            # 16..24:  virtual_token_reserves (u64 LE, raw token units)
            # 24..32:  real_sol_reserves (u64 LE)
            # 32..40:  real_token_reserves (u64 LE)
            # 40..48:  token_total_supply (u64 LE)
            # 48:      token_decimals (u8)
            virtual_sol_lamports = int.from_bytes(data[8:16], "little")
            virtual_token_raw = int.from_bytes(data[16:24], "little")

            if virtual_token_raw <= 0:
                continue

            # Determine token decimals (default 6 for Pump.fun)
            token_decimals = 6
            if len(data) > 48:
                token_decimals = data[48]
                if token_decimals == 0:
                    token_decimals = 6  # fallback

            price_sol = (virtual_sol_lamports / 1e9) / (virtual_token_raw / (10 ** token_decimals))
            if price_sol > 0:
                return price_sol

        except Exception as exc:
            logger.debug("Failed to read bonding curve for {} (program {}): {}", token_mint, program_id, exc)
            continue

    return None
