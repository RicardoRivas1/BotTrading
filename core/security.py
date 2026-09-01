"""Validación de seguridad de contratos de memecoins.

Consulta la API de RugCheck para obtener un score de riesgo, y verifica las
authorities de Mint y Freeze del token mediante el RPC de Helius. Rechaza
cualquier token que incumpla los umbrales configurados.

Toda la comunicación con APIs externas se hace con `aiohttp` de forma
asíncrona: nunca se bloquea el event loop de asyncio.
"""

from __future__ import annotations

import base64
from typing import Any, Optional

import aiohttp
from loguru import logger
from solders.pubkey import Pubkey

from config import SecuritySettings

# -- Constantes ---------------------------------------------------------------
# Endpoint de análisis de RugCheck (la ruta incluye la red: mainnet).
RUGCHECK_API = "https://api.rugcheck.xyz/v1/tokens"

# Descriptor del account info del token mint.
_MINT_ACCOUNT_SIZE = 165


class SecurityValidationError(Exception):
    """Se lanza cuando un token no supera los controles de seguridad."""


class TokenSecurityValidator:
    """Valida un token de forma independiente usando RugCheck + RPC."""

    def __init__(self, rpc_url: str, security: SecuritySettings) -> None:
        self.rpc_url = rpc_url
        self.security = security

    # ------------------------------------------------------------------ RPC
    async def _get_mint_account_info(self, session: aiohttp.ClientSession, mint: str) -> Optional[dict[str, Any]]:
        """Consulta el estado del mint del token vía JSON‑RPC de Solana."""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [
                mint,
                {"encoding": "jsonParsed"},
            ],
        }
        async with session.post(self.rpc_url, json=payload) as resp:
            if resp.status != 200:
                logger.warning("RPC getAccountInfo status {} para {}", resp.status, mint)
                return None
            data = await resp.json()
            return data.get("result", {}).get("value")

    def _parse_auth_from_mint(self, account_info: Optional[dict[str, Any]]) -> tuple[Optional[str], Optional[str], Optional[float]]:
        """Extrae mint/freeze authority y % de supply del Dev del mint.

        Devuelve (mint_authority, freeze_authority, dev_pct).
        Ambos authorities 'None' en jsonParsed significa que están renunciadas.
        """
        if not account_info:
            return None, None, None

        parsed = account_info.get("data", {}).get("parsed", {})
        info = parsed.get("info", {})

        mint_authority = info.get("mintAuthority")
        freeze_authority = info.get("freezeAuthority")
        supply = float(info.get("supply", "0"))

        # estimateDevPct no se puede derivar del mint directamente; RugCheck
        # entrega en 'risks'. Aquí devolvemos 0.0 y delegamos a RugCheck.
        dev_pct = 0.0
        return mint_authority, freeze_authority, dev_pct

    # -------------------------------------------------------------- RugCheck
    async def _fetch_rugcheck(self, session: aiohttp.ClientSession, mint: str) -> Optional[dict[str, Any]]:
        """Obtiene el reporte de riesgo completo de RugCheck."""
        url = f"{RUGCHECK_API}/{mint}/report"
        try:
            async with session.get(url) as resp:
                if resp.status == 404:
                    logger.warning("RugCheck no encontró reporte para {}", mint)
                    return None
                if resp.status != 200:
                    logger.warning("RugCheck status {} para {}", resp.status, mint)
                    return None
                return await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("Error HTTP en RugCheck para {}: {}", mint, exc)
            return None

    def _rugcheck_score(self, report: Optional[dict[str, Any]]) -> int:
        """Extrae el score numérico del reporte RugCheck."""
        if not report:
            # Sin reporte = se asume alto riesgo para ser conservadores.
            return self.security.RUGCHECK_MAX_SCORE + 1
        scan = report.get("token", {}).get("mintAuthority")
        if "risks" in report:
            total = sum(int(risk.get("score", 0)) for risk in report.get("risks", []))
            return total
        return 0

    def _rugcheck_dev_pct(self, report: Optional[dict[str, Any]]) -> float:
        """Extrae el % del supply en manos del Dev desde RugCheck."""
        if not report:
            return 0.0
        holders = report.get("topHolders", [])
        if not holders:
            return 0.0
        # El primer top-holder suele ser el Dev/creador del token.
        return float(holders[0].get("pct", 0.0))

    # ---------------------------------------------------------------- Public
    async def is_token_safe(self, mint: str) -> bool:
        """Evalúa todos los controles de seguridad sobre un token.

        Returns:
            True si el token es seguro; lanza SecurityValidationError si no.
        """
        logger.info("Validando seguridad del token {}", mint)

        async with aiohttp.ClientSession() as session:
            # Ejecución concurrente (no bloquea el loop) de RPC y RugCheck.
            account_info, report = await asyncio_runner(
                self._get_mint_account_info(session, mint),
                self._fetch_rugcheck(session, mint),
            )

        mint_auth, freeze_auth, _ = self._parse_auth_from_mint(account_info)
        score = self._rugcheck_score(report)
        dev_pct = self._rugcheck_dev_pct(report)

        # 1) Score RugCheck mayor al umbral -> rechazar.
        if score > self.security.RUGCHECK_MAX_SCORE:
            raise SecurityValidationError(
                f"RugCheck score {score} > max {self.security.RUGCHECK_MAX_SCORE}"
            )

        # 2) Mint authority no renunciada -> rechazar.
        if self.security.REQUIRE_MINT_RENOUNCED and mint_auth is not None:
            raise SecurityValidationError(
                f"Mint authority no renunciada: {mint_auth}"
            )

        # 3) Freeze authority no renunciada -> rechazar.
        if self.security.REQUIRE_FREEZE_RENOUNCED and freeze_auth is not None:
            raise SecurityValidationError(
                f"Freeze authority no renunciada: {freeze_auth}"
            )

        # 4) Dev con más del umbral de supply -> rechazar.
        if dev_pct > self.security.DEV_MAX_SUPPLY_PCT:
            raise SecurityValidationError(
                f"Dev posee {dev_pct:.1f}% del supply (> {self.security.DEV_MAX_SUPPLY_PCT:.0f}%)"
            )

        logger.success("Token {} es seguro (score={}, dev={:.1f}%)", mint, score, dev_pct)
        return True


async def asyncio_runner(*awaitables: Any) -> list[Any]:
    """Ejecuta múltiples awaitables y devuelve una lista de resultados.

    Envuelve varios coroutines en gather para lanzarlos en paralelo respetando
    la naturaleza asíncrona (no bloqueante) del módulo asyncio.
    """
    results = await asyncio.gather(*awaitables, return_exceptions=True)
    # Convierte las excepciones internas en None para simplificar el manejo.
    return [None if isinstance(r, Exception) else r for r in results]
