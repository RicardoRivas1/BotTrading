"""Ejecución de swaps en Solana mediante la Jupiter Swap API v6.

Construye la transacción de swap, la firma localmente con `solders.Keypair`
y la envía a la red vía RPC. Es 100% asíncrona: las llamadas HTTP a Jupiter
y al RPC usan `aiohttp` para no bloquear el event loop.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp
import base58
from bip_utils import Bip39SeedGenerator, Bip44, Bip44Changes, Bip44Coins
from loguru import logger
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solders.signature import Signature
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey

from config import TradingSettings

# -- Constantes ---------------------------------------------------------------
# Endpoint público de la Swap API de Jupiter v6 (configurable por .env).
_trading_settings = TradingSettings()
JUPITER_QUOTE_URL = _trading_settings.JUPITER_QUOTE_URL
JUPITER_FALLBACK_URL = _trading_settings.JUPITER_FALLBACK_URL
JUPITER_SWAP = "https://quote-api.jup.ag/v6/swap"

# User-Agent de navegador para eludir bloqueos básicos de Cloudflare.
_USER_AGENT_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# SPL Token (USDC / WSOL) y el token de referencia (SOL).
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOL_MINT = "So11111111111111111111111111111111111111112"

# Palabras mínimas que delatan una frase de recuperación (mnemonic).
WORD_COUNT_THRESHOLD = 11


class SwapExecutionError(Exception):
    """Se lanza cuando un swap no puede completarse."""


def cargar_keypair(key_str: str) -> Keypair:
    """Carga una wallet de Solana desde una clave Base58 o una frase mnemonic.

    Soporta dos formatos de entrada:
    - Private key Base58 estándar de 64 bytes.
    - Frase de recuperación de 12/24 palabras separadas por espacios.

    Si la cadena no coincide con ninguno de los formatos, registra un error
    crítico con loguru y lanza SwapExecutionError (detiene el arranque).
    """
    key_str = key_str.strip()
    if not key_str:
        logger.critical("PRIVATE_KEY vacío en configuración.")
        raise SwapExecutionError("PRIVATE_KEY vacío en configuración.")

    # Detección por cantidad de palabras separadas por espacios.
    words = key_str.split()
    if len(words) > WORD_COUNT_THRESHOLD:
        return _cargar_desde_mnemonic(key_str)
    if len(words) == 1:
        return _cargar_desde_base58(key_str)

    # Formato desconocido (p. ej. 2-11 palabras sin ser clave ni frase válida).
    logger.critical(
        "Formato de PRIVATE_KEY no reconocido: "
        "se esperaba Base58 (1 palabra) o mnemonic (12/24 palabras), "
        "se recibieron {} palabras.", len(words)
    )
    raise SwapExecutionError(f"FORMATO DE CLAVE INVALIDO: {len(words)} palabras.")


def _cargar_desde_mnemonic(mnemonic: str) -> Keypair:
    """Deriva el Keypair desde una frase de 12/24 palabras (BIP44 / SLIP-0010).

    Usa `bip-utils` para generar la semilla a partir de la frase y derivar la
    cuenta de Solana en el path estándar de Phantom (m/44'/501'/0'/0').
    """
    try:
        seed = Bip39SeedGenerator(mnemonic).Generate()
        bip44_ctx = Bip44.FromSeed(seed, Bip44Coins.SOLANA)
        # Deriva la cuenta en el path estándar de Phantom (m/44'/501'/0'/0').
        # Se usa Bip44Changes.CHAIN_EXT (cadena externa, index 0) porque la
        # cadena interna (CHANGE) espera '.Change(1)' y rompería la derivación.
        private_bytes = (
            bip44_ctx.Purpose()
            .Coin()
            .Account(0)
            .Change(Bip44Changes.CHAIN_EXT)
            .AddressIndex(0)
            .PrivateKey()
            .Raw()
            .ToBytes()
        )
        return Keypair.from_seed(private_bytes)
    except Exception as exc:
        logger.critical("Frase mnemonic inválida: {}", exc)
        raise SwapExecutionError(f"Mnemonic inválido: {exc}") from exc


def _cargar_desde_base58(key_str: str) -> Keypair:
    """Carga un Keypair desde una private key Base58 estándar de 64 bytes."""
    try:
        raw = base58.b58decode(key_str)
        if len(raw) != 64:
            raise ValueError(f"La clave Base58 debe ser de 64 bytes, se obtuvieron {len(raw)}.")
        return Keypair.from_bytes(raw)
    except Exception as exc:
        logger.critical("Private key Base58 inválida: {}", exc)
        raise SwapExecutionError(
            "PRIVATE_KEY Base58 inválida. Verifica que sea una clave de 64 bytes."
        ) from exc


@dataclass
class Position:
    """Estado mutable de una posición abierta en un token."""

    mint: str
    token_amount_ui: float = 0.0          # Cantidad de tokens comprados (UI).
    entry_price: float = 0.0              # Precio de entrada del token vs SOL.
    peak_price: float = 0.0               # Precio máximo alcanzado desde la compra.
    sol_invested: float = 0.0             # SOL invertidos inicialmente.
    trailing_active: bool = False         # True una vez que se activó la protección trailing.
    created_at: float = field(default_factory=time.time)  # Timestamp de creación.


class JupiterExecutor:
    """Orquesta compra/venta de memecoins usando Jupiter v6."""

    def __init__(
        self,
        private_key: str,
        rpc_url: str,
        slippage_bps: int,
        buy_amount_sol: float,
        take_profit_pct: float = 100.0,
        stop_loss_pct: float = 30.0,
        trailing_activation_pct: float = 20.0,
        trailing_distance_pct: float = 15.0,
        dry_run: bool = True,
    ) -> None:
        # Carga la wallet desde Base58 o mnemonic (lanza SwapExecutionError
        # crítico y detiene el arranque si el formato es inválido).
        self.keypair: Keypair = cargar_keypair(private_key)
        self.rpc_url = rpc_url
        self.slippage_bps = slippage_bps
        self.buy_amount_sol = buy_amount_sol
        self.wallet_pubkey = str(self.keypair.pubkey())

        # Parámetros del motor de salida automático.
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        self.trailing_activation_pct = trailing_activation_pct
        self.trailing_distance_pct = trailing_distance_pct
        # Si DRY_RUN es True, las ventas se simulan (no se envían transacciones).
        self.dry_run = dry_run

        # Posiciones abiertas indexadas por mint.
        self.positions: dict[str, Position] = {}

    # ------------------------------------------------------------ Quote
    async def _get_quote(
        self,
        session: aiohttp.ClientSession,
        input_mint: str,
        output_mint: str,
        amount_lamports: int,
        simulate: bool = False,
        slippage_bps: Optional[int] = None,
    ) -> dict[str, Any]:
        """Solicita una cotización de precios y rutas a Jupiter.

        Si el endpoint principal falla por DNS o error de conexión
        (`aiohttp.ClientConnectorError`), reintenta automáticamente contra la
        URL de fallback antes de registrar el error.

        Si la consulta falla por 404 de Jupiter (token recién creado en
        Pump.fun sin ruta), error de red o timeout, y `simulate` es True,
        genera una cotización simulada en lugar de abortar el flujo de test
        (DRY_RUN / FORCE_TEST_BUY).

        `slippage_bps` permite aumentar el slippage puntualmente (p. ej.
        ventas de emergencia en stop-loss).
        """
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount_lamports),
            "slippageBps": self.slippage_bps if slippage_bps is None else slippage_bps,
            "onlyDirectRoutes": "false",
        }
        try:
            try:
                return await self._request_quote(session, JUPITER_QUOTE_URL, params)
            except (aiohttp.ClientConnectorError, OSError, asyncio.TimeoutError) as exc:
                logger.warning(
                    "Jupiter principal {} falló por red ({}); reintentando con fallback {}",
                    JUPITER_QUOTE_URL, exc, JUPITER_FALLBACK_URL,
                )
                return await self._request_quote(session, JUPITER_FALLBACK_URL, params)
        except (SwapExecutionError, aiohttp.ClientConnectorError, OSError, asyncio.TimeoutError) as exc:
            if simulate:
                logger.warning(
                    "⚠️ Token sin ruta en Jupiter (Pump.fun reciente). "
                    "Generando cotización simulada para test. ({})",
                    exc,
                )
                return self._simulated_quote(input_mint, output_mint, amount_lamports)
            raise

    def _simulated_quote(
        self,
        input_mint: str,
        output_mint: str,
        amount_lamports: int,
    ) -> dict[str, Any]:
        """Genera una cotización simulada para flujos de test (DRY_RUN/FORCE_TEST_BUY).

        Modela un precio de entrada de 1:1 (1 lamport de token por 1 lamport
        de SOL) para que el flujo de simulación no se aborte.
        """
        return {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount_lamports),
            "outAmount": str(amount_lamports),
            "routePlan": [{"outAmount": str(amount_lamports)}],
            "simulated": True,
        }

    async def _request_quote(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """GET de cotización a Jupiter con User-Agent de navegador."""
        async with session.get(url, params=params, headers=_USER_AGENT_HEADERS) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise SwapExecutionError(
                    f"Jupiter quote falló en {url} ({resp.status}): {text[:200]}"
                )
            return await resp.json()

    # ------------------------------------------------------------ Swap
    async def _build_and_send_swap(
        self,
        session: aiohttp.ClientSession,
        quote: dict[str, Any],
    ) -> Signature:
        """Solicita la transacción instantánea, la firma y la envía."""
        swap_payload = {
            "quoteResponse": quote,
            "userPublicKey": self.wallet_pubkey,
            "wrapAndUnwrapSol": True,
            "computeUnitLimit": None,
            "computeUnitPriceMicroLamports": None,
        }
        async with session.post(
            JUPITER_SWAP, json=swap_payload, headers=_USER_AGENT_HEADERS
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise SwapExecutionError(f"Jupiter swap falló ({resp.status}): {text[:200]}")
            swap_data = await resp.json()

        # La transacción llega como una lista de bytes (base64 o enteros).
        raw_tx = swap_data["transaction"]
        tx_bytes = self._decode_transaction(raw_tx)

        # Firma local con solders: deserializamos la VersionedTransaction.
        tx = VersionedTransaction.from_bytes(bytes(tx_bytes))
        signature = self.keypair.sign_message(tx.message.to_bytes())
        signed_tx = VersionedTransaction.populate(tx.message, [signature])

        # Envío a la red a través del RPC de Helius.
        async with AsyncClient(self.rpc_url) as client:
            res = await client.send_raw_transaction(
                bytes(signed_tx.to_bytes()),
                opts={"skipPreflight": False},
            )
        if not res.value:
            raise SwapExecutionError("Respuesta de envío sin firma")

        logger.success("Swap enviado: {}", res.value)
        return res.value

    @staticmethod
    def _decode_transaction(raw_tx: Any) -> bytes:
        """Decodifica la transacción devuelta por Jupiter (str o lista)."""
        if isinstance(raw_tx, str):
            return bytes.fromhex(raw_tx) if raw_tx.startswith("0x") else base58.b58decode(raw_tx)
        if isinstance(raw_tx, list):
            return bytes(raw_tx)
        raise SwapExecutionError(f"Formato de transacción no soportado: {type(raw_tx)}")

    # ------------------------------------------------------------ Public
    async def buy_token(self, token_mint: str, dry_run: Optional[bool] = None) -> Signature | str:
        """Compra un token usando BUY_AMOUNT_SOL de SOL.

        Si `dry_run` es True (o si el executor está en simulación), no envía
        ninguna transacción real: solo obtiene la cotización de Jupiter, registra
        la posición simulada y devuelve la cadena "DRY_RUN" como firma sintética.

        Registra la posición abierta con su precio de entrada y el peak inicial.
        """
        simulate = self.dry_run if dry_run is None else dry_run
        amount_lamports = int(self.buy_amount_sol * 1_000_000_000)
        async with aiohttp.ClientSession() as session:
            quote = await self._get_quote(
                session, SOL_MINT, token_mint, amount_lamports,
                simulate=simulate,
            )
            if simulate:
                logger.info(
                    "[DRY_RUN] Compra simulada de {} | Monto: {} SOL",
                    token_mint, self.buy_amount_sol,
                )
                sig: Signature | str = "DRY_RUN"
            else:
                sig = await self._build_and_send_swap(session, quote)

        # Precio de entrada por token en SOL calculado desde la bonding curve:
        # entry_price_sol = amount_sol / tokens_out.
        out_amount = float(
            quote.get("outAmount", quote.get("routePlan", [{}])[0].get("outAmount", 0))
            or 0
        )
        decimals = await self._get_token_decimals(token_mint)
        token_qty_ui = out_amount / (10 ** decimals) if decimals else 0.0
        entry_price_sol = (
            self.buy_amount_sol / token_qty_ui if token_qty_ui else 0.0
        )

        # Último recurso (sin inventar precio): si la curva no entregó tokens,
        # se consulta la cotización actual vía Jupiter.
        if not entry_price_sol or entry_price_sol <= 0:
            try:
                entry_price_sol = await self.get_token_price(token_mint)
            except Exception as exc:  # noqa: BLE001 - fallo de red no bloqueante
                logger.warning("No se pudo obtener precio base de {}: {}", token_mint, exc)
        entry_price_sol = max(entry_price_sol, 0.0)

        self.positions[token_mint] = Position(
            mint=token_mint,
            token_amount_ui=token_qty_ui,
            entry_price=entry_price_sol,
            peak_price=entry_price_sol,
            sol_invested=self.buy_amount_sol,
        )
        logger.info("Posición registrada para {} @ entry={:.9f} SOL", token_mint, entry_price_sol)
        return sig

    async def sell_token(
        self,
        token_mint: str,
        token_balance_ui: float,
        slippage_bps: Optional[int] = None,
    ) -> Signature:
        """Vende la totalidad del balance de un token.

        `slippage_bps` permite usar un slippage distinto del configurado
        (p. ej. 15-20% en ventas de emergencia por stop-loss).
        """
        # El balance del token suele venir en unidades decimales UI.
        decimals = await self._get_token_decimals(token_mint)
        raw_amount = int(token_balance_ui * (10 ** decimals))

        async with aiohttp.ClientSession() as session:
            quote = await self._get_quote(
                session, token_mint, SOL_MINT, raw_amount,
                slippage_bps=slippage_bps,
            )
            return await self._build_and_send_swap(session, quote)

    # --------------------------------------------------- Price / Monitoring
    async def get_token_price(self, token_mint: str) -> float:
        """Consulta el precio del token contra SOL con fallback encadenado.

        Orden de consulta:
        1. Jupiter v6 Quote API (preferido).
        2. DexScreener API (fallback si Jupiter falla por 404 o red).
        3. Pump.fun API (último recurso para tokens < 30s sin listing en DexScreener).

        Devuelve el precio expresado en SOL.
        """
        # --- 1. Jupiter v6 (principal) ---
        try:
            decimals = await self._get_token_decimals(token_mint)
            amount_lamports = int(1 * (10 ** decimals))
            async with aiohttp.ClientSession() as session:
                quote = await self._get_quote(
                    session, token_mint, SOL_MINT, amount_lamports
                )
            out = float(quote.get("outAmount") or 0)
            price_sol = out / 1_000_000_000
            if price_sol > 0:
                return price_sol
        except Exception as exc:  # noqa: BLE001
            logger.warning("Jupiter sin precio para {} ({}); intentando fallbacks.", token_mint, exc)

        # --- 2. DexScreener ---
        try:
            price_sol = await self._get_price_from_dexscreener(token_mint)
            if price_sol > 0:
                return price_sol
        except Exception as exc:  # noqa: BLE001
            logger.warning("DexScreener sin precio para {} ({}); intentando Pump.fun.", token_mint, exc)

        # --- 3. Pump.fun (curva de bonding) ---
        try:
            price_sol = await self._get_price_from_pumpfun(token_mint)
            if price_sol > 0:
                return price_sol
        except Exception as exc:  # noqa: BLE001
            logger.error("Pump.fun sin precio para {}: {}", token_mint, exc)

        raise SwapExecutionError(f"No se pudo obtener precio de {token_mint} desde ningún endpoint.")

    async def _get_price_from_dexscreener(self, token_mint: str) -> float:
        """Consulta precio vía DexScreener. Extrae priceNative (SOL) o priceUsd."""
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_USER_AGENT_HEADERS) as resp:
                if resp.status != 200:
                    logger.debug("DexScreener respondió {} para {}", resp.status, token_mint)
                    return 0.0
                data = await resp.json()

        pairs = data.get("pairs") or []
        if not pairs:
            logger.debug("DexScreener sin pairs para {}", token_mint)
            return 0.0

        # Prioriza la pair con más liquidez.
        best = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))

        # priceNative es el precio en SOL (ej: "0.000123").
        price_native = best.get("priceNative")
        if price_native:
            try:
                return float(price_native)
            except (ValueError, TypeError):
                pass

        # Fallback a priceUsd y convertir a SOL usando el precio de SOL en USD.
        price_usd = best.get("priceUsd")
        if price_usd:
            try:
                sol_usd = await self._get_sol_usd_price()
                if sol_usd > 0:
                    return float(price_usd) / sol_usd
            except Exception:  # noqa: BLE001
                pass

        return 0.0

    async def _get_price_from_pumpfun(self, token_mint: str) -> float:
        """Consulta precio vía Pump.fun API; calcula precio SOL desde reservas."""
        url = f"https://frontend-api.pump.fun/coins/{token_mint}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_USER_AGENT_HEADERS) as resp:
                if resp.status != 200:
                    logger.debug("Pump.fun respondió {} para {}", resp.status, token_mint)
                    return 0.0
                data = await resp.json()

        # La bonding curve de Pump.fun:
        #   precio_sol = (sol_reserves) / (token_reserves)
        # donde sol_reserves y token_reserves son las cantidades actuales en la curva.
        sol_reserves = data.get("sol_reserves")
        token_reserves = data.get("token_reserves")

        if sol_reserves and token_reserves:
            try:
                sol_r = float(sol_reserves)
                tok_r = float(token_reserves)
                if tok_r > 0:
                    return sol_r / tok_r
            except (ValueError, TypeError):
                pass

        # Alternativa: precio en SOL directo si la API lo expone.
        sol_supply = data.get("sol_supply")
        token_supply = data.get("token_supply")
        if sol_supply and token_supply:
            try:
                return float(sol_supply) / float(token_supply)
            except (ValueError, TypeError):
                pass

        return 0.0

    async def _get_sol_usd_price(self) -> float:
        """Obtiene el precio de SOL en USD para convertir precios de DexScreener."""
        url = "https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_USER_AGENT_HEADERS) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get("pairs") or []
                    if pairs:
                        price_usd = pairs[0].get("priceUsd")
                        if price_usd:
                            return float(price_usd)
        # Valor de respaldo razonable si DexScreener falla.
        return 180.0

    async def monitor_position(self, token_mint: str) -> tuple[str, float]:
        """Evalúa una posición y ejecuta la salida según sea necesario.

        Returns:
            Una tupla (motivo, pnl_pct) donde `motivo` es "TAKE_PROFIT",
            "STOP_LOSS", "TRAILING_STOP" o "" (sin salida) y `pnl_pct` es el
            porcentaje de ganancia/pérdida en el momento de la evaluación.
        """
        position = self.positions.get(token_mint)
        if position is None:
            return "", 0.0

        # Actualiza el precio actual del token.
        try:
            current_price = await self.get_token_price(token_mint)
        except Exception as exc:  # noqa: BLE001 - fallo de red no bloqueante
            logger.warning("No se pudo consultar precio de {}: {}", token_mint, exc)
            return "", 0.0

        # Si falta un precio de entrada válido, se re-consulta el precio base
        # para no ignorar la posición ni provocar una división por cero.
        if not position.entry_price or position.entry_price <= 0:
            try:
                position.entry_price = await self.get_token_price(token_mint)
            except Exception as exc:  # noqa: BLE001 - fallo de red no bloqueante
                logger.warning("No se pudo re-consultar precio de entrada de {}: {}", token_mint, exc)
                return "", 0.0
            logger.info("Precio de entrada re-establecido para {}: {:.10g}", token_mint, position.entry_price)

        # Actualizamos el peak: el máximo alcanzado jamás puede bajar.
        position.peak_price = max(position.peak_price, current_price)
        pnl_pct = (
            (current_price - position.entry_price) / position.entry_price * 100
        ) if position.entry_price else 0.0

        # Regla 1: Stop-Loss inicial fijo (-30%).
        if pnl_pct <= -self.stop_loss_pct:
            await self.close_position(token_mint, "STOP_LOSS", pnl_pct)
            return "STOP_LOSS", pnl_pct

        # Regla 2: Take-Profit fijo (+100%).
        if pnl_pct >= self.take_profit_pct:
            await self.close_position(token_mint, "TAKE_PROFIT", pnl_pct)
            return "TAKE_PROFIT", pnl_pct

        # Regla 3: Trailing Stop tras ganancia >= +20%.
        if pnl_pct >= self.trailing_activation_pct:
            position.trailing_active = True

        if position.trailing_active:
            # Cuando el precio retrocede DISTANCIA desde el peak, cerramos.
            drawdown = (
                (position.peak_price - current_price) / position.peak_price * 100
            ) if position.peak_price else 0.0
            if drawdown >= self.trailing_distance_pct:
                await self.close_position(token_mint, "TRAILING_STOP", pnl_pct)
                return "TRAILING_STOP", pnl_pct

        return "", 0.0

    async def close_position(
        self,
        token_mint: str,
        reason: str,
        pnl_pct: float,
        slippage_bps: Optional[int] = None,
    ) -> None:
        """Cierra una posición vendiendo el total (real o simulado).

        Con `DRY_RUN=False`, la transacción de venta se firma localmente con
        la `PRIVATE_KEY` (`self.keypair`) y se envía a la red de Solana.
        `slippage_bps` permite forzar slippage alto en ventas de emergencia.
        """
        position = self.positions.get(token_mint)
        if position is None:
            return

        if self.dry_run:
            # Simulación: no se envía transacción real, solo se registra.
            logger.info(
                "[DRY_RUN] Venta simulada de {} por {} (PnL {:.2f}%)",
                token_mint, reason, pnl_pct,
            )
            self.positions.pop(token_mint, None)
            return

        try:
            sig = await self.sell_token(
                token_mint, position.token_amount_ui,
                slippage_bps=slippage_bps,
            )
        except Exception as exc:  # noqa: BLE001 - persistimos la posición en fallo
            logger.error("Error vendiendo {} ({}): {}", token_mint, reason, exc)
            return

        self.positions.pop(token_mint, None)
        logger.success("Posición {} cerrada por {} (PnL {:.2f}%)", token_mint, reason, pnl_pct)

    # ------------------------------------------------------------ Decimals
    async def _get_token_decimals(self, token_mint: str) -> int:
        """Consulta los decimales del token vía RPC (usa data program info)."""
        async with AsyncClient(self.rpc_url) as client:
            account = await client.get_account_info_json_parsed(Pubkey.from_string(token_mint))
        parsed = account.value.data if account.value else None
        try:
            return parsed.parsed["info"]["decimals"]
        except (AttributeError, KeyError, TypeError):
            logger.warning("No se pudieron obtener decimals, asumiendo 9 (memecoin).")
            return 9
