"""Ejecución de swaps en Solana mediante la Jupiter Swap API v6.

Construye la transacción de swap, la firma localmente con `solders.Keypair`
y la envía a la red vía RPC. Es 100% asíncrona: las llamadas HTTP a Jupiter
y al RPC usan `aiohttp` para no bloquear el event loop.
"""

from __future__ import annotations

import asyncio
import base64
import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp
import base58
import httpx
from bip_utils import Bip39SeedGenerator
from loguru import logger
from solana.rpc.async_api import AsyncClient
from solana.rpc.core import TokenAccountOpts
from solana.rpc.models import TxOpts
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.token.associated import get_associated_token_address
from solders.transaction import VersionedTransaction

from config import TradingSettings

# -- Constantes ---------------------------------------------------------------
_trading_settings = TradingSettings()
JUPITER_QUOTE_URL = _trading_settings.JUPITER_QUOTE_URL
JUPITER_FALLBACK_URL = _trading_settings.JUPITER_FALLBACK_URL
JUPITER_SWAP = "https://lite-api.jup.ag/v6/swap"

# API de PumpPortal para trades directos en la bonding curve de Pump.fun.
PUMPPORTAL_TRADE_URL = "https://pumpportal.fun/api/trade-local"

# Prioridad mínima objetivo para las compras: pagar ~0.0001 SOL extra por swap
# (200_000 CU x 500_000 micro-lamports/CU = 1e5 lamports = 0.0001 SOL) para
# reducir los descartes por prioridad baja en los picos de congestión.
COMPUTE_UNIT_LIMIT = 200_000
COMPUTE_UNIT_PRICE_MICRO_LAMPORTS = 500_000

# Slippage de compra en la bonding curve de Pump.fun (porcentaje).
BUY_SLIPPAGE_MIN_PCT = 15.0
BUY_SLIPPAGE_MAX_PCT = 20.0

_USER_AGENT_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOL_MINT = "So11111111111111111111111111111111111111112"


class SwapExecutionError(Exception):
    """Se lanza cuando un swap no puede completarse."""


def _is_route_not_found(exc: Exception) -> bool:
    """True si el error de Jupiter indica que no existe ruta de swap.

    Ocurre típicamente cuando el token aún vive en la bonding curve de
    Pump.fun y todavía no existe en Raydium: Jupiter responde 404 o
    "Route not found".
    """
    message = str(exc).lower()
    return any(phrase in message for phrase in ("route not found", "no route", "404"))


def _is_rate_limit(exc: Exception) -> bool:
    """True si el error de Jupiter indica saturación de la API (429 / Rate limit)."""
    message = str(exc).lower()
    return any(phrase in message for phrase in ("429", "rate limit", "too many requests"))

# Presión a la API de Jupiter: caché de quotes (mismas compras DCA del mismo
# token y mismo monto reutilizan la última cotización real) + intervalo mínimo
# entre llamadas HTTP + circuit-breaker de 10s tras un 429.
JUPITER_QUOTE_CACHE_TTL_SECONDS = 2.0
JUPITER_MIN_CALL_INTERVAL_SECONDS = 0.25
JUPITER_RATE_LIMIT_BACKOFF_SECONDS = 10.0


def cargar_keypair_desde_env(rpc_url: str = "") -> Keypair:
    """Carga la wallet escaneando rutas BIP44 y seleccionando la que tenga saldo SOL."""
    raw_key = (os.getenv("PRIVATE_KEY") or os.getenv("SOLANA_PRIVATE_KEY") or "").strip()
    if not raw_key:
        raise ValueError("No se encontro la variable PRIVATE_KEY en el entorno.")

    # Clave Base58 directa
    if " " not in raw_key:
        try:
            secret_key = base58.b58decode(raw_key)
            if len(secret_key) == 32:
                kp = Keypair.from_seed(secret_key)
            elif len(secret_key) == 64:
                kp = Keypair.from_bytes(secret_key)
            else:
                raise ValueError(f"Longitud de clave inesperada: {len(secret_key)} bytes")
        except Exception as exc:
            raise SwapExecutionError(f"Clave Base58 invalida: {exc}") from exc
        logger.info("Wallet Base58 cargada: {}", kp.pubkey())
        return kp

    # Mnemonic: escanear rutas candidatas en la red
    seed = Bip39SeedGenerator(raw_key).Generate()
    if not rpc_url:
        rpc_url = os.getenv("HELIUS_RPC_URL", "https://api.mainnet-beta.solana.com")

    rutas_candidatas = [f"m/44'/501'/{i}'/0'" for i in range(5)] + [f"m/44'/501'/{i}'" for i in range(5)]

    candidato_seleccionado = None
    max_balance: float = -1

    logger.info("Escaneando subcuentas de las 24 palabras en la red Solana...")

    with httpx.Client(timeout=10) as client:
        for path in rutas_candidatas:
            try:
                kp = Keypair.from_seed_and_derivation_path(seed, path)
                pubkey = kp.pubkey()
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getBalance",
                    "params": [str(pubkey)],
                }
                resp = client.post(rpc_url, json=payload)
                data = resp.json()
                balance_lamports = data.get("result", {}).get("value", 0) or 0
                balance_sol = balance_lamports / 1_000_000_000

                logger.info("  Ruta `{}` -> Wallet: {} | Saldo: {} SOL", path, pubkey, balance_sol)

                if balance_sol > max_balance:
                    max_balance = balance_sol
                    candidato_seleccionado = kp

                if balance_sol > 0:
                    break
            except Exception:
                continue

    if not candidato_seleccionado:
        candidato_seleccionado = Keypair.from_seed_and_derivation_path(seed, "m/44'/501'/0'/0'")

    logger.info(
        "WALLET SELECCIONADA: {} (Saldo: {} SOL)",
        candidato_seleccionado.pubkey(),
        max_balance if max_balance >= 0 else 0,
    )
    return candidato_seleccionado


@dataclass
class Position:
    """Estado mutable de una posición abierta en un token."""

    mint: str
    token_amount_ui: float = 0.0
    entry_price: float = 0.0
    peak_price: float = 0.0
    sol_invested: float = 0.0
    trailing_active: bool = False
    created_at: float = field(default_factory=time.time)


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
        self.keypair: Keypair = cargar_keypair_desde_env(rpc_url)
        self.rpc_url = rpc_url
        self.slippage_bps = slippage_bps
        self.buy_amount_sol = buy_amount_sol
        self.wallet_pubkey = str(self.keypair.pubkey())

        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        self.trailing_activation_pct = trailing_activation_pct
        self.trailing_distance_pct = trailing_distance_pct
        self.dry_run = dry_run

        # Cliente RPC persistente: se reutiliza en todas las llamadas en vez
        # de crear un nuevo AsyncClient (y su pool HTTP) por cada petición.
        self._rpc_client: AsyncClient = AsyncClient(rpc_url)

        self.positions: dict[str, Position] = {}
        self._load_exec_positions()
        # Tokens detectados en la bonding curve de Pump.fun (sin ruta en
        # Jupiter/Raydium): para ellos se salta Jupiter y se vende directo.
        self.pump_bonding_tokens: set[str] = set()
        # Caché de decimales (mint -> (decimales|None, expira_en)).
        # Positiva: 24h (los decimales de un token no cambian).
        # Negativa: 10 min tras un fallo/429, para no martillar al RPC
        # (GetTokenSupply erró todos a la vez cada ~2.5s antes del fix).
        self._decimals_cache: dict[str, tuple[Optional[int], float]] = {}
        # Caché de quotes de Jupiter: (input, output, amount, slippage) -> (ts, quote).
        # Las compras DCA del mismo token y mismo SOL (muy frecuentes) reaprovechan
        # la última cotización real en vez de multiplicar llamadas a Jupiter.
        self._quote_cache: dict[tuple, tuple[float, dict]] = {}
        # Throttle global: nunca dejar pasar menos de JUPITER_MIN_CALL_INTERVAL_SECONDS
        # entre llamadas HTTP a Jupiter, sea cual sea el llamador (buy/sell/price).
        self._jupiter_lock = asyncio.Lock()
        self._jupiter_last_call = 0.0
        # Circuit-breaker: tras un 429 se evita llamar a Jupiter durante
        # JUPITER_RATE_LIMIT_BACKOFF_SECONDS (el fallback cae a caché/cotización
        # simulada en DRY_RUN en vez de re-peguntar y quemar el rate limit).
        self._jupiter_blocked_until = 0.0

    @staticmethod
    def _decode_transaction(raw_tx: Any) -> bytes:
        """Decodifica la transacción devuelta por Jupiter (str o lista)."""
        if isinstance(raw_tx, str):
            return bytes.fromhex(raw_tx[2:]) if raw_tx.startswith("0x") else base58.b58decode(raw_tx)
        elif isinstance(raw_tx, list):
            return bytes(raw_tx)
        raise SwapExecutionError("Formato de transacción no soportado")

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
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount_lamports),
            "slippageBps": self.slippage_bps if slippage_bps is None else slippage_bps,
            "onlyDirectRoutes": "false",
        }
        cache_key = (input_mint, output_mint, amount_lamports, params["slippageBps"])

        # Caché: cotización real reciente -> reutilizar sin llamar a Jupiter.
        # Evita que las ráfagas DCA (mismo mint, mismo monto) martillen la API.
        now = time.monotonic()
        cached = self._quote_cache.get(cache_key)
        if cached and now - cached[0] < JUPITER_QUOTE_CACHE_TTL_SECONDS:
            return cached[1]

        async with self._jupiter_lock:
            now = time.monotonic()
            cached = self._quote_cache.get(cache_key)
            if cached and now - cached[0] < JUPITER_QUOTE_CACHE_TTL_SECONDS:
                return cached[1]

            # Circuit-breaker: tras un 429 no tocar Jupiter un rato.
            if now < self._jupiter_blocked_until:
                if simulate:
                    logger.warning(
                        "⚠️ Jupiter en backoff tras 429 ({:.0f}s restantes). Cotización simulada.",
                        self._jupiter_blocked_until - now,
                    )
                    return self._simulated_quote(input_mint, output_mint, amount_lamports)
                raise SwapExecutionError(
                    f"Jupiter rate limit (backoff {self._jupiter_blocked_until - now:.0f}s)"
                )

            # Throttle: respetar el intervalo mínimo entre llamadas HTTP.
            wait = self._jupiter_last_call + JUPITER_MIN_CALL_INTERVAL_SECONDS - now
            if wait > 0:
                await asyncio.sleep(wait)

            try:
                try:
                    quote = await self._request_quote(session, JUPITER_QUOTE_URL, params)
                except (aiohttp.ClientConnectorError, OSError, asyncio.TimeoutError) as exc:
                    logger.warning(
                        "Jupiter principal {} falló por red ({}); reintentando con fallback {}",
                        JUPITER_QUOTE_URL, exc, JUPITER_FALLBACK_URL,
                    )
                    quote = await self._request_quote(session, JUPITER_FALLBACK_URL, params)
            except (SwapExecutionError, aiohttp.ClientConnectorError, OSError, asyncio.TimeoutError) as exc:
                if _is_rate_limit(exc):
                    self._jupiter_blocked_until = time.monotonic() + JUPITER_RATE_LIMIT_BACKOFF_SECONDS
                    logger.warning(
                        "Jupiter 429 → backoff de {:.0f}s. ({})",
                        JUPITER_RATE_LIMIT_BACKOFF_SECONDS, exc,
                    )
                    if simulate:
                        return self._simulated_quote(input_mint, output_mint, amount_lamports)
                    raise
                if simulate:
                    logger.warning(
                        "⚠️ Token sin ruta en Jupiter (Pump.fun reciente). "
                        "Generando cotización simulada para test. ({})", exc,
                    )
                    return self._simulated_quote(input_mint, output_mint, amount_lamports)
                raise
            finally:
                self._jupiter_last_call = time.monotonic()

            # Solo cachear cotizaciones reales (no simuladas).
            if not quote.get("simulated"):
                self._quote_cache[cache_key] = (time.monotonic(), quote)
            return quote

    def _simulated_quote(
        self,
        input_mint: str,
        output_mint: str,
        amount_lamports: int,
    ) -> dict[str, Any]:
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
        require_confirmation: bool = True,
    ) -> Signature:
        swap_payload = {
            "quoteResponse": quote,
            "userPublicKey": self.wallet_pubkey,
            "wrapAndUnwrapSol": True,
            "computeUnitLimit": COMPUTE_UNIT_LIMIT,
            "computeUnitPriceMicroLamports": COMPUTE_UNIT_PRICE_MICRO_LAMPORTS,
        }
        async with session.post(
            JUPITER_SWAP, json=swap_payload, headers=_USER_AGENT_HEADERS
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise SwapExecutionError(f"Jupiter swap falló ({resp.status}): {text[:200]}")
            swap_data = await resp.json()

        raw_tx = swap_data["transaction"]
        tx_bytes = self._decode_transaction(raw_tx)

        tx = VersionedTransaction.from_bytes(bytes(tx_bytes))
        signed_tx = VersionedTransaction(tx.message, [self.keypair])

        return await self._submit_signed_transaction(
            signed_tx, require_confirmation=require_confirmation
        )

    async def _await_signature_settled(
        self, txid: Signature, *, attempts: int = 4, delay: float = 1.5
    ) -> tuple[bool, Optional[str]]:
        """Consulta el estado de una tx hasta que se resuelve o se agotan los intentos.

        Devuelve `(resuelta, error)`:
          - `(True, None)`     -> la tx está en un bloque y NO falló.
          - `(True, "<err>")`  -> la tx cayó/revirtió on-chain.
          - `(False, None)`    -> sigue sin resolverse (no se puede afirmar nada).
        """
        client = self._rpc_client
        for attempt in range(attempts):
            try:
                resp = await client.get_signature_statuses([txid])
                value = resp.value[0] if resp.value else None
                if value is not None:
                    if value.err is None:
                        return True, None
                    return True, str(value.err)
            except Exception as exc:  # noqa: BLE001 - timeout/RPC del proveedor
                logger.debug("get_signature_statuses falló para {}: {}", txid, exc)
            if attempt < attempts - 1:
                await asyncio.sleep(delay)
        return False, None

    async def _submit_signed_transaction(
        self,
        signed_tx: VersionedTransaction,
        *,
        require_confirmation: bool = True,
    ) -> Signature | None:
        """Envía y confirma una transacción firmada vía RPC.

        En modo estricto (`require_confirmation=True`, compras) la operación
        SOLO es exitosa si la transacción queda confirmada en un bloque de
        Solana con commitment "confirmed". Si se cae, vence por timeout o el
        RPC devuelve error/reversión, se registra el error exacto y se lanza
        `SwapExecutionError` para que el llamador NO notifique la compra como
        ejecutada.

        En modo venta (`require_confirmation=False`) NUNCA se declara exitosa una
        venta que no se puede verificar on-chain: se devuelve `None`. Antes esto
        devolvía el txid de una tx caída/revertida, y el llamador (que solo
        comprueba la verdad del txid) borraba la posición y marcaba la firma como
        ejecutada: los tokens se quedaban en la wallet sin dueño y el reintento
        era imposible. Ante un timeout/RPC se reconsulta el estado de la tx un
        número acotado de veces; si aun así no se resuelve, la venta se considera
        NO ocurida y la posición se conserva para reintentarla.
        """
        client = self._rpc_client
        opts = TxOpts(skip_preflight=True, skip_confirmation=True)
        res = await client.send_raw_transaction(
            bytes(signed_tx), opts=opts  # type: ignore[arg-type]
        )
        if not res.value:
            raise SwapExecutionError("Respuesta de envío sin firma")
        txid = res.value

        try:
            confirmation = await client.confirm_transaction(txid, commitment="confirmed")
        except Exception as exc:  # noqa: BLE001 - timeout/RPC del proveedor
            logger.error(
                "No se pudo confirmar la transacción {} (timeout/RPC): {}", txid, exc
            )
            if require_confirmation:
                raise SwapExecutionError(
                    f"Transacción NO confirmada on-chain ({txid}): {exc}"
                ) from exc

            settled, status_err = await self._await_signature_settled(txid)
            if settled and status_err is None:
                logger.success("Venta confirmada on-chain (reconsulta): {}", txid)
                return txid
            if settled:
                logger.error(
                    "Venta revertida/caída on-chain ({}): {} | la posición se conserva",
                    status_err, txid,
                )
                return None
            logger.error(
                "Venta NO verificable on-chain tras reconsultas: {} | "
                "la posición se conserva para reintentar", txid,
            )
            return None

        status = confirmation.value[0] if confirmation.value else None
        if status is None or status.err is not None:
            logger.error(
                "Transacción NO confirmada/revertida on-chain (estado={}): {}",
                status.err if status else "sin estado", txid,
            )
            if require_confirmation:
                raise SwapExecutionError(
                    f"Transacción reversada/descartada ({txid}): "
                    f"{status.err if status else 'sin estado'}"
                )
            logger.error(
                "Venta no ejecutada on-chain: {} | la posición se conserva para reintentar",
                txid,
            )
            return None

        logger.success("Transacción confirmada on-chain: {}", txid)
        return txid

    # ------------------------------------------------------------ Public
    @staticmethod
    def _is_pump_fun_mint(mint: str) -> bool:
        """True si el mint parece estar en la bonding curve de Pump.fun (termina en 'pump')."""
        return str(mint).lower().endswith("pump")


    async def buy_token(self, token_mint: str, dry_run: Optional[bool] = None) -> Signature | str | None:
        """Compra un token. Ruteo estricto: Pump.fun → PumpPortal, resto → Jupiter.

        - Si `mint_address` termina en "pump" → compra directa por PumpPortal.
        - Si NO termina en "pump" → intenta Jupiter. Si Jupiter devuelve 404
          (sin liquidez), **omite en silencio** (sin error a Telegram).
        """
        simulate = self.dry_run if dry_run is None else dry_run
        amount_lamports = int(self.buy_amount_sol * 1_000_000_000)

        # --- Ruteo estricto por sufijo ---
        if self._is_pump_fun_mint(token_mint):
            if simulate:
                logger.info("[DRY_RUN] Compra simulada (Pump.fun) de {} | Monto: {} SOL", token_mint, self.buy_amount_sol)
                await self._register_position(token_mint, None, via_pumpfun=True, simulate=True)
                return "DRY_RUN"
            logger.info("Token Pump.fun detectado: comprando directo por PumpPortal.", token_mint)
            sig = await self._buy_via_pumpportal(token_mint)
            await self._register_position(token_mint, None, via_pumpfun=True)
            return sig

        # --- Tokens normales: Jupiter ---
        if simulate:
            logger.info("[DRY_RUN] Compra simulada de {} | Monto: {} SOL", token_mint, self.buy_amount_sol)
            async with aiohttp.ClientSession() as session:
                quote = await self._get_quote(session, SOL_MINT, token_mint, amount_lamports, simulate=True)
            if not quote:
                logger.info("Omitiendo {}: token sin liquidez en Jupiter/DEX.", token_mint)
                return None
            await self._register_position(token_mint, quote, via_pumpfun=False, simulate=True)
            return "DRY_RUN"

        try:
            async with aiohttp.ClientSession() as session:
                quote = await self._get_quote(session, SOL_MINT, token_mint, amount_lamports, simulate=False)
                if not quote:
                    logger.info("Omitiendo {}: token sin liquidez en Jupiter/DEX.", token_mint)
                    return None
                sig = await self._build_and_send_swap(session, quote, require_confirmation=True)
        except Exception as exc:
            if _is_rate_limit(exc):
                logger.warning("Jupiter rate limit (429) al comprar {}. Reintentando tras pausa...", token_mint)
                await asyncio.sleep(2.0)
                try:
                    async with aiohttp.ClientSession() as session:
                        quote = await self._get_quote(session, SOL_MINT, token_mint, amount_lamports, simulate=False)
                        if not quote:
                            logger.info("Omitiendo {} tras reintento: sin liquidez.", token_mint)
                            return None
                        sig = await self._build_and_send_swap(session, quote, require_confirmation=True)
                except Exception as retry_exc:
                    logger.warning("Reintento de compra de {} también falló: {}", token_mint, retry_exc)
                    return None
            elif _is_route_not_found(exc):
                logger.info("Omitiendo {}: token sin liquidez en Jupiter/DEX.", token_mint)
                return None
            else:
                raise

        await self._register_position(token_mint, quote, via_pumpfun=False)
        return sig

    async def _register_position(
        self, token_mint: str, quote: Optional[dict], *, via_pumpfun: bool, simulate: bool = False,
    ) -> None:
        """Registra la posición comprada en `self.positions`."""
        if via_pumpfun:
            if simulate:
                token_qty_ui = 0.0
            else:
                # -1.0 = error de RPC (no "no tengo el token"): tratarlo como 0
                # para no registrar una cantidad negativa en la posicion.
                token_qty_ui = max(0.0, await self._get_token_balance_ui(token_mint))
        else:
            out_amount = float(
                (quote or {}).get("outAmount", (quote or {}).get("routePlan", [{}])[0].get("outAmount", 0)) or 0
            )
            decimals = await self._get_token_decimals(token_mint)
            token_qty_ui = out_amount / (10 ** decimals) if decimals else 0.0

        if simulate:
            # Usar la MISMA secuencia de fuentes que al vender (get_token_price),
            # no una fuente distinta por pump/non-pump. Si la compra y la venta
            # usan precios de fuentes distintas (curva virtual vs pool), el PnL
            # se infla artificialmente cuando el token migra a pool entre ambas.
            entry_price_sol = 0.0
            try:
                entry_price_sol = await self.get_token_price(token_mint)
            except Exception:
                pass
            if entry_price_sol <= 0 and via_pumpfun:
                try:
                    entry_price_sol = await self._get_price_from_pumpfun(token_mint)
                except Exception:
                    pass
            if entry_price_sol <= 0 and token_qty_ui > 0 and self.buy_amount_sol > 0:
                entry_price_sol = self.buy_amount_sol / token_qty_ui
            # Estimate token_qty_ui from entry_price for DRY_RUN pump.fun
            if entry_price_sol > 0 and token_qty_ui <= 0 and self.buy_amount_sol > 0:
                token_qty_ui = self.buy_amount_sol / entry_price_sol
        else:
            entry_price_sol = self.buy_amount_sol / token_qty_ui if token_qty_ui else 0.0
            if entry_price_sol <= 0:
                try:
                    entry_price_sol = await self.get_token_price(token_mint)
                except Exception:
                    pass
        entry_price_sol = max(entry_price_sol, 0.0)

        self.positions[token_mint] = Position(
            mint=token_mint,
            token_amount_ui=token_qty_ui,
            entry_price=entry_price_sol,
            peak_price=entry_price_sol,
            sol_invested=self.buy_amount_sol,
        )
        self._save_exec_positions()
        if entry_price_sol > 0:
            logger.info("Posición registrada para {} @ entry={:.9f} SOL", token_mint, entry_price_sol)
        else:
            logger.warning("📌 Posición registrada con entry_price PENDIENTE para {}", token_mint)

    async def _buy_via_pumpportal(
        self,
        mint: str,
        amount_sol: Optional[float] = None,
    ) -> Signature:
        """Compra directa en la bonding curve de Pump.fun vía PumpPortal.

        Payload en SOL (`denominatedInSol="true"`), con `pool: "auto"` para
        detectar automáticamente la mejor ruta, `slippage` 20% y
        `priorityFee` 0.001 SOL. La confirmación on-chain es obligatoria
        (`require_confirmation=True`).
        """
        wallet_pubkey_str = str(self.keypair.pubkey()).strip()
        # Sanitizar amount: aceptar "0.005 SOL", 0.005, "0.005" → float puro
        raw_amount = self.buy_amount_sol if amount_sol is None else amount_sol
        if isinstance(raw_amount, str):
            amount_sol = float(raw_amount.replace("SOL", "").strip())
        else:
            amount_sol = float(raw_amount)
        logger.info(
            "Comprando {} SOL de {} por PumpPortal (bonding curve, slippage 15%)",
            amount_sol, mint,
        )
        # Pre-flight: verificar balance SOL de la wallet
        try:
            balance_resp = await self._rpc_client.get_balance(Pubkey.from_string(wallet_pubkey_str))
            wallet_sol = balance_resp.value / 1_000_000_000 if balance_resp.value else 0.0
            logger.debug("Wallet SOL balance: {:.6f} SOL (necesario: ~{:.6f})", wallet_sol, amount_sol + 0.001)
            if wallet_sol < amount_sol + 0.001:
                raise SwapExecutionError(
                    f"Balance SOL insuficiente: {wallet_sol:.6f} SOL (requiere ~{amount_sol + 0.001:.6f} SOL)"
                )
        except Exception as exc:
            logger.warning("No se pudo verificar balance SOL pre-vuelo: {}", exc)

        payload = {
            "publicKey": wallet_pubkey_str,
            "action": "buy",
            "mint": str(mint).strip(),
            "denominatedInSol": "true",
            "amount": float(amount_sol),
            "slippage": 20,
            "priorityFee": 0.001,
            "pool": "auto",
        }
        logger.debug("PumpPortal payload: {}", payload)
        async with aiohttp.ClientSession() as session:
            async with session.post(
                PUMPPORTAL_TRADE_URL, json=payload, headers=_USER_AGENT_HEADERS
            ) as resp:
                if resp.status != 200:
                    error_body = await resp.text()
                    logger.error("❌ PumpPortal API Error ({}): {} | Payload: {}", resp.status, error_body, payload)
                    raise SwapExecutionError(
                        f"PumpPortal buy falló ({resp.status}): {error_body}. "
                        f"Mint: {mint}, Amount: {amount_sol} SOL, Wallet: {wallet_pubkey_str}"
                    )
                tx_bytes = self._decode_trade_local(await resp.read())
        tx = VersionedTransaction.from_bytes(tx_bytes)
        signed_tx = VersionedTransaction(tx.message, [self.keypair])

        logger.info("Compra directa Pump.fun de {} enviada a la red.", mint)
        return await self._submit_signed_transaction(
            signed_tx, require_confirmation=True
        )

    async def get_token_balance_ui(self, token_mint: str) -> float:
        """Saldo real (en unidades humanas) de un token en la wallet vía RPC.

        Necesario tras una compra directa por Pump.fun, donde no hay quote de
        Jupiter para estimar la cantidad recibida, y para vender SIEMPRE sobre
        el saldo on-chain (única fuente fiel): estimar la cantidad a partir de
        `amount / buy_price` falla cuando el precio de entrada quedó sin
        registrar y hacía que el bot se saltase la venta entera.
        """
        try:
            ata = get_associated_token_address(
                self.keypair.pubkey(), Pubkey.from_string(token_mint)
            )
            resp = await self._rpc_client.get_token_account_balance(ata)
        except Exception as exc:
            logger.debug(
                "get_token_balance_ui fallo para {}: {}",
                token_mint[:8] + "...", exc,
            )
            return -1.0
        if resp is None or resp.value is None or resp.value.ui_amount is None:
            return 0.0
        return float(resp.value.ui_amount)

    # Alias historico interno (mismo comportamiento).
    _get_token_balance_ui = get_token_balance_ui

    async def get_sol_balance(self) -> float:
        """Balance actual de SOL (en unidades humanas) de la wallet del bot.

        Usado para el tope de capital por trade de copy trading.
        """
        try:
            resp = await self._rpc_client.get_balance(self.keypair.pubkey())
            if resp.value is None:
                return 0.0
            return resp.value / 1_000_000_000
        except Exception:
            return 0.0

    async def get_wallet_token_balance(self, wallet_pubkey: str, token_mint: str) -> float:
        """Saldo real (unidades humanas) del token en TODAS las cuentas SPL de
        una wallet arbitraria vía RPC.

        Suma el balance de todas las cuentas token del trader para ese mint
        (ATA estándar, cuentas no derivadas, etc.). El ATA estándar solo no
        sirve: los traders de pump.fun a veces la token sale de cuentas alternas
        y get_token_account_balance fallaba => saldo 0 => el bot creia que
        vendio el 100% cuando en realidad vendio 50% (PnL inflado).

        Devuelve -1.0 si hubo un ERROR de RPC (para distinguirlo de saldo real 0,
        que significaria que el trader vendio el 100%).
        """
        try:
            owner = Pubkey.from_string(wallet_pubkey)
            mint = Pubkey.from_string(token_mint)
            resp = await self._rpc_client.get_token_accounts_by_owner_json_parsed(
                owner,
                TokenAccountOpts(mint=mint),
            )
            total = 0.0
            for item in resp.value or []:
                try:
                    data = getattr(item.account, "data", None)
                    parsed = getattr(data, "parsed", None)
                    if parsed is None:
                        continue
                    info = parsed.info if hasattr(parsed, "info") else (parsed.get("info") if isinstance(parsed, dict) else None)
                    token_amount = getattr(info, "tokenAmount", None) if hasattr(info, "tokenAmount") else ((info or {}).get("tokenAmount") if isinstance(info, dict) else None)
                    ui_amount = getattr(token_amount, "uiAmount", None) if hasattr(token_amount, "uiAmount") else ((token_amount or {}).get("uiAmount") if isinstance(token_amount, dict) else None)
                    if ui_amount is not None:
                        total += float(ui_amount)
                except Exception:
                    # Una cuenta ilegible no debe invalidar el total de las demas
                    continue
            return total
        except Exception as exc:
            logger.debug(
                "get_wallet_token_balance RPC fallo para {} ({}): {}",
                wallet_pubkey[:8] + "...", token_mint[:8] + "...", exc,
            )
            return -1.0

    async def sell_token(
        self,
        token_mint: str,
        token_balance_ui: float,
        slippage_bps: Optional[int] = None,
    ) -> Signature | None:
        """Vende un token, con fallback directo en Pump.fun para la bonding curve.

        Intenta primero la cotización y el swap vía Jupiter. Si Jupiter no
        encuentra ruta (404 / "Route not found", token en bonding curve) o
        satura la API (429 / "Rate limit"), redirige a `_sell_via_pumpportal`
        para una venta directa por PumpPortal. Los tokens ya marcados como
        bonding curve saltan directamente a PumpPortal.

        Devuelve `None` si la venta NO se pudo verificar como ejecutada
        on-chain (tx caída, revertida o irresoluble): el llamador debe entonces
        conservar la posición y reintentar.
        """
        decimals = await self._get_token_decimals(token_mint)
        raw_amount = int(token_balance_ui * (10 ** decimals))

        if token_mint in self.pump_bonding_tokens:
            logger.info(
                "{} ya marcado como bonding curve: venta directa por Pump.fun", token_mint
            )
            return await self._sell_via_pumpportal(
                token_mint, token_balance_ui, slippage_bps=slippage_bps
            )

        try:
            async with aiohttp.ClientSession() as session:
                quote = await self._get_quote(
                    session, token_mint, SOL_MINT, raw_amount,
                    slippage_bps=slippage_bps,
                )
                if not quote:
                    raise SwapExecutionError("Jupiter quote vacía (Route not found)")
                return await self._build_and_send_swap(
                    session, quote, require_confirmation=False
                )
        except SwapExecutionError as exc:
            if _is_rate_limit(exc):
                logger.warning(
                    "Jupiter saturó la API ({}). Pausa de 1s y venta de respaldo por Pump.fun.",
                    exc,
                )
                await asyncio.sleep(1.0)
                self.pump_bonding_tokens.add(token_mint)
                return await self._sell_via_pumpportal(
                    token_mint, token_balance_ui, slippage_bps=slippage_bps
                )
            if _is_route_not_found(exc):
                logger.warning(
                    "Jupiter sin ruta para vender {} (bonding curve): {}. "
                    "Ejecutando venta directa por Pump.fun.",
                    token_mint, exc,
                )
                self.pump_bonding_tokens.add(token_mint)
                return await self._sell_via_pumpportal(
                    token_mint, token_balance_ui, slippage_bps=slippage_bps
                )
            raise

    async def _sell_via_pumpportal(
        self,
        mint: str,
        amount_ui: float,
        slippage_bps: Optional[int] = None,
    ) -> Signature | None:
        """Venta directa en la bonding curve de Pump.fun vía PumpPortal.

        Realiza el POST a `https://pumpportal.fun/api/trade-local` con el
        payload de venta (tipos estrictos: string "true"/"false", float amount,
        pool "auto"), decodifica la transacción, la firma localmente y la
        envía/confirma por el RPC. Devuelve el txid si la venta queda
        confirmada on-chain, o `None` si no se puede verificar.
        """
        wallet_pubkey_str = str(self.wallet_pubkey).strip()
        slippage_pct = float((self.slippage_bps if slippage_bps is None else slippage_bps) / 100.0)
        logger.debug("Vendiendo balance de {} (100%) para {}", amount_ui, mint)
        payload = {
            "publicKey": wallet_pubkey_str,
            "action": "sell",
            "mint": str(mint).strip(),
            "amount": "100%",
            "denominatedInSol": "false",
            "slippage": slippage_pct,
            "priorityFee": 0.00005,
            "pool": "auto",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                PUMPPORTAL_TRADE_URL, json=payload, headers=_USER_AGENT_HEADERS
            ) as resp:
                if resp.status != 200:
                    error_body = await resp.text()
                    logger.error("❌ PumpPortal API Error ({}): {}", resp.status, error_body)
                    raise SwapExecutionError(f"PumpPortal sell falló ({resp.status}): {error_body}")
                tx_bytes = self._decode_trade_local(await resp.read())

        tx = VersionedTransaction.from_bytes(tx_bytes)
        signed_tx = VersionedTransaction(tx.message, [self.keypair])

        logger.info("Venta directa Pump.fun de {} enviada a la red.", mint)
        return await self._submit_signed_transaction(
            signed_tx, require_confirmation=False
        )

    @staticmethod
    def _decode_trade_local(raw_tx: Any) -> bytes:
        """Decodifica la transacción devuelta por PumpPortal (bytes, base64, hex o base58)."""
        if isinstance(raw_tx, bytes):
            return raw_tx
        if isinstance(raw_tx, list):
            return bytes(raw_tx)
        raw = str(raw_tx)
        if raw.startswith("0x"):
            return bytes.fromhex(raw[2:])
        try:
            return base64.b64decode(raw, validate=True)
        except Exception:
            return base58.b58decode(raw)

    # --------------------------------------------------- Price / Monitoring
    async def get_token_price(self, token_mint: str) -> float:
        # Secuencia UNICA y determinista para todas las llamadas (compra y venta).
        # Usar secuencias distintas segun el estado del token hacia que el
        # entry_price (bonding curve virtual) y el current_price (jupiter/gecko)
        # fueran de ordenes de magnitud distintos => PnL inflado falso.
        # Regla: Pump.fun API si sigue en bonding curve, si no DexScreener, fallback Jupiter.
        # En DRY_RUN se omite Jupiter: no hay swap real que cotizar, consume el
        # rate limit de la API de trading y las quotes simuladas (~1 SOL/tk)
        # contaminarían el PnL del monitor con valores absurdos.
        try_sequence = (
            ("pumpfun", "bonding_curve", "dexscreener")
            if self.dry_run
            else ("pumpfun", "bonding_curve", "dexscreener", "jupiter")
        )

        price_sol = 0.0
        for source in try_sequence:
            if price_sol > 0:
                break

            if source == "jupiter":
                try:
                    decimals = await self._get_token_decimals(token_mint)
                    amount_lamports = int(1 * (10 ** decimals))
                    async with aiohttp.ClientSession() as session:
                        quote = await self._get_quote(
                            session, token_mint, SOL_MINT, amount_lamports
                        )
                    out = float(quote.get("outAmount") or 0)
                    p = out / 1_000_000_000
                    if p > 0:
                        price_sol = p
                        logger.info("Precio de {} vía Jupiter (SOL): {:.10g}", token_mint, price_sol)
                except Exception as exc:
                    logger.debug("Jupiter sin precio para {} ({}); intentando fallbacks.", token_mint, exc)

            elif source == "dexscreener":
                try:
                    p = await self._get_price_from_dexscreener(token_mint)
                    if p and p > 0:
                        price_sol = float(p)
                        logger.info("Precio de {} vía DexScreener (SOL): {:.10g}", token_mint, price_sol)
                except Exception as exc:
                    logger.warning("DexScreener sin precio para {} ({}); intentando fallbacks.", token_mint, exc)

            elif source == "pumpfun":
                try:
                    p = await self._get_price_from_pumpfun(token_mint)
                    if p and p > 0:
                        price_sol = float(p)
                        logger.info("Precio de {} vía Pump.fun (SOL): {:.10g}", token_mint, price_sol)
                except Exception as exc:
                    logger.error("Pump.fun sin precio para {}: {}", token_mint, exc)

            elif source == "bonding_curve":
                try:
                    from core.bonding_curve import get_bonding_curve_price
                    rpc_url = self.rpc_url or ""
                    if rpc_url:
                        p = await get_bonding_curve_price(rpc_url, token_mint)
                        if p is not None and p > 0:
                            price_sol = float(p)
                            logger.info("Precio de {} vía bonding curve RPC (SOL): {:.10g}", token_mint, price_sol)
                except Exception as exc:
                    logger.debug("Bonding curve RPC sin precio para {} ({}); intentando fallbacks.", token_mint, exc)

            elif source == "gecko":
                try:
                    from core.gecko_price import get_price_from_geckoterminal
                    p = await get_price_from_geckoterminal(token_mint)
                    if p is not None and p > 0:
                        price_sol = float(p)
                        logger.info("Precio de {} vía GeckoTerminal (SOL): {:.10g}", token_mint, price_sol)
                except Exception as exc:
                    logger.debug("GeckoTerminal sin precio para {} ({}); intentando fallbacks.", token_mint, exc)

        price_sol = float(price_sol or 0.0)
        if price_sol <= 0:
            raise SwapExecutionError(f"No se pudo obtener precio de {token_mint} desde ningún endpoint.")

        position = self.positions.get(token_mint)
        if position is not None and (not position.entry_price or position.entry_price <= 0):
            position.entry_price = price_sol
            position.peak_price = max(position.peak_price, price_sol)
            logger.info("Entry_price base adoptado para {} @ {:.10g} SOL", token_mint, price_sol)

        return price_sol

    async def _get_price_from_dexscreener(self, token_mint: str) -> float:
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

        best = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))

        quote_token = best.get("quoteToken", {}) or {}
        quote_symbol = str(quote_token.get("symbol", "")).upper()
        quote_addr = str(quote_token.get("address", ""))
        is_sol_quote = quote_symbol in ("SOL", "WSOL", "") or quote_addr == SOL_MINT

        # Si el par es contra SOL, priceNative es directamente el precio en SOL
        if is_sol_quote:
            try:
                price_native = float(best.get("priceNative", 0) or 0)
                if price_native > 0:
                    return price_native
            except (ValueError, TypeError):
                pass

        # Si el par es contra USD (USDC/USDT) o no se pudo leer priceNative, convertir USD a SOL
        price_usd = best.get("priceUsd")
        if price_usd:
            try:
                sol_usd = await self._get_sol_usd_price()
                if sol_usd > 0:
                    converted = float(price_usd) / sol_usd
                    if converted > 0:
                        return converted
            except Exception:
                pass

        return 0.0

    async def _get_price_from_pumpfun(self, token_mint: str) -> float:
        url = f"https://frontend-api.pump.fun/coins/{token_mint}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_USER_AGENT_HEADERS) as resp:
                if resp.status != 200:
                    logger.debug("Pump.fun respondió {} para {}", resp.status, token_mint)
                    return 0.0
                data = await resp.json()

        # Si el token ya completó la curva de bonding (migró a Raydium), la bonding curve
        # de Pump.fun está congelada y no refleja el precio real actual de mercado.
        if data.get("complete") is True:
            logger.debug("Token {} ya completó bonding curve en Pump.fun; delegando a DEX", token_mint)
            return 0.0

        virtual_sol = data.get("virtual_sol_reserves")
        virtual_tokens = data.get("virtual_token_reserves")
        if virtual_sol and virtual_tokens:
            try:
                vsol = float(virtual_sol) / 1e9
                vtok = float(virtual_tokens) / 1e6
                if vtok > 0:
                    return vsol / vtok
            except (ValueError, TypeError):
                pass

        sol_reserves = data.get("sol_reserves")
        token_reserves = data.get("token_reserves")
        if sol_reserves and token_reserves:
            try:
                sol_r = float(sol_reserves) / 1e9
                tok_r = float(token_reserves) / 1e6
                if tok_r > 0:
                    return sol_r / tok_r
            except (ValueError, TypeError):
                pass

        return 0.0

    async def _get_sol_usd_price(self) -> float:
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
        return 180.0

    # ------------------------------------------------------- Persistencia
    EXEC_POSITIONS_FILE = "exec_positions.json"

    def _save_exec_positions(self) -> None:
        """Guarda posiciones del executor a disco."""
        import json as _json
        from pathlib import Path
        try:
            data = {}
            for mint, pos in self.positions.items():
                data[mint] = {
                    "mint": pos.mint,
                    "token_amount_ui": pos.token_amount_ui,
                    "entry_price": pos.entry_price,
                    "peak_price": pos.peak_price,
                    "sol_invested": pos.sol_invested,
                    "trailing_active": pos.trailing_active,
                    "created_at": pos.created_at,
                }
            Path(self.EXEC_POSITIONS_FILE).write_text(
                _json.dumps(data, indent=2), encoding="utf-8"
            )
        except Exception:
            pass

    def _load_exec_positions(self) -> None:
        """Carga posiciones del executor desde disco."""
        import json as _json
        from pathlib import Path
        path = Path(self.EXEC_POSITIONS_FILE)
        if not path.exists():
            return
        try:
            data = _json.loads(path.read_text(encoding="utf-8"))
            for mint, info in data.items():
                self.positions[mint] = Position(
                    mint=info["mint"],
                    token_amount_ui=info.get("token_amount_ui", 0.0),
                    entry_price=info.get("entry_price", 0.0),
                    peak_price=info.get("peak_price", 0.0),
                    sol_invested=info.get("sol_invested", 0.0),
                    trailing_active=info.get("trailing_active", False),
                    created_at=info.get("created_at", 0.0),
                )
            if self.positions:
                logger.info(
                    "Executor: posiciones cargadas desde disco: {}",
                    len(self.positions),
                )
        except Exception:
            pass

    async def get_token_symbol(self, token_mint: str) -> str:
        symbol = await self._get_symbol_from_dexscreener(token_mint)
        if symbol:
            return symbol
        symbol = await self._get_symbol_from_pumpfun(token_mint)
        if symbol:
            return symbol
        try:
            from core.gecko_price import get_symbol_from_geckoterminal
            symbol = await get_symbol_from_geckoterminal(token_mint)
            if symbol:
                return symbol
        except Exception:
            pass
        fallback = str(token_mint)[:6].upper()
        logger.info("Sin ticker en APIs para {}; usando fallback {}", token_mint, fallback)
        return fallback

    async def _get_symbol_from_dexscreener(self, token_mint: str) -> str:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_USER_AGENT_HEADERS) as resp:
                if resp.status != 200:
                    return ""
                data = await resp.json()

        pairs = data.get("pairs") or []
        if not pairs:
            return ""
        best = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
        base_token = best.get("baseToken") or {}
        symbol = str(base_token.get("symbol", "") or "").strip()
        return symbol.upper()

    async def _get_symbol_from_pumpfun(self, token_mint: str) -> str:
        url = f"https://frontend-api.pump.fun/coins/{token_mint}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_USER_AGENT_HEADERS) as resp:
                if resp.status != 200:
                    return ""
                data = await resp.json()

        symbol = str(data.get("symbol", "") or "").strip()
        return symbol.upper()

    async def monitor_position(self, token_mint: str) -> tuple[str, float]:
        position = self.positions.get(token_mint)
        if position is None:
            return "", 0.0

        try:
            current_price = await self.get_token_price(token_mint)
        except Exception as exc:
            logger.warning("No se pudo consultar precio de {}: {}", token_mint, exc)
            return "", 0.0

        if not position.entry_price or position.entry_price <= 0:
            try:
                position.entry_price = await self.get_token_price(token_mint)
            except Exception as exc:
                logger.warning("No se pudo re-consultar precio de entrada de {}: {}", token_mint, exc)
                return "", 0.0
            logger.info("Precio de entrada re-establecido para {}: {:.10g}", token_mint, position.entry_price)

        position.peak_price = max(position.peak_price, current_price)
        pnl_pct = (
            (current_price - position.entry_price) / position.entry_price * 100
        ) if position.entry_price else 0.0

        if pnl_pct <= -self.stop_loss_pct:
            await self.close_position(token_mint, "STOP_LOSS", pnl_pct)
            return "STOP_LOSS", pnl_pct

        if pnl_pct >= self.take_profit_pct:
            await self.close_position(token_mint, "TAKE_PROFIT", pnl_pct)
            return "TAKE_PROFIT", pnl_pct

        if pnl_pct >= self.trailing_activation_pct:
            position.trailing_active = True

        if position.trailing_active:
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
        position = self.positions.get(token_mint)
        if position is None:
            return

        if self.dry_run:
            logger.info(
                "[DRY_RUN] Venta simulada de {} por {} (PnL {:.2f}%)",
                token_mint, reason, pnl_pct,
            )
            self.positions.pop(token_mint, None)
            self._save_exec_positions()
            return

        try:
            amount = float(position.token_amount_ui or 0.0)
            if amount <= 0:
                # La cantidad registrada quedo vacia (no se pudo estimar al
                # comprar): el SALDO ON-CHAIN es la unica fuente fiel. Vender
                # 0 tokens hacia que Jupiter no encontrase ruta y la posicion
                # se quedara abierta para siempre ("no vende").
                amount = await self.get_token_balance_ui(token_mint)
                if amount > 0:
                    position.token_amount_ui = amount
                    logger.info(
                        "Cantidad a vender recuperada on-chain para {}: {:.6g} tokens",
                        token_mint, amount,
                    )
            if amount <= 0:
                logger.error(
                    "Sin saldo de {} para cerrar por {}: la posicion se mantiene abierta",
                    token_mint, reason,
                )
                return
            sig = await self.sell_token(
                token_mint, amount,
                slippage_bps=slippage_bps,
            )
            if not sig:
                # Enviada pero NO confirmada (descartada/revertida): NO borrar
                # la posicion ni declararla cerrada.
                logger.error(
                    "Cierre de {} por {} no confirmado on-chain; la posicion sigue abierta",
                    token_mint, reason,
                )
                return
        except Exception as exc:
            logger.error("Error vendiendo {} ({}): {}", token_mint, reason, exc)
            return

        self.positions.pop(token_mint, None)
        self._save_exec_positions()
        logger.success("Posición {} cerrada por {} (PnL {:.2f}%)", token_mint, reason, pnl_pct)

    # ------------------------------------------------------------ Decimals
    async def _get_token_decimals(self, token_mint: str) -> int:
        """Consulta los decimales del token vía RPC mediante get_token_supply.

        Con caché: los aciertos duran 24h y los fallos (p.ej. 429 de
        GetTokenSupply) se guardan 10 min y se anotan solo una vez, para no
        inundar de warnings y no volver a pegarle al RPC con cada polling.
        """
        now = time.monotonic()
        cached = self._decimals_cache.get(token_mint)
        if cached is not None:
            decimals, expires = cached
            if now < expires:
                if decimals is None:
                    return 6
                return decimals

        try:
            resp = await self._rpc_client.get_token_supply(Pubkey.from_string(token_mint))
            if resp.value and resp.value.decimals is not None:
                self._decimals_cache[token_mint] = (resp.value.decimals, now + 86400)
                return resp.value.decimals
            self._decimals_cache[token_mint] = (None, now + 600)
        except Exception as exc:
            logger.warning(
                "No se pudieron obtener decimales para {}: {} (se reintentará en ~10 min)",
                token_mint, exc,
            )
            self._decimals_cache[token_mint] = (None, now + 600)

        return 6
