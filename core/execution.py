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

        self.positions: dict[str, Position] = {}
        # Tokens detectados en la bonding curve de Pump.fun (sin ruta en
        # Jupiter/Raydium): para ellos se salta Jupiter y se vende directo.
        self.pump_bonding_tokens: set[str] = set()

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
                    "Generando cotización simulada para test. ({})", exc,
                )
                return self._simulated_quote(input_mint, output_mint, amount_lamports)
            raise

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

    async def _submit_signed_transaction(
        self,
        signed_tx: VersionedTransaction,
        *,
        require_confirmation: bool = True,
    ) -> Signature:
        """Envía y confirma una transacción firmada vía RPC.

        En modo estricto (`require_confirmation=True`, compras) la operación
        SOLO es exitosa si la transacción queda confirmada en un bloque de
        Solana con commitment "confirmed". Si se cae, vence por timeout o el
        RPC devuelve error/reversión, se registra el error exacto y se lanza
        `SwapExecutionError` para que el llamador NO notifique la compra como
        ejecutada.

        En modo venta (`require_confirmation=False`) no se re-lanza: se devuelve
        el txid recibido para evitar reintentos que dupliquen la salida.
        """
        async with AsyncClient(self.rpc_url) as client:
            res = await client.send_raw_transaction(
                bytes(signed_tx),
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
                logger.success("Swap enviado pero sin confirmar: {}", txid)
                return txid

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
                logger.success("Swap enviado: {}", txid)
                return txid

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
            err = str(exc)
            if "404" in err or "Route not found" in err or "no route" in err.lower():
                logger.info("Omitiendo {}: token sin liquidez en Jupiter/DEX.", token_mint)
                return None
            raise

        await self._register_position(token_mint, quote, via_pumpfun=False)
        return sig

    async def _register_position(
        self, token_mint: str, quote: Optional[dict], *, via_pumpfun: bool, simulate: bool = False,
    ) -> None:
        """Registra la posición comprada en `self.positions`."""
        if via_pumpfun:
            token_qty_ui = await self._get_token_balance_ui(token_mint)
        else:
            out_amount = float(
                (quote or {}).get("outAmount", (quote or {}).get("routePlan", [{}])[0].get("outAmount", 0)) or 0
            )
            decimals = await self._get_token_decimals(token_mint)
            token_qty_ui = out_amount / (10 ** decimals) if decimals else 0.0

        if simulate:
            entry_price_sol = 0.0
            try:
                entry_price_sol = await self.get_token_price(token_mint)
            except Exception:
                pass
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

        Payload en SOL (`denominatedInSol="true"`), con `pool: "pump"` para la
        bonding curve, `slippage` 15% y `priorityFee` 0.0001 SOL. La
        confirmación on-chain es obligatoria (`require_confirmation=True`).
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
            async with AsyncClient(self.rpc_url) as client:
                balance_resp = await client.get_balance(Pubkey.from_string(wallet_pubkey_str))
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
            "slippage": 15,
            "priorityFee": 0.0005,
            "pool": "auto",
        }
        logger.debug("PumpPortal payload: {}", payload)
        async with aiohttp.ClientSession() as session:
            async with session.post(
                PUMPPORTAL_TRADE_URL, data=payload, headers=_USER_AGENT_HEADERS
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

    async def _get_token_balance_ui(self, token_mint: str) -> float:
        """Saldo real (en unidades humanas) de un token en la wallet vía RPC.

        Necesario tras una compra directa por Pump.fun, donde no hay quote de
        Jupiter para estimar la cantidad recibida.
        """
        ata = get_associated_token_address(
            self.keypair.pubkey(), Pubkey.from_string(token_mint)
        )
        async with AsyncClient(self.rpc_url) as client:
            resp = await client.get_token_account_balance(ata)
            if not resp.value or resp.value.ui_amount is None:
                return 0.0
            return float(resp.value.ui_amount)

    async def sell_token(
        self,
        token_mint: str,
        token_balance_ui: float,
        slippage_bps: Optional[int] = None,
    ) -> Signature:
        """Vende un token, con fallback directo en Pump.fun para la bonding curve.

        Intenta primero la cotización y el swap vía Jupiter. Si Jupiter no
        encuentra ruta (404 / "Route not found", token en bonding curve) o
        satura la API (429 / "Rate limit"), redirige a `_sell_via_pumpportal`
        para una venta directa por PumpPortal. Los tokens ya marcados como
        bonding curve saltan directamente a PumpPortal.
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
    ) -> Signature:
        """Venta directa en la bonding curve de Pump.fun vía PumpPortal.

        Realiza el POST a `https://pumpportal.fun/api/trade-local` con el
        payload de venta (tipos estrictos: string "true"/"false", float amount,
        pool "pump"), decodifica la transacción, la firma localmente y la
        envía/confirma por el RPC. Devuelve el txid (Signature).
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
                PUMPPORTAL_TRADE_URL, data=payload, headers=_USER_AGENT_HEADERS
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
        is_pump_mint = str(token_mint).lower().endswith("pump")
        try_sequence = (
            ("dexscreener", "pumpfun", "jupiter")
            if is_pump_mint
            else ("jupiter", "dexscreener", "pumpfun")
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
                    price_sol = out / 1_000_000_000
                    if price_sol > 0:
                        logger.info("Precio de {} vía Jupiter (SOL): {:.10g}", token_mint, price_sol)
                except Exception as exc:
                    logger.debug("Jupiter sin precio para {} ({}); intentando fallbacks.", token_mint, exc)

            elif source == "dexscreener":
                try:
                    price_sol = await self._get_price_from_dexscreener(token_mint)
                    if price_sol > 0:
                        logger.info("Precio de {} vía DexScreener (SOL): {:.10g}", token_mint, price_sol)
                except Exception as exc:
                    logger.warning("DexScreener sin precio para {} ({}); intentando fallbacks.", token_mint, exc)

            elif source == "pumpfun":
                try:
                    price_sol = await self._get_price_from_pumpfun(token_mint)
                    if price_sol > 0:
                        logger.info("Precio de {} vía Pump.fun (SOL): {:.10g}", token_mint, price_sol)
                except Exception as exc:
                    logger.error("Pump.fun sin precio para {}: {}", token_mint, exc)

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

        try:
            price_native = float(best.get("priceNative", 0) or 0)
        except (ValueError, TypeError):
            price_native = 0.0
        if price_native > 0:
            return price_native

        price_usd = best.get("priceUsd")
        if price_usd:
            try:
                sol_usd = await self._get_sol_usd_price()
                if sol_usd > 0:
                    return float(price_usd) / sol_usd
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

        virtual_sol = data.get("virtual_sol_reserves")
        virtual_tokens = data.get("virtual_token_reserves")
        if virtual_sol and virtual_tokens:
            try:
                vsol = float(virtual_sol)
                vtok = float(virtual_tokens)
                if vtok > 0:
                    return vsol / vtok
            except (ValueError, TypeError):
                pass

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

        sol_supply = data.get("sol_supply")
        token_supply = data.get("token_supply")
        if sol_supply and token_supply:
            try:
                return float(sol_supply) / float(token_supply)
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

    async def get_token_symbol(self, token_mint: str) -> str:
        symbol = await self._get_symbol_from_dexscreener(token_mint)
        if symbol:
            return symbol
        symbol = await self._get_symbol_from_pumpfun(token_mint)
        if symbol:
            return symbol
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
            return

        try:
            await self.sell_token(
                token_mint, position.token_amount_ui,
                slippage_bps=slippage_bps,
            )
        except Exception as exc:
            logger.error("Error vendiendo {} ({}): {}", token_mint, reason, exc)
            return

        self.positions.pop(token_mint, None)
        logger.success("Posición {} cerrada por {} (PnL {:.2f}%)", token_mint, reason, pnl_pct)

    # ------------------------------------------------------------ Decimals
    async def _get_token_decimals(self, token_mint: str) -> int:
        """Consulta los decimales del token vía RPC mediante get_token_supply."""
        try:
            async with AsyncClient(self.rpc_url) as client:
                resp = await client.get_token_supply(Pubkey.from_string(token_mint))
                if resp.value and resp.value.decimals is not None:
                    return resp.value.decimals
        except Exception as exc:
            logger.warning("No se pudieron obtener decimales para {}: {}", token_mint, exc)

        return 6
