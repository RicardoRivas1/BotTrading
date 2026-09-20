"""Estrategia de Copy Trading para Pump.fun y DEXs en Solana.

Monitorea wallets de traders conocidos via Helius Enhanced Webhooks
y replica sus operaciones de compra/venta de tokens.

Soporta:
- Compras/ventas en Pump.fun bonding curve (via PumpPortal)
- Swaps en Jupiter, Raydium, Orca
- Deteccion de tokens nuevos en pump.fun
"""

from __future__ import annotations

import asyncio
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp
from loguru import logger

from core.engine.strategy import Strategy, StrategyState

# Token SOL nativo
SOL_MINT = "So11111111111111111111111111111111111111112"

# Tipos de transaccion que nos interesan
COPY_TRADE_TYPES = {"SWAP", "TRANSFER"}

# Programas DEX conocidos
DEX_PROGRAMS = {
    "PUMP_FUN": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    "PUMP_AMM": "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
    "JUPITER": "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
    "RAYDIUM": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "RAYDIUM_CLMM": "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
    "ORCA": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
}


@dataclass
class CopyTradeSignal:
    """Senal de trading detectada de una wallet monitoreada."""

    wallet: str
    action: str  # "buy" o "sell"
    token_mint: str
    token_symbol: str
    amount_sol: float
    tx_signature: str
    timestamp: float = field(default_factory=time.time)
    source: str = "helius_webhook"
    sell_pct: float = 0.0  # 0-100: percentage to sell (100 = full, 25 = quarter)
    trader_label: str = ""  # nombre del trader copiado (ej: cupsey)
    sell_sol_raw: float = 0.0  # SOL real recibido en la venta (sin cap MAX_COPY_TRADE_SOL)
    trade_token_amount: float = 0.0  # cantidad real de tokens del mint involucrados
    buy_sol_raw: float = 0.0  # SOL real gastado por el trader en el BUY (sin cap)


@dataclass
class TrackedWallet:
    """Wallet monitoreada para copy trading."""

    address: str
    label: str
    enabled: bool = True
    last_trade_at: float = 0.0
    total_trades: int = 0


class CopyTradingStrategy(Strategy):
    """Estrategia de copy trading que replica operaciones de wallets conocidas.

    Usa Helius Enhanced Webhooks para recibir transacciones en tiempo real.
    """

    def __init__(
        self,
        executor: Any,
        notifier: Any,
        tracker: Any,
        config: Any,
    ) -> None:
        super().__init__(executor, notifier, tracker, config)
        self.wallets: dict[str, TrackedWallet] = {}
        self._recent_signals: dict[str, float] = {}
        self._traded_mints: set[str] = set()  # mints que el bot ha comprado alguna vez
        self._wallet_mint_tokens: dict[tuple[str, str], float] = {}  # (wallet,mint)->tokens acumulados (SUMA via BUY)
        self._wallet_mint_sol: dict[tuple[str, str], float] = {}  # (wallet,mint)->SOL invertido acumulado (sin cap)
        self._lock = asyncio.Lock()
        self.webhook_path = "/webhook/copy-trading"
        self._load_wallets()

    def _notify(self, coro: Any) -> None:
        """Notifica en background (fire-and-forget) sin bloquear la ruta del trade.

        Telegram en modo secuencial tarda 1-5s por mensaje (HTTP + limite de 1
        msg/s del bot). Esperarlo dentro del lock atrasa/descarta sells cuando
        llegan webhooks con volumen; la notificacion no es critica para el trade.
        """
        try:
            task = asyncio.get_event_loop().create_task(coro)

            def _guard(t: Any) -> None:
                try:
                    t.result()
                except Exception:
                    pass

            task.add_done_callback(_guard)
        except Exception:
            pass

    @property
    def name(self) -> str:
        return "CopyTrading"

    @property
    def is_enabled(self) -> bool:
        return self.config.copy_trading.COPY_TRADING_ENABLED

    def _load_wallets(self) -> None:
        """Carga las wallets a monitorear desde variables de entorno."""
        for key, value in os.environ.items():
            if key.startswith("COPY_TRADE_WALLET_") and value:
                address = key.replace("COPY_TRADE_WALLET_", "")
                if len(address) >= 32:
                    self.wallets[address] = TrackedWallet(
                        address=address, label=value.strip(),
                    )
                    logger.info(
                        "CopyTrading: wallet cargada: {} ({})",
                        value.strip(), address[:8] + "...",
                    )

        addresses_str = os.getenv("COPY_TRADE_WALLET_ADDRESSES", "")
        labels_str = os.getenv("COPY_TRADE_WALLET_LABELS", "")
        if addresses_str:
            addresses = [a.strip() for a in addresses_str.split(",") if a.strip()]
            labels = [l.strip() for l in labels_str.split(",")] if labels_str else []
            for i, addr in enumerate(addresses):
                label = labels[i] if i < len(labels) else f"Trader-{i+1}"
                if addr not in self.wallets:
                    self.wallets[addr] = TrackedWallet(address=addr, label=label)
                    logger.info(
                        "CopyTrading: wallet cargada: {} ({})",
                        label, addr[:8] + "...",
                    )

        if not self.wallets:
            logger.warning(
                "CopyTrading: no hay wallets configuradas. "
                "Usa COPY_TRADE_WALLET_<ADDRESS>=<LABEL>"
            )
        else:
            logger.info(
                "CopyTrading: {} wallets monitoreadas", len(self.wallets),
            )

    # ----------------------------------------------------------- Mint extraction helpers
    _KNOWN_ADDRESSES: set[str] = {
        SOL_MINT,
        # USDC / USDT (stablecoins que aparecen en swaps)
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
        # Wrapped SOL
        "So11111111111111111111111111111111111111112",
        # Axiom program
        "AxiomRYA1zHVkpmvMtNPmBMzYFnM3RYqM3a7EMzN1t",
        "AxiomRXZAq1Jgjj9hKcPgCMbrZJzYVLpTLpTmZEu7AxF",
    } | set(DEX_PROGRAMS.values())

    # Known program/authority accounts that are NOT token mints
    _NON_MINT_ADDRESSES: set[str] = {
        "SysvarRent111111111111111111111111111111111",
        "SysvarC1ock11111111111111111111111111111111",
        "11111111111111111111111111111111",
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
        "39azUYFWPz3VHgKCf3VChSWJ4GDQ5K7JeaKauKwit8Ky",
        "CebN5WGUA4hG37LkS87YDh2XZ9P5Yq5R5h5h5h5h5h5",
    }

    def _get_all_excluded_addresses(self, extra: Optional[set[str]] = None) -> set[str]:
        """Returns ALL addresses that should never be treated as token mints."""
        excluded = self._KNOWN_ADDRESSES | self._NON_MINT_ADDRESSES
        excluded = excluded | set(self.wallets.keys())
        if hasattr(self.executor, 'wallet_pubkey') and self.executor.wallet_pubkey:
            excluded.add(self.executor.wallet_pubkey)
        if extra:
            excluded = excluded | extra
        return excluded

    @staticmethod
    def _is_likely_valid_mint(addr: str) -> bool:
        if not addr or len(addr) < 32 or len(addr) > 44:
            return False
        import re
        if not re.match(r'^[1-9A-HJ-NP-Za-km-z]+$', addr):
            return False
        return True

    def _extract_mint_from_account_data(
        self, account_data: list[dict[str, Any]], fee_payer: str, extra_exclude: Optional[set[str]] = None
    ) -> Optional[str]:
        excluded = self._get_all_excluded_addresses(extra_exclude)
        best_mint: Optional[str] = None
        best_abs: int = 0
        for acct in account_data:
            for tbc in acct.get("tokenBalanceChanges", []):
                user = tbc.get("userAccount", "")
                if user != fee_payer:
                    continue
                mint = tbc.get("mint", "")
                if not mint or mint in excluded:
                    continue
                if not self._is_likely_valid_mint(mint):
                    continue
                raw = tbc.get("rawTokenAmount", {})
                amount_str = raw.get("tokenAmount", "0")
                try:
                    amount = int(amount_str)
                except (ValueError, TypeError):
                    amount = 0
                if abs(amount) > best_abs:
                    best_abs = abs(amount)
                    best_mint = mint
        return best_mint

    def _extract_mint_from_sent_tokens(
        self, account_data: list[dict[str, Any]], fee_payer: str, extra_exclude: Optional[set[str]] = None
    ) -> Optional[str]:
        excluded = self._get_all_excluded_addresses(extra_exclude)
        best_mint: Optional[str] = None
        best_abs: int = 0
        for acct in account_data:
            for tbc in acct.get("tokenBalanceChanges", []):
                user = tbc.get("userAccount", "")
                if user != fee_payer:
                    continue
                mint = tbc.get("mint", "")
                if not mint or mint in excluded:
                    continue
                if not self._is_likely_valid_mint(mint):
                    continue
                raw = tbc.get("rawTokenAmount", {})
                amount_str = raw.get("tokenAmount", "0")
                try:
                    amount = int(amount_str)
                except (ValueError, TypeError):
                    amount = 0
                if amount < 0 and abs(amount) > best_abs:
                    best_abs = abs(amount)
                    best_mint = mint
        return best_mint

    def _extract_mint_from_description(self, description: str, extra_exclude: Optional[set[str]] = None) -> Optional[str]:
        excluded = self._get_all_excluded_addresses(extra_exclude)
        candidates = re.findall(r'[1-9A-HJ-NP-Za-km-z]{32,44}', description)
        for candidate in candidates:
            if candidate not in excluded:
                return candidate
        return None

    def _find_tracked_wallet_in_transfers(self, tx: dict[str, Any]) -> Optional[TrackedWallet]:
        """Si fee_payer no es una wallet monitoreada, busca en nativeTransfers
        y tokenTransfers para ver si alguna wallet monitoreada participo como REMITENTE.

        Solo coincidimos si la wallet monitoreada es el REMITENTE (fromUserAccount),
        no el destinatario. Si alguien envia tokens A una wallet monitoreada,
        eso no es un trade de esa wallet.
        """
        native_transfers = tx.get("nativeTransfers", [])
        token_transfers = tx.get("tokenTransfers", [])
        senders: set[str] = set()
        for nt in native_transfers:
            senders.add(nt.get("fromUserAccount", ""))
        for tt in token_transfers:
            senders.add(tt.get("fromUserAccount", ""))
        for addr in senders:
            tracked = self.wallets.get(addr)
            if tracked and tracked.enabled:
                return tracked
        return None

    def _calc_sell_pct(self, mint: str, tokens_sent_amount: float, wallet: str) -> float:
        """Calculate what percentage of our position is being sold.

        Compares tokens_sent in the transfer vs what we hold in executor
        and tracker positions. Returns 0-100.
        """
        if tokens_sent_amount <= 0:
            return 0.0

        # Try executor position first (has token_amount_ui)
        pos = self.executor.positions.get(mint)
        if pos and pos.token_amount_ui > 0:
            pct = (tokens_sent_amount / pos.token_amount_ui) * 100.0
            return min(pct, 100.0)

        # Try tracker position (has amount in SOL, estimate tokens)
        tracker_pos = self.tracker.positions.get(mint)
        if tracker_pos and tracker_pos.amount > 0 and tracker_pos.buy_price > 0:
            estimated_tokens = tracker_pos.amount / tracker_pos.buy_price
            if estimated_tokens > 0:
                pct = (tokens_sent_amount / estimated_tokens) * 100.0
                # Guard: if pct > 1000, tokens_sent is likely in raw units (lamports),
                # divide by 10^decimals to convert to UI
                if pct > 1000.0:
                    pct = (tokens_sent_amount / (estimated_tokens * 1e6)) * 100.0
                return min(pct, 100.0)

        # If we have a position but can't estimate, assume full sell
        if tracker_pos or pos:
            return 100.0

        # No position found - not a sell at all
        return 0.0

    async def start(self) -> None:
        """Inicia la estrategia de copy trading y el polling de RPC."""
        self._set_state(StrategyState.RUNNING)
        logger.info("CopyTrading: estrategia iniciada ({} wallets)", len(self.wallets))
        asyncio.create_task(self._rpc_poll_loop())

    async def _rpc_poll_loop(self) -> None:
        """Poll wallets via getSignaturesForAddress to detect trades.

        Rotates 1 wallet per cycle to stay within Helius rate limits.
        On persistent 429, disables RPC polling and relies on webhooks.
        """
        from config import load_config

        cfg = load_config()
        rpc_url = cfg.solana.HELIUS_RPC_URL

        if rpc_url and not rpc_url.startswith("http"):
            rpc_url = f"https://mainnet.helius-rpc.com/?api-key={rpc_url}"

        helius_api_key = os.getenv("HELIUS_API_KEY", "")
        if not helius_api_key and "api-key=" in rpc_url:
            helius_api_key = rpc_url.split("api-key=")[-1].split("&")[0]

        if not rpc_url or not rpc_url.startswith("http"):
            logger.error("CopyTrading: RPC URL invalida. Polling deshabilitado.")
            return

        last_sig = {}
        POLL_INTERVAL = 30.0
        MAX_429_BEFORE_DISABLE = 3
        consecutive_429 = 0
        wallet_index = 0
        disabled = False

        import aiohttp

        async with aiohttp.ClientSession() as session:
            wallet_addrs = list(self.wallets.keys())
            if not wallet_addrs:
                return

            logger.info(
                "CopyTrading: RPC polling activo ({} wallets, 1 por ciclo de {}s)",
                len(wallet_addrs), POLL_INTERVAL,
            )

            while not disabled:
                await asyncio.sleep(POLL_INTERVAL)
                if self._state != StrategyState.RUNNING:
                    continue

                addr = wallet_addrs[wallet_index % len(wallet_addrs)]
                wallet_index += 1

                try:
                    payload = {
                        "jsonrpc": "2.0", "id": 1,
                        "method": "getSignaturesForAddress",
                        "params": [addr, {"limit": 5}],
                    }
                    async with session.post(rpc_url, json=payload) as resp:
                        if resp.status == 429:
                            consecutive_429 += 1
                            if consecutive_429 >= MAX_429_BEFORE_DISABLE:
                                logger.warning(
                                    "CopyTrading: {} 429 seguidos. RPC polling DESHABILITADO. "
                                    "Usando solo webhooks de Helius.",
                                    consecutive_429,
                                )
                                disabled = True
                                continue
                            logger.warning(
                                "CopyTrading: 429 ({}/{}). Reintentando en {}s...",
                                consecutive_429, MAX_429_BEFORE_DISABLE,
                                POLL_INTERVAL * 2,
                            )
                            await asyncio.sleep(POLL_INTERVAL)
                            continue
                        data = await resp.json()

                    consecutive_429 = 0

                    sigs = data.get("result", [])
                    if not sigs:
                        continue

                    prev = last_sig.get(addr, "")
                    new_sigs = []
                    for s in sigs:
                        if s["signature"] == prev:
                            break
                        new_sigs.append(s["signature"])
                    if not new_sigs:
                        continue

                    last_sig[addr] = sigs[0]["signature"]

                    if not helius_api_key:
                        continue
                    enhance_url = f"https://api.helius.xyz/v0/transactions/?api-key={helius_api_key}"
                    enhance_payload = {"transactions": new_sigs}
                    async with session.post(enhance_url, json=enhance_payload) as resp:
                        if resp.status == 429:
                            consecutive_429 += 1
                            continue
                        if resp.status != 200:
                            continue
                        enhanced_txs = await resp.json()

                    for tx in enhanced_txs:
                        await self._process_transaction(tx)

                except Exception as exc:
                    logger.debug("CopyTrading: poll error para {}: {}", addr[:8], exc)

        if disabled:
            logger.info("CopyTrading: operando solo via webhooks (RPC polling deshabilitado)")


    async def stop(self) -> None:
        """Detiene la estrategia."""
        self._set_state(StrategyState.STOPPED)
        logger.info("CopyTrading: estrategia detenida")

    # ----------------------------------------------------------- Webhook handler
    async def handle_webhook(self, payload: dict[str, Any]) -> dict[str, str]:
        """Procesa un webhook de Helius Enhanced Transaction."""
        self._inc_stat("signals_received")
        tx_count = len(payload) if isinstance(payload, list) else 1
        logger.info(
            "CopyTrading: webhook recibido ({} txs)", tx_count,
        )

        try:
            transactions = payload if isinstance(payload, list) else [payload]
            for tx in transactions:
                await self._process_transaction(tx)
            return {"status": "ok", "processed": str(len(transactions))}
        except Exception as exc:
            logger.error("CopyTrading: error procesando webhook: {}", exc)
            return {"status": "error", "message": str(exc)}

    async def _process_transaction(self, tx: dict[str, Any]) -> None:
        """Parsea una transaccion Enhanced de Helius y detecta trades."""
        tx_type = tx.get("type", "")
        fee_payer = tx.get("feePayer", "")
        signature = tx.get("signature", "")

        logger.info(
            "CopyTrading: tx type={} fee_payer={} sig={}",
            tx_type, fee_payer[:12] + "..." if fee_payer else "?",
            signature[:16] + "..." if signature else "?",
        )

        if tx_type not in COPY_TRADE_TYPES:
            logger.warning(
                "CopyTrading: tx filtrada por tipo '{}' (esperado {}): sig={}",
                tx_type, COPY_TRADE_TYPES, signature[:16] + "...",
            )
            return

        tracked = self.wallets.get(fee_payer)

        # When wallet is found via transfers, use its address as the trader
        wallet_address = ""
        if not tracked or not tracked.enabled:
            # Log transfer details for debugging
            nt = tx.get("nativeTransfers", [])
            tt = tx.get("tokenTransfers", [])
            senders = set()
            receivers = set()
            for n in nt:
                senders.add(n.get("fromUserAccount", ""))
                receivers.add(n.get("toUserAccount", ""))
            for t in tt:
                senders.add(t.get("fromUserAccount", ""))
                receivers.add(t.get("toUserAccount", ""))
            tracked_in_s = senders.intersection(self.wallets.keys())
            tracked_in_r = receivers.intersection(self.wallets.keys())
            if tracked_in_s or tracked_in_r:
                logger.info(
                    "CopyTrading: wallets monitoreadas en transfers! senders={} receivers={}",
                    [w[:8] for w in tracked_in_s],
                    [w[:8] for w in tracked_in_r],
                )

            tracked = self._find_tracked_wallet_in_transfers(tx)
            if not tracked:
                logger.debug(
                    "CopyTrading: fee_payer {} no es wallet monitoreada. "
                    "senders={} receivers={} sig={}",
                    fee_payer[:12] + "...",
                    [s[:8] for s in list(senders)[:3]],
                    [r[:8] for r in list(receivers)[:3]],
                    signature[:16] + "...",
                )
                return
            wallet_address = tracked.address

        # Deduplicacion
        if signature in self._recent_signals:
            return
        self._recent_signals[signature] = time.time()
        cutoff = time.time() - 300
        self._recent_signals = {
            k: v for k, v in self._recent_signals.items() if v > cutoff
        }

        self._inc_stat("trades_executed")
        logger.info(
            "CopyTrading: trade detectado de {} ({}) | type={} | sig={}",
            tracked.label, fee_payer[:8] + "...", tx_type, signature[:16] + "...",
        )

        signal = self._parse_trade(tx, tracked, wallet_address=wallet_address)

        if signal:
            await self._execute_copy_trade(signal)

    def _parse_trade(self, tx: dict[str, Any], tracked: TrackedWallet, wallet_address: str = "") -> Optional[CopyTradeSignal]:
        """Extrae la senal de trading de una transaccion.

        Detecta especialmente transacciones de Pump.fun (bonding curve)
        analizando los programas involucrados en la transaccion.
        """
        signature = tx.get("signature", "")
        fee_payer = tx.get("feePayer", "")
        timestamp = tx.get("timestamp", 0)
        token_transfers = tx.get("tokenTransfers", [])
        native_transfers = tx.get("nativeTransfers", [])
        account_data = tx.get("accountData", [])

        trader = wallet_address or fee_payer

        # Detectar si es transaccion de Pump.fun
        is_pump_fun = False
        programs_involved = set()
        for acct in account_data:
            owner = acct.get("account", "")
            if owner in DEX_PROGRAMS.values():
                programs_involved.add(owner)
                if owner in (DEX_PROGRAMS["PUMP_FUN"], DEX_PROGRAMS["PUMP_AMM"]):
                    is_pump_fun = True

        # Calcular SOL enviado/recibido por el trader
        sol_spent = sum(
            nt.get("amount", 0) / 1e9
            for nt in native_transfers
            if nt.get("fromUserAccount") == trader
        )
        sol_received = sum(
            nt.get("amount", 0) / 1e9
            for nt in native_transfers
            if nt.get("toUserAccount") == trader
        )

        # Fallback: si no hay nativeTransfers del trader, usar accountData.nativeBalanceChange
        # Esto captura ventas en bonding curve donde el SOL viene via programa
        if sol_spent == 0 and sol_received == 0:
            for acct in account_data:
                if acct.get("account") != trader:
                    continue
                nbc = acct.get("nativeBalanceChange", 0)
                if nbc > 0:
                    sol_received = nbc / 1e9
                elif nbc < 0:
                    sol_spent = abs(nbc) / 1e9
                break

        # Analizar transfers de tokens (excluyendo SOL, stablecoins y wallets)
        excluded = self._get_all_excluded_addresses()
        tokens_received = []
        tokens_sent = []
        for tt in token_transfers:
            mint = tt.get("mint", "")
            if not mint or mint in excluded:
                continue
            amount = tt.get("tokenAmount", 0)
            if tt.get("toUserAccount") == trader:
                tokens_received.append({"mint": mint, "amount": amount})
            elif tt.get("fromUserAccount") == trader:
                tokens_sent.append({"mint": mint, "amount": amount})

        # Eliminar "hops neutrales": el MISMO mint con el MISMO monto aparece en
        # ambos lados (sent y received). Es un salto intermedio del agregador/loop
        # (bounce in/out de la pool con impacto neto cero para el trader), NO una
        # venta real. Es la causa de buys con wrapping clasificados como SELL
        # cuando accountData no trae balances (deltas={}).
        def _same_amount(a: float, b: float) -> bool:
            return abs(a - b) <= max(1e-6, abs(a) * 1e-6)

        neutral_sent: list[dict] = []
        neutral_rcvd_idx: set[int] = set()
        for sent_item in tokens_sent:
            match_idx = next(
                (
                    i for i, r in enumerate(tokens_received)
                    if i not in neutral_rcvd_idx
                    and r["mint"] == sent_item["mint"]
                    and _same_amount(r["amount"], sent_item["amount"])
                ),
                None,
            )
            if match_idx is not None:
                neutral_rcvd_idx.add(match_idx)
                logger.debug(
                    "CopyTrade: hop neutral eliminado {} ({:.4f})",
                    sent_item["mint"][:12] + "...", sent_item["amount"],
                )
            else:
                neutral_sent.append(sent_item)
        tokens_sent = neutral_sent
        tokens_received = [
            r for i, r in enumerate(tokens_received) if i not in neutral_rcvd_idx
        ]
        n_hops_neutral = len(neutral_rcvd_idx)

        # Detectar porcentaje de venta desde tokenBalanceChanges
        # Usamos el CAMBIO DE BALANCE DEL TRADER (no nuestra posicion) como
        # señal fiel de compra/venta. mintAmount > 0 = balance subio = compra
        # (incluye DCA: compras repetidas del mismo token). < 0 = vendio.
        sell_pct = 0.0
        sell_mint_from_balance = None
        trader_delta_by_mint: dict[str, float] = {}
        for acct in account_data:
            if acct.get("account") != trader:
                continue
            for tbc in acct.get("tokenBalanceChanges", []):
                tbc_mint = tbc.get("mint", "")
                if not tbc_mint or tbc_mint in excluded:
                    continue
                # mintAmount = tokens ganados (>0) o perdidos (<0)
                mint_delta = tbc.get("mintAmount", 0)
                final_balance = tbc.get("tokenAmount", 0)
                trader_delta_by_mint[tbc_mint] = mint_delta
                if mint_delta < 0 and final_balance >= 0:
                    tokens_sold = abs(mint_delta)
                    remaining = final_balance
                    total_before = tokens_sold + remaining
                    if total_before > 0:
                        sell_pct = (tokens_sold / total_before) * 100
                        sell_mint_from_balance = tbc_mint
                        logger.debug(
                            "Sell pct calculado: {:.1f}% | sold={:.4f} remaining={:.4f} mint={}",
                            sell_pct, tokens_sold, remaining, tbc_mint[:12],
                        )

        # Determinar accion
        action = None
        token_mint = None
        amount_sol = 0.0

        logger.info(
            "CopyTrade classify: trader={} tokens_sent={} tokens_rcvd={} sol_spent={:.6f} sol_rcvd={:.6f} is_pump={} deltas={} neutral_hops={}",
            trader[:8] + "..." if trader else "?",
            [(t["mint"][:8] + "...", t["amount"]) for t in tokens_sent[:2]],
            [(t["mint"][:8] + "...", t["amount"]) for t in tokens_received[:2]],
            sol_spent, sol_received, is_pump_fun,
            {m[:8] + "...": round(d, 4) for m, d in list(trader_delta_by_mint.items())[:3]},
            n_hops_neutral,
        )

        # IMPORTANT: Check tokens_sent FIRST (sells) before tokens_received (buys)
        # When a DEX sell happens, BOTH tokens_sent AND tokens_received can be present
        # (trader sends tokens, receives change + SOL). Checking tokens_sent first
        # prevents misclassifying sells as buys.

        # Caso 2: Envia tokens y recibe SOL = VENTA
        # BUT: buys con wrapping intermedio (pump.fun) o swaps multi-hop (DEX)
        # muestran un token "enviado" cuyo balance en realidad no baja.
        # Señal fiel: el delta de balance del trader por mint.
        if tokens_sent and sol_received > 0:
            tokens_sent.sort(key=lambda t: t["amount"], reverse=True)
            tokens_received.sort(key=lambda t: t["amount"], reverse=True)
            sent_mint = tokens_sent[0]["mint"]
            sent_delta = trader_delta_by_mint.get(sent_mint, 0.0)
            # (1) Un token RECIBIDO con balance del trader positivo = compra real.
            #     Cubre swapping multi-hop: SOL -> intermedio -> token final.
            #     Guard: en venta real sol_received > sol_spent, en compra al reves.
            received_buy = [
                t for t in tokens_received
                if trader_delta_by_mint.get(t["mint"], 0.0) > 1e-6
            ]
            if received_buy and sol_spent > sol_received:
                token_mint = received_buy[0]["mint"]
                action = "buy"
                amount_sol = sol_spent if sol_spent > 0 else sol_received
                logger.info(
                    "CopyTrade: buy (balance recibido +{:.6f}) {} | mint={} | SOL={:.6f} | mid_tk={}",
                    trader_delta_by_mint.get(token_mint, 0.0), signature[:16] + "...",
                    token_mint[:12] + "...", amount_sol, sent_mint[:12] + "...",
                )
            elif sent_delta < 0:
                # (2) Balance del token enviado BAJO = venta real
                token_mint = sent_mint
                action = "sell"
                amount_sol = sol_received if sol_received > 0 else sol_spent
                logger.info(
                    "CopyTrading: SELL (balance -{:.6f}) {} | tokens_sent={} | SOL_rcvd={:.6f}",
                    -sent_delta, signature[:16] + "...", token_mint[:12] + "...", sol_received,
                )
            else:
                sol_ratio = sol_spent / sol_received if sol_received > 0 else 999
                # Compra multi-hop: el trader GASTO SOL neto y RECIBE tokens.
                # (cubre pump.fun wrapping, agregadores y hops asimetricos cuando
                # accountData no trae balances). Guard rotacion: si ya tenemos
                # posicion del token ENVIADO, es una rotacion -> vender la nuestra.
                if tokens_received and sol_spent > sol_received:
                    sent_held = (
                        self.executor.positions.get(sent_mint)
                        or self.tracker.positions.get(sent_mint)
                    )
                    rcvd_held = (
                        self.executor.positions.get(tokens_received[0]["mint"])
                        or self.tracker.positions.get(tokens_received[0]["mint"])
                    )
                    if sent_held and not rcvd_held:
                        token_mint = sent_mint
                        action = "sell"
                        amount_sol = sol_received if sol_received > 0 else sol_spent
                        logger.info(
                            "CopyTrade: rotacion (vendo algo que tenemos) {} | mint={} | SOL_rcvd={:.6f}",
                            signature[:16] + "...", token_mint[:12] + "...", sol_received,
                        )
                    else:
                        token_mint = tokens_received[0]["mint"]
                        action = "buy"
                        amount_sol = sol_spent if sol_spent > 0 else sol_received
                        logger.info(
                            "CopyTrade: multi-hop buy (SOL neto gastado) {} | mint={} | SOL_spent={:.6f} sol_rcvd={:.6f} ratio={:.0f}x",
                            signature[:16] + "...", token_mint[:12] + "...", sol_spent, sol_received, sol_ratio,
                        )
                elif sol_ratio > 10 and tokens_received:
                    token_mint = tokens_received[0]["mint"]
                    action = "buy"
                    amount_sol = sol_spent
                    logger.info(
                        "CopyTrade: buy con wrapper (ratio {:.0f}x) {} | mint={} | SOL_spent={:.6f} sol_rcvd={:.6f}",
                        sol_ratio, signature[:16] + "...", token_mint[:12] + "...", sol_spent, sol_received,
                    )
                else:
                    token_mint = sent_mint
                    action = "sell"
                    amount_sol = sol_received

        # Caso 2b: Envia tokens + envia SOL (DEX sell - SOL return via program, not nativeTransfers)
        # Axiom/Pump sells: CENTED sends tokens to buyer, SOL goes to buyer's ATA,
        # but the DEX return SOL comes via the program, not shown in nativeTransfers.
        # NOTA: Para compras DCA (el trader acumula el MISMO token), el balance del
        # trader SUBE (mintAmount > 0). Eso es COMPRA, no venta. Solo si el balance
        # del trader BAJA es venta real.
        elif tokens_sent and sol_spent > 0 and sol_received == 0:
            tokens_sent.sort(key=lambda t: t["amount"], reverse=True)
            token_mint = tokens_sent[0]["mint"]
            amount_sol = sol_spent
            trader_delta = trader_delta_by_mint.get(token_mint, 0.0)
            logger.debug(
                "CopyTrade CASE2b: trader_delta={:.4f} is_pump={} | mint={} | tokens_amt={}",
                trader_delta, is_pump_fun, token_mint[:12] + "...", tokens_sent[0]["amount"],
            )
            if trader_delta > 0:
                # El trader INCREMENTO su balance del token = compra (DCA)
                action = "buy"
                sell_pct = 0.0
                logger.info(
                    "CopyTrade: DCA buy (balance +{:.4f}) {} | mint={} | SOL_spent={:.6f} | pump={}",
                    trader_delta, signature[:16] + "...", token_mint[:12] + "...", sol_spent, is_pump_fun,
                )
            elif trader_delta < 0:
                # Balance del trader bajo = venta real
                action = "sell"
                logger.info(
                    "CopyTrading: SELL (dex program, balance -{:.4f}) {} | tokens_sent={} | SOL_out={:.6f}",
                    -trader_delta, signature[:16] + "...", token_mint[:12] + "...", sol_spent,
                )
            else:
                # Delta no disponible (fallback): usar sell_pct vs nuestra posicion
                sell_pct = self._calc_sell_pct(token_mint, tokens_sent[0]["amount"], tracked.address)
                # Si ya tenemos posicion del mint, siempre es venta aunque el % sea
                # pequeno: los scalpers venden fracciones chicas varias veces.
                have_pos = bool(
                    self.executor.positions.get(token_mint)
                    or self.tracker.positions.get(token_mint)
                )
                if have_pos:
                    action = "sell"
                    logger.info(
                        "CopyTrading: SELL (dex program, posicion) {} | tokens_sent={} | SOL_out={:.6f} | sell_pct={:.0f}%",
                        signature[:16] + "...", token_mint[:12] + "...", sol_spent, max(sell_pct, 1.0),
                    )
                elif sell_pct < 5.0:
                    if is_pump_fun:
                        logger.info(
                            "CopyTrade: pump.fun buy (Helius inverted transfer) {} | tokens_sent would-be {} | SOL_spent={:.6f}",
                            signature[:16] + "...", token_mint[:12] + "...", sol_spent,
                        )
                        action = "buy"
                    else:
                        logger.debug(
                            "CopyTrade ignorado: small token transfer ({:.1f}%) {} | {}",
                            sell_pct, signature[:16] + "...", token_mint[:12] + "...",
                        )
                        return None
                else:
                    action = "sell"
                    logger.info(
                        "CopyTrading: SELL (dex program) {} | tokens_sent={} | SOL_out={:.6f} | sell_pct={:.0f}%",
                        signature[:16] + "...", token_mint[:12] + "...", sol_spent, sell_pct,
                    )

        # Caso 2c: Envia tokens sin SOL = possible DEX sell (SOL via program) or wallet transfer
        elif tokens_sent and sol_spent == 0 and sol_received == 0:
            tokens_sent.sort(key=lambda t: t["amount"], reverse=True)
            candidate_mint = tokens_sent[0]["mint"]
            sell_pct = self._calc_sell_pct(candidate_mint, tokens_sent[0]["amount"], tracked.address)
            # Si ya tenemos posicion del mint, es venta aunque el % sea pequeno.
            have_pos = bool(
                self.executor.positions.get(candidate_mint)
                or self.tracker.positions.get(candidate_mint)
            )
            if sell_pct < 5.0 and not have_pos:
                return None
            if have_pos:
                token_mint = candidate_mint
                action = "sell"
                amount_sol = 0.0
                logger.info(
                    "CopyTrading: SELL (token transfer, posicion) {} -> {} | sell_pct={:.0f}%",
                    tracked.label, token_mint[:12] + "...", sell_pct,
                )
            elif tracked.address:
                wallet_positions = self.tracker.get_positions_by_wallet(tracked.address)
                for wp in wallet_positions:
                    if wp.mint == candidate_mint:
                        token_mint = wp.mint
                        action = "sell"
                        amount_sol = 0.0
                        logger.info(
                            "CopyTrading: SELL (token transfer) {} -> {} ({}) | sell_pct={:.0f}%",
                            tracked.label, wp.symbol, wp.mint[:12] + "...", sell_pct,
                        )
                        break
            if action is None:
                return None

        # Caso 3b: Recibe SOL neto + tiene posiciones abiertas = VENTA
        # (Axiom sells: no tokenTransfers, trader receives SOL net)
        # Guard: solo aplica si NO hay tokens involucrados. Si el trader RECIBE
        # tokens y gasta SOL, eso es una COMPRA (Caso 1) y no debe ser capturado
        # aqui por el pequeno cambio SOL (sol_received ~0.002).
        elif sol_received > 0 and not tokens_received and not tokens_sent and tracked.address:
            wallet_positions = self.tracker.get_positions_by_wallet(tracked.address)
            if wallet_positions and sol_received > max(sol_spent, 0.001):
                pos = wallet_positions[-1]
                token_mint = pos.mint
                action = "sell"
                amount_sol = sol_received
                sell_pct = 100.0
                logger.info(
                    "CopyTrading: SELL by wallet match (net SOL) {} -> {} ({}) | {:.6f} SOL",
                    tracked.label, pos.symbol, pos.mint[:12] + "...", sol_received,
                )

        # Caso 1: Recibe tokens y envia SOL = COMPRA
        # This is checked AFTER all sell cases to prevent misclassifying sells as buys.
        # When DEX sell happens, both tokens_sent and tokens_received can be present.
        elif tokens_received and sol_spent > 0:
            tokens_received.sort(key=lambda t: t["amount"], reverse=True)
            token_mint = tokens_received[0]["mint"]
            action = "buy"
            amount_sol = sol_spent

        # Caso 3: Solo envio de SOL (posible compra en bonding curve)
        if action is None and sol_spent > 0 and not tokens_received:
            # 1) Intentar desde accountData (tokenBalanceChanges)
            token_mint = self._extract_mint_from_account_data(
                account_data, trader, extra_exclude={fee_payer}
            )
            if token_mint:
                action = "buy"
                amount_sol = sol_spent
            else:
                # 2) Fallback: descripcion de la transaccion
                token_mint = self._extract_mint_from_description(
                    tx.get("description", ""), extra_exclude={fee_payer}
                )
                if token_mint:
                    action = "buy"
                    amount_sol = sol_spent

        # Caso 4: Solo recibe SOL (posible venta en bonding curve / Axiom)
        elif action is None and sol_received > 0 and not tokens_sent and not tokens_received:
            # Usar sell_mint_from_balance si se detecto
            if sell_mint_from_balance:
                token_mint = sell_mint_from_balance
                action = "sell"
                amount_sol = sol_received
            else:
                token_mint = self._extract_mint_from_sent_tokens(
                    account_data, trader, extra_exclude={fee_payer}
                )
                if token_mint:
                    action = "sell"
                    amount_sol = sol_received
                else:
                    token_mint = self._extract_mint_from_description(
                        tx.get("description", ""), extra_exclude={fee_payer}
                    )
                    if token_mint:
                        action = "sell"
                        amount_sol = sol_received
                    elif tracked.address:
                        # Axiom sells: no token data, match by wallet position
                        wallet_positions = self.tracker.get_positions_by_wallet(tracked.address)
                        if wallet_positions:
                            pos = wallet_positions[-1]
                            token_mint = pos.mint
                            action = "sell"
                            amount_sol = sol_received
                            sell_pct = 100.0
                            logger.info(
                                "CopyTrading: SELL by wallet match {} -> {} ({}) | {:.6f} SOL",
                                tracked.label, pos.symbol, pos.mint[:12] + "...", sol_received,
                            )

        # Caso 5: tokenBalanceChanges muestra trader perdiendo tokens (venta via programa)
        elif action is None and sell_mint_from_balance and sell_pct > 0:
            action = "sell"
            token_mint = sell_mint_from_balance
            amount_sol = sol_received if sol_received > 0 else 0.0

        if not action or not token_mint:
            logger.warning(
                "CopyTrading: no se pudo determinar mint para {} | "
                "trader={} | accounts={} | tokenTransfers={} | "
                "sol_spent={:.6f} | sol_received={:.6f} | "
                "tokens_rcvd={} | tokens_sent={} | desc={}",
                signature[:16] + "...", trader[:8] + "...", len(account_data),
                len(token_transfers), sol_spent, sol_received,
                len(tokens_received), len(tokens_sent),
                tx.get("description", "")[:120],
            )
            return None

        # Filtrar transfers de SOL minimos (fees de red) - solo para buys
        if action == "buy" and amount_sol < 0.0001:
            return None

        # Para sells con amount_sol=0 (DEX program), usar sol_spent como referencia
        if action == "sell" and amount_sol <= 0:
            amount_sol = sol_spent if sol_spent > 0 else 0.01

        # Para buys: monto minimo realista (evita falsos positivos de fees/tiny transfers)
        MIN_BUY_SOL = 0.005
        if action == "buy" and amount_sol < MIN_BUY_SOL:
            logger.debug(
                "CopyTrade ignorado: buy demasiado pequeno ({:.6f} < {:.6f} SOL) | {}",
                amount_sol, MIN_BUY_SOL, signature[:16] + "...",
            )
            return None

        # Limitar monto al maximo configurado
        max_amount = float(
            getattr(self.config.copy_trading, "MAX_COPY_TRADE_SOL", 0.01)
        )
        # Capturar el SOL real de la venta ANTES del cap (para PnL real)
        sell_sol_raw = amount_sol if action == "sell" else 0.0
        buy_sol_raw = amount_sol if action == "buy" else 0.0
        if amount_sol > max_amount:
            amount_sol = max_amount

        source_label = "pump.fun" if is_pump_fun else "dex"
        sell_info = f" | sell_pct={sell_pct:.0f}%" if action == "sell" and sell_pct > 0 else ""
        chosen_delta = trader_delta_by_mint.get(token_mint, 0.0)

        # Cantidad real de tokens del mint elegido (para calcular % real vendido
        # contra lo que EL TRADER acumulo, no contra nuestra posicion pequena).
        trade_token_amount = 0.0
        if action == "sell":
            for t in tokens_sent:
                if t["mint"] == token_mint:
                    trade_token_amount = t["amount"]
                    break
        elif action == "buy":
            for t in tokens_received:
                if t["mint"] == token_mint:
                    trade_token_amount = t["amount"]
                    break

        logger.info(
            "CopyTrading: signal {} {} | mint={} | {:.6f} SOL | trader={} | src={}{} | delta={:+.4f} | tk={:.4g}",
            action.upper(), signature[:16] + "...", token_mint[:12] + "...",
            amount_sol, trader[:8] + "...", source_label, sell_info, chosen_delta,
            trade_token_amount,
        )
        return CopyTradeSignal(
            wallet=tracked.address,
            action=action,
            token_mint=token_mint,
            token_symbol=token_mint[:6].upper(),
            amount_sol=amount_sol,
            tx_signature=signature,
            timestamp=float(timestamp) if timestamp else time.time(),
            source=f"helius:{tracked.label}:{source_label}",
            sell_pct=sell_pct,
            trader_label=tracked.label,
            sell_sol_raw=sell_sol_raw,
            trade_token_amount=trade_token_amount,
            buy_sol_raw=buy_sol_raw,
        )

    async def _execute_copy_trade(self, signal: CopyTradeSignal) -> None:
        """Ejecuta un copy trade."""
        from core.websocket import process_buy_and_notify, process_sell_and_notify

        async with self._lock:
            try:
                if signal.action == "buy":
                    existing = self.executor.positions.get(signal.token_mint)
                    tracker_pos = self.tracker.positions.get(signal.token_mint)

                    # Tracking de tokens acumulados por (wallet, mint). Esto es lo
                    # que permite calcular el % REAL vendido del trader (vendio
                    # "tantos de los X que acumulo"), no comparado contra nuestra
                    # posicion copiada (que es mucho mas pequena).
                    wt_key = (signal.wallet, signal.token_mint)
                    acc_before = self._wallet_mint_tokens.get(wt_key, 0.0)
                    if signal.trade_token_amount > 0:
                        self._wallet_mint_tokens[wt_key] = (
                            acc_before + signal.trade_token_amount
                        )
                    sol_before = self._wallet_mint_sol.get(wt_key, 0.0)
                    if signal.buy_sol_raw > 0:
                        self._wallet_mint_sol[wt_key] = sol_before + signal.buy_sol_raw

                    # Accumulate if same token bought again (distributed buys)
                    if existing or tracker_pos:
                        if existing:
                            existing.sol_invested += signal.amount_sol
                            if existing.entry_price and existing.entry_price > 0:
                                existing.token_amount_ui += signal.amount_sol / existing.entry_price
                                existing.token_amount_ui = max(
                                    existing.token_amount_ui,
                                    existing.sol_invested / existing.entry_price,
                                )
                        if tracker_pos:
                            tracker_pos.amount += signal.amount_sol
                        logger.info(
                            "CopyTrading: BUY acumulado {} ({}) | +{:.6f} SOL ({}) | total_investido={:.6f}",
                            signal.trader_label or signal.source, signal.wallet[:8] + "...",
                            signal.amount_sol,
                            signal.token_mint[:8] + "...",
                            existing.sol_invested if existing else tracker_pos.amount,
                        )
                        from core.stats import get_trade_stats
                        get_trade_stats().record_buy(signal.wallet, new_position=False)
                        self._notify(self.notifier.send_buy(
                            signal.token_mint,
                            signal.amount_sol,
                            symbol=signal.token_symbol or signal.token_mint[:6].upper(),
                            dry_run=self.config.trading.DRY_RUN,
                            trader=signal.trader_label,
                        ))
                        return

                    max_pos = int(
                        getattr(self.config.copy_trading, "MAX_COPY_TRADE_POSITIONS", 50)
                    )
                    if len(self.executor.positions) >= max_pos:
                        logger.info(
                            "CopyTrading: BUY ignorado {} - max posiciones ({})",
                            signal.source, signal.token_mint[:8] + "...",
                        )
                        return

                    original_amount = self.executor.buy_amount_sol
                    self.executor.buy_amount_sol = signal.amount_sol
                    try:
                        sig = await self.executor.buy_token(signal.token_mint)
                    finally:
                        self.executor.buy_amount_sol = original_amount

                    if sig is None:
                        return

                    self._traded_mints.add(signal.token_mint)

                    try:
                        symbol = await self.executor.get_token_symbol(signal.token_mint)
                    except Exception:
                        symbol = signal.token_mint[:6].upper()

                    entry_price = 0.0
                    position = self.executor.positions.get(signal.token_mint)
                    if position and position.entry_price and position.entry_price > 0:
                        entry_price = position.entry_price

                    self.tracker.add_position(
                        mint=signal.token_mint,
                        symbol=symbol,
                        buy_price=entry_price,
                        amount=signal.amount_sol,
                        source_wallet=signal.wallet,
                    )

                    self._notify(self.notifier.send_buy(
                        signal.token_mint,
                        signal.amount_sol,
                        symbol=symbol,
                        dry_run=self.config.trading.DRY_RUN,
                        trader=signal.trader_label,
                    ))

                    # Record buy in stats
                    from core.stats import get_trade_stats
                    get_trade_stats().record_buy(signal.wallet)

                    logger.success(
                        "CopyTrading: BUY {} ({}) | {} SOL | {} ({})",
                        signal.trader_label or signal.source, signal.wallet[:8] + "...",
                        f"{signal.amount_sol:.6f}",
                        symbol, signal.token_mint[:8] + "...",
                    )

                elif signal.action == "sell":
                    # First try exact mint match
                    position = self.executor.positions.get(signal.token_mint)
                    tracker_pos = self.tracker.positions.get(signal.token_mint)

                    if not position and not tracker_pos:
                        if signal.token_mint in self._traded_mints:
                            motivo = "ya vendida por el bot antes"
                        else:
                            motivo = "el bot nunca la compro"
                        logger.info(
                            "CopyTrading: SELL ignorado {} ({}) - posicion no encontrada: {}",
                            signal.source, signal.token_mint[:8] + "...", motivo,
                        )
                        return

                    pnl_pct = 0.0
                    entry = 0.0
                    if position and position.entry_price and position.entry_price > 0:
                        entry = position.entry_price
                    elif tracker_pos and tracker_pos.buy_price and tracker_pos.buy_price > 0:
                        entry = tracker_pos.buy_price

                    current_price = 0.0
                    try:
                        current_price = await self.executor.get_token_price(signal.token_mint)
                    except Exception:
                        pass

                    # get_token_price may have updated position.entry_price as side-effect
                    if entry <= 0 and position and position.entry_price and position.entry_price > 0:
                        entry = position.entry_price
                        logger.debug(
                            "PnL: entry_price adoptado de get_token_price side-effect = {:.10g} for {}",
                            entry, signal.token_mint[:12] + "...",
                        )

                    # SOL real recibido por el trader (sin el cap de MAX_COPY_TRADE_SOL)
                    sell_proceeds = signal.sell_sol_raw if signal.sell_sol_raw > 0 else signal.amount_sol

                    def _sol_invested():
                        si = 0.0
                        if position:
                            si = getattr(position, "sol_invested", 0.0) or 0.0
                        if si <= 0 and tracker_pos:
                            si = getattr(tracker_pos, "amount", 0.0) or 0.0
                        if si <= 0:
                            si = signal.amount_sol
                        return si

                    # Default to 100% if sell_pct not detected
                    pct = signal.sell_pct if signal.sell_pct > 0 else 100.0

                    # Calcular el porcentaje REAL vendido por el trader usando los
                    # tokens que acumulo (via BUY) para ese mint. El signal.sell_pct
                    # normalmente es 0 (sin accountData), y asumir 100% confunde:
                    # los traders venden 20-40% de su posicion, no todo.
                    wm_key = (signal.wallet, signal.token_mint)
                    accumulated = self._wallet_mint_tokens.get(wm_key, 0.0)
                    if signal.trade_token_amount > 0 and accumulated > 0:
                        real_pct = (signal.trade_token_amount / accumulated) * 100.0
                        real_pct = min(real_pct, 100.0)
                        if real_pct > 1.0:
                            pct = real_pct
                            logger.info(
                                "CopyTrading: sell pct recalculado {:.1f}% (trader acumulo {:.4g} tk, vendio {:.4g}) para {}",
                                real_pct, accumulated, signal.trade_token_amount,
                                signal.token_mint[:8] + "...",
                            )
                        # Descontar lo vendido del acumulado del trader para
                        # proximas ventas parciales del mismo trader/token.
                        self._wallet_mint_tokens[wm_key] = max(
                            0.0, accumulated - signal.trade_token_amount
                        )
                    elif signal.trade_token_amount > 0:
                        logger.debug(
                            "CopyTrading: sin tracking de tokens acumulados para {:.4g} tk de {} (posiblemente compro de la mano de otra)",
                            signal.trade_token_amount, signal.token_mint[:8] + "...",
                        )

                    if entry > 0 and current_price > 0:
                        pnl_pct = (current_price - entry) / entry * 100
                        logger.debug(
                            "PnL calculado: entry={:.10g} current={:.10g} => {:.2f}% for {}",
                            entry, current_price, pnl_pct, signal.token_mint[:12] + "...",
                        )
                    elif current_price > 0 and entry <= 0:
                        # Precio disponible sin entry: valor de tokens vs invertido
                        si = _sol_invested()
                        token_held = 0.0
                        if position:
                            token_held = getattr(position, "token_amount_ui", 0.0) or 0.0
                        if token_held > 0 and si > 0:
                            current_value = token_held * current_price
                            pnl_pct = (current_value - si) / si * 100
                            logger.info(
                                "PnL estimado (precio sin entry): valor={:.6f} vs invested={:.6f} => {:.2f}%",
                                current_value, si, pnl_pct,
                            )
                        else:
                            logger.debug(
                                "PnL no disponible: sin token_amount_ui para {} | invested={:.6f}",
                                signal.token_mint[:12] + "...", si,
                            )
                    elif sell_proceeds > 0:
                        # Fallback final: SOL recibido (sin cap) vs el COSTO del
                        # trader para esos tokens vendidos. Usamos sus numeros reales:
                        # costo_por_tk = SOL_invertido_total / tokens_acumulados.
                        # asi el PnL refleja la venta PARCIAL real (20-40%), no
                        # "vendio todo lo que metio" como asumiamos antes.
                        # `accumulated` ya fue capturado ANTES del descuento en el
                        # bloque de pct, asi usamos la base correcta del costo.
                        w_sol = self._wallet_mint_sol.get(wm_key, 0.0)
                        sold_tok = (
                            signal.trade_token_amount
                            if signal.trade_token_amount > 0
                            else 0.0
                        )
                        if accumulated > 0 and sold_tok > 0:
                            cost_per_tk = w_sol / accumulated if w_sol > 0 else 0.0
                            sold_cost = cost_per_tk * sold_tok
                            if sold_cost > 0:
                                pnl_pct = (sell_proceeds - sold_cost) / sold_cost * 100
                                logger.info(
                                    "PnL estimado (costo trader): sell={:.6f} vs costo vendido={:.6f} ({} tk) => {:.2f}%",
                                    sell_proceeds, sold_cost, f"{sold_tok:.6g}", pnl_pct,
                                )
                            else:
                                si = _sol_invested()
                                if si > 0:
                                    sold_port = si * (pct / 100.0)
                                    if sold_port > 0:
                                        pnl_pct = (sell_proceeds - sold_port) / sold_port * 100
                                        logger.info(
                                            "PnL estimado (SOL/inv parcial): sell={:.6f} vs {:.1f}% vendido (inv={:.6f}) => {:.2f}%",
                                            sell_proceeds, pct, sold_port, pnl_pct,
                                        )
                        else:
                            si = _sol_invested()
                            if si > 0:
                                sold_port = si * (pct / 100.0)
                                if sold_port > 0:
                                    pnl_pct = (sell_proceeds - sold_port) / sold_port * 100
                                    logger.info(
                                        "PnL estimado (SOL/inv): sell={:.6f} vs {:.1f}% vendido (inv={:.6f}) => {:.2f}%",
                                        sell_proceeds, pct, sold_port, pnl_pct,
                                    )

                    # Record sell in stats before cleaning positions
                    from core.stats import get_trade_stats
                    _entry = entry if entry > 0 else 0.0
                    _buy_time = 0.0
                    _sol_invested = signal.amount_sol
                    _wallet_for_stats = signal.wallet
                    if tracker_pos:
                        _buy_time = float(getattr(tracker_pos, "created_at", 0.0) or 0.0)
                        _sol_invested = getattr(tracker_pos, "amount", signal.amount_sol)
                        if not _wallet_for_stats:
                            _wallet_for_stats = getattr(tracker_pos, "source_wallet", "")
                    if not _wallet_for_stats:
                        _wallet_for_stats = signal.source

                    await process_sell_and_notify(
                        signal.token_mint,
                        symbol=signal.token_symbol,
                        reason="COPY_TRADE_SELL",
                        pnl=pnl_pct,
                        sell_pct=pct,
                    )

                    # Clean up positions from both executor and tracker
                    if pct >= 99.0:
                        self.executor.positions.pop(signal.token_mint, None)
                        self.tracker.positions.pop(signal.token_mint, None)

                    # Record completed trade in stats
                    get_trade_stats().record_sell(
                        mint=signal.token_mint,
                        symbol=signal.token_symbol or signal.token_mint[:6].upper(),
                        wallet=_wallet_for_stats,
                        entry_price=_entry,
                        exit_price=0.0,  # price at sell time unknown in dry_run
                        pnl_pct=pnl_pct,
                        sol_invested=_sol_invested,
                        sol_received=0.0,
                        buy_time=_buy_time,
                        sell_time=time.time(),
                        sell_reason="COPY_TRADE_SELL",
                        sell_pct=pct,
                    )

                    logger.success(
                        "CopyTrading: SELL {} ({}) | {} | PnL: {:.2f}% | sell_pct: {:.0f}%",
                        signal.trader_label or signal.source, signal.wallet[:8] + "...",
                        signal.token_mint[:8] + "...", pnl_pct, pct,
                    )

            except Exception as exc:
                self._inc_stat("trades_failed")
                logger.error("CopyTrading: error ejecutando copy trade: {}", exc)
                self._notify(self.notifier.send_error(f"Copy trade fallido ({signal.source}): {exc}"))

    # ----------------------------------------------------------- Helius webhook setup
    async def setup_helius_webhook(self, webhook_url: str) -> Optional[str]:
        """Crea o actualiza el webhook de Helius.

        Usa webhookType "raw" para capturar TODAS las transacciones
        incluyendo las de Pump.fun que el parser enhanced podria no detectar.
        """
        helius_api_key = os.getenv("HELIUS_API_KEY", "")
        if not helius_api_key:
            rpc_url = os.getenv("HELIUS_RPC_URL", "")
            if "api-key=" in rpc_url:
                helius_api_key = rpc_url.split("api-key=")[-1].split("&")[0]
            elif rpc_url and not rpc_url.startswith("http"):
                # Si es solo el key (sin URL), usarlo directamente
                helius_api_key = rpc_url

        if not helius_api_key:
            logger.warning("CopyTrading: no se encontro HELIUS_API_KEY")
            return None

        if not self.wallets:
            return None

        payload = {
            "webhookURL": webhook_url + self.webhook_path,
            "transactionTypes": ["SWAP", "TRANSFER"],
            "accountAddresses": list(self.wallets.keys()),
            "webhookType": "enhanced",
            "txnStatus": "all",
        }

        try:
            async with aiohttp.ClientSession() as session:
                list_url = f"https://api.helius.xyz/v0/webhooks?api-key={helius_api_key}"
                async with session.get(list_url) as resp:
                    if resp.status == 200:
                        existing = await resp.json()
                        for wh in existing:
                            wh_url = wh.get("webhookURL", "")
                            wh_id = wh.get("webhookID")
                            if wh_url.endswith(self.webhook_path):
                                # Update our own webhook
                                update_url = f"https://api.helius.xyz/v0/webhooks/{wh_id}?api-key={helius_api_key}"
                                async with session.put(update_url, json=payload) as update_resp:
                                    if update_resp.status == 200:
                                        logger.success("Helius webhook actualizado: {}", wh_id)
                                        return wh_id
                            elif wh_id:
                                # Delete stale/other webhooks to free up slots
                                try:
                                    del_url = f"https://api.helius.xyz/v0/webhooks/{wh_id}?api-key={helius_api_key}"
                                    async with session.delete(del_url) as del_resp:
                                        if del_resp.status == 200:
                                            logger.info("Helius webhook viejo eliminado: {}", wh_id)
                                except Exception:
                                    pass

                create_url = f"https://api.helius.xyz/v0/webhooks?api-key={helius_api_key}"
                async with session.post(create_url, json=payload) as resp:
                    data = await resp.json()
                    wh_id = data.get("webhookID")
                    if wh_id:
                        logger.success("Helius webhook creado: {}", wh_id)
                        return wh_id
                    else:
                        error = data.get("message", str(data))
                        logger.error("Error creando webhook: {}", error)

        except Exception as exc:
            logger.error("CopyTrading: error configurando webhook: {}", exc)

        return None


# ----------------------------------------------------------- Utilidades para encontrar wallets
async def find_wallet_by_username(username: str) -> Optional[str]:
    """Busca la wallet de un trader por su username en redes sociales.

    Busca en GMGN, Birdeye y otras fuentes publicas.
    Devuelve la direccion de la wallet o None si no se encuentra.
    """
    sources = [
        f"https://gmgn.ai/sol/wallet/{username}",
        f"https://birdeye.so/address/{username}",
    ]

    for url in sources:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        # Buscar patrones de wallet Solana (32-44 chars base58)
                        import re
                        wallets = re.findall(r'[1-9A-HJ-NP-Za-km-z]{32,44}', text)
                        for w in wallets:
                            if len(w) >= 32:
                                logger.info("Wallet encontrada para {}: {}", username, w)
                                return w
        except Exception:
            continue

    return None


async def search_pumpfun_traders(min_trades: int = 10) -> list[dict[str, Any]]:
    """Busca traders activos en Pump.fun con multiples trades.

    Usa la API publica de Pump.fun para encontrar wallets activas.
    """
    traders = []
    try:
        url = "https://frontend-api.pump.fun/leaderboard"
        headers = {"User-Agent": "Mozilla/5.0"}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for entry in data[:20]:  # Top 20
                        wallet = entry.get("address", "")
                        trades = entry.get("trade_count", 0)
                        pnl = entry.get("total_pnl", 0)
                        if wallet and trades >= min_trades:
                            traders.append({
                                "wallet": wallet,
                                "trades": trades,
                                "pnl": pnl,
                                "label": entry.get("username", f"trader-{wallet[:8]}"),
                            })
    except Exception as exc:
        logger.debug("Error buscando traders de pump.fun: {}", exc)

    return traders
