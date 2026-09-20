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
        self._lock = asyncio.Lock()
        self.webhook_path = "/webhook/copy-trading"
        self._load_wallets()

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
        y tokenTransfers para ver si alguna wallet monitoreada participo."""
        native_transfers = tx.get("nativeTransfers", [])
        token_transfers = tx.get("tokenTransfers", [])
        involved: set[str] = set()
        for nt in native_transfers:
            involved.add(nt.get("fromUserAccount", ""))
            involved.add(nt.get("toUserAccount", ""))
        for tt in token_transfers:
            involved.add(tt.get("fromUserAccount", ""))
            involved.add(tt.get("toUserAccount", ""))
        for addr in involved:
            tracked = self.wallets.get(addr)
            if tracked and tracked.enabled:
                return tracked
        return None

    async def start(self) -> None:
        """Inicia la estrategia de copy trading y el polling de RPC."""
        self._set_state(StrategyState.RUNNING)
        logger.info("CopyTrading: estrategia iniciada ({} wallets)", len(self.wallets))
        asyncio.create_task(self._rpc_poll_loop())

    async def _rpc_poll_loop(self) -> None:
        """Poll every 1.5s via getSignaturesForAddress to detect new trades faster than webhooks."""
        import aiohttp
        from config import load_config

        cfg = load_config()
        rpc_url = cfg.solana.HELIUS_RPC_URL
        helius_api_key = os.getenv("HELIUS_API_KEY", "")
        if not helius_api_key and "api-key=" in rpc_url:
            helius_api_key = rpc_url.split("api-key=")[-1].split("&")[0]

        # Track last seen signature per wallet
        last_sig: dict[str, str] = {}
        # Initialize with current latest sig for each wallet
        try:
            async with aiohttp.ClientSession() as session:
                for addr in self.wallets:
                    payload = {
                        "jsonrpc": "2.0", "id": 1,
                        "method": "getSignaturesForAddress",
                        "params": [addr, {"limit": 1}],
                    }
                    async with session.post(rpc_url, json=payload) as resp:
                        data = await resp.json()
                        sigs = data.get("result", [])
                        if sigs:
                            last_sig[addr] = sigs[0]["signature"]
            logger.info("RPC polling initialized for {} wallets", len(last_sig))
        except Exception as exc:
            logger.warning("RPC polling init failed: {}", exc)

        while True:
            await asyncio.sleep(1.5)
            if self._state != StrategyState.RUNNING:
                continue
            try:
                await self._poll_wallets(rpc_url, helius_api_key, last_sig)
            except Exception as exc:
                logger.debug("RPC poll error: {}", exc)

    async def _poll_wallets(
        self, rpc_url: str, helius_api_key: str, last_sig: dict[str, str]
    ) -> None:
        """Check each wallet for new signatures and fetch enhanced tx data."""
        import aiohttp

        async with aiohttp.ClientSession() as session:
            for addr in self.wallets:
                try:
                    payload = {
                        "jsonrpc": "2.0", "id": 1,
                        "method": "getSignaturesForAddress",
                        "params": [addr, {"limit": 5}],
                    }
                    async with session.post(rpc_url, json=payload) as resp:
                        data = await resp.json()

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

                    # Fetch enhanced transaction data from Helius
                    if not helius_api_key:
                        continue
                    enhance_url = f"https://api.helius.xyz/v0/transactions/?api-key={helius_api_key}"
                    enhance_payload = {"transactions": new_sigs}
                    async with session.post(enhance_url, json=enhance_payload) as resp:
                        if resp.status != 200:
                            continue
                        enhanced_txs = await resp.json()

                    for tx in enhanced_txs:
                        await self._process_transaction(tx)

                except Exception as exc:
                    logger.debug("Poll error for {}: {}", addr[:8], exc)

    async def stop(self) -> None:
        """Detiene la estrategia."""
        self._set_state(StrategyState.STOPPED)
        logger.info("CopyTrading: estrategia detenida")

    # ----------------------------------------------------------- Webhook handler
    async def handle_webhook(self, payload: dict[str, Any]) -> dict[str, str]:
        """Procesa un webhook de Helius Enhanced Transaction."""
        self._inc_stat("signals_received")

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

        if tx_type not in COPY_TRADE_TYPES:
            logger.debug(
                "CopyTrading: tx filtrada por tipo '{}' (esperado {}): sig={}",
                tx_type, COPY_TRADE_TYPES, signature[:16] + "...",
            )
            return

        tracked = self.wallets.get(fee_payer)

        # When wallet is found via transfers, use its address as the trader
        wallet_address = ""
        if not tracked or not tracked.enabled:
            tracked = self._find_tracked_wallet_in_transfers(tx)
            if not tracked:
                logger.debug(
                    "CopyTrading: fee_payer {} no es wallet monitoreada. sig={}",
                    fee_payer[:12] + "...", signature[:16] + "...",
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

        # Detectar porcentaje de venta desde tokenBalanceChanges
        sell_pct = 0.0
        sell_mint_from_balance = None
        for acct in account_data:
            if acct.get("account") != trader:
                continue
            for tbc in acct.get("tokenBalanceChanges", []):
                tbc_mint = tbc.get("mint", "")
                if not tbc_mint or tbc_mint in excluded:
                    continue
                # tokenAmount = balance DESPUES de la tx
                # mintAmount = tokens ganados (>0) o perdidos (<0)
                mint_delta = tbc.get("mintAmount", 0)
                final_balance = tbc.get("tokenAmount", 0)
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

        # Caso 1: Recibe tokens y envia SOL = COMPRA
        if tokens_received and sol_spent > 0:
            action = "buy"
            tokens_received.sort(key=lambda t: t["amount"], reverse=True)
            token_mint = tokens_received[0]["mint"]
            amount_sol = sol_spent

        # Caso 2: Envia tokens y recibe SOL = VENTA
        elif tokens_sent and sol_received > 0:
            action = "sell"
            tokens_sent.sort(key=lambda t: t["amount"], reverse=True)
            token_mint = tokens_sent[0]["mint"]
            amount_sol = sol_received

        # Caso 3: Solo envio de SOL (posible compra en bonding curve)
        elif sol_spent > 0 and not tokens_received:
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

        # Caso 4: Solo recibe SOL (posible venta en bonding curve)
        elif sol_received > 0 and not tokens_sent:
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

        if not action or not token_mint:
            logger.debug(
                "CopyTrading: no se pudo determinar mint para {} | "
                "trader={} | accounts={} | tokenTransfers={} | "
                "sol_spent={:.6f} | sol_received={:.6f} | "
                "tokens_rcvd={} | tokens_sent={}",
                signature[:16] + "...", trader[:8] + "...", len(account_data),
                len(token_transfers), sol_spent, sol_received,
                len(tokens_received), len(tokens_sent),
            )
            return None

        # Filtrar transfers de SOL minimos (fees de red)
        if amount_sol < 0.0001:
            return None

        # Limitar monto al maximo configurado
        max_amount = float(
            getattr(self.config.copy_trading, "MAX_COPY_TRADE_SOL", 0.01)
        )
        if amount_sol > max_amount:
            amount_sol = max_amount

        source_label = "pump.fun" if is_pump_fun else "dex"
        sell_info = f" | sell_pct={sell_pct:.0f}%" if action == "sell" and sell_pct > 0 else ""
        logger.info(
            "CopyTrading: signal {} {} | mint={} | {:.6f} SOL | trader={} | src={}{}",
            action.upper(), signature[:16] + "...", token_mint[:12] + "...",
            amount_sol, trader[:8] + "...", source_label, sell_info,
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
        )

    async def _execute_copy_trade(self, signal: CopyTradeSignal) -> None:
        """Ejecuta un copy trade."""
        from core.websocket import process_buy_and_notify, process_sell_and_notify

        async with self._lock:
            try:
                if signal.action == "buy":
                    existing = self.executor.positions.get(signal.token_mint)
                    if existing:
                        return

                    # If max positions reached, close oldest from same wallet to rotate
                    max_pos = int(getattr(self.config.trading, "MAX_OPEN_POSITIONS", 10))
                    if len(self.executor.positions) >= max_pos and signal.wallet:
                        wallet_positions = self.tracker.get_positions_by_wallet(signal.wallet)
                        if wallet_positions:
                            oldest = wallet_positions[0]
                            logger.info(
                                "CopyTrading: Rotacion - cerrando {} ({}) para abrir nuevo token",
                                oldest.symbol, oldest.mint[:12] + "...",
                            )
                            await process_sell_and_notify(
                                oldest.mint,
                                symbol=oldest.symbol,
                                reason="ROTATION",
                                sell_pct=100.0,
                            )
                            self.executor.positions.pop(oldest.mint, None)
                            self.tracker.positions.pop(oldest.mint, None)
                        elif len(self.executor.positions) >= max_pos:
                            return
                    elif len(self.executor.positions) >= max_pos:
                        return

                    original_amount = self.executor.buy_amount_sol
                    self.executor.buy_amount_sol = signal.amount_sol
                    try:
                        sig = await self.executor.buy_token(signal.token_mint)
                    finally:
                        self.executor.buy_amount_sol = original_amount

                    if sig is None:
                        return

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

                    await self.notifier.send_buy(
                        signal.token_mint,
                        signal.amount_sol,
                        symbol=symbol,
                        dry_run=self.config.trading.DRY_RUN,
                    )

                    logger.success(
                        "CopyTrading: BUY {} | {} SOL | {} ({})",
                        signal.source, f"{signal.amount_sol:.6f}",
                        symbol, signal.token_mint[:8] + "...",
                    )

                elif signal.action == "sell":
                    # First try exact mint match
                    position = self.executor.positions.get(signal.token_mint)
                    tracker_pos = self.tracker.positions.get(signal.token_mint)

                    # Fallback: find ALL positions from this wallet and sell them
                    if not position and not tracker_pos and signal.wallet:
                        wallet_positions = self.tracker.get_positions_by_wallet(signal.wallet)
                        if wallet_positions:
                            # Sell the most recent position from this wallet
                            pos = wallet_positions[-1]
                            signal.token_mint = pos.mint
                            signal.token_symbol = pos.symbol
                            tracker_pos = pos
                            logger.info(
                                "CopyTrading: SELL por wallet {} -> vendiendo {} ({})",
                                signal.wallet[:8] + "...", pos.symbol, pos.mint[:12] + "...",
                            )

                    if not position and not tracker_pos:
                        logger.info(
                            "CopyTrading: SELL ignorado {} ({}) - no hay posicion",
                            signal.source, signal.token_mint[:8] + "...",
                        )
                        return

                    pnl_pct = 0.0
                    entry = 0.0
                    if position and position.entry_price and position.entry_price > 0:
                        entry = position.entry_price
                    elif tracker_pos and tracker_pos.buy_price and tracker_pos.buy_price > 0:
                        entry = tracker_pos.buy_price
                    if entry > 0:
                        try:
                            current_price = await self.executor.get_token_price(signal.token_mint)
                            if current_price > 0:
                                pnl_pct = (current_price - entry) / entry * 100
                        except Exception:
                            pass

                    # Default to 100% if sell_pct not detected
                    pct = signal.sell_pct if signal.sell_pct > 0 else 100.0

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

                    logger.success(
                        "CopyTrading: SELL {} | {} | PnL: {:.2f}% | sell_pct: {:.0f}%",
                        signal.source, signal.token_mint[:8] + "...", pnl_pct, pct,
                    )

            except Exception as exc:
                self._inc_stat("trades_failed")
                logger.error("CopyTrading: error ejecutando copy trade: {}", exc)
                await self.notifier.send_error(f"Copy trade fallido ({signal.source}): {exc}")

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
                            if wh.get("webhookURL", "").endswith(self.webhook_path):
                                wh_id = wh.get("webhookID")
                                update_url = f"https://api.helius.xyz/v0/webhooks/{wh_id}?api-key={helius_api_key}"
                                async with session.put(update_url, json=payload) as update_resp:
                                    if update_resp.status == 200:
                                        logger.success("Helius webhook actualizado: {}", wh_id)
                                        return wh_id

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
