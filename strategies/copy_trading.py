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
COPY_TRADE_TYPES = {"SWAP", "BUY", "SELL", "TRANSFER"}

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
    _KNOWN_ADDRESSES: set[str] = {SOL_MINT} | set(DEX_PROGRAMS.values())

    def _extract_mint_from_account_data(
        self, account_data: list[dict[str, Any]], fee_payer: str
    ) -> Optional[str]:
        """Intenta encontrar el mint del token que compro/vendio el fee_payer
        analizando tokenBalanceChanges en accountData de Helius Enhanced."""
        for acct in account_data:
            for tbc in acct.get("tokenBalanceChanges", []):
                user = tbc.get("userAccount", "")
                if user != fee_payer:
                    continue
                mint = tbc.get("mint", "")
                if not mint or mint in self._KNOWN_ADDRESSES:
                    continue
                raw = tbc.get("rawTokenAmount", {})
                amount_str = raw.get("tokenAmount", "0")
                try:
                    amount = int(amount_str)
                except (ValueError, TypeError):
                    amount = 0
                if amount > 0:
                    return mint
        return None

    def _extract_mint_from_sent_tokens(
        self, account_data: list[dict[str, Any]], fee_payer: str
    ) -> Optional[str]:
        """Extrae el mint del token que envio (vendio) el fee_payer."""
        for acct in account_data:
            for tbc in acct.get("tokenBalanceChanges", []):
                user = tbc.get("userAccount", "")
                if user != fee_payer:
                    continue
                mint = tbc.get("mint", "")
                if not mint or mint in self._KNOWN_ADDRESSES:
                    continue
                raw = tbc.get("rawTokenAmount", {})
                amount_str = raw.get("tokenAmount", "0")
                try:
                    amount = int(amount_str)
                except (ValueError, TypeError):
                    amount = 0
                if amount < 0:
                    return mint
        return None

    def _extract_mint_from_description(self, description: str) -> Optional[str]:
        """Busca un mint en la descripcion de la transaccion, excluyendo
        direcciones conocidas (programas DEX, SOL mint)."""
        candidates = re.findall(r'[1-9A-HJ-NP-Za-km-z]{32,44}', description)
        for candidate in candidates:
            if candidate not in self._KNOWN_ADDRESSES:
                return candidate
        return None

    async def start(self) -> None:
        """Inicia la estrategia de copy trading."""
        self._set_state(StrategyState.RUNNING)
        logger.info("CopyTrading: estrategia iniciada ({} wallets)", len(self.wallets))

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
            return

        tracked = self.wallets.get(fee_payer)
        if not tracked or not tracked.enabled:
            return

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
            "CopyTrading: trade detectado de {} ({})",
            tracked.label, fee_payer[:8] + "...",
        )

        signal = self._parse_trade(tx, tracked)
        if signal:
            await self._execute_copy_trade(signal)

    def _parse_trade(self, tx: dict[str, Any], tracked: TrackedWallet) -> Optional[CopyTradeSignal]:
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
            if nt.get("fromUserAccount") == fee_payer
        )
        sol_received = sum(
            nt.get("amount", 0) / 1e9
            for nt in native_transfers
            if nt.get("toUserAccount") == fee_payer
        )

        # Analizar transfers de tokens (excluyendo SOL)
        tokens_received = []
        tokens_sent = []
        for tt in token_transfers:
            mint = tt.get("mint", "")
            if not mint or mint == SOL_MINT:
                continue
            amount = tt.get("tokenAmount", 0)
            if tt.get("toUserAccount") == fee_payer:
                tokens_received.append({"mint": mint, "amount": amount})
            elif tt.get("fromUserAccount") == fee_payer:
                tokens_sent.append({"mint": mint, "amount": amount})

        # Determinar accion
        action = None
        token_mint = None
        amount_sol = 0.0

        # Caso 1: Recibe tokens y envia SOL = COMPRA
        if tokens_received and sol_spent > 0:
            action = "buy"
            token_mint = tokens_received[0]["mint"]
            amount_sol = sol_spent

        # Caso 2: Envia tokens y recibe SOL = VENTA
        elif tokens_sent and sol_received > 0:
            action = "sell"
            token_mint = tokens_sent[0]["mint"]
            amount_sol = sol_received

        # Caso 3: Solo envio de SOL (posible compra en bonding curve)
        elif sol_spent > 0 and not tokens_received:
            # 1) Intentar desde accountData (tokenBalanceChanges)
            token_mint = self._extract_mint_from_account_data(account_data, fee_payer)
            if token_mint:
                action = "buy"
                amount_sol = sol_spent
            else:
                # 2) Fallback: descripcion de la transaccion
                token_mint = self._extract_mint_from_description(
                    tx.get("description", "")
                )
                if token_mint:
                    action = "buy"
                    amount_sol = sol_spent

        # Caso 4: Solo recibe SOL (posible venta en bonding curve)
        elif sol_received > 0 and not tokens_sent:
            token_mint = self._extract_mint_from_sent_tokens(account_data, fee_payer)
            if token_mint:
                action = "sell"
                amount_sol = sol_received
            else:
                token_mint = self._extract_mint_from_description(
                    tx.get("description", "")
                )
                if token_mint:
                    action = "sell"
                    amount_sol = sol_received

        if not action or not token_mint:
            logger.debug(
                "CopyTrading: no se pudo determinar mint para {} | "
                "accounts={} | tokenTransfers={} | "
                "sol_spent={:.6f} | sol_received={:.6f}",
                signature[:16] + "...", len(account_data),
                len(token_transfers), sol_spent, sol_received,
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
        return CopyTradeSignal(
            wallet=tracked.address,
            action=action,
            token_mint=token_mint,
            token_symbol=token_mint[:6].upper(),
            amount_sol=amount_sol,
            tx_signature=signature,
            timestamp=float(timestamp) if timestamp else time.time(),
            source=f"helius:{tracked.label}:{source_label}",
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

                    max_pos = int(getattr(self.config.trading, "MAX_OPEN_POSITIONS", 3))
                    if len(self.executor.positions) >= max_pos:
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
                    position = self.executor.positions.get(signal.token_mint)
                    if not position:
                        return

                    pnl_pct = 0.0
                    if position.entry_price and position.entry_price > 0:
                        try:
                            current_price = await self.executor.get_token_price(signal.token_mint)
                            pnl_pct = (current_price - position.entry_price) / position.entry_price * 100
                        except Exception:
                            pass

                    await process_sell_and_notify(
                        signal.token_mint,
                        reason="COPY_TRADE_SELL",
                        pnl=pnl_pct,
                    )

                    logger.success(
                        "CopyTrading: SELL {} | {} | PnL: {:.2f}%",
                        signal.source, signal.token_mint[:8] + "...", pnl_pct,
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

        # Usar raw webhook para capturar pump.fun transactions
        payload = {
            "webhookURL": webhook_url + self.webhook_path,
            "transactionTypes": ["SWAP"],
            "accountAddresses": list(self.wallets.keys()),
            "webhookType": "enhanced",
            "txnStatus": "success",
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
