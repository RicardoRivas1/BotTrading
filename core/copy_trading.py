"""Copy Trading module - Monitorea wallets de traders conocidos en Solana.

Recibe transacciones en tiempo real via Helius Enhanced Webhooks, parsea
operaciones SWAP/BUY/SELL y ejecuta trades replicados a traves de Jupiter.

Flujo:
1. Helius envia HTTP POST a nuestro endpoint /webhook/copy-trading
2. Parseamos la transaccion para detectar compras/ventas de tokens
3. Ejecutamos la misma operacion via JupiterExecutor
4. Notificamos a Telegram la operacion replicada
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp
from loguru import logger

# Programas DEX conocidos en Solana
DEX_PROGRAMS = {
    "JUPITER": "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
    "RAYDIUM": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "RAYDIUM_CLMM": "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
    "PUMP_FUN": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    "PUMP_AMM": "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
    "ORCA": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
    "PHOTON": "AE5WQ8hJXJpEKkX49bC3TfRbMhFpXbE6V2BbN7jF9gH",
}

# Token SOL nativo
SOL_MINT = "So11111111111111111111111111111111111111112"

# Tipos de transaccion que nos interesan para copy trading
COPY_TRADE_TYPES = {"SWAP", "BUY", "SELL"}


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


class CopyTrader:
    """Motor de copy trading que monitorea wallets y replica operaciones.

    Usa Helius Enhanced Webhooks para recibir transacciones en tiempo real
    de las wallets configuradas. Parsea el payload para detectar swaps y
    ejecuta la misma operacion via JupiterExecutor.
    """

    def __init__(
        self,
        executor: Any,
        notifier: Any,
        config: Any,
        webhook_path: str = "/webhook/copy-trading",
    ) -> None:
        self.executor = executor
        self.notifier = notifier
        self.config = config
        self.webhook_path = webhook_path

        # Wallets monitoreadas
        self.wallets: dict[str, TrackedWallet] = {}
        # Senales recientes (deduplicacion)
        self._recent_signals: dict[str, float] = {}
        # Lock para operaciones concurrentes
        self._lock = asyncio.Lock()
        # Estadisticas
        self.stats = {
            "webhooks_received": 0,
            "trades_detected": 0,
            "trades_copied": 0,
            "trades_failed": 0,
        }

        # Cargar wallets desde config
        self._load_wallets()

    def _load_wallets(self) -> None:
        """Carga las wallets a monitorear desde variables de entorno."""
        # Wallets configuradas via COPY_TRADE_WALLET_<ADDRESS>=<LABEL>
        # Ejemplo: COPY_TRADE_WALLET_7YttLkHDoNj9wyDur5pM1ejNaAvT9X4eSTaXF54GkEhJ=Cupsey
        for key, value in os.environ.items():
            if key.startswith("COPY_TRADE_WALLET_") and value:
                address = key.replace("COPY_TRADE_WALLET_", "")
                if len(address) >= 32:  # Valida que sea una direccion Solana
                    self.wallets[address] = TrackedWallet(
                        address=address,
                        label=value.strip(),
                    )
                    logger.info(
                        " wallet de copy trading cargada: {} ({})",
                        value.strip(), address[:8] + "...",
                    )

        # Wallets via COPY_TRADE_WALLET_ADDRESSES (separadas por coma)
        addresses_str = os.getenv("COPY_TRADE_WALLET_ADDRESSES", "")
        labels_str = os.getenv("COPY_TRADE_WALLET_LABELS", "")
        if addresses_str:
            addresses = [a.strip() for a in addresses_str.split(",") if a.strip()]
            labels = [lbl.strip() for lbl in labels_str.split(",")] if labels_str else []
            for i, addr in enumerate(addresses):
                label = labels[i] if i < len(labels) else f"Trader-{i+1}"
                if addr not in self.wallets:
                    self.wallets[addr] = TrackedWallet(address=addr, label=label)
                    logger.info(
                        " wallet de copy trading cargada: {} ({})",
                        label, addr[:8] + "...",
                    )

        if not self.wallets:
            logger.warning(
                " No hay wallets de copy trading configuradas. "
                "Usa COPY_TRADE_WALLET_<ADDRESS>=<LABEL> o "
                "COPY_TRADE_WALLET_ADDRESSES para configurar."
            )
        else:
            logger.info(
                " Copy trading activo: {} wallets monitoreadas",
                len(self.wallets),
            )

    # ----------------------------------------------------------- Webhook handler
    async def handle_webhook(self, payload: dict[str, Any]) -> dict[str, str]:
        """Procesa un webhook de Helius Enhanced Transaction.

        Devuelve un dict con el status del procesamiento.
        """
        self.stats["webhooks_received"] += 1

        try:
            # El payload de Helius es un array de transacciones
            transactions = payload if isinstance(payload, list) else [payload]

            for tx in transactions:
                await self._process_transaction(tx)

            return {"status": "ok", "processed": str(len(transactions))}

        except Exception as exc:
            logger.error("Error procesando webhook de copy trading: {}", exc)
            return {"status": "error", "message": str(exc)}

    async def _process_transaction(self, tx: dict[str, Any]) -> None:
        """Parsea una transaccion Enhanced de Helius y detecta trades."""
        tx_type = tx.get("type", "")
        fee_payer = tx.get("feePayer", "")
        signature = tx.get("signature", "")
        _timestamp = tx.get("timestamp", 0)

        # Solo procesar tipos de swap/compra/venta
        if tx_type not in COPY_TRADE_TYPES:
            return

        # Verificar si el fee payer es una wallet que monitoreamos
        tracked = self.wallets.get(fee_payer)
        if not tracked or not tracked.enabled:
            return

        # Deduplicacion: no procesar la misma transaccion dos veces
        if signature in self._recent_signals:
            return
        self._recent_signals[signature] = time.time()
        # Limpiar senales antiguas (> 5 minutos)
        cutoff = time.time() - 300
        self._recent_signals = {
            k: v for k, v in self._recent_signals.items() if v > cutoff
        }

        self.stats["trades_detected"] += 1
        logger.info(
            " Copy trade detectado de {} ({}): tipo={}",
            tracked.label, fee_payer[:8] + "...", tx_type,
        )

        # Parsear la transaccion para extraer el token y la accion
        signal = self._parse_trade(tx, tracked)
        if signal:
            await self._execute_copy_trade(signal)

    def _parse_trade(self, tx: dict[str, Any], tracked: TrackedWallet) -> Optional[CopyTradeSignal]:
        """Extrae la senal de trading de una transaccion Enhanced."""
        signature = tx.get("signature", "")
        fee_payer = tx.get("feePayer", "")
        timestamp = tx.get("timestamp", 0)

        # Analizar tokenTransfers para determinar la accion
        token_transfers = tx.get("tokenTransfers", [])
        native_transfers = tx.get("nativeTransfers", [])
        _account_data = tx.get("accountData", [])

        # Buscar transfers de SOL del fee_payer (indicador de compra)
        sol_spent = 0.0
        for nt in native_transfers:
            if nt.get("fromUserAccount") == fee_payer:
                sol_spent += nt.get("amount", 0) / 1e9

        sol_received = 0.0
        for nt in native_transfers:
            if nt.get("toUserAccount") == fee_payer:
                sol_received += nt.get("amount", 0) / 1e9

        # Analizar token transfers
        tokens_received = []
        tokens_sent = []
        for tt in token_transfers:
            mint = tt.get("mint", "")
            if mint == SOL_MINT or not mint:
                continue
            amount = tt.get("tokenAmount", 0)
            if tt.get("toUserAccount") == fee_payer:
                tokens_received.append({"mint": mint, "amount": amount})
            elif tt.get("fromUserAccount") == fee_payer:
                tokens_sent.append({"mint": mint, "amount": amount})

        # Determinar accion: si recibe tokens y envia SOL = BUY
        # Si envia tokens y recibe SOL = SELL
        action = None
        token_mint = None
        amount_sol = 0.0

        if tokens_received and sol_spent > 0:
            action = "buy"
            token_mint = tokens_received[0]["mint"]
            amount_sol = sol_spent
        elif tokens_sent and sol_received > 0:
            action = "sell"
            token_mint = tokens_sent[0]["mint"]
            amount_sol = sol_received

        if not action or not token_mint:
            logger.debug(
                " No se pudo determinar accion de copy trade para {}",
                signature[:16] + "...",
            )
            return None

        # Limitar monto al maximo configurado
        max_amount = float(
            getattr(self.config.copy_trading, "MAX_COPY_TRADE_SOL", 0.01)
        )
        if amount_sol > max_amount:
            logger.info(
                " Copy trade de {} excede max ({} > {} SOL); usando maximo",
                tracked.label, f"{amount_sol:.4f}", f"{max_amount:.4f}",
            )
            amount_sol = max_amount

        return CopyTradeSignal(
            wallet=tracked.address,
            action=action,
            token_mint=token_mint,
            token_symbol=token_mint[:6].upper(),
            amount_sol=amount_sol,
            tx_signature=signature,
            timestamp=float(timestamp) if timestamp else time.time(),
            source=f"helius:{tracked.label}",
        )

    async def _execute_copy_trade(self, signal: CopyTradeSignal) -> None:
        """Ejecuta un copy trade basado en la senal detectada."""

        async with self._lock:
            # Verificar si ya tenemos posicion en este token
            existing = self.executor.positions.get(signal.token_mint)
            if existing and signal.action == "buy":
                logger.info(
                    " Ya tenemos posicion en {}; omitiendo copy buy de {}",
                    signal.token_mint[:8] + "...", signal.source,
                )
                return

            # Verificar max posiciones abiertas
            max_positions = int(
                getattr(self.config.trading, "MAX_OPEN_POSITIONS", 3)
            )
            if signal.action == "buy" and len(self.executor.positions) >= max_positions:
                logger.info(
                    " Maximo de posiciones abiertas ({}) alcanzado; omitiendo copy buy",
                    max_positions,
                )
                return

            try:
                if signal.action == "buy":
                    await self._execute_buy(signal)
                elif signal.action == "sell":
                    await self._execute_sell(signal)

                self.stats["trades_copied"] += 1
                tracked = self.wallets.get(signal.wallet)
                if tracked:
                    tracked.total_trades += 1
                    tracked.last_trade_at = time.time()

            except Exception as exc:
                self.stats["trades_failed"] += 1
                logger.error(
                    " Error ejecutando copy trade de {}: {}",
                    signal.source, exc,
                )
                await self.notifier.send_error(
                    f"Copy trade fallido ({signal.source}): {exc}"
                )

    async def _execute_buy(self, signal: CopyTradeSignal) -> None:
        """Ejecuta un copy buy."""
        logger.info(
            " EJECUTANDO COPY BUY de {}: {} | {} SOL | Token: {}",
            signal.source, signal.action.upper(),
            f"{signal.amount_sol:.6f}", signal.token_mint[:8] + "...",
        )

        # Override temporal del buy_amount_sol para este trade
        original_amount = self.executor.buy_amount_sol
        self.executor.buy_amount_sol = signal.amount_sol

        try:
            sig = await self.executor.buy_token(signal.token_mint)
        finally:
            self.executor.buy_amount_sol = original_amount

        if sig is None:
            logger.info(
                " Copy buy de {} omitido (sin liquidez): {}",
                signal.source, signal.token_mint[:8] + "...",
            )
            return

        # Obtener symbol
        try:
            symbol = await self.executor.get_token_symbol(signal.token_mint)
        except Exception:
            symbol = signal.token_mint[:6].upper()

        # Registrar en tracker
        from core.tracker import get_global_tracker
        tracker = get_global_tracker()
        entry_price = 0.0
        position = self.executor.positions.get(signal.token_mint)
        if position and position.entry_price and position.entry_price > 0:
            entry_price = position.entry_price
        else:
            try:
                entry_price = await self.executor.get_token_price(signal.token_mint)
            except Exception:
                pass

        tracker.add_position(
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
            " Copy BUY ejecutado de {} | {} SOL | {} ({}) | sig={}",
            signal.source, f"{signal.amount_sol:.6f}",
            symbol, signal.token_mint[:8] + "...",
            str(sig)[:16] + "..." if sig else "DRY_RUN",
        )

    async def _execute_sell(self, signal: CopyTradeSignal) -> None:
        """Ejecuta un copy sell (vende la posicion que tenemos)."""
        position = self.executor.positions.get(signal.token_mint)
        if not position:
            logger.info(
                " Copy sell de {} ignorado: no tenemos posicion en {}",
                signal.source, signal.token_mint[:8] + "...",
            )
            return

        logger.info(
            " EJECUTANDO COPY SELL de {}: {} | Token: {} | Balance: {}",
            signal.source, signal.action.upper(),
            signal.token_mint[:8] + "...",
            f"{position.token_amount_ui:.2f}",
        )

        try:
            from core.websocket import process_sell_and_notify
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
        except Exception as exc:
            logger.error(
                " Error en copy sell de {}: {}", signal.source, exc
            )
            raise

        logger.success(
            " Copy SELL ejecutado de {} | {} ({}) | PnL: {:.2f}%",
            signal.source, signal.token_mint[:8] + "...",
            signal.token_mint[:6].upper(), pnl_pct,
        )

    # ----------------------------------------------------------- Setup Helius webhook
    async def setup_helius_webhook(self, webhook_url: str) -> Optional[str]:
        """Crea o actualiza el webhook de Helius para monitorear las wallets.

        Args:
            webhook_url: URL publica donde Helius enviara los webhooks
                         (ej: https://tu-bot.onrender.com/webhook/copy-trading)

        Returns:
            El webhook ID si se creo correctamente, None si falla.
        """
        helius_api_key = os.getenv("HELIUS_API_KEY", "")
        if not helius_api_key:
            # Intentar extraer del HELIUS_RPC_URL (formato: https://mainnet.helius-rpc.com/?api-key=XXX)
            rpc_url = os.getenv("HELIUS_RPC_URL", "")
            if "api-key=" in rpc_url:
                helius_api_key = rpc_url.split("api-key=")[-1].split("&")[0]

        if not helius_api_key:
            logger.warning(
                " No se encontro HELIUS_API_KEY para crear webhook. "
                "Configura HELIUS_API_KEY o usa HELIUS_RPC_URL con api-key."
            )
            return None

        if not self.wallets:
            logger.warning(" No hay wallets configuradas para el webhook.")
            return None

        wallet_addresses = list(self.wallets.keys())

        payload = {
            "webhookURL": webhook_url + self.webhook_path,
            "transactionTypes": ["SWAP", "BUY", "SELL"],
            "accountAddresses": wallet_addresses,
            "webhookType": "enhanced",
            "txnStatus": "confirmed",
        }

        try:
            async with aiohttp.ClientSession() as session:
                # Verificar webhooks existentes
                list_url = f"https://api.helius.xyz/v0/webhooks?api-key={helius_api_key}"
                async with session.get(list_url) as resp:
                    if resp.status == 200:
                        existing = await resp.json()
                        for wh in existing:
                            if wh.get("webhookURL", "").endswith(self.webhook_path):
                                # Actualizar webhook existente
                                wh_id = wh.get("webhookID")
                                update_url = f"https://api.helius.xyz/v0/webhooks/{wh_id}?api-key={helius_api_key}"
                                async with session.put(update_url, json=payload) as update_resp:
                                    if update_resp.status == 200:
                                        logger.success(
                                            " Helius webhook actualizado: {} wallets -> {}",
                                            len(wallet_addresses), webhook_url + self.webhook_path,
                                        )
                                        return wh_id
                                    else:
                                        error = await update_resp.text()
                                        logger.error("Error actualizando webhook: {}", error)

                # Crear nuevo webhook
                create_url = f"https://api.helius.xyz/v0/webhooks?api-key={helius_api_key}"
                async with session.post(create_url, json=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        wh_id = data.get("webhookID")
                        logger.success(
                            " Helius webhook creado: {} -> {} | Wallets: {}",
                            wh_id, webhook_url + self.webhook_path,
                            len(wallet_addresses),
                        )
                        return wh_id
                    else:
                        error = await resp.text()
                        logger.error("Error creando webhook: {}", error)
                        return None

        except Exception as exc:
            logger.error("Error configurando Helius webhook: {}", exc)
            return None

    # ----------------------------------------------------------- Stats
    def get_stats(self) -> dict[str, Any]:
        """Devuelve estadisticas del copy trader."""
        return {
            **self.stats,
            "wallets_monitored": len(self.wallets),
            "wallets": {
                addr[:8] + "...": {
                    "label": w.label,
                    "enabled": w.enabled,
                    "total_trades": w.total_trades,
                }
                for addr, w in self.wallets.items()
            },
        }


# -- Instancia global compartida -----------------------------------------------
_copy_trader: Optional[CopyTrader] = None


def set_global_copy_trader(trader: CopyTrader) -> None:
    """Establece la instancia global del copy trader."""
    global _copy_trader
    _copy_trader = trader


def get_global_copy_trader() -> Optional[CopyTrader]:
    """Devuelve la instancia global del copy trader (o None)."""
    return _copy_trader
