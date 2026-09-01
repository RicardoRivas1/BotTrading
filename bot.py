"""Orquestador principal asíncrono del bot de memecoins en Solana.

Responsabilidades:
1. Arrancar el listener de WebSocket de nuevos tokens.
2. Validar la seguridad de cada token detectado.
3. Ejecutar compra/venta a través de Jupiter.
4. Notificar a Telegram cada operación.

Todo el flujo es asíncrono y nunca bloquea el event loop de asyncio.
Los fallos de red se capturan por módulo y se registran con loguru.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Optional

from loguru import logger

from config import AppConfig, load_config
from core.websocket import TokenWebSocket, create_listener
from core.security import TokenSecurityValidator, SecurityValidationError
from core.execution import JupiterExecutor
from core.notifier import TelegramNotifier

# -- Configuración inicial de loguru ------------------------------------------
logger.remove()
logger.add(sys.stdout, level="INFO", colorize=True)
logger.add("bot_memecoin.log", rotation="5 MB", retention=3, level="DEBUG")


class MemecoinBot:
    """Orquesta el lifecycle completo del bot de trading."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config

        # Componentes (dependencias inyectadas desde config).
        self.listener: TokenWebSocket = create_listener()
        self.validator = TokenSecurityValidator(
            rpc_url=config.solana.HELIUS_RPC_URL,
            security=config.security,
        )
        self.executor = JupiterExecutor(
            private_key=config.solana.PRIVATE_KEY,
            rpc_url=config.solana.HELIUS_RPC_URL,
            slippage_bps=config.trading.SLIPPAGE_BPS,
            buy_amount_sol=config.trading.BUY_AMOUNT_SOL,
            take_profit_pct=config.trading.TAKE_PROFIT_PCT,
            stop_loss_pct=config.trading.STOP_LOSS_PCT,
            trailing_activation_pct=config.trading.TRAILING_STOP_ACTIVATION_PCT,
            trailing_distance_pct=config.trading.TRAILING_STOP_DISTANCE_PCT,
            dry_run=config.trading.DRY_RUN,
        )
        self.notifier = TelegramNotifier(
            token=config.telegram.TELEGRAM_TOKEN,
            chat_id=config.telegram.TELEGRAM_CHAT_ID,
        )

    # ------------------------------------------------------------ Trading
    async def _process_new_token(self, mint: str) -> None:
        """Flujo completo: validar -> comprar si es seguro."""
        logger.info("Procesando nuevo token: {}", mint)

        # Paso 1: Seguridad. Cualquier rechazo se registra y se descarta.
        try:
            is_safe = await self.validator.is_token_safe(mint)
        except SecurityValidationError as exc:
            logger.warning("Token {} rechazado: {}", mint, exc)
            return
        if not is_safe:
            logger.warning("Token {} falló la validación de seguridad.", mint)
            return

        # Paso 2: Compra.
        try:
            sig = await self.executor.buy_token(mint)
        except Exception as exc:  # noqa: BLE001 - fallo operativo no bloqueante
            logger.error("Error comprando {}: {}", mint, exc)
            await self.notifier.send_error(f"No se pudo comprar {mint}: {exc}")
            return

        await self.notifier.send_buy(
            mint,
            self.config.trading.BUY_AMOUNT_SOL,
        )
        logger.success("Compra de {} ejecutada: {}", mint, sig)

    # ------------------------------------------------- Positions monitor
    async def _monitor_positions(self, interval_seconds: float) -> None:
        """Revisa periódicamente las posiciones abiertas (TP/SL/trailing).

        Corre en segundo plano tras cada compra. Una salida detectada por el
        motor (`executor.monitor_position`) se notifica a Telegram según el
        motivo acompañado del PnL real calculado.
        """
        while True:
            await asyncio.sleep(interval_seconds)
            opens = list(self.executor.positions.keys())
            for mint in opens:
                try:
                    reason, pnl = await self.executor.monitor_position(mint)
                except Exception as exc:  # noqa: BLE001 - nunca detener el bot
                    logger.warning("Error monitoreando {}: {}", mint, exc)
                    continue

                if reason == "TAKE_PROFIT":
                    await self.notifier.send_take_profit(mint, pnl)
                elif reason == "STOP_LOSS":
                    await self.notifier.send_stop_loss(mint, pnl)
                elif reason == "TRAILING_STOP":
                    await self.notifier.send_trailing_stop(mint, pnl)

    # ------------------------------------------------------------ Loop
    async def run(self) -> None:
        """Lanza el listener, el heartbeat y el monitor de posiciones."""
        logger.info("Iniciando bot de memecoins en Solana...")

        # Tareas de fondo concurrentes (ninguna bloquea el loop principal).
        listener_task = asyncio.create_task(self.listener.run())
        heartbeat_task = asyncio.create_task(
            self.notifier.start_heartbeat(interval_minutes=30.0)
        )
        monitor_task = asyncio.create_task(
            self._monitor_positions(
                interval_seconds=self.config.bot.POLL_INTERVAL_SECONDS
            )
        )

        try:
            # Consumimos los eventos que llegan de forma asíncrona.
            async for event in self.listener.events():
                mint = (
                    event.get("mint")
                    or event.get("token", {}).get("mint")
                    or event.get("address")
                )
                if not mint:
                    logger.debug("Evento sin mint, ignorado: {}", event)
                    continue

                # Procesamiento concurrente: permite varios tokens en paralelo
                # mientras seguimos escuchando (no bloquea el loop).
                asyncio.create_task(self._process_new_token(mint))
        finally:
            self.listener.stop()
            for task in (listener_task, heartbeat_task, monitor_task):
                task.cancel()
            await asyncio.gather(*(
                task for task in (listener_task, heartbeat_task, monitor_task)
            ), return_exceptions=True)

    async def shutdown(self) -> None:
        """Apagado ordenado."""
        logger.info("Apagando bot...")
        self.listener.stop()


async def main() -> None:
    """Punto de entrada asíncrono."""
    config = load_config()
    bot = MemecoinBot(config)

    try:
        await bot.run()
    except KeyboardInterrupt:
        pass
    finally:
        await bot.shutdown()
        logger.info("Bot finalizado.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupción del usuario.")
