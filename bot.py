"""Orquestador principal del bot de trading en Solana.

Framework modular con estrategias intercambiables:
- MemecoinSniper: snipeo de nuevos tokens
- CopyTrading: copia trades de wallets conocidas
- Arbitrage: arbitraje entre DEXs
- DCA: Dollar Cost Averaging periodico

El StrategyEngine gestiona el lifecycle de todas las estrategias activas.
"""

from __future__ import annotations

import asyncio
import os
import sys

from aiohttp import web
from loguru import logger

from config import AppConfig, load_config
from core.engine import StrategyEngine
from core.execution import JupiterExecutor
from core.notifier import TelegramNotifier
from core.tracker import PositionTracker, set_global_tracker
from strategies.arbitrage import ArbitrageStrategy
from strategies.copy_trading import CopyTradingStrategy
from strategies.dca import DCAStrategy

# -- Configuracion inicial de loguru ------------------------------------------
logger.remove()
logger.add(sys.stdout, level="INFO", colorize=True)
logger.add("bot_memecoin.log", rotation="5 MB", retention=3, level="DEBUG")


async def start_health_server(engine: StrategyEngine) -> None:
    """Servidor HTTP de salud, webhooks y stats.

    Endpoints:
    - GET /: Health check
    - POST /webhook/copy-trading: Webhook de Helius para copy trading
    - GET /stats: Estadisticas del engine y todas las estrategias
    """
    app = web.Application()

    async def health_handler(request: web.Request) -> web.Response:
        return web.Response(text="Bot running")

    async def copy_trade_webhook(request: web.Request) -> web.Response:
        # Buscar la estrategia de copy trading
        copy_strategy = None
        for name, strategy in engine.strategies.items():
            if isinstance(strategy, CopyTradingStrategy):
                copy_strategy = strategy
                break

        if copy_strategy is None:
            return web.json_response(
                {"error": "Copy trading not enabled"}, status=404
            )
        try:
            payload = await request.json()
            result = await copy_strategy.handle_webhook(payload)
            return web.json_response(result)
        except Exception as exc:
            logger.error("Error en webhook de copy trading: {}", exc)
            return web.json_response(
                {"status": "error", "message": str(exc)}, status=500
            )

    async def stats_handler(request: web.Request) -> web.Response:
        from core.stats import get_trade_stats
        engine_stats = engine.get_stats()
        trade_stats = get_trade_stats().summary()
        engine_stats["trade_stats"] = trade_stats
        return web.json_response(engine_stats)

    app.router.add_get("/", health_handler)
    app.router.add_post("/webhook/copy-trading", copy_trade_webhook)
    app.router.add_get("/stats", stats_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()


class TradingBot:
    """Bot de trading modular con motor de estrategias."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config

        # -- Componentes compartidos (inyectados en todas las estrategias)
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
        self.tracker = PositionTracker(
            executor=self.executor,
            notifier=self.notifier,
            config=config,
        )
        set_global_tracker(self.tracker)

        # -- Motor de estrategias
        self.engine = StrategyEngine()
        self._register_strategies()

    def _register_strategies(self) -> None:
        """Registra las estrategias habilitadas en el engine."""
        # 1. Memecoin Sniper - DESHABILITADO (muy riesgoso para pruebas)
        # Para activar: descomentar la siguiente linea
        # self.engine.register(MemecoinSniper(
        #     executor=self.executor,
        #     notifier=self.notifier,
        #     tracker=self.tracker,
        #     config=self.config,
        # ))

        # 2. Copy Trading (si esta habilitado)
        if self.config.copy_trading.COPY_TRADING_ENABLED:
            self.engine.register(CopyTradingStrategy(
                executor=self.executor,
                notifier=self.notifier,
                tracker=self.tracker,
                config=self.config,
            ))

        # 3. Arbitrage (si hay tokens configurados)
        arb_tokens = os.getenv("ARBITRAGE_TOKENS", "")
        if arb_tokens:
            self.engine.register(ArbitrageStrategy(
                executor=self.executor,
                notifier=self.notifier,
                tracker=self.tracker,
                config=self.config,
            ))

        # 4. DCA (si hay tokens configurados)
        dca_tokens = os.getenv("DCA_TOKENS", "")
        if dca_tokens:
            self.engine.register(DCAStrategy(
                executor=self.executor,
                notifier=self.notifier,
                tracker=self.tracker,
                config=self.config,
            ))

    async def run(self) -> None:
        """Inicia el bot con todas las estrategias."""
        logger.info("=== TradingBot Framework Modular ===")
        logger.info("Wallet: {}", self.executor.wallet_pubkey)
        logger.info("RPC: {}", self.executor.rpc_url)
        logger.info("Estrategias: {}", list(self.engine.strategies.keys()))

        # Configurar webhook de Helius para copy trading
        await self._setup_helius_webhook()

        # Lanzar servidor HTTP (health + webhooks + stats)
        await start_health_server(self.engine)

        # Iniciar todas las estrategias
        await self.engine.start_all()

        # Heartbeat periodico
        heartbeat_task = asyncio.create_task(
            self.notifier.start_heartbeat(interval_minutes=30.0)
        )

        # Telegram command listener (/stats, /wallets)
        cmd_task = asyncio.create_task(self._telegram_command_loop())

        # Monitor de posiciones
        monitor_task = asyncio.create_task(self.tracker.start_monitoring())

        try:
            # Mantener el bot vivo
            while True:
                await asyncio.sleep(60)
                stats = self.engine.get_stats()
                active = stats.get("active_strategies", 0)
                total = stats.get("total_strategies", 0)
                logger.debug(
                    "Engine: {} estrategias activas/{} | Uptime: {:.0f}s",
                    active, total, stats.get("uptime_seconds", 0),
                )
        except asyncio.CancelledError:
            pass
        finally:
            await self.engine.stop_all()
            heartbeat_task.cancel()
            cmd_task.cancel()
            monitor_task.cancel()
            await asyncio.gather(heartbeat_task, cmd_task, monitor_task, return_exceptions=True)

    async def _telegram_command_loop(self) -> None:
        """Polls Telegram for /stats and /wallets commands."""
        import aiohttp
        from core.stats import get_trade_stats

        cfg = self.config
        if not cfg.telegram.TELEGRAM_TOKEN or not cfg.telegram.TELEGRAM_CHAT_ID:
            return

        token = cfg.telegram.TELEGRAM_TOKEN
        chat_id = cfg.telegram.TELEGRAM_CHAT_ID
        offset = 0
        logger.info("Telegram command listener iniciado (/stats, /wallets)")

        while True:
            try:
                url = f"https://api.telegram.org/bot{token}/getUpdates?offset={offset}&timeout=5"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            await asyncio.sleep(5)
                            continue
                        data = await resp.json()

                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    text = msg.get("text", "").strip().lower()
                    from_chat = str(msg.get("chat", {}).get("id", ""))

                    if from_chat != chat_id:
                        continue

                    if text == "/stats":
                        stats = get_trade_stats()
                        # Usar posiciones REALES (tracker) en vez del contador
                        # acumulado positions_opened - positions_closed, que se
                        # desincroniza (cierre por TP/SL/time, reinicios DRY_RUN).
                        real_open = len(self.tracker.positions)
                        await self.notifier.send(stats.format_summary(open_positions=real_open))
                    elif text == "/wallets":
                        stats = get_trade_stats()
                        s = stats.summary()
                        if not s["wallets"]:
                            await self.notifier.send_status("No hay trades registrados aun.")
                        else:
                            lines = ["<b>Wallets monitoreadas:</b>\n"]
                            for w, ws in s["wallets"].items():
                                lines.append(
                                    f"• <code>{w[:12]}...</code>: "
                                    f"{ws['buys']} buys / {ws['sells']} sells | "
                                    f"WR {ws['win_rate']}% | PnL {ws['pnl_pct']:+.2f}% ({ws['net_pnl_sol']:+.4f} SOL)"
                                )
                            await self.notifier.send("\n".join(lines))

            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(10)

            await asyncio.sleep(3)

    async def _setup_helius_webhook(self) -> None:
        """Configura el webhook de Helius para copy trading."""
        copy_strategy = None
        for name, strategy in self.engine.strategies.items():
            if isinstance(strategy, CopyTradingStrategy):
                copy_strategy = strategy
                break

        if not copy_strategy or not self.config.copy_trading.AUTO_SETUP_WEBHOOK:
            return

        base_url = self.config.copy_trading.WEBHOOK_BASE_URL
        if not base_url:
            render_url = os.getenv("RENDER_EXTERNAL_URL", "")
            if render_url:
                base_url = render_url.rstrip("/")
            else:
                public_host = os.getenv("PUBLIC_HOST", "")
                public_port = os.getenv("PUBLIC_PORT", "443")
                if public_host:
                    scheme = "https" if public_port == "443" else "http"
                    base_url = f"{scheme}://{public_host}"
                    if public_port not in ("80", "443"):
                        base_url += f":{public_port}"

        if base_url:
            logger.info("Configurando Helius webhook...")
            wh_id = await copy_strategy.setup_helius_webhook(base_url)
            if wh_id:
                logger.success("Helius webhook listo: {}", wh_id)
            else:
                logger.warning(
                    "No se pudo crear webhook. Crea manualmente en "
                    "https://dashboard.helius.dev/webhooks"
                )
        else:
            logger.warning(
                "URL publica no detectada. Configura WEBHOOK_BASE_URL en .env"
            )

    async def shutdown(self) -> None:
        logger.info("Apagando bot...")
        await self.engine.stop_all()


async def main() -> None:
    config = load_config()
    bot = TradingBot(config)

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
        logger.info("Interrupcion del usuario.")
