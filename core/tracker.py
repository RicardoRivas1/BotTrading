"""Motor de seguimiento y salida de posiciones compradas.

Consulta la cotización actual de cada token vía Jupiter v6, calcula el PnL
porcentual desde el precio de entrada y dispara la venta (take-profit o
stop-loss de emergencia con slippage alto) notificando a Telegram. Cuando
DRY_RUN=False, la venta se firma localmente con la PRIVATE_KEY y se envía a
la red de Solana.

El tracker mantiene su propia memoria de posiciones activas (`add_position` /
`remove_position`) que alimenta el bucle `monitor_positions()` en segundo
plano. `get_global_tracker()` expone la instancia compartida para que los
flujos de compra (p. ej. `core/websocket.py`) registren posiciones y el
punto de entrada de la app la ponga a monitorear.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp
from loguru import logger

# Intervalo del bucle de monitoreo de posiciones activas (segundos).
CHECK_INTERVAL_SEC = 2.0

# Tiempo máximo que una posición puede permanecer activa antes de forzar la
# salida (TIME_EXPIRED), independientemente de su PnL. Configurable via .env.
MAX_HOLD_TIME_SEC = int(os.getenv("MAX_HOLD_TIME_SEC", "180"))

# API directa de Pump.fun para la cotización en SOL por token. Se usan
# cabeceras de navegador para evitar el bloqueo 403 de Cloudflare.
PUMPFUN_API = "https://frontend-api.pump.fun/coins/{mint}"
PUMPFUN_API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


async def get_pumpfun_price(mint: str) -> Optional[float]:
    """Precio real en SOL por token desde la bonding curve de Pump.fun.

    Consulta `https://frontend-api.pump.fun/coins/{mint}` y deriva la cotización
    de las reservas virtuales ajustando los decimales de Solana (SOL = 9
    decimales, token = 6):

        v_sol = virtual_sol_reserves  / 1e9
        v_tokens = virtual_token_reserves / 1e6
        price_in_sol = v_sol / v_tokens

    Devuelve None si el token no cotiza en Pump.fun, la API responde un estado
    no-200 o el cálculo no es posible (nunca lanza excepciones hacia el caller).
    """
    url = PUMPFUN_API.format(mint=mint)
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=PUMPFUN_API_HEADERS) as resp:
                if resp.status != 200:
                    logger.debug("Pump.fun respondió {} para {}", resp.status, mint)
                    return None
                data = await resp.json(content_type=None)

        v_sol = float(data["virtual_sol_reserves"]) / 1e9
        v_tokens = float(data["virtual_token_reserves"]) / 1e6
        if v_sol > 0 and v_tokens > 0:
            return v_sol / v_tokens
        logger.debug("Pump.fun sin reservas válidas para {}", mint)
        return None
    except (aiohttp.ClientError, KeyError, TypeError, ValueError) as exc:
        logger.debug("Pump.fun sin precio para {}: {}", mint, exc)
        return None


@dataclass
class TrackerPosition:
    """Posición activa registrada en la memoria del tracker."""

    mint: str
    symbol: str
    buy_price: float
    amount: float  # SOL invertidos en la compra.
    created_at: float = field(default_factory=time.time)
    last_log_time: float = field(default_factory=time.time)
    last_no_price_log: float = 0.0
    current_price: float = 0.0
    current_price_updated_at: float = 0.0
    latest_pnl_pct: float = 0.0
    highest_pnl_pct: float = 0.0
    max_hold_seconds: float = field(default_factory=lambda: float(MAX_HOLD_TIME_SEC))
    last_progress_notify_at: float = 0.0
    last_notified_pnl_pct: float = 0.0


class PositionTracker:
    """Supervisa las posiciones activas y ejecuta las salidas TP/SL."""

    # Slippage alto para ventas de emergencia en stop-loss (20% = 2000 bps).
    EMERGENCY_SLIPPAGE_BPS = 2000

    def __init__(self, executor: Any, notifier: Any, config: Any) -> None:
        self.executor = executor
        self.notifier = notifier
        self.config = config
        # Memoría global de posiciones activas (independiente del executor).
        self.positions: dict[str, TrackerPosition] = {}

    # ------------------------------------------------------------- Público
    def add_position(
        self,
        mint: str,
        symbol: str,
        buy_price: float,
        amount: float,
    ) -> None:
        """Registra una posición activa para que el monitor la vigile."""
        # Nunca guardar symbol "N/A": si no hay ticker, usar los primeros 6
        # caracteres del mint en mayúsculas (ej: "METVSV").
        if symbol and str(symbol).strip() and str(symbol).strip().upper() != "N/A":
            symbol = str(symbol).strip()
        else:
            symbol = str(mint)[:6].upper()

        self.positions[mint] = TrackerPosition(
            mint=mint,
            symbol=symbol,
            buy_price=buy_price,
            amount=amount,
        )

    def remove_position(self, mint: str) -> bool:
        """Elimina la posición; devuelve True si existía."""
        return self.positions.pop(mint, None) is not None

    def get_position(self, mint: str) -> Optional[TrackerPosition]:
        """Devuelve la posición registrada (o None)."""
        return self.positions.get(mint)

    async def start_monitoring(self) -> None:
        """Bucle de monitoreo en segundo plano; lánzalo con `asyncio.create_task`."""
        await self.monitor_positions()

    async def monitor_positions(self) -> None:
        """Revisa todas las posiciones activas cada CHECK_INTERVAL_SEC.

        Para cada posición obtiene el precio actual, calcula el PnL% y dispara
        la venta cuando se alcanza TAKE_PROFIT_PCT o STOP_LOSS_PCT, removiendo
        la posición del tracker tras ejecutar la salida.
        """
        logger.info("Monitoreo de posiciones iniciado (cada {:.0f}s)", CHECK_INTERVAL_SEC)
        try:
            while True:
                await asyncio.sleep(CHECK_INTERVAL_SEC)
                for mint in list(self.positions.keys()):
                    try:
                        await self._evaluate(mint)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001 - nunca detener el monitor
                        logger.warning("Error monitoreando {}: {}", mint, exc)
        except asyncio.CancelledError:
            logger.info("Monitoreo de posiciones detenido.")
            raise

    # ------------------------------------------------------- Evaluación
    async def _evaluate(self, mint: str) -> None:
        """Consulta precio y dispara TP/SL / TIME_EXPIRED para una posición."""
        from core.websocket import process_sell_and_notify

        pos = self.positions.get(mint)
        if pos is None:
            return

        now = time.time()

        try:
            current_price = await self._refresh_position_price(pos)
        except Exception as exc:  # noqa: BLE001
            current_price = 0.0
            logger.warning("No se pudo obtener precio de {} ({}): {}", mint, pos.symbol, exc)

        if current_price is not None and current_price > 0:
            pos.current_price = current_price
            pos.current_price_updated_at = now

        if current_price is None or current_price <= 0:
            # Sin ningún precio real conocido: esperar al siguiente ciclo sin
            # calcular PnL ni ejecutar salidas.
            return

        # --- Asignación dinámica del precio de entrada (BASE) ---
        # Si el entry sigue PENDIENTE (None, 0.0 o el sentinel 1.0 del default
        # antiguo), el PRIMER precio válido (>0) obtenido en el bucle se fija
        # como base de entrada y se salta la evaluación de PnL en este ciclo.
        entry_pending = (
            pos.buy_price is None
            or pos.buy_price <= 0
            or abs(pos.buy_price - 1.0) < 1e-9
        )
        if entry_pending:
            pos.buy_price = current_price
            logger.info(f"🎯 Precio de entrada BASE fijado para {pos.symbol}: {current_price} SOL")
            return

        # PnL en las mismas unidades (SOL por Token). Si None retorna 0 sobre
        # señales de refresco, _refresh_position_price conserva la última
        # cotización válida conocida (jamás se iguala al precio de entrada).
        pnl_pct = (current_price - pos.buy_price) / pos.buy_price * 100.0
        pos.latest_pnl_pct = pnl_pct
        if pnl_pct > pos.highest_pnl_pct:
            pos.highest_pnl_pct = pnl_pct
        take_profit_pct = float(self.config.trading.TAKE_PROFIT_PCT)
        stop_loss_pct = float(self.config.trading.STOP_LOSS_PCT)

        # --- Log periódico cada 15-30 segundos ---
        if now - pos.last_log_time >= 20:
            logger.info(
                "📊 Monitoreando {}: PnL actual {:+.2f}% (Precio: {:.10g} SOL)",
                pos.symbol, pnl_pct, current_price,
            )
            pos.last_log_time = now

        # --- TP / SL primero (incluso en DRY_RUN) para salir antes de agotar
        # el hold máximo ---
        if take_profit_pct and pnl_pct >= take_profit_pct:
            logger.info("🎯 TAKE PROFIT (+{:.2f}%) para {} ({})", pnl_pct, mint, pos.symbol)
            ok = await process_sell_and_notify(
                pos.mint, pos.symbol, reason="TAKE_PROFIT", pnl=pnl_pct
            )
            if ok:
                self.remove_position(mint)
            return
        if stop_loss_pct and pnl_pct <= -stop_loss_pct:
            logger.info("🛑 STOP LOSS ({:.2f}%) para {} ({})", pnl_pct, mint, pos.symbol)
            ok = await process_sell_and_notify(
                pos.mint, pos.symbol, reason="STOP_LOSS", pnl=pnl_pct
            )
            if ok:
                self.remove_position(mint)
            return

        # --- TIME_EXPIRED: cierre forzado si se superó el hold máximo ---
        if now - pos.created_at > MAX_HOLD_TIME_SEC:
            logger.info(
                "⏳ TIME EXPIRED para {} ({}) tras {:.0f}s (PnL {:+.2f}%)",
                mint, pos.symbol, MAX_HOLD_TIME_SEC, pnl_pct,
            )
            ok = await process_sell_and_notify(
                pos.mint, pos.symbol, reason="TIME_EXPIRED", pnl=pnl_pct
            )
            if ok:
                self.remove_position(mint)
            return

        # Progreso: si la posición sigue abierta (no se vendió por TP/SL o
        # TIME_EXPIRED), se reporta su estado periódicamente o ante saltos de
        # PnL ≥ ±2%.
        await self._maybe_notify_progress(pos, pnl_pct)

    async def _get_current_price(self, mint: str) -> float:
        """Precio real en cascada: Pump.fun → Jupiter/DexScreener (executor).

        El precio siempre está en las mismas unidades que el de entrada
        (SOL por Token). Orden de fuentes:

        1. `get_pumpfun_price(mint)`: bonding curve de Pump.fun directamente.
        2. `executor.get_token_price(mint)`: Jupiter v6 → DexScreener → Pump.fun.

        Si ninguna fuente entrega precio válido devuelve 0.0 (el caller decide
        conservar la última cotización conocida) y registra un `warning`
        aplacado (máx. 1 cada 30s) con la causa del fallo.
        """
        pos = self.positions.get(mint)
        cause: Optional[str] = None

        try:
            price = await get_pumpfun_price(mint)
            if price is not None and price > 0:
                return price
            cause = "Pump.fun sin cotización"
        except Exception as exc:  # noqa: BLE001
            cause = f"Pump.fun: {exc}"

        try:
            price = await self.executor.get_token_price(mint)
            if price is not None and price > 0:
                return price
            cause = f"{cause or 'executor'}: sin cotización"
        except Exception as exc:  # noqa: BLE001
            cause = f"{cause or 'executor'}: {exc}"

        if pos is not None:
            now = time.time()
            if now - pos.last_no_price_log >= 30:
                logger.warning(
                    "Sin precio real para {} ({}): {}",
                    mint, pos.symbol, cause,
                )
                pos.last_no_price_log = now
        return 0.0

    async def _refresh_position_price(self, pos: TrackerPosition) -> float:
        """Precio real actualizado, con fallback en cascada y último precio válido.

        Si existe una cotización fresca reciente (menos de
        PRICE_POLL_FALLBACK_SECONDS) se reutiliza sin golpear la red. En caso
        contrario consulta la cascada Pump.fun → Jupiter/DexScreener. Si ninguna
        fuente devuelve precio:

        - Si ya había una cotización conocida previa, la conserva (NUNCA la
          sustituye por `buy_price`) y avisa con un `warning`.
        - Si no hay ningún precio conocido, devuelve 0.0.
        """
        now = time.time()
        fallback_gap = float(
            getattr(self.config.trading, "PRICE_POLL_FALLBACK_SECONDS", 5.0)
        )
        if pos.current_price_updated_at:
            fresh = pos.current_price > 0 and (now - pos.current_price_updated_at) < fallback_gap
            if fresh:
                return pos.current_price

        try:
            price = await self._get_current_price(pos.mint)
        except Exception as exc:  # noqa: BLE001
            price = 0.0
            logger.warning("No se pudo obtener precio de {} ({}): {}", pos.mint, pos.symbol, exc)

        if price is not None and price > 0:
            pos.current_price = price
            pos.current_price_updated_at = now
            return price

        # Fallback total: conservar la última cotización válida conocida. Jamás
        # se iguala current_price con el precio de entrada para no enmascarar
        # un PnL congelado en 0.00%.
        if pos.current_price > 0:
            logger.warning(
                "Sin precio real para {} ({}); conservando última cotización {:.10g} SOL",
                pos.mint, pos.symbol, pos.current_price,
            )
            return pos.current_price
        return 0.0

    async def _maybe_notify_progress(self, pos: TrackerPosition, pnl_pct: float) -> None:
        """Notifica el progreso de la posición de forma periódica o por salto de PnL.

        Envía `notify_position_progress` cuando ha transcurrido el intervalo
        POSITION_UPDATE_INTERVAL_SECONDS (default 30s) o cuando el PnL varía
        ≥ ±2 puntos porcentuales respecto de la última notificación.
        """
        now = time.time()
        interval = float(
            getattr(self.config.trading, "POSITION_UPDATE_INTERVAL_SECONDS", 30.0)
        )
        if interval <= 0:
            return
        period_expired = (now - pos.last_progress_notify_at) >= interval
        big_jump = abs(pnl_pct - pos.last_notified_pnl_pct) >= 2.0
        if not (period_expired or big_jump):
            return
        result = self.notifier.notify_position_progress(
            pos,
            take_profit_pct=float(
                getattr(self.config.trading, "TAKE_PROFIT_PCT", 0.0) or 0.0
            ),
            stop_loss_pct=float(
                getattr(self.config.trading, "STOP_LOSS_PCT", 0.0) or 0.0
            ),
        )
        if inspect.isawaitable(result):
            await result
        pos.last_progress_notify_at = now
        pos.last_notified_pnl_pct = pnl_pct
        logger.info(
            "📣 Progreso de {}: PnL {:+.2f}% (máx {:+.2f}%)",
            pos.symbol, pnl_pct, pos.highest_pnl_pct,
        )

    async def check_position(self, token_mint: str) -> tuple[str, float]:
        """Evalúa una posición y ejecuta la salida si corresponde.

        Obtiene la cotización actual vía Jupiter v6 y calcula la variación
        porcentual desde el precio de entrada:

        - PnL >= TAKE_PROFIT_PCT: venta inmediata y notifica
          "🎯 TAKE PROFIT (+X%)".
        - PnL <= -STOP_LOSS_PCT: venta de emergencia con slippage alto y
          notifica "🛑 STOP LOSS (-X%)".

        Returns:
            Una tupla (motivo, pnl_pct) donde `motivo` es "TAKE_PROFIT",
            "STOP_LOSS" o "" (sin salida).
        """
        position = self.executor.positions.get(token_mint)
        if position is None:
            return "", 0.0

        try:
            current_price = await self.executor.get_token_price(token_mint)
        except Exception as exc:  # noqa: BLE001 - fallo de red no bloqueante
            logger.warning("No se pudo consultar precio de {}: {}", token_mint, exc)
            return "", 0.0

        # Si falta un precio de entrada válido, se re-consulta el precio base
        # para no ignorar la posición ni provocar una división por cero.
        if not position.entry_price or position.entry_price <= 0:
            try:
                position.entry_price = await self.executor.get_token_price(token_mint)
            except Exception as exc:  # noqa: BLE001 - fallo de red no bloqueante
                logger.warning("No se pudo re-consultar precio de entrada de {}: {}", token_mint, exc)
                return "", 0.0
            logger.info("Precio de entrada re-establecido para {}: {:.10g}", token_mint, position.entry_price)

        pnl_pct = (
            (current_price - position.entry_price) / position.entry_price * 100
        ) if position.entry_price else 0.0

        take_profit_pct = float(self.config.trading.TAKE_PROFIT_PCT)
        stop_loss_pct = float(self.config.trading.STOP_LOSS_PCT)

        if take_profit_pct and pnl_pct >= take_profit_pct:
            logger.info("🎯 TAKE PROFIT (+{:.2f}%) para {}", pnl_pct, token_mint)
            await self._close(token_mint, "TAKE_PROFIT", pnl_pct)
            await self.notifier.send_take_profit(token_mint, pnl_pct)
            return "TAKE_PROFIT", pnl_pct

        if stop_loss_pct and pnl_pct <= -stop_loss_pct:
            logger.info("🛑 STOP LOSS (-{:.2f}%) para {}", abs(pnl_pct), token_mint)
            await self._close(
                token_mint, "STOP_LOSS", pnl_pct,
                slippage_bps=self.EMERGENCY_SLIPPAGE_BPS,
            )
            await self.notifier.send_stop_loss(token_mint, pnl_pct)
            return "STOP_LOSS", pnl_pct

        return "", 0.0

    async def _close(
        self,
        token_mint: str,
        reason: str,
        pnl_pct: float,
        slippage_bps: Optional[int] = None,
    ) -> None:
        """Delega la venta (real o simulada) en el executor."""
        await self.executor.close_position(
            token_mint,
            reason,
            pnl_pct,
            slippage_bps=slippage_bps,
        )


# -- Memoria global compartida ------------------------------------------------
# Instancia única del tracker usada por los flujos de compra (websocket) y el
# punto de entrada (bot.py). bot.py puede inyectar la suya con set_global_tracker.
_TRACKER: Optional[PositionTracker] = None


def set_global_tracker(tracker: PositionTracker) -> None:
    """Establece la instancia compartida del tracker (llamado por bot.py)."""
    global _TRACKER
    _TRACKER = tracker


def get_global_tracker() -> PositionTracker:
    """Devuelve el tracker global, construyéndolo bajo demanda si es necesario."""
    global _TRACKER
    if _TRACKER is None:
        from config import load_config
        from core.execution import JupiterExecutor
        from core.notifier import TelegramNotifier

        cfg = load_config()
        _TRACKER = PositionTracker(
            executor=JupiterExecutor(
                private_key=cfg.solana.PRIVATE_KEY,
                rpc_url=cfg.solana.HELIUS_RPC_URL,
                slippage_bps=cfg.trading.SLIPPAGE_BPS,
                buy_amount_sol=cfg.trading.BUY_AMOUNT_SOL,
                take_profit_pct=cfg.trading.TAKE_PROFIT_PCT,
                stop_loss_pct=cfg.trading.STOP_LOSS_PCT,
                trailing_activation_pct=cfg.trading.TRAILING_STOP_ACTIVATION_PCT,
                trailing_distance_pct=cfg.trading.TRAILING_STOP_DISTANCE_PCT,
                dry_run=cfg.trading.DRY_RUN,
            ),
            notifier=TelegramNotifier(
                token=cfg.telegram.TELEGRAM_TOKEN,
                chat_id=cfg.telegram.TELEGRAM_CHAT_ID,
            ),
            config=cfg,
        )
    return _TRACKER