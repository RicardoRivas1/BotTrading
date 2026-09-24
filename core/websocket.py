"""Escuchador persistente de eventos de nuevos tokens.

Conecta vía WebSocket a la API de PumpPortal (o endpoint de streaming de
Helius) y emite cada nuevo par/token detectado. Implementa reconexión
automática con backoff exponencial para soportar cortes de red prolongados
sin detener el bucle de eventos de asyncio.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any, AsyncIterator, Optional

import aiohttp
from loguru import logger

import config

# -- Constantes ---------------------------------------------------------------
# Endpoint público de PumpPortal para escuchar tokens/liquidez nuevos.
PUMP_PORTAL_WS = "wss://pumpportal.fun/api/data"

# Heartbeat: ping cada X segundos si el servidor no envía datos. Mantiene la
# tubería abierta a través de proxies (Cloudflare/Render) que cierran
# conexiones consideradas ociosas.
_HEARTBEAT_SECONDS = 15.0

# Timeout de recepción: si no llega ningún token en este tiempo, se considera
# el feed inactivo y se fuerza la reconexión (watchdog).
_RECEIVE_TIMEOUT_SECONDS = 30.0

# Reintento fijo para errores 502/503 (proxy/Cloudflare): espera prudente para
# no saturar la red y evitar que el upstream nos banea por reintentos en ráfaga.
_ERROR_502_RETRY_SECONDS = 10.0

# Espera antes de reintentar la suscripción cuando el servidor responde con un
# mensaje de error (clave 'errors') dentro de una conexión ya establecida.
_ERROR_RESUBSCRIBE_SECONDS = 5.0

# Payload oficial de suscripción al feed de nuevos tokens de PumpPortal.
# Solo se usa la clave soportada 'method' (sin 'op' ni 'action').
_SUBSCRIBE_PAYLOAD = {"method": "subscribeNewToken"}

# Liquidez mínima en SOL para aceptar un token en Pump.fun bonding curve.
# Tokens con menos de esto son demasiado ilíquidos para tradeo rentable.
_MIN_LIQUIDITY_SOL = float(os.getenv("MIN_LIQUIDITY_SOL", "0.5"))

# User-Agent de navegador para eludir bloqueos básicos de Cloudflare.
_USER_AGENT_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# Backoff exponencial de reconexión (segundos).
_RECONNECT_MIN = 1.0
_RECONNECT_MAX = 60.0


async def check_rugcheck(mint: str) -> int:
    """Consulta el score de riesgo de RugCheck para un mint."""
    from core.security import TokenSecurityValidator

    cfg = config.load_config()
    validator = TokenSecurityValidator(
        rpc_url=cfg.solana.HELIUS_RPC_URL,
        security=cfg.security,
    )
    async with aiohttp.ClientSession() as session:
        report = await validator._fetch_rugcheck(session, mint)
    return validator._rugcheck_score(report)


def resolve_symbol(symbol: Any, mint: Any = None) -> str:
    """Devuelve un ticker/símbolo válido y nunca "N/A".

    Si `symbol` llega vacío, "N/A" o nulo en la señal, deriva un ticker de
    respaldo desde el mint (primeros 6 caracteres en mayúsculas, ej: "METVSV").
    La búsqueda por API (DexScreener/Pump.fun -> baseToken.symbol) la realiza
    `JupiterExecutor.get_token_symbol` en el flujo de compra.
    """
    if symbol and str(symbol).strip() and str(symbol).strip().upper() != "N/A":
        return str(symbol).strip()
    if mint:
        return str(mint)[:6].upper()
    return "N/A"


async def check_liquidity(mint: str) -> bool:
    """Verifica que el token tenga liquidez mínima en la bonding curve.

    Para tokens de Pump.fun (sufijo 'pump') consulta las reservas virtuales
    de SOL. Para otros tokens consulta DexScreener. Devuelve False si la
    liquidez es insuficiente o la API no responde.
    """
    if not str(mint).lower().endswith("pump"):
        return True

    try:
        url = f"https://frontend-api.pump.fun/coins/{mint}"
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.debug("Pump.fun no respondió para liquidez de {}", mint)
                    return True
                data = await resp.json()

        virtual_sol = data.get("virtual_sol_reserves")
        if virtual_sol:
            sol_liquidity = float(virtual_sol) / 1e9
            if sol_liquidity < _MIN_LIQUIDITY_SOL:
                logger.info(
                    "💧 Liquidez insuficiente para {}: {:.4f} SOL (mínimo: {:.4f} SOL)",
                    mint, sol_liquidity, _MIN_LIQUIDITY_SOL,
                )
                return False
            logger.debug("Liquidez de {}: {:.4f} SOL ✓", mint, sol_liquidity)
    except Exception as exc:
        logger.debug("No se pudo verificar liquidez de {}: {}", mint, exc)

    return True


async def process_buy_and_notify(
    mint: str, symbol: str = "N/A", score: Optional[float] = None
) -> None:
    """Ejecuta una compra (real o simulada) y notifica a Telegram.

    Reutiliza el executor global del tracker en lugar de crear uno nuevo
    cada vez, lo que evita escaneos BIP44 redundantes y mantiene el estado
    de posiciones centralizado.
    """
    from core.notifier import TelegramNotifier
    from core.tracker import get_global_tracker

    cfg = config.load_config()
    tracker = get_global_tracker()
    executor = tracker.executor

    # Nunca registrar ni notificar symbol "N/A": si el ticker no viene en la
    # señal, se consulta DexScreener/Pump.fun (baseToken.symbol -> ej: "MET")
    # y, si falla la API, se usan los primeros 6 caracteres del mint en
    # mayúsculas (ej: "METVSV").
    if symbol and str(symbol).strip() and str(symbol).strip().upper() != "N/A":
        symbol = str(symbol).strip()
    else:
        try:
            symbol = await executor.get_token_symbol(mint)
        except Exception as exc:  # noqa: BLE001 - fallo de red no bloqueante
            logger.warning(
                f"No se pudo obtener ticker de {mint} por API: {exc}; usando fallback del mint."
            )
            symbol = str(mint)[:6].upper()

    notifier = TelegramNotifier(
        token=cfg.telegram.TELEGRAM_TOKEN,
        chat_id=cfg.telegram.TELEGRAM_CHAT_ID,
    )
    # Etiqueta según el estado real de ejecución.
    if os.getenv("FORCE_TEST_BUY", "False").lower() == "true":
        label = "TEST FORZADO"
    elif cfg.trading.DRY_RUN:
        label = "SIMULACIÓN DRY_RUN"
    else:
        label = "COMPRA REAL"
    try:
        sig = await executor.buy_token(mint, dry_run=cfg.trading.DRY_RUN)
    except Exception as exc:  # noqa: BLE001 - fallo operativo no bloqueante
        logger.error(f"Error comprando {mint} ({label}): {exc}")
        await notifier.send_error(f"No se pudo comprar {mint} ({label}): {exc}")
        return

    if sig is None:
        logger.info(f"Compra de {mint} ({label}) omitida (sin liquidez).")
        return

    # Captura del precio de entrada real (positivo) desde la posición abierta.
    entry_price = 0.0
    position = executor.positions.get(mint)
    if position and position.entry_price and position.entry_price > 0:
        entry_price = position.entry_price
    else:
        try:
            entry_price = await executor.get_token_price(mint)
        except Exception as exc:  # noqa: BLE001 - fallo de red no bloqueante
            logger.warning(f"No se pudo consultar precio de entrada de {mint}: {exc}")

    if not entry_price or entry_price <= 0:
        logger.warning(
            f"📌 Precio de entrada PENDIENTE para {symbol} ({mint}); "
            f"el tracker fijará el entry base real."
        )

    await notifier.send_buy(
        mint,
        cfg.trading.BUY_AMOUNT_SOL,
        symbol=symbol,
        score=score,
        price=entry_price,
        dry_run=cfg.trading.DRY_RUN,
    )

    tracker.add_position(
        mint=mint,
        symbol=symbol,
        buy_price=entry_price,
        amount=cfg.trading.BUY_AMOUNT_SOL,
    )
    logger.info(f"📌 Posición registrada en tracker para {symbol} ({mint})")

    logger.success(
        f"Compra de {label} ({mint}) ejecutada: {sig} @ entry={entry_price:.10g} SOL"
    )


async def process_sell_and_notify(
    mint: str,
    symbol: str = "N/A",
    reason: str = "",
    pnl: float = 0.0,
    pnl_copy: Optional[float] = None,
    sell_pct: float = 100.0,
    trader: str = "",
) -> bool:
    """Vende (real o simulado) y notifica el motivo de la salida TP/SL.

    sell_pct: porcentaje a vender (100 = todo, 25 = cuarta parte, etc.)
    En DRY_RUN solo registra la venta simulada. En modo real, el balance de
    tokens se estima desde la posición del tracker (`amount / buy_price`) y la
    cantidad se ajusta por sell_pct. Devuelve True si la salida se ejecutó.
    """
    from core.notifier import TelegramNotifier
    from core.tracker import get_global_tracker

    cfg = config.load_config()
    notifier = TelegramNotifier(
        token=cfg.telegram.TELEGRAM_TOKEN,
        chat_id=cfg.telegram.TELEGRAM_CHAT_ID,
    )
    tracker = get_global_tracker()
    pos = tracker.get_position(mint)

    sell_pct = max(0.0, min(100.0, sell_pct))

    # Red de seguridad central: ningún PnL absurdo (cotización dust / entrada mal
    # calculada) debe colarse a las stats ni a las notificaciones.
    from core.stats import MAX_PLAUSIBLE_PNL_PCT
    raw_pnl = float(pnl)
    pnl = max(-100.0, min(raw_pnl, MAX_PLAUSIBLE_PNL_PCT))

    # En DRY_RUN las salidas automáticas del tracker (TAKE_PROFIT / STOP_LOSS /
    # TRAILING_STOP / TIME_EXPIRED) nacen de cotizaciones simuladas, a menudo
    # oscilantes (+59% y luego -13% para el mismo token) o dust, y no representan
    # un resultado real. No deben spamear Telegram ni contaminar las stats de
    # /stats. Las salidas por COPY_TRADE_SELL (el trader vendió de verdad) sí se
    # notifican y se registran por la estrategia de copy trading.
    quiet_dry_run_exit = (
        bool(cfg.trading.DRY_RUN)
        and reason in ("TAKE_PROFIT", "STOP_LOSS", "TRAILING_STOP", "TIME_EXPIRED")
    )

    # Registrar venta en estadísticas para salidas del tracker (TP/SL/TRAILING_STOP/etc.)
    if reason != "COPY_TRADE_SELL" and pos and not quiet_dry_run_exit:
        try:
            from core.stats import get_trade_stats
            _entry = getattr(pos, "buy_price", 0.0) or 0.0
            _sol_inv = getattr(pos, "amount", 0.0) or 0.0
            _portion_inv = _sol_inv * (sell_pct / 100.0)
            _pnl_pct = max(-100.0, float(pnl))
            _sol_rec = max(0.0, _portion_inv * (1.0 + _pnl_pct / 100.0))
            _exit_price = _entry * (1.0 + _pnl_pct / 100.0) if _entry > 0 else 0.0
            _wallet = getattr(pos, "source_wallet", "") or "tracker"
            _buy_time = float(getattr(pos, "created_at", 0.0) or 0.0)
            get_trade_stats().record_sell(
                mint=mint,
                symbol=symbol if symbol != "N/A" else getattr(pos, "symbol", mint[:6].upper()),
                wallet=_wallet,
                entry_price=_entry,
                exit_price=_exit_price,
                pnl_pct=_pnl_pct,
                sol_invested=_portion_inv,
                sol_received=_sol_rec,
                buy_time=_buy_time,
                sell_time=time.time(),
                sell_reason=reason,
                sell_pct=sell_pct,
            )
        except Exception as st_exc:
            logger.debug("Error registrando venta en stats para {}: {}", mint, st_exc)

    try:
        if cfg.trading.DRY_RUN:
            logger.info(
                "[DRY_RUN] Venta simulada de {} ({}) por {} (PnL {:.2f}%, pct={:.0f}%)",
                symbol, mint, reason, pnl, sell_pct,
            )
            # If full sell, clean up positions and persist
            if sell_pct >= 99.0:
                tracker.executor.positions.pop(mint, None)
                tracker.positions.pop(mint, None)
                tracker.executor._save_exec_positions()
                tracker._save_positions()
            elif pos:
                pos.amount = max(0.0, getattr(pos, "amount", 0.0) * (1.0 - sell_pct / 100.0))
                tracker._save_positions()
        else:
            token_amount = (pos.amount / pos.buy_price) if pos and pos.buy_price else 0.0
            if token_amount <= 0:
                logger.warning(f"Sin balance estimado para vender {symbol} ({mint}); omitiendo.")
                return False
            # Adjust by sell percentage
            token_amount = token_amount * (sell_pct / 100.0)
            if token_amount <= 0:
                logger.warning(f"sell_pct={sell_pct:.0f}% resulta en 0 tokens para {symbol}; omitiendo.")
                return False
            await tracker.executor.sell_token(mint, token_amount)
            logger.success(
                "Venta de {} ({}) ejecutada por {} ({:.0f}% | PnL {:.2f}%)",
                symbol, mint, reason, sell_pct, pnl,
            )
            # If full sell, clean up positions and persist
            if sell_pct >= 99.0:
                tracker.executor.positions.pop(mint, None)
                tracker.positions.pop(mint, None)
                tracker.executor._save_exec_positions()
                tracker._save_positions()
            elif pos:
                pos.amount = max(0.0, getattr(pos, "amount", 0.0) * (1.0 - sell_pct / 100.0))
                tracker._save_positions()
    except Exception as exc:  # noqa: BLE001 - fallo operativo no bloqueante
        logger.error(f"Error vendiendo {symbol} ({reason}): {exc}")
        await notifier.send_error(f"No se pudo vender {symbol} ({mint}) por {reason}: {exc}")
        return False

    if quiet_dry_run_exit:
        logger.info(
            "[DRY_RUN] Salida {} silenciada (sin stats ni notificación): {} ({}) PnL {:.2f}%",
            reason, symbol, mint, pnl,
        )
    elif reason == "TAKE_PROFIT":
        if raw_pnl > MAX_PLAUSIBLE_PNL_PCT:
            logger.warning(
                "TAKE PROFIT con PnL +{:.2f}% implausible para {} ({}) no se notifica",
                raw_pnl, symbol, mint,
            )
        else:
            await notifier.send_take_profit(mint, pnl)
    elif reason == "STOP_LOSS":
        await notifier.send_stop_loss(mint, pnl)
    elif reason == "TRAILING_STOP":
        await notifier.send_trailing_stop(mint, pnl)
    else:
        copy_txt = ""
        if pnl_copy is not None:
            copy_txt = f" | Copia {pnl_copy:+.2f}%"
        trader_txt = f" | Trader: {trader}" if trader else ""
        await notifier.send_status(
            f"Venta de {symbol} ({mint}) por {reason} "
            f"({sell_pct:.0f}% | PnL {pnl:.2f}%){copy_txt}{trader_txt}"
        )
    return True


class TokenWebSocket:
    """Listener asíncrono de nuevos tokens emitidos en Solana.

    Attributes:
        retry_delay: Retardo actual para la próxima reconexión.
        running: Flag que detiene el listener al ponerse en False.
    """

    def __init__(self, uri: str = PUMP_PORTAL_WS) -> None:
        self.uri = uri
        self.retry_delay: float = _RECONNECT_MIN
        self.running: bool = True
        # Cola no bloqueante: el consumidor recibe los eventos desde aquí.
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    async def _connect_and_listen(self) -> None:
        """Mantiene una conexión WS y encola mensajes JSON entrantes.

        Cualquier excepción (timeout, desconexión, error del servidor) se
        captura y provoca un retorno para lanzar una reconexión. Nunca se
        propaga al llamador, por lo que el bot nunca se detiene por la red.
        """
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                self.uri,
                # Keep-alive: pings cada 15s para mantener la tubería abierta
                # de forma activa en el proxy de Render/Cloudflare.
                heartbeat=_HEARTBEAT_SECONDS,
                # Si en 30s no llega ningún dato, aiohttp fuerza TimeoutError y
                # gatea la reconexión sin esperar al watchdog.
                receive_timeout=_RECEIVE_TIMEOUT_SECONDS,
                # User-Agent de navegador para evitar bloqueos básicos.
                headers=_USER_AGENT_HEADERS,
                ssl=False,
                max_msg_size=8 * 1024 * 1024,
            ) as ws:
                logger.info(f"Conectado al WebSocket: {self.uri}")
                self.retry_delay = _RECONNECT_MIN

                # Suscripción a eventos de creación de nuevos tokens (payload oficial).
                await ws.send_json(_SUBSCRIBE_PAYLOAD)
                logger.info("Suscrito exitosamente al feed de nuevos tokens (subscribeNewToken).")

                # Watchdog: si no llega ningún token en _RECEIVE_TIMEOUT_SECONDS,
                # la conexión se considera inactiva y se fuerza la reconexión.
                last_token_at: float = asyncio.get_event_loop().time()

                async for raw in ws:
                    if not self.running:
                        break
                    # Log de depuración: muestra CUALQUIER paquete recibido para
                    # confirmar que el feed sigue fluyendo en tiempo real (msg.data[:100]).
                    logger.info(f"📩 Evento raw recibido: {raw.data[:100]}")
                    try:
                        payload: dict[str, Any] = raw.json()
                    except (TypeError, ValueError):
                        logger.warning("Mensaje JSON inválido recibido, ignorando.")
                        continue

                    # Control de errores en respuestas raw: si el servidor responde
                    # con la clave 'errors' (p. ej. {"errors": "..."}), lo registramos
                    # y reintentamos la suscripción tras _ERROR_RESUBSCRIBE_SECONDS.
                    if "errors" in payload:
                        logger.error(f"⚠️ Error del WebSocket de PumpPortal: {payload.get('errors')}")
                        logger.warning(
                            f"Reintentando suscripción en {_ERROR_RESUBSCRIBE_SECONDS:.0f}s..."
                        )
                        await asyncio.sleep(_ERROR_RESUBSCRIBE_SECONDS)
                        await ws.send_json(_SUBSCRIBE_PAYLOAD)
                        logger.info("Suscrito nuevamente al feed de nuevos tokens (subscribeNewToken).")
                        last_token_at = asyncio.get_event_loop().time()
                        continue

                    mint = payload.get("mint") or payload.get("token", {}).get("mint")
                    raw_symbol = payload.get("symbol") or payload.get("token", {}).get("symbol", "N/A")
                    symbol = resolve_symbol(raw_symbol, mint)
                    if mint:
                        logger.info(f"🔎 Analizando mint: {mint} | Ticker: {symbol}")

                        # FORCE_TEST_BUY: disparo único de compra de prueba (modo diagnóstico).
                        if os.getenv("FORCE_TEST_BUY", "False").lower() == "true":
                            logger.info(f"🚀 [FORCE_TEST_BUY ACTIVADO] Forzando compra de prueba para {mint} ({symbol})")
                            if not await check_liquidity(mint):
                                logger.info(f"Omitiendo {mint} por liquidez insuficiente.")
                                os.environ["FORCE_TEST_BUY"] = "False"
                                continue
                            try:
                                await process_buy_and_notify(mint, symbol)
                            except Exception as exc:  # noqa: BLE001 - nunca colgar el listener
                                logger.error(f"❌ Error al forzar compra de prueba para {mint}: {exc}")
                            os.environ["FORCE_TEST_BUY"] = "False"
                            continue

                        # Evaluación de seguridad: consulta a RugCheck con captura de errores.
                        try:
                            score = await check_rugcheck(mint)
                            max_score = float(os.getenv("RUGCHECK_MAX_SCORE", "10000"))
                            logger.info(f"📊 Score RugCheck para {mint}: {score}")
                            if score == 0:
                                logger.warning(
                                    f"⚠️ Token con Score 0 (sin analizar en RugCheck) para {mint}. Omitiendo por seguridad..."
                                )
                            elif score <= max_score:
                                if not await check_liquidity(mint):
                                    continue
                                logger.info(
                                    f"✅ Token APROBADO por RugCheck (Score: {score} <= {max_score}). Ejecutando compra..."
                                )
                                await process_buy_and_notify(mint, symbol, score=score)
                            else:
                                logger.info(
                                    f"❌ Token RECHAZADO por RugCheck (Score: {score} > {max_score})"
                                )
                        except Exception as e:
                            logger.error(f"❌ Error al evaluar RugCheck para {mint}: {e}")
                    if payload.get("type") in ("tokenCreation", "create"):
                        # Información válida de token: se encola para que el bot
                        # continúe con la evaluación de RugCheck y filtros de seguridad.
                        # Log en tiempo real para confirmar la recepción de cada token.
                        logger.info(f"📥 Token detectado: {mint}")
                        last_token_at = asyncio.get_event_loop().time()
                        # No bloquea: la cola es interna e ilimitada.
                        self._queue.put_nowait(payload)

                    # Watchdog: si han pasado más de _RECEIVE_TIMEOUT_SECONDS desde
                    # el último token, cerrar la conexión para forzar una reconexión
                    # y re-suscripción.
                    if asyncio.get_event_loop().time() - last_token_at >= _RECEIVE_TIMEOUT_SECONDS:
                        logger.warning(
                            f"⚠️ WebSocket inactivo por {_RECEIVE_TIMEOUT_SECONDS:.0f}s. Reconectando..."
                        )
                        break

    async def run(self) -> None:
        """Bucle principal con reconexión automática infinita."""
        while self.running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                logger.info("Listener cancelado.")
                break
            except aiohttp.WSServerHandshakeError as exc:
                # Handshakes HTTP erróneos (típicos 502/503 de proxies/Cloudflare
                # tras un deploy en Render). Reintentar en ráfaga solo empeora el
                # bloqueo del upstream, así que esperamos 10s fijos.
                logger.error(f"Handshake HTTP {exc.status} fallido: {exc}")
                if exc.status in (502, 503):
                    self.retry_delay = _ERROR_502_RETRY_SECONDS
                    logger.warning(
                        f"⚠️ HTTP {exc.status} detectado: reintentando en {self.retry_delay:.0f}s para no saturar la red."
                    )
            except Exception as exc:  # noqa: BLE001 - fallo de red manejado aquí
                logger.error(f"Error en WebSocket: {exc}")

            if not self.running:
                break

            # Backoff exponencial: se queda bloqueado 'sleep' pero con
            # `asyncio.sleep` para no bloquear el event loop.
            logger.warning(f"Reconectando en {self.retry_delay:.1f} s (backoff)...")
            await asyncio.sleep(self.retry_delay)
            self.retry_delay = min(self.retry_delay * 2, _RECONNECT_MAX)

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        """Itera sobre los eventos encolados (consumidor del bot)."""
        while True:
            yield await self._queue.get()

    def stop(self) -> None:
        """Solicita el cierre ordenado del listener."""
        self.running = False


def create_listener(uri: str = PUMP_PORTAL_WS) -> TokenWebSocket:
    """Factory para instanciar un listener por defecto."""
    return TokenWebSocket(uri)
