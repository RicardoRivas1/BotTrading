"""Escuchador persistente de eventos de nuevos tokens.

Conecta vía WebSocket a la API de PumpPortal (o endpoint de streaming de
Helius) y emite cada nuevo par/token detectado. Implementa reconexión
automática con backoff exponencial para soportar cortes de red prolongados
sin detener el bucle de eventos de asyncio.
"""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Optional

import aiohttp
from loguru import logger

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

# User-Agent de navegador para eludir bloqueos básicos de Cloudflare.
_USER_AGENT_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# Backoff exponencial de reconexión (segundos).
_RECONNECT_MIN = 1.0
_RECONNECT_MAX = 60.0


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
                logger.info("Conectado al WebSocket: {}", self.uri)
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
                    logger.info("📩 Evento raw recibido: {}", raw.data[:100])
                    try:
                        payload: dict[str, Any] = raw.json()
                    except (TypeError, ValueError):
                        logger.warning("Mensaje JSON inválido recibido, ignorando.")
                        continue

                    # Control de errores en respuestas raw: si el servidor responde
                    # con la clave 'errors' (p. ej. {"errors": "..."}), lo registramos
                    # y reintentamos la suscripción tras _ERROR_RESUBSCRIBE_SECONDS.
                    if "errors" in payload:
                        logger.error(
                            "⚠️ Error del WebSocket de PumpPortal: {}", payload.get("errors")
                        )
                        logger.warning(
                            f"Reintentando suscripción en {_ERROR_RESUBSCRIBE_SECONDS:.0f}s..."
                        )
                        await asyncio.sleep(_ERROR_RESUBSCRIBE_SECONDS)
                        await ws.send_json(_SUBSCRIBE_PAYLOAD)
                        logger.info("Suscrito nuevamente al feed de nuevos tokens (subscribeNewToken).")
                        last_token_at = asyncio.get_event_loop().time()
                        continue

                    mint = payload.get("mint") or payload.get("token", {}).get("mint")
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
                logger.error("Handshake HTTP {} fallido: {}", exc.status, exc)
                if exc.status in (502, 503):
                    self.retry_delay = _ERROR_502_RETRY_SECONDS
                    logger.warning(
                        "⚠️ HTTP {} detectado: reintentando en {:.0f}s para no saturar la red.",
                        exc.status, self.retry_delay,
                    )
            except Exception as exc:  # noqa: BLE001 - fallo de red manejado aquí
                logger.error("Error en WebSocket: {}", exc)

            if not self.running:
                break

            # Backoff exponencial: se queda bloqueado 'sleep' pero con
            # `asyncio.sleep` para no bloquear el event loop.
            logger.warning(
                "Reconectando en {:.1f} s (backoff)...", self.retry_delay
            )
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
