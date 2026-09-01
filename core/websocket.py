"""Escuchador persistente de eventos de nuevos tokens.

Conecta vía WebSocket a la API de PumpPortal (o endpoint de streaming de
Helius) y emite cada nuevo par/token detectado. Implementa reconexión
automática con backoff exponencial para soportar cortes de red prolongados
sin detener el bucle de eventos de asyncio.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator, Optional

import aiohttp
from loguru import logger

# -- Constantes ---------------------------------------------------------------
# Endpoint público de PumpPortal para escuchar tokens/liquidez nuevos.
PUMP_PORTAL_WS = "wss://pumpportal.fun/api/data"

# Heartbeat: ping cada X segundos si el servidor no envía datos.
_HEARTBEAT_SECONDS = 30.0

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
                heartbeat=_HEARTBEAT_SECONDS,
                ssl=False,
                max_msg_size=8 * 1024 * 1024,
            ) as ws:
                logger.info("Conectado al WebSocket: {}", self.uri)
                self.retry_delay = _RECONNECT_MIN

                # Suscripción a eventos de creación de nuevos tokens.
                await ws.send_str(json.dumps({"op": "subscribeNewToken"}))
                logger.info("Suscrito exitosamente al feed de nuevos tokens (subscribeNewToken).")

                async for raw in ws:
                    if not self.running:
                        break
                    try:
                        payload: dict[str, Any] = raw.json()
                    except (TypeError, ValueError):
                        logger.warning("Mensaje JSON inválido recibido, ignorando.")
                        continue

                    mint = payload.get("mint") or payload.get("token", {}).get("mint")
                    if payload.get("type") in ("tokenCreation", "create"):
                        # Log breve en tiempo real para confirmar la recepción.
                        logger.debug(f"Nuevo token detectado: {mint}")
                        # No bloquea: la cola es interna e ilimitada.
                        self._queue.put_nowait(payload)

    async def run(self) -> None:
        """Bucle principal con reconexión automática infinita."""
        while self.running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                logger.info("Listener cancelado.")
                break
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
