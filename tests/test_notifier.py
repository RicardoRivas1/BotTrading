"""Pruebas unitarias de las notificaciones de Telegram (core/notifier.py).

Todas las peticiones HTTP se simulan con mocks en `_post_text`; nunca se toca
la red. Cubre el envío HTML, el reintento en texto plano ante errores 400,
los mensajes de compra/venta/error/TP/SL y el heartbeat periódico.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

import core.notifier as notifier_module
from core.notifier import TelegramNotifier


@pytest.fixture
def notifier() -> TelegramNotifier:
    return TelegramNotifier(token="token-de-prueba", chat_id="chat-id")


class TestSend:
    """Envío genérico de mensajes a Telegram."""

    async def test_send_deshabilitado_devuelve_false_sin_postear(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        n = TelegramNotifier(token="", chat_id="")
        n._post_text = AsyncMock()
        assert await n.send("<b>hola</b>") is False
        n._post_text.assert_not_awaited()

    async def test_send_exitoso(self, notifier: TelegramNotifier) -> None:
        notifier._post_text = AsyncMock(return_value=(200, "ok"))
        assert await notifier.send("<b>hola</b>") is True
        notifier._post_text.assert_awaited_once()

    async def test_reintenta_plano_cuando_html_da_400(
        self, notifier: TelegramNotifier
    ) -> None:
        notifier._post_text = AsyncMock(side_effect=[(400, "bad html"), (200, "ok")])
        assert await notifier.send("<b>hola</b>") is True
        assert notifier._post_text.await_count == 2

    async def test_reintento_plano_tambien_falla(
        self, notifier: TelegramNotifier
    ) -> None:
        notifier._post_text = AsyncMock(side_effect=[(400, "bad html"), (400, "still bad")])
        assert await notifier.send("<b>hola</b>") is False

    async def test_400_sin_parse_mode_html_no_reintenta(
        self, notifier: TelegramNotifier
    ) -> None:
        notifier._post_text = AsyncMock(return_value=(400, "no html"))
        assert await notifier.send("texto plano", parse_mode="Markdown") is False
        notifier._post_text.assert_awaited_once()

    async def test_status_no_200_no_reintenta(self, notifier: TelegramNotifier) -> None:
        notifier._post_text = AsyncMock(return_value=(500, "boom"))
        assert await notifier.send("<b>hola</b>") is False
        notifier._post_text.assert_awaited_once()

    async def test_fallo_de_red_devuelve_false(self, notifier: TelegramNotifier) -> None:
        notifier._post_text = AsyncMock(side_effect=__import__("aiohttp").ClientError("net"))
        assert await notifier.send("<b>hola</b>") is False

    async def test_fallo_de_red_en_reintento_devuelve_false(
        self, notifier: TelegramNotifier
    ) -> None:
        aiohttp = __import__("aiohttp")
        notifier._post_text = AsyncMock(
            side_effect=[(400, "bad"), aiohttp.ClientError("net")]
        )
        assert await notifier.send("<b>hola</b>") is False


class TestMensajes:
    """Mensajes predefinidos (buy/sell/error/TP/SL/estado)."""

    async def test_send_buy_compone_html(self, notifier: TelegramNotifier) -> None:
        notifier.send = AsyncMock(return_value=True)
        assert await notifier.send_buy("MINT123ABC", 0.05, symbol="MET", score=42, price=0.001) is True
        sent: str = notifier.send.await_args.args[0]
        assert "COMPRA EJECUTADA" in sent
        assert "MET" in sent
        assert "0.0500 SOL" in sent

    async def test_send_buy_sin_precio_muestra_nd(self, notifier: TelegramNotifier) -> None:
        notifier.send = AsyncMock(return_value=True)
        await notifier.send_buy("MINT123ABC", 0.05, symbol="MET")
        sent: str = notifier.send.await_args.args[0]
        assert "N/D" in sent

    async def test_send_buy_notification_es_alias(self, notifier: TelegramNotifier) -> None:
        notifier.send = AsyncMock(return_value=True)
        assert await notifier.send_buy_notification("MINT123ABC", 0.05, symbol="MET") is True

    async def test_send_sell(self, notifier: TelegramNotifier) -> None:
        notifier.send = AsyncMock(return_value=True)
        assert await notifier.send_sell("MINT123ABC", 1000000, 0.049) is True
        assert "VENTA EJECUTADA" in notifier.send.await_args.args[0]

    async def test_send_error(self, notifier: TelegramNotifier) -> None:
        notifier.send = AsyncMock(return_value=True)
        assert await notifier.send_error("algo falló") is True

    async def test_send_take_profit(self, notifier: TelegramNotifier) -> None:
        notifier.send = AsyncMock(return_value=True)
        assert await notifier.send_take_profit("MINT123ABC", 150.5) is True
        assert "TAKE PROFIT" in notifier.send.await_args.args[0]

    async def test_send_stop_loss(self, notifier: TelegramNotifier) -> None:
        notifier.send = AsyncMock(return_value=True)
        assert await notifier.send_stop_loss("MINT123ABC", -40.2) is True
        assert "STOP LOSS" in notifier.send.await_args.args[0]

    async def test_send_trailing_stop(self, notifier: TelegramNotifier) -> None:
        notifier.send = AsyncMock(return_value=True)
        assert await notifier.send_trailing_stop("MINT123ABC", 35.0) is True
        assert "TRAILING STOP" in notifier.send.await_args.args[0]

    async def test_send_status(self, notifier: TelegramNotifier) -> None:
        notifier.send = AsyncMock(return_value=True)
        assert await notifier.send_status("bot activo") is True
        assert "ESTADO" in notifier.send.await_args.args[0]

    async def test_send_buy_escapa_html(self, notifier: TelegramNotifier) -> None:
        """El ticker/mint se escapan para evitar errores 400 de Telegram."""
        notifier.send = AsyncMock(return_value=True)
        await notifier.send_buy("<script>", 0.05, symbol="<b>")
        sent: str = notifier.send.await_args.args[0]
        assert "<script>" not in sent
        assert "&lt;script&gt;" in sent

    async def test_send_buy_precio_en_usd(self, notifier: TelegramNotifier) -> None:
        """Precio en USD (rama de formato distinta a SOL)."""
        notifier.send = AsyncMock(return_value=True)
        await notifier.send_buy("MINT123ABC", 0.5, symbol="MET", price=0.00012345, price_unit="USD")
        sent: str = notifier.send.await_args.args[0]
        assert "$0.00012345" in sent

    async def test_post_text_publica_real(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """El método real `_post_text` (sin mock) publica y devuelve al parseo."""

        class _FakeResp:
            def __init__(self, status: int, body: str) -> None:
                self.status = status
                self._body = body

            async def __aenter__(self) -> "_FakeResp":
                return self

            async def __aexit__(self, *exc_info: object) -> bool:
                return False

            async def text(self) -> str:
                return self._body

        session = MagicMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        # `_post_text` usa `async with session.post(...)`, así que devuelve
        # el objeto de respuesta de forma síncrona (como aiohttp real).
        session.post = MagicMock(return_value=_FakeResp(200, "ok"))
        monkeypatch.setattr("aiohttp.ClientSession", lambda: session)

        status, body = await notifier_module.TelegramNotifier._post_text(
            "https://api.telegram.org/bot/botx/sendMessage", {"text": "hola"}
        )

        assert (status, body) == (200, "ok")
        session.post.assert_called_once()


class TestHeartbeatFallo:
    """Heartbeat que no puede enviar su estado."""

    async def test_fallo_de_envio_registra_warning(
        self, notifier: TelegramNotifier, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class _Stop(Exception):
            pass

        calls = {"sleeps": 0}

        async def _sleep(_seconds: float) -> None:
            calls["sleeps"] += 1
            if calls["sleeps"] >= 2:
                raise _Stop()

        monkeypatch.setattr(notifier_module.asyncio, "sleep", _sleep)
        notifier.send_status = AsyncMock(return_value=False)  # type: ignore[attr-defined]

        with pytest.raises(_Stop):
            await notifier.start_heartbeat(interval_minutes=1)

        assert calls["sleeps"] == 2
        notifier.send_status.assert_awaited()  # type: ignore[attr-defined]


class TestHeartbeat:
    """Heartbeat periódico (envía estado cada N minutos)."""

    async def test_heartbeat_envia_estado_cada_intervalo(
        self, notifier: TelegramNotifier, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class _StopHeartbeat(Exception):
            pass

        calls: dict[str, int] = {"sleeps": 0, "status": 0}

        async def _sleep(_seconds: float) -> None:
            calls["sleeps"] += 1
            if calls["sleeps"] >= 2:
                raise _StopHeartbeat()

        async def _send_status(_msg: str) -> bool:
            calls["status"] += 1
            return True

        monkeypatch.setattr(notifier_module.asyncio, "sleep", _sleep)
        notifier.send_status = AsyncMock(side_effect=_send_status)  # type: ignore[attr-defined]

        with pytest.raises(_StopHeartbeat):
            await notifier.start_heartbeat(interval_minutes=1)

        assert calls["status"] == 1


class TestFactory:
    def test_create_notifier(self) -> None:
        n = notifier_module.create_notifier("t", "c")
        assert isinstance(n, TelegramNotifier)
        assert n.enabled is True