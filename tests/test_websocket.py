"""Pruebas unitarias de los helpers de core/websocket.py.

Cubre la resolución de símbolos, la consulta de score RugCheck, los flujos
de compra/venta con notificación (DRY_RUN) y la fábrica/listener. Todas las
APIs y el WebSocket se simulan; nunca se toca la red.
"""

import asyncio
import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

import core.websocket as ws_module
from core.websocket import TokenWebSocket, create_listener, resolve_symbol


def _make_notifier() -> MagicMock:
    notifier = MagicMock()
    for name in (
        "send_error",
        "send_status",
        "send_take_profit",
        "send_stop_loss",
        "send_buy",
    ):
        setattr(notifier, name, AsyncMock(return_value=True))
    return notifier


class TestResolveSymbol:
    """Nunca se devuelve "N/A" como ticker."""

    def test_symbol_valido_se_conserva(self) -> None:
        assert resolve_symbol("MET") == "MET"

    def test_na_usa_fallback_del_mint(self) -> None:
        assert resolve_symbol("N/A", mint="METVSVabc") == "METVSV"

    def test_vacio_usa_fallback_del_mint(self) -> None:
        assert resolve_symbol("", mint="METVSVabc") == "METVSV"

    def test_vacio_sin_mint_devuelve_na(self) -> None:
        assert resolve_symbol("") == "N/A"

    def test_sin_symbol_sin_mint_devuelve_na(self) -> None:
        assert resolve_symbol(None) == "N/A"


class TestCheckRugcheck:
    """Consulta del score de riesgo vía el validador."""

    async def test_devuelve_score_del_report(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_cfg = SimpleNamespace(
            solana=SimpleNamespace(HELIUS_RPC_URL="rpc"),
            security=MagicMock(),
        )
        monkeypatch.setattr("config.load_config", lambda: fake_cfg)

        fake_validator = MagicMock()
        fake_validator._fetch_rugcheck = AsyncMock(return_value={"risks": [{"score": 42}]})
        fake_validator._rugcheck_score = MagicMock(return_value=42)
        monkeypatch.setattr("core.security.TokenSecurityValidator", lambda **kwargs: fake_validator)

        score = await ws_module.check_rugcheck("MINT123ABC")

        assert score == 42
        fake_validator._fetch_rugcheck.assert_awaited_once()


class TestProcessSellAndNotify:
    """Venta y notificación de salidas TP/SL."""

    def _fake_cfg(self, dry_run: bool) -> SimpleNamespace:
        return SimpleNamespace(
            trading=SimpleNamespace(DRY_RUN=dry_run),
            telegram=SimpleNamespace(TELEGRAM_TOKEN="t", TELEGRAM_CHAT_ID="c"),
        )

    async def test_dry_run_registra_salida(self, monkeypatch: pytest.MonkeyPatch) -> None:
        notifier = _make_notifier()
        monkeypatch.setattr("config.load_config", lambda: self._fake_cfg(dry_run=True))
        monkeypatch.setattr(
            "core.notifier.TelegramNotifier", lambda **kwargs: notifier
        )
        tracker = MagicMock()
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)

        ok = await ws_module.process_sell_and_notify("MINT123ABC", "MET", "TAKE_PROFIT", 150.0)

        assert ok is True
        notifier.send_take_profit.assert_awaited_once()

    async def test_real_sin_posicion_omite_y_devuelve_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        notifier = _make_notifier()
        monkeypatch.setattr("config.load_config", lambda: self._fake_cfg(dry_run=False))
        monkeypatch.setattr(
            "core.notifier.TelegramNotifier", lambda **kwargs: notifier
        )
        tracker = MagicMock()
        tracker.get_position.return_value = None
        tracker.executor.sell_token = AsyncMock()
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)

        ok = await ws_module.process_sell_and_notify("MINT123ABC", "MET", "STOP_LOSS", -50.0)

        assert ok is False
        tracker.executor.sell_token.assert_not_awaited()
        notifier.send_error.assert_not_awaited()

    async def test_real_vende_con_balance_estimado(self, monkeypatch: pytest.MonkeyPatch) -> None:
        notifier = _make_notifier()
        monkeypatch.setattr("config.load_config", lambda: self._fake_cfg(dry_run=False))
        monkeypatch.setattr(
            "core.notifier.TelegramNotifier", lambda **kwargs: notifier
        )
        pos = SimpleNamespace(amount=0.05, buy_price=0.001)
        tracker = MagicMock()
        tracker.get_position.return_value = pos
        tracker.executor.sell_token = AsyncMock()
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)

        ok = await ws_module.process_sell_and_notify("MINT123ABC", "MET", "STOP_LOSS", -30.0)

        assert ok is True
        tracker.executor.sell_token.assert_awaited_once()
        assert tracker.executor.sell_token.await_args.args[0] == "MINT123ABC"
        notifier.send_stop_loss.assert_awaited_once()

    async def test_fallo_vende_y_notifica_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        notifier = _make_notifier()
        notifier.send_error = AsyncMock()
        monkeypatch.setattr("config.load_config", lambda: self._fake_cfg(dry_run=False))
        monkeypatch.setattr(
            "core.notifier.TelegramNotifier", lambda **kwargs: notifier
        )
        pos = SimpleNamespace(amount=0.05, buy_price=0.001)
        tracker = MagicMock()
        tracker.get_position.return_value = pos
        tracker.executor.sell_token = AsyncMock(side_effect=RuntimeError("jupiter caído"))
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)

        ok = await ws_module.process_sell_and_notify("MINT123ABC", "MET", "STOP_LOSS", -30.0)

        assert ok is False
        notifier.send_error.assert_awaited_once()

    async def test_razon_no_tp_nl_notifica_estado(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Cualquier motivo que no sea TP/SL se reporta como estado."""
        notifier = _make_notifier()
        monkeypatch.setattr("config.load_config", lambda: self._fake_cfg(dry_run=True))
        monkeypatch.setattr(
            "core.notifier.TelegramNotifier", lambda **kwargs: notifier
        )
        tracker = MagicMock()
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)

        ok = await ws_module.process_sell_and_notify("MINT123ABC", "MET", "TIME_EXPIRED", 5.0)

        assert ok is True
        notifier.send_status.assert_awaited_once()


class TestProcessBuyAndNotify:
    """Compra simulada y registro de la posición en el tracker."""

    async def test_flujo_completo(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from core.execution import Position

        fake_cfg = SimpleNamespace(
            solana=SimpleNamespace(PRIVATE_KEY="x", HELIUS_RPC_URL="rpc"),
            trading=SimpleNamespace(
                SLIPPAGE_BPS=500,
                BUY_AMOUNT_SOL=0.05,
                TAKE_PROFIT_PCT=100.0,
                STOP_LOSS_PCT=30.0,
                TRAILING_STOP_ACTIVATION_PCT=20.0,
                TRAILING_STOP_DISTANCE_PCT=15.0,
                DRY_RUN=True,
            ),
            telegram=SimpleNamespace(TELEGRAM_TOKEN="t", TELEGRAM_CHAT_ID="c"),
        )
        monkeypatch.setattr("config.load_config", lambda: fake_cfg)

        executor = MagicMock()
        executor.buy_token = AsyncMock(return_value="DRY_RUN")
        executor.get_token_symbol = AsyncMock(return_value="MET")
        executor.get_token_price = AsyncMock(return_value=0.001)
        executor.positions = {
            "MINT123ABC": Position(mint="MINT123ABC", token_amount_ui=50.0, entry_price=0.001)
        }
        monkeypatch.setattr("core.execution.JupiterExecutor", lambda **kwargs: executor)

        notifier = _make_notifier()
        monkeypatch.setattr("core.notifier.TelegramNotifier", lambda **kwargs: notifier)

        tracker = MagicMock()
        tracker.add_position = MagicMock()
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)

        await ws_module.process_buy_and_notify("MINT123ABC", "MET", score=100.0)

        executor.buy_token.assert_awaited_once()
        notifier.send_buy.assert_awaited_once()
        tracker.add_position.assert_called_once()
        _, kwargs = tracker.add_position.call_args
        assert kwargs["buy_price"] == 0.001

    async def test_error_de_compra_envia_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_cfg = SimpleNamespace(
            solana=SimpleNamespace(PRIVATE_KEY="x", HELIUS_RPC_URL="rpc"),
            trading=SimpleNamespace(
                SLIPPAGE_BPS=500,
                BUY_AMOUNT_SOL=0.05,
                TAKE_PROFIT_PCT=100.0,
                STOP_LOSS_PCT=30.0,
                TRAILING_STOP_ACTIVATION_PCT=20.0,
                TRAILING_STOP_DISTANCE_PCT=15.0,
                DRY_RUN=True,
            ),
            telegram=SimpleNamespace(TELEGRAM_TOKEN="t", TELEGRAM_CHAT_ID="c"),
        )
        monkeypatch.setattr("config.load_config", lambda: fake_cfg)

        executor = MagicMock()
        executor.buy_token = AsyncMock(side_effect=RuntimeError("swap falló"))
        monkeypatch.setattr("core.execution.JupiterExecutor", lambda **kwargs: executor)

        notifier = _make_notifier()
        monkeypatch.setattr("core.notifier.TelegramNotifier", lambda **kwargs: notifier)

        await ws_module.process_buy_and_notify("MINT123ABC")

        notifier.send_error.assert_awaited_once()


def _fake_buy_cfg(dry_run: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        solana=SimpleNamespace(PRIVATE_KEY="x", HELIUS_RPC_URL="rpc"),
        trading=SimpleNamespace(
            SLIPPAGE_BPS=500,
            BUY_AMOUNT_SOL=0.05,
            TAKE_PROFIT_PCT=100.0,
            STOP_LOSS_PCT=30.0,
            TRAILING_STOP_ACTIVATION_PCT=20.0,
            TRAILING_STOP_DISTANCE_PCT=15.0,
            DRY_RUN=dry_run,
        ),
        telegram=SimpleNamespace(TELEGRAM_TOKEN="t", TELEGRAM_CHAT_ID="c"),
    )


class _Scaffold:
    """Fixtures compartidos de compra (executor/notifier/tracker mockeados)."""

    def __init__(self, monkeypatch, *, dry_run: bool = True) -> None:
        self.monkeypatch = monkeypatch
        self.dry_run = dry_run
        monkeypatch.setattr("config.load_config", lambda: _fake_buy_cfg(dry_run))
        self.executor = MagicMock()
        self.executor.buy_token = AsyncMock(return_value="DRY_RUN")
        self.executor.get_token_symbol = AsyncMock(return_value="MET")
        monkeypatch.setattr("core.execution.JupiterExecutor", lambda **kwargs: self.executor)
        self.notifier = _make_notifier()
        monkeypatch.setattr("core.notifier.TelegramNotifier", lambda **kwargs: self.notifier)
        self.tracker = MagicMock()
        self.tracker.add_position = MagicMock()
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: self.tracker)


class TestProcessBuyBranches:
    """Ramas alternativas de la compra: ticker por API, precio pendiente, etiquetas."""

    async def test_ticker_desde_api_cuando_falta_symbol(self, monkeypatch) -> None:
        s = _Scaffold(monkeypatch)
        s.monkeypatch.setenv("FORCE_TEST_BUY", "false")
        s.executor.get_token_price = AsyncMock(return_value=0.001)
        s.executor.positions = {}

        await ws_module.process_buy_and_notify("MINT123ABC", symbol="", score=10.0)

        kwargs = s.notifier.send_buy.await_args.kwargs
        assert kwargs["symbol"] == "MET"
        s.tracker.add_position.assert_called_once()

    async def test_ticker_fallback_al_mint_si_api_falla(self, monkeypatch) -> None:
        s = _Scaffold(monkeypatch)
        s.executor.get_token_symbol = AsyncMock(side_effect=RuntimeError("no API"))
        s.executor.get_token_price = AsyncMock(return_value=0.001)
        s.executor.positions = {}

        await ws_module.process_buy_and_notify("MINT123ABC", symbol="", score=10.0)

        assert s.notifier.send_buy.await_args.kwargs["symbol"] == "MINT12"

    async def test_precio_pendiente_si_api_falla(self, monkeypatch) -> None:
        s = _Scaffold(monkeypatch)
        s.executor.get_token_price = AsyncMock(side_effect=RuntimeError("no API"))
        s.executor.positions = {}

        await ws_module.process_buy_and_notify("MINT123ABC", "MET", score=5.0)

        # Entrada PENDIENTE: el tracker fijará el entry base real después.
        assert s.notifier.send_buy.await_args.kwargs["price"] == 0.0
        assert s.tracker.add_position.call_args.kwargs["buy_price"] == 0.0

    async def test_etiqueta_real_para_compra(self, monkeypatch) -> None:
        s = _Scaffold(monkeypatch, dry_run=False)
        s.executor.get_token_price = AsyncMock(return_value=0.001)
        s.executor.positions = {}

        await ws_module.process_buy_and_notify("MINT123ABC", "MET", score=5.0)

        assert s.notifier.send_buy.await_args.kwargs["dry_run"] is False

    async def test_etiqueta_test_forzado_en_error(self, monkeypatch) -> None:
        monkeypatch.setenv("FORCE_TEST_BUY", "true")
        s = _Scaffold(monkeypatch)
        s.executor.buy_token = AsyncMock(side_effect=RuntimeError("swap falló"))

        await ws_module.process_buy_and_notify("MINT123ABC", "MET", score=5.0)

        msg = s.notifier.send_error.await_args.args[0]
        assert "TEST FORZADO" in msg


class TestListener:
    """Fábrica y ciclo de vida básico del listener."""

    def test_create_listener_retorna_token_websocket(self) -> None:
        listener = create_listener()
        assert isinstance(listener, TokenWebSocket)

    def test_stop_detiene_el_listener(self) -> None:
        listener = TokenWebSocket()
        assert listener.running is True
        listener.stop()
        assert listener.running is False

    def test_events_encola_y_genera(self) -> None:
        async def _run() -> None:
            listener = TokenWebSocket()
            listener._queue.put_nowait({"mint": "MINT123ABC"})
            first = await anext(listener.events())
            assert first == {"mint": "MINT123ABC"}

        asyncio.run(_run())


class _FakeRaw:
    """Mensaje WS simulado con .data y .json() (misma API que aiohttp)."""

    def __init__(self, payload=None, invalid: bool = False) -> None:
        self.data = json.dumps(payload) if payload is not None else "{ no-json"
        self._payload = payload
        self._invalid = invalid

    def json(self) -> dict:
        if self._invalid or self._payload is None:
            raise ValueError("json inválido")
        return self._payload


class _FakeWS:
    """WebSocket simulado: permite async for y send_json."""

    def __init__(self, raws) -> None:
        self._raws = raws
        self.sent: list = []

    async def __aenter__(self) -> "_FakeWS":
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False

    async def send_json(self, payload) -> None:
        self.sent.append(payload)

    def __aiter__(self):
        async def _gen():
            for raw in self._raws:
                yield raw

        return _gen()


class _FakeSession:
    """aiohttp.ClientSession simulado con una conexión WS fake."""

    def __init__(self, ws: _FakeWS) -> None:
        self._ws = ws

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False

    def ws_connect(self, *args, **kwargs) -> _FakeWS:
        # Síncrona a propósito: aiohttp devuelve el objeto (que se usa como
        # contexto async en `async with session.ws_connect(...) as ws:`).
        return self._ws


class TestWebSocketLoop:
    """Bucle de conexión: consume el feed, encola tokens y aplica filtros."""

    async def _run_connect(self, monkeypatch, raws, *, rugcheck=5.0, max_score="10"):
        monkeypatch.setattr(ws_module, "check_rugcheck", AsyncMock(return_value=rugcheck))
        buy = AsyncMock()
        monkeypatch.setattr(ws_module, "process_buy_and_notify", buy)
        monkeypatch.setenv("RUGCHECK_MAX_SCORE", max_score)
        fake_ws = _FakeWS(raws)
        monkeypatch.setattr(aiohttp, "ClientSession", lambda: _FakeSession(fake_ws))
        listener = TokenWebSocket()
        await listener._connect_and_listen()
        return listener, fake_ws, buy

    async def test_encola_y_compra_token_valido(self, monkeypatch) -> None:
        msg = {"type": "tokenCreation", "mint": "MINT123ABC", "symbol": "MET"}
        listener, fake_ws, buy = await self._run_connect(monkeypatch, [_FakeRaw(payload=msg)])
        assert fake_ws.sent == [ws_module._SUBSCRIBE_PAYLOAD]
        buy.assert_awaited_once()
        assert listener._queue.qsize() == 1
        assert listener._queue.get_nowait() == msg

    async def test_json_invalido_se_ignora_y_continua(self, monkeypatch) -> None:
        msg = {"type": "tokenCreation", "mint": "MINT123ABC", "symbol": "MET"}
        listener, _, buy = await self._run_connect(
            monkeypatch, [_FakeRaw(invalid=True), _FakeRaw(payload=msg)]
        )
        buy.assert_awaited_once()
        assert listener._queue.qsize() == 1

    async def test_error_del_servidor_resuscribe(self, monkeypatch) -> None:
        monkeypatch.setattr(ws_module, "_ERROR_RESUBSCRIBE_SECONDS", 0.0)
        msg = {"type": "tokenCreation", "mint": "MINT123ABC", "symbol": "MET"}
        listener, fake_ws, buy = await self._run_connect(
            monkeypatch,
            [_FakeRaw(payload={"errors": "timeout"}), _FakeRaw(payload=msg)],
        )
        assert fake_ws.sent == [ws_module._SUBSCRIBE_PAYLOAD, ws_module._SUBSCRIBE_PAYLOAD]
        assert listener._queue.qsize() == 1

    async def test_score_cero_omite_compra(self, monkeypatch) -> None:
        msg = {"type": "tokenCreation", "mint": "MINT123ABC", "symbol": "MET"}
        listener, _, buy = await self._run_connect(monkeypatch, [_FakeRaw(payload=msg)], rugcheck=0.0)
        buy.assert_not_awaited()
        assert listener._queue.qsize() == 1

    async def test_score_alto_rechaza_compra(self, monkeypatch) -> None:
        msg = {"type": "tokenCreation", "mint": "MINT123ABC", "symbol": "MET"}
        _, _, buy = await self._run_connect(monkeypatch, [_FakeRaw(payload=msg)], rugcheck=15.0)
        buy.assert_not_awaited()

    async def test_error_de_rugcheck_no_detiene_el_feed(self, monkeypatch) -> None:
        monkeypatch.setattr(
            ws_module, "check_rugcheck", AsyncMock(side_effect=RuntimeError("api caída"))
        )
        buy = AsyncMock()
        monkeypatch.setattr(ws_module, "process_buy_and_notify", buy)
        monkeypatch.setenv("RUGCHECK_MAX_SCORE", "10")
        msg = {"type": "tokenCreation", "mint": "MINT123ABC", "symbol": "MET"}
        fake_ws = _FakeWS([_FakeRaw(payload=msg)])
        monkeypatch.setattr(aiohttp, "ClientSession", lambda: _FakeSession(fake_ws))
        listener = TokenWebSocket()
        await listener._connect_and_listen()
        buy.assert_not_awaited()
        assert listener._queue.qsize() == 1

    async def test_force_test_buy_dispara_y_desactiva(self, monkeypatch) -> None:
        monkeypatch.setenv("FORCE_TEST_BUY", "true")
        msg = {"type": "tokenCreation", "mint": "MINT123ABC", "symbol": "MET"}
        listener, _, buy = await self._run_connect(monkeypatch, [_FakeRaw(payload=msg)])
        buy.assert_awaited_once()
        assert listener._queue.qsize() == 0  # continue antes de encolar
        assert os.environ["FORCE_TEST_BUY"] == "False"


class TestRunLoop:
    """Bucle principal con reconocción y backoff exponencial."""

    async def test_run_reconecta_y_para_limpio(self, monkeypatch) -> None:
        monkeypatch.setattr(ws_module, "_RECEIVE_TIMEOUT_SECONDS", 0.0)
        monkeypatch.setattr(ws_module, "_RECONNECT_MIN", 0.01)
        monkeypatch.setattr(ws_module, "_RECONNECT_MAX", 0.05)
        monkeypatch.setattr(ws_module, "check_rugcheck", AsyncMock(return_value=5.0))
        monkeypatch.setattr(ws_module, "process_buy_and_notify", AsyncMock())
        msg = {"type": "tokenCreation", "mint": "MINT123ABC", "symbol": "MET"}
        fake_ws = _FakeWS([_FakeRaw(payload=msg)])
        monkeypatch.setattr(aiohttp, "ClientSession", lambda: _FakeSession(fake_ws))

        listener = TokenWebSocket()
        task = asyncio.create_task(listener.run())
        await asyncio.sleep(0.1)
        listener.stop()
        await asyncio.wait_for(task, timeout=3)


class TestFactory:
    def test_create_listener_con_uri_personalizada(self) -> None:
        listener = create_listener(uri="wss://custom.invalid")
        assert listener.uri == "wss://custom.invalid"