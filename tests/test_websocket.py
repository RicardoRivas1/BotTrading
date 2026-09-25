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
        "send_trailing_stop",
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


def _fake_cfg(dry_run: bool) -> SimpleNamespace:
    return SimpleNamespace(
        trading=SimpleNamespace(DRY_RUN=dry_run),
        telegram=SimpleNamespace(TELEGRAM_TOKEN="t", TELEGRAM_CHAT_ID="c"),
    )


class TestUnVendedorPorMint:
    """Dos salidas del mismo token a la vez no pueden caer sobre el mismo saldo.

    El tracker (task propia) y la estrategia de copy trading (webhook/poll) piden
    la salida del MISMO mint de forma independiente. Sin barrera, ambos leían el
    saldo on-chain y lanzaban dos ventas sobre los mismos tokens.
    """

    async def test_ventas_concurrentes_del_mismo_mint_se_serializan(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        notifier = _make_notifier()
        monkeypatch.setattr("config.load_config", lambda: _fake_cfg(dry_run=False))
        monkeypatch.setattr(
            "core.notifier.TelegramNotifier", lambda **kwargs: notifier
        )
        pos = SimpleNamespace(amount=0.05, buy_price=0.001)
        tracker = MagicMock()
        tracker.get_position.return_value = pos
        tracker.executor.positions = {}
        tracker.executor.sell_token = AsyncMock(return_value="sig")
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)
        monkeypatch.setattr(ws_module, "_resolve_tokens_to_sell", AsyncMock(return_value=1000.0))

        in_flight = 0
        max_in_flight = 0

        async def _slow_sell(*args: object, **kwargs: object) -> str:
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await asyncio.sleep(0.02)
            in_flight -= 1
            return "sig"

        tracker.executor.sell_token = _slow_sell  # type: ignore[assignment]

        results = await asyncio.gather(
            ws_module.process_sell_and_notify("MINT123ABC", "MET", "TAKE_PROFIT", 20.0),
            ws_module.process_sell_and_notify("MINT123ABC", "MET", "STOP_LOSS", -20.0),
        )

        assert results == [True, True]
        # Nunca dos ventas simultáneas del mismo mint.
        assert max_in_flight == 1

    def test_el_lock_se_reutiliza_para_el_mismo_mint(self) -> None:
        first = ws_module._sell_lock_for("MINT_A")
        assert ws_module._sell_lock_for("MINT_A") is first
        assert ws_module._sell_lock_for("MINT_B") is not first


class TestStatsDeSalidasDelTracker:
    """Las stats de TP/SL usan el resultado real, no una identidad algebraica."""

    def _tracker(self, balance_before: float, balance_after: float) -> MagicMock:
        tracker = MagicMock()
        pos = SimpleNamespace(
            amount=0.05, buy_price=0.001, mint="MINT123ABC", symbol="MET",
            created_at=1000.0, source_wallet="",
        )
        tracker.get_position.return_value = pos
        tracker.executor.positions = {
            "MINT123ABC": SimpleNamespace(
                mint="MINT123ABC", token_amount_ui=1000.0, sol_invested=0.05
            )
        }
        tracker.executor.sell_token = AsyncMock(return_value="sig")
        tracker.executor.get_sol_balance = AsyncMock(
            side_effect=[balance_before, balance_after]
        )
        return tracker

    async def test_registra_el_sol_real_y_el_pnl_medido(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """El tracker decía +100% pero la wallet devolvió 0.045 sobre 0.05.

        Antes se registraba `invertido * (1 + pnl/100)` = 0.10 SOL, o sea el
        doble de lo obtenido: la gráfica de /stats mentía por construcción.
        """
        notifier = _make_notifier()
        monkeypatch.setattr("config.load_config", lambda: _fake_cfg(dry_run=False))
        monkeypatch.setattr("core.notifier.TelegramNotifier", lambda **kwargs: notifier)
        # 0.05 -> 0.045 = -10% real, aunque el tracker calculó +100%.
        tracker = self._tracker(1.0, 1.045)
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)
        monkeypatch.setattr(ws_module, "_resolve_tokens_to_sell", AsyncMock(return_value=1000.0))
        stats_mock = MagicMock()
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: stats_mock)

        ok = await ws_module.process_sell_and_notify(
            "MINT123ABC", "MET", "TAKE_PROFIT", 100.0
        )

        assert ok is True
        stats_mock.record_sell.assert_called_once()
        kwargs = stats_mock.record_sell.call_args.kwargs
        assert kwargs["sol_received"] == pytest.approx(0.045)  # SOL real
        assert kwargs["sol_invested"] == pytest.approx(0.05)
        assert kwargs["pnl_pct"] == pytest.approx(-10.0)  # medido, no el +100%
        assert kwargs["pnl_unreliable"] is False

    async def test_sin_medir_queda_marcado_como_no_fiable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        notifier = _make_notifier()
        monkeypatch.setattr("config.load_config", lambda: _fake_cfg(dry_run=False))
        monkeypatch.setattr("core.notifier.TelegramNotifier", lambda **kwargs: notifier)
        # El RPC del saldo no responde: no se puede medir el resultado real.
        tracker = self._tracker(0.0, 0.0)
        tracker.executor.get_sol_balance = AsyncMock(side_effect=TimeoutError("RPC"))
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)
        monkeypatch.setattr(ws_module, "_resolve_tokens_to_sell", AsyncMock(return_value=1000.0))
        stats_mock = MagicMock()
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: stats_mock)

        await ws_module.process_sell_and_notify(
            "MINT123ABC", "MET", "TAKE_PROFIT", 100.0
        )

        kwargs = stats_mock.record_sell.call_args.kwargs
        # No se inventa un resultado: el trade queda fuera de las metricas.
        assert kwargs["pnl_unreliable"] is True
        assert kwargs["sol_received"] == 0.0

    async def test_una_venta_fallida_no_entra_en_las_stats(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        notifier = _make_notifier()
        monkeypatch.setattr("config.load_config", lambda: _fake_cfg(dry_run=False))
        monkeypatch.setattr("core.notifier.TelegramNotifier", lambda **kwargs: notifier)
        tracker = self._tracker(1.0, 1.0)
        tracker.executor.sell_token = AsyncMock(return_value=None)  # no confirmada
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)
        monkeypatch.setattr(ws_module, "_resolve_tokens_to_sell", AsyncMock(return_value=1000.0))
        stats_mock = MagicMock()
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: stats_mock)

        ok = await ws_module.process_sell_and_notify(
            "MINT123ABC", "MET", "TAKE_PROFIT", 100.0
        )

        assert ok is False
        stats_mock.record_sell.assert_not_called()


class TestMedicionDeSaldoCompartida:
    """El delta de saldo es de la WALLET: dos mints no pueden medirse a la vez.

    Sin serializar, el SOL que devuelve la venta de un token se colaba en el PnL
    del otro (el "PnL medido" pasaba a ser tan inventado como el estimado).
    """

    async def test_ventas_de_mints_distintos_no_se_solapan(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        notifier = _make_notifier()
        monkeypatch.setattr("config.load_config", lambda: _fake_cfg(dry_run=False))
        monkeypatch.setattr(
            "core.notifier.TelegramNotifier", lambda **kwargs: notifier
        )
        tracker = MagicMock()
        tracker.executor.positions = {}
        tracker.get_position.side_effect = lambda mint: SimpleNamespace(
            amount=0.05, buy_price=0.001, mint=mint
        )
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)
        monkeypatch.setattr(ws_module, "_resolve_tokens_to_sell", AsyncMock(return_value=1000.0))

        # Saldo que SOLO avanza dentro de la ventana de medición de cada venta.
        balance = {"now": 1.0}
        in_window = 0
        max_in_window = 0

        async def _get_sol_balance() -> float:
            return balance["now"]

        async def _sell(mint: str, amount: float) -> str:
            nonlocal in_window, max_in_window
            in_window += 1
            max_in_window = max(max_in_window, in_window)
            balance["now"] += 0.02  # el SOL de esta venta entra YA
            await asyncio.sleep(0.02)
            in_window -= 1
            return f"sig_{mint}"

        tracker.executor.get_sol_balance = _get_sol_balance
        tracker.executor.sell_token = _sell

        await asyncio.gather(
            ws_module.process_sell_and_notify("MINT_A", "A", "TAKE_PROFIT", 10.0),
            ws_module.process_sell_and_notify("MINT_B", "B", "TAKE_PROFIT", 10.0),
        )

        # Nunca dos ventanas "leer -> vender -> releer" simultáneas.
        assert max_in_window == 1


class TestContabilidadParcial:
    """Una venta parcial descuenta el saldo en TODOS los registros."""

    async def test_venta_parcial_descuenta_tambien_el_saldo_del_executor(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        notifier = _make_notifier()
        monkeypatch.setattr("config.load_config", lambda: _fake_cfg(dry_run=False))
        monkeypatch.setattr(
            "core.notifier.TelegramNotifier", lambda **kwargs: notifier
        )
        pos = SimpleNamespace(amount=0.05, buy_price=0.001)
        exec_pos = SimpleNamespace(mint="MINT123ABC", token_amount_ui=1000.0, sol_invested=0.05)
        tracker = MagicMock()
        tracker.get_position.return_value = pos
        tracker.executor.positions = {"MINT123ABC": exec_pos}
        tracker.executor.sell_token = AsyncMock(return_value="sig")
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)
        monkeypatch.setattr(ws_module, "_resolve_tokens_to_sell", AsyncMock(return_value=1000.0))

        ok = await ws_module.process_sell_and_notify(
            "MINT123ABC", "MET", "TAKE_PROFIT", 20.0, sell_pct=25.0
        )

        assert ok is True
        # Vendido el 25%: al executor le quedan 750 tokens y al tracker el 75% del
        # capital. Antes el executor seguía afirmando que teníamos los 1000.
        assert exec_pos.token_amount_ui == pytest.approx(750.0)
        assert pos.amount == pytest.approx(0.0375)
        assert "MINT123ABC" in tracker.executor.positions

    async def test_la_venta_parcial_avisa_a_los_hooks_de_cierre(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """El cierre del tracker (parcial) debe descontar el acumulado del trader.

        Es el mismo cierre que en `_finalize_full_sell` dispara los hooks: sin
        esto, un TP al 50% dejaba al bot creyendo que el trader aún tenía el
        100% y la siguiente venta copiaba un porcentaje cada vez más pequeño.
        """
        notifier = _make_notifier()
        monkeypatch.setattr("config.load_config", lambda: _fake_cfg(dry_run=False))
        monkeypatch.setattr(
            "core.notifier.TelegramNotifier", lambda **kwargs: notifier
        )
        pos = SimpleNamespace(amount=0.05, buy_price=0.001, mint="MINT123ABC")
        tracker = MagicMock()
        tracker.get_position.return_value = pos
        tracker.executor.positions = {}
        tracker.executor.sell_token = AsyncMock(return_value="sig")
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)
        monkeypatch.setattr(ws_module, "_resolve_tokens_to_sell", AsyncMock(return_value=1000.0))

        fired: list[tuple[str, str, float, str]] = []
        tracker._fire_close_hooks = lambda p, pct, reason: fired.append(
            (p.mint, reason, pct, "hook")
        )

        ok = await ws_module.process_sell_and_notify(
            "MINT123ABC", "MET", "TAKE_PROFIT", 20.0, sell_pct=50.0
        )

        assert ok is True
        assert fired == [("MINT123ABC", "TAKE_PROFIT", 50.0, "hook")]
        # La posición sobrevive (es parcial) pero con el capital ya descontado.
        tracker.remove_position.assert_not_called()
        assert pos.amount == pytest.approx(0.025)

    async def test_la_venta_parcial_no_avisa_si_la_cuenta_es_la_estrategia(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Con `account_source=False` la estrategia ya descontó su propio libro."""
        notifier = _make_notifier()
        monkeypatch.setattr("config.load_config", lambda: _fake_cfg(dry_run=False))
        monkeypatch.setattr(
            "core.notifier.TelegramNotifier", lambda **kwargs: notifier
        )
        pos = SimpleNamespace(amount=0.05, buy_price=0.001, mint="MINT123ABC")
        tracker = MagicMock()
        tracker.get_position.return_value = pos
        tracker.executor.positions = {}
        tracker.executor.sell_token = AsyncMock(return_value="sig")
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)
        monkeypatch.setattr(ws_module, "_resolve_tokens_to_sell", AsyncMock(return_value=1000.0))

        fired: list[tuple[str, str, float, str]] = []
        tracker._fire_close_hooks = lambda p, pct, reason: fired.append(
            (p.mint, reason, pct, "hook")
        )

        await ws_module.process_sell_and_notify(
            "MINT123ABC", "MET", "COPY_TRADE_SELL", 20.0, sell_pct=50.0, account_source=False
        )

        assert fired == []


class TestProcessSellAndNotify:
    """Venta y notificación de salidas TP/SL."""

    def _fake_cfg(self, dry_run: bool) -> SimpleNamespace:
        return SimpleNamespace(
            trading=SimpleNamespace(DRY_RUN=dry_run),
            telegram=SimpleNamespace(TELEGRAM_TOKEN="t", TELEGRAM_CHAT_ID="c"),
        )

    async def test_dry_run_no_notifica_tp_ni_stats(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """En DRY_RUN el TP del tracker se simula en silencio (sin Telegram ni stats)."""
        notifier = _make_notifier()
        monkeypatch.setattr("config.load_config", lambda: self._fake_cfg(dry_run=True))
        monkeypatch.setattr(
            "core.notifier.TelegramNotifier", lambda **kwargs: notifier
        )
        tracker = MagicMock()
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)

        ok = await ws_module.process_sell_and_notify("MINT123ABC", "MET", "TAKE_PROFIT", 150.0)

        assert ok is True
        notifier.send_take_profit.assert_not_awaited()
        # El cierre pasa por remove_position (punto único) para que los hooks
        # que descuentan la contabilidad del trader copiado se disparen siempre.
        tracker.remove_position.assert_called_once()
        assert tracker.remove_position.call_args.args[0] == "MINT123ABC"

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

    async def test_real_tp_sigue_notificando(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """En modo REAL el TP del tracker sí notifica (resultado real)."""
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

        ok = await ws_module.process_sell_and_notify("MINT123ABC", "MET", "TAKE_PROFIT", 59.14)

        assert ok is True
        notifier.send_take_profit.assert_awaited_once()

    async def test_dry_run_tp_no_registra_stats(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """El TP simulado en DRY_RUN no contamina las stats de /stats."""
        stats_mock = MagicMock()
        monkeypatch.setattr(
            "core.stats.get_trade_stats", lambda: stats_mock
        )
        monkeypatch.setattr("config.load_config", lambda: self._fake_cfg(dry_run=True))
        monkeypatch.setattr(
            "core.notifier.TelegramNotifier", lambda **kwargs: _make_notifier()
        )
        pos = SimpleNamespace(
            buy_price=0.001, amount=0.05, symbol="MET",
            source_wallet="", created_at=0.0,
        )
        tracker = MagicMock()
        tracker.get_position.return_value = pos
        tracker.positions.pop = MagicMock()
        tracker.executor._save_exec_positions = MagicMock()
        tracker._save_positions = MagicMock()
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)

        ok = await ws_module.process_sell_and_notify(
            "MINT123ABC", "MET", "TAKE_PROFIT", 59.14
        )

        assert ok is True
        stats_mock.record_sell.assert_not_called()

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

    async def test_dry_run_no_notifica_salidas_varias(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """TIME_EXPIRED/TRAILING_STOP en DRY_RUN tampoco notifican."""
        notifier = _make_notifier()
        monkeypatch.setattr("config.load_config", lambda: self._fake_cfg(dry_run=True))
        monkeypatch.setattr(
            "core.notifier.TelegramNotifier", lambda **kwargs: notifier
        )
        tracker = MagicMock()
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)

        ok = await ws_module.process_sell_and_notify("MINT123ABC", "MET", "TIME_EXPIRED", 5.0)

        assert ok is True
        notifier.send_status.assert_not_awaited()
        notifier.send_trailing_stop.assert_not_awaited()


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

        notifier = _make_notifier()
        monkeypatch.setattr("core.notifier.TelegramNotifier", lambda **kwargs: notifier)

        tracker = MagicMock()
        tracker.executor = executor
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

        notifier = _make_notifier()
        monkeypatch.setattr("core.notifier.TelegramNotifier", lambda **kwargs: notifier)

        tracker = MagicMock()
        tracker.executor = executor
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)

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
        self.notifier = _make_notifier()
        monkeypatch.setattr("core.notifier.TelegramNotifier", lambda **kwargs: self.notifier)
        self.tracker = MagicMock()
        self.tracker.executor = self.executor
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


class TestCheckLiquidity:
    """Verificación de liquidez mínima antes de comprar tokens pump.fun."""

    async def test_token_no_pump_devuelve_true(self, monkeypatch) -> None:
        from core.websocket import check_liquidity

        assert await check_liquidity("So11111111111111111111111111111111") is True

    async def test_pump_con_liquidez_suficiente(self, monkeypatch) -> None:
        from core.websocket import check_liquidity

        class _FakeResp:
            status = 200

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def json(self):
                return {"virtual_sol_reserves": 10_000_000_000}  # 10 SOL

        session = MagicMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        session.get = MagicMock(return_value=_FakeResp())
        monkeypatch.setattr(aiohttp, "ClientSession", lambda **kw: session)

        assert await check_liquidity("TEST123pump") is True

    async def test_pump_con_liquidez_insuficiente(self, monkeypatch) -> None:
        from core.websocket import check_liquidity

        class _FakeResp:
            status = 200

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def json(self):
                return {"virtual_sol_reserves": 100_000}  # 0.0001 SOL

        session = MagicMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        session.get = MagicMock(return_value=_FakeResp())
        monkeypatch.setattr(aiohttp, "ClientSession", lambda **kw: session)

        assert await check_liquidity("TEST123pump") is False

    async def test_pump_api_error_devuelve_true(self, monkeypatch) -> None:
        from core.websocket import check_liquidity

        session = MagicMock()
        session.__aenter__ = AsyncMock(side_effect=RuntimeError("network"))
        session.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(aiohttp, "ClientSession", lambda **kw: session)

        assert await check_liquidity("TEST123pump") is True

    async def test_pump_status_no_200_devuelve_true(self, monkeypatch) -> None:
        from core.websocket import check_liquidity

        class _FakeResp:
            status = 500

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

        session = MagicMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        session.get = MagicMock(return_value=_FakeResp())
        monkeypatch.setattr(aiohttp, "ClientSession", lambda **kw: session)

        assert await check_liquidity("TEST123pump") is True


class TestProcessBuySinLiquidez:
    """Cuando buy_token devuelve None (sin liquidez)."""

    async def test_sig_none_no_registra_posicion(self, monkeypatch) -> None:
        s = _Scaffold(monkeypatch)
        s.executor.buy_token = AsyncMock(return_value=None)
        s.executor.get_token_price = AsyncMock(return_value=0.001)
        s.executor.positions = {}

        await ws_module.process_buy_and_notify("MINT123ABC", "MET", score=5.0)

        s.tracker.add_position.assert_not_called()
        s.notifier.send_buy.assert_not_awaited()


class TestResolucionSaldoVenta:
    """La cantidad a vender sale del SALDO REAL, no de una posición incompleta.

    Regresión del bug "no vende": con `buy_price == 0` (entrada pendiente) o
    con la posición solo en el executor, la estimación `amount / buy_price`
    daba 0 tokens y la venta se omitía entera.
    """

    def _tracker(self, pos: object | None, chain_balance: float) -> MagicMock:
        tracker = MagicMock()
        tracker.get_position.return_value = pos
        tracker.positions = {}
        tracker._save_positions = MagicMock()
        tracker.executor.positions = {}
        tracker.executor._save_exec_positions = MagicMock()
        tracker.executor.get_token_balance_ui = AsyncMock(return_value=chain_balance)
        tracker.executor.sell_token = AsyncMock(return_value="sig_ok")
        return tracker

    def _setup(self, monkeypatch: pytest.MonkeyPatch, tracker: MagicMock) -> MagicMock:
        monkeypatch.setattr(
            "config.load_config",
            lambda: SimpleNamespace(
                trading=SimpleNamespace(DRY_RUN=False),
                telegram=SimpleNamespace(TELEGRAM_TOKEN="t", TELEGRAM_CHAT_ID="c"),
            ),
        )
        notifier = _make_notifier()
        monkeypatch.setattr("core.notifier.TelegramNotifier", lambda **kw: notifier)
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker)
        return notifier

    async def test_entrada_pendiente_usa_saldo_onchain(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """buy_price == 0: antes la venta se omitía; ahora usa el saldo real."""
        tracker = self._tracker(
            SimpleNamespace(amount=0.01, buy_price=0.0, symbol="MET", source_wallet="", created_at=0.0),
            chain_balance=1234.0,
        )
        self._setup(monkeypatch, tracker)

        ok = await ws_module.process_sell_and_notify("MINT123ABC", "MET", "COPY_TRADE_SELL", 10.0)

        assert ok is True
        tracker.executor.sell_token.assert_awaited_once_with("MINT123ABC", 1234.0)

    async def test_solo_en_executor_tambien_vende(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Posición solo en executor.positions (p. ej. tras reinicio): se vende."""
        tracker = self._tracker(None, chain_balance=500.0)
        tracker.executor.positions = {
            "MINT123ABC": SimpleNamespace(token_amount_ui=500.0, entry_price=2e-5, sol_invested=0.01)
        }
        self._setup(monkeypatch, tracker)

        ok = await ws_module.process_sell_and_notify("MINT123ABC", "MET", "COPY_TRADE_SELL", 10.0)

        assert ok is True
        tracker.executor.sell_token.assert_awaited_once_with("MINT123ABC", 500.0)

    async def test_rpc_fallido_usa_estimacion_de_la_posicion(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Si el RPC no responde, se vende con lo estimado (no se pierde la salida)."""
        tracker = self._tracker(
            SimpleNamespace(amount=0.05, buy_price=0.001, symbol="MET", source_wallet="", created_at=0.0),
            chain_balance=-1.0,
        )
        self._setup(monkeypatch, tracker)

        ok = await ws_module.process_sell_and_notify("MINT123ABC", "MET", "COPY_TRADE_SELL", 10.0)

        assert ok is True
        tracker.executor.sell_token.assert_awaited_once_with("MINT123ABC", 50.0)

    async def test_sin_saldo_onde_sea_no_cierra_posicion(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ni on-chain ni posición: la venta se omite y se devuelve False."""
        tracker = self._tracker(None, chain_balance=0.0)
        self._setup(monkeypatch, tracker)

        ok = await ws_module.process_sell_and_notify("MINT123ABC", "MET", "COPY_TRADE_SELL", 10.0)

        assert ok is False
        tracker.executor.sell_token.assert_not_awaited()

    async def test_venta_no_confirmada_no_cierra_posicion(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Orden enviada pero sin confirmar: la posición NO se da por cerrada."""
        pos = SimpleNamespace(
            amount=0.05, buy_price=0.001, symbol="MET", source_wallet="", created_at=0.0
        )
        tracker = self._tracker(pos, chain_balance=50.0)
        tracker.executor.sell_token = AsyncMock(return_value=None)
        tracker.executor.positions = MagicMock()
        tracker.positions = MagicMock()
        self._setup(monkeypatch, tracker)

        ok = await ws_module.process_sell_and_notify("MINT123ABC", "MET", "COPY_TRADE_SELL", 10.0)

        assert ok is False
        tracker.executor.positions.pop.assert_not_called()
        tracker.positions.pop.assert_not_called()

    async def test_sell_pct_parcial_escala_el_saldo_real(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        tracker = self._tracker(
            SimpleNamespace(amount=0.05, buy_price=0.001, symbol="MET", source_wallet="", created_at=0.0),
            chain_balance=1000.0,
        )
        self._setup(monkeypatch, tracker)

        ok = await ws_module.process_sell_and_notify(
            "MINT123ABC", "MET", "COPY_TRADE_SELL", 10.0, sell_pct=25.0
        )

        assert ok is True
        tracker.executor.sell_token.assert_awaited_once_with("MINT123ABC", 250.0)


class TestTrailingStopNotification:
    """process_sell_and_notify con reason=TRAILING_STOP notifica correctamente."""

    async def test_trailing_stop_dry_run(self, monkeypatch) -> None:
        from core.websocket import process_sell_and_notify as real_sell

        monkeypatch.setattr(ws_module.config, "load_config", lambda: _fake_buy_cfg(dry_run=True))
        notifier = _make_notifier()
        monkeypatch.setattr(
            "core.notifier.TelegramNotifier",
            lambda **kw: notifier,
        )
        tracker_mock = MagicMock()
        tracker_mock.get_position.return_value = None
        monkeypatch.setattr("core.tracker.get_global_tracker", lambda: tracker_mock)

        await real_sell("MINT123", "MET", reason="TRAILING_STOP", pnl=25.0)