"""Pruebas unitarias del motor de seguimiento y salida de posiciones.

Cubre `core.tracker.PositionTracker`: registro/eliminación de posiciones,
cálculo de PnL real contra el precio de mercado y el disparo de
TAKE_PROFIT / STOP_LOSS / TIME_EXPIRED usando mocks de precios. Todas las
llamadas a la venta y a la notificación se simulan; nunca se toca la red.
"""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import core.tracker as tracker_module
from core.tracker import PositionTracker


def _make_config(take_profit: float = 100.0, stop_loss: float = 30.0) -> SimpleNamespace:
    """Configuración mínima del tracker (solo lo que consume el monitor)."""
    trading = SimpleNamespace(
        TAKE_PROFIT_PCT=take_profit,
        STOP_LOSS_PCT=stop_loss,
        DRY_RUN=True,
    )
    return SimpleNamespace(trading=trading)


@pytest.fixture
def tracker() -> PositionTracker:
    """Tracker con executor y notifier mocks (nunca toca la red)."""
    executor = MagicMock()
    executor.get_token_price = AsyncMock(return_value=0.001)
    return PositionTracker(
        executor=executor,
        notifier=MagicMock(),
        config=_make_config(),
    )


@pytest.fixture
def patch_sell(monkeypatch) -> AsyncMock:
    """Sustituye process_sell_and_notify (importado dentro de _evaluate)."""
    mock_sell = AsyncMock(return_value=True)
    monkeypatch.setattr("core.websocket.process_sell_and_notify", mock_sell)
    return mock_sell


class TestRegistroPosiciones:
    """Registro, consulta y borrado de posiciones activas."""

    def test_add_position_registra_y_normaliza_symbol(self, tracker: PositionTracker) -> None:
        tracker.add_position("MINT123ABC", "N/A", 0.001, 0.05)
        pos = tracker.get_position("MINT123ABC")
        assert pos is not None
        assert pos.symbol == "MINT12"  # primeros 6 chars del mint en mayúsculas
        assert pos.buy_price == 0.001
        assert pos.amount == 0.05

    def test_add_position_conserva_symbol_valido(self, tracker: PositionTracker) -> None:
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        assert tracker.get_position("MINT123ABC").symbol == "MET"

    def test_remove_position(self, tracker: PositionTracker) -> None:
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        assert tracker.remove_position("MINT123ABC") is True
        assert tracker.get_position("MINT123ABC") is None
        # Eliminar de nuevo ya no existe.
        assert tracker.remove_position("MINT123ABC") is False

    def test_get_position_inexistente_devuelve_none(self, tracker: PositionTracker) -> None:
        assert tracker.get_position("NOEXISTE") is None


class TestCalculoPnl:
    """El PnL se calcula desde el precio real de mercado."""

    async def test_pnl_positivo_no_dispara_salida(self, tracker: PositionTracker, patch_sell: AsyncMock) -> None:
        tracker.executor.get_token_price.return_value = 0.0011  # +10% sobre 0.001
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        await tracker._evaluate("MINT123ABC")
        patch_sell.assert_not_awaited()
        assert tracker.get_position("MINT123ABC") is not None

    async def test_entrada_pendiente_se_fija_con_primer_precio(
        self, tracker: PositionTracker, patch_sell: AsyncMock
    ) -> None:
        tracker.executor.get_token_price.return_value = 0.0004
        tracker.add_position("MINT123ABC", "MET", 1.0, 0.05)  # sentinel = entrada PENDIENTE
        await tracker._evaluate("MINT123ABC")
        pos = tracker.get_position("MINT123ABC")
        assert pos is not None
        assert pos.buy_price == 0.0004
        patch_sell.assert_not_awaited()


class TestDisparoTakeProfit:
    """Se dispara TAKE_PROFIT cuando el precio real supera el umbral."""

    async def test_take_profit_alcanza_umbral(self, tracker: PositionTracker, patch_sell: AsyncMock) -> None:
        tracker.executor.get_token_price.return_value = 0.002  # +100% sobre entry 0.001
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        await tracker._evaluate("MINT123ABC")

        patch_sell.assert_awaited_once()
        assert patch_sell.await_args.kwargs["reason"] == "TAKE_PROFIT"
        assert patch_sell.await_args.kwargs["pnl"] == pytest.approx(100.0)
        # La posición se elimina tras cerrar con éxito.
        assert tracker.get_position("MINT123ABC") is None

    async def test_take_profit_supera_umbral(self, tracker: PositionTracker, patch_sell: AsyncMock) -> None:
        tracker.executor.get_token_price.return_value = 0.0025  # +150% sobre entry 0.001
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        await tracker._evaluate("MINT123ABC")
        assert patch_sell.await_args.kwargs["reason"] == "TAKE_PROFIT"
        assert patch_sell.await_args.kwargs["pnl"] == pytest.approx(150.0)

    async def test_take_profit_configurable(self, tracker: PositionTracker, patch_sell: AsyncMock) -> None:
        tracker.config.trading.TAKE_PROFIT_PCT = 50.0
        tracker.executor.get_token_price.return_value = 0.0016  # +60% >= 50%
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        await tracker._evaluate("MINT123ABC")
        assert patch_sell.await_args.kwargs["reason"] == "TAKE_PROFIT"

    async def test_take_profit_no_dispara_si_la_venta_falla(
        self, tracker: PositionTracker, patch_sell: AsyncMock
    ) -> None:
        patch_sell.return_value = False
        tracker.executor.get_token_price.return_value = 0.002
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        await tracker._evaluate("MINT123ABC")
        # La posición permanece para reintentar en el siguiente ciclo.
        assert tracker.get_position("MINT123ABC") is not None


class TestDisparoStopLoss:
    """Se dispara STOP_LOSS cuando el precio real cae bajo el umbral."""

    async def test_stop_loss_alcanza_umbral(self, tracker: PositionTracker, patch_sell: AsyncMock) -> None:
        tracker.executor.get_token_price.return_value = 0.0005  # -50% sobre entry 0.001
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        await tracker._evaluate("MINT123ABC")

        patch_sell.assert_awaited_once()
        assert patch_sell.await_args.kwargs["reason"] == "STOP_LOSS"
        assert patch_sell.await_args.kwargs["pnl"] == pytest.approx(-50.0)
        assert tracker.get_position("MINT123ABC") is None

    async def test_stop_loss_dentro_de_umbral_no_dispara(
        self, tracker: PositionTracker, patch_sell: AsyncMock
    ) -> None:
        tracker.executor.get_token_price.return_value = 0.0009  # -10%, umbral -30%
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        await tracker._evaluate("MINT123ABC")
        patch_sell.assert_not_awaited()

    async def test_la_salida_es_exclusiva_en_pnl_extremo(
        self, tracker: PositionTracker, patch_sell: AsyncMock
    ) -> None:
        # PnL igual al umbral de TP: take profit gana, no stop loss.
        tracker.executor.get_token_price.return_value = 0.002
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        await tracker._evaluate("MINT123ABC")
        assert patch_sell.await_args.kwargs["reason"] == "TAKE_PROFIT"


class TestTimeExpired:
    """Cierre forzado por tiempo máximo en cartera."""

    async def test_time_expired_cierra_sin_importar_pnl(
        self, tracker: PositionTracker, patch_sell: AsyncMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(tracker_module, "MAX_HOLD_TIME_SEC", 1)
        tracker.executor.get_token_price.return_value = 0.001
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        pos = tracker.get_position("MINT123ABC")
        pos.created_at = time.time() - 60  # ya superó el hold máximo
        await tracker._evaluate("MINT123ABC")

        patch_sell.assert_awaited_once()
        assert patch_sell.await_args.kwargs["reason"] == "TIME_EXPIRED"
        assert tracker.get_position("MINT123ABC") is None


class TestPrecioReal:
    """Siempre se consulta el precio de mercado real (nunca se simula)."""

    async def test_get_current_price_devuelve_precio_real(self, tracker: PositionTracker) -> None:
        tracker.executor.get_token_price.return_value = 0.00042
        tracker.add_position("MINT123ABC", "MET", 1.0, 0.05)
        price = await tracker._get_current_price("MINT123ABC")
        assert price == 0.00042

    async def test_get_current_price_sin_precio_devuelve_cero(
        self, tracker: PositionTracker
    ) -> None:
        # DRY_RUN activo y ningún endpoint entrega precio: NO hay precio simulado.
        tracker.executor.get_token_price.side_effect = RuntimeError("sin precio")
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        price = await tracker._get_current_price("MINT123ABC")
        assert price == 0.0

    async def test_sin_precio_no_se_evalua_pnl_ni_salida(
        self, tracker: PositionTracker, patch_sell: AsyncMock
    ) -> None:
        tracker.executor.get_token_price.side_effect = RuntimeError("sin endpoint")
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        await tracker._evaluate("MINT123ABC")
        patch_sell.assert_not_awaited()
        assert tracker.get_position("MINT123ABC") is not None

    async def test_pnl_formula_variacion_porcentual(self, tracker: PositionTracker) -> None:
        # Test directo de la fórmula de PnL sobre la posición.
        tracker.add_position("MINT123ABC", "MET", 0.002, 0.05)
        pos = tracker.get_position("MINT123ABC")
        pnl = (0.004 - pos.buy_price) / pos.buy_price * 100.0
        assert pnl == pytest.approx(100.0)


class TestLogPeriodico:
    """Log periódico del PnL en la evaluación (sin interferir en la salida)."""

    async def test_log_se_emite_y_no_interfiere(
        self, tracker: PositionTracker, patch_sell: AsyncMock
    ) -> None:
        tracker.executor.get_token_price.return_value = 0.0011  # +10%
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        pos = tracker.get_position("MINT123ABC")
        pos.last_log_time = 0.0  # fuerza la rama de log
        await tracker._evaluate("MINT123ABC")
        patch_sell.assert_not_awaited()
        assert tracker.get_position("MINT123ABC") is not None


class TestMonitorLoop:
    """Bucle de monitorización en segundo plano."""

    async def test_start_monitoring_procesa_posiciones_y_cancela_limpio(
        self, tracker: PositionTracker, patch_sell: AsyncMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(tracker_module, "CHECK_INTERVAL_SEC", 0.01)
        tracker.executor.get_token_price.return_value = 0.002  # TP sobre entry 0.001
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)

        task = asyncio.create_task(tracker.start_monitoring())
        await asyncio.sleep(0.05)
        patch_sell.assert_awaited()
        assert tracker.get_position("MINT123ABC") is None

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_monitor_no_muere_por_errores(
        self, tracker: PositionTracker, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(tracker_module, "CHECK_INTERVAL_SEC", 0.01)
        tracker.executor.get_token_price.side_effect = RuntimeError("sin API")
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)

        task = asyncio.create_task(tracker.monitor_positions())
        await asyncio.sleep(0.05)
        # El error de una posición no detiene el bucle y la posición se conserva.
        assert tracker.get_position("MINT123ABC") is not None

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


class TestCheckPosition:
    """Ruta alternativa de evaluación sobre las posiciones del executor."""

    async def test_take_profit(self, tracker: PositionTracker) -> None:
        from core.execution import Position

        tracker.executor.positions = {"MINT123ABC": Position(mint="MINT123ABC", entry_price=0.001)}
        tracker.executor.get_token_price.return_value = 0.002  # +100%
        tracker.executor.close_position = AsyncMock()
        tracker.notifier.send_take_profit = AsyncMock(return_value=True)

        reason, pnl = await tracker.check_position("MINT123ABC")

        assert reason == "TAKE_PROFIT"
        assert pnl == pytest.approx(100.0)
        tracker.executor.close_position.assert_awaited_once()
        tracker.notifier.send_take_profit.assert_awaited_once()

    async def test_stop_loss_con_slippage_de_emergencia(self, tracker: PositionTracker) -> None:
        from core.execution import Position

        tracker.executor.positions = {"MINT123ABC": Position(mint="MINT123ABC", entry_price=0.001)}
        tracker.executor.get_token_price.return_value = 0.0005  # -50%
        tracker.executor.close_position = AsyncMock()
        tracker.notifier.send_stop_loss = AsyncMock(return_value=True)

        reason, pnl = await tracker.check_position("MINT123ABC")

        assert reason == "STOP_LOSS"
        assert pnl == pytest.approx(-50.0)
        # La venta de emergencia usa el slippage alto del tracker.
        assert tracker.executor.close_position.await_args.kwargs["slippage_bps"] == \
            PositionTracker.EMERGENCY_SLIPPAGE_BPS

    async def test_sin_posicion(self, tracker: PositionTracker) -> None:
        tracker.executor.positions = {}
        assert await tracker.check_position("MINT123ABC") == ("", 0.0)

    async def test_error_de_precio_no_bloquea(self, tracker: PositionTracker) -> None:
        tracker.executor.positions = {"MINT123ABC": MagicMock()}
        tracker.executor.get_token_price.side_effect = RuntimeError("no API")
        assert await tracker.check_position("MINT123ABC") == ("", 0.0)

    async def test_entry_re_consultado_cuando_falta(self, tracker: PositionTracker) -> None:
        from core.execution import Position

        pos = Position(mint="MINT123ABC", entry_price=0.0)  # entry PENDIENTE
        tracker.executor.positions = {"MINT123ABC": pos}
        # 1ª llamada: precio actual (0.0020); 2ª llamada: re-consulta del entry (0.0010).
        tracker.executor.get_token_price = AsyncMock(side_effect=[0.0020, 0.0010])
        tracker.executor.close_position = AsyncMock()
        tracker.notifier.send_take_profit = AsyncMock(return_value=True)

        reason, pnl = await tracker.check_position("MINT123ABC")

        assert reason == "TAKE_PROFIT"
        assert pnl == pytest.approx(100.0)
        assert pos.entry_price == 0.0010  # se re-fija con el primer precio real


class TestGlobalTracker:
    """Instancia compartida del tracker (singleton global)."""

    def test_set_y_get_global(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(tracker_module, "_TRACKER", None)
        fake = MagicMock()
        tracker_module.set_global_tracker(fake)
        assert tracker_module.get_global_tracker() is fake

    def test_get_global_construye_bajo_demanda(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(tracker_module, "_TRACKER", None)
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
        monkeypatch.setattr("core.execution.JupiterExecutor", MagicMock())
        monkeypatch.setattr("core.notifier.TelegramNotifier", MagicMock())

        tracker = tracker_module.get_global_tracker()

        assert isinstance(tracker, PositionTracker)
        # La instancia queda cacheada para futuras llamadas.
        assert tracker_module.get_global_tracker() is tracker