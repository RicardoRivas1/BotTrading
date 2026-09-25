"""Pruebas unitarias del motor de seguimiento y salida de posiciones.

Cubre `core.tracker.PositionTracker`: registro/eliminación de posiciones,
cálculo de PnL real contra el precio de mercado y el disparo de
TAKE_PROFIT / STOP_LOSS / TIME_EXPIRED usando mocks de precios. Todas las
llamadas a la venta y a la notificación se simulan; nunca se toca la red.
"""

import asyncio
import time
from types import SimpleNamespace
from typing import Optional
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
def tracker(monkeypatch: pytest.MonkeyPatch) -> PositionTracker:
    """Tracker con executor y notifier mocks (nunca toca la red)."""
    executor = MagicMock()
    executor.get_token_price = AsyncMock(return_value=0.001)
    # Neutralizar la consulta directa a la bonding curve de Pump.fun: sin red.
    monkeypatch.setattr(tracker_module, "get_pumpfun_price", AsyncMock(return_value=None))
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


class TestCascadaPrecio:
    """La cotización se obtiene en cascada: Pump.fun primero, luego el executor."""

    async def test_pumpfun_es_la_primera_fuente(
        self, tracker: PositionTracker, patch_sell: AsyncMock
    ) -> None:
        tracker_module.get_pumpfun_price = AsyncMock(return_value=0.002)  # +100%
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        await tracker._evaluate("MINT123ABC")

        assert isinstance(tracker_module.get_pumpfun_price, AsyncMock)
        tracker_module.get_pumpfun_price.assert_awaited_once_with("MINT123ABC")
        tracker.executor.get_token_price.assert_not_awaited()
        assert patch_sell.await_args.kwargs["reason"] == "TAKE_PROFIT"

    async def test_fallback_al_executor_si_pumpfun_falla(
        self, tracker: PositionTracker, patch_sell: AsyncMock
    ) -> None:
        tracker_module.get_pumpfun_price = AsyncMock(return_value=None)
        tracker.executor.get_token_price.return_value = 0.002
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        await tracker._evaluate("MINT123ABC")

        tracker.executor.get_token_price.assert_awaited_once()
        assert patch_sell.await_args.kwargs["reason"] == "TAKE_PROFIT"

    async def test_sin_precio_conserva_la_ultima_cotizacion(
        self, tracker: PositionTracker, patch_sell: AsyncMock
    ) -> None:
        # Ambas fuentes fallan y ya existía una cotización conocida previa.
        tracker_module.get_pumpfun_price = AsyncMock(return_value=None)
        tracker.executor.get_token_price.side_effect = RuntimeError("sin API")
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        tracker.get_position("MINT123ABC").current_price = 0.0015  # últ. conocida

        await tracker._evaluate("MINT123ABC")

        pos = tracker.get_position("MINT123ABC")
        assert pos is not None
        # Se conserva el último precio válido y jamás se iguala a la entrada.
        assert pos.current_price == pytest.approx(0.0015)
        assert pos.current_price != pos.buy_price
        patch_sell.assert_not_awaited()

    async def test_pumpfun_precio_desde_reservas_virtuales(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """get_pumpfun_price calcula SOL por token con los decimales de Solana."""

        class _FakeResp:
            status = 200

            async def __aenter__(self) -> "_FakeResp":
                return self

            async def __aexit__(self, *exc_info: object) -> bool:
                return False

            async def json(self, content_type: Optional[str] = None) -> dict[str, int]:
                return {
                    "virtual_sol_reserves": 25_125_850_000,
                    "virtual_token_reserves": 707_770_150_000_000,
                }

        session = MagicMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        session.get = MagicMock(return_value=_FakeResp())
        monkeypatch.setattr("aiohttp.ClientSession", lambda *args, **kwargs: session)

        price = await tracker_module.get_pumpfun_price("MINT123ABCpump")

        vsol = 25_125_850_000 / 1e9
        vtok = 707_770_150_000_000 / 1e6
        assert price is not None
        assert price == pytest.approx(vsol / vtok)


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


class TestBackoffDePrecio:
    """Un precio que no llega NO puede martillear la API cada 2 segundos.

    El síntoma era un bot que "se queda pegado": el monitor secuencial esperaba
    a cada proveedor caído y, con muchas posiciones, el ciclo entero se iba en
    timeouts. Con el backoff, la posición se reintenta cada
    PRICE_RETRY_BACKOFF_SEC y el ciclo no se bloquea.
    """

    async def test_precio_ausente_arma_el_reintento(
        self, tracker: PositionTracker, patch_sell: AsyncMock
    ) -> None:
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        tracker.executor.get_token_price = AsyncMock(return_value=0.0)

        await tracker._evaluate("MINT123ABC")

        assert tracker.get_position("MINT123ABC").price_retry_at > 0.0
        assert tracker.executor.get_token_price.await_count == 1

    async def test_durante_el_backoff_no_se_vuelve_a_consultar(
        self, tracker: PositionTracker, patch_sell: AsyncMock
    ) -> None:
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        tracker.executor.get_token_price = AsyncMock(return_value=0.0)

        await tracker._evaluate("MINT123ABC")
        await tracker._evaluate("MINT123ABC")
        await tracker._evaluate("MINT123ABC")

        # Una sola consulta para tres ciclos: el proveedor caido no se reintenta.
        assert tracker.executor.get_token_price.await_count == 1
        # Y la posicion sigue viva (no se cierra por falta de precio).
        assert tracker.get_position("MINT123ABC") is not None

    async def test_un_precio_valido_reabre_el_ciclo(
        self, tracker: PositionTracker, patch_sell: AsyncMock
    ) -> None:
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)
        tracker.executor.get_token_price = AsyncMock(side_effect=[0.0, 0.0015])

        await tracker._evaluate("MINT123ABC")
        tracker.get_position("MINT123ABC").price_retry_at = 0.0
        await tracker._evaluate("MINT123ABC")

        assert tracker.executor.get_token_price.await_count == 2
        assert tracker.get_position("MINT123ABC").latest_pnl_pct == pytest.approx(50.0)


class TestHookDeCierre:
    """remove_position avisa a los hooks con el % realmente vendido."""

    def test_hook_recibe_mint_wallet_porcentaje_y_motivo(
        self, tracker: PositionTracker
    ) -> None:
        calls: list[tuple[str, str, float, str]] = []
        tracker.register_close_hook(
            lambda mint, wallet, pct, reason: calls.append((mint, wallet, pct, reason))
        )
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)

        assert tracker.remove_position("MINT123ABC", reason="TAKE_PROFIT") is True

        assert calls == [("MINT123ABC", "", 100.0, "TAKE_PROFIT")]

    def test_hook_recibe_el_porcentaje_parcial(
        self, tracker: PositionTracker
    ) -> None:
        seen: list[float] = []
        tracker.register_close_hook(
            lambda mint, wallet, pct, reason: seen.append(pct)
        )
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)

        tracker.remove_position("MINT123ABC", reason="TRAILING_STOP", sold_pct=25.0)

        assert seen == [25.0]

    def test_un_hook_roto_no_impide_cierre_ni_otros_hooks(
        self, tracker: PositionTracker
    ) -> None:
        visto: list[str] = []

        def _roto(mint: str, wallet: str, pct: float, reason: str) -> None:
            raise RuntimeError("hook roto")

        tracker.register_close_hook(_roto)
        tracker.register_close_hook(lambda mint, wallet, pct, reason: visto.append(mint))
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)

        assert tracker.remove_position("MINT123ABC") is True
        assert tracker.get_position("MINT123ABC") is None
        assert visto == ["MINT123ABC"]

    def test_el_registro_marca_como_ya_registrado(
        self, tracker: PositionTracker
    ) -> None:
        cb = MagicMock()
        tracker.register_close_hook(cb)
        tracker.register_close_hook(cb)
        tracker.add_position("MINT123ABC", "MET", 0.001, 0.05)

        tracker.remove_position("MINT123ABC")

        cb.assert_called_once()


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


class TestTrailingStop:
    """Trailing stop: activación en umbral y venta por retroceso desde pico."""

    def _make_config(
        self,
        tp: float = 100.0,
        sl: float = 30.0,
        activation: float = 20.0,
        distance: float = 15.0,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            trading=SimpleNamespace(
                TAKE_PROFIT_PCT=tp,
                STOP_LOSS_PCT=sl,
                TRAILING_STOP_ACTIVATION_PCT=activation,
                TRAILING_STOP_DISTANCE_PCT=distance,
                DRY_RUN=True,
            ),
        )

    async def test_trailing_activa_y_dispara(
        self, monkeypatch: pytest.MonkeyPatch, patch_sell: AsyncMock
    ) -> None:
        """PnL supera umbral de activación y luego retroceso excede distancia."""
        cfg = self._make_config(activation=20.0, distance=15.0)
        cfg.trading.PRICE_POLL_FALLBACK_SECONDS = 0.0  # sin caché de precio
        monkeypatch.setattr(tracker_module, "get_pumpfun_price", AsyncMock(return_value=None))
        tracker = PositionTracker(
            executor=MagicMock(), notifier=MagicMock(), config=cfg,
        )
        tracker.executor.get_token_price = AsyncMock(return_value=0.001)
        tracker.add_position("MINT1", "MET", 0.001, 0.05)

        # Primer evaluate: precio actual = 0.0013 → PnL 30% → activa trailing
        tracker.executor.get_token_price.return_value = 0.0013
        await tracker._evaluate("MINT1")
        pos = tracker.get_position("MINT1")
        assert pos is not None
        assert pos.trailing_active is True
        patch_sell.assert_not_awaited()

        # Segundo evaluate: precio cae a 0.0011 → PnL 10% → drawdown 20% > 15%
        tracker.executor.get_token_price.return_value = 0.0011
        await tracker._evaluate("MINT1")
        patch_sell.assert_awaited_once()
        assert patch_sell.await_args.kwargs["reason"] == "TRAILING_STOP"

    async def test_trailing_no_activa_si_no_alcanza_umbral(
        self, monkeypatch: pytest.MonkeyPatch, patch_sell: AsyncMock
    ) -> None:
        """PnL no llega al umbral de activación: trailing permanece inactivo."""
        cfg = self._make_config(activation=20.0, distance=15.0)
        cfg.trading.PRICE_POLL_FALLBACK_SECONDS = 0.0
        monkeypatch.setattr(tracker_module, "get_pumpfun_price", AsyncMock(return_value=None))
        tracker = PositionTracker(
            executor=MagicMock(), notifier=MagicMock(), config=cfg,
        )
        tracker.executor.get_token_price = AsyncMock(return_value=0.001)
        tracker.add_position("MINT1", "MET", 0.001, 0.05)

        # PnL = 10% < 20% activation → no activa trailing
        tracker.executor.get_token_price.return_value = 0.0011
        await tracker._evaluate("MINT1")
        pos = tracker.get_position("MINT1")
        assert pos is not None
        assert pos.trailing_active is False
        patch_sell.assert_not_awaited()


class TestTimeExpiredSinPrecio:
    """TIME_EXPIRED para tokens muertos sin precio conocido."""

    async def test_time_expired_sin_precio_cierra(
        self, tracker: PositionTracker, patch_sell: AsyncMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(tracker_module, "MAX_HOLD_TIME_SEC", 1)
        # Sin precio de ninguna fuente
        tracker_module.get_pumpfun_price = AsyncMock(return_value=None)
        tracker.executor.get_token_price = AsyncMock(side_effect=RuntimeError("sin API"))
        tracker.add_position("MINT1", "MET", 0.001, 0.05)
        pos = tracker.get_position("MINT1")
        pos.created_at = time.time() - 60
        await tracker._evaluate("MINT1")
        patch_sell.assert_awaited_once()
        assert patch_sell.await_args.kwargs["reason"] == "TIME_EXPIRED"

    async def test_time_expired_sin_precio_no_cierra_si_vigente(
        self, tracker: PositionTracker, patch_sell: AsyncMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(tracker_module, "MAX_HOLD_TIME_SEC", 9999)
        tracker_module.get_pumpfun_price = AsyncMock(return_value=None)
        tracker.executor.get_token_price = AsyncMock(side_effect=RuntimeError("sin API"))
        tracker.add_position("MINT1", "MET", 0.001, 0.05)
        await tracker._evaluate("MINT1")
        patch_sell.assert_not_awaited()


class TestRefreshException:
    """Excepción en _refresh_position_price no bloquea el monitor."""

    async def test_excepcion_en_refresh_retorna_cero(
        self, tracker: PositionTracker, patch_sell: AsyncMock
    ) -> None:
        tracker_module.get_pumpfun_price = AsyncMock(side_effect=RuntimeError("boom"))
        tracker.executor.get_token_price = AsyncMock(side_effect=RuntimeError("boom"))
        tracker.add_position("MINT1", "MET", 0.001, 0.05)
        await tracker._evaluate("MINT1")
        pos = tracker.get_position("MINT1")
        assert pos is not None
        assert pos.current_price == 0.0