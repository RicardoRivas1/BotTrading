"""Pruebas de regresión del copy trading (clasificación y ejecución).

Cubre los fallos que hacían que el bot "se confundiera" en operaciones reales:

- Una VETA que no llega a ejecutarse (sin saldo, orden sin confirmar) NO puede
  cerrar la posición ni marcarse como ejecutada: si lo hacía, los tokens
  quedaban atrapados en la wallet y el bot perdía el registro para siempre
  ("no vende" y, al reintentar, "no compra").
- El % de venta se calcula siempre contra los tokens acumulados con la misma
  clave de trader con la que se anotan.
"""

from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from strategies.copy_trading import (
    CopyTradeSignal,
    CopyTradingStrategy,
    TrackedWallet,
)

MINT = "SoMeMiNt11111111111111111111111111111111111111pump"
TRADER = "Tr4d3rW4ll3t111111111111111111111111111111111"


def _config() -> SimpleNamespace:
    return SimpleNamespace(
        copy_trading=SimpleNamespace(
            COPY_TRADING_ENABLED=True,
            MAX_COPY_TRADE_SOL=0.01,
            MIN_COPY_TRADE_SOL=0.005,
            MAX_COPY_TRADE_POSITIONS=0,
            COPY_TRADE_BUY_DEDUP_SECONDS=3.0,
            COPY_TRADE_ALLOW_ACCUMULATE=False,
            COPY_TRADE_ORDER_BUFFER_SECONDS=0.0,
            COPY_TRADE_CAPITAL_PERCENT=0.0,
        ),
        trading=SimpleNamespace(DRY_RUN=False),
    )


def _strategy(tmp_path: Path) -> tuple[CopyTradingStrategy, MagicMock, MagicMock]:
    """Estrategia con executor/tracker simulados y dedup en disco aislado."""
    executor = MagicMock()
    executor.positions = {}
    executor.buy_amount_sol = 0.01
    executor.get_token_price = AsyncMock(return_value=0.0)
    executor.get_wallet_token_balance = AsyncMock(return_value=0.0)
    executor.get_sol_balance = AsyncMock(return_value=1.0)
    executor._save_exec_positions = MagicMock()
    executor.sell_token = AsyncMock(return_value="sig")

    tracker = MagicMock()
    tracker.positions = {}
    tracker.get_position = MagicMock(return_value=None)
    tracker.get_positions_by_wallet = MagicMock(return_value=[])
    tracker._save_positions = MagicMock()

    notifier = MagicMock()
    for meth in ("send_buy", "send_error", "send_status"):
        setattr(notifier, meth, AsyncMock(return_value=True))

    strategy = CopyTradingStrategy(
        executor=executor, notifier=notifier, tracker=tracker, config=_config()
    )
    # Archivos de dedup/bueno en un directorio temporal: la estrategia escribe
    # en disco al arrancar y no debe tocar los del repo.
    strategy._signatures_file = str(tmp_path / "sigs.json")
    strategy._diag_path = str(tmp_path / "diag.jsonl")
    strategy._executed_signatures = {}
    strategy.wallets = {TRADER: TrackedWallet(address=TRADER, label="trader")}
    return strategy, executor, tracker


def _sell_signal() -> CopyTradeSignal:
    return CopyTradeSignal(
        wallet=TRADER,
        action="sell",
        token_mint=MINT,
        token_symbol="TEST",
        amount_sol=0.0,
        tx_signature="sig_sell_1",
        block_time=1000.0,
        sell_sol_raw=0.0,
        trade_token_amount=1000.0,
        buy_sol_raw=0.0,
        trader_label="trader",
    )


class TestVentaNoEjecutada:
    """Si la venta no ocurrió, el estado NO puedeadvance como si hubiera."""

    async def test_venta_fallida_no_cierra_posicion_ni_marca_ejecutada(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, executor, tracker = _strategy(tmp_path)
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        sent = {"n": 0}

        async def _fake_sell(*args: object, **kwargs: object) -> bool:
            sent["n"] += 1
            return False  # la wallet no tenía saldo / la orden no se confirmó

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        stats_mock = MagicMock()
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: stats_mock)

        await strategy._execute_copy_trade(_sell_signal())

        assert sent["n"] == 1
        # La posición sigue viva: si se borrara, los tokens quedan huérfanos.
        assert MINT in strategy.executor.positions
        assert MINT in strategy.tracker.positions
        # La tx NO se marca como ejecutada: debe poder reintentarse.
        assert "sig_sell_1" not in strategy._executed_signatures
        # No se contabiliza una venta inexistente.
        stats_mock.record_sell.assert_not_called()
        # El % del trader se restaura para el reintento.
        assert strategy._wallet_mint_tokens[(TRADER, MINT)] == 1000.0

    async def test_venta_ok_cierra_posicion_y_marca_ejecutada(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, executor, tracker = _strategy(tmp_path)
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        async def _fake_sell(*args: object, **kwargs: object) -> bool:
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        stats_mock = MagicMock()
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: stats_mock)

        await strategy._execute_copy_trade(_sell_signal())

        assert MINT not in strategy.executor.positions
        assert MINT not in strategy.tracker.positions
        assert "sig_sell_1" in strategy._executed_signatures
        stats_mock.record_sell.assert_called_once()


class TestVentaParcial:
    """La venta parcial descuenta el capital UNA sola vez."""

    async def test_no_descuenta_dos_veces_el_invertido(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        exec_pos = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        tracker_pos = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        strategy.executor.positions[MINT] = exec_pos
        strategy.tracker.positions[MINT] = tracker_pos
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            # El helper compartido descuenta el capital del tracker; el test
            # reproduce ese descuento para detectar el doble.
            tracker_pos.amount = max(0.0, tracker_pos.amount * 0.75)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        signal = _sell_signal()
        signal.trade_token_amount = 250.0  # el trader vendio el 25%
        await strategy._execute_copy_trade(signal)

        # 0.01 -> 0.0075 (un solo descuento), no 0.005 (doble descuento).
        assert tracker_pos.amount == pytest.approx(0.0075)
        assert exec_pos.sol_invested == pytest.approx(0.0075)


class TestBuyEstancado:
    """El descarte de compras tardias solo aplica con blockTime conocido."""

    async def test_reentry_sin_blocktime_no_se_descarta(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Con blockTime ausente (0) el re-entry legitimo debe pasar."""
        strategy, executor, _tracker = _strategy(tmp_path)
        executor.buy_token = AsyncMock(return_value="sig_buy")
        executor.get_token_symbol = AsyncMock(return_value="TEST")
        executor.positions = {}

        async def _fake_sell(*args: object, **kwargs: object) -> bool:
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        # El trader ya vendio este token (registrado sin blockTime).
        strategy._last_sell_block[(TRADER, MINT)] = 0.0

        signal = CopyTradeSignal(
            wallet=TRADER,
            action="buy",
            token_mint=MINT,
            token_symbol="TEST",
            amount_sol=0.01,
            tx_signature="sig_buy",
            block_time=0.0,
            trade_token_amount=1000.0,
            buy_sol_raw=0.01,
            trader_label="trader",
        )
        await strategy._execute_copy_trade(signal)

        # La compra se ejecutó: no se descartó como "estancada".
        executor.buy_token.assert_awaited_once()
        assert "sig_buy" in strategy._executed_signatures


class TestPodaDeMintsObsoletos:
    """Los contadores por mint no pueden crecer sin limite durante semanas."""

    def test_olvida_lo_que_ya_no_sirve(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        # Acumulado agotado: ya no queda nada que recordar de este mint.
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 0.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.0
        strategy._last_buy_key[(TRADER, MINT)] = (time.time(), 0.01)
        strategy._last_sell_block[(TRADER, MINT)] = time.time()

        removed = strategy._prune_stale_mints()

        assert removed == 1
        assert (TRADER, MINT) not in strategy._wallet_mint_tokens
        assert (TRADER, MINT) not in strategy._wallet_mint_sol
        assert (TRADER, MINT) not in strategy._last_buy_key

    def test_nunca_poda_un_mint_con_tokens_en_la_wallet(self, tmp_path: Path) -> None:
        strategy, executor, _tracker = _strategy(tmp_path)
        executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=500.0, entry_price=1e-6, sol_invested=0.005
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 0.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.0

        strategy._prune_stale_mints()

        assert (TRADER, MINT) in strategy._wallet_mint_tokens

    def test_nunca_poda_un_mint_con_acumulado_vivo(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        strategy._prune_stale_mints()

        assert strategy._wallet_mint_tokens[(TRADER, MINT)] == 1000.0

    def test_nunca_poda_un_mint_de_costo_desconocido(
        self, tmp_path: Path
    ) -> None:
        """Si se podara, la siguiente venta volveria a inventar el PnL."""
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 0.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.0
        strategy._wallet_mint_cost_unknown.add((TRADER, MINT))

        strategy._prune_stale_mints()

        assert (TRADER, MINT) in strategy._wallet_mint_tokens

    def test_el_dedup_caducado_tambien_se_olvida(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        # Marca de hace 1 hora: su ventana util (el dedup) ya paso.
        strategy._last_buy_key[(TRADER, MINT)] = (time.time() - 3600.0, 0.01)
        strategy._last_sell_block[(TRADER, MINT)] = time.time() - 3600.0

        strategy._prune_stale_mints()

        assert (TRADER, MINT) not in strategy._last_buy_key
        assert (TRADER, MINT) not in strategy._last_sell_block

    def test_un_dedup_reciente_se_conserva(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._last_buy_key[(TRADER, MINT)] = (time.time(), 0.01)

        strategy._prune_stale_mints()

        assert (TRADER, MINT) in strategy._last_buy_key


class TestCalcSellPct:
    """El % de venta debe medirse contra los tokens del MISMO trader."""

    def test_usa_la_clave_canonica_del_trader(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 10000.0
        assert strategy._calc_sell_pct(MINT, 2500.0, TRADER) == pytest.approx(25.0)

    def test_sin_tracking_devuelve_0(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        assert strategy._calc_sell_pct(MINT, 2500.0, TRADER) == 0.0


class TestDescontrolPorAcumuladoDelTrader:
    """El acumulado del trader debe descontarse en TODO cierre.

    El síntoma era: el bot "marca bien y avanza" y de golpe, para el mismo mint,
    solo vendía el 50%, luego el 33%... Porque cada cierre que no pasaba por la
    señal de venta (TP/SL/trailing del tracker) dejaba al bot creyendo que el
    trader seguía con sus tokens, y el siguiente ciclo comparaba contra un
    acumulado cada vez mayor.
    """

    def test_cierre_del_tracker_descuenta_el_acumulado(self, tmp_path: Path) -> None:
        strategy, _executor, tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        strategy._on_tracked_position_closed(MINT, TRADER, 100.0, "TAKE_PROFIT")

        assert strategy._wallet_mint_tokens.get((TRADER, MINT), 0.0) == 0.0
        # Al agotarse, la entrada se borra: si no, arrastra el costo viejo al
        # siguiente ciclo del mismo mint.
        assert (TRADER, MINT) not in strategy._wallet_mint_sol
        assert tracker.remove_position.call_count == 0

    def test_cierre_parcial_descuenta_la_fraccion(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        strategy._on_tracked_position_closed(MINT, TRADER, 25.0, "TRAILING_STOP")

        assert strategy._wallet_mint_tokens[(TRADER, MINT)] == pytest.approx(750.0)
        # El costo se descuenta en la MISMA proporción (si no, el costo medio
        # queda mezclado y el PnL del trader se inventa).
        assert strategy._wallet_mint_sol[(TRADER, MINT)] == pytest.approx(0.0075)

    def test_hook_ignora_posiciones_sin_wallet_origen(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[("otro", MINT)] = 1000.0
        strategy._on_tracked_position_closed(MINT, "", 100.0, "TAKE_PROFIT")
        assert strategy._wallet_mint_tokens[("otro", MINT)] == 1000.0

    async def test_venta_de_mint_no_poseido_tambien_descuenta(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """El bot no tiene la posición, pero el trader sí vendió: su acumulado baja.

        Es el caso normal cuando el bot empieza a copiar tarde. Antes se
        ignoraba la venta sin tocar el acumulado y este crecía para siempre.
        """
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        await strategy._execute_copy_trade(_sell_signal())

        assert strategy._wallet_mint_tokens.get((TRADER, MINT), 0.0) == 0.0
        assert (TRADER, MINT) not in strategy._wallet_mint_sol

    async def test_venta_sin_cantidad_y_sin_rpc_no_deja_el_acumulado_inflado(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sin cantidad vendida ni RPC, el acumulado se cierra en vez de crecer."""
        strategy, executor, _tracker = _strategy(tmp_path)
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01
        executor.get_wallet_token_balance = AsyncMock(return_value=-1.0)  # RPC caído

        sent: list[float] = []

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            sent.append(float(kwargs.get("sell_pct", 100.0)))
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        signal = _sell_signal()
        signal.trade_token_amount = 0.0  # Helius no quantifiable
        await strategy._execute_copy_trade(signal)

        # El acumulado se cerró (no quedó en 1000 para el siguiente ciclo) y la
        # venta se ejecutó al 100%, que es lo único defendible sin datos.
        assert strategy._wallet_mint_tokens.get((TRADER, MINT), 0.0) == 0.0
        assert sent == [100.0]


class TestPnlHonesto:
    """El PnL que se reporta debe existir; si no, se dice 'n/d'."""

    async def test_pnl_trader_no_disponible_no_se_inventa(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        # Acumulado sincronizado on-chain: son tokens de compras que el bot nunca
        # vio, así que no hay costo válido para el PnL del trader.
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.0
        strategy._wallet_mint_cost_unknown.add((TRADER, MINT))

        seen: dict[str, object] = {}

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            seen.update(kwargs)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        await strategy._execute_copy_trade(_sell_signal())

        assert seen.get("pnl_known") is False
        # Y no se cuela un PnL inventado del trader en las stats: lo que se
        # registra es el de NUESTRA copia.
        assert seen.get("pnl") == 0.0

    async def test_stats_registran_el_pnl_medido_de_nuestra_venta(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`sol_received` es el SOL real obtenido, no `invertido x (1+pnl)`."""
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            # La venta real midió: Reidio 0.015 SOL habiendo invertido 0.01.
            result = kwargs.get("result")
            if isinstance(result, dict):
                result["sol_proceeds"] = 0.015
                result["copy_pnl_measured"] = 50.0
                result["invested_portion"] = 0.01
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        stats_mock = MagicMock()
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: stats_mock)

        await strategy._execute_copy_trade(_sell_signal())

        stats_mock.record_sell.assert_called_once()
        kwargs = stats_mock.record_sell.call_args.kwargs
        assert kwargs["pnl_pct"] == 50.0  # el de la copia, no el del trader
        assert kwargs["sol_invested"] == pytest.approx(0.01)
        assert kwargs["sol_received"] == pytest.approx(0.015)  # SOL real


class TestAcumulacionSinCapitalFantasma:
    """La rama de acumulación no puede inventar capital."""

    async def test_no_infla_el_invertido_sin_comprar(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, executor, _tracker = _strategy(tmp_path)
        strategy.config.copy_trading.COPY_TRADE_ALLOW_ACCUMULATE = True
        exec_pos = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        tracker_pos = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        executor.positions[MINT] = exec_pos
        strategy.tracker.positions[MINT] = tracker_pos
        executor.buy_token = AsyncMock(return_value="sig")
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        signal = CopyTradeSignal(
            wallet=TRADER,
            action="buy",
            token_mint=MINT,
            token_symbol="TEST",
            amount_sol=0.01,
            tx_signature="sig_buy_2",
            block_time=2000.0,
            trade_token_amount=1000.0,
            buy_sol_raw=0.01,
            trader_label="trader",
        )
        await strategy._execute_copy_trade(signal)

        executor.buy_token.assert_not_awaited()
        # Lo que no se compró no puede figurar como invertido: ese número es la
        # base del PnL y del "capital invertido" de /stats.
        assert exec_pos.sol_invested == pytest.approx(0.01)
        assert exec_pos.token_amount_ui == pytest.approx(1000.0)
        assert tracker_pos.amount == pytest.approx(0.01)
