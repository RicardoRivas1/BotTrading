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


class TestCalcSellPct:
    """El % de venta debe medirse contra los tokens del MISMO trader."""

    def test_usa_la_clave_canonica_del_trader(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 10000.0
        assert strategy._calc_sell_pct(MINT, 2500.0, TRADER) == pytest.approx(25.0)

    def test_sin_tracking_devuelve_0(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        assert strategy._calc_sell_pct(MINT, 2500.0, TRADER) == 0.0
