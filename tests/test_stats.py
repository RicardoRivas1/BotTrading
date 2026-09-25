"""Tests del tope de PnL plausible en las estadísticas.

Un PnL disparatado (+1659118%, +6072%...) por cotización dust o costo mal
calculado ya no debe colarse a las stats: se registra con `excluded=True`
(historial conservado, clamp de seguridad) y queda FUERA de promedio, win
rate, mejor/peor trade y net SOL para no inflar la gráfica.
"""

from core.stats import MAX_PLAUSIBLE_PNL_PCT, MAX_TRUSTED_PNL_PCT, TradeStats


def _record(stats: TradeStats, pnl_pct: float, sol_inv: float = 0.05) -> None:
    stats.record_sell(
        mint="MINTTEST",
        symbol="TEST",
        wallet="wallet1",
        entry_price=1.0,
        exit_price=1.0 + pnl_pct / 100.0,
        pnl_pct=pnl_pct,
        sol_invested=sol_inv,
        sol_received=0.05 * (1.0 + pnl_pct / 100.0),
        buy_time=1.0,
        sell_time=2.0,
        sell_reason="COPY_TRADE_SELL",
    )


def test_pnl_absurdo_se_excluye(tmp_path):
    stats = TradeStats(path=str(tmp_path / "stats.json"))
    _record(stats, 1_659_118.72)
    _record(stats, 110_541.12)

    s = stats.summary()
    # Quedan en el historial (clamped por seguridad) pero marcados excluidos
    assert stats.trades[0].pnl_pct == MAX_PLAUSIBLE_PNL_PCT
    assert stats.trades[0].excluded is True
    assert stats.trades[1].excluded is True
    assert stats.excluded_count == 2
    # No contaminan promedio ni mejor trade
    assert s["best_trade_pct"] == 0.0
    assert s["avg_pnl_pct"] == 0.0
    assert s["wins"] == 0 and s["losses"] == 0


def test_pnl_sobre_max_trusted_tambien_queda_fuera(tmp_path):
    stats = TradeStats(path=str(tmp_path / "stats.json"))
    _record(stats, 2_000.0)  # +2000% = 20x, absurdo aunque < MAX_PLAUSIBLE
    _record(stats, 60.0)     # trade normal

    s = stats.summary()
    assert stats.trades[0].excluded is True
    assert stats.trades[1].excluded is False
    assert stats.excluded_count == 1
    valid = [t for t in stats.trades if not t.excluded]
    assert len(valid) == 1 and valid[0].pnl_pct == 60.0
    assert s["best_trade_pct"] == 60.0
    assert s["avg_pnl_pct"] == 60.0
    assert s["net_pnl_sol"] == 0.03  # 0.05 * 1.60 - 0.05


def test_pnl_perdida_no_baja_de_mas_100(tmp_path):
    stats = TradeStats(path=str(tmp_path / "stats.json"))
    _record(stats, -200.0)

    assert stats.trades[0].pnl_pct == -100.0
    assert stats.summary()["worst_trade_pct"] == -100.0


def test_pnl_normal_no_se_toca(tmp_path):
    stats = TradeStats(path=str(tmp_path / "stats.json"))
    _record(stats, 0.0)
    _record(stats, 50.0)
    _record(stats, -30.0)

    assert [t.pnl_pct for t in stats.trades] == [0.0, 50.0, -30.0]
    assert stats.summary()["best_trade_pct"] == 50.0
    assert stats.summary()["worst_trade_pct"] == -30.0