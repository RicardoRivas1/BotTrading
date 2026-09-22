"""Tests del tope de PnL plausible en las estadísticas.

Un PnL disparatado (+1659118%, +110541%...) por cotización dust o costo mal
calculado ya no debe colarse a las stats: se limita a MAX_PLAUSIBLE_PNL_PCT
para no inflar promedio ni mejor trade.
"""

from core.stats import MAX_PLAUSIBLE_PNL_PCT, TradeStats


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


def test_pnl_absurdo_se_limita(tmp_path):
    stats = TradeStats(path=str(tmp_path / "stats.json"))
    _record(stats, 1_659_118.72)
    _record(stats, 110_541.12)

    s = stats.summary()
    assert stats.trades[0].pnl_pct == MAX_PLAUSIBLE_PNL_PCT
    assert stats.trades[1].pnl_pct == MAX_PLAUSIBLE_PNL_PCT
    # Ni promedio ni mejor trade se disparan
    assert s["best_trade_pct"] == MAX_PLAUSIBLE_PNL_PCT
    assert s["avg_pnl_pct"] == MAX_PLAUSIBLE_PNL_PCT


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