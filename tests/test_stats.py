"""Tests del tope de PnL plausible y de la reinicialización del historial.

Un PnL disparatado (+1659118%, +6072%...) por cotización dust o costo mal
calculado ya no debe colarse a las stats: se registra con `excluded=True`
(historial conservado, clamp de seguridad) y queda FUERA de promedio, win
rate, mejor/peor trade y net SOL para no inflar la gráfica.

El historial anterior además mezclaba el PnL del TRADER copiado con nuestro
capital invertido y derivaba el `sol_received` de ese PnL inventado, así que se
descarta al arrancar (decisión del usuario: empezar de cero).
"""

import json

import pytest

from core.stats import (
    MAX_PLAUSIBLE_PNL_PCT,
    STATS_VERSION,
    TradeStats,
)


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


class TestHistorialSeReinicia:
    """El fichero viejo (PnL del trader mezclado con nuestro capital) se borra."""

    def test_historial_v1_se_descarta(self, tmp_path):
        path = tmp_path / "stats.json"
        _legacy(path, stats_version=1, trades=3)

        stats = TradeStats(path=str(path))

        assert stats.trades == []
        assert stats.total_sells == 0
        assert stats.wallets == {}
        assert not path.exists()  # el fichero viejo ya no está

    def test_historial_v2_se_conserva(self, tmp_path):
        path = tmp_path / "stats.json"
        _legacy(path, stats_version=STATS_VERSION, trades=1)

        stats = TradeStats(path=str(path))

        assert len(stats.trades) == 1
        assert stats.total_sells == 1

    def test_historial_v2_sin_el_campo_nuevo_sigue_cargando(self, tmp_path):
        """Un `trade_stats.json` v2 ya escrito (sin `estimated`) debe cargar.

        El campo se añadió después de la migración v2, así que los ficheros que
        el bot dejó en DRY_RUN no lo tienen. Deben seguir abriendose (con
        `estimated=False`) en vez de reventar el arranque.
        """
        path = tmp_path / "stats.json"
        _legacy(path, stats_version=STATS_VERSION, trades=1)
        data = json.loads(path.read_text(encoding="utf-8"))
        for trade in data["trades"]:
            trade.pop("estimated", None)
            trade.pop("pnl_unreliable", None)
        path.write_text(json.dumps(data), encoding="utf-8")

        stats = TradeStats(path=str(path))

        assert len(stats.trades) == 1
        assert stats.trades[0].estimated is False

    def test_tras_reiniciar_lo_nuevo_ya_no_se_borra(self, tmp_path):
        path = tmp_path / "stats.json"
        _legacy(path, stats_version=1, trades=3)

        stats = TradeStats(path=str(path))
        _record(stats, 25.0)
        reloaded = TradeStats(path=str(path))

        assert len(reloaded.trades) == 1  # el v2 ya escrito no se toca

    def test_sin_fichero_arranca_en_blanco(self, tmp_path):
        stats = TradeStats(path=str(tmp_path / "no-existe.json"))
        assert stats.trades == [] and stats.total_sells == 0


class TestContadoresDeVentas:
    """`total_sells` debe contar SOLO trades válidos (excluidos no son ventas)."""

    def test_venta_excluida_no_cuenta_como_venta(self, tmp_path):
        stats = TradeStats(path=str(tmp_path / "stats.json"))
        _record(stats, 1_659_118.72)  # excluido
        _record(stats, 20.0)          # válida

        s = stats.summary()
        assert s["total_sells"] == 1
        assert stats.excluded_count == 1

    def test_trade_sin_pnl_medible_queda_fuera_de_las_metricas(self, tmp_path):
        """Sin PnL real, guardar 0 como si fuera un win inventa un resultado."""
        stats = TradeStats(path=str(tmp_path / "stats.json"))
        stats.record_sell(
            mint="MINT_SIN_PNL",
            symbol="NOP",
            wallet="wallet1",
            entry_price=0.0,
            exit_price=0.0,
            pnl_pct=0.0,
            sol_invested=0.05,
            sol_received=0.05,
            buy_time=1.0,
            sell_time=2.0,
            pnl_unreliable=True,
        )
        _record(stats, 20.0)

        s = stats.summary()
        assert stats.trades[0].excluded is True
        # Ni entra como win, ni su capital falsea el PnL neto.
        assert s["wins"] == 1 and s["losses"] == 0
        assert s["total_sells"] == 1
        assert s["net_pnl_sol"] == pytest.approx(0.01)  # solo el trade válido

    def test_trade_estimado_cuenta_pero_se_avisa(self, tmp_path):
        """Una estimación cuenta, pero /stats debe decir que lo es.

        En DRY_RUN no hay venta real que medir, así que el PnL se estima. Si se
        cuenta en silencio, /stats presenta simulaciones como resultados reales.
        """
        stats = TradeStats(path=str(tmp_path / "stats.json"))
        stats.record_sell(
            mint="MINT_ESTIMADO",
            symbol="EST",
            wallet="wallet1",
            entry_price=1e-6,
            exit_price=1.5e-6,
            pnl_pct=50.0,
            sol_invested=0.01,
            sol_received=0.015,
            buy_time=1.0,
            sell_time=2.0,
            estimated=True,
        )
        _record(stats, 20.0)

        s = stats.summary()
        assert s["total_sells"] == 2      # la estimación cuenta para el ciclo
        assert s["simulated"] == 1        # pero queda marcada
        assert stats.trades[0].estimated is True
        assert stats.trades[1].estimated is False

        out = stats.format_summary()
        assert "ESTIMACIONES" in out
        assert "1 de 2" in out

    def test_sin_estimaciones_no_hay_aviso(self, tmp_path):
        stats = TradeStats(path=str(tmp_path / "stats.json"))
        _record(stats, 20.0)
        assert "ESTIMACIONES" not in stats.format_summary()


def _legacy(path, *, stats_version: int, trades: int) -> None:
    """Escribe un `trade_stats.json` de la era anterior a la migración."""
    path.write_text(
        json.dumps(
            {
                "stats_version": stats_version,
                "total_buys": trades * 3,
                "total_sells": trades,
                "positions_opened": trades,
                "positions_closed": trades,
                "trades": [
                    {
                        "mint": f"MINT{i}",
                        "symbol": "OLD",
                        "wallet": "wallet1",
                        "entry_price": 1.0,
                        "exit_price": 2.0,
                        "pnl_pct": 100.0,
                        "sol_invested": 0.05,
                        "sol_received": 0.10,  # fabricado a partir del PnL
                        "buy_time": 1.0,
                        "sell_time": 2.0,
                        "hold_seconds": 1.0,
                        "sell_reason": "COPY_TRADE_SELL",
                        "sell_pct": 100.0,
                        "excluded": False,
                    }
                    for i in range(trades)
                ],
                "wallets": {},
            }
        ),
        encoding="utf-8",
    )
