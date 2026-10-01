"""Pruebas del escaner de arbitraje en seco.

El escaner sirve para una sola cosa: decir la verdad sobre si el arbitrageo
es alcanzable. Estos tests fijan las tres afirmaciones que sostienen esa
respuesta:

1. El coste de un ciclo DOMINA con capital pequeño (la rent de ATA son SOL
   fijos: 0.002 SOL sobre un ciclo de 0.01 es 20% antes de empezar).
2. Un edge bruto pequeno nunca sobrevive al coste.
3. Hallar edge no basta: si la latencia supera la ventana, no es viable.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.arb_scanner import (
    ArbCosts,
    ArbScanner,
    CycleResult,
    ScanReport,
    _extract_out_amount,
    report_from_saved,
    report_to_dict,
)

MINT = "SoMeMiNt11111111111111111111111111111111111111pump"


def _costs(**kw: float) -> ArbCosts:
    base = {
        "swap_fee_pct": 0.25,
        "ata_rent_sol": 0.00203928,
        "slippage_bps": 500.0,
        "trade_size_sol": 0.01,
    }
    base.update(kw)
    return ArbCosts(**base)


class TestCostes:
    def test_el_rent_de_ata_come_en_sol_no_en_porcentaje(self) -> None:
        # Es el error de razon mas importante del modulo: 0.002 SOL sobre un
        # ciclo de 0.01 es un 20% FIJO, no un 0.2%.
        c = _costs(trade_size_sol=0.01)
        variable = c.fee_cost_sol * 100.0
        fijo = c.ata_rent_sol / c.trade_size_sol * 100.0
        assert fijo == pytest.approx(20.3928)
        assert c.total_cost_pct == pytest.approx(variable + 20.3928)

    def test_un_ciclo_de_10_sol_cuesta_menos_del_1(self) -> None:
        c = _costs(trade_size_sol=10.0)
        assert c.total_cost_pct < 1.0, "con capital decente el coste es ruido"

    def test_mas_capital_siempre_abarata_el_ciclo(self) -> None:
        costes = [_costs(trade_size_sol=s).total_cost_pct for s in (0.01, 0.1, 1.0, 10.0)]
        assert costes == sorted(costes, reverse=True)
        assert costes[0] > costes[-1] * 10

    def test_no_revienta_si_el_tamano_es_cero(self) -> None:
        # Sin capital solo queda la parte variable: 2x0.25% fee + 2x5% slippage.
        c = _costs(trade_size_sol=0.0)
        assert c.total_cost_pct == pytest.approx(0.6)

    def test_las_fees_se_cobran_dos_veces(self) -> None:
        c = _costs(swap_fee_pct=0.25, slippage_bps=0.0)
        assert c.fee_cost_sol == pytest.approx(0.005)


class TestRoundTrip:
    def _scanner(self, gross_edge_pct: float, **cost_kw: float) -> ArbScanner:
        """Monta un round-trip que devuelve exactamente `gross_edge_pct`.

        Los lamports se derivan del tamaño del ciclo: codificarlos a mano
        arrastraba el tamaño de un test a otro y hacia fallar casos que no
        tienen nada que ver.
        """
        costs = _costs(**cost_kw)
        invested_lamports = int(costs.trade_size_sol * 1_000_000_000)
        returned = int(invested_lamports * (1 + gross_edge_pct / 100.0))
        executor = MagicMock()
        executor._get_quote = AsyncMock(side_effect=[
            {"outAmount": "1000", "routePlan": []},
            {"outAmount": str(returned), "routePlan": []},
        ])
        return ArbScanner(executor, costs, min_edge_pct=0.3)

    async def test_una_operacion_neutra_no_es_oportunidad(self) -> None:
        # Compras y ventas al mismo precio: 0.01 -> 0.01. Bruto 0%, y el coste
        # se come un 30%. Esto es el caso real mas comun.
        s = self._scanner(0.0, trade_size_sol=0.01)
        r = await s._round_trip(MagicMock(), MINT)
        assert r.gross_edge_pct == pytest.approx(0.0)
        assert r.net_edge_pct < 0
        assert not r.viable

    async def test_un_edge_bruto_pequeno_muere_en_el_coste(self) -> None:
        # +1% bruto suena bien. Pero el ciclo de 0.01 SOL paga 20.4% de rent
        # de ATA + 0.6% de fees: el neto sale en -20%.
        s = self._scanner(1.0, trade_size_sol=0.01)
        r = await s._round_trip(MagicMock(), MINT)
        assert r.gross_edge_pct == pytest.approx(1.0)
        assert r.net_edge_pct == pytest.approx(-19.99, abs=0.05)
        assert not r.viable

    async def test_solo_con_capital_grande_aparece_el_edge(self) -> None:
        # El MISMO +1% bruto, pero con 10 SOL de ciclo: el coste cae a ~0.6%.
        # Por eso el capital, y no el codigo, es lo que mata el arbitraje.
        s = self._scanner(1.0, trade_size_sol=10.0)
        r = await s._round_trip(MagicMock(), MINT)
        assert r.net_edge_pct == pytest.approx(1.0 - 0.6, abs=0.05)
        assert r.viable

    async def test_la_venta_devuelve_lamports(self) -> None:
        s = self._scanner(10.0, trade_size_sol=0.01)
        r = await s._round_trip(MagicMock(), MINT)
        assert r.sol_back == pytest.approx(0.011)

    async def test_una_cotizacion_simulada_no_cuenta_como_dato(self) -> None:
        executor = MagicMock()
        executor._get_quote = AsyncMock(return_value={"outAmount": "1", "simulated": True})
        r = await ArbScanner(executor, _costs())._round_trip(MagicMock(), MINT)
        assert not r.has_route
        assert "simulada" in r.error

    async def test_sin_ruta_de_compra_no_inventa_nada(self) -> None:
        executor = MagicMock()
        executor._get_quote = AsyncMock(side_effect=[{"outAmount": "0"}, {"outAmount": "0"}])
        r = await ArbScanner(executor, _costs())._round_trip(MagicMock(), MINT)
        assert not r.has_route
        assert r.net_edge_pct == 0.0

    async def test_mide_latencia_aunque_no_haya_ruta(self) -> None:
        executor = MagicMock()
        executor._get_quote = AsyncMock(side_effect=[{"outAmount": "0"}, {"outAmount": "0"}])
        r = await ArbScanner(executor, _costs())._round_trip(MagicMock(), MINT)
        assert r.latency_ms > 0


class TestViabilidad:
    def test_sin_oportunidades_nunca_es_viable(self) -> None:
        r = ScanReport(opportunities=[], latencies_ms=[10.0])
        assert not r.viable(200.0)

    def test_con_edge_pero_lento_no_es_viable(self) -> None:
        # El caso que importa: hay oportunidad, pero llegamos tarde.
        r = ScanReport(opportunities=[CycleResult(mint=MINT, viable=True)], latencies_ms=[400.0])
        assert not r.viable(200.0)

    def test_con_edge_y_rapido_si_es_viable(self) -> None:
        r = ScanReport(opportunities=[CycleResult(mint=MINT, viable=True)], latencies_ms=[40.0])
        assert r.viable(200.0)

    def test_el_aviso_de_latencia_aparece_en_el_texto(self) -> None:
        r = ScanReport(
            opportunities=[CycleResult(mint=MINT, net_edge_pct=1.2, viable=True)],
            latencies_ms=[400.0],
        )
        texto = r.format(200.0)
        assert "NO VIABLE" in texto
        assert "400 ms" in texto

    def test_sin_datos_no_inventa_una_latencia(self) -> None:
        assert ScanReport().median_latency_ms == 0.0
        assert "Latencia" not in ScanReport().format(200.0)


class TestInforme:
    def test_ida_y_vuelta_conserva_los_datos(self) -> None:
        original = ScanReport(
            trade_size_sol=0.05,
            cost_pct=4.5,
            scanned=5,
            with_route=3,
            latencies_ms=[100.0, 120.0],
            opportunities=[
                CycleResult(mint=MINT, net_edge_pct=0.9, gross_edge_pct=5.4, latency_ms=110.0)
            ],
        )
        texto = report_from_saved(report_to_dict(original), 200.0)
        assert "0.05" in texto
        assert "+0.900%" in texto
        assert "110 ms" in texto

    def test_un_informe_vacio_lo_dice(self) -> None:
        assert "no hay ninguna pasada" in report_from_saved(None, 200.0).lower()


class TestExtraccion:
    @pytest.mark.parametrize(
        "quote,esperado",
        [
            ({"outAmount": "12345"}, 12345.0),
            ({"outAmount": ""}, 0.0),
            ({}, 0.0),
            ({"outAmount": None}, 0.0),
            ({"outAmount": "no-es-un-numero"}, 0.0),
        ],
    )
    def test_una_cotizacion_rota_no_rompe_el_scanner(
        self, quote: dict, esperado: float
    ) -> None:
        assert _extract_out_amount(quote) == esperado

    def test_el_trade_size_viene_de_la_config(self) -> None:
        cfg = SimpleNamespace(
            ARB_SWAP_FEE_PCT=0.2, ARB_ATA_RENT_SOL=0.001, ARB_TRADE_SIZE_SOL=0.5
        )
        from core.arb_scanner import costs_from_config

        c = costs_from_config(cfg, slippage_bps=300.0)
        assert c.trade_size_sol == 0.5
        assert c.slippage_bps == 300.0