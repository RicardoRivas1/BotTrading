"""Pruebas del escaner cross-pool.

Este modulo nacio de un error concreto: comparar el `priceUsd` de DexScreener
sin mirar que token estaba siendo el BASE en cada par. Ese error producia
spreads del 1.6 MILLON %, y los tests de aqui existen para que no vuelva a
colarse. La mayoria de lo que hay mas abajo son datos ROTOS a proposito: si el
modulo los acepta, el escaner vuelve a mentir.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.arb_scanner import ArbCosts
from core.cross_pool_scanner import (
    CrossPoolReport,
    CrossPoolScanner,
    PoolPrice,
    SpreadCandidate,
    report_to_dict,
)

SOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
BONK = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
JTO = "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL"
# Un mint cualquiera que actua como BASE cuando nuestro token va en el QUOTE.
OTRO = "11111111111111111111111111111111111111112"


def _costs(**kw: float) -> ArbCosts:
    base = {
        "swap_fee_pct": 0.25,
        "ata_rent_sol": 0.00203928,
        "slippage_bps": 50.0,
        "trade_size_sol": 1.0,
    }
    base.update(kw)
    return ArbCosts(**base)


def _scanner(**kw) -> CrossPoolScanner:
    executor = MagicMock()
    opts = {
        "min_liquidity_usd": 10_000.0,
        "min_hold_ms": 3_000.0,
        "max_snapshot_age_ms": 15_000.0,
        "min_hits": 3,
    }
    opts.update(kw)
    return CrossPoolScanner(executor, _costs(), **opts)


def _pair(
    *,
    base: str,
    quote: str,
    base_liq: float,
    quote_liq: float,
    liq_usd: float = 100_000.0,
    timestamp: float | None = None,
    created_at: float | None = None,
    address: str = "PAIR1",
    dex: str = "raydium",
) -> dict:
    """Par de DexScreener.

    `timestamp` es la marca del ultimo cambio de PRECIO (frescura del dato).
    `created_at` es la fecha de creacion del POOL, que en la API real llega en
    milisegundos y es totalmente distinta: un pool puede ser de hace anos y
    seguir cotizando al dia.
    """
    pair = {
        "pairAddress": address,
        "dexId": dex,
        "baseToken": {"address": base},
        "quoteToken": {"address": quote},
        "liquidity": {
            "base": base_liq,
            "quote": quote_liq,
            "usd": liq_usd,
        },
    }
    if timestamp is not None:
        pair["priceTimestamp"] = timestamp
    if created_at is not None:
        pair["pairCreatedAt"] = created_at
    return pair


class TestNormalizacionDeOrientacion:
    """El error queåˆ¶é€  los spreads absurdos."""

    def test_precio_de_un_par_base_mint(self) -> None:
        # 1.000.000 tokens contra 100 SOL -> 0.0001 SOL por token.
        s = _scanner()
        pair = _pair(base=BONK, quote=SOL, base_liq=1_000_000, quote_liq=100)
        price, reason = s._normalize_pair(pair, BONK, 0.0, time.time() * 1000)
        assert reason == ""
        assert price is not None
        assert price.price_per_sol == pytest.approx(0.0001)

    def test_el_mismo_precio_con_el_mint_como_quote(self) -> None:
        # Aqui el mint buscado es el QUOTE: 100 tokens contra 1.000.000 SOL.
        # El precio del token es el MISMO, pero leer priceUsd a pelo daria
        # 10.000 (el precio del otro activo) y un spread inventado de un
        # millon por ciento.
        s = _scanner()
        pair = _pair(base=SOL, quote=BONK, base_liq=100, quote_liq=1_000_000)
        price, reason = s._normalize_pair(pair, BONK, 0.0, time.time() * 1000)
        assert reason == ""
        assert price is not None
        assert price.price_per_sol == pytest.approx(0.0001)

    def test_un_par_de_otro_token_se_descarta(self) -> None:
        # DexScreener devuelve tambien pools de otras cryptos en la misma
        # respuesta. Su precio no dice NADA sobre el mint que buscamos.
        s = _scanner()
        pair = _pair(base=JTO, quote=SOL, base_liq=1000, quote_liq=100)
        price, reason = s._normalize_pair(pair, BONK, 0.0, time.time() * 1000)
        assert price is None
        assert "no contiene el mint" in reason

    def test_pools_en_usdc_se_normalizan_a_sol(self) -> None:
        # Mismo precio en USD, dos pools comparables porque ambos se pasan a SOL.
        s = _scanner()
        par_usdc = _pair(base=BONK, quote=USDC, base_liq=1_000_000, quote_liq=150.0)
        precio, reason = s._normalize_pair(
            par_usdc, BONK, sol_usd=150.0, now_ms=time.time() * 1000
        )
        assert reason == ""
        assert precio is not None
        assert precio.price_per_sol == pytest.approx(1e-6)

    def test_sin_precio_de_sol_los_pools_usdc_se_descartan(self) -> None:
        # Sin referencia no se puede comparar un pool en USDC con uno en SOL.
        # Inventar la conversion seria justo el error quedahacemos no.
        s = _scanner()
        pair = _pair(base=BONK, quote=USDC, base_liq=1_000_000, quote_liq=150.0)
        precio, reason = s._normalize_pair(pair, BONK, 0.0, time.time() * 1000)
        assert precio is None
        assert "sin precio de SOL" in reason


class TestDatosRotos:
    def test_pool_vacio_se_descarta(self) -> None:
        s = _scanner()
        pair = _pair(base=BONK, quote=SOL, base_liq=0, quote_liq=0, liq_usd=0)
        precio, reason = s._normalize_pair(pair, BONK, 0.0, time.time() * 1000)
        assert precio is None
        assert "reservas" in reason

    def test_numerario_desconocido_se_descarta(self) -> None:
        # Un par contra una moneda que no sea SOL o USDC no es comparable.
        s = _scanner()
        raro = "Rar0Mint11111111111111111111111111111111111111"
        pair = _pair(base=BONK, quote=raro, base_liq=1000, quote_liq=100)
        precio, reason = s._normalize_pair(pair, BONK, 0.0, time.time() * 1000)
        assert precio is None
        assert "numerario desconocido" in reason

    def test_sin_pair_address_se_descarta(self) -> None:
        s = _scanner()
        pair = _pair(base=BONK, quote=SOL, base_liq=1000, quote_liq=100)
        pair.pop("pairAddress")
        precio, reason = s._normalize_pair(pair, BONK, 0.0, time.time() * 1000)
        assert precio is None
        assert "pairAddress" in reason

    def test_precios_no_numericos_no_rompen_la_pasada(self) -> None:
        # DexScreener manda null/"" en campos de liquidez cuando el pool es
        # nuevo. Un float(None) aqui tumbaba el escaner entero.
        s = _scanner()
        pair = _pair(base=BONK, quote=SOL, base_liq=1000, quote_liq=100)
        pair["liquidity"] = {"base": None, "quote": "", "usd": None}
        precio, reason = s._normalize_pair(pair, BONK, 0.0, time.time() * 1000)
        assert precio is None
        assert "reservas" in reason

    def test_precio_absurdo_se_descarta(self) -> None:
        # Un pool con 1.000.000 de SOL contra 105.000 de tokens vale 9.5 SOL
        # por token. No es un precio: son reservas mal escritas, y si se
        # aceptara produciria un spread del 94.000% que "encontrariamos" en
        # cada escaneo.
        s = _scanner()
        par = _pair(base=SOL, quote=BONK, base_liq=1_000_000, quote_liq=105_000)
        precio, reason = s._normalize_pair(par, BONK, 0.0, time.time() * 1000)
        assert precio is None
        assert "fuera de rango" in reason

    def test_un_spread_de_94_000_por_ciento_es_imposible(self) -> None:
        # El caso exacto que ensucio los experimentos del scratchpad: dos pools
        # del mismo token donde uno tiene reservas absurdas. El filtro de rango
        # lo corta antes de que llegue a compararse.
        s = _scanner()
        bueno = _pair(base=BONK, quote=SOL, base_liq=1_000_000, quote_liq=100,
                      liq_usd=200_000.0, address="OK")
        roto = _pair(base=SOL, quote=BONK, base_liq=1_000_000, quote_liq=105_000,
                     liq_usd=200_000.0, address="ROTO")
        precios = []
        for par in (bueno, roto):
            p, motivo = s._normalize_pair(par, BONK, 0.0, time.time() * 1000)
            if p is not None:
                precios.append(p)
        assert len(precios) == 1, "el pool de reservas absurdas debe descartarse"

    def test_liquidez_nan_no_pasa_el_filtro(self) -> None:
        s = _scanner(min_liquidity_usd=25_000.0)
        pair = _pair(
            base=BONK, quote=SOL, base_liq=1000, quote_liq=100, liq_usd=float("nan")
        )
        precio, reason = s._normalize_pair(pair, BONK, 0.0, time.time() * 1000)
        assert precio is None
        assert "liquidez en USD" in reason


class TestFrescuraDelDato:
    def test_un_snapshot_viejo_se_descarta(self) -> None:
        # Comparar precio de hace un dia contra precio de ahora inventa
        # spreads que no existen.
        s = _scanner(max_snapshot_age_ms=15_000.0)
        viejo = time.time() - 3_600
        pair = _pair(
            base=BONK, quote=SOL, base_liq=1000, quote_liq=100, timestamp=viejo
        )
        precio, reason = s._normalize_pair(pair, BONK, 0.0, time.time() * 1000)
        assert precio is None
        assert "snapshot viejo" in reason

    def test_un_snapshot_fresco_se_acepta(self) -> None:
        s = _scanner(max_snapshot_age_ms=15_000.0)
        pair = _pair(
            base=BONK, quote=SOL, base_liq=1000, quote_liq=100, timestamp=time.time()
        )
        precio, reason = s._normalize_pair(pair, BONK, 0.0, time.time() * 1000)
        assert reason == ""
        assert precio is not None

    def test_timestamp_en_el_futuro_se_trata_como_roto(self) -> None:
        # Un reloj desincronizado produce "fechas" del futuro. Aceptarlas
        # seria confiarle el edge a un reloj roto.
        s = _scanner()
        pair = _pair(
            base=BONK, quote=SOL, base_liq=1000, quote_liq=100,
            timestamp=time.time() + 86_400,
        )
        precio, reason = s._normalize_pair(pair, BONK, 0.0, time.time() * 1000)
        assert precio is None
        assert "viejo" in reason

    def test_sin_timestamp_no_se_inventa_una_edad(self) -> None:
        # La fuente a veces no lo manda. Se acepta el dato (no hay evidencia de
        # que sea viejo) pero la edad se queda en 0, no inventada.
        s = _scanner()
        pair = _pair(base=BONK, quote=SOL, base_liq=1000, quote_liq=100)
        precio, reason = s._normalize_pair(pair, BONK, 0.0, time.time() * 1000)
        assert reason == ""
        assert precio is not None
        assert precio.age_ms == 0.0


class TestFiltroDeLiquidez:
    def test_pool_por_debajo_del_minimo_no_es_usable(self) -> None:
        pool = PoolPrice(
            mint=BONK, pair_address="P", dex_id="raydium", quote_mint=SOL,
            price_per_sol=0.0001, liquidity_usd=500.0, age_ms=0.0,
        )
        assert pool.is_tradable(25_000.0) is False
        assert pool.is_tradable(100.0) is True


def _dos_pools(barato: float, caro: float) -> list[PoolPrice]:
    """Dos pools del mismo token, uno mas barato que el otro."""
    return [
        PoolPrice(BONK, "P_BARATO", "orca", SOL, barato, 200_000.0, 0.0),
        PoolPrice(BONK, "P_CARO", "raydium", SOL, caro, 200_000.0, 0.0),
    ]


class TestDeteccionDeSpread:
    def test_detecta_el_spread_y_lo_convierte_en_neto(self) -> None:
        s = _scanner()
        report = CrossPoolReport(cost_pct=s.costs.total_cost_pct)
        # 5% de diferencia bruta sobre 1 SOL de ciclo.
        candidatos = s._evaluate(_dos_pools(1.0, 1.05), report)
        assert len(candidatos) == 1
        c = candidatos[0]
        assert c.gross_edge_pct == pytest.approx(5.0)
        assert c.net_edge_pct == pytest.approx(5.0 - s.costs.total_cost_pct)

    def test_un_spread_menor_que_el_coste_no_es_oportunidad(self) -> None:
        # Este es el corazon del escaner: un 0.1% de spread sobre un 1.9% de
        # coste es dinero que se PIERDE, no ganado.
        s = _scanner()
        report = CrossPoolReport()
        candidatos = s._evaluate(_dos_pools(1.0, 1.001), report)
        assert candidatos == []
        assert report.candidates == []
        assert "spread menor que el coste" in report.rejected

    def test_precios_iguales_no_generan_nada(self) -> None:
        s = _scanner()
        report = CrossPoolReport()
        assert s._evaluate(_dos_pools(1.0, 1.0), report) == []

    def test_hace_falta_mas_de_un_pool(self) -> None:
        s = _scanner()
        report = CrossPoolReport()
        assert s._evaluate(_dos_pools(1.0, 1.05)[:1], report) == []

    def test_el_mismo_pool_repetido_no_cuenta(self) -> None:
        # DexScreener devuelve pools duplicados; comparar uno consigo mismo
        # daria 0% y contaminaria el analisis.
        s = _scanner()
        report = CrossPoolReport()
        pool = PoolPrice(
            BONK, "MISMO", "orca", SOL, 1.0, 200_000.0, 0.0
        )
        assert s._evaluate([pool, pool], report) == []

    def test_el_precio_mas_barato_es_el_de_compra(self) -> None:
        s = _scanner()
        report = CrossPoolReport()
        c = s._evaluate(_dos_pools(1.0, 1.05), report)[0]
        assert c.cheap.price_per_sol < c.rich.price_per_sol


class TestDuracion:
    def test_una_sola_pasada_no_confirma(self) -> None:
        # El punto central: un spread que solo hemos visto una vez y que dura
        # menos que nuestra latencia NO es capturable.
        s = _scanner(min_hold_ms=3_000.0, min_hits=3)
        report = CrossPoolReport()
        c = s._evaluate(_dos_pools(1.0, 1.05), report)[0]
        assert c.hits == 1
        assert c.confirmed(3_000.0, 3) is False

    def test_confirma_tras_varias_pasadas_y_suficiente_tiempo(self) -> None:
        s = _scanner(min_hold_ms=0.0, min_hits=3)
        report = CrossPoolReport()
        candidatos = None
        for _ in range(3):
            candidatos = s._evaluate(_dos_pools(1.0, 1.05), report)
        c = candidatos[0]
        assert c.hits == 3
        assert c.confirmed(0.0, 3) is True

    def test_el_spread_que_desaparece_se_olvida(self) -> None:
        # Si el edge se cierra, su duracion deja de contar: un edge que ya no
        # existe no puede "confirmarse" por haber durado antes.
        s = _scanner(min_hold_ms=0.0, min_hits=2)
        report = CrossPoolReport()
        for _ in range(3):
            s._evaluate(_dos_pools(1.0, 1.05), report)
        s._evaluate(_dos_pools(1.0, 1.0001), report)  # se cierra
        nuevos = s._evaluate(_dos_pools(1.0, 1.05), report)
        assert nuevos[0].hits == 1

    def test_la_duracion_crece_con_el_tiempo(self) -> None:
        s = _scanner(min_hold_ms=0.0, min_hits=1)
        report = CrossPoolReport()
        c = s._evaluate(_dos_pools(1.0, 1.05), report)[0]
        primera = c.duration_ms
        c2 = s._evaluate(_dos_pools(1.0, 1.05), report)[0]
        assert c2.duration_ms >= primera

    def test_la_memoria_esta_acotada(self) -> None:
        # Si no se limpia, un bot que corre dias acumula candidatos para siempre.
        s = _scanner(min_hold_ms=0.0, min_hits=1)
        report = CrossPoolReport()
        for i in range(80):
            pools = [
                PoolPrice(BONK, f"P1_{i}", "orca", SOL, 1.0, 200_000.0, 0.0),
                PoolPrice(BONK, f"P2_{i}", "raydium", SOL, 1.05, 200_000.0, 0.0),
            ]
            s._evaluate(pools, report)
        assert len(s._history) <= 50


class TestFuenteDelPrecio:
    """`priceUsd`/`priceNative` mandan sobre el cociente de las reservas.

    Medido contra la API real: con el mismo `priceUsd` en las 30 filas,
    `quote/base` variaba x10 entre pools (daba un +863% de spread en BONK que
    no existia), mientras `priceNative` coincidia con `priceUsd/SOLUSD` en
    todos los pools con numerario nativo. El cociente se queda de respaldo.
    """

    def test_usa_price_native_antes_que_las_reservas(self) -> None:
        s = _scanner()
        par = _pair(base=BONK, quote=SOL, base_liq=1_000_000, quote_liq=100)
        # Las reservas dicen 0.0001, pero el precio de la API dice otra cosa.
        par["priceNative"] = 0.00003
        precio, reason = s._normalize_pair(par, BONK, 0.0, time.time() * 1000)
        assert reason == ""
        assert precio.price_per_sol == pytest.approx(0.00003)

    def test_el_precio_de_un_mint_que_es_el_quote_se_invierte(self) -> None:
        # Con el mint como QUOTE, `priceNative` describe al BASE (el otro), asi
        # que el precio del nuestro es su inverso. Sin invertir, un pool
        # aparecia 10.000 veces mas caro que si mismo.
        s = _scanner()
        # Con el mint como QUOTE, `priceNative` describe al token BASE, no al
        # nuestro, asi que el precio de BONK es su INVERSO. Sin invertir se
        # leeria un numero 10.000 veces mas pequeno y el pool apareceria como
        # una oportunidad gigante inexistente. Para que el caso sea
        # representativo Y plausible, el BASE es SOL con el numerario en la
        # posicion correcta y priceNative ajusta a un valor coherente.
        par = _pair(base=BONK, quote=SOL, base_liq=1_000_000, quote_liq=100)
        par["priceNative"] = 3.17e-08
        par["priceUsd"] = 3.7e-06
        s._normalize_pair(par, BONK, 0.0, time.time() * 1000)
        # Ahora el mismo par visto desde el otro lado: el numerario pasa a ser
        # el BASE y nuestro token el QUOTE. priceNative sigue describiendo al
        # token que NO buscamos, asi que hay que invertirlo.
        par2 = _pair(base=SOL, quote=BONK, base_liq=100, quote_liq=1_000_000)
        par2["priceNative"] = 118.0  # SOL en USD: describe al BASE (SOL)
        par2["priceUsd"] = 118.0
        precio_quote, reason = s._normalize_pair(par2, BONK, 0.0, time.time() * 1000)
        assert reason == ""
        # El precio del token es 1/118 SOL, no 118.
        assert precio_quote.price_per_sol == pytest.approx(1.0 / 118.0)

    def test_sin_campos_de_precio_cae_a_las_reservas(self) -> None:
        s = _scanner()
        par = _pair(base=BONK, quote=SOL, base_liq=1_000_000, quote_liq=100)
        precio, reason = s._normalize_pair(par, BONK, 0.0, time.time() * 1000)
        assert reason == ""
        assert precio.price_per_sol == pytest.approx(0.0001)

    def test_un_spread_real_de_0_8_por_ciento_no_es_oportunidad(self) -> None:
        # El caso de BONK en la API real: ~0.82% entre el mejor y el peor pool
        # con 1.70% de coste. Aunque los numeros sean reales, esto es dinero que
        # se pierde, y el escaner tiene que decirlo.
        s = _scanner()
        report = CrossPoolReport(cost_pct=s.costs.total_cost_pct)
        pools = [
            PoolPrice(BONK, "A", "orca", SOL, 3.167e-08, 265_217.0, 0.0),
            PoolPrice(BONK, "B", "meteora", SOL, 3.193e-08, 165_528.0, 0.0),
        ]
        candidatos = s._evaluate(pools, report)
        assert candidatos == []
        assert "spread menor que el coste" in report.rejected


class TestEdadDelPoolVsEdadDelDato:
    def test_un_pool_viejo_con_precio_fresco_sirve(self) -> None:
        # El bug que hacia fallar el escaner entero contra la API real: un pool
        # de 2023 (orca/BONK) con el precio al dia. Usar `pairCreatedAt` como
        # medida de frescura descartaba TODOS los pools-liquidos veteranos, que
        # son justo donde vive el arbitrageo.
        s = _scanner(max_snapshot_age_ms=15_000.0)
        pair = _pair(
            base=BONK, quote=SOL, base_liq=1_000_000, quote_liq=100,
            timestamp=time.time(),
            created_at=(time.time() - 3 * 365 * 86_400) * 1000,
        )
        precio, reason = s._normalize_pair(pair, BONK, 0.0, time.time() * 1000)
        assert reason == ""
        assert precio is not None
        assert precio.age_ms < 15_000.0

    def test_la_fecha_de_creacion_se_guarda_como_edad_del_pool(self) -> None:
        # Y se conserva como dato informativo: sirve para saber si un pool es
        # viejo o lleva horas existiendo.
        s = _scanner()
        hace_10_dias = (time.time() - 10 * 86_400) * 1000
        pair = _pair(
            base=BONK, quote=SOL, base_liq=1000, quote_liq=100,
            created_at=hace_10_dias,
        )
        assert s._pool_age_days(pair, time.time() * 1000) == pytest.approx(10.0, abs=0.1)

    def test_sin_antiguedad_devuelve_cero_dias(self) -> None:
        s = _scanner()
        pair = _pair(base=BONK, quote=SOL, base_liq=1000, quote_liq=100)
        assert s._pool_age_days(pair, time.time() * 1000) == 0.0


class TestPrecioDeSOL:
    def test_deriva_sol_usd_de_la_propia_respuesta(self) -> None:
        s = _scanner()
        pares = [
            _pair(base=SOL, quote=USDC, base_liq=100, quote_liq=15_000),
            _pair(base=BONK, quote=SOL, base_liq=1000, quote_liq=100),
        ]
        assert s._sol_usd_from_pairs(pares) == pytest.approx(150.0)

    def test_sin_par_sol_usdc_devuelve_cero(self) -> None:
        s = _scanner()
        pares = [_pair(base=BONK, quote=SOL, base_liq=1000, quote_liq=100)]
        assert s._sol_usd_from_pairs(pares) == 0.0

    def test_ignorando_pares_sin_reservas(self) -> None:
        s = _scanner()
        pares = [_pair(base=SOL, quote=USDC, base_liq=0, quote_liq=0)]
        assert s._sol_usd_from_pairs(pares) == 0.0


class TestScanCompleto:
    @pytest.mark.asyncio
    async def test_el_escaneo_ignora_basura_y_reporta_motivos(self) -> None:
        s = _scanner(min_liquidity_usd=25_000.0)
        pares = [
            _pair(base=BONK, quote=SOL, base_liq=1_000_000, quote_liq=100,
                  liq_usd=200_000.0, address="BARATO", dex="orca"),
            _pair(base=SOL, quote=BONK, base_liq=1_000_000, quote_liq=105_000,
                  liq_usd=200_000.0, address="CARO", dex="raydium"),
            _pair(base=JTO, quote=SOL, base_liq=1000, quote_liq=100),
            _pair(base=BONK, quote=SOL, base_liq=0, quote_liq=0, liq_usd=0),
        ]
        s._fetch_pairs = AsyncMock(return_value=pares)

        report = await s.scan([BONK])
        assert report.pools_seen == 4
        # De los 4 pares solo sobrevive UNO con precio utilizable: el de otro
        # token, el vacio y el de reservas absurdas (9.52 SOL por token) se
        # descartan los tres.
        assert report.pools_usable == 1
        # Los motivos llevan el valor concreto ("precio fuera de rango (9.52)"),
        # asi que se comprueban por prefijo y no por igualdad exacta.
        assert any(k.startswith("el par no contiene el mint") for k in report.rejected)
        assert "pool sin reservas" in report.rejected
        assert any(k.startswith("precio fuera de rango") for k in report.rejected)

    @pytest.mark.asyncio
    async def test_caida_de_la_fuente_no_tumba_el_escaneo(self) -> None:
        from core.execution import SwapExecutionError

        s = _scanner()
        s._fetch_pairs = AsyncMock(side_effect=SwapExecutionError("HTTP 429"))
        report = await s.scan([BONK])
        assert report.candidates == []
        assert report.error
        assert "fuente caida" in report.rejected

    @pytest.mark.asyncio
    async def test_token_sin_pools_no_es_error_de_fuente(self) -> None:
        # DexScreener devuelve {"pairs": null} para un token recien lanzado que
        # aun no ha indexado. Eso es NORMAL, no una caida: reportarlo como
        # "fuente caida" hacia pensar que el escaner estaba roto.
        from core.execution import SwapExecutionError

        s = _scanner()
        s._fetch_pairs = AsyncMock(
            side_effect=SwapExecutionError("respuesta sin lista de pares")
        )
        report = await s.scan([BONK])
        assert report.candidates == []
        assert report.unindexed == [BONK]
        assert "fuente caida" not in report.rejected
        # Lo que NO debe pasar: que se announce un error de fuente.
        assert not report.error

    @pytest.mark.asyncio
    async def test_token_con_lista_vacia_tambien_cuenta_como_sin_indexar(self) -> None:
        s = _scanner()
        s._fetch_pairs = AsyncMock(return_value=[])
        report = await s.scan([BONK])
        assert report.unindexed == [BONK]
        assert "token aun sin pools indexados" in report.rejected
        assert not report.error

    @pytest.mark.asyncio
    async def test_caida_real_de_la_fuente_sigue_siendo_error(self) -> None:
        # El caso contrario: un 429 es una caida de verdad y no se debe
        # camuflar de "token sin indexar".
        from core.execution import SwapExecutionError

        s = _scanner()
        s._fetch_pairs = AsyncMock(side_effect=SwapExecutionError("HTTP 429"))
        report = await s.scan([BONK])
        assert report.unindexed == []
        assert "fuente caida" in report.rejected
        assert report.error

    @pytest.mark.asyncio
    async def test_sin_mints_devuelve_vacio(self) -> None:
        s = _scanner()
        report = await s.scan([])
        assert report.pools_seen == 0
        assert report.candidates == []


class TestInforme:
    def test_tamano_inviable_avisa_que_el_cero_no_dice_nada(self) -> None:
        # El fallo original: con ARB_TRADE_SIZE_SOL=0.01 el coste es del 21.9%
        # y ningun arbitrageo puede salir. Un "0 oportunidades" ahi se lee como
        # "el mercado no tiene spreads" cuando en realidad es una verdad
        # tautologica del tamaño elegido.
        report = CrossPoolReport(trade_size_sol=0.01, cost_pct=21.89, pools_usable=0)
        texto = report.format(3000.0)
        assert "AVISO" in texto
        assert "ningun" in texto.lower() and "viable" in texto.lower()
        assert "ARB_TRADE_SIZE_SOL" in texto

    def test_tamano_util_no_avisa_que_es_inviable(self) -> None:
        # El aviso no puede contaminar la lectura de un resultado real.
        report = CrossPoolReport(trade_size_sol=0.05, cost_pct=2.7, pools_usable=14)
        texto = report.format(3000.0)
        assert "AVISO" not in texto
        assert "21.9" not in texto

    def test_lista_los_tokens_sin_pools(self) -> None:
        report = CrossPoolReport(trade_size_sol=1.0, cost_pct=1.7)
        report.unindexed = [BONK, JTO]
        texto = report.format(3000.0)
        assert "sin pools" in texto
        assert BONK[:8] in texto and JTO[:8] in texto

    def test_avisa_de_que_no_es_senal_de_compra(self) -> None:
        # Esto va a Telegram. Si alguien lo lee como "compra esto", el modulo
        # ha propuesto una perdida.
        report = CrossPoolReport(trade_size_sol=1.0, cost_pct=1.9)
        texto = report.format(3000.0)
        assert "no ejecuta" in texto.lower()
        assert "NO es una senal de compra" in texto

    def test_muestra_los_confirmados(self) -> None:
        c = SpreadCandidate(
            mint=BONK,
            cheap=PoolPrice(BONK, "A", "orca", SOL, 1.0, 1e5, 0.0),
            rich=PoolPrice(BONK, "B", "raydium", SOL, 1.05, 1e5, 0.0),
            net_edge_pct=3.1,
        )
        c.first_seen_ms, c.last_seen_ms, c.hits = 0.0, 5000.0, 3
        report = CrossPoolReport(confirmed=[c], candidates=[c])
        texto = report.format(3000.0)
        assert "orca" in texto and "raydium" in texto
        assert "+3.100%" in texto or "+3.1%" in texto

    def test_serializa_para_guardar(self) -> None:
        c = SpreadCandidate(
            mint=BONK,
            cheap=PoolPrice(BONK, "A", "orca", SOL, 1.0, 1e5, 0.0),
            rich=PoolPrice(BONK, "B", "raydium", SOL, 1.05, 1e5, 0.0),
            net_edge_pct=3.1,
        )
        c.first_seen_ms, c.last_seen_ms, c.hits = 0.0, 5000.0, 3
        data = report_to_dict(CrossPoolReport(confirmed=[c], candidates=[c]))
        assert data["confirmed"][0]["cheap_venue"] == "orca"
        assert data["confirmed"][0]["hits"] == 3
        assert data["confirmed"][0]["duration_ms"] == 5000.0


class TestSeparacionDeSlippage:
    def test_el_arb_no_hereda_el_slippage_del_copy_trading(self) -> None:
        # El bug conceptual: usar 500 bps (5% por lado) para arbitraje hace que
        # TODO parezca inviable. Usar el de copy trading para el round-trip si
        # es correcto, pero el cross-pool necesita el suyo.
        from core.arb_scanner import costs_from_config

        cfg = SimpleNamespace(
            ARB_SLIPPAGE_BPS=50.0,
            ARB_SWAP_FEE_PCT=0.25,
            ARB_ATA_RENT_SOL=0.00203928,
            ARB_TRADE_SIZE_SOL=1.0,
        )
        coste_arb = costs_from_config(cfg)
        coste_copy = costs_from_config(cfg, slippage_bps=500.0)
        assert coste_arb.slippage_bps == 50.0
        assert coste_copy.slippage_bps == 500.0
        assert coste_arb.total_cost_pct < coste_copy.total_cost_pct