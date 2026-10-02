"""Escaner de arbitraje ENTRE POOLS, en seco. No ejecuta nada.

POR QUE ESTE MODULO EXISTE

El escaner de `core/arb_scanner.py` hace `SOL -> token -> SOL` pasando siempre
por Jupiter, o sea por la MEJOR ruta que Jupiter encuentra. Si esa ruta existe,
no hay nada que arbitrar. Lo unico que mide ese modulo es cuanto tardamos, y su
conclusion ("no hay edge") no dice nada sobre lo que pasa entre dos AMMs
distintos: Jupiter ya habria ejecutado la combinacion optima si existiera.

Este modulo mira otra cosa: el MISMO token cotizado en DEX DISTINTAS, y si esa
diferencia de precio se puede capturar. Que es el arbitraje que de verdad existe,
y tambien el que se puede perder de la vista en cuanto se mira dos veces.

QUE MIDE

Para un mint, precio normalizado del token en cada pool, el spread entre el pool
mas barato y el mas caro, ese spread descontando TODOS los costes de un ciclo
reciente, y sobre todo CUANTO TIEMPO lleva existiendo. Un spread que dura
80ms no se puede capturar desde un bot que tarda 500ms en mirar. Uno que lleva
5 segundos visto, repetido en N pasadas, es otra historia.

QUE NO MIDE, Y HAY QUE DECIRLO

No mide si ese spread es EJECUTABLE. No construye transaccion, no simula el
impacto real en las reservas, no envia bundle. Mide una condicion necesaria, no suficiente:
que el spread exista, sea netto tras costes, y dure mas que nosotros. Que un
spread de este tipo se convierta en dinero exige un ejecutor atomico que este
modulo no construye. Confundir "hallo un spread" con "gano dinero" es el error
que este modulo existe para no cometer.

EL ERROR QUE ARREGLA

Medir `priceUsd` a pelo sobre la respuesta de DexScreener produce numeros
absurdos (spreads del 1.6 MILLON %) porque el endpoint devuelve pares donde el
mint buscado es el token BASE en unos y el QUOTE en otros, y `priceUsd` siempre
describe el BASE. Comparar esas filas sin normalizar compara dos activos
distintos. Ademas hay pools vacios, snapshots desincronizados y precios de pools
que nadie toca hace dias. Este modulo normaliza por `baseToken.address` /
`quoteToken.address`, exige liquidez minima, y descarta lo que no puede
demostrar que exista ahora.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

import aiohttp
from loguru import logger

from core.arb_scanner import ArbCosts
from core.execution import SOL_MINT, USDC_MINT, JupiterExecutor, SwapExecutionError

# Mints que sirven como numerario para comparar pools entre si.
QUOTE_MINTS = {
    SOL_MINT: "SOL",
    USDC_MINT: "USDC",
}

# Ordenado por fiabilidad. `pairCreatedAt` es la FECHA DE CREACION DEL POOL, no
# la del snapshot: un pool de hace un ano con precio fresco tiene dos años de
# edad y hay que ignorarla para el filtro de frescura. `priceTimestamp` /
# `timestamp` si son del ultimo cambio de precio. La clave del snapshot va
# aparte porque manda en unix de SEGUNDOS (no de milisegundos) y siempre es
# la mas fiable cuando esta.
_SNAPSHOT_KEYS = ("priceTimestamp", "timestamp")
_PAIR_CREATED_KEY = "pairCreatedAt"


def _to_float(value: Any) -> float:
    """Convierte a float sin romper la pasada si viene null o texto raro."""
    try:
        out = float(value)
    except (TypeError, ValueError):
        return 0.0
    if out != out or out in (float("inf"), float("-inf")):  # NaN / inf
        return 0.0
    return out


def _token_address(token: Any) -> str:
    """Direccion de un bloque `{"address": ...}` o de un string plano."""
    if isinstance(token, dict):
        return str(token.get("address") or "")
    return str(token or "")


@dataclass(frozen=True)
class PoolPrice:
    """Precio de UN token en UN pool, ya normalizado a SOL por token."""

    mint: str
    pair_address: str
    dex_id: str
    quote_mint: str
    price_per_sol: float
    liquidity_usd: float
    age_ms: float

    @property
    def venue(self) -> str:
        return self.dex_id or self.pair_address[:8]

    def is_tradable(self, min_liquidity_usd: float) -> bool:
        return self.price_per_sol > 0 and self.liquidity_usd >= min_liquidity_usd


@dataclass
class SpreadCandidate:
    """Un par (pool barata, pool cara) que hoy tiene spread."""

    mint: str
    cheap: PoolPrice
    rich: PoolPrice
    gross_edge_pct: float = 0.0
    net_edge_pct: float = 0.0
    first_seen_ms: Optional[float] = None
    hits: int = 0
    last_seen_ms: float = 0.0
    latency_ms: float = 0.0

    @property
    def duration_ms(self) -> float:
        # `first_seen_ms` es Optional a proposito: un 0.0 de reloj es un valor
        # legitimo, asi que `if not first_seen` lo confundiria con "nunca lo
        # vimos" y le mediria duracion 0 a un spread que si lleva tiempo ahi.
        if self.first_seen_ms is None:
            return 0.0
        return max(0.0, self.last_seen_ms - self.first_seen_ms)

    def confirmed(self, min_hold_ms: float, min_hits: int) -> bool:
        """Persiste lo suficiente como para que un bot lento lo alcance."""
        return self.hits >= min_hits and self.duration_ms >= min_hold_ms


@dataclass
class CrossPoolReport:
    """Una pasada del escaner cross-pool."""

    trade_size_sol: float = 0.0
    cost_pct: float = 0.0
    pools_seen: int = 0
    pools_usable: int = 0
    unindexed: list[str] = field(default_factory=list)
    candidates: list[SpreadCandidate] = field(default_factory=list)
    confirmed: list[SpreadCandidate] = field(default_factory=list)
    rejected: dict[str, int] = field(default_factory=dict)
    latency_ms: float = 0.0
    error: str = ""

    def _reject(self, reason: str) -> None:
        self.rejected[reason] = self.rejected.get(reason, 0) + 1

    @property
    def best_net_edge_pct(self) -> float:
        return max((c.net_edge_pct for c in self.candidates), default=0.0)

    def format(self, min_hold_ms: float) -> str:
        # Un ciclo cuyo coste se acerca al 10% hace que "0 oportunidades" sea
        # una verdad tautologica, no una conclusion sobre el mercado: ningun
        # spread entre dos pools de un mismo token llega a eso, asi que el
        # escaner no puede encontrar nada SIN QUE DIGA NADA. El 10% sale de ahi:
        # un arbitrageo de un mismo token en dos DEX se mueve en centimas, y
        # la rent de ATA (0.002 SOL fijos) ya son 20% del ciclo de 0.01 SOL.
        hopeless = self.cost_pct >= 10.0
        lines = [
            "<b>ESCANER CROSS-POOL (seco, no ejecuta)</b>",
            "",
            "Compara el mismo token en DEX distintas y exige que el",
            "spread sobreviva a los costes y dure mas que nosotros.",
            "",
            f"Pools con precio utilizable: <b>{self.pools_usable}</b>",
            f"Spreads netos positivos: <b>{len(self.candidates)}</b>",
            f"Confirmados (persisten >= {min_hold_ms/1000:.1f}s): "
            f"<b>{len(self.confirmed)}</b>",
            f"Ciclo de {self.trade_size_sol:g} SOL | Coste: <b>{self.cost_pct:.2f}%</b>",
        ]

        if hopeless:
            lines += [
                "",
                f"<b>AVISO:</b> con {self.trade_size_sol:g} SOL el coste del ciclo "
                f"es {self.cost_pct:.1f}%, y la rent de ATA (SOL fijos) se come "
                "casi todo. <b>Ningun arbitrageo puede ser viable a este tamaño</b>, "
                "asi que un 0 aqui no dice nada sobre el mercado. Sube "
                "ARB_TRADE_SIZE_SOL para que la medicion sirva.",
            ]
        elif not self.candidates:
            lines += [
                "",
                "<i>Sin spreads netos. Ningun par de pools ofrece una ventaja que",
                "sobreviva a fees, slippage y rent tras los costes.</i>",
            ]

        if self.confirmed:
            lines.append("")
            for c in sorted(
                self.confirmed, key=lambda x: x.net_edge_pct, reverse=True
            )[:5]:
                lines.append(
                    f"<code>{c.mint[:10]}...</code> "
                    f"{c.cheap.venue} -> {c.rich.venue}\n"
                    f"  neto <b>{c.net_edge_pct:+.3f}%</b> | "
                    f"visto {c.duration_ms/1000:.1f}s ({c.hits} pasadas)"
                )
        elif self.candidates and not hopeless:
            lines += [
                "",
                "<i>Hay spread pero ninguno ha durado lo suficiente todavia.</i>",
            ]

        if self.rejected:
            top = ", ".join(f"{k} ({v})" for k, v in list(self.rejected.items())[:4])
            lines += ["", f"Descartados: {top}"]

        if self.unindexed:
            # No es un error: son tokens sin pools todavia. Decirlo evita que
            # se lean como una caida de la fuente.
            lines += [
                "",
                f"ℹ️ {len(self.unindexed)} token(s) sin pools Todavia (recien "
                "lanzados, DexScreener aun no los ha indexado). No comparables:",
                "   " + ", ".join(m[:8] + "..." for m in self.unindexed[:6]),
            ]

        if self.error:
            lines += ["", f"<b>Error de fuente:</b> {self.error[:120]}"]

        lines += [
            "",
            "<i>Esto NO es una senal de compra. Es una medicion: un spread que",
            "no aparece aqui no existe, y uno que aparece sigue sin estar</i>",
            "<i>ejecutado. Requiere un ejecutor atomico que este modulo no construye.</i>",
        ]
        return "\n".join(lines)


class CrossPoolScanner:
    """Mide dispersiones de precio entre pools para una lista de mints.

    No usa el `_get_quote` del executor: aqui el precio sale de las reservas
    del propio pool, no de la ruta optima de Jupiter. Son cosas distintas y
    confundirlas es justo el error que el otro escaner no puede detectar.
    """

    def __init__(
        self,
        executor: JupiterExecutor,
        costs: ArbCosts,
        *,
        source_url: str = "https://api.dexscreener.com/latest/dex/tokens",
        min_liquidity_usd: float = 25_000.0,
        min_hold_ms: float = 3_000.0,
        max_snapshot_age_ms: float = 15_000.0,
        min_hits: int = 3,
        request_timeout_s: float = 10.0,
    ) -> None:
        self.executor = executor
        self.costs = costs
        self.source_url = source_url
        self.min_liquidity_usd = min_liquidity_usd
        self.min_hold_ms = min_hold_ms
        self.max_snapshot_age_ms = max_snapshot_age_ms
        self.min_hits = max(1, min_hits)
        self.request_timeout_s = request_timeout_s
        # (mint, cheap_pair, rich_pair) -> candidato vivo entre pasadas. Aqui se
        # guarda la DURACION, que es lo que un snapshot suelto no puede dar.
        self._history: dict[tuple[str, str, str], SpreadCandidate] = {}
        self._history_limit = 50

    # ------------------------------------------------------------ normalizacion
    def _normalize_pair(
        self, pair: dict[str, Any], mint: str, sol_usd: float, now_ms: float
    ) -> tuple[Optional[PoolPrice], str]:
        """Convierte un par crudo de DexScreener en un `PoolPrice` en SOL.

        Devuelve el precio, o `None` + el motivo del descarte. El motivo importa
        tanto como el precio: si el modulo solo dijera "no hay nada", no se
        podria distinguir "no hay opportunity" de "la fuente va rota".
        """
        pair_address = str(pair.get("pairAddress") or "")
        if not pair_address:
            return None, "sin pairAddress"

        dex_id = str(pair.get("dexId") or "")

        base_mint = _token_address(pair.get("baseToken"))
        quote_mint = _token_address(pair.get("quoteToken"))

        # Solo nos sirve un pool donde el token Y el numerario esten los dos
        # presentes. Si aparece un mint que no es el nuestro, el par no es de
        # este token y su precio no dice nada (asi se fabrican spreads de un
        # millon por ciento).
        if base_mint != mint and quote_mint != mint:
            return None, "el par no contiene el mint buscado"

        # El numerario es SOL o USDC, pero NO tiene que ser el quote: un pool
        # `SOL/BONK` con BONK como quote sigue siendo un pool SOL/BONK. Exigir
        # que el numerario fuera el `quoteToken` descartaba justo la mitad de los
        # pools bonsimos, segun como la API los ordenara.
        if quote_mint in QUOTE_MINTS:
            numerario_mint = quote_mint
        elif base_mint in QUOTE_MINTS:
            numerario_mint = base_mint
        else:
            return None, f"numerario desconocido ({quote_mint[:6] or 'vacio'})"

        liquidity_usd = _to_float(
            (pair.get("liquidity") or {}).get("usd")
        )
        base_liq = _to_float((pair.get("liquidity") or {}).get("base"))
        quote_liq = _to_float((pair.get("liquidity") or {}).get("quote"))

        if base_liq <= 0 or quote_liq <= 0:
            return None, "pool sin reservas"
        if liquidity_usd <= 0:
            return None, "pool sin liquidez en USD"

        # Precio del token en el numerario del pool.
        #
        # PRIORIDAD: los campos de precio de DexScreener (`priceUsd` /
        # `priceNative`), NO el cociente de las reservas. Medido contra la API
        # real, `quote/base` se desincroniza brutalmente entre pools del mismo
        # token (variaba x10 con `priceUsd` identico en las 30 filas), mientras
        # `priceNative` coincide con `priceUsd/SOLUSD` en todos los pools con
        # numerario nativo. Las reservas se quedan como respaldo para cuando
        # esos campos no vienen.
        #
        # Y `priceUsd` SIEMPRE describe el token BASE. Si nuestro mint es el
        # QUOTE, el precio es el inverso. Ese fue el error original: leer
        # `priceUsd` a pelo y comparar contra pools donde el mint era el base
        # daba spreads de 1.6 MILLON %.
        price_native = _to_float(pair.get("priceNative"))
        price_usd = _to_float(pair.get("priceUsd"))

        price_in_numerario = 0.0
        if base_mint == mint:
            if numerario_mint == SOL_MINT and price_native > 0:
                price_in_numerario = price_native
            elif price_usd > 0:
                # Numerario USDC (u otro con equivalente USD directo).
                price_in_numerario = price_usd
        else:
            # El mint buscado es el QUOTE: `priceUsd`/`priceNative` describen
            # el otro activo, asi que el precio del nuestro es su inverso.
            if numerario_mint == SOL_MINT and price_native > 0:
                price_in_numerario = 1.0 / price_native
            elif price_usd > 0:
                price_in_numerario = 1.0 / price_usd

        if price_in_numerario <= 0:
            # Respaldo: cociente de reservas. Menos fiable, pero mejor que nada.
            liq_de_mint = base_liq if base_mint == mint else quote_liq
            liq_del_numerario = (
                quote_liq if numerario_mint == quote_mint else base_liq
            )
            price_in_numerario = liq_del_numerario / liq_de_mint if liq_de_mint > 0 else 0.0

        if numerario_mint == SOL_MINT:
            price_per_sol = price_in_numerario
        else:  # USDC -> convertir a SOL para que ambos pools sean comparables
            if sol_usd <= 0:
                return None, "sin precio de SOL para normalizar USDC"
            price_per_sol = price_in_numerario / sol_usd

        if price_per_sol <= 0:
            return None, "precio cero o negativo"

        age_ms = self._pair_age_ms(pair, now_ms)
        if age_ms > self.max_snapshot_age_ms:
            return None, f"snapshot viejo ({age_ms/1000:.1f}s)"

        # El valor del pool tiene que ser creible. DexScreener devuelve a veces
        # pares con reservas absurdas (1.000.000 de SOL contra 105.000 de un
        # token que vale 0.0001) que producen spreads de 94.000% y son
        # del todo mentira. Se descarta si el pool no llega al minimo de
        # liquidez o si el precio cae fuera de un rango plausible.
        if liquidity_usd < self.min_liquidity_usd:
            return None, "liquidez por debajo del minimo"
        if not self._plausible_price(price_per_sol):
            return None, f"precio fuera de rango ({price_per_sol:.3g})"

        return (
            PoolPrice(
                mint=mint,
                pair_address=pair_address,
                dex_id=dex_id,
                quote_mint=numerario_mint,
                price_per_sol=price_per_sol,
                liquidity_usd=liquidity_usd,
                age_ms=age_ms,
            ),
            "",
        )

    def _pair_age_ms(self, pair: dict[str, Any], now_ms: float) -> float:
        """Antiguedad del DATO DE PRECIO del pool; 0 si la fuente no lo da.

        `now_ms` TIENE que venir de `time.time()`, no de `time.monotonic()`: los
        timestamps de DexScreener son unix, y compararlos contra un reloj
        monotono daria siempre negativo (y por tanto "sin edad", el peor caso:
        un dato viejo haciendose pasar por fresco).

        Se mira `priceTimestamp`/`timestamp`, NUNCA la fecha de creacion del
        pool: usar la creacion descartaba pools liquidsisimos de hace meses,
        que es justo donde vive el arbitrageo real.
        """
        stamp_ms = 0.0
        for key in _SNAPSHOT_KEYS:
            raw = pair.get(key)
            if raw in (None, ""):
                continue
            value = _to_float(raw)
            if value <= 0:
                continue
            # Unix en segundos, salvo que venga en milisegundos (>1e11).
            stamp_ms = value / 1000.0 if value > 1e11 else value
            break
        if stamp_ms <= 0:
            # Sin timestamp de precio no hay evidencia de que sea viejo: se
            # acepta con edad 0, que es "desconocida", no "recien hecho".
            return 0.0
        age_ms = now_ms - stamp_ms * 1000.0
        # Un timestamp en el futuro es dato roto, no(pool muy fresco).
        if age_ms < 0:
            return float("inf")
        return age_ms

    @staticmethod
    def _pool_age_days(pair: dict[str, Any], now_ms: float) -> float:
        """Antidad de dias que lleva existiendo el pool (dato informativo)."""
        created = _to_float(pair.get(_PAIR_CREATED_KEY))
        if created <= 0:
            return 0.0
        secs = created / 1000.0 if created > 1e11 else created
        return max(0.0, (now_ms / 1000.0 - secs) / 86_400.0)

    async def _fetch_sol_usd(self, session: aiohttp.ClientSession) -> float:
        """Precio de SOL en USD, desde el par SOL/USDC mas liquido.

        Es IMPRESCINDIBLE, no un detalle: los pools de un mismo token vienen
        mitad cotizados en SOL y mitad en USDC. Compararlos sin convertir es
        lo que produce el factor-100 fantasma que ensucio las mediciones
        previas (un pool a 3.2e-8 SOL/token contra otro a 3.8e-6 "USDC/token").
        Con un precio de SOL equivocado, el spread sale del signo contrario y
        el escaner inventa oportunidades, o las esconde.

        Se pide el par mas LIQUIDO y no el primero que aparezca: entre pools
        del mismo par hay diferencias de precio, y con los que casi no tienen
        volumen el precio esta distorsionado.
        """
        url = f"{self.source_url}/{SOL_MINT}"
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_s)
        async with session.get(url, timeout=timeout) as resp:
            if resp.status != 200:
                raise SwapExecutionError(f"HTTP {resp.status} al pedir el precio de SOL")
            data = await resp.json()
        pairs = data.get("pairs") if isinstance(data, dict) else None
        if not isinstance(pairs, list):
            raise SwapExecutionError("respuesta sin precio de SOL")

        best_price = 0.0
        best_liquidity = 0.0
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            base = _token_address(pair.get("baseToken"))
            quote = _token_address(pair.get("quoteToken"))
            liquidity = pair.get("liquidity") or {}
            base_liq = _to_float(liquidity.get("base"))
            quote_liq = _to_float(liquidity.get("quote"))
            usd_liq = _to_float(liquidity.get("usd"))
            if base_liq <= 0 or quote_liq <= 0:
                continue
            if base == SOL_MINT and quote == USDC_MINT:
                price = quote_liq / base_liq
            elif base == USDC_MINT and quote == SOL_MINT:
                price = base_liq / quote_liq
            else:
                continue
            if usd_liq > best_liquidity:
                best_liquidity = usd_liq
                best_price = price

        if best_price <= 0:
            raise SwapExecutionError("sin par SOL/USDC utilizable")
        return best_price

    def _sol_usd_from_pairs(self, pairs: Sequence[dict[str, Any]]) -> float:
        """Precio de SOL deducido de los pools de la propia respuesta, si sale.

        Es un atajo opportunistico: si la respuesta ya trae un par SOL/USDC no
        hace falta otra llamada. Pero NO es la fuente principal porque el
        endpoint se pregunta por el token objetivo, asi que casi nunca viene
        ese par. El precio de SOL se resuelve con `_fetch_sol_usd`, que ademas
        elige el pool mas liquido en lugar de fiarse del primero.
        """
        for pair in pairs:
            base = _token_address(pair.get("baseToken"))
            quote = _token_address(pair.get("quoteToken"))
            bl = _to_float((pair.get("liquidity") or {}).get("base"))
            ql = _to_float((pair.get("liquidity") or {}).get("quote"))
            if bl <= 0 or ql <= 0:
                continue
            if base == SOL_MINT and quote == USDC_MINT:
                return ql / bl
            if base == USDC_MINT and quote == SOL_MINT:
                return bl / ql
        return 0.0

    @staticmethod
    def _plausible_price(price_per_sol: float) -> bool:
        """Filtro de escala contra reservas absurdas o decimales corruptos.

        Un precio de 1e-14 SOL por token o de 9.5 SOL por token casi nunca es
        un precio real: es un par con las reservas mal escritas o con la
        orientacion mal interpretada. CORTAR esto no garantiza que el precio
        sea correcto, solo descarta los casos donde es demostrablemente
        imposible que lo sea. Un precio "plausible pero equivocado" sigue
        siendo posible, y por eso este modulo no ejecuta nada.
        """
        return 1e-12 <= price_per_sol <= 1.0

    # ------------------------------------------------------------- red
    async def _fetch_pairs(self, session: aiohttp.ClientSession, mint: str) -> list[dict[str, Any]]:
        url = f"{self.source_url}/{mint}"
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_s)
        async with session.get(url, timeout=timeout) as resp:
            if resp.status != 200:
                raise SwapExecutionError(f"HTTP {resp.status} al pedir pares")
            data = await resp.json()
        pairs = data.get("pairs") if isinstance(data, dict) else None
        if not isinstance(pairs, list):
            raise SwapExecutionError("respuesta sin lista de pares")
        return [p for p in pairs if isinstance(p, dict)]

    # ------------------------------------------------------------- analisis
    def _evaluate(
        self, pools: Sequence[PoolPrice], report: CrossPoolReport
    ) -> list[SpreadCandidate]:
        """Compara pools de un mismo token y busca el spread que survive."""
        if len(pools) < 2:
            return []

        # Un mismo pool puede venir repetido en la respuesta; comparar un pool
        # consigo mismo da 0% y falsea la mediana de liquidez.
        unique: dict[str, PoolPrice] = {}
        for p in pools:
            unique.setdefault(p.pair_address, p)
        distinct = list(unique.values())
        if len(distinct) < 2:
            return []

        cheapest = min(distinct, key=lambda p: p.price_per_sol)
        richest = max(distinct, key=lambda p: p.price_per_sol)
        if cheapest.price_per_sol <= 0 or richest.price_per_sol <= 0:
            return []

        gross = (richest.price_per_sol / cheapest.price_per_sol - 1.0) * 100.0
        if gross <= 0:
            return []

        net = gross - self.costs.total_cost_pct

        key = (cheapest.mint, cheapest.pair_address, richest.pair_address)
        now_ms = time.monotonic() * 1000.0

        if net <= 0:
            # Un spread que no cubre los costes se OLVIDA, no se queda guardado
            # con hits=1 esperando a que vuelva. Si se guardara, el siguiente
            # sighting suyo sumaria hits sobre una duracion inventada y el
            # escaner "confirmaria" un edge que en realidad nunca existio.
            self._history.pop(key, None)
            report._reject("spread menor que el coste")
            return []

        candidate = self._history.get(key)
        if candidate is None:
            candidate = SpreadCandidate(
                mint=cheapest.mint,
                cheap=cheapest,
                rich=richest,
                first_seen_ms=now_ms,
                net_edge_pct=net,
            )
        else:
            candidate.cheap = cheapest
            candidate.rich = richest
            candidate.net_edge_pct = net

        candidate.gross_edge_pct = gross
        candidate.hits += 1
        candidate.last_seen_ms = now_ms

        self._history[key] = candidate
        self._prune_history()
        return [candidate]

    def _prune_history(self) -> None:
        """Acota el historial para que un bot de dias no crezca sin limite.

        Descarta lo mas VIEJO (menor `last_seen_ms`), no lo que mas hits lleva:
        un spread confirmado lleva mas tiempo visto que uno recien aparecido, y
        ese es justo el que nos interesa conservar.
        """
        if len(self._history) <= self._history_limit:
            return
        ordered = sorted(
            self._history.items(), key=lambda kv: kv[1].last_seen_ms
        )
        for key, _ in ordered[: len(self._history) - self._history_limit]:
            self._history.pop(key, None)

    async def scan(self, mints: Sequence[str]) -> CrossPoolReport:
        """Una pasada por los mints dados."""
        report = CrossPoolReport(
            trade_size_sol=self.costs.trade_size_sol,
            cost_pct=self.costs.total_cost_pct,
        )
        if not mints:
            return report

        started = time.monotonic()
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # El precio de SOL se resuelve UNA vez para toda la pasada: es el
            # mismo para todos los mints, y pedirlo por token multiplicaria las
            # llamadas por el numero de mints (justo lo que nos esta dando 429).
            sol_usd = 0.0
            try:
                sol_usd = await self._fetch_sol_usd(session)
            except SwapExecutionError as exc:
                report.error = f"precio de SOL no disponible: {exc}"[:120]
                report._reject("sin precio de SOL")

            for mint in mints:
                try:
                    pairs = await self._fetch_pairs(session, mint)
                except SwapExecutionError as exc:
                    # "sin lista de pares" es la respuesta NORMAL de un token
                    # recién lanzado que DexScreener aún no ha indexado, no una
                    # caída. Se separa para que /xarb no lo reporte como error.
                    if "lista de pares" in str(exc):
                        report._reject("token aun sin pools indexados")
                        report.unindexed.append(mint)
                        continue
                    report._reject("fuente caida")
                    report.error = f"{mint[:8]}...: {exc}"[:120]
                    continue
                except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as exc:
                    report._reject("red")
                    report.error = f"{mint[:8]}...: red: {exc}"[:120]
                    continue

                if not pairs:
                    report._reject("token aun sin pools indexados")
                    report.unindexed.append(mint)
                    continue

                report.pools_seen += len(pairs)
                # Reloj de pared para comparar con los unix de la fuente; el
                # monotono de mas abajo es solo para DURACION, que es otra cosa.
                now_ms = time.time() * 1000.0
                # Si la pasada trajo su propio par SOL/USDC, tiene prioridad
                # sobre el fetched: son datos del MISMO instante y no cuestan
                # una llamada extra.
                local_sol_usd = self._sol_usd_from_pairs(pairs) or sol_usd

                usable: list[PoolPrice] = []
                for pair in pairs:
                    price, reason = self._normalize_pair(pair, mint, local_sol_usd, now_ms)
                    if price is None:
                        report._reject(reason)
                        continue
                    if not price.is_tradable(self.min_liquidity_usd):
                        report._reject("liquidez por debajo del minimo")
                        continue
                    usable.append(price)

                report.pools_usable += len(usable)
                found = self._evaluate(usable, report)
                for candidate in found:
                    candidate.latency_ms = (time.monotonic() - started) * 1000.0
                    report.candidates.append(candidate)
                    if candidate.confirmed(self.min_hold_ms, self.min_hits):
                        report.confirmed.append(candidate)
                        logger.info(
                            "CrossPool CONFIRMADO {} {} -> {} con {:+.3f}% neto "
                            "durante {:.1f}s ({} pasadas)",
                            mint[:10], candidate.cheap.venue, candidate.rich.venue,
                            candidate.net_edge_pct,
                            candidate.duration_ms / 1000.0,
                            candidate.hits,
                        )

        report.latency_ms = (time.monotonic() - started) * 1000.0
        return report


def scanner_from_config(arb_cfg: Any, executor: JupiterExecutor) -> CrossPoolScanner:
    """Construye el escaner desde `ArbSettings`."""
    from core.arb_scanner import costs_from_config

    return CrossPoolScanner(
        executor,
        costs_from_config(arb_cfg),
        source_url=str(getattr(arb_cfg, "ARB_XPOOL_URL",
                               "https://api.dexscreener.com/latest/dex/tokens")),
        min_liquidity_usd=float(getattr(arb_cfg, "ARB_XPOOL_MIN_LIQ_USD", 25_000.0)),
        min_hold_ms=float(getattr(arb_cfg, "ARB_XPOOL_MIN_HOLD_MS", 3_000)),
        max_snapshot_age_ms=float(
            getattr(arb_cfg, "ARB_XPOOL_MAX_SNAPSHOT_AGE_MS", 15_000)
        ),
        min_hits=int(getattr(arb_cfg, "ARB_XPOOL_SNAPSHOTS", 3)),
    )


def report_to_dict(report: CrossPoolReport) -> dict[str, Any]:
    """Serializa para guardar entre reinicios."""
    return {
        "trade_size_sol": report.trade_size_sol,
        "cost_pct": report.cost_pct,
        "pools_seen": report.pools_seen,
        "pools_usable": report.pools_usable,
        "latency_ms": report.latency_ms,
        "rejected": report.rejected,
        "confirmed": [
            {
                "mint": c.mint,
                "cheap_venue": c.cheap.venue,
                "rich_venue": c.rich.venue,
                "cheap_pair": c.cheap.pair_address,
                "rich_pair": c.rich.pair_address,
                "gross_edge_pct": c.gross_edge_pct,
                "net_edge_pct": c.net_edge_pct,
                "duration_ms": c.duration_ms,
                "hits": c.hits,
            }
            for c in report.confirmed
        ],
    }