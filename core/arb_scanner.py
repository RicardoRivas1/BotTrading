"""Escaner de arbitraje en SECO: mide si hay edge real y, sobre todo, si es
alcanzable con NUESTRA latencia.

QUE MIDE (y que NO)

Mide el round-trip `SOL -> token -> SOL` cotizado por Jupiter, descontando las
dos fees, el slippage y la rent de ATA. Si el ciclo sale con beneficio, hay
oportunidad.

NO mide, y conviene decirlo claro, el arbitrageo de latencia/MEV que es el
que mueve las wallets grandes (bundles de prioridad, ejecucion colocada junto
al validador). Ese edge desaparece en ~200ms y solo existe para quien llega
primero. Aqui no se puede replicar: el unico dato honesto que sale de este
modulo es la LATENCIA del ciclo, y esa es justo la razon por la que ese
arbitraje no se puede copiar desde un servicio web normal.

Por eso el escaner NO ejecuta nada: es un instrumento de medida, no una
estrategia. Si no encuentra oportunidades, la conclusion honesta es que este
camino no da dinero, no que falte probarlo mas tiempo.
"""

from __future__ import annotations

import asyncio
import statistics
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Sequence

import aiohttp
from loguru import logger

from core.execution import SOL_MINT, JupiterExecutor, SwapExecutionError


@dataclass(frozen=True)
class ArbCosts:
    """Costes de un ciclo completo, en SOL y en % del capital."""

    swap_fee_pct: float
    ata_rent_sol: float
    slippage_bps: float
    trade_size_sol: float

    @property
    def slippage_pct(self) -> float:
        """Slippage como PORCENTAJE (no como fraccion).

        500 bps son un 5%, o sea `bps / 100 = 5.0` en unidades de porcentaje.
        Dividir entre 10_000 daba 0.05 y se sumaba a un valor que ya estaba en
        porcentaje: el slippage quedaba CONTADO 100 veces mas pequeno. Con eso
        el ciclo de 0.01 SOL salia "barato" cuando en realidad era carisimo.
        """
        return self.slippage_bps / 100.0

    @property
    def fee_cost_sol(self) -> float:
        """Fees de los dos swaps + slippage, como fraccion del ciclo."""
        return (self.swap_fee_pct * 2.0 + self.slippage_pct * 2.0) / 100.0

    @property
    def fixed_cost_sol(self) -> float:
        return self.ata_rent_sol

    @property
    def total_cost_pct(self) -> float:
        """Coste total como % del tamaño del ciclo.

        El rent de ATA no es un porcentaje: son SOL fijos, asi que se DIVIDE
        entre el tamaño del ciclo. Por eso un ciclo pequeño tiene un coste
        brutal y uno grande es casi gratis.
        """
        variable = self.fee_cost_sol * 100.0
        fixed = (
            (self.fixed_cost_sol / self.trade_size_sol * 100.0)
            if self.trade_size_sol > 0
            else 0.0
        )
        return variable + fixed


@dataclass
class CycleResult:
    """Resultado de un round-trip sobre un mint."""

    mint: str
    tokens_bought: float = 0.0
    sol_back: float = 0.0
    gross_edge_pct: float = 0.0
    net_edge_pct: float = 0.0
    latency_ms: float = 0.0
    viable: bool = False
    error: str = ""

    @property
    def has_route(self) -> bool:
        return not self.error


@dataclass
class ScanReport:
    """Resumen de una pasada del escaner."""

    trade_size_sol: float = 0.0
    cost_pct: float = 0.0
    scanned: int = 0
    with_route: int = 0
    opportunities: list[CycleResult] = field(default_factory=list)
    latencies_ms: list[float] = field(default_factory=list)
    errors: dict[str, int] = field(default_factory=dict)
    elapsed_ms: float = 0.0

    @property
    def best_edge_pct(self) -> float:
        return max((c.net_edge_pct for c in self.opportunities), default=0.0)

    @property
    def median_latency_ms(self) -> float:
        return statistics.median(self.latencies_ms) if self.latencies_ms else 0.0

    def viable(self, max_latency_ms: float) -> bool:
        """Es util solo si ademas de hallar edge somos mas rapido que la ventana."""
        if not self.opportunities:
            return False
        return self.median_latency_ms <= max_latency_ms

    def format(self, max_latency_ms: float) -> str:
        """Resumen legible para Telegram."""
        lines = [
            "<b>ESCANER DE ARBITRAJE (seco, no ejecuta)</b>",
            "",
            f"Ciclo: {self.trade_size_sol:g} SOL | Coste total: <b>{self.cost_pct:.2f}%</b>",
            f"Tokens con ruta: {self.with_route}/{self.scanned}",
            f"Oportunidades netas: <b>{len(self.opportunities)}</b>",
        ]
        if self.best_edge_pct > 0:
            lines.append(f"Mejor edge: <b>{self.best_edge_pct:+.3f}%</b>")

        if self.latencies_ms:
            lines += [
                "",
                f"Latencia mediana del ciclo: <b>{self.median_latency_ms:.0f} ms</b> "
                f"(ventana tipica: {max_latency_ms:.0f} ms)",
            ]
            if self.median_latency_ms > max_latency_ms:
                lines.append(
                    f"<b>NO VIABLE</b>: tardamos "
                    f"{self.median_latency_ms - max_latency_ms:.0f} ms mas de lo que "
                    "dura la oportunidad. Aunque hallemos edge, llegariamos tarde."
                )
        if self.errors:
            top = ", ".join(f"{m[:28]} ({n})" for m, n in list(self.errors.items())[:3])
            lines.append(f"Sin cotizar: {top}")

        if not self.opportunities:
            lines += [
                "",
                "<i>Sin oportunidades. Con este tamaño el coste se come el edge: "
                "subir ARB_TRADE_SIZE_SOL o cambiar de estrategia.</i>",
            ]
        return "\n".join(lines)


def _extract_out_amount(quote: dict[str, Any]) -> float:
    """Saca el outAmount de una respuesta de Jupiter de forma defensiva."""
    try:
        return float(quote.get("outAmount", 0) or 0)
    except (TypeError, ValueError):
        return 0.0


class ArbScanner:
    """Escanea round-trips SOL->token->SOL con las cotizaciones del executor.

    Reusa `_get_quote` a proposito: hereda su cache, su throttle, su
    circuit-breaker y su fallback. Si el escaner abriera su propia sesion HTTP
    se saltaria el rate limit de Jupiter y nos tumbaria la API, que es
    justamente lo que hay que proteger mientras el bot opera.
    """

    def __init__(
        self,
        executor: JupiterExecutor,
        costs: ArbCosts,
        min_edge_pct: float = 0.30,
    ) -> None:
        self.executor = executor
        self.costs = costs
        self.min_edge_pct = min_edge_pct

    async def _round_trip(self, session: aiohttp.ClientSession, mint: str) -> CycleResult:
        """Un ciclo completo para un mint, con su latencia medida."""
        result = CycleResult(mint=mint)
        amount_lamports = int(self.costs.trade_size_sol * 1_000_000_000)
        started = time.monotonic()

        try:
            buy_quote = await self.executor._get_quote(session, SOL_MINT, mint, amount_lamports)
            if buy_quote.get("simulated"):
                result.error = "cotizacion simulada (sin ruta real)"
                return result

            tokens = _extract_out_amount(buy_quote)
            if tokens <= 0:
                result.error = "sin ruta de compra"
                return result
            result.tokens_bought = tokens

            sell_quote = await self.executor._get_quote(session, mint, SOL_MINT, int(tokens))
            if sell_quote.get("simulated"):
                result.error = "cotizacion simulada (sin ruta de venta)"
                return result

            sol_back_raw = _extract_out_amount(sell_quote)
            if sol_back_raw <= 0:
                result.error = "sin ruta de venta"
                return result

        except SwapExecutionError as exc:
            result.error = str(exc)[:120]
            return result
        except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as exc:
            result.error = f"red: {exc}"[:120]
            return result
        finally:
            result.latency_ms = (time.monotonic() - started) * 1000.0

        # La compra devuelve tokens; la venta devuelve lamports de SOL.
        result.sol_back = sol_back_raw / 1_000_000_000
        invested = self.costs.trade_size_sol
        result.gross_edge_pct = ((result.sol_back - invested) / invested) * 100.0
        result.net_edge_pct = result.gross_edge_pct - self.costs.total_cost_pct
        result.viable = result.net_edge_pct >= self.min_edge_pct
        return result

    async def scan(self, mints: Sequence[str]) -> ScanReport:
        """Escanea los mints dados y devuelve el informe."""
        report = ScanReport(
            trade_size_sol=self.costs.trade_size_sol,
            cost_pct=self.costs.total_cost_pct,
            scanned=len(mints),
        )
        if not mints:
            return report

        started = time.monotonic()
        async with aiohttp.ClientSession() as session:
            for mint in mints:
                cycle = await self._round_trip(session, mint)
                if cycle.has_route:
                    report.with_route += 1
                    report.latencies_ms.append(cycle.latency_ms)
                else:
                    report.errors[cycle.error] = report.errors.get(cycle.error, 0) + 1
                    continue
                if cycle.viable:
                    report.opportunities.append(cycle)
                    logger.info(
                        "ArbScanner: OPORTUNIDAD {} con {:+.3f}% neto en {:.0f} ms",
                        mint[:12] + "...", cycle.net_edge_pct, cycle.latency_ms,
                    )
        report.elapsed_ms = (time.monotonic() - started) * 1000.0
        return report


async def scan_from_mints(
    executor: JupiterExecutor,
    mints: Iterable[str],
    *,
    trade_size_sol: float,
    swap_fee_pct: float = 0.25,
    ata_rent_sol: float = 0.00203928,
    slippage_bps: float = 500.0,
    min_edge_pct: float = 0.30,
) -> ScanReport:
    """Atajo: construye el scanner y lanza una pasada."""
    costs = ArbCosts(
        swap_fee_pct=swap_fee_pct,
        ata_rent_sol=ata_rent_sol,
        slippage_bps=slippage_bps,
        trade_size_sol=trade_size_sol,
    )
    scanner = ArbScanner(executor, costs, min_edge_pct=min_edge_pct)
    return await scanner.scan(list(mints))


def costs_from_config(arb_cfg: Any, slippage_bps: Optional[float] = None) -> ArbCosts:
    """Construye los costes desde la config.

    `slippage_bps` va aparte a proposito: el del copy trading vive en
    `trading.SLIPPAGE_BPS` y son 500 bps = 5% por lado, tolerancia correcta para
    una memecoin volatil pero absurda para arbitraje entre pools liquidos.
    Mezclarlos hacia que el escaner O NO vea ningun caso viable, que es una
    conclusion falsa tan peligrosa como la contraria. Por defecto se usa
    `ARB_SLIPPAGE_BPS`, que es lo que el arbitraje necesita; el de trading solo
    se usa si se pide expresamente.
    """
    resolved = slippage_bps if slippage_bps is not None else float(
        getattr(arb_cfg, "ARB_SLIPPAGE_BPS", 50.0)
    )
    return ArbCosts(
        swap_fee_pct=float(getattr(arb_cfg, "ARB_SWAP_FEE_PCT", 0.25)),
        ata_rent_sol=float(getattr(arb_cfg, "ARB_ATA_RENT_SOL", 0.00203928)),
        slippage_bps=resolved,
        trade_size_sol=float(getattr(arb_cfg, "ARB_TRADE_SIZE_SOL", 0.01)),
    )


def report_to_dict(report: ScanReport) -> dict[str, Any]:
    """Serializa el informe para guardarlo y consultarlo despues."""
    return {
        "trade_size_sol": report.trade_size_sol,
        "cost_pct": report.cost_pct,
        "scanned": report.scanned,
        "with_route": report.with_route,
        "latencies_ms": report.latencies_ms,
        "errors": report.errors,
        "opportunities": [
            {
                "mint": c.mint,
                "tokens_bought": c.tokens_bought,
                "sol_back": c.sol_back,
                "gross_edge_pct": c.gross_edge_pct,
                "net_edge_pct": c.net_edge_pct,
                "latency_ms": c.latency_ms,
            }
            for c in report.opportunities
        ],
    }


def report_from_saved(data: Optional[dict[str, Any]], max_latency_ms: float) -> str:
    """Formatea un informe previamente serializado."""
    if not data:
        return "<b>ESCANER DE ARBITRAJE</b>\n\nTodavia no hay ninguna pasada registrada."
    report = ScanReport(
        trade_size_sol=data.get("trade_size_sol", 0.0),
        cost_pct=data.get("cost_pct", 0.0),
        scanned=data.get("scanned", 0),
        with_route=data.get("with_route", 0),
        latencies_ms=list(data.get("latencies_ms", [])),
        errors=dict(data.get("errors", {})),
        opportunities=[
            CycleResult(
                mint=o.get("mint", ""),
                net_edge_pct=o.get("net_edge_pct", 0.0),
                gross_edge_pct=o.get("gross_edge_pct", 0.0),
                latency_ms=o.get("latency_ms", 0.0),
                tokens_bought=o.get("tokens_bought", 0.0),
                sol_back=o.get("sol_back", 0.0),
                viable=True,
            )
            for o in data.get("opportunities", [])
        ],
    )
    return report.format(max_latency_ms)