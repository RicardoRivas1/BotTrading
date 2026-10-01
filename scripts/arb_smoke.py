"""Smoke test real del escaner: una pasada contra Jupiter en vivo.

No es un test unitario (va contra la red), asi que va aparte. Su proposito es
comprobar de verdad la afirmacion central: el escaner mide, y lo que mide
sobre un par LIQUIDO dice lo que tiene que decir.
"""

import asyncio
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.arb_scanner import ArbScanner
from core.execution import JupiterExecutor


async def main() -> None:
    executor = JupiterExecutor.__new__(JupiterExecutor)
    # Sesion minima: el escaner hereda throttle/cache del executor real, pero
    # para este smoke test solo necesitamos el cotizador.
    import time

    executor._quote_cache = {}
    executor._jupiter_lock = asyncio.Lock()
    executor._jupiter_last_call = 0.0
    executor._jupiter_blocked_until = 0.0
    executor.slippage_bps = 500
    from core.arb_scanner import ArbCosts

    # USDC: el par mas liquido de Solana. Un round-trip debe ser ~neutro.
    mints = {
        "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    }

    for size in (0.01, 1.0):
        costs = ArbCosts(
            swap_fee_pct=0.25, ata_rent_sol=0.00203928, slippage_bps=500.0,
            trade_size_sol=size,
        )
        scanner = ArbScanner(executor, costs, min_edge_pct=0.30)
        report = await scanner.scan(list(mints.values()))
        print("=" * 62)
        print(f"CICLO DE {size} SOL -> coste total {report.cost_pct:.2f}%")
        print(f"con ruta: {report.with_route}/{report.scanned} | "
              f"latencia mediana: {report.median_latency_ms:.0f} ms")
        print(f"oportunidades: {len(report.opportunities)} | "
              f"mejor edge: {report.best_edge_pct:+.3f}%")
        if report.errors:
            print(f"errores: {report.errors}")
        print("-" * 62)
        for c in report.opportunities:
            print(f"  {c.mint[:10]} bruto {c.gross_edge_pct:+.3f}% -> "
                  f"neto {c.net_edge_pct:+.3f}% en {c.latency_ms:.0f} ms")
        print(f"VIABLE (ventana 200ms): {report.viable(200.0)}")
        # Jupiter limita por rafaga (429): hay que dejar respirar entre pasadas
        # o la medicion mide el rate limit, no el mercado.
        await asyncio.sleep(12.0)


if __name__ == "__main__":
    asyncio.run(main())