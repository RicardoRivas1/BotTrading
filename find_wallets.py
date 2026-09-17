"""Utilidad para encontrar wallets de traders conocidos en Solana.

Uso:
    python find_wallets.py cupsey
    python find_wallets.py "barron trump"
    python find_wallets.py --top-pumpfun
"""

from __future__ import annotations

import asyncio
import sys
from typing import Optional

import aiohttp


async def search_gmgn(query: str) -> list[dict]:
    """Busca traders en GMGN."""
    results = []
    try:
        url = f"https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?orderby=profit_percent&direction=desc&filters[]=pump"
        headers = {"User-Agent": "Mozilla/5.0"}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    wallets = data.get("data", {}).get("rank", [])
                    for w in wallets[:20]:
                        addr = w.get("address", "")
                        label = w.get("name", w.get("address", "")[:8])
                        pnl = w.get("total_profit_percent", 0)
                        trades = w.get("swap_count", 0)
                        if addr:
                            results.append({
                                "wallet": addr,
                                "label": label,
                                "pnl": pnl,
                                "trades": trades,
                                "source": "gmgn",
                            })
    except Exception as exc:
        print(f"Error buscando en GMGN: {exc}")
    return results


async def search_pumpfun() -> list[dict]:
    """Busca traders activos en Pump.fun."""
    results = []
    try:
        url = "https://frontend-api.pump.fun/leaderboard"
        headers = {"User-Agent": "Mozilla/5.0"}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for entry in data[:20]:
                        wallet = entry.get("address", "")
                        username = entry.get("username", "")
                        pnl = entry.get("total_pnl", 0)
                        trades = entry.get("trade_count", 0)
                        if wallet:
                            results.append({
                                "wallet": wallet,
                                "label": username or wallet[:8],
                                "pnl": pnl,
                                "trades": trades,
                                "source": "pump.fun",
                            })
    except Exception as exc:
        print(f"Error buscando en Pump.fun: {exc}")
    return results


async def search_birdeye() -> list[dict]:
    """Busca traders en Birdeye."""
    results = []
    try:
        url = "https://public-api.birdeye.so/defi/v3/token/holder?address=So11111111111111111111111111111111111111112&offset=0&limit=20"
        headers = {"User-Agent": "Mozilla/5.0"}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    holders = data.get("data", {}).get("items", [])
                    for h in holders[:20]:
                        addr = h.get("holderAddress", "")
                        amount = h.get("amount", 0)
                        if addr and amount > 0:
                            results.append({
                                "wallet": addr,
                                "label": f"holder-{addr[:8]}",
                                "pnl": 0,
                                "trades": 0,
                                "source": "birdeye",
                            })
    except Exception as exc:
        print(f"Error buscando en Birdeye: {exc}")
    return results


def print_results(results: list[dict], query: str = "") -> None:
    """Imprime resultados de busqueda."""
    if not results:
        print(f"\nNo se encontraron wallets para '{query}'")
        print("\nSugerencias:")
        print("1. Busca manualmente en https://gmgn.ai/sol")
        print("2. Busca en https://birdeye.so/leaderboard/solana")
        print("3. Busca en https://axiom.trade/leaderboard")
        print("4. Usa https://photon-sol.tinyastro.io para analizar wallets")
        return

    print(f"\n{'='*80}")
    print(f"WALLETS ENCONTRADAS ({len(results)} resultados)")
    print(f"{'='*80}")
    print(f"{'Label':<20} {'Wallet':<45} {'PnL':<12} {'Trades':<10} {'Fuente'}")
    print(f"{'-'*80}")

    for r in results:
        label = r["label"][:18]
        wallet = r["wallet"][:44]
        pnl = f"{r['pnl']:.1f}%" if isinstance(r['pnl'], (int, float)) else str(r['pnl'])
        trades = str(r["trades"])
        source = r["source"]
        print(f"{label:<20} {wallet:<45} {pnl:<12} {trades:<10} {source}")

    print(f"\n{'='*80}")
    print("PARA CONFIGURAR EN .env:")
    print("-" * 80)
    wallets = [r["wallet"] for r in results[:5]]
    labels = [r["label"] for r in results[:5]]
    print(f"COPY_TRADE_WALLET_ADDRESSES={','.join(wallets)}")
    print(f"COPY_TRADE_WALLET_LABELS={','.join(labels)}")
    print(f"{'='*80}")


async def main():
    if len(sys.argv) < 2:
        print("Uso:")
        print("  python find_wallets.py --top-pumpfun    # Top traders de Pump.fun")
        print("  python find_wallets.py --top-gmgn       # Top traders en GMGN")
        print("  python find_wallets.py <nombre>          # Buscar por nombre")
        return

    arg = sys.argv[1]

    if arg == "--top-pumpfun":
        print("Buscando top traders en Pump.fun...")
        results = await search_pumpfun()
        print_results(results, "top pump.fun")

    elif arg == "--top-gmgn":
        print("Buscando top traders en GMGN...")
        results = await search_gmgn("")
        print_results(results, "top gmgn")

    elif arg == "--all":
        print("Buscando en todas las fuentes...")
        all_results = []
        all_results.extend(await search_pumpfun())
        all_results.extend(await search_gmgn(""))
        print_results(all_results, "todas las fuentes")

    else:
        print(f"Buscando wallets para '{arg}'...")
        # Buscar en GMGN
        results = await search_gmgn(arg)
        # Agregar resultados de pump.fun filtrados
        pf_results = await search_pumpfun()
        for r in pf_results:
            if arg.lower() in r.get("label", "").lower():
                results.append(r)
        print_results(results, arg)


if __name__ == "__main__":
    asyncio.run(main())
