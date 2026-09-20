"""Trade statistics tracker for win rate, PnL, and per-wallet performance.

Persists to a JSON file so stats survive restarts.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class TradeRecord:
    """Single completed trade (sell)."""

    mint: str
    symbol: str
    wallet: str  # source wallet that triggered the buy
    entry_price: float  # SOL price at buy
    exit_price: float  # SOL price at sell
    pnl_pct: float  # percentage
    sol_invested: float  # SOL spent on buy
    sol_received: float  # SOL received from sell (0 if unknown)
    buy_time: float  # timestamp
    sell_time: float  # timestamp
    hold_seconds: float = 0.0
    sell_reason: str = "COPY_TRADE_SELL"
    sell_pct: float = 100.0  # percentage of position sold

    def __post_init__(self) -> None:
        if self.hold_seconds == 0.0:
            self.hold_seconds = max(0.0, self.sell_time - self.buy_time)


@dataclass
class WalletStats:
    """Aggregated stats for a single tracked wallet."""

    wallet: str
    buys: int = 0
    sells: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl_pct: float = 0.0
    total_sol_invested: float = 0.0
    total_sol_received: float = 0.0
    best_trade_pct: float = 0.0
    worst_trade_pct: float = 0.0
    avg_hold_seconds: float = 0.0

    @property
    def win_rate(self) -> float:
        return (self.wins / self.sells * 100) if self.sells > 0 else 0.0

    @property
    def net_pnl_sol(self) -> float:
        return self.total_sol_received - self.total_sol_invested


class TradeStats:
    """Global trade statistics with JSON persistence."""

    def __init__(self, path: str = "trade_stats.json") -> None:
        self.path = Path(path)
        self.trades: list[TradeRecord] = []
        self.wallets: dict[str, WalletStats] = {}
        self.total_buys: int = 0
        self.total_sells: int = 0
        self.positions_opened: int = 0
        self.positions_closed: int = 0
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            # Migracion: stats legacy (antes de "positions_opened") contaban cada
            # BUY de acumulacion DCA como compra, inflando total_buys/open_positions
            # y el PnL (fórmula vieja comparaba SOL del trader vs inversión del bot).
            # Se descartan para arrancar desde cero con métricas correctas.
            if "positions_opened" not in data:
                self.path.unlink(missing_ok=True)
                return
            self.total_buys = data.get("total_buys", 0)
            self.total_sells = data.get("total_sells", 0)
            self.positions_opened = data.get("positions_opened", 0)
            self.positions_closed = data.get("positions_closed", 0)
            for t in data.get("trades", []):
                self.trades.append(TradeRecord(**t))
            for w_data in data.get("wallets", {}).values():
                ws = WalletStats(**w_data)
                self.wallets[ws.wallet] = ws
        except Exception:
            pass

    def _save(self) -> None:
        try:
            data = {
                "total_buys": self.total_buys,
                "total_sells": self.total_sells,
                "positions_opened": self.positions_opened,
                "positions_closed": self.positions_closed,
                "trades": [asdict(t) for t in self.trades[-500:]],  # keep last 500
                "wallets": {w: asdict(ws) for w, ws in self.wallets.items()},
            }
            self.path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception:
            pass

    def record_buy(self, wallet: str, new_position: bool = True) -> None:
        """Record that a buy signal was received from a wallet.

        Args:
            wallet: wallet that triggered the buy.
            new_position: True si abre una posición NUEVA (default). False para
                acumulaciones DCA (se registran en total_buys pero NO abren una
                nueva posición abierta).
        """
        self.total_buys += 1
        ws = self._get_wallet(wallet)
        if new_position:
            self.positions_opened += 1
            ws.buys += 1
        self._save()

    def record_sell(
        self,
        mint: str,
        symbol: str,
        wallet: str,
        entry_price: float,
        exit_price: float,
        pnl_pct: float,
        sol_invested: float,
        sol_received: float,
        buy_time: float,
        sell_time: float,
        sell_reason: str = "COPY_TRADE_SELL",
        sell_pct: float = 100.0,
    ) -> TradeRecord:
        """Record a completed sell trade."""
        trade = TradeRecord(
            mint=mint,
            symbol=symbol,
            wallet=wallet,
            entry_price=entry_price,
            exit_price=exit_price,
            pnl_pct=pnl_pct,
            sol_invested=sol_invested,
            sol_received=sol_received,
            buy_time=buy_time,
            sell_time=sell_time,
            sell_reason=sell_reason,
            sell_pct=sell_pct,
        )
        self.trades.append(trade)
        self.total_sells += 1
        if sell_pct >= 99.0:
            self.positions_closed += 1

        ws = self._get_wallet(wallet)
        ws.sells += 1
        ws.total_pnl_pct += pnl_pct
        ws.total_sol_invested += sol_invested
        ws.total_sol_received += sol_received
        if pnl_pct >= 0:
            ws.wins += 1
        else:
            ws.losses += 1
        if pnl_pct > ws.best_trade_pct:
            ws.best_trade_pct = pnl_pct
        if pnl_pct < ws.worst_trade_pct:
            ws.worst_trade_pct = pnl_pct
        # Running average hold time
        n = ws.sells
        ws.avg_hold_seconds = (
            ws.avg_hold_seconds * (n - 1) + trade.hold_seconds
        ) / n if n > 0 else trade.hold_seconds

        self._save()
        return trade

    def _get_wallet(self, wallet: str) -> WalletStats:
        if wallet not in self.wallets:
            self.wallets[wallet] = WalletStats(wallet=wallet)
        return self.wallets[wallet]

    # ---- Summary methods ----

    @property
    def win_rate(self) -> float:
        return (self.total_sells - self._losses) / self.total_sells * 100 if self.total_sells > 0 else 0.0

    @property
    def _losses(self) -> int:
        return sum(1 for t in self.trades if t.pnl_pct < 0)

    @property
    def total_pnl_pct(self) -> float:
        return sum(t.pnl_pct for t in self.trades)

    @property
    def avg_pnl_pct(self) -> float:
        return self.total_pnl_pct / len(self.trades) if self.trades else 0.0

    @property
    def best_trade(self) -> Optional[TradeRecord]:
        return max(self.trades, key=lambda t: t.pnl_pct) if self.trades else None

    @property
    def worst_trade(self) -> Optional[TradeRecord]:
        return min(self.trades, key=lambda t: t.pnl_pct) if self.trades else None

    def summary(self) -> dict:
        """Return a dict summary of all stats."""
        wins = sum(1 for t in self.trades if t.pnl_pct >= 0)
        losses = sum(1 for t in self.trades if t.pnl_pct < 0)
        open_positions = self.positions_opened - self.positions_closed
        return {
            "total_buys": self.positions_opened,
            "total_sells": self.total_sells,
            "open_positions": max(0, open_positions),
            "wins": wins,
            "losses": losses,
            "win_rate_pct": round(wins / self.total_sells * 100, 1) if self.total_sells > 0 else 0.0,
            "total_pnl_pct": round(self.total_pnl_pct, 2),
            "avg_pnl_pct": round(self.avg_pnl_pct, 2),
            "best_trade_pct": round(self.best_trade.pnl_pct, 2) if self.best_trade else 0.0,
            "worst_trade_pct": round(self.worst_trade.pnl_pct, 2) if self.worst_trade else 0.0,
            "avg_hold_seconds": round(
                sum(t.hold_seconds for t in self.trades) / len(self.trades), 0
            ) if self.trades else 0.0,
            "wallets": {w: {
                "buys": ws.buys,
                "sells": ws.sells,
                "win_rate": round(ws.win_rate, 1),
                "pnl_pct": round(ws.total_pnl_pct, 2),
                "best": round(ws.best_trade_pct, 2),
                "worst": round(ws.worst_trade_pct, 2),
                "sol_invested": round(ws.total_sol_invested, 4),
            } for w, ws in self.wallets.items()},
        }

    def format_summary(self, open_positions: Optional[int] = None) -> str:
        """Human-readable summary for Telegram.

        Si se pasa `open_positions` se usa ese valor (posiciones reales actuales)
        en lugar del contador acumulado positions_opened - positions_closed, que
        se desincroniza (p.ej. el tracker cierra por TP/SL/time sin llamar
        record_sell, o se reinicia DRY_RUN con posiciones previas).
        """
        s = self.summary()
        if open_positions is not None:
            s["open_positions"] = max(0, int(open_positions))
        lines = [
            "📊 <b>ESTADISTICAS DEL BOT</b>",
            "",
            f"🔄 Compras: {s['total_buys']} | Ventas: {s['total_sells']} | Abiertas: {s['open_positions']}",
            f"✅ Wins: {s['wins']} | ❌ Losses: {s['losses']}",
            f"🎯 Win Rate: <b>{s['win_rate_pct']}%</b>",
            f"💰 PnL Total: <b>{s['total_pnl_pct']:+.2f}%</b> | Promedio: {s['avg_pnl_pct']:+.2f}%",
            f"🏆 Mejor: {s['best_trade_pct']:+.2f}% | 📉 Peor: {s['worst_trade_pct']:+.2f}%",
        ]
        if s["avg_hold_seconds"] > 0:
            m, sec = divmod(int(s["avg_hold_seconds"]), 60)
            lines.append(f"⏱️ Hold promedio: {m}m {sec}s")

        if s["wallets"]:
            lines.append("")
            lines.append("<b>Por wallet:</b>")
            for w, ws in s["wallets"].items():
                short = w[:8] + "..."
                lines.append(
                    f"  {short}: {ws['sells']} sells | WR {ws['win_rate']}% | "
                    f"PnL {ws['pnl_pct']:+.2f}% | {ws['sol_invested']:.4f} SOL"
                )

        return "\n".join(lines)


# Global singleton
_stats: Optional[TradeStats] = None


def get_trade_stats() -> TradeStats:
    global _stats
    if _stats is None:
        _stats = TradeStats()
    return _stats
