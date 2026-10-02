"""Pruebas de regresión del copy trading (clasificación y ejecución).

Cubre los fallos que hacían que el bot "se confundiera" en operaciones reales:

- Una VETA que no llega a ejecutarse (sin saldo, orden sin confirmar) NO puede
  cerrar la posición ni marcarse como ejecutada: si lo hacía, los tokens
  quedaban atrapados en la wallet y el bot perdía el registro para siempre
  ("no vende" y, al reintentar, "no compra").
- El % de venta se calcula siempre contra los tokens acumulados con la misma
  clave de trader con la que se anotan.
"""

from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from strategies.copy_trading import (
    CopyTradeSignal,
    CopyTradingStrategy,
    TrackedWallet,
)

MINT = "SoMeMiNt11111111111111111111111111111111111111pump"
TRADER = "Tr4d3rW4ll3t111111111111111111111111111111111"


def _config() -> SimpleNamespace:
    return SimpleNamespace(
        copy_trading=SimpleNamespace(
            COPY_TRADING_ENABLED=True,
            MAX_COPY_TRADE_SOL=0.01,
            MIN_COPY_TRADE_SOL=0.005,
            MAX_COPY_TRADE_POSITIONS=0,
            COPY_TRADE_BUY_DEDUP_SECONDS=3.0,
            COPY_TRADE_ALLOW_ACCUMULATE=False,
            COPY_TRADE_ORDER_BUFFER_SECONDS=0.0,
            COPY_TRADE_CAPITAL_PERCENT=0.0,
            MIN_COPY_TRADE_HOLD_SECONDS=0,
            COPY_ESTIMATED_BUY_FEE_SOL=0.001,
            COPY_ESTIMATED_SELL_FEE_SOL=0.00005,
            COPY_ESTIMATED_ATA_RENT_SOL=0.00203928,
        ),
        SLIPPAGE_BPS=500,
        trading=SimpleNamespace(DRY_RUN=False),
    )


def _strategy(tmp_path: Path) -> tuple[CopyTradingStrategy, MagicMock, MagicMock]:
    """Estrategia con executor/tracker simulados y dedup en disco aislado."""
    executor = MagicMock()
    executor.positions = {}
    executor.dry_run = False
    executor.buy_amount_sol = 0.01
    executor.get_token_price = AsyncMock(return_value=0.0)
    executor.get_wallet_token_balance = AsyncMock(return_value=0.0)
    executor.get_sol_balance = AsyncMock(return_value=1.0)
    executor._save_exec_positions = MagicMock()
    executor.sell_token = AsyncMock(return_value="sig")

    tracker = MagicMock()
    tracker.positions = {}
    tracker.get_position = MagicMock(return_value=None)
    tracker.get_positions_by_wallet = MagicMock(return_value=[])
    tracker._save_positions = MagicMock()

    notifier = MagicMock()
    for meth in ("send_buy", "send_error", "send_status"):
        setattr(notifier, meth, AsyncMock(return_value=True))

    strategy = CopyTradingStrategy(
        executor=executor, notifier=notifier, tracker=tracker, config=_config()
    )
    # Archivos de dedup/bueno en un directorio temporal: la estrategia escribe
    # en disco al arrancar y no debe tocar los del repo.
    strategy._signatures_file = str(tmp_path / "sigs.json")
    strategy._diag_path = str(tmp_path / "diag.jsonl")
    strategy._executed_signatures = {}
    strategy.wallets = {TRADER: TrackedWallet(address=TRADER, label="trader")}
    return strategy, executor, tracker


def _sell_signal() -> CopyTradeSignal:
    return CopyTradeSignal(
        wallet=TRADER,
        action="sell",
        token_mint=MINT,
        token_symbol="TEST",
        amount_sol=0.0,
        tx_signature="sig_sell_1",
        block_time=1000.0,
        sell_sol_raw=0.0,
        trade_token_amount=1000.0,
        buy_sol_raw=0.0,
        trader_label="trader",
    )


class TestVentaNoEjecutada:
    """Si la venta no ocurrió, el estado NO puedeadvance como si hubiera."""

    async def test_venta_fallida_no_cierra_posicion_ni_marca_ejecutada(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, executor, tracker = _strategy(tmp_path)
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        sent = {"n": 0}

        async def _fake_sell(*args: object, **kwargs: object) -> bool:
            sent["n"] += 1
            return False  # la wallet no tenía saldo / la orden no se confirmó

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        stats_mock = MagicMock()
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: stats_mock)

        await strategy._execute_copy_trade(_sell_signal())

        assert sent["n"] == 1
        # La posición sigue viva: si se borrara, los tokens quedan huérfanos.
        assert MINT in strategy.executor.positions
        assert MINT in strategy.tracker.positions
        # La tx NO se marca como ejecutada: debe poder reintentarse.
        assert "sig_sell_1" not in strategy._executed_signatures
        # No se contabiliza una venta inexistente.
        stats_mock.record_sell.assert_not_called()
        # El % del trader se restaura para el reintento.
        assert strategy._wallet_mint_tokens[(TRADER, MINT)] == 1000.0

    async def test_venta_ok_cierra_posicion_y_marca_ejecutada(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, executor, tracker = _strategy(tmp_path)
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        async def _fake_sell(*args: object, **kwargs: object) -> bool:
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        stats_mock = MagicMock()
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: stats_mock)

        await strategy._execute_copy_trade(_sell_signal())

        assert MINT not in strategy.executor.positions
        assert MINT not in strategy.tracker.positions
        assert "sig_sell_1" in strategy._executed_signatures
        stats_mock.record_sell.assert_called_once()


class TestVentaParcial:
    """La venta parcial descuenta el capital UNA sola vez."""

    async def test_no_descuenta_dos_veces_el_invertido(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        exec_pos = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        tracker_pos = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        strategy.executor.positions[MINT] = exec_pos
        strategy.tracker.positions[MINT] = tracker_pos
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            # El helper compartido descuenta el capital del tracker; el test
            # reproduce ese descuento para detectar el doble.
            tracker_pos.amount = max(0.0, tracker_pos.amount * 0.75)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        signal = _sell_signal()
        signal.trade_token_amount = 250.0  # el trader vendio el 25%
        await strategy._execute_copy_trade(signal)

        # 0.01 -> 0.0075 (un solo descuento), no 0.005 (doble descuento).
        assert tracker_pos.amount == pytest.approx(0.0075)
        assert exec_pos.sol_invested == pytest.approx(0.0075)


class TestBuyEstancado:
    """El descarte de compras tardias solo aplica con blockTime conocido."""

    async def test_reentry_sin_blocktime_no_se_descarta(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Con blockTime ausente (0) el re-entry legitimo debe pasar."""
        strategy, executor, _tracker = _strategy(tmp_path)
        executor.buy_token = AsyncMock(return_value="sig_buy")
        executor.get_token_symbol = AsyncMock(return_value="TEST")
        executor.positions = {}

        async def _fake_sell(*args: object, **kwargs: object) -> bool:
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        # El trader ya vendio este token (registrado sin blockTime).
        strategy._last_sell_block[(TRADER, MINT)] = 0.0

        signal = CopyTradeSignal(
            wallet=TRADER,
            action="buy",
            token_mint=MINT,
            token_symbol="TEST",
            amount_sol=0.01,
            tx_signature="sig_buy",
            block_time=0.0,
            trade_token_amount=1000.0,
            buy_sol_raw=0.01,
            trader_label="trader",
        )
        await strategy._execute_copy_trade(signal)

        # La compra se ejecutó: no se descartó como "estancada".
        executor.buy_token.assert_awaited_once()
        assert "sig_buy" in strategy._executed_signatures


class TestPodaDeMintsObsoletos:
    """Los contadores por mint no pueden crecer sin limite durante semanas."""

    def test_olvida_lo_que_ya_no_sirve(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        # Acumulado agotado: ya no queda nada que recordar de este mint.
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 0.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.0
        strategy._last_buy_key[(TRADER, MINT)] = (time.time(), 0.01)
        strategy._last_sell_block[(TRADER, MINT)] = time.time()

        removed = strategy._prune_stale_mints()

        assert removed == 1
        assert (TRADER, MINT) not in strategy._wallet_mint_tokens
        assert (TRADER, MINT) not in strategy._wallet_mint_sol
        assert (TRADER, MINT) not in strategy._last_buy_key

    def test_nunca_poda_un_mint_con_tokens_en_la_wallet(self, tmp_path: Path) -> None:
        strategy, executor, _tracker = _strategy(tmp_path)
        executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=500.0, entry_price=1e-6, sol_invested=0.005
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 0.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.0

        strategy._prune_stale_mints()

        assert (TRADER, MINT) in strategy._wallet_mint_tokens

    def test_nunca_poda_un_mint_con_acumulado_vivo(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        strategy._prune_stale_mints()

        assert strategy._wallet_mint_tokens[(TRADER, MINT)] == 1000.0

    def test_nunca_poda_un_mint_de_costo_desconocido(
        self, tmp_path: Path
    ) -> None:
        """Si se podara, la siguiente venta volveria a inventar el PnL."""
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 0.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.0
        strategy._wallet_mint_cost_unknown.add((TRADER, MINT))

        strategy._prune_stale_mints()

        assert (TRADER, MINT) in strategy._wallet_mint_tokens

    def test_el_dedup_caducado_tambien_se_olvida(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        # Marca de hace 1 hora: su ventana util (el dedup) ya paso.
        strategy._last_buy_key[(TRADER, MINT)] = (time.time() - 3600.0, 0.01)
        strategy._last_sell_block[(TRADER, MINT)] = time.time() - 3600.0

        strategy._prune_stale_mints()

        assert (TRADER, MINT) not in strategy._last_buy_key
        assert (TRADER, MINT) not in strategy._last_sell_block

    def test_un_dedup_reciente_se_conserva(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._last_buy_key[(TRADER, MINT)] = (time.time(), 0.01)

        strategy._prune_stale_mints()

        assert (TRADER, MINT) in strategy._last_buy_key


class TestCalcSellPct:
    """El % de venta debe medirse contra los tokens del MISMO trader."""

    def test_usa_la_clave_canonica_del_trader(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 10000.0
        assert strategy._calc_sell_pct(MINT, 2500.0, TRADER) == pytest.approx(25.0)

    def test_sin_tracking_devuelve_0(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        assert strategy._calc_sell_pct(MINT, 2500.0, TRADER) == 0.0


class TestDescontrolPorAcumuladoDelTrader:
    """El acumulado del trader debe descontarse en TODO cierre.

    El síntoma era: el bot "marca bien y avanza" y de golpe, para el mismo mint,
    solo vendía el 50%, luego el 33%... Porque cada cierre que no pasaba por la
    señal de venta (TP/SL/trailing del tracker) dejaba al bot creyendo que el
    trader seguía con sus tokens, y el siguiente ciclo comparaba contra un
    acumulado cada vez mayor.
    """

    def test_cierre_del_tracker_descuenta_el_acumulado(self, tmp_path: Path) -> None:
        strategy, _executor, tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        strategy._on_tracked_position_closed(MINT, TRADER, 100.0, "TAKE_PROFIT")

        assert strategy._wallet_mint_tokens.get((TRADER, MINT), 0.0) == 0.0
        # Al agotarse, la entrada se borra: si no, arrastra el costo viejo al
        # siguiente ciclo del mismo mint.
        assert (TRADER, MINT) not in strategy._wallet_mint_sol
        assert tracker.remove_position.call_count == 0

    def test_cierre_parcial_descuenta_la_fraccion(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        strategy._on_tracked_position_closed(MINT, TRADER, 25.0, "TRAILING_STOP")

        assert strategy._wallet_mint_tokens[(TRADER, MINT)] == pytest.approx(750.0)
        # El costo se descuenta en la MISMA proporción (si no, el costo medio
        # queda mezclado y el PnL del trader se inventa).
        assert strategy._wallet_mint_sol[(TRADER, MINT)] == pytest.approx(0.0075)

    def test_hook_ignora_posiciones_sin_wallet_origen(self, tmp_path: Path) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[("otro", MINT)] = 1000.0
        strategy._on_tracked_position_closed(MINT, "", 100.0, "TAKE_PROFIT")
        assert strategy._wallet_mint_tokens[("otro", MINT)] == 1000.0

    async def test_venta_de_mint_no_poseido_tambien_descuenta(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """El bot no tiene la posición, pero el trader sí vendió: su acumulado baja.

        Es el caso normal cuando el bot empieza a copiar tarde. Antes se
        ignoraba la venta sin tocar el acumulado y este crecía para siempre.
        """
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        await strategy._execute_copy_trade(_sell_signal())

        assert strategy._wallet_mint_tokens.get((TRADER, MINT), 0.0) == 0.0
        assert (TRADER, MINT) not in strategy._wallet_mint_sol

    async def test_venta_sin_cantidad_y_sin_rpc_no_deja_el_acumulado_inflado(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sin cantidad vendida ni RPC, el acumulado se cierra en vez de crecer."""
        strategy, executor, _tracker = _strategy(tmp_path)
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01
        executor.get_wallet_token_balance = AsyncMock(return_value=-1.0)  # RPC caído

        sent: list[float] = []

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            sent.append(float(kwargs.get("sell_pct", 100.0)))
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        signal = _sell_signal()
        signal.trade_token_amount = 0.0  # Helius no quantifiable
        await strategy._execute_copy_trade(signal)

        # El acumulado se cerró (no quedó en 1000 para el siguiente ciclo) y la
        # venta se ejecutó al 100%, que es lo único defendible sin datos.
        assert strategy._wallet_mint_tokens.get((TRADER, MINT), 0.0) == 0.0
        assert sent == [100.0]


class TestPnlHonesto:
    """El PnL que se reporta debe existir; si no, se dice 'n/d'."""

    async def test_pnl_trader_no_disponible_no_se_inventa(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        # Acumulado sincronizado on-chain: son tokens de compras que el bot nunca
        # vio, así que no hay costo válido para el PnL del trader.
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.0
        strategy._wallet_mint_cost_unknown.add((TRADER, MINT))

        seen: dict[str, object] = {}

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            seen.update(kwargs)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        await strategy._execute_copy_trade(_sell_signal())

        assert seen.get("pnl_known") is False
        # Y no se cuela un PnL inventado del trader en las stats: lo que se
        # registra es el de NUESTRA copia.
        assert seen.get("pnl") == 0.0

    async def test_stats_registran_el_pnl_medido_de_nuestra_venta(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`sol_received` es el SOL real obtenido, no `invertido x (1+pnl)`."""
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            # La venta real midió: Reidio 0.015 SOL habiendo invertido 0.01.
            result = kwargs.get("result")
            if isinstance(result, dict):
                result["sol_proceeds"] = 0.015
                result["copy_pnl_measured"] = 50.0
                result["invested_portion"] = 0.01
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        stats_mock = MagicMock()
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: stats_mock)

        await strategy._execute_copy_trade(_sell_signal())

        stats_mock.record_sell.assert_called_once()
        kwargs = stats_mock.record_sell.call_args.kwargs
        assert kwargs["pnl_pct"] == 50.0  # el de la copia, no el del trader
        assert kwargs["sol_invested"] == pytest.approx(0.01)
        assert kwargs["sol_received"] == pytest.approx(0.015)  # SOL real
        assert kwargs["estimated"] is False  # medido de verdad

    async def test_venta_por_encima_del_acumulado_no_inventa_pnl(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """El trader vendió más tokens de los que le vimos comprar.

        Es el caso normal al empezar a copiar con la wallet ya cargada: nuestro
        acumulado cubre una fracción de lo que el trader tiene, así que
        `costo vendido` se recorta y el % se dispara (se vio +2090%). Sin base
        para el costo, la respuesta honesta es 'n/d'.
        """
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        # Solo le vimos comprar 1000 tk por 0.01 SOL, pero el trader vendio 2.8e7
        # (el resto venía de antes de que el bot empezara a copiar).
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        seen: dict[str, object] = {}

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            seen.update(kwargs)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        signal = _sell_signal()
        signal.trade_token_amount = 28_100_000.0
        signal.sell_sol_raw = 2.06134
        await strategy._execute_copy_trade(signal)

        assert seen.get("pnl_known") is False
        assert seen.get("pnl") == 0.0

    async def test_precio_de_polvo_no_fabrica_un_m99_99(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """La cotizacion publica de un micro-cap puede estar 10.000x bajo el real.

        En estos tokens el pool esta casi vacio y `get_token_price` devuelve un
        precio de polvo: la copia daba -99.99% mientras el trader cerraba en
        +21% sobre el MISMO token en el MISMO segundo. La referencia de salida
        correcta es el SOL que el trader acaba de negociar de verdad.
        """
        strategy, _executor, _tracker = _strategy(tmp_path)
        # Precio de mercado publicado: polvo (1e-9) frente al precio real (1e-5).
        strategy.executor.get_token_price = AsyncMock(return_value=1e-9)
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-5, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-5, amount=0.01, source_wallet=TRADER
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        seen: dict[str, object] = {}

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            seen.update(kwargs)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        signal = _sell_signal()
        signal.sell_sol_raw = 0.0121  # el trader cerro en +21%
        await strategy._execute_copy_trade(signal)

        assert seen.get("pnl") == pytest.approx(21.0, abs=0.5)
        # La estimacion de la copia se apoya en el cierre real, no en la cotizacion
        # de polvo: queda cerca del resultado del trader, nunca en -99.99%.
        est = float(seen["pnl_copy"])
        assert est == pytest.approx(21.0, abs=5.0)

    async def test_estimacion_incompatible_con_el_trader_se_descarta(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Si precio y cierre del trader se contradicen de forma absurda, no se muestra.

        Mejor `n/d` que un -99.99% (o un +120000%) inventado en la notificacion.
        """
        strategy, _executor, _tracker = _strategy(tmp_path)
        # El precio "actual" coincide con nuestra entrada, así que por precio la
        # copia rindió 0%... pero el trader negoció 1.21e-5, mil veces por encima.
        strategy.executor.get_token_price = AsyncMock(return_value=1e-11)
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-11, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-11, amount=0.01, source_wallet=TRADER
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        seen: dict[str, object] = {}

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            seen.update(kwargs)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        signal = _sell_signal()
        signal.sell_sol_raw = 0.0121
        await strategy._execute_copy_trade(signal)

        assert seen.get("pnl_known") is True
        assert seen.get("pnl_copy") is None  # la notificacion dira "Mi copia n/d"

    async def test_costo_desconocido_no_imprime_un_m100_de_mercado(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Con el trader en n/d y el pool vacío, -100% es la cotización rota.

        Se vio 'PnL trader n/d | Mi copia -100.00% (estimado)': sin costo
        conocido no hay referencia del cierre, así que se usaba la foto de
        liquidez. Un -100% "de mercado" en estos tokens no se distingue de una
        fuente rota, así que sale n/d.
        """
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy.executor.get_token_price = AsyncMock(return_value=1e-18)  # polvo
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-9, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-9, amount=0.01, source_wallet=TRADER
        )
        # Acumulado sincronizado on-chain: tokens de compras que nunca vimos.
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.0
        strategy._wallet_mint_cost_unknown.add((TRADER, MINT))

        seen: dict[str, object] = {}

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            seen.update(kwargs)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        signal = _sell_signal()
        signal.sell_sol_raw = 0.0  # sin referencia de cierre
        await strategy._execute_copy_trade(signal)

        assert seen.get("pnl_known") is False
        assert seen.get("pnl_copy") is None  # "Mi copia n/d", no un -100.00% inventado

    async def test_el_cierre_del_trader_no_necesita_costo_conocido(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """El precio de cierre es SOL recibido / tokens, no depende del costo.

        Con el costo desconocido el PnL del trader es n/d, pero el SOL que
        devolvió su venta sigue siendo una referencia de salida válida para
        estimar la copia. Antes se perdía y se caía a la cotización de polvo.
        """
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy.executor.get_token_price = AsyncMock(return_value=1e-18)  # polvo
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-5, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-5, amount=0.01, source_wallet=TRADER
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.0
        strategy._wallet_mint_cost_unknown.add((TRADER, MINT))

        seen: dict[str, object] = {}

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            seen.update(kwargs)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        signal = _sell_signal()
        signal.sell_sol_raw = 0.0121  # 1000 tk por 0.0121 SOL = 1.21e-5 por tk
        await strategy._execute_copy_trade(signal)

        assert seen.get("pnl_known") is False  # el costo sigue sin ser atribuible
        assert float(seen["pnl_copy"]) == pytest.approx(21.0, abs=5.0)

    async def test_dry_run_estima_el_pnl_de_la_copia_con_el_del_trader(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """En DRY_RUN se estima, se marca como tal y se descuenta el coste.

        Sin esto cada venta simulada acababa como 'n/d' y el trade quedaba
        fuera de las métricas, dejando /stats vacío en simulación. Pero el %
        del trader NO es nuestro: con 0.01 SOL y ~0.0031 de coste, un +20%
        suyo se convierte en una perdida para nosotros.
        """
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy.executor.dry_run = True
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        stats_mock = MagicMock()
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: stats_mock)

        signal = _sell_signal()
        signal.sell_sol_raw = 0.012
        await strategy._execute_copy_trade(signal)

        stats_mock.record_sell.assert_called_once()
        kwargs = stats_mock.record_sell.call_args.kwargs
        # El trader hizo +20%, pero el ciclo cuesta ~31% de 0.01 SOL mas el
        # 5% de slippage por lado: nuestra copia sale en perdida.
        esperado = strategy._estimate_after_costs(20.0, 0.01)
        assert kwargs["pnl_pct"] == pytest.approx(esperado)
        assert esperado < 0, "con este capital el coste debe dominar al +20%"
        assert kwargs["estimated"] is True
        assert kwargs["pnl_unreliable"] is False
        # Sin SOL observado hay que derivarlo del PnL (ya neto de costes),
        # pero declarado estimado.
        assert kwargs["sol_received"] == pytest.approx(0.01 * (1 + esperado / 100.0))

    async def test_pnl_por_precio_se_marca_como_estimado(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Con precio pero sin delta de saldo, el PnL es una estimación.

        Antes se contaba como fiable con `sol_received=0`, lo que rompía el
        'PnL Neto' (sumaba 0 de cobros sobre capital invertido).
        """
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy.executor.get_token_price = AsyncMock(return_value=1.5e-6)
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        stats_mock = MagicMock()
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: stats_mock)

        await strategy._execute_copy_trade(_sell_signal())

        kwargs = stats_mock.record_sell.call_args.kwargs
        assert kwargs["pnl_pct"] == pytest.approx(50.0)  # por precio
        assert kwargs["estimated"] is True
        assert kwargs["pnl_unreliable"] is False
        assert kwargs["sol_received"] == pytest.approx(0.015)


class TestAcumulacionSinCapitalFantasma:
    """La rama de acumulación no puede inventar capital."""

    async def test_no_infla_el_invertido_sin_comprar(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, executor, _tracker = _strategy(tmp_path)
        strategy.config.copy_trading.COPY_TRADE_ALLOW_ACCUMULATE = True
        exec_pos = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        tracker_pos = SimpleNamespace(
            mint=MINT, symbol="TEST", buy_price=1e-6, amount=0.01, source_wallet=TRADER
        )
        executor.positions[MINT] = exec_pos
        strategy.tracker.positions[MINT] = tracker_pos
        executor.buy_token = AsyncMock(return_value="sig")
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        signal = CopyTradeSignal(
            wallet=TRADER,
            action="buy",
            token_mint=MINT,
            token_symbol="TEST",
            amount_sol=0.01,
            tx_signature="sig_buy_2",
            block_time=2000.0,
            trade_token_amount=1000.0,
            buy_sol_raw=0.01,
            trader_label="trader",
        )
        await strategy._execute_copy_trade(signal)

        executor.buy_token.assert_not_awaited()
        # Lo que no se compró no puede figurar como invertido: ese número es la
        # base del PnL y del "capital invertido" de /stats.
        assert exec_pos.sol_invested == pytest.approx(0.01)
        assert exec_pos.token_amount_ui == pytest.approx(1000.0)
        assert tracker_pos.amount == pytest.approx(0.01)


class TestTopeDeCapitalNuncaProduceCero:
    """El tope por % de capital no puede degenerar en un monto 0 SOL.

    Cuando el RPC no devolvia el balance (Helius sin cuota -> 429), el tope se
    calculaba sobre 0 y se mandaba `amount: 0.0` a PumpPortal: HTTP 400 y
    "Copy trade fallido" en Telegram, una vez por cada trader que copiaba.
    """

    @staticmethod
    def _buy_signal(amount_sol: float = 0.01) -> CopyTradeSignal:
        return CopyTradeSignal(
            wallet=TRADER,
            action="buy",
            token_mint=MINT,
            token_symbol="TEST",
            amount_sol=amount_sol,
            tx_signature="sig_buy_cap",
            block_time=3000.0,
            trade_token_amount=1000.0,
            buy_sol_raw=amount_sol,
            trader_label="trader",
        )

    @staticmethod
    def _prepare(strategy, executor, monkeypatch, *, balance: float) -> dict:
        """Arma el buy y captura el monto CON EL QUE SE COMPRA.

        `buy_amount_sol` se restaura en un `finally` justo después de
        `buy_token`, así que hay que leerlo dentro del propio mock: si no, la
        aserción vería siempre el valor original y no detectaría un 0.
        """
        strategy.config.copy_trading.COPY_TRADE_CAPITAL_PERCENT = 5.0
        executor.get_sol_balance = AsyncMock(return_value=balance)
        used: dict = {}

        async def _buy(mint: str, **kwargs):
            used["amount"] = executor.buy_amount_sol
            used["mint"] = mint
            used["kwargs"] = kwargs
            return "sig"

        executor.buy_token = AsyncMock(side_effect=_buy)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())
        return used

    async def test_balance_desconocido_no_aplica_el_tope(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, executor, _tracker = _strategy(tmp_path)
        used = self._prepare(strategy, executor, monkeypatch, balance=-1.0)

        await strategy._execute_copy_trade(self._buy_signal())

        executor.buy_token.assert_awaited_once()
        # Sin tope: se compra el monto del trader (ya limitado por
        # MAX_COPY_TRADE_SOL al parsear la señal). Lo importante es que NO sea 0.
        assert used["amount"] == pytest.approx(0.01)

    async def test_wallet_vacia_no_aplica_el_tope(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, executor, _tracker = _strategy(tmp_path)
        used = self._prepare(strategy, executor, monkeypatch, balance=0.0)

        await strategy._execute_copy_trade(self._buy_signal())

        executor.buy_token.assert_awaited_once()
        assert used["amount"] == pytest.approx(0.01)

    async def test_balance_valido_si_limita_el_monto(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, executor, _tracker = _strategy(tmp_path)
        # 1 SOL de capital -> el tope del 5% es 0.05 SOL: el monto del trader
        # (0.01) queda por debajo y NO se toca.
        used = self._prepare(strategy, executor, monkeypatch, balance=1.0)

        await strategy._execute_copy_trade(self._buy_signal())

        executor.buy_token.assert_awaited_once()
        assert used["amount"] == pytest.approx(0.01)

    async def test_balance_valido_aplica_el_tope_porcentual(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, executor, _tracker = _strategy(tmp_path)
        strategy.config.copy_trading.MAX_COPY_TRADE_SOL = 1.0
        strategy.config.copy_trading.MIN_COPY_TRADE_SOL = 0.0001
        # 0.5 SOL de capital -> tope 5% = 0.025 SOL, por debajo de los 0.05
        # que gastó el trader.
        used = self._prepare(strategy, executor, monkeypatch, balance=0.5)

        await strategy._execute_copy_trade(self._buy_signal(0.05))

        executor.buy_token.assert_awaited_once()
        assert used["amount"] == pytest.approx(0.025)

    async def test_la_busqueda_de_liquidez_no_mata_las_compras_de_pumpfun(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Un mint de Pump.fun nuevo no termina en "pump" y Jupiter da 404.

        Antes el executor ruteaba solo por sufijo, asi que la compra se
        descartaba como "sin liquidez" en cada redelivery y el bot nunca
        entraba, aunque el parser hubiera detectado Pump.fun en la tx.
        """
        strategy, executor, _tracker = _strategy(tmp_path)
        self._prepare(strategy, executor, monkeypatch, balance=-1.0)
        MINT_NUEVO = "7MDhfoxFtq5SphF6V8hhxV84mubzQwxb516HXxcddD6p"
        assert not MINT_NUEVO.lower().endswith("pump")

        signal = self._buy_signal()
        signal.token_mint = MINT_NUEVO
        signal.is_pump_fun = True
        await strategy._execute_copy_trade(signal)

        executor.buy_token.assert_awaited_once()
        assert executor.buy_token.await_args.kwargs["via_pumpfun"] is True
        assert executor.buy_token.await_args.args[0] == MINT_NUEVO

    async def test_sin_dato_de_pumpfun_se_deja_la_heuristica_del_executor(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, executor, _tracker = _strategy(tmp_path)
        self._prepare(strategy, executor, monkeypatch, balance=-1.0)

        await strategy._execute_copy_trade(self._buy_signal())

        executor.buy_token.assert_awaited_once()
        assert executor.buy_token.await_args.kwargs["via_pumpfun"] is None


class TestScalperSinPnlMedible:
    """Un trade puede ser scalper Y no tener PnL medible a la vez.

    Antes ese caso dejaba `_stats_pnl` en None y reventaba con
    "float() argument must be a string or a real number, not 'NoneType'"
    justo despues de ejecutar la venta: el trade se perdia entero.
    """

    async def test_scalper_sin_pnl_no_revienta_al_registrar(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        strategy.executor.dry_run = True
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT,
            symbol="TEST",
            buy_price=1e-6,
            amount=0.01,
            source_wallet=TRADER,
            # Scalper: el trader cerro hace nada (el hold sale de aqui).
            created_at=time.time(),
        )
        # Acumulado sin costo valido: el PnL del trader no existe.
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.0
        strategy._wallet_mint_cost_unknown.add((TRADER, MINT))
        strategy.config.copy_trading.MIN_COPY_TRADE_HOLD_SECONDS = 300

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        stats_mock = MagicMock()
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: stats_mock)

        await strategy._execute_copy_trade(_sell_signal())

        # Lo importante: se registro la stats (no hubo excepcion) y el PnL es
        # un numero, no None.
        stats_mock.record_sell.assert_called_once()
        kwargs = stats_mock.record_sell.call_args.kwargs
        assert kwargs["pnl_pct"] is not None
        assert kwargs["pnl_unreliable"] is True


class TestNotificacionHonesta:
    """Lo que sale en Telegram tiene que ser verdad.

    Tres fallos que se leian en los avisos de venta:

    - El ticker de la VENTA era `mint[:6].upper()` (lo que rellena `_parse_trade`,
      porque en la tx no viaja el ticker), asi que una compra de `$SOLLM` se
      ceraba como "Venta de 8NZ5FX".
    - Un PnL por encima de `MAX_TRUSTED_PNL_PCT` se limitaba a 500 y se
      notificaba como "+500.00%" real, siete veces seguidas para el mismo token.
    - "Trader: cup2" cuando la posicion la habia abierto cup3: la etiqueta es la
      de quien VENDIO, y no se decia quien la abrio.
    """

    def _preparar_venta(self, strategy, symbol: str = "SOLLM") -> dict[str, object]:
        """Monta una posicion abierta por `TRADER` con PnL del trader creible."""
        strategy.executor.positions[MINT] = SimpleNamespace(
            mint=MINT, token_amount_ui=1000.0, entry_price=1e-6, sol_invested=0.01
        )
        strategy.tracker.positions[MINT] = SimpleNamespace(
            mint=MINT, symbol=symbol, buy_price=1e-6, amount=0.01,
            source_wallet=TRADER,
        )
        strategy._wallet_mint_tokens[(TRADER, MINT)] = 1000.0
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.01
        return {}

    async def test_la_venta_usa_el_ticker_real_y_no_el_prefijo_del_mint(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, executor, _tracker = _strategy(tmp_path)
        self._preparar_venta(strategy)
        # `_parse_trade` no conoce el ticker: rellena el prefijo del mint.
        signal = _sell_signal()
        signal.token_symbol = MINT[:6].upper()
        executor.get_token_symbol = AsyncMock(return_value="SOLLM")

        seen: dict[str, object] = {}

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            seen.update(kwargs)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        stats_mock = MagicMock()
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: stats_mock)

        await strategy._execute_copy_trade(signal)

        # El ticker resuelto al comprar (guardado en la posicion del tracker) es
        # el que sale, no el prefijo del mint.
        assert seen.get("symbol") == "SOLLM"
        assert stats_mock.record_sell.call_args.kwargs["symbol"] == "SOLLM"

    async def test_pnl_por_encima_del_tope_se_notifica_como_nd(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Un "+500%" era el tope del rango, no una medicion."""
        strategy, _executor, _tracker = _strategy(tmp_path)
        self._preparar_venta(strategy)
        # Sin precio de entrada no hay PnL por precio con el que "corregir" el
        # dato del trader, asi que el valor imposible llega entero hasta el tope.
        strategy.executor.positions[MINT].entry_price = 0.0
        strategy.tracker.positions[MINT].buy_price = 0.0
        # Costo de compras que nunca vimos: con proceeds normal el PnL sale por
        # encima de MAX_TRUSTED_PNL_PCT.
        strategy._wallet_mint_sol[(TRADER, MINT)] = 1e-9
        strategy.executor.get_token_price = AsyncMock(return_value=1e-6)

        seen: dict[str, object] = {}

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            seen.update(kwargs)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        await strategy._execute_copy_trade(_sell_signal())

        assert seen.get("pnl_known") is False, "un PnL >500% no es una medicion"

    async def test_pnl_creible_sigue_siendo_una_medicion(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """El cambio anterior no puede tapar un PnL bueno con un 'n/d'."""
        strategy, _executor, _tracker = _strategy(tmp_path)
        self._preparar_venta(strategy)
        # Costo coherente con la entrada (1000 tk a 1e-6) y salida al doble.
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.001
        strategy.executor.get_token_price = AsyncMock(return_value=2e-6)

        seen: dict[str, object] = {}

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            seen.update(kwargs)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        await strategy._execute_copy_trade(_sell_signal())

        assert seen.get("pnl_known") is True
        assert float(seen.get("pnl", 0.0)) == pytest.approx(100.0)

    async def test_una_estimacion_por_encima_del_techo_no_se_muestra(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """El PnL ESTIMADO tenia suelo (-99%) pero no techo.

        Una entrada de polvo contra una salida de un solo comprador daba
        "+613.19% (estimado)": es el mismo dato sucio que un -100%, y salia
        como si fuera el resultado de la copia.
        """
        strategy, _executor, _tracker = _strategy(tmp_path)
        self._preparar_venta(strategy)
        # Precio de salida 100x la entrada: la estimacion por precio da +9900%.
        strategy.executor.get_token_price = AsyncMock(return_value=1e-4)
        # Se evita la referencia de cierre del trader (que acota el precio) para
        # que la estimacion salga directamente del precio de mercado.
        strategy._wallet_mint_sol[(TRADER, MINT)] = 0.0
        strategy._wallet_mint_cost_unknown.add((TRADER, MINT))

        seen: dict[str, object] = {}

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            seen.update(kwargs)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        await strategy._execute_copy_trade(_sell_signal())

        assert seen.get("pnl_copy") is None, "una estimacion >500% debe salir n/d"

    async def test_la_venta_distingue_quien_vendio_de_quien_abrio_la_posicion(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Compró cup3, vendió cup2: el aviso tiene que decirlo.

        Cuando la tx de venta no identifica el mint, `_pick_wallet_position` la
        adjudica a la wallet que mas tiene del token, que no es necesariamente
        la que lo compro. Antes solo se anotaba en el log, asi que en Telegram
        se leia como un bug.
        """
        strategy, _executor, _tracker = _strategy(tmp_path)
        OTRO = "0Tr0W4ll3t1111111111111111111111111111111111"
        strategy.wallets[OTRO] = TrackedWallet(address=OTRO, label="otro")
        # La posicion la abrio OTRO, pero la venta la disparo TRADER.
        self._preparar_venta(strategy)
        strategy.tracker.positions[MINT].source_wallet = OTRO

        seen: dict[str, object] = {}

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            seen.update(kwargs)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        await strategy._execute_copy_trade(_sell_signal())

        trader_txt = str(seen.get("trader", ""))
        assert "trader" in trader_txt, "sigue nombrando a quien vendio"
        assert "otro" in trader_txt, "y dice quien abrio la posicion"

    async def test_si_la_posicion_la_abrio_el_mismo_no_anade_ruido(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy, _executor, _tracker = _strategy(tmp_path)
        self._preparar_venta(strategy)

        seen: dict[str, object] = {}

        async def _fake_sell(mint: str, *args: object, **kwargs: object) -> bool:
            seen.update(kwargs)
            return True

        monkeypatch.setattr("core.websocket.process_sell_and_notify", _fake_sell)
        monkeypatch.setattr("core.stats.get_trade_stats", lambda: MagicMock())

        await strategy._execute_copy_trade(_sell_signal())

        assert seen.get("trader") == "trader"


class TestDRYRunHonesto:
    """El PnL simulado debe descontar los costes reales del ciclo.

    Antes DRY_run reutilizaba el PnL BRUTO del trader: sin fees, sin rent de
    ATA, sin slippage. Con posiciones de 0.0018 SOL y ~0.002 SOL de coste por
    ciclo eso daba una simulacion verde que en real era pérdida segura.
    """

    def test_el_trader_gana_pero_la_copia_pierde(self, tmp_path: Path) -> None:
        strategy, _ex, _tr = _strategy(tmp_path)
        # Trader +200%. Nuestra posicion es 0.001829 SOL (la mediana real).
        net = strategy._estimate_after_costs(200.0, 0.001829)
        assert net < 0, f"un +200% con 109% de coste debe quedar en perdida, dio {net}"

    def test_cuanto_cuesta_un_ciclo(self, tmp_path: Path) -> None:
        strategy, _ex, _tr = _strategy(tmp_path)
        assert strategy._estimated_costs_sol() == pytest.approx(0.00308928)

    def test_una_posicion_grande_sigue_siendo_viable(self, tmp_path: Path) -> None:
        # Con 1 SOL el mismo +200% sobrevive: el filtro no debe aniquilar
        # las operaciones cuando el coste es pequeno respecto al capital.
        strategy, _ex, _tr = _strategy(tmp_path)
        assert strategy._estimate_after_costs(200.0, 1.0) > 0

    def test_el_slippage_se_cobra_en_ambos_lados(self, tmp_path: Path) -> None:
        strategy, _ex, _tr = _strategy(tmp_path)
        # Con 1 SOL el coste es casi despreciable, asi que lo que queda es el
        # slippage: 5% al entrar y 5% al salir sobre un +100%. El 2x del trader
        # se convierte en 2 x 0.95 x 0.95 = 1.805, o sea +80.5%, no +100%.
        fees = strategy._estimated_costs_sol() / 1.0
        esperado = ((1.0 + 1.0) * (1.0 - fees) * (0.95**2) - 1.0) * 100.0
        assert strategy._estimate_after_costs(100.0, 1.0) == pytest.approx(esperado)
        assert esperado < 100.0, "el slippage por los dos lados siempre resta"

    def test_sin_capital_no_se_inventa_nada(self, tmp_path: Path) -> None:
        strategy, _ex, _tr = _strategy(tmp_path)
        assert strategy._estimate_after_costs(100.0, 0.0) == 100.0

    def test_el_slippage_se_lee_de_la_config_real(self, tmp_path: Path) -> None:
        """El slippage vivia en `self.config.SLIPPAGE_BPS`, que NO existe.

        El valor real esta en `config.trading.SLIPPAGE_BPS`. El `getattr` con
        default 500 hacia que saliera bien POR ACCIDENTE, y como 500 era justo
        el valor de trading, el bug era invisible. Aqui se baja el valor real a
        0 y se comprueba que el resultado cambia: si el codigo se quedase en el
        500 de siempre, este test falla.
        """
        strategy, _ex, _tr = _strategy(tmp_path)
        fees = strategy._estimated_costs_sol() / 1.0

        con_slippage_real = strategy._estimate_after_costs(100.0, 1.0)
        strategy.config.trading.SLIPPAGE_BPS = 0.0
        sin_slippage = strategy._estimate_after_costs(100.0, 1.0)

        esperado = ((1.0 + 1.0) * (1.0 - fees) * 1.0 - 1.0) * 100.0
        assert sin_slippage == pytest.approx(esperado)
        # Si el codigo leyera el default fijo de 500 en vez de la config real,
        # bajar SLIPPAGE_BPS a 0 no cambiaria nada y estos dos valores serian
        # iguales. La diferencia es la prueba de que ahora SI lee la config.
        assert con_slippage_real < sin_slippage
        assert con_slippage_real == pytest.approx(
            ((1.0 + 1.0) * (1.0 - fees) * 0.95**2 - 1.0) * 100.0
        )

    def test_caida_de_dust_arrastra_las_ganancias_normales(self, tmp_path: Path) -> None:
        """Documenta el -98% real que aparece en los logs.

        Con 0.0001 SOL de posicion, el ciclo cuesta 0.00308928 SOL: son 3089%
        de coste, asi que el `min(..., 0.99)` se come el 99% del capital y
        solo queda un 1% del multiplicador. Una ganancia NORMAL (la del trader,
        entre +15% y +250%) acaba alrededor de -98% por muy grande que sea.
        """
        strategy, _ex, _tr = _strategy(tmp_path)
        for pnl_trader in (15.28, 56.80, 74.43, 191.81, 246.61):
            dust = strategy._estimate_after_costs(pnl_trader, 0.0001)
            assert dust < -95.0, f"trader +{pnl_trader}% dio {dust:.2f}% con dust"

        # La excepcion que demuestra que la regla es "ganancia vs coste", no
        # "dust = -98% garantizado": un +8232% (83x) deja un 1% vivo, y aunque
        # el 5% de slippage por lado lo deja igualmente en -24.8%, esta a anos
        # luz del -98% de las ganancias normales. El tamano importa mas que el
        # porcentaje del trader, pero un multiplicador enorme compra margen.
        extremo = strategy._estimate_after_costs(8232.0, 0.0001)
        assert -50.0 < extremo < 0.0, f"un 83x deberia recuperar bastante, dio {extremo}"

        # Y el tamano manda: a 0.05 SOL las mismas ganancias son rentables.
        assert strategy._estimate_after_costs(246.61, 0.05) > 0

    def test_ganar_todo_no_basta_si_el_capital_es_dust(self, tmp_path: Path) -> None:
        # +8232% fue el "mejor trade" historico. Aun asi, con una posicion de
        # polvo, el coste se lo come. Por eso el filtro de scalpes importa mas
        # que el porcentaje bruto.
        strategy, _ex, _tr = _strategy(tmp_path)
        assert strategy._estimate_after_costs(8232.0, 0.001829) < 0
