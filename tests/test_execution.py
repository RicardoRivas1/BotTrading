"""Pruebas unitarias del fallback de lectura de precios (core/execution.py).

Valida la cadena de precios de `JupiterExecutor.get_token_price`:
Jupiter -> DexScreener -> Pump.fun, incluyendo el orden especial de los
mints de Pump.fun, y el parseo de las respuestas de DexScreener / Pump.fun.
Toda llamada HTTP se simula; nunca se consulta la red.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import base58
import pytest
from solders.keypair import Keypair
from solders.signature import Signature

import core.execution as execution_mod
from core.execution import JupiterExecutor, Position, SwapExecutionError

# Mint que NO termina en "pump" (usa orden jupiter -> dexscreener -> pumpfun).
MINT_RAYDIUM = "1thX6LZfHDZZKUs92febYZhYRcXddmzfzF2NvTkPNE"
# Mint simulado estilo Pump.fun (termina en "pump", usa dexscreener primero).
MINT_PUMP = "FUELoUmnYbR5Pm1VFD8hzpRZ7NmnJQVifKdC9pump"


@pytest.fixture
def executor(monkeypatch) -> JupiterExecutor:
    """Executor con clave simulada (nunca valida contra Solana)."""
    monkeypatch.setattr(execution_mod, "cargar_keypair", lambda key: Keypair())
    return JupiterExecutor(
        private_key="clave-de-prueba",
        rpc_url="https://rpc.example.invalid",
        slippage_bps=500,
        buy_amount_sol=0.05,
        dry_run=True,
    )


class _FakeResponse:
    """Respuesta HTTP simulada para aiohttp (contexto async)."""

    def __init__(self, status: int, payload: dict) -> None:
        self.status = status
        self._payload = payload

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False

    async def json(self) -> dict:
        return self._payload

    async def text(self) -> str:
        return str(self._payload)


def _patch_session(monkeypatch, fake_resp: _FakeResponse, session_get_ret) -> None:
    """Sustituye aiohttp.ClientSession por una sesión mock con la respuesta dada."""
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    session.get.return_value = session_get_ret(fake_resp)
    monkeypatch.setattr(aiohttp, "ClientSession", lambda: session)


class TestFallbackPrecio:
    """Cadena de fuentes de precio: Jupiter -> DexScreener -> Pump.fun."""

    async def test_jupiter_primero_y_dexscreener_de_fallback(
        self, executor: JupiterExecutor
    ) -> None:
        executor._get_token_decimals = AsyncMock(return_value=6)
        executor._get_quote = AsyncMock(side_effect=SwapExecutionError("sin ruta"))
        executor._get_price_from_dexscreener = AsyncMock(return_value=0.000123)
        executor._get_price_from_pumpfun = AsyncMock(return_value=0.0)

        price = await executor.get_token_price(MINT_RAYDIUM)

        assert price == pytest.approx(0.000123)
        executor._get_quote.assert_awaited()
        executor._get_price_from_dexscreener.assert_awaited_once()
        executor._get_price_from_pumpfun.assert_not_awaited()

    async def test_dexscreener_y_pumpfun_como_cadena_completa(
        self, executor: JupiterExecutor
    ) -> None:
        executor._get_token_decimals = AsyncMock(return_value=6)
        executor._get_quote = AsyncMock(side_effect=SwapExecutionError("no route"))
        executor._get_price_from_dexscreener = AsyncMock(return_value=0.0)
        executor._get_price_from_pumpfun = AsyncMock(return_value=0.0000009)

        price = await executor.get_token_price(MINT_RAYDIUM)

        assert price == pytest.approx(0.0000009)
        executor._get_price_from_dexscreener.assert_awaited_once()
        executor._get_price_from_pumpfun.assert_awaited_once()

    async def test_pump_mint_consulta_dexscreener_primero_sin_jupiter(
        self, executor: JupiterExecutor
    ) -> None:
        executor._get_quote = AsyncMock(return_value={"outAmount": 1_000_000})
        executor._get_price_from_dexscreener = AsyncMock(return_value=0.0001)
        executor._get_price_from_pumpfun = AsyncMock(return_value=0.0002)

        price = await executor.get_token_price(MINT_PUMP)

        assert price == pytest.approx(0.0001)
        executor._get_quote.assert_not_awaited()

    async def test_pump_mint_usa_jupiter_como_ultimo_recurso(
        self, executor: JupiterExecutor
    ) -> None:
        executor._get_token_decimals = AsyncMock(return_value=6)
        executor._get_quote = AsyncMock(return_value={"outAmount": 1_000_000})  # 0.001 SOL
        executor._get_price_from_dexscreener = AsyncMock(return_value=0.0)
        executor._get_price_from_pumpfun = AsyncMock(return_value=0.0)

        price = await executor.get_token_price(MINT_PUMP)

        assert price == pytest.approx(0.001)

    async def test_todas_las_fuentes_fallan_lanza_error(self, executor: JupiterExecutor) -> None:
        executor._get_token_decimals = AsyncMock(return_value=6)
        executor._get_quote = AsyncMock(side_effect=SwapExecutionError("no route"))
        executor._get_price_from_dexscreener = AsyncMock(return_value=0.0)
        executor._get_price_from_pumpfun = AsyncMock(return_value=0.0)

        with pytest.raises(SwapExecutionError, match="No se pudo obtener precio"):
            await executor.get_token_price(MINT_RAYDIUM)

    async def test_precio_adopta_entry_base_de_la_posicion(self, executor: JupiterExecutor) -> None:
        executor.positions[MINT_RAYDIUM] = Position(mint=MINT_RAYDIUM, entry_price=0.0)
        executor._get_token_decimals = AsyncMock(return_value=6)
        executor._get_quote = AsyncMock(side_effect=SwapExecutionError("no route"))
        executor._get_price_from_dexscreener = AsyncMock(return_value=0.0003)

        price = await executor.get_token_price(MINT_RAYDIUM)

        assert executor.positions[MINT_RAYDIUM].entry_price == pytest.approx(0.0003)
        assert price == pytest.approx(0.0003)


class TestDexScreener:
    """Parseo de la respuesta de DexScreener (priceNative / priceUsd)."""

    async def test_extrae_price_native_sol_de_la_pair_con_mas_liquidez(
        self, executor: JupiterExecutor, monkeypatch
    ) -> None:
        payload = {
            "pairs": [
                {"liquidity": {"usd": 5000}, "priceNative": "0.000123", "baseToken": {"symbol": "MET"}},
                {"liquidity": {"usd": 90000}, "priceNative": "0.000999", "baseToken": {"symbol": "MET"}},
            ]
        }
        _patch_session(monkeypatch, _FakeResponse(200, payload), lambda resp: resp)

        price = await executor._get_price_from_dexscreener(MINT_RAYDIUM)

        assert price == pytest.approx(0.000999)  # prioriza la pair con más liquidez

    async def test_sin_pairs_devuelve_cero(self, executor: JupiterExecutor, monkeypatch) -> None:
        _patch_session(monkeypatch, _FakeResponse(200, {"pairs": []}), lambda resp: resp)
        assert await executor._get_price_from_dexscreener(MINT_RAYDIUM) == 0.0

    async def test_status_no_200_devuelve_cero(self, executor: JupiterExecutor, monkeypatch) -> None:
        _patch_session(monkeypatch, _FakeResponse(404, {}), lambda resp: resp)
        assert await executor._get_price_from_dexscreener(MINT_RAYDIUM) == 0.0

    async def test_fallback_a_price_usd_convertido_a_sol(
        self, executor: JupiterExecutor, monkeypatch
    ) -> None:
        payload = {
            "pairs": [
                {
                    "liquidity": {"usd": 1000},
                    "priceNative": None,
                    "priceUsd": "0.012",  # 0.012 USD
                }
            ]
        }
        _patch_session(monkeypatch, _FakeResponse(200, payload), lambda resp: resp)
        executor._get_sol_usd_price = AsyncMock(return_value=120.0)  # SOL = 120 USD

        price = await executor._get_price_from_dexscreener(MINT_RAYDIUM)

        # 0.012 USD / 120 USD por SOL = 0.0001 SOL
        assert price == pytest.approx(0.0001)


class TestPumpFun:
    """Parseo de la respuesta de Pump.fun (reservas virtuales de la curva)."""

    async def test_precio_desde_reservas_virtuales(self, executor: JupiterExecutor, monkeypatch) -> None:
        payload = {"virtual_sol_reserves": 100_000, "virtual_token_reserves": 1_000_000_000}
        _patch_session(monkeypatch, _FakeResponse(200, payload), lambda resp: resp)

        price = await executor._get_price_from_pumpfun(MINT_PUMP)

        # 100000 / 1000000000 = 0.0001 SOL
        assert price == pytest.approx(0.0001)

    async def test_fallback_a_reservas_reales(self, executor: JupiterExecutor, monkeypatch) -> None:
        payload = {"sol_reserves": "50000", "token_reserves": "1000000000"}
        _patch_session(monkeypatch, _FakeResponse(200, payload), lambda resp: resp)

        price = await executor._get_price_from_pumpfun(MINT_PUMP)

        assert price == pytest.approx(0.00005)

    async def test_sin_precio_devuelve_cero(self, executor: JupiterExecutor, monkeypatch) -> None:
        _patch_session(monkeypatch, _FakeResponse(200, {}), lambda resp: resp)
        assert await executor._get_price_from_pumpfun(MINT_PUMP) == 0.0

    async def test_status_no_200_devuelve_cero(self, executor: JupiterExecutor, monkeypatch) -> None:
        _patch_session(monkeypatch, _FakeResponse(500, {}), lambda resp: resp)
        assert await executor._get_price_from_pumpfun(MINT_PUMP) == 0.0


class TestCargarKeypair:
    """Carga de la wallet desde clave Base58 o frase mnemonic."""

    def test_devuelve_keypair_valido_desde_base58(self) -> None:
        kp = Keypair()
        encoded = base58.b58encode(kp.to_bytes()).decode()
        loaded = execution_mod.cargar_keypair(encoded)
        assert str(loaded.pubkey()) == str(kp.pubkey())

    def test_base58_invalida_lanza_error(self) -> None:
        with pytest.raises(SwapExecutionError, match="Base58"):
            execution_mod.cargar_keypair("clave-corta")

    def test_vacia_lanza_error(self) -> None:
        with pytest.raises(SwapExecutionError, match="PRIVATE_KEY vacío"):
            execution_mod.cargar_keypair("   ")

    def test_numero_de_palabras_no_valido_lanza_error(self) -> None:
        with pytest.raises(SwapExecutionError, match="FORMATO DE CLAVE INVALIDO"):
            execution_mod.cargar_keypair("dos tres palabras clave corta")

    def test_mnemonic_invalida_lanza_error(self, monkeypatch) -> None:
        def _blow(_phrase: str) -> MagicMock:
            raise ValueError("frase no válida")

        monkeypatch.setattr(execution_mod, "Bip39SeedGenerator", _blow)
        with pytest.raises(SwapExecutionError, match="Mnemonic inválido"):
            execution_mod.cargar_keypair("una dos tres cuatro cinco seis siete ocho nueve diez once doce")


class TestUtilities:
    """Utilidades internas del executor."""

    def test_simulated_quote_mantiene_1_a_1(self, executor: JupiterExecutor) -> None:
        quote = executor._simulated_quote("INPUT", "OUTPUT", 1_000)
        assert quote["outAmount"] == "1000"
        assert quote["simulated"] is True
        assert quote["amount"] == "1000"

    def test_decode_transaction_hex(self) -> None:
        b = b"\x01\x02\x03"
        assert execution_mod.JupiterExecutor._decode_transaction("0x010203") == b

    def test_decode_transaction_base58(self) -> None:
        b = b"\xde\xad\xbe\xef"
        assert execution_mod.JupiterExecutor._decode_transaction(base58.b58encode(b).decode()) == b

    def test_decode_transaction_lista(self) -> None:
        assert execution_mod.JupiterExecutor._decode_transaction([1, 2, 3]) == bytes([1, 2, 3])

    def test_decode_transaction_invalida(self) -> None:
        with pytest.raises(SwapExecutionError, match="Formato"):
            execution_mod.JupiterExecutor._decode_transaction({"nope": 1})

    async def test_get_quote_fallback_por_fallo_de_red(
        self, executor: JupiterExecutor
    ) -> None:
        fake_quote = {"outAmount": "42"}
        executor._request_quote = AsyncMock(side_effect=[OSError("dns"), fake_quote])
        quote = await executor._get_quote(
            session=MagicMock(), input_mint="A", output_mint="B", amount_lamports=1_000
        )
        assert quote == fake_quote
        assert executor._request_quote.await_count == 2

    async def test_get_quote_todos_fallan_simula_en_test(
        self, executor: JupiterExecutor
    ) -> None:
        executor._request_quote = AsyncMock(side_effect=[OSError("a"), OSError("b")])
        quote = await executor._get_quote(
            session=MagicMock(), input_mint="A", output_mint="B",
            amount_lamports=1_000, simulate=True,
        )
        assert quote["simulated"] is True

    async def test_get_quote_sin_simulacion_todos_fallan_lanza(
        self, executor: JupiterExecutor
    ) -> None:
        executor._request_quote = AsyncMock(side_effect=[OSError("a"), OSError("b")])
        with pytest.raises(OSError):
            await executor._get_quote(
                session=MagicMock(), input_mint="A", output_mint="B", amount_lamports=1_000
            )

    async def test_sol_usd_price_exitoso(self, executor: JupiterExecutor, monkeypatch) -> None:
        payload = {"pairs": [{"priceUsd": "143.50"}]}
        _patch_session(monkeypatch, _FakeResponse(200, payload), lambda resp: resp)
        assert await executor._get_sol_usd_price() == pytest.approx(143.50)

    async def test_sol_usd_price_fallback(self, executor: JupiterExecutor, monkeypatch) -> None:
        _patch_session(monkeypatch, _FakeResponse(500, {}), lambda resp: resp)
        assert await executor._get_sol_usd_price() == 180.0

    async def test_get_token_decimals_parsed(self, executor: JupiterExecutor, monkeypatch) -> None:
        class _FakeResp:
            class _Inner:
                decimals = 6

            value = _Inner()

        client = AsyncMock()
        client.get_token_supply.return_value = _FakeResp()
        monkeypatch.setattr("core.execution.AsyncClient", lambda *a, **k: client)
        assert await executor._get_token_decimals(MINT_RAYDIUM) == 6

    async def test_get_token_decimals_fallback(self, executor: JupiterExecutor, monkeypatch) -> None:
        class _FakeResp:
            value = None

        client = AsyncMock()
        client.get_token_supply.return_value = _FakeResp()
        monkeypatch.setattr("core.execution.AsyncClient", lambda *a, **k: client)
        assert await executor._get_token_decimals(MINT_RAYDIUM) == 9


class TestCompraVenta:
    """Flujo de compra/venta (según DRY_RUN)."""

    def _quote(self, out_amount: str) -> dict:
        return {"outAmount": out_amount, "routePlan": [{"outAmount": out_amount}]}

    async def test_buy_token_dry_run_registra_posicion_con_precio_real(
        self, executor: JupiterExecutor
    ) -> None:
        executor._get_quote = AsyncMock(return_value=self._quote("5000000000"))
        executor._get_token_decimals = AsyncMock(return_value=6)
        executor.get_token_price = AsyncMock(return_value=0.001)

        sig = await executor.buy_token(MINT_RAYDIUM)

        assert sig == "DRY_RUN"
        pos = executor.positions[MINT_RAYDIUM]
        assert pos.token_amount_ui == pytest.approx(5000.0)
        assert pos.entry_price == pytest.approx(0.001)
        assert pos.sol_invested == pytest.approx(0.05)

    async def test_buy_token_real_calcula_entry_desde_curva(
        self, executor: JupiterExecutor
    ) -> None:
        executor._get_quote = AsyncMock(return_value=self._quote("5000000000"))
        executor._build_and_send_swap = AsyncMock(return_value=Signature.default())
        executor._get_token_decimals = AsyncMock(return_value=6)
        executor.get_token_price = AsyncMock(return_value=0.0)

        sig = await executor.buy_token(MINT_RAYDIUM, dry_run=False)

        assert sig == Signature.default()
        pos = executor.positions[MINT_RAYDIUM]
        # entry = 0.05 SOL / 5000 tokens = 0.00001
        assert pos.entry_price == pytest.approx(0.00001)

    async def test_sell_token_usa_quote_y_swap(self, executor: JupiterExecutor) -> None:
        executor._get_quote = AsyncMock(return_value=self._quote("999"))
        executor._build_and_send_swap = AsyncMock(return_value=Signature.default())
        executor._get_token_decimals = AsyncMock(return_value=6)

        sig = await executor.sell_token(MINT_RAYDIUM, 100.0)

        assert sig == Signature.default()
        executor._build_and_send_swap.assert_awaited_once()


class TestClosePosition:
    """Cierre de posiciones (simulado vs real)."""

    def _attach(self, executor: JupiterExecutor, mint: str = MINT_RAYDIUM) -> None:
        executor.positions[mint] = Position(
            mint=mint, token_amount_ui=1.0, entry_price=0.001, sol_invested=0.05
        )

    async def test_dry_run_solo_registra_y_elimina(self, executor: JupiterExecutor) -> None:
        self._attach(executor)
        await executor.close_position(MINT_RAYDIUM, "TAKE_PROFIT", 100.0)
        assert MINT_RAYDIUM not in executor.positions

    async def test_real_vende_y_elimina(self, executor: JupiterExecutor) -> None:
        self._attach(executor)
        executor.dry_run = False
        executor.sell_token = AsyncMock(return_value=Signature.default())
        await executor.close_position(MINT_RAYDIUM, "STOP_LOSS", -50.0)
        executor.sell_token.assert_awaited_once()
        assert MINT_RAYDIUM not in executor.positions

    async def test_real_error_de_venta_persistira_la_posicion(
        self, executor: JupiterExecutor
    ) -> None:
        self._attach(executor)
        executor.dry_run = False
        executor.sell_token = AsyncMock(side_effect=SwapExecutionError("boom"))
        await executor.close_position(MINT_RAYDIUM, "STOP_LOSS", -50.0)
        assert MINT_RAYDIUM in executor.positions

    async def test_close_sin_posicion_no_hace_nada(self, executor: JupiterExecutor) -> None:
        await executor.close_position(MINT_RAYDIUM, "TAKE_PROFIT", 10.0)
        assert MINT_RAYDIUM not in executor.positions


class TestMonitorPosition:
    """Monitor del executor: TP, SL y trailing stop."""

    def _attach(self, executor: JupiterExecutor) -> None:
        executor.positions[MINT_RAYDIUM] = Position(
            mint=MINT_RAYDIUM, token_amount_ui=1.0, entry_price=0.001, peak_price=0.0
        )

    def _price(self, executor: JupiterExecutor, value: float) -> None:
        executor.get_token_price = AsyncMock(return_value=value)

    async def test_take_profit(self, executor: JupiterExecutor) -> None:
        self._attach(executor)
        self._price(executor, 0.002)  # +100%
        reason, pnl = await executor.monitor_position(MINT_RAYDIUM)
        assert reason == "TAKE_PROFIT"
        assert pnl == pytest.approx(100.0)

    async def test_stop_loss(self, executor: JupiterExecutor) -> None:
        self._attach(executor)
        self._price(executor, 0.0005)  # -50%
        reason, pnl = await executor.monitor_position(MINT_RAYDIUM)
        assert reason == "STOP_LOSS"
        assert pnl == pytest.approx(-50.0)

    async def test_trailing_stop(self, executor: JupiterExecutor) -> None:
        self._attach(executor)
        self._price(executor, 0.0016)  # +60% (muy por encima del +20% de activación)
        reason, _ = await executor.monitor_position(MINT_RAYDIUM)
        assert reason == ""
        assert executor.positions[MINT_RAYDIUM].trailing_active is True

        self._price(executor, 0.0013)  # -18.75% desde el peak (distancia > 15%)
        reason, pnl = await executor.monitor_position(MINT_RAYDIUM)
        assert reason == "TRAILING_STOP"
        assert pnl == pytest.approx(30.0)

    async def test_sin_posicion(self, executor: JupiterExecutor) -> None:
        reason, pnl = await executor.monitor_position(MINT_RAYDIUM)
        assert reason == ""
        assert pnl == 0.0

    async def test_error_de_precio_no_bloquea(self, executor: JupiterExecutor) -> None:
        self._attach(executor)
        executor.get_token_price = AsyncMock(side_effect=SwapExecutionError("sin precio"))
        reason, pnl = await executor.monitor_position(MINT_RAYDIUM)
        assert reason == ""
        assert pnl == 0.0


class TestTokenSymbol:
    """Resolución del ticker/símbolo del token."""

    async def test_dexscreener(self, executor: JupiterExecutor) -> None:
        executor._get_symbol_from_dexscreener = AsyncMock(return_value="MET")
        assert await executor.get_token_symbol(MINT_RAYDIUM) == "MET"

    async def test_pumpfun_fallback(self, executor: JupiterExecutor) -> None:
        executor._get_symbol_from_dexscreener = AsyncMock(return_value="")
        executor._get_symbol_from_pumpfun = AsyncMock(return_value="PMP")
        assert await executor.get_token_symbol(MINT_RAYDIUM) == "PMP"

    async def test_fallback_del_mint(self, executor: JupiterExecutor) -> None:
        executor._get_symbol_from_dexscreener = AsyncMock(return_value="")
        executor._get_symbol_from_pumpfun = AsyncMock(return_value="")
        assert await executor.get_token_symbol(MINT_RAYDIUM) == MINT_RAYDIUM[:6].upper()
