"""Pruebas unitarias para el módulo de acceso a datos del exchange."""

import pytest
import sys
from unittest.mock import MagicMock, patch, PropertyMock

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.dirname(__import__("os").path.abspath(__file__))))

import ccxt
import pandas as pd

from config import AppConfig, ExchangeConfig, TradingConfig
from data import ExchangeClient, retry_on_error


class TestRetryOnError:

    def test_retry_exito_primer_intento(self) -> None:
        @retry_on_error(max_retries=3, base_delay=0.01)
        def funcion_exitosa() -> str:
            return "éxito"

        result = funcion_exitosa()
        assert result == "éxito"

    def test_retry_exito_tras_fallos(self) -> None:
        call_count = 0

        @retry_on_error(max_retries=3, base_delay=0.01)
        def funcion_con_fallos() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ccxt.NetworkError("Error de red")
            return "éxito"

        result = funcion_con_fallos()
        assert result == "éxito"
        assert call_count == 3

    def test_retry_falla_permanente(self) -> None:
        @retry_on_error(max_retries=2, base_delay=0.01)
        def funcion_fallida() -> str:
            raise ccxt.NetworkError("Error persistente")

        with pytest.raises(ccxt.NetworkError):
            funcion_fallida()

    def test_retry_no_reintenta_errores_no_red(self) -> None:
        @retry_on_error(max_retries=3, base_delay=0.01)
        def funcion_error_otro() -> str:
            raise ValueError("Error diferente")

        with pytest.raises(ValueError):
            funcion_error_otro()

    def test_retry_rate_limit_exceeded(self) -> None:
        call_count = 0

        @retry_on_error(max_retries=2, base_delay=0.01)
        def funcion_rate_limit() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ccxt.RateLimitExceeded("Rate limit")
            return "éxito"

        result = funcion_rate_limit()
        assert result == "éxito"


class TestExchangeClient:

    def test_inicializacion_demo(self) -> None:
        config = AppConfig()
        config = AppConfig(
            exchange=ExchangeConfig(use_demo=True, api_key="", api_secret=""),
        )
        client = ExchangeClient(config)
        assert client._exchange is not None

    def test_exchange_property(self) -> None:
        config = AppConfig(
            exchange=ExchangeConfig(use_demo=True, api_key="", api_secret=""),
        )
        client = ExchangeClient(config)
        assert client.exchange is not None

    def test_fetch_ohlcv(self) -> None:
        config = AppConfig(
            exchange=ExchangeConfig(use_demo=True, api_key="", api_secret=""),
        )
        client = ExchangeClient(config)
        client._exchange.fetch_ohlcv = MagicMock(
            return_value=[
                [i, 100 + i, 102 + i, 98 + i, 101 + i, 1000 + i * 10]
                for i in range(1, 51)
            ]
        )
        df = client.fetch_ohlcv("BTC/USDT", "1m", limit=50)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 50
        assert list(df.columns) == ["timestamp", "open", "high", "low", "close", "volume"]

    def test_fetch_ticker(self) -> None:
        config = AppConfig(
            exchange=ExchangeConfig(use_demo=True, api_key="", api_secret=""),
        )
        client = ExchangeClient(config)
        client._exchange.fetch_ticker = MagicMock(return_value={"last": 150.0})
        ticker = client.fetch_ticker("BTC/USDT")
        assert ticker["last"] == 150.0

    def test_fetch_balance(self) -> None:
        config = AppConfig(
            exchange=ExchangeConfig(use_demo=True, api_key="", api_secret=""),
        )
        client = ExchangeClient(config)
        client._exchange.fetch_balance = MagicMock(
            return_value={"free": {"USDT": 100.0, "BTC": 0.5}}
        )
        balance = client.fetch_balance()
        assert balance["free"]["USDT"] == 100.0

    def test_get_market_info(self) -> None:
        config = AppConfig(
            exchange=ExchangeConfig(use_demo=True, api_key="", api_secret=""),
        )
        client = ExchangeClient(config)
        client._exchange.market = MagicMock(
            return_value={"limits": {"cost": {"min": 10.0}}}
        )
        info = client.get_market_info("BTC/USDT")
        assert info is not None
        assert info["min_order_usdt"] == 10.0

    def test_get_market_info_none(self) -> None:
        config = AppConfig(
            exchange=ExchangeConfig(use_demo=True, api_key="", api_secret=""),
        )
        client = ExchangeClient(config)
        client._exchange.market = MagicMock(return_value=None)
        info = client.get_market_info("BTC/USDT")
        assert info is None

    def test_create_market_buy_order(self) -> None:
        config = AppConfig(
            exchange=ExchangeConfig(use_demo=True, api_key="", api_secret=""),
        )
        client = ExchangeClient(config)
        client._exchange.create_market_buy_order = MagicMock(
            return_value={"id": "order_123", "status": "closed"}
        )
        result = client.create_market_buy_order("BTC/USDT", 0.5)
        assert result["id"] == "order_123"

    def test_create_market_sell_order(self) -> None:
        config = AppConfig(
            exchange=ExchangeConfig(use_demo=True, api_key="", api_secret=""),
        )
        client = ExchangeClient(config)
        client._exchange.create_market_sell_order = MagicMock(
            return_value={"id": "order_456", "status": "closed"}
        )
        result = client.create_market_sell_order("BTC/USDT", 0.5)
        assert result["id"] == "order_456"

    def test_error_autenticacion_no_reintenta(self) -> None:
        config = AppConfig(
            exchange=ExchangeConfig(use_demo=True, api_key="", api_secret=""),
        )
        client = ExchangeClient(config)
        client._exchange.fetch_ohlcv = MagicMock(
            side_effect=ccxt.AuthenticationError("Auth error")
        )
        with pytest.raises(ccxt.AuthenticationError):
            client.fetch_ohlcv("BTC/USDT", "1m")
