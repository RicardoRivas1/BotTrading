"""Configuración común para pruebas pytest."""

import os
import sys
from unittest.mock import MagicMock

import pandas as pd
import pytest

# Añadir directorio padre al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def sample_ohlcv_data() -> pd.DataFrame:
    """Proporciona datos OHLCV de ejemplo para pruebas."""
    return pd.DataFrame({
        "timestamp": list(range(1, 51)),
        "open": [100 + i * 0.5 for i in range(50)],
        "high": [102 + i * 0.5 for i in range(50)],
        "low": [98 + i * 0.5 for i in range(50)],
        "close": [101 + i * 0.5 for i in range(50)],
        "volume": [1000 + i * 10 for i in range(50)],
    })


@pytest.fixture
def sample_ohlcv_long() -> pd.DataFrame:
    """Proporciona datos OHLCV largos para indicadores que requieren más datos."""
    n = 100
    return pd.DataFrame({
        "timestamp": list(range(1, n + 1)),
        "open": [100 + i * 0.3 for i in range(n)],
        "high": [102 + i * 0.3 for i in range(n)],
        "low": [98 + i * 0.3 for i in range(n)],
        "close": [101 + i * 0.3 for i in range(n)],
        "volume": [1000 + i * 5 for i in range(n)],
    })


@pytest.fixture
def mock_exchange() -> MagicMock:
    """Proporciona un mock del exchange para pruebas."""
    exchange = MagicMock()
    exchange.fetch_ohlcv.return_value = [
        [i, 100 + i, 102 + i, 98 + i, 101 + i, 1000 + i * 10]
        for i in range(1, 51)
    ]
    exchange.fetch_ticker.return_value = {"last": 150.0}
    exchange.fetch_balance.return_value = {
        "free": {"USDT": 100.0, "BTC": 0.5},
        "total": {"USDT": 100.0, "BTC": 0.5},
    }
    exchange.market.return_value = {
        "limits": {"cost": {"min": 10.0}},
        "symbol": "BTC/USDT",
    }
    return exchange


@pytest.fixture
def sample_mtf_data() -> pd.DataFrame:
    """Proporciona datos de timeframe superior para pruebas MTF."""
    return pd.DataFrame({
        "timestamp": list(range(1, 201)),
        "open": [100 + i * 0.2 for i in range(200)],
        "high": [102 + i * 0.2 for i in range(200)],
        "low": [98 + i * 0.2 for i in range(200)],
        "close": [101 + i * 0.2 for i in range(200)],
        "volume": [5000 + i * 20 for i in range(200)],
    })


@pytest.fixture
def sample_atr_series() -> pd.Series:
    """Proporciona una serie de ATR de ejemplo."""
    return pd.Series([5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0, 8.5, 9.0, 9.5])
