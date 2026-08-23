"""Configuración común para pruebas pytest."""

import pytest
import pandas as pd
import ccxt
import sys
import os

# Añadir directorio padre al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def sample_ohlcv_data():
    """Proporciona datos OHLCV de ejemplo para pruebas."""
    return pd.DataFrame({
        'timestamp': [1, 2, 3, 4, 5],
        'open': [100, 102, 105, 103, 108],
        'high': [102, 105, 107, 106, 110],
        'low': [98, 100, 103, 101, 106],
        'close': [101, 103, 104, 107, 107],
        'volume': [1000, 1200, 1500, 1300, 1400]
    })


@pytest.fixture
def mock_exchange():
    """Proporciona un mock del exchange para pruebas."""
    exchange = ccxt.binance({'enableRateLimit': True})
    return exchange