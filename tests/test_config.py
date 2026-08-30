"""Pruebas unitarias para el módulo de configuración."""

import os
import pytest
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    AppConfig,
    ExchangeConfig,
    StrategyConfig,
    TelegramConfig,
    TradingConfig,
    _get_env,
    _get_env_bool,
    _get_env_float,
    _get_env_int,
    load_config,
)


class TestHelperFunctions:

    def test_get_env_default(self) -> None:
        result = _get_env("NONEXISTENT_VAR", "default_value")
        assert result == "default_value"

    def test_get_env_required_missing(self) -> None:
        with pytest.raises(ValueError):
            _get_env("NONEXISTENT_REQUIRED_VAR", required=True)

    def test_get_env_bool_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_BOOL_VAR", "true")
        assert _get_env_bool("TEST_BOOL_VAR") is True

    def test_get_env_bool_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_BOOL_VAR", "false")
        assert _get_env_bool("TEST_BOOL_VAR") is False

    def test_get_env_int_valid(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_INT_VAR", "42")
        assert _get_env_int("TEST_INT_VAR") == 42

    def test_get_env_int_invalid(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_INT_VAR", "not_a_number")
        assert _get_env_int("TEST_INT_VAR", default=10) == 10

    def test_get_env_float_valid(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_FLOAT_VAR", "3.14")
        assert _get_env_float("TEST_FLOAT_VAR") == 3.14

    def test_get_env_float_invalid(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_FLOAT_VAR", "not_a_float")
        assert _get_env_float("TEST_FLOAT_VAR", default=1.5) == 1.5


class TestExchangeConfig:

    def test_default_values(self) -> None:
        config = ExchangeConfig()
        assert config.exchange_id == "binance" or config.exchange_id == _get_env("EXCHANGE_ID", "binance")
        assert config.enable_rate_limit is True
        assert config.default_type == "spot"


class TestTradingConfig:

    def test_default_values(self) -> None:
        config = TradingConfig()
        assert config.symbol == "BTC/USDT"
        assert config.timeframe == "1m"
        assert config.timeframe_mtf == "1h"
        assert config.limit_ohlcv == 100
        assert config.limit_mtf == 200
        assert config.min_order_usdt == 10.0
        assert config.initial_balance_usdt == 10.00


class TestStrategyConfig:

    def test_default_values(self) -> None:
        config = StrategyConfig()
        assert config.ema_fast == 9
        assert config.ema_slow == 21
        assert config.ema_mtf_period == 200
        assert config.rsi_period == 14
        assert config.rsi_buy_min == 30.0
        assert config.rsi_buy_max == 70.0
        assert config.atr_period == 14
        assert config.atr_sl_multiplier == 1.2
        assert config.atr_tp_multiplier == 1.5
        assert config.adx_period == 14
        assert config.adx_threshold == 25.0
        assert config.volume_avg_window == 20
        assert config.trailing_be_threshold_atr == 0.5


class TestTelegramConfig:

    def test_configuracion(self) -> None:
        config = TelegramConfig()
        assert isinstance(config.token, str)
        assert isinstance(config.chat_id, str)
        assert isinstance(config.enabled, bool)


class TestAppConfig:

    def test_configuracion_completa(self) -> None:
        config = AppConfig()
        assert isinstance(config.exchange, ExchangeConfig)
        assert isinstance(config.trading, TradingConfig)
        assert isinstance(config.strategy, StrategyConfig)
        assert isinstance(config.telegram, TelegramConfig)
        assert config.health_check_port > 0
        assert config.csv_file.endswith(".csv")
        assert config.loop_interval_seconds > 0
        assert config.error_interval_seconds > 0

    def test_load_config(self) -> None:
        config = load_config()
        assert isinstance(config, AppConfig)
