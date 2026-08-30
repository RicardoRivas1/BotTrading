"""Configuración centralizada del bot de trading con validación de tipos."""

from dataclasses import dataclass, field
import os
from dotenv import load_dotenv

load_dotenv()


def _get_env(key: str, default: str = "", required: bool = False) -> str:
    """Obtiene una variable de entorno con validación opcional."""
    value = os.getenv(key, default)
    if required and not value:
        raise ValueError(f"Variable de entorno requerida no encontrada: {key}")
    return value


def _get_env_bool(key: str, default: bool = False) -> bool:
    """Obtiene una variable de entorno booleana."""
    return os.getenv(key, str(default)).lower() in ("true", "1", "yes")


def _get_env_int(key: str, default: int = 0) -> int:
    """Obtiene una variable de entorno entera."""
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


def _get_env_float(key: str, default: float = 0.0) -> float:
    """Obtiene una variable de entorno flotante."""
    try:
        return float(os.getenv(key, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class ExchangeConfig:
    """Configuración del exchange de trading."""
    exchange_id: str = field(default_factory=lambda: _get_env("EXCHANGE_ID", "binance"))
    api_key: str = field(default_factory=lambda: _get_env("BINANCE_API_KEY_REAL"))
    api_secret: str = field(default_factory=lambda: _get_env("BINANCE_SECRET_KEY_REAL"))
    api_key_demo: str = field(default_factory=lambda: _get_env("BINANCE_API_KEY_DEMO"))
    api_secret_demo: str = field(default_factory=lambda: _get_env("BINANCE_SECRET_KEY_DEMO"))
    use_demo: bool = field(default_factory=lambda: _get_env_bool("USE_DEMO_ACCOUNT"))
    enable_rate_limit: bool = True
    default_type: str = "spot"


@dataclass(frozen=True)
class TradingConfig:
    """Parámetros de trading y estrategia."""
    symbol: str = "BTC/USDT"
    timeframe: str = "1m"
    timeframe_mtf: str = "1h"
    limit_ohlcv: int = 100
    limit_mtf: int = 200
    min_order_usdt: float = 10.0
    initial_balance_usdt: float = 10.00
    simulation_mode: bool = field(default_factory=lambda: _get_env_bool("MODO_SIMULACION"))


@dataclass(frozen=True)
class StrategyConfig:
    """Parámetros de la estrategia multivariable."""
    ema_fast: int = 9
    ema_slow: int = 21
    ema_mtf_period: int = 200
    rsi_period: int = 14
    rsi_buy_min: float = 30.0
    rsi_buy_max: float = 70.0
    atr_period: int = 14
    atr_sl_multiplier: float = 1.2
    atr_tp_multiplier: float = 1.5
    adx_period: int = 14
    adx_threshold: float = 25.0
    volume_avg_window: int = 20
    trailing_be_threshold_atr: float = 0.5


@dataclass(frozen=True)
class TelegramConfig:
    """Configuración de notificaciones Telegram."""
    token: str = field(default_factory=lambda: _get_env("TELEGRAM_TOKEN"))
    chat_id: str = field(default_factory=lambda: _get_env("TELEGRAM_CHAT_ID"))
    enabled: bool = field(
        default_factory=lambda: bool(_get_env("TELEGRAM_TOKEN")) and bool(_get_env("TELEGRAM_CHAT_ID"))
    )


@dataclass(frozen=True)
class AppConfig:
    """Configuración general de la aplicación."""
    health_check_port: int = field(default_factory=lambda: _get_env_int("PORT", 10000))
    log_level: str = field(default_factory=lambda: _get_env("LOG_LEVEL", "INFO"))
    csv_file: str = "historial_trading.csv"
    loop_interval_seconds: int = 60
    error_interval_seconds: int = 10

    exchange: ExchangeConfig = field(default_factory=ExchangeConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)


def load_config() -> AppConfig:
    """Carga y valida la configuración completa desde variables de entorno."""
    return AppConfig()
