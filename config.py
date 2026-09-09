"""Configuración centralizada del bot de memecoins en Solana.

Usa Pydantic Settings para validar y cargar variables de entorno de forma
tipada y segura desde `.env`. Los nombres canónicos de las variables son los
nuevos (SOLANA_RPC_URL, SOLANA_PRIVATE_KEY, TELEGRAM_BOT_TOKEN,
TRADE_AMOUNT_SOL, TAKE_PROFIT_PERCENT, ...); los nombres históricos
(HELIUS_RPC_URL, PRIVATE_KEY, TELEGRAM_TOKEN, BUY_AMOUNT_SOL,
TAKE_PROFIT_PCT, ...) se siguen leyendo como alias para no romper `.env`
existentes ni los módulos que los consumen (`bot.py`, `core/websocket.py`,
`core/execution.py`, `core/tracker.py`).
"""

from __future__ import annotations

import os

from dotenv import load_dotenv
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()

# --- MODO DE EJECUCIÓN ---
# False = REAL (firma y envía swaps a la red) | True = SIMULACIÓN.
# `.env` puede sobrescribirlo con DRY_RUN=True/False.
DRY_RUN = False


# Configuración base compartida por todas las clases de settings.
# `extra="ignore"` permite que el .env contenga variables adicionales sin
# lanzar errores de validación. Con `env_prefix=""` los campos se mapean
# directamente por su nombre (o alias) en mayúsculas.
BASE_SETTINGS = SettingsConfigDict(
    env_prefix="",
    env_file=".env",
    env_file_encoding="utf-8",
    extra="ignore",
)


class SolanaSettings(BaseSettings):
    """Credenciales y conexión a la red Solana."""

    model_config = BASE_SETTINGS

    # Credenciales y wallet.
    SOLANA_PRIVATE_KEY: str = Field(
        default="",
        validation_alias=AliasChoices("SOLANA_PRIVATE_KEY", "PRIVATE_KEY"),
        description="Clave privada de la wallet en Base58 o mnemonic (12/24 palabras)",
    )
    SOLANA_RPC_URL: str = Field(
        default="https://api.mainnet-beta.solana.com",
        validation_alias=AliasChoices("SOLANA_RPC_URL", "HELIUS_RPC_URL"),
        description="URL HTTP(S) del RPC de Solana",
    )

    @property
    def PRIVATE_KEY(self) -> str:
        """Alias histórico de `SOLANA_PRIVATE_KEY` (compatibilidad)."""
        return self.SOLANA_PRIVATE_KEY

    @property
    def HELIUS_RPC_URL(self) -> str:
        """Alias histórico de `SOLANA_RPC_URL` (compatibilidad)."""
        return self.SOLANA_RPC_URL


class TradingSettings(BaseSettings):
    """Parámetros de ejecución, riesgo y tiempos de actualización."""

    model_config = BASE_SETTINGS

    # --- Gestión de riesgo y monto por trade ---
    TRADE_AMOUNT_SOL: float = Field(
        default=0.005,
        gt=0,
        validation_alias=AliasChoices("TRADE_AMOUNT_SOL", "BUY_AMOUNT_SOL"),
        description="Monto fijo por compra en SOL (~$0.80-$1.00 USD)",)
    MAX_OPEN_POSITIONS: int = Field(
        default=1, gt=0, description="Número máximo de posiciones abiertas simultáneas"
    )
    TAKE_PROFIT_PERCENT: float = Field(
        default=15.0,
        gt=0,
        validation_alias=AliasChoices("TAKE_PROFIT_PERCENT", "TAKE_PROFIT_PCT"),
        description="Ganancia objetivo para vender: +15%",
    )
    STOP_LOSS_PERCENT: float = Field(
        default=8.0,
        gt=0,
        validation_alias=AliasChoices("STOP_LOSS_PERCENT", "STOP_LOSS_PCT"),
        description="Límite de pérdida para vender: -8%",
    )
    MAX_HOLD_TIME_SECONDS: int = Field(
        default=int(os.getenv("MAX_HOLD_TIME_SEC", "180")),
        gt=0,
        description="Tiempo máximo de permanencia de una posición antes de TIME_EXPIRED (s)",
    )

    # --- Parámetros de red y ejecución en Solana ---
    SLIPPAGE_BPS: int = Field(default=500, ge=1, le=10000, description="Slippage máximo en basis points (5%)")
    COMPUTE_UNIT_PRICE_MICRO_LAMPORTS: int = Field(
        default=50000, ge=0, description="Priority fee en micro-lamports por unidad de cómputo"
    )
    JUPITER_QUOTE_URL: str = Field(
        default="https://lite-api.jup.ag/v6/quote",
        description="Endpoint principal de cotización de la Jupiter Swap API v6",
    )
    JUPITER_FALLBACK_URL: str = Field(
        default="https://api.jup.ag/swap/v1/quote",
        description="Endpoint secundario (fallback) de Jupiter ante fallos de DNS/red",
    )

    # --- Tiempos de actualización ---
    POSITION_UPDATE_INTERVAL_SECONDS: int = Field(
        default=30,
        gt=0,
        description="Intervalo (s) entre notificaciones periódicas de progreso de posiciones",
    )
    PRICE_POLL_FALLBACK_SECONDS: int = Field(
        default=5,
        gt=0,
        description="Intervalo (s) sin cotización fresca antes del fallback HTTP de precio",
    )

    # --- Comportamiento de trading ---
    AUTO_SELL: bool = Field(default=True, description="Si vende automáticamente tras take-profit/stop-loss")
    TRAILING_STOP_PCT: float = Field(
        default=float(os.getenv("TRAILING_STOP_PCT", "0.0")),
        ge=0,
        description="Ganancia que activa el trailing stop (0 = desactivado)",
    )
    TRAILING_STOP_ACTIVATION_PCT: float = Field(
        default=20.0, gt=0, description="Ganancia mínima para activar el trailing stop: +20%"
    )
    TRAILING_STOP_DISTANCE_PCT: float = Field(
        default=15.0, gt=0, description="Distancia de retroceso tolerada desde el máximo: -15%"
    )
    DRY_RUN: bool = Field(default=DRY_RUN, description="False = REAL | True = SIMULACIÓN")
    FORCE_TEST_BUY: bool = Field(
        default=False,
        description=(
            "Modo diagnóstico (TEST_MODE): si True, el PRIMER token que llegue por "
            "el WebSocket omite la validación de RugCheck, ejecuta una compra simulada "
            "en Jupiter y envía la alerta a Telegram. Después vuelve a False."
        ),
    )

    @property
    def BUY_AMOUNT_SOL(self) -> float:
        """Alias histórico de `TRADE_AMOUNT_SOL` (compatibilidad)."""
        return float(self.TRADE_AMOUNT_SOL)

    @property
    def TAKE_PROFIT_PCT(self) -> float:
        """Alias histórico de `TAKE_PROFIT_PERCENT` (compatibilidad)."""
        return float(self.TAKE_PROFIT_PERCENT)

    @property
    def STOP_LOSS_PCT(self) -> float:
        """Alias histórico de `STOP_LOSS_PERCENT` (compatibilidad)."""
        return float(self.STOP_LOSS_PERCENT)


class SecuritySettings(BaseSettings):
    """Umbrales de validación de seguridad para tokens."""

    model_config = BASE_SETTINGS

    RUGCHECK_MAX_SCORE: int = Field(default=1500, ge=0, description="Score máximo aceptable de RugCheck")
    DEV_MAX_SUPPLY_PCT: float = Field(default=10.0, ge=0, le=100, description="% máximo del supply que puede tener el Dev")
    REQUIRE_MINT_RENOUNCED: bool = Field(default=True, description="Rechazar si Mint authority no está renunciada")
    REQUIRE_FREEZE_RENOUNCED: bool = Field(default=True, description="Rechazar si Freeze authority no está renunciada")


class TelegramSettings(BaseSettings):
    """Credenciales de Telegram para alertas."""

    model_config = BASE_SETTINGS

    TELEGRAM_BOT_TOKEN: str = Field(
        default="",
        validation_alias=AliasChoices("TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN"),
        description="Token del bot de Telegram",
    )
    TELEGRAM_CHAT_ID: str = Field(default="", description="Chat ID de destino")

    @property
    def TELEGRAM_TOKEN(self) -> str:
        """Alias histórico de `TELEGRAM_BOT_TOKEN` (compatibilidad)."""
        return self.TELEGRAM_BOT_TOKEN

    @property
    def enabled(self) -> bool:
        return bool(self.TELEGRAM_BOT_TOKEN) and bool(self.TELEGRAM_CHAT_ID)


class BotSettings(BaseSettings):
    """Comportamiento general del bot."""

    model_config = BASE_SETTINGS

    LOG_LEVEL: str = Field(default="INFO", description="Nivel de logging")
    POLL_INTERVAL_SECONDS: float = Field(default=2.0, gt=0, description="Intervalo entre ciclos del bot")


class AppConfig:
    """Contenedor de configuración agrupado.

    Carga todos los sub‑conjuntos desde el mismo .env de forma independiente.
    """

    def __init__(self) -> None:
        self.solana = SolanaSettings()
        self.trading = TradingSettings()
        self.security = SecuritySettings()
        self.telegram = TelegramSettings()
        self.bot = BotSettings()


def load_config() -> AppConfig:
    """Factory que construye y valida la configuración completa."""
    return AppConfig()