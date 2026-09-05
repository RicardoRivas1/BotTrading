"""Configuración centralizada del bot de memecoins en Solana.

Usa Pydantic Settings para validar y cargar variables de entorno de forma
tipada y segura. Nunca se ejecuta sin que todas las credenciales estén
definidas.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Configuración base compartida por todas las clases de settings.
# `extra="ignore"` permite que el .env contenga variables adicionales (p. ej.
# de otra herramienta) sin lanzar errores de validación (extra_forbidden).
# Con `env_prefix=""` los campos se mapean directamente por su nombre en mayúsculas.
BASE_SETTINGS = SettingsConfigDict(
    env_prefix="",
    env_file=".env",
    env_file_encoding="utf-8",
    extra="ignore",
)


class SolanaSettings(BaseSettings):
    """Parámetros de conexión a la red Solana."""

    model_config = BASE_SETTINGS

    HELIUS_RPC_URL: str = Field(..., description="URL HTTP(S) del RPC de Helius")
    HELIUS_WS_URL: str = Field(..., description="URL WebSocket del RPC de Helius")
    PRIVATE_KEY: str = Field(..., description="Clave privada de la wallet en Base58 o mnemonic (12/24 palabras)")


class TradingSettings(BaseSettings):
    """Parámetros de ejecución de trades."""

    model_config = BASE_SETTINGS

    BUY_AMOUNT_SOL: float = Field(default=0.05, gt=0, description="Monto fijo por compra en SOL")
    SLIPPAGE_BPS: int = Field(default=500, ge=1, le=10000, description="Slippage máximo en basis points")
    AUTO_SELL: bool = Field(default=True, description="Si vende automáticamente tras take-profit/stop-loss")
    TAKE_PROFIT_PCT: float = Field(default=100.0, gt=0, description="Ganancia objetivo: +100%")
    STOP_LOSS_PCT: float = Field(default=30.0, gt=0, description="Límite de pérdida: -30%")
    TRAILING_STOP_ACTIVATION_PCT: float = Field(
        default=20.0, gt=0, description="Ganancia mínima para activar el trailing stop: +20%"
    )
    TRAILING_STOP_DISTANCE_PCT: float = Field(
        default=15.0, gt=0, description="Distancia de retroceso tolerada desde el máximo: -15%"
    )
    MAX_SOL_BALANCE: float = Field(default=1.0, gt=0, description="Máximo SOL a invertir por operación")
    DRY_RUN: bool = Field(default=True, description="Si True, no ejecuta transacciones reales (simulación)")
    FORCE_TEST_BUY: bool = Field(
        default=False,
        description=(
            "Modo diagnóstico (TEST_MODE): si True, el PRIMER token que llegue por "
            "el WebSocket omite la validación de RugCheck, ejecuta una compra simulada "
            "en Jupiter y envía la alerta a Telegram. Después vuelve a False."
        ),
    )


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

    TELEGRAM_TOKEN: str = Field(default="", description="Token del bot de Telegram")
    TELEGRAM_CHAT_ID: str = Field(default="", description="Chat ID de destino")

    @property
    def enabled(self) -> bool:
        return bool(self.TELEGRAM_TOKEN) and bool(self.TELEGRAM_CHAT_ID)


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
