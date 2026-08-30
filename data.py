"""Módulo de acceso a datos del exchange.

Encapsula toda la interacción con CCXT incluyendo reintentos, manejo de
errores de red, y rate limits. Separa la capa de datos de la lógica de negocio.
"""

import logging
import time
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

import ccxt
import pandas as pd

from config import AppConfig, ExchangeConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


def retry_on_error(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exceptions: Tuple = (ccxt.NetworkError, ccxt.RequestTimeout),
) -> Callable:
    """Decorador para reintentar operaciones de red con backoff exponencial.

    Args:
        max_retries: Número máximo de reintentos.
        base_delay: Delay base en segundos.
        max_delay: Delay máximo en segundos.
        exceptions: Tupla de excepciones que activan reintento.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        logger.warning(
                            "Reintentando %s (intento %d/%d) tras error de red: %s. Esperando %.1fs",
                            func.__name__,
                            attempt + 1,
                            max_retries,
                            type(e).__name__,
                            delay,
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            "Error persistente en %s tras %d reintentos: %s",
                            func.__name__,
                            max_retries,
                            e,
                        )
                except ccxt.RateLimitExceeded as e:
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning(
                        "Rate limit alcanzado en %s. Esperando %.1fs: %s",
                        func.__name__,
                        delay,
                        e,
                    )
                    time.sleep(delay)
                    last_exception = e
                except ccxt.AuthenticationError:
                    raise
                except ccxt.PermissionDenied:
                    raise
                except Exception as e:
                    logger.error("Error inesperado en %s: %s", func.__name__, e)
                    raise
            raise last_exception  # type: ignore
        return wrapper
    return decorator


class ExchangeClient:
    """Cliente del exchange con manejo robusto de errores y reintentos."""

    def __init__(self, config: AppConfig) -> None:
        """Inicializa el cliente del exchange según la configuración.

        Args:
            config: Configuración completa de la aplicación.
        """
        self.config = config
        self.exchange_config = config.exchange
        self._exchange: Optional[ccxt.Exchange] = None
        self._initialize_exchange()

    def _initialize_exchange(self) -> None:
        """Crea la instancia del exchange CCXT."""
        if self.exchange_config.use_demo or self._is_render_env():
            self._exchange = ccxt.kraken({
                "enableRateLimit": self.exchange_config.enable_rate_limit,
            })
            logger.info("Exchange inicializado: Kraken (modo demo/público)")
        else:
            self._exchange = ccxt.binance({
                "apiKey": self.exchange_config.api_key,
                "secret": self.exchange_config.api_secret,
                "enableRateLimit": self.exchange_config.enable_rate_limit,
                "options": {"defaultType": self.exchange_config.default_type},
            })
            logger.info("Exchange inicializado: Binance (cuenta real)")

    def _is_render_env(self) -> bool:
        """Detecta si estamos en el entorno de Render."""
        import os
        return "RENDER" in os.environ or "RENDER_EXTERNAL_URL" in os.environ

    @property
    def exchange(self) -> ccxt.Exchange:
        """Retorna la instancia del exchange."""
        if self._exchange is None:
            raise RuntimeError("Exchange no inicializado")
        return self._exchange

    @retry_on_error(max_retries=3, base_delay=2.0)
    def fetch_ohlcv(
        self, symbol: str, timeframe: str, limit: int = 100
    ) -> pd.DataFrame:
        """Obtiene datos OHLCV del exchange con reintentos.

        Args:
            symbol: Par de trading (ej: "BTC/USDT").
            timeframe: Temporalidad (ej: "1m", "1h").
            limit: Número de velas a obtener.

        Returns:
            DataFrame con columnas: timestamp, open, high, low, close, volume.
        """
        ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(
            ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        logger.debug(
            "OHLCV obtenido: %s %s (%d velas)", symbol, timeframe, len(df)
        )
        return df

    @retry_on_error(max_retries=2, base_delay=1.0)
    def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """Obtiene el ticker actual del exchange.

        Args:
            symbol: Par de trading.

        Returns:
            Diccionario con datos del ticker.
        """
        return self.exchange.fetch_ticker(symbol)

    @retry_on_error(max_retries=2, base_delay=1.0)
    def fetch_balance(self) -> Dict[str, Any]:
        """Obtiene el balance de la cuenta.

        Returns:
            Diccionario con el balance.
        """
        return self.exchange.fetch_balance()

    def get_market_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Obtiene información del mercado (límites de orden, etc.).

        Args:
            symbol: Par de trading.

        Returns:
            Información del mercado o None si hay error.
        """
        try:
            market = self.exchange.market(symbol)
            if market and "limits" in market and "cost" in market["limits"]:
                min_cost = market["limits"]["cost"].get("min")
                if min_cost is not None:
                    return {"min_order_usdt": float(min_cost)}
        except Exception as e:
            logger.warning("No se pudo obtener info del mercado para %s: %s", symbol, e)
        return None

    @retry_on_error(max_retries=2, base_delay=1.0)
    def create_market_buy_order(
        self, symbol: str, amount: float
    ) -> Dict[str, Any]:
        """Ejecuta una orden de compra a mercado.

        Args:
            symbol: Par de trading.
            amount: Cantidad a comprar.

        Returns:
            Resultado de la orden.
        """
        logger.info("Ejecutando orden COMPRA: %s %.6f", symbol, amount)
        return self.exchange.create_market_buy_order(symbol, amount)

    @retry_on_error(max_retries=2, base_delay=1.0)
    def create_market_sell_order(
        self, symbol: str, amount: float
    ) -> Dict[str, Any]:
        """Ejecuta una orden de venta a mercado.

        Args:
            symbol: Par de trading.
            amount: Cantidad a vender.

        Returns:
            Resultado de la orden.
        """
        logger.info("Ejecutando orden VENTA: %s %.6f", symbol, amount)
        return self.exchange.create_market_sell_order(symbol, amount)
