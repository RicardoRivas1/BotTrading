"""Módulo con la lógica pura de negocio para el bot de trading.

Todas las funciones son puras: reciben datos y devuelven resultados sin
efectos secundarios (sin llamadas a API, sin E/S, sin estado global).
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# Indicadores técnicos (funciones puras)
# ---------------------------------------------------------------------------

def calcular_sma(precios: pd.Series, period: int = 20) -> pd.Series:
    """Calcula la Media Móvil Simple (SMA)."""
    if len(precios) < period:
        raise ValueError("Datos insuficientes para el período solicitado")
    return precios.rolling(window=period).mean()


def calcular_ema(precios: pd.Series, period: int) -> pd.Series:
    """Calcula la Media Móvil Exponencial (EMA)."""
    if len(precios) < period:
        raise ValueError("Datos insuficientes para el período solicitado")
    return precios.ewm(span=period, adjust=False).mean()


def calcular_rsi(precios: pd.Series, period: int = 14) -> pd.Series:
    """Calcula el Índice de Fuerza Relativa (RSI)."""
    if len(precios) < period + 1:
        raise ValueError("Datos insuficientes para el período solicitado")

    delta = precios.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calcular_atr(
    high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14
) -> pd.Series:
    """Calcula el Average True Range (ATR)."""
    if len(high) < period + 1:
        raise ValueError("Datos insuficientes para el período solicitado")

    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))

    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = true_range.rolling(window=period).mean()
    return atr


def _rma(series: pd.Series, length: int) -> pd.Series:
    """Suavizado Wilder (RMA) auxiliar."""
    result = series.copy()
    first_valid = series.first_valid_index()
    if first_valid is None:
        return result
    first_pos = series.index.get_loc(first_valid)
    result.iloc[: first_pos + length] = float("nan")
    result.iloc[first_pos + length - 1] = series.iloc[first_pos : first_pos + length].mean()
    for i in range(first_pos + length, len(series)):
        result.iloc[i] = (result.iloc[i - 1] * (length - 1) + series.iloc[i]) / length
    return result


def calcular_adx(
    high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14
) -> pd.Series:
    """Calcula el Average Directional Index (ADX) con suavizado Wilder, rango 0-100."""
    if len(high) < period * 2 + 1:
        raise ValueError("Datos insuficientes para el período solicitado")

    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    up_move = high - high.shift(1)
    down_move = low.shift(1) - low
    plus_dm = pd.Series(
        up_move.where((up_move > down_move) & (up_move > 0), 0.0),
        index=high.index,
    )
    minus_dm = pd.Series(
        down_move.where((down_move > up_move) & (down_move > 0), 0.0),
        index=high.index,
    )

    smoothed_tr = _rma(tr, period)
    smoothed_plus_dm = _rma(plus_dm, period)
    smoothed_minus_dm = _rma(minus_dm, period)

    plus_di = 100 * smoothed_plus_dm / smoothed_tr
    minus_di = 100 * smoothed_minus_dm / smoothed_tr

    di_sum = plus_di + minus_di
    dx = (100 * (plus_di - minus_di).abs() / di_sum).fillna(0)

    adx = _rma(dx, period)
    return adx.clip(0, 100)


def calcular_sma_atr(atr_series: pd.Series, period: int = 20) -> pd.Series:
    """Calcula la SMA del ATR para filtro de volatilidad relativa."""
    return calcular_sma(atr_series, period)


# ---------------------------------------------------------------------------
# Estrategia multivariable (funciones puras)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EstrategiaMultivariable:
    """Contenedor inmutable para los parámetros de la estrategia."""
    precio_actual: float
    ema_9: float
    ema_21: float
    rsi: float
    volumen: float
    volumen_promedio: float
    prev_precio: float
    prev_ema_9: float
    prev_ema_21: Optional[float] = None


def evaluar_estrategia_multivariable(estrategia: EstrategiaMultivariable) -> str:
    """Evalúa estrategia multivariable con cruce EMA 9/21, RSI y filtro de volumen.

    Returns:
        "COMPRA", "VENTA" o "NEUTRAL"
    """
    cruce_alcista = (
        estrategia.prev_ema_21 is not None
        and estrategia.prev_ema_9 <= estrategia.prev_ema_21
        and estrategia.ema_9 > estrategia.ema_21
    )
    tendencia_alcista = estrategia.ema_9 > estrategia.ema_21
    precio_sobre_ema9 = estrategia.precio_actual > estrategia.ema_9
    rsi_ok = 30 < estrategia.rsi < 70
    volumen_ok = estrategia.volumen > estrategia.volumen_promedio

    cruce_bajista = (
        estrategia.prev_ema_21 is not None
        and estrategia.prev_ema_9 >= estrategia.prev_ema_21
        and estrategia.ema_9 < estrategia.ema_21
    )
    tendencia_bajista = estrategia.ema_9 < estrategia.ema_21
    precio_bajo_ema9 = estrategia.precio_actual < estrategia.ema_9
    volumen_ok_venta = estrategia.volumen > estrategia.volumen_promedio * 0.8

    if cruce_alcista and tendencia_alcista and precio_sobre_ema9 and rsi_ok and volumen_ok:
        return "COMPRA"

    if cruce_bajista and tendencia_bajista and precio_bajo_ema9 and volumen_ok_venta:
        return "VENTA"

    return "NEUTRAL"


# ---------------------------------------------------------------------------
# Resultado de filtros cuantitativos (tipado, sin hardcodes)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ResultadoFiltros:
    """Resultado tipado de la validación de filtros cuantitativos."""
    adx_valido: bool
    ema_mtf_valido: bool
    horario_valido: bool
    volatilidad_valida: bool

    @property
    def todos_validos(self) -> bool:
        return all([
            self.adx_valido,
            self.ema_mtf_valido,
            self.horario_valido,
            self.volatilidad_valida,
        ])


def validar_adx(
    high: pd.Series, low: pd.Series, close: pd.Series, threshold: float = 25.0
) -> bool:
    """Valida que el mercado está en tendencia (ADX > threshold)."""
    try:
        adx = calcular_adx(high, low, close, period=14)
        current_adx = adx.iloc[-1]
        return bool(current_adx > threshold) if not pd.isna(current_adx) else False
    except Exception:
        return False


def validar_ema_200_mtf(df_mtf: pd.DataFrame, period: int = 200) -> bool:
    """Valida si el precio actual está por encima de EMA 200 en timeframe superior."""
    if df_mtf is None or len(df_mtf) < period:
        return False

    ema_200 = calcular_ema(df_mtf["close"], period=period)
    current_price_mtf = df_mtf["close"].iloc[-1]
    current_ema_200 = ema_200.iloc[-1]

    return bool(current_price_mtf > current_ema_200)


def validar_volatilidad_relativa(atr_series: pd.Series, period: int = 20) -> bool:
    """Valida que el ATR actual es mayor que la SMA del ATR (volatilidad alta)."""
    try:
        sma_atr = calcular_sma_atr(atr_series, period)
        current_atr = atr_series.iloc[-1]
        current_sma_atr = sma_atr.iloc[-1]
        return bool(current_atr > current_sma_atr) if not pd.isna(current_sma_atr) else False
    except Exception:
        return False


def validar_horario_mercado(hora_inicio: int = 0, hora_fin: int = 24) -> bool:
    """Valida si la hora actual UTC está dentro del rango permitido."""
    current_hour = datetime.now(timezone.utc).hour
    return hora_inicio <= current_hour < hora_fin


def validar_filtros_cuantitativos(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    df_mtf: Optional[pd.DataFrame],
    atr_series: Optional[pd.Series] = None,
    adx_threshold: float = 25.0,
    hora_inicio: int = 0,
    hora_fin: int = 24,
) -> ResultadoFiltros:
    """Evalúa todos los filtros cuantitativos de forma pura y dinámica.

    Returns:
        ResultadoFiltros con cada condición evaluada (sin hardcodes).
    """
    adx_ok = validar_adx(high, low, close, threshold=adx_threshold)
    ema_mtf_ok = validar_ema_200_mtf(df_mtf)
    horario_ok = validar_horario_mercado(hora_inicio, hora_fin)

    volatilidad_ok = True
    if atr_series is not None and len(atr_series) > 0:
        volatilidad_ok = validar_volatilidad_relativa(atr_series)

    return ResultadoFiltros(
        adx_valido=adx_ok,
        ema_mtf_valido=ema_mtf_ok,
        horario_valido=horario_ok,
        volatilidad_valida=volatilidad_ok,
    )


# ---------------------------------------------------------------------------
# Cálculos financieros (funciones puras)
# ---------------------------------------------------------------------------

def calcular_ganancia_con_stoploss(
    precio_venta: float,
    precio_compra: float,
    saldo_btc: float,
    atr: float,
    tipo_salida: str = "TP",
) -> Tuple[float, float]:
    """Calcula la ganancia con Stop-Loss/Take-Profit dinámico basado en ATR.

    Returns:
        Tupla de (ganancia_usdt, ganancia_porcentaje).
    """
    if precio_compra <= 0 or saldo_btc <= 0 or atr <= 0:
        return 0.0, 0.0

    if tipo_salida == "TP":
        precio_venta_real = precio_compra + (atr * 1.5)
    elif tipo_salida == "SL":
        precio_venta_real = precio_compra - (atr * 1.2)
    else:
        precio_venta_real = precio_venta

    monto_usdt_inicial = saldo_btc * precio_compra
    monto_usdt_final = saldo_btc * precio_venta_real
    ganancia_usdt = monto_usdt_final - monto_usdt_inicial
    ganancia_pct = ((precio_venta_real - precio_compra) / precio_compra) * 100

    return round(ganancia_usdt, 2), round(ganancia_pct, 2)


def calcular_profit_factor(ganancias_totales: float, perdidas_totales: float) -> float:
    """Calcula el Profit Factor (ganancias/perdidas)."""
    if perdidas_totales == 0:
        return float("inf") if ganancias_totales > 0 else 1.0
    return round(abs(ganancias_totales) / abs(perdidas_totales), 2)


def validar_profit_factor_minimo(
    ganancias_totales: float, perdidas_totales: float, minimo: float = 1.2
) -> bool:
    """Valida si el Profit Factor cumple con el mínimo requerido."""
    pf = calcular_profit_factor(ganancias_totales, perdidas_totales)
    return pf >= minimo


def crear_registro_csv(
    tipo: str, precio: float, btc: float, usdt: float, ganancias: Tuple[float, float] = (0.0, 0.0)
) -> dict:
    """Estructura una fila lista para persistir en el historial CSV."""
    ganancia_usdt, ganancia_pct = ganancias
    return {
        "Fecha_Hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Tipo_Operacion": tipo,
        "Precio_BTC": round(precio, 2),
        "Monto_BTC": round(btc, 6),
        "Monto_USDT": round(usdt, 2),
        "Ganancia_USDT": round(ganancia_usdt, 2),
        "Ganancia_Porcentaje": f"{round(ganancia_pct, 2)}%"
        if tipo == "VENTA"
        else "N/A",
    }
