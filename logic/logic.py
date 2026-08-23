"""Módulo con la lógica pura de negocio para el bot de trading."""

from datetime import datetime
import os
import pandas as pd
import requests
import ccxt


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


def calcular_adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Calcula el Average Directional Index (ADX) con suavizado Wilder, rango 0-100."""
    if len(high) < period * 2 + 1:
        raise ValueError("Datos insuficientes para el período solicitado")

    # True Range
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # +DM y -DM
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

    # Wilder smoothing (RMA): valor anterior * (period-1) + actual / period
    def rma(series, length):
        result = series.copy()
        first_valid = series.first_valid_index()
        first_pos = series.index.get_loc(first_valid)
        result.iloc[:first_pos + length] = float('nan')
        result.iloc[first_pos + length - 1] = series.iloc[first_pos:first_pos + length].mean()
        for i in range(first_pos + length, len(series)):
            result.iloc[i] = (result.iloc[i - 1] * (length - 1) + series.iloc[i]) / length
        return result

    smoothed_tr = rma(tr, period)
    smoothed_plus_dm = rma(plus_dm, period)
    smoothed_minus_dm = rma(minus_dm, period)

    # +DI y -DI
    plus_di = 100 * smoothed_plus_dm / smoothed_tr
    minus_di = 100 * smoothed_minus_dm / smoothed_tr

    # DX
    di_sum = plus_di + minus_di
    dx = (100 * (plus_di - minus_di).abs() / di_sum).fillna(0)

    # ADX = Wilder smoothing de DX
    adx = rma(dx, period)

    return adx.clip(0, 100)


def calcular_sma_atr(atr_series: pd.Series, period: int = 20) -> pd.Series:
    """Calcula la SMA del ATR para filtro de volatilidad relativa."""
    return calcular_sma(atr_series, period)


class EstrategiaMultivariable:
    """Contenedor para los parámetros de la estrategia multivariable."""

    def __init__(
        self,
        precio_actual,
        ema_9,
        ema_21,
        rsi,
        volumen,
        volumen_promedio,
        prev_precio,
        prev_ema_9,
    ):
        self.precio_actual = precio_actual
        self.ema_9 = ema_9
        self.ema_21 = ema_21
        self.rsi = rsi
        self.volumen = volumen
        self.volumen_promedio = volumen_promedio
        self.prev_precio = prev_precio
        self.prev_ema_9 = prev_ema_9


class FiltrosCuantitativos:
    """Contenedor para todos los filtros cuantitativos del sistema."""
    
    def __init__(self, exchange: ccxt.Exchange, symbol: str = "BTC/USDT"):
        self.exchange = exchange
        self.symbol = symbol
    
    def obtener_datos_mtf(self, timeframe_superior: str = "1h", limit: int = 200) -> pd.DataFrame:
        """Obtiene datos del timeframe superior para confirmación MTF."""
        try:
            ohlcv = self.exchange.fetch_ohlcv(self.symbol, timeframe_superior, limit=limit)
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            return df
        except Exception as e:
            print(f"Error obteniendo datos MTF {timeframe_superior}: {e}")
            return None
    
    def confirmar_ema_200_mtf(self, df_mtf: pd.DataFrame) -> bool:
        """Valida si el precio actual está por encima de EMA 200 en timeframe superior."""
        if df_mtf is None or len(df_mtf) < 200:
            return False
        
        df_mtf["ema_200"] = calcular_ema(df_mtf["close"], period=200)
        current_price_mtf = df_mtf["close"].iloc[-1]
        current_ema_200_mtf = df_mtf["ema_200"].iloc[-1]
        
        return current_price_mtf > current_ema_200_mtf
    
    def validar_adx_tendencia(self, high: pd.Series, low: pd.Series, close: pd.Series, threshold: float = 25.0) -> bool:
        """Valida que el mercado está en tendencia (ADX > threshold)."""
        try:
            adx = calcular_adx(high, low, close, period=14)
            current_adx = adx.iloc[-1]
            return bool(current_adx > threshold) if not pd.isna(current_adx) else False
        except Exception:
            return False
    
    def validar_volatilidad_relativa(self, atr_series: pd.Series, period: int = 20) -> bool:
        """Valida que el ATR actual es mayor que la SMA del ATR."""
        try:
            sma_atr = calcular_sma_atr(atr_series, period)
            current_atr = atr_series.iloc[-1]
            current_sma_atr = sma_atr.iloc[-1]
            return bool(current_atr > current_sma_atr)
        except Exception:
            return False
    
    def validar_horario_mercado(self, hora_inicio: int = 13, hora_fin: int = 21) -> bool:
        """Valida si la hora actual está dentro del rango de máxima liquidez (UTC)."""
        from datetime import datetime
        current_hour = datetime.utcnow().hour
        return hora_inicio <= current_hour < hora_fin


def evaluar_estrategia_multivariable(estrategia: EstrategiaMultivariable) -> str:
    """Evalúa estrategia multivariable con EMA 9/21, RSI y filtro de volumen."""
    # Condiciones de COMPRA
    compra_ema = (
        estrategia.prev_precio <= estrategia.prev_ema_9
        and estrategia.precio_actual > estrategia.ema_9
    )
    compra_tendencia = estrategia.ema_9 > estrategia.ema_21
    compra_rsi = 35 < estrategia.rsi < 65  # Más estricto: 35-65 en lugar de 30-70
    compra_volumen = estrategia.volumen > estrategia.volumen_promedio * 1.2

    # Condiciones de VENTA
    venta_ema = (
        estrategia.prev_precio >= estrategia.prev_ema_9
        and estrategia.precio_actual < estrategia.ema_9
    )
    venta_tendencia = estrategia.ema_9 < estrategia.ema_21
    venta_rsi = estrategia.rsi > 40  # Más estricto: RSI > 40 para venta (antes > 30)
    venta_volumen = estrategia.volumen > estrategia.volumen_promedio * 0.8

    if compra_ema and compra_tendencia and compra_rsi and compra_volumen:
        return "COMPRA"
    if venta_ema and venta_tendencia and venta_rsi and venta_volumen:
        return "VENTA"
    return "NEUTRAL"


def calcular_ganancia_con_stoploss(
    precio_venta: float,
    precio_compra: float,
    saldo_btc: float,
    atr: float,
    tipo_salida: str = "TP",
) -> tuple:
    """Calcula la ganancia con Stop-Loss/Take-Profit dinámico basado en ATR."""
    if precio_compra <= 0 or saldo_btc <= 0 or atr <= 0:
        return 0.0, 0.0

    # Definir niveles basados en ATR - MÁS CONSERVADORES
    atr_multiplier_tp = 1.5  # Take-Profit a 1.5 ATRs (reducido de 2.0)
    atr_multiplier_sl = 1.2  # Stop-Loss a 1.2 ATRs (aumentado de 1.0)

    if tipo_salida == "TP":
        precio_venta_real = precio_compra + (atr * atr_multiplier_tp)
    elif tipo_salida == "SL":
        precio_venta_real = precio_compra - (atr * atr_multiplier_sl)
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
    tipo: str, precio: float, btc: float, usdt: float, ganancias: tuple = (0.0, 0.0)
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


# ==========================================
# MÓDULO DE NOTIFICACIONES TELEGRAM
# ==========================================


def enviar_notificacion_telegram(mensaje: str) -> bool:
    """Envía un mensaje a Telegram utilizando las variables de entorno de Render."""
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": mensaje, "parse_mode": "Markdown"}

    try:
        response = requests.post(url, json=payload, timeout=5)
        return response.status_code == 200
    except Exception:
        return False


def notificar_operacion_telegram(
    tipo: str, precio: float, btc: float, usdt: float, ganancias: tuple = (0.0, 0.0)
):
    """Genera y envía la alerta visual a Telegram al ejecutar compra/venta."""
    ganancia_usdt, ganancia_pct = ganancias

    if tipo == "COMPRA":
        mensaje = (
            f"🟢 *ORDEN DE COMPRA EJECUTADA*\n\n"
            f"• *Precio BTC:* ${precio:,.2f}\n"
            f"• *Monto BTC:* {btc:.6f}\n"
            f"• *Total USDT:* ${usdt:,.2f}"
        )
    elif tipo == "VENTA":
        mensaje = (
            f"🔴 *ORDEN DE VENTA EJECUTADA*\n\n"
            f"• *Precio Venta:* ${precio:,.2f}\n"
            f"• *Monto BTC:* {btc:.6f}\n"
            f"• *Total USDT:* ${usdt:,.2f}\n"
            f"• *Ganancia USDT:* ${ganancia_usdt:,.2f}\n"
            f"• *Rendimiento:* {ganancia_pct:.2f}%"
        )
    else:
        return

    enviar_notificacion_telegram(mensaje)