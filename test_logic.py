import pytest
import pandas as pd
from logic import calcular_sma, calcular_ema, calcular_rsi, calcular_atr, EstrategiaMultivariable, evaluar_estrategia_multivariable, calcular_ganancia_con_stoploss, calcular_profit_factor, validar_profit_factor_minimo, crear_registro_csv

def test_calcular_sma_exito():
    data = pd.Series([float(i) for i in range(1, 25)])
    sma = calcular_sma(data, period=20)
    assert sma.iloc[-1] == 14.5

def test_calcular_sma_error_datos_insuficientes():
    data = pd.Series([10.0, 20.0])
    with pytest.raises(ValueError):
        calcular_sma(data, period=20)

def test_calcular_ema():
    data = pd.Series([float(i) for i in range(1, 30)])
    ema = calcular_ema(data, period=9)
    assert len(ema) == len(data)
    assert not pd.isna(ema.iloc[-1])

def test_calcular_rsi():
    data = pd.Series([100, 101, 102, 101, 100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90])
    rsi = calcular_rsi(data, period=14)
    assert len(rsi) == len(data)
    assert 0 <= rsi.iloc[-1] <= 100

def test_calcular_atr():
    high = pd.Series([100, 105, 110, 108, 112])
    low = pd.Series([95, 100, 105, 103, 107])
    close = pd.Series([98, 103, 108, 106, 110])
    atr = calcular_atr(high, low, close, period=3)
    assert len(atr) == len(high)
    assert atr.iloc[-1] > 0

def test_evaluar_estrategia_multivariable_compra():
    # Test with conditions that should trigger a COMPRA
    estrategia = EstrategiaMultivariable(
        precio_actual=105.0,
        ema_9=100.0,
        ema_21=95.0,
        rsi=40.0,
        volumen=1300.0,  # Changed from 1200 to 1300 to be > 1000*1.2
        volumen_promedio=1000.0,
        prev_precio=95.0,
        prev_ema_9=100.0
    )
    result = evaluar_estrategia_multivariable(estrategia)
    print(f"Test result: {result}")  # Debug output
    assert result == "COMPRA"

def test_evaluar_estrategia_multivariable_venta():
    estrategia = EstrategiaMultivariable(
        precio_actual=95.0,
        ema_9=100.0,
        ema_21=105.0,
        rsi=50.0,
        volumen=850.0,  # Changed from 900 to 850 to be > 1000*0.8
        volumen_promedio=1000.0,
        prev_precio=105.0,
        prev_ema_9=100.0
    )
    assert evaluar_estrategia_multivariable(estrategia) == "VENTA"

def test_evaluar_estrategia_multivariable_neutral():
    estrategia = EstrategiaMultivariable(
        precio_actual=100.0,
        ema_9=100.0,
        ema_21=100.0,
        rsi=80.0,
        volumen=800.0,
        volumen_promedio=1000.0,
        prev_precio=100.0,
        prev_ema_9=100.0
    )
    assert evaluar_estrategia_multivariable(estrategia) == "NEUTRAL"

def test_calcular_ganancia_con_stoploss_tp():
    usdt, pct = calcular_ganancia_con_stoploss(
        precio_venta=11000.0,
        precio_compra=10000.0,
        saldo_btc=0.1,
        atr=500.0,
        tipo_salida="TP"
    )
    assert usdt == 100.0  # 0.1 * (10000 + 2*500 - 10000) = 0.1 * 1000 = 100
    assert pct == 10.0

def test_calcular_ganancia_con_stoploss_sl():
    usdt, pct = calcular_ganancia_con_stoploss(
        precio_venta=9000.0,
        precio_compra=10000.0,
        saldo_btc=0.1,
        atr=500.0,
        tipo_salida="SL"
    )
    assert usdt == -50.0  # 0.1 * (10000 - 500 - 10000) = 0.1 * -500 = -50
    assert pct == -5.0

def test_calcular_profit_factor_ganancia():
    assert calcular_profit_factor(1200.0, 800.0) == 1.5

def test_calcular_profit_factor_perdida():
    assert calcular_profit_factor(0.0, 1000.0) == 0.0

def test_calcular_profit_factor_infinito():
    assert calcular_profit_factor(1000.0, 0.0) == float('inf')

def test_validar_profit_factor_minimo_aprobado():
    assert validar_profit_factor_minimo(1200.0, 800.0, 1.2) == True

def test_calcular_ema_error_datos_insuficientes():
    data = pd.Series([10.0, 20.0])
    with pytest.raises(ValueError):
        calcular_ema(data, period=9)

def test_calcular_rsi_error_datos_insuficientes():
    data = pd.Series([100.0, 101.0, 102.0])
    with pytest.raises(ValueError):
        calcular_rsi(data, period=14)

def test_calcular_atr_error_datos_insuficientes():
    high = pd.Series([100, 105])
    low = pd.Series([95, 100])
    close = pd.Series([98, 103])
    with pytest.raises(ValueError):
        calcular_atr(high, low, close, period=14)

def test_calcular_ganancia_con_stoploss_invalid_data():
    usdt, pct = calcular_ganancia_con_stoploss(
        precio_venta=11000.0,
        precio_compra=0.0,
        saldo_btc=0.1,
        atr=500.0,
        tipo_salida="TP"
    )
    assert usdt == 0.0
    assert pct == 0.0

def test_calcular_ganancia_con_stoploss_regular():
    usdt, pct = calcular_ganancia_con_stoploss(
        precio_venta=11000.0,
        precio_compra=10000.0,
        saldo_btc=0.1,
        atr=500.0,
        tipo_salida="REGULAR"
    )
    assert usdt == 100.0
    assert pct == 10.0

def test_crear_registro_csv():
    registro = crear_registro_csv("COMPRA", 50000.0, 0.02, 1000.0)
    assert registro["Tipo_Operacion"] == "COMPRA"
    assert registro["Monto_USDT"] == 1000.0
    assert registro["Ganancia_Porcentaje"] == "N/A"

def test_crear_registro_csv_venta():
    registro = crear_registro_csv("VENTA", 55000.0, 0.02, 1100.0, ganancias=(100.0, 10.0))
    assert registro["Tipo_Operacion"] == "VENTA"
    assert registro["Ganancia_USDT"] == 100.0
    assert "10.0%" in registro["Ganancia_Porcentaje"]