"""Pruebas de integración para la lógica de negocio del bot."""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from logic import (
    calcular_ganancia_con_stoploss,
    calcular_profit_factor,
    crear_registro_csv,
    validar_profit_factor_minimo,
)


class TestCalculosFinancieros:

    def test_ganancia_compra_venta_beneficio(self) -> None:
        ganancia_usdt, ganancia_pct = calcular_ganancia_con_stoploss(
            precio_venta=110.0,
            precio_compra=100.0,
            saldo_btc=1.0,
            atr=5.0,
            tipo_salida="MANUAL",
        )
        assert ganancia_usdt == 10.0
        assert ganancia_pct == 10.0

    def test_ganancia_stop_loss(self) -> None:
        ganancia_usdt, ganancia_pct = calcular_ganancia_con_stoploss(
            precio_venta=90.0,
            precio_compra=100.0,
            saldo_btc=1.0,
            atr=10.0,
            tipo_salida="SL",
        )
        assert ganancia_usdt < 0

    def test_ganancia_take_profit(self) -> None:
        ganancia_usdt, ganancia_pct = calcular_ganancia_con_stoploss(
            precio_venta=120.0,
            precio_compra=100.0,
            saldo_btc=1.0,
            atr=10.0,
            tipo_salida="TP",
        )
        assert ganancia_usdt > 0

    def test_ganancia_datos_invalidos(self) -> None:
        ganancia_usdt, ganancia_pct = calcular_ganancia_con_stoploss(
            precio_venta=100.0,
            precio_compra=0.0,
            saldo_btc=1.0,
            atr=5.0,
        )
        assert ganancia_usdt == 0.0
        assert ganancia_pct == 0.0


class TestMetricasCalidad:

    def test_profit_factor_positivo(self) -> None:
        assert calcular_profit_factor(1000.0, 500.0) == 2.0

    def test_profit_factor_break_even(self) -> None:
        assert calcular_profit_factor(1000.0, 1000.0) == 1.0

    def test_profit_factor_perdidas(self) -> None:
        assert calcular_profit_factor(500.0, 1000.0) == 0.5

    def test_profit_factor_sin_perdidas(self) -> None:
        assert calcular_profit_factor(1000.0, 0.0) == float("inf")

    def test_profit_factor_cero_total(self) -> None:
        assert calcular_profit_factor(0.0, 0.0) == 1.0

    def test_validar_profit_factor_minimo(self) -> None:
        assert validar_profit_factor_minimo(1000.0, 500.0, minimo=1.5) is True
        assert validar_profit_factor_minimo(500.0, 1000.0, minimo=1.5) is False


class TestRegistroCSV:

    def test_crear_registro_compra(self) -> None:
        reg = crear_registro_csv("COMPRA", 100.0, 0.5, 50.0)
        assert reg["Tipo_Operacion"] == "COMPRA"
        assert reg["Precio_BTC"] == 100.0
        assert reg["Monto_BTC"] == 0.5
        assert reg["Monto_USDT"] == 50.0
        assert reg["Ganancia_Porcentaje"] == "N/A"

    def test_crear_registro_venta(self) -> None:
        reg = crear_registro_csv("VENTA", 110.0, 0.5, 55.0, ganancias=(5.0, 10.0))
        assert reg["Tipo_Operacion"] == "VENTA"
        assert reg["Ganancia_USDT"] == 5.0
        assert reg["Ganancia_Porcentaje"] == "10.0%"

    def test_crear_registro_tiene_fecha(self) -> None:
        reg = crear_registro_csv("COMPRA", 100.0, 0.5, 50.0)
        assert "Fecha_Hora" in reg
        assert len(reg["Fecha_Hora"]) > 0
