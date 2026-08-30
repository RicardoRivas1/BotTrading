"""Pruebas unitarias para la estrategia multivariable."""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logic import EstrategiaMultivariable, evaluar_estrategia_multivariable


class TestEstrategiaTrading:

    def test_estrategia_compra(self) -> None:
        """Test de señal de COMPRA con cruce alcista EMA 9/21."""
        estrategia = EstrategiaMultivariable(
            precio_actual=105.0,
            ema_9=101.0,
            ema_21=100.0,
            rsi=45.0,
            volumen=1500.0,
            volumen_promedio=1000.0,
            prev_precio=104.0,
            prev_ema_9=99.0,
            prev_ema_21=100.0,
        )
        senial = evaluar_estrategia_multivariable(estrategia)
        assert senial == "COMPRA"

    def test_estrategia_venta(self) -> None:
        """Test de señal de VENTA con cruce bajista EMA 9/21."""
        estrategia = EstrategiaMultivariable(
            precio_actual=95.0,
            ema_9=99.0,
            ema_21=100.0,
            rsi=60.0,
            volumen=1200.0,
            volumen_promedio=1000.0,
            prev_precio=96.0,
            prev_ema_9=101.0,
            prev_ema_21=100.0,
        )
        senial = evaluar_estrategia_multivariable(estrategia)
        assert senial == "VENTA"

    def test_estrategia_neutral(self) -> None:
        """Test de señal NEUTRAL con condiciones mixtas."""
        estrategia = EstrategiaMultivariable(
            precio_actual=100.0,
            ema_9=100.0,
            ema_21=100.0,
            rsi=50.0,
            volumen=1000.0,
            volumen_promedio=1000.0,
            prev_precio=100.0,
            prev_ema_9=100.0,
            prev_ema_21=100.0,
        )
        senial = evaluar_estrategia_multivariable(estrategia)
        assert senial == "NEUTRAL"

    def test_compra_requiere_cruce_alcista(self) -> None:
        """Sin cruce alcista previo, no debe dar COMPRA."""
        estrategia = EstrategiaMultivariable(
            precio_actual=105.0,
            ema_9=103.0,
            ema_21=100.0,
            rsi=45.0,
            volumen=1500.0,
            volumen_promedio=1000.0,
            prev_precio=104.0,
            prev_ema_9=102.0,  # Ya estaba por encima
            prev_ema_21=100.0,
        )
        senial = evaluar_estrategia_multivariable(estrategia)
        assert senial == "NEUTRAL"

    def test_compra_requiere_rsi_en_rango(self) -> None:
        """RSI fuera de rango 30-70 bloquea COMPRA."""
        estrategia = EstrategiaMultivariable(
            precio_actual=105.0,
            ema_9=101.0,
            ema_21=100.0,
            rsi=75.0,  # Fuera de rango
            volumen=1500.0,
            volumen_promedio=1000.0,
            prev_precio=104.0,
            prev_ema_9=99.0,
            prev_ema_21=100.0,
        )
        senial = evaluar_estrategia_multivariable(estrategia)
        assert senial == "NEUTRAL"

    def test_venta_requiere_cruce_bajista(self) -> None:
        """Sin cruce bajista previo, no debe dar VENTA."""
        estrategia = EstrategiaMultivariable(
            precio_actual=95.0,
            ema_9=99.0,
            ema_21=100.0,
            rsi=60.0,
            volumen=1200.0,
            volumen_promedio=1000.0,
            prev_precio=96.0,
            prev_ema_9=98.0,  # Ya estaba por debajo
            prev_ema_21=100.0,
        )
        senial = evaluar_estrategia_multivariable(estrategia)
        assert senial == "NEUTRAL"

    def test_venta_permite_volumen_80_pct(self) -> None:
        """VENTA acepta volumen al 80% del promedio."""
        estrategia = EstrategiaMultivariable(
            precio_actual=95.0,
            ema_9=99.0,
            ema_21=100.0,
            rsi=60.0,
            volumen=850.0,  # 85% de 1000
            volumen_promedio=1000.0,
            prev_precio=96.0,
            prev_ema_9=101.0,
            prev_ema_21=100.0,
        )
        senial = evaluar_estrategia_multivariable(estrategia)
        assert senial == "VENTA"

    def test_venta_falla_con_volumen_bajo(self) -> None:
        """VENTA falla si volumen < 80% del promedio."""
        estrategia = EstrategiaMultivariable(
            precio_actual=95.0,
            ema_9=99.0,
            ema_21=100.0,
            rsi=60.0,
            volumen=700.0,  # 70% de 1000
            volumen_promedio=1000.0,
            prev_precio=96.0,
            prev_ema_9=101.0,
            prev_ema_21=100.0,
        )
        senial = evaluar_estrategia_multivariable(estrategia)
        assert senial == "NEUTRAL"
