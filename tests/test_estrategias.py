import pytest
import sys
import os

# Añadir directorio padre al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logic import EstrategiaMultivariable, evaluar_estrategia_multivariable


class TestEstrategiaTrading:
    
    def test_estrategia_compra(self):
        """Test de señal de COMPRA con condiciones ideales."""
        estrategia = EstrategiaMultivariable(
            precio_actual=105,
            ema_9=100,
            ema_21=95,
            rsi=45,
            volumen=1500,
            volumen_promedio=1000,
            prev_precio=98,
            prev_ema_9=99
        )
        
        señal = evaluar_estrategia_multivariable(estrategia)
        assert señal == "COMPRA"
    
    def test_estrategia_venta(self):
        """Test de señal de VENTA con condiciones ideales."""
        estrategia = EstrategiaMultivariable(
            precio_actual=95,
            ema_9=100,
            ema_21=105,
            rsi=60,  # Cambiado a > 30 para cumplir condición
            volumen=1200,  # Cambiado a > 800 (0.8 * 1000)
            volumen_promedio=1000,
            prev_precio=102,
            prev_ema_9=101
        )
        
        señal = evaluar_estrategia_multivariable(estrategia)
        assert señal == "VENTA"
    
    def test_estrategia_neutral(self):
        """Test de señal NEUTRAL con condiciones mixtas."""
        estrategia = EstrategiaMultivariable(
            precio_actual=100,
            ema_9=100,
            ema_21=100,
            rsi=50,
            volumen=1000,
            volumen_promedio=1000,
            prev_precio=100,
            prev_ema_9=100
        )
        
        señal = evaluar_estrategia_multivariable(estrategia)
        assert señal == "NEUTRAL"