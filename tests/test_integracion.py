"""Pruebas de integración para el bot completo."""

import pytest
import sys
import os

# Añadir directorio padre al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from bot import validar_filtros_cuantitativos


class TestIntegracionBot:
    
    def test_validar_filtros_cuantitativos(self):
        # Crear datos de prueba
        df = pd.DataFrame({
            'high': [100, 105, 110, 115, 120],
            'low': [95, 100, 105, 110, 115],
            'close': [98, 103, 108, 113, 118],
            'atr': [5, 6, 7, 8, 9]
        })
        
        df_mtf = pd.DataFrame({
            'close': [90, 95, 100, 105, 110, 115, 120]
        })
        
        # Mock de filtros (simplificado para prueba)
        adx_valido, ema_mtf_valido, horario_valido = validar_filtros_cuantitativos(df, df_mtf)
        
        # Verificar que retorna tupla booleana
        assert isinstance(adx_valido, bool)
        assert isinstance(ema_mtf_valido, bool)
        assert isinstance(horario_valido, bool)


class TestMetricasCalidad:
    
    def test_profit_factor_calculation(self):
        from logic import calcular_profit_factor
        
        # Caso positivo
        assert calcular_profit_factor(1000, 500) == 2.0
        
        # Caso break-even
        assert calcular_profit_factor(1000, 1000) == 1.0
        
        # Caso pérdidas
        assert calcular_profit_factor(500, 1000) == 0.5
        
        # Caso sin pérdidas
        assert calcular_profit_factor(1000, 0) == float('inf')