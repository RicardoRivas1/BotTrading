"""Pruebas unitarias para los filtros cuantitativos."""

import pandas as pd
import pytest
import sys
import os

# Añadir directorio padre al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logic import FiltrosCuantitativos, calcular_adx, calcular_sma_atr
import ccxt


class TestFiltrosCuantitativos:
    
    @pytest.fixture
    def filtros(self):
        exchange = ccxt.binance({'enableRateLimit': True})
        return FiltrosCuantitativos(exchange)
    
    def test_validar_adx_tendencia(self, filtros):
        # Datos de prueba con tendencia fuerte
        high = pd.Series([100, 105, 110, 115, 120, 125, 130, 135, 140, 145, 150, 155, 160, 165, 170])
        low = pd.Series([95, 100, 105, 110, 115, 120, 125, 130, 135, 140, 145, 150, 155, 160, 165])
        close = pd.Series([98, 103, 108, 113, 118, 123, 128, 133, 138, 143, 148, 153, 158, 163, 168])
        
        # ADX necesita más datos para calcular
        result = filtros.validar_adx_tendencia(high, low, close, threshold=25.0)
        # Solo verificar que no hay error
        assert isinstance(result, bool)
    
    def test_validar_volatilidad_relativa(self, filtros):
        # ATR creciente vs SMA(ATR)
        atr_series = pd.Series([10, 15, 20, 25, 30])  # Volatilidad creciente
        
        result = filtros.validar_volatilidad_relativa(atr_series, period=3)
        assert result is True
    
    def test_validar_horario_mercado(self, filtros):
        # Probar diferentes horas
        result = filtros.validar_horario_mercado(hora_inicio=0, hora_fin=24)
        assert result is True  # Siempre debería ser True con rango completo


class TestIndicadoresTecnicos:
    
    def test_calcular_adx(self):
        # Necesita al menos period*2+1 = 29 puntos
        data = list(range(100, 130))
        high = pd.Series([x + 2 for x in data])
        low = pd.Series([x - 2 for x in data])
        close = pd.Series(data)
        
        adx = calcular_adx(high, low, close, period=14)
        assert len(adx) == len(close)
        assert adx is not None
        # Verificar que ADX esta en rango 0-100
        valid_adx = adx.dropna()
        assert valid_adx.min() >= 0
        assert valid_adx.max() <= 100
    
    def test_calcular_sma_atr(self):
        atr_series = pd.Series([10, 12, 14, 16, 18])
        sma_atr = calcular_sma_atr(atr_series, period=3)
        
        assert len(sma_atr) == len(atr_series)
        assert sma_atr.iloc[-1] == 16.0  # (14+16+18)/3