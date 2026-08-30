"""Pruebas unitarias para los filtros cuantitativos y indicadores técnicos."""

import pandas as pd
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logic import (
    ResultadoFiltros,
    calcular_adx,
    calcular_atr,
    calcular_ema,
    calcular_rsi,
    calcular_sma,
    calcular_sma_atr,
    validar_adx,
    validar_ema_200_mtf,
    validar_filtros_cuantitativos,
    validar_horario_mercado,
    validar_volatilidad_relativa,
)


class TestIndicadoresTecnicos:

    def test_calcular_sma(self) -> None:
        precios = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0])
        sma = calcular_sma(precios, period=3)
        assert len(sma) == 5
        assert sma.iloc[-1] == 40.0  # (30+40+50)/3

    def test_calcular_sma_datos_insuficientes(self) -> None:
        precios = pd.Series([10.0, 20.0])
        with pytest.raises(ValueError):
            calcular_sma(precios, period=5)

    def test_calcular_ema(self) -> None:
        precios = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0])
        ema = calcular_ema(precios, period=3)
        assert len(ema) == 5
        assert ema.iloc[-1] > ema.iloc[0]

    def test_calcular_ema_datos_insuficientes(self) -> None:
        precios = pd.Series([10.0])
        with pytest.raises(ValueError):
            calcular_ema(precios, period=5)

    def test_calcular_rsi(self) -> None:
        precios = pd.Series([44.0, 44.34, 44.09, 43.61, 44.33, 44.83,
                             45.10, 45.42, 45.84, 46.08, 45.89, 46.03,
                             45.61, 46.28, 46.28, 46.00, 46.03, 46.41,
                             46.22, 45.64])
        rsi = calcular_rsi(precios, period=14)
        assert len(rsi) == 20
        valid_rsi = rsi.dropna()
        assert valid_rsi.min() >= 0
        assert valid_rsi.max() <= 100

    def test_calcular_rsi_datos_insuficientes(self) -> None:
        precios = pd.Series([10.0, 20.0, 30.0])
        with pytest.raises(ValueError):
            calcular_rsi(precios, period=14)

    def test_calcular_atr(self) -> None:
        high = pd.Series([110.0, 115.0, 120.0, 118.0, 122.0])
        low = pd.Series([100.0, 105.0, 110.0, 108.0, 112.0])
        close = pd.Series([105.0, 110.0, 115.0, 112.0, 118.0])
        atr = calcular_atr(high, low, close, period=3)
        assert len(atr) == 5
        assert atr.dropna().min() > 0

    def test_calcular_atr_datos_insuficientes(self) -> None:
        high = pd.Series([110.0, 115.0])
        low = pd.Series([100.0, 105.0])
        close = pd.Series([105.0, 110.0])
        with pytest.raises(ValueError):
            calcular_atr(high, low, close, period=14)

    def test_calcular_adx(self) -> None:
        data = list(range(100, 130))
        high = pd.Series([float(x + 2) for x in data])
        low = pd.Series([float(x - 2) for x in data])
        close = pd.Series([float(x) for x in data])
        adx = calcular_adx(high, low, close, period=14)
        assert len(adx) == len(close)
        valid_adx = adx.dropna()
        assert valid_adx.min() >= 0
        assert valid_adx.max() <= 100

    def test_calcular_adx_datos_insuficientes(self) -> None:
        high = pd.Series([100.0] * 5)
        low = pd.Series([98.0] * 5)
        close = pd.Series([99.0] * 5)
        with pytest.raises(ValueError):
            calcular_adx(high, low, close, period=14)

    def test_calcular_sma_atr(self) -> None:
        atr_series = pd.Series([10.0, 12.0, 14.0, 16.0, 18.0])
        sma_atr = calcular_sma_atr(atr_series, period=3)
        assert len(sma_atr) == 5
        assert sma_atr.iloc[-1] == 16.0  # (14+16+18)/3


class TestFiltrosCuantitativos:

    def test_validar_adx_tendencia(self) -> None:
        high = pd.Series([100 + i for i in range(40)])
        low = pd.Series([95 + i for i in range(40)])
        close = pd.Series([98 + i for i in range(40)])
        result = validar_adx(high, low, close, threshold=25.0)
        assert isinstance(result, bool)

    def test_validar_adx_datos_insuficientes(self) -> None:
        high = pd.Series([100.0] * 5)
        low = pd.Series([98.0] * 5)
        close = pd.Series([99.0] * 5)
        result = validar_adx(high, low, close, threshold=25.0)
        assert result is False

    def test_validar_ema_200_mtf(self, sample_mtf_data: pd.DataFrame) -> None:
        result = validar_ema_200_mtf(sample_mtf_data)
        assert isinstance(result, bool)

    def test_validar_ema_200_mtf_datos_insuficientes(self) -> None:
        df = pd.DataFrame({"close": [100.0] * 10})
        result = validar_ema_200_mtf(df)
        assert result is False

    def test_validar_ema_200_mtf_none(self) -> None:
        result = validar_ema_200_mtf(None)
        assert result is False

    def test_validar_volatilidad_relativa(self, sample_atr_series: pd.Series) -> None:
        result = validar_volatilidad_relativa(sample_atr_series, period=3)
        assert result is True

    def test_validar_volatilidad_relativa_datos_insuficientes(self) -> None:
        atr = pd.Series([5.0])
        result = validar_volatilidad_relativa(atr, period=20)
        assert result is False

    def test_validar_horario_mercado_rango_completo(self) -> None:
        result = validar_horario_mercado(hora_inicio=0, hora_fin=24)
        assert result is True

    def test_validar_horario_mercado_rango_estrecho(self) -> None:
        from datetime import datetime, timezone
        current_hour = datetime.now(timezone.utc).hour
        result = validar_horario_mercado(hora_inicio=current_hour, hora_fin=current_hour + 1)
        assert result is True

    def test_resultado_filtros_todos_validos(self) -> None:
        resultado = ResultadoFiltros(
            adx_valido=True,
            ema_mtf_valido=True,
            horario_valido=True,
            volatilidad_valida=True,
        )
        assert resultado.todos_validos is True

    def test_resultado_filtros_alguno_invalido(self) -> None:
        resultado = ResultadoFiltros(
            adx_valido=True,
            ema_mtf_valido=False,
            horario_valido=True,
            volatilidad_valida=True,
        )
        assert resultado.todos_validos is False

    def test_validar_filtros_cuantitativos_completo(
        self, sample_ohlcv_long: pd.DataFrame, sample_mtf_data: pd.DataFrame
    ) -> None:
        resultado = validar_filtros_cuantitativos(
            high=sample_ohlcv_long["high"],
            low=sample_ohlcv_long["low"],
            close=sample_ohlcv_long["close"],
            df_mtf=sample_mtf_data,
            atr_series=sample_ohlcv_long["atr"] if "atr" in sample_ohlcv_long.columns else None,
        )
        assert isinstance(resultado, ResultadoFiltros)
        assert isinstance(resultado.adx_valido, bool)
        assert isinstance(resultado.ema_mtf_valido, bool)
        assert isinstance(resultado.horario_valido, bool)
        assert isinstance(resultado.volatilidad_valida, bool)
