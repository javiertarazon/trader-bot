"""
Módulo de indicadores técnicos para análisis de mercado.

Incluye:
- Volatilidad del mercado
- Velas Heiken Ashi
- Tendencia de velas Heiken Ashi
- Tamaño de vela Heiken Ashi comparativo
- ATR (Average True Range)
- ADX (Average Directional Index)
- EMAs de 10, 20 y 200 períodos
- SAR (Parabolic SAR)
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Tuple
import logging
from dataclasses import dataclass
import os
import numpy as np
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Any

# Import existing storage utilities
from descarga_datos.utils.storage import save_to_csv, save_to_sqlite
from descarga_datos.utils.normalization import DataNormalizer
import talib


@dataclass
class IndicatorConfig:
    """Configuración para los parámetros de los indicadores."""
    # Parámetros ATR
    atr_period: int = 14
    
    # Parámetros ADX
    adx_period: int = 14
    
    # Parámetros EMA
    ema_fast_period: int = 10
    ema_medium_period: int = 20
    ema_slow_period: int = 200
    
    # Parámetros SAR
    sar_acceleration: float = 0.02
    sar_maximum: float = 0.2
    
    # Parámetros de volatilidad
    volatility_period: int = 20
    
    # Parámetros Heiken Ashi
    ha_trend_period: int = 3


class TechnicalIndicators:
    """Clase principal para el cálculo de indicadores técnicos."""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.normalizer = DataNormalizer()
        
        # Extraer parámetros de configuración usando la estructura correcta
        self.volatility_period = config.indicators.volatility.get('period', 14)
        self.ha_trend_period = config.indicators.heiken_ashi.get('trend_period', 3)
        self.ha_size_threshold = config.indicators.heiken_ashi.get('size_comparison_threshold', 1.2)
        self.atr_period = config.indicators.atr.get('period', 14)
        self.adx_period = config.indicators.adx.get('period', 14)
        self.adx_threshold = config.indicators.adx.get('threshold', 25)
        self.ema_periods = config.indicators.ema.get('periods', [10, 20, 200])
        self.sar_acceleration = config.indicators.parabolic_sar.get('acceleration', 0.02)
        self.sar_maximum = config.indicators.parabolic_sar.get('maximum', 0.2)
    
    def calculate_volatility(self, df: pd.DataFrame) -> pd.Series:
        """Calculate market volatility using standard deviation of returns."""
        try:
            returns = df['close'].pct_change()
            volatility = returns.rolling(window=self.volatility_period).std()
            return volatility.fillna(0)
        except Exception as e:
            self.logger.error(f"Error calculating volatility: {e}")
            return pd.Series([0] * len(df))
    
    def calculate_heiken_ashi(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula las velas Heiken Ashi y su tendencia.
        
        Args:
            df: DataFrame con columnas 'open', 'high', 'low', 'close'
            
        Returns:
            DataFrame con columnas Heiken Ashi y tendencia
        """
        try:
            ha_df = pd.DataFrame(index=df.index)
            
            # Cálculo de Heiken Ashi
            ha_df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
            ha_df['ha_open'] = (df['open'].shift(1) + df['close'].shift(1)) / 2
            ha_df['ha_open'].iloc[0] = (df['open'].iloc[0] + df['close'].iloc[0]) / 2
            
            # Recálculo de open para mantener la fórmula correcta
            for i in range(1, len(ha_df)):
                ha_df['ha_open'].iloc[i] = (ha_df['ha_open'].iloc[i-1] + ha_df['ha_close'].iloc[i-1]) / 2
            
            ha_df['ha_high'] = pd.concat([df['high'], ha_df['ha_open'], ha_df['ha_close']], axis=1).max(axis=1)
            ha_df['ha_low'] = pd.concat([df['low'], ha_df['ha_open'], ha_df['ha_close']], axis=1).min(axis=1)
            
            # Cálculo de tendencia basada en la dirección de la vela
            ha_df['ha_trend'] = np.where(ha_df['ha_close'] > ha_df['ha_open'], 1, -1)
            
            # Cálculo de fuerza de tendencia sobre período
            trend_period = self.ha_trend_period
            ha_df['trend_strength'] = ha_df['ha_trend'].rolling(window=trend_period).sum()
            
            return ha_df
        except Exception as e:
            self.logger.error(f"Error calculando Heiken Ashi: {e}")
            return pd.DataFrame(index=df.index)
    
    def calculate_ha_trend(self, ha_df: pd.DataFrame) -> pd.Series:
        """
        Determina la tendencia de las velas Heiken Ashi.
        
        Args:
            ha_df: DataFrame con columnas Heiken Ashi
            
        Returns:
            pd.Series con la tendencia (1=bullish, -1=bearish, 0=neutral)
        """
        try:
            bullish = (ha_df['ha_close'] > ha_df['ha_open']) & (ha_df['ha_close'] > ha_df['ha_open'].shift(1))
            bearish = (ha_df['ha_close'] < ha_df['ha_open']) & (ha_df['ha_close'] < ha_df['ha_open'].shift(1))
            
            trend = pd.Series(0, index=ha_df.index)
            trend[bullish] = 1
            trend[bearish] = -1
            
            return trend
        except Exception as e:
            self.logger.error(f"Error calculando tendencia HA: {e}")
            return pd.Series([0] * len(ha_df))
    
    def calculate_ha_candle_size_comparison(self, ha_df: pd.DataFrame) -> pd.Series:
        """
        Calcula el tamaño de la vela actual comparada con la anterior.
        
        Args:
            ha_df: DataFrame con columnas Heiken Ashi
            
        Returns:
            pd.Series con la relación de tamaño
        """
        try:
            current_size = abs(ha_df['ha_close'] - ha_df['ha_open'])
            previous_size = current_size.shift(1)
            
            ratio = current_size / previous_size
            
            # Marcar cambios significativos de tamaño
            threshold = 1.5  # Umbral configurable
            significant_change = np.where(
                ratio > threshold, 
                'large_increase',
                np.where(
                    ratio < (1/threshold),
                    'large_decrease',
                    'normal'
                )
            )
            
            return pd.Series(significant_change, index=ha_df.index)
        except Exception as e:
            self.logger.error(f"Error calculando comparación de velas HA: {e}")
            return pd.Series(['normal'] * len(ha_df))
    
    def calculate_atr(self, df: pd.DataFrame) -> pd.Series:
        """Calculate Average True Range (ATR)."""
        try:
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift(1))
            low_close = np.abs(df['low'] - df['close'].shift(1))
            
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            
            atr = tr.ewm(span=self.atr_period, adjust=False).mean()
            
            return atr.fillna(0)
        except Exception as e:
            self.logger.error(f"Error calculating ATR: {e}")
            return pd.Series([0] * len(df))
    
    def calculate_adx(self, df: pd.DataFrame) -> pd.Series:
        """
        Calcula el Average Directional Index (ADX).
        """
        try:
            # Calculate directional movement
            high_diff = df['high'].diff()
            low_diff = df['low'].diff()
            
            plus_dm = np.where((high_diff > low_diff) & (high_diff > 0), high_diff, 0)
            minus_dm = np.where((low_diff > high_diff) & (low_diff > 0), low_diff, 0)
            
            # Calculate true range
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift(1))
            low_close = np.abs(df['low'] - df['close'].shift(1))
            
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            
            # Calculate directional indicators using EMA for smoother results
            period = self.adx_period
            atr = true_range.ewm(span=period, adjust=False).mean()
            plus_di = 100 * (pd.Series(plus_dm).ewm(span=period, adjust=False).mean() / atr)
            minus_di = 100 * (pd.Series(minus_dm).ewm(span=period, adjust=False).mean() / atr)
            
            # Calculate ADX
            dx = 100 * np.abs((plus_di - minus_di) / ((plus_di + minus_di) + 1e-9))
            adx = dx.ewm(span=period, adjust=False).mean()
            
            return adx.fillna(0)
        except Exception as e:
            self.logger.error(f"Error calculating ADX: {e}")
            return pd.Series([0] * len(df))
    
    def calculate_emas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Exponential Moving Averages (EMAs)."""
        try:
            ema_df = pd.DataFrame(index=df.index)
            
            for period in self.ema_periods:
                ema_df[f'ema_{period}'] = df['close'].ewm(span=period, adjust=False).mean()
            
            return ema_df.fillna(0)
        except Exception as e:
            self.logger.error(f"Error calculating EMAs: {e}")
            return pd.DataFrame(index=df.index)
    
    def calculate_sar(self, df: pd.DataFrame) -> pd.Series:
        """
        Calcula el Parabolic SAR.
        """
        try:
            # Asegurar que los datos sean numéricos y válidos
            high = pd.to_numeric(df['high'], errors='coerce').astype(float)
            low = pd.to_numeric(df['low'], errors='coerce').astype(float)
            
            # Verificar que hay suficientes datos
            if len(high) < 2 or len(low) < 2:
                self.logger.warning("Datos insuficientes para calcular SAR")
                return pd.Series([0.0] * len(df), index=df.index)
            
            # Calcular SAR con TA-Lib
            acceleration = float(self.sar_acceleration)
            maximum = float(self.sar_maximum)
            
            sar_values = talib.SAR(
                high.values, 
                low.values, 
                acceleration=acceleration, 
                maximum=maximum
            )
            
            # Convertir a Series con el índice correcto
            sar_series = pd.Series(sar_values, index=df.index)
            
            # Reemplazar NaN iniciales con el primer valor válido o 0
            if sar_series.isna().all():
                # Si todos son NaN, usar 0
                sar_series = pd.Series([0.0] * len(df), index=df.index)
            else:
                # Forward fill para NaN iniciales, luego fillna(0) para cualquier restante
                sar_series = sar_series.fillna(method='ffill').fillna(0.0)
            
            return sar_series
            
        except Exception as e:
            self.logger.error(f"Error calculating SAR: {e}")
            return pd.Series([0.0] * len(df), index=df.index)
    
    def normalize_sar(self, sar_values: pd.Series, df: pd.DataFrame) -> pd.Series:
        """
        Normaliza los valores del SAR de una manera específica que preserva su significado.
        
        Args:
            sar_values: Serie con los valores SAR
            df: DataFrame con datos OHLCV para referencia
            
        Returns:
            pd.Series: Valores SAR normalizados
        """
        try:
            # Obtener el rango de precios para el período
            price_range = df['high'].max() - df['low'].min()
            price_min = df['low'].min()
            
            if price_range == 0:
                return pd.Series([0.0] * len(df), index=df.index)
            
            # Normalizar SAR relativo al rango de precios
            normalized_sar = (sar_values - price_min) / price_range
            
            # Asegurar que los valores estén en el rango [0, 1]
            normalized_sar = normalized_sar.clip(0, 1)
            
            return normalized_sar
            
        except Exception as e:
            self.logger.error(f"Error normalizando SAR: {e}")
            return pd.Series([0.0] * len(df), index=df.index)
    
    def calculate_all_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula todos los indicadores técnicos y retorna un DataFrame consolidado.
        
        Args:
            df: DataFrame con datos OHLCV
            
        Returns:
            DataFrame con todos los indicadores calculados
        """
        try:
            result_df = pd.DataFrame(index=df.index)
            
            # Volatilidad
            result_df['volatility'] = self.calculate_volatility(df)
            
            # Heiken Ashi
            ha_df = self.calculate_heiken_ashi(df)
            result_df['ha_close'] = ha_df['ha_close']
            result_df['ha_open'] = ha_df['ha_open']
            result_df['ha_high'] = ha_df['ha_high']
            result_df['ha_low'] = ha_df['ha_low']
            result_df['ha_trend'] = self.calculate_ha_trend(ha_df)
            result_df['ha_candle_size_ratio'] = self.calculate_ha_candle_size_comparison(ha_df)
            
            # ATR y ADX
            result_df['atr'] = self.calculate_atr(df)
            result_df['adx'] = self.calculate_adx(df)
            
            # EMAs
            emas_df = self.calculate_emas(df)
            result_df = pd.concat([result_df, emas_df], axis=1)
            
            # SAR - Calculado y normalizado de forma especial
            sar_values = self.calculate_sar(df)
            result_df['sar'] = self.normalize_sar(sar_values, df)
            
            return result_df
        except Exception as e:
            self.logger.error(f"Error calculando todos los indicadores: {e}")
            return pd.DataFrame(index=df.index)

    

    

    def normalize_indicators(self, df: pd.DataFrame, method: str = "minmax") -> pd.DataFrame:
        """
        Normalize technical indicators using the specified method.
        
        Args:
            df (pd.DataFrame): DataFrame with calculated indicators
            method (str): Normalization method ('minmax', 'standard', 'robust')
            
        Returns:
            pd.DataFrame: Normalized indicators
        """
        # Identify indicator columns (exclude OHLCV)
        ohlcv_cols = ['open', 'high', 'low', 'close', 'volume']
        indicator_cols = [col for col in df.columns if col not in ohlcv_cols]
        
        if not indicator_cols:
            self.logger.warning("No indicators found to normalize")
            return df
        
        # Create a copy and normalize only indicator columns
        normalized_df = df.copy()
        
        # Create normalizer with specified method
        from ..config.config import NormalizationConfig
        config = NormalizationConfig(method=method)
        normalizer = DataNormalizer(config)
        
        # Fit and transform indicator columns
        indicator_data = df[indicator_cols]
        normalized_indicators = normalizer.fit_transform(indicator_data)
        
        # Update the DataFrame with normalized values
        normalized_df[indicator_cols] = normalized_indicators
        
        return normalized_df

    def save_normalized_indicators_to_csv(self, df: pd.DataFrame, exchange: str, symbol: str, timeframe: str, 
                                        output_dir: str = "data/csv", method: str = "minmax") -> bool:
        """
        Save normalized indicators to CSV file.
        
        Args:
            df (pd.DataFrame): Data with indicators
            exchange (str): Exchange name
            symbol (str): Trading symbol
            timeframe (str): Timeframe (e.g., '1h', '4h', '1d')
            output_dir (str): Directory to save CSV files
            method (str): Normalization method
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Calculate all indicators
            indicators_df = self.calculate_all_indicators(df)
            
            # Normalize indicators
            normalized_df = self.normalize_indicators(indicators_df, method)
            
            # Save normalized data
            filename = f"{exchange}_{symbol}_{timeframe}_indicators_normalized.csv"
            filepath = os.path.join(output_dir, filename)
            return save_to_csv(normalized_df, filepath)
        except Exception as e:
            self.logger.error(f"Error saving normalized indicators to CSV: {e}")
            return False

    def save_normalized_indicators_to_sqlite(self, df: pd.DataFrame, exchange: str, symbol: str, timeframe: str, 
                                           db_path: str = "data/indicators.db", method: str = "minmax") -> bool:
        """
        Save normalized indicators to SQLite database.
        
        Args:
            df (pd.DataFrame): Data with indicators
            exchange (str): Exchange name
            symbol (str): Trading symbol
            timeframe (str): Timeframe (e.g., '1h', '4h', '1d')
            db_path (str): Path to SQLite database
            method (str): Normalization method
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Calculate all indicators
            indicators_df = self.calculate_all_indicators(df)
            
            # Normalize indicators
            normalized_df = self.normalize_indicators(indicators_df, method)
            
            # Save normalized data
            table_name = f"{exchange}_{symbol}_{timeframe}_indicators_normalized"
            return save_to_sqlite(normalized_df, table_name, db_path)
        except Exception as e:
            self.logger.error(f"Error saving normalized indicators to SQLite: {e}")
            return False