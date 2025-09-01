"""
Sistema de validación de datos y detección de anomalías.
"""
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from dataclasses import dataclass
import logging

@dataclass
class ValidationResult:
    """Resultado de una validación de datos."""
    passed: bool
    errors: List[str]
    warnings: List[str]
    stats: Dict[str, Any]

class DataValidator:
    """
    Validador de datos con detección de anomalías.
    
    Características:
    - Verificación de continuidad temporal
    - Detección de valores atípicos
    - Validación de rangos
    - Detección de gaps
    - Verificación de integridad
    """
    
    def __init__(self, config: Optional[dict] = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        
    def validate_ohlcv_data(self, df: pd.DataFrame) -> ValidationResult:
        """
        Valida un DataFrame de datos OHLCV.
        
        Args:
            df: DataFrame con columnas timestamp, open, high, low, close, volume
            
        Returns:
            ValidationResult con el resultado de la validación
        """
        errors = []
        warnings = []
        stats = {}
        
        # Verificar columnas requeridas
        required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            errors.append(f"Columnas faltantes: {', '.join(missing_columns)}")
            return ValidationResult(False, errors, warnings, stats)
        
        # Verificar valores nulos
        null_counts = df[required_columns].isnull().sum()
        if null_counts.any():
            for col in null_counts[null_counts > 0].index:
                errors.append(f"Valores nulos encontrados en columna {col}: {null_counts[col]}")
        
        # Verificar orden de precios
        invalid_prices = df[df.high < df.low].index
        if len(invalid_prices) > 0:
            errors.append(f"Precios high < low encontrados en {len(invalid_prices)} filas")
        
        # Verificar continuidad temporal
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        timeframe = getattr(self.config, 'timeframe', '1h') if self.config else '1h'
        gaps = self._check_time_continuity(df, timeframe)
        if gaps:
            warnings.extend(gaps)
        
        # Detectar valores atípicos
        outliers = self._detect_outliers(df)
        if outliers:
            warnings.extend(outliers)
        
        # Calcular estadísticas
        stats = self._calculate_statistics(df)
        
        return ValidationResult(
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            stats=stats
        )
    
    def _check_time_continuity(self, df: pd.DataFrame, timeframe: str = '1h') -> List[str]:
        """
        Verifica la continuidad temporal de los datos.
        
        Args:
            df: DataFrame con columna timestamp
            timeframe: Periodo de tiempo entre cada vela ('1h', '1d', etc.)
        """
        warnings = []
        
        # Asegurar que timestamp es datetime
        if isinstance(df['timestamp'].iloc[0], (int, float)):
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # Ordenar por timestamp
        df = df.sort_values('timestamp')
        
        # Determinar el intervalo esperado según el timeframe
        if timeframe.endswith('h'):
            expected_interval = pd.Timedelta(hours=int(timeframe[:-1]))
        elif timeframe.endswith('d'):
            expected_interval = pd.Timedelta(days=int(timeframe[:-1]))
        else:
            expected_interval = pd.Timedelta(hours=1)  # default a 1h
        
        # Calcular diferencias de tiempo
        time_diff = df['timestamp'].diff()
        
        # Calcular tolerancia (1% del intervalo esperado)
        tolerance = expected_interval * 0.01
        
        # Detectar gaps (diferencias fuera del rango esperado ± tolerancia)
        gaps = time_diff[
            (time_diff > expected_interval + tolerance) | 
            (time_diff < expected_interval - tolerance)
        ]
        
        if not gaps.empty:
            for idx in gaps.index:
                gap_size = gaps[idx]
                warnings.append(
                    f"Gap temporal detectado: {gap_size} entre "
                    f"{df['timestamp'][idx - 1]} y {df['timestamp'][idx]}"
                )
        
        return warnings
    
    def _detect_outliers(self, df: pd.DataFrame) -> List[str]:
        """Detecta valores atípicos en los datos."""
        warnings = []
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            # Calcular estadísticas
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            # Definir límites
            lower_bound = Q1 - 3 * IQR
            upper_bound = Q3 + 3 * IQR
            
            # Encontrar outliers
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
            
            if len(outliers) > 0:
                warnings.append(
                    f"Valores atípicos detectados en {col}: {len(outliers)} valores "
                    f"fuera del rango [{lower_bound:.2f}, {upper_bound:.2f}]"
                )
        
        return warnings
    
    def _calculate_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calcula estadísticas descriptivas de los datos."""
        # Convertir timestamps si están en milisegundos
        if 'timestamp' in df.columns:
            if isinstance(df['timestamp'].iloc[0], (int, float)):
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        stats = {
            'row_count': len(df),
            'time_range': {
                'start': df['timestamp'].min().strftime('%Y-%m-%d %H:%M:%S'),
                'end': df['timestamp'].max().strftime('%Y-%m-%d %H:%M:%S'),
                'duration_hours': (df['timestamp'].max() - df['timestamp'].min()).total_seconds() / 3600
            },
            'price_stats': {
                'min_price': df['low'].min(),
                'max_price': df['high'].max(),
                'avg_price': df['close'].mean(),
                'price_volatility': df['close'].std() / df['close'].mean()
            },
            'volume_stats': {
                'total_volume': df['volume'].sum(),
                'avg_volume': df['volume'].mean(),
                'volume_volatility': df['volume'].std() / df['volume'].mean()
            }
        }
        
        return stats
