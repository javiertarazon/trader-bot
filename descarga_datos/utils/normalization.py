"""
Data normalization module for machine learning preprocessing.
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
import logging
from ..config.config import NormalizationConfig

logger = logging.getLogger(__name__)


class DataNormalizer:
    """
    Class for normalizing and scaling financial data for machine learning.
    """
    
    def __init__(self, config: NormalizationConfig = None):
        """
        Initialize the data normalizer.
        
        Args:
            config (NormalizationConfig): Configuration for scaling
        """
        self.config = config or NormalizationConfig()
        self.scalers: Dict[str, any] = {}
        self.feature_names: List[str] = []
        self.is_fitted = False
    
    def fit(self, data: pd.DataFrame, features: Optional[List[str]] = None):
        """
        Fit the scaler to the data.
        
        Args:
            data (pd.DataFrame): Input data to fit
            features (List[str]): List of features to scale. If None, uses all numeric columns.
        """
        if features is None:
            features = data.select_dtypes(include=[np.number]).columns.tolist()
        
        self.feature_names = features
        
        for feature in features:
            if feature not in data.columns:
                logger.warning(f"Feature '{feature}' not found in data, skipping")
                continue
            
            feature_data = data[feature].dropna().values.reshape(-1, 1)
            
            if self.config.method == "minmax":
                scaler = MinMaxScaler(feature_range=self.config.feature_range)
            elif self.config.method == "standard":
                scaler = StandardScaler(with_mean=self.config.with_mean, with_std=self.config.with_std)
            elif self.config.method == "robust":
                scaler = RobustScaler(quantile_range=self.config.quantile_range)
            else:
                raise ValueError(f"Unknown scaling method: {self.config.method}")
            
            scaler.fit(feature_data)
            self.scalers[feature] = scaler
        
        self.is_fitted = True
        logger.info(f"Fitted scaler for features: {features}")
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the data using fitted scalers.
        
        Args:
            data (pd.DataFrame): Data to transform
            
        Returns:
            pd.DataFrame: Transformed data
        """
        if not self.is_fitted:
            raise RuntimeError("Scaler must be fitted before transformation")
        
        transformed_data = data.copy()
        
        for feature, scaler in self.scalers.items():
            if feature not in data.columns:
                logger.warning(f"Feature '{feature}' not found in data, skipping transformation")
                continue
            
            # Handle missing values
            feature_data = data[feature].values.reshape(-1, 1)
            mask = ~np.isnan(feature_data.flatten())
            
            if np.any(mask):
                transformed_values = np.full_like(feature_data, np.nan)
                transformed_values[mask] = scaler.transform(feature_data[mask])
                transformed_data[feature] = transformed_values.flatten()
        
        return transformed_data
    
    def fit_transform(self, data: pd.DataFrame, features: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Fit and transform the data.
        
        Args:
            data (pd.DataFrame): Data to fit and transform
            features (List[str]): Features to scale
            
        Returns:
            pd.DataFrame: Transformed data
        """
        self.fit(data, features)
        return self.transform(data)
    
    def inverse_transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Inverse transform the data.
        
        Args:
            data (pd.DataFrame): Transformed data to invert
            
        Returns:
            pd.DataFrame: Original scale data
        """
        if not self.is_fitted:
            raise RuntimeError("Scaler must be fitted before inverse transformation")
        
        original_data = data.copy()
        
        for feature, scaler in self.scalers.items():
            if feature not in data.columns:
                continue
            
            # Handle missing values
            feature_data = data[feature].values.reshape(-1, 1)
            mask = ~np.isnan(feature_data.flatten())
            
            if np.any(mask):
                original_values = np.full_like(feature_data, np.nan)
                original_values[mask] = scaler.inverse_transform(feature_data[mask])
                original_data[feature] = original_values.flatten()
        
        return original_data


def normalize_ohlcv_data(df: pd.DataFrame, method: str = "minmax") -> pd.DataFrame:
    """
    Normalize OHLCV data for machine learning.
    
    Args:
        df (pd.DataFrame): OHLCV data with columns ['open', 'high', 'low', 'close', 'volume']
        method (str): Scaling method
        
    Returns:
        pd.DataFrame: Normalized data
    """
    ohlcv_columns = ['open', 'high', 'low', 'close', 'volume']
    
    # Check if all required columns are present
    missing_columns = [col for col in ohlcv_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing OHLCV columns: {missing_columns}")
    
    config = ScalerConfig(method=method)
    normalizer = DataNormalizer(config)
    
    return normalizer.fit_transform(df, ohlcv_columns)


def create_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create technical indicators from OHLCV data.
    
    Args:
        df (pd.DataFrame): OHLCV data
        
    Returns:
        pd.DataFrame: Data with technical indicators
    """
    data = df.copy()
    
    # Simple Moving Averages
    data['sma_20'] = data['close'].rolling(window=20).mean()
    data['sma_50'] = data['close'].rolling(window=50).mean()
    
    # Exponential Moving Averages
    data['ema_12'] = data['close'].ewm(span=12).mean()
    data['ema_26'] = data['close'].ewm(span=26).mean()
    
    # MACD
    data['macd'] = data['ema_12'] - data['ema_26']
    data['macd_signal'] = data['macd'].ewm(span=9).mean()
    
    # RSI
    delta = data['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    data['rsi'] = 100 - (100 / (1 + rs))
    
    # Bollinger Bands
    data['bb_middle'] = data['close'].rolling(window=20).mean()
    bb_std = data['close'].rolling(window=20).std()
    data['bb_upper'] = data['bb_middle'] + (bb_std * 2)
    data['bb_lower'] = data['bb_middle'] - (bb_std * 2)
    
    # Volume indicators
    data['volume_sma_20'] = data['volume'].rolling(window=20).mean()
    
    return data