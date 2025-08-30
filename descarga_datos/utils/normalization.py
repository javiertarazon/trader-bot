"""
Data normalization module for machine learning preprocessing.
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
import logging
from descarga_datos.config.config import NormalizationConfig

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