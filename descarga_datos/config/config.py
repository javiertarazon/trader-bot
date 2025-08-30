import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

@dataclass
class StorageConfig:
    path: str = "data"
    csv: Dict[str, bool] = field(default_factory=lambda: {'enabled': True})
    sqlite: Dict[str, bool] = field(default_factory=lambda: {'enabled': True})

@dataclass
class NormalizationConfig:
    enabled: bool = True
    method: str = "minmax"
    feature_range: tuple = (0, 1)
    with_mean: bool = True
    with_std: bool = True
    quantile_range: tuple = (25.0, 75.0)

@dataclass
class IndicatorConfig:
    volatility: Dict[str, any] = field(default_factory=lambda: {
        'enabled': True,
        'period': 14,
        'method': 'standard_deviation'
    })
    heiken_ashi: Dict[str, any] = field(default_factory=lambda: {
        'enabled': True,
        'trend_period': 3,
        'size_comparison_threshold': 1.2
    })
    atr: Dict[str, any] = field(default_factory=lambda: {
        'enabled': True,
        'period': 14
    })
    adx: Dict[str, any] = field(default_factory=lambda: {
        'enabled': True,
        'period': 14,
        'threshold': 25
    })
    ema: Dict[str, any] = field(default_factory=lambda: {
        'enabled': True,
        'periods': [10, 20, 200]
    })
    parabolic_sar: Dict[str, any] = field(default_factory=lambda: {
        'enabled': True,
        'acceleration': 0.02,
        'maximum': 0.2
    })
    normalize_output: bool = True

@dataclass
class Config:
    active_exchange: str = "bybit"
    exchanges: Dict[str, Dict[str, str]] = None
    default_symbols: List[str] = None
    data_types: List[str] = None
    max_retries: int = 3
    retry_delay: int = 5
    log_level: str = "INFO"
    log_file: str = "data_downloader.log"
    storage: StorageConfig = field(default_factory=StorageConfig)
    normalization: NormalizationConfig = field(default_factory=NormalizationConfig)
    indicators: IndicatorConfig = field(default_factory=IndicatorConfig)
    timeframe: str = "1d"

    def __post_init__(self):
        if self.exchanges is None:
            self.exchanges = {}
        if self.default_symbols is None:
            self.default_symbols = ["BTC/USDT", "ETH/USDT"]
        if self.data_types is None:
            self.data_types = ["ohlcv"]