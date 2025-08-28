"""
Config loader module for loading configuration from YAML files.
"""
import yaml
from typing import Dict, Any
import os
from .config import Config, StorageConfig, NormalizationConfig

def load_config_from_yaml(file_path: str = None) -> Config:
    """
    Load configuration from a YAML file.
    
    Args:
        file_path (str): Path to the YAML config file. If None, uses default path.
        
    Returns:
        Config: The loaded configuration object
    """
    if file_path is None:
        # Asciende un nivel para llegar a la raíz de descarga_datos y luego entra en config
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        file_path = os.path.join(base_dir, "config", "config.yaml")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Config file not found: {file_path}")
    
    with open(file_path, 'r') as f:
        config_data = yaml.safe_load(f)
    
    # Extraer configuraciones anidadas
    storage_config_data = config_data.pop('storage', {})
    normalization_config_data = config_data.pop('normalization', {})
    
    # Crear instancias de las clases de configuración anidadas
    storage_config = StorageConfig(**storage_config_data)
    normalization_config = NormalizationConfig(**normalization_config_data)
    
    # Crear la instancia de Config principal
    return Config(
        storage=storage_config,
        normalization=normalization_config,
        **config_data
    )

def save_config_to_yaml(config: Config, file_path: str = None):
    """
    Save configuration to a YAML file.
    
    Args:
        config (Config): Configuration object to save
        file_path (str): Path to save the YAML file. If None, uses default path.
    """
    if file_path is None:
        # Asciende un nivel para llegar a la raíz de descarga_datos y luego entra en config
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        file_path = os.path.join(base_dir, "config", "config.yaml")

    config_dict = {
        'active_exchange': config.active_exchange,
        'exchanges': config.exchanges,
        'default_symbols': config.default_symbols,
        'data_types': config.data_types,
        'max_retries': config.max_retries,
        'retry_delay': config.retry_delay,
        'log_level': config.log_level,
        'log_file': config.log_file,
        'timeframe': config.timeframe,
        'storage': {
            'path': config.storage.path,
            'csv': config.storage.csv,
            'sqlite': config.storage.sqlite
        },
        'normalization': {
            'enabled': config.normalization.enabled,
            'method': config.normalization.method
        }
    }
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, 'w') as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)