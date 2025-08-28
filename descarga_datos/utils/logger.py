#!/usr/bin/env python3
"""
Módulo de logging centralizado para el data downloader.
Configura el logging basado en la configuración proporcionada.
"""
import logging
import os
from typing import Optional
from ..config.config import Config

def setup_logging(config: Config) -> None:
    """
    Configura el sistema de logging basado en la configuración.
    
    Args:
        config: Instancia de Config con parámetros de logging.
    """
    # Crear directorio de logs si no existe
    log_dir = os.path.dirname(config.log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Configurar el nivel de logging
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)
    
    # Configurar el formato del log
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Handler para archivo
    file_handler = logging.FileHandler(config.log_file)
    file_handler.setFormatter(formatter)
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Configurar el logger raíz
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Reducir el nivel de logging para librerías externas si es necesario
    logging.getLogger('ccxt').setLevel(logging.WARNING)

def get_logger(name: str) -> logging.Logger:
    """
    Obtiene un logger con el nombre especificado.
    
    Args:
        name: Nombre del logger, usualmente __name__ del módulo.
        
    Returns:
        Instancia de Logger configurada.
    """
    return logging.getLogger(name)

def close_logging() -> None:
    """
    Cierra todos los handlers de logging para liberar archivos.
    """
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        handler.close()
        root_logger.removeHandler(handler)