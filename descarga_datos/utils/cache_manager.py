"""
Sistema de caché para datos frecuentemente accedidos.
"""
import pandas as pd
from typing import Optional, Dict, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import json
import logging

class CacheManager:
    """
    Gestiona el caché de datos frecuentemente accedidos.
    
    Características:
    - Caché en memoria y disco
    - Políticas de expiración configurables
    - Validación de frescura de datos
    """
    
    def __init__(self, cache_dir: str = "cache", max_age: timedelta = timedelta(hours=1)):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_age = max_age
        self.memory_cache: Dict[str, Tuple[pd.DataFrame, datetime]] = {}
        self.logger = logging.getLogger(__name__)
        
    def get_cache_key(self, exchange: str, symbol: str, timeframe: str) -> str:
        """Genera una clave única para el caché."""
        return f"{exchange}_{symbol.replace('/', '_')}_{timeframe}"
    
    def get_from_cache(self, exchange: str, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """
        Intenta obtener datos del caché.
        
        Returns:
            DataFrame si está en caché y válido, None si no existe o expiró
        """
        cache_key = self.get_cache_key(exchange, symbol, timeframe)
        
        # Intentar obtener de la memoria
        if cache_key in self.memory_cache:
            data, timestamp = self.memory_cache[cache_key]
            if datetime.now() - timestamp <= self.max_age:
                self.logger.debug(f"Cache hit (memory): {cache_key}")
                return data
            else:
                del self.memory_cache[cache_key]
        
        # Intentar obtener del disco
        cache_file = self.cache_dir / f"{cache_key}.parquet"
        if cache_file.exists():
            metadata_file = self.cache_dir / f"{cache_key}.meta"
            if metadata_file.exists():
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    cached_time = datetime.fromisoformat(metadata['timestamp'])
                    
                    if datetime.now() - cached_time <= self.max_age:
                        self.logger.debug(f"Cache hit (disk): {cache_key}")
                        data = pd.read_parquet(cache_file)
                        # Actualizar caché en memoria
                        self.memory_cache[cache_key] = (data, cached_time)
                        return data
                    
        return None
    
    def save_to_cache(self, data: pd.DataFrame, exchange: str, symbol: str, timeframe: str):
        """Guarda datos en el caché."""
        cache_key = self.get_cache_key(exchange, symbol, timeframe)
        current_time = datetime.now()
        
        # Guardar en memoria
        self.memory_cache[cache_key] = (data, current_time)
        
        # Guardar en disco
        cache_file = self.cache_dir / f"{cache_key}.parquet"
        metadata_file = self.cache_dir / f"{cache_key}.meta"
        
        data.to_parquet(cache_file, index=False)
        with open(metadata_file, 'w') as f:
            json.dump({
                'timestamp': current_time.isoformat(),
                'rows': len(data),
                'columns': list(data.columns)
            }, f, indent=2)
        
        self.logger.debug(f"Datos guardados en caché: {cache_key}")
    
    def invalidate_cache(self, exchange: str, symbol: str, timeframe: str):
        """Invalida una entrada específica del caché."""
        cache_key = self.get_cache_key(exchange, symbol, timeframe)
        
        # Limpiar memoria
        if cache_key in self.memory_cache:
            del self.memory_cache[cache_key]
        
        # Limpiar disco
        cache_file = self.cache_dir / f"{cache_key}.parquet"
        metadata_file = self.cache_dir / f"{cache_key}.meta"
        
        if cache_file.exists():
            cache_file.unlink()
        if metadata_file.exists():
            metadata_file.unlink()
            
        self.logger.debug(f"Caché invalidado: {cache_key}")
    
    def clear_expired(self):
        """Limpia todas las entradas expiradas del caché."""
        # Limpiar memoria
        current_time = datetime.now()
        expired_keys = [
            key for key, (_, timestamp) in self.memory_cache.items()
            if current_time - timestamp > self.max_age
        ]
        for key in expired_keys:
            del self.memory_cache[key]
        
        # Limpiar disco
        for metadata_file in self.cache_dir.glob("*.meta"):
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
                cached_time = datetime.fromisoformat(metadata['timestamp'])
                if current_time - cached_time > self.max_age:
                    cache_file = self.cache_dir / f"{metadata_file.stem}.parquet"
                    if cache_file.exists():
                        cache_file.unlink()
                    metadata_file.unlink()
        
        self.logger.info(f"Limpiadas {len(expired_keys)} entradas expiradas del caché")
