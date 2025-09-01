import ccxt
import time
import asyncio
import os
import numpy as np
from typing import Optional, Dict, Any, List, Tuple
import pandas as pd
import ccxt.async_support as ccxt  # Import async version
from datetime import datetime
from ..config.config import Config
from ..utils.logger import setup_logging, get_logger
from ..utils.storage import save_to_csv, save_to_sqlite
from ..utils.retry_manager import RetryManager, with_retry
from ..utils.monitoring import PerformanceMonitor
from ..utils.data_validator import DataValidator
from ..utils.cache_manager import CacheManager
from datetime import timedelta

class DataDownloader:
    def __init__(self, config: Config):
        self.config = config
        self.exchanges = {}
        setup_logging(config)
        self.logger = get_logger(__name__)
        
        # Inicializar sistemas de soporte
        self.retry_manager = RetryManager(
            max_retries=config.max_retries,
            base_delay=config.retry_delay,
            max_delay=60.0
        )
        self.monitor = PerformanceMonitor(
            metrics_dir=os.path.join(config.storage.path, "metrics")
        )
        self.validator = DataValidator(config)
        self.cache = CacheManager(
            cache_dir=os.path.join(config.storage.path, "cache"),
            max_age=timedelta(minutes=30)  # Configurable según necesidades
        )

    async def setup_exchanges(self):
        """Initialize exchange instances based on the configuration."""
        for ex_name, ex_config in self.config.exchanges.items():
            try:
                exchange_class = getattr(ccxt, ex_name)
                
                # Start with basic config
                ccxt_config = {
                    'enableRateLimit': ex_config.get('enableRateLimit', True),
                }

                # Add API keys only if they are provided and not placeholders
                api_key = ex_config.get('api_key')
                secret = ex_config.get('secret')

                if api_key and api_key != 'your_api_key_here':
                    ccxt_config['apiKey'] = api_key
                
                if secret and secret != 'your_secret_here':
                    ccxt_config['secret'] = secret

                self.exchanges[ex_name] = exchange_class(ccxt_config)
                self.logger.info(f"Initialized {ex_name} exchange.")
            except AttributeError:
                self.logger.error(f"Exchange {ex_name} not found in ccxt.")
            except Exception as e:
                self.logger.error(f"Error initializing {ex_name}: {e}")

    async def close_exchanges(self):
        """Close all active exchange sessions."""
        for exchange in self.exchanges.values():
            if hasattr(exchange, 'close'):
                await exchange.close()
        self.logger.info("All exchange sessions closed.")

    def _get_exchange(self, exchange_name: str):
        """Get an initialized exchange instance."""
        exchange = self.exchanges.get(exchange_name)
        if not exchange:
            raise ValueError(f"Exchange {exchange_name} not initialized.")
        return exchange
        
    async def download_multiple_symbols(self, symbols: List[str], exchange_name: str, 
                                     timeframe: str = '1h', since: Optional[int] = None, 
                                     limit: int = 100, batch_size: int = 5) -> Dict[str, pd.DataFrame]:
        """
        Descarga datos para múltiples símbolos en paralelo.
        
        Args:
            symbols: Lista de símbolos a descargar
            exchange_name: Nombre del exchange
            timeframe: Intervalo de tiempo
            since: Timestamp inicial
            limit: Límite de registros
            batch_size: Número máximo de descargas simultáneas
            
        Returns:
            Dict[str, DataFrame]: Diccionario con los datos por símbolo
        """
        results = {}
        
        # Procesar símbolos en lotes para no sobrecargar la API
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            tasks = []
            
            for symbol in batch:
                task = asyncio.create_task(
                    self.async_download_ohlcv(
                        symbol=symbol,
                        exchange_name=exchange_name,
                        timeframe=timeframe,
                        since=since,
                        limit=limit
                    )
                )
                tasks.append((symbol, task))
            
            # Esperar que se complete el lote actual
            for symbol, task in tasks:
                try:
                    data, stats = await task
                    if data is not None:
                        results[symbol] = data
                        self.logger.info(f"Descarga completada para {symbol}")
                    else:
                        self.logger.warning(f"No se obtuvieron datos para {symbol}")
                except Exception as e:
                    self.logger.error(f"Error descargando {symbol}: {str(e)}")
            
            # Pequeña pausa entre lotes para evitar sobrecarga
            if i + batch_size < len(symbols):
                await asyncio.sleep(1)
        
        return results

    @with_retry()
    async def async_download_ohlcv(self, symbol: str, exchange_name: str, timeframe: str = '1d', 
                                 since: Optional[int] = None, limit: int = 100, 
                                 params: dict = {}, use_cache: bool = True) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        """
        Descarga datos OHLCV con manejo de reintentos, monitoreo y validación.
        
        Returns:
            Tuple[DataFrame, Dict]: DataFrame con datos y diccionario con métricas
        """
        # Iniciar monitoreo de la operación
        operation_id = self.monitor.start_operation(symbol, exchange_name)
        
        try:
            # Intentar obtener datos del caché si está habilitado
            if use_cache:
                cached_data = self.cache.get_from_cache(exchange_name, symbol, timeframe)
                if cached_data is not None:
                    self.logger.info(f"Datos obtenidos del caché para {symbol}")
                    validation_result = self.validator.validate_ohlcv_data(cached_data)
                    return cached_data, validation_result.stats
            
            exchange = self._get_exchange(exchange_name)
            
            # Si no se proporciona since, usar un tiempo por defecto
            if since is None:
                # Calcular timestamp para los últimos períodos
                now = pd.Timestamp.now()
                if timeframe == '1h':
                    # Para datos horarios, obtener las últimas 100 horas
                    since = int((now - pd.Timedelta(hours=limit)).timestamp() * 1000)
                else:
                    # Para otros timeframes, obtener los últimos días
                    since = int((now - pd.Timedelta(days=limit)).timestamp() * 1000)
            
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, since, limit, params)

            if not ohlcv:
                self.monitor.update_metrics(
                    operation_id,
                    errors=["No data received from exchange"]
                )
                return None, {}

            # Crear DataFrame y validar datos
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            validation_result = self.validator.validate_ohlcv_data(df)
            
            # Actualizar métricas con resultados de validación
            self.monitor.update_metrics(
                operation_id,
                rows_downloaded=len(df),
                data_validation_passed=validation_result.passed,
                errors=validation_result.errors,
                stats=validation_result.stats
            )

            if not validation_result.passed:
                self.logger.warning(
                    f"Validación fallida para {symbol} en {exchange_name}: "
                    f"{', '.join(validation_result.errors)}"
                )
            
            # Guardar datos si pasan validación
            if validation_result.passed:
                self._save_data(df, exchange_name, symbol, 'ohlcv', timeframe)
                
                # Guardar en caché si está habilitado
                if use_cache:
                    self.cache.save_to_cache(df, exchange_name, symbol, timeframe)
                
                self.monitor.complete_operation(operation_id, success=True)
                return df, validation_result.stats
            else:
                self.monitor.complete_operation(operation_id, success=False)
                return None, validation_result.stats

        except Exception as e:
            self.monitor.update_metrics(
                operation_id,
                errors=[str(e)]
            )
            self.monitor.complete_operation(operation_id, success=False)
            raise  # RetryManager manejará la excepción

    async def async_download_trades(self, symbol: str, exchange_name: str, since: Optional[int] = None, limit: int = 100, params: dict = {}):
        """Download trade data asynchronously with retries and automatic storage."""
        for attempt in range(self.config.max_retries):
            try:
                exchange = self._get_exchange(exchange_name)
                trades = await exchange.fetch_trades(symbol, since=since, limit=limit, params=params)

                if trades:
                    df = pd.DataFrame(trades)
                    self._save_data(df, exchange_name, symbol, 'trades')
                    return df

                return pd.DataFrame()
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {symbol} on {exchange_name}: {e}")
                await asyncio.sleep(self.config.retry_delay)
        self.logger.error(f"Failed to download trades for {symbol} after {self.config.max_retries} attempts.")
        return None

    def _save_data(self, data: pd.DataFrame, exchange_name: str, symbol: str, data_type: str, timeframe: Optional[str] = None):
        """Save data to CSV and SQLite."""
        try:
            # Crear una copia del DataFrame para no modificar el original
            df_to_save = data.copy()
            
            # Asegurarse de que los timestamps estén en el formato correcto
            if 'timestamp' in df_to_save.columns:
                if isinstance(df_to_save['timestamp'].iloc[0], (int, float)):
                    df_to_save['timestamp'] = pd.to_datetime(df_to_save['timestamp'], unit='ms')
            
            # Generate filename
            symbol_safe = symbol.replace('/', '_')
            timeframe_safe = f"_{timeframe}" if timeframe else ""
            csv_filename = f"{exchange_name}_{symbol_safe}{timeframe_safe}_{data_type}.csv"

            # Get storage path from config
            storage_path = self.config.storage.path
            
            # Create CSV directory if it doesn't exist
            csv_dir = os.path.join(storage_path, 'csv')
            os.makedirs(csv_dir, exist_ok=True)
            csv_path = os.path.join(csv_dir, csv_filename)

            # Para SQLite, mantener timestamps en milisegundos
            df_sqlite = df_to_save.copy()
            if isinstance(df_sqlite['timestamp'].iloc[0], pd.Timestamp):
                df_sqlite['timestamp'] = df_sqlite['timestamp'].astype(np.int64) // 10**6
            
            # Save to CSV (con timestamps legibles)
            save_to_csv(df_to_save, csv_path)

            # Save to SQLite (con timestamps en milisegundos)
            table_name = f"{exchange_name}_{symbol_safe}{timeframe_safe}_{data_type}"
            db_path = os.path.join(storage_path, 'data.db')
            save_to_sqlite(df_sqlite, table_name, db_path)

            self.logger.info(f"{data_type} data saved for {symbol} on {exchange_name}")

        except Exception as e:
            self.logger.error(f"Error saving {data_type} data for {symbol}: {e}")