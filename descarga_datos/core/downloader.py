import ccxt
import time
import asyncio
import os
from typing import Optional, Dict, Any, List
import pandas as pd
import ccxt.async_support as ccxt  # Import async version
from datetime import datetime
from ..config.config import Config
from ..utils.logger import setup_logging, get_logger
from ..utils.storage import save_to_csv, save_to_sqlite

class DataDownloader:
    def __init__(self, config: Config):
        self.config = config
        self.exchanges = {}
        setup_logging(config)
        self.logger = get_logger(__name__)

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

    async def async_download_ohlcv(self, symbol: str, exchange_name: str, timeframe: str = '1d', since: Optional[int] = None, limit: int = 100, params: dict = {}):
        """Download OHLCV data asynchronously with retries and automatic storage."""
        for attempt in range(self.config.max_retries):
            try:
                exchange = self._get_exchange(exchange_name)
                ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, since, limit, params)

                if ohlcv:
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    self._save_data(df, exchange_name, symbol, 'ohlcv', timeframe)
                    return df

                return pd.DataFrame()
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {symbol} on {exchange_name}: {e}")
                await asyncio.sleep(self.config.retry_delay)
        self.logger.error(f"Failed to download data for {symbol} after {self.config.max_retries} attempts.")
        return None

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

            # Save to CSV
            save_to_csv(data, csv_path)

            # Save to SQLite
            table_name = f"{exchange_name}_{symbol_safe}{timeframe_safe}_{data_type}"
            db_path = os.path.join(storage_path, 'data.db')
            save_to_sqlite(data, table_name, db_path)

            self.logger.info(f"{data_type} data saved for {symbol} on {exchange_name}")

        except Exception as e:
            self.logger.error(f"Error saving {data_type} data for {symbol}: {e}")