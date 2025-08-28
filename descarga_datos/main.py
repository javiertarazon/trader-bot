import asyncio
import pandas as pd
from .config.config_loader import load_config_from_yaml
from .core.downloader import DataDownloader
from .utils.normalization import DataNormalizer
from .utils.storage import save_to_csv, save_to_sqlite
import os
import logging

async def main():
    """Main function to run the data downloader."""
    # Load configuration
    config = load_config_from_yaml()
    
    # Initialize downloader
    downloader = DataDownloader(config)
    
    try:
        # Setup exchanges
        await downloader.setup_exchanges()

        # Get active exchange
        active_exchange = config.active_exchange
        timeframe = config.timeframe

        # Download data
        for symbol in config.default_symbols:
            for data_type in config.data_types:
                raw_df = None
                if data_type == "ohlcv":
                    raw_df = await downloader.async_download_ohlcv(symbol, active_exchange, timeframe=timeframe)
                elif data_type == "trades":
                    # For now, we only normalize ohlcv data
                    await downloader.async_download_trades(symbol, active_exchange)

                if raw_df is not None and not raw_df.empty and data_type == "ohlcv":
                    # Normalize the data
                    normalizer = DataNormalizer(config.normalization)
                    normalized_df = normalizer.fit_transform(raw_df)

                    # Save normalized data
                    storage_path = config.storage.path
                    
                    # Define paths for normalized data
                    normalized_csv_path = os.path.join(storage_path, 'csv', f"{active_exchange}_{symbol.replace('/', '_')}_{timeframe}_{data_type}_normalized.csv")
                    normalized_table_name = f"{active_exchange}_{symbol.replace('/', '_')}_{timeframe}_{data_type}_normalized"
                    
                    # Save to CSV and SQLite
                    os.makedirs(os.path.dirname(normalized_csv_path), exist_ok=True)
                    save_to_csv(normalized_df, normalized_csv_path)
                    save_to_sqlite(normalized_df, normalized_table_name, os.path.join(storage_path, 'data.db'))
                    
                    print(f"Datos normalizados guardados para {symbol} en {active_exchange} con timeframe {timeframe}")
    
    except Exception as e:
        logging.error(f"Error en la ejecución principal: {e}")
        raise
    finally:
        # Asegurar que los exchanges se cierren correctamente
        await downloader.close_exchanges()

if __name__ == "__main__":
    asyncio.run(main())