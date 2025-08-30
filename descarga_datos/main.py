import sys
import os
import asyncio
import pandas as pd

# Agregar el directorio raíz del proyecto al path de Python
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from descarga_datos.core.downloader import DataDownloader
from descarga_datos.indicators.technical_indicators import TechnicalIndicators
from descarga_datos.utils.normalization import DataNormalizer
from descarga_datos.utils.storage import save_to_csv, save_to_sqlite
from descarga_datos.config.config_loader import load_config_from_yaml
from descarga_datos.utils.logger import setup_logging, get_logger

async def main():
    # Cargar configuración
    config = load_config_from_yaml()
    
    # Configurar logging
    setup_logging(config)
    logger = get_logger(__name__)
    
    logger.info("Iniciando proceso de descarga y cálculo de indicadores...")
    
    # Inicializar downloader
    downloader = DataDownloader(config)
    
    try:
        # Configurar exchanges
        await downloader.setup_exchanges()
        
        # Obtener exchange activo y timeframe
        active_exchange = config.active_exchange
        timeframe = config.exchanges[active_exchange]['timeframe']
        
        # Descargar datos para cada símbolo
        for symbol in config.default_symbols:
            logger.info(f"Procesando {symbol} en {active_exchange} con timeframe {timeframe}")
            
            # Descargar datos OHLCV
            ohlcv_data = await downloader.async_download_ohlcv(
                symbol, 
                active_exchange, 
                timeframe=timeframe,
                limit=1000  # Descargar más datos para tener suficientes para indicadores
            )
            
            if ohlcv_data is not None and not ohlcv_data.empty:
                logger.info(f"Datos descargados: {len(ohlcv_data)} filas")
                
                # Calcular indicadores
                indicators = TechnicalIndicators(config)
                data_with_indicators = indicators.calculate_all_indicators(ohlcv_data)
                
                if data_with_indicators is not None:
                    logger.info("Indicadores calculados exitosamente")
                    
                    # Guardar datos crudos con indicadores
                    symbol_safe = symbol.replace('/', '_')
                    output_dir = f"{config.storage.path}/csv"
                    output_file_raw = f"{output_dir}/{config.active_exchange}_{symbol_safe}_{timeframe}_ohlcv_indicators.csv"
                    db_path = f"{config.storage.path}/data.db"
                    table_raw = f"{config.active_exchange}_{symbol_safe}_{timeframe}_indicators_raw"
                    
                    save_to_csv(data_with_indicators, output_file_raw)
                    save_to_sqlite(data_with_indicators, table_raw, db_path)
                    logger.info(f"Datos crudos guardados en CSV: {output_file_raw} y DB: {table_raw}")
                    
                    # Normalizar datos
                    logger.info("Normalizando datos...")
                    normalizer = DataNormalizer(config.normalization)
                    normalized_data = normalizer.fit_transform(data_with_indicators)
                    
                    output_file_normalized = f"{output_dir}/{config.active_exchange}_{symbol_safe}_{timeframe}_ohlcv_indicators_normalized.csv"
                    table_normalized = f"{config.active_exchange}_{symbol_safe}_{timeframe}_indicators_normalized"
                    
                    save_to_csv(normalized_data, output_file_normalized)
                    save_to_sqlite(normalized_data, table_normalized, db_path)
                    logger.info(f"Datos normalizados guardados en CSV: {output_file_normalized} y DB: {table_normalized}")
                    
                    logger.info("Proceso completado exitosamente")
                else:
                    logger.error("Error al calcular los indicadores")
            else:
                logger.error(f"No se pudieron descargar los datos para {symbol}")
    
    finally:
        # Cerrar exchanges
        await downloader.close_exchanges()

if __name__ == "__main__":
    asyncio.run(main())