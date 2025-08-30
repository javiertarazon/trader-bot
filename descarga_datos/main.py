import sys
import os
import asyncio
import pandas as pd
import sqlite3
from datetime import datetime

# Agregar el directorio raíz del proyecto al path de Python
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from descarga_datos.core.downloader import DataDownloader
from descarga_datos.indicators.technical_indicators import TechnicalIndicators
from descarga_datos.utils.normalization import DataNormalizer
from descarga_datos.utils.storage import save_to_csv, save_to_sqlite
from descarga_datos.config.config_loader import load_config_from_yaml
from descarga_datos.utils.logger import setup_logging, get_logger

def check_data_exists(db_path: str, table_name: str, start_date: str, end_date: str) -> tuple[bool, pd.DataFrame]:
    """
    Verifica si los datos ya existen en la base de datos para el período especificado.
    
    Args:
        db_path: Ruta a la base de datos SQLite
        table_name: Nombre de la tabla a verificar
        start_date: Fecha de inicio en formato 'YYYY-MM-DD'
        end_date: Fecha de fin en formato 'YYYY-MM-DD'
        
    Returns:
        tuple[bool, pd.DataFrame]: (True si los datos existen, DataFrame con los datos)
    """
    try:
        # Verificar si el archivo de base de datos existe
        if not os.path.exists(db_path):
            return False, pd.DataFrame()
            
        # Convertir fechas a timestamps para comparación
        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
        end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000)
        
        # Verificar si la tabla existe y tiene datos para el período
        conn = sqlite3.connect(db_path)
        
        try:
            # Primero verificar si la tabla existe
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cursor.fetchone():
                return False, pd.DataFrame()
            
            # Consultar los datos existentes
            query = f"""
                SELECT timestamp
                FROM {table_name}
                WHERE timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp
            """
            
            df = pd.read_sql_query(query, conn, params=(start_ts, end_ts))
            
            if df.empty:
                return False, pd.DataFrame()
                
            # Verificar continuidad de datos
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='ms')
            df = df.set_index('timestamp')
            expected_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='1h')
            
            # Calcular el porcentaje de datos presentes
            completeness = len(df) / len(expected_index)
            
            if completeness >= 0.95:  # Permitimos un 5% de datos faltantes
                return True, df
                
            return False, pd.DataFrame()
            
        finally:
            conn.close()
            
    except Exception as e:
        logger = get_logger(__name__)
        logger.error(f"Error verificando datos existentes: {e}")
        return False, pd.DataFrame()

async def main():
    # Cargar configuración
    config = load_config_from_yaml()
    
    # Configurar logging
    setup_logging(config)
    logger = get_logger(__name__)
    
    logger.info("Iniciando proceso de verificación y descarga de datos...")
    
    # Inicializar downloader
    downloader = DataDownloader(config)
    
    try:
        # Configurar exchanges
        await downloader.setup_exchanges()
        
        # Obtener exchange activo y timeframe
        active_exchange = config.active_exchange
        timeframe = config.exchanges[active_exchange]['timeframe']
        start_date = config.exchanges[active_exchange]['start_date']
        end_date = config.exchanges[active_exchange]['end_date']
        
        # Descargar datos para cada símbolo
        for symbol in config.default_symbols:
            logger.info(f"Verificando datos existentes para {symbol} en {active_exchange}")
            
            symbol_safe = symbol.replace('/', '_')
            db_path = f"{config.storage.path}/data.db"
            table_raw = f"{config.active_exchange}_{symbol_safe}_{timeframe}_indicators_raw"
            table_normalized = f"{config.active_exchange}_{symbol_safe}_{timeframe}_indicators_normalized"
            
            # Verificar si los datos ya existen en la tabla de datos crudos
            table_ohlcv = f"{config.active_exchange}_{symbol_safe}_{timeframe}_ohlcv"
            data_exists, existing_data = check_data_exists(db_path, table_ohlcv, start_date, end_date)
            
            if data_exists:
                logger.info(f"Datos existentes encontrados para {symbol}, usando datos almacenados")
                # Cargar datos normalizados
                conn = sqlite3.connect(db_path)
                data_with_indicators = pd.read_sql_query(f"SELECT * FROM {table_normalized}", conn)
                conn.close()
            else:
                logger.info(f"Descargando nuevos datos para {symbol}")
                # Descargar datos OHLCV
                ohlcv_data = await downloader.async_download_ohlcv(
                    symbol, 
                    active_exchange, 
                    timeframe=timeframe,
                    limit=1000
                )
                
                if ohlcv_data is not None and not ohlcv_data.empty:
                    logger.info(f"Datos descargados: {len(ohlcv_data)} filas")
                    
                    # Calcular indicadores
                    indicators = TechnicalIndicators(config)
                    data_with_indicators = indicators.calculate_all_indicators(ohlcv_data)
                else:
                    logger.error(f"No se pudieron descargar los datos para {symbol}")
                    continue
            
            if data_with_indicators is not None:
                if not data_exists:
                    logger.info("Indicadores calculados exitosamente")
                    
                    # Guardar datos crudos con indicadores
                    output_dir = f"{config.storage.path}/csv"
                    output_file_raw = f"{output_dir}/{config.active_exchange}_{symbol_safe}_{timeframe}_ohlcv_indicators.csv"
                    
                    save_to_csv(data_with_indicators, output_file_raw)
                    save_to_sqlite(data_with_indicators, table_raw, db_path)
                    logger.info(f"Datos crudos guardados en CSV: {output_file_raw} y DB: {table_raw}")
                    
                    # Normalizar datos
                    logger.info("Normalizando datos...")
                    normalizer = DataNormalizer(config.normalization)
                    normalized_data = normalizer.fit_transform(data_with_indicators)
                    
                    output_file_normalized = f"{output_dir}/{config.active_exchange}_{symbol_safe}_{timeframe}_ohlcv_indicators_normalized.csv"
                    
                    save_to_csv(normalized_data, output_file_normalized)
                    save_to_sqlite(normalized_data, table_normalized, db_path)
                    logger.info(f"Datos normalizados guardados en CSV: {output_file_normalized} y DB: {table_normalized}")
                
                logger.info("Proceso completado exitosamente")
            else:
                logger.error("Error al calcular los indicadores")
    
    finally:
        # Cerrar exchanges
        await downloader.close_exchanges()

if __name__ == "__main__":
    asyncio.run(main())