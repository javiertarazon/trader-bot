"""
Script de pruebas para las nuevas características del sistema.
"""
import asyncio
import sys
import os
import time
import pandas as pd
from datetime import datetime, timedelta

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from descarga_datos.core.downloader import DataDownloader
from descarga_datos.config.config_loader import load_config_from_yaml
from descarga_datos.utils.retry_manager import RetryManager
from descarga_datos.utils.monitoring import PerformanceMonitor
from descarga_datos.utils.data_validator import DataValidator

async def test_retry_system():
    """Prueba el sistema de reintentos."""
    print("\n1. Probando sistema de reintentos...")
    config = load_config_from_yaml()
    downloader = DataDownloader(config)
    
    try:
        # Intentar descargar con un símbolo inválido para forzar error
        await downloader.setup_exchanges()
        result, stats = await downloader.async_download_ohlcv(
            symbol="INVALID/USDT",
            exchange_name="bybit",
            timeframe="1h",
            limit=10
        )
        
        print("❌ Error: La descarga con símbolo inválido no falló como se esperaba")
    except Exception as e:
        print("✅ Sistema de reintentos funcionó correctamente")
        print(f"   Error capturado como se esperaba: {str(e)}")
    finally:
        await downloader.close_exchanges()

async def test_data_validation():
    """Prueba el sistema de validación de datos."""
    print("\n2. Probando sistema de validación...")
    config = load_config_from_yaml()
    downloader = DataDownloader(config)
    
    try:
        await downloader.setup_exchanges()
        # Descargar datos válidos primero
        result, stats = await downloader.async_download_ohlcv(
            symbol="BTC/USDT",
            exchange_name="bybit",
            timeframe="1h",
            limit=100
        )
        
        if result is not None and not result.empty:
            print("✅ Descarga de datos exitosa")
            print(f"   Filas descargadas: {len(result)}")
            print(f"   Estadísticas calculadas: {bool(stats)}")
            
            # Verificar continuidad temporal
            timestamps = pd.to_datetime(result['timestamp'], unit='ms')
            gaps = timestamps.diff()[1:] != pd.Timedelta(hours=1)
            if gaps.any():
                print("❌ Se detectaron gaps en los datos")
            else:
                print("✅ Continuidad temporal verificada")
            
            # Verificar rangos de precios
            if (result['high'] >= result['low']).all():
                print("✅ Validación de precios correcta")
            else:
                print("❌ Error en validación de precios")
        else:
            print("❌ Error: No se pudieron descargar datos")
            
    except Exception as e:
        print(f"❌ Error en prueba de validación: {str(e)}")
    finally:
        await downloader.close_exchanges()

async def test_monitoring():
    """Prueba el sistema de monitoreo."""
    print("\n3. Probando sistema de monitoreo...")
    config = load_config_from_yaml()
    downloader = DataDownloader(config)
    
    try:
        await downloader.setup_exchanges()
        
        # Realizar algunas descargas para generar métricas
        symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
        for symbol in symbols:
            print(f"\nDescargando datos para {symbol}...")
            result, stats = await downloader.async_download_ohlcv(
                symbol=symbol,
                exchange_name="bybit",
                timeframe="1h",
                limit=100
            )
            
            if stats:
                print(f"✅ Métricas generadas para {symbol}")
                print(f"   Rango temporal: {stats['time_range']['start']} a {stats['time_range']['end']}")
                print(f"   Filas: {stats['row_count']}")
                print(f"   Volatilidad: {stats['price_stats']['price_volatility']:.4f}")
            else:
                print(f"❌ No se generaron métricas para {symbol}")
        
        # Verificar archivos de métricas
        metrics_dir = os.path.join(config.storage.path, "metrics")
        if os.path.exists(metrics_dir):
            metric_files = os.listdir(metrics_dir)
            print(f"\nArchivos de métricas generados: {len(metric_files)}")
            print("✅ Sistema de monitoreo funcionando")
        else:
            print("❌ No se encontró directorio de métricas")
            
    except Exception as e:
        print(f"❌ Error en prueba de monitoreo: {str(e)}")
    finally:
        await downloader.close_exchanges()

async def test_parallel_download():
    """Prueba la descarga paralela de múltiples símbolos."""
    print("\n4. Probando descarga paralela...")
    config = load_config_from_yaml()
    downloader = DataDownloader(config)
    
    try:
        await downloader.setup_exchanges()
        symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "ADA/USDT", "XRP/USDT"]
        
        print(f"Descargando {len(symbols)} símbolos en paralelo...")
        start_time = time.time()
        
        results = await downloader.download_multiple_symbols(
            symbols=symbols,
            exchange_name="bybit",
            timeframe="1h",
            limit=100,
            batch_size=3
        )
        
        duration = time.time() - start_time
        successful = len([s for s in results if results[s] is not None])
        
        print(f"✅ Descarga paralela completada en {duration:.2f} segundos")
        print(f"   Símbolos exitosos: {successful}/{len(symbols)}")
        
        for symbol in results:
            if results[symbol] is not None:
                print(f"   ✅ {symbol}: {len(results[symbol])} registros")
            else:
                print(f"   ❌ {symbol}: Error en descarga")
                
    except Exception as e:
        print(f"❌ Error en prueba de descarga paralela: {str(e)}")
    finally:
        await downloader.close_exchanges()

async def test_cache_system():
    """Prueba el sistema de caché."""
    print("\n5. Probando sistema de caché...")
    config = load_config_from_yaml()
    downloader = DataDownloader(config)
    
    try:
        await downloader.setup_exchanges()
        symbol = "BTC/USDT"
        
        # Primera descarga (sin caché)
        print(f"Primera descarga de {symbol}...")
        start_time = time.time()
        result1, _ = await downloader.async_download_ohlcv(
            symbol=symbol,
            exchange_name="bybit",
            timeframe="1h",
            limit=100,
            use_cache=True
        )
        time1 = time.time() - start_time
        
        if result1 is not None:
            print(f"✅ Primera descarga completada en {time1:.2f} segundos")
            
            # Segunda descarga (debería usar caché)
            print(f"Segunda descarga de {symbol} (desde caché)...")
            start_time = time.time()
            result2, _ = await downloader.async_download_ohlcv(
                symbol=symbol,
                exchange_name="bybit",
                timeframe="1h",
                limit=100,
                use_cache=True
            )
            time2 = time.time() - start_time
            
            if result2 is not None:
                print(f"✅ Segunda descarga completada en {time2:.2f} segundos")
                print(f"   Mejora de velocidad: {((time1 - time2) / time1 * 100):.1f}%")
                
                # Verificar que los datos son idénticos
                if result1.equals(result2):
                    print("✅ Datos del caché verificados correctamente")
                else:
                    print("❌ Los datos del caché no coinciden")
            else:
                print("❌ Error en segunda descarga")
        else:
            print("❌ Error en primera descarga")
            
    except Exception as e:
        print(f"❌ Error en prueba de caché: {str(e)}")
    finally:
        await downloader.close_exchanges()

async def main():
    """Ejecuta todas las pruebas."""
    print("Iniciando pruebas completas del sistema...")
    
    # Pruebas básicas
    await test_retry_system()
    await test_data_validation()
    await test_monitoring()
    
    # Pruebas de nuevas características
    await test_parallel_download()
    await test_cache_system()
    
    print("\nPruebas completadas.")

if __name__ == "__main__":
    asyncio.run(main())
