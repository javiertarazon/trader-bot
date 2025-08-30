# Sistema de Descarga y Cálculo de Indicadores Técnicos

Este proyecto proporciona un sistema completo para descargar datos OHLCV de criptomonedas desde exchanges y calcular indicadores técnicos para análisis de mercado y machine learning.

## Características

- **Descarga de datos OHLCV** desde múltiples exchanges (Binance, Bybit, etc.)
- **Indicadores técnicos calculados**:
  - Volatilidad del mercado
  - Velas Heiken Ashi con tendencia
  - Tamaño relativo de velas Heiken Ashi
  - ATR (Average True Range)
  - ADX (Average Directional Index)
  - EMAs de 10, 20 y 200 períodos
  - SAR Parabólico
- **Normalización de datos** para machine learning
- **Almacenamiento flexible** en CSV crudo y normalizado
- **Configuración centralizada** mediante YAML
- **Procesamiento asíncrono** para mejor rendimiento

## Requisitos

- Python 3.8+
- Dependencias listadas en `requirements.txt`

## Instalación

1. Clonar el repositorio:
```bash
git clone https://github.com/tu-usuario/data_downloader.git
cd data_downloader
```

2. Crear y activar un entorno virtual:
```bash
python -m venv venv
# En Windows
venv\Scripts\activate
# En Linux/Mac
source venv/bin/activate
```

3. Instalar dependencias:
```bash
pip install -r requirements.txt
```

## Estructura del Proyecto

```
data_downloader/
├── data/                   # Directorio de datos descargados
│   ├── csv/                # Archivos CSV
│   └── data.db             # Base de datos SQLite
├── descarga_datos/         # Código fuente
│   ├── config/             # Configuración
│   ├── core/               # Funcionalidad principal
│   ├── indicators/         # Módulo de indicadores técnicos
│   │   ├── technical_indicators.py
│   │   ├── example_usage.py
│   │   └── __init__.py
│   ├── tests/              # Tests unitarios
│   ├── utils/              # Utilidades (incluye storage.py)
│   └── main.py             # Script principal
└── requirements.txt        # Dependencias
```

## Configuración

Editar el archivo `descarga_datos/config/config.yaml` para configurar:

```yaml
# Exchange activo
active_exchange: "bybit"

# Símbolos a procesar
default_symbols:
  - "SOL/USDT"
  - "BTC/USDT"
  - "ETH/USDT"

# Configuración de exchanges
exchanges:
  bybit:
    timeframe: "1h"
    api_key: null
    secret: null
  binance:
    timeframe: "1h"
    api_key: null
    secret: null

# Configuración de indicadores
indicators:
  volatility:
    period: 14
  heiken_ashi:
    enabled: true
  atr:
    period: 14
  adx:
    period: 14
  ema:
    fast_period: 10
    medium_period: 20
    slow_period: 200
  parabolic_sar:
    acceleration: 0.02
    maximum: 0.2

# Configuración de almacenamiento
storage:
  path: "data"

# Configuración de normalización
normalization:
  method: "minmax"
  feature_range: [0, 1]
```

## Uso

### Ejecutar el proceso completo
```bash
python -m descarga_datos.main
```

### Ejemplo de uso programático
```python
import asyncio
import pandas as pd
from descarga_datos.core.downloader import DataDownloader
from descarga_datos.indicators.technical_indicators import TechnicalIndicators
from descarga_datos.utils.normalization import DataNormalizer
from descarga_datos.config.config_loader import load_config_from_yaml

async def main():
    # Cargar configuración
    config = load_config_from_yaml()
    
    # Inicializar componentes
    downloader = DataDownloader(config)
    indicators = TechnicalIndicators(config)
    normalizer = DataNormalizer(config.normalization)
    
    # Descargar datos
    await downloader.setup_exchanges()
    data = await downloader.async_download_ohlcv(
        symbol="BTC/USDT",
        exchange="bybit",
        timeframe="1h",
        limit=1000
    )
    
    # Calcular indicadores
    data_with_indicators = indicators.calculate_all_indicators(data)
    
    # Normalizar datos
    normalized_data = normalizer.fit_transform(data_with_indicators)
    
    # Guardar resultados
    normalized_data.to_csv("btc_indicators_normalized.csv")
    
    await downloader.close_exchanges()

# Ejecutar
asyncio.run(main())
```

## Módulo de Indicadores Técnicos

El sistema incluye un módulo completo de indicadores técnicos con las siguientes características:

### Indicadores Disponibles

- **Volatilidad del mercado**: Cálculo basado en desviación estándar de retornos
- **Velas Heiken Ashi**: Incluye tendencia y comparación de tamaño de velas
- **ATR (Average True Range)**: Medida de volatilidad basada en rango verdadero
- **ADX (Average Directional Index)**: Indicador de fuerza de tendencia
- **EMAs**: Promedios móviles exponenciales de 10, 20 y 200 períodos
- **Parabolic SAR**: Sistema de parada y reversión

### Configuración

Los parámetros de todos los indicadores son configurables centralmente en `config/config.yaml`:

```yaml
indicators:
  volatility:
    period: 20
    method: "standard"
  heiken_ashi:
    trend_period: 5
    size_threshold: 0.02
  atr:
    period: 14
  adx:
    period: 14
  ema:
    periods: [10, 20, 200]
  parabolic_sar:
    acceleration: 0.02
    max_acceleration: 0.2
```

### Uso del Módulo de Indicadores

```python
from descarga_datos.indicators.technical_indicators import TechnicalIndicators
from descarga_datos.config.config_loader import Config

# Cargar configuración
config = Config()

# Crear instancia de indicadores
indicators = TechnicalIndicators(config.indicators)

# Calcular indicadores para un DataFrame de OHLCV
results = indicators.calculate_all_indicators(ohlcv_data)

# Guardar resultados (crudos y normalizados)
indicators.save_indicators_to_csv(data, "bybit", "SOLUSDT", "1h")
indicators.save_normalized_indicators_to_csv(data, "bybit", "SOLUSDT", "1h", method="minmax")
indicators.save_indicators_to_sqlite(data, "bybit", "SOLUSDT", "1h")
indicators.save_normalized_indicators_to_sqlite(data, "bybit", "SOLUSDT", "1h", method="minmax")

# Normalización individual
normalized_df = indicators.normalize_indicators(results, method="standard")
```

Para ver un ejemplo completo, consultar `descarga_datos/indicators/example_usage.py`.

## Estructura del Proyecto

```
data_downloader/
├── data/                   # Directorio de datos descargados
│   ├── csv/                # Archivos CSV
│   └── data.db             # Base de datos SQLite
├── descarga_datos/         # Código fuente
│   ├── config/             # Configuración
│   ├── core/               # Funcionalidad principal
│   ├── indicators/         # Módulo de indicadores técnicos
│   │   ├── technical_indicators.py
│   │   ├── example_usage.py
│   │   └── __init__.py
│   ├── tests/              # Tests unitarios
│   ├── utils/              # Utilidades (incluye storage.py)
│   └── main.py             # Script principal
└── requirements.txt        # Dependencias
```

## Licencia

Este proyecto está licenciado bajo la Licencia MIT - ver el archivo LICENSE para más detalles.