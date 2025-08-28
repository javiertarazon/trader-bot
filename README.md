# Sistema de Descarga de Datos de Criptomonedas

Este sistema permite la descarga, normalización y almacenamiento de datos históricos de criptomonedas desde diferentes exchanges.

## Características

- Descarga de datos OHLCV (Open, High, Low, Close, Volume) y trades
- Normalización de datos utilizando diferentes métodos (MinMax, Standard, Robust)
- Almacenamiento en formato CSV y SQLite
- Configuración centralizada mediante archivos YAML
- Soporte para múltiples exchanges (KuCoin, Binance, etc.)
- Manejo de errores y reintentos automáticos
- Logging detallado

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

## Configuración

Editar el archivo `descarga_datos/config/config.yaml` para configurar:

- Exchange activo
- Símbolos a descargar
- Tipos de datos (OHLCV, trades)
- Configuración de almacenamiento
- Parámetros de normalización

## Uso

Ejecutar el script principal:

```bash
python -m descarga_datos.main
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
│   ├── tests/              # Tests unitarios
│   ├── utils/              # Utilidades
│   └── main.py             # Script principal
└── requirements.txt        # Dependencias
```

## Licencia

Este proyecto está licenciado bajo la Licencia MIT - ver el archivo LICENSE para más detalles.