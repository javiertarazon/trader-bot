import unittest
import os
import pandas as pd
import sqlite3
from descarga_datos.config.config import Config
from descarga_datos.core.downloader import DataDownloader
from descarga_datos.utils.normalization import DataNormalizer

class TestFunctionalWorkflow(unittest.TestCase):
    """Test que verifica la funcionalidad principal del sistema."""
    
    def setUp(self):
        """Configuración inicial para las pruebas."""
        # Crear una configuración de prueba
        self.test_dir = "test_data"
        os.makedirs(self.test_dir, exist_ok=True)
        
        # Crear datos de prueba
        self.test_data = pd.DataFrame({
            'timestamp': [1622505600000, 1622505700000],
            'open': [40000, 40100],
            'high': [41000, 41100],
            'low': [39000, 39100],
            'close': [40500, 40600],
            'volume': [100, 110]
        })
        
        # Guardar datos de prueba
        self.test_data.to_csv(os.path.join(self.test_dir, "test_data.csv"), index=False)
    
    def tearDown(self):
        """Limpieza después de las pruebas."""
        # Eliminar archivos y directorios de prueba
        for root, dirs, files in os.walk(self.test_dir, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))
        if os.path.exists(self.test_dir):
            os.rmdir(self.test_dir)
    
    def test_data_normalization(self):
        """Prueba la normalización de datos."""
        # Crear normalizador con la configuración
        from descarga_datos.config.config import NormalizationConfig
        config = NormalizationConfig(method="minmax", feature_range=(0, 1))
        normalizer = DataNormalizer(config)
        
        # Normalizar datos
        normalized_data = normalizer.normalize(self.test_data)
        
        # Verificar que los datos se normalizaron correctamente
        self.assertIsInstance(normalized_data, pd.DataFrame)
        self.assertEqual(len(normalized_data), len(self.test_data))
        
        # Verificar que los valores están en el rango [0, 1]
        for col in ['open', 'high', 'low', 'close', 'volume']:
            self.assertTrue((normalized_data[col] >= 0).all())
            self.assertTrue((normalized_data[col] <= 1).all())
    
    def test_data_storage(self):
        """Prueba el almacenamiento de datos en CSV y SQLite."""
        from descarga_datos.utils.storage import save_to_csv, save_to_sqlite
        
        # Guardar en CSV
        csv_path = os.path.join(self.test_dir, "test_output.csv")
        save_to_csv(self.test_data, csv_path)
        
        # Verificar que el archivo CSV existe y contiene los datos correctos
        self.assertTrue(os.path.exists(csv_path))
        loaded_csv = pd.read_csv(csv_path)
        self.assertEqual(len(loaded_csv), len(self.test_data))
        
        # Guardar en SQLite
        db_path = os.path.join(self.test_dir, "test.db")
        # Asegurar que el directorio existe
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        # Crear la tabla en SQLite
        save_to_sqlite(self.test_data, db_path, "test_table")
        
        # Verificar que la base de datos existe y contiene los datos correctos
        self.assertTrue(os.path.exists(db_path))
        conn = sqlite3.connect(db_path)
        loaded_sqlite = pd.read_sql("SELECT * FROM test_table", conn)
        conn.close()
        self.assertEqual(len(loaded_sqlite), len(self.test_data))

if __name__ == "__main__":
    unittest.main()