import unittest
import os

if __name__ == "__main__":
    # Descubrir y ejecutar todas las pruebas en el directorio 'tests'
    loader = unittest.TestLoader()
    # Obtener la ruta absoluta al directorio de tests
    current_dir = os.path.dirname(os.path.abspath(__file__))
    tests_dir = os.path.join(current_dir, 'tests')
    suite = loader.discover(tests_dir)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)