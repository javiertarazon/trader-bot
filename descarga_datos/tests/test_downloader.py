import unittest
from unittest.mock import patch, AsyncMock, MagicMock
import pandas as pd
import os
from descarga_datos.config.config import Config
from descarga_datos.core.downloader import DataDownloader

class TestDataDownloader(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        """Set up test configuration and downloader instance."""
        from descarga_datos.config.config import StorageConfig
        self.config = Config(
            active_exchange='binance',
            exchanges={
                'binance': {
                    'apiKey': 'test_key',
                    'secret': 'test_secret'
                }
            },
            max_retries=3,
            retry_delay=0.1,
            storage=StorageConfig(path='test_csv')
        )
        self.downloader = DataDownloader(self.config)

    async def asyncSetUp(self):
        """Set up mocks for ccxt."""
        self.patcher = patch('descarga_datos.core.downloader.ccxt')
        mock_ccxt = self.patcher.start()

        self.mock_exchange_instance = AsyncMock()
        self.mock_exchange_instance.fetch_ohlcv = AsyncMock()
        self.mock_exchange_instance.fetch_trades = AsyncMock()
        self.mock_exchange_instance.close = AsyncMock()

        self.mock_binance_class = MagicMock(return_value=self.mock_exchange_instance)
        setattr(mock_ccxt, 'binance', self.mock_binance_class)

    async def asyncTearDown(self):
        """Clean up resources and stop patcher."""
        self.patcher.stop()
        storage_path = self.config.storage.path
        if os.path.exists(storage_path):
            for f in os.listdir(storage_path):
                os.remove(os.path.join(storage_path, f))
            os.rmdir(storage_path)
        db_path = os.path.join(storage_path, 'data.db')
        if os.path.exists(db_path):
            os.remove(db_path)

    async def test_setup_exchanges(self):
        """Test that exchanges are set up correctly."""
        await self.downloader.setup_exchanges()
        self.assertIn('binance', self.downloader.exchanges)
        self.mock_binance_class.assert_called_with({
            'apiKey': 'test_key',
            'secret': 'test_secret',
            'enableRateLimit': True,
        })
        self.assertEqual(self.downloader.exchanges['binance'], self.mock_exchange_instance)
        await self.downloader.close_exchanges()
        self.mock_exchange_instance.close.assert_awaited_once()

    @patch('descarga_datos.utils.storage.save_to_csv')
    @patch('descarga_datos.utils.storage.save_to_sqlite')
    async def test_async_download_ohlcv_success(self, mock_save_to_sqlite, mock_save_to_csv):
        """Test successful download of OHLCV data."""
        await self.downloader.setup_exchanges()
        self.mock_exchange_instance.fetch_ohlcv.return_value = [[1622505600000, 40000, 41000, 39000, 40500, 100]]

        df = await self.downloader.async_download_ohlcv('BTC/USDT', 'binance')

        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        self.mock_exchange_instance.fetch_ohlcv.assert_awaited_once()
        mock_save_to_csv.assert_called_once()
        mock_save_to_sqlite.assert_called_once()
        await self.downloader.close_exchanges()

    @patch('descarga_datos.utils.storage.save_to_csv')
    @patch('descarga_datos.utils.storage.save_to_sqlite')
    async def test_async_download_ohlcv_retry(self, mock_save_to_sqlite, mock_save_to_csv):
        """Test retry mechanism for OHLCV download."""
        await self.downloader.setup_exchanges()
        self.mock_exchange_instance.fetch_ohlcv.side_effect = [Exception("Network Error"), [[1622505600000, 40000, 41000, 39000, 40500, 100]]]

        df = await self.downloader.async_download_ohlcv('BTC/USDT', 'binance')

        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        self.assertEqual(self.mock_exchange_instance.fetch_ohlcv.await_count, 2)
        await self.downloader.close_exchanges()

    async def test_async_download_ohlcv_max_retries(self):
        """Test that download fails after max retries for OHLCV."""
        await self.downloader.setup_exchanges()
        self.mock_exchange_instance.fetch_ohlcv.side_effect = Exception("Network Error")

        df = await self.downloader.async_download_ohlcv('BTC/USDT', 'binance')

        self.assertIsNone(df)
        self.assertEqual(self.mock_exchange_instance.fetch_ohlcv.await_count, self.config.max_retries)
        await self.downloader.close_exchanges()

    @patch('core.downloader.save_to_csv')
    @patch('core.downloader.save_to_sqlite')
    async def test_async_download_trades_success(self, mock_save_to_sqlite, mock_save_to_csv):
        """Test successful download of trades data."""
        await self.downloader.setup_exchanges()
        self.mock_exchange_instance.fetch_trades.return_value = [{'id': '1', 'timestamp': 1622505600000, 'datetime': '2021-06-01T00:00:00.000Z', 'symbol': 'BTC/USDT', 'side': 'buy', 'price': 40000, 'amount': 1, 'cost': 40000}]

        df = await self.downloader.async_download_trades('BTC/USDT', 'binance')

        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        self.mock_exchange_instance.fetch_trades.assert_awaited_once()
        mock_save_to_csv.assert_called_once()
        mock_save_to_sqlite.assert_called_once()
        await self.downloader.close_exchanges()

if __name__ == '__main__':
    unittest.main()