import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.visualizer.data_cache import ChartDataCache


@pytest.fixture
def mock_config():
    return {
        "visualizer": {
            "default_period": "daily",
            "available_periods": ["daily", "weekly", "monthly"],
            "default_date_range_days": 365,
        }
    }


@pytest.fixture
def sample_kline_df():
    dates = pd.date_range(end="2025-01-10", periods=10, freq="B")
    return pd.DataFrame({
        "trade_date": dates.strftime("%Y%m%d"),
        "open": [10.0 + i * 0.5 for i in range(10)],
        "high": [10.5 + i * 0.5 for i in range(10)],
        "low": [9.5 + i * 0.5 for i in range(10)],
        "close": [10.2 + i * 0.5 for i in range(10)],
        "volume": [1000000 + i * 100000 for i in range(10)],
        "ts_code": "000001.SZ",
        "data_type": "daily",
    })


class TestChartDataCacheInit:

    @patch("modules.visualizer.data_cache.load_config")
    def test_init_loads_config(self, mock_load):
        mock_load.return_value = {"visualizer": {}}
        cache = ChartDataCache()
        assert cache.config == {"visualizer": {}}

    @patch("modules.visualizer.data_cache.load_config")
    def test_lazy_fetcher_init(self, mock_load):
        mock_load.return_value = {}
        cache = ChartDataCache()
        assert cache._fetcher is None
        assert cache._db_manager is None


class TestChartDataCacheGetKlineData:

    @patch("modules.visualizer.data_cache.load_config")
    @patch("modules.visualizer.data_cache.DataFetcher")
    def test_get_kline_returns_dataframe(self, mock_fetcher_cls, mock_load, sample_kline_df):
        mock_load.return_value = {"visualizer": {"default_date_range_days": 365}}
        mock_fetcher = MagicMock()
        mock_fetcher.get_stock_data.return_value = sample_kline_df
        mock_fetcher_cls.return_value = mock_fetcher

        cache = ChartDataCache()
        result = cache.get_kline_data("000001", period="daily")

        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert "trade_date" in result.columns
        mock_fetcher.get_stock_data.assert_called_once()

    @patch("modules.visualizer.data_cache.load_config")
    @patch("modules.visualizer.data_cache.DataFetcher")
    def test_get_kline_empty_result(self, mock_fetcher_cls, mock_load):
        mock_load.return_value = {"visualizer": {"default_date_range_days": 365}}
        mock_fetcher = MagicMock()
        mock_fetcher.get_stock_data.return_value = pd.DataFrame()
        mock_fetcher_cls.return_value = mock_fetcher

        cache = ChartDataCache()
        result = cache.get_kline_data("000001")

        assert result.empty

    @patch("modules.visualizer.data_cache.load_config")
    @patch("modules.visualizer.data_cache.DataFetcher")
    def test_get_kline_trade_date_converted(self, mock_fetcher_cls, mock_load, sample_kline_df):
        mock_load.return_value = {"visualizer": {"default_date_range_days": 365}}
        mock_fetcher = MagicMock()
        mock_fetcher.get_stock_data.return_value = sample_kline_df
        mock_fetcher_cls.return_value = mock_fetcher

        cache = ChartDataCache()
        result = cache.get_kline_data("000001")

        assert pd.api.types.is_datetime64_any_dtype(result["trade_date"])


class TestChartDataCacheGetAnalysisData:

    @patch("modules.visualizer.data_cache.load_config")
    @patch("modules.visualizer.data_cache.DataFetcher")
    @patch("modules.visualizer.data_cache.DBManager")
    def test_get_analysis_cache_hit(self, mock_db_cls, mock_fetcher_cls, mock_load):
        mock_load.return_value = {"visualizer": {}}
        mock_fetcher = MagicMock()
        mock_fetcher._format_tushare_code.return_value = "000001.SZ"
        mock_fetcher_cls.return_value = mock_fetcher

        mock_db = MagicMock()
        cached_data = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "data_type": ["chanlun_daily"],
            "bi_direction": ["up"],
        })
        mock_db.find_to_dataframe.return_value = cached_data
        mock_db_cls.return_value = mock_db

        cache = ChartDataCache()
        result = cache.get_analysis_data("000001", period="daily", analysis_type="chanlun")

        assert not result.empty
        mock_db.find_to_dataframe.assert_called_once()

    @patch("modules.visualizer.data_cache.load_config")
    @patch("modules.visualizer.data_cache.DataFetcher")
    @patch("modules.visualizer.data_cache.DBManager")
    def test_get_analysis_cache_miss_module_not_implemented(
        self, mock_db_cls, mock_fetcher_cls, mock_load
    ):
        mock_load.return_value = {"visualizer": {}}
        mock_fetcher = MagicMock()
        mock_fetcher._format_tushare_code.return_value = "000001.SZ"
        mock_fetcher_cls.return_value = mock_fetcher

        mock_db = MagicMock()
        mock_db.find_to_dataframe.return_value = pd.DataFrame()
        mock_db_cls.return_value = mock_db

        cache = ChartDataCache()
        result = cache.get_analysis_data("000001", period="daily", analysis_type="chanlun")

        assert result.empty

    @patch("modules.visualizer.data_cache.load_config")
    @patch("modules.visualizer.data_cache.DataFetcher")
    @patch("modules.visualizer.data_cache.DBManager")
    def test_get_analysis_caisen_module_not_implemented(
        self, mock_db_cls, mock_fetcher_cls, mock_load
    ):
        mock_load.return_value = {"visualizer": {}}
        mock_fetcher = MagicMock()
        mock_fetcher._format_tushare_code.return_value = "000001.SZ"
        mock_fetcher_cls.return_value = mock_fetcher

        mock_db = MagicMock()
        mock_db.find_to_dataframe.return_value = pd.DataFrame()
        mock_db_cls.return_value = mock_db

        cache = ChartDataCache()
        result = cache.get_analysis_data("000001", period="daily", analysis_type="caisen")

        assert result.empty


class TestChartDataCacheGetStockList:

    @patch("modules.visualizer.data_cache.load_config")
    @patch("modules.visualizer.data_cache.DataFetcher")
    def test_get_stock_list(self, mock_fetcher_cls, mock_load):
        mock_load.return_value = {}
        mock_fetcher = MagicMock()
        stock_list = pd.DataFrame({"code": ["000001"], "name": ["平安银行"]})
        mock_fetcher.fetch_stock_list.return_value = stock_list
        mock_fetcher_cls.return_value = mock_fetcher

        cache = ChartDataCache()
        result = cache.get_stock_list()

        assert not result.empty
        assert len(result) == 1


class TestChartDataCacheClose:

    @patch("modules.visualizer.data_cache.load_config")
    @patch("modules.visualizer.data_cache.DataFetcher")
    @patch("modules.visualizer.data_cache.DBManager")
    def test_close(self, mock_db_cls, mock_fetcher_cls, mock_load):
        mock_load.return_value = {}
        mock_fetcher = MagicMock()
        mock_db = MagicMock()
        mock_fetcher_cls.return_value = mock_fetcher
        mock_db_cls.return_value = mock_db

        cache = ChartDataCache()
        _ = cache.fetcher
        _ = cache.db_manager

        cache.close()

        assert cache._fetcher is None
        assert cache._db_manager is None
        mock_fetcher.close.assert_called_once()
        mock_db.close.assert_called_once()

    @patch("modules.visualizer.data_cache.load_config")
    def test_context_manager(self, mock_load):
        mock_load.return_value = {}
        with ChartDataCache() as cache:
            assert cache is not None
