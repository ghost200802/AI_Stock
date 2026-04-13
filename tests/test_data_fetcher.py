from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from lib.data_fetcher import DataFetcher


@pytest.fixture
def mock_config(tmp_path):
    token_file = tmp_path / "tushare_token.txt"
    token_file.write_text("test_token_123", encoding="utf-8")
    config = {
        "data_source": {
            "default": "tushare",
            "tushare": {"token_file": str(token_file)},
            "baostock": {"adjust": "3"},
        },
        "default_dates": {"start": "2020-01-01", "end": "2026-12-31"},
        "mongodb": {"host": "localhost", "port": 27017, "database": "DB_Stock"},
    }
    return config


@pytest.fixture
def mock_db_manager():
    mgr = MagicMock()
    mgr.find_to_dataframe.return_value = pd.DataFrame()
    mgr.upsert_many.return_value = None
    return mgr


@pytest.fixture
def fetcher_no_cache(mock_config):
    mock_pro = MagicMock()
    with patch("lib.data_fetcher.load_config", return_value=mock_config), \
         patch("lib.data_fetcher.ts.pro_api", return_value=mock_pro), \
         patch("lib.data_fetcher.bs.login"):
        df = DataFetcher(use_cache=False)
        yield df
        df.close()


@pytest.fixture
def fetcher_with_cache(mock_config, mock_db_manager):
    mock_pro = MagicMock()
    with patch("lib.data_fetcher.load_config", return_value=mock_config), \
         patch("lib.data_fetcher.ts.pro_api", return_value=mock_pro), \
         patch("lib.data_fetcher.bs.login"), \
         patch("lib.data_fetcher.DBManager", return_value=mock_db_manager):
        df = DataFetcher(use_cache=True)
        yield df, mock_db_manager
        df.close()


class TestFetchStockHistoryTushare:
    @patch("lib.data_fetcher.DataFetcher._fetch_history_tushare")
    def test_calls_tushare_by_default(self, mock_ts, fetcher_no_cache):
        mock_ts.return_value = pd.DataFrame({"trade_date": ["20250101"], "close": [10.0]})
        result = fetcher_no_cache.fetch_stock_history("000001")
        mock_ts.assert_called_once()
        assert not result.empty

    @patch("lib.data_fetcher.DataFetcher._fetch_history_tushare")
    def test_calls_tushare_explicit(self, mock_ts, fetcher_no_cache):
        mock_ts.return_value = pd.DataFrame()
        fetcher_no_cache.fetch_stock_history("000001", source="tushare")
        mock_ts.assert_called_once()

    @patch("lib.data_fetcher.DataFetcher._fetch_history_tushare")
    def test_passes_params(self, mock_ts, fetcher_no_cache):
        mock_ts.return_value = pd.DataFrame()
        fetcher_no_cache.fetch_stock_history(
            "000001", start_date="2025-01-01", end_date="2025-06-30",
            source="tushare", period="weekly", adjust=None,
        )
        args, _ = mock_ts.call_args
        assert args[3] == "weekly"


class TestFetchStockHistoryBaostock:
    @patch("lib.data_fetcher.DataFetcher._fetch_history_baostock")
    def test_calls_baostock(self, mock_bs, fetcher_no_cache):
        mock_bs.return_value = pd.DataFrame({"trade_date": ["2025-01-01"], "close": [10.0]})
        result = fetcher_no_cache.fetch_stock_history("000001", source="baostock")
        mock_bs.assert_called_once()
        assert not result.empty


class TestFetchHistoryTushareInternal:
    def test_fetch_history_tushare_daily(self, fetcher_no_cache):
        mock_df = pd.DataFrame({
            "trade_date": ["20250101", "20250102"],
            "close": [10.0, 11.0],
        })
        fetcher_no_cache.pro_api.daily = MagicMock(return_value=mock_df)

        result = fetcher_no_cache._fetch_history_tushare("000001", "2025-01-01", "2025-01-10", "daily", None)
        fetcher_no_cache.pro_api.daily.assert_called_once_with(
            ts_code="000001.SZ",
            start_date="20250101",
            end_date="20250110",
        )
        assert len(result) == 2

    def test_fetch_history_tushare_weekly(self, fetcher_no_cache):
        mock_df = pd.DataFrame({
            "trade_date": ["20250103"],
            "close": [11.0],
        })
        fetcher_no_cache.pro_api.weekly = MagicMock(return_value=mock_df)

        result = fetcher_no_cache._fetch_history_tushare("000001", "2025-01-01", "2025-01-10", "weekly", None)
        fetcher_no_cache.pro_api.weekly.assert_called_once_with(
            ts_code="000001.SZ",
            start_date="20250101",
            end_date="20250110",
        )
        assert len(result) == 1

    def test_fetch_history_tushare_monthly(self, fetcher_no_cache):
        mock_df = pd.DataFrame({
            "trade_date": ["20250131"],
            "close": [11.0],
        })
        fetcher_no_cache.pro_api.monthly = MagicMock(return_value=mock_df)

        result = fetcher_no_cache._fetch_history_tushare("000001", "2025-01-01", "2025-06-30", "monthly", None)
        fetcher_no_cache.pro_api.monthly.assert_called_once_with(
            ts_code="000001.SZ",
            start_date="20250101",
            end_date="20250630",
        )
        assert len(result) == 1

    def test_fetch_history_tushare_exception(self, fetcher_no_cache):
        fetcher_no_cache.pro_api.daily = MagicMock(side_effect=Exception("network error"))

        result = fetcher_no_cache._fetch_history_tushare("000001", "2025-01-01", "2025-01-10", "daily", None)
        assert result.empty


class TestFetchHistoryBaostockInternal:
    def test_fetch_history_baostock(self, fetcher_no_cache):
        mock_rs = MagicMock()
        mock_rs.error_code = "0"
        mock_rs.fields = ["date", "open", "high", "low", "close", "volume"]
        mock_rs.next = MagicMock(side_effect=[True, True, False])
        mock_rs.get_row_data = MagicMock(side_effect=[
            ["2025-01-01", "10.0", "11.0", "9.5", "10.5", "1000"],
            ["2025-01-02", "10.5", "11.5", "10.0", "11.0", "1200"],
        ])

        with patch("lib.data_fetcher.bs.query_history_k_data_plus", return_value=mock_rs):
            result = fetcher_no_cache._fetch_history_baostock("000001", "2025-01-01", "2025-01-10", "daily", "3")

        assert len(result) == 2
        assert "trade_date" in result.columns

    def test_fetch_history_baostock_code_format(self, fetcher_no_cache):
        mock_rs = MagicMock()
        mock_rs.error_code = "0"
        mock_rs.fields = ["date", "close"]
        mock_rs.next = MagicMock(return_value=False)

        with patch("lib.data_fetcher.bs.query_history_k_data_plus", return_value=mock_rs) as mock_query:
            fetcher_no_cache._fetch_history_baostock("688001", "2025-01-01", "2025-01-10", "daily", "3")
            call_args = mock_query.call_args[0]
            assert call_args[0] == "sh.688001"

    def test_fetch_history_baostock_empty(self, fetcher_no_cache):
        mock_rs = MagicMock()
        mock_rs.error_code = "0"
        mock_rs.fields = ["date", "close"]
        mock_rs.next = MagicMock(return_value=False)

        with patch("lib.data_fetcher.bs.query_history_k_data_plus", return_value=mock_rs):
            result = fetcher_no_cache._fetch_history_baostock("000001", "2025-01-01", "2025-01-10", "daily", "3")

        assert result.empty


class TestFetchRealtimeQuotes:
    def test_without_filter(self, fetcher_no_cache):
        mock_df = pd.DataFrame({
            "ts_code": ["000001.SZ", "600036.SH"],
            "close": [10.0, 35.0],
        })
        fetcher_no_cache.pro_api.daily = MagicMock(return_value=mock_df)

        result = fetcher_no_cache.fetch_realtime_quotes()
        assert len(result) == 2

    def test_with_symbol_filter(self, fetcher_no_cache):
        mock_df = pd.DataFrame({
            "ts_code": ["000001.SZ", "600036.SH", "000858.SZ"],
            "close": [10.0, 35.0, 150.0],
        })
        fetcher_no_cache.pro_api.daily = MagicMock(return_value=mock_df)

        result = fetcher_no_cache.fetch_realtime_quotes(symbols=["000001", "000858"])
        assert len(result) == 2
        codes = result["ts_code"].tolist()
        assert "000001.SZ" in codes
        assert "000858.SZ" in codes

    def test_empty_response(self, fetcher_no_cache):
        fetcher_no_cache.pro_api.daily = MagicMock(return_value=pd.DataFrame())

        result = fetcher_no_cache.fetch_realtime_quotes()
        assert result.empty


class TestFetchFinancialData:
    def test_income_report(self, fetcher_no_cache):
        mock_rs = MagicMock()
        mock_rs.error_code = "0"
        mock_rs.fields = ["code", "pubDate", "roeAvg", "npMargin"]
        mock_rs.next = MagicMock(side_effect=[True, False])
        mock_rs.get_row_data = MagicMock(return_value=["sh.601398", "2024-10-30", "12.5", "35.2"])

        with patch("lib.data_fetcher.bs.query_profit_data", return_value=mock_rs) as mock_query:
            result = fetcher_no_cache.fetch_financial_data("601398", report_type="income", year=2024, quarter=3)

        mock_query.assert_called_once_with(code="sh.601398", year=2024, quarter=3)
        assert len(result) == 1
        assert result["roeAvg"].iloc[0] == "12.5"

    def test_balance_report(self, fetcher_no_cache):
        mock_rs = MagicMock()
        mock_rs.error_code = "0"
        mock_rs.fields = ["code", "totalAssets"]
        mock_rs.next = MagicMock(side_effect=[True, False])
        mock_rs.get_row_data = MagicMock(return_value=["sh.601398", "1000000"])

        with patch("lib.data_fetcher.bs.query_balance_data", return_value=mock_rs):
            result = fetcher_no_cache.fetch_financial_data("601398", report_type="balance", year=2024, quarter=3)

        assert len(result) == 1

    def test_cashflow_report(self, fetcher_no_cache):
        mock_rs = MagicMock()
        mock_rs.error_code = "0"
        mock_rs.fields = ["code", "netCashFlow"]
        mock_rs.next = MagicMock(side_effect=[True, False])
        mock_rs.get_row_data = MagicMock(return_value=["sh.601398", "500000"])

        with patch("lib.data_fetcher.bs.query_cash_flow_data", return_value=mock_rs):
            result = fetcher_no_cache.fetch_financial_data("601398", report_type="cashflow", year=2024, quarter=3)

        assert len(result) == 1

    def test_unsupported_report_type(self, fetcher_no_cache):
        result = fetcher_no_cache.fetch_financial_data("601398", report_type="unknown")
        assert result.empty

    def test_default_year_quarter(self, fetcher_no_cache):
        mock_rs = MagicMock()
        mock_rs.error_code = "0"
        mock_rs.fields = ["code"]
        mock_rs.next = MagicMock(return_value=False)

        with patch("lib.data_fetcher.bs.query_profit_data", return_value=mock_rs) as mock_query:
            fetcher_no_cache.fetch_financial_data("601398")

        call_kwargs = mock_query.call_args[1]
        assert isinstance(call_kwargs["year"], int)
        assert isinstance(call_kwargs["quarter"], int)


class TestFetchStockList:
    def test_fetch_stock_list(self, fetcher_no_cache):
        mock_df = pd.DataFrame({
            "ts_code": ["000001.SZ", "600036.SH"],
            "symbol": ["000001", "600036"],
            "name": ["平安银行", "招商银行"],
        })
        fetcher_no_cache.pro_api.stock_basic = MagicMock(return_value=mock_df)

        result = fetcher_no_cache.fetch_stock_list()
        assert len(result) == 2
        assert list(result.columns) == ["code", "name"]
        assert result["code"].iloc[0] == "000001"

    def test_fetch_stock_list_empty(self, fetcher_no_cache):
        fetcher_no_cache.pro_api.stock_basic = MagicMock(return_value=pd.DataFrame())

        result = fetcher_no_cache.fetch_stock_list()
        assert result.empty


class TestGetStockDataWithCache:
    def test_cache_miss_full_fetch(self, fetcher_with_cache):
        fetcher, mock_db = fetcher_with_cache
        mock_db.find_to_dataframe.return_value = pd.DataFrame()

        api_df = pd.DataFrame({
            "trade_date": ["20250101", "20250102"],
            "close": [10.0, 11.0],
        })
        with patch.object(fetcher, "_fetch_from_api", return_value=api_df):
            result = fetcher.get_stock_data("000001", "daily", "2025-01-01", "2025-01-02")

        assert mock_db.upsert_many.called
        assert mock_db.find_to_dataframe.call_count == 2

    def test_cache_hit_return_directly(self, fetcher_with_cache):
        fetcher, mock_db = fetcher_with_cache
        cached_df = pd.DataFrame({
            "trade_date": ["20250101", "20250102", "20250103"],
            "close": [10.0, 11.0, 12.0],
        })
        mock_db.find_to_dataframe.return_value = cached_df

        result = fetcher.get_stock_data("000001", "daily", "2025-01-01", "2025-01-03")

        assert len(result) == 3
        assert not mock_db.upsert_many.called

    def test_cache_query_no_source_filter(self, fetcher_with_cache):
        fetcher, mock_db = fetcher_with_cache
        mock_db.find_to_dataframe.return_value = pd.DataFrame()

        with patch.object(fetcher, "_fetch_from_api", return_value=pd.DataFrame()):
            fetcher.get_stock_data("000001", "daily", "2025-01-01", "2025-01-01")

        call_args = mock_db.find_to_dataframe.call_args
        query = call_args[0][1] if len(call_args[0]) > 1 else call_args[1].get("query", call_args[0][0] if call_args[0] else {})
        assert "source" not in query

    def test_incremental_update(self, fetcher_with_cache):
        fetcher, mock_db = fetcher_with_cache
        cached_df = pd.DataFrame({
            "trade_date": ["20250101", "20250102"],
            "close": [10.0, 11.0],
        })
        mock_db.find_to_dataframe.return_value = cached_df

        new_df = pd.DataFrame({
            "trade_date": ["20250103", "20250104"],
            "close": [12.0, 13.0],
        })
        full_df = pd.DataFrame({
            "trade_date": ["20250101", "20250102", "20250103", "20250104"],
            "close": [10.0, 11.0, 12.0, 13.0],
        })
        mock_db.find_to_dataframe.side_effect = [cached_df, full_df]

        with patch.object(fetcher, "_fetch_from_api", return_value=new_df):
            result = fetcher.get_stock_data("000001", "daily", "2025-01-01", "2025-01-04")

        assert mock_db.upsert_many.called
        assert mock_db.find_to_dataframe.call_count == 2

    def test_no_cache_mode(self, fetcher_no_cache):
        api_df = pd.DataFrame({
            "trade_date": ["20250101"],
            "close": [10.0],
        })
        with patch.object(fetcher_no_cache, "_fetch_from_api", return_value=api_df):
            result = fetcher_no_cache.get_stock_data("000001", "daily", "2025-01-01", "2025-01-01")

        assert len(result) == 1


class TestBatchFetchMarketDaily:
    def test_batch_fetch(self, fetcher_with_cache):
        fetcher, mock_db = fetcher_with_cache
        mock_df = pd.DataFrame({
            "ts_code": ["000001.SZ", "600036.SH"],
            "trade_date": ["20250101", "20250101"],
            "close": [10.0, 35.0],
        })
        fetcher.pro_api.daily = MagicMock(return_value=mock_df)

        result = fetcher.batch_fetch_market_daily("2025-01-01")
        assert len(result) == 2
        assert mock_db.upsert_many.call_count == 2


class TestContextManager:
    def test_context_manager_no_cache(self, mock_config):
        mock_pro = MagicMock()
        with patch("lib.data_fetcher.load_config", return_value=mock_config), \
             patch("lib.data_fetcher.ts.pro_api", return_value=mock_pro), \
             patch("lib.data_fetcher.bs.login"), \
             patch("lib.data_fetcher.bs.logout") as mock_logout:
            with DataFetcher(use_cache=False) as fetcher:
                assert isinstance(fetcher, DataFetcher)
            mock_logout.assert_called_once()

    def test_close_no_cache(self, mock_config):
        mock_pro = MagicMock()
        with patch("lib.data_fetcher.load_config", return_value=mock_config), \
             patch("lib.data_fetcher.ts.pro_api", return_value=mock_pro), \
             patch("lib.data_fetcher.bs.login"), \
             patch("lib.data_fetcher.bs.logout") as mock_logout:
            fetcher = DataFetcher(use_cache=False)
            fetcher.close()
            mock_logout.assert_called_once()

    def test_close_with_db_manager(self, mock_config, mock_db_manager):
        mock_pro = MagicMock()
        with patch("lib.data_fetcher.load_config", return_value=mock_config), \
             patch("lib.data_fetcher.ts.pro_api", return_value=mock_pro), \
             patch("lib.data_fetcher.bs.login"), \
             patch("lib.data_fetcher.bs.logout"), \
             patch("lib.data_fetcher.DBManager", return_value=mock_db_manager):
            fetcher = DataFetcher(use_cache=True)
            fetcher.close()
            mock_db_manager.close.assert_called_once()

    def test_mongodb_init_failure_fallback(self, mock_config):
        mock_pro = MagicMock()
        with patch("lib.data_fetcher.load_config", return_value=mock_config), \
             patch("lib.data_fetcher.ts.pro_api", return_value=mock_pro), \
             patch("lib.data_fetcher.bs.login"), \
             patch("lib.data_fetcher.DBManager", side_effect=Exception("MongoDB not available")):
            fetcher = DataFetcher(use_cache=True)
            assert fetcher.use_cache is False
            assert fetcher.db_manager is None
            fetcher.close()


class TestUnsupportedSource:
    def test_fetch_stock_history_unsupported(self, fetcher_no_cache):
        with pytest.raises(ValueError, match="不支持的数据�?):
            fetcher_no_cache.fetch_stock_history("000001", source="akshare")

    def test_unsupported_data_type(self, fetcher_no_cache):
        with pytest.raises(ValueError, match="不支持的数据类型"):
            fetcher_no_cache._fetch_from_api("000001", "unknown_type", "2025-01-01", "2025-01-01", "tushare")
