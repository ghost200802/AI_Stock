from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.data_fetcher import DataFetcher


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
    }
    return config


@pytest.fixture
def fetcher(mock_config):
    mock_pro = MagicMock()
    with patch("src.data_fetcher.load_config", return_value=mock_config), \
         patch("src.data_fetcher.ts.pro_api", return_value=mock_pro), \
         patch("src.data_fetcher.bs.login"):
        df = DataFetcher()
        yield df
        df.close()


class TestFetchStockHistoryTushare:
    @patch("src.data_fetcher.DataFetcher._fetch_history_tushare")
    def test_calls_tushare_by_default(self, mock_ts, fetcher):
        mock_ts.return_value = pd.DataFrame({"trade_date": ["20250101"], "close": [10.0]})
        result = fetcher.fetch_stock_history("000001")
        mock_ts.assert_called_once()
        assert not result.empty

    @patch("src.data_fetcher.DataFetcher._fetch_history_tushare")
    def test_calls_tushare_explicit(self, mock_ts, fetcher):
        mock_ts.return_value = pd.DataFrame()
        fetcher.fetch_stock_history("000001", source="tushare")
        mock_ts.assert_called_once()

    @patch("src.data_fetcher.DataFetcher._fetch_history_tushare")
    def test_passes_params(self, mock_ts, fetcher):
        mock_ts.return_value = pd.DataFrame()
        fetcher.fetch_stock_history(
            "000001", start_date="2025-01-01", end_date="2025-06-30",
            source="tushare", period="weekly", adjust=None,
        )
        args, _ = mock_ts.call_args
        assert args[3] == "weekly"


class TestFetchStockHistoryBaostock:
    @patch("src.data_fetcher.DataFetcher._fetch_history_baostock")
    def test_calls_baostock(self, mock_bs, fetcher):
        mock_bs.return_value = pd.DataFrame({"date": ["2025-01-01"], "close": [10.0]})
        result = fetcher.fetch_stock_history("000001", source="baostock")
        mock_bs.assert_called_once()
        assert not result.empty


class TestFetchHistoryTushareInternal:
    def test_fetch_history_tushare_daily(self, fetcher):
        mock_df = pd.DataFrame({
            "trade_date": ["20250101", "20250102"],
            "close": [10.0, 11.0],
        })
        fetcher.pro_api.daily = MagicMock(return_value=mock_df)

        result = fetcher._fetch_history_tushare("000001", "2025-01-01", "2025-01-10", "daily", None)
        fetcher.pro_api.daily.assert_called_once_with(
            ts_code="000001.SZ",
            start_date="20250101",
            end_date="20250110",
        )
        assert len(result) == 2

    def test_fetch_history_tushare_weekly(self, fetcher):
        mock_df = pd.DataFrame({
            "trade_date": ["20250103"],
            "close": [11.0],
        })
        fetcher.pro_api.weekly = MagicMock(return_value=mock_df)

        result = fetcher._fetch_history_tushare("000001", "2025-01-01", "2025-01-10", "weekly", None)
        fetcher.pro_api.weekly.assert_called_once_with(
            ts_code="000001.SZ",
            start_date="20250101",
            end_date="20250110",
        )
        assert len(result) == 1

    def test_fetch_history_tushare_monthly(self, fetcher):
        mock_df = pd.DataFrame({
            "trade_date": ["20250131"],
            "close": [11.0],
        })
        fetcher.pro_api.monthly = MagicMock(return_value=mock_df)

        result = fetcher._fetch_history_tushare("000001", "2025-01-01", "2025-06-30", "monthly", None)
        fetcher.pro_api.monthly.assert_called_once_with(
            ts_code="000001.SZ",
            start_date="20250101",
            end_date="20250630",
        )
        assert len(result) == 1

    def test_fetch_history_tushare_exception(self, fetcher):
        fetcher.pro_api.daily = MagicMock(side_effect=Exception("network error"))

        result = fetcher._fetch_history_tushare("000001", "2025-01-01", "2025-01-10", "daily", None)
        assert result.empty


class TestFetchHistoryBaostockInternal:
    def test_fetch_history_baostock(self, fetcher):
        mock_rs = MagicMock()
        mock_rs.error_code = "0"
        mock_rs.fields = ["date", "open", "high", "low", "close", "volume"]
        mock_rs.next = MagicMock(side_effect=[True, True, False])
        mock_rs.get_row_data = MagicMock(side_effect=[
            ["2025-01-01", "10.0", "11.0", "9.5", "10.5", "1000"],
            ["2025-01-02", "10.5", "11.5", "10.0", "11.0", "1200"],
        ])

        with patch("src.data_fetcher.bs.query_history_k_data_plus", return_value=mock_rs):
            result = fetcher._fetch_history_baostock("000001", "2025-01-01", "2025-01-10", "daily", "3")

        assert len(result) == 2
        assert result["close"].iloc[1] == 11.0

    def test_fetch_history_baostock_code_format(self, fetcher):
        mock_rs = MagicMock()
        mock_rs.error_code = "0"
        mock_rs.fields = ["date", "close"]
        mock_rs.next = MagicMock(return_value=False)

        with patch("src.data_fetcher.bs.query_history_k_data_plus", return_value=mock_rs) as mock_query:
            fetcher._fetch_history_baostock("688001", "2025-01-01", "2025-01-10", "daily", "3")
            call_args = mock_query.call_args[0]
            assert call_args[0] == "sh.688001"

    def test_fetch_history_baostock_empty(self, fetcher):
        mock_rs = MagicMock()
        mock_rs.error_code = "0"
        mock_rs.fields = ["date", "close"]
        mock_rs.next = MagicMock(return_value=False)

        with patch("src.data_fetcher.bs.query_history_k_data_plus", return_value=mock_rs):
            result = fetcher._fetch_history_baostock("000001", "2025-01-01", "2025-01-10", "daily", "3")

        assert result.empty


class TestFetchRealtimeQuotes:
    def test_without_filter(self, fetcher):
        mock_df = pd.DataFrame({
            "ts_code": ["000001.SZ", "600036.SH"],
            "close": [10.0, 35.0],
        })
        fetcher.pro_api.daily = MagicMock(return_value=mock_df)

        result = fetcher.fetch_realtime_quotes()
        assert len(result) == 2

    def test_with_symbol_filter(self, fetcher):
        mock_df = pd.DataFrame({
            "ts_code": ["000001.SZ", "600036.SH", "000858.SZ"],
            "close": [10.0, 35.0, 150.0],
        })
        fetcher.pro_api.daily = MagicMock(return_value=mock_df)

        result = fetcher.fetch_realtime_quotes(symbols=["000001", "000858"])
        assert len(result) == 2
        codes = result["ts_code"].tolist()
        assert "000001.SZ" in codes
        assert "000858.SZ" in codes

    def test_empty_response(self, fetcher):
        fetcher.pro_api.daily = MagicMock(return_value=pd.DataFrame())

        result = fetcher.fetch_realtime_quotes()
        assert result.empty


class TestFetchFinancialData:
    def test_income_report(self, fetcher):
        mock_rs = MagicMock()
        mock_rs.error_code = "0"
        mock_rs.fields = ["code", "pubDate", "roeAvg", "npMargin"]
        mock_rs.next = MagicMock(side_effect=[True, False])
        mock_rs.get_row_data = MagicMock(return_value=["sh.601398", "2024-10-30", "12.5", "35.2"])

        with patch("src.data_fetcher.bs.query_profit_data", return_value=mock_rs) as mock_query:
            result = fetcher.fetch_financial_data("601398", report_type="income", year=2024, quarter=3)

        mock_query.assert_called_once_with(code="sh.601398", year=2024, quarter=3)
        assert len(result) == 1
        assert result["roeAvg"].iloc[0] == "12.5"

    def test_balance_report(self, fetcher):
        mock_rs = MagicMock()
        mock_rs.error_code = "0"
        mock_rs.fields = ["code", "totalAssets"]
        mock_rs.next = MagicMock(side_effect=[True, False])
        mock_rs.get_row_data = MagicMock(return_value=["sh.601398", "1000000"])

        with patch("src.data_fetcher.bs.query_balance_data", return_value=mock_rs):
            result = fetcher.fetch_financial_data("601398", report_type="balance", year=2024, quarter=3)

        assert len(result) == 1

    def test_cashflow_report(self, fetcher):
        mock_rs = MagicMock()
        mock_rs.error_code = "0"
        mock_rs.fields = ["code", "netCashFlow"]
        mock_rs.next = MagicMock(side_effect=[True, False])
        mock_rs.get_row_data = MagicMock(return_value=["sh.601398", "500000"])

        with patch("src.data_fetcher.bs.query_cash_flow_data", return_value=mock_rs):
            result = fetcher.fetch_financial_data("601398", report_type="cashflow", year=2024, quarter=3)

        assert len(result) == 1

    def test_unsupported_report_type(self, fetcher):
        result = fetcher.fetch_financial_data("601398", report_type="unknown")
        assert result.empty

    def test_default_year_quarter(self, fetcher):
        mock_rs = MagicMock()
        mock_rs.error_code = "0"
        mock_rs.fields = ["code"]
        mock_rs.next = MagicMock(return_value=False)

        with patch("src.data_fetcher.bs.query_profit_data", return_value=mock_rs) as mock_query:
            fetcher.fetch_financial_data("601398")

        call_kwargs = mock_query.call_args[1]
        assert isinstance(call_kwargs["year"], int)
        assert isinstance(call_kwargs["quarter"], int)


class TestFetchStockList:
    def test_fetch_stock_list(self, fetcher):
        mock_df = pd.DataFrame({
            "ts_code": ["000001.SZ", "600036.SH"],
            "symbol": ["000001", "600036"],
            "name": ["平安银行", "招商银行"],
        })
        fetcher.pro_api.stock_basic = MagicMock(return_value=mock_df)

        result = fetcher.fetch_stock_list()
        assert len(result) == 2
        assert list(result.columns) == ["code", "name"]
        assert result["code"].iloc[0] == "000001"

    def test_fetch_stock_list_empty(self, fetcher):
        fetcher.pro_api.stock_basic = MagicMock(return_value=pd.DataFrame())

        result = fetcher.fetch_stock_list()
        assert result.empty


class TestContextManager:
    def test_context_manager(self, mock_config):
        mock_pro = MagicMock()
        with patch("src.data_fetcher.load_config", return_value=mock_config), \
             patch("src.data_fetcher.ts.pro_api", return_value=mock_pro), \
             patch("src.data_fetcher.bs.login"), \
             patch("src.data_fetcher.bs.logout") as mock_logout:
            with DataFetcher() as fetcher:
                assert isinstance(fetcher, DataFetcher)
            mock_logout.assert_called_once()

    def test_close(self, mock_config):
        mock_pro = MagicMock()
        with patch("src.data_fetcher.load_config", return_value=mock_config), \
             patch("src.data_fetcher.ts.pro_api", return_value=mock_pro), \
             patch("src.data_fetcher.bs.login"), \
             patch("src.data_fetcher.bs.logout") as mock_logout:
            fetcher = DataFetcher()
            fetcher.close()
            mock_logout.assert_called_once()

    def test_close_exception_safe(self, mock_config):
        mock_pro = MagicMock()
        with patch("src.data_fetcher.load_config", return_value=mock_config), \
             patch("src.data_fetcher.ts.pro_api", return_value=mock_pro), \
             patch("src.data_fetcher.bs.login"), \
             patch("src.data_fetcher.bs.logout", side_effect=Exception("logout error")):
            fetcher = DataFetcher()
            fetcher.close()


class TestUnsupportedSource:
    def test_fetch_stock_history_unsupported(self, fetcher):
        with pytest.raises(ValueError, match="不支持的数据源"):
            fetcher.fetch_stock_history("000001", source="akshare")
