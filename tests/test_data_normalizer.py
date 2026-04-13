import math

import numpy as np
import pandas as pd
import pytest

from lib.data_normalizer import (
    BAOSTOCK_FIELD_MAP,
    FLOAT_COLUMNS,
    STRING_COLUMNS,
    TUSHARE_FIELD_MAP,
    UNIFIED_DAILY_COLUMNS,
    merge_records,
    normalize_daily,
)


class TestNormalizeDailyTushare:
    def test_vol_renamed_to_volume(self):
        df = pd.DataFrame({
            "trade_date": ["20250101"],
            "ts_code": ["000001.SZ"],
            "vol": [1000.0],
            "close": [10.0],
        })
        result = normalize_daily(df, "tushare")
        assert "volume" in result.columns
        assert "vol" not in result.columns
        assert result["volume"].iloc[0] == 1000.0

    def test_missing_columns_filled_with_nan(self):
        df = pd.DataFrame({
            "trade_date": ["20250101"],
            "ts_code": ["000001.SZ"],
            "close": [10.0],
        })
        result = normalize_daily(df, "tushare")
        for col in ["open", "high", "low", "volume", "amount", "pct_chg", "change",
                     "pre_close", "turn", "pe_ttm", "pb_mrq", "ps_ttm", "pcf_ncf_ttm"]:
            assert math.isnan(result[col].iloc[0])

    def test_existing_columns_preserved(self):
        df = pd.DataFrame({
            "trade_date": ["20250101"],
            "ts_code": ["000001.SZ"],
            "open": [10.0],
            "high": [11.0],
            "low": [9.5],
            "close": [10.5],
            "vol": [1000.0],
            "amount": [10500.0],
            "pct_chg": [5.0],
            "change": [0.5],
            "pre_close": [10.0],
        })
        result = normalize_daily(df, "tushare")
        assert result["open"].iloc[0] == 10.0
        assert result["high"].iloc[0] == 11.0
        assert result["low"].iloc[0] == 9.5
        assert result["close"].iloc[0] == 10.5
        assert result["volume"].iloc[0] == 1000.0
        assert result["amount"].iloc[0] == 10500.0
        assert result["pct_chg"].iloc[0] == 5.0
        assert result["change"].iloc[0] == 0.5
        assert result["pre_close"].iloc[0] == 10.0


class TestNormalizeDailyBaostock:
    def test_camel_case_to_snake_case(self):
        df = pd.DataFrame({
            "date": ["2025-01-01"],
            "open": ["10.0"],
            "close": ["10.5"],
            "peTTM": ["15.5"],
            "pbMRQ": ["1.2"],
            "psTTM": ["3.0"],
            "pcfNcfTTM": ["8.0"],
            "isST": ["1"],
        })
        result = normalize_daily(df, "baostock")
        assert "pe_ttm" in result.columns
        assert "pb_mrq" in result.columns
        assert "ps_ttm" in result.columns
        assert "pcf_ncf_ttm" in result.columns
        assert "is_st" in result.columns
        assert "peTTM" not in result.columns
        assert "pbMRQ" not in result.columns

    def test_date_renamed_to_trade_date(self):
        df = pd.DataFrame({
            "date": ["2025-01-01"],
            "close": ["10.5"],
        })
        result = normalize_daily(df, "baostock")
        assert "trade_date" in result.columns

    def test_missing_columns_filled_with_nan(self):
        df = pd.DataFrame({
            "date": ["2025-01-01"],
            "close": ["10.5"],
        })
        result = normalize_daily(df, "baostock")
        for col in ["open", "high", "low", "volume", "amount", "pct_chg", "change",
                     "pre_close", "turn", "pe_ttm", "pb_mrq", "ps_ttm", "pcf_ncf_ttm"]:
            assert math.isnan(result[col].iloc[0])

    def test_baostock_string_values_converted_to_float(self):
        df = pd.DataFrame({
            "date": ["2025-01-01"],
            "open": ["10.5"],
            "high": ["11.0"],
            "low": ["10.0"],
            "close": ["10.5"],
            "volume": ["1000"],
            "amount": ["10500"],
        })
        result = normalize_daily(df, "baostock")
        assert isinstance(result["open"].iloc[0], (float, np.floating))
        assert isinstance(result["close"].iloc[0], (float, np.floating))
        assert isinstance(result["volume"].iloc[0], (int, float, np.integer, np.floating))


class TestNormalizeDailyEmpty:
    def test_empty_dataframe_returns_empty(self):
        df = pd.DataFrame()
        result = normalize_daily(df, "tushare")
        assert result.empty
        assert list(result.columns) == UNIFIED_DAILY_COLUMNS

    def test_empty_dataframe_baostock(self):
        df = pd.DataFrame()
        result = normalize_daily(df, "baostock")
        assert result.empty
        assert list(result.columns) == UNIFIED_DAILY_COLUMNS


class TestNormalizeDailyColumnOrder:
    def test_column_order_matches_schema(self):
        df = pd.DataFrame({
            "trade_date": ["20250101"],
            "close": [10.0],
        })
        result = normalize_daily(df, "tushare")
        assert list(result.columns) == UNIFIED_DAILY_COLUMNS

    def test_column_order_baostock(self):
        df = pd.DataFrame({
            "date": ["2025-01-01"],
            "close": ["10.5"],
        })
        result = normalize_daily(df, "baostock")
        assert list(result.columns) == UNIFIED_DAILY_COLUMNS


class TestNormalizeDailyTypes:
    def test_float_columns_are_float(self):
        df = pd.DataFrame({
            "trade_date": ["20250101"],
            "close": [10.0],
            "open": [10.0],
            "volume": [1000.0],
        })
        result = normalize_daily(df, "tushare")
        for col in FLOAT_COLUMNS:
            assert pd.api.types.is_float_dtype(result[col]), f"Column {col} should be float"

    def test_string_columns_are_string(self):
        df = pd.DataFrame({
            "trade_date": ["20250101"],
            "close": [10.0],
        })
        result = normalize_daily(df, "tushare")
        for col in ["ts_code", "trade_date", "data_type", "source", "update_time"]:
            assert pd.api.types.is_string_dtype(result[col]) or pd.api.types.is_object_dtype(result[col]), \
                f"Column {col} should be string"

    def test_is_st_default_empty_string(self):
        df = pd.DataFrame({
            "trade_date": ["20250101"],
            "close": [10.0],
        })
        result = normalize_daily(df, "tushare")
        assert result["is_st"].iloc[0] == ""


class TestMergeRecords:
    def test_high_priority_overrides_low(self):
        existing = pd.DataFrame([
            {"ts_code": "000001.SZ", "data_type": "daily", "trade_date": "20250101",
             "close": 10.0, "source": "baostock", "open": 9.5, "high": 10.5,
             "low": 9.0, "volume": 1000.0, "amount": 10000.0, "pct_chg": 5.0,
             "change": 0.5, "pre_close": 9.5, "turn": 1.0, "pe_ttm": 15.0,
             "pb_mrq": 1.2, "ps_ttm": 3.0, "pcf_ncf_ttm": 8.0, "is_st": "N",
             "update_time": ""},
        ])
        incoming = pd.DataFrame([
            {"ts_code": "000001.SZ", "data_type": "daily", "trade_date": "20250101",
             "close": 10.5, "source": "tushare", "open": 10.0, "high": 11.0,
             "low": 9.5, "volume": 1200.0, "amount": 12600.0, "pct_chg": 5.26,
             "change": 0.5, "pre_close": 10.0, "turn": 1.2, "pe_ttm": 16.0,
             "pb_mrq": 1.3, "ps_ttm": 3.2, "pcf_ncf_ttm": 8.5, "is_st": "N",
             "update_time": ""},
        ])
        result = merge_records(existing, incoming, priority=["tushare", "baostock"])
        assert len(result) == 1
        assert result["close"].iloc[0] == 10.5
        assert result["source"].iloc[0] == "tushare"

    def test_low_priority_does_not_override_high(self):
        existing = pd.DataFrame([
            {"ts_code": "000001.SZ", "data_type": "daily", "trade_date": "20250101",
             "close": 10.5, "source": "tushare", "open": 10.0, "high": 11.0,
             "low": 9.5, "volume": 1200.0, "amount": 12600.0, "pct_chg": 5.26,
             "change": 0.5, "pre_close": 10.0, "turn": 1.2, "pe_ttm": 16.0,
             "pb_mrq": 1.3, "ps_ttm": 3.2, "pcf_ncf_ttm": 8.5, "is_st": "N",
             "update_time": ""},
        ])
        incoming = pd.DataFrame([
            {"ts_code": "000001.SZ", "data_type": "daily", "trade_date": "20250101",
             "close": 10.0, "source": "baostock", "open": 9.5, "high": 10.5,
             "low": 9.0, "volume": 1000.0, "amount": 10000.0, "pct_chg": 5.0,
             "change": 0.5, "pre_close": 9.5, "turn": 1.0, "pe_ttm": 15.0,
             "pb_mrq": 1.2, "ps_ttm": 3.0, "pcf_ncf_ttm": 8.0, "is_st": "N",
             "update_time": ""},
        ])
        result = merge_records(existing, incoming, priority=["tushare", "baostock"])
        assert len(result) == 1
        assert result["close"].iloc[0] == 10.5
        assert result["source"].iloc[0] == "tushare"

    def test_different_dates_both_kept(self):
        existing = pd.DataFrame([
            {"ts_code": "000001.SZ", "data_type": "daily", "trade_date": "20250101",
             "close": 10.0, "source": "tushare", "open": 10.0, "high": 11.0,
             "low": 9.5, "volume": 1000.0, "amount": 10500.0, "pct_chg": 0.0,
             "change": 0.0, "pre_close": 10.0, "turn": 1.0, "pe_ttm": 15.0,
             "pb_mrq": 1.2, "ps_ttm": 3.0, "pcf_ncf_ttm": 8.0, "is_st": "N",
             "update_time": ""},
        ])
        incoming = pd.DataFrame([
            {"ts_code": "000001.SZ", "data_type": "daily", "trade_date": "20250102",
             "close": 11.0, "source": "baostock", "open": 10.0, "high": 11.5,
             "low": 10.0, "volume": 1200.0, "amount": 13200.0, "pct_chg": 10.0,
             "change": 1.0, "pre_close": 10.0, "turn": 1.2, "pe_ttm": 16.5,
             "pb_mrq": 1.3, "ps_ttm": 3.3, "pcf_ncf_ttm": 8.8, "is_st": "N",
             "update_time": ""},
        ])
        result = merge_records(existing, incoming, priority=["tushare", "baostock"])
        assert len(result) == 2

    def test_empty_existing(self):
        incoming = pd.DataFrame([
            {"ts_code": "000001.SZ", "data_type": "daily", "trade_date": "20250101",
             "close": 10.0, "source": "tushare", "open": 10.0, "high": 11.0,
             "low": 9.5, "volume": 1000.0, "amount": 10500.0, "pct_chg": 0.0,
             "change": 0.0, "pre_close": 10.0, "turn": 1.0, "pe_ttm": 15.0,
             "pb_mrq": 1.2, "ps_ttm": 3.0, "pcf_ncf_ttm": 8.0, "is_st": "N",
             "update_time": ""},
        ])
        result = merge_records(pd.DataFrame(), incoming, priority=["tushare", "baostock"])
        assert len(result) == 1
        assert result["source"].iloc[0] == "tushare"

    def test_empty_incoming(self):
        existing = pd.DataFrame([
            {"ts_code": "000001.SZ", "data_type": "daily", "trade_date": "20250101",
             "close": 10.0, "source": "baostock", "open": 10.0, "high": 11.0,
             "low": 9.5, "volume": 1000.0, "amount": 10500.0, "pct_chg": 0.0,
             "change": 0.0, "pre_close": 10.0, "turn": 1.0, "pe_ttm": 15.0,
             "pb_mrq": 1.2, "ps_ttm": 3.0, "pcf_ncf_ttm": 8.0, "is_st": "N",
             "update_time": ""},
        ])
        result = merge_records(existing, pd.DataFrame(), priority=["tushare", "baostock"])
        assert len(result) == 1
        assert result["source"].iloc[0] == "baostock"


class TestConstants:
    def test_unified_daily_columns_is_list(self):
        assert isinstance(UNIFIED_DAILY_COLUMNS, list)
        assert len(UNIFIED_DAILY_COLUMNS) == 20

    def test_tushare_field_map(self):
        assert "vol" in TUSHARE_FIELD_MAP
        assert TUSHARE_FIELD_MAP["vol"] == "volume"

    def test_baostock_field_map(self):
        assert BAOSTOCK_FIELD_MAP["peTTM"] == "pe_ttm"
        assert BAOSTOCK_FIELD_MAP["pbMRQ"] == "pb_mrq"
        assert BAOSTOCK_FIELD_MAP["psTTM"] == "ps_ttm"
        assert BAOSTOCK_FIELD_MAP["pcfNcfTTM"] == "pcf_ncf_ttm"
        assert BAOSTOCK_FIELD_MAP["isST"] == "is_st"

    def test_float_and_string_columns_no_overlap(self):
        assert FLOAT_COLUMNS.isdisjoint(STRING_COLUMNS)
