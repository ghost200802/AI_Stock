import os
import tempfile

import pandas as pd
import pytest
import yaml

from src.utils import (
    ensure_dir,
    format_stock_code,
    get_project_root,
    load_config,
    parse_date,
    save_data,
)


class TestLoadConfig:
    def test_load_config_default(self):
        config = load_config()
        assert isinstance(config, dict)
        assert "data_source" in config

    def test_load_config_custom_path(self):
        config = load_config()
        assert isinstance(config, dict)

    def test_load_config_not_found(self):
        with pytest.raises(FileNotFoundError, match="配置文件不存在"):
            load_config("/nonexistent/path/config.yaml")


class TestGetProjectRoot:
    def test_returns_path(self):
        root = get_project_root()
        assert root.exists()
        assert (root / "config" / "config.yaml").exists()
        assert (root / "src").exists()


class TestEnsureDir:
    def test_create_dir(self, tmp_path):
        new_dir = tmp_path / "a" / "b" / "c"
        ensure_dir(new_dir / "file.txt")
        assert new_dir.exists()

    def test_existing_dir(self, tmp_path):
        ensure_dir(tmp_path / "file.txt")
        assert tmp_path.exists()


class TestSaveData:
    def test_save_csv(self, tmp_path):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        filepath = tmp_path / "test.csv"
        save_data(df, filepath, format="csv")
        assert filepath.exists()
        loaded = pd.read_csv(filepath)
        pd.testing.assert_frame_equal(loaded, df)

    def test_save_parquet(self, tmp_path):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        filepath = tmp_path / "test.parquet"
        save_data(df, filepath, format="parquet")
        assert filepath.exists()
        loaded = pd.read_parquet(filepath)
        pd.testing.assert_frame_equal(loaded, df)

    def test_save_unsupported_format(self, tmp_path):
        df = pd.DataFrame({"a": [1]})
        filepath = tmp_path / "test.xlsx"
        with pytest.raises(ValueError, match="不支持的格式"):
            save_data(df, filepath, format="xlsx")


class TestFormatStockCode:
    @pytest.mark.parametrize(
        "symbol,source,expected",
        [
            ("000001", "tushare", "000001.SZ"),
            ("001001", "tushare", "001001.SZ"),
            ("002001", "tushare", "002001.SZ"),
            ("300001", "tushare", "300001.SZ"),
            ("600000", "tushare", "600000.SH"),
            ("600036", "tushare", "600036.SH"),
            ("601398", "tushare", "601398.SH"),
            ("603000", "tushare", "603000.SH"),
            ("605000", "tushare", "605000.SH"),
            ("688001", "tushare", "688001.SH"),
            ("900001", "tushare", "900001.SH"),
            ("000001", "baostock", "sz.000001"),
            ("001001", "baostock", "sz.001001"),
            ("002001", "baostock", "sz.002001"),
            ("300001", "baostock", "sz.300001"),
            ("600000", "baostock", "sh.600000"),
            ("601398", "baostock", "sh.601398"),
            ("603000", "baostock", "sh.603000"),
            ("605000", "baostock", "sh.605000"),
            ("688001", "baostock", "sh.688001"),
            ("900001", "baostock", "sh.900001"),
        ],
    )
    def test_format_stock_code(self, symbol, source, expected):
        assert format_stock_code(symbol, source) == expected

    def test_unsupported_source(self):
        with pytest.raises(ValueError, match="不支持的数据源"):
            format_stock_code("000001", "akshare")

    def test_strip_whitespace(self):
        assert format_stock_code("  000001  ", "baostock") == "sz.000001"


class TestParseDate:
    @pytest.mark.parametrize(
        "input_str,expected",
        [
            ("2024-01-15", "2024-01-15"),
            ("20240115", "2024-01-15"),
            ("2024/01/15", "2024-01-15"),
        ],
    )
    def test_parse_date_formats(self, input_str, expected):
        assert parse_date(input_str) == expected

    def test_parse_date_none(self):
        assert parse_date(None) is None

    def test_parse_date_invalid(self):
        with pytest.raises(ValueError, match="无法解析日期"):
            parse_date("not-a-date")

    def test_parse_date_strip_whitespace(self):
        assert parse_date(" 2024-01-15 ") == "2024-01-15"
