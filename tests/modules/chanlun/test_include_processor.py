import pandas as pd
import pytest

from modules.chanlun.include_processor import IncludeProcessor, ProcessedKLine


@pytest.fixture
def processor():
    return IncludeProcessor()


def _make_df(data):
    return pd.DataFrame(data)


class TestNoIncludeRelation:

    def test_no_include(self, processor):
        df = _make_df([
            {"trade_date": "2024-01-01", "open": 10, "high": 12, "low": 9, "close": 11, "volume": 100},
            {"trade_date": "2024-01-02", "open": 11, "high": 14, "low": 10, "close": 13, "volume": 200},
            {"trade_date": "2024-01-03", "open": 13, "high": 15, "low": 12, "close": 14, "volume": 150},
        ])
        result = processor.process(df)
        assert len(result) == 3
        assert result[0].high == 12
        assert result[1].high == 14
        assert result[2].high == 15


class TestIncludeMerge:

    def test_up_include(self, processor):
        df = _make_df([
            {"trade_date": "2024-01-01", "open": 10, "high": 15, "low": 8, "close": 12, "volume": 100},
            {"trade_date": "2024-01-02", "open": 11, "high": 13, "low": 9, "close": 11, "volume": 50},
        ])
        result = processor.process(df)
        assert len(result) == 1
        assert result[0].high == 15
        assert result[0].low == 9
        assert result[0].volume == 150

    def test_down_include(self, processor):
        df = _make_df([
            {"trade_date": "2024-01-01", "open": 10, "high": 13, "low": 8, "close": 12, "volume": 100},
            {"trade_date": "2024-01-02", "open": 10, "high": 12, "low": 7, "close": 9, "volume": 50},
        ])
        result = processor.process(df)
        assert len(result) == 2
        assert result[1].high == 12
        assert result[1].low == 7


class TestChainInclude:

    def test_chain_include(self, processor):
        df = _make_df([
            {"trade_date": "2024-01-01", "open": 10, "high": 20, "low": 5, "close": 15, "volume": 100},
            {"trade_date": "2024-01-02", "open": 11, "high": 18, "low": 7, "close": 14, "volume": 50},
            {"trade_date": "2024-01-03", "open": 12, "high": 16, "low": 8, "close": 13, "volume": 30},
        ])
        result = processor.process(df)
        assert len(result) == 1
        assert result[0].volume == 180

    def test_chain_then_normal(self, processor):
        df = _make_df([
            {"trade_date": "2024-01-01", "open": 10, "high": 20, "low": 5, "close": 15, "volume": 100},
            {"trade_date": "2024-01-02", "open": 11, "high": 18, "low": 7, "close": 14, "volume": 50},
            {"trade_date": "2024-01-03", "open": 12, "high": 16, "low": 8, "close": 13, "volume": 30},
            {"trade_date": "2024-01-04", "open": 13, "high": 25, "low": 12, "close": 22, "volume": 200},
        ])
        result = processor.process(df)
        assert len(result) == 2
        assert result[0].high == 20
        assert result[1].high == 25


class TestEdgeCases:

    def test_empty_df(self, processor):
        result = processor.process(pd.DataFrame())
        assert result == []

    def test_none_df(self, processor):
        result = processor.process(None)
        assert result == []

    def test_single_kline(self, processor):
        df = _make_df([
            {"trade_date": "2024-01-01", "open": 10, "high": 12, "low": 9, "close": 11, "volume": 100},
        ])
        result = processor.process(df)
        assert len(result) == 1

    def test_two_klines(self, processor):
        df = _make_df([
            {"trade_date": "2024-01-01", "open": 10, "high": 12, "low": 9, "close": 11, "volume": 100},
            {"trade_date": "2024-01-02", "open": 11, "high": 14, "low": 10, "close": 13, "volume": 200},
        ])
        result = processor.process(df)
        assert len(result) == 2

    def test_original_indices_preserved(self, processor):
        df = _make_df([
            {"trade_date": "2024-01-01", "open": 10, "high": 20, "low": 5, "close": 15, "volume": 100},
            {"trade_date": "2024-01-02", "open": 11, "high": 18, "low": 7, "close": 14, "volume": 50},
            {"trade_date": "2024-01-03", "open": 12, "high": 25, "low": 12, "close": 22, "volume": 200},
        ])
        result = processor.process(df)
        assert len(result) == 2
        assert result[0].original_indices == [0, 1]
        assert result[1].original_indices == [2]
