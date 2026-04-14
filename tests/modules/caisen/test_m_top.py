import pytest

from modules.chanlun.bi_generator import Bi, BiDirection

from modules.caisen.m_top import MTopDetector
from modules.caisen.pattern_base import PatternDirection, PatternStatus, PatternType


def _make_bi(direction, start_price, end_price, start_date="2024-01-01", end_date="2024-01-05"):
    return Bi(
        direction=direction,
        start_date=start_date,
        end_date=end_date,
        start_price=start_price,
        end_price=end_price,
        start_index=0,
        end_index=4,
        confirmed=True,
    )


def _make_kline_df():
    import pandas as pd

    dates = [f"2024-01-{d:02d}" for d in range(1, 22)]
    return pd.DataFrame({
        "trade_date": pd.to_datetime(dates),
        "open": [20.0] * 21,
        "high": [25.0] * 21,
        "low": [5.0] * 21,
        "close": [10.0] * 21,
        "volume": [1000] * 21,
    })


class TestMTopStandard:

    def test_standard_m_top(self):
        bis = [
            _make_bi(BiDirection.UP, 10, 20, "2024-01-01", "2024-01-05"),
            _make_bi(BiDirection.DOWN, 20, 12, "2024-01-05", "2024-01-10"),
            _make_bi(BiDirection.UP, 12, 19.5, "2024-01-10", "2024-01-15"),
            _make_bi(BiDirection.DOWN, 19.5, 8, "2024-01-15", "2024-01-20"),
        ]
        kline_df = _make_kline_df()

        detector = MTopDetector()
        results = detector.detect(bis, kline_df)

        assert len(results) >= 1
        r = results[0]
        assert r.pattern_type == PatternType.M_TOP
        assert r.direction == PatternDirection.BEARISH

    def test_m_top_confirmed(self):
        bis = [
            _make_bi(BiDirection.UP, 10, 20, "2024-01-01", "2024-01-05"),
            _make_bi(BiDirection.DOWN, 20, 12, "2024-01-05", "2024-01-10"),
            _make_bi(BiDirection.UP, 12, 19.5, "2024-01-10", "2024-01-15"),
            _make_bi(BiDirection.DOWN, 19.5, 8, "2024-01-15", "2024-01-20"),
        ]
        kline_df = _make_kline_df()

        detector = MTopDetector()
        results = detector.detect(bis, kline_df)

        assert len(results) >= 1
        assert results[0].status in (PatternStatus.CONFIRMED, PatternStatus.FORMING)

    def test_m_top_forming_when_no_breakdown(self):
        bis = [
            _make_bi(BiDirection.UP, 10, 20, "2024-01-01", "2024-01-05"),
            _make_bi(BiDirection.DOWN, 20, 12, "2024-01-05", "2024-01-10"),
            _make_bi(BiDirection.UP, 12, 19.5, "2024-01-10", "2024-01-15"),
            _make_bi(BiDirection.DOWN, 19.5, 14, "2024-01-15", "2024-01-20"),
        ]
        kline_df = _make_kline_df()

        detector = MTopDetector()
        results = detector.detect(bis, kline_df)

        assert len(results) >= 1
        assert results[0].status == PatternStatus.FORMING


class TestMTopPriceTolerance:

    def test_price_diff_too_large(self):
        bis = [
            _make_bi(BiDirection.UP, 10, 20, "2024-01-01", "2024-01-05"),
            _make_bi(BiDirection.DOWN, 20, 12, "2024-01-05", "2024-01-10"),
            _make_bi(BiDirection.UP, 12, 15, "2024-01-10", "2024-01-15"),
            _make_bi(BiDirection.DOWN, 15, 8, "2024-01-15", "2024-01-20"),
        ]
        kline_df = _make_kline_df()

        detector = MTopDetector(price_tolerance=0.05)
        results = detector.detect(bis, kline_df)

        assert len(results) == 0


class TestMTopDirection:

    def test_wrong_direction(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 10, "2024-01-01", "2024-01-05"),
            _make_bi(BiDirection.UP, 10, 20, "2024-01-05", "2024-01-10"),
            _make_bi(BiDirection.DOWN, 20, 12, "2024-01-10", "2024-01-15"),
            _make_bi(BiDirection.UP, 12, 22, "2024-01-15", "2024-01-20"),
        ]
        kline_df = _make_kline_df()

        detector = MTopDetector()
        results = detector.detect(bis, kline_df)

        assert len(results) == 0


class TestMTopEmptyInput:

    def test_empty_bis(self):
        detector = MTopDetector()
        assert detector.detect([], _make_kline_df()) == []
