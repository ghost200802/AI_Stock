import pytest

from modules.chanlun.bi_generator import Bi, BiDirection

from modules.caisen.triangle import TriangleDetector
from modules.caisen.ascending_triangle import AscendingTriangleDetector
from modules.caisen.descending_triangle import DescendingTriangleDetector
from modules.caisen.pattern_base import PatternDirection, PatternStatus, PatternType


def _make_bi(direction, start_price, end_price, start_date="2024-01-01", end_date="2024-01-05", start_idx=0, end_idx=4):
    return Bi(
        direction=direction,
        start_date=start_date,
        end_date=end_date,
        start_price=start_price,
        end_price=end_price,
        start_index=start_idx,
        end_index=end_idx,
        confirmed=True,
    )


def _make_kline_df():
    import pandas as pd

    dates = pd.date_range("2024-01-01", periods=31, freq="D")
    return pd.DataFrame({
        "trade_date": dates,
        "open": [10.0] * 31,
        "high": [25.0] * 31,
        "low": [3.0] * 31,
        "close": [12.0] * 31,
        "volume": [1000] * 31,
    })


class TestConvergentTriangle:

    def test_triangle_bottom(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 12, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.UP, 12, 18, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.DOWN, 18, 13, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.UP, 13, 16, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.DOWN, 16, 14, "2024-01-20", "2024-01-25", 19, 24),
            _make_bi(BiDirection.UP, 14, 22, "2024-01-25", "2024-01-30", 24, 29),
        ]

        detector = TriangleDetector()
        results = detector.detect(bis, _make_kline_df())

        bottom_results = [r for r in results if r.pattern_type == PatternType.TRIANGLE_BOTTOM]
        assert len(bottom_results) >= 1
        r = bottom_results[0]
        assert r.direction == PatternDirection.BULLISH

    def test_triangle_head(self):
        bis = [
            _make_bi(BiDirection.UP, 12, 20, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.DOWN, 20, 15, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.UP, 15, 18, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.DOWN, 18, 16, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.UP, 16, 17, "2024-01-20", "2024-01-25", 19, 24),
            _make_bi(BiDirection.DOWN, 17, 8, "2024-01-25", "2024-01-30", 24, 29),
        ]

        detector = TriangleDetector()
        results = detector.detect(bis, _make_kline_df())

        head_results = [r for r in results if r.pattern_type == PatternType.TRIANGLE_HEAD]
        assert len(head_results) >= 1
        r = head_results[0]
        assert r.direction == PatternDirection.BEARISH

    def test_no_convergence(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 12, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.UP, 12, 22, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.DOWN, 22, 10, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.UP, 10, 24, "2024-01-15", "2024-01-20", 14, 19),
        ]

        detector = TriangleDetector()
        results = detector.detect(bis, _make_kline_df())

        assert len(results) == 0

    def test_empty_bis(self):
        detector = TriangleDetector()
        assert detector.detect([], _make_kline_df()) == []


class TestAscendingTriangle:

    def test_standard_ascending_triangle(self):
        bis = [
            _make_bi(BiDirection.DOWN, 18, 12, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.UP, 12, 20, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.DOWN, 20, 14, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.UP, 14, 20, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.DOWN, 20, 16, "2024-01-20", "2024-01-25", 19, 24),
            _make_bi(BiDirection.UP, 16, 24, "2024-01-25", "2024-01-30", 24, 29),
        ]

        detector = AscendingTriangleDetector()
        results = detector.detect(bis, _make_kline_df())

        assert len(results) >= 1
        r = results[0]
        assert r.pattern_type == PatternType.ASCENDING_TRIANGLE
        assert r.direction == PatternDirection.BULLISH

    def test_empty_bis(self):
        detector = AscendingTriangleDetector()
        assert detector.detect([], _make_kline_df()) == []


class TestDescendingTriangle:

    def test_standard_descending_triangle(self):
        bis = [
            _make_bi(BiDirection.UP, 12, 20, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.DOWN, 20, 10, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.UP, 10, 18, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.DOWN, 18, 10, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.UP, 10, 16, "2024-01-20", "2024-01-25", 19, 24),
            _make_bi(BiDirection.DOWN, 16, 6, "2024-01-25", "2024-01-30", 24, 29),
        ]

        detector = DescendingTriangleDetector()
        results = detector.detect(bis, _make_kline_df())

        assert len(results) >= 1
        r = results[0]
        assert r.pattern_type == PatternType.DESCENDING_TRIANGLE
        assert r.direction == PatternDirection.BEARISH

    def test_empty_bis(self):
        detector = DescendingTriangleDetector()
        assert detector.detect([], _make_kline_df()) == []
