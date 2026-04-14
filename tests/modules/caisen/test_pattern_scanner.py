import pytest

from modules.chanlun.bi_generator import Bi, BiDirection

from modules.caisen.pattern_base import PatternType
from modules.caisen.pattern_scanner import PatternScanner


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

    dates = pd.date_range("2024-01-01", periods=41, freq="D")
    return pd.DataFrame({
        "trade_date": dates,
        "open": [10.0] * 41,
        "high": [25.0] * 41,
        "low": [3.0] * 41,
        "close": [12.0] * 41,
        "volume": [1000] * 41,
    })


class TestPatternScanner:

    def test_scanner_runs_all_detectors(self):
        scanner = PatternScanner()
        assert len(scanner.detectors) == 11

    def test_scan_returns_list(self):
        scanner = PatternScanner()
        bis = [
            _make_bi(BiDirection.DOWN, 20, 10, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.UP, 10, 18, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.DOWN, 18, 10.2, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.UP, 10.2, 22, "2024-01-15", "2024-01-20", 14, 19),
        ]

        results = scanner.scan(bis, _make_kline_df())

        assert isinstance(results, list)

    def test_scan_empty_bis(self):
        scanner = PatternScanner()
        results = scanner.scan([], _make_kline_df())

        assert results == []

    def test_dedup_removes_overlapping(self):
        scanner = PatternScanner()

        from modules.caisen.pattern_base import (
            KeyPoint,
            PatternDirection,
            PatternResult,
            PatternStatus,
        )

        r1 = PatternResult(
            pattern_type=PatternType.W_BOTTOM,
            direction=PatternDirection.BULLISH,
            status=PatternStatus.CONFIRMED,
            start_date="2024-01-01",
            end_date="2024-01-20",
            confidence=0.9,
            key_points=[KeyPoint(name="test", date="2024-01-01", price=10.0, bi_index=0)],
        )
        r2 = PatternResult(
            pattern_type=PatternType.TRIANGLE_BOTTOM,
            direction=PatternDirection.BULLISH,
            status=PatternStatus.CONFIRMED,
            start_date="2024-01-05",
            end_date="2024-01-18",
            confidence=0.7,
            key_points=[KeyPoint(name="test", date="2024-01-05", price=10.0, bi_index=0)],
        )

        deduped = scanner._deduplicate([r1, r2])

        assert len(deduped) == 1
        assert deduped[0].pattern_type == PatternType.W_BOTTOM

    def test_dedup_keeps_non_overlapping(self):
        scanner = PatternScanner()

        from modules.caisen.pattern_base import (
            KeyPoint,
            PatternDirection,
            PatternResult,
            PatternStatus,
        )

        r1 = PatternResult(
            pattern_type=PatternType.W_BOTTOM,
            direction=PatternDirection.BULLISH,
            status=PatternStatus.CONFIRMED,
            start_date="2024-01-01",
            end_date="2024-01-10",
            confidence=0.9,
            key_points=[],
        )
        r2 = PatternResult(
            pattern_type=PatternType.M_TOP,
            direction=PatternDirection.BEARISH,
            status=PatternStatus.CONFIRMED,
            start_date="2024-02-01",
            end_date="2024-02-10",
            confidence=0.8,
            key_points=[],
        )

        deduped = scanner._deduplicate([r1, r2])

        assert len(deduped) == 2

    def test_detector_exception_handling(self):
        scanner = PatternScanner()
        bis = [_make_bi(BiDirection.DOWN, 20, 10)]

        results = scanner.scan(bis, None)

        assert isinstance(results, list)
