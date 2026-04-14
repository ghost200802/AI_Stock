import pytest

from modules.chanlun.bi_generator import Bi, BiDirection

from modules.caisen.flag_down import FlagDownDetector
from modules.caisen.flag_up import FlagUpDetector
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

    dates = pd.date_range("2024-01-01", periods=41, freq="D")
    return pd.DataFrame({
        "trade_date": dates,
        "open": [10.0] * 41,
        "high": [25.0] * 41,
        "low": [3.0] * 41,
        "close": [12.0] * 41,
        "volume": [1000] * 41,
    })


class TestFlagDown:

    def test_standard_flag_down(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 16, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.UP, 16, 18, "2024-01-05", "2024-01-09", 4, 8),
            _make_bi(BiDirection.DOWN, 18, 14, "2024-01-09", "2024-01-13", 8, 12),
            _make_bi(BiDirection.UP, 14, 16, "2024-01-13", "2024-01-17", 12, 16),
            _make_bi(BiDirection.DOWN, 16, 12, "2024-01-17", "2024-01-21", 16, 20),
            _make_bi(BiDirection.UP, 12, 14, "2024-01-21", "2024-01-25", 20, 24),
            _make_bi(BiDirection.DOWN, 14, 11, "2024-01-25", "2024-01-29", 24, 28),
            _make_bi(BiDirection.UP, 11, 22, "2024-01-29", "2024-02-02", 28, 32),
        ]

        detector = FlagDownDetector(min_bi_count=6)
        results = detector.detect(bis, _make_kline_df())

        assert len(results) >= 1
        r = results[0]
        assert r.pattern_type == PatternType.FLAG_DOWN
        assert r.direction == PatternDirection.BULLISH

    def test_not_parallel_rejected(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 16, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.UP, 16, 18, "2024-01-05", "2024-01-09", 4, 8),
            _make_bi(BiDirection.DOWN, 18, 10, "2024-01-09", "2024-01-13", 8, 12),
            _make_bi(BiDirection.UP, 10, 17, "2024-01-13", "2024-01-17", 12, 16),
            _make_bi(BiDirection.DOWN, 17, 15, "2024-01-17", "2024-01-21", 16, 20),
            _make_bi(BiDirection.UP, 15, 16, "2024-01-21", "2024-01-25", 20, 24),
            _make_bi(BiDirection.DOWN, 16, 14, "2024-01-25", "2024-01-29", 24, 28),
            _make_bi(BiDirection.UP, 14, 22, "2024-01-29", "2024-02-02", 28, 32),
        ]

        detector = FlagDownDetector(min_bi_count=6, parallel_tolerance=0.30)
        results = detector.detect(bis, _make_kline_df())

        for r in results:
            assert r.pattern_type == PatternType.FLAG_DOWN

    def test_flagpole_height_recorded(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 16, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.UP, 16, 18, "2024-01-05", "2024-01-09", 4, 8),
            _make_bi(BiDirection.DOWN, 18, 14, "2024-01-09", "2024-01-13", 8, 12),
            _make_bi(BiDirection.UP, 14, 16, "2024-01-13", "2024-01-17", 12, 16),
            _make_bi(BiDirection.DOWN, 16, 12, "2024-01-17", "2024-01-21", 16, 20),
            _make_bi(BiDirection.UP, 12, 14, "2024-01-21", "2024-01-25", 20, 24),
            _make_bi(BiDirection.DOWN, 14, 11, "2024-01-25", "2024-01-29", 24, 28),
            _make_bi(BiDirection.UP, 11, 22, "2024-01-29", "2024-02-02", 28, 32),
        ]

        detector = FlagDownDetector(min_bi_count=6)
        results = detector.detect(bis, _make_kline_df())

        assert len(results) >= 1
        kp_names = [kp.name for kp in results[0].key_points]
        assert "flagpole_high" in kp_names
        assert "flagpole_low" in kp_names

    def test_empty_bis(self):
        detector = FlagDownDetector()
        assert detector.detect([], _make_kline_df()) == []

    def test_insufficient_bis(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 16),
            _make_bi(BiDirection.UP, 16, 18),
            _make_bi(BiDirection.DOWN, 18, 14),
        ]
        detector = FlagDownDetector(min_bi_count=6)
        assert detector.detect(bis, _make_kline_df()) == []


class TestFlagUp:

    def test_standard_flag_up(self):
        bis = [
            _make_bi(BiDirection.UP, 10, 14, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.DOWN, 14, 12, "2024-01-05", "2024-01-09", 4, 8),
            _make_bi(BiDirection.UP, 12, 16, "2024-01-09", "2024-01-13", 8, 12),
            _make_bi(BiDirection.DOWN, 16, 14, "2024-01-13", "2024-01-17", 12, 16),
            _make_bi(BiDirection.UP, 14, 18, "2024-01-17", "2024-01-21", 16, 20),
            _make_bi(BiDirection.DOWN, 18, 16, "2024-01-21", "2024-01-25", 20, 24),
            _make_bi(BiDirection.UP, 16, 20, "2024-01-25", "2024-01-29", 24, 28),
            _make_bi(BiDirection.DOWN, 20, 8, "2024-01-29", "2024-02-02", 28, 32),
        ]

        detector = FlagUpDetector(min_bi_count=6)
        results = detector.detect(bis, _make_kline_df())

        assert len(results) >= 1
        r = results[0]
        assert r.pattern_type == PatternType.FLAG_UP
        assert r.direction == PatternDirection.BEARISH

    def test_empty_bis(self):
        detector = FlagUpDetector()
        assert detector.detect([], _make_kline_df()) == []
