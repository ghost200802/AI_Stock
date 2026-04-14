import pytest

from modules.chanlun.bi_generator import Bi, BiDirection

from modules.caisen.head_shoulder_bottom import HeadShoulderBottomDetector
from modules.caisen.head_shoulder_top import HeadShoulderTopDetector
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

    dates = [f"2024-01-{d:02d}" for d in range(1, 32)]
    return pd.DataFrame({
        "trade_date": pd.to_datetime(dates),
        "open": [10.0] * 31,
        "high": [20.0] * 31,
        "low": [3.0] * 31,
        "close": [12.0] * 31,
        "volume": [1000] * 31,
    })


class TestHeadShoulderBottom:

    def test_standard_head_shoulder_bottom(self):
        bis = [
            _make_bi(BiDirection.DOWN, 18, 12, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.UP, 12, 16, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.DOWN, 16, 8, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.UP, 8, 15, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.DOWN, 15, 11.5, "2024-01-20", "2024-01-25", 19, 24),
            _make_bi(BiDirection.UP, 11.5, 20, "2024-01-25", "2024-01-30", 24, 29),
        ]
        kline_df = _make_kline_df()

        detector = HeadShoulderBottomDetector()
        results = detector.detect(bis, kline_df)

        assert len(results) >= 1
        r = results[0]
        assert r.pattern_type == PatternType.HEAD_SHOULDER_BOTTOM
        assert r.direction == PatternDirection.BULLISH
        assert r.status in (PatternStatus.CONFIRMED, PatternStatus.FORMING)

    def test_head_lowest(self):
        bis = [
            _make_bi(BiDirection.DOWN, 18, 12, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.UP, 12, 16, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.DOWN, 16, 8, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.UP, 8, 15, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.DOWN, 15, 11.5, "2024-01-20", "2024-01-25", 19, 24),
            _make_bi(BiDirection.UP, 11.5, 20, "2024-01-25", "2024-01-30", 24, 29),
        ]

        detector = HeadShoulderBottomDetector()
        results = detector.detect(bis, _make_kline_df())

        assert len(results) >= 1
        head_kp = [kp for kp in results[0].key_points if kp.name == "head"]
        assert len(head_kp) == 1
        assert head_kp[0].price == 8

    def test_shoulder_tolerance(self):
        bis = [
            _make_bi(BiDirection.DOWN, 18, 12, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.UP, 12, 16, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.DOWN, 16, 8, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.UP, 8, 15, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.DOWN, 15, 9, "2024-01-20", "2024-01-25", 19, 24),
            _make_bi(BiDirection.UP, 9, 20, "2024-01-25", "2024-01-30", 24, 29),
        ]

        detector = HeadShoulderBottomDetector(shoulder_tolerance=0.10)
        results = detector.detect(bis, _make_kline_df())

        assert len(results) == 0

    def test_neckline_slope(self):
        bis = [
            _make_bi(BiDirection.DOWN, 18, 12, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.UP, 12, 14, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.DOWN, 14, 8, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.UP, 8, 16, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.DOWN, 16, 12, "2024-01-20", "2024-01-25", 19, 24),
            _make_bi(BiDirection.UP, 12, 20, "2024-01-25", "2024-01-30", 24, 29),
        ]

        detector = HeadShoulderBottomDetector()
        results = detector.detect(bis, _make_kline_df())

        assert len(results) >= 1
        assert results[0].neckline_slope > 0


class TestHeadShoulderTop:

    def test_standard_head_shoulder_top(self):
        bis = [
            _make_bi(BiDirection.UP, 12, 18, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.DOWN, 18, 14, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.UP, 14, 22, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.DOWN, 22, 13, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.UP, 13, 17, "2024-01-20", "2024-01-25", 19, 24),
            _make_bi(BiDirection.DOWN, 17, 8, "2024-01-25", "2024-01-30", 24, 29),
        ]
        kline_df = _make_kline_df()

        detector = HeadShoulderTopDetector()
        results = detector.detect(bis, kline_df)

        assert len(results) >= 1
        r = results[0]
        assert r.pattern_type == PatternType.HEAD_SHOULDER_TOP
        assert r.direction == PatternDirection.BEARISH

    def test_head_highest(self):
        bis = [
            _make_bi(BiDirection.UP, 12, 18, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.DOWN, 18, 14, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.UP, 14, 22, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.DOWN, 22, 13, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.UP, 13, 17, "2024-01-20", "2024-01-25", 19, 24),
            _make_bi(BiDirection.DOWN, 17, 8, "2024-01-25", "2024-01-30", 24, 29),
        ]

        detector = HeadShoulderTopDetector()
        results = detector.detect(bis, _make_kline_df())

        assert len(results) >= 1
        head_kp = [kp for kp in results[0].key_points if kp.name == "head"]
        assert len(head_kp) == 1
        assert head_kp[0].price == 22


class TestEmptyInput:

    def test_empty_bis_bottom(self):
        detector = HeadShoulderBottomDetector()
        assert detector.detect([], _make_kline_df()) == []

    def test_empty_bis_top(self):
        detector = HeadShoulderTopDetector()
        assert detector.detect([], _make_kline_df()) == []
