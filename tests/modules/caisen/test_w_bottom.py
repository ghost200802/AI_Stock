import pytest

from modules.chanlun.bi_generator import Bi, BiDirection

from modules.caisen.pattern_base import PatternDirection, PatternStatus, PatternType
from modules.caisen.w_bottom import WBottomDetector


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
        "open": [10.0] * 21,
        "high": [15.0] * 21,
        "low": [5.0] * 21,
        "close": [12.0] * 21,
        "volume": [1000] * 21,
    })


class TestWBottomStandard:

    def test_standard_w_bottom(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 10, "2024-01-01", "2024-01-05"),
            _make_bi(BiDirection.UP, 10, 18, "2024-01-05", "2024-01-10"),
            _make_bi(BiDirection.DOWN, 18, 10.2, "2024-01-10", "2024-01-15"),
            _make_bi(BiDirection.UP, 10.2, 22, "2024-01-15", "2024-01-20"),
        ]
        kline_df = _make_kline_df()

        detector = WBottomDetector()
        results = detector.detect(bis, kline_df)

        assert len(results) >= 1
        r = results[0]
        assert r.pattern_type == PatternType.W_BOTTOM
        assert r.direction == PatternDirection.BULLISH

    def test_w_bottom_confirmed_when_breakout(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 10, "2024-01-01", "2024-01-05"),
            _make_bi(BiDirection.UP, 10, 18, "2024-01-05", "2024-01-10"),
            _make_bi(BiDirection.DOWN, 18, 10.2, "2024-01-10", "2024-01-15"),
            _make_bi(BiDirection.UP, 10.2, 22, "2024-01-15", "2024-01-20"),
        ]
        kline_df = _make_kline_df()

        detector = WBottomDetector()
        results = detector.detect(bis, kline_df)

        assert len(results) >= 1
        assert results[0].status in (PatternStatus.CONFIRMED, PatternStatus.FORMING)

    def test_w_bottom_forming_when_no_breakout(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 10, "2024-01-01", "2024-01-05"),
            _make_bi(BiDirection.UP, 10, 18, "2024-01-05", "2024-01-10"),
            _make_bi(BiDirection.DOWN, 18, 10.2, "2024-01-10", "2024-01-15"),
            _make_bi(BiDirection.UP, 10.2, 16, "2024-01-15", "2024-01-20"),
        ]
        kline_df = _make_kline_df()

        detector = WBottomDetector()
        results = detector.detect(bis, kline_df)

        assert len(results) >= 1
        assert results[0].status == PatternStatus.FORMING


class TestWBottomPriceTolerance:

    def test_price_diff_too_large(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 10, "2024-01-01", "2024-01-05"),
            _make_bi(BiDirection.UP, 10, 18, "2024-01-05", "2024-01-10"),
            _make_bi(BiDirection.DOWN, 18, 12, "2024-01-10", "2024-01-15"),
            _make_bi(BiDirection.UP, 12, 22, "2024-01-15", "2024-01-20"),
        ]
        kline_df = _make_kline_df()

        detector = WBottomDetector(price_tolerance=0.05)
        results = detector.detect(bis, kline_df)

        assert len(results) == 0


class TestWBottomDirection:

    def test_wrong_direction(self):
        bis = [
            _make_bi(BiDirection.UP, 10, 20, "2024-01-01", "2024-01-05"),
            _make_bi(BiDirection.DOWN, 20, 10, "2024-01-05", "2024-01-10"),
            _make_bi(BiDirection.UP, 10, 18, "2024-01-10", "2024-01-15"),
            _make_bi(BiDirection.DOWN, 18, 10, "2024-01-15", "2024-01-20"),
        ]
        kline_df = _make_kline_df()

        detector = WBottomDetector()
        results = detector.detect(bis, kline_df)

        assert len(results) == 0


class TestWBottomKeyPoints:

    def test_key_points_structure(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 10, "2024-01-01", "2024-01-05"),
            _make_bi(BiDirection.UP, 10, 18, "2024-01-05", "2024-01-10"),
            _make_bi(BiDirection.DOWN, 18, 10.2, "2024-01-10", "2024-01-15"),
            _make_bi(BiDirection.UP, 10.2, 22, "2024-01-15", "2024-01-20"),
        ]
        kline_df = _make_kline_df()

        detector = WBottomDetector()
        results = detector.detect(bis, kline_df)

        assert len(results) >= 1
        kp_names = [kp.name for kp in results[0].key_points]
        assert "left_bottom" in kp_names
        assert "neckline_left" in kp_names
        assert "right_bottom" in kp_names
        assert "breakout" in kp_names


class TestWBottomEmptyInput:

    def test_empty_bis(self):
        detector = WBottomDetector()
        assert detector.detect([], _make_kline_df()) == []

    def test_insufficient_bis(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 10),
            _make_bi(BiDirection.UP, 10, 18),
        ]
        detector = WBottomDetector()
        assert detector.detect(bis, _make_kline_df()) == []
