import pytest

from modules.chanlun.bi_generator import Bi, BiDirection

from modules.caisen.breakout_fail import BreakoutFailDetector
from modules.caisen.podie_fan import PoDieFanDetector
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
        "high": [25.0] * 31,
        "low": [5.0] * 31,
        "close": [12.0] * 31,
        "volume": [1000] * 31,
    })


class TestBreakoutFail:

    def test_standard_breakout_fail(self):
        bis = [
            _make_bi(BiDirection.UP, 10, 20, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.DOWN, 20, 15, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.UP, 15, 22, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.DOWN, 22, 12, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.UP, 12, 14, "2024-01-20", "2024-01-25", 19, 24),
        ]

        detector = BreakoutFailDetector()
        results = detector.detect(bis, _make_kline_df())

        assert len(results) >= 1
        r = results[0]
        assert r.pattern_type == PatternType.BREAKOUT_FAIL
        assert r.direction == PatternDirection.BEARISH
        assert r.status == PatternStatus.CONFIRMED

    def test_b2_not_higher_than_b0(self):
        bis = [
            _make_bi(BiDirection.UP, 10, 20, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.DOWN, 20, 15, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.UP, 15, 18, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.DOWN, 18, 12, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.UP, 12, 14, "2024-01-20", "2024-01-25", 19, 24),
        ]

        detector = BreakoutFailDetector()
        results = detector.detect(bis, _make_kline_df())

        assert len(results) == 0

    def test_b4_lower_increases_confidence(self):
        bis_high_conf = [
            _make_bi(BiDirection.UP, 10, 20, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.DOWN, 20, 15, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.UP, 15, 22, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.DOWN, 22, 12, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.UP, 12, 10, "2024-01-20", "2024-01-25", 19, 24),
        ]
        bis_low_conf = [
            _make_bi(BiDirection.UP, 10, 20, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.DOWN, 20, 15, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.UP, 15, 22, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.DOWN, 22, 12, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.UP, 12, 16, "2024-01-20", "2024-01-25", 19, 24),
        ]

        detector = BreakoutFailDetector()
        results_high = detector.detect(bis_high_conf, _make_kline_df())
        results_low = detector.detect(bis_low_conf, _make_kline_df())

        assert len(results_high) >= 1
        assert len(results_low) >= 1
        assert results_high[0].confidence > results_low[0].confidence

    def test_empty_bis(self):
        detector = BreakoutFailDetector()
        assert detector.detect([], _make_kline_df()) == []


class TestPoDieFan:

    def test_standard_podie_fan(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 10, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.UP, 10, 18, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.DOWN, 18, 8, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.UP, 8, 22, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.DOWN, 22, 15, "2024-01-20", "2024-01-25", 19, 24),
        ]

        detector = PoDieFanDetector()
        results = detector.detect(bis, _make_kline_df())

        assert len(results) >= 1
        r = results[0]
        assert r.pattern_type == PatternType.PODIE_FAN
        assert r.direction == PatternDirection.BULLISH
        assert r.status == PatternStatus.CONFIRMED

    def test_b2_not_lower_than_b0(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 10, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.UP, 10, 18, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.DOWN, 18, 12, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.UP, 12, 22, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.DOWN, 22, 15, "2024-01-20", "2024-01-25", 19, 24),
        ]

        detector = PoDieFanDetector()
        results = detector.detect(bis, _make_kline_df())

        assert len(results) == 0

    def test_stop_loss_at_neckline(self):
        bis = [
            _make_bi(BiDirection.DOWN, 20, 10, "2024-01-01", "2024-01-05", 0, 4),
            _make_bi(BiDirection.UP, 10, 18, "2024-01-05", "2024-01-10", 4, 9),
            _make_bi(BiDirection.DOWN, 18, 8, "2024-01-10", "2024-01-15", 9, 14),
            _make_bi(BiDirection.UP, 8, 22, "2024-01-15", "2024-01-20", 14, 19),
            _make_bi(BiDirection.DOWN, 22, 15, "2024-01-20", "2024-01-25", 19, 24),
        ]

        from modules.caisen.target_price import TargetPriceCalculator

        detector = PoDieFanDetector()
        results = detector.detect(bis, _make_kline_df())
        assert len(results) >= 1

        calculator = TargetPriceCalculator()
        calculator.calculate(results[0])

        lower_kps = [kp for kp in results[0].key_points if kp.name in ("consolidation_low", "breakdown_low")]
        assert len(lower_kps) >= 1
        assert results[0].stop_loss_price > 0

    def test_empty_bis(self):
        detector = PoDieFanDetector()
        assert detector.detect([], _make_kline_df()) == []
