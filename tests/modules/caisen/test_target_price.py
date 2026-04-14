import pytest

from modules.caisen.pattern_base import (
    KeyPoint,
    PatternDirection,
    PatternResult,
    PatternStatus,
    PatternType,
)
from modules.caisen.target_price import TargetPriceCalculator


def _make_result(pattern_type, direction, neckline_price=15.0, key_points=None):
    return PatternResult(
        pattern_type=pattern_type,
        direction=direction,
        status=PatternStatus.CONFIRMED,
        start_date="2024-01-01",
        end_date="2024-01-20",
        neckline_price=neckline_price,
        neckline_slope=0.0,
        key_points=key_points or [
            KeyPoint(name="test", date="2024-01-01", price=10.0, bi_index=0),
            KeyPoint(name="test2", date="2024-01-10", price=20.0, bi_index=1),
        ],
        confidence=0.8,
    )


class TestWBottomTarget:

    def test_w_bottom_target_price(self):
        result = _make_result(PatternType.W_BOTTOM, PatternDirection.BULLISH, neckline_price=18.0, key_points=[
            KeyPoint(name="left_bottom", date="2024-01-01", price=10.0, bi_index=0),
            KeyPoint(name="neckline_left", date="2024-01-05", price=18.0, bi_index=1),
            KeyPoint(name="right_bottom", date="2024-01-10", price=10.5, bi_index=2),
            KeyPoint(name="breakout", date="2024-01-15", price=20.0, bi_index=3),
        ])

        calc = TargetPriceCalculator()
        calc.calculate(result)

        expected = 18.0 + (18.0 - 10.0)
        assert abs(result.target_price - expected) < 0.1

    def test_w_bottom_stop_loss(self):
        result = _make_result(PatternType.W_BOTTOM, PatternDirection.BULLISH, key_points=[
            KeyPoint(name="left_bottom", date="2024-01-01", price=10.0, bi_index=0),
            KeyPoint(name="right_bottom", date="2024-01-10", price=10.5, bi_index=1),
            KeyPoint(name="breakout", date="2024-01-15", price=20.0, bi_index=2),
        ])

        calc = TargetPriceCalculator()
        calc.calculate(result)

        assert result.stop_loss_price > 0
        assert result.stop_loss_price < 10.5


class TestMTopTarget:

    def test_m_top_target_price(self):
        result = _make_result(PatternType.M_TOP, PatternDirection.BEARISH, neckline_price=12.0, key_points=[
            KeyPoint(name="left_top", date="2024-01-01", price=20.0, bi_index=0),
            KeyPoint(name="neckline_left", date="2024-01-05", price=12.0, bi_index=1),
            KeyPoint(name="right_top", date="2024-01-10", price=19.5, bi_index=2),
            KeyPoint(name="breakdown", date="2024-01-15", price=8.0, bi_index=3),
        ])

        calc = TargetPriceCalculator()
        calc.calculate(result)

        expected = 12.0 - (20.0 - 12.0)
        assert abs(result.target_price - expected) < 0.1


class TestHeadShoulderTarget:

    def test_head_shoulder_bottom_target(self):
        result = _make_result(PatternType.HEAD_SHOULDER_BOTTOM, PatternDirection.BULLISH, neckline_price=15.0, key_points=[
            KeyPoint(name="left_shoulder", date="2024-01-01", price=12.0, bi_index=0),
            KeyPoint(name="neckline_left", date="2024-01-05", price=14.0, bi_index=1),
            KeyPoint(name="head", date="2024-01-10", price=8.0, bi_index=2),
            KeyPoint(name="neckline_right", date="2024-01-15", price=16.0, bi_index=3),
            KeyPoint(name="right_shoulder", date="2024-01-18", price=12.5, bi_index=4),
            KeyPoint(name="breakout", date="2024-01-20", price=18.0, bi_index=5),
        ])

        calc = TargetPriceCalculator()
        calc.calculate(result)

        expected = 15.0 + 1.382 * (15.0 - 8.0)
        assert abs(result.target_price - expected) < 0.1

    def test_head_shoulder_top_target(self):
        result = _make_result(PatternType.HEAD_SHOULDER_TOP, PatternDirection.BEARISH, neckline_price=15.0, key_points=[
            KeyPoint(name="left_shoulder", date="2024-01-01", price=18.0, bi_index=0),
            KeyPoint(name="neckline_left", date="2024-01-05", price=16.0, bi_index=1),
            KeyPoint(name="head", date="2024-01-10", price=22.0, bi_index=2),
            KeyPoint(name="neckline_right", date="2024-01-15", price=14.0, bi_index=3),
            KeyPoint(name="right_shoulder", date="2024-01-18", price=17.5, bi_index=4),
            KeyPoint(name="breakdown", date="2024-01-20", price=12.0, bi_index=5),
        ])

        calc = TargetPriceCalculator()
        calc.calculate(result)

        expected = 15.0 - 1.382 * (22.0 - 15.0)
        assert abs(result.target_price - expected) < 0.1


class TestBreakoutFailTarget:

    def test_breakout_fail_target(self):
        result = _make_result(PatternType.BREAKOUT_FAIL, PatternDirection.BEARISH, neckline_price=18.0, key_points=[
            KeyPoint(name="consolidation_high", date="2024-01-01", price=20.0, bi_index=0),
            KeyPoint(name="fake_breakout_high", date="2024-01-10", price=22.0, bi_index=2),
            KeyPoint(name="breakdown", date="2024-01-15", price=12.0, bi_index=3),
        ])

        calc = TargetPriceCalculator()
        calc.calculate(result)

        expected = 18.0 - (22.0 - 18.0)
        assert abs(result.target_price - expected) < 0.1


class TestPoDieFanTarget:

    def test_podie_fan_target(self):
        result = _make_result(PatternType.PODIE_FAN, PatternDirection.BULLISH, key_points=[
            KeyPoint(name="consolidation_low", date="2024-01-01", price=10.0, bi_index=0),
            KeyPoint(name="consolidation_high", date="2024-01-05", price=18.0, bi_index=1),
            KeyPoint(name="breakdown_low", date="2024-01-10", price=8.0, bi_index=2),
            KeyPoint(name="recovery_high", date="2024-01-15", price=20.0, bi_index=3),
            KeyPoint(name="continuation", date="2024-01-20", price=22.0, bi_index=4),
        ])

        calc = TargetPriceCalculator()
        calc.calculate(result)

        upper = 20.0
        lower = 8.0
        expected = upper + (upper - lower)
        assert abs(result.target_price - expected) < 0.1

    def test_podie_fan_stop_loss_at_neckline(self):
        result = _make_result(PatternType.PODIE_FAN, PatternDirection.BULLISH, key_points=[
            KeyPoint(name="consolidation_low", date="2024-01-01", price=10.0, bi_index=0),
            KeyPoint(name="consolidation_high", date="2024-01-05", price=18.0, bi_index=1),
            KeyPoint(name="breakdown_low", date="2024-01-10", price=8.0, bi_index=2),
            KeyPoint(name="recovery_high", date="2024-01-15", price=20.0, bi_index=3),
        ])

        calc = TargetPriceCalculator()
        calc.calculate(result)

        lower = min(kp.price for kp in result.key_points if kp.name in ("consolidation_low", "breakdown_low"))
        assert abs(result.stop_loss_price - lower) < 0.1


class TestFlagTarget:

    def test_flag_down_target(self):
        result = _make_result(PatternType.FLAG_DOWN, PatternDirection.BULLISH, key_points=[
            KeyPoint(name="flagpole_high", date="2024-01-01", price=20.0, bi_index=0),
            KeyPoint(name="flagpole_low", date="2024-01-01", price=10.0, bi_index=0),
            KeyPoint(name="breakout", date="2024-01-20", price=16.0, bi_index=3),
        ])

        calc = TargetPriceCalculator()
        calc.calculate(result)

        expected = 16.0 + (20.0 - 10.0)
        assert abs(result.target_price - expected) < 0.1

    def test_flag_up_target(self):
        result = _make_result(PatternType.FLAG_UP, PatternDirection.BEARISH, key_points=[
            KeyPoint(name="flagpole_high", date="2024-01-01", price=20.0, bi_index=0),
            KeyPoint(name="flagpole_low", date="2024-01-01", price=10.0, bi_index=0),
            KeyPoint(name="breakdown", date="2024-01-20", price=14.0, bi_index=3),
        ])

        calc = TargetPriceCalculator()
        calc.calculate(result)

        expected = 14.0 - (20.0 - 10.0)
        assert abs(result.target_price - expected) < 0.1


class TestTriangleTarget:

    def test_triangle_bottom_target(self):
        result = _make_result(PatternType.TRIANGLE_BOTTOM, PatternDirection.BULLISH, key_points=[
            KeyPoint(name="upper_start", date="2024-01-01", price=20.0, bi_index=0),
            KeyPoint(name="lower_start", date="2024-01-01", price=10.0, bi_index=0),
            KeyPoint(name="breakout", date="2024-01-15", price=22.0, bi_index=3),
        ])

        calc = TargetPriceCalculator()
        calc.calculate(result)

        height = 20.0 - 10.0
        expected = 22.0 + height
        assert abs(result.target_price - expected) < 0.1

    def test_triangle_head_target(self):
        result = _make_result(PatternType.TRIANGLE_HEAD, PatternDirection.BEARISH, key_points=[
            KeyPoint(name="upper_start", date="2024-01-01", price=20.0, bi_index=0),
            KeyPoint(name="lower_start", date="2024-01-01", price=10.0, bi_index=0),
            KeyPoint(name="breakdown", date="2024-01-15", price=8.0, bi_index=3),
        ])

        calc = TargetPriceCalculator()
        calc.calculate(result)

        height = 20.0 - 10.0
        expected = 8.0 - height
        assert abs(result.target_price - expected) < 0.1

    def test_ascending_triangle_target(self):
        result = _make_result(PatternType.ASCENDING_TRIANGLE, PatternDirection.BULLISH, key_points=[
            KeyPoint(name="resistance", date="2024-01-01", price=20.0, bi_index=0),
            KeyPoint(name="support_start", date="2024-01-01", price=12.0, bi_index=0),
            KeyPoint(name="breakout", date="2024-01-15", price=22.0, bi_index=3),
        ])

        calc = TargetPriceCalculator()
        calc.calculate(result)

        height = 20.0 - 12.0
        expected = 22.0 + height
        assert abs(result.target_price - expected) < 0.1

    def test_descending_triangle_target(self):
        result = _make_result(PatternType.DESCENDING_TRIANGLE, PatternDirection.BEARISH, key_points=[
            KeyPoint(name="resistance_start", date="2024-01-01", price=20.0, bi_index=0),
            KeyPoint(name="support", date="2024-01-01", price=10.0, bi_index=0),
            KeyPoint(name="breakdown", date="2024-01-15", price=8.0, bi_index=3),
        ])

        calc = TargetPriceCalculator()
        calc.calculate(result)

        height = 20.0 - 10.0
        expected = 8.0 - height
        assert abs(result.target_price - expected) < 0.1


class TestGenericStopLoss:

    def test_bull_stop_loss_3_percent(self):
        result = _make_result(PatternType.W_BOTTOM, PatternDirection.BULLISH, key_points=[
            KeyPoint(name="bottom", date="2024-01-01", price=10.0, bi_index=0),
        ])

        calc = TargetPriceCalculator(stop_loss_pct=0.03)
        calc.calculate(result)

        expected = 10.0 * 0.97
        assert abs(result.stop_loss_price - expected) < 0.01

    def test_bear_stop_loss_3_percent(self):
        result = _make_result(PatternType.M_TOP, PatternDirection.BEARISH, key_points=[
            KeyPoint(name="top", date="2024-01-01", price=20.0, bi_index=0),
        ])

        calc = TargetPriceCalculator(stop_loss_pct=0.03)
        calc.calculate(result)

        expected = 20.0 * 1.03
        assert abs(result.stop_loss_price - expected) < 0.01

    def test_no_key_points(self):
        result = PatternResult(
            pattern_type=PatternType.W_BOTTOM,
            direction=PatternDirection.BULLISH,
            status=PatternStatus.CONFIRMED,
            key_points=[],
        )

        calc = TargetPriceCalculator()
        calc.calculate(result)

        assert result.target_price == 0.0
        assert result.stop_loss_price == 0.0
