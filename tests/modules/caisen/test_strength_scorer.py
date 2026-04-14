import pytest

import pandas as pd

from modules.chanlun.bi_generator import Bi, BiDirection

from modules.caisen.pattern_base import (
    KeyPoint,
    PatternDirection,
    PatternResult,
    PatternStatus,
    PatternType,
)
from modules.caisen.strength_scorer import StrengthScorer


def _make_bi(direction, start_price, end_price):
    return Bi(
        direction=direction,
        start_date="2024-01-01",
        end_date="2024-01-10",
        start_price=start_price,
        end_price=end_price,
        start_index=0,
        end_index=9,
        confirmed=True,
    )


def _make_kline_df():
    dates = pd.date_range("2024-01-01", periods=30, freq="D")
    return pd.DataFrame({
        "trade_date": dates,
        "open": [10.0] * 30,
        "high": [20.0] * 30,
        "low": [5.0] * 30,
        "close": list(range(10, 40)),
        "volume": [1000] * 30,
    })


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


class TestStrengthScore:

    def test_score_range(self):
        scorer = StrengthScorer()
        result = _make_result(PatternType.W_BOTTOM, PatternDirection.BULLISH)
        bis = [_make_bi(BiDirection.DOWN, 20, 10)]

        scorer.score(result, bis, _make_kline_df())

        assert 0 <= result.strength_score <= 100

    def test_bull_position_at_low(self):
        scorer = StrengthScorer()
        result = _make_result(PatternType.W_BOTTOM, PatternDirection.BULLISH, key_points=[
            KeyPoint(name="left_bottom", date="2024-01-01", price=10.0, bi_index=0),
            KeyPoint(name="right_bottom", date="2024-01-10", price=10.5, bi_index=1),
            KeyPoint(name="breakout", date="2024-01-15", price=16.0, bi_index=2),
        ])
        bis = [_make_bi(BiDirection.DOWN, 20, 10), _make_bi(BiDirection.UP, 10, 16)]

        scorer.score(result, bis, _make_kline_df())

        assert result.strength_score > 0

    def test_bear_position_at_high(self):
        scorer = StrengthScorer()
        result = _make_result(PatternType.M_TOP, PatternDirection.BEARISH, key_points=[
            KeyPoint(name="left_top", date="2024-01-01", price=30.0, bi_index=0),
            KeyPoint(name="right_top", date="2024-01-10", price=29.0, bi_index=1),
            KeyPoint(name="breakdown", date="2024-01-15", price=25.0, bi_index=2),
        ])
        bis = [_make_bi(BiDirection.UP, 10, 30), _make_bi(BiDirection.DOWN, 30, 25)]

        scorer.score(result, bis, _make_kline_df())

        assert result.strength_score > 0

    def test_neckline_slope_bonus_head_shoulder_bottom(self):
        scorer = StrengthScorer()
        result_up_slope = _make_result(PatternType.HEAD_SHOULDER_BOTTOM, PatternDirection.BULLISH)
        result_up_slope.neckline_slope = 0.01

        result_down_slope = _make_result(PatternType.HEAD_SHOULDER_BOTTOM, PatternDirection.BULLISH)
        result_down_slope.neckline_slope = -0.01

        bis = [_make_bi(BiDirection.DOWN, 20, 10)]
        scorer.score(result_up_slope, bis, _make_kline_df())
        scorer.score(result_down_slope, bis, _make_kline_df())

        assert result_up_slope.strength_score > result_down_slope.strength_score

    def test_neckline_slope_bonus_head_shoulder_top(self):
        scorer = StrengthScorer()
        result_down = _make_result(PatternType.HEAD_SHOULDER_TOP, PatternDirection.BEARISH)
        result_down.neckline_slope = -0.01

        result_up = _make_result(PatternType.HEAD_SHOULDER_TOP, PatternDirection.BEARISH)
        result_up.neckline_slope = 0.01

        bis = [_make_bi(BiDirection.DOWN, 20, 10)]
        scorer.score(result_down, bis, _make_kline_df())
        scorer.score(result_up, bis, _make_kline_df())

        assert result_down.strength_score > result_up.strength_score


class TestVolumeScore:

    def test_volume_score_range(self):
        scorer = StrengthScorer()
        result = _make_result(PatternType.W_BOTTOM, PatternDirection.BULLISH)
        bis = [_make_bi(BiDirection.DOWN, 20, 10)]

        scorer.score(result, bis, _make_kline_df())

        assert 0 <= result.volume_score <= 100

    def test_bull_volume_score(self):
        scorer = StrengthScorer()
        result = _make_result(PatternType.W_BOTTOM, PatternDirection.BULLISH)
        bis = [_make_bi(BiDirection.DOWN, 20, 10)]

        scorer.score(result, bis, _make_kline_df())

        assert result.volume_score >= 0

    def test_bear_volume_score(self):
        scorer = StrengthScorer()
        result = _make_result(PatternType.M_TOP, PatternDirection.BEARISH)
        bis = [_make_bi(BiDirection.UP, 10, 20)]

        scorer.score(result, bis, _make_kline_df())

        assert result.volume_score >= 0

    def test_no_kline_data(self):
        scorer = StrengthScorer()
        result = _make_result(PatternType.W_BOTTOM, PatternDirection.BULLISH)
        bis = [_make_bi(BiDirection.DOWN, 20, 10)]
        empty_df = pd.DataFrame()

        scorer.score(result, bis, empty_df)

        assert result.strength_score >= 0
        assert result.volume_score >= 0
