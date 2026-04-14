import pytest

import pandas as pd

from modules.caisen.neckline_validator import NecklineValidator, ValidationResult


def _make_kline_df(bullish=True, high_volume=True, breakout_idx=5):
    dates = pd.date_range("2024-01-01", periods=15, freq="D")
    n = len(dates)
    opens = [10.0] * n
    highs = [25.0] * n
    lows = [5.0] * n
    closes = [10.0] * n
    volumes = [1000] * n

    if bullish:
        opens[breakout_idx] = 16.0
        highs[breakout_idx] = 24.0
        lows[breakout_idx] = 15.0
        closes[breakout_idx] = 23.0
    else:
        opens[breakout_idx] = 16.0
        highs[breakout_idx] = 17.0
        lows[breakout_idx] = 5.0
        closes[breakout_idx] = 6.0

    if high_volume:
        volumes[breakout_idx] = 2000

    return pd.DataFrame({
        "trade_date": dates,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": volumes,
    })


BREAKOUT_DATE = pd.Timestamp("2024-01-06")


class TestVolumeValidation:

    def test_bull_volume_sufficient(self):
        df = _make_kline_df(bullish=True, high_volume=True, breakout_idx=5)
        validator = NecklineValidator(volume_ratio_threshold=1.5)
        result = validator.validate(df, BREAKOUT_DATE, neckline_price=20.0, direction="bull")

        assert result.volume_ok is True

    def test_bull_volume_insufficient(self):
        df = _make_kline_df(bullish=True, high_volume=False, breakout_idx=5)
        validator = NecklineValidator(volume_ratio_threshold=1.5)
        result = validator.validate(df, BREAKOUT_DATE, neckline_price=20.0, direction="bull")

        assert result.volume_ok is False

    def test_bear_volume_always_ok(self):
        df = _make_kline_df(bullish=False, high_volume=False, breakout_idx=5)
        validator = NecklineValidator(volume_ratio_threshold=1.5)
        result = validator.validate(df, BREAKOUT_DATE, neckline_price=10.0, direction="bear")

        assert result.volume_ok is True


class TestCandleBodyValidation:

    def test_bull_candle_body_valid(self):
        df = _make_kline_df(bullish=True, high_volume=True, breakout_idx=5)
        validator = NecklineValidator()
        result = validator.validate(df, BREAKOUT_DATE, neckline_price=15.0, direction="bull")

        assert result.candle_body_ok is True

    def test_bear_candle_body_valid(self):
        df = _make_kline_df(bullish=False, high_volume=True, breakout_idx=5)
        validator = NecklineValidator()
        result = validator.validate(df, BREAKOUT_DATE, neckline_price=10.0, direction="bear")

        assert result.candle_body_ok is True

    def test_candle_below_neckline(self):
        df = _make_kline_df(bullish=True, high_volume=True, breakout_idx=5)
        df.loc[5, "close"] = 12.0
        df.loc[5, "open"] = 14.0
        validator = NecklineValidator()
        result = validator.validate(df, BREAKOUT_DATE, neckline_price=20.0, direction="bull")

        assert result.candle_body_ok is False


class TestNextDayValidation:

    def test_next_day_stays_above(self):
        df = _make_kline_df(bullish=True, high_volume=True, breakout_idx=5)
        df.loc[6, "close"] = 18.0
        validator = NecklineValidator()
        result = validator.validate(df, BREAKOUT_DATE, neckline_price=15.0, direction="bull")

        assert result.next_day_ok is True

    def test_no_next_day_data(self):
        df = _make_kline_df(bullish=True, high_volume=True, breakout_idx=14)
        validator = NecklineValidator()
        result = validator.validate(df, pd.Timestamp("2024-01-15"), neckline_price=15.0, direction="bull")

        assert result.next_day_ok is True


class TestOverallValidation:

    def test_confidence_range(self):
        df = _make_kline_df(bullish=True, high_volume=True, breakout_idx=5)
        validator = NecklineValidator()
        result = validator.validate(df, BREAKOUT_DATE, neckline_price=15.0, direction="bull")

        assert 0.0 <= result.confidence <= 1.0

    def test_is_valid_when_both_pass(self):
        df = _make_kline_df(bullish=True, high_volume=True, breakout_idx=5)
        validator = NecklineValidator()
        result = validator.validate(df, BREAKOUT_DATE, neckline_price=15.0, direction="bull")

        assert result.is_valid is True

    def test_missing_volume_column(self):
        df = pd.DataFrame({
            "trade_date": pd.date_range("2024-01-01", periods=15),
            "open": [10.0] * 15,
            "high": [15.0] * 15,
            "low": [5.0] * 15,
            "close": [12.0] * 15,
        })
        validator = NecklineValidator()
        result = validator.validate(df, pd.Timestamp("2024-01-06"), neckline_price=10.0, direction="bull")

        assert result.volume_ok is False
