import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    is_valid: bool
    confidence: float
    volume_ok: bool
    candle_body_ok: bool
    next_day_ok: bool


class NecklineValidator:

    def __init__(
        self,
        volume_ratio_threshold: float = 1.5,
        avg_volume_window: int = 10,
        body_ratio_threshold: float = 0.5,
    ):
        self.volume_ratio_threshold = volume_ratio_threshold
        self.avg_volume_window = avg_volume_window
        self.body_ratio_threshold = body_ratio_threshold

    def validate(
        self,
        kline_df: pd.DataFrame,
        breakout_date,
        neckline_price: float,
        direction: str = "bull",
    ) -> ValidationResult:
        volume_ok = self._check_volume(kline_df, breakout_date, direction)
        candle_body_ok = self._check_candle_body(kline_df, breakout_date, neckline_price, direction)
        next_day_ok = self._check_next_day(kline_df, breakout_date, neckline_price, direction)

        valid_count = sum([volume_ok, candle_body_ok])
        is_valid = valid_count >= 2

        confidence = 0.0
        if volume_ok:
            confidence += 0.4
        if candle_body_ok:
            confidence += 0.4
        if next_day_ok:
            confidence += 0.2

        if is_valid and next_day_ok:
            confidence = min(confidence + 0.1, 1.0)

        return ValidationResult(
            is_valid=is_valid,
            confidence=confidence,
            volume_ok=volume_ok,
            candle_body_ok=candle_body_ok,
            next_day_ok=next_day_ok,
        )

    def _check_volume(self, kline_df: pd.DataFrame, breakout_date, direction: str) -> bool:
        if "volume" not in kline_df.columns or "trade_date" not in kline_df.columns:
            return False

        try:
            df_sorted = kline_df.sort_values("trade_date").reset_index(drop=True)
            date_series = df_sorted["trade_date"]

            if hasattr(date_series, "dt"):
                match_mask = date_series.dt.normalize() == pd.Timestamp(breakout_date).normalize()
            else:
                match_mask = date_series.astype(str).str[:10] == str(breakout_date)[:10]

            idx_arr = np.where(match_mask)[0]
            if len(idx_arr) == 0:
                return False
            breakout_idx = idx_arr[-1]

            start_idx = max(0, breakout_idx - self.avg_volume_window)
            recent_vols = df_sorted.iloc[start_idx:breakout_idx]["volume"]
            if len(recent_vols) == 0:
                return False

            avg_vol = float(recent_vols.mean())
            if avg_vol <= 0:
                return False

            breakout_vol = float(df_sorted.iloc[breakout_idx]["volume"])
            ratio = breakout_vol / avg_vol

            if direction == "bull":
                return bool(ratio >= self.volume_ratio_threshold)
            else:
                return True
        except Exception:
            logger.debug("量能验证异常", exc_info=True)
            return False

    def _check_candle_body(
        self, kline_df: pd.DataFrame, breakout_date, neckline_price: float, direction: str
    ) -> bool:
        required_cols = ["open", "high", "low", "close", "trade_date"]
        if not all(c in kline_df.columns for c in required_cols):
            return False

        try:
            df_sorted = kline_df.sort_values("trade_date").reset_index(drop=True)
            date_series = df_sorted["trade_date"]

            if hasattr(date_series, "dt"):
                match_mask = date_series.dt.normalize() == pd.Timestamp(breakout_date).normalize()
            else:
                match_mask = date_series.astype(str).str[:10] == str(breakout_date)[:10]

            idx_arr = np.where(match_mask)[0]
            if len(idx_arr) == 0:
                return False
            row = df_sorted.iloc[idx_arr[-1]]

            open_p = float(row["open"])
            close_p = float(row["close"])
            high_p = float(row["high"])
            low_p = float(row["low"])
            total_range = high_p - low_p
            if total_range <= 0:
                return False

            body_size = abs(close_p - open_p)
            body_ratio = body_size / total_range

            if direction == "bull":
                is_bullish_candle = close_p > open_p
                closes_above = close_p > neckline_price
                return bool(is_bullish_candle and closes_above and body_ratio >= self.body_ratio_threshold)
            else:
                is_bearish_candle = close_p < open_p
                closes_below = close_p < neckline_price
                return bool(is_bearish_candle and closes_below and body_ratio >= self.body_ratio_threshold)
        except Exception:
            logger.debug("K线实体验证异常", exc_info=True)
            return False

    def _check_next_day(
        self, kline_df: pd.DataFrame, breakout_date, neckline_price: float, direction: str
    ) -> bool:
        required_cols = ["close", "trade_date"]
        if not all(c in kline_df.columns for c in required_cols):
            return True

        try:
            df_sorted = kline_df.sort_values("trade_date").reset_index(drop=True)
            date_series = df_sorted["trade_date"]

            if hasattr(date_series, "dt"):
                match_mask = date_series.dt.normalize() == pd.Timestamp(breakout_date).normalize()
            else:
                match_mask = date_series.astype(str).str[:10] == str(breakout_date)[:10]

            idx_arr = np.where(match_mask)[0]
            if len(idx_arr) == 0:
                return True
            breakout_idx = idx_arr[-1]

            if breakout_idx + 1 >= len(df_sorted):
                return True

            next_close = float(df_sorted.iloc[breakout_idx + 1]["close"])
            if direction == "bull":
                return bool(next_close >= neckline_price)
            else:
                return bool(next_close <= neckline_price)
        except Exception:
            return True
