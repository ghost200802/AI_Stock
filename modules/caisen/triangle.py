import logging

import pandas as pd

from modules.chanlun.bi_generator import BiDirection

from .neckline_validator import NecklineValidator
from .pattern_base import (
    BasePatternDetector,
    KeyPoint,
    PatternDirection,
    PatternResult,
    PatternStatus,
    PatternType,
)

logger = logging.getLogger(__name__)


class TriangleDetector(BasePatternDetector):

    def __init__(self):
        self.validator = NecklineValidator()

    @property
    def required_bi_count(self) -> int:
        return 4

    def detect(self, bis: list, kline_df) -> list:
        results = []
        if len(bis) < 4:
            return results

        for window_size in range(4, min(len(bis) + 1, 10)):
            for i in range(len(bis) - window_size + 1):
                window = bis[i : i + window_size]
                result = self._check_window(window, kline_df, global_start=i)
                if result:
                    results.append(result)
        return results

    @staticmethod
    def _extract_turning_points(window: list):
        if not window:
            return [], []
        points = [(window[0].start_date, window[0].start_price)]
        for bi in window:
            points.append((bi.end_date, bi.end_price))
        ups = []
        downs = []
        for i in range(len(points)):
            if i == 0:
                if len(window) > 0 and window[0].direction == BiDirection.DOWN:
                    ups.append(points[i])
                else:
                    downs.append(points[i])
            elif i == len(points) - 1:
                if window[-1].direction == BiDirection.UP:
                    ups.append(points[i])
                else:
                    downs.append(points[i])
            else:
                bi_idx = i - 1
                if bi_idx < len(window) and window[bi_idx].direction == BiDirection.UP:
                    ups.append(points[i])
                else:
                    downs.append(points[i])
        return ups, downs

    def _check_window(self, window: list, kline_df, global_start: int = 0) -> "PatternResult | None":
        if len(window) < 4:
            return None

        all_highs, all_lows = self._extract_turning_points(window)

        if len(all_highs) < 2 or len(all_lows) < 2:
            return None

        last_close = window[-1].end_price
        breakout_date = window[-1].end_date
        last_bi_dir = window[-1].direction

        if last_bi_dir == BiDirection.UP:
            consolidation_highs = all_highs[:-1]
            consolidation_lows = all_lows
        else:
            consolidation_highs = all_highs
            consolidation_lows = all_lows[:-1]

        if len(consolidation_highs) < 2 or len(consolidation_lows) < 2:
            return None

        upper_trend = consolidation_highs[-1][1] < consolidation_highs[0][1]
        lower_trend = consolidation_lows[-1][1] > consolidation_lows[0][1]

        if not (upper_trend and lower_trend):
            return None

        from .flag_down import _calc_slope

        upper_slope = _calc_slope([(d, p, p) for d, p in consolidation_highs])
        lower_slope = _calc_slope([(d, p, p) for d, p in consolidation_lows])

        if upper_slope >= 0:
            return None
        if lower_slope <= 0:
            return None

        upper_line_start = consolidation_highs[0][1]
        upper_line_end = consolidation_highs[-1][1]
        lower_line_start = consolidation_lows[0][1]
        lower_line_end = consolidation_lows[-1][1]

        max_height = upper_line_start - lower_line_start
        if max_height <= 0:
            return None

        try:
            first_date = pd.Timestamp(consolidation_highs[0][0])
            last_upper_date = pd.Timestamp(consolidation_highs[-1][0])
            last_low_date = pd.Timestamp(consolidation_lows[-1][0])
            apex_date = pd.Timestamp(max(last_upper_date, last_low_date))
            total_days = max((apex_date - first_date).days, 1)

            try:
                breakout_ts = pd.Timestamp(breakout_date)
                breakout_days = max((breakout_ts - first_date).days, 0)
            except Exception:
                breakout_days = total_days

            position_ratio = breakout_days / total_days
        except Exception:
            position_ratio = 0.5

        if 0.5 <= position_ratio <= 0.75:
            position_bonus = 1.0
        elif 0.25 <= position_ratio < 0.5:
            position_bonus = 0.8
        elif 0.75 < position_ratio <= 0.9:
            position_bonus = 0.6
        else:
            position_bonus = 0.4

        key_points = [
            KeyPoint(name="upper_start", date=all_highs[0][0], price=all_highs[0][1], bi_index=global_start),
            KeyPoint(name="lower_start", date=all_lows[0][0], price=all_lows[0][1], bi_index=global_start),
            KeyPoint(name="upper_end", date=all_highs[-1][0], price=all_highs[-1][1], bi_index=global_start + len(window) - 1),
            KeyPoint(name="lower_end", date=all_lows[-1][0], price=all_lows[-1][1], bi_index=global_start + len(window) - 1),
            KeyPoint(name="breakout", date=breakout_date, price=last_close, bi_index=global_start + len(window) - 1),
        ]

        neckline_price = (upper_line_end + lower_line_end) / 2.0

        if last_close > upper_line_end:
            result = PatternResult(
                pattern_type=PatternType.TRIANGLE_BOTTOM,
                direction=PatternDirection.BULLISH,
                start_date=window[0].start_date,
                end_date=window[-1].end_date,
                neckline_price=upper_line_end,
                neckline_slope=upper_slope,
                key_points=key_points,
                bi_indices=list(range(global_start, global_start + len(window))),
                confidence=0.5 * position_bonus,
            )

            validation = self.validator.validate(
                kline_df, breakout_date, upper_line_end, direction="bull"
            )
            if validation.is_valid:
                result.status = PatternStatus.CONFIRMED
                result.confidence = validation.confidence * position_bonus
            else:
                result.status = PatternStatus.FORMING
                result.confidence = validation.confidence * 0.5 * position_bonus

            return result

        if last_close < lower_line_end:
            result = PatternResult(
                pattern_type=PatternType.TRIANGLE_HEAD,
                direction=PatternDirection.BEARISH,
                start_date=window[0].start_date,
                end_date=window[-1].end_date,
                neckline_price=lower_line_end,
                neckline_slope=lower_slope,
                key_points=key_points,
                bi_indices=list(range(global_start, global_start + len(window))),
                confidence=0.5 * position_bonus,
            )

            validation = self.validator.validate(
                kline_df, breakout_date, lower_line_end, direction="bear"
            )
            if validation.is_valid:
                result.status = PatternStatus.CONFIRMED
                result.confidence = validation.confidence * position_bonus
            else:
                result.status = PatternStatus.FORMING
                result.confidence = validation.confidence * 0.5 * position_bonus

            return result

        return None
