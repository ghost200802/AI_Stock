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


class DescendingTriangleDetector(BasePatternDetector):

    def __init__(self, level_tolerance: float = 0.03):
        self.level_tolerance = level_tolerance
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

        highs, lows = self._extract_turning_points(window)

        if len(highs) < 2 or len(lows) < 2:
            return None

        last_close = window[-1].end_price
        breakout_date = window[-1].end_date
        last_low_price = lows[-1][1]

        if last_close <= last_low_price:
            check_lows = lows[:-1]
        else:
            check_lows = lows

        if len(check_lows) < 2:
            return None

        check_low_prices = [l[1] for l in check_lows]
        avg_low = sum(check_low_prices) / len(check_low_prices)
        low_range = max(check_low_prices) - min(check_low_prices)
        if avg_low > 0 and low_range / avg_low > self.level_tolerance:
            return None

        support_level = avg_low

        high_prices = [h[1] for h in highs]
        if high_prices[-1] >= high_prices[0]:
            return None

        from .flag_down import _calc_slope

        upper_slope = _calc_slope([(d, p, p) for d, p in highs])
        lower_slope = _calc_slope([(d, p, p) for d, p in check_lows])

        if upper_slope >= 0:
            return None
        if lower_slope < 0:
            return None

        max_height = high_prices[0] - support_level
        if max_height <= 0:
            return None

        key_points = [
            KeyPoint(name="resistance_start", date=highs[0][0], price=highs[0][1], bi_index=global_start),
            KeyPoint(name="support", date=lows[0][0], price=lows[0][1], bi_index=global_start),
            KeyPoint(name="resistance_end", date=highs[-1][0], price=highs[-1][1], bi_index=global_start + len(window) - 1),
            KeyPoint(name="support_end", date=check_lows[-1][0], price=check_lows[-1][1], bi_index=global_start + len(window) - 1),
            KeyPoint(name="breakdown", date=breakout_date, price=last_close, bi_index=global_start + len(window) - 1),
        ]

        if last_close < support_level:
            result = PatternResult(
                pattern_type=PatternType.DESCENDING_TRIANGLE,
                direction=PatternDirection.BEARISH,
                start_date=window[0].start_date,
                end_date=window[-1].end_date,
                neckline_price=support_level,
                neckline_slope=0.0,
                key_points=key_points,
                bi_indices=list(range(global_start, global_start + len(window))),
                confidence=0.5,
            )

            validation = self.validator.validate(
                kline_df, breakout_date, support_level, direction="bear"
            )
            if validation.is_valid:
                result.status = PatternStatus.CONFIRMED
                result.confidence = validation.confidence
            else:
                result.status = PatternStatus.FORMING
                result.confidence = validation.confidence * 0.5

            return result

        return None
