import logging

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


class FlagUpDetector(BasePatternDetector):

    def __init__(self, parallel_tolerance: float = 0.30, min_bi_count: int = 6):
        self.parallel_tolerance = parallel_tolerance
        self.min_bi_count = min_bi_count
        self.validator = NecklineValidator()

    @property
    def required_bi_count(self) -> int:
        return self.min_bi_count

    def detect(self, bis: list, kline_df) -> list:
        results = []
        if len(bis) < self.min_bi_count:
            return results

        for i in range(len(bis) - self.min_bi_count + 1):
            for window_size in range(self.min_bi_count, min(len(bis) - i + 1, self.min_bi_count + 4)):
                window = bis[i : i + window_size]
                result = self._check_window(window, kline_df, global_start=i)
                if result:
                    results.append(result)
                    break
        return results

    def _check_window(self, window: list, kline_df, global_start: int = 0) -> "PatternResult | None":
        if len(window) < self.min_bi_count:
            return None

        highs = []
        lows = []
        for bi in window:
            high = max(bi.start_price, bi.end_price)
            low = min(bi.start_price, bi.end_price)
            if bi.direction == BiDirection.UP:
                highs.append((bi.end_date, bi.end_price, high))
                lows.append((bi.start_date, bi.start_price, low))
            else:
                highs.append((bi.start_date, bi.start_price, high))
                lows.append((bi.end_date, bi.end_price, low))

        if len(highs) < 2 or len(lows) < 2:
            return None

        from .flag_down import _calc_slope

        upper_slope = _calc_slope(highs)
        lower_slope = _calc_slope(lows)

        if upper_slope <= 0 or lower_slope <= 0:
            return None

        if abs(upper_slope) < 1e-10 or abs(lower_slope) < 1e-10:
            return None

        slope_ratio = abs(upper_slope - lower_slope) / max(abs(upper_slope), abs(lower_slope))
        if slope_ratio > self.parallel_tolerance:
            return None

        flagpole_high = highs[0][2] if highs else 0
        flagpole_low = lows[0][2] if lows else 0
        flagpole_height = flagpole_high - flagpole_low

        if flagpole_height <= 0:
            return None

        lower_line = lows[-1][1]
        last_close = window[-1].end_price
        breakout_date = window[-1].end_date

        key_points = [
            KeyPoint(name="flagpole_high", date=highs[0][0], price=highs[0][2], bi_index=global_start),
            KeyPoint(name="flagpole_low", date=lows[0][0], price=lows[0][2], bi_index=global_start),
            KeyPoint(name="flag_high", date=highs[-1][0], price=highs[-1][2], bi_index=global_start + len(window) - 1),
            KeyPoint(name="flag_low", date=lows[-1][0], price=lows[-1][2], bi_index=global_start + len(window) - 1),
            KeyPoint(name="breakdown", date=breakout_date, price=last_close, bi_index=global_start + len(window) - 1),
        ]

        result = PatternResult(
            pattern_type=PatternType.FLAG_UP,
            direction=PatternDirection.BEARISH,
            start_date=window[0].start_date,
            end_date=window[-1].end_date,
            neckline_price=lower_line,
            neckline_slope=lower_slope,
            key_points=key_points,
            bi_indices=list(range(global_start, global_start + len(window))),
            confidence=0.5,
        )

        if last_close < lower_line:
            validation = self.validator.validate(
                kline_df, breakout_date, lower_line, direction="bear"
            )
            if validation.is_valid:
                result.status = PatternStatus.CONFIRMED
                result.confidence = validation.confidence
            else:
                result.status = PatternStatus.FORMING
                result.confidence = validation.confidence * 0.5
        else:
            result.status = PatternStatus.FORMING
            result.confidence = 0.3

        return result
