import logging

from modules.chanlun.bi_generator import BiDirection

from .pattern_base import (
    BasePatternDetector,
    KeyPoint,
    PatternDirection,
    PatternResult,
    PatternStatus,
    PatternType,
)

logger = logging.getLogger(__name__)


class PoDieFanDetector(BasePatternDetector):

    def __init__(self):
        pass

    @property
    def required_bi_count(self) -> int:
        return 5

    def detect(self, bis: list, kline_df) -> list:
        results = []
        if len(bis) < 5:
            return results

        for i in range(len(bis) - 4):
            window = bis[i : i + 5]
            result = self._check_window(window, kline_df)
            if result:
                results.append(result)
        return results

    def _check_window(self, window: list, kline_df) -> "PatternResult | None":
        b0, b1, b2, b3, b4 = window

        expected_dirs = [BiDirection.DOWN, BiDirection.UP, BiDirection.DOWN, BiDirection.UP, BiDirection.DOWN]
        for bi, expected in zip(window, expected_dirs):
            if bi.direction != expected:
                return None

        b0_low = b0.end_price
        b1_high = b1.end_price
        b2_low = b2.end_price
        b3_high = b3.end_price

        if b2_low >= b0_low:
            return None
        if b3_high <= b1_high:
            return None

        neckline_upper = (b1_high + b3_high) / 2.0
        neckline_lower = (b0_low + b2_low) / 2.0
        neckline_price = neckline_lower

        if b4.end_price > b3_high:
            confidence = 0.8
        else:
            confidence = 0.6

        key_points = [
            KeyPoint(name="consolidation_low", date=b0.end_date, price=b0_low, bi_index=0),
            KeyPoint(name="consolidation_high", date=b1.end_date, price=b1_high, bi_index=1),
            KeyPoint(name="breakdown_low", date=b2.end_date, price=b2_low, bi_index=2),
            KeyPoint(name="recovery_high", date=b3.end_date, price=b3_high, bi_index=3),
            KeyPoint(name="continuation", date=b4.end_date, price=b4.end_price, bi_index=4),
        ]

        result = PatternResult(
            pattern_type=PatternType.PODIE_FAN,
            direction=PatternDirection.BULLISH,
            start_date=b0.start_date,
            end_date=b4.end_date,
            neckline_price=neckline_price,
            neckline_slope=0.0,
            key_points=key_points,
            bi_indices=[0, 1, 2, 3, 4],
            status=PatternStatus.CONFIRMED,
            confidence=confidence,
        )

        return result
