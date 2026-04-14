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


class MTopDetector(BasePatternDetector):

    def __init__(self, price_tolerance: float = 0.05):
        self.price_tolerance = price_tolerance
        self.validator = NecklineValidator()

    @property
    def required_bi_count(self) -> int:
        return 4

    def detect(self, bis: list, kline_df) -> list:
        results = []
        if len(bis) < 4:
            return results

        for i in range(len(bis) - 3):
            window = bis[i : i + 4]
            result = self._check_window(window, kline_df)
            if result:
                results.append(result)
        return results

    def _check_window(self, window: list, kline_df) -> "PatternResult | None":
        b0, b1, b2, b3 = window

        if b0.direction != BiDirection.UP:
            return None
        if b1.direction != BiDirection.DOWN:
            return None
        if b2.direction != BiDirection.UP:
            return None
        if b3.direction != BiDirection.DOWN:
            return None

        left_top = b0.end_price
        right_top = b2.end_price
        price_diff = abs(left_top - right_top) / max(left_top, right_top)

        if price_diff > self.price_tolerance:
            return None

        neckline_price = b1.end_price
        last_close = b3.end_price
        breakout_date = b3.end_date

        key_points = [
            KeyPoint(name="left_top", date=b0.end_date, price=left_top, bi_index=0),
            KeyPoint(name="neckline_left", date=b1.end_date, price=neckline_price, bi_index=1),
            KeyPoint(name="right_top", date=b2.end_date, price=right_top, bi_index=2),
            KeyPoint(name="breakdown", date=b3.end_date, price=last_close, bi_index=3),
        ]

        result = PatternResult(
            pattern_type=PatternType.M_TOP,
            direction=PatternDirection.BEARISH,
            start_date=b0.start_date,
            end_date=b3.end_date,
            neckline_price=neckline_price,
            neckline_slope=0.0,
            key_points=key_points,
            bi_indices=[0, 1, 2, 3],
            confidence=0.5,
        )

        if last_close < neckline_price:
            validation = self.validator.validate(
                kline_df, breakout_date, neckline_price, direction="bear"
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
