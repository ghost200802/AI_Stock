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


class HeadShoulderTopDetector(BasePatternDetector):

    def __init__(self, shoulder_tolerance: float = 0.10):
        self.shoulder_tolerance = shoulder_tolerance
        self.validator = NecklineValidator()

    @property
    def required_bi_count(self) -> int:
        return 6

    def detect(self, bis: list, kline_df) -> list:
        results = []
        if len(bis) < 6:
            return results

        for i in range(len(bis) - 5):
            window = bis[i : i + 6]
            result = self._check_window(window, kline_df)
            if result:
                results.append(result)
        return results

    def _check_window(self, window: list, kline_df) -> "PatternResult | None":
        b0, b1, b2, b3, b4, b5 = window

        expected_dirs = [BiDirection.UP, BiDirection.DOWN, BiDirection.UP, BiDirection.DOWN, BiDirection.UP, BiDirection.DOWN]
        for bi, expected in zip(window, expected_dirs):
            if bi.direction != expected:
                return None

        left_shoulder = b0.end_price
        head = b2.end_price
        right_shoulder = b4.end_price

        if head <= left_shoulder or head <= right_shoulder:
            return None

        shoulder_diff = abs(left_shoulder - right_shoulder) / max(left_shoulder, right_shoulder)
        if shoulder_diff > self.shoulder_tolerance:
            return None

        neckline_start_price = b1.end_price
        neckline_end_price = b3.end_price
        neckline_price = (neckline_start_price + neckline_end_price) / 2.0

        from .head_shoulder_bottom import _estimate_days

        date_diff_days = _estimate_days(b1.end_date, b3.end_date)
        if date_diff_days > 0:
            price_diff_line = neckline_end_price - neckline_start_price
            neckline_slope = price_diff_line / date_diff_days
        else:
            neckline_slope = 0.0

        last_close = b5.end_price
        breakout_date = b5.end_date

        key_points = [
            KeyPoint(name="left_shoulder", date=b0.end_date, price=left_shoulder, bi_index=0),
            KeyPoint(name="neckline_left", date=b1.end_date, price=neckline_start_price, bi_index=1),
            KeyPoint(name="head", date=b2.end_date, price=head, bi_index=2),
            KeyPoint(name="neckline_right", date=b3.end_date, price=neckline_end_price, bi_index=3),
            KeyPoint(name="right_shoulder", date=b4.end_date, price=right_shoulder, bi_index=4),
            KeyPoint(name="breakdown", date=b5.end_date, price=last_close, bi_index=5),
        ]

        result = PatternResult(
            pattern_type=PatternType.HEAD_SHOULDER_TOP,
            direction=PatternDirection.BEARISH,
            start_date=b0.start_date,
            end_date=b5.end_date,
            neckline_price=neckline_price,
            neckline_slope=neckline_slope,
            key_points=key_points,
            bi_indices=[0, 1, 2, 3, 4, 5],
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
