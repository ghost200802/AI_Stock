import logging

from .pattern_base import PatternDirection, PatternResult, PatternType

logger = logging.getLogger(__name__)


class TargetPriceCalculator:

    def __init__(self, stop_loss_pct: float = 0.03):
        self.stop_loss_pct = stop_loss_pct

    def calculate(self, result: PatternResult):
        result.target_price = self._calc_target_price(result)
        result.stop_loss_price = self._calc_stop_loss(result)

    def _calc_target_price(self, result: PatternResult) -> float:
        pt = result.pattern_type
        nl = result.neckline_price

        if pt == PatternType.W_BOTTOM:
            bottom = self._get_extreme_price(result, ["left_bottom", "right_bottom"], min)
            if bottom <= 0 or nl <= 0:
                return 0.0
            return round(nl + (nl - bottom), 2)

        elif pt == PatternType.M_TOP:
            top = self._get_extreme_price(result, ["left_top", "right_top"], max)
            if top <= 0 or nl <= 0:
                return 0.0
            return round(nl - (top - nl), 2)

        elif pt == PatternType.HEAD_SHOULDER_BOTTOM:
            head = self._get_extreme_price(result, ["head"], min)
            if head <= 0 or nl <= 0:
                return 0.0
            return round(nl + 1.382 * (nl - head), 2)

        elif pt == PatternType.HEAD_SHOULDER_TOP:
            head = self._get_extreme_price(result, ["head"], max)
            if head <= 0 or nl <= 0:
                return 0.0
            return round(nl - 1.382 * (head - nl), 2)

        elif pt == PatternType.BREAKOUT_FAIL:
            fake_high = self._get_extreme_price(result, ["fake_breakout_high"], max)
            if fake_high <= 0 or nl <= 0:
                return 0.0
            return round(nl - (fake_high - nl), 2)

        elif pt == PatternType.PODIE_FAN:
            upper = self._get_extreme_price(result, ["consolidation_high", "recovery_high"], max)
            lower = self._get_extreme_price(result, ["consolidation_low", "breakdown_low"], min)
            if upper <= 0 or lower <= 0:
                return 0.0
            return round(upper + (upper - lower), 2)

        elif pt == PatternType.FLAG_DOWN:
            pole_high = self._get_extreme_price(result, ["flagpole_high"], max)
            pole_low = self._get_extreme_price(result, ["flagpole_low"], min)
            if pole_high <= 0 or pole_low <= 0:
                return 0.0
            pole_height = pole_high - pole_low
            breakout_price = self._get_last_price(result)
            return round(breakout_price + pole_height, 2)

        elif pt == PatternType.FLAG_UP:
            pole_high = self._get_extreme_price(result, ["flagpole_high"], max)
            pole_low = self._get_extreme_price(result, ["flagpole_low"], min)
            if pole_high <= 0 or pole_low <= 0:
                return 0.0
            pole_height = pole_high - pole_low
            breakdown_price = self._get_last_price(result)
            return round(breakdown_price - pole_height, 2)

        elif pt in (PatternType.TRIANGLE_BOTTOM, PatternType.ASCENDING_TRIANGLE):
            upper_start = self._get_extreme_price(result, ["upper_start", "resistance"], max)
            lower_start = self._get_extreme_price(result, ["lower_start", "support_start"], min)
            if upper_start <= 0 or lower_start <= 0:
                return 0.0
            height = upper_start - lower_start
            breakout_price = self._get_last_price(result)
            return round(breakout_price + height, 2)

        elif pt in (PatternType.TRIANGLE_HEAD, PatternType.DESCENDING_TRIANGLE):
            upper_start = self._get_extreme_price(result, ["upper_start", "resistance_start"], max)
            lower_start = self._get_extreme_price(result, ["lower_start", "support"], min)
            if upper_start <= 0 or lower_start <= 0:
                return 0.0
            height = upper_start - lower_start
            breakdown_price = self._get_last_price(result)
            return round(breakdown_price - height, 2)

        return 0.0

    def _calc_stop_loss(self, result: PatternResult) -> float:
        pt = result.pattern_type

        if pt == PatternType.PODIE_FAN:
            lower = self._get_extreme_price(result, ["consolidation_low", "breakdown_low"], min)
            if lower > 0:
                return round(lower, 2)

        if result.direction == PatternDirection.BULLISH:
            lowest = self._get_lowest_price(result)
            if lowest > 0:
                return round(lowest * (1 - self.stop_loss_pct), 2)
        elif result.direction == PatternDirection.BEARISH:
            highest = self._get_highest_price(result)
            if highest > 0:
                return round(highest * (1 + self.stop_loss_pct), 2)

        return 0.0

    def _get_extreme_price(self, result: PatternResult, names: list, func) -> float:
        prices = []
        for kp in result.key_points:
            if kp.name in names:
                prices.append(kp.price)
        if not prices:
            return 0.0
        return func(prices)

    def _get_lowest_price(self, result: PatternResult) -> float:
        if not result.key_points:
            return 0.0
        return min(kp.price for kp in result.key_points)

    def _get_highest_price(self, result: PatternResult) -> float:
        if not result.key_points:
            return 0.0
        return max(kp.price for kp in result.key_points)

    def _get_last_price(self, result: PatternResult) -> float:
        if result.key_points:
            return result.key_points[-1].price
        return result.neckline_price
