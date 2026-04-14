import logging
from dataclasses import dataclass
from enum import Enum
from typing import List

logger = logging.getLogger(__name__)


class FractalType(Enum):
    TOP = "top"
    BOTTOM = "bottom"


@dataclass
class Fractal:
    fractal_type: FractalType
    index: int
    high: float
    low: float
    trade_date: object


class FractalDetector:

    def detect(self, processed_klines: List) -> List[Fractal]:
        if not processed_klines or len(processed_klines) < 3:
            return []

        raw_fractals = self._find_fractals(processed_klines)
        filtered = self._filter_consecutive_same_type(raw_fractals, processed_klines)
        filtered = self._filter_by_distance_and_alternation(filtered)
        return filtered

    def _find_fractals(self, klines: List) -> List[Fractal]:
        fractals = []
        for i in range(1, len(klines) - 1):
            prev_k = klines[i - 1]
            curr_k = klines[i]
            next_k = klines[i + 1]

            if self._is_top_fractal(prev_k, curr_k, next_k):
                fractals.append(Fractal(
                    fractal_type=FractalType.TOP,
                    index=i,
                    high=curr_k.high,
                    low=curr_k.low,
                    trade_date=curr_k.trade_date,
                ))
            elif self._is_bottom_fractal(prev_k, curr_k, next_k):
                fractals.append(Fractal(
                    fractal_type=FractalType.BOTTOM,
                    index=i,
                    high=curr_k.high,
                    low=curr_k.low,
                    trade_date=curr_k.trade_date,
                ))

        return fractals

    @staticmethod
    def _is_top_fractal(prev, curr, next_k) -> bool:
        return curr.high >= prev.high and curr.high >= next_k.high and \
               curr.low >= prev.low and curr.low >= next_k.low

    @staticmethod
    def _is_bottom_fractal(prev, curr, next_k) -> bool:
        return curr.high <= prev.high and curr.high <= next_k.high and \
               curr.low <= prev.low and curr.low <= next_k.low

    @staticmethod
    def _filter_consecutive_same_type(fractals: List[Fractal], klines: List) -> List[Fractal]:
        if not fractals:
            return []

        filtered = [fractals[0]]

        for f in fractals[1:]:
            last = filtered[-1]

            if f.fractal_type == last.fractal_type:
                if f.fractal_type == FractalType.TOP:
                    if f.high > last.high:
                        filtered[-1] = f
                elif f.fractal_type == FractalType.BOTTOM:
                    if f.low < last.low:
                        filtered[-1] = f
            else:
                filtered.append(f)

        return filtered

    @staticmethod
    def _filter_by_distance_and_alternation(fractals: List[Fractal]) -> List[Fractal]:
        if not fractals or len(fractals) < 2:
            return list(fractals)

        result = [fractals[0]]

        for idx in range(1, len(fractals)):
            f = fractals[idx]
            last = result[-1]

            if f.fractal_type == last.fractal_type:
                if f.fractal_type == FractalType.TOP:
                    if f.high > last.high:
                        result[-1] = f
                else:
                    if f.low < last.low:
                        result[-1] = f
                continue

            if f.index - last.index >= 3:
                result.append(f)
                continue

            next_f = fractals[idx + 1] if idx + 1 < len(fractals) else None

            keep_last = False
            keep_f = False

            if next_f is not None and next_f.fractal_type == last.fractal_type and next_f.index - last.index >= 3:
                keep_last = True

            if next_f is not None and next_f.fractal_type == f.fractal_type and next_f.index - f.index >= 3:
                keep_f = True

            if keep_last and not keep_f:
                continue

            if keep_f and not keep_last:
                result[-1] = f
            elif keep_last and keep_f:
                if f.fractal_type == FractalType.TOP:
                    if f.high > last.high:
                        result[-1] = f
                else:
                    if f.low < last.low:
                        result[-1] = f
            else:
                result[-1] = f

            while len(result) >= 2 and result[-2].fractal_type == result[-1].fractal_type:
                pre = result[-2]
                cur = result[-1]
                if pre.fractal_type == FractalType.TOP:
                    if cur.high > pre.high:
                        result.pop(-2)
                    else:
                        result.pop(-1)
                        break
                else:
                    if cur.low < pre.low:
                        result.pop(-2)
                    else:
                        result.pop(-1)
                        break

        return result
