import logging
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

logger = logging.getLogger(__name__)


class BiDirection(Enum):
    UP = "up"
    DOWN = "down"


@dataclass
class Bi:
    direction: BiDirection
    start_date: object
    end_date: object
    start_price: float
    end_price: float
    start_index: int
    end_index: int
    confirmed: bool = True


class BiGenerator:

    def __init__(self, min_independent_klines: int = 1):
        self.min_independent_klines = min_independent_klines

    def generate(self, fractals: List, processed_klines: List) -> List[Bi]:
        if not fractals or len(fractals) < 2 or not processed_klines:
            return []

        from .fractal_detector import FractalType

        merged = self._merge_consecutive_same_type(fractals)
        if len(merged) < 2:
            return []

        resolved = self._resolve_consecutive_same_type(merged)
        if len(resolved) < 2:
            return []

        bis = []
        prev = resolved[0]

        for i in range(1, len(resolved)):
            curr = resolved[i]

            if not self._check_distance(prev, curr, processed_klines):
                prev = curr
                continue

            direction = self._determine_direction(prev, curr)

            bi = Bi(
                direction=direction,
                start_date=prev.trade_date,
                end_date=curr.trade_date,
                start_price=prev.high if direction == BiDirection.DOWN else prev.low,
                end_price=curr.low if direction == BiDirection.DOWN else curr.high,
                start_index=prev.index,
                end_index=curr.index,
                confirmed=True,
            )
            bis.append(bi)
            prev = curr

        bis = self._ensure_alternating_direction(bis, resolved)

        if bis:
            bis[-1].confirmed = False

        return bis

    @staticmethod
    def _merge_consecutive_same_type(fractals: List) -> List:
        if not fractals:
            return []

        from .fractal_detector import FractalType

        merged = [fractals[0]]

        for f in fractals[1:]:
            last = merged[-1]
            if f.fractal_type == last.fractal_type:
                if f.fractal_type == FractalType.TOP:
                    if f.high > last.high:
                        merged[-1] = f
                elif f.fractal_type == FractalType.BOTTOM:
                    if f.low < last.low:
                        merged[-1] = f
            else:
                merged.append(f)

        return merged

    @staticmethod
    def _resolve_consecutive_same_type(fractals: List) -> List:
        if not fractals or len(fractals) < 2:
            return list(fractals)

        from .fractal_detector import FractalType

        result = [fractals[0]]

        for f in fractals[1:]:
            last = result[-1]
            if f.fractal_type == last.fractal_type:
                if f.fractal_type == FractalType.TOP:
                    if f.high > last.high:
                        result[-1] = f
                elif f.fractal_type == FractalType.BOTTOM:
                    if f.low < last.low:
                        result[-1] = f
            else:
                result.append(f)

        return result

    @staticmethod
    def _ensure_alternating_direction(bis: List, resolved: List) -> List:
        if len(bis) < 2:
            return bis

        cleaned = [bis[0]]
        for i in range(1, len(bis)):
            if bis[i].direction == cleaned[-1].direction:
                if bis[i].direction == BiDirection.DOWN:
                    if bis[i].end_price < cleaned[-1].end_price:
                        cleaned[-1] = bis[i]
                else:
                    if bis[i].end_price > cleaned[-1].end_price:
                        cleaned[-1] = bis[i]
            else:
                cleaned.append(bis[i])

        return cleaned

    def _check_distance(self, prev, curr, klines: List) -> bool:
        independent_count = curr.index - prev.index - 1
        return independent_count >= self.min_independent_klines

    @staticmethod
    def _determine_direction(prev, curr) -> 'BiDirection':
        from .fractal_detector import FractalType

        if prev.fractal_type == FractalType.TOP:
            return BiDirection.DOWN
        else:
            return BiDirection.UP

    @staticmethod
    def _calc_price_change(prev, curr, direction) -> float:
        from .fractal_detector import FractalType

        if direction == BiDirection.UP:
            start = prev.low
            end = curr.high
        else:
            start = prev.high
            end = curr.low

        if start == 0:
            return 0.0
        return (end - start) / abs(start) * 100
