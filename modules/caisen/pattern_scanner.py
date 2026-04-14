import logging

from .pattern_base import (
    BasePatternDetector,
    PatternResult,
    PatternType,
)
from .breakout_fail import BreakoutFailDetector
from .podie_fan import PoDieFanDetector
from .flag_down import FlagDownDetector
from .flag_up import FlagUpDetector
from .head_shoulder_bottom import HeadShoulderBottomDetector
from .head_shoulder_top import HeadShoulderTopDetector
from .m_top import MTopDetector
from .triangle import TriangleDetector
from .ascending_triangle import AscendingTriangleDetector
from .descending_triangle import DescendingTriangleDetector
from .w_bottom import WBottomDetector

logger = logging.getLogger(__name__)


class PatternScanner:

    def __init__(self):
        self.detectors = self._create_detectors()

    def _create_detectors(self) -> list:
        return [
            WBottomDetector(),
            MTopDetector(),
            HeadShoulderBottomDetector(),
            HeadShoulderTopDetector(),
            BreakoutFailDetector(),
            PoDieFanDetector(),
            FlagDownDetector(),
            FlagUpDetector(),
            TriangleDetector(),
            AscendingTriangleDetector(),
            DescendingTriangleDetector(),
        ]

    def scan(self, bis: list, kline_df) -> list:
        all_results = []

        for detector in self.detectors:
            try:
                results = detector.detect(bis, kline_df)
                all_results.extend(results)
            except Exception:
                logger.debug("检测器 %s 执行异常", type(detector).__name__, exc_info=True)

        deduped = self._deduplicate(all_results)
        return deduped

    def _deduplicate(self, results: list) -> list:
        if not results:
            return []

        results.sort(key=lambda r: r.confidence, reverse=True)

        kept = []
        for r in results:
            overlaps = False
            for k in kept:
                if self._time_overlaps(r, k):
                    overlaps = True
                    break
            if not overlaps:
                kept.append(r)

        return kept

    def _time_overlaps(self, a: PatternResult, b: PatternResult) -> bool:
        if a.start_date is None or a.end_date is None:
            return False
        if b.start_date is None or b.end_date is None:
            return False

        try:
            import pandas as pd

            a_start = pd.Timestamp(a.start_date)
            a_end = pd.Timestamp(a.end_date)
            b_start = pd.Timestamp(b.start_date)
            b_end = pd.Timestamp(b.end_date)

            overlap_start = max(a_start, b_start)
            overlap_end = min(a_end, b_end)
            if overlap_start > overlap_end:
                return False

            a_len = max((a_end - a_start).days, 1)
            b_len = max((b_end - b_start).days, 1)
            overlap_len = (overlap_end - overlap_start).days
            min_len = min(a_len, b_len)

            return overlap_len / min_len > 0.5
        except Exception:
            return False
