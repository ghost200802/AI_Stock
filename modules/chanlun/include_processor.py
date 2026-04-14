import logging
from dataclasses import dataclass, field
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ProcessedKLine:
    trade_date: object
    open: float
    high: float
    low: float
    close: float
    volume: float
    original_indices: List[int] = field(default_factory=list)


class IncludeProcessor:

    def process(self, df: pd.DataFrame) -> List[ProcessedKLine]:
        if df is None or df.empty:
            return []

        required_cols = ["high", "low"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"输入 DataFrame 缺少必要列: {col}")

        klines = self._to_kline_list(df)
        if len(klines) < 1:
            return []

        result = [klines[0]]

        for i in range(1, len(klines)):
            current = klines[i]
            prev = result[-1]

            if self._is_include(prev, current):
                direction = self._determine_direction(result, current)
                merged = self._merge(prev, current, direction)
                result[-1] = merged
            else:
                result.append(current)

        return result

    @staticmethod
    def _is_include(prev: ProcessedKLine, current: ProcessedKLine) -> bool:
        return (prev.high >= current.high and prev.low <= current.low) or \
               (prev.high <= current.high and prev.low >= current.low)

    def _determine_direction(self, result: List[ProcessedKLine], current: ProcessedKLine) -> str:
        if len(result) >= 2:
            first = result[-2]
            second = result[-1]
            if second.high > first.high:
                return "up"
            elif second.high < first.high:
                return "down"

        if result[0].close >= result[0].open:
            return "up"
        else:
            return "down"

    @staticmethod
    def _merge(prev: ProcessedKLine, current: ProcessedKLine, direction: str) -> ProcessedKLine:
        if direction == "up":
            high = max(prev.high, current.high)
            low = max(prev.low, current.low)
        else:
            high = min(prev.high, current.high)
            low = min(prev.low, current.low)

        return ProcessedKLine(
            trade_date=current.trade_date,
            open=prev.open,
            high=high,
            low=low,
            close=current.close,
            volume=prev.volume + current.volume,
            original_indices=prev.original_indices + current.original_indices,
        )

    @staticmethod
    def _to_kline_list(df: pd.DataFrame) -> List[ProcessedKLine]:
        klines = []
        for idx, row in df.iterrows():
            klines.append(ProcessedKLine(
                trade_date=row.get("trade_date"),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row.get("volume", 0)),
                original_indices=[idx],
            ))
        return klines
