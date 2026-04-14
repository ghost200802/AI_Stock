import logging
from enum import Enum

import pandas as pd

from .include_processor import IncludeProcessor, ProcessedKLine
from .fractal_detector import FractalDetector, Fractal, FractalType
from .bi_generator import BiGenerator, Bi, BiDirection

logger = logging.getLogger(__name__)

__all__ = [
    "FractalType",
    "BiDirection",
    "ProcessedKLine",
    "Fractal",
    "Bi",
    "IncludeProcessor",
    "FractalDetector",
    "BiGenerator",
    "compute_bi",
]


def compute_bi(kline_df: pd.DataFrame) -> pd.DataFrame:
    if kline_df is None or kline_df.empty:
        return pd.DataFrame()

    include_processor = IncludeProcessor()
    processed_klines = include_processor.process(kline_df)

    if len(processed_klines) < 3:
        return pd.DataFrame()

    fractal_detector = FractalDetector()
    fractals = fractal_detector.detect(processed_klines)

    if not fractals:
        return pd.DataFrame()

    bi_generator = BiGenerator()
    bis = bi_generator.generate(fractals, processed_klines)

    valid_fractals = _extract_valid_fractals(fractals, bis)

    return _convert_to_overlay_df(valid_fractals, bis, kline_df)


def _extract_valid_fractals(fractals: list, bis: list) -> list:
    if not bis:
        return []

    fractal_map = {}
    for f in fractals:
        key = (f.trade_date, f.fractal_type)
        fractal_map[key] = f

    valid = []
    for b in bis:
        start_key = (b.start_date, FractalType.TOP if b.direction == BiDirection.DOWN else FractalType.BOTTOM)
        end_key = (b.end_date, FractalType.BOTTOM if b.direction == BiDirection.DOWN else FractalType.TOP)
        if start_key in fractal_map and fractal_map[start_key] not in valid:
            valid.append(fractal_map[start_key])
        if end_key in fractal_map and fractal_map[end_key] not in valid:
            valid.append(fractal_map[end_key])

    return valid


def _convert_to_overlay_df(
    fractals: list,
    bis: list,
    kline_df: pd.DataFrame,
) -> pd.DataFrame:
    rows = []

    for f in fractals:
        price = f.high if f.fractal_type == FractalType.TOP else f.low
        rows.append({
            "trade_date": f.trade_date,
            "price": price,
            "fractal_type": f.fractal_type.value,
            "bi_direction": None,
            "start_date": None,
            "end_date": None,
            "start_price": None,
            "end_price": None,
        })

    for b in bis:
        rows.append({
            "trade_date": None,
            "price": None,
            "fractal_type": None,
            "bi_direction": b.direction.value,
            "start_date": b.start_date,
            "end_date": b.end_date,
            "start_price": b.start_price,
            "end_price": b.end_price,
        })

    return pd.DataFrame(rows)
