import logging

import pandas as pd

from .pattern_base import (
    BasePatternDetector,
    KeyPoint,
    PatternDirection,
    PatternResult,
    PatternStatus,
    PatternType,
)

logger = logging.getLogger(__name__)

__all__ = [
    "PatternType",
    "PatternStatus",
    "PatternDirection",
    "KeyPoint",
    "PatternResult",
    "BasePatternDetector",
    "compute_patterns",
]


def compute_patterns(kline_df: pd.DataFrame) -> pd.DataFrame:
    if kline_df is None or kline_df.empty:
        return pd.DataFrame()

    from modules.chanlun import IncludeProcessor, FractalDetector, BiGenerator

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

    if not bis:
        return pd.DataFrame()

    from .pattern_scanner import PatternScanner
    from .strength_scorer import StrengthScorer
    from .target_price import TargetPriceCalculator

    scanner = PatternScanner()
    results = scanner.scan(bis, kline_df)

    if not results:
        return pd.DataFrame()

    scorer = StrengthScorer()
    calculator = TargetPriceCalculator()

    for r in results:
        scorer.score(r, bis, kline_df)
        calculator.calculate(r)

    return _results_to_dataframe(results)


def _results_to_dataframe(results: list) -> pd.DataFrame:
    rows = []
    for r in results:
        if r.pattern_type is None:
            continue

        kp_dates = []
        kp_prices = []
        for kp in r.key_points:
            kp_dates.append(kp.date)
            kp_prices.append(kp.price)

        neck_start = r.key_points[0].date if r.key_points else None
        neck_end = r.key_points[1].date if len(r.key_points) > 1 else None
        target_date = r.end_date
        stop_loss_date = r.end_date
        key_point_date = kp_dates[0] if kp_dates else None
        key_point_price = kp_prices[0] if kp_prices else None

        rows.append({
            "pattern_type": r.pattern_type.value,
            "direction": r.direction.value if r.direction else None,
            "status": r.status.value if r.status else None,
            "strength_score": r.strength_score,
            "volume_score": r.volume_score,
            "target_price": r.target_price,
            "stop_loss_price": r.stop_loss_price,
            "neckline_start_date": neck_start,
            "neckline_end_date": neck_end,
            "neckline_price": r.neckline_price,
            "target_date": target_date,
            "stop_loss_date": stop_loss_date,
            "key_point_date": key_point_date,
            "key_point_price": key_point_price,
            "confidence": r.confidence,
            "start_date": r.start_date,
            "end_date": r.end_date,
        })

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)
