import pytest

from modules.chanlun.include_processor import IncludeProcessor, ProcessedKLine
from modules.chanlun.fractal_detector import FractalDetector, Fractal, FractalType


def _make_processed_klines(data):
    klines = []
    for i, d in enumerate(data):
        klines.append(ProcessedKLine(
            trade_date=d[0],
            open=d[1],
            high=d[2],
            low=d[3],
            close=d[4],
            volume=d[5] if len(d) > 5 else 0,
            original_indices=[i],
        ))
    return klines


class TestFractalDetection:

    def test_top_fractal(self):
        klines = _make_processed_klines([
            ("2024-01-01", 10, 12, 8, 11),
            ("2024-01-02", 12, 15, 11, 14),
            ("2024-01-03", 14, 13, 10, 12),
        ])
        detector = FractalDetector()
        fractals = detector.detect(klines)
        assert len(fractals) == 1
        assert fractals[0].fractal_type == FractalType.TOP
        assert fractals[0].high == 15

    def test_bottom_fractal(self):
        klines = _make_processed_klines([
            ("2024-01-01", 14, 15, 12, 13),
            ("2024-01-02", 12, 13, 8, 9),
            ("2024-01-03", 10, 14, 11, 13),
        ])
        detector = FractalDetector()
        fractals = detector.detect(klines)
        assert len(fractals) == 1
        assert fractals[0].fractal_type == FractalType.BOTTOM
        assert fractals[0].low == 8

    def test_both_fractals(self):
        klines = _make_processed_klines([
            ("2024-01-01", 11, 12, 10, 11),
            ("2024-01-02", 13, 18, 14, 16),
            ("2024-01-03", 12, 13, 12, 12),
            ("2024-01-04", 10, 11, 10, 10),
            ("2024-01-05", 7, 8, 7, 7),
            ("2024-01-06", 5, 6, 4, 5),
            ("2024-01-07", 7, 9, 6, 8),
            ("2024-01-08", 8, 10, 8, 9),
        ])
        detector = FractalDetector()
        fractals = detector.detect(klines)
        types = [f.fractal_type for f in fractals]
        assert FractalType.TOP in types
        assert FractalType.BOTTOM in types
        top_fractals = [f for f in fractals if f.fractal_type == FractalType.TOP]
        bottom_fractals = [f for f in fractals if f.fractal_type == FractalType.BOTTOM]
        assert len(top_fractals) == 1
        assert len(bottom_fractals) == 1
        assert top_fractals[0].index == 1
        assert bottom_fractals[0].index == 5


class TestNoFractals:

    def test_monotonic_up(self):
        klines = _make_processed_klines([
            ("2024-01-01", 10, 11, 9, 10),
            ("2024-01-02", 11, 13, 10, 12),
            ("2024-01-03", 12, 15, 11, 14),
            ("2024-01-04", 14, 17, 13, 16),
        ])
        detector = FractalDetector()
        fractals = detector.detect(klines)
        assert len(fractals) == 0

    def test_monotonic_down(self):
        klines = _make_processed_klines([
            ("2024-01-01", 15, 16, 14, 14),
            ("2024-01-02", 14, 14, 11, 12),
            ("2024-01-03", 12, 12, 9, 10),
            ("2024-01-04", 10, 10, 7, 8),
        ])
        detector = FractalDetector()
        fractals = detector.detect(klines)
        assert len(fractals) == 0


class TestEdgeCases:

    def test_empty_klines(self):
        detector = FractalDetector()
        assert detector.detect([]) == []

    def test_less_than_3_klines(self):
        klines = _make_processed_klines([
            ("2024-01-01", 10, 12, 8, 11),
            ("2024-01-02", 12, 14, 10, 13),
        ])
        detector = FractalDetector()
        assert detector.detect(klines) == []


class TestConsecutiveSameTypeFilter:

    def test_consecutive_tops(self):
        klines = _make_processed_klines([
            ("2024-01-01", 10, 12, 8, 11),
            ("2024-01-02", 12, 18, 9, 14),
            ("2024-01-03", 14, 16, 13, 15),
            ("2024-01-04", 15, 20, 14, 17),
            ("2024-01-05", 17, 14, 10, 13),
        ])
        detector = FractalDetector()
        fractals = detector.detect(klines)
        top_fractals = [f for f in fractals if f.fractal_type == FractalType.TOP]
        assert len(top_fractals) == 1
        assert top_fractals[0].high == 20

    def test_equal_top_keeps_first(self):
        fractals = [
            Fractal(FractalType.TOP, 2, 20, 15, "2024-01-03"),
            Fractal(FractalType.TOP, 5, 20, 14, "2024-01-06"),
        ]
        result = FractalDetector._filter_consecutive_same_type(fractals, [])
        assert len(result) == 1
        assert result[0].high == 20

    def test_equal_bottom_keeps_first(self):
        fractals = [
            Fractal(FractalType.BOTTOM, 2, 12, 10, "2024-01-03"),
            Fractal(FractalType.BOTTOM, 5, 14, 10, "2024-01-06"),
        ]
        result = FractalDetector._filter_consecutive_same_type(fractals, [])
        assert len(result) == 1
        assert result[0].low == 10
