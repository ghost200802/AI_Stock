import pytest

from modules.chanlun.include_processor import ProcessedKLine
from modules.chanlun.fractal_detector import Fractal, FractalType
from modules.chanlun.bi_generator import BiGenerator, BiDirection


def _make_klines(count, base_high=10, base_low=5):
    klines = []
    for i in range(count):
        klines.append(ProcessedKLine(
            trade_date=f"2024-01-{i+1:02d}",
            open=base_low,
            high=base_high,
            low=base_low,
            close=base_low,
            volume=100,
            original_indices=[i],
        ))
    return klines


def _make_fractal(ftype, index, high, low, date_str="2024-01-01"):
    return Fractal(
        fractal_type=ftype,
        index=index,
        high=high,
        low=low,
        trade_date=date_str,
    )


class TestSimpleBiGeneration:

    def test_top_bottom_alternating(self):
        fractals = [
            _make_fractal(FractalType.TOP, 2, 20, 15, "2024-01-03"),
            _make_fractal(FractalType.BOTTOM, 5, 12, 8, "2024-01-06"),
            _make_fractal(FractalType.TOP, 8, 18, 14, "2024-01-09"),
        ]
        klines = _make_klines(10, base_high=25, base_low=5)

        gen = BiGenerator()
        bis = gen.generate(fractals, klines)

        assert len(bis) == 2
        assert bis[0].direction == BiDirection.DOWN
        assert bis[1].direction == BiDirection.UP

    def test_bottom_top_alternating(self):
        fractals = [
            _make_fractal(FractalType.BOTTOM, 2, 12, 8, "2024-01-03"),
            _make_fractal(FractalType.TOP, 5, 20, 15, "2024-01-06"),
            _make_fractal(FractalType.BOTTOM, 8, 12, 6, "2024-01-09"),
        ]
        klines = _make_klines(10, base_high=25, base_low=5)

        gen = BiGenerator()
        bis = gen.generate(fractals, klines)

        assert len(bis) == 2
        assert bis[0].direction == BiDirection.UP
        assert bis[1].direction == BiDirection.DOWN


class TestDistanceCheck:

    def test_insufficient_distance(self):
        fractals = [
            _make_fractal(FractalType.TOP, 2, 20, 15, "2024-01-03"),
            _make_fractal(FractalType.BOTTOM, 3, 12, 8, "2024-01-04"),
        ]
        klines = _make_klines(5, base_high=25, base_low=5)

        gen = BiGenerator()
        bis = gen.generate(fractals, klines)

        assert len(bis) == 0

    def test_exactly_min_distance(self):
        fractals = [
            _make_fractal(FractalType.TOP, 2, 20, 15, "2024-01-03"),
            _make_fractal(FractalType.BOTTOM, 4, 12, 8, "2024-01-05"),
        ]
        klines = _make_klines(6, base_high=25, base_low=5)

        gen = BiGenerator()
        bis = gen.generate(fractals, klines)

        assert len(bis) == 1


class TestPriceChange:

    def test_price_change_calculation(self):
        fractals = [
            _make_fractal(FractalType.BOTTOM, 2, 12, 8, "2024-01-03"),
            _make_fractal(FractalType.TOP, 5, 20, 15, "2024-01-06"),
        ]
        klines = _make_klines(7, base_high=25, base_low=5)

        gen = BiGenerator()
        bis = gen.generate(fractals, klines)

        assert len(bis) == 1
        assert bis[0].start_price == 8
        assert bis[0].end_price == 20


class TestConfirmedFlag:

    def test_last_bi_unconfirmed(self):
        fractals = [
            _make_fractal(FractalType.BOTTOM, 2, 12, 8, "2024-01-03"),
            _make_fractal(FractalType.TOP, 5, 20, 15, "2024-01-06"),
            _make_fractal(FractalType.BOTTOM, 8, 12, 6, "2024-01-09"),
        ]
        klines = _make_klines(10, base_high=25, base_low=5)

        gen = BiGenerator()
        bis = gen.generate(fractals, klines)

        assert len(bis) == 2
        assert bis[0].confirmed is True
        assert bis[1].confirmed is False


class TestEmptyInput:

    def test_empty_fractals(self):
        gen = BiGenerator()
        assert gen.generate([], []) == []

    def test_single_fractal(self):
        fractals = [_make_fractal(FractalType.TOP, 2, 20, 15)]
        klines = _make_klines(5)
        gen = BiGenerator()
        assert gen.generate(fractals, klines) == []


class TestMergeConsecutiveSameType:

    def test_equal_top_keeps_first(self):
        fractals = [
            _make_fractal(FractalType.TOP, 2, 20, 15, "2024-01-03"),
            _make_fractal(FractalType.TOP, 5, 20, 14, "2024-01-06"),
        ]
        result = BiGenerator._merge_consecutive_same_type(fractals)
        assert len(result) == 1
        assert result[0].high == 20

    def test_equal_bottom_keeps_first(self):
        fractals = [
            _make_fractal(FractalType.BOTTOM, 2, 12, 10, "2024-01-03"),
            _make_fractal(FractalType.BOTTOM, 5, 14, 10, "2024-01-06"),
        ]
        result = BiGenerator._merge_consecutive_same_type(fractals)
        assert len(result) == 1
        assert result[0].low == 10

    def test_top_higher_replaces(self):
        fractals = [
            _make_fractal(FractalType.TOP, 2, 18, 15, "2024-01-03"),
            _make_fractal(FractalType.TOP, 5, 20, 14, "2024-01-06"),
        ]
        result = BiGenerator._merge_consecutive_same_type(fractals)
        assert len(result) == 1
        assert result[0].high == 20

    def test_top_lower_keeps_first(self):
        fractals = [
            _make_fractal(FractalType.TOP, 2, 20, 15, "2024-01-03"),
            _make_fractal(FractalType.TOP, 5, 18, 14, "2024-01-06"),
        ]
        result = BiGenerator._merge_consecutive_same_type(fractals)
        assert len(result) == 1
        assert result[0].high == 20

    def test_bottom_lower_replaces(self):
        fractals = [
            _make_fractal(FractalType.BOTTOM, 2, 12, 10, "2024-01-03"),
            _make_fractal(FractalType.BOTTOM, 5, 14, 8, "2024-01-06"),
        ]
        result = BiGenerator._merge_consecutive_same_type(fractals)
        assert len(result) == 1
        assert result[0].low == 8

    def test_bottom_higher_keeps_first(self):
        fractals = [
            _make_fractal(FractalType.BOTTOM, 2, 12, 8, "2024-01-03"),
            _make_fractal(FractalType.BOTTOM, 5, 14, 10, "2024-01-06"),
        ]
        result = BiGenerator._merge_consecutive_same_type(fractals)
        assert len(result) == 1
        assert result[0].low == 8


class TestResolveConsecutiveSameType:

    def test_consecutive_tops_keeps_first(self):
        fractals = [
            _make_fractal(FractalType.TOP, 2, 20, 15, "2024-01-03"),
            _make_fractal(FractalType.TOP, 5, 18, 14, "2024-01-06"),
            _make_fractal(FractalType.BOTTOM, 8, 12, 10, "2024-01-09"),
        ]
        result = BiGenerator._resolve_consecutive_same_type(fractals)
        assert len(result) == 2
        assert result[0].fractal_type == FractalType.TOP
        assert result[0].high == 20
        assert result[1].fractal_type == FractalType.BOTTOM

    def test_consecutive_bottoms_keeps_first(self):
        fractals = [
            _make_fractal(FractalType.BOTTOM, 2, 12, 8, "2024-01-03"),
            _make_fractal(FractalType.BOTTOM, 5, 14, 10, "2024-01-06"),
            _make_fractal(FractalType.TOP, 8, 20, 15, "2024-01-09"),
        ]
        result = BiGenerator._resolve_consecutive_same_type(fractals)
        assert len(result) == 2
        assert result[0].fractal_type == FractalType.BOTTOM
        assert result[0].low == 8
        assert result[1].fractal_type == FractalType.TOP

    def test_alternating_fractals_unchanged(self):
        fractals = [
            _make_fractal(FractalType.TOP, 2, 20, 15, "2024-01-03"),
            _make_fractal(FractalType.BOTTOM, 5, 12, 8, "2024-01-06"),
            _make_fractal(FractalType.TOP, 8, 18, 14, "2024-01-09"),
        ]
        result = BiGenerator._resolve_consecutive_same_type(fractals)
        assert len(result) == 3

    def test_consecutive_tops_with_higher_later(self):
        fractals = [
            _make_fractal(FractalType.TOP, 2, 20, 15, "2024-01-03"),
            _make_fractal(FractalType.TOP, 5, 22, 14, "2024-01-06"),
            _make_fractal(FractalType.BOTTOM, 8, 12, 10, "2024-01-09"),
        ]
        result = BiGenerator._resolve_consecutive_same_type(fractals)
        assert len(result) == 2
        assert result[0].fractal_type == FractalType.TOP
        assert result[0].high == 22
        assert result[1].fractal_type == FractalType.BOTTOM


class TestDistanceCheckNoUpdate:

    def test_distance_skip_updates_prev(self):
        fractals = [
            _make_fractal(FractalType.TOP, 2, 20, 15, "2024-01-03"),
            _make_fractal(FractalType.BOTTOM, 3, 12, 8, "2024-01-04"),
            _make_fractal(FractalType.BOTTOM, 5, 14, 6, "2024-01-06"),
        ]
        klines = _make_klines(10, base_high=25, base_low=5)

        gen = BiGenerator()
        bis = gen.generate(fractals, klines)

        assert len(bis) == 1
        assert bis[0].direction == BiDirection.DOWN
        assert bis[0].start_index == 2
        assert bis[0].end_index == 5


class TestEndToEndWithConsecutiveSameType:

    def test_consecutive_tops_resolved_in_generate(self):
        fractals = [
            _make_fractal(FractalType.TOP, 2, 20, 15, "2024-01-03"),
            _make_fractal(FractalType.TOP, 5, 18, 14, "2024-01-06"),
            _make_fractal(FractalType.BOTTOM, 8, 12, 10, "2024-01-09"),
            _make_fractal(FractalType.TOP, 11, 15, 12, "2024-01-12"),
        ]
        klines = _make_klines(15, base_high=25, base_low=5)

        gen = BiGenerator()
        bis = gen.generate(fractals, klines)

        directions = [b.direction for b in bis]
        for i in range(1, len(directions)):
            assert directions[i] != directions[i - 1], "Directions must alternate"

    def test_consecutive_bottoms_resolved_in_generate(self):
        fractals = [
            _make_fractal(FractalType.BOTTOM, 2, 12, 8, "2024-01-03"),
            _make_fractal(FractalType.BOTTOM, 5, 14, 10, "2024-01-06"),
            _make_fractal(FractalType.TOP, 8, 20, 15, "2024-01-09"),
            _make_fractal(FractalType.BOTTOM, 11, 14, 12, "2024-01-12"),
        ]
        klines = _make_klines(15, base_high=25, base_low=5)

        gen = BiGenerator()
        bis = gen.generate(fractals, klines)

        directions = [b.direction for b in bis]
        for i in range(1, len(directions)):
            assert directions[i] != directions[i - 1], "Directions must alternate"


class TestDirectionSafetyNet:

    def test_safety_net_removes_consecutive_same_direction(self):
        from modules.chanlun.bi_generator import Bi
        from modules.chanlun.fractal_detector import Fractal

        bis = [
            Bi(
                direction=BiDirection.DOWN,
                start_date="2024-01-01", end_date="2024-01-02",
                start_price=20, end_price=10,
                start_index=0, end_index=2,
            ),
            Bi(
                direction=BiDirection.DOWN,
                start_date="2024-01-02", end_date="2024-01-03",
                start_price=10, end_price=5,
                start_index=2, end_index=4,
            ),
            Bi(
                direction=BiDirection.UP,
                start_date="2024-01-03", end_date="2024-01-04",
                start_price=5, end_price=15,
                start_index=4, end_index=6,
            ),
        ]
        result = BiGenerator._ensure_alternating_direction(bis, [])
        assert len(result) == 2
        assert result[0].direction == BiDirection.DOWN
        assert result[0].end_price == 5
        assert result[1].direction == BiDirection.UP

    def test_safety_net_keeps_better_consecutive(self):
        from modules.chanlun.bi_generator import Bi

        bis = [
            Bi(
                direction=BiDirection.DOWN,
                start_date="2024-01-01", end_date="2024-01-02",
                start_price=20, end_price=12,
                start_index=0, end_index=2,
            ),
            Bi(
                direction=BiDirection.DOWN,
                start_date="2024-01-02", end_date="2024-01-03",
                start_price=12, end_price=8,
                start_index=2, end_index=4,
            ),
        ]
        result = BiGenerator._ensure_alternating_direction(bis, [])
        assert len(result) == 1
        assert result[0].end_price == 8
