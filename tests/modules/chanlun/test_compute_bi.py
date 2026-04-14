import pandas as pd
import pytest

from modules.chanlun import compute_bi, FractalType, BiDirection, ProcessedKLine, Fractal, Bi


def _make_kline_df(data):
    rows = []
    for d in data:
        rows.append({
            "trade_date": d[0],
            "open": d[1],
            "high": d[2],
            "low": d[3],
            "close": d[4],
            "volume": d[5] if len(d) > 5 else 100,
        })
    return pd.DataFrame(rows)


class TestComputeBiEndToEnd:

    def test_basic_bi_computation(self):
        kline_data = [
            ("2024-01-01", 10, 12, 9, 11, 100),
            ("2024-01-02", 11, 14, 10, 13, 100),
            ("2024-01-03", 13, 16, 12, 15, 100),
            ("2024-01-04", 15, 18, 14, 17, 100),
            ("2024-01-05", 17, 20, 16, 19, 100),
            ("2024-01-06", 19, 18, 15, 16, 100),
            ("2024-01-07", 16, 15, 12, 13, 100),
            ("2024-01-08", 13, 12, 9, 10, 100),
            ("2024-01-09", 10, 9, 6, 7, 100),
            ("2024-01-10", 7, 6, 3, 4, 100),
            ("2024-01-11", 4, 5, 2, 3, 100),
            ("2024-01-12", 3, 4, 1, 2, 100),
            ("2024-01-13", 2, 3, 0.5, 1, 100),
            ("2024-01-14", 1, 2, 0.5, 1.5, 100),
            ("2024-01-15", 1.5, 3, 1, 2, 100),
            ("2024-01-16", 2, 5, 1.5, 4, 100),
            ("2024-01-17", 4, 8, 3, 7, 100),
            ("2024-01-18", 7, 12, 6, 11, 100),
            ("2024-01-19", 11, 15, 10, 14, 100),
            ("2024-01-20", 14, 18, 13, 17, 100),
        ]
        df = _make_kline_df(kline_data)
        result = compute_bi(df)

        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_returns_expected_columns(self):
        kline_data = [
            ("2024-01-01", 10, 12, 9, 11, 100),
            ("2024-01-02", 11, 14, 10, 13, 100),
            ("2024-01-03", 13, 16, 12, 15, 100),
            ("2024-01-04", 15, 18, 14, 17, 100),
            ("2024-01-05", 17, 20, 16, 19, 100),
            ("2024-01-06", 19, 18, 15, 16, 100),
            ("2024-01-07", 16, 15, 12, 13, 100),
            ("2024-01-08", 13, 12, 9, 10, 100),
            ("2024-01-09", 10, 9, 6, 7, 100),
            ("2024-01-10", 7, 6, 3, 4, 100),
            ("2024-01-11", 4, 5, 2, 3, 100),
            ("2024-01-12", 3, 4, 1, 2, 100),
            ("2024-01-13", 2, 3, 0.5, 1, 100),
            ("2024-01-14", 1, 2, 0.5, 1.5, 100),
            ("2024-01-15", 1.5, 3, 1, 2, 100),
            ("2024-01-16", 2, 5, 1.5, 4, 100),
            ("2024-01-17", 4, 8, 3, 7, 100),
            ("2024-01-18", 7, 12, 6, 11, 100),
            ("2024-01-19", 11, 15, 10, 14, 100),
            ("2024-01-20", 14, 18, 13, 17, 100),
        ]
        df = _make_kline_df(kline_data)
        result = compute_bi(df)

        expected_cols = [
            "fractal_type", "trade_date", "price",
            "bi_direction", "start_date", "end_date",
            "start_price", "end_price",
        ]
        for col in expected_cols:
            assert col in result.columns, f"缺少列: {col}"


class TestChanlunOverlayCompatibility:

    def test_fractal_rows_have_required_fields(self):
        kline_data = [
            ("2024-01-01", 10, 15, 9, 14, 100),
            ("2024-01-02", 14, 16, 13, 15, 100),
            ("2024-01-03", 15, 14, 11, 12, 100),
            ("2024-01-04", 12, 13, 10, 11, 100),
            ("2024-01-05", 11, 12, 6, 7, 100),
            ("2024-01-06", 7, 11, 6, 10, 100),
            ("2024-01-07", 10, 13, 9, 12, 100),
            ("2024-01-08", 12, 18, 11, 17, 100),
            ("2024-01-09", 17, 19, 16, 18, 100),
            ("2024-01-10", 18, 17, 13, 14, 100),
            ("2024-01-11", 14, 15, 11, 12, 100),
            ("2024-01-12", 12, 13, 5, 6, 100),
        ]
        df = _make_kline_df(kline_data)
        result = compute_bi(df)

        if not result.empty:
            fractal_rows = result[result["fractal_type"].notna()]
            for _, row in fractal_rows.iterrows():
                assert row["fractal_type"] in ("top", "bottom")
                assert row["trade_date"] is not None
                assert row["price"] is not None

            bi_rows = result[result["bi_direction"].notna()]
            for _, row in bi_rows.iterrows():
                assert row["bi_direction"] in ("up", "down")
                assert row["start_date"] is not None
                assert row["end_date"] is not None
                assert row["start_price"] is not None
                assert row["end_price"] is not None


class TestEmptyInput:

    def test_empty_dataframe(self):
        result = compute_bi(pd.DataFrame())
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_none_input(self):
        result = compute_bi(None)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_insufficient_klines(self):
        df = _make_kline_df([
            ("2024-01-01", 10, 12, 9, 11, 100),
            ("2024-01-02", 11, 14, 10, 13, 100),
        ])
        result = compute_bi(df)
        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestDataStructures:

    def test_fractal_type_enum(self):
        assert FractalType.TOP.value == "top"
        assert FractalType.BOTTOM.value == "bottom"

    def test_bi_direction_enum(self):
        assert BiDirection.UP.value == "up"
        assert BiDirection.DOWN.value == "down"

    def test_processed_kline(self):
        kline = ProcessedKLine(
            trade_date="2024-01-01",
            open=10, high=12, low=9, close=11, volume=100,
        )
        assert kline.high == 12
        assert kline.original_indices == []

    def test_fractal(self):
        f = Fractal(
            fractal_type=FractalType.TOP,
            index=5, high=20, low=15,
            trade_date="2024-01-06",
        )
        assert f.fractal_type == FractalType.TOP
        assert f.index == 5

    def test_bi(self):
        b = Bi(
            direction=BiDirection.UP,
            start_date="2024-01-01", end_date="2024-01-06",
            start_price=8, end_price=20,
            start_index=2, end_index=5,
        )
        assert b.confirmed is True
        assert b.direction == BiDirection.UP
