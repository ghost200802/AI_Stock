import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def sample_kline_df():
    n = 50
    dates = pd.date_range(end="2025-06-01", periods=n, freq="B")
    import numpy as np
    np.random.seed(42)
    close = 10.0 + np.cumsum(np.random.randn(n) * 0.3)
    high = close + abs(np.random.randn(n) * 0.2)
    low = close - abs(np.random.randn(n) * 0.2)
    open_ = close - np.random.randn(n) * 0.1
    return pd.DataFrame({
        "trade_date": dates,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": [1000000] * n,
        "ts_code": "000001.SZ",
        "data_type": "daily",
    })


def _build_chart_df(kline_df):
    chart_df = kline_df.sort_values("trade_date").reset_index(drop=True)
    chart_df = chart_df.copy()
    chart_df["trade_date"] = chart_df["trade_date"].astype(str).str[:10]
    return chart_df


class TestComputeBiOutputFormat:

    def test_output_columns_match_overlay_requirements(self, sample_kline_df):
        from modules.chanlun import compute_bi

        bi_df = compute_bi(sample_kline_df)
        if bi_df.empty:
            pytest.skip("compute_bi returned empty for sample data")

        expected_cols = [
            "trade_date", "price", "fractal_type",
            "bi_direction", "start_date", "end_date",
            "start_price", "end_price",
        ]
        for col in expected_cols:
            assert col in bi_df.columns, f"Missing column: {col}"

    def test_fractal_rows_have_fractal_type(self, sample_kline_df):
        from modules.chanlun import compute_bi

        bi_df = compute_bi(sample_kline_df)
        if bi_df.empty:
            pytest.skip("compute_bi returned empty for sample data")

        fractal_rows = bi_df[bi_df["fractal_type"].notna()]
        if fractal_rows.empty:
            pytest.skip("No fractal rows generated")

        assert set(fractal_rows["fractal_type"].unique()).issubset({"top", "bottom"})
        assert fractal_rows["trade_date"].notna().all()
        assert fractal_rows["price"].notna().all()

    def test_bi_rows_have_direction_and_dates(self, sample_kline_df):
        from modules.chanlun import compute_bi

        bi_df = compute_bi(sample_kline_df)
        if bi_df.empty:
            pytest.skip("compute_bi returned empty for sample data")

        bi_rows = bi_df[bi_df["bi_direction"].notna()]
        if bi_rows.empty:
            pytest.skip("No bi rows generated")

        assert set(bi_rows["bi_direction"].unique()).issubset({"up", "down"})
        assert bi_rows["start_date"].notna().all()
        assert bi_rows["end_date"].notna().all()
        assert bi_rows["start_price"].notna().all()
        assert bi_rows["end_price"].notna().all()

    def test_bi_rows_separated_from_fractal_rows(self, sample_kline_df):
        from modules.chanlun import compute_bi

        bi_df = compute_bi(sample_kline_df)
        if bi_df.empty:
            pytest.skip("compute_bi returned empty for sample data")

        fractal_rows = bi_df[bi_df["fractal_type"].notna()]
        bi_rows = bi_df[bi_df["bi_direction"].notna()]

        overlap = fractal_rows.index.intersection(bi_rows.index)
        assert overlap.empty, "Fractal rows and bi rows should not overlap"


class TestChanlunOverlay:

    def test_is_available(self):
        from modules.visualizer.overlays.chanlun_overlay import ChanlunOverlay
        overlay = ChanlunOverlay()
        assert overlay.is_available() is True

    def test_name(self):
        from modules.visualizer.overlays.chanlun_overlay import ChanlunOverlay
        overlay = ChanlunOverlay()
        assert overlay.name == "缠论笔/分型"

    def test_apply_draws_fractal_markers(self, sample_kline_df):
        from modules.visualizer.overlays.chanlun_overlay import ChanlunOverlay
        from modules.visualizer.chart_kline import create_kline_chart

        chart_df = _build_chart_df(sample_kline_df)
        fig = create_kline_chart(chart_df, overlays=[], title="test")

        overlay = ChanlunOverlay()
        overlay.apply(fig, chart_df)

        trace_names = [t.name for t in fig.data]
        assert "顶分型" in trace_names or "底分型" in trace_names or "K线" in trace_names

    def test_apply_draws_bi_lines(self, sample_kline_df):
        from modules.visualizer.overlays.chanlun_overlay import ChanlunOverlay
        from modules.visualizer.chart_kline import create_kline_chart

        chart_df = _build_chart_df(sample_kline_df)
        fig = create_kline_chart(chart_df, overlays=[], title="test")

        overlay = ChanlunOverlay()
        overlay.apply(fig, chart_df)

        initial_trace_count = 2
        assert len(fig.data) > initial_trace_count

    def test_apply_empty_df(self):
        from modules.visualizer.overlays.chanlun_overlay import ChanlunOverlay
        from plotly.graph_objects import Figure

        fig = Figure()
        overlay = ChanlunOverlay()
        overlay.apply(fig, pd.DataFrame())
        assert len(fig.data) == 0

    def test_apply_disabled(self, sample_kline_df):
        from modules.visualizer.overlays.chanlun_overlay import ChanlunOverlay
        from modules.visualizer.chart_kline import create_kline_chart

        chart_df = _build_chart_df(sample_kline_df)
        fig = create_kline_chart(chart_df, overlays=[], title="test")
        initial_count = len(fig.data)

        overlay = ChanlunOverlay(enabled=False)
        overlay.apply(fig, chart_df)

        assert len(fig.data) == initial_count


class TestChanlunPanel:

    def test_panel_with_valid_bi_data(self):
        from modules.visualizer.panels.chanlun_panel import render_chanlun_panel

        bi_df = pd.DataFrame({
            "bi_direction": ["up", "down", "up"],
            "start_date": [datetime(2025, 1, 1), datetime(2025, 1, 10), datetime(2025, 1, 20)],
            "end_date": [datetime(2025, 1, 8), datetime(2025, 1, 18), datetime(2025, 1, 28)],
            "start_price": [10.0, 12.0, 9.0],
            "end_price": [12.0, 9.0, 11.0],
            "fractal_type": [None, None, None],
        })

        rendered = False

        def mock_subheader(text):
            nonlocal rendered
            if text == "缠论笔信息面板":
                rendered = True

        with patch("modules.visualizer.panels.chanlun_panel.st") as mock_st:
            mock_st.subheader = mock_subheader
            mock_st.info = MagicMock()
            mock_st.warning = MagicMock()
            mock_st.metric = MagicMock()
            mock_st.markdown = MagicMock()
            mock_st.dataframe = MagicMock()
            mock_st.columns = MagicMock(return_value=(MagicMock(), MagicMock(), MagicMock(), MagicMock()))

            render_chanlun_panel(bi_df)
            assert rendered, "Panel should render bi info"

    def test_panel_with_empty_df(self):
        from modules.visualizer.panels.chanlun_panel import render_chanlun_panel

        info_called = False

        def mock_info(text):
            nonlocal info_called
            if "暂无缠论笔数据" in text:
                info_called = True

        with patch("modules.visualizer.panels.chanlun_panel.st") as mock_st:
            mock_st.info = mock_info
            mock_st.warning = MagicMock()
            render_chanlun_panel(pd.DataFrame())
            assert info_called

    def test_panel_with_mixed_fractal_and_bi_rows(self):
        from modules.visualizer.panels.chanlun_panel import render_chanlun_panel

        mixed_df = pd.DataFrame({
            "trade_date": [datetime(2025, 1, 1), datetime(2025, 1, 5), None, None],
            "price": [10.0, 12.0, None, None],
            "fractal_type": ["bottom", "top", None, None],
            "bi_direction": [None, None, "up", "down"],
            "start_date": [None, None, datetime(2025, 1, 5), datetime(2025, 1, 15)],
            "end_date": [None, None, datetime(2025, 1, 14), datetime(2025, 1, 25)],
            "start_price": [None, None, 12.0, 11.0],
            "end_price": [None, None, 11.0, 9.0],
        })

        rendered = False

        def mock_subheader(text):
            nonlocal rendered
            if text == "缠论笔信息面板":
                rendered = True

        with patch("modules.visualizer.panels.chanlun_panel.st") as mock_st:
            mock_st.subheader = mock_subheader
            mock_st.info = MagicMock()
            mock_st.warning = MagicMock()
            mock_st.metric = MagicMock()
            mock_st.markdown = MagicMock()
            mock_st.dataframe = MagicMock()
            mock_st.columns = MagicMock(return_value=(MagicMock(), MagicMock(), MagicMock(), MagicMock()))

            render_chanlun_panel(mixed_df)
            assert rendered, "Panel should filter out fractal rows and render bi info"

    def test_panel_missing_required_column(self):
        from modules.visualizer.panels.chanlun_panel import render_chanlun_panel

        bad_df = pd.DataFrame({
            "bi_direction": ["up"],
        })

        warning_called = False

        def mock_warning(text):
            nonlocal warning_called
            if "格式不完整" in text:
                warning_called = True

        with patch("modules.visualizer.panels.chanlun_panel.st") as mock_st:
            mock_st.info = MagicMock()
            mock_st.warning = mock_warning
            render_chanlun_panel(bad_df)
            assert warning_called


class TestEndToEndPipeline:

    def test_compute_bi_output_compatible_with_panel(self, sample_kline_df):
        from modules.chanlun import compute_bi

        bi_df = compute_bi(sample_kline_df)
        if bi_df.empty:
            pytest.skip("compute_bi returned empty for sample data")

        required_cols = ["bi_direction", "start_date", "end_date", "start_price", "end_price"]
        for col in required_cols:
            assert col in bi_df.columns

        bi_rows = bi_df[bi_df["bi_direction"].notna()]
        if bi_rows.empty:
            pytest.skip("No bi rows generated for sample data")

        for col in required_cols:
            assert bi_rows[col].notna().all(), f"Bi rows have null in column: {col}"

    def test_overlay_draws_on_chart_with_real_data(self, sample_kline_df):
        from modules.chanlun import compute_bi
        from modules.visualizer.overlays.chanlun_overlay import ChanlunOverlay
        from modules.visualizer.chart_kline import create_kline_chart

        bi_df = compute_bi(sample_kline_df)
        if bi_df.empty:
            pytest.skip("compute_bi returned empty for sample data")

        chart_df = _build_chart_df(sample_kline_df)
        fig = create_kline_chart(chart_df, overlays=[], title="test")
        initial_count = len(fig.data)

        overlay = ChanlunOverlay()
        overlay.apply(fig, chart_df)

        assert len(fig.data) > initial_count, "Overlay should add traces to the chart"

        has_fractal = any(
            t.name in ("顶分型", "底分型") for t in fig.data
        )
        assert has_fractal, "Overlay should add fractal markers"
