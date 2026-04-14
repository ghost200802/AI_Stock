import logging

import pandas as pd
import plotly.graph_objects as go

from .base import ChartOverlay

logger = logging.getLogger(__name__)


class ChanlunOverlay(ChartOverlay):

    def __init__(self, enabled=True):
        super().__init__(enabled)
        self._available = None

    @property
    def name(self):
        return "缠论笔/分型"

    def is_available(self):
        if self._available is None:
            try:
                from modules.chanlun import compute_bi
                self._available = compute_bi is not None
            except (ImportError, AttributeError):
                self._available = False
        return self._available

    def apply(self, fig, df):
        if not self.enabled or not self.is_available():
            return
        if df is None or df.empty:
            return

        try:
            from modules.chanlun import compute_bi
            bi_df = compute_bi(df)
            if bi_df is None or bi_df.empty:
                logger.info("缠论笔计算结果为空")
                return

            self._draw_fractal_points(fig, bi_df, df)
            self._draw_bi_lines(fig, bi_df, df)
        except Exception as e:
            logger.error("缠论叠加层绘制失败: %s", e)

    def _normalize_date(self, val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        if isinstance(val, str):
            return val[:10]
        if hasattr(val, "strftime"):
            return val.strftime("%Y-%m-%d")
        return str(val)

    def _draw_fractal_points(self, fig, bi_df, kline_df):
        if "fractal_type" not in bi_df.columns:
            return

        top_points = bi_df[bi_df["fractal_type"] == "top"]
        bottom_points = bi_df[bi_df["fractal_type"] == "bottom"]

        if not top_points.empty:
            fig.add_trace(
                go.Scatter(
                    x=[self._normalize_date(d) for d in top_points.get("trade_date", [])],
                    y=top_points.get("price", []),
                    mode="markers",
                    marker=dict(symbol="triangle-down", size=10, color="red"),
                    name="顶分型",
                ),
                row=1,
                col=1,
            )

        if not bottom_points.empty:
            fig.add_trace(
                go.Scatter(
                    x=[self._normalize_date(d) for d in bottom_points.get("trade_date", [])],
                    y=bottom_points.get("price", []),
                    mode="markers",
                    marker=dict(symbol="triangle-up", size=10, color="green"),
                    name="底分型",
                ),
                row=1,
                col=1,
            )

    def _draw_bi_lines(self, fig, bi_df, kline_df):
        if "bi_direction" not in bi_df.columns:
            return

        up_bi = bi_df[bi_df["bi_direction"] == "up"]
        down_bi = bi_df[bi_df["bi_direction"] == "down"]

        for direction, color, name in [
            (up_bi, "red", "向上笔"),
            (down_bi, "green", "向下笔"),
        ]:
            if direction.empty:
                continue
            if "start_date" in direction.columns and "end_date" in direction.columns:
                for _, row in direction.iterrows():
                    fig.add_trace(
                        go.Scatter(
                            x=[self._normalize_date(row["start_date"]), self._normalize_date(row["end_date"])],
                            y=[row["start_price"], row["end_price"]],
                            mode="lines",
                            line=dict(width=2, color=color),
                            showlegend=False,
                            hoverinfo="skip",
                        ),
                        row=1,
                        col=1,
                    )
