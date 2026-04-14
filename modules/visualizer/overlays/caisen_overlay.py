import logging

import plotly.graph_objects as go

from .base import ChartOverlay

logger = logging.getLogger(__name__)


class CaisenOverlay(ChartOverlay):

    def __init__(self, enabled=True):
        super().__init__(enabled)
        self._available = None

    @property
    def name(self):
        return "蔡森形态"

    def is_available(self):
        if self._available is None:
            try:
                from modules.caisen import compute_patterns
                self._available = compute_patterns is not None
            except (ImportError, AttributeError):
                self._available = False
        return self._available

    def apply(self, fig, df):
        if not self.enabled or not self.is_available():
            return
        if df is None or df.empty:
            return

        try:
            from modules.caisen import compute_patterns
            patterns_df = compute_patterns(df)
            if patterns_df is None or patterns_df.empty:
                logger.info("蔡森形态计算结果为空")
                return

            self._draw_pattern_annotations(fig, patterns_df)
        except Exception as e:
            logger.error("蔡森叠加层绘制失败: %s", e)

    def _draw_pattern_annotations(self, fig, patterns_df):
        if "pattern_type" not in patterns_df.columns:
            return

        bull_colors = {"neckline": "#2ECC71", "target": "#E74C3C", "stop_loss": "#F39C12"}
        bear_colors = {"neckline": "#E74C3C", "target": "#2ECC71", "stop_loss": "#F39C12"}

        for _, row in patterns_df.iterrows():
            direction = row.get("direction", "bull")
            colors = bull_colors if direction == "bull" else bear_colors

            if "neckline_start_date" in row.index and "neckline_end_date" in row.index:
                fig.add_trace(
                    go.Scatter(
                        x=[row.get("neckline_start_date"), row.get("neckline_end_date")],
                        y=[row.get("neckline_price"), row.get("neckline_price")],
                        mode="lines",
                        line=dict(width=1.5, dash="dash", color=colors["neckline"]),
                        showlegend=False,
                        hoverinfo="skip",
                    ),
                    row=1,
                    col=1,
                )

            if "target_price" in row.index and "target_date" in row.index:
                fig.add_trace(
                    go.Scatter(
                        x=[row.get("target_date")],
                        y=[row.get("target_price")],
                        mode="markers+text",
                        marker=dict(symbol="star", size=12, color=colors["target"]),
                        text=[f"T:{row.get('target_price', ''):.2f}"],
                        textposition="top center",
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )

            if "stop_loss_price" in row.index and "stop_loss_date" in row.index:
                fig.add_trace(
                    go.Scatter(
                        x=[row.get("stop_loss_date")],
                        y=[row.get("stop_loss_price")],
                        mode="markers+text",
                        marker=dict(symbol="x", size=12, color=colors["stop_loss"]),
                        text=[f"S:{row.get('stop_loss_price', ''):.2f}"],
                        textposition="bottom center",
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )

            if "key_point_date" in row.index and "key_point_price" in row.index:
                pattern_type = row.get("pattern_type", "")
                fig.add_annotation(
                    x=row.get("key_point_date"),
                    y=row.get("key_point_price"),
                    text=pattern_type,
                    showarrow=True,
                    arrowhead=2,
                    arrowsize=1,
                    arrowcolor="#666",
                    row=1,
                    col=1,
                )
