import logging

import pandas as pd
import plotly.graph_objects as go

from .base import ChartOverlay

logger = logging.getLogger(__name__)

MA_COLORS = {
    5: "#FF6B6B",
    20: "#FFA500",
    60: "#4ECDC4",
    120: "#9B59B6",
    250: "#3498DB",
}


class MAOverlay(ChartOverlay):

    def __init__(self, periods=None, enabled=True):
        super().__init__(enabled)
        self.periods = periods or [5, 20, 60, 120, 250]

    @property
    def name(self):
        return "MA均线"

    def apply(self, fig, df):
        if not self.enabled or df is None or df.empty:
            return

        if "close" not in df.columns:
            logger.warning("数据缺少 close 列，跳过均线绘制")
            return

        close = pd.to_numeric(df["close"], errors="coerce")
        for period in self.periods:
            if len(close) < period:
                logger.debug("数据量不足 %d 条，跳过 MA%d", period, period)
                continue
            ma = close.rolling(window=period, min_periods=period).mean()
            color = MA_COLORS.get(period, "#999999")
            fig.add_trace(
                go.Scatter(
                    x=df["trade_date"],
                    y=ma,
                    mode="lines",
                    line=dict(width=1, color=color),
                    name=f"MA{period}",
                    hovertemplate=f"MA{period}: %{{y:.2f}}<extra></extra>",
                ),
                row=1,
                col=1,
            )
