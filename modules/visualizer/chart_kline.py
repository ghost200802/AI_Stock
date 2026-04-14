import logging

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

logger = logging.getLogger(__name__)


PERIOD_LABELS = {
    "daily": "日K",
    "weekly": "周K",
    "monthly": "月K",
}


def create_kline_chart(df, overlays=None, title="", period="daily"):
    if df is None or df.empty:
        fig = go.Figure()
        fig.update_layout(title="暂无数据", height=600)
        return fig

    required_cols = ["trade_date", "open", "high", "low", "close", "volume"]
    for col in required_cols:
        if col not in df.columns:
            logger.error("K线数据缺少必要列: %s", col)
            fig = go.Figure()
            fig.update_layout(title=f"数据不完整（缺少 {col}）", height=600)
            return fig

    chart_df = df.sort_values("trade_date").reset_index(drop=True)

    chart_df = chart_df.copy()
    chart_df["trade_date"] = chart_df["trade_date"].astype(str).str[:10]

    subplot_heights = [0.75, 0.25]
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=subplot_heights,
    )

    colors = ["red" if c >= o else "green" for o, c in zip(chart_df["open"], chart_df["close"])]

    fig.add_trace(
        go.Candlestick(
            x=chart_df["trade_date"],
            open=chart_df["open"],
            high=chart_df["high"],
            low=chart_df["low"],
            close=chart_df["close"],
            increasing_line_color="red",
            decreasing_line_color="green",
            increasing_fillcolor="red",
            decreasing_fillcolor="green",
            name="K线",
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            x=chart_df["trade_date"],
            y=chart_df["volume"],
            marker_color=colors,
            opacity=0.7,
            name="成交量",
            showlegend=False,
        ),
        row=2,
        col=1,
    )

    if overlays:
        for overlay in overlays:
            try:
                overlay.apply(fig, chart_df)
            except Exception as e:
                logger.error("叠加层 %s 应用失败: %s", type(overlay).__name__, e)

    period_label = PERIOD_LABELS.get(period, period)
    chart_title = title if title else f"{period_label}线图"

    fig.update_layout(
        title=chart_title,
        xaxis_rangeslider_visible=False,
        height=700,
        margin=dict(l=50, r=50, t=60, b=30),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        dragmode="zoom",
    )

    fig.update_xaxes(
        row=1,
        col=1,
        rangeslider_visible=False,
        gridcolor="#f0f0f0",
    )
    fig.update_xaxes(
        row=2,
        col=1,
        gridcolor="#f0f0f0",
    )
    fig.update_yaxes(
        row=1,
        col=1,
        gridcolor="#f0f0f0",
        side="right",
    )
    fig.update_yaxes(
        row=2,
        col=1,
        gridcolor="#f0f0f0",
        side="right",
    )

    return fig
