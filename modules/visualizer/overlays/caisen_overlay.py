import logging

import pandas as pd
import plotly.graph_objects as go

from .base import ChartOverlay

logger = logging.getLogger(__name__)


class CaisenOverlay(ChartOverlay):

    def __init__(self, enabled=True, patterns_df=None, highlight_idx=None,
                 score_threshold=70, show_all=False):
        super().__init__(enabled)
        self._available = None
        self._patterns_df = patterns_df
        self._highlight_idx = highlight_idx
        self._score_threshold = score_threshold
        self._show_all = show_all

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
            # 优先使用外部传入的形态数据
            patterns_df = self._patterns_df
            if patterns_df is None:
                from modules.caisen import compute_patterns
                patterns_df = compute_patterns(df)
            if patterns_df is None or patterns_df.empty:
                logger.info("蔡森形态计算结果为空")
                return

            x_end = str(df["trade_date"].iloc[-1])[:10]
            self._draw_pattern_annotations(fig, patterns_df, x_end)
        except Exception as e:
            logger.error("蔡森叠加层绘制失败: %s", e)

    def _draw_pattern_annotations(self, fig, patterns_df, x_end=None):
        if "pattern_type" not in patterns_df.columns:
            return

        bull_colors = {
            "neckline": "#2ECC71",
            "target": "#E74C3C",
            "stop_loss": "#F39C12",
            "fill": "rgba(46, 204, 113, 0.1)",
            "marker": "#2ECC71",
        }
        bear_colors = {
            "neckline": "#E74C3C",
            "target": "#2ECC71",
            "stop_loss": "#F39C12",
            "fill": "rgba(231, 76, 60, 0.1)",
            "marker": "#E74C3C",
        }

        for idx, (i, row) in enumerate(patterns_df.iterrows()):
            direction = row.get("direction", "bull")
            colors = bull_colors if direction == "bull" else bear_colors

            strength_score = row.get("strength_score", None)
            is_below_threshold = False
            if not self._show_all:
                if pd.isna(strength_score):
                    is_below_threshold = True
                elif float(strength_score) < self._score_threshold:
                    is_below_threshold = True

            is_highlighted = (self._highlight_idx is None) or (self._highlight_idx == idx)
            if is_below_threshold and not is_highlighted:
                opacity = 0.1
            elif is_below_threshold and is_highlighted:
                opacity = 0.5
            elif not is_highlighted:
                opacity = 0.2
            else:
                opacity = 1.0

            self._draw_neckline(fig, row, colors, opacity)
            self._draw_start_end_markers(fig, row, colors, opacity)
            self._draw_target_price(fig, row, colors, opacity, x_end)
            self._draw_stop_loss_price(fig, row, colors, opacity, x_end)
            self._draw_process_area(fig, row, colors, opacity)
            self._draw_key_points(fig, row, colors, opacity)
            self._draw_pattern_label(fig, row, opacity)

    # ---------- 颈线绘制 ----------

    def _draw_neckline(self, fig, row, colors, opacity):
        neckline_start = row.get("neckline_start_date")
        neckline_end = row.get("neckline_end_date")
        neckline_price = row.get("neckline_price")

        if neckline_start is None or neckline_end is None or neckline_price is None:
            return

        fig.add_trace(
            go.Scatter(
                x=[neckline_start, neckline_end],
                y=[neckline_price, neckline_price],
                mode="lines",
                line=dict(width=2.5, dash="dash", color=colors["neckline"]),
                opacity=opacity,
                showlegend=False,
                hoverinfo="skip",
            ),
            row=1,
            col=1,
        )

        # 在颈线中间位置添加"颈线"文字标注
        fig.add_annotation(
            x=neckline_start,
            y=neckline_price,
            text="颈线",
            xanchor="left",
            yanchor="bottom",
            showarrow=False,
            opacity=opacity,
            font=dict(size=10, color=colors["neckline"]),
            row=1,
            col=1,
        )

    # ---------- 起点终点标识 ----------

    def _draw_start_end_markers(self, fig, row, colors, opacity):
        start_date = row.get("start_date")
        end_date = row.get("end_date")
        start_price = row.get("start_price") if "start_price" in row.index else None
        end_price = row.get("end_price") if "end_price" in row.index else None

        # 如果没有独立的 start_price / end_price，尝试从 key_points 推断
        key_points = row.get("key_points")
        if start_date is not None and start_price is None and key_points:
            for kp in key_points:
                if str(kp.get("date", "")) == str(start_date):
                    start_price = kp.get("price")
                    break
        if end_date is not None and end_price is None and key_points:
            for kp in key_points:
                if str(kp.get("date", "")) == str(end_date):
                    end_price = kp.get("price")
                    break

        if start_date is not None and start_price is not None:
            fig.add_trace(
                go.Scatter(
                    x=[start_date],
                    y=[start_price],
                    mode="markers+text",
                    marker=dict(symbol="circle", size=15, color=colors["marker"]),
                    text=["起"],
                    textposition="top center",
                    textfont=dict(size=11, color=colors["marker"]),
                    opacity=opacity,
                    showlegend=False,
                ),
                row=1,
                col=1,
            )

        if end_date is not None and end_price is not None:
            fig.add_trace(
                go.Scatter(
                    x=[end_date],
                    y=[end_price],
                    mode="markers+text",
                    marker=dict(symbol="circle", size=15, color=colors["marker"]),
                    text=["终"],
                    textposition="top center",
                    textfont=dict(size=11, color=colors["marker"]),
                    opacity=opacity,
                    showlegend=False,
                ),
                row=1,
                col=1,
            )

    # ---------- 目标价绘制（含水平延伸虚线） ----------

    def _draw_target_price(self, fig, row, colors, opacity, x_end=None):
        target_price = row.get("target_price")
        target_date = row.get("target_date")

        if target_price is None or target_date is None:
            return

        target_date_str = str(target_date)[:10]

        fig.add_trace(
            go.Scatter(
                x=[target_date_str],
                y=[target_price],
                mode="markers+text",
                marker=dict(symbol="star", size=12, color=colors["target"]),
                text=[f"T:{target_price:.2f}"],
                textposition="top center",
                opacity=opacity,
                showlegend=False,
            ),
            row=1,
            col=1,
        )

        line_end = x_end if x_end and x_end != target_date_str else None
        if line_end:
            fig.add_trace(
                go.Scatter(
                    x=[target_date_str, line_end],
                    y=[target_price, target_price],
                    mode="lines",
                    line=dict(width=1, dash="dot", color=colors["target"]),
                    opacity=opacity,
                    showlegend=False,
                    hoverinfo="skip",
                ),
                row=1,
                col=1,
            )
            fig.add_annotation(
                x=line_end,
                y=target_price,
                text="目标价",
                xanchor="left",
                yanchor="middle",
                showarrow=False,
                opacity=opacity,
                font=dict(size=10, color=colors["target"]),
                row=1,
                col=1,
            )

    # ---------- 止损价绘制（含水平延伸虚线） ----------

    def _draw_stop_loss_price(self, fig, row, colors, opacity, x_end=None):
        stop_loss_price = row.get("stop_loss_price")
        stop_loss_date = row.get("stop_loss_date")

        if stop_loss_price is None or stop_loss_date is None:
            return

        stop_loss_date_str = str(stop_loss_date)[:10]

        fig.add_trace(
            go.Scatter(
                x=[stop_loss_date_str],
                y=[stop_loss_price],
                mode="markers+text",
                marker=dict(symbol="x", size=12, color=colors["stop_loss"]),
                text=[f"S:{stop_loss_price:.2f}"],
                textposition="bottom center",
                opacity=opacity,
                showlegend=False,
            ),
            row=1,
            col=1,
        )

        line_end = x_end if x_end and x_end != stop_loss_date_str else None
        if line_end:
            fig.add_trace(
                go.Scatter(
                    x=[stop_loss_date_str, line_end],
                    y=[stop_loss_price, stop_loss_price],
                    mode="lines",
                    line=dict(width=1, dash="dot", color=colors["stop_loss"]),
                    opacity=opacity,
                    showlegend=False,
                    hoverinfo="skip",
                ),
                row=1,
                col=1,
            )
            fig.add_annotation(
                x=line_end,
                y=stop_loss_price,
                text="止损价",
                xanchor="left",
                yanchor="middle",
                showarrow=False,
                opacity=opacity,
                font=dict(size=10, color=colors["stop_loss"]),
                row=1,
                col=1,
            )

    # ---------- 过程走势区域 ----------

    def _draw_process_area(self, fig, row, colors, opacity):
        start_date = row.get("start_date")
        end_date = row.get("end_date")
        key_points = row.get("key_points")

        if not key_points or start_date is None or end_date is None:
            return

        # 收集关键点中的价格
        prices = []
        for kp in key_points:
            p = kp.get("price")
            if p is not None:
                prices.append(p)
        if len(prices) < 2:
            return

        high_price = max(prices)
        low_price = min(prices)
        price_range = high_price - low_price
        # 缓冲为价格区间的 10%，最小 0.01
        buffer = max(price_range * 0.1, 0.01)

        upper = high_price + buffer
        lower = low_price - buffer

        # 半透明填充区域：上边界 -> 下边界 -> 下边界 -> 上边界 闭合
        fig.add_trace(
            go.Scatter(
                x=[start_date, end_date, end_date, start_date],
                y=[upper, upper, lower, lower],
                mode="lines",
                fill="toself",
                fillcolor=colors["fill"],
                line=dict(width=0, color="rgba(0,0,0,0)"),
                opacity=opacity,
                showlegend=False,
                hoverinfo="skip",
            ),
            row=1,
            col=1,
        )

    # ---------- 所有关键点标注 ----------

    def _draw_key_points(self, fig, row, colors, opacity):
        key_points = row.get("key_points")

        if not key_points:
            return

        xs = []
        ys = []
        texts = []

        for kp in key_points:
            kp_date = kp.get("date")
            kp_price = kp.get("price")
            kp_name = kp.get("name", "")
            if kp_date is None or kp_price is None:
                continue
            xs.append(str(kp_date))
            ys.append(kp_price)
            texts.append(f"{kp_name}: {kp_price:.2f}")

        if not xs:
            return

        fig.add_trace(
            go.Scatter(
                x=xs,
                y=ys,
                mode="markers",
                marker=dict(symbol="circle", size=8, color=colors["marker"]),
                text=texts,
                opacity=opacity,
                showlegend=False,
                hoverinfo="text",
            ),
            row=1,
            col=1,
        )

    # ---------- 形态类型标注 ----------

    def _draw_pattern_label(self, fig, row, opacity):
        key_point_date = row.get("key_point_date")
        key_point_price = row.get("key_point_price")

        if key_point_date is None or key_point_price is None:
            return

        pattern_type = row.get("pattern_type", "")
        if not pattern_type:
            return

        fig.add_annotation(
            x=key_point_date,
            y=key_point_price,
            text=pattern_type,
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            arrowcolor="#666",
            opacity=opacity,
            row=1,
            col=1,
        )
