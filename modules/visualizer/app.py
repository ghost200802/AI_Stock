import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from lib.utils import load_config
from modules.visualizer.data_cache import ChartDataCache
from modules.visualizer.chart_kline import create_kline_chart, PERIOD_LABELS
from modules.visualizer.overlays import MAOverlay, ChanlunOverlay, CaisenOverlay
from modules.visualizer.panels.pattern_panel import render_pattern_panel
from modules.visualizer.panels.chanlun_panel import render_chanlun_panel

logger = logging.getLogger(__name__)

PERIOD_OPTIONS = {
    "daily": "日K",
    "weekly": "周K",
    "monthly": "月K",
}


def get_data_cache():
    if "data_cache" not in st.session_state:
        st.session_state.data_cache = ChartDataCache()
    return st.session_state.data_cache


def render_stock_selector(cache):
    st.sidebar.subheader("股票选择")

    stock_list = cache.get_stock_list()

    if stock_list is None or stock_list.empty:
        symbol = st.sidebar.text_input("股票代码", value="000001")
        return symbol.strip() if symbol else None

    search_term = st.sidebar.text_input("搜索代码/名称", placeholder="输入代码或名称...")
    if search_term:
        mask = (
            stock_list["code"].astype(str).str.contains(search_term, na=False)
            | stock_list["name"].astype(str).str.contains(search_term, na=False)
        )
        filtered = stock_list[mask].head(20)
    else:
        config = load_config()
        pool = config.get("stock_pool", ["000001"])
        mask = stock_list["code"].isin(pool)
        filtered = stock_list[mask] if mask.any() else stock_list.head(10)

    if filtered.empty:
        symbol = st.sidebar.text_input("股票代码", value="000001")
        return symbol.strip() if symbol else None

    options = [f"{row['code']} {row['name']}" for _, row in filtered.iterrows()]
    selected = st.sidebar.selectbox("选择股票", options=options)

    if selected:
        return selected.split()[0]
    return None


def render_period_selector(config):
    vis_config = config.get("visualizer", {})
    default_period = vis_config.get("default_period", "daily")
    available_periods = vis_config.get("available_periods", ["daily", "weekly", "monthly"])

    period_labels = {k: PERIOD_OPTIONS.get(k, k) for k in available_periods}

    selected_label = st.sidebar.selectbox(
        "周期",
        options=list(period_labels.values()),
        index=list(period_labels.values()).index(PERIOD_OPTIONS.get(default_period, "日K")),
    )

    for key, label in period_labels.items():
        if label == selected_label:
            return key
    return default_period


def build_overlays(config, patterns_df=None, highlight_idx=None, score_threshold=70, show_all=False):
    vis_config = config.get("visualizer", {})
    overlays_config = vis_config.get("overlays", {})
    overlays = []

    ma_periods = overlays_config.get("ma_lines", [5, 20, 60, 120, 250])
    if ma_periods:
        overlays.append(MAOverlay(periods=ma_periods))

    if overlays_config.get("chanlun", True):
        chanlun = ChanlunOverlay()
        if chanlun.is_available():
            overlays.append(chanlun)

    if overlays_config.get("caisen", True):
        caisen_df = patterns_df if (patterns_df is not None and not patterns_df.empty) else None
        caisen = CaisenOverlay(
            patterns_df=caisen_df,
            highlight_idx=highlight_idx,
            score_threshold=score_threshold,
            show_all=show_all,
        )
        if caisen.is_available():
            overlays.append(caisen)

    return overlays


def main():
    st.set_page_config(
        page_title="AI Stock 可视化分析",
        page_icon="📈",
        layout="wide",
    )

    st.title("📈 AI Stock 可视化分析系统")

    config = load_config()
    cache = get_data_cache()

    symbol = render_stock_selector(cache)
    period = render_period_selector(config)

    if not symbol:
        st.warning("请选择一只股票")
        return

    with st.spinner(f"正在加载 {symbol} 数据..."):
        try:
            kline_df = cache.get_kline_data(symbol, period=period)
        except Exception as e:
            st.error(f"获取数据失败: {e}")
            logger.exception("获取K线数据失败")
            return

    if kline_df.empty:
        st.warning(f"未找到 {symbol} 的K线数据，请检查股票代码或先获取数据")
        return

    with st.spinner("正在获取形态分析数据..."):
        try:
            chanlun_df = cache.get_analysis_data(symbol, period=period, analysis_type="chanlun")
            caisen_df = cache.get_analysis_data(symbol, period=period, analysis_type="caisen")

            all_patterns = []
            if not caisen_df.empty:
                all_patterns.append(caisen_df)
            if all_patterns:
                patterns_df = pd.concat(all_patterns, ignore_index=True)
            else:
                patterns_df = pd.DataFrame()
        except Exception as e:
            st.error(f"获取形态数据失败: {e}")
            logger.exception("获取形态数据失败")
            patterns_df = pd.DataFrame()
            chanlun_df = pd.DataFrame()

    highlight_idx = st.session_state.get("pattern_selected_idx", None)

    ctrl_col1, ctrl_col2 = st.columns([3, 1])
    with ctrl_col1:
        score_threshold = st.slider(
            "评分阈值", min_value=0, max_value=100, value=70, key="pattern_score_threshold"
        )
    with ctrl_col2:
        show_all = st.toggle("显示全部形态", value=False, key="pattern_show_all")

    overlays = build_overlays(
        config,
        patterns_df=patterns_df,
        highlight_idx=highlight_idx,
        score_threshold=score_threshold,
        show_all=show_all,
    )

    period_label = PERIOD_LABELS.get(period, period)
    fig = create_kline_chart(
        kline_df,
        overlays=overlays,
        title=f"{symbol} {period_label}线图",
        period=period,
    )

    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    render_pattern_panel(patterns_df, score_threshold=score_threshold, show_all=show_all)

    st.markdown("---")

    render_chanlun_panel(chanlun_df)


if __name__ == "__main__":
    main()
