import logging

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

DIRECTION_LABELS = {
    "up": "向上",
    "down": "向下",
}

DIRECTION_EMOJI = {
    "up": "🔴",
    "down": "🟢",
}


def render_chanlun_panel(bis_df):
    if bis_df is None or bis_df.empty:
        st.info("暂无缠论笔数据。")
        return

    required_cols = ["bi_direction", "start_date", "end_date", "start_price", "end_price"]
    for col in required_cols:
        if col not in bis_df.columns:
            st.warning(f"缠论笔数据格式不完整（缺少 {col} 列）")
            return

    bi_rows = bis_df[bis_df["bi_direction"].notna()].copy()
    if bi_rows.empty:
        st.info("当前周期未生成缠论笔。")
        return

    st.subheader("缠论笔信息面板")

    _render_statistics(bi_rows)

    st.markdown("---")

    _render_bi_table(bi_rows)


def _render_statistics(bi_df):
    total = len(bi_df)
    up_count = len(bi_df[bi_df["bi_direction"] == "up"])
    down_count = len(bi_df[bi_df["bi_direction"] == "down"])

    bi_df = bi_df.copy()
    bi_df["change_pct"] = _calc_change_pct(bi_df)
    avg_change = bi_df["change_pct"].mean()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("总笔数", total)
    with col2:
        st.metric("向上笔", up_count)
    with col3:
        st.metric("向下笔", down_count)
    with col4:
        st.metric("平均变动", f"{avg_change:.2f}%")


def _calc_change_pct(bi_df):
    def _pct(row):
        start = row.get("start_price", 0)
        end = row.get("end_price", 0)
        if start and start != 0:
            return (end - start) / abs(start) * 100
        return 0.0

    return bi_df.apply(_pct, axis=1)


def _render_bi_table(bi_df):
    display = pd.DataFrame()

    display["序号"] = range(1, len(bi_df) + 1)
    display["方向"] = bi_df["bi_direction"].map(
        lambda x: f"{DIRECTION_EMOJI.get(x, '')} {DIRECTION_LABELS.get(x, str(x))}"
    )

    display["开始日期"] = bi_df["start_date"].apply(_fmt_date)
    display["结束日期"] = bi_df["end_date"].apply(_fmt_date)
    display["起始价"] = bi_df["start_price"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "-")
    display["结束价"] = bi_df["end_price"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "-")

    bi_df_copy = bi_df.copy()
    display["变动%"] = _calc_change_pct(bi_df_copy).apply(lambda x: f"{x:+.2f}%")

    if "confirmed" in bi_df.columns:
        display["状态"] = bi_df["confirmed"].map(
            lambda x: "未确认" if x is False or x is pd.NA or x == 0 else "已确认"
        )
    else:
        last_idx = len(display) - 1
        display["状态"] = ["已确认"] * last_idx + ["未确认"] if last_idx >= 0 else []

    st.dataframe(display, use_container_width=True, hide_index=True)


def _fmt_date(val):
    if pd.isna(val) or val is None:
        return "-"
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    return str(val)
