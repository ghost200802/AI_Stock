import logging

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

STATUS_LABELS = {
    "forming": "形成中",
    "confirmed": "已确认",
    "invalidated": "已失效",
}

DIRECTION_LABELS = {
    "bull": "看涨",
    "bear": "看跌",
}

DIRECTION_EMOJI = {
    "bull": "🔴",
    "bear": "🟢",
}


def render_pattern_panel(patterns_df):
    if patterns_df is None or patterns_df.empty:
        st.info("暂无形态识别结果。缠论/蔡森模块实现后将自动展示形态信息。")
        return

    required_cols = ["pattern_type"]
    for col in required_cols:
        if col not in patterns_df.columns:
            st.warning(f"形态数据格式不完整（缺少 {col} 列）")
            return

    st.subheader("形态判断评价面板")

    display_df = _prepare_display_data(patterns_df)

    if display_df.empty:
        st.info("当前周期未识别到有效形态。")
        return

    for idx, row in display_df.iterrows():
        _render_pattern_card(idx, row)


def _prepare_display_data(df):
    display = pd.DataFrame()

    if "pattern_type" in df.columns:
        display["形态类型"] = df["pattern_type"]

    if "direction" in df.columns:
        display["方向"] = df["direction"].map(
            lambda x: DIRECTION_LABELS.get(x, str(x))
        )

    if "status" in df.columns:
        display["状态"] = df["status"].map(
            lambda x: STATUS_LABELS.get(x, str(x))
        )

    if "strength_score" in df.columns:
        display["强弱评分"] = pd.to_numeric(df["strength_score"], errors="coerce")

    if "volume_score" in df.columns:
        display["量价评分"] = pd.to_numeric(df["volume_score"], errors="coerce")

    if "target_price" in df.columns:
        display["目标价"] = pd.to_numeric(df["target_price"], errors="coerce")

    if "stop_loss_price" in df.columns:
        display["止损价"] = pd.to_numeric(df["stop_loss_price"], errors="coerce")

    display["_source"] = df.reset_index(drop=True).index

    return display


def _render_pattern_card(idx, row):
    source_idx = row.get("_source", idx)
    pattern_type = row.get("形态类型", "未知")
    direction = row.get("方向", "")
    status = row.get("状态", "")
    strength_score = row.get("强弱评分", None)
    volume_score = row.get("量价评分", None)
    target_price = row.get("目标价", None)
    stop_loss_price = row.get("止损价", None)

    emoji = ""
    if "方向" in row.index:
        raw_dir = direction
        emoji = DIRECTION_EMOJI.get(raw_dir, "")

    header = f"{emoji} {pattern_type}"
    if direction:
        header += f" · {direction}"
    if status:
        header += f" · {status}"

    with st.expander(header, expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            if pd.notna(strength_score):
                _render_score_bar("强弱评分", strength_score)

        with col2:
            if pd.notna(volume_score):
                _render_score_bar("量价评分", volume_score)

        with col3:
            if pd.notna(target_price):
                st.metric("目标价", f"{target_price:.2f}")
            if pd.notna(stop_loss_price):
                st.metric("止损价", f"{stop_loss_price:.2f}")

        st.markdown("---")
        st.caption(f"形态索引: #{source_idx}")


def _render_score_bar(label, score):
    st.markdown(f"**{label}**")
    score = float(score)
    score = max(0.0, min(100.0, score))

    if score >= 70:
        bar_color = "#2ECC71"
    elif score >= 40:
        bar_color = "#F39C12"
    else:
        bar_color = "#E74C3C"

    st.markdown(
        f"""
        <div style="display:flex;align-items:center;gap:8px;">
            <div style="flex:1;background:#f0f0f0;border-radius:4px;height:8px;overflow:hidden;">
                <div style="width:{score}%;background:{bar_color};height:100%;border-radius:4px;"></div>
            </div>
            <span style="font-size:14px;font-weight:bold;min-width:40px;text-align:right;">{score:.0f}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
