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

# 状态标签颜色映射
STATUS_COLOR = {
    "形成中": "#3498DB",   # 蓝色
    "已确认": "#2ECC71",   # 绿色
    "已失效": "#95A5A6",   # 灰色
}


def render_pattern_panel(patterns_df, score_threshold=70, show_all=False):
    """渲染形态总览面板，返回选中的形态索引（int 或 None）"""
    if patterns_df is None or patterns_df.empty:
        st.info("暂无形态识别结果。缠论/蔡森模块实现后将自动展示形态信息。")
        return None

    required_cols = ["pattern_type"]
    for col in required_cols:
        if col not in patterns_df.columns:
            st.warning(f"形态数据格式不完整（缺少 {col} 列）")
            return None

    st.subheader("形态总览")

    # ---------- 准备数据 ----------
    display_df = _prepare_display_data(patterns_df)

    if display_df.empty:
        st.info("当前周期未识别到有效形态。")
        return None

    # ---------- 确保 session_state 中有选中索引 ----------
    if "pattern_selected_idx" not in st.session_state:
        st.session_state["pattern_selected_idx"] = None

    selected_idx = st.session_state["pattern_selected_idx"]

    # ---------- 判断哪些是高分 / 低分 ----------
    # strength_score 可能为 NaN，NaN 视为低于阈值
    def _is_high_score(score):
        if pd.isna(score):
            return False
        return float(score) >= score_threshold

    # ---------- 渲染卡片列表 ----------
    for idx, row in display_df.iterrows():
        source_idx = row["_source"]
        strength_score = row.get("强弱评分", None)
        is_high = _is_high_score(strength_score) if not show_all else True
        is_dimmed = not is_high

        # 点击逻辑：toggle 选中
        is_selected = (selected_idx == source_idx)
        clicked = st.button(
            key=f"pattern_btn_{source_idx}",
            label="",  # 使用自定义 HTML，label 留空
            use_container_width=True,
        )

        if clicked:
            if is_selected:
                st.session_state["pattern_selected_idx"] = None
            else:
                st.session_state["pattern_selected_idx"] = source_idx
            st.rerun()

        # 当前选中状态（可能在上面刚更新过）
        is_selected_now = (st.session_state["pattern_selected_idx"] == source_idx)

        # 构建卡片 HTML
        card_html = _build_card_html(
            row=row,
            source_idx=source_idx,
            is_dimmed=is_dimmed,
            is_selected=is_selected_now,
        )
        st.markdown(card_html, unsafe_allow_html=True)

    return st.session_state["pattern_selected_idx"]


def _prepare_display_data(df):
    """将原始 DataFrame 转换为显示用 DataFrame"""
    display = pd.DataFrame()

    if "pattern_type" in df.columns:
        display["形态类型"] = df["pattern_type"]

    if "direction" in df.columns:
        display["方向"] = df["direction"].map(
            lambda x: DIRECTION_LABELS.get(x, str(x))
        )
        # 保留原始 direction 以便查 emoji
        display["_direction_raw"] = df["direction"]

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

    if "start_date" in df.columns:
        display["起始日期"] = df["start_date"]

    if "end_date" in df.columns:
        display["结束日期"] = df["end_date"]

    display["_source"] = df.reset_index(drop=True).index

    return display


def _score_bar_html(score):
    """生成评分进度条 HTML 片段"""
    if pd.isna(score):
        return '<span style="color:#aaa;">N/A</span>'

    score = float(score)
    score = max(0.0, min(100.0, score))

    if score >= 70:
        bar_color = "#2ECC71"
    elif score >= 40:
        bar_color = "#F39C12"
    else:
        bar_color = "#E74C3C"

    return (
        f'<div style="display:flex;align-items:center;gap:6px;min-width:100px;">'
        f'  <div style="flex:1;background:#f0f0f0;border-radius:4px;height:6px;overflow:hidden;">'
        f'    <div style="width:{score}%;background:{bar_color};height:100%;border-radius:4px;"></div>'
        f'  </div>'
        f'  <span style="font-size:13px;font-weight:bold;min-width:30px;text-align:right;">{score:.0f}</span>'
        f'</div>'
    )


def _status_tag_html(status_text):
    """生成状态标签 HTML 片段"""
    color = STATUS_COLOR.get(status_text, "#95A5A6")
    return (
        f'<span style="display:inline-block;padding:1px 8px;border-radius:10px;'
        f'font-size:12px;color:#fff;background:{color};white-space:nowrap;">'
        f'{status_text}</span>'
    )


def _build_card_html(row, source_idx, is_dimmed, is_selected):
    """构建一张紧凑形态卡片的完整 HTML"""

    pattern_type = row.get("形态类型", "未知")
    direction = row.get("方向", "")
    direction_raw = row.get("_direction_raw", "")
    status = row.get("状态", "")
    strength_score = row.get("强弱评分", None)
    volume_score = row.get("量价评分", None)
    target_price = row.get("目标价", None)
    stop_loss_price = row.get("止损价", None)
    start_date = row.get("起始日期", None)
    end_date = row.get("结束日期", None)

    emoji = DIRECTION_EMOJI.get(direction_raw, "")

    # 整体样式
    opacity = "0.5" if is_dimmed else "1.0"
    border = "2px solid #3498DB;" if is_selected else "1px solid #e0e0e0;"
    bg_color = "#EBF5FB;" if is_selected else "#ffffff;"

    # 状态标签
    status_tag = _status_tag_html(status) if status else ""

    # 评分条
    strength_bar = _score_bar_html(strength_score)
    volume_bar = _score_bar_html(volume_score)

    # 目标价 / 止损价
    price_parts = []
    if pd.notna(target_price):
        price_parts.append(f"目标: {target_price:.2f}")
    if pd.notna(stop_loss_price):
        price_parts.append(f"止损: {stop_loss_price:.2f}")
    price_text = " &nbsp;|&nbsp; ".join(price_parts) if price_parts else ""

    # 日期范围
    date_parts = []
    if pd.notna(start_date):
        date_parts.append(str(start_date))
    if pd.notna(end_date):
        date_parts.append(str(end_date))
    date_text = " ~ ".join(date_parts) if date_parts else ""

    html = (
        f'<div style="border:{border}border-radius:8px;padding:8px 12px;margin-bottom:6px;'
        f'background:{bg_color}opacity:{opacity};cursor:pointer;'
        f'box-shadow:0 1px 3px rgba(0,0,0,0.08);">'
        # 上半部分：类型 + 方向 + 状态
        f'  <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">'
        f'    <span style="font-size:14px;font-weight:600;">{emoji} {pattern_type}</span>'
        f'    <span style="font-size:12px;color:#666;">{direction}</span>'
        f'    {status_tag}'
        f'  </div>'
        # 下半部分：评分 + 价格 + 日期
        f'  <div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap;">'
        f'    <div style="display:flex;align-items:center;gap:4px;font-size:11px;color:#888;">'
        f'      强弱 {strength_bar}'
        f'    </div>'
        f'    <div style="display:flex;align-items:center;gap:4px;font-size:11px;color:#888;">'
        f'      量价 {volume_bar}'
        f'    </div>'
    )

    if price_text:
        html += (
            f'    <div style="font-size:12px;color:#555;margin-left:auto;">'
            f'      {price_text}'
            f'    </div>'
        )

    html += f'  </div>'

    if date_text:
        html += (
            f'  <div style="font-size:11px;color:#aaa;margin-top:4px;">{date_text}</div>'
        )

    html += f'</div>'

    return html
