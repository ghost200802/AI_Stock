import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

UNIFIED_DAILY_COLUMNS = [
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "pct_chg",
    "change",
    "pre_close",
    "turn",
    "pe_ttm",
    "pb_mrq",
    "ps_ttm",
    "pcf_ncf_ttm",
    "is_st",
    "data_type",
    "source",
    "update_time",
]

FLOAT_COLUMNS = {
    "open", "high", "low", "close", "volume", "amount",
    "pct_chg", "change", "pre_close", "turn",
    "pe_ttm", "pb_mrq", "ps_ttm", "pcf_ncf_ttm",
}

STRING_COLUMNS = {
    "ts_code", "trade_date", "is_st", "data_type", "source", "update_time",
}

TUSHARE_FIELD_MAP = {
    "vol": "volume",
}

BAOSTOCK_FIELD_MAP = {
    "peTTM": "pe_ttm",
    "pbMRQ": "pb_mrq",
    "psTTM": "ps_ttm",
    "pcfNcfTTM": "pcf_ncf_ttm",
    "isST": "is_st",
}


def normalize_daily(df: pd.DataFrame, source: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=UNIFIED_DAILY_COLUMNS)

    df = df.copy()

    if source == "tushare":
        field_map = TUSHARE_FIELD_MAP
    elif source == "baostock":
        field_map = BAOSTOCK_FIELD_MAP
    else:
        logger.warning("未知的数据来源 '%s'，将不做字段映射", source)
        field_map = {}

    df = df.rename(columns=field_map)

    for col in UNIFIED_DAILY_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    for col in FLOAT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in STRING_COLUMNS:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    df = df[UNIFIED_DAILY_COLUMNS]

    return df


def merge_records(existing: pd.DataFrame, incoming: pd.DataFrame, priority: list) -> pd.DataFrame:
    if existing.empty:
        return incoming
    if incoming.empty:
        return existing

    combined = pd.concat([existing, incoming], ignore_index=True)

    source_rank = {s: i for i, s in enumerate(priority)}

    combined["_priority_rank"] = combined["source"].map(source_rank).fillna(len(priority))

    combined = combined.sort_values("_priority_rank").drop_duplicates(
        subset=["ts_code", "data_type", "trade_date"], keep="first"
    )

    combined = combined.drop(columns=["_priority_rank"])

    return combined.reset_index(drop=True)
