import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml


def get_project_root():
    return Path(__file__).resolve().parent.parent


def load_config(config_path=None):
    if config_path is None:
        config_path = get_project_root() / "config" / "config.yaml"
    else:
        config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config


def ensure_dir(filepath):
    dir_path = Path(filepath).parent
    dir_path.mkdir(parents=True, exist_ok=True)


def save_data(df, filepath, format="csv", encoding="utf-8-sig"):
    filepath = Path(filepath)
    ensure_dir(filepath)
    if format == "csv":
        df.to_csv(filepath, index=False, encoding=encoding)
    elif format == "parquet":
        df.to_parquet(filepath, index=False)
    else:
        raise ValueError(f"不支持的格式: {format}")


def format_stock_code(symbol, source="tushare"):
    symbol = str(symbol).strip()
    if source == "tushare":
        if symbol.startswith(("000", "001", "002", "300")):
            return f"{symbol}.SZ"
        elif symbol.startswith(("600", "601", "603", "605", "688", "900")):
            return f"{symbol}.SH"
        else:
            return f"{symbol}.SH"
    elif source == "baostock":
        if symbol.startswith(("000", "001", "002", "300")):
            return f"sz.{symbol}"
        elif symbol.startswith(("600", "601", "603", "605", "688", "900")):
            return f"sh.{symbol}"
        else:
            return f"sh.{symbol}"
    else:
        raise ValueError(f"不支持的数据源: {source}")


def parse_date(date_str):
    if date_str is None:
        return None
    date_str = str(date_str).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"无法解析日期: {date_str}")
