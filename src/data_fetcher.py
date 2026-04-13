import logging
from datetime import datetime
from pathlib import Path

import baostock as bs
import pandas as pd
import tushare as ts

from .utils import get_project_root, load_config, parse_date

logger = logging.getLogger(__name__)


class DataFetcher:

    def __init__(self, config_path=None):
        self.config = load_config(config_path)
        self.default_source = self.config.get("data_source", {}).get("default", "tushare")
        self.default_dates = self.config.get("default_dates", {})
        self.pro_api = self._load_tushare_token()
        bs.login()

    def _load_tushare_token(self):
        token_file = self.config.get("data_source", {}).get("tushare", {}).get(
            "token_file", "config/tushare_token.txt"
        )
        token_path = get_project_root() / token_file
        if not token_path.exists():
            raise FileNotFoundError(
                f"TuShare token 文件不存在: {token_path}\n"
                f"请创建 {token_path} 并写入您的 TuShare Pro token"
            )
        token = token_path.read_text(encoding="utf-8").strip()
        if not token or token == "YOUR_TUSHARE_TOKEN_HERE":
            raise ValueError(
                f"TuShare token 无效，请在 {token_path} 中设置您的真实 token"
            )
        pro = ts.pro_api(token)
        logger.info("TuShare Pro API 初始化成功")
        return pro

    def _format_tushare_code(self, symbol):
        symbol = str(symbol).strip()
        if symbol.startswith(("000", "001", "002", "300")):
            return f"{symbol}.SZ"
        elif symbol.startswith(("600", "601", "603", "605", "688")):
            return f"{symbol}.SH"
        elif symbol.startswith("900"):
            return f"{symbol}.SH"
        else:
            return f"{symbol}.SH"

    def _format_baostock_code(self, symbol):
        symbol = str(symbol).strip()
        if symbol.startswith(("000", "001", "002", "300")):
            return f"sz.{symbol}"
        elif symbol.startswith(("600", "601", "603", "605", "688")):
            return f"sh.{symbol}"
        elif symbol.startswith("900"):
            return f"sh.{symbol}"
        else:
            return f"sh.{symbol}"

    def fetch_stock_history(self, symbol, start_date=None, end_date=None, source=None, period="daily", adjust=None):
        source = source or self.default_source
        start_date = parse_date(start_date) or self.default_dates.get("start", "2020-01-01")
        end_date = parse_date(end_date) or self.default_dates.get("end", "2026-12-31")

        if source == "tushare":
            return self._fetch_history_tushare(symbol, start_date, end_date, period, adjust)
        elif source == "baostock":
            return self._fetch_history_baostock(symbol, start_date, end_date, period, adjust)
        else:
            raise ValueError(f"不支持的数据源: {source}")

    def _fetch_history_tushare(self, symbol, start_date, end_date, period, adjust):
        ts_code = self._format_tushare_code(symbol)
        start_date_fmt = start_date.replace("-", "")
        end_date_fmt = end_date.replace("-", "")

        period_map = {
            "daily": "daily",
            "weekly": "weekly",
            "monthly": "monthly",
        }
        ts_period = period_map.get(period, "daily")

        try:
            if ts_period == "daily":
                df = self.pro_api.daily(
                    ts_code=ts_code,
                    start_date=start_date_fmt,
                    end_date=end_date_fmt,
                )
            elif ts_period == "weekly":
                df = self.pro_api.weekly(
                    ts_code=ts_code,
                    start_date=start_date_fmt,
                    end_date=end_date_fmt,
                )
            elif ts_period == "monthly":
                df = self.pro_api.monthly(
                    ts_code=ts_code,
                    start_date=start_date_fmt,
                    end_date=end_date_fmt,
                )
            else:
                df = pd.DataFrame()

            if df is not None and not df.empty:
                df = df.sort_values("trade_date").reset_index(drop=True)
            return df if df is not None else pd.DataFrame()
        except Exception as e:
            logger.error(f"TuShare 获取 {symbol} 历史数据失败: {e}")
            return pd.DataFrame()

    def _fetch_history_baostock(self, symbol, start_date, end_date, period, adjust):
        if adjust is None:
            adjust = self.config.get("data_source", {}).get("baostock", {}).get("adjust", "3")

        bs_code = self._format_baostock_code(symbol)
        period_map = {"daily": "d", "weekly": "w", "monthly": "m"}
        bs_period = period_map.get(period, "d")

        fields = "date,open,high,low,close,volume,amount,turn,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"

        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                fields,
                start_date=start_date,
                end_date=end_date,
                frequency=bs_period,
                adjustflag=adjust,
            )
            rows = []
            while rs.error_code == "0" and rs.next():
                rows.append(rs.get_row_data())
            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame(rows, columns=rs.fields)
            numeric_cols = ["open", "high", "low", "close", "volume", "amount", "turn",
                            "peTTM", "pbMRQ", "psTTM", "pcfNcfTTM"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            return df
        except Exception as e:
            logger.error(f"BaoStock 获取 {symbol} 历史数据失败: {e}")
            return pd.DataFrame()

    def fetch_realtime_quotes(self, symbols=None):
        try:
            today = datetime.now().strftime("%Y%m%d")
            df = self.pro_api.daily(trade_date=today)
            if df is None or df.empty:
                return pd.DataFrame()
            if symbols is not None:
                ts_symbols = [self._format_tushare_code(s) for s in symbols]
                df = df[df["ts_code"].isin(ts_symbols)]
            return df
        except Exception as e:
            logger.error(f"获取实时行情失败: {e}")
            return pd.DataFrame()

    def fetch_financial_data(self, symbol, report_type="income", year=None, quarter=None):
        bs_code = self._format_baostock_code(symbol)

        now = pd.Timestamp.now()
        if year is None:
            year = now.year
        else:
            year = int(year)
        if quarter is not None:
            quarter = int(quarter)
        else:
            if now.month >= 10:
                quarter = 3
            elif now.month >= 7:
                quarter = 2
            elif now.month >= 4:
                quarter = 1
            else:
                year -= 1
                quarter = 4

        try:
            if report_type == "income":
                rs = bs.query_profit_data(code=bs_code, year=year, quarter=quarter)
            elif report_type == "balance":
                rs = bs.query_balance_data(code=bs_code, year=year, quarter=quarter)
            elif report_type == "cashflow":
                rs = bs.query_cash_flow_data(code=bs_code, year=year, quarter=quarter)
            else:
                raise ValueError(f"不支持的报表类型: {report_type}")

            rows = []
            while rs.error_code == "0" and rs.next():
                rows.append(rs.get_row_data())
            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame(rows, columns=rs.fields)
            return df
        except Exception as e:
            logger.error(f"获取 {symbol} {report_type} 财务数据失败: {e}")
            return pd.DataFrame()

    def fetch_stock_list(self):
        try:
            df = self.pro_api.stock_basic(
                exchange="",
                list_status="L",
                fields="ts_code,symbol,name",
            )
            if df is None or df.empty:
                return pd.DataFrame()
            result = df[["symbol", "name"]].copy()
            result.columns = ["code", "name"]
            return result.reset_index(drop=True)
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        try:
            bs.logout()
        except Exception:
            pass
