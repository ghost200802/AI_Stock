import logging
from datetime import datetime, timedelta

import baostock as bs
import pandas as pd
import tushare as ts

from .data_normalizer import normalize_daily
from .db_manager import DBManager
from .utils import get_project_root, load_config, parse_date

logger = logging.getLogger(__name__)


class DataFetcher:

    def __init__(self, config_path=None, use_cache=True):
        self.config = load_config(config_path)
        self.default_source = self.config.get("data_source", {}).get("default", "tushare")
        self.default_dates = self.config.get("default_dates", {})
        self.use_cache = use_cache

        self.db_manager = None
        if self.use_cache:
            try:
                self.db_manager = DBManager(config_path)
            except Exception as e:
                logger.warning("MongoDB 初始化失败，将仅使用 API 获取数据: %s", e)
                self.use_cache = False

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

    @staticmethod
    def _collection_name(symbol):
        return f"stock_{str(symbol).strip()}"

    def get_stock_data(self, symbol, data_type="daily", start_date=None, end_date=None, source=None):
        source = source or self.default_source
        start_date = parse_date(start_date) or self.default_dates.get("start", "2020-01-01")
        end_date = parse_date(end_date) or self.default_dates.get("end", "2026-12-31")

        if not self.use_cache:
            return self._fetch_from_api(symbol, data_type, start_date, end_date, source)

        collection = self._collection_name(symbol)
        ts_code = self._format_tushare_code(symbol)
        query = {
            "ts_code": ts_code,
            "data_type": data_type,
            "trade_date": {"$gte": start_date.replace("-", ""), "$lte": end_date.replace("-", "")},
        }
        cached_df = self.db_manager.find_to_dataframe(collection, query, sort=[("trade_date", 1)])

        if cached_df.empty:
            logger.info("本地无缓存，从 API 全量获取 %s %s 数据 (source=%s)", symbol, data_type, source)
            new_df = self._fetch_from_api(symbol, data_type, start_date, end_date, source)
            if not new_df.empty:
                self._save_to_cache(collection, new_df, ts_code, data_type, source)
            return self.db_manager.find_to_dataframe(collection, query, sort=[("trade_date", 1)])

        latest_date = cached_df["trade_date"].max()
        latest_date_fmt = latest_date.replace("-", "")
        end_date_fmt = end_date.replace("-", "")

        if latest_date_fmt < end_date_fmt:
            next_date = (datetime.strptime(latest_date_fmt, "%Y%m%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            logger.info("增量更新 %s %s: 从 %s 到 %s (source=%s)", symbol, data_type, next_date, end_date, source)
            new_df = self._fetch_from_api(symbol, data_type, next_date, end_date, source)
            if not new_df.empty:
                self._save_to_cache(collection, new_df, ts_code, data_type, source)
            return self.db_manager.find_to_dataframe(collection, query, sort=[("trade_date", 1)])

        logger.info("本地缓存完整，直接返回 %s %s 数据 (%d 条, source=%s)", symbol, data_type, len(cached_df), source)
        return cached_df

    def _save_to_cache(self, collection, df, ts_code, data_type, source):
        if df.empty:
            return
        df = normalize_daily(df, source)
        records = df.to_dict("records")
        for rec in records:
            rec["ts_code"] = ts_code
            rec["data_type"] = data_type
            rec["source"] = source
        self.db_manager.upsert_many(collection, records)

    def _fetch_from_api(self, symbol, data_type, start_date, end_date, source):
        if data_type in ("daily", "weekly", "monthly"):
            return self._fetch_history(symbol, start_date, end_date, source, data_type)
        elif data_type in ("income", "balance", "cashflow"):
            return self._fetch_financial(symbol, data_type, end_date)
        elif data_type == "realtime":
            return self._fetch_realtime(symbol)
        else:
            raise ValueError(f"不支持的数据类型: {data_type}")

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

    def _fetch_history(self, symbol, start_date, end_date, source, period):
        source = source or self.default_source
        if source == "tushare":
            return self._fetch_history_tushare(symbol, start_date, end_date, period, None)
        elif source == "baostock":
            return self._fetch_history_baostock(symbol, start_date, end_date, period, None)
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
            logger.error("TuShare 获取 %s 历史数据失败: %s", symbol, e)
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
            if "date" in df.columns:
                df = df.rename(columns={"date": "trade_date"})
            if "trade_date" in df.columns:
                df["trade_date"] = df["trade_date"].str.replace("-", "")
            return df
        except Exception as e:
            logger.error("BaoStock 获取 %s 历史数据失败: %s", symbol, e)
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
            logger.error("获取实时行情失败: %s", e)
            return pd.DataFrame()

    def _fetch_realtime(self, symbol):
        try:
            ts_code = self._format_tushare_code(symbol)
            today = datetime.now().strftime("%Y%m%d")
            df = self.pro_api.daily(ts_code=ts_code, start_date=today, end_date=today)
            return df if df is not None and not df.empty else pd.DataFrame()
        except Exception as e:
            logger.error("获取 %s 实时行情失败: %s", symbol, e)
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
            logger.error("获取 %s %s 财务数据失败: %s", symbol, report_type, e)
            return pd.DataFrame()

    def _fetch_financial(self, symbol, report_type, end_date):
        try:
            year = int(end_date[:4])
            quarter_map = {1: 1, 2: 1, 3: 2, 4: 2, 5: 2, 6: 2, 7: 3, 8: 3, 9: 3, 10: 4, 11: 4, 12: 4}
            month = int(end_date[5:7])
            quarter = quarter_map.get(month, 4)
            return self.fetch_financial_data(symbol, report_type, year, quarter)
        except Exception as e:
            logger.error("获取 %s %s 财务数据失败: %s", symbol, report_type, e)
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
            logger.error("获取股票列表失败: %s", e)
            return pd.DataFrame()

    def batch_fetch_market_daily(self, trade_date_str):
        try:
            trade_date_fmt = trade_date_str.replace("-", "")
            df = self.pro_api.daily(trade_date=trade_date_fmt)
            if df is None or df.empty:
                return df

            grouped = df.groupby("ts_code")
            for ts_code, group in grouped:
                symbol = ts_code.split(".")[0]
                collection = self._collection_name(symbol)
                records = group.to_dict("records")
                for rec in records:
                    rec["data_type"] = "daily"
                    rec["source"] = self.default_source
                self.db_manager.upsert_many(collection, records)

            logger.info("批量获取并缓存 %s 全市场日线数据，共 %d 条", trade_date_str, len(df))
            return df
        except Exception as e:
            logger.error("批量获取全市场 %s 日线失败: %s", trade_date_str, e)
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
        if self.db_manager:
            try:
                self.db_manager.close()
            except Exception:
                pass
