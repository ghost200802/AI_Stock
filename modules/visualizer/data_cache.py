import logging
from datetime import datetime, timedelta

import pandas as pd

from lib.data_fetcher import DataFetcher
from lib.db_manager import DBManager
from lib.utils import load_config

logger = logging.getLogger(__name__)


class ChartDataCache:

    def __init__(self, config_path=None):
        self.config = load_config(config_path)
        self._fetcher = None
        self._db_manager = None

    @property
    def fetcher(self):
        if self._fetcher is None:
            self._fetcher = DataFetcher()
        return self._fetcher

    @property
    def db_manager(self):
        if self._db_manager is None:
            self._db_manager = DBManager()
        return self._db_manager

    def get_kline_data(self, symbol, period="daily", start_date=None, end_date=None):
        vis_config = self.config.get("visualizer", {})
        if start_date is None:
            days = vis_config.get("default_date_range_days", 365)
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        df = self.fetcher.get_stock_data(
            symbol, data_type=period, start_date=start_date, end_date=end_date
        )

        if not df.empty and "trade_date" in df.columns:
            df = df.copy()
            df["trade_date"] = pd.to_datetime(df["trade_date"].astype(str).str.replace("-", ""), format="%Y%m%d")

        return df

    def get_analysis_data(self, symbol, period="daily", analysis_type="chanlun"):
        collection = f"stock_{symbol}"
        ts_code = self.fetcher._format_tushare_code(symbol)

        query = {
            "ts_code": ts_code,
            "data_type": f"{analysis_type}_{period}",
        }

        cached_df = self.db_manager.find_to_dataframe(collection, query)
        if not cached_df.empty:
            logger.info("命中 %s 分析缓存: %s %s", analysis_type, symbol, period)
            return cached_df

        logger.info("未命中 %s 分析缓存，尝试触发计算: %s %s", analysis_type, symbol, period)
        result = self._try_compute_analysis(symbol, period, analysis_type, ts_code)

        if result is not None and not result.empty:
            return result

        logger.warning("分析模块 %s 尚未实现，返回空数据", analysis_type)
        return pd.DataFrame()

    def _try_compute_analysis(self, symbol, period, analysis_type, ts_code):
        try:
            if analysis_type == "chanlun":
                from modules.chanlun import compute_bi
                from modules.chanlun.cache_manager import ChanLunCacheManager
                from modules.chanlun.include_processor import IncludeProcessor
                from modules.chanlun.fractal_detector import FractalDetector
                from modules.chanlun.bi_generator import BiGenerator

                kline_df = self.get_kline_data(symbol, period)
                if kline_df.empty:
                    return None

                include_processor = IncludeProcessor()
                processed_klines = include_processor.process(kline_df)

                fractal_detector = FractalDetector()
                fractals = fractal_detector.detect(processed_klines)

                bi_generator = BiGenerator()
                bis = bi_generator.generate(fractals, processed_klines)

                collection = f"stock_{symbol}"
                chanlun_cache = ChanLunCacheManager(self.db_manager)
                chanlun_cache.ensure_indexes(collection)
                chanlun_cache.save_fractals(collection, ts_code, period, fractals)
                chanlun_cache.save_bis(collection, ts_code, period, bis)

                result_df = compute_bi(kline_df)
                if result_df is not None and not result_df.empty:
                    return result_df
                return None
            elif analysis_type == "caisen":
                from modules.caisen import compute_patterns
                kline_df = self.get_kline_data(symbol, period)
                if kline_df.empty:
                    return None
                result_df = compute_patterns(kline_df)
                if result_df is not None and not result_df.empty:
                    records = result_df.to_dict("records")
                    for rec in records:
                        rec["ts_code"] = ts_code
                        rec["data_type"] = f"caisen_{period}"
                    collection = f"stock_{symbol}"
                    coll = self.db_manager.get_collection(collection)
                    coll.delete_many({
                        "ts_code": ts_code,
                        "data_type": f"caisen_{period}",
                    })
                    self.db_manager.insert_many(collection, records)
                    return result_df
        except (ImportError, AttributeError):
            logger.debug("分析模块 %s 尚未实现或缺少入口函数", analysis_type)
        except Exception as e:
            logger.error("触发 %s 计算失败: %s", analysis_type, e)
        return None

    def get_stock_list(self):
        return self.fetcher.fetch_stock_list()

    def close(self):
        if self._fetcher:
            self._fetcher.close()
            self._fetcher = None
        if self._db_manager:
            self._db_manager.close()
            self._db_manager = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
