import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class ChanLunCacheManager:

    FRACTAL_DATA_TYPE = "chanlun_fractions_daily"
    BI_DATA_TYPE = "chanlun_bis_daily"

    def __init__(self, db_manager):
        self._db = db_manager

    def save_fractals(
        self,
        collection: str,
        ts_code: str,
        period: str,
        fractals: list,
    ):
        if not fractals:
            return

        data_type = self._make_data_type("fractions", period)
        records = []
        for f in fractals:
            records.append({
                "ts_code": ts_code,
                "data_type": data_type,
                "trade_date": f.trade_date,
                "fractal_type": f.fractal_type.value,
                "price": f.high if f.fractal_type.value == "top" else f.low,
                "high": f.high,
                "low": f.low,
                "index": f.index,
                "update_time": datetime.now().isoformat(),
            })

        self._db.upsert_many(collection, records)
        logger.info("缓存 %d 条分型数据: %s %s", len(records), ts_code, period)

    def load_fractals(
        self,
        collection: str,
        ts_code: str,
        period: str,
    ) -> pd.DataFrame:
        data_type = self._make_data_type("fractions", period)
        query = {"ts_code": ts_code, "data_type": data_type}
        df = self._db.find_to_dataframe(collection, query, sort=[("index", 1)])
        if not df.empty and "_id" in df.columns:
            df = df.drop(columns=["_id"])
        return df

    def save_bis(
        self,
        collection: str,
        ts_code: str,
        period: str,
        bis: list,
    ):
        if not bis:
            return

        data_type = self._make_data_type("bis", period)
        records = []
        for i, b in enumerate(bis):
            records.append({
                "ts_code": ts_code,
                "data_type": data_type,
                "trade_date": b.end_date,
                "bi_index": i,
                "bi_direction": b.direction.value,
                "start_date": b.start_date,
                "end_date": b.end_date,
                "start_price": b.start_price,
                "end_price": b.end_price,
                "confirmed": b.confirmed,
                "update_time": datetime.now().isoformat(),
            })

        self._db.upsert_many(collection, records)
        logger.info("缓存 %d 条笔数据: %s %s", len(records), ts_code, period)

    def load_bis(
        self,
        collection: str,
        ts_code: str,
        period: str,
    ) -> pd.DataFrame:
        data_type = self._make_data_type("bis", period)
        query = {"ts_code": ts_code, "data_type": data_type}
        df = self._db.find_to_dataframe(collection, query, sort=[("bi_index", 1)])
        if not df.empty and "_id" in df.columns:
            df = df.drop(columns=["_id"])
        return df

    def invalidate_after(
        self,
        collection: str,
        ts_code: str,
        period: str,
        after_date,
    ):
        for sub_type in ["fractions", "bis"]:
            data_type = self._make_data_type(sub_type, period)
            query = {
                "ts_code": ts_code,
                "data_type": data_type,
                "trade_date": {"$gt": after_date},
            }
            col = self._db.get_collection(collection)
            result = col.delete_many(query)
            if result.deleted_count > 0:
                logger.info(
                    "删除 %s 缓存 %d 条 (%s 之后): %s %s",
                    sub_type, result.deleted_count, after_date, ts_code, period,
                )

    def ensure_indexes(self, collection: str):
        col = self._db.get_collection(collection)
        col.create_index(
            [("ts_code", 1), ("data_type", 1), ("trade_date", 1)],
            unique=True,
            background=True,
        )
        col.create_index(
            [("ts_code", 1), ("data_type", 1), ("bi_index", 1)],
            unique=False,
            background=True,
        )

    @classmethod
    def _make_data_type(cls, sub_type: str, period: str) -> str:
        return f"chanlun_{sub_type}_{period}"
