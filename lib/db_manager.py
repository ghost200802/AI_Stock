import logging
from datetime import datetime

import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure

logger = logging.getLogger(__name__)


class DBManager:

    def __init__(self, config_path=None):
        from .utils import load_config
        config = load_config(config_path)
        mongo_conf = config.get("mongodb", {})
        host = mongo_conf.get("host", "localhost")
        port = mongo_conf.get("port", 27017)
        database = mongo_conf.get("database", "DB_Stock")

        self._client = MongoClient(host, port, serverSelectionTimeoutMS=5000)
        try:
            self._client.admin.command("ping")
        except ConnectionFailure:
            raise ConnectionFailure(
                f"无法连接到 MongoDB ({host}:{port})，请检查 MongoDB 服务是否已启动"
            )

        self._db = self._client[database]
        logger.info("MongoDB 连接成功: %s:%s/%s", host, port, database)

    @property
    def database(self):
        return self._db

    @property
    def client(self):
        return self._client

    def get_collection(self, collection_name):
        return self._db[collection_name]

    def _ensure_index(self, collection_name):
        collection = self._db[collection_name]
        collection.create_index(
            [("ts_code", 1), ("data_type", 1), ("trade_date", 1)],
            unique=True,
            background=True,
        )
        collection.create_index(
            [("trade_date", 1)],
            background=True,
        )

    def find(self, collection_name, query=None, projection=None, sort=None, limit=0):
        collection = self._db[collection_name]
        cursor = collection.find(query or {}, projection or {})
        if sort:
            cursor = cursor.sort(sort)
        if limit > 0:
            cursor = cursor.limit(limit)
        return list(cursor)

    def find_to_dataframe(self, collection_name, query=None, projection=None, sort=None):
        docs = self.find(collection_name, query, projection, sort)
        if not docs:
            return pd.DataFrame()
        df = pd.DataFrame(docs)
        if "_id" in df.columns:
            df = df.drop(columns=["_id"])
        return df

    def insert_many(self, collection_name, documents):
        if not documents:
            return
        collection = self._db[collection_name]
        self._ensure_index(collection_name)
        now = datetime.now().isoformat()
        for doc in documents:
            if "update_time" not in doc:
                doc["update_time"] = now
        result = collection.insert_many(documents)
        logger.info("插入 %d 条数据到 %s", len(result.inserted_ids), collection_name)
        return result

    def upsert_many(self, collection_name, documents, unique_keys=("ts_code", "data_type", "trade_date")):
        if not documents:
            return
        collection = self._db[collection_name]
        self._ensure_index(collection_name)
        now = datetime.now().isoformat()
        ops = []
        for doc in documents:
            if "update_time" not in doc:
                doc["update_time"] = now
            filter_doc = {k: doc[k] for k in unique_keys if k in doc}
            if not filter_doc:
                continue
            update_doc = {"$set": doc}
            ops.append({"filter": filter_doc, "update": update_doc, "upsert": True})

        if ops:
            result = collection.bulk_write(
                [UpdateOne(op["filter"], op["update"], upsert=op["upsert"]) for op in ops]
            )
            logger.info(
                "upsert 到 %s: 插入 %d, 更新 %d, upsert %d",
                collection_name,
                result.inserted_count,
                result.modified_count,
                result.upserted_count,
            )
            return result
        return None

    def find_latest_trade_date(self, collection_name, ts_code=None, data_type=None):
        query = {}
        if ts_code:
            query["ts_code"] = ts_code
        if data_type:
            query["data_type"] = data_type
        docs = self.find(
            collection_name,
            query,
            projection={"trade_date": 1},
            sort=[("trade_date", -1)],
            limit=1,
        )
        if docs:
            return docs[0].get("trade_date")
        return None

    def close(self):
        if self._client:
            self._client.close()
            logger.info("MongoDB 连接已关闭")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
