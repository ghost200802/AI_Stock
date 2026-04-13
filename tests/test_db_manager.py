from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pymongo.errors import ConnectionFailure

from lib.db_manager import DBManager


@pytest.fixture
def mock_config(tmp_path):
    config = {
        "mongodb": {
            "host": "localhost",
            "port": 27017,
            "database": "DB_Stock",
        }
    }
    return config


@pytest.fixture
def mock_db_manager(mock_config):
    mock_client = MagicMock()
    mock_db = MagicMock()
    manager = DBManager.__new__(DBManager)
    manager._client = mock_client
    manager._db = mock_db
    yield manager


class TestDBManagerInit:
    def test_init_success(self, mock_config):
        with patch("lib.utils.load_config", return_value=mock_config):
            with patch("lib.db_manager.MongoClient") as mock_client_cls:
                mock_client = MagicMock()
                mock_db = MagicMock()
                mock_client.__getitem__ = MagicMock(return_value=mock_db)
                mock_client.admin.command = MagicMock()
                mock_client_cls.return_value = mock_client

                manager = DBManager()
                assert manager._db is mock_db
                assert manager._client is mock_client
                mock_client.admin.command.assert_called_once_with("ping")

    def test_init_connection_failure(self, mock_config):
        with patch("lib.utils.load_config", return_value=mock_config):
            with patch("lib.db_manager.MongoClient") as mock_client_cls:
                mock_client = MagicMock()
                mock_client.admin.command = MagicMock(side_effect=ConnectionFailure("connection refused"))
                mock_client_cls.return_value = mock_client

                with pytest.raises(ConnectionFailure, match="无法连接�?MongoDB"):
                    DBManager()

    def test_init_default_config(self):
        config = {}
        with patch("lib.utils.load_config", return_value=config):
            with patch("lib.db_manager.MongoClient") as mock_client_cls:
                mock_client = MagicMock()
                mock_db = MagicMock()
                mock_client.__getitem__ = MagicMock(return_value=mock_db)
                mock_client.admin.command = MagicMock()
                mock_client_cls.return_value = mock_client

                manager = DBManager()
                mock_client_cls.assert_called_once_with("localhost", 27017, serverSelectionTimeoutMS=5000)


class TestGetCollection:
    def test_get_collection(self, mock_db_manager):
        collection = mock_db_manager.get_collection("stock_000001")
        mock_db_manager._db.__getitem__.assert_called_with("stock_000001")


class TestFind:
    def test_find_with_query(self, mock_db_manager):
        mock_collection = MagicMock()
        mock_db_manager._db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_collection.find.return_value = [{"ts_code": "000001.SZ", "trade_date": "20250101"}]

        result = mock_db_manager.find("stock_000001", {"ts_code": "000001.SZ"})
        assert len(result) == 1
        assert result[0]["ts_code"] == "000001.SZ"

    def test_find_empty(self, mock_db_manager):
        mock_collection = MagicMock()
        mock_db_manager._db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_collection.find.return_value = []

        result = mock_db_manager.find("stock_000001")
        assert result == []


class TestFindToDataFrame:
    def test_find_to_dataframe(self, mock_db_manager):
        mock_collection = MagicMock()
        mock_db_manager._db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_collection.find.return_value = [
            {"_id": "abc", "ts_code": "000001.SZ", "trade_date": "20250101", "close": 10.0},
            {"_id": "def", "ts_code": "000001.SZ", "trade_date": "20250102", "close": 11.0},
        ]

        df = mock_db_manager.find_to_dataframe("stock_000001")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "_id" not in df.columns
        assert "close" in df.columns

    def test_find_to_dataframe_empty(self, mock_db_manager):
        mock_collection = MagicMock()
        mock_db_manager._db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_collection.find.return_value = []

        df = mock_db_manager.find_to_dataframe("stock_000001")
        assert isinstance(df, pd.DataFrame)
        assert df.empty


class TestInsertMany:
    def test_insert_many(self, mock_db_manager):
        mock_collection = MagicMock()
        mock_db_manager._db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_result = MagicMock(inserted_ids=["id1", "id2"])
        mock_collection.insert_many.return_value = mock_result

        docs = [{"ts_code": "000001.SZ", "trade_date": "20250101"}, {"ts_code": "000001.SZ", "trade_date": "20250102"}]
        result = mock_db_manager.insert_many("stock_000001", docs)
        assert result is not None
        mock_collection.insert_many.assert_called_once()

    def test_insert_many_empty(self, mock_db_manager):
        result = mock_db_manager.insert_many("stock_000001", [])
        assert result is None


class TestUpsertMany:
    def test_upsert_many(self, mock_db_manager):
        mock_collection = MagicMock()
        mock_db_manager._db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_result = MagicMock(inserted_count=2, modified_count=0)
        mock_collection.bulk_write.return_value = mock_result

        docs = [
            {"ts_code": "000001.SZ", "data_type": "daily", "trade_date": "20250101", "close": 10.0},
            {"ts_code": "000001.SZ", "data_type": "daily", "trade_date": "20250102", "close": 11.0},
        ]
        result = mock_db_manager.upsert_many("stock_000001", docs)
        assert result is not None
        mock_collection.bulk_write.assert_called_once()

    def test_upsert_many_default_unique_keys_no_source(self, mock_db_manager):
        mock_collection = MagicMock()
        mock_db_manager._db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_result = MagicMock(inserted_count=1, modified_count=0)
        mock_collection.bulk_write.return_value = mock_result

        docs = [
            {"ts_code": "000001.SZ", "data_type": "daily", "trade_date": "20250101",
             "close": 10.0, "source": "tushare"},
        ]
        mock_db_manager.upsert_many("stock_000001", docs)

        mock_collection.bulk_write.assert_called_once()
        update_one_list = mock_collection.bulk_write.call_args[0][0]
        filter_doc = update_one_list[0]._filter
        assert "source" not in filter_doc
        assert filter_doc == {"ts_code": "000001.SZ", "data_type": "daily", "trade_date": "20250101"}

    def test_upsert_many_empty(self, mock_db_manager):
        result = mock_db_manager.upsert_many("stock_000001", [])
        assert result is None


class TestFindLatestTradeDate:
    def test_find_latest(self, mock_db_manager):
        mock_collection = MagicMock()
        mock_db_manager._db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_cursor = MagicMock()
        mock_cursor.sort.return_value = mock_cursor
        mock_cursor.limit.return_value = [{"trade_date": "20250630"}]
        mock_collection.find.return_value = mock_cursor

        result = mock_db_manager.find_latest_trade_date("stock_000001", ts_code="000001.SZ", data_type="daily")
        assert result == "20250630"

    def test_find_latest_empty(self, mock_db_manager):
        mock_collection = MagicMock()
        mock_db_manager._db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_cursor = MagicMock()
        mock_cursor.sort.return_value = mock_cursor
        mock_cursor.limit.return_value = []
        mock_collection.find.return_value = mock_cursor

        result = mock_db_manager.find_latest_trade_date("stock_000001")
        assert result is None


class TestClose:
    def test_close(self, mock_db_manager):
        mock_db_manager.close()
        mock_db_manager._client.close.assert_called_once()

    def test_context_manager(self, mock_config):
        with patch("lib.utils.load_config", return_value=mock_config):
            with patch("lib.db_manager.MongoClient") as mock_client_cls:
                mock_client = MagicMock()
                mock_db = MagicMock()
                mock_client.__getitem__ = MagicMock(return_value=mock_db)
                mock_client.admin.command = MagicMock()
                mock_client_cls.return_value = mock_client

                with DBManager() as manager:
                    assert isinstance(manager, DBManager)
                mock_client.close.assert_called_once()
