import sys
import os
import logging
import traceback

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("test_cache_688110")

SYMBOL = "688110"
START_DATE = "2024-01-01"
END_DATE = "2025-04-10"

passed = 0
failed = 0


def check(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        logger.info("[PASS] %s", name)
    else:
        failed += 1
        logger.error("[FAIL] %s %s", name, detail)


def cleanup():
    try:
        from lib.db_manager import DBManager
        mgr = DBManager()
        mgr.get_collection("stock_688110").drop()
        logger.info("已清理集合stock_688110")
        mgr.close()
    except Exception:
        pass


def test_tushare():
    logger.info("=" * 60)
    logger.info("测试1: tushare 数据源+ MongoDB 缓存")
    logger.info("=" * 60)

    from lib.data_fetcher import DataFetcher

    fetcher = DataFetcher(use_cache=True)
    try:
        ts_code = fetcher._format_tushare_code(SYMBOL)
        check("tushare code 格式化, ts_code == "688110.SH", f"got {ts_code}")

        collection = fetcher._collection_name(SYMBOL)
        check("collection 名称", collection == "stock_688110", f"got {collection}")

        logger.info("首次获取（应从API 拉取并写入缓存）...")
        df1 = fetcher.get_stock_data(SYMBOL, "daily", START_DATE, END_DATE, source="tushare")
        check("tushare 首次获取非空", not df1.empty, f"len={len(df1)}")
        if not df1.empty:
            check("tushare 数据列包含close", "close" in df1.columns)
            check("tushare 数据列包含ts_code", "ts_code" in df1.columns)
            check("tushare 数据列包含data_type", "data_type" in df1.columns)
            check("tushare 数据列包含trade_date", "trade_date" in df1.columns)
            check("tushare 数据列包含source", "source" in df1.columns, "缓存文档应包含source 字段")
            if "source" in df1.columns:
                check("tushare source 值正确, (df1["source"] == "tushare").all(),
                      f"got {df1['source'].unique()}")
            logger.info("  数据条数: %d, 列 %s", len(df1), list(df1.columns))
            logger.info("  日期范围: %s ~ %s", df1["trade_date"].iloc[0], df1["trade_date"].iloc[-1])
            logger.info("  最新收盘价: %s", df1["close"].iloc[-1])

        logger.info("二次获取（应从缓存读取）...")
        df2 = fetcher.get_stock_data(SYMBOL, "daily", START_DATE, END_DATE, source="tushare")
        check("tushare 二次获取非空", not df2.empty)
        check("tushare 两次获取条数一致, len(df1) == len(df2), f"{len(df1)} vs {len(df2)}")

        logger.info("直接从MongoDB 验证缓存...")
        db_collection = fetcher.db_manager.get_collection(collection)
        cached_count = db_collection.count_documents({
            "ts_code": ts_code,
            "data_type": "daily",
            "source": "tushare",
        })
        check("MongoDB tushare 缓存条数 > 0", cached_count > 0, f"count={cached_count}")

        cached_doc = db_collection.find_one(
            {"ts_code": ts_code, "data_type": "daily", "source": "tushare"},
            sort=[("trade_date", -1)]
        )
        if cached_doc:
            check("MongoDB 缓存文档有update_time", "update_time" in cached_doc)
            check("MongoDB 缓存文档有source 字段", "source" in cached_doc)
            logger.info("  缓存最新日期 %s, update_time: %s, source: %s",
                        cached_doc.get("trade_date"), cached_doc.get("update_time"), cached_doc.get("source"))

    except Exception as e:
        logger.error("tushare 测试异常: %s\n%s", e, traceback.format_exc())
        check("tushare 测试无异常, False, str(e))
    finally:
        fetcher.close()


def test_baostock():
    logger.info("=" * 60)
    logger.info("测试2: baostock 数据源+ MongoDB 缓存（独立于 tushare 缓存）
    logger.info("=" * 60)

    from lib.data_fetcher import DataFetcher

    fetcher = DataFetcher(use_cache=True)
    try:
        bs_code = fetcher._format_baostock_code(SYMBOL)
        check("baostock code 格式化, bs_code == "sh.688110", f"got {bs_code}")

        logger.info("首次获取 baostock 数据（应从API 拉取并写入缓存，不应命中 tushare 缓存）..")
        df1 = fetcher.get_stock_data(SYMBOL, "daily", START_DATE, END_DATE, source="baostock")
        check("baostock 首次获取非空", not df1.empty, f"len={len(df1)}")
        if not df1.empty:
            check("baostock 数据列包含close", "close" in df1.columns)
            check("baostock 数据列包含trade_date", "trade_date" in df1.columns)
            check("baostock 数据列包含source", "source" in df1.columns, "缓存文档应包含source 字段")
            if "source" in df1.columns:
                check("baostock source 值正确, (df1["source"] == "baostock").all(),
                      f"got {df1['source'].unique()}")
            logger.info("  数据条数: %d, 列 %s", len(df1), list(df1.columns))
            logger.info("  日期范围: %s ~ %s", df1["trade_date"].iloc[0], df1["trade_date"].iloc[-1])
            logger.info("  最新收盘价: %s", df1["close"].iloc[-1])

        logger.info("验证 baostock 缓存独立于tushare 缓存...")
        db_collection = fetcher.db_manager.get_collection("stock_688110")
        ts_count = db_collection.count_documents({"source": "tushare", "data_type": "daily"})
        bs_count = db_collection.count_documents({"source": "baostock", "data_type": "daily"})
        check("tushare 和baostock 缓存各自独立", ts_count > 0 and bs_count > 0,
              f"tushare={ts_count}, baostock={bs_count}")
        check("tushare 和baostock 缓存条数可以不同（数据源差异）, True, "")

        logger.info("二次获取 baostock 数据（应从缓存读取）...")
        df2 = fetcher.get_stock_data(SYMBOL, "daily", START_DATE, END_DATE, source="baostock")
        check("baostock 二次获取非空", not df2.empty)
        check("baostock 两次获取条数一致, len(df1) == len(df2), f"{len(df1)} vs {len(df2)}")

    except Exception as e:
        logger.error("baostock 测试异常: %s\n%s", e, traceback.format_exc())
        check("baostock 测试无异常, False, str(e))
    finally:
        fetcher.close()


def test_incremental_update():
    logger.info("=" * 60)
    logger.info("测试3: 增量更新机制")
    logger.info("=" * 60)

    from lib.data_fetcher import DataFetcher

    cleanup()

    fetcher = DataFetcher(use_cache=True)
    try:
        short_end = "2024-06-30"
        full_end = "2024-12-31"

        logger.info("先获取短期数据 %s ~ %s", START_DATE, short_end)
        df_short = fetcher.get_stock_data(SYMBOL, "daily", START_DATE, short_end, source="tushare")
        check("短期数据获取成功", not df_short.empty, f"len={len(df_short)}")

        if not df_short.empty:
            latest_cached = df_short["trade_date"].max()
            logger.info("  缓存最新日期 %s", latest_cached)

            db_collection = fetcher.db_manager.get_collection("stock_688110")
            short_count = db_collection.count_documents({
                "ts_code": "688110.SH", "data_type": "daily", "source": "tushare"
            })
            logger.info("  MongoDB 中tushare 缓存条数: %d", short_count)

        logger.info("再获取更长期数据: %s ~ %s（应触发增量更新）, START_DATE, full_end)
        df_full = fetcher.get_stock_data(SYMBOL, "daily", START_DATE, full_end, source="tushare")
        check("长期数据获取成功", not df_full.empty, f"len={len(df_full)}")
        check("长期数据条数 >= 短期数据条数", len(df_full) >= len(df_short),
              f"{len(df_full)} vs {len(df_short)}")

        if not df_full.empty:
            db_collection = fetcher.db_manager.get_collection("stock_688110")
            full_count = db_collection.count_documents({
                "ts_code": "688110.SH", "data_type": "daily", "source": "tushare"
            })
            check("增量更新后MongoDB 条数增加", full_count > short_count,
                  f"after={full_count}, before={short_count}")
            logger.info("  增量更新后MongoDB 条数: %d", full_count)

    except Exception as e:
        logger.error("增量更新测试异常: %s\n%s", e, traceback.format_exc())
        check("增量更新测试无异常, False, str(e))
    finally:
        fetcher.close()


def test_price_trend():
    logger.info("=" * 60)
    logger.info("测试4: 东芯股份(688110) 价格走势分析")
    logger.info("=" * 60)

    from lib.data_fetcher import DataFetcher
    import pandas as pd

    fetcher = DataFetcher(use_cache=True)
    try:
        df = fetcher.get_stock_data(SYMBOL, "daily", START_DATE, END_DATE, source="tushare")
        if df.empty:
            logger.warning("无数据，尝试 baostock...")
            df = fetcher.get_stock_data(SYMBOL, "daily", START_DATE, END_DATE, source="baostock")

        if df.empty:
            check("价格走势分析有数据, False, "tushare 和baostock 均无数据")
            return

        check("价格走势分析有数据, True, f"共{len(df)} 条)

        close = pd.to_numeric(df["close"], errors="coerce").dropna()
        if close.empty:
            check("close 列可转为数值, False)
            return
        check("close 列可转为数值, True)

        first_price = close.iloc[0]
        last_price = close.iloc[-1]
        max_price = close.max()
        min_price = close.min()
        total_return = (last_price - first_price) / first_price * 100

        logger.info("  起始价 %.2f (%s)", first_price, df["trade_date"].iloc[0])
        logger.info("  最新价: %.2f (%s)", last_price, df["trade_date"].iloc[-1])
        logger.info("  最高价: %.2f", max_price)
        logger.info("  最低价: %.2f", min_price)
        logger.info("  区间涨跌幅 %.2f%%", total_return)

        if len(close) >= 5:
            ma5 = close.rolling(5).mean().iloc[-1]
            logger.info("  最新日均线 %.2f", ma5)
            check("最新价 vs MA5", last_price > ma5,
                  f"收盘价{last_price:.2f} {'>' if last_price > ma5 else '<='} MA5 {ma5:.2f}")

        if len(close) >= 20:
            ma20 = close.rolling(20).mean().iloc[-1]
            logger.info("  最新20日均线 %.2f", ma20)
            check("最新价 vs MA20", last_price > ma20,
                  f"收盘价{last_price:.2f} {'>' if last_price > ma20 else '<='} MA20 {ma20:.2f}")

        if len(close) >= 60:
            ma60 = close.rolling(60).mean().iloc[-1]
            logger.info("  最新60日均线 %.2f", ma60)
            check("最新价 vs MA60", last_price > ma60,
                  f"收盘价{last_price:.2f} {'>' if last_price > ma60 else '<='} MA60 {ma60:.2f}")

        if total_return > 10:
            trend = "走强（涨幅较大）"
        elif total_return > 0:
            trend = "小幅上涨"
        elif total_return > -10:
            trend = "小幅下跌"
        else:
            trend = "走弱（跌幅较大）"

        logger.info("  综合判断: %s", trend)

    except Exception as e:
        logger.error("价格走势分析异常: %s\n%s", e, traceback.format_exc())
        check("价格走势分析无异常, False, str(e))
    finally:
        fetcher.close()


if __name__ == "__main__":
    import pandas as pd

    cleanup()
    try:
        test_tushare()
        test_baostock()
        test_incremental_update()
        test_price_trend()
    finally:
        logger.info("=" * 60)
        logger.info("测试结果汇总 %d 通过, %d 失败", passed, failed)
        logger.info("=" * 60)
        if failed > 0:
            sys.exit(1)
