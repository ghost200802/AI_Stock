import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.data_fetcher import DataFetcher


def main():
    parser = argparse.ArgumentParser(description="获取股票历史K线数据（自动缓存到 MongoDB）")
    parser.add_argument("--symbol", required=True, help="股票代码，如 000001")
    parser.add_argument("--start-date", default=None, help="开始日期，默认从配置文件读取")
    parser.add_argument("--end-date", default=None, help="结束日期，默认从配置文件读取")
    parser.add_argument("--source", default=None, choices=["tushare", "baostock"], help="数据源，默认从配置文件读取")
    parser.add_argument("--period", default="daily", choices=["daily", "weekly", "monthly"], help="K线周期，默认 daily")
    parser.add_argument("--no-cache", action="store_true", help="禁用 MongoDB 缓存，直接从 API 获取")
    args = parser.parse_args()

    try:
        print(f"正在获取 {args.symbol} 的历史K线数据（{args.period}）...")
        use_cache = not args.no_cache
        with DataFetcher(use_cache=use_cache) as fetcher:
            df = fetcher.get_stock_data(
                symbol=args.symbol,
                data_type=args.period,
                start_date=args.start_date,
                end_date=args.end_date,
                source=args.source,
            )
    except Exception as e:
        print(f"获取数据失败: {e}")
        sys.exit(1)

    if df is None or df.empty:
        print("未获取到数据，请检查股票代码和日期范围")
        sys.exit(1)

    print(f"成功获取 {len(df)} 条数据")
    if use_cache:
        print(f"数据已缓存到 MongoDB 集合: stock_{args.symbol}")
    else:
        print("未使用缓存（--no-cache 模式）")


if __name__ == "__main__":
    main()
