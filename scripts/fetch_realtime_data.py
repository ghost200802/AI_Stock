import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.data_fetcher import DataFetcher


def main():
    parser = argparse.ArgumentParser(description="获取实时行情数据（自动缓存到 MongoDB）")
    parser.add_argument("--symbol", nargs="+", default=None, help="股票代码列表，如 000001 600036")
    parser.add_argument("--all", action="store_true", help="获取全部A股实时行情")
    parser.add_argument("--no-cache", action="store_true", help="禁用 MongoDB 缓存")
    args = parser.parse_args()

    if not args.all and args.symbol is None:
        parser.error("请指定 --symbol 或 --all 参数")

    try:
        use_cache = not args.no_cache
        if args.all:
            print("正在获取全部A股实时行情...")
            symbols = None
        else:
            symbols = args.symbol
            print(f"正在获取 {len(symbols)} 只股票的实时行情...")

        with DataFetcher(use_cache=use_cache) as fetcher:
            df = fetcher.fetch_realtime_quotes(symbols=symbols)
    except Exception as e:
        print(f"获取实时行情失败: {e}")
        sys.exit(1)

    if df is None or df.empty:
        print("未获取到实时行情数据")
        sys.exit(1)

    print(f"成功获取 {len(df)} 条实时行情数据")
    if use_cache and symbols:
        print(f"数据已缓存到 MongoDB")
    elif use_cache:
        print("全市场行情已缓存到各股票 MongoDB 集合")


if __name__ == "__main__":
    main()
