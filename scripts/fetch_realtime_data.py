import sys
import os
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetcher import DataFetcher


def main():
    parser = argparse.ArgumentParser(description="获取实时行情数据并保存")
    parser.add_argument("--symbol", nargs="+", default=None, help="股票代码列表，如 000001 600036")
    parser.add_argument("--all", action="store_true", help="获取全部A股实时行情")
    parser.add_argument("--output", default="data/raw/realtime", help="输出目录，默认 data/raw/realtime")
    args = parser.parse_args()

    if not args.all and args.symbol is None:
        parser.error("请指定 --symbol 或 --all 参数")

    try:
        if args.all:
            print("正在获取全部A股实时行情...")
            symbols = None
        else:
            symbols = args.symbol
            print(f"正在获取 {len(symbols)} 只股票的实时行情...")

        with DataFetcher() as fetcher:
            df = fetcher.fetch_realtime_quotes(symbols=symbols)
    except Exception as e:
        print(f"获取实时行情失败: {e}")
        sys.exit(1)

    if df is None or df.empty:
        print("未获取到实时行情数据")
        sys.exit(1)

    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), args.output)
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"realtime_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, encoding="utf-8-sig")
    print(f"成功获取 {len(df)} 条实时行情数据")
    print(f"数据已保存至: {filepath}")


if __name__ == "__main__":
    main()
