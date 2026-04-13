import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetcher import DataFetcher


def main():
    parser = argparse.ArgumentParser(description="获取股票历史K线数据并保存为文件")
    parser.add_argument("--symbol", required=True, help="股票代码，如 000001")
    parser.add_argument("--start-date", default=None, help="开始日期，默认从配置文件读取")
    parser.add_argument("--end-date", default=None, help="结束日期，默认从配置文件读取")
    parser.add_argument("--source", default=None, choices=["tushare", "baostock"], help="数据源，默认从配置文件读取")
    parser.add_argument("--period", default="daily", choices=["daily", "weekly", "monthly"], help="K线周期，默认 daily")
    parser.add_argument("--adjust", default=None, help="复权方式，默认从配置文件读取")
    parser.add_argument("--output", default="data/raw/daily", help="输出目录，默认 data/raw/daily")
    args = parser.parse_args()

    try:
        print(f"正在获取 {args.symbol} 的历史K线数据...")
        with DataFetcher() as fetcher:
            df = fetcher.fetch_stock_history(
                symbol=args.symbol,
                start_date=args.start_date,
                end_date=args.end_date,
                source=args.source,
                period=args.period,
                adjust=args.adjust,
            )
    except Exception as e:
        print(f"获取数据失败: {e}")
        sys.exit(1)

    if df is None or df.empty:
        print("未获取到数据，请检查股票代码和日期范围")
        sys.exit(1)

    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), args.output)
    os.makedirs(output_dir, exist_ok=True)

    date_col = None
    for candidate in ("日期", "date", "trade_date"):
        if candidate in df.columns:
            date_col = candidate
            break
    if date_col:
        start_val = args.start_date or str(df[date_col].iloc[0]).replace("-", "")
        end_val = args.end_date or str(df[date_col].iloc[-1]).replace("-", "")
    else:
        start_val = args.start_date or str(df.index[0])
        end_val = args.end_date or str(df.index[-1])

    filename = f"{args.symbol}_{start_val}_{end_val}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, encoding="utf-8-sig")
    print(f"成功获取 {len(df)} 条数据")
    print(f"数据已保存至: {filepath}")


if __name__ == "__main__":
    main()
