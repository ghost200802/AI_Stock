import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.data_fetcher import DataFetcher


def main():
    parser = argparse.ArgumentParser(description="获取财务报表数据（自动缓存到 MongoDB）")
    parser.add_argument("--symbol", required=True, help="股票代码")
    parser.add_argument("--report-type", default="income", choices=["income", "balance", "cashflow"], help="报表类型，默认 income")
    parser.add_argument("--year", type=int, default=None, help="年份，如 2024")
    parser.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], default=None, help="季度 1-4")
    parser.add_argument("--no-cache", action="store_true", help="禁用 MongoDB 缓存")
    args = parser.parse_args()

    report_type_names = {
        "income": "利润表",
        "balance": "资产负债表",
        "cashflow": "现金流量表",
    }

    try:
        print(f"正在获取 {args.symbol} 的{report_type_names[args.report_type]}数据...")
        use_cache = not args.no_cache
        with DataFetcher(use_cache=use_cache) as fetcher:
            if use_cache:
                end_date = f"{args.year or 2025}-{'{:02d}'.format(args.quarter * 3 if args.quarter else 12)}-31"
                df = fetcher.get_stock_data(
                    symbol=args.symbol,
                    data_type=args.report_type,
                    end_date=end_date,
                )
            else:
                df = fetcher.fetch_financial_data(
                    symbol=args.symbol,
                    report_type=args.report_type,
                    year=args.year,
                    quarter=args.quarter,
                )
    except Exception as e:
        print(f"获取财务数据失败: {e}")
        sys.exit(1)

    if df is None or df.empty:
        print("未获取到财务数据，请检查参数")
        sys.exit(1)

    print(f"成功获取 {len(df)} 条{report_type_names[args.report_type]}数据")
    if use_cache:
        print(f"数据已缓存到 MongoDB 集合: stock_{args.symbol}")


if __name__ == "__main__":
    main()
