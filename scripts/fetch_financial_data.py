import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetcher import DataFetcher


def main():
    parser = argparse.ArgumentParser(description="获取财务报表数据")
    parser.add_argument("--symbol", required=True, help="股票代码")
    parser.add_argument("--report-type", default="income", choices=["income", "balance", "cashflow"], help="报表类型，默认 income")
    parser.add_argument("--year", type=int, default=None, help="年份，如 2024")
    parser.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], default=None, help="季度 1-4")
    parser.add_argument("--output", default="data/raw/financial", help="输出目录，默认 data/raw/financial")
    args = parser.parse_args()

    report_type_names = {
        "income": "利润表",
        "balance": "资产负债表",
        "cashflow": "现金流量表",
    }

    try:
        print(f"正在获取 {args.symbol} 的{report_type_names[args.report_type]}数据...")
        with DataFetcher() as fetcher:
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

    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), args.output)
    os.makedirs(output_dir, exist_ok=True)

    if args.year and args.quarter:
        filename = f"{args.symbol}_{args.report_type}_{args.year}Q{args.quarter}.csv"
    else:
        filename = f"{args.symbol}_{args.report_type}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, encoding="utf-8-sig")
    print(f"成功获取 {len(df)} 条{report_type_names[args.report_type]}数据")
    print(f"数据已保存至: {filepath}")


if __name__ == "__main__":
    main()
