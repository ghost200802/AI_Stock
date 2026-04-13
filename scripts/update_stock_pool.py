import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_fetcher import DataFetcher


def main():
    parser = argparse.ArgumentParser(description="更新A股股票列表到本地")
    parser.add_argument("--output", default="data/stock_pool.csv", help="输出文件路径，默认 data/stock_pool.csv")
    args = parser.parse_args()

    try:
        print("正在获取A股股票列表...")
        with DataFetcher() as fetcher:
            df = fetcher.fetch_stock_list()
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        sys.exit(1)

    if df is None or df.empty:
        print("未获取到股票列表数据")
        sys.exit(1)

    output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), args.output)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, encoding="utf-8-sig", index=False)
    print(f"成功获取 {len(df)} 只股票")
    print(f"股票列表已保存至: {output_path}")


if __name__ == "__main__":
    main()
