import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from lib.data_fetcher import DataFetcher
from modules.chanlun import IncludeProcessor, FractalDetector, FractalType, BiGenerator, BiDirection
import pandas as pd

SYMBOL = "688110"
START_DATE = "2020-01-01"
END_DATE = "2026-04-13"

fetcher = DataFetcher(use_cache=True)
df = fetcher.get_stock_data(SYMBOL, "daily", START_DATE, END_DATE, source="tushare")
fetcher.close()

df["trade_date"] = pd.to_datetime(df["trade_date"])

include_processor = IncludeProcessor()
processed_klines = include_processor.process(df)

fractal_detector = FractalDetector()
fractals = fractal_detector.detect(processed_klines)

bi_gen = BiGenerator()
merged = BiGenerator._merge_consecutive_same_type(fractals)
resolved = BiGenerator._resolve_consecutive_same_type(merged)
valid = bi_gen._filter_by_distance_and_relation(resolved)

def fmt(f):
    td = f.trade_date
    return td.strftime('%Y-%m-%d') if hasattr(td, 'strftime') else str(td)[:10]

print("过滤后分型与原始分型对比:")
print("=" * 100)
print(f"原始分型(经合并后): {len(resolved)} 个")
print(f"过滤后分型: {len(valid)} 个")
print(f"被过滤掉: {len(resolved) - len(valid)} 个")
print()

for i, f in enumerate(resolved):
    in_valid = f in valid
    marker = "✓" if in_valid else "✗ 被过滤"
    print(f"  [{i:3d}] idx={f.index:4d}, date={fmt(f)}, type={f.fractal_type.value:6s}, high={f.high:.2f}, low={f.low:.2f}  {marker}")

print()
print("=" * 100)
print("被过滤的分型详细分析:")
print("=" * 100)
valid_set = set(id(f) for f in valid)
for i, f in enumerate(resolved):
    if id(f) not in valid_set:
        print(f"  [{i:3d}] idx={f.index:4d}, date={fmt(f)}, type={f.fractal_type.value:6s}, high={f.high:.2f}, low={f.low:.2f}")
