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

if df.empty:
    print("no data")
    sys.exit(1)

df["trade_date"] = pd.to_datetime(df["trade_date"])

include_processor = IncludeProcessor()
processed_klines = include_processor.process(df)

fractal_detector = FractalDetector()
fractals = fractal_detector.detect(processed_klines)

print(f"原始K线数: {len(df)}, 包含处理后: {len(processed_klines)}, 分型数: {len(fractals)}")

bi_gen = BiGenerator()
bis = bi_gen.generate(fractals, processed_klines)

print(f"笔总数: {len(bis)}")
print()
print("=" * 100)
print("全部笔列表:")
print("=" * 100)
for i, b in enumerate(bis):
    sd_str = b.start_date.strftime('%Y-%m-%d') if hasattr(b.start_date, 'strftime') else str(b.start_date)[:10]
    ed_str = b.end_date.strftime('%Y-%m-%d') if hasattr(b.end_date, 'strftime') else str(b.end_date)[:10]
    print(f"  [{i:3d}] {b.direction.value:4s}: {sd_str}({b.start_price:.2f}) -> {ed_str}({b.end_price:.2f}) {'[confirmed]' if b.confirmed else '[unconfirmed]'}")

print()
direction_issue = False
for i in range(1, len(bis)):
    if bis[i].direction == bis[i-1].direction:
        direction_issue = True
        print(f"  *** 方向相同: [{i-1}] {bis[i-1].direction.value} <-> [{i}] {bis[i].direction.value}")
if not direction_issue:
    print("笔方向严格交替 ✓")

print()
print("=" * 100)
print("验证过滤后的分型间距和价格关系:")
print("=" * 100)

MIN_GAP = 4

def check_price_relation(prev, curr):
    top = prev if prev.fractal_type == FractalType.TOP else curr
    bottom = curr if curr.fractal_type == FractalType.BOTTOM else prev
    return top.high > bottom.high and bottom.low < top.low

def fmt(f):
    td = f.trade_date
    return td.strftime('%Y-%m-%d') if hasattr(td, 'strftime') else str(td)[:10]

merged = BiGenerator._merge_consecutive_same_type(fractals)
resolved = BiGenerator._resolve_consecutive_same_type(merged)
valid = bi_gen._filter_by_distance_and_relation(resolved)

for i, f in enumerate(valid):
    print(f"  [{i:3d}] idx={f.index:4d}, date={fmt(f)}, type={f.fractal_type.value:6s}, high={f.high:.2f}, low={f.low:.2f}")

print()
gap_ok = True
for i in range(1, len(valid)):
    gap = valid[i].index - valid[i-1].index
    price_ok = check_price_relation(valid[i-1], valid[i])
    if gap < MIN_GAP or not price_ok:
        gap_ok = False
        print(f"  FAIL [{i-1}]->[{i}] {fmt(valid[i-1])} {valid[i-1].fractal_type.value} <-> {fmt(valid[i])} {valid[i].fractal_type.value} gap={gap} price_ok={price_ok}")

if gap_ok:
    print("所有过滤后分型间距>=4且价格关系满足 ✓")
else:
    print("存在不满足间距或价格关系的分型对！")
