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

def fmt(f):
    td = f.trade_date
    return td.strftime('%Y-%m-%d') if hasattr(td, 'strftime') else str(td)[:10]

MIN_GAP = 4

def check_price_relation(prev, curr):
    top = prev if prev.fractal_type == FractalType.TOP else curr
    bottom = curr if curr.fractal_type == FractalType.BOTTOM else prev
    return top.high > bottom.high and bottom.low < top.low

result = [resolved[0]]

for i in range(1, len(resolved)):
    f = resolved[i]
    last = result[-1]

    if f.fractal_type == last.fractal_type:
        old = fmt(last)
        if f.fractal_type == FractalType.TOP:
            if f.high > last.high:
                result[-1] = f
                print(f"  [{i:3d}] {fmt(f)} TOP 同类型替换 {old}(h={last.high:.2f})")
        elif f.fractal_type == FractalType.BOTTOM:
            if f.low < last.low:
                result[-1] = f
                print(f"  [{i:3d}] {fmt(f)} BOT 同类型替换 {old}(l={last.low:.2f})")
        continue

    gap = f.index - last.index
    price_ok = check_price_relation(last, f)

    if gap >= MIN_GAP and price_ok:
        result.append(f)
        print(f"  [{i:3d}] {fmt(f)} {f.fractal_type.value:6s} -> 加入 (gap={gap}, price_ok={price_ok}) len={len(result)}")
        continue

    print(f"  [{i:3d}] {fmt(f)} {f.fractal_type.value:6s} -> 不满足 (gap={gap}, price_ok={price_ok}) last={fmt(last)} {last.fractal_type.value} len={len(result)}")

    if len(result) >= 2 and result[-2].fractal_type == f.fractal_type:
        same_type_prev = result[-2]
        is_more_extreme = False
        if f.fractal_type == FractalType.TOP and f.high > same_type_prev.high:
            is_more_extreme = True
        elif f.fractal_type == FractalType.BOTTOM and f.low < same_type_prev.low:
            is_more_extreme = True

        if is_more_extreme:
            gap_to_prev = f.index - same_type_prev.index
            if gap_to_prev >= MIN_GAP:
                removed = result.pop(-1)
                result[-1] = f
                print(f"         -> 更新同类型: 移除 {fmt(removed)} {removed.fractal_type.value}, 更新 {fmt(same_type_prev)} -> {fmt(f)} (gap_to_prev={gap_to_prev}) len={len(result)}")
            else:
                print(f"         -> 同类型更极端但间距不够 (gap_to_prev={gap_to_prev})")
        else:
            print(f"         -> 同类型但不够极端 ({fmt(same_type_prev)} {same_type_prev.fractal_type.value})")
    else:
        print(f"         -> 无同类型前驱可更新")

print()
print("=" * 100)
print(f"过滤后分型数: {len(result)}")
for i, f in enumerate(result):
    print(f"  [{i:3d}] idx={f.index:4d}, date={fmt(f)}, type={f.fractal_type.value:6s}, high={f.high:.2f}, low={f.low:.2f}")
