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

MIN_GAP = 4

def check_price_relation(prev, curr):
    top = prev if prev.fractal_type == FractalType.TOP else curr
    bottom = curr if curr.fractal_type == FractalType.BOTTOM else prev
    return top.high > bottom.high and bottom.low < top.low

def fmt(f):
    td = f.trade_date
    return td.strftime('%Y-%m-%d') if hasattr(td, 'strftime') else str(td)[:10]

result = [fractals[0]]
print(f"初始: result[0] = {fmt(fractals[0])} {fractals[0].fractal_type.value} idx={fractals[0].index}")

for i in range(1, len(fractals)):
    f = fractals[i]
    last = result[-1]

    if f.fractal_type == last.fractal_type:
        if f.fractal_type == FractalType.TOP:
            if f.high > last.high:
                result[-1] = f
                print(f"  [{i:3d}] {fmt(f)} TOP(h={f.high:.2f}) 同类型替换 {fmt(last)}(h={last.high:.2f})")
            else:
                print(f"  [{i:3d}] {fmt(f)} TOP(h={f.high:.2f}) 同类型跳过 {fmt(last)}(h={last.high:.2f})")
        elif f.fractal_type == FractalType.BOTTOM:
            if f.low < last.low:
                result[-1] = f
                print(f"  [{i:3d}] {fmt(f)} BOT(l={f.low:.2f}) 同类型替换 {fmt(last)}(l={last.low:.2f})")
            else:
                print(f"  [{i:3d}] {fmt(f)} BOT(l={f.low:.2f}) 同类型跳过 {fmt(last)}(l={last.low:.2f})")
        continue

    gap = f.index - last.index
    price_ok = check_price_relation(last, f)

    if gap >= MIN_GAP and price_ok:
        result.append(f)
        print(f"  [{i:3d}] {fmt(f)} {f.fractal_type.value:6s} -> 加入 (gap={gap}, price_ok={price_ok}) result len={len(result)}")
    else:
        action = "跳过"
        if f.fractal_type == FractalType.TOP and f.high > last.high:
            old_last_info = f"{fmt(last)} {last.fractal_type.value}(h={last.high:.2f})"
            result[-1] = f
            removed = ""
            if len(result) >= 2 and result[-2].fractal_type == FractalType.TOP:
                if f.high > result[-2].high:
                    removed = f", 移除 {fmt(result[-2])} TOP(h={result[-2].high:.2f})"
                    result.pop(-2)
                else:
                    removed = f", 恢复(被 {fmt(result[-2])} TOP(h={result[-2].high:.2f}) 顶回)"
                    result.pop(-1)
            action = f"替换 {old_last_info}{removed}"
        elif f.fractal_type == FractalType.BOTTOM and f.low < last.low:
            old_last_info = f"{fmt(last)} {last.fractal_type.value}(l={last.low:.2f})"
            result[-1] = f
            removed = ""
            if len(result) >= 2 and result[-2].fractal_type == FractalType.BOTTOM:
                if f.low < result[-2].low:
                    removed = f", 移除 {fmt(result[-2])} BOT(l={result[-2].low:.2f})"
                    result.pop(-2)
                else:
                    removed = f", 恢复(被 {fmt(result[-2])} BOT(l={result[-2].low:.2f}) 顶回)"
                    result.pop(-1)
            action = f"替换 {old_last_info}{removed}"
        print(f"  [{i:3d}] {fmt(f)} {f.fractal_type.value:6s} -> {action} (gap={gap}, price_ok={price_ok}) result len={len(result)}")

print()
print("=" * 100)
print(f"过滤后分型数: {len(result)}")
for i, f in enumerate(result):
    print(f"  [{i:3d}] idx={f.index:4d}, date={fmt(f)}, type={f.fractal_type.value:6s}, high={f.high:.2f}, low={f.low:.2f}")

print()
print("过滤后相邻分型间距:")
for i in range(1, len(result)):
    gap = result[i].index - result[i-1].index
    price_ok = check_price_relation(result[i-1], result[i])
    ok = "OK" if gap >= MIN_GAP and price_ok else f"FAIL(gap={gap},price={price_ok})"
    print(f"  [{i-1}]->[{i}] {fmt(result[i-1])} {result[i-1].fractal_type.value} <-> {fmt(result[i])} {result[i].fractal_type.value} gap={gap} {ok}")
