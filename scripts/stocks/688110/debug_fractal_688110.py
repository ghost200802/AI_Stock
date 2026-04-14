import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from lib.data_fetcher import DataFetcher
from modules.chanlun import compute_bi, IncludeProcessor, FractalDetector, FractalType
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

from modules.chanlun import BiGenerator
bi_gen = BiGenerator()
bis = bi_gen.generate(fractals, processed_klines)

print("=" * 100)
print("修改后的分型检测结果")
print("=" * 100)
print(f"分型总数: {len(fractals)}")
print(f"笔总数: {len(bis)}")
print()

print("全部分型列表:")
for f in fractals:
    td = f.trade_date
    td_str = td.strftime('%Y-%m-%d') if hasattr(td, 'strftime') else str(td)[:10]
    marker = " <<<" if td_str in ["2025-09-03", "2025-09-18"] else ""
    print(f"  index={f.index}, date={td_str}, type={f.fractal_type.value}, high={f.high:.2f}, low={f.low:.2f}{marker}")

print()
print("=" * 100)
print("笔列表:")
print("=" * 100)
for b in bis:
    sd = b.start_date
    ed = b.end_date
    sd_str = sd.strftime('%Y-%m-%d') if hasattr(sd, 'strftime') else str(sd)[:10]
    ed_str = ed.strftime('%Y-%m-%d') if hasattr(ed, 'strftime') else str(ed)[:10]
    print(f"  {b.direction.value:4s}: {sd_str}({b.start_price:.2f}) -> {ed_str}({b.end_price:.2f}) {'[confirmed]' if b.confirmed else '[unconfirmed]'}")

print()
print("=" * 100)
print("验证 2025-09-03 和 2025-09-18 是否在分型中")
print("=" * 100)
found_0903 = any(
    (f.trade_date.strftime('%Y-%m-%d') if hasattr(f.trade_date, 'strftime') else str(f.trade_date)[:10]) == "2025-09-03"
    for f in fractals
)
found_0918 = any(
    (f.trade_date.strftime('%Y-%m-%d') if hasattr(f.trade_date, 'strftime') else str(f.trade_date)[:10]) == "2025-09-18"
    for f in fractals
)
print(f"2025-09-03 顶分型: {'存在 ✓' if found_0903 else '不存在 ✗'}")
print(f"2025-09-18 顶分型: {'存在 ✓' if found_0918 else '不存在 ✗'}")

print()
print("=" * 100)
print("验证: 检查包含处理后的K线在 2025-09-03 和 2025-09-18 附近")
print("=" * 100)
for i, kline in enumerate(processed_klines):
    td = kline.trade_date
    td_str = td.strftime('%Y-%m-%d') if hasattr(td, 'strftime') else str(td)[:10]
    if "2025-08-28" <= td_str <= "2025-09-26":
        prev_k = processed_klines[i-1] if i > 0 else None
        next_k = processed_klines[i+1] if i < len(processed_klines)-1 else None
        is_top = FractalDetector._is_top_fractal(prev_k, kline, next_k) if prev_k and next_k else None
        is_bottom = FractalDetector._is_bottom_fractal(prev_k, kline, next_k) if prev_k and next_k else None
        marker = ""
        if is_top:
            marker += " [TOP]"
        if is_bottom:
            marker += " [BOTTOM]"
        print(f"  idx={i}, date={td_str}, O={kline.open:.2f}, H={kline.high:.2f}, L={kline.low:.2f}, C={kline.close:.2f}{marker}")
