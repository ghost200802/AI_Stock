import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
from modules.chanlun import compute_bi, IncludeProcessor, FractalDetector, FractalType, BiGenerator

def _make_kline_df(data):
    rows = []
    for d in data:
        rows.append({"trade_date": d[0], "open": d[1], "high": d[2], "low": d[3], "close": d[4], "volume": d[5] if len(d) > 5 else 100})
    return pd.DataFrame(rows)

kline_data = [
    ("2024-01-01", 10, 12, 9, 11, 100),
    ("2024-01-02", 11, 14, 10, 13, 100),
    ("2024-01-03", 13, 16, 12, 15, 100),
    ("2024-01-04", 15, 18, 14, 17, 100),
    ("2024-01-05", 17, 20, 16, 19, 100),
    ("2024-01-06", 19, 18, 15, 16, 100),
    ("2024-01-07", 16, 15, 12, 13, 100),
    ("2024-01-08", 13, 12, 9, 10, 100),
    ("2024-01-09", 10, 9, 6, 7, 100),
    ("2024-01-10", 7, 6, 3, 4, 100),
    ("2024-01-11", 4, 5, 2, 3, 100),
    ("2024-01-12", 3, 4, 1, 2, 100),
    ("2024-01-13", 2, 3, 0.5, 1, 100),
    ("2024-01-14", 1, 2, 0.5, 1.5, 100),
    ("2024-01-15", 1.5, 3, 1, 2, 100),
    ("2024-01-16", 2, 5, 1.5, 4, 100),
    ("2024-01-17", 4, 8, 3, 7, 100),
    ("2024-01-18", 7, 12, 6, 11, 100),
    ("2024-01-19", 11, 15, 10, 14, 100),
    ("2024-01-20", 14, 18, 13, 17, 100),
]
df = _make_kline_df(kline_data)

ip = IncludeProcessor()
pk = ip.process(df)
print(f"K线: {len(df)} -> 包含处理: {len(pk)}")
for i, kl in enumerate(pk):
    print(f"  [{i:3d}] {kl.trade_date} h={kl.high:.1f} l={kl.low:.1f}")

fd = FractalDetector()
fracs = fd.detect(pk)
print(f"\n分型数: {len(fracs)}")
for i, f in enumerate(fracs):
    print(f"  [{i:3d}] idx={f.index} {f.trade_date} {f.fractal_type.value} h={f.high:.1f} l={f.low:.1f}")

bg = BiGenerator()
bis = bg.generate(fracs, pk)
print(f"\n笔数: {len(bis)}")
for i, b in enumerate(bis):
    print(f"  [{i}] {b.direction.value} {b.start_date}->{b.end_date} {b.start_price:.1f}->{b.end_price:.1f}")

result = compute_bi(df)
print(f"\ncompute_bi: {len(result)} rows")
print(result.to_string())
