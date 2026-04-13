import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from lib.data_fetcher import DataFetcher
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

close = pd.to_numeric(df["close"], errors="coerce").dropna()
first_price = close.iloc[0]
last_price = close.iloc[-1]
max_price = close.max()
min_price = close.min()
total_return = (last_price - first_price) / first_price * 100

print(f"数据条数: {len(df)}")
print(f"日期范围: {df['trade_date'].iloc[0]} ~ {df['trade_date'].iloc[-1]}")
print(f"起始价: {first_price:.2f}")
print(f"最新价: {last_price:.2f}")
print(f"最高价: {max_price:.2f}")
print(f"最低价: {min_price:.2f}")
print(f"区间涨跌幅: {total_return:.2f}%")

if len(close) >= 5:
    ma5 = close.rolling(5).mean().iloc[-1]
    print(f"MA5: {ma5:.2f} (收盘价 {'>' if last_price > ma5 else '<='})")

if len(close) >= 20:
    ma20 = close.rolling(20).mean().iloc[-1]
    print(f"MA20: {ma20:.2f} (收盘价 {'>' if last_price > ma20 else '<='})")

if len(close) >= 60:
    ma60 = close.rolling(60).mean().iloc[-1]
    print(f"MA60: {ma60:.2f} (收盘价 {'>' if last_price > ma60 else '<='})")

if len(close) >= 120:
    ma120 = close.rolling(120).mean().iloc[-1]
    print(f"MA120: {ma120:.2f} (收盘价 {'>' if last_price > ma120 else '<='})")

if len(close) >= 250:
    ma250 = close.rolling(250).mean().iloc[-1]
    print(f"MA250: {ma250:.2f} (收盘价 {'>' if last_price > ma250 else '<='})")

if total_return > 10:
    trend = "走强（涨幅较大）"
elif total_return > 0:
    trend = "小幅上涨"
elif total_return > -10:
    trend = "小幅下跌"
else:
    trend = "走弱（跌幅较大）"
print(f"综合判断: {trend}")
