# AI_Stock - A股数据分析研究框架

本项目基于 AKShare 和 BaoStock，提供 A 股市场历史行情、实时行情和财务数据的获取与分析能力。

## 数据源说明

| 数据源 | 说明 |
|--------|------|
| **AKShare** | 全面的金融数据接口，覆盖实时行情、历史K线、资金流向等 |
| **BaoStock** | 免费证券数据平台，提供历史行情和完整财务数据 |

## 安装

```bash
# 克隆仓库（含 git submodule）
git clone --recursive <url>

# 安装依赖
pip install -r requirements.txt
```

如果已 clone 但未初始化 submodule：

```bash
git submodule update --init --recursive
```

## 配置

编辑 `config/config.yaml` 修改默认数据源、日期范围、股票池等参数。

## 使用示例

### 命令行

```bash
# 获取历史K线数据
python scripts/fetch_stock_data.py --symbol 000001 --start-date 2024-01-01

# 获取实时行情
python scripts/fetch_realtime_data.py --symbol 000001 600036

# 获取全部实时行情
python scripts/fetch_realtime_data.py --all

# 获取财务数据
python scripts/fetch_financial_data.py --symbol 000001 --report-type income --year 2024 --quarter 4

# 更新股票池
python scripts/update_stock_pool.py
```

### Python API

```python
from src.data_fetcher import DataFetcher

with DataFetcher() as fetcher:
    # 获取历史K线
    df = fetcher.fetch_stock_history(symbol="000001", start_date="2024-01-01")
    print(df.head())

    # 获取实时行情
    df = fetcher.fetch_realtime_quotes(symbols=["000001", "600036"])
    print(df.head())
```

## 项目结构

```
AI_Stock/
├── config/
│   └── config.yaml
├── lib/                               # 第三方数据源（git submodule）
│   ├── akshare/
│   └── baostock/
├── src/
│   ├── __init__.py
│   ├── data_fetcher.py                # 核心数据获取模块
│   └── utils.py                       # 通用工具函数
├── scripts/
│   ├── fetch_stock_data.py            # 获取历史K线数据
│   ├── fetch_realtime_data.py         # 获取实时行情
│   ├── fetch_financial_data.py        # 获取财务报表
│   └── update_stock_pool.py           # 更新股票池列表
├── data/
│   ├── raw/                           # 原始数据
│   │   ├── daily/
│   │   ├── realtime/
│   │   └── financial/
│   └── processed/
├── notebooks/
└── output/
```
