# AI_Stock - A股数据分析研究框架

本项目基于 TuShare Pro 和 BaoStock，提供 A 股市场历史行情、实时行情和财务数据的获取与分析能力。

## 数据源说明

| 数据源 | 说明 |
|--------|------|
| **TuShare Pro** | 专业金融数据接口，需 token 认证，覆盖历史K线、股票列表、日线行情等 |
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

### 1. TuShare Token

编辑 `config/tushare_token.txt`，写入您的 TuShare Pro token（可在 [https://tushare.pro](https://tushare.pro) 注册获取）。

### 2. 项目配置

编辑 `config/config.yaml` 修改默认数据源、日期范围、股票池等参数。

## 使用示例

### 命令行

```bash
# 获取历史K线数据
python scripts/fetch_stock_data.py --symbol 000001 --start-date 2024-01-01

# 指定数据源 (tushare / baostock)
python scripts/fetch_stock_data.py --symbol 000001 --source tushare --period weekly

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
    # 获取历史K线 (默认使用 tushare)
    df = fetcher.fetch_stock_history(symbol="000001", start_date="2024-01-01")
    print(df.head())

    # 使用 BaoStock 获取历史K线
    df = fetcher.fetch_stock_history(symbol="000001", source="baostock")
    print(df.head())

    # 获取实时行情
    df = fetcher.fetch_realtime_quotes(symbols=["000001", "600036"])
    print(df.head())

    # 获取股票列表
    df = fetcher.fetch_stock_list()
    print(df.head())
```

## 项目结构

```
AI_Stock/
├── config/
│   ├── config.yaml                   # 项目配置
│   └── tushare_token.txt             # TuShare Pro token
├── lib/                              # 第三方数据源（git submodule）
│   └── baostock/
├── src/
│   ├── __init__.py
│   ├── data_fetcher.py               # 核心数据获取模块
│   └── utils.py                      # 通用工具函数
├── scripts/
│   ├── fetch_stock_data.py           # 获取历史K线数据
│   ├── fetch_realtime_data.py        # 获取实时行情
│   ├── fetch_financial_data.py       # 获取财务报表
│   └── update_stock_pool.py          # 更新股票池列表
├── tests/
│   ├── test_data_fetcher.py          # 数据获取模块单元测试
│   ├── test_utils.py                 # 工具函数单元测试
│   └── test_submodules.py            # 集成测试（需网络）
├── data/
│   ├── raw/                          # 原始数据
│   │   ├── daily/
│   │   ├── realtime/
│   │   └── financial/
│   └── processed/
├── notebooks/
└── output/
```

## 测试

```bash
# 运行单元测试
python -m pytest tests/test_utils.py tests/test_data_fetcher.py -v

# 运行集成测试（需网络和有效 TuShare token）
python -m pytest tests/test_submodules.py -v -s

# 运行全部测试
python -m pytest tests/ -v
```
