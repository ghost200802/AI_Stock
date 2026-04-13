@echo off
chcp 65001 >nul 2>&1
setlocal

echo ========================================
echo   AI_Stock 项目环境初始化
echo ========================================
echo.

where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 未找到 Python，请先安装 Python 3.9+ 并添加到 PATH
    pause
    exit /b 1
)

echo [1/3] 升级 pip ...
python -m pip install --upgrade pip -q
if %errorlevel% neq 0 (
    echo [错误] pip 升级失败
    pause
    exit /b 1
)
echo       pip 升级完成
echo.

echo [2/3] 安装项目依赖 (requirements.txt) ...
python -m pip install -r requirements.txt -q
if %errorlevel% neq 0 (
    echo [错误] 依赖安装失败
    pause
    exit /b 1
)
echo       依赖安装完成
echo.

echo [3/3] 验证关键包 ...
python -c "import tushare; print('       tushare  ' + tushare.__version__)" 2>nul || echo       [警告] tushare 验证失败
python -c "import baostock; print('       baostock  OK')" 2>nul || echo       [警告] baostock 验证失败
python -c "import pandas; print('       pandas   ' + pandas.__version__)" 2>nul || echo       [警告] pandas 验证失败
python -c "import yaml; print('       pyyaml   OK')" 2>nul || echo       [警告] pyyaml 验证失败
python -c "import pytest; print('       pytest   ' + pytest.__version__)" 2>nul || echo       [警告] pytest 验证失败
echo.

echo ========================================
echo   初始化完成！
echo ========================================
echo.
echo 可用命令:
echo   python -m pytest tests/test_utils.py tests/test_data_fetcher.py -v   运行单元测试
echo   python -m pytest tests/test_submodules.py -v -s                       运行集成测试（需网络）
echo   python -m pytest tests/ -v                                            运行全部测试
echo   python scripts/fetch_stock_data.py --symbol 000001                    获取股票数据示例
echo.
pause
