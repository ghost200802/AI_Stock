import pandas as pd
import pytest


@pytest.fixture(scope="module")
def bs_session():
    import baostock as bs
    lg = bs.login()
    assert lg.error_code == "0", f"BaoStock 登录失败: {lg.error_msg}"
    yield bs
    bs.logout()


class TestBaoStockPackage:
    def test_import(self):
        import baostock as bs
        assert hasattr(bs, "login")
        assert hasattr(bs, "logout")

    def test_login_logout(self):
        import baostock as bs
        lg = bs.login()
        assert lg.error_code == "0", f"BaoStock 登录失败: {lg.error_msg}"
        bs.logout()

    def test_query_history_k_data(self, bs_session):
        rs = bs_session.query_history_k_data_plus(
            "sh.601398",
            "date,open,high,low,close,volume",
            start_date="2025-04-01",
            end_date="2025-04-30",
            frequency="d",
            adjustflag="3",
        )
        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())
        df = pd.DataFrame(rows, columns=rs.fields)
        assert not df.empty, "sh.601398 日K线数据为空
        assert "date" in df.columns
        assert "close" in df.columns

    def test_query_profit_data(self, bs_session):
        rs = bs_session.query_profit_data(code="sh.601398", year=2024, quarter=3)
        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())
        df = pd.DataFrame(rows, columns=rs.fields)
        assert not df.empty, "利润表数据为空
        expected_fields = ["code", "pubDate", "statDate"]
        for field in expected_fields:
            assert field in rs.fields, f"利润表缺少字段 {field}"


class TestTusharePackage:
    def test_import(self):
        import tushare as ts
        assert hasattr(ts, "pro_api")

    def test_pro_api_init(self, tmp_path):
        import tushare as ts
        token_file = tmp_path / "test_token.txt"
        token_file.write_text("test_token", encoding="utf-8")
        token = token_file.read_text(encoding="utf-8").strip()
        pro = ts.pro_api(token)
        assert pro is not None

    def test_stock_basic(self):
        import tushare as ts
        from lib.utils import load_config
        config = load_config()
        token_file = config.get("data_source", {}).get("tushare", {}).get("token_file", "config/tushare_token.txt")
        from lib.utils import get_project_root
        token_path = get_project_root() / token_file
        if not token_path.exists():
            pytest.skip("TuShare token 文件不存在，跳过集成测试")
        token = token_path.read_text(encoding="utf-8").strip()
        if token == "YOUR_TUSHARE_TOKEN_HERE":
            pytest.skip("TuShare token 未配置，跳过集成测试")
        pro = ts.pro_api(token)
        df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol,name")
        assert df is not None and not df.empty, "股票列表为空"
        assert "symbol" in df.columns, "缺少'symbol'列
        assert "name" in df.columns, "缺少'name'列

    def test_daily(self):
        import tushare as ts
        from lib.utils import load_config, get_project_root
        config = load_config()
        token_file = config.get("data_source", {}).get("tushare", {}).get("token_file", "config/tushare_token.txt")
        token_path = get_project_root() / token_file
        if not token_path.exists():
            pytest.skip("TuShare token 文件不存在，跳过集成测试")
        token = token_path.read_text(encoding="utf-8").strip()
        if token == "YOUR_TUSHARE_TOKEN_HERE":
            pytest.skip("TuShare token 未配置，跳过集成测试")
        pro = ts.pro_api(token)
        df = pro.daily(ts_code="000001.SZ", start_date="20250101", end_date="20250110")
        assert df is not None and not df.empty, "000001.SZ 日线数据为空"
        assert "trade_date" in df.columns, "缺少'trade_date'列
        assert "close" in df.columns, "缺少'close'列
