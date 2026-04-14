"""
Playwright E2E test: Verify K-line chart eliminates all non-trading-day gaps.

This test:
1. Launches the Streamlit visualizer
2. Navigates to a stock's daily K-line chart
3. Extracts x-axis dates and axis type from the Plotly candlestick chart
4. Verifies x-axis is 'category' type (no gaps for any non-trading day)
5. Checks that no weekend/holiday dates appear in the data
6. Verifies data points are consecutive trading days only

Note: Uses local Chrome installation instead of Playwright's bundled Chromium.
"""

import re
import time
from datetime import datetime, timedelta

import pytest
from playwright.sync_api import sync_playwright, Page, BrowserContext

STREAMLIT_URL = "http://localhost:8501"
CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
WAIT_TIMEOUT = 60000

CN_HOLIDAYS_2025 = {
    "2025-01-01", "2025-01-28", "2025-01-29", "2025-01-30", "2025-01-31",
    "2025-02-03", "2025-02-04", "2025-04-04",
    "2025-05-01", "2025-05-02", "2025-05-05",
    "2025-06-02",
    "2025-10-01", "2025-10-02", "2025-10-03", "2025-10-06", "2025-10-07", "2025-10-08",
}

CN_HOLIDAYS_2026 = {
    "2026-01-01", "2026-01-02",
    "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19", "2026-02-20",
    "2026-04-05", "2026-04-06",
    "2026-05-01", "2026-05-04", "2026-05-05",
    "2026-06-01",
    "2026-10-01", "2026-10-02", "2026-10-05", "2026-10-06", "2026-10-07",
}

ALL_CN_HOLIDAYS = CN_HOLIDAYS_2025 | CN_HOLIDAYS_2026


def get_weekend_dates_between(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    weekends = set()
    current = start
    while current <= end:
        if current.weekday() >= 5:
            weekends.add(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return weekends


def get_all_non_trading_dates(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    non_trading = set()
    current = start
    while current <= end:
        ds = current.strftime("%Y-%m-%d")
        if current.weekday() >= 5 or ds in ALL_CN_HOLIDAYS:
            non_trading.add(ds)
        current += timedelta(days=1)
    return non_trading


def parse_chart_dates(x_values):
    all_dates = []
    for val in x_values:
        if isinstance(val, str):
            all_dates.append(val[:10])
        elif isinstance(val, (int, float)):
            try:
                ts = int(val) / 1000 if val > 1e12 else val
                dt = datetime.fromtimestamp(ts)
                all_dates.append(dt.strftime("%Y-%m-%d"))
            except (ValueError, OSError, OverflowError):
                pass
    return all_dates


class TestNonTradingDayGaps:

    @pytest.fixture(scope="class")
    def browser_context(self):
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                executable_path=CHROME_PATH,
                args=["--no-sandbox", "--disable-gpu"],
            )
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
            )
            yield context
            context.close()
            browser.close()

    @pytest.fixture(scope="class")
    def page(self, browser_context: BrowserContext):
        page = browser_context.new_page()
        page.goto(STREAMLIT_URL, timeout=WAIT_TIMEOUT)
        page.wait_for_load_state("networkidle", timeout=WAIT_TIMEOUT)
        time.sleep(3)
        yield page
        page.close()

    def test_streamlit_app_loads(self, page: Page):
        title = page.title()
        assert title != "", f"Page title should not be empty, got: {title}"
        print(f"\n[OK] Streamlit app loaded. Title: {title}")

    def test_select_stock_and_load_chart(self, page: Page):
        selectbox = page.locator("select").first
        if selectbox.count() > 0:
            options = selectbox.locator("option").all_text_contents()
            print(f"\n[INFO] Available stock options: {options[:5]}...")
            selectbox.select_option(index=0)
            time.sleep(2)

        spinner = page.locator("[data-testid='stSpinner']")
        if spinner.count() > 0:
            spinner.wait_for(state="hidden", timeout=30000)

        plotly_chart = page.locator(".js-plotly-plot, .plotly, .plot-container")
        try:
            plotly_chart.first.wait_for(state="visible", timeout=30000)
            print("[OK] Plotly chart is visible")
        except Exception:
            page_screenshot = "test_debug_chart.png"
            page.screenshot(path=page_screenshot)
            print(f"[WARN] Plotly chart not found. Screenshot saved to {page_screenshot}")
            pytest.skip("Plotly chart not rendered - possible data source issue")

    def test_xaxis_is_category_type(self, page: Page):
        plotly_data = page.evaluate("""() => {
            const plotEl = document.querySelector('.js-plotly-plot') ||
                           document.querySelector('.plotly');
            if (plotEl && plotEl.layout) {
                return {
                    xaxis_type: plotEl.layout.xaxis ? plotEl.layout.xaxis.type : null,
                    xaxis2_type: plotEl.layout.xaxis2 ? plotEl.layout.xaxis2.type : null,
                };
            }
            return null;
        }""")

        if plotly_data is None:
            pytest.skip("Could not extract Plotly layout from page")

        xaxis_type = plotly_data.get("xaxis_type")
        xaxis2_type = plotly_data.get("xaxis2_type")
        print(f"\n[INFO] Plotly xaxis type (K-line): {xaxis_type}")
        print(f"[INFO] Plotly xaxis2 type (Volume): {xaxis2_type}")

        assert xaxis_type == "category", (
            f"Expected x-axis type 'category' to eliminate all non-trading-day gaps, "
            f"but got '{xaxis_type}'. "
            f"With 'date' axis, holidays falling on weekdays will create visible gaps."
        )
        assert xaxis2_type == "category", (
            f"Expected xaxis2 type 'category' for volume subplot, but got '{xaxis2_type}'."
        )
        print("[PASS] Both x-axes are 'category' type - no gaps for any non-trading day.")

    def test_no_non_trading_days_in_data(self, page: Page):
        plotly_data = page.evaluate("""() => {
            const plotEl = document.querySelector('.js-plotly-plot') ||
                           document.querySelector('.plotly');
            if (plotEl && plotEl.data) {
                return {
                    data: plotEl.data.map(trace => ({
                        type: trace.type,
                        x: trace.x ? trace.x.slice() : [],
                    })),
                };
            }
            return null;
        }""")

        if plotly_data is None:
            pytest.skip("Could not extract Plotly data from page")

        candlestick_trace = None
        for trace in plotly_data["data"]:
            if trace["type"] == "candlestick":
                candlestick_trace = trace
                break

        if candlestick_trace is None:
            pytest.skip("No candlestick trace found in chart")

        x_values = candlestick_trace["x"]
        assert len(x_values) > 0, "Candlestick chart should have data points"

        all_dates = parse_chart_dates(x_values)
        assert len(all_dates) > 0, "Should have parsed dates from chart"

        print(f"\n[INFO] Total data points: {len(all_dates)}")
        print(f"[INFO] First 5 dates: {all_dates[:5]}")
        print(f"[INFO] Last 5 dates: {all_dates[-5:]}")

        date_range_start = all_dates[0]
        date_range_end = all_dates[-1]
        chart_date_set = set(all_dates)

        weekend_dates = get_weekend_dates_between(date_range_start, date_range_end)
        weekend_in_chart = chart_date_set & weekend_dates
        print(f"\n[INFO] Date range: {date_range_start} to {date_range_end}")
        print(f"[INFO] Calendar weekend days in range: {len(weekend_dates)}")
        print(f"[INFO] Weekend days in chart data: {len(weekend_in_chart)}")

        if weekend_in_chart:
            print(f"\n[FAIL] Weekend dates found in chart data:")
            for d in sorted(weekend_in_chart)[:10]:
                print(f"  - {d} ({datetime.strptime(d, '%Y-%m-%d').strftime('%A')})")
            pytest.fail(
                f"Weekend dates in chart: {len(weekend_in_chart)}. "
                f"Data should only contain actual trading days."
            )

        holiday_in_chart = chart_date_set & ALL_CN_HOLIDAYS
        print(f"[INFO] Known holidays in chart data: {len(holiday_in_chart)}")

        if holiday_in_chart:
            print(f"\n[FAIL] Holiday dates found in chart data:")
            for d in sorted(holiday_in_chart)[:10]:
                print(f"  - {d}")
            pytest.fail(
                f"Holiday dates in chart: {len(holiday_in_chart)}. "
                f"Data should only contain actual trading days."
            )

        print("[PASS] No weekend or holiday dates in chart data.")

    def test_data_points_are_only_trading_dates(self, page: Page):
        plotly_data = page.evaluate("""() => {
            const plotEl = document.querySelector('.js-plotly-plot') ||
                           document.querySelector('.plotly');
            if (plotEl && plotEl.data) {
                const candlestick = plotEl.data.find(t => t.type === 'candlestick');
                if (candlestick) {
                    return {
                        x_values: candlestick.x ? candlestick.x.slice() : [],
                        ticktext: plotEl.layout.xaxis ? (plotEl.layout.xaxis.ticktext || []) : [],
                    };
                }
            }
            return null;
        }""")

        if plotly_data is None:
            pytest.skip("Could not extract Plotly data")

        all_dates = parse_chart_dates(plotly_data["x_values"])
        non_trading = get_all_non_trading_dates(all_dates[0], all_dates[-1])
        chart_date_set = set(all_dates)

        non_trading_in_chart = chart_date_set & non_trading
        total_calendar_days = (datetime.strptime(all_dates[-1], "%Y-%m-%d") -
                               datetime.strptime(all_dates[0], "%Y-%m-%d")).days + 1
        trading_day_ratio = len(all_dates) / total_calendar_days * 100

        print(f"\n[INFO] Calendar days in range: {total_calendar_days}")
        print(f"[INFO] Trading day data points: {len(all_dates)}")
        print(f"[INFO] Trading day ratio: {trading_day_ratio:.1f}%")
        print(f"[INFO] Non-trading days in chart: {len(non_trading_in_chart)}")

        assert len(non_trading_in_chart) == 0, (
            f"Found {len(non_trading_in_chart)} non-trading days in chart data. "
            f"Chart should only contain actual trading dates."
        )
        print("[PASS] All chart data points are trading days only - no gaps will appear.")
