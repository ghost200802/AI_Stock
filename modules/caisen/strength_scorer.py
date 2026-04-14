import logging

import numpy as np
import pandas as pd

from modules.chanlun.bi_generator import BiDirection

from .pattern_base import PatternDirection, PatternResult, PatternType

logger = logging.getLogger(__name__)


class StrengthScorer:

    def __init__(self):
        pass

    def score(self, result: PatternResult, bis: list, kline_df):
        result.strength_score = self._calc_strength_score(result, bis, kline_df)
        result.volume_score = self._calc_volume_score(result, bis, kline_df)

    def _calc_strength_score(self, result: PatternResult, bis: list, kline_df) -> float:
        volume_score = self._score_volume_factor(result, kline_df) * 30
        breakout_score = self._score_breakout_magnitude(result) * 20
        time_score = self._score_time_factor(result) * 20
        position_score = self._score_position_factor(result, bis) * 30
        slope_bonus = self._score_neckline_slope(result)

        total = volume_score + breakout_score + time_score + position_score + slope_bonus
        return round(min(max(total, 0), 100), 2)

    def _calc_volume_score(self, result: PatternResult, bis: list, kline_df) -> float:
        if result.direction == PatternDirection.BULLISH:
            return self._calc_bull_volume_score(result, bis, kline_df)
        else:
            return self._calc_bear_volume_score(result, bis, kline_df)

    def _calc_bull_volume_score(self, result: PatternResult, bis: list, kline_df) -> float:
        right_bottom_shrink = self._score_volume_shrink(result, kline_df) * 20
        breakout_volume = self._score_breakout_volume(result, kline_df) * 30
        bottom_exceed_top = self._score_bottom_exceed_top_volume(result, kline_df) * 20
        trend_match = self._score_volume_price_trend(result, kline_df, "bull") * 30

        total = right_bottom_shrink + breakout_volume + bottom_exceed_top + trend_match
        return round(min(max(total, 0), 100), 2)

    def _calc_bear_volume_score(self, result: PatternResult, bis: list, kline_df) -> float:
        right_top_shrink = self._score_volume_shrink(result, kline_df) * 20
        breakdown_volume = self._score_breakout_volume(result, kline_df) * 30
        top_volume_peak = self._score_top_volume_peak(result, kline_df) * 20
        divergence = self._score_volume_price_trend(result, kline_df, "bear") * 30

        total = right_top_shrink + breakdown_volume + top_volume_peak + divergence
        return round(min(max(total, 0), 100), 2)

    def _score_volume_factor(self, result: PatternResult, kline_df) -> float:
        if "volume" not in kline_df.columns or not result.key_points:
            return 0.5

        try:
            breakout_kp = None
            for kp in result.key_points:
                if kp.name in ("breakout", "breakdown", "continuation"):
                    breakout_kp = kp
                    break
            if not breakout_kp:
                return 0.5

            return self._get_volume_ratio(kline_df, breakout_kp.date)
        except Exception:
            return 0.5

    def _get_volume_ratio(self, kline_df: pd.DataFrame, date) -> float:
        try:
            df_sorted = kline_df.sort_values("trade_date").reset_index(drop=True)
            date_series = df_sorted["trade_date"]

            if hasattr(date_series, "dt"):
                match_mask = date_series.dt.normalize() == pd.Timestamp(date).normalize()
            else:
                match_mask = date_series.astype(str).str[:10] == str(date)[:10]

            idx_arr = np.where(match_mask)[0]
            if len(idx_arr) == 0:
                return 0.5
            idx = idx_arr[-1]

            start = max(0, idx - 10)
            recent = df_sorted.iloc[start:idx]["volume"]
            if len(recent) == 0 or recent.mean() <= 0:
                return 0.5

            ratio = df_sorted.iloc[idx]["volume"] / recent.mean()
            return min(ratio / 2.0, 1.0)
        except Exception:
            return 0.5

    def _score_breakout_magnitude(self, result: PatternResult) -> float:
        if result.neckline_price <= 0:
            return 0.5

        if not result.key_points:
            return 0.5

        last_kp = result.key_points[-1]
        magnitude = abs(last_kp.price - result.neckline_price) / result.neckline_price
        return min(magnitude / 0.05, 1.0)

    def _score_time_factor(self, result: PatternResult) -> float:
        if result.start_date is None or result.end_date is None:
            return 0.5

        try:
            import pandas as pd

            start = pd.Timestamp(result.start_date)
            end = pd.Timestamp(result.end_date)
            days = (end - start).days

            if days < 5:
                return 0.3
            elif days <= 60:
                return 1.0
            elif days <= 120:
                return 0.7
            else:
                return 0.4
        except Exception:
            return 0.5

    def _score_position_factor(self, result: PatternResult, bis: list) -> float:
        if not bis:
            return 0.5

        all_prices = []
        for bi in bis:
            all_prices.append(bi.start_price)
            all_prices.append(bi.end_price)

        if not all_prices:
            return 0.5

        price_min = min(all_prices)
        price_max = max(all_prices)
        price_range = price_max - price_min
        if price_range <= 0:
            return 0.5

        if not result.key_points:
            return 0.5

        pattern_prices = [kp.price for kp in result.key_points]
        avg_pattern_price = sum(pattern_prices) / len(pattern_prices)
        relative_pos = (avg_pattern_price - price_min) / price_range

        if result.direction == PatternDirection.BULLISH:
            if relative_pos < 0.3:
                return 1.0
            elif relative_pos < 0.5:
                return 0.7
            else:
                return 0.3
        else:
            if relative_pos > 0.7:
                return 1.0
            elif relative_pos > 0.5:
                return 0.7
            else:
                return 0.3

    def _score_neckline_slope(self, result: PatternResult) -> float:
        slope = result.neckline_slope

        if result.pattern_type == PatternType.HEAD_SHOULDER_BOTTOM:
            if slope > 0:
                return 5.0
            elif slope == 0:
                return 2.0
            else:
                return 0.0
        elif result.pattern_type == PatternType.HEAD_SHOULDER_TOP:
            if slope < 0:
                return 5.0
            elif slope == 0:
                return 2.0
            else:
                return 0.0

        return 0.0

    def _score_volume_shrink(self, result: PatternResult, kline_df) -> float:
        if len(result.key_points) < 2 or "volume" not in kline_df.columns:
            return 0.5

        try:
            first_kp = result.key_points[0]
            second_kp = result.key_points[-2] if len(result.key_points) > 2 else result.key_points[1]

            vol1 = self._get_kline_volume(kline_df, first_kp.date)
            vol2 = self._get_kline_volume(kline_df, second_kp.date)

            if vol1 <= 0:
                return 0.5
            ratio = vol2 / vol1
            if ratio < 0.7:
                return 1.0
            elif ratio < 1.0:
                return 0.7
            else:
                return 0.3
        except Exception:
            return 0.5

    def _score_breakout_volume(self, result: PatternResult, kline_df) -> float:
        return self._score_volume_factor(result, kline_df)

    def _score_bottom_exceed_top_volume(self, result: PatternResult, kline_df) -> float:
        if len(result.key_points) < 3 or "volume" not in kline_df.columns:
            return 0.5

        try:
            bottom_vols = []
            top_vols = []
            for kp in result.key_points:
                vol = self._get_kline_volume(kline_df, kp.date)
                if "bottom" in kp.name or "head" in kp.name or "low" in kp.name:
                    bottom_vols.append(vol)
                elif "top" in kp.name or "neckline" in kp.name or "high" in kp.name or "resistance" in kp.name:
                    top_vols.append(vol)

            if not bottom_vols or not top_vols:
                return 0.5

            avg_bottom = sum(bottom_vols) / len(bottom_vols)
            avg_top = sum(top_vols) / len(top_vols)

            if avg_top <= 0:
                return 0.5
            if avg_bottom > avg_top:
                return 1.0
            else:
                return 0.3
        except Exception:
            return 0.5

    def _score_top_volume_peak(self, result: PatternResult, kline_df) -> float:
        if len(result.key_points) < 2 or "volume" not in kline_df.columns:
            return 0.5

        try:
            top_vols = []
            other_vols = []
            for kp in result.key_points:
                vol = self._get_kline_volume(kline_df, kp.date)
                if "top" in kp.name or "head" in kp.name or "high" in kp.name or "resistance" in kp.name:
                    top_vols.append(vol)
                else:
                    other_vols.append(vol)

            if not top_vols:
                return 0.5

            avg_top = sum(top_vols) / len(top_vols)
            if other_vols:
                avg_other = sum(other_vols) / len(other_vols)
                if avg_other <= 0:
                    return 0.5
                if avg_top > avg_other * 1.3:
                    return 1.0
                elif avg_top > avg_other:
                    return 0.7
                else:
                    return 0.3
            return 0.5
        except Exception:
            return 0.5

    def _score_volume_price_trend(self, result: PatternResult, kline_df, direction: str) -> float:
        if "volume" not in kline_df.columns or result.start_date is None or result.end_date is None:
            return 0.5

        try:
            import pandas as pd

            df_sorted = kline_df.sort_values("trade_date").reset_index(drop=True)
            date_series = df_sorted["trade_date"]

            if hasattr(date_series, "dt"):
                start_ts = pd.Timestamp(result.start_date)
                end_ts = pd.Timestamp(result.end_date)
                mask = (date_series.dt.normalize() >= start_ts.normalize()) & (date_series.dt.normalize() <= end_ts.normalize())
            else:
                start_str = str(result.start_date)[:10]
                end_str = str(result.end_date)[:10]
                mask = (date_series.astype(str).str[:10] >= start_str) & (date_series.astype(str).str[:10] <= end_str)

            subset = df_sorted[mask]
            if len(subset) < 3:
                return 0.5

            prices = subset["close"].values
            volumes = subset["volume"].values

            price_change = prices[-1] - prices[0]
            vol_change = volumes[-1] - volumes[0]

            if direction == "bull":
                if price_change > 0 and vol_change > 0:
                    return 1.0
                elif price_change > 0:
                    return 0.6
                else:
                    return 0.3
            else:
                if price_change < 0 and vol_change > 0:
                    return 1.0
                elif price_change < 0:
                    return 0.6
                else:
                    return 0.3
        except Exception:
            return 0.5

    def _get_kline_volume(self, kline_df: pd.DataFrame, date) -> float:
        try:
            df_sorted = kline_df.sort_values("trade_date").reset_index(drop=True)
            date_series = df_sorted["trade_date"]

            if hasattr(date_series, "dt"):
                match_mask = date_series.dt.normalize() == pd.Timestamp(date).normalize()
            else:
                match_mask = date_series.astype(str).str[:10] == str(date)[:10]

            idx_arr = np.where(match_mask)[0]
            if len(idx_arr) == 0:
                return 0.0
            return float(df_sorted.iloc[idx_arr[-1]]["volume"])
        except Exception:
            return 0.0
