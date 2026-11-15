"""
strategy.py — Production-Grade Quantitative Analytics & Position State Machine
Features:
  - Wilder/EMA RSI-14 on real 1-minute OHLCV candles
  - True 5-minute Volume-Weighted Average Price (VWAP) from typical price * volume
  - 1-minute Volume Breakout Spike Ratio (latest 1m volume vs rolling average)
  - Exact integer atomic token quantity tracking (`initial_token_amount_atomic` & `remaining_token_amount_atomic`)
    so Jupiter SELL swaps always use exact token base units rather than SOL lamports.
"""
from dataclasses import dataclass
from typing import List, Optional
import numpy as np


@dataclass
class ActivePosition:
    mint: str
    symbol: str
    pair_address: str
    entry_price: float
    current_price: float
    highest_price: float
    initial_size_sol: float
    initial_token_amount_atomic: int = 0
    remaining_token_amount_atomic: int = 0
    remaining_ratio: float = 1.0
    tp1_executed: bool = False
    tp2_executed: bool = False
    trailing_active: bool = False


class QuantitativeStrategyEngine:
    def __init__(self, config: dict):
        self.cfg = config["strategy_risk"]
        self.scan_cfg = config["scanner_filters"]

    @staticmethod
    def compute_rsi(closes: List[float], period: int = 14) -> float:
        """Calculates Wilder's smoothed RSI-14 from a chronological list of closing prices."""
        if len(closes) < period + 1:
            return 55.0
        deltas = np.diff(np.array(closes, dtype=float))
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)

        avg_gain = float(np.mean(gains[:period]))
        avg_loss = float(np.mean(losses[:period]))

        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + float(gains[i])) / period
            avg_loss = (avg_loss * (period - 1) + float(losses[i])) / period

        if avg_loss <= 1e-12:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100.0 - (100.0 / (1.0 + rs)), 2)

    @staticmethod
    def compute_vwap_and_vol_spike(ohlcv_1m: List[List[float]]) -> dict:
        """
        Computes true 5m VWAP and 1m Volume Breakout Spike from chronological 1m candles:
        Each candle format: [timestamp, open, high, low, close, volume]
        """
        if not ohlcv_1m:
            return {"vwap_5m": 0.0, "vol_spike_ratio": 2.0, "closes_1m": []}

        closes = [float(c[4]) for c in ohlcv_1m if len(c) >= 6]
        recent_5 = ohlcv_1m[-5:]
        pv_sum = 0.0
        vol_sum = 0.0
        for c in recent_5:
            typical_price = (float(c[2]) + float(c[3]) + float(c[4])) / 3.0
            v = max(0.0, float(c[5]))
            pv_sum += typical_price * v
            vol_sum += v

        vwap_5m = (pv_sum / vol_sum) if vol_sum > 0 else (closes[-1] if closes else 0.0)

        volumes = [max(0.0, float(c[5])) for c in ohlcv_1m if len(c) >= 6]
        if len(volumes) >= 4:
            latest_vol = max(volumes[-1], volumes[-2])
            baseline_vols = volumes[:-2][-10:]
            avg_baseline = float(np.mean(baseline_vols)) if baseline_vols else 1.0
            vol_spike = (latest_vol / avg_baseline) if avg_baseline > 0 else 2.0
        else:
            vol_spike = 2.0

        return {
            "vwap_5m": float(vwap_5m),
            "vol_spike_ratio": round(float(vol_spike), 2),
            "closes_1m": closes,
        }

    def evaluate_entry(
        self,
        metrics: dict,
        closes_1m: List[float],
        vwap_5m: float,
        vol_spike_ratio: Optional[float] = None,
    ) -> dict:
        buys_5m = max(0, int(metrics.get("buys_5m", 0)))
        sells_5m = max(1, int(metrics.get("sells_5m", 1)))
        vol_ratio_5m = buys_5m / sells_5m
        liq_to_mcap = float(metrics.get("liquidity_usd", 0.0)) / max(1.0, float(metrics.get("market_cap_usd", 1.0)))
        rsi_14 = self.compute_rsi(closes_1m, period=14)
        price = float(metrics.get("price_usd", 0.0))
        min_vol_spike = float(self.cfg.get("min_vol_spike_1m", 1.5))

        if vol_ratio_5m < float(self.cfg["min_buy_sell_vol_ratio_5m"]):
            return {"trigger": False, "reason": f"5m Buy/Sell ratio {vol_ratio_5m:.2f}x < {self.cfg['min_buy_sell_vol_ratio_5m']}x"}
        if buys_5m < int(self.cfg["min_unique_buyers_5m"]):
            return {"trigger": False, "reason": f"5m buyer velocity {buys_5m} < {self.cfg['min_unique_buyers_5m']}"}
        if liq_to_mcap < float(self.scan_cfg["min_liq_to_mcap_ratio"]):
            return {"trigger": False, "reason": f"Liquidity/MCap ratio {liq_to_mcap:.2%} too thin"}
        if not (float(self.cfg["rsi_14_min"]) <= rsi_14 <= float(self.cfg["rsi_14_max"])):
            return {"trigger": False, "reason": f"RSI-14 ({rsi_14:.1f}) outside momentum band [{self.cfg['rsi_14_min']}, {self.cfg['rsi_14_max']}]"}
        if vwap_5m > 0 and price < vwap_5m:
            return {"trigger": False, "reason": f"Price (${price:.6f}) below 5m VWAP (${vwap_5m:.6f})"}
        if vol_spike_ratio is not None and vol_spike_ratio < min_vol_spike:
            return {"trigger": False, "reason": f"1m volume spike {vol_spike_ratio:.2f}x < {min_vol_spike}x"}

        return {
            "trigger": True,
            "vol_ratio_5m": round(vol_ratio_5m, 2),
            "rsi_14": round(rsi_14, 2),
            "vwap_5m": round(vwap_5m, 8),
            "vol_spike_1m": round(vol_spike_ratio or 2.0, 2),
            "liq_to_mcap": round(liq_to_mcap, 4),
            "reason": "Passed all quantitative momentum, RSI-14, VWAP & liquidity triggers",
        }

    def evaluate_position_tick(self, pos: ActivePosition, new_price: float) -> dict:
        """
        Evaluates multi-tier Take Profit, Hard Stop Loss, and Dynamic Trailing Stop.
        Returns both `sell_ratio` (of initial position) and `sell_token_amount_atomic` (exact integer token units).
        """
        pos.current_price = new_price
        if new_price > pos.highest_price:
            pos.highest_price = new_price

        pnl_pct = ((new_price - pos.entry_price) / pos.entry_price) * 100.0
        trail_cfg = self.cfg["trailing_stop"]

        if trail_cfg["enabled"] and pnl_pct >= float(trail_cfg["activation_pnl_pct"]):
            pos.trailing_active = True

        # 1. Dynamic Trailing Stop Exit (100% of remaining position)
        if pos.trailing_active:
            trigger_price = pos.highest_price * (1.0 - float(trail_cfg["trail_distance_pct"]) / 100.0)
            if new_price <= trigger_price:
                sell_ratio = pos.remaining_ratio
                sell_atomic = pos.remaining_token_amount_atomic
                pos.remaining_ratio = 0.0
                pos.remaining_token_amount_atomic = 0
                return {
                    "action": "EXIT_TRAILING_STOP",
                    "sell_ratio": sell_ratio,
                    "sell_token_amount_atomic": sell_atomic,
                    "pnl_pct": round(pnl_pct, 2),
                    "trigger_price": trigger_price,
                }

        # 2. Hard Stop Loss Exit (100% of remaining position)
        if pnl_pct <= float(self.cfg["hard_stop_loss_pct"]):
            sell_ratio = pos.remaining_ratio
            sell_atomic = pos.remaining_token_amount_atomic
            pos.remaining_ratio = 0.0
            pos.remaining_token_amount_atomic = 0
            return {
                "action": "EXIT_HARD_STOP_LOSS",
                "sell_ratio": sell_ratio,
                "sell_token_amount_atomic": sell_atomic,
                "pnl_pct": round(pnl_pct, 2),
                "trigger_price": new_price,
            }

        # 3. Multi-Tier Take Profit 1 (+50% PnL -> Sell 50% of initial position)
        tp_tiers = self.cfg.get("take_profit_tiers") or [
            {"target_pnl_pct": 50.0, "sell_portion_pct": 50.0},
            {"target_pnl_pct": 100.0, "sell_portion_pct": 25.0},
        ]
        tp1_target = float(tp_tiers[0]["target_pnl_pct"])
        tp1_portion = float(tp_tiers[0]["sell_portion_pct"]) / 100.0

        if not pos.tp1_executed and pnl_pct >= tp1_target:
            pos.tp1_executed = True
            sell_atomic = int(pos.initial_token_amount_atomic * tp1_portion)
            sell_atomic = min(sell_atomic, pos.remaining_token_amount_atomic)
            pos.remaining_token_amount_atomic = max(0, pos.remaining_token_amount_atomic - sell_atomic)
            pos.remaining_ratio = max(0.0, pos.remaining_ratio - tp1_portion)
            return {
                "action": "TAKE_PROFIT_1",
                "sell_ratio": tp1_portion,
                "sell_token_amount_atomic": sell_atomic,
                "pnl_pct": round(pnl_pct, 2),
            }

        # 4. Multi-Tier Take Profit 2 (+100% PnL -> Sell 25% of initial position)
        if len(tp_tiers) > 1:
            tp2_target = float(tp_tiers[1]["target_pnl_pct"])
            tp2_portion = float(tp_tiers[1]["sell_portion_pct"]) / 100.0
            if not pos.tp2_executed and pnl_pct >= tp2_target:
                pos.tp2_executed = True
                sell_atomic = int(pos.initial_token_amount_atomic * tp2_portion)
                sell_atomic = min(sell_atomic, pos.remaining_token_amount_atomic)
                pos.remaining_token_amount_atomic = max(0, pos.remaining_token_amount_atomic - sell_atomic)
                pos.remaining_ratio = max(0.0, pos.remaining_ratio - tp2_portion)
                return {
                    "action": "TAKE_PROFIT_2",
                    "sell_ratio": tp2_portion,
                    "sell_token_amount_atomic": sell_atomic,
                    "pnl_pct": round(pnl_pct, 2),
                }

        return {"action": "HOLD", "sell_ratio": 0.0, "sell_token_amount_atomic": 0, "pnl_pct": round(pnl_pct, 2)}
