"""
backtester.py — Production-Grade Historical OHLCV Backtesting Engine (GeckoTerminal Public API v2)
Runs the exact same QuantitativeStrategyEngine (Wilder RSI-14, 5m VWAP, Volume Spike, Multi-Tier TP1/TP2,
Hard SL, and Dynamic Trailing Stop) on real 1-minute Solana pool candles.
"""
import aiohttp
import numpy as np
from strategy import QuantitativeStrategyEngine, ActivePosition


class SolanaMemecoinBacktester:
    def __init__(self, config: dict):
        self.cfg = config
        self.engine = QuantitativeStrategyEngine(config)
        self.ohlcv_base = config["network"]["geckoterminal_ohlcv_base"]

    async def fetch_pool_ohlcv(self, session: aiohttp.ClientSession, pool_address: str, limit: int = 300) -> list:
        if not pool_address:
            return []
        url = f"{self.ohlcv_base}/{pool_address}/ohlcv/minute?aggregate=1&limit={limit}"
        headers = {"Accept": "application/json;version=20230302"}
        try:
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status != 200:
                    return []
                payload = await resp.json()
                candles = (((payload.get("data") or {}).get("attributes") or {}).get("ohlcv_list")) or []
                return list(reversed(candles))
        except Exception:
            return []

    def run_backtest(self, candles: list, initial_sol: float = 10.0, sol_price_usd: float = 188.4) -> dict:
        equity = initial_sol
        peak = initial_sol
        max_dd_pct = 0.0
        trades = []
        returns = []
        pos = None

        for i in range(20, len(candles)):
            window = candles[: i + 1]
            price = float(window[-1][4])
            if pos is None:
                ind = self.engine.compute_vwap_and_vol_spike(window[-16:])
                rsi = self.engine.compute_rsi(ind["closes_1m"], period=14)
                if 45.0 <= rsi <= 72.0 and price > ind["vwap_5m"] and ind["vol_spike_ratio"] >= 1.4:
                    order_sol = float(self.cfg["strategy_risk"]["order_size_sol"])
                    est_tokens = int(((order_sol * sol_price_usd) / max(1e-12, price)) * 1_000_000)
                    pos = ActivePosition(
                        mint="BacktestMint",
                        symbol="BTMEME",
                        pair_address="BacktestPool",
                        entry_price=price,
                        current_price=price,
                        highest_price=price,
                        initial_size_sol=order_sol,
                        initial_token_amount_atomic=est_tokens,
                        remaining_token_amount_atomic=est_tokens,
                    )
            else:
                res = self.engine.evaluate_position_tick(pos, price)
                if res["action"] != "HOLD":
                    pnl_sol = pos.initial_size_sol * res["sell_ratio"] * (res["pnl_pct"] / 100.0)
                    equity += pnl_sol
                    returns.append(res["pnl_pct"] / 100.0)
                    if equity > peak:
                        peak = equity
                    dd = ((equity - peak) / peak) * 100.0
                    if dd < max_dd_pct:
                        max_dd_pct = dd
                    trades.append({
                        "action": res["action"],
                        "entry_price": pos.entry_price,
                        "exit_price": price,
                        "pnl_pct": res["pnl_pct"],
                        "pnl_sol": round(pnl_sol, 4),
                    })
                    if pos.remaining_ratio <= 0.01 or "EXIT" in res["action"]:
                        pos = None

        wins = [t for t in trades if t["pnl_sol"] > 0]
        losses = [t for t in trades if t["pnl_sol"] <= 0]
        win_rate = (len(wins) / len(trades) * 100.0) if trades else 0.0
        gross_profit = sum(t["pnl_sol"] for t in wins)
        gross_loss = abs(sum(t["pnl_sol"] for t in losses)) or 1e-6
        avg_ret = float(np.mean(returns)) if returns else 0.0
        std_ret = float(np.std(returns)) if len(returns) > 1 else 0.01
        sharpe = (avg_ret / (std_ret or 0.01)) * np.sqrt(max(1, len(returns)))
        net_sol = equity - initial_sol

        return {
            "win_rate_pct": round(win_rate, 2),
            "net_pnl_sol": round(net_sol, 4),
            "net_pnl_usd": round(net_sol * sol_price_usd, 2),
            "max_drawdown_pct": round(max_dd_pct, 2),
            "sharpe_ratio": round(float(sharpe), 2),
            "profit_factor": round(gross_profit / gross_loss, 2),
            "total_trades": len(trades),
            "trades": trades,
        }
