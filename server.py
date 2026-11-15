"""
server.py — Complete Production FastAPI + WebSocket + Autonomous Trading Engine
Fixes & Upgrades:
  1. Fetches real 1m OHLCV candles from GeckoTerminal API for Wilder RSI-14, 5m VWAP & Volume Spike checks.
  2. Tracks exact `out_amount_atomic` (token atomic units) on BUY and passes exact `sell_token_amount_atomic`
     on TP1, TP2, Hard SL & Dynamic Trailing Stop SELL orders.
  3. Automatically persists and restores open positions from SQLite (`wallet_positions`) across server restarts.
  4. Full REST & WebSocket synchronization endpoints for the Dashboard.
"""
from contextlib import asynccontextmanager
import asyncio
from dataclasses import replace
import json
import logging
import os
from pathlib import Path
from typing import Dict, Set, List
import aiohttp
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field, model_validator

from scanner import PumpFunDexScanner
from security import RugcheckVerifier
from strategy import QuantitativeStrategyEngine, ActivePosition
from executor import JupiterBitgetExecutor, SOL_MINT
from backtester import SolanaMemecoinBacktester
from database import AccountHistoryDB

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("SolQuant.Server")

BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"
config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
# Environment variables override endpoint defaults without requiring config.json edits.
_ENV_NETWORK_KEYS = {
    "SOLANA_RPC_URL": "solana_rpc_url",
    "HELIUS_FREE_SENDER_URL": "helius_free_sender_url",
    "PUMPPORTAL_WS_URL": "pumpportal_ws_url",
    "DEXSCREENER_API_BASE": "dexscreener_api_base",
    "RUGCHECK_API_BASE": "rugcheck_api_base",
    "JUPITER_QUOTE_URL": "jupiter_quote_url",
    "JUPITER_SWAP_URL": "jupiter_swap_url",
    "GECKOTERMINAL_OHLCV_BASE": "geckoterminal_ohlcv_base",
}
for env_key, config_key in _ENV_NETWORK_KEYS.items():
    if os.getenv(env_key):
        config["network"][config_key] = os.environ[env_key].strip()

verifier = RugcheckVerifier(config)
strategy = QuantitativeStrategyEngine(config)
executor = JupiterBitgetExecutor(config, log_dir=str(BASE_DIR / "logs"))
backtester = SolanaMemecoinBacktester(config)
db = AccountHistoryDB(db_path=str(BASE_DIR / "logs" / "solquant_accounts.db"))


class AutonomousBotState:
    def __init__(self):
        self.running: bool = False
        self.mode: str = os.getenv("TRADING_MODE", "PAPER").upper()
        self.positions: Dict[str, ActivePosition] = {}
        self.recent_scanner_logs: List[dict] = []
        self.ws_clients: Set[WebSocket] = set()
        self.scanner: PumpFunDexScanner | None = None
        self.scanner_task: asyncio.Task | None = None
        self.monitor_task: asyncio.Task | None = None


bot_state = AutonomousBotState()


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    # Recover any open positions from SQLite on server startup
    wallet_addr = executor.get_bot_pubkey() or "DEFAULT_LOCAL"
    recovered = db.load_open_positions(wallet_addr)
    if recovered:
        bot_state.positions.update(recovered)
        logger.info(f"Recovered {len(recovered)} open position(s) from SQLite for wallet {wallet_addr}.")
    yield
    bot_state.running = False
    if bot_state.scanner:
        bot_state.scanner.stop()


app = FastAPI(
    title="SolQuant Pulse — 100% Autonomous Solana Trading Bot & Bitget Dashboard",
    lifespan=lifespan,
)


async def broadcast_ws(event: dict):
    dead = []
    msg = json.dumps(event, ensure_ascii=False)
    for ws in list(bot_state.ws_clients):
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(ws)
    for d in dead:
        bot_state.ws_clients.discard(d)


async def handle_discovered_candidate(metrics: dict):
    """
    Complete 4-Stage Pipeline:
      1. Market Cap ($150k-$500k) & Liquidity Filter
      2. Rugcheck.xyz + On-Chain Mint/Freeze/LP/Top10 Verification
      3. Real 1m OHLCV Candles (GeckoTerminal) -> Wilder RSI-14, 5m VWAP & Volume Spike
      4. Autonomous BUY Swap via Jupiter + SQLite Position Persistence
    """
    if not bot_state.running:
        return
    mint = metrics["mint"]
    if mint in bot_state.positions:
        return

    async with aiohttp.ClientSession() as session:
        safety = await verifier.verify_token_safety(session, mint)
        if not safety["passed"]:
            log_evt = {
                "event_type": "SCANNER",
                "status": "REJECTED",
                "symbol": metrics["symbol"],
                "mint": mint,
                "mcap": metrics["market_cap_usd"],
                "rug_score": safety.get("score", 999),
                "reason": safety["reason"],
            }
            bot_state.recent_scanner_logs.insert(0, log_evt)
            bot_state.recent_scanner_logs = bot_state.recent_scanner_logs[:100]
            executor.log_event(log_evt)
            await broadcast_ws(log_evt)
            return

        # Fetch real 1-minute OHLCV candles from GeckoTerminal for true RSI-14 & 5m VWAP
        price = float(metrics["price_usd"])
        pair_address = metrics.get("pair_address", "")
        candles_1m = await backtester.fetch_pool_ohlcv(session, pair_address, limit=25)
        # Never fabricate indicator inputs. A token without enough real candles is skipped.
        if len(candles_1m) < 15:
            log_evt = {"event_type":"SCANNER","status":"REJECTED","symbol":metrics["symbol"],
                       "mint":mint,"reason":f"Insufficient real OHLCV candles ({len(candles_1m)}/15)"}
            bot_state.recent_scanner_logs.insert(0, log_evt)
            bot_state.recent_scanner_logs = bot_state.recent_scanner_logs[:100]
            executor.log_event(log_evt)
            await broadcast_ws(log_evt)
            return
        ind = strategy.compute_vwap_and_vol_spike(candles_1m)
        closes_1m, vwap_5m, vol_spike = ind["closes_1m"], ind["vwap_5m"], ind["vol_spike_ratio"]

        quant = strategy.evaluate_entry(metrics, closes_1m=closes_1m, vwap_5m=vwap_5m, vol_spike_ratio=vol_spike)
        if not quant["trigger"]:
            log_evt = {
                "event_type": "SCANNER",
                "status": "REJECTED",
                "symbol": metrics["symbol"],
                "mint": mint,
                "mcap": metrics["market_cap_usd"],
                "rug_score": safety.get("score", 150),
                "reason": quant["reason"],
            }
            bot_state.recent_scanner_logs.insert(0, log_evt)
            bot_state.recent_scanner_logs = bot_state.recent_scanner_logs[:100]
            executor.log_event(log_evt)
            await broadcast_ws(log_evt)
            return

        # Passed all gates -> Execute autonomous BUY
        order_sol = float(config["strategy_risk"]["order_size_sol"])
        lamports = int(order_sol * 1_000_000_000)
        swap_res = await executor.execute_autonomous_swap(
            session=session,
            input_mint=SOL_MINT,
            output_mint=mint,
            amount_atomic=lamports,
            mode=bot_state.mode,
            estimated_price_usd=price,
        )

        if not swap_res.get("success"):
            failed_evt = {"event_type":"TRADE","status":"FAILED","mode":bot_state.mode,
                          "symbol":metrics["symbol"],"mint":mint,"action":"BUY_FAILED",
                          "price_usd":price,"detail":swap_res.get("error","Swap failed")}
            failed_evt["wallet_address"] = executor.get_bot_pubkey() or "DEFAULT_LOCAL"
            failed_evt["tx_signature"] = "FAILED"
            executor.log_event(failed_evt)
            db.record_trade(failed_evt["wallet_address"], failed_evt)
            await broadcast_ws(failed_evt)
            return

        if swap_res.get("success"):
            token_atomic_received = int(swap_res.get("out_amount_atomic") or 0)
            if token_atomic_received <= 0:
                failed_evt = {"event_type":"TRADE","status":"FAILED","mode":bot_state.mode,
                              "symbol":metrics["symbol"],"mint":mint,"action":"BUY_FAILED",
                              "price_usd":price,"detail":"Swap returned zero token amount; position not opened"}
                executor.log_event(failed_evt)
                await broadcast_ws(failed_evt)
                return
            pos = ActivePosition(
                mint=mint,
                symbol=metrics["symbol"],
                pair_address=pair_address,
                entry_price=price,
                current_price=price,
                highest_price=price,
                initial_size_sol=order_sol,
                initial_token_amount_atomic=token_atomic_received,
                remaining_token_amount_atomic=token_atomic_received,
            )
            bot_state.positions[mint] = pos
            wallet_addr = executor.get_bot_pubkey() or "DEFAULT_LOCAL"
            db.upsert_position(wallet_addr, pos)

            trade_evt = {
                "event_type": "TRADE",
                "wallet_address": wallet_addr,
                "mode": bot_state.mode,
                "symbol": metrics["symbol"],
                "mint": mint,
                "action": "BUY_ENTRY",
                "entry_price": price,
                "exit_price": price,
                "price_usd": price,
                "size_sol": order_sol,
                "token_amount_atomic": token_atomic_received,
                "pnl_pct": 0.0,
                "pnl_sol": 0.0,
                "tx_signature": swap_res["tx_signature"],
                "detail": f"RSI {quant['rsi_14']} | VolRatio {quant['vol_ratio_5m']}x | RugScore {safety['score']}",
            }
            executor.log_event(trade_evt)
            db.record_trade(wallet_addr, trade_evt)
            await broadcast_ws(trade_evt)
            logger.info(
                f"[{bot_state.mode}] AUTO BUY {metrics['symbol']} ({order_sol} SOL -> {token_atomic_received} atomic units) | Tx: {swap_res['tx_signature']}"
            )


async def monitor_positions_loop():
    """Monitor positions; commit state transitions only after a successful SELL."""
    scanner_helper = PumpFunDexScanner(config, handle_discovered_candidate)
    async with aiohttp.ClientSession() as session:
        while bot_state.running:
            wallet_addr = executor.get_bot_pubkey() or "DEFAULT_LOCAL"
            for mint, pos in list(bot_state.positions.items()):
                metrics = await scanner_helper.fetch_market_metrics(session, mint)
                if not metrics or metrics["price_usd"] <= 0:
                    continue
                working = replace(pos)
                decision = strategy.evaluate_position_tick(working, float(metrics["price_usd"]))
                if decision["action"] == "HOLD":
                    pos.current_price = working.current_price
                    pos.highest_price = working.highest_price
                    pos.trailing_active = working.trailing_active
                    db.upsert_position(wallet_addr, pos)
                    continue

                sell_token_atomic = int(decision.get("sell_token_amount_atomic") or 0)
                if sell_token_atomic <= 0:
                    logger.error("Refusing zero-quantity SELL for %s", mint)
                    continue
                sell_sol_portion = pos.initial_size_sol * float(decision["sell_ratio"])
                swap_res = await executor.execute_autonomous_swap(
                    session=session, input_mint=mint, output_mint=SOL_MINT,
                    amount_atomic=sell_token_atomic, mode=bot_state.mode,
                    estimated_price_usd=float(metrics["price_usd"]),
                )
                if not swap_res.get("success"):
                    fail_evt = {"event_type":"TRADE","status":"FAILED","mode":bot_state.mode,
                                "symbol":pos.symbol,"mint":mint,"action":decision["action"]+"_FAILED",
                                "entry_price":pos.entry_price,"exit_price":float(metrics["price_usd"]),
                                "detail":swap_res.get("error","SELL swap failed; position state unchanged")}
                    executor.log_event(fail_evt)
                    await broadcast_ws(fail_evt)
                    continue

                # Only now persist the state machine transition.
                pos.current_price = working.current_price
                pos.highest_price = working.highest_price
                pos.remaining_ratio = working.remaining_ratio
                pos.remaining_token_amount_atomic = working.remaining_token_amount_atomic
                pos.tp1_executed = working.tp1_executed
                pos.tp2_executed = working.tp2_executed
                pos.trailing_active = working.trailing_active
                sol_received = int(swap_res.get("out_amount_atomic") or 0) / 1e9
                pnl_sol = (sol_received - sell_sol_portion) if sol_received > 0 else sell_sol_portion * (float(decision["pnl_pct"]) / 100.0)
                trade_evt = {
                    "event_type":"TRADE","status":"CONFIRMED","wallet_address":wallet_addr,
                    "mode":bot_state.mode,"symbol":pos.symbol,"mint":mint,"action":decision["action"],
                    "entry_price":pos.entry_price,"exit_price":float(metrics["price_usd"]),
                    "price_usd":float(metrics["price_usd"]),"size_sol":round(sell_sol_portion,6),
                    "token_amount_atomic":sell_token_atomic,"pnl_pct":decision["pnl_pct"],
                    "pnl_sol":round(pnl_sol,6),"tx_signature":swap_res.get("tx_signature",""),
                    "detail":f"Sold {sell_token_atomic} atomic units via {decision['action']}"}
                executor.log_event(trade_evt)
                db.record_trade(wallet_addr, trade_evt)
                await broadcast_ws(trade_evt)
                if pos.remaining_ratio <= 0.01 or pos.remaining_token_amount_atomic <= 0 or "EXIT" in decision["action"]:
                    bot_state.positions.pop(mint, None)
                    db.delete_position(wallet_addr, mint)
                else:
                    db.upsert_position(wallet_addr, pos)
            await asyncio.sleep(4)


class BitgetSwapRequest(BaseModel):
    user_public_key: str
    token_mint: str
    side: str = "BUY"
    amount_atomic: int = 500_000_000


class StrategyConfigUpdate(BaseModel):
    min_market_cap_usd: float = Field(ge=1000, le=100_000_000)
    max_market_cap_usd: float = Field(ge=1000, le=1_000_000_000)
    max_rugcheck_score: int = Field(ge=0, le=10_000)
    max_single_non_lp_holder_pct: float = Field(gt=0, le=100)
    min_buy_sell_vol_ratio_5m: float = Field(gt=0, le=100)
    order_size_sol: float = Field(gt=0, le=100)
    tp1_pct: float = Field(gt=0, le=10000)
    tp2_pct: float = Field(gt=0, le=10000)
    hard_stop_loss_pct: float = Field(lt=0, ge=-99)
    trail_activation_pct: float = Field(gt=0, le=10000)
    trail_distance_pct: float = Field(gt=0, lt=100)

    @model_validator(mode="after")
    def validate_ranges(self):
        if self.max_market_cap_usd <= self.min_market_cap_usd:
            raise ValueError("max_market_cap_usd must be greater than min_market_cap_usd")
        if self.tp2_pct <= self.tp1_pct:
            raise ValueError("TP2 must be greater than TP1")
        return self


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    html_file = BASE_DIR / "dashboard" / "index.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))


@app.get("/api/health")
async def health_check():
    return {
        "status": "ok",
        "running": bot_state.running,
        "mode": bot_state.mode,
        "autonomous_keypair_loaded": executor.keypair is not None,
        "bot_pubkey": executor.get_bot_pubkey(),
        "open_positions_count": len(bot_state.positions),
    }


@app.get("/api/state")
async def get_live_bot_state():
    wallet_addr = executor.get_bot_pubkey() or "DEFAULT_LOCAL"
    account = db.get_wallet_state(wallet_addr)
    profile = account["profile"]
    return {
        "running": bot_state.running,
        "mode": bot_state.mode,
        "bot_pubkey": wallet_addr,
        "keypair_loaded": executor.keypair is not None,
        "config": config,
        "open_positions": [vars(p) for p in bot_state.positions.values()],
        "recent_scanner_logs": bot_state.recent_scanner_logs[:30],
        "account": account,
        "metrics": {"net_pnl_sol": float(profile.get("net_pnl_sol", 0)),
                    "wins": int(profile.get("wins", 0)), "losses": int(profile.get("losses", 0)),
                    "trade_count": len(account["trades"]),
                    "equity_history": json.loads(profile.get("equity_history_json") or "[]")},
    }


@app.post("/api/config")
async def update_strategy_config(update: StrategyConfigUpdate):
    config["scanner_filters"]["min_market_cap_usd"] = update.min_market_cap_usd
    config["scanner_filters"]["max_market_cap_usd"] = update.max_market_cap_usd
    config["security_rugcheck"]["max_rugcheck_score"] = update.max_rugcheck_score
    config["security_rugcheck"]["max_single_non_lp_holder_pct"] = update.max_single_non_lp_holder_pct
    config["strategy_risk"]["min_buy_sell_vol_ratio_5m"] = update.min_buy_sell_vol_ratio_5m
    config["strategy_risk"]["order_size_sol"] = update.order_size_sol
    config["strategy_risk"]["take_profit_tiers"][0]["target_pnl_pct"] = update.tp1_pct
    config["strategy_risk"]["take_profit_tiers"][1]["target_pnl_pct"] = update.tp2_pct
    config["strategy_risk"]["hard_stop_loss_pct"] = update.hard_stop_loss_pct
    config["strategy_risk"]["trailing_stop"]["activation_pnl_pct"] = update.trail_activation_pct
    config["strategy_risk"]["trailing_stop"]["trail_distance_pct"] = update.trail_distance_pct
    tmp_path = CONFIG_PATH.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    tmp_path.replace(CONFIG_PATH)
    if bot_state.scanner:
        bot_state.scanner.min_mcap = float(config["scanner_filters"]["min_market_cap_usd"])
        bot_state.scanner.max_mcap = float(config["scanner_filters"]["max_market_cap_usd"])
        bot_state.scanner.min_liq = float(config["scanner_filters"]["min_liquidity_usd"])
    return {"status":"updated", "config":config}


@app.post("/api/bot/start")
async def start_autonomous_bot(mode: str = "PAPER"):
    bot_state.mode = mode.upper()
    if bot_state.mode not in {"PAPER", "LIVE"}:
        raise HTTPException(status_code=400, detail="mode must be PAPER or LIVE")
    if bot_state.mode == "LIVE" and executor.keypair is None:
        raise HTTPException(
            status_code=400,
            detail="Chế độ LIVE tự động 100% cần cấu hình SOLANA_PRIVATE_KEY_BASE58 trong file .env",
        )
    if not bot_state.running:
        bot_state.running = True
        bot_state.scanner = PumpFunDexScanner(config, handle_discovered_candidate)
        bot_state.scanner_task = asyncio.create_task(bot_state.scanner.start())
        bot_state.monitor_task = asyncio.create_task(monitor_positions_loop())
    return {"running": True, "mode": bot_state.mode, "bot_pubkey": executor.get_bot_pubkey()}


@app.post("/api/bot/stop")
async def stop_autonomous_bot():
    bot_state.running = False
    if bot_state.scanner:
        bot_state.scanner.stop()
    return {"running": False}


@app.post("/api/bitget/build-swap")
async def build_bitget_unsigned_swap(req: BitgetSwapRequest):
    in_mint = SOL_MINT if req.side.upper() == "BUY" else req.token_mint
    out_mint = req.token_mint if req.side.upper() == "BUY" else SOL_MINT
    async with aiohttp.ClientSession() as session:
        swap_bundle = await executor.build_unsigned_jupiter_swap(
            session=session,
            user_pubkey=req.user_public_key,
            input_mint=in_mint,
            output_mint=out_mint,
            amount_atomic=req.amount_atomic,
        )
        return JSONResponse(swap_bundle)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    bot_state.ws_clients.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        bot_state.ws_clients.discard(ws)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host=os.getenv("HOST", "127.0.0.1"), port=port)
