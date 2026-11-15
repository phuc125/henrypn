"""
scanner.py — Real-Time Token Discovery (Pump.fun WebSocket & DexScreener Market Cap Filter)
Filters tokens strictly within the $150,000 to $500,000 market cap range using 100% free APIs.
"""
import asyncio
import json
import logging
from typing import Callable, Awaitable, Optional
import aiohttp
import websockets

logger = logging.getLogger("SolQuant.Scanner")


class PumpFunDexScanner:
    def __init__(self, config: dict, on_candidate: Callable[[dict], Awaitable[None]]):
        self.cfg = config
        self.ws_url = config["network"]["pumpportal_ws_url"]
        self.dex_base = config["network"]["dexscreener_api_base"]
        self.min_mcap = float(config["scanner_filters"]["min_market_cap_usd"])
        self.max_mcap = float(config["scanner_filters"]["max_market_cap_usd"])
        self.min_liq = float(config["scanner_filters"]["min_liquidity_usd"])
        self.on_candidate = on_candidate
        self.running = False

    async def fetch_market_metrics(self, session: aiohttp.ClientSession, mint: str) -> Optional[dict]:
        url = f"{self.dex_base}/{mint}"
        try:
            async with session.get(url, timeout=8) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                pairs = [p for p in (data.get("pairs") or []) if p.get("chainId") == "solana"]
                if not pairs:
                    return None
                # Prefer the deepest available Solana pool for more representative metrics.
                pair = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0.0))
                mcap = float(pair.get("marketCap") or pair.get("fdv") or 0.0)
                liq_usd = float((pair.get("liquidity") or {}).get("usd") or 0.0)
                txns_1m = (pair.get("txns") or {}).get("m5") or {}
                txns_5m = (pair.get("txns") or {}).get("m5") or {}
                vol_5m = float((pair.get("volume") or {}).get("m5") or 0.0)
                vol_1h = float((pair.get("volume") or {}).get("h1") or 0.0)
                return {
                    "mint": mint,
                    "symbol": (pair.get("baseToken") or {}).get("symbol", "UNKNOWN"),
                    "name": (pair.get("baseToken") or {}).get("name", "Unknown Token"),
                    "pair_address": pair.get("pairAddress", ""),
                    "dex_id": pair.get("dexId", "raydium"),
                    "price_usd": float(pair.get("priceUsd") or 0.0),
                    "market_cap_usd": mcap,
                    "liquidity_usd": liq_usd,
                    "buys_1m": int(txns_1m.get("buys") or 0),
                    "sells_1m": int(txns_1m.get("sells") or 0),
                    "buys_5m": int(txns_5m.get("buys") or 0),
                    "sells_5m": int(txns_5m.get("sells") or 0),
                    "volume_5m_usd": vol_5m,
                    "volume_1h_usd": vol_1h,
                }
        except Exception as exc:
            logger.debug(f"DexScreener fetch error for {mint}: {exc}")
            return None

    async def start(self):
        self.running = True
        async with aiohttp.ClientSession() as session:
            while self.running:
                try:
                    # Single WebSocket connection rule per PumpPortal specification
                    async with websockets.connect(self.ws_url, ping_interval=20) as ws:
                        await ws.send(json.dumps({"method": "subscribeNewToken"}))
                        await ws.send(json.dumps({"method": "subscribeMigration"}))
                        logger.info("Connected to PumpPortal real-time stream (NewToken + Migration).")

                        async for raw_msg in ws:
                            if not self.running:
                                break
                            payload = json.loads(raw_msg)
                            mint = payload.get("mint")
                            if not mint:
                                continue
                            metrics = await self.fetch_market_metrics(session, mint)
                            if not metrics:
                                continue
                            mcap = metrics["market_cap_usd"]
                            if self.min_mcap <= mcap <= self.max_mcap and metrics["liquidity_usd"] >= self.min_liq:
                                await self.on_candidate(metrics)
                except Exception as err:
                    logger.warning(f"PumpPortal stream interrupted ({err}), reconnecting in 5s...")
                    await asyncio.sleep(5)

    def stop(self):
        self.running = False
