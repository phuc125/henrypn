"""
executor.py — Production-Grade Jupiter V1/V6 Swap Executor + On-Chain SPL Token Balance Verification
Features:
  - Extracts exact `inAmount` and `outAmount` from Jupiter quote responses
  - Queries on-chain SPL token balance (`getTokenAccountsByOwner`) before live SELL orders to prevent
    rounding/slippage mismatch errors
  - Supports 100% autonomous backend signing (`solders.keypair.Keypair`) via Helius Free Sender + Solana RPC
  - Supports Non-Custodial Bitget Wallet unsigned VersionedTransaction building (`window.bitkeep.solana`)
"""
import base64
import csv
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional
import aiohttp
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

logger = logging.getLogger("SolQuant.Executor")
SOL_MINT = "So11111111111111111111111111111111111111112"


class JupiterBitgetExecutor:
    def __init__(self, config: dict, log_dir: str = "logs"):
        self.cfg = config
        self.log_path = Path(log_dir)
        self.log_path.mkdir(parents=True, exist_ok=True)
        self.jsonl_file = self.log_path / "structured_events.jsonl"
        self.csv_file = self.log_path / "trades_history.csv"
        self.keypair: Optional[Keypair] = self._load_keypair()

        if not self.csv_file.exists():
            with open(self.csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp", "wallet_address", "mode", "symbol", "mint", "action",
                    "price_usd", "size_sol", "token_amount_atomic", "pnl_pct", "pnl_sol", "tx_signature"
                ])

    @staticmethod
    def _load_keypair() -> Optional[Keypair]:
        priv = os.getenv("SOLANA_PRIVATE_KEY_BASE58", "").strip()
        if not priv:
            return None
        try:
            return Keypair.from_base58_string(priv)
        except Exception as exc:
            logger.error(f"Invalid SOLANA_PRIVATE_KEY_BASE58: {exc}")
            return None

    def get_bot_pubkey(self) -> Optional[str]:
        if self.keypair:
            return str(self.keypair.pubkey())
        return os.getenv("BITGET_WALLET_PUBLIC_KEY", "").strip() or None

    async def get_spl_token_atomic_balance(
        self, session: aiohttp.ClientSession, owner_pubkey: str, mint: str
    ) -> Optional[int]:
        """Queries Solana JSON-RPC `getTokenAccountsByOwner` for the exact integer atomic token balance."""
        rpc_url = self.cfg["network"].get("solana_rpc_url", "https://api.mainnet-beta.solana.com")
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                owner_pubkey,
                {"mint": mint},
                {"encoding": "jsonParsed"},
            ],
        }
        try:
            async with session.post(rpc_url, json=payload, timeout=8) as resp:
                data = await resp.json()
                if resp.status != 200 or data.get("error") or not isinstance(data.get("result"), dict):
                    return None
                accounts = ((data.get("result") or {}).get("value")) or []
                total_atomic = 0
                for acc in accounts:
                    token_amount = (
                        (((acc.get("account") or {}).get("data") or {}).get("parsed") or {})
                        .get("info", {})
                        .get("tokenAmount", {})
                    )
                    total_atomic += int(token_amount.get("amount") or 0)
                return total_atomic
        except Exception as exc:
            logger.warning(f"RPC getTokenAccountsByOwner failed for {mint}: {exc}")
            return None

    async def build_unsigned_jupiter_swap(
        self,
        session: aiohttp.ClientSession,
        user_pubkey: str,
        input_mint: str,
        output_mint: str,
        amount_atomic: int,
    ) -> dict:
        quote_url = self.cfg["network"]["jupiter_quote_url"]
        swap_url = self.cfg["network"]["jupiter_swap_url"]
        slippage_bps = self.cfg["wallet"]["slippage_bps"]

        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(int(amount_atomic)),
            "slippageBps": str(slippage_bps),
        }
        async with session.get(quote_url, params=params, timeout=10) as q_resp:
            if q_resp.status != 200:
                err_txt = await q_resp.text()
                raise RuntimeError(f"Jupiter Quote HTTP {q_resp.status}: {err_txt}")
            quote_data = await q_resp.json()

        payload = {
            "quoteResponse": quote_data,
            "userPublicKey": user_pubkey,
            "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
            "prioritizationFeeLamports": self.cfg["wallet"]["priority_fee_lamports"],
        }
        async with session.post(swap_url, json=payload, timeout=10) as s_resp:
            if s_resp.status != 200:
                err_txt = await s_resp.text()
                raise RuntimeError(f"Jupiter Swap HTTP {s_resp.status}: {err_txt}")
            swap_data = await s_resp.json()

        return {
            "unsigned_tx_base64": swap_data.get("swapTransaction"),
            "quote": quote_data,
            "in_amount_atomic": int(quote_data.get("inAmount") or amount_atomic),
            "out_amount_atomic": int(quote_data.get("outAmount") or 0),
            "signing_target": "window.bitkeep.solana.signAndSendTransaction",
        }

    async def execute_autonomous_swap(
        self,
        session: aiohttp.ClientSession,
        input_mint: str,
        output_mint: str,
        amount_atomic: int,
        mode: str = "LIVE",
        estimated_price_usd: float = 0.00025,
        sol_price_usd: float = 188.40,
    ) -> dict:
        """
        Executes a swap in PAPER or 100% Autonomous LIVE mode.
        Always returns `in_amount_atomic` and `out_amount_atomic` so BUY orders record exact token units
        and SELL orders record exact SOL lamports received.
        """
        is_buy = (input_mint == SOL_MINT)
        if mode.upper() != "LIVE":
            sim_sig = f"PAPER_SIM_{int(time.time() * 1000)}"
            if is_buy:
                sol_spent = amount_atomic / 1e9
                usd_spent = sol_spent * sol_price_usd
                tokens_bought = (usd_spent / max(1e-12, estimated_price_usd))
                # Pump.fun SPL tokens use 6 decimals (10^6 atomic units)
                out_atomic = int(tokens_bought * 1_000_000)
            else:
                tokens_sold = amount_atomic / 1_000_000
                usd_received = tokens_sold * estimated_price_usd
                sol_received = usd_received / max(1.0, sol_price_usd)
                out_atomic = int(sol_received * 1e9)
            return {
                "success": True,
                "mode": "PAPER",
                "tx_signature": sim_sig,
                "in_amount_atomic": int(amount_atomic),
                "out_amount_atomic": int(out_atomic),
            }

        if not self.keypair:
            return {
                "success": False,
                "mode": "LIVE",
                "error": "Missing SOLANA_PRIVATE_KEY_BASE58 in .env for 100% autonomous signing.",
            }

        user_pubkey = str(self.keypair.pubkey())

        # If selling a token on LIVE Mainnet, cap amount_atomic at actual on-chain balance to prevent over-sell failure
        if not is_buy:
            onchain_bal = await self.get_spl_token_atomic_balance(session, user_pubkey, input_mint)
            if onchain_bal is None:
                return {"success": False, "mode": "LIVE", "error": "Could not verify on-chain token balance; SELL aborted safely."}
            if onchain_bal <= 0:
                return {"success": False, "mode": "LIVE", "error": "On-chain token balance is zero; SELL aborted safely."}
            if amount_atomic <= 0 or amount_atomic > onchain_bal:
                amount_atomic = onchain_bal

        bundle = await self.build_unsigned_jupiter_swap(
            session=session,
            user_pubkey=user_pubkey,
            input_mint=input_mint,
            output_mint=output_mint,
            amount_atomic=amount_atomic,
        )
        raw_b64 = bundle.get("unsigned_tx_base64")
        if not raw_b64:
            return {"success": False, "mode": "LIVE", "error": "Jupiter Swap API did not return swapTransaction"}

        tx_bytes = base64.b64decode(raw_b64)
        unsigned_tx = VersionedTransaction.from_bytes(tx_bytes)
        signed_tx = VersionedTransaction(unsigned_tx.message, [self.keypair])
        signed_b64 = base64.b64encode(bytes(signed_tx)).decode("utf-8")

        rpc_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                signed_b64,
                {"encoding": "base64", "skipPreflight": False, "preflightCommitment": "confirmed", "maxRetries": 3},
            ],
        }

        sender_url = self.cfg["network"].get("helius_free_sender_url", "https://sender.helius-rpc.com/fast")
        rpc_url = self.cfg["network"].get("solana_rpc_url", "https://api.mainnet-beta.solana.com")

        for endpoint in (sender_url, rpc_url):
            try:
                async with session.post(endpoint, json=rpc_payload, timeout=10) as resp:
                    res_json = await resp.json()
                    if "result" in res_json and res_json["result"]:
                        return {
                            "success": True,
                            "mode": "LIVE",
                            "tx_signature": res_json["result"],
                            "endpoint": endpoint,
                            "in_amount_atomic": bundle["in_amount_atomic"],
                            "out_amount_atomic": bundle["out_amount_atomic"],
                        }
            except Exception as exc:
                logger.warning(f"Broadcast failed on {endpoint}: {exc}")

        return {"success": False, "mode": "LIVE", "error": "Failed to broadcast transaction on all endpoints"}

    def log_event(self, event: dict):
        event["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(self.jsonl_file, "a", encoding="utf-8") as jf:
            jf.write(json.dumps(event, ensure_ascii=False) + "\n")
        if event.get("event_type") == "TRADE":
            with open(self.csv_file, "a", newline="", encoding="utf-8") as cf:
                writer = csv.writer(cf)
                writer.writerow([
                    event["timestamp"],
                    event.get("wallet_address", "DEFAULT_LOCAL"),
                    event.get("mode", "PAPER"),
                    event.get("symbol", ""),
                    event.get("mint", ""),
                    event.get("action", ""),
                    event.get("price_usd", 0.0),
                    event.get("size_sol", 0.0),
                    event.get("token_amount_atomic", 0),
                    event.get("pnl_pct", 0.0),
                    event.get("pnl_sol", 0.0),
                    event.get("tx_signature", ""),
                ])
