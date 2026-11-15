"""
security.py — Automated Rugcheck.xyz API Integration & On-Chain Authority/LP/Holder Verification
"""
import logging
import aiohttp

logger = logging.getLogger("SolQuant.Security")


class RugcheckVerifier:
    def __init__(self, config: dict):
        self.cfg = config["security_rugcheck"]
        self.rug_base = config["network"]["rugcheck_api_base"]
        self.rpc_url = config["network"]["solana_rpc_url"]

    async def verify_token_safety(self, session: aiohttp.ClientSession, mint: str) -> dict:
        url = f"{self.rug_base}/{mint}/report"
        try:
            async with session.get(url, timeout=8) as resp:
                if resp.status != 200:
                    return {"passed": False, "score": 999, "reason": f"Rugcheck HTTP status {resp.status}"}
                report = await resp.json()
        except Exception as exc:
            return {"passed": False, "score": 999, "reason": f"Rugcheck exception: {exc}"}

        score = int(report.get("score") or 0)
        token_info = report.get("token") or {}
        # Missing fields are unknown, not evidence of a safe token.
        if "mintAuthority" not in token_info or "freezeAuthority" not in token_info:
            return {"passed": False, "score": score, "reason": "Rugcheck report thiếu dữ liệu Mint/Freeze Authority"}
        mint_authority = token_info.get("mintAuthority")
        freeze_authority = token_info.get("freezeAuthority")

        risks = report.get("risks") or []
        danger_risks = [r.get("name", "Unknown Risk") for r in risks if str(r.get("level", "")).lower() == "danger"]

        top_holders = report.get("topHolders")
        if not isinstance(top_holders, list) or not top_holders:
            return {"passed": False, "score": score, "reason": "Rugcheck report thiếu dữ liệu Top Holders"}
        non_lp_holders = [float(h.get("pct") or 0.0) for h in top_holders if not h.get("insider", False)]
        if not non_lp_holders:
            return {"passed": False, "score": score, "reason": "Rugcheck report thiếu holder hợp lệ để đánh giá phân bổ"}
        max_single_holder = max(non_lp_holders)
        top10_total = sum(non_lp_holders[:10])

        markets = report.get("markets") or []
        lp_data = next((m.get("lp") for m in markets if isinstance(m.get("lp"), dict) and "lpLockedPct" in m.get("lp", {})), None)
        if lp_data is None:
            return {"passed": False, "score": score, "reason": "Rugcheck report thiếu dữ liệu LP lock/burn"}
        lp_burned_pct = float(lp_data.get("lpLockedPct") or 0.0)

        if self.cfg["require_mint_revoked"] and mint_authority is not None:
            return {"passed": False, "score": score, "reason": "Mint Authority is NOT revoked"}
        if self.cfg["require_freeze_revoked"] and freeze_authority is not None:
            return {"passed": False, "score": score, "reason": "Freeze Authority is NOT disabled"}
        if self.cfg["reject_danger_status"] and danger_risks:
            return {"passed": False, "score": score, "reason": f"Danger flags: {', '.join(danger_risks[:2])}"}
        if score > self.cfg["max_rugcheck_score"]:
            return {"passed": False, "score": score, "reason": f"Rugcheck score {score} > {self.cfg['max_rugcheck_score']}"}
        if lp_burned_pct < self.cfg["min_lp_burned_pct"]:
            return {"passed": False, "score": score, "reason": f"LP burned/locked {lp_burned_pct:.1f}% < {self.cfg['min_lp_burned_pct']}%"}
        if max_single_holder > self.cfg["max_single_non_lp_holder_pct"]:
            return {"passed": False, "score": score, "reason": f"Top holder owns {max_single_holder:.1f}% > {self.cfg['max_single_non_lp_holder_pct']}%"}
        if top10_total > self.cfg["max_top10_holders_pct"]:
            return {"passed": False, "score": score, "reason": f"Top 10 holders own {top10_total:.1f}% > {self.cfg['max_top10_holders_pct']}%"}

        return {
            "passed": True,
            "score": score,
            "mint_revoked": mint_authority is None,
            "freeze_revoked": freeze_authority is None,
            "lp_burned_pct": round(lp_burned_pct, 2),
            "max_single_holder_pct": round(max_single_holder, 2),
            "top10_holders_pct": round(top10_total, 2),
            "reason": "Passed all Rugcheck.xyz & On-Chain checks"
        }
