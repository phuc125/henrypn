"""SQLite persistence for wallet trades and open positions."""
import json
import sqlite3
import time
from pathlib import Path
from typing import Dict, Any, List
from strategy import ActivePosition


class AccountHistoryDB:
    def __init__(self, db_path: str = "logs/solquant_accounts.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self._init_schema()

    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def _init_schema(self):
        with self._conn() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS wallet_profiles (
                wallet_address TEXT PRIMARY KEY, net_pnl_sol REAL DEFAULT 0,
                wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0,
                equity_history_json TEXT DEFAULT '[]', config_json TEXT DEFAULT '{}', updated_at TEXT)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS wallet_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT, wallet_address TEXT NOT NULL,
                timestamp TEXT NOT NULL, mode TEXT NOT NULL, symbol TEXT NOT NULL,
                mint TEXT NOT NULL, action TEXT NOT NULL, entry_price REAL DEFAULT 0,
                exit_price REAL DEFAULT 0, size_sol REAL DEFAULT 0,
                token_amount_atomic INTEGER DEFAULT 0, pnl_pct REAL DEFAULT 0,
                pnl_sol REAL DEFAULT 0, tx_signature TEXT, detail TEXT)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS wallet_positions (
                wallet_address TEXT NOT NULL, mint TEXT NOT NULL, symbol TEXT NOT NULL,
                pair_address TEXT DEFAULT '', entry_price REAL NOT NULL,
                current_price REAL NOT NULL, highest_price REAL NOT NULL,
                initial_size_sol REAL NOT NULL, initial_token_amount_atomic INTEGER DEFAULT 0,
                remaining_token_amount_atomic INTEGER DEFAULT 0, remaining_ratio REAL NOT NULL,
                tp1_executed INTEGER DEFAULT 0, tp2_executed INTEGER DEFAULT 0,
                trailing_active INTEGER DEFAULT 0, PRIMARY KEY(wallet_address, mint))""")
            # Migrate databases created by older builds without this field.
            cols = {r[1] for r in conn.execute("PRAGMA table_info(wallet_trades)").fetchall()}
            if "token_amount_atomic" not in cols:
                conn.execute("ALTER TABLE wallet_trades ADD COLUMN token_amount_atomic INTEGER DEFAULT 0")

    def upsert_position(self, wallet_address: str, pos: ActivePosition):
        with self._conn() as conn:
            conn.execute("""INSERT INTO wallet_positions (
                wallet_address,mint,symbol,pair_address,entry_price,current_price,highest_price,
                initial_size_sol,initial_token_amount_atomic,remaining_token_amount_atomic,
                remaining_ratio,tp1_executed,tp2_executed,trailing_active)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(wallet_address,mint) DO UPDATE SET
                current_price=excluded.current_price, highest_price=excluded.highest_price,
                remaining_token_amount_atomic=excluded.remaining_token_amount_atomic,
                remaining_ratio=excluded.remaining_ratio,tp1_executed=excluded.tp1_executed,
                tp2_executed=excluded.tp2_executed,trailing_active=excluded.trailing_active""",
                (wallet_address,pos.mint,pos.symbol,pos.pair_address,pos.entry_price,pos.current_price,
                 pos.highest_price,pos.initial_size_sol,pos.initial_token_amount_atomic,
                 pos.remaining_token_amount_atomic,pos.remaining_ratio,int(pos.tp1_executed),
                 int(pos.tp2_executed),int(pos.trailing_active)))

    def delete_position(self, wallet_address: str, mint: str):
        with self._conn() as conn:
            conn.execute("DELETE FROM wallet_positions WHERE wallet_address=? AND mint=?", (wallet_address,mint))

    def load_open_positions(self, wallet_address: str) -> Dict[str, ActivePosition]:
        with self._conn() as conn:
            rows=conn.execute("SELECT * FROM wallet_positions WHERE wallet_address=?",(wallet_address,)).fetchall()
        return {r["mint"]: ActivePosition(
            mint=r["mint"],symbol=r["symbol"],pair_address=r["pair_address"] or "",
            entry_price=float(r["entry_price"]),current_price=float(r["current_price"]),
            highest_price=float(r["highest_price"]),initial_size_sol=float(r["initial_size_sol"]),
            initial_token_amount_atomic=int(r["initial_token_amount_atomic"] or 0),
            remaining_token_amount_atomic=int(r["remaining_token_amount_atomic"] or 0),
            remaining_ratio=float(r["remaining_ratio"]),tp1_executed=bool(r["tp1_executed"]),
            tp2_executed=bool(r["tp2_executed"]),trailing_active=bool(r["trailing_active"])) for r in rows}

    def record_trade(self, wallet_address: str, trade: Dict[str, Any]):
        now=trade.get("timestamp") or time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())
        pnl=float(trade.get("pnl_sol") or 0)
        is_exit=(trade.get("action") != "BUY_ENTRY" and not str(trade.get("action", "")).endswith("_FAILED") and trade.get("tx_signature") != "FAILED")
        with self._conn() as conn:
            conn.execute("""INSERT INTO wallet_trades (wallet_address,timestamp,mode,symbol,mint,action,
                entry_price,exit_price,size_sol,token_amount_atomic,pnl_pct,pnl_sol,tx_signature,detail)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (wallet_address,now,trade.get("mode","PAPER"),trade.get("symbol",""),trade.get("mint",""),
                 trade.get("action",""),float(trade.get("entry_price") or trade.get("price_usd") or 0),
                 float(trade.get("exit_price") or trade.get("price_usd") or 0),float(trade.get("size_sol") or 0),
                 int(trade.get("token_amount_atomic") or 0),float(trade.get("pnl_pct") or 0),pnl,
                 trade.get("tx_signature",""),trade.get("detail","")))
            row=conn.execute("SELECT net_pnl_sol,wins,losses,equity_history_json FROM wallet_profiles WHERE wallet_address=?",(wallet_address,)).fetchone()
            net=(float(row["net_pnl_sol"]) if row else 0.0)+(pnl if is_exit else 0.0)
            wins=(int(row["wins"]) if row else 0)+(1 if is_exit and pnl>0 else 0)
            losses=(int(row["losses"]) if row else 0)+(1 if is_exit and pnl<0 else 0)
            equity=json.loads(row["equity_history_json"] or "[]") if row else []
            if is_exit:
                equity.append({"time":now,"pnlSol":round(net,6)})
                equity=equity[-500:]
            conn.execute("""INSERT INTO wallet_profiles(wallet_address,net_pnl_sol,wins,losses,equity_history_json,updated_at)
                VALUES(?,?,?,?,?,?) ON CONFLICT(wallet_address) DO UPDATE SET
                net_pnl_sol=excluded.net_pnl_sol,wins=excluded.wins,losses=excluded.losses,
                equity_history_json=excluded.equity_history_json,updated_at=excluded.updated_at""",
                (wallet_address,net,wins,losses,json.dumps(equity),now))

    def get_wallet_state(self, wallet_address: str) -> Dict[str, Any]:
        with self._conn() as conn:
            prof=conn.execute("SELECT * FROM wallet_profiles WHERE wallet_address=?",(wallet_address,)).fetchone()
            trades=[dict(r) for r in conn.execute("SELECT * FROM wallet_trades WHERE wallet_address=? ORDER BY id DESC LIMIT 500",(wallet_address,)).fetchall()]
            positions=[dict(r) for r in conn.execute("SELECT * FROM wallet_positions WHERE wallet_address=?",(wallet_address,)).fetchall()]
        profile=dict(prof) if prof else {"wallet_address":wallet_address,"net_pnl_sol":0.0,"wins":0,"losses":0,"equity_history_json":"[]"}
        return {"wallet_address":wallet_address,"profile":profile,"trades":trades,"positions":positions}
