# Changelog · Dashboard completion

## Dashboard
- Replaced the previous multi-tab/simulation interface with three live areas: Profit, Execution Terminal, and Bot Configuration.
- Removed hard-coded sample KPIs, demo positions, random PnL ticker, and fake terminal orders.
- Dashboard state now comes from `/api/state`; bot controls use `/api/bot/start` and `/api/bot/stop`; config is saved through `/api/config`.
- WebSocket messages trigger a refresh; terminal history and realized PnL are persisted in SQLite.

## Backend reliability
- Consolidated the duplicated SQLite class and added migration support for `token_amount_atomic`.
- Fixed trade-history writes and separated failed swap records from realized win/loss/equity calculations.
- Position state transitions are committed only after a successful SELL response; failed sells no longer silently consume the in-memory position.
- BUY failures are recorded. SELLs abort if the on-chain token balance cannot be verified or is zero.
- Transaction preflight is enabled for LIVE sends.
- Removed fabricated OHLCV fallback candles; new entries require sufficient real 1m candles.
- Rugcheck verification fails closed when authority, holder, or LP evidence is missing.
- Config values are validated and atomically written; live scanner filters are refreshed after config updates.
- Environment endpoint overrides are now read from `.env`.
- Default mode is PAPER; local server binds to loopback by default.

## Explicit limitations
- Bitget Wallet extension signing is not wired end-to-end. LIVE auto-execution currently requires a backend keypair in `SOLANA_PRIVATE_KEY_BASE58`.
- The configured `min_unique_buyers_5m` threshold currently compares DEX Screener BUY transaction count, not distinct wallet addresses.
- PnL shown is realized PnL in SOL from recorded exits; it is not a mark-to-market valuation of open positions.
- This project has not undergone an independent security audit and does not guarantee profitability.
