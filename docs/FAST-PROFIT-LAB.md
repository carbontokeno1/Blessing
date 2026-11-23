# BLESSING X — FAST PROFIT LAB

Isolated paper-trading module. Production URL: /fast-profit-lab/.

## Safety boundary
- No live order execution.
- No API, Supabase, backend, authentication, or member-data integration.
- Uses only browser localStorage key blessing_x_fast_profit_lab_v1.
- Shipped as a standalone static route; it remains isolated from the main trading engine and backend.
- Browser storage is local to this browser/profile; use JSON backup to move data.

## Experiment
- Calendar duration: 20 days from first recorded paper trade.
- Two independent virtual portfolios: Original THB 10,000 and Fast THB 10,000.
- Markets: US Stocks and Crypto.
- Prices are manually entered in this initial prototype. No live market feed is connected.
- Quote currency is USD/USDT; FX conversion to THB is editable (default 34).
- Entry validation: max THB 50 risk-to-stop including estimated round-trip fee/slippage, THB 2,000 notional, 3 open positions per portfolio, 8 entries/day per portfolio, THB 200 daily cumulative realized-loss plus open-risk stop, 3-loss streak stop within the same calendar day.

## Review limitations
- Original is a comparison ledger; it does not automatically import or reproduce the existing strategy engine.
- No AI signals or live market data are connected.
- Costs are estimates based on entered fee and slippage per side.
- The 20-day counter starts when the first paper trade is saved; this is not a historical backtest.
- Results are not evidence of expected live returns.
