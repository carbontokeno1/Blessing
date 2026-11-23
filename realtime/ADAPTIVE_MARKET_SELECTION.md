# Adaptive cross-market Paper Trading

The adaptive Paper worker evaluates Binance TH Spot crypto and US equities from Alpaca IEX in the same cycle.

## Selection policy

- Reject stale or fallback-only market data.
- Require aligned momentum, relative volume, breakout and a valid spread.
- Estimate round-trip fees, slippage and quoted spread before ranking.
- Rank eligible candidates with a relative opportunity score and net-edge proxy.
- Open at most one new Paper position per user per worker cycle, selecting the highest-ranked opportunity across both markets.
- Apply a five-minute per-user entry cooldown to reduce churn and repeated exposure.
- If no candidate clears the cost and regime filters, do not open a position.

The score is a screening/ranking heuristic, not a forecast or a promise of profit. It does not establish which market will earn the most in future sessions.

## Risk and execution constraints

- Paper portfolios only. No broker order endpoint is called; live trading remains disabled.
- Risk budget is capped at 0.5% of portfolio equity per entry, with a 20% notional cap and the portfolio cash reserve.
- Existing stock stop-loss (-25%) and take-profit (+20%) settings remain unchanged.
- Crypto Paper entries use the strategy's configured stop/target.
- Existing server-side portfolio risk checks and automatic exits remain authoritative.
- Existing open positions continue to be monitored even when new entries are paused.

## Reporting

The 14-day report records equity marks, net P/L after fees and estimated slippage, and peak-to-trough drawdown. Render Free may suspend the process, so the experiment is best-effort and cannot be described as uninterrupted 24/7 monitoring.
