# BLESSING X — FAST PROFIT LAB Smart Paper Engine

A separate, always-on-capable Node.js service for automatic **paper simulation only**.

## Safety boundary

- The engine hard-codes `PAPER_ONLY`; the API reports `liveTradingEnabled: false`.
- It contains no live-order endpoint and never submits orders to Alpaca or Binance.
- Alpaca paper credentials are used only for market-data reads and the paper clock.
- Binance uses public Spot market-data endpoints only.
- Binance Futures, leverage, margin, short selling, and live trading are not implemented.
- The engine starts STOPPED. The owner must press START in the control dashboard.
- STOP halts the scan loop and all automatic position monitoring; existing simulated positions remain open and will not be auto-exited while stopped. Restart to resume management, or use the emergency close action to close all at simulated market prices.
- The control API requires a strong `FAST_PROFIT_CONTROL_TOKEN` and restricts browser origin.
- Never put broker API keys in the browser or commit them to Git.

## Portfolios and risk

Independent virtual portfolios: US stocks THB 10,000 and Binance Spot crypto THB 10,000.

- US stocks: scan and rank candidates, but **never auto-buy**. A user must press BUY for each stock position.
- US stocks: maximum 2 concurrent positions; user selects Take Profit from +5% to +15%; Stop Loss is fixed at -25%.
- US stock exits are monitored by the backend and simulated automatically at TP/SL; no 24-hour time exit is applied to stocks.
- US stock fee: 0.10% per side; slippage: 0.05% per side.
- Crypto auto-entry behavior remains separate and unchanged by the US stock rules.
- Maximum position notional: THB 2,000, subject to available cash and existing portfolio risk limits.
- Maximum 3 concurrent positions for the crypto portfolio; trade-cycle count is not capped while risk checks remain satisfied.
- Daily realized losses plus open stop-risk: THB 200.
- Stop new entries after 3 consecutive same-day losses.
- Long-only. Fees and slippage are conservative estimates, not exchange fee quotes.
- USD/USDT to THB uses configurable `USD_THB_FX` (default 34); set a current rate before testing.

## Smart strategy selection

The engine evaluates three transparent rule-based candidates: Trend Follow, Momentum, and Volume Breakout. It scores recent rolling historical bars using a conservative stop-first intrabar assumption, fees/slippage, win rate, profit factor, net R, and drawdown. It requires at least 8 simulated historical trades before selecting an adaptive winner. If sample size is insufficient, it falls back to Trend Follow.

This is deterministic adaptive strategy selection, not a predictive AI model and not a guarantee of profit. The chosen strategy can differ by market.

## Market data

- US stocks: Alpaca market-data bars (IEX by default) plus the Alpaca paper clock. Configure `ALPACA_PAPER_KEY` and `ALPACA_PAPER_SECRET`. These credentials must be paper-account credentials.
- Crypto: public Binance Spot ticker, klines, and book ticker. No account key is used.
- The scanner runs every 60 seconds by default. US stock entries are blocked while Alpaca reports the market closed; crypto scanning runs continuously.

## API

All control endpoints except `GET /health` require header `X-Fast-Profit-Token`.

- `GET /health`
- `GET /api/status`
- `POST /api/control/start`
- `POST /api/control/stop`
- `POST /api/control/close-all` — simulated emergency close only
- `POST /api/scan`
- `POST /api/stocks/buy` with JSON `{"symbol":"NVDA","targetPct":10}` — opens one user-requested US stock paper position only; targetPct must be 5–15.

## Required environment

- `FAST_PROFIT_CONTROL_TOKEN`: strong random secret, required.
- `DASHBOARD_ORIGIN`: exact BLESSING dashboard origin.
- `ALPACA_PAPER_KEY`, `ALPACA_PAPER_SECRET`: optional until stock data is enabled; paper account only.
- `ALPACA_DATA_FEED`: `iex` default.
- `USD_THB_FX`: configurable conversion rate, default 34.
- `SCAN_INTERVAL_SECONDS`: default 60.
- `STATE_FILE`: must point to persistent storage, recommended `/var/data/fast-profit-state.json`.

## Deployment requirements

Run as a **single paid Render web service** in the Singapore region. It serves a private-by-token control API and runs the scanner loop in the same process. Attach a Render persistent disk mounted at `/var/data`; otherwise trade history and the running flag can be lost on restart/redeploy. Do not scale to multiple instances: this prototype uses a single-process state file and is not a distributed engine.

Do not deploy until a persistent disk is attached, secrets are set, API origin is restricted, and the dashboard has been tested. This repository change does not create a Render service or incur hosting charges.

## Validation

Run `npm test` from this directory. Tests cover indicator calculations, fallback strategy selection, and position risk sizing.
