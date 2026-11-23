"""Independent BLESSING paper portfolios for crypto and US equities.

No live broker orders, transfers, or cross-portfolio collateral are supported here.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation

LIVE_ORDERS_ENABLED = False
CROSS_PORTFOLIO_TRANSFERS_ENABLED = False

PORTFOLIOS = {
    "crypto": {
        "portfolio_id": "blessing_crypto",
        "market": "crypto",
        "broker": "binance_th",
        "currency": "USDT",
        "starting_capital": "100",
        "reserve_cash_pct": "0",
        "risk_per_trade_pct": "1",
        "max_daily_loss_pct": "3",
        "max_weekly_loss_pct": "6",
        "max_drawdown_pct": "10",
        "max_open_positions": 2,
        "max_aggregate_open_risk_pct": "2",
        "allowed_instruments": ["spot"],
    },
    "stocks": {
        "portfolio_id": "blessing_us_stocks",
        "market": "us_equities",
        "broker": "webull",
        "currency": "USD",
        "starting_capital": "100",
        "reserve_cash_pct": "0",
        "risk_per_trade_pct": "1",
        "max_daily_loss_pct": "3",
        "max_weekly_loss_pct": "6",
        "max_drawdown_pct": "10",
        "max_open_positions": 2,
        "max_aggregate_open_risk_pct": "2",
        "allowed_instruments": ["cash_equity"],
    },
}


def _decimal(value) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("value must be a valid decimal")
    if not result.is_finite():
        raise ValueError("value must be finite")
    return result


def portfolio_config(market: str) -> dict:
    """Return a copy of one portfolio's static limits and paper-only state."""
    key = str(market or "").strip().lower()
    if key not in PORTFOLIOS:
        raise ValueError("market must be 'crypto' or 'stocks'")
    config = dict(PORTFOLIOS[key])
    config.update({
        "mode": "paper",
        "liveOrdersEnabled": LIVE_ORDERS_ENABLED,
        "crossPortfolioTransfersEnabled": CROSS_PORTFOLIO_TRANSFERS_ENABLED,
    })
    return config


def portfolio_overview() -> dict:
    """Return both isolated paper portfolio definitions; no balances are fetched."""
    return {
        "ok": True,
        "mode": "paper",
        "liveOrdersEnabled": LIVE_ORDERS_ENABLED,
        "crossPortfolioTransfersEnabled": CROSS_PORTFOLIO_TRANSFERS_ENABLED,
        "portfolios": {
            key: portfolio_config(key) for key in ("crypto", "stocks")
        },
        "isolationRules": [
            "separate portfolio identifiers and base currencies",
            "separate starting capital, PnL, fees, positions, and risk limits",
            "no cross-portfolio transfers or shared collateral",
            "all available cash is deployable subject to per-trade and portfolio risk limits",
        ],
    }


def calculate_position_size(market: str, equity, stop_distance_pct) -> dict:
    """Risk-based notional sizing, capped by that portfolio's deployable cash.

    Example: $100 equity, 1% risk budget, 1.5% stop distance gives $66.66
    notional before fees/slippage, capped only by the full available cash balance.
    """
    config = portfolio_config(market)
    equity_value = _decimal(equity)
    stop_pct = _decimal(stop_distance_pct)
    if equity_value <= 0 or stop_pct <= 0:
        raise ValueError("equity and stop distance must be greater than zero")

    risk_budget = equity_value * _decimal(config["risk_per_trade_pct"]) / Decimal("100")
    risk_sized_notional = risk_budget / (stop_pct / Decimal("100"))
    deployable_cap = equity_value * (
        Decimal("100") - _decimal(config["reserve_cash_pct"])
    ) / Decimal("100")
    notional = min(risk_sized_notional, deployable_cap)
    return {
        "market": config["market"],
        "currency": config["currency"],
        "riskBudget": str(risk_budget),
        "riskSizedNotional": str(risk_sized_notional),
        "deployableCashCap": str(deployable_cap),
        "maxNotional": str(notional),
        "liveOrdersEnabled": False,
    }
