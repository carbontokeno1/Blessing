"""Adaptive, explainable Paper Trading strategy selection.

This module only evaluates market data and performance. It never places orders.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def _n(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        return number if number == number and abs(number) != float("inf") else default
    except (TypeError, ValueError):
        return default


def classify_regime(market: str, metrics: dict) -> dict:
    """Classify trend/range/volatility using closed-candle momentum and liquidity."""
    market = str(market or "").lower()
    m1 = _n(metrics.get("m1"))
    m5 = _n(metrics.get("m5"))
    m15 = _n(metrics.get("m15"))
    m60 = _n(metrics.get("m60"))
    rvol = _n(metrics.get("rvol"), _n(metrics.get("relativeVolume")))
    spread = _n(metrics.get("spreadPct"), 999.0)
    breakout = bool(metrics.get("breakout"))
    volatility_limit = 1.5 if market == "crypto" else 1.0

    if spread <= 0 or spread > (0.5 if market == "crypto" else 0.5):
        return {"regime": "illiquid", "strategy": "capital_protection", "allowEntry": False,
                "reason": "spread_out_of_range"}
    if abs(m1) >= volatility_limit or abs(m5) >= volatility_limit * 2.5:
        return {"regime": "high_volatility", "strategy": "capital_protection", "allowEntry": False,
                "reason": "volatility_filter"}
    aligned = (m1 > 0 and m5 > 0 and (m15 > 0 if market == "crypto" else True)
               and (m60 > 0 if market == "crypto" else True))
    down = (m1 < 0 and m5 < 0 and (m15 < 0 if market == "crypto" else True)
            and (m60 < 0 if market == "crypto" else True))
    if down:
        return {"regime": "downtrend", "strategy": "capital_protection", "allowEntry": False,
                "reason": "trend_not_positive"}
    if aligned and rvol >= 1.5 and breakout:
        return {"regime": "uptrend_breakout", "strategy": "trend_momentum", "allowEntry": True,
                "reason": "aligned_momentum_volume_breakout"}
    if aligned and rvol >= 1.5:
        return {"regime": "uptrend", "strategy": "trend_following", "allowEntry": True,
                "reason": "aligned_momentum_and_volume"}
    return {"regime": "range_or_unclear", "strategy": "wait_for_confirmation", "allowEntry": False,
            "reason": "insufficient_alignment"}


def adaptive_decision(market: str, candidate: dict, fee_bps: float = 10.0,
                      slippage_bps: float = 10.0) -> dict:
    """Return an entry/watch decision after estimated round-trip cost checks."""
    regime = classify_regime(market, candidate)
    spread = _n(candidate.get("spreadPct"), 999.0)
    # Approximate round-trip friction: fee + slippage on both sides, plus quoted spread.
    round_trip_cost_pct = (2 * max(0.0, fee_bps) + 2 * max(0.0, slippage_bps)) / 100
    round_trip_cost_pct += spread
    momentum = max(0.0, _n(candidate.get("m1"))) + max(0.0, _n(candidate.get("m5")))
    # A modest hurdle: expected short-term movement proxy must exceed estimated friction.
    cost_ok = momentum > round_trip_cost_pct * 1.5
    allowed = bool(regime["allowEntry"] and cost_ok)
    return {
        **regime,
        "market": market,
        "symbol": str(candidate.get("symbol") or "").upper(),
        "decision": "PAPER_WATCH" if not allowed else "PAPER_CANDIDATE",
        "estimatedRoundTripCostPct": round(round_trip_cost_pct, 4),
        "momentumProxyPct": round(momentum, 4),
        "costFilterPassed": cost_ok,
        "entryAllowed": False,
        "note": "เป็นเพียงสัญญาณคัดกรอง ยังไม่ใช่คำสั่งซื้อขายอัตโนมัติ",
    }


def experiment_window(days: int = 14, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    end = now + timedelta(days=max(1, min(30, int(days))))
    return {"days": max(1, min(30, int(days))), "startedAt": now.isoformat(),
            "endsAt": end.isoformat(), "status": "ready_to_start"}


def performance_summary(starting_equity: float, equity_points: list[dict],
                        fees_paid: float = 0.0, estimated_slippage: float = 0.0) -> dict:
    """Compute net result and peak-to-trough drawdown from chronological equity marks."""
    start = max(0.0, _n(starting_equity))
    points = sorted(equity_points or [], key=lambda row: str(row.get("at") or ""))
    peak = start
    max_dd = 0.0
    final_equity = start
    for point in points:
        equity = _n(point.get("equity"), final_equity)
        final_equity = equity
        peak = max(peak, equity)
        if peak > 0:
            max_dd = max(max_dd, (peak - equity) / peak * 100)
    net_pnl = final_equity - start - max(0.0, _n(fees_paid)) - max(0.0, _n(estimated_slippage))
    return {
        "startingEquity": round(start, 8),
        "endingEquityBeforeCostAdjustment": round(final_equity, 8),
        "feesPaid": round(max(0.0, _n(fees_paid)), 8),
        "estimatedSlippage": round(max(0.0, _n(estimated_slippage)), 8),
        "netPnlAfterCosts": round(net_pnl, 8),
        "netReturnPct": round(net_pnl / start * 100, 4) if start else 0.0,
        "maxDrawdownPct": round(max_dd, 4),
        "marks": len(points),
    }
