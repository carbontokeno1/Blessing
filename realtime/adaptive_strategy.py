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
    # Identical entry filters for crypto and equities; only exit targets differ.
    volatility_limit = 1.5
    if spread <= 0 or spread > 0.5:
        return {"regime": "illiquid", "strategy": "capital_protection", "allowEntry": False,
                "reason": "spread_out_of_range"}
    if abs(m1) >= volatility_limit or abs(m5) >= volatility_limit * 2.5:
        return {"regime": "high_volatility", "strategy": "capital_protection", "allowEntry": False,
                "reason": "volatility_filter"}
    aligned = m1 > 0 and m5 > 0
    down = m1 < 0 and m5 < 0
    if down:
        return {"regime": "downtrend", "strategy": "capital_protection", "allowEntry": False,
                "reason": "trend_not_positive"}
    if aligned and rvol >= 2.0 and breakout:
        return {"regime": "uptrend_breakout", "strategy": "trend_momentum", "allowEntry": True,
                "reason": "aligned_momentum_volume_breakout"}
    return {"regime": "range_or_unclear", "strategy": "wait_for_confirmation", "allowEntry": False,
            "reason": "insufficient_alignment"}



def build_binance_th_candidates(history_by_symbol: dict, now: float | None = None) -> list[dict]:
    """Derive multi-horizon momentum, relative volume and breakout from Binance TH ticker history.

    Each history row is (unix_time, price, cumulative_quote_volume, bid, ask).
    At least 60 minutes of recent live observations are required; incomplete data fails closed.
    """
    import time
    now = time.time() if now is None else float(now)
    output = []
    excluded = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "EUR", "USDE", "USTC", "BUSD", "BFUSD"}
    leveraged = ("UP", "DOWN", "BULL", "BEAR", "3L", "3S", "5L", "5S")

    def at_or_before(rows, target):
        return next((row for row in reversed(rows) if row[0] <= target), None)

    for symbol, rows in (history_by_symbol or {}).items():
        symbol = str(symbol).upper()
        if not symbol.endswith("USDT"):
            continue
        base = symbol[:-4]
        if not base or base in excluded or base.endswith(leveraged) or len(rows) < 30:
            continue
        current = rows[-1]
        if now - current[0] > 5 or current[1] <= 0:
            continue
        points = {seconds: at_or_before(rows, now - seconds)
                  for seconds in (60, 300, 900, 3600, 1200, 360)}
        if any(points[seconds] is None for seconds in (60, 300, 900, 3600, 1200, 360)):
            continue
        price = current[1]
        m1 = (price / points[60][1] - 1) * 100 if points[60][1] else 0
        m5 = (price / points[300][1] - 1) * 100 if points[300][1] else 0
        m15 = (price / points[900][1] - 1) * 100 if points[900][1] else 0
        m60 = (price / points[3600][1] - 1) * 100 if points[3600][1] else 0
        latest_volume = max(0, current[2] - points[60][2])
        previous = []
        for end_seconds in (120, 180, 240, 300, 360):
            newer = at_or_before(rows, now - (end_seconds - 60))
            older = at_or_before(rows, now - end_seconds)
            if newer and older and newer[2] >= older[2]:
                previous.append(newer[2] - older[2])
        average_volume = sum(previous) / len(previous) if previous else 0
        rvol = latest_volume / average_volume if average_volume > 0 else 0
        lookback = [row[1] for row in rows if now - 1200 <= row[0] < now - 60]
        breakout = bool(lookback and price > max(lookback))
        bid, ask = current[3], current[4]
        spread = ((ask - bid) / price * 100) if bid > 0 and ask >= bid else None
        quote_volume_24h = current[2]
        if quote_volume_24h < 250000 or spread is None:
            continue
        output.append({
            "symbol": symbol, "source": "Binance TH", "price": price,
            "m1": m1, "m5": m5, "m15": m15, "m60": m60,
            "rvol": rvol, "relativeVolume": rvol, "breakout": breakout,
            "spreadPct": spread, "quoteVolume24h": quote_volume_24h,
            "momentumProxyPct": max(0, m1) + max(0, m5),
        })
    return output


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
    rvol = max(0.0, _n(candidate.get("rvol"), _n(candidate.get("relativeVolume"))))
    # A relative cross-market ranking proxy, not a forecast or promise of returns.
    edge_ratio = momentum / max(round_trip_cost_pct, 0.01)
    trend_points = min(40.0, edge_ratio * 8.0)
    volume_points = min(25.0, rvol / 4.0 * 25.0)
    breakout_points = 20.0 if bool(candidate.get("breakout")) else 0.0
    alignment_points = 15.0 if regime["allowEntry"] else 0.0
    opportunity_score = round(trend_points + volume_points + breakout_points + alignment_points, 2)
    return {
        **regime,
        "market": market,
        "symbol": str(candidate.get("symbol") or "").upper(),
        "decision": "PAPER_WATCH" if not allowed else "PAPER_CANDIDATE",
        "estimatedRoundTripCostPct": round(round_trip_cost_pct, 4),
        "momentumProxyPct": round(momentum, 4),
        "netEdgeProxyPct": round(momentum - round_trip_cost_pct, 4),
        "opportunityScore": opportunity_score if allowed else 0.0,
        "costFilterPassed": cost_ok,
        "entryAllowed": False,
        "note": "คะแนนใช้จัดอันดับสัญญาณข้ามตลาด ไม่ใช่การคาดการณ์หรือรับประกันผลตอบแทน",
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
