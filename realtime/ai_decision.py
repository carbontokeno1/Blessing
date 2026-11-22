"""BLESSING AI decision preview and paper-risk policy.

Deterministic, explainable signal ranking only. This module does not place orders.
All outputs are recommendations for Paper Trading review, not investment advice.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any


def _number(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if result == result and abs(result) != float("inf") else default
    except (TypeError, ValueError):
        return default


def risk_policy() -> dict:
    return {
        "mode": "paper_only",
        "liveOrdersEnabled": False,
        "riskPerTradePct": max(0.1, min(1.0, _number(os.getenv("BLESSING_RISK_PER_TRADE_PCT"), 0.5))),
        "maxDailyLossPct": max(0.5, min(3.0, _number(os.getenv("BLESSING_MAX_DAILY_LOSS_PCT"), 2.0))),
        "maxDrawdownPct": max(1.0, min(7.5, _number(os.getenv("BLESSING_MAX_DRAWDOWN_PCT"), 5.0))),
        "reserveCashPct": max(10.0, min(50.0, _number(os.getenv("BLESSING_RESERVE_CASH_PCT"), 20.0))),
        "maxOpenPositions": max(1, min(10, int(_number(os.getenv("BLESSING_MAX_OPEN_POSITIONS"), 5)))),
        "requireFreshMarketData": True,
        "requireBinanceCandlesForCryptoEntry": True,
        "stopOnStalePrice": True,
    }


def _candidate_score(item: dict) -> tuple[float, list[str]]:
    score = _number(item.get("score"), _number(item.get("signalScore"), 0.0))
    reasons = []
    change = _number(item.get("change24h"), _number(item.get("priceChangePercent"), 0.0))
    volume = _number(item.get("quoteVolume"), _number(item.get("volume24h"), 0.0))
    rvol = _number(item.get("rvol"), 0.0)
    spread = _number(item.get("spreadPct"), 999.0)

    if change > 0:
        reasons.append("โมเมนตัม 24 ชั่วโมงเป็นบวก")
    if volume >= 1_000_000:
        reasons.append("สภาพคล่อง 24 ชั่วโมงอยู่ในเกณฑ์")
    if rvol >= 1.5:
        reasons.append("ปริมาณซื้อขายสูงกว่าค่าเฉลี่ย")
    if 0 <= spread <= 0.25:
        reasons.append("ส่วนต่างราคาเสนอซื้อขายไม่สูง")
    if item.get("breakout") or item.get("isBreakout"):
        reasons.append("ผ่านเงื่อนไข Breakout")
    return max(0.0, min(100.0, score)), reasons


def build_decision_preview(scanner_payload: dict, open_positions: int = 0,
                           daily_loss_pct: float = 0.0,
                           drawdown_pct: float = 0.0) -> dict:
    policy = risk_policy()
    provider = str(scanner_payload.get("provider") or "unknown")
    candidates = scanner_payload.get("candidates") or []
    source_is_executable = provider.lower() == "binance"
    fresh = scanner_payload.get("status") not in ("source_error", "error") and bool(scanner_payload.get("updatedAt"))
    halt_reasons = []
    if not fresh:
        halt_reasons.append("ข้อมูลตลาดไม่พร้อมหรือไม่มีเวลาปรับปรุงล่าสุด")
    if not source_is_executable:
        halt_reasons.append("แหล่งข้อมูลสำรองใช้แสดงผลเท่านั้น ไม่อนุญาตเปิดสถานะ Paper")
    if open_positions >= policy["maxOpenPositions"]:
        halt_reasons.append("ถึงจำนวนสถานะเปิดสูงสุด")
    if daily_loss_pct >= policy["maxDailyLossPct"]:
        halt_reasons.append("ขาดทุนรายวันถึงขีดจำกัด")
    if drawdown_pct >= policy["maxDrawdownPct"]:
        halt_reasons.append("Drawdown ถึงขีดจำกัด")

    ranked = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or item.get("pair") or "").upper()
        score, reasons = _candidate_score(item)
        eligible = (
            bool(symbol) and symbol.endswith("USDT") and score >= 70
            and source_is_executable and fresh and not halt_reasons
        )
        ranked.append({
            "symbol": symbol,
            "score": round(score, 2),
            "decision": "PAPER_CANDIDATE" if eligible else "WATCH",
            "reasons": reasons,
            "entryAllowed": False,
            "note": "ต้องผ่านตัวตรวจสอบคำสั่ง Paper และตรวจราคาสดอีกครั้ง" if eligible else "ยังไม่ผ่านเกณฑ์เปิดสถานะ",
        })
    ranked.sort(key=lambda x: x["score"], reverse=True)

    return {
        "ok": True,
        "engine": "BLESSING_EXPLAINABLE_DECISION_PREVIEW",
        "mode": "paper_only",
        "liveOrdersEnabled": False,
        "provider": provider,
        "marketDataFresh": fresh,
        "entryGateOpen": False,
        "haltReasons": halt_reasons,
        "policy": policy,
        "summary": {
            "candidatesReviewed": len(ranked),
            "paperCandidates": sum(1 for x in ranked if x["decision"] == "PAPER_CANDIDATE"),
            "watchlist": sum(1 for x in ranked if x["decision"] == "WATCH"),
            "openPositions": int(open_positions),
            "dailyLossPct": round(_number(daily_loss_pct), 3),
            "drawdownPct": round(_number(drawdown_pct), 3),
        },
        "decisions": ranked[:30],
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "disclaimer": "ระบบจัดอันดับเพื่อทดสอบ Paper เท่านั้น ไม่ได้ส่งคำสั่งซื้อขายและไม่รับประกันผลตอบแทน",
    }
