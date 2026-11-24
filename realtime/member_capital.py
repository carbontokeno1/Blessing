import uuid
from decimal import Decimal, InvalidOperation

from fastapi import HTTPException, Request


def register_member_capital_routes(app, owner_auth, sb_rest, sb_rpc):
    """Owner-only capital authorization controls. NULL means no fixed system ceiling."""

    @app.post("/admin/member-capital/limit")
    async def admin_set_member_capital_limit(request: Request):
        owner = owner_auth(request)
        try:
            payload = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="ข้อมูลคำสั่งไม่ถูกต้อง")

        try:
            member_no = int(payload.get("memberNo"))
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="กรุณาระบุหมายเลขสมาชิก 701–710")

        if member_no < 701 or member_no > 710:
            raise HTTPException(status_code=400, detail="หมายเลขสมาชิกต้องอยู่ระหว่าง 701–710")

        market = str(payload.get("market") or "").strip().lower()
        mode = str(payload.get("mode") or "").strip().lower()
        action = str(payload.get("action") or "").strip().lower()
        if market not in ("crypto", "stocks") or mode not in ("paper", "live"):
            raise HTTPException(status_code=400, detail="market ต้องเป็น crypto/stocks และ mode ต้องเป็น paper/live")
        if action not in ("set", "add", "subtract", "unlimited"):
            raise HTTPException(status_code=400, detail="action ต้องเป็น set/add/subtract/unlimited")

        amount = None
        if action != "unlimited":
            try:
                amount_decimal = Decimal(str(payload.get("amount")))
                if not amount_decimal.is_finite() or amount_decimal <= 0:
                    raise InvalidOperation()
                amount = format(amount_decimal, "f")
            except (InvalidOperation, TypeError, ValueError):
                raise HTTPException(status_code=400, detail="จำนวนวงเงินต้องเป็นตัวเลขมากกว่า 0")

        portfolio_type = market + "_" + mode
        rows = sb_rest("blessing_member_portfolio_registry", {
            "select": "id,member_no,portfolio_type,mode,authorized_limit,enabled,live_trading_approved",
            "member_no": "eq." + str(member_no),
            "portfolio_type": "eq." + portfolio_type,
            "limit": "1",
        }) or []
        if not rows:
            raise HTTPException(status_code=404, detail="ไม่พบพอร์ตของสมาชิกและประเภทที่ระบุ")
        portfolio = rows[0]

        # A limit authorization never enables a portfolio or approves Live trading.
        result = sb_rpc("blessing_admin_set_member_capital_limit", {
            "p_portfolio_id": portfolio["id"],
            "p_action": action,
            "p_amount": amount,
            "p_actor": owner.get("id"),
            "p_reason": str(payload.get("reason") or "Owner capital authorization"),
            "p_idempotency_key": str(payload.get("idempotencyKey") or uuid.uuid4()),
        })
        if isinstance(result, dict):
            result["liveTradingApproved"] = bool(portfolio.get("live_trading_approved"))
            result["portfolioEnabled"] = bool(portfolio.get("enabled"))
            result["liveExecutionEnabled"] = False if mode == "live" else None
        return result

    @app.get("/admin/member-capital/report")
    def admin_member_capital_report(request: Request):
        owner_auth(request)
        rows = sb_rest("blessing_member_portfolio_registry", {
            "select": "id,member_no,member_label,portfolio_type,market,mode,authorized_limit,allocated_capital,enabled,live_trading_approved,status,updated_at",
            "order": "member_no.asc,portfolio_type.asc",
            "limit": "100",
        }) or []
        return {
            "ok": True,
            "policy": "No fixed system ceiling. NULL authorized_limit means unlimited authorization.",
            "actualFundsRule": "Trading remains limited by real available cash, broker buying power, and risk controls.",
            "liveExecutionEnabled": False,
            "portfolios": rows,
        }
