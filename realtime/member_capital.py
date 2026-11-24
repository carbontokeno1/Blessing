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


    def parse_command(command):
        import re
        raw = str(command or "").strip()
        if not raw or len(raw) > 500:
            raise HTTPException(status_code=400, detail="กรุณาพิมพ์คำสั่งไม่เกิน 500 ตัวอักษร")
        member_match = re.search(r"(?:สมาชิก\\s*)?(70[1-9]|710)\\b", raw)
        if not member_match:
            raise HTTPException(status_code=422, detail="กรุณาระบุหมายเลขสมาชิก 701–710")
        member_no = int(member_match.group(1))
        lower = raw.lower()
        market = None
        if any(x in lower for x in ("crypto", "คริปโต", "เหรียญ", "coin")):
            market = "crypto"
        if any(x in lower for x in ("หุ้น", "stock", "stocks")):
            if market:
                raise HTTPException(status_code=422, detail="พบทั้งคำว่าคริปโตและหุ้น กรุณาระบุตลาดเดียว")
            market = "stocks"
        mode = None
        if any(x in lower for x in ("เงินจริง", "live", "เงินจริง")):
            mode = "live"
        if any(x in lower for x in ("ทดลอง", "paper", "จำลอง")):
            if mode:
                raise HTTPException(status_code=422, detail="พบทั้ง Live และ Paper กรุณาระบุโหมดเดียว")
            mode = "paper"
        if any(x in lower for x in ("ตรวจสอบ", "ดูวงเงิน", "รายงานวงเงิน", "แสดงวงเงิน", "เช็กวงเงิน")):
            return {"kind":"report", "memberNo":member_no, "market":market, "mode":mode}
        if not market or not mode:
            missing = []
            if not market: missing.append("ตลาด (คริปโต หรือ หุ้น)")
            if not mode: missing.append("โหมด (เงินจริง/Live หรือ ทดลอง/Paper)")
            raise HTTPException(status_code=422, detail="คำสั่งยังไม่ชัดเจน กรุณาระบุ " + " และ ".join(missing))
        if any(x in lower for x in ("ไม่จำกัด", "unlimited", "ไม่มีลิมิต")):
            action, amount = "unlimited", None
        else:
            amount_match = re.search(r"([0-9][0-9,]*(?:\\.[0-9]+)?)\\s*(ล้าน|ล\\.?|พัน|k)?\\s*(?:บาท|thb)?", lower)
            if not amount_match:
                raise HTTPException(status_code=422, detail="กรุณาระบุจำนวนเงิน หรือใช้คำว่าไม่จำกัด")
            amount = float(amount_match.group(1).replace(",", ""))
            suffix = amount_match.group(2)
            if suffix in ("ล้าน", "ล.", "ล"): amount *= 1000000
            elif suffix == "พัน": amount *= 1000
            elif suffix == "k": amount *= 1000
            if not amount > 0 or amount > 1e15:
                raise HTTPException(status_code=422, detail="จำนวนเงินไม่อยู่ในช่วงที่รองรับ")
            if any(x in lower for x in ("เพิ่ม", "อีก")): action = "add"
            elif any(x in lower for x in ("ลด", "หัก")): action = "subtract"
            elif any(x in lower for x in ("ตั้ง", "เป็น", "เหลือ", "กำหนด")): action = "set"
            else:
                raise HTTPException(status_code=422, detail="กรุณาระบุการกระทำ: เพิ่ม, ลด หรือ ตั้งวงเงิน")
        return {"kind":"mutation", "memberNo":member_no, "market":market, "mode":mode,
                "action":action, "amount":amount, "reason":"Owner natural-language command"}

    @app.post("/admin/member-capital/command")
    async def admin_member_capital_command(request: Request):
        owner = owner_auth(request)
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="ข้อมูลคำสั่งไม่ถูกต้อง")
        parsed = parse_command(body.get("command"))
        if parsed["kind"] == "report":
            rows = sb_rest("blessing_member_portfolio_registry", {
                "select":"id,member_no,member_label,portfolio_type,market,mode,authorized_limit,allocated_capital,enabled,live_trading_approved,status,updated_at",
                "member_no":"eq." + str(parsed["memberNo"]),
                "order":"portfolio_type.asc", "limit":"4"
            }) or []
            if parsed.get("market"):
                rows = [r for r in rows if r.get("market") == parsed["market"]]
            if parsed.get("mode"):
                rows = [r for r in rows if r.get("mode") == parsed["mode"]]
            return {"ok":True,"kind":"report","memberNo":parsed["memberNo"],
                    "count":len(rows),"portfolios":rows,
                    "policy":"NULL authorized_limit means unlimited authorization; actual broker funds and buying power still apply.",
                    "liveExecutionEnabled":False}
        payload = {k:v for k,v in parsed.items() if k not in ("kind",)}
        # Reuse the same validated owner-only mutation path as the structured endpoint.
        member_no = payload["memberNo"]
        portfolio_type = payload["market"] + "_" + payload["mode"]
        rows = sb_rest("blessing_member_portfolio_registry", {
            "select":"id,member_no,portfolio_type,mode,authorized_limit,enabled,live_trading_approved",
            "member_no":"eq." + str(member_no), "portfolio_type":"eq." + portfolio_type, "limit":"1"
        }) or []
        if not rows:
            raise HTTPException(status_code=404, detail="ไม่พบพอร์ตของสมาชิกและประเภทที่ระบุ")
        portfolio = rows[0]
        result = sb_rpc("blessing_admin_set_member_capital_limit", {
            "p_portfolio_id":portfolio["id"], "p_action":payload["action"],
            "p_amount":str(payload["amount"]) if payload.get("amount") is not None else None,
            "p_actor":owner.get("id"), "p_reason":payload.get("reason"),
            "p_idempotency_key":str(uuid.uuid4())
        })
        if isinstance(result, dict):
            result["memberNo"] = member_no
            result["market"] = payload["market"]
            result["mode"] = payload["mode"]
            result["liveTradingApproved"] = bool(portfolio.get("live_trading_approved"))
            result["portfolioEnabled"] = bool(portfolio.get("enabled"))
            result["liveExecutionEnabled"] = False if payload["mode"] == "live" else None
        return {"ok":True,"kind":"mutation","parsedCommand":parsed,"result":result,
                "notice":"เปลี่ยนวงเงินอนุมัติเท่านั้น ไม่ได้เปิดใช้ Live Trading หรือเพิ่มเงินจริงในบัญชีโบรกเกอร์"}

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
