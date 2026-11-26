import requests
from datetime import datetime, timezone
from fastapi import HTTPException, Request

SAFE_MEMBER_PAGES = {"overview", "portfolio", "settings"}
DEFAULT_MEMBER_PAGES = ["overview", "portfolio", "settings"]
PORTFOLIO_TYPES = {"crypto_paper", "stock_paper", "crypto_live", "stock_live"}


def register_member_access_routes(app, owner_auth, sb_rest, sb_rpc, supabase_url, service_key):
    def member_user(request: Request):
        if not (supabase_url and service_key):
            raise HTTPException(status_code=503, detail="Render ยังไม่ได้ตั้งค่า Supabase")
        authorization = request.headers.get("authorization", "")
        if not authorization.lower().startswith("bearer "):
            raise HTTPException(status_code=401, detail="กรุณาเข้าสู่ระบบก่อน")
        token = authorization.split(" ", 1)[1].strip()
        response = requests.get(
            supabase_url + "/auth/v1/user",
            headers={"apikey": service_key, "Authorization": "Bearer " + token},
            timeout=(5, 12),
        )
        if response.status_code != 200:
            raise HTTPException(status_code=401, detail="เซสชันหมดอายุ กรุณาเข้าสู่ระบบใหม่")
        user = response.json()
        profiles = sb_rest("profiles", {
            "select": "role,status", "user_id": "eq." + str(user.get("id")), "limit": "1"
        })
        if not profiles or profiles[0].get("role") != "member" or profiles[0].get("status") != "active":
            raise HTTPException(status_code=403, detail="บัญชีสมาชิกยังไม่ได้รับอนุมัติ")
        return user

    @app.get("/admin/member-access")
    def admin_get_member_access(request: Request):
        owner_auth(request)
        registry = sb_rest("blessing_member_portfolio_registry", {
            "select": "id,member_no,member_label,assigned_user_id,portfolio_type,authorized_limit,allocated_capital,enabled,status,live_trading_approved",
            "order": "member_no.asc,portfolio_type.asc", "limit": "200"
        }) or []
        # Live rows are intentionally unassigned/locked. Pair them to the
        # member's Paper rows by member_no for display and limit management only.
        member_user_by_no = {}
        for row in registry:
            uid = str(row.get("assigned_user_id") or "")
            if uid and row.get("member_no") is not None:
                member_user_by_no[str(row["member_no"])] = uid
        row_user = {}
        for row in registry:
            uid = str(row.get("assigned_user_id") or "")
            if not uid and str(row.get("portfolio_type") or "").endswith("_live"):
                uid = member_user_by_no.get(str(row.get("member_no")), "")
            if uid:
                row_user[id(row)] = uid
        user_ids = list(dict.fromkeys(row_user.values()))
        profiles = sb_rest("profiles", {
            "select": "user_id,email,display_name,status", "user_id": "in.(" + ",".join(user_ids) + ")",
            "role": "eq.member", "limit": "200"
        }) if user_ids else []
        settings = sb_rest("blessing_member_access_settings", {
            "select": "user_id,visible_pages,updated_at", "user_id": "in.(" + ",".join(user_ids) + ")",
            "limit": "200"
        }) if user_ids else []
        profile_map = {str(x["user_id"]): x for x in (profiles or [])}
        setting_map = {str(x["user_id"]): x for x in (settings or [])}
        grouped = {}
        for row in registry:
            uid = row_user.get(id(row), "")
            if not uid or uid not in profile_map:
                continue
            m = grouped.setdefault(uid, {
                "userId": uid, "memberNo": row.get("member_no"),
                "name": profile_map[uid].get("display_name") or row.get("member_label"),
                "email": profile_map[uid].get("email"),
                "status": profile_map[uid].get("status"),
                "visiblePages": (setting_map.get(uid) or {}).get("visible_pages") or DEFAULT_MEMBER_PAGES,
                "portfolios": []
            })
            m["portfolios"].append({
                "portfolioId": row.get("id"),
                "portfolioType": row.get("portfolio_type"),
                "authorizedLimit": row.get("authorized_limit"),
                "allocatedCapital": row.get("allocated_capital"),
                "enabled": row.get("enabled"),
                "status": row.get("status"),
                "liveTradingApproved": row.get("live_trading_approved")
            })
        return {"ok": True, "safePages": sorted(SAFE_MEMBER_PAGES), "members": list(grouped.values())}

    @app.put("/admin/member-access")
    async def admin_set_member_access(request: Request):
        owner = owner_auth(request)
        try:
            payload = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="ข้อมูลไม่ถูกต้อง")
        user_id = str(payload.get("userId") or "").strip()
        pages = payload.get("visiblePages")
        if not user_id or not isinstance(pages, list):
            raise HTTPException(status_code=400, detail="ต้องระบุสมาชิกและรายการหน้าที่อนุญาต")
        clean = list(dict.fromkeys(str(x) for x in pages if str(x) in SAFE_MEMBER_PAGES))
        if "overview" not in clean:
            clean.insert(0, "overview")
        profiles = sb_rest("profiles", {
            "select": "user_id", "user_id": "eq." + user_id, "role": "eq.member", "limit": "1"
        }) or []
        if not profiles:
            raise HTTPException(status_code=404, detail="ไม่พบบัญชีสมาชิก")
        existing = sb_rest("blessing_member_access_settings", {
            "select": "user_id", "user_id": "eq." + user_id, "limit": "1"
        }) or []
        values = {"visible_pages": clean, "updated_by": owner.get("id"), "updated_at": datetime.now(timezone.utc).isoformat()}
        if existing:
            result = sb_rest("blessing_member_access_settings", {
                "user_id": "eq." + user_id
            }, method="PATCH", body=values)
        else:
            result = sb_rest("blessing_member_access_settings", {}, method="POST", body={
                "user_id": user_id, **values
            })
        return {"ok": True, "userId": user_id, "visiblePages": clean, "saved": bool(result)}

    @app.put("/admin/member-access/portfolio-limit")
    async def admin_set_member_portfolio_limit(request: Request):
        owner = owner_auth(request)
        try:
            payload = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="ข้อมูลไม่ถูกต้อง")
        portfolio_id = str(payload.get("portfolioId") or "").strip()
        amount = payload.get("limit")
        if not portfolio_id:
            raise HTTPException(status_code=400, detail="ต้องระบุพอร์ต")
        rows = sb_rest("blessing_member_portfolio_registry", {
            "select": "id,member_no,portfolio_type,assigned_user_id,authorized_limit,base_currency,live_trading_approved,enabled",
            "id": "eq." + portfolio_id, "limit": "1"
        }) or []
        if not rows:
            raise HTTPException(status_code=404, detail="ไม่พบพอร์ต")
        if amount is None or str(amount).strip() == "":
            action, normalized = "unlimited", None
        else:
            try:
                from decimal import Decimal, InvalidOperation
                value = Decimal(str(amount))
                if not value.is_finite() or value <= 0:
                    raise InvalidOperation()
                normalized = format(value, "f")
                action = "set"
            except (InvalidOperation, TypeError, ValueError):
                raise HTTPException(status_code=400, detail="วงเงินต้องเป็นตัวเลขมากกว่า 0 หรือเว้นว่างเพื่อไม่จำกัด")
        result = sb_rpc("blessing_admin_set_member_capital_limit", {
            "p_portfolio_id": portfolio_id, "p_action": action, "p_amount": normalized,
            "p_actor": owner.get("id"), "p_reason": "Owner per-member portfolio limit",
            "p_idempotency_key": __import__("uuid").uuid4().hex
        })
        return {"ok": True, "portfolioId": portfolio_id, "portfolioType": rows[0].get("portfolio_type"),
                "limit": (result.get("authorizedLimit", result.get("newLimit")) if isinstance(result, dict) else normalized),
                "result": result, "liveOrdersStillLocked": str(rows[0].get("portfolio_type", "")).endswith("_live")}

    @app.get("/member/member-access")
    def member_get_access(request: Request):
        user = member_user(request)
        rows = sb_rest("blessing_member_access_settings", {
            "select": "visible_pages,updated_at", "user_id": "eq." + str(user["id"]), "limit": "1"
        }) or []
        pages = (rows[0].get("visible_pages") if rows else None) or DEFAULT_MEMBER_PAGES
        clean = [x for x in pages if x in SAFE_MEMBER_PAGES]
        if "overview" not in clean:
            clean.insert(0, "overview")
        return {"ok": True, "visiblePages": clean}
