import requests
from datetime import datetime, timezone
from fastapi import HTTPException, Request

SAFE_MEMBER_PAGES = {"overview", "reports", "history", "settings"}
DEFAULT_MEMBER_PAGES = ["overview", "settings"]
PORTFOLIO_TYPES = {"crypto_paper", "stock_paper", "crypto_live", "stock_live"}


def register_member_access_routes(app, owner_auth, sb_rest, supabase_url, service_key):
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
            "select": "member_no,member_label,assigned_user_id,portfolio_type,authorized_limit,allocated_capital,enabled,status,live_trading_approved",
            "order": "member_no.asc,portfolio_type.asc", "limit": "100"
        }) or []
        user_ids = list(dict.fromkeys(str(x["assigned_user_id"]) for x in registry if x.get("assigned_user_id")))
        profiles = sb_rest("profiles", {
            "select": "user_id,email,display_name,status", "user_id": "in.(" + ",".join(user_ids) + ")",
            "role": "eq.member", "limit": "100"
        }) if user_ids else []
        settings = sb_rest("blessing_member_access_settings", {
            "select": "user_id,visible_pages,updated_at", "user_id": "in.(" + ",".join(user_ids) + ")",
            "limit": "100"
        }) if user_ids else []
        profile_map = {str(x["user_id"]): x for x in (profiles or [])}
        setting_map = {str(x["user_id"]): x for x in (settings or [])}
        grouped = {}
        for row in registry:
            uid = str(row.get("assigned_user_id") or "")
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
