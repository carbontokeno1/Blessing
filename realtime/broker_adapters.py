"""Read-only brokerage account adapters for BLESSING.

This module intentionally contains no order-placement, cancel, transfer, or withdrawal
functions. Credentials are read only from server-side environment variables.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import time
from urllib.parse import urlencode
from typing import Any

import requests


def _masked(value: Any) -> str:
    raw = str(value or "")
    return ("••••" + raw[-4:]) if raw else ""


def _response_payload(response: Any) -> tuple[bool, Any, int]:
    status = int(getattr(response, "status_code", 0) or 0)
    try:
        payload = response.json()
    except Exception:
        payload = {}
    return 200 <= status < 300, payload, status


def _as_list(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("accounts", "account_list", "data", "items", "positions"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
            if isinstance(value, dict):
                for nested_key in ("accounts", "account_list", "items", "positions"):
                    nested = value.get(nested_key)
                    if isinstance(nested, list):
                        return [x for x in nested if isinstance(x, dict)]
    return []


def _binance_account_snapshot() -> dict:
    api_key = os.getenv("BINANCE_API_KEY", "").strip()
    api_secret = os.getenv("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        return {"status": "not_configured", "credentialsConfigured": False}

    base_url = os.getenv("BINANCE_API_BASE_URL", "https://api.binance.com").rstrip("/")
    params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
    query = urlencode(params)
    signature = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    try:
        response = requests.get(
            base_url + "/api/v3/account?" + query + "&signature=" + signature,
            headers={"X-MBX-APIKEY": api_key, "Accept": "application/json"},
            timeout=(4, 12),
        )
        ok, payload, status = _response_payload(response)
        if not ok or not isinstance(payload, dict):
            return {
                "status": "connection_error",
                "credentialsConfigured": True,
                "httpStatus": status,
                "apiCode": payload.get("code") if isinstance(payload, dict) else None,
            }
        # Verify actual API-key permissions separately from account-level canTrade.
        restrictions = {}
        restrictions_status = None
        try:
            rparams = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
            rquery = urlencode(rparams)
            rsig = hmac.new(api_secret.encode("utf-8"), rquery.encode("utf-8"), hashlib.sha256).hexdigest()
            rresponse = requests.get(
                base_url + "/sapi/v1/account/apiRestrictions?" + rquery + "&signature=" + rsig,
                headers={"X-MBX-APIKEY": api_key, "Accept": "application/json"},
                timeout=(4, 12),
            )
            rok, rpayload, restrictions_status = _response_payload(rresponse)
            if rok and isinstance(rpayload, dict):
                restrictions = rpayload
        except Exception:
            restrictions = {}

        balances = []
        for item in payload.get("balances", []) or []:
            try:
                free = float(item.get("free") or 0)
                locked = float(item.get("locked") or 0)
            except (TypeError, ValueError):
                continue
            if free > 0 or locked > 0:
                balances.append({
                    "asset": str(item.get("asset") or ""),
                    "free": free,
                    "locked": locked,
                    "total": free + locked,
                })
        trade_enabled = restrictions.get("enableSpotAndMarginTrading")
        withdrawal_enabled = restrictions.get("enableWithdrawals")
        ip_restricted = restrictions.get("ipRestrict")
        permissions_verified = bool(restrictions)
        warnings = []
        if not permissions_verified:
            warnings.append("ไม่สามารถอ่าน API key restrictions ได้ ต้องตรวจสอบสิทธิ์ใน Binance ด้วยตนเอง")
        if trade_enabled is not True:
            warnings.append("ยังไม่เปิดสิทธิ์ Spot Trading ให้ API Key")
        if withdrawal_enabled is not False:
            warnings.append("คำเตือน: ต้องปิดสิทธิ์ถอนเงินผ่าน API")
        if ip_restricted is not True:
            warnings.append("API Key ยังไม่ได้จำกัด IP; ควร whitelist IP ขาออกของเซิร์ฟเวอร์ก่อนเปิดซื้อขาย")
        if payload.get("canTrade") is not True:
            warnings.append("บัญชี Spot ยังไม่ยืนยันสิทธิ์ซื้อขาย")
        return {
            "status": "connected",
            "credentialsConfigured": True,
            "accountType": payload.get("accountType"),
            "accountCanTrade": payload.get("canTrade"),
            "accountCanWithdraw": payload.get("canWithdraw"),
            "apiKeyPermissions": {
                "spotTrading": trade_enabled,
                "withdrawals": withdrawal_enabled,
                "ipRestricted": ip_restricted,
                "reading": restrictions.get("enableReading"),
                "margin": restrictions.get("enableMargin"),
                "futures": restrictions.get("enableFutures"),
                "internalTransfer": restrictions.get("enableInternalTransfer"),
                "universalTransfer": restrictions.get("permitsUniversalTransfer"),
                "verified": permissions_verified,
                "httpStatus": restrictions_status,
            },
            "liveTradingPermissionReady": (
                permissions_verified and trade_enabled is True
                and withdrawal_enabled is False and ip_restricted is True
                and payload.get("canTrade") is True
            ),
            "securityWarnings": warnings,
            "balances": balances,
            "permissionMode": "spot_trade_no_withdrawal",
        }
    except Exception as exc:
        return {
            "status": "connection_error",
            "credentialsConfigured": True,
            "errorType": type(exc).__name__,
        }


def _webull_readonly() -> dict:
    app_key = os.getenv("WEBULL_APP_KEY", "").strip()
    app_secret = os.getenv("WEBULL_APP_SECRET", "").strip()
    if not app_key or not app_secret:
        return {"status": "not_configured", "credentialsConfigured": False}

    region = os.getenv("WEBULL_REGION", "us").strip().lower()
    endpoint = os.getenv("WEBULL_HTTP_HOST", "api.webull.com").strip()
    try:
        from webull.core.client import ApiClient
        from webull.trade.trade_client import TradeClient

        api_client = ApiClient(app_key, app_secret, region)
        api_client.add_endpoint(region, endpoint)
        trade_client = TradeClient(api_client)
        account_response = trade_client.account_v2.get_account_list()
        ok, account_payload, status = _response_payload(account_response)
        if not ok:
            return {
                "status": "connection_error",
                "credentialsConfigured": True,
                "region": region,
                "httpStatus": status,
            }

        accounts = []
        for account in _as_list(account_payload)[:10]:
            account_id = account.get("account_id") or account.get("accountId")
            if not account_id:
                continue
            item = {
                "accountId": _masked(account_id),
                "accountType": account.get("account_type") or account.get("accountType"),
                "balance": None,
                "positions": [],
            }
            try:
                balance_response = trade_client.account_v2.get_account_balance(str(account_id))
                balance_ok, balance_payload, balance_status = _response_payload(balance_response)
                item["balance"] = balance_payload if balance_ok else {"status": "unavailable", "httpStatus": balance_status}
            except Exception as exc:
                item["balance"] = {"status": "unavailable", "errorType": type(exc).__name__}
            try:
                position_response = trade_client.account_v2.get_account_position(str(account_id))
                position_ok, position_payload, position_status = _response_payload(position_response)
                item["positions"] = _as_list(position_payload) if position_ok else []
                if not position_ok:
                    item["positionsStatus"] = "unavailable"
                    item["positionsHttpStatus"] = position_status
            except Exception as exc:
                item["positionsStatus"] = "unavailable"
                item["positionsErrorType"] = type(exc).__name__
            accounts.append(item)

        return {
            "status": "connected" if accounts else "connected_no_accounts",
            "credentialsConfigured": True,
            "region": region,
            "accounts": accounts,
            "permissionMode": "read_only_required",
        }
    except Exception as exc:
        return {
            "status": "connection_error",
            "credentialsConfigured": True,
            "region": region,
            "errorType": type(exc).__name__,
        }


def brokerage_readonly_snapshot() -> dict:
    """Return sanitized account snapshots. Never sends a trading instruction."""
    return {
        "mode": "account_permission_check",
        "liveOrdersEnabled": False,
        "checkedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "binance": _binance_account_snapshot(),
        "webull": _webull_readonly(),
    }
