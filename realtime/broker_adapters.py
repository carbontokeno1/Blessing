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
    # Binance TH documents account information at /api/v1/accountV2 and
    # authenticates this read endpoint with X-MBX-APIKEY. Do not call Binance
    # Global /sapi routes from a Binance TH account integration.
    api_key = (os.getenv("BINANCE_TH_API_KEY") or os.getenv("BINANCE_API_KEY") or "").strip()
    if not api_key:
        return {"status": "not_configured", "credentialsConfigured": False}

    base_url = (os.getenv("BINANCE_TH_API_BASE_URL") or os.getenv("BINANCE_API_BASE_URL") or "https://api.binance.th").rstrip("/")
    params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
    try:
        response = requests.get(
            base_url + "/api/v1/accountV2",
            params=params,
            headers={"X-MBX-APIKEY": api_key, "Accept": "application/json"},
            timeout=(4, 12),
        )
        ok, raw_payload, status = _response_payload(response)
        if not ok or not isinstance(raw_payload, dict):
            return {
                "status": "connection_error",
                "credentialsConfigured": True,
                "httpStatus": status,
                "apiCode": raw_payload.get("code") if isinstance(raw_payload, dict) else None,
            }

        # Binance TH may return the documented object directly or wrap it in
        # {code, msg, timestamp, data}. Fail closed on an explicit API error.
        if "code" in raw_payload and raw_payload.get("code") not in (0, "0", None):
            return {
                "status": "connection_error",
                "credentialsConfigured": True,
                "httpStatus": status,
                "apiCode": raw_payload.get("code"),
                "apiMessage": str(raw_payload.get("msg") or "")[:180],
            }
        payload = raw_payload.get("data") if isinstance(raw_payload.get("data"), dict) else raw_payload

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

        # /accountV2 confirms account data and account-level canTrade/canWithdraw.
        # It does not prove API-key withdrawal restrictions. Keep live readiness
        # false until key permissions are independently verified in the exchange UI.
        warnings = [
            "เชื่อมต่อ Binance TH แบบอ่านข้อมูลบัญชีเท่านั้น",
            "ยังยืนยันสิทธิ์ API Key สำหรับการซื้อขายและการถอนจาก endpoint นี้ไม่ได้",
            "คำสั่งซื้อขายจริงถูกปิดไว้จนกว่าจะตรวจสอบสิทธิ์และความปลอดภัยแยกต่างหาก",
        ]
        return {
            "status": "connected",
            "credentialsConfigured": True,
            "broker": "binance_th",
            "accountCanTrade": payload.get("canTrade"),
            "accountCanWithdraw": payload.get("canWithdraw"),
            "accountCanDeposit": payload.get("canDeposit"),
            "apiKeyPermissions": {
                "spotTrading": None,
                "withdrawals": None,
                "ipRestricted": None,
                "verified": False,
                "verificationSource": "not_available_from_accountV2",
            },
            "liveTradingPermissionReady": False,
            "securityWarnings": warnings,
            "balances": balances,
            "permissionMode": "read_only_snapshot",
        }
    except Exception as exc:
        return {
            "status": "connection_error",
            "credentialsConfigured": True,
            "broker": "binance_th",
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
