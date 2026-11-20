import asyncio
import base64
import hashlib
import hmac
import urllib.parse
import requests
import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("blessing-realtime")

SYMBOLS = [s.strip().upper() for s in os.getenv("WEBULL_SYMBOLS", "AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA,AMD").split(",") if s.strip()]
APP_KEY = os.getenv("WEBULL_APP_KEY", "")
APP_SECRET = os.getenv("WEBULL_APP_SECRET", "")
REGION = os.getenv("WEBULL_REGION", "us")
HTTP_HOST = os.getenv("WEBULL_HTTP_HOST", "api.webull.com")
MQTT_HOST = os.getenv("WEBULL_MQTT_HOST", "data-api.webull.com")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "https://blessing.carbontokeno1.workers.dev").split(",")

app = FastAPI(title="Blessing Webull Market Data Bridge", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=[x.strip() for x in ALLOWED_ORIGINS if x.strip()], allow_credentials=False, allow_methods=["GET"], allow_headers=["*"])

state = {
    "provider": "webull",
    "connection": "waiting_credentials",
    "marketDataEntitlement": "not_verified",
    "symbols": SYMBOLS,
    "lastMessageAt": None,
    "lastError": None,
    "quotes": {},
}
clients: set[WebSocket] = set()
loop: asyncio.AbstractEventLoop | None = None
stream_thread: threading.Thread | None = None


def plain(value: Any, depth: int = 0) -> Any:
    if depth > 5:
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): plain(v, depth + 1) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [plain(v, depth + 1) for v in value]
    if hasattr(value, "to_dict"):
        try:
            return plain(value.to_dict(), depth + 1)
        except Exception:
            pass
    if hasattr(value, "__dict__"):
        try:
            return {str(k): plain(v, depth + 1) for k, v in vars(value).items() if not str(k).startswith("_")}
        except Exception:
            pass
    if hasattr(value, "DESCRIPTOR"):  # protobuf message
        try:
            from google.protobuf.json_format import MessageToDict
            return plain(MessageToDict(value, preserving_proto_field_name=True), depth + 1)
        except Exception:
            pass
    return str(value)


def extract_symbol(obj: Any, data: Any) -> str | None:
    candidates = []
    if isinstance(data, dict):
        candidates += [data.get("symbol"), data.get("ticker"), data.get("ticker_symbol")]
    if isinstance(obj, dict):
        candidates += [obj.get("symbol"), obj.get("ticker"), obj.get("ticker_symbol")]
    for candidate in candidates:
        if candidate:
            symbol = str(candidate).upper()
            if symbol in SYMBOLS:
                return symbol
    return None


def publish_quote(topic: str, payload: Any) -> None:
    data = plain(payload)
    items = data if isinstance(data, list) else [data]
    now = datetime.now(timezone.utc).isoformat()
    changed = False
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = extract_symbol(item, data)
        if not symbol:
            # Some SDK versions wrap the quote in a nested object.
            for key in ("quote", "snapshot", "tick", "data", "basic"):
                nested = item.get(key)
                if isinstance(nested, dict):
                    symbol = extract_symbol(nested, nested)
                    if symbol:
                        item = {**item, **nested}
                        break
        if not symbol:
            continue
        state["quotes"][symbol] = {
            "symbol": symbol,
            "topic": topic,
            "receivedAt": now,
            "raw": item,
        }
        changed = True
    if changed:
        state["lastMessageAt"] = now
        state["connection"] = "streaming"


async def broadcast(message: dict) -> None:
    dead = []
    encoded = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
    for ws in list(clients):
        try:
            await ws.send_text(encoded)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


def on_message(client, topic, quotes):
    publish_quote(str(topic), quotes)
    if loop and loop.is_running():
        asyncio.run_coroutine_threadsafe(broadcast({"type": "quotes", "data": state["quotes"], "status": public_status()}), loop)


def on_connect(client, api_client, session_id):
    log.info("Webull market stream connected")
    state["connection"] = "connected"
    state["lastError"] = None
    try:
        from webull.data.common.category import Category
        from webull.data.common.subscribe_type import SubscribeType
        client.subscribe(SYMBOLS, Category.US_STOCK.name, [SubscribeType.QUOTE.name, SubscribeType.SNAPSHOT.name, SubscribeType.TICK.name])
        log.info("Requested subscriptions for %d symbols", len(SYMBOLS))
    except Exception as exc:
        state["connection"] = "subscription_error"
        state["lastError"] = type(exc).__name__
        log.exception("Webull subscription failed")


def stream_main():
    try:
        from webull.data.data_streaming_client import DataStreamingClient
        client = DataStreamingClient(
            APP_KEY, APP_SECRET, REGION, uuid.uuid4().hex,
            http_host=HTTP_HOST, mqtt_host=MQTT_HOST
        )
        client.on_connect_success = on_connect
        client.on_quotes_message = on_message
        log.info("Starting Webull MQTT stream for %s", ",".join(SYMBOLS))
        client.connect_and_loop_forever()
    except Exception as exc:
        state["connection"] = "error"
        state["lastError"] = type(exc).__name__
        log.exception("Webull stream stopped")


def public_status() -> dict:
    return {
        "provider": state["provider"],
        "connection": state["connection"],
        "marketDataEntitlement": state["marketDataEntitlement"],
        "symbols": state["symbols"],
        "quoteCount": len(state["quotes"]),
        "lastMessageAt": state["lastMessageAt"],
        "lastError": state["lastError"],
    }


@app.on_event("startup")
async def startup():
    global loop, stream_thread
    loop = asyncio.get_running_loop()
    if not APP_KEY or not APP_SECRET:
        state["connection"] = "waiting_credentials"
        log.warning("Webull credentials are not configured; no market data will be fabricated.")
        return
    state["connection"] = "connecting"
    stream_thread = threading.Thread(target=stream_main, name="webull-mqtt", daemon=True)
    stream_thread.start()



# Read-only US stock momentum scanner. Credentials are server-side only.
WEBULL_HOST = os.getenv("WEBULL_HTTP_HOST", "api.webull.com")
WEBULL_ACCESS_TOKEN = os.getenv("WEBULL_ACCESS_TOKEN", "")
SCANNER_CACHE: dict[str, Any] = {"at": 0.0, "payload": None}
SCANNER_CACHE_SECONDS = 20


def webull_signature(path: str, params: dict, timestamp: str, nonce: str) -> str:
    signing = {
        **{str(k): str(v) for k, v in params.items()},
        "x-app-key": APP_KEY,
        "x-timestamp": timestamp,
        "x-signature-algorithm": "HMAC-SHA1",
        "x-signature-version": "1.0",
        "x-signature-nonce": nonce,
        "host": WEBULL_HOST,
    }
    raw = path + "&" + "&".join(f"{k}={signing[k]}" for k in sorted(signing))
    encoded = urllib.parse.quote(raw, safe="")
    digest = hmac.new((APP_SECRET + "&").encode(), encoded.encode(), hashlib.sha1).digest()
    return base64.b64encode(digest).decode()


def webull_screener(rank_type: str) -> list[dict]:
    path = "/market-data/screeners/gainers-losers/list"
    params = {"rank_type": rank_type, "category": "US_STOCK", "sort_by": "CHANGE_RATIO", "direction": "DESC"}
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    nonce = uuid.uuid4().hex
    headers = {
        "Accept": "application/json",
        "x-app-key": APP_KEY,
        "x-timestamp": timestamp,
        "x-signature": webull_signature(path, params, timestamp, nonce),
        "x-signature-algorithm": "HMAC-SHA1",
        "x-signature-version": "1.0",
        "x-signature-nonce": nonce,
        "x-version": "v3",
        "x-access-token": WEBULL_ACCESS_TOKEN,
    }
    response = requests.get(f"https://{WEBULL_HOST}{path}", params=params, headers=headers, timeout=15)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict):
        payload = payload.get("data", payload.get("list", []))
    return payload if isinstance(payload, list) else []


def build_momentum_scanner() -> dict:
    missing = [name for name, value in (
        ("WEBULL_APP_KEY", APP_KEY), ("WEBULL_APP_SECRET", APP_SECRET),
        ("WEBULL_ACCESS_TOKEN", WEBULL_ACCESS_TOKEN),
    ) if not value]
    if missing:
        return {
            "provider": "Webull OpenAPI", "status": "waiting_credentials",
            "message": "รอ Webull อนุมัติ App Key / App Secret และตั้งค่า Access Token ใน Render",
            "missing": missing, "scanned": 0, "candidates": [],
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        }

    merged: dict[str, dict] = {}
    errors = []
    for rank_type in ("PRE_MARKET", "MIN_3", "MIN_5"):
        try:
            for row in webull_screener(rank_type):
                symbol = str(row.get("symbol", "")).upper()
                if not symbol:
                    continue
                item = merged.setdefault(symbol, {"symbol": symbol, "name": row.get("name", symbol)})
                item[rank_type.lower()] = row
        except Exception as exc:
            errors.append(f"{rank_type}:{type(exc).__name__}")

    candidates = []
    for symbol, item in merged.items():
        pre = item.get("pre_market", {})
        m3 = item.get("min_3", {})
        m5 = item.get("min_5", {})
        price = float(pre.get("price") or m3.get("price") or m5.get("price") or 0)
        volume = int(float(pre.get("volume") or m3.get("volume") or m5.get("volume") or 0))
        pre_pct = float(pre.get("change_ratio") or 0) * 100
        m3_pct = float(m3.get("change_ratio") or 0) * 100
        m5_pct = float(m5.get("change_ratio") or 0) * 100
        rvol = float(pre.get("relative_volume_10d") or m3.get("relative_volume_10d") or m5.get("relative_volume_10d") or 0)
        if price < 2 or volume < 100000:
            continue
        if not (pre_pct >= 2 or m3_pct >= 0.8 or m5_pct >= 1.2):
            continue
        # Entry is a confirmation trigger, not a promise that the order will fill.
        entry = round(price * 1.001, 4)
        target_pct = min(10.0, max(3.0, 3.0 + max(m3_pct, m5_pct, 0) * 0.8 + min(rvol, 5) * 0.4))
        stop_pct = 2.5
        fast = pre_pct >= 3 and m3_pct >= 0.8 and m5_pct >= 1.2 and rvol >= 2
        score = min(100, round(max(pre_pct, 0) * 3 + max(m3_pct, 0) * 8 + max(m5_pct, 0) * 5 + min(rvol, 5) * 5))
        candidates.append({
            "symbol": symbol, "name": item.get("name", symbol), "price": price,
            "premarketPct": round(pre_pct, 2), "m3Pct": round(m3_pct, 2),
            "m5Pct": round(m5_pct, 2), "relativeVolume10d": round(rvol, 2),
            "volume": volume, "entry": entry,
            "target": round(entry * (1 + target_pct / 100), 4),
            "targetPct": round(target_pct, 1),
            "stop": round(entry * (1 - stop_pct / 100), 4),
            "stopPct": stop_pct, "score": score,
            "signal": "FAST_ENTRY" if fast else "WATCH",
        })
    candidates.sort(key=lambda x: (x["signal"] == "FAST_ENTRY", x["score"]), reverse=True)
    return {
        "provider": "Webull OpenAPI", "status": "live" if not errors else "partial",
        "message": "คัดกรองจากอันดับ Pre-market, 3 นาที และ 5 นาที",
        "scanned": len(merged), "candidateCount": len(candidates),
        "candidates": candidates[:100], "errors": errors,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/scanner")
def scanner():
    now = time.time()
    if SCANNER_CACHE["payload"] is not None and now - SCANNER_CACHE["at"] < SCANNER_CACHE_SECONDS:
        return SCANNER_CACHE["payload"]
    payload = build_momentum_scanner()
    SCANNER_CACHE.update({"at": now, "payload": payload})
    return payload


@app.get("/health")
async def health():
    return {"ok": True, "service": "blessing-webull-bridge"}


@app.get("/status")
async def status():
    return public_status()


@app.get("/quotes")
async def quotes():
    return {"status": public_status(), "quotes": state["quotes"]}


@app.websocket("/stream")
async def stream(ws: WebSocket):
    origin = ws.headers.get("origin", "")
    if ALLOWED_ORIGINS and origin and origin not in [x.strip() for x in ALLOWED_ORIGINS]:
        await ws.close(code=1008)
        return
    await ws.accept()
    clients.add(ws)
    try:
        await ws.send_json({"type": "snapshot", "status": public_status(), "data": state["quotes"]})
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)
    except Exception:
        clients.discard(ws)
