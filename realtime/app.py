import asyncio
import concurrent.futures
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



# Temporary public market screener fallback while Webull API approval is pending.
# Yahoo Finance screener is unofficial; results can be delayed, blocked, or changed.
SCANNER_TYPES = {"PRE_MARKET", "AFTER_MARKET", "MIN_3", "MIN_5", "DAY_1"}
SCANNER_CACHE: dict[str, Any] = {}
SCANNER_CACHE_SECONDS = 30
YAHOO_SCREEN_URL = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"


def _yahoo_screener(scr_id: str, sort_type: str):
    body = {
        "offset": 0,
        "size": 25,
        "sortField": "percentchange",
        "sortType": sort_type,
        "quoteType": "EQUITY",
        "query": {"operator": "AND", "operands": [
            {"operator": "eq", "operands": ["region", "us"]},
            {"operator": "gt", "operands": ["dayvolume", 100000]}
        ]}
    }
    response = requests.post(
        YAHOO_SCREEN_URL,
        params={"formatted": "false", "scrIds": scr_id},
        json=body,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=12,
    )
    response.raise_for_status()
    payload = response.json()
    result = (payload.get("finance", {}).get("result") or [{}])[0]
    quotes = result.get("quotes") or []
    rows = []
    for q in quotes:
        symbol = q.get("symbol")
        price = q.get("regularMarketPrice") or q.get("preMarketPrice") or q.get("postMarketPrice")
        change = q.get("regularMarketChangePercent")
        if not symbol or price is None or change is None:
            continue
        rows.append({
            "symbol": symbol,
            "name": q.get("shortName") or q.get("longName") or "",
            "price": float(price),
            "changePct": float(change),
            "volume": int(q.get("regularMarketVolume") or q.get("preMarketVolume") or 0),
            "relativeVolume": float(q.get("averageDailyVolume3Month") or 0) and round(
                float(q.get("regularMarketVolume") or 0) / max(float(q.get("averageDailyVolume3Month") or 1), 1), 3
            ),
            "marketTime": q.get("regularMarketTime"),
            "source": "Yahoo Finance (unofficial)",
        })
    return rows


@app.get("/scanner")
def scanner(rank_type: str = "DAY_1", direction: str = "DESC"):
    rank_type, direction = rank_type.upper(), direction.upper()
    if rank_type not in SCANNER_TYPES or direction not in {"ASC", "DESC"}:
        return {"status": "error", "message": "พารามิเตอร์การสแกนไม่ถูกต้อง", "candidates": [], "scanned": 0}
    cache_key = rank_type + ":" + direction
    now = time.time()
    cached = SCANNER_CACHE.get(cache_key)
    if cached and now - cached["at"] < SCANNER_CACHE_SECONDS:
        return cached["payload"]
    # Yahoo's public screener supplies day gainers/losers only. It does not
    # provide verified Webull-style 3m/5m or extended-hours rankings here.
    if rank_type in {"MIN_3", "MIN_5", "PRE_MARKET", "AFTER_MARKET"}:
        message = "แหล่งข้อมูลสำรองไม่รองรับการจัดอันดับช่วงนี้ ต้องรอ Webull API"
        return {"provider": "Yahoo Finance (unofficial fallback)", "status": "unsupported_interval",
                "message": message, "rankType": rank_type, "direction": direction,
                "scanned": 0, "candidates": [], "updatedAt": datetime.now(timezone.utc).isoformat()}
    scr_id = "day_gainers" if direction == "DESC" else "day_losers"
    try:
        candidates = _yahoo_screener(scr_id, "DESC" if direction == "DESC" else "ASC")
        payload = {
            "provider": "Yahoo Finance (unofficial fallback)",
            "status": "ok" if candidates else "empty",
            "message": "ข้อมูลจัดอันดับรายวันจาก Yahoo Finance; ไม่ใช่ Webull และอาจล่าช้าหรือถูกจำกัด",
            "rankType": rank_type, "direction": direction,
            "scanned": len(candidates), "candidates": candidates,
            "updatedAt": datetime.now(timezone.utc).isoformat(),
            "dataDisclaimer": "Unofficial source. Not guaranteed real-time; verify quotes with your broker before trading."
        }
        SCANNER_CACHE[cache_key] = {"at": now, "payload": payload}
        return payload
    except Exception as exc:
        log.warning("Yahoo fallback screener failed: %s", type(exc).__name__)
        return {"provider": "Yahoo Finance (unofficial fallback)", "status": "source_unavailable",
                "message": "แหล่งข้อมูลสำรองยังตอบกลับไม่ได้: " + type(exc).__name__,
                "rankType": rank_type, "direction": direction, "scanned": 0,
                "candidates": [], "updatedAt": datetime.now(timezone.utc).isoformat()}




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
