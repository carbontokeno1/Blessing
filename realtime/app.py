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



# No-key experimental US stock scanner using Yahoo Finance public web endpoints.
# Yahoo's endpoints are undocumented and may be delayed, rate-limited, changed, or blocked.
# This is a read-only prototype, not a licensed real-time trading feed.
YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BlessingMarketResearch/1.0)",
    "Accept": "application/json",
}
SCANNER_CACHE: dict[str, Any] = {"at": 0.0, "payload": None}
SCANNER_CACHE_SECONDS = 25
SCANNER_LOCK = threading.Lock()


def yahoo_json(url: str, params: dict | None = None) -> dict:
    response = requests.get(url, params=params, headers=YAHOO_HEADERS, timeout=12)
    response.raise_for_status()
    return response.json()


def yahoo_screener(scr_id: str, count: int = 60) -> list[dict]:
    payload = yahoo_json(
        "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved",
        {"formatted": "false", "scrIds": scr_id, "count": count, "start": 0, "region": "US", "lang": "en-US"},
    )
    return payload.get("finance", {}).get("result", [{}])[0].get("quotes", []) or []


def yahoo_intraday(symbol: str) -> dict:
    now = int(time.time())
    payload = yahoo_json(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol, safe='')}",
        {"period1": now - 86400, "period2": now, "interval": "1m", "includePrePost": "true", "events": "div,splits"},
    )
    result = payload.get("chart", {}).get("result") or []
    if not result:
        return {}
    item = result[0]
    timestamps = item.get("timestamp") or []
    quote = ((item.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    points = [(int(t), float(c)) for t, c in zip(timestamps, closes) if c is not None]
    if not points:
        return {}
    latest_t, latest = points[-1]
    def momentum(minutes: int) -> float | None:
        cutoff = latest_t - minutes * 60
        prior = next((p for p in reversed(points[:-1]) if p[0] <= cutoff), None)
        return round((latest / prior[1] - 1) * 100, 3) if prior and prior[1] else None
    return {
        "lastPrice": latest,
        "momentum3m": momentum(3),
        "momentum5m": momentum(5),
        "priceTime": datetime.fromtimestamp(latest_t, timezone.utc).isoformat(),
        "barCount": len(points),
    }


def build_momentum_scanner() -> dict:
    updated = datetime.now(timezone.utc).isoformat()
    try:
        # Screeners discover the current market leaders without a user API key.
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            gainers_f = pool.submit(yahoo_screener, "day_gainers", 60)
            losers_f = pool.submit(yahoo_screener, "day_losers", 60)
            active_f = pool.submit(yahoo_screener, "most_actives", 60)
            gainers, losers, actives = gainers_f.result(), losers_f.result(), active_f.result()

        by_symbol: dict[str, dict] = {}
        for label, rows in (("DAY GAINERS", gainers), ("DAY LOSERS", losers), ("MOST ACTIVE", actives)):
            for rank, row in enumerate(rows, 1):
                symbol = str(row.get("symbol") or "").upper()
                if not symbol:
                    continue
                item = by_symbol.setdefault(symbol, {
                    "symbol": symbol,
                    "name": row.get("shortName") or row.get("longName") or symbol,
                    "price": row.get("regularMarketPrice"),
                    "changePct": row.get("regularMarketChangePercent"),
                    "volume": row.get("regularMarketVolume") or 0,
                    "rankings": [],
                    "gainerHits": 0,
                    "loserHits": 0,
                    "activeHits": 0,
                })
                item["rankings"].append(label + " #" + str(rank))
                if label == "DAY GAINERS":
                    item["gainerHits"] += 1
                elif label == "DAY LOSERS":
                    item["loserHits"] += 1
                else:
                    item["activeHits"] += 1

        symbols = list(by_symbol.keys())[:100]
        def enrich(symbol: str):
            try:
                return symbol, yahoo_intraday(symbol), None
            except Exception as exc:
                return symbol, {}, type(exc).__name__
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            enriched = list(pool.map(enrich, symbols))

        candidates = []
        for symbol, intraday, error in enriched:
            item = by_symbol[symbol]
            item["momentum3m"] = intraday.get("momentum3m")
            item["momentum5m"] = intraday.get("momentum5m")
            item["priceTime"] = intraday.get("priceTime")
            item["relativeVolume"] = None
            item["intradayStatus"] = "available" if intraday else ("unavailable" if error else "no_intraday_bars")
            price = item.get("price") or intraday.get("lastPrice") or 0
            item["price"] = price
            m3 = item["momentum3m"] or 0
            m5 = item["momentum5m"] or 0
            score = max(0, min(100, round(
                max(float(item.get("changePct") or 0), 0) * 2
                + max(m3, 0) * 8
                + max(m5, 0) * 5
                + item["activeHits"] * 5
                - item["loserHits"] * 20
            )))
            item["score"] = score
            item["signal"] = "WATCH" if item["loserHits"] else ("MOMENTUM" if score >= 35 else "EARLY")
            item["riskFlag"] = "ALSO_IN_LOSERS" if item["loserHits"] else None
            candidates.append(item)

        candidates.sort(key=lambda x: (x["score"], float(x.get("changePct") or 0)), reverse=True)
        return {
            "provider": "Yahoo Finance public endpoints (unofficial; no API key)",
            "status": "experimental" if candidates else "empty",
            "message": "ทดลองสแกนจาก Day Gainers / Day Losers / Most Active · แหล่งข้อมูลไม่เป็นทางการและอาจล่าช้า",
            "scanned": len(by_symbol),
            "candidateCount": len(candidates),
            "candidates": candidates[:100],
            "rankings": [
                {"key": "day_gainers", "label": "DAY GAINERS", "count": len(gainers)},
                {"key": "day_losers", "label": "DAY LOSERS", "count": len(losers)},
                {"key": "most_actives", "label": "MOST ACTIVE", "count": len(actives)},
            ],
            "lastError": None,
            "refreshEverySeconds": 5,
            "upstreamRefreshSeconds": SCANNER_CACHE_SECONDS,
            "dataDisclaimer": "Unofficial public endpoints; data may be delayed, incomplete, rate-limited, or unavailable. Not for execution or redistribution.",
            "updatedAt": updated,
        }
    except Exception as exc:
        return {
            "provider": "Yahoo Finance public endpoints (unofficial; no API key)",
            "status": "error",
            "message": "แหล่งข้อมูลทดลองไม่พร้อมใช้งาน · อาจถูกจำกัดการเรียก",
            "scanned": 0,
            "candidateCount": 0,
            "candidates": [],
            "rankings": [],
            "lastError": f"{type(exc).__name__}: {str(exc)[:180]}",
            "refreshEverySeconds": 5,
            "updatedAt": updated,
        }


@app.get("/scanner")
def scanner():
    now = time.time()
    if SCANNER_CACHE["payload"] is not None and now - SCANNER_CACHE["at"] < SCANNER_CACHE_SECONDS:
        return SCANNER_CACHE["payload"]
    with SCANNER_LOCK:
        now = time.time()
        if SCANNER_CACHE["payload"] is not None and now - SCANNER_CACHE["at"] < SCANNER_CACHE_SECONDS:
            return SCANNER_CACHE["payload"]
        payload = build_momentum_scanner()
        SCANNER_CACHE.update({"at": time.time(), "payload": payload})
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
