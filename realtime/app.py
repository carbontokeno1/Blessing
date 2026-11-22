import asyncio
import concurrent.futures
import base64
import hashlib
import hmac
import urllib.parse
import requests
import json
import websockets
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("blessing-realtime")

SYMBOLS = [s.strip().upper() for s in os.getenv("WEBULL_SYMBOLS", "AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA,AMD").split(",") if s.strip()]
APP_KEY = os.getenv("WEBULL_APP_KEY", "")
APP_SECRET = os.getenv("WEBULL_APP_SECRET", "")
ALPACA_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET = os.getenv("ALPACA_API_SECRET", "")
ALPACA_FEED = os.getenv("ALPACA_FEED", "iex").lower()
REGION = os.getenv("WEBULL_REGION", "us")
HTTP_HOST = os.getenv("WEBULL_HTTP_HOST", "api.webull.com")
MQTT_HOST = os.getenv("WEBULL_MQTT_HOST", "data-api.webull.com")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "https://blessing.carbontokeno1.workers.dev").split(",")

app = FastAPI(title="Blessing Market Data Bridge", version="0.2.0")
app.add_middleware(CORSMiddleware, allow_origins=[x.strip() for x in ALLOWED_ORIGINS if x.strip()], allow_credentials=False, allow_methods=["GET", "POST"], allow_headers=["*"])

state = {
    "provider": "webull",
    "connection": "waiting_credentials",
    "marketDataEntitlement": "not_verified",
    "symbols": SYMBOLS,
    "lastMessageAt": None,
    "lastError": None,
    "alpacaAuthenticated": False,
    "alpacaAuthenticatedAt": None,
    "quotes": {},
    "barHistory": {},
    "sessionOpen": {},
    "sessionDate": {},
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


def alpaca_stream_main():
    """Connect to Alpaca's real-time stock stream (IEX on the free Basic plan)."""
    feed = ALPACA_FEED if ALPACA_FEED in {"iex", "sip", "delayed_sip"} else "iex"
    url = f"wss://stream.data.alpaca.markets/v2/{feed}"
    while True:
        try:
            async def run_stream():
                state["provider"] = "alpaca"
                state["marketDataEntitlement"] = f"{feed.upper()} feed; entitlement depends on Alpaca plan"
                state["connection"] = "connecting"
                state["alpacaAuthenticated"] = False
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5, max_size=2**20) as ws:
                    await ws.send(json.dumps({"action": "auth", "key": ALPACA_KEY, "secret": ALPACA_SECRET}))
                    # Alpaca sends a separate "connected" acknowledgement before
                    # the authentication response. Read messages until auth succeeds
                    # or the server explicitly rejects the credentials.
                    authenticated = False
                    for _ in range(5):
                        auth_message = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
                        auth_items = auth_message if isinstance(auth_message, list) else [auth_message]
                        for item in auth_items:
                            if not isinstance(item, dict):
                                continue
                            if item.get("T") == "error":
                                raise RuntimeError("Alpaca authentication error: " + str(item.get("msg", "unknown"))[:140])
                            if item.get("T") == "success" and item.get("msg") == "authenticated":
                                authenticated = True
                        if authenticated:
                            break
                    if not authenticated:
                        raise RuntimeError("Alpaca authentication was not confirmed after connected acknowledgement")
                    state["alpacaAuthenticated"] = True
                    state["alpacaAuthenticatedAt"] = datetime.now(timezone.utc).isoformat()
                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "trades": SYMBOLS,
                        "quotes": SYMBOLS,
                        "bars": SYMBOLS,
                        "dailyBars": SYMBOLS
                    }))
                    state["connection"] = "connected"
                    state["lastError"] = None
                    log.info("Alpaca %s stream authenticated; subscribed to %d watchlist symbols", feed, len(SYMBOLS))
                    async for raw_message in ws:
                        messages = json.loads(raw_message)
                        if not isinstance(messages, list):
                            messages = [messages]
                        changed = False
                        now = datetime.now(timezone.utc).isoformat()
                        for msg in messages:
                            if not isinstance(msg, dict):
                                continue
                            kind = msg.get("T")
                            symbol = msg.get("S")
                            if kind not in {"t", "q", "b", "d"} or symbol not in SYMBOLS:
                                if kind == "error":
                                    state["lastError"] = str(msg.get("msg", "Alpaca stream error"))[:180]
                                    log.warning("Alpaca stream reported an error: %s", state["lastError"])
                                continue
                            prior = state["quotes"].get(symbol, {})
                            quote = dict(prior.get("raw") or {})
                            if kind == "t":
                                quote.update({"price": msg.get("p"), "last_price": msg.get("p"), "trade_size": msg.get("s"), "timestamp": msg.get("t")})
                            elif kind == "q":
                                quote.update({"bid": msg.get("bp"), "ask": msg.get("ap"), "bid_size": msg.get("bs"), "ask_size": msg.get("as"), "quote_timestamp": msg.get("t")})
                                if quote.get("price") is None and msg.get("bp") is not None and msg.get("ap") is not None:
                                    quote["price"] = (float(msg["bp"]) + float(msg["ap"])) / 2
                            elif kind == "b":
                                quote.update({"open": msg.get("o"), "high": msg.get("h"), "low": msg.get("l"), "price": msg.get("c"), "last_price": msg.get("c"), "volume": msg.get("v"), "timestamp": msg.get("t"), "bar_interval": "1m"})
                                bar_time = str(msg.get("t") or now)
                                session_day = bar_time[:10]
                                if state["sessionDate"].get(symbol) != session_day:
                                    state["sessionDate"][symbol] = session_day
                                    state["sessionOpen"][symbol] = msg.get("o")
                                    state["barHistory"][symbol] = []
                                history = state["barHistory"].setdefault(symbol, [])
                                history.append({"open": msg.get("o"), "high": msg.get("h"), "low": msg.get("l"), "close": msg.get("c"), "volume": msg.get("v"), "timestamp": bar_time})
                                state["barHistory"][symbol] = history[-30:]
                            elif kind == "d":
                                quote.update({"daily_open": msg.get("o"), "daily_high": msg.get("h"), "daily_low": msg.get("l"), "daily_close": msg.get("c"), "daily_volume": msg.get("v"), "daily_timestamp": msg.get("t")})
                                daily_time = str(msg.get("t") or now)
                                daily_day = daily_time[:10]
                                if state["sessionDate"].get(symbol) != daily_day:
                                    state["sessionDate"][symbol] = daily_day
                                    state["barHistory"][symbol] = []
                                if msg.get("o") is not None:
                                    state["sessionOpen"][symbol] = msg.get("o")
                            state["quotes"][symbol] = {
                                "symbol": symbol, "topic": kind, "receivedAt": now,
                                "source": f"Alpaca {feed.upper()} real-time stream", "raw": quote
                            }
                            changed = True
                        if changed:
                            state["lastMessageAt"] = now
                            state["connection"] = "streaming"
                            if loop and loop.is_running():
                                asyncio.run_coroutine_threadsafe(
                                    broadcast({"type": "quotes", "data": state["quotes"], "status": public_status()}), loop
                                )
            asyncio.run(run_stream())
        except Exception as exc:
            state["alpacaAuthenticated"] = False
            state["connection"] = "reconnecting"
            state["lastError"] = type(exc).__name__
            log.warning("Alpaca stream disconnected (%s); retrying shortly", type(exc).__name__)
            time.sleep(5)


def public_status() -> dict:
    alpaca_configured = bool(ALPACA_KEY and ALPACA_SECRET)
    return {
        "provider": state["provider"],
        "connection": state["connection"],
        "marketDataEntitlement": state["marketDataEntitlement"],
        "symbols": state["symbols"],
        "quoteCount": len(state["quotes"]),
        "lastMessageAt": state["lastMessageAt"],
        "lastMarketDataAt": state["lastMessageAt"],
        "lastError": state["lastError"],
        "alpacaConfigured": alpaca_configured,
        "alpacaKeyConfigured": bool(ALPACA_KEY),
        "alpacaSecretConfigured": bool(ALPACA_SECRET),
        "alpacaAuthenticated": bool(state["alpacaAuthenticated"]),
        "alpacaAuthenticatedAt": state["alpacaAuthenticatedAt"],
        "alpacaConnected": bool(state["provider"] == "alpaca" and state["connection"] in {"connected", "streaming"}),
        "alpacaFeed": ALPACA_FEED if ALPACA_FEED in {"iex", "sip", "delayed_sip"} else "iex",
        "universeScanner": {
            "status": UNIVERSE_STATE.get("status"),
            "universeCount": UNIVERSE_STATE.get("universeCount"),
            "scanned": UNIVERSE_STATE.get("scanned"),
            "candidateCount": UNIVERSE_STATE.get("candidateCount"),
            "updatedAt": UNIVERSE_STATE.get("updatedAt"),
            "feed": "iex",
        },
        "marketDataReceived": bool(state["lastMessageAt"]),
        "serverTime": datetime.now(timezone.utc).isoformat(),
    }


@app.on_event("startup")
async def startup():
    global loop, stream_thread
    loop = asyncio.get_running_loop()
    if ALPACA_KEY and ALPACA_SECRET:
        state["provider"] = "alpaca"
        state["connection"] = "connecting"
        stream_thread = threading.Thread(target=alpaca_stream_main, name="alpaca-websocket", daemon=True)
        stream_thread.start()
        global UNIVERSE_THREAD
        UNIVERSE_THREAD = threading.Thread(target=_universe_scanner_loop, name="alpaca-universe-scanner", daemon=True)
        UNIVERSE_THREAD.start()
        return
    if APP_KEY and APP_SECRET:
        state["provider"] = "webull"
        state["connection"] = "connecting"
        stream_thread = threading.Thread(target=stream_main, name="webull-mqtt", daemon=True)
        stream_thread.start()
        return
    state["provider"] = "alpaca"
    state["connection"] = "waiting_credentials"
    log.warning("Alpaca credentials are not configured; no market data will be fabricated.")



# Temporary public market screener fallback while Webull API approval is pending.
# Yahoo Finance is unofficial and may be delayed/blocked. Keep the last good result
# so a transient upstream failure never clears the user's scanner table.
SCANNER_TYPES = {"PRE_MARKET", "AFTER_MARKET", "MIN_3", "MIN_5", "DAY_1"}
SCANNER_CACHE: dict[str, Any] = {}
SCANNER_CACHE_SECONDS = 30
SCANNER_STALE_MAX_SECONDS = 900
SCANNER_LOCK = threading.Lock()
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
    last_error = None
    for attempt in range(2):
        try:
            response = requests.post(
                YAHOO_SCREEN_URL,
                params={"formatted": "false", "scrIds": scr_id},
                json=body,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; BlessingMarketScanner/1.0)",
                    "Accept": "application/json",
                },
                timeout=(4, 8),
            )
            if response.status_code == 429 or response.status_code >= 500:
                response.raise_for_status()
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
                avg_volume = float(q.get("averageDailyVolume3Month") or 0)
                current_volume = float(q.get("regularMarketVolume") or q.get("preMarketVolume") or 0)
                rows.append({
                    "symbol": symbol,
                    "name": q.get("shortName") or q.get("longName") or "",
                    "price": float(price),
                    "changePct": float(change),
                    "volume": int(current_volume),
                    "relativeVolume": round(current_volume / max(avg_volume, 1), 3) if avg_volume else None,
                    "marketTime": q.get("regularMarketTime"),
                    "source": "Yahoo Finance (unofficial)",
                })
            return rows
        except Exception as exc:
            last_error = exc
            if attempt == 0:
                time.sleep(0.5)
    raise last_error


def _scanner_stale_payload(cached: dict, rank_type: str, direction: str, error: str):
    age = max(0, int(time.time() - cached["at"]))
    payload = dict(cached["payload"])
    payload.update({
        "status": "stale",
        "message": "แหล่งข้อมูลต้นทางขัดข้องชั่วคราว · กำลังแสดงข้อมูลล่าสุดที่บันทึกไว้",
        "rankType": rank_type,
        "direction": direction,
        "staleSeconds": age,
        "sourceError": error,
        "cacheState": "stale",
    })
    return payload


@app.get("/scanner")
def scanner(rank_type: str = "DAY_1", direction: str = "DESC"):
    rank_type, direction = rank_type.upper(), direction.upper()
    if rank_type not in SCANNER_TYPES or direction not in {"ASC", "DESC"}:
        return {"status": "error", "message": "พารามิเตอร์การสแกนไม่ถูกต้อง", "candidates": [], "scanned": 0}
    cache_key = rank_type + ":" + direction
    now = time.time()
    cached = SCANNER_CACHE.get(cache_key)
    if cached and now - cached["at"] < SCANNER_CACHE_SECONDS:
        payload = dict(cached["payload"])
        payload["cacheState"] = "fresh"
        return payload

    # The public fallback does not provide verified intraday or extended-hours rankings.
    if rank_type in {"MIN_3", "MIN_5", "PRE_MARKET", "AFTER_MARKET"}:
        return {
            "provider": "Yahoo Finance (unofficial fallback)",
            "status": "unsupported_interval",
            "message": "แหล่งข้อมูลสำรองไม่รองรับช่วงเวลานี้ ต้องรอ Webull Market Data API",
            "rankType": rank_type, "direction": direction,
            "scanned": 0, "candidates": [],
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        }

    # Serialize refreshes: multiple visitors should not all hit Yahoo simultaneously.
    with SCANNER_LOCK:
        now = time.time()
        cached = SCANNER_CACHE.get(cache_key)
        if cached and now - cached["at"] < SCANNER_CACHE_SECONDS:
            payload = dict(cached["payload"])
            payload["cacheState"] = "fresh"
            return payload
        scr_id = "day_gainers" if direction == "DESC" else "day_losers"
        try:
            candidates = _yahoo_screener(scr_id, "DESC" if direction == "DESC" else "ASC")
            if not candidates:
                raise ValueError("Yahoo returned an empty screener result")
            fetched_at = datetime.now(timezone.utc).isoformat()
            payload = {
                "provider": "Yahoo Finance (unofficial fallback)",
                "status": "ok",
                "message": "ข้อมูลจัดอันดับรายวันจาก Yahoo Finance · ไม่ใช่ Webull และอาจล่าช้า",
                "rankType": rank_type, "direction": direction,
                "scanned": len(candidates), "candidates": candidates,
                "updatedAt": fetched_at,
                "cacheState": "fresh",
                "dataDisclaimer": "Unofficial source. Not guaranteed real-time; verify quotes with your broker before trading."
            }
            SCANNER_CACHE[cache_key] = {"at": time.time(), "payload": payload}
            return payload
        except Exception as exc:
            error = type(exc).__name__
            log.warning("Yahoo fallback screener failed: %s", error)
            cached = SCANNER_CACHE.get(cache_key)
            if cached and time.time() - cached["at"] <= SCANNER_STALE_MAX_SECONDS:
                return _scanner_stale_payload(cached, rank_type, direction, error)
            return {
                "provider": "Yahoo Finance (unofficial fallback)",
                "status": "source_unavailable",
                "message": "แหล่งข้อมูลหุ้นยังไม่ตอบกลับ · กรุณาลองใหม่ภายหลัง",
                "rankType": rank_type, "direction": direction,
                "scanned": 0, "candidates": [],
                "updatedAt": datetime.now(timezone.utc).isoformat(),
                "sourceError": error,
            }


# Server-side Yahoo chart cache for the eight watchlist symbols. This avoids
# depending on a third-party browser CORS proxy and preserves last-good quotes.
QUOTE_CACHE: dict[str, Any] = {}
QUOTE_CACHE_SECONDS = 20
QUOTE_STALE_MAX_SECONDS = 900
QUOTE_LOCK = threading.Lock()


def _fetch_yahoo_chart_quote(symbol: str):
    url = "https://query1.finance.yahoo.com/v8/finance/chart/" + urllib.parse.quote(symbol)
    response = requests.get(
        url,
        params={"range": "1d", "interval": "1m", "includePrePost": "true"},
        headers={"User-Agent": "Mozilla/5.0 (compatible; BlessingMarketScanner/1.0)", "Accept": "application/json"},
        timeout=(4, 8),
    )
    response.raise_for_status()
    chart = response.json().get("chart", {})
    result = (chart.get("result") or [None])[0]
    if not result:
        raise ValueError("Yahoo chart returned no result")
    meta = result.get("meta") or {}
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    timestamps = result.get("timestamp") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []
    points = []
    for index, stamp in enumerate(timestamps):
        price = closes[index] if index < len(closes) else None
        if price is None:
            continue
        points.append({
            "time": int(stamp) * 1000,
            "price": float(price),
            "volume": int(volumes[index] or 0) if index < len(volumes) else 0,
        })
    if not points:
        raise ValueError("Yahoo chart returned no price bars")
    last = points[-1]
    previous_close = float(meta.get("chartPreviousClose") or meta.get("previousClose") or 0)
    def move_back(seconds):
        target = last["time"] - seconds * 1000
        old = next((point for point in reversed(points[:-1]) if point["time"] <= target), None)
        return round((last["price"] / old["price"] - 1) * 100, 4) if old and old["price"] else None
    return {
        "symbol": symbol,
        "price": last["price"],
        "changePct": round((last["price"] / previous_close - 1) * 100, 4) if previous_close else None,
        "volume": last["volume"],
        "m1": move_back(60),
        "m5": move_back(300),
        "session": meta.get("marketState") or "UNKNOWN",
        "priceTime": last["time"],
        "receivedAt": datetime.now(timezone.utc).isoformat(),
        "source": "Yahoo Finance chart (unofficial)",
        "stale": False,
    }


@app.get("/quotes")
def quotes(symbols: str = "AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA,AMD"):
    requested = []
    for item in symbols.split(","):
        symbol = item.strip().upper()
        if symbol and symbol.replace(".", "").replace("-", "").isalnum() and symbol not in requested:
            requested.append(symbol)
    requested = requested[:20]
    if not requested:
        return {"status": "error", "message": "ไม่มีสัญลักษณ์หุ้นที่ถูกต้อง", "quotes": [], "count": 0}

    # Prefer actual Alpaca/Webull stream data when available; never label cached Yahoo data as live.
    live_rows = []
    for symbol in requested:
        live = state["quotes"].get(symbol)
        if live and live.get("raw"):
            raw = live["raw"]
            price = raw.get("price") or raw.get("last_price")
            if price is not None:
                live_rows.append({
                    "symbol": symbol, "price": price, "bid": raw.get("bid"), "ask": raw.get("ask"),
                    "volume": raw.get("volume"), "priceTime": raw.get("timestamp") or raw.get("quote_timestamp"),
                    "receivedAt": live.get("receivedAt"), "source": live.get("source") or state["provider"],
                    "stale": False
                })
    if live_rows:
        return {
            "status": "ok", "provider": state["provider"],
            "message": f"ข้อมูลจากสตรีม {state['provider']} สำหรับหุ้นในรายการติดตามเท่านั้น",
            "quotes": live_rows, "count": len(live_rows),
            "streamStatus": public_status(), "updatedAt": datetime.now(timezone.utc).isoformat()
        }

    fresh = {}
    now = time.time()
    for symbol in requested:
        cached = QUOTE_CACHE.get(symbol)
        if cached and now - cached["at"] < QUOTE_CACHE_SECONDS:
            fresh[symbol] = dict(cached["payload"])

    missing = [symbol for symbol in requested if symbol not in fresh]
    errors = {}
    if missing:
        with QUOTE_LOCK:
            # Another visitor may have refreshed while this request waited.
            now = time.time()
            for symbol in list(missing):
                cached = QUOTE_CACHE.get(symbol)
                if cached and now - cached["at"] < QUOTE_CACHE_SECONDS:
                    fresh[symbol] = dict(cached["payload"])
                    missing.remove(symbol)
            if missing:
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                    futures = {pool.submit(_fetch_yahoo_chart_quote, symbol): symbol for symbol in missing}
                    for future in concurrent.futures.as_completed(futures):
                        symbol = futures[future]
                        try:
                            payload = future.result()
                            QUOTE_CACHE[symbol] = {"at": time.time(), "payload": payload}
                            fresh[symbol] = dict(payload)
                        except Exception as exc:
                            errors[symbol] = type(exc).__name__

    stale_count = 0
    output = []
    for symbol in requested:
        if symbol in fresh:
            output.append(fresh[symbol])
            continue
        cached = QUOTE_CACHE.get(symbol)
        if cached and time.time() - cached["at"] <= QUOTE_STALE_MAX_SECONDS:
            payload = dict(cached["payload"])
            payload["stale"] = True
            payload["staleSeconds"] = max(0, int(time.time() - cached["at"]))
            output.append(payload)
            stale_count += 1

    return {
        "status": "ok" if output else "source_unavailable",
        "provider": "Yahoo Finance chart (unofficial fallback)",
        "message": "ข้อมูลสำรองจาก Yahoo Finance · ไม่รับประกันว่าเป็น real-time",
        "quotes": output,
        "count": len(output),
        "staleCount": stale_count,
        "errors": errors,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
    }




def forecast_10m_from_bars(bars, price):
    """Linear-trend estimate from recent 1-minute closes; informational, not a guarantee."""
    points = []
    for index, bar in enumerate((bars or [])[-12:]):
        try:
            value = float(bar.get("close"))
            if value > 0:
                points.append((index, value))
        except (TypeError, ValueError):
            continue
    if len(points) < 5:
        return None
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    x_mean = sum(xs) / len(xs)
    y_mean = sum(ys) / len(ys)
    denom = sum((x - x_mean) ** 2 for x in xs)
    if denom <= 0:
        return None
    slope = sum((x - x_mean) * (y - y_mean) for x, y in points) / denom
    projected = y_mean + slope * ((xs[-1] + 10) - x_mean)
    if price <= 0:
        return None
    # Limit extrapolation to avoid extreme outputs from noisy short windows.
    projected = max(price * 0.985, min(price * 1.015, projected))
    return round(projected, 6)




# Alpaca Basic IEX all-universe scanner. REST batches are used because the
# Basic websocket trade/quote subscription is limited to 30 symbols.
UNIVERSE_SCAN_INTERVAL = max(60, int(os.getenv("UNIVERSE_SCAN_INTERVAL", "60")))
UNIVERSE_LIQUIDITY_LIMIT = max(200, min(2000, int(os.getenv("UNIVERSE_LIQUIDITY_LIMIT", "1200"))))
UNIVERSE_BATCH_SIZE = 100
UNIVERSE_CANDIDATE_LIMIT = 150
UNIVERSE_CACHE_SECONDS = 6 * 60 * 60
UNIVERSE_STATE: dict[str, Any] = {
    "status": "waiting_credentials", "provider": "Alpaca IEX",
    "universeCount": 0, "scanned": 0, "evaluated": 0,
    "candidateCount": 0, "candidates": [], "updatedAt": None,
    "lastError": None, "cycleSeconds": None, "feed": "iex",
    "coverage": "IEX-only; not consolidated US market coverage",
}
UNIVERSE_LOCK = threading.Lock()
UNIVERSE_ASSETS: list[dict] = []
UNIVERSE_ASSETS_AT = 0.0
UNIVERSE_THREAD: threading.Thread | None = None
UNIVERSE_EXCHANGES = {"NASDAQ", "NYSE", "AMEX", "ARCA", "BATS", "NYSEARCA", "NYSEAMERICAN", "IEX"}


def _alpaca_headers():
    return {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET}


def _load_active_universe(force: bool = False) -> list[dict]:
    global UNIVERSE_ASSETS, UNIVERSE_ASSETS_AT
    if UNIVERSE_ASSETS and not force and time.time() - UNIVERSE_ASSETS_AT < UNIVERSE_CACHE_SECONDS:
        return UNIVERSE_ASSETS
    response = requests.get(
        "https://paper-api.alpaca.markets/v2/assets",
        params={"status": "active", "asset_class": "us_equity"},
        headers=_alpaca_headers(), timeout=(5, 25),
    )
    response.raise_for_status()
    payload = response.json()
    assets = []
    for asset in payload if isinstance(payload, list) else []:
        symbol = str(asset.get("symbol") or "").upper()
        exchange = str(asset.get("exchange") or "").upper()
        if (symbol and asset.get("status") == "active" and asset.get("tradable") is True
                and exchange in UNIVERSE_EXCHANGES
                and symbol.replace(".", "").replace("-", "").isalnum()):
            assets.append({"symbol": symbol, "exchange": exchange, "name": asset.get("name") or ""})
    UNIVERSE_ASSETS = assets
    UNIVERSE_ASSETS_AT = time.time()
    return assets


def _alpaca_snapshots(symbols: list[str]) -> dict:
    if not symbols:
        return {}
    response = requests.get(
        "https://data.alpaca.markets/v2/stocks/snapshots",
        params={"symbols": ",".join(symbols), "feed": "iex"},
        headers=_alpaca_headers(), timeout=(5, 20),
    )
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, dict) else {}


def _alpaca_recent_bars(symbols: list[str]) -> dict:
    if not symbols:
        return {}
    end = datetime.now(timezone.utc)
    start = end.timestamp() - 15 * 60
    response = requests.get(
        "https://data.alpaca.markets/v2/stocks/bars",
        params={
            "symbols": ",".join(symbols),
            "timeframe": "1Min",
            "start": datetime.fromtimestamp(start, timezone.utc).isoformat(),
            "end": end.isoformat(),
            "feed": "iex",
            "limit": 10000,
            "sort": "asc",
        },
        headers=_alpaca_headers(), timeout=(5, 25),
    )
    response.raise_for_status()
    data = response.json()
    return data.get("bars") or {}


def _num(value, default=None):
    try:
        result = float(value)
        return result if result == result and abs(result) != float("inf") else default
    except (TypeError, ValueError):
        return default


def _universe_candidate(symbol: str, snap: dict, bars_raw: list, exchange: str) -> dict | None:
    trade = snap.get("latestTrade") or {}
    quote = snap.get("latestQuote") or {}
    minute = snap.get("minuteBar") or {}
    daily = snap.get("dailyBar") or {}
    price = _num(trade.get("p"), _num(minute.get("c")))
    daily_open = _num(daily.get("o"))
    daily_volume = _num(daily.get("v"), 0) or 0
    if not price or price < 1 or not daily_open or daily_volume < 10000:
        return None
    bid, ask = _num(quote.get("bp")), _num(quote.get("ap"))
    spread = ((ask - bid) / price) * 100 if bid and ask and ask >= bid else None
    change = ((price / daily_open) - 1) * 100 if daily_open else None
    bars = []
    for b in (bars_raw or []):
        close = _num(b.get("c"))
        if close and close > 0:
            bars.append({
                "open": _num(b.get("o")), "high": _num(b.get("h")),
                "low": _num(b.get("l")), "close": close,
                "volume": _num(b.get("v"), 0) or 0, "time": b.get("t"),
            })
    bars = bars[-15:]
    if len(bars) < 6:
        return None
    closes = [x["close"] for x in bars]
    m1 = ((closes[-1] / closes[-2]) - 1) * 100 if closes[-2] else None
    m5 = ((closes[-1] / closes[-6]) - 1) * 100 if closes[-6] else None
    previous_volumes = [x["volume"] for x in bars[-6:-1]]
    average_volume = sum(previous_volumes) / len(previous_volumes) if previous_volumes else 0
    rvol = bars[-1]["volume"] / average_volume if average_volume > 0 else None
    prior_highs = [x["high"] for x in bars[-6:-1] if x["high"]]
    breakout = bool(prior_highs and price > max(prior_highs))
    forecast = forecast_10m_from_bars(bars, price)
    fast_entry = bool(
        change is not None and change >= 3
        and m1 is not None and m1 > 0 and m5 is not None and m5 > 0
        and rvol is not None and rvol >= 2
        and spread is not None and 0 < spread <= 0.5
        and breakout
    )
    if not fast_entry:
        return None
    return {
        "symbol": symbol, "exchange": "Alpaca IEX/" + exchange,
        "price": price, "changePct": change, "m1": m1, "m5": m5,
        "relativeVolume": rvol, "spreadPct": spread,
        "barsCount": len(bars), "breakout": breakout, "fastEntry": True,
        "forecast10m": forecast, "receivedAt": datetime.now(timezone.utc).isoformat(),
        "dataFeed": "IEX", "dailyVolumeIex": int(daily_volume),
    }


def _universe_scan_cycle():
    started = time.time()
    assets = _load_active_universe()
    snapshots: dict[str, dict] = {}
    symbols = [x["symbol"] for x in assets]
    batches = [symbols[i:i + UNIVERSE_BATCH_SIZE] for i in range(0, len(symbols), UNIVERSE_BATCH_SIZE)]
    # A bounded worker pool keeps the Basic 200-requests/minute quota in view.
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_alpaca_snapshots, batch): batch for batch in batches}
        for future in concurrent.futures.as_completed(futures):
            batch_data = future.result()
            for symbol, value in batch_data.items():
                if isinstance(value, dict):
                    snapshots[str(symbol).upper()] = value

    exchange_by_symbol = {x["symbol"]: x["exchange"] for x in assets}
    liquid = []
    for symbol, snap in snapshots.items():
        daily = snap.get("dailyBar") or {}
        volume = _num(daily.get("v"), 0) or 0
        price = _num((snap.get("latestTrade") or {}).get("p"), _num((snap.get("minuteBar") or {}).get("c")))
        if price and price >= 1 and volume >= 10000:
            liquid.append((volume * price, symbol))
    liquid.sort(reverse=True)
    selected = [symbol for _, symbol in liquid[:UNIVERSE_LIQUIDITY_LIMIT]]

    bars_by_symbol: dict[str, list] = {}
    bar_batches = [selected[i:i + UNIVERSE_BATCH_SIZE] for i in range(0, len(selected), UNIVERSE_BATCH_SIZE)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_alpaca_recent_bars, batch): batch for batch in bar_batches}
        for future in concurrent.futures.as_completed(futures):
            batch_bars = future.result()
            for symbol, values in batch_bars.items():
                bars_by_symbol[str(symbol).upper()] = values if isinstance(values, list) else []

    candidates = []
    for symbol in selected:
        snap = snapshots.get(symbol) or {}
        row = _universe_candidate(symbol, snap, bars_by_symbol.get(symbol) or [], exchange_by_symbol.get(symbol, ""))
        if row:
            candidates.append(row)
    candidates.sort(key=lambda x: (
        (x.get("relativeVolume") or 0) * 2
        + (x.get("m5") or 0) + (x.get("m1") or 0)
        + min((x.get("dailyVolumeIex") or 0) / 1000000, 5)
    ), reverse=True)
    now = datetime.now(timezone.utc).isoformat()
    elapsed = round(time.time() - started, 2)
    with UNIVERSE_LOCK:
        UNIVERSE_STATE.update({
            "status": "ok", "provider": "Alpaca Basic · IEX",
            "universeCount": len(assets), "snapshotCount": len(snapshots),
            "scanned": len(snapshots), "evaluated": len(selected),
            "candidateCount": len(candidates),
            "candidates": candidates[:UNIVERSE_CANDIDATE_LIMIT],
            "updatedAt": now, "lastError": None, "cycleSeconds": elapsed,
            "batchCount": len(batches) + len(bar_batches),
            "feed": "iex",
            "coverage": "IEX-only real-time feed; not consolidated all-exchange data",
        })


def _universe_scanner_loop():
    while True:
        if not (ALPACA_KEY and ALPACA_SECRET):
            UNIVERSE_STATE.update({"status": "waiting_credentials", "lastError": "ALPACA_CREDENTIALS_MISSING"})
            time.sleep(30)
            continue
        try:
            _universe_scan_cycle()
        except Exception as exc:
            log.exception("Alpaca IEX universe scan failed")
            with UNIVERSE_LOCK:
                UNIVERSE_STATE.update({
                    "status": "source_error", "lastError": type(exc).__name__,
                    "updatedAt": datetime.now(timezone.utc).isoformat(),
                })
        time.sleep(UNIVERSE_SCAN_INTERVAL)


@app.get("/universe-scanner")
def universe_scanner():
    with UNIVERSE_LOCK:
        result = dict(UNIVERSE_STATE)
    updated = result.get("updatedAt")
    age = None
    if updated:
        try:
            age = max(0, int((datetime.now(timezone.utc) - datetime.fromisoformat(updated.replace("Z", "+00:00"))).total_seconds()))
        except Exception:
            age = None
    result["staleSeconds"] = age
    if age is not None and age > max(180, UNIVERSE_SCAN_INTERVAL * 3):
        result["status"] = "stale"
    result["limitations"] = [
        "ราคาหุ้นและปริมาณซื้อขายมาจาก IEX เท่านั้น ไม่ใช่ consolidated SIP ทุกตลาด",
        "Universe เป็นสินทรัพย์ US equity ที่ Alpaca ระบุว่า active/tradable; อาจรวม ETF",
        "สแกนเป็นรอบด้วย REST batch ไม่ใช่ real-time stream สำหรับหุ้นทุกตัว",
        "หุ้นที่ไม่มีข้อมูล IEX ในรอบนั้นอาจไม่ถูกประเมินหรือไม่ปรากฏในรายการ",
        "Basic จำกัด historical data ไว้ในช่วง 15 นาทีล่าสุด และ API 200 requests/minute",
    ]
    return result


@app.get("/alert-snapshot")
def alert_snapshot():
    """Feed fresh IEX universe candidates into the existing alert evaluator."""
    if not (ALPACA_KEY and ALPACA_SECRET):
        return {"status": "waiting_credentials", "provider": "alpaca", "quotes": []}
    with UNIVERSE_LOCK:
        snapshot = dict(UNIVERSE_STATE)
    updated = snapshot.get("updatedAt")
    if snapshot.get("status") != "ok" or not updated:
        return {"status": snapshot.get("status", "waiting_scan"), "provider": "Alpaca IEX", "quotes": [], "scanner": snapshot}
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(updated.replace("Z", "+00:00"))).total_seconds()
    except Exception:
        age = 999999
    if age > max(180, UNIVERSE_SCAN_INTERVAL * 3):
        return {"status": "stale_feed", "provider": "Alpaca IEX", "quotes": [], "staleSeconds": int(age), "scanner": snapshot}
    rows = snapshot.get("candidates") or []
    return {
        "status": "ok", "provider": "Alpaca IEX", "feed": "iex",
        "quotes": rows, "quoteCount": len(rows),
        "scanner": {
            "universeCount": snapshot.get("universeCount"),
            "scanned": snapshot.get("scanned"),
            "evaluated": snapshot.get("evaluated"),
            "candidateCount": snapshot.get("candidateCount"),
            "updatedAt": updated,
            "cycleSeconds": snapshot.get("cycleSeconds"),
            "coverage": snapshot.get("coverage"),
        },
        "updatedAt": datetime.now(timezone.utc).isoformat(),
    }



# Public BitMart Spot altcoin momentum scanner. Public market data only; no keys or order endpoints.
ALTCOIN_CACHE = {"at": 0.0, "payload": None}
ALTCOIN_CACHE_LOCK = threading.Lock()
ALTCOIN_EXCLUDED = {"USDC","FDUSD","TUSD","USDP","DAI","EUR","USDE","USTC","BUSD"}
ALTCOIN_LEVERAGED_SUFFIXES = ("UP","DOWN","BULL","BEAR","3L","3S","5L","5S")


def _bitmart_public_tickers():
    response = requests.get(
        "https://api-cloud.bitmart.com/spot/quotation/v3/tickers",
        timeout=(5, 15),
        headers={"User-Agent": "BlessingTrade-MarketScanner/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or payload.get("code") != 1000:
        raise RuntimeError("BITMART_PUBLIC_API_ERROR")
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise RuntimeError("BITMART_INVALID_TICKER_RESPONSE")
    return rows

def _binance_public_tickers():
    response = requests.get(
        "https://data-api.binance.vision/api/v3/ticker/24hr",
        timeout=(5, 15),
        headers={"User-Agent": "BlessingTrade-MarketScanner/1.0"},
    )
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list):
        raise RuntimeError("BINANCE_INVALID_TICKER_RESPONSE")
    normalized = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        pair = str(row.get("symbol") or "").upper()
        if not pair.endswith("USDT"):
            continue
        normalized.append({
            "symbol": pair,
            "lastPrice": _num(row.get("lastPrice"), 0),
            "quoteVolume": _num(row.get("quoteVolume"), 0),
            "openPrice": _num(row.get("openPrice"), 0),
            "bidPrice": _num(row.get("bidPrice"), 0),
            "askPrice": _num(row.get("askPrice"), 0),
            "closeTime": int(_num(row.get("closeTime"), 0) or 0),
        })
    return normalized


def _binance_1m_klines(symbol: str):
    last_error = None
    rows = None
    for host in ("https://data-api.binance.vision", "https://api.binance.com"):
        try:
            response = requests.get(
                host + "/api/v3/klines",
                params={"symbol": symbol, "interval": "1m", "limit": 65},
                timeout=(4, 10),
                headers={"User-Agent": "BlessingTrade-MarketScanner/1.0"},
            )
            response.raise_for_status()
            rows = response.json()
            if isinstance(rows, list):
                break
            raise RuntimeError("BINANCE_INVALID_KLINE_RESPONSE")
        except Exception as exc:
            last_error = exc
    if not isinstance(rows, list):
        raise RuntimeError("BINANCE_KLINE_ALL_HOSTS_FAILED:" + type(last_error).__name__)
    now_ms = int(time.time() * 1000)
    bars = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 8:
            continue
        try:
            close_time = int(row[6])
            if close_time >= now_ms:
                continue
            bars.append({
                "openTime": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "closeTime": close_time,
                "quoteVolume": float(row[7]),
            })
        except (TypeError, ValueError, IndexError):
            continue
    return bars


def _altcoin_metrics(row: dict, bars: list[dict]) -> dict:
    price = float(row.get("price") or row.get("lastPrice") or 0)
    closes = [b["close"] for b in bars]
    latest = bars[-1] if bars else None
    m1 = ((closes[-1] / closes[-2]) - 1) * 100 if len(closes) >= 2 and closes[-2] else None
    m5 = ((closes[-1] / closes[-6]) - 1) * 100 if len(closes) >= 6 and closes[-6] else None
    m15 = ((closes[-1] / closes[-16]) - 1) * 100 if len(closes) >= 16 and closes[-16] else None
    m60 = ((closes[-1] / closes[-61]) - 1) * 100 if len(closes) >= 61 and closes[-61] else None
    previous = bars[-21:-1] if len(bars) >= 21 else []
    avg_quote_volume = sum(b["quoteVolume"] for b in previous) / len(previous) if previous else 0
    rvol = latest["quoteVolume"] / avg_quote_volume if latest and avg_quote_volume > 0 else None
    prior_highs = [b["high"] for b in bars[-21:-1] if b.get("high") is not None]
    breakout = bool(latest and len(prior_highs) >= 10 and latest["close"] > max(prior_highs))
    spread = None
    bid, ask = float(row.get("bidPrice") or row.get("bid") or 0), float(row.get("askPrice") or row.get("ask") or 0)
    if price > 0 and bid > 0 and ask >= bid:
        spread = ((ask - bid) / price) * 100
    momentum_points = sum(1 for value in (m1, m5, m15, m60) if value is not None and value > 0)
    score = min(100, momentum_points * 15
                + (25 if rvol is not None and rvol >= 2 else 0)
                + (20 if breakout else 0)
                + (15 if spread is not None and 0 < spread <= 0.5 else 0))
    fast_entry = bool(
        m1 is not None and m1 > 0
        and m5 is not None and m5 > 0
        and rvol is not None and rvol >= 2
        and breakout
        and spread is not None and 0 < spread <= 0.5
        and score >= 60
    )
    return {
        "m1": m1, "m5": m5, "m15": m15, "m60": m60,
        "rvol": rvol, "breakout": breakout,
        "volumeSpike": bool(rvol is not None and rvol >= 2),
        "spreadPct": spread, "score": score, "fastEntry": fast_entry,
        "bid": bid or None, "ask": ask or None,
        "candleCount": len(bars),
        "candleTime": latest.get("closeTime") if latest else None,
    }


@app.get("/altcoin-scanner")
def altcoin_scanner(refresh: int = 0):
    now = time.time()
    with ALTCOIN_CACHE_LOCK:
        cached = ALTCOIN_CACHE.get("payload")
        if cached and now - ALTCOIN_CACHE["at"] < 30 and not refresh:
            result = dict(cached)
            result["cacheAgeSeconds"] = int(now - ALTCOIN_CACHE["at"])
            return result

    provider = "Binance"
    bitmart_error = None
    try:
        # Binance is the execution venue for Paper Trading; use it as the primary scanner.
        try:
            tickers = _binance_public_tickers()
        except Exception as primary_error:
            log.warning("Binance primary ticker endpoint failed: %s", type(primary_error).__name__)
            # Try the alternate Binance host before falling back to BitMart display-only data.
            tickers = None
            alternate_error = primary_error
            for host in ("https://api.binance.com", "https://data-api.binance.vision"):
                try:
                    response = requests.get(
                        host + "/api/v3/ticker/24hr", timeout=(4, 10),
                        headers={"User-Agent": "BlessingTrade-MarketScanner/1.0"})
                    response.raise_for_status()
                    raw = response.json()
                    if not isinstance(raw, list):
                        raise RuntimeError("BINANCE_INVALID_TICKER_RESPONSE")
                    tickers = [{
                        "symbol": str(x.get("symbol") or "").upper(),
                        "lastPrice": _num(x.get("lastPrice"), 0),
                        "quoteVolume": _num(x.get("quoteVolume"), 0),
                        "openPrice": _num(x.get("openPrice"), 0),
                        "bidPrice": _num(x.get("bidPrice"), 0),
                        "askPrice": _num(x.get("askPrice"), 0),
                        "closeTime": int(_num(x.get("closeTime"), 0) or 0),
                    } for x in raw if isinstance(x, dict) and str(x.get("symbol") or "").upper().endswith("USDT")]
                    break
                except Exception as fallback_error:
                    alternate_error = fallback_error
            if tickers is None:
                raise RuntimeError("BINANCE_ALL_TICKER_HOSTS_FAILED:" + type(alternate_error).__name__)
    except Exception as binance_error:
        log.error("Binance scanner unavailable: %s", type(binance_error).__name__)
        try:
            tickers = _bitmart_public_tickers()
            provider = "BitMart"
            bitmart_error = type(binance_error).__name__
            log.warning("Using BitMart fallback; Paper entries remain disabled for this feed")
        except Exception as bm_error:
            log.exception("All crypto market data providers failed")
            return {
                "status": "source_error", "provider": "Binance/BitMart Spot",
                "message": "ไม่สามารถเชื่อมต่อแหล่งข้อมูล Binance หรือ BitMart ได้",
                "sourceErrors": {"binance": type(binance_error).__name__, "bitmart": type(bm_error).__name__},
                "scanned": 0, "evaluated": 0, "candidateCount": 0, "candidates": [],
                "fastEntryCount": 0, "updatedAt": datetime.now(timezone.utc).isoformat(),
            }

    eligible = []
    reject = {"not_usdt": 0, "stable_or_leveraged": 0, "low_volume": 0, "invalid_price": 0}
    for item in tickers:
        if provider == "Binance":
            pair = str(item.get("symbol") or "").upper()
            price = _num(item.get("lastPrice"), 0) or 0
            quote_volume = _num(item.get("quoteVolume"), 0) or 0
            open_24h = _num(item.get("openPrice"), 0) or 0
            bid = _num(item.get("bidPrice"), 0) or 0
            ask = _num(item.get("askPrice"), 0) or 0
            stamp = int(_num(item.get("closeTime"), 0) or 0)
        else:
            if not isinstance(item, (list, tuple)) or len(item) < 13:
                continue
            pair = str(item[0] or "").upper().replace("_", "")
            price = _num(item[1], 0) or 0
            quote_volume = _num(item[3], 0) or 0
            open_24h = _num(item[4], 0) or 0
            bid = _num(item[8], 0) or 0
            ask = _num(item[10], 0) or 0
            stamp = int(_num(item[12], 0) or 0)
        if not pair.endswith("USDT"):
            reject["not_usdt"] += 1
            continue
        base = pair[:-4]
        if not base or base in ALTCOIN_EXCLUDED or base.endswith(ALTCOIN_LEVERAGED_SUFFIXES):
            reject["stable_or_leveraged"] += 1
            continue
        if price <= 0 or open_24h <= 0:
            reject["invalid_price"] += 1
            continue
        if quote_volume < 250000:
            reject["low_volume"] += 1
            continue
        change = (price / open_24h - 1) * 100
        spread = ((ask - bid) / price * 100) if bid > 0 and ask >= bid else None
        eligible.append({
            "symbol": pair, "exchangeSymbol": pair,
            "price": price, "change24h": change, "quoteVolume24h": quote_volume,
            "m1": None, "m5": None, "m15": None, "m60": None,
            "rvol": None, "breakout": False, "volumeSpike": False,
            "momentum": change > 0, "spreadPct": spread,
            "score": 0, "fastEntry": False,
            "source": provider, "marketTimestamp": stamp,
            "bidPrice": bid, "askPrice": ask,
        })

    eligible.sort(key=lambda x: x["quoteVolume24h"], reverse=True)
    # Evaluate a broad, liquid top-100 universe; all eligible pairs are counted in scanned.
    selected = eligible[:100]
    candle_errors = 0
    if provider == "Binance" and selected:
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            futures = {pool.submit(_binance_1m_klines, row["symbol"]): row for row in selected}
            for future in concurrent.futures.as_completed(futures):
                row = futures[future]
                try:
                    bars = future.result()
                    row.update(_altcoin_metrics(row, bars))
                except Exception as candle_error:
                    candle_errors += 1
                    row["candleError"] = type(candle_error).__name__
                    row["rejectReason"] = "CANDLE_DATA_UNAVAILABLE"
        metrics_source = "Binance 1m closed candles; 100 highest-volume eligible USDT pairs"
    else:
        metrics_source = "BitMart ticker snapshot only; Paper entries disabled without Binance candles"

    selected.sort(key=lambda x: (
        x.get("fastEntry", False), x.get("score", 0),
        x.get("rvol") or 0, x["change24h"], x["quoteVolume24h"]
    ), reverse=True)
    # Transparent diagnostics: a non-entry candidate is retained with its specific reason.
    for row in selected:
        if row.get("fastEntry"):
            row["rejectReason"] = None
        elif row.get("candleError"):
            row["rejectReason"] = "CANDLE_DATA_UNAVAILABLE"
        elif row.get("m1") is None or row.get("m5") is None:
            row["rejectReason"] = "MOMENTUM_DATA_MISSING"
        elif row.get("m1", 0) <= 0 or row.get("m5", 0) <= 0:
            row["rejectReason"] = "MOMENTUM_NOT_POSITIVE"
        elif (row.get("rvol") or 0) < 2:
            row["rejectReason"] = "RVOL_BELOW_2"
        elif not row.get("breakout"):
            row["rejectReason"] = "NO_20_BAR_BREAKOUT"
        elif not row.get("spreadPct") or row["spreadPct"] <= 0 or row["spreadPct"] > 0.5:
            row["rejectReason"] = "SPREAD_OUT_OF_RANGE"
        else:
            row["rejectReason"] = "SCORE_BELOW_60"

    reject_signals = {}
    for row in selected:
        reason = row.get("rejectReason") or "FAST_ENTRY"
        reject_signals[reason] = reject_signals.get(reason, 0) + 1

    payload = {
        "status": "ok" if selected else "empty",
        "provider": provider + " Spot public market data",
        "exchange": provider,
        "scanned": len(eligible),
        "eligibleTotal": len(eligible),
        "evaluated": len(selected),
        "candidateCount": len(selected),
        "fastEntryCount": sum(1 for x in selected if x.get("fastEntry")),
        "candleErrors": candle_errors,
        "filterCounts": reject,
        "signalDiagnostics": reject_signals,
        "fallbackFrom": bitmart_error,
        "metricsSource": metrics_source,
        "candidates": selected,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "coverage": provider + " Spot USDT pairs; all eligible pairs counted; top 100 by 24h quote volume evaluated; minimum 250,000 USDT",
        "metricsNote": (
            "FAST_ENTRY requires positive 1m/5m momentum, RVOL >= 2, 20-bar breakout, spread > 0 and <= 0.5%, and score >= 60. "
            "Non-entry candidates include rejectReason for diagnostics."
            if provider == "Binance" else
            "Fallback feed is display-only; no Paper entries are allowed without Binance candle data."
        ),
    }
    log.info("Altcoin scan provider=%s eligible=%s evaluated=%s fast_entries=%s candle_errors=%s rejects=%s",
             provider, len(eligible), len(selected), payload["fastEntryCount"], candle_errors, reject_signals)
    with ALTCOIN_CACHE_LOCK:
        ALTCOIN_CACHE.update({"at": time.time(), "payload": payload})
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


# Persistent Paper Trading worker. It runs on the Render service process, not in the browser.
# Render Free may suspend this process while idle; therefore this is best-effort, not guaranteed 24/7.
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
PAPER_WORKER_INTERVAL = max(10, int(os.getenv("PAPER_WORKER_INTERVAL", "15")))
PAPER_WORKER_STATE = {"status": "waiting_config" if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY) else "starting",
                      "lastCycleAt": None, "lastError": None, "cycles": 0, "actions": []}
PAPER_WORKER_THREAD = None
PAPER_WORKER_LOCK = threading.Lock()
PAPER_ENTRY_COOLDOWNS = {}


def _sb_headers(prefer=None):
    headers = {"apikey": SUPABASE_SERVICE_ROLE_KEY,
               "Content-Type": "application/json"}
    if prefer:
        headers["Prefer"] = prefer
    return headers


def _sb_rest(table, params=None, method="GET", body=None):
    if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
        raise RuntimeError("SUPABASE_WORKER_CONFIG_MISSING")
    response = requests.request(method, SUPABASE_URL + "/rest/v1/" + table,
                                params=params or {}, json=body, headers=_sb_headers("return=representation"),
                                timeout=(5, 15))
    response.raise_for_status()
    return response.json() if response.content else []


def _sb_rpc(name, payload):
    response = requests.post(SUPABASE_URL + "/rest/v1/rpc/" + name, json=payload,
                             headers=_sb_headers(), timeout=(5, 15))
    response.raise_for_status()
    return response.json()


def _paper_auth_user(request: Request):
    if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
        raise HTTPException(status_code=503, detail="Paper worker ยังไม่ได้ตั้งค่า Supabase บน Render")
    authorization = request.headers.get("authorization", "")
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="กรุณาเข้าสู่ระบบก่อน")
    token = authorization.split(" ", 1)[1].strip()
    response = requests.get(SUPABASE_URL + "/auth/v1/user",
                            headers={"apikey": SUPABASE_SERVICE_ROLE_KEY, "Authorization": "Bearer " + token},
                            timeout=(5, 12))
    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="เซสชันหมดอายุ กรุณาเข้าสู่ระบบใหม่")
    user = response.json()
    profiles = _sb_rest("profiles", {"select":"role,status", "user_id":"eq." + str(user.get("id")), "limit":"1"})
    if not profiles or profiles[0].get("role") not in ("super_admin", "admin") or profiles[0].get("status") != "active":
        raise HTTPException(status_code=403, detail="สิทธิ์ไม่เพียงพอสำหรับควบคุม Paper Engine")
    return user


@app.post("/paper-engine/start")
def paper_engine_start(request: Request):
    user = _paper_auth_user(request)
    rows = _sb_rest("paper_bot_settings", {"select":"user_id,mode", "user_id":"eq." + str(user["id"]), "limit":"1"})
    if not rows or rows[0].get("mode") != "paper":
        raise HTTPException(status_code=404, detail="ไม่พบการตั้งค่า Paper Trading ของบัญชีนี้")
    account = _sb_rest("paper_bot_accounts", {"select":"user_id", "user_id":"eq." + str(user["id"]), "limit":"1"})
    if not account:
        _sb_rest("paper_bot_accounts", {}, "POST", {"user_id":str(user["id"]), "status":"active"})
    _sb_rest("paper_bot_settings", {"user_id":"eq." + str(user["id"])}, "PATCH",
             {"enabled": True, "updated_at": datetime.now(timezone.utc).isoformat()})
    return {"ok": True, "enabled": True, "mode": "paper", "message": "ส่งคำสั่งเริ่ม Paper Engine ไปยัง Backend แล้ว"}


@app.post("/paper-engine/stop")
def paper_engine_stop(request: Request):
    user = _paper_auth_user(request)
    _sb_rest("paper_bot_settings", {"user_id":"eq." + str(user["id"])}, "PATCH",
             {"enabled": False, "updated_at": datetime.now(timezone.utc).isoformat()})
    return {"ok": True, "enabled": False, "message": "หยุดเปิดสถานะใหม่แล้ว; Backend จะจัดการสถานะ Paper ที่ยังเปิดอยู่ต่อจนปิดตาม TP/SL"}


@app.get("/paper-engine/status")
def paper_engine_status(request: Request):
    user = _paper_auth_user(request)
    settings = _sb_rest("paper_bot_settings", {"select":"enabled,mode,updated_at", "user_id":"eq." + str(user["id"]), "limit":"1"})
    positions = _sb_rest("paper_bot_positions", {"select":"id,symbol,quantity,entry_price,current_price,take_profit_price,stop_loss_price,status",
                                                  "user_id":"eq." + str(user["id"]), "status":"eq.open"})
    return {"ok": True, "configured": True, "worker": dict(PAPER_WORKER_STATE),
            "enabled": bool(settings and settings[0].get("enabled")), "mode": "paper",
            "openPositions": positions}


def _binance_public_price(symbol: str):
    response = requests.get("https://data-api.binance.vision/api/v3/ticker/price",
                            params={"symbol": symbol}, timeout=(4, 8),
                            headers={"User-Agent":"BlessingPaperWorker/1.0"})
    response.raise_for_status()
    payload = response.json()
    price = _num(payload.get("price"), 0) if isinstance(payload, dict) else 0
    return price if price and price > 0 else None

def _paper_worker_cycle():
    settings_rows = _sb_rest("paper_bot_settings", {"select":"user_id,enabled,mode,starting_balance,order_budget,max_open_positions,take_profit_pct,stop_loss_pct,daily_loss_limit_pct,allowed_exchanges",
                                                     "mode":"eq.paper", "limit":"100"})
    for setting in settings_rows:
        uid = setting["user_id"]
        positions = _sb_rest("paper_bot_positions", {"select":"*", "user_id":"eq." + uid, "status":"eq.open", "limit":"20"})
        if not setting.get("enabled") and not positions:
            continue
        market = altcoin_scanner(0)
        updated_at = market.get("updatedAt")
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(updated_at.replace("Z","+00:00"))).total_seconds() if updated_at else 999999
        except Exception:
            age = 999999
        fresh = market.get("status") == "ok" and age <= 90
        candidates = {x.get("symbol"): x for x in (market.get("candidates") or [])
                      if x.get("symbol") and x.get("source") == "Binance"}
        held = {p.get("symbol") for p in positions}
        # Manage existing positions first. No exit is made from stale or missing market prices.
        for pos in positions:
            row = candidates.get(pos.get("symbol"))
            price = (_num(row.get("price"), 0) or 0) if row and fresh else None
            if not price:
                try:
                    price = _binance_public_price(pos.get("symbol"))
                except Exception as price_error:
                    log.warning("Could not refresh open paper position %s: %s", pos.get("symbol"), type(price_error).__name__)
                    continue
            if not price:
                continue
            _sb_rpc("paper_bot_worker_trade", {"p_user_id":uid,"p_action":"mark","p_symbol":pos["symbol"],
                    "p_exchange":"Binance","p_price":price,"p_position_id":pos["id"],"p_reason":"MARK_TO_MARKET","p_signal_score":None})
            reason = "TAKE_PROFIT" if price >= float(pos["take_profit_price"]) else (
                     "STOP_LOSS" if price <= float(pos["stop_loss_price"]) else None)
            if reason:
                result = _sb_rpc("paper_bot_worker_trade", {"p_user_id":uid,"p_action":"sell","p_symbol":pos["symbol"],
                                 "p_exchange":"Binance","p_price":price,"p_position_id":pos["id"],"p_reason":reason,"p_signal_score":None})
                PAPER_WORKER_STATE["actions"].append({"at":datetime.now(timezone.utc).isoformat(),"action":"sell",
                                                       "symbol":pos["symbol"],"reason":reason,"result":result})
                PAPER_WORKER_STATE["actions"] = PAPER_WORKER_STATE["actions"][-20:]
        if not setting.get("enabled") or not fresh:
            continue
        # Refresh open positions after exits and enforce the persisted position cap.
        positions = _sb_rest("paper_bot_positions", {"select":"id,symbol,status", "user_id":"eq." + uid, "status":"eq.open", "limit":"20"})
        if len(positions) >= int(setting.get("max_open_positions") or 5):
            continue
        account_rows = _sb_rest("paper_bot_accounts", {"select":"cash_balance,starting_balance,realized_pnl", "user_id":"eq." + uid, "limit":"1"})
        if not account_rows:
            continue
        account = account_rows[0]
        open_pnl = 0.0
        for p in positions:
            q = candidates.get(p.get("symbol"))
            if q and _num(q.get("price"),0):
                open_pnl += (_num(q.get("price"),0) - _num(p.get("entry_price"),0)) * _num(p.get("quantity"),0)
        total_pnl = float(account.get("realized_pnl") or 0) + open_pnl
        start_balance = float(account.get("starting_balance") or setting.get("starting_balance") or 88.4173298)
        if total_pnl <= -(start_balance * float(setting.get("daily_loss_limit_pct") or 3) / 100):
            continue
        held = {p.get("symbol") for p in positions}
        ranked = [x for x in candidates.values() if x.get("fastEntry") is True and x.get("symbol") not in held
                  and x.get("exchangeSymbol") == x.get("symbol") and
                  _num(x.get("price"),0) and _num(x.get("score"),0) >= 60 and
                  _num(x.get("rvol"),0) >= 2 and _num(x.get("m1"),0) > 0 and _num(x.get("m5"),0) > 0 and
                  x.get("breakout") is True and 0 < (_num(x.get("spreadPct"),0) or 0) <= 0.5]
        ranked.sort(key=lambda x:(x.get("score",0),x.get("rvol",0)),reverse=True)
        allowed = setting.get("allowed_exchanges") or ["Binance"]
        if "Binance" not in allowed:
            continue
        for candidate in ranked:
            if len(positions) >= int(setting.get("max_open_positions") or 5):
                break
            cooldown_key = uid + ":" + candidate["symbol"]
            if time.time() - PAPER_ENTRY_COOLDOWNS.get(cooldown_key,0) < 180:
                continue
            try:
                result = _sb_rpc("paper_bot_worker_trade", {"p_user_id":uid,"p_action":"buy","p_symbol":candidate["symbol"],
                                  "p_exchange":"Binance","p_price":float(candidate["price"]),"p_position_id":None,
                                  "p_reason":"FAST_ENTRY","p_signal_score":int(candidate.get("score") or 0)})
                PAPER_ENTRY_COOLDOWNS[cooldown_key] = time.time()
                PAPER_WORKER_STATE["actions"].append({"at":datetime.now(timezone.utc).isoformat(),"action":"buy",
                                                       "symbol":candidate["symbol"],"result":result})
                PAPER_WORKER_STATE["actions"] = PAPER_WORKER_STATE["actions"][-20:]
                positions.append({"symbol":candidate["symbol"]})
            except Exception as exc:
                log.warning("Paper worker buy skipped for %s: %s", candidate["symbol"], type(exc).__name__)
                continue


def _paper_worker_loop():
    while True:
        try:
            if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
                PAPER_WORKER_STATE.update({"status":"waiting_config","lastError":"SUPABASE_WORKER_CONFIG_MISSING"})
                time.sleep(30)
                continue
            _paper_worker_cycle()
            PAPER_WORKER_STATE.update({"status":"running","lastCycleAt":datetime.now(timezone.utc).isoformat(),
                                       "lastError":None,"cycles":PAPER_WORKER_STATE.get("cycles",0)+1})
        except Exception as exc:
            PAPER_WORKER_STATE.update({"status":"error","lastError":type(exc).__name__,
                                       "lastCycleAt":datetime.now(timezone.utc).isoformat()})
            log.exception("Persistent Paper Trading worker cycle failed")
        time.sleep(PAPER_WORKER_INTERVAL)


@app.on_event("startup")
async def start_paper_worker():
    global PAPER_WORKER_THREAD
    if PAPER_WORKER_THREAD is None or not PAPER_WORKER_THREAD.is_alive():
        PAPER_WORKER_THREAD = threading.Thread(target=_paper_worker_loop, name="paper-trading-worker", daemon=True)
        PAPER_WORKER_THREAD.start()

