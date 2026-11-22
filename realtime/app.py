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

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
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
app.add_middleware(CORSMiddleware, allow_origins=[x.strip() for x in ALLOWED_ORIGINS if x.strip()], allow_credentials=False, allow_methods=["GET"], allow_headers=["*"])

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



# Public Binance Spot altcoin momentum scanner. No account credentials or order endpoints.
ALTCOIN_CACHE = {"at": 0.0, "payload": None}
ALTCOIN_CACHE_LOCK = threading.Lock()
ALTCOIN_EXCLUDED = {"BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","USDCUSDT","FDUSDUSDT","TUSDUSDT","USDPUSDT","DAIUSDT","EURUSDT","USDEUSDT"}
ALTCOIN_LEVERAGED_SUFFIXES = ("UPUSDT","DOWNUSDT","BULLUSDT","BEARUSDT","3LUSDT","3SUSDT","5LUSDT","5SUSDT")


def _binance_public_get(path: str, params: dict | None = None):
    errors = []
    for base in ("https://data-api.binance.vision", "https://api.binance.com"):
        try:
            response = requests.get(base + path, params=params or {}, timeout=(5, 12),
                                    headers={"User-Agent": "BlessingTrade-MarketScanner/1.0"})
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            errors.append(type(exc).__name__)
    raise RuntimeError("BINANCE_PUBLIC_DATA_UNAVAILABLE:" + ",".join(errors))


def _altcoin_kline_metrics(symbol: str):
    data = _binance_public_get("/api/v3/klines", {"symbol": symbol, "interval": "1m", "limit": 8})
    if not isinstance(data, list) or len(data) < 6:
        return None
    bars = data[-6:]
    closes = [float(x[4]) for x in bars]
    highs = [float(x[2]) for x in bars[:-1]]
    quote_volumes = [float(x[7]) for x in bars]
    previous_avg = sum(quote_volumes[:-1]) / max(1, len(quote_volumes) - 1)
    last = closes[-1]
    m1 = (closes[-1] / closes[-2] - 1) * 100 if closes[-2] else 0.0
    m5 = (closes[-1] / closes[0] - 1) * 100 if closes[0] else 0.0
    rvol = quote_volumes[-1] / previous_avg if previous_avg > 0 else 0.0
    breakout = bool(highs and last > max(highs))
    return {"price": last, "m1": m1, "m5": m5, "rvol": rvol,
            "breakout": breakout, "volumeSpike": rvol >= 2.0,
            "momentum": m1 > 0 and m5 > 0}


@app.get("/altcoin-scanner")
def altcoin_scanner(refresh: int = 0):
    now = time.time()
    with ALTCOIN_CACHE_LOCK:
        cached = ALTCOIN_CACHE.get("payload")
        if cached and now - ALTCOIN_CACHE["at"] < 45 and not refresh:
            result = dict(cached)
            result["cacheAgeSeconds"] = int(now - ALTCOIN_CACHE["at"])
            return result
    try:
        tickers = _binance_public_get("/api/v3/ticker/24hr")
        eligible = []
        for item in tickers if isinstance(tickers, list) else []:
            symbol = str(item.get("symbol") or "").upper()
            if not symbol.endswith("USDT") or symbol in ALTCOIN_EXCLUDED:
                continue
            if symbol.endswith(ALTCOIN_LEVERAGED_SUFFIXES):
                continue
            try:
                price = float(item.get("lastPrice") or 0)
                quote_volume = float(item.get("quoteVolume") or 0)
                change = float(item.get("priceChangePercent") or 0)
            except (TypeError, ValueError):
                continue
            if price <= 0 or quote_volume < 500000:
                continue
            eligible.append({"symbol": symbol, "price": price,
                             "change24h": change, "quoteVolume24h": quote_volume})
        eligible.sort(key=lambda x: x["quoteVolume24h"], reverse=True)
        selected = eligible[:50]
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(_altcoin_kline_metrics, row["symbol"]): row for row in selected}
            for future in concurrent.futures.as_completed(futures):
                row = futures[future]
                try:
                    metrics = future.result()
                    if metrics:
                        results.append({**row, **metrics})
                except Exception as exc:
                    log.debug("Altcoin kline skipped %s: %s", row["symbol"], type(exc).__name__)
        results.sort(key=lambda x: (
            int(x["breakout"]) * 3 + int(x["volumeSpike"]) * 2 + int(x["momentum"])
            + max(-5, min(10, x["m5"])) * 0.25
            + max(0, min(8, x["change24h"])) * 0.1
        ), reverse=True)
        payload = {"status": "ok", "provider": "Binance Spot public market data",
                   "scanned": len(eligible), "evaluated": len(results),
                   "candidateCount": len(results), "candidates": results[:40],
                   "updatedAt": datetime.now(timezone.utc).isoformat(),
                   "coverage": "Binance Spot USDT pairs only; top 50 by 24h quote volume are evaluated"}
        with ALTCOIN_CACHE_LOCK:
            ALTCOIN_CACHE.update({"at": time.time(), "payload": payload})
        return payload
    except Exception as exc:
        log.exception("Altcoin scanner failed")
        return {"status": "source_error", "provider": "Binance Spot",
                "message": "แหล่งข้อมูล Binance ไม่พร้อมใช้งานในขณะนี้ (" + type(exc).__name__ + ")",
                "scanned": 0, "evaluated": 0, "candidateCount": 0,
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
