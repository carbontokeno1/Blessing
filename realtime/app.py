import asyncio
import concurrent.futures
import base64
import hashlib
import hmac
import urllib.parse
import requests
from requests.adapters import HTTPAdapter
import json
import websockets
import logging
import math
import os
import threading
import time
import uuid
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from broker_adapters import brokerage_readonly_snapshot, set_webull_api_client
from ai_decision import build_decision_preview
from adaptive_strategy import adaptive_decision, paper_opportunity_decision, build_binance_th_candidates, experiment_window
from dual_portfolio import portfolio_overview
from fastapi.middleware.cors import CORSMiddleware
from member_capital import register_member_capital_routes
from member_access import register_member_access_routes
from owner_auto_trade import register_owner_auto_trade

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("blessing-realtime")

SYMBOLS = [s.strip().upper() for s in os.getenv("WEBULL_SYMBOLS", "AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA,AMD").split(",") if s.strip()]
APP_KEY = (os.getenv("WEBULL_APP_KEY") or "").strip()
APP_SECRET = (os.getenv("WEBULL_APP_SECRET") or "").strip()
# Alpaca IEX REST market data powers the stock universe scanner and Paper engine.
# Accept standard Alpaca dashboard variable names plus legacy aliases.
ALPACA_KEY = (os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY") or os.getenv("ALPACA_KEY") or "").strip()
ALPACA_SECRET = (os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("ALPACA_SECRET") or "").strip()
ALPACA_FEED = (os.getenv("ALPACA_DATA_FEED") or "iex").strip().lower()
if ALPACA_FEED not in {"iex", "sip", "delayed_sip"}:
    ALPACA_FEED = "iex"
BINANCE_TH_API_BASE = os.getenv("BINANCE_TH_API_BASE", "https://api.binance.th").rstrip("/")
BINANCE_API_KEY = (os.getenv("BINANCE_TH_API_KEY", "") or os.getenv("BINANCE_API_KEY", "")).strip()
BINANCE_API_SECRET = (os.getenv("BINANCE_TH_API_SECRET", "") or os.getenv("BINANCE_API_SECRET", "")).strip()
# REST batch scanning avoids Alpaca Basic websocket symbol limits.
ALPACA_DISABLED = not bool(ALPACA_KEY and ALPACA_SECRET)
ALPACA_STREAM_ENABLED = os.getenv("ALPACA_STREAM_ENABLED", "true").strip().lower() in {"1","true","yes","on"}
REGION = os.getenv("WEBULL_REGION", "th")
HTTP_HOST = os.getenv("WEBULL_HTTP_HOST", "api.webull.co.th")
MQTT_HOST = os.getenv("WEBULL_MQTT_HOST", "data-api.webull.co.th")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "https://blessing.carbontokeno1.workers.dev").split(",")

app = FastAPI(title="Blessing Market Data Bridge", version="0.2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False, allow_methods=["GET", "POST", "PUT", "OPTIONS"], allow_headers=["*"], max_age=600)

state = {
    "provider": "webull",
    "connection": "waiting_credentials",
    "marketDataEntitlement": "not_verified",
    "symbols": SYMBOLS,
    "lastMessageAt": None,
    "lastError": None,
    "webullConnection": "waiting_credentials",
    "webullLastError": None,
    "webullLastMessageAt": None,
    "alpacaAuthenticated": False,
    "alpacaAuthenticatedAt": None,
    "quotes": {},
    "alpaca_quotes": {},
    "crypto_quotes": {},
    "crypto_book_quotes": {},
    "cryptoConnection": "starting",
    "lastCryptoMessageAt": None,
    "barHistory": {},
    "sessionOpen": {},
    "sessionDate": {},
}
CRYPTO_TICKER_HISTORY: dict[str, list] = {}
CRYPTO_TICKER_HISTORY_LOCK = threading.Lock()
CRYPTO_TICKER_HISTORY_MAX = 3700
CRYPTO_TICKER_HISTORY_SYMBOL_LIMIT = 150

clients: set[WebSocket] = set()
loop: asyncio.AbstractEventLoop | None = None
stream_thread: threading.Thread | None = None
webull_stream_thread: threading.Thread | None = None
_WEBULL_SHARED_API_CLIENT = None
_WEBULL_SHARED_API_CLIENT_LOCK = threading.Lock()


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
        previous = state["quotes"].get(symbol, {})
        previous_raw = previous.get("raw", {}) if isinstance(previous, dict) else {}
        merged_raw = {**previous_raw, **item} if isinstance(previous_raw, dict) else item
        state["quotes"][symbol] = {
            **(previous if isinstance(previous, dict) else {}),
            "symbol": symbol,
            "topic": topic,
            "receivedAt": now,
            "source": "Webull OpenAPI real-time stream",
            "raw": merged_raw,
        }
        state["webullLastMessageAt"] = now
        state["webullConnection"] = "streaming"
        state["webullLastError"] = None
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


def on_subscribe(client, api_client, session_id):
    log.info("Webull market-data subscription accepted for %d symbols", len(SYMBOLS))
    if state.get("overnightMarketDataAvailable") is False:
        state["marketDataEntitlement"] = "regular_session_fallback"
    else:
        state["marketDataEntitlement"] = "subscription_accepted"
        state["overnightMarketDataAvailable"] = True
    state["webullLastError"] = None
    state["lastError"] = None


def on_connect(client, api_client, session_id):
    global _WEBULL_SHARED_API_CLIENT
    # Reuse the authenticated API client created by the MQTT stream.
    # Creating a second ApiClient triggers a separate Webull verification flow
    # and can exhaust the account's verification-attempt limit.
    with _WEBULL_SHARED_API_CLIENT_LOCK:
        _WEBULL_SHARED_API_CLIENT = api_client
    # Share the stream's already-authenticated client with read-only account checks.
    # A second ApiClient would start another Webull 2FA/token flow.
    set_webull_api_client(api_client)
    log.info("Webull market stream connected; shared authenticated REST client ready")
    state["connection"] = "connected"
    state["webullConnection"] = "connected"
    state["webullLastError"] = None
    state["lastError"] = None
    try:
        from webull.data.common.category import Category
        from webull.data.common.subscribe_type import SubscribeType
        # Webull OpenAPI supports independent QUOTE (BBO/order book),
        # SNAPSHOT, and TICK (time-and-sales) stream subscriptions for US stocks.
        # Request each separately so one entitlement rejection does not block
        # the other authorized data types.
        requested = []
        state["overnightMarketDataAvailable"] = None
        for data_type in (SubscribeType.SNAPSHOT, SubscribeType.QUOTE, SubscribeType.TICK):
            try:
                client.subscribe(SYMBOLS, Category.US_STOCK.name, [data_type.name], overnight_required=True)
                requested.append(data_type.name + ":OVERNIGHT")
                log.info("Requested Webull %s subscription with overnight data for %d symbols", data_type.name, len(SYMBOLS))
            except Exception as sub_exc:
                error_text = str(sub_exc).upper()
                overnight_denied = ("MARKET_DATA_NOT_SUBSCRIBED" in error_text and
                                    ("US_STOCK OVERNIGHT" in error_text or "NIGHT TRADING STOCK QUOTES" in error_text))
                if overnight_denied:
                    state["overnightMarketDataAvailable"] = False
                    state["marketDataEntitlement"] = "regular_session_fallback"
                    try:
                        client.subscribe(SYMBOLS, Category.US_STOCK.name, [data_type.name], overnight_required=False)
                        requested.append(data_type.name + ":REGULAR")
                        log.warning("Webull %s overnight data not entitled; regular-session subscription accepted", data_type.name)
                        continue
                    except Exception:
                        log.exception("Webull %s regular-session fallback subscription failed", data_type.name)
                log.exception("Webull %s subscription request failed", data_type.name)
        state["webullRequestedSubscriptions"] = requested
        if not requested:
            raise RuntimeError("All Webull market-data subscriptions were rejected")
    except Exception as exc:
        state["connection"] = "subscription_error"
        state["webullConnection"] = "subscription_error"
        state["marketDataEntitlement"] = "subscription_rejected"
        state["webullLastError"] = type(exc).__name__
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
        client.on_subscribe_success = on_subscribe
        client.on_quotes_message = on_message
        log.info("Starting Webull MQTT stream for %s in region %s", ",".join(SYMBOLS), REGION)
        state["webullConnection"] = "connecting"
        client.connect_and_loop_forever()
    except Exception as exc:
        state["connection"] = "error"
        state["webullConnection"] = "error"
        state["webullLastError"] = type(exc).__name__
        state["lastError"] = type(exc).__name__
        log.exception("Webull stream stopped")


def alpaca_stream_main():
    """Run an independent Alpaca market-data stream; never overwrite Webull quotes."""
    if not (ALPACA_KEY and ALPACA_SECRET):
        state["alpacaConnection"] = "waiting_credentials"
        log.warning("Alpaca stream not started: credentials are missing")
        return
    feed = ALPACA_FEED if ALPACA_FEED in {"iex", "sip", "delayed_sip"} else "iex"
    url = f"wss://stream.data.alpaca.markets/v2/{feed}"
    while True:
        try:
            async def run_stream():
                state["alpacaConnection"] = "connecting"
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
                    state["alpacaConnection"] = "connected"
                    state["alpacaLastError"] = None
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
                            prior = state["alpaca_quotes"].get(symbol, {})
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
                            state["alpaca_quotes"][symbol] = {
                                "symbol": symbol, "topic": kind, "receivedAt": now,
                                "source": f"Alpaca {feed.upper()} real-time stream", "raw": quote
                            }
                            changed = True
                        if changed:
                            state["alpacaLastMessageAt"] = now
                            state["alpacaConnection"] = "streaming"
                            if loop and loop.is_running():
                                asyncio.run_coroutine_threadsafe(
                                    broadcast({"type": "alpaca_quotes", "data": state["alpaca_quotes"], "status": public_status()}), loop
                                )
            asyncio.run(run_stream())
        except Exception as exc:
            state["alpacaAuthenticated"] = False
            state["alpacaConnection"] = "reconnecting"
            state["alpacaLastError"] = type(exc).__name__
            # Keep the diagnostic traceback so transient server-side closes can be
            # distinguished from authentication/subscription errors without logging secrets.
            log.exception("Alpaca stream disconnected (%s); retrying shortly", type(exc).__name__)
            time.sleep(5)


def binance_th_stream_main():
    """Consume Binance TH all-market ticker updates over its official public WebSocket."""
    url = "wss://nbstream.binance.th/w3w/wsa/stream?streams=!ticker@arr"
    while True:
        try:
            async def run_stream():
                state["cryptoConnection"] = "connecting"
                async with websockets.connect(url, ping_interval=60, ping_timeout=30, close_timeout=5, max_size=2**22) as ws:
                    state["cryptoConnection"] = "connected"
                    log.info("Binance TH all-market 24h ticker stream connected")
                    async for raw_message in ws:
                        payload = json.loads(raw_message)
                        data = payload.get("data") if isinstance(payload, dict) and "data" in payload else payload
                        items = data if isinstance(data, list) else [data]
                        now = datetime.now(timezone.utc).isoformat()
                        changed = False
                        changed_quotes = {}
                        for item in items:
                            if not isinstance(item, dict):
                                continue
                            symbol = str(item.get("s") or "").upper()
                            # The all-book-tickers stream carries best bid/ask separately
                            # from miniTicker. Cache it and merge it into the ticker snapshot.
                            if symbol and "c" not in item and "b" in item and "a" in item:
                                book = {
                                    "bid": _num(item.get("b"), 0),
                                    "ask": _num(item.get("a"), 0),
                                    "receivedAt": now,
                                }
                                state["crypto_book_quotes"][symbol] = book
                                existing = state["crypto_quotes"].get(symbol)
                                if isinstance(existing, dict):
                                    existing.update({"bid": book["bid"], "ask": book["ask"],
                                                     "bookReceivedAt": now})
                                    changed_quotes[symbol] = dict(existing)
                                    changed = True
                                continue
                            price = _num(item.get("c"), 0)
                            if not symbol or price <= 0:
                                continue
                            # Binance TH miniTicker payload uses open (o), close (c),
                            # base volume (v), and quote volume (q). Best bid/ask comes
                            # from the companion !bookTicker stream.
                            open_price = _num(item.get("o"), 0)
                            book = state["crypto_book_quotes"].get(symbol) or {}
                            bid = _num(book.get("bid"), 0)
                            ask = _num(book.get("ask"), 0)
                            quote_volume = _num(item.get("q"), 0)
                            change_pct = ((price / open_price) - 1) * 100 if open_price > 0 else 0
                            quote_row = {
                                "symbol": symbol,
                                "price": float(price),
                                "bid": bid,
                                "ask": ask,
                                "quoteVolume24h": quote_volume,
                                "change24hPct": change_pct,
                                "eventTime": item.get("E"),
                                "receivedAt": now,
                                "source": "Binance TH WebSocket live ticker",
                            }
                            state["crypto_quotes"][symbol] = quote_row
                            base_asset = symbol[:-4] if symbol.endswith("USDT") else ""
                            if (quote_volume and quote_volume >= 250000 and base_asset
                                    and base_asset not in {"USDC","FDUSD","TUSD","USDP","DAI","EUR","USDE","USTC","BUSD","BFUSD"}
                                    and not base_asset.endswith(("UP","DOWN","BULL","BEAR","3L","3S","5L","5S"))):
                                with CRYPTO_TICKER_HISTORY_LOCK:
                                    if symbol not in CRYPTO_TICKER_HISTORY and len(CRYPTO_TICKER_HISTORY) >= CRYPTO_TICKER_HISTORY_SYMBOL_LIMIT:
                                        weakest = min(CRYPTO_TICKER_HISTORY, key=lambda key: CRYPTO_TICKER_HISTORY[key][-1][2] if CRYPTO_TICKER_HISTORY[key] else 0)
                                        weakest_volume = CRYPTO_TICKER_HISTORY[weakest][-1][2] if CRYPTO_TICKER_HISTORY[weakest] else 0
                                        if quote_volume > weakest_volume:
                                            CRYPTO_TICKER_HISTORY.pop(weakest, None)
                                    if symbol in CRYPTO_TICKER_HISTORY or len(CRYPTO_TICKER_HISTORY) < CRYPTO_TICKER_HISTORY_SYMBOL_LIMIT:
                                        history = CRYPTO_TICKER_HISTORY.setdefault(symbol, [])
                                        history.append((time.time(), float(price), float(quote_volume), float(bid or 0), float(ask or 0)))
                                        if len(history) > CRYPTO_TICKER_HISTORY_MAX:
                                            del history[:-CRYPTO_TICKER_HISTORY_MAX]
                            changed_quotes[symbol] = quote_row
                            changed = True
                        if changed:
                            state["lastCryptoMessageAt"] = now
                            state["cryptoConnection"] = "streaming"
                            if not state.get("cryptoStreamFirstFrameLogged"):
                                log.info("Binance TH ticker stream first data frame: received=%d cached=%d",
                                         len(items), len(state.get("crypto_quotes") or {}))
                                state["cryptoStreamFirstFrameLogged"] = True
                            if loop and loop.is_running():
                                asyncio.run_coroutine_threadsafe(
                                    broadcast({
                                        "type": "market_data",
                                        "data": state["quotes"],
                                        "cryptoData": changed_quotes,
                                        "status": public_status(),
                                    }), loop
                                )
            asyncio.run(run_stream())
        except Exception as exc:
            state["cryptoConnection"] = "reconnecting"
            log.warning("Binance TH WebSocket disconnected (%s); retrying", type(exc).__name__)
            time.sleep(5)


def public_status() -> dict:
    return {
        "provider": state["provider"],
        "connection": state["connection"],
        "marketDataEntitlement": state["marketDataEntitlement"],
        "symbols": state["symbols"],
        "quoteCount": len(state["quotes"]),
        "lastMessageAt": state["lastMessageAt"],
        "lastMarketDataAt": state["lastMessageAt"],
        "lastError": state["lastError"],
        "alpacaConfigured": bool(ALPACA_KEY and ALPACA_SECRET),
        "alpacaKeyConfigured": bool(ALPACA_KEY),
        "alpacaSecretConfigured": bool(ALPACA_SECRET),
        "alpacaAuthenticated": bool(state.get("alpacaAuthenticated")),
        "alpacaAuthenticatedAt": state.get("alpacaAuthenticatedAt"),
        "alpacaConnected": UNIVERSE_STATE.get("status") == "ok",
        "stockProvider": "Alpaca IEX REST universe scanner" if ALPACA_KEY and ALPACA_SECRET else "Webull OpenAPI fallback" if APP_KEY and APP_SECRET else "waiting for market-data credentials",
        "webullConfigured": bool(APP_KEY and APP_SECRET),
        "webullRegion": REGION,
        "webullConnection": state.get("webullConnection"),
        "webullLastError": state.get("webullLastError"),
        "webullLastMessageAt": state.get("webullLastMessageAt"),
        "webullQuoteCount": sum(1 for q in state["quotes"].values() if isinstance(q, dict) and q.get("source") == "Webull OpenAPI real-time stream"),
        "overnightMarketDataAvailable": state.get("overnightMarketDataAvailable"),
        "marketDataMode": "regular_session_fallback" if state.get("overnightMarketDataAvailable") is False else "overnight_and_regular" if state.get("overnightMarketDataAvailable") is True else "not_verified",
        "universeScanner": {
            "status": UNIVERSE_STATE.get("status"),
            "universeCount": UNIVERSE_STATE.get("universeCount"),
            "scanned": UNIVERSE_STATE.get("scanned"),
            "candidateCount": UNIVERSE_STATE.get("candidateCount"),
            "updatedAt": UNIVERSE_STATE.get("updatedAt"),
            "feed": "iex",
        },
        "marketDataReceived": bool(state["lastMessageAt"]),
        "cryptoConnection": state.get("cryptoConnection"),
        "cryptoQuoteCount": len(state.get("crypto_quotes") or {}),
        "lastCryptoMarketDataAt": state.get("lastCryptoMessageAt"),
        "serverTime": datetime.now(timezone.utc).isoformat(),
    }


def binance_th_book_stream_main():
    """Consume Binance TH best-bid/ask updates on a separate official public stream."""
    url = "wss://nbstream.binance.th/w3w/wsa/stream?streams=!bookTicker"
    while True:
        try:
            async def run_stream():
                async with websockets.connect(url, ping_interval=60, ping_timeout=30, close_timeout=5, max_size=2**22) as ws:
                    log.info("Binance TH all-market book-ticker stream connected")
                    async for raw_message in ws:
                        payload = json.loads(raw_message)
                        data = payload.get("data") if isinstance(payload, dict) and "data" in payload else payload
                        items = data if isinstance(data, list) else [data]
                        now = datetime.now(timezone.utc).isoformat()
                        changed_quotes = {}
                        for item in items:
                            if not isinstance(item, dict):
                                continue
                            symbol = str(item.get("s") or "").upper()
                            bid = _num(item.get("b"), 0)
                            ask = _num(item.get("a"), 0)
                            if not symbol or bid <= 0 or ask <= 0 or ask < bid:
                                continue
                            state["crypto_book_quotes"][symbol] = {
                                "bid": bid, "ask": ask, "receivedAt": now
                            }
                            existing = state["crypto_quotes"].get(symbol)
                            if isinstance(existing, dict):
                                existing.update({
                                    "bid": bid, "ask": ask, "bookReceivedAt": now
                                })
                                changed_quotes[symbol] = dict(existing)
                        if changed_quotes and loop and loop.is_running():
                            asyncio.run_coroutine_threadsafe(
                                broadcast({
                                    "type": "market_data",
                                    "data": state["quotes"],
                                    "cryptoData": changed_quotes,
                                    "status": public_status(),
                                }), loop
                            )
            asyncio.run(run_stream())
        except Exception as exc:
            log.warning("Binance TH book-ticker WebSocket disconnected (%s); retrying", type(exc).__name__)
            time.sleep(5)


@app.on_event("startup")
async def startup():
    global loop, stream_thread
    loop = asyncio.get_running_loop()
    global crypto_stream_thread
    crypto_stream_thread = threading.Thread(target=binance_th_stream_main, name="binance-th-live-ticker", daemon=True)
    crypto_stream_thread.start()
    global crypto_book_stream_thread
    crypto_book_stream_thread = threading.Thread(target=binance_th_book_stream_main, name="binance-th-book-ticker", daemon=True)
    crypto_book_stream_thread.start()
    # Alpaca IEX is the temporary primary US-equity source; Webull is fallback.
    alpaca_ready = bool(ALPACA_KEY and ALPACA_SECRET)
    state["provider"] = "alpaca_iex" if alpaca_ready else "webull"
    if alpaca_ready and ALPACA_STREAM_ENABLED:
        alpaca_thread = threading.Thread(target=alpaca_stream_main, name="alpaca-us-equity-stream", daemon=True)
        alpaca_thread.start()
        log.info("Alpaca %s stream started as PRIMARY US-stock market-data provider", ALPACA_FEED.upper())
    if not alpaca_ready and APP_KEY and APP_SECRET:
        state["connection"] = "connecting"
        state["marketDataEntitlement"] = "webull_subscription_pending"
        state["webullConnection"] = "connecting"
        stream_thread = threading.Thread(target=stream_main, name="webull-mqtt", daemon=True)
        stream_thread.start()
        log.info("Webull stream started as fallback US-stock market-data provider")
    elif not alpaca_ready:
        state["connection"] = "waiting_credentials"
        state["webullConnection"] = "waiting_credentials"
        present = sorted(k for k in os.environ if k.startswith("WEBULL_"))
        log.warning("No primary US stock market data credentials; Webull variable names=%s; webull_key_present=%s webull_secret_present=%s", present, bool(APP_KEY), bool(APP_SECRET))
    global UNIVERSE_THREAD
    if alpaca_ready or (APP_KEY and APP_SECRET):
        if UNIVERSE_THREAD is None or not UNIVERSE_THREAD.is_alive():
            UNIVERSE_THREAD = threading.Thread(target=_universe_scanner_loop, name="us-equity-universe-scanner", daemon=True)
            UNIVERSE_THREAD.start()
        log.info("%s US universe scanner started; target_interval=%ss", "Alpaca IEX" if alpaca_ready else "Webull fallback", UNIVERSE_SCAN_INTERVAL)
    else:
        with UNIVERSE_LOCK:
            UNIVERSE_STATE.update({"status":"waiting_credentials","provider":"Alpaca IEX / Webull fallback","lastError":"NO_MARKET_DATA_CREDENTIALS"})
        log.warning("US equity universe scanner waiting for Alpaca or Webull credentials.")



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
    if rank_type not in SCANNER_TYPES or direction not in {"ASC","DESC"}:
        return {"status":"error","message":"พารามิเตอร์การสแกนไม่ถูกต้อง","candidates":[],"scanned":0}
    with UNIVERSE_LOCK:
        scan = dict(UNIVERSE_STATE)
    candidates = list(scan.get("candidates") or [])
    if rank_type == "PRE_MARKET":
        candidates = [x for x in candidates if x.get("session") == "PRE_MARKET"]
        key = lambda x: _num(x.get("changePct"),-999) or -999
    elif rank_type == "AFTER_MARKET":
        candidates = [x for x in candidates if x.get("session") == "AFTER_HOURS"]
        key = lambda x: _num(x.get("changePct"),-999) or -999
    elif rank_type == "MIN_3":
        candidates = [x for x in candidates if x.get("m3") is not None]
        key = lambda x: _num(x.get("m3"),-999) or -999
    elif rank_type == "MIN_5":
        candidates = [x for x in candidates if x.get("m5") is not None]
        key = lambda x: _num(x.get("m5"),-999) or -999
    else:
        key = lambda x: _num(x.get("changePct"),-999) or -999
    candidates.sort(key=key, reverse=(direction=="DESC"))
    return {
        "provider":scan.get("feed") or scan.get("provider") or "Alpaca IEX","status":scan.get("status","waiting_scan"),
        "message":"จัดอันดับจากแหล่งข้อมูลหุ้นที่ระบุใน feed; แสดงข้อจำกัดของ IEX/ผู้ให้บริการตามจริง",
        "rankType":rank_type,"direction":direction,"scanned":scan.get("scanned",0),
        "candidates":candidates[:100],"updatedAt":scan.get("updatedAt"),
        "fastEntryCount":scan.get("fastEntryCount",0),
        "fastEntryRejectionCounts":scan.get("fastEntryRejectionCounts",{}),
        "feed":scan.get("feed") or scan.get("provider") or "Alpaca IEX","coverage":scan.get("coverage"),
        "lastError":scan.get("lastError"),
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


@app.get("/quotes-fallback")
def quotes_fallback(symbols: str = "AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA,AMD"):
    """Fresh Alpaca/Webull first; Yahoo is display-only and never a trade signal."""
    requested = []
    for item in symbols.split(","):
        symbol = item.strip().upper()
        if symbol and symbol.replace(".", "").replace("-", "").isalnum() and symbol not in requested:
            requested.append(symbol)
    requested = requested[:20]
    if not requested:
        return {"status":"error","message":"ไม่มีสัญลักษณ์หุ้นที่ถูกต้อง","quotes":[],"count":0}
    now = datetime.now(timezone.utc)
    output, errors = {}, {}

    def age_seconds(stamp):
        if not stamp:
            return None
        try:
            parsed = datetime.fromisoformat(str(stamp).replace("Z","+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return max(0,(now-parsed.astimezone(timezone.utc)).total_seconds())
        except Exception:
            return None

    for symbol in requested:
        live = state.get("alpaca_quotes",{}).get(symbol) or {}
        raw = live.get("raw") or {}
        price = _num(raw.get("price"),_num(raw.get("last_price"),0)) or 0
        stamp = raw.get("timestamp") or raw.get("quote_timestamp")
        age = age_seconds(stamp)
        if price > 0 and age is not None and age <= 180:
            history = state.get("barHistory",{}).get(symbol,[])
            closes = [_num(bar.get("close"),None) for bar in history]
            closes = [v for v in closes if v and v > 0]
            m1 = ((closes[-1]/closes[-2])-1)*100 if len(closes)>=2 else None
            m5 = ((closes[-1]/closes[-6])-1)*100 if len(closes)>=6 else None
            daily_open = _num(raw.get("daily_open"),None)
            output[symbol] = {"symbol":symbol,"price":price,
                "changePct":((price/daily_open)-1)*100 if daily_open else None,
                "volume":int(_num(raw.get("daily_volume"),_num(raw.get("volume"),0)) or 0),
                "m1":m1,"m5":m5,"session":"UNKNOWN",
                "priceTime":int((now.timestamp()-age)*1000),"receivedAt":live.get("receivedAt"),
                "source":live.get("source") or "Alpaca stream","stale":False,"staleSeconds":int(age)}

    for symbol in requested:
        if symbol in output:
            continue
        live = state.get("quotes",{}).get(symbol) or {}
        raw = live.get("raw") or {}
        price = _num(raw.get("price"),_num(raw.get("last_price"),0)) or 0
        age = age_seconds(raw.get("timestamp") or raw.get("quote_timestamp"))
        if price > 0 and age is not None and age <= 60:
            output[symbol] = {"symbol":symbol,"price":price,"changePct":None,
                "volume":int(_num(raw.get("daily_volume"),_num(raw.get("volume"),0)) or 0),
                "m1":None,"m5":None,"session":"UNKNOWN",
                "priceTime":int((now.timestamp()-age)*1000),"receivedAt":live.get("receivedAt"),
                "source":live.get("source") or "Webull stream","stale":False,"staleSeconds":int(age)}

    missing = [symbol for symbol in requested if symbol not in output]
    if missing and ALPACA_KEY and ALPACA_SECRET:
        try:
            snapshots = _alpaca_snapshots(missing)
            for symbol in missing:
                snap = snapshots.get(symbol) or {}
                trade, minute = snap.get("latestTrade") or {}, snap.get("minuteBar") or {}
                daily, previous = snap.get("dailyBar") or {}, snap.get("prevDailyBar") or {}
                price = _num(trade.get("p"),_num(minute.get("c"),0)) or 0
                age = age_seconds(trade.get("t") or minute.get("t"))
                if price <= 0 or age is None or age > 180:
                    continue
                prev_close = _num(previous.get("c"),None)
                output[symbol] = {"symbol":symbol,"price":price,
                    "changePct":((price/prev_close)-1)*100 if prev_close else None,
                    "volume":int(_num(daily.get("v"),0) or 0),"m1":None,"m5":None,
                    "session":"UNKNOWN","priceTime":int((now.timestamp()-age)*1000),
                    "receivedAt":now.isoformat(),"source":"Alpaca IEX REST snapshot",
                    "stale":False,"staleSeconds":int(age)}
        except Exception as exc:
            errors["alpaca"] = type(exc).__name__

    missing = [symbol for symbol in requested if symbol not in output]
    if missing:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(4,len(missing))) as pool:
            futures = {pool.submit(_fetch_yahoo_chart_quote,symbol):symbol for symbol in missing}
            for future in concurrent.futures.as_completed(futures):
                symbol = futures[future]
                try:
                    row = future.result()
                    row["stale"] = True
                    row["source"] = "Yahoo Finance chart (display-only; not execution data)"
                    output[symbol] = row
                except Exception as exc:
                    errors[symbol] = type(exc).__name__
    rows = [output[s] for s in requested if s in output]
    return {"status":"ok" if rows else "source_unavailable",
        "provider":"Alpaca/Webull live + Yahoo display-only fallback",
        "message":"ข้อมูล stale ใช้แสดงเป็นข้อมูลอ้างอิงเท่านั้น ไม่ใช้ส่งคำสั่ง Paper",
        "quotes":rows,"count":len(rows),
        "staleCount":sum(1 for row in rows if row.get("stale")),
        "missingSymbols":[s for s in requested if s not in output],
        "errors":errors,"streamStatus":public_status(),"updatedAt":now.isoformat()}


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
UNIVERSE_SCAN_INTERVAL = max(180, int(os.getenv("UNIVERSE_SCAN_INTERVAL", "300")))
UNIVERSE_LIQUIDITY_LIMIT = max(200, min(2000, int(os.getenv("UNIVERSE_LIQUIDITY_LIMIT", "800"))))
UNIVERSE_BATCH_SIZE = 100
UNIVERSE_CANDIDATE_LIMIT = 150
UNIVERSE_CACHE_SECONDS = 6 * 60 * 60
UNIVERSE_STATE: dict[str, Any] = {
    "status": "waiting_scan", "provider": "Webull OpenAPI",
    "universeCount": 0, "scanned": 0, "evaluated": 0,
    "candidateCount": 0, "candidates": [], "updatedAt": None,
    "lastError": None, "cycleSeconds": None, "feed": "Webull OpenAPI",
    "coverage": "Webull US_STOCK + US_ETF catalogue; data entitlement and exchange coverage apply",
}
UNIVERSE_LOCK = threading.Lock()
UNIVERSE_ASSETS: list[dict] = []
UNIVERSE_ASSETS_AT = 0.0
UNIVERSE_THREAD: threading.Thread | None = None
UNIVERSE_EXCHANGES = {"NASDAQ", "NYSE", "AMEX", "ARCA", "BATS", "NYSEARCA", "NYSEAMERICAN", "IEX"}


def _alpaca_headers():
    return {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET}


def _alpaca_news_catalysts(symbols: list[str]) -> dict[str, dict]:
    """Return only recent, explicitly positive, symbol-matched news catalysts."""
    if not ALPACA_KEY or not ALPACA_SECRET or not symbols:
        return {}
    try:
        response = requests.get(
            "https://data.alpaca.markets/v1beta1/news",
            params={
                "symbols": ",".join(list(dict.fromkeys(symbols))[:50]),
                "start": (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat(),
                "limit": 100, "sort": "desc",
            },
            headers=_alpaca_headers(), timeout=(3, 8),
        )
        response.raise_for_status()
        payload = response.json()
        positive_terms = (
            "beats estimates", "earnings beat", "raises guidance", "raised guidance",
            "fda approval", "wins contract", "contract award", "acquisition",
            "merger agreement", "price target raised", "upgraded to buy",
            "record revenue", "strategic partnership", "positive trial results",
        )
        negative_terms = (
            "offering", "dilution", "cuts guidance", "misses estimates",
            "downgraded", "investigation", "bankruptcy", "recall", "fraud",
        )
        catalysts = {}
        for item in payload.get("news") or []:
            headline = str(item.get("headline") or "")
            text_value = (headline + " " + str(item.get("summary") or "")).lower()
            if not any(term in text_value for term in positive_terms):
                continue
            if any(term in text_value for term in negative_terms):
                continue
            published = item.get("created_at") or item.get("updated_at")
            if not published:
                continue
            for symbol in item.get("symbols") or []:
                key = str(symbol).upper()
                if key not in symbols or key in catalysts:
                    continue
                catalysts[key] = {
                    "verified": True, "publishedAt": published,
                    "source": str(item.get("source") or "Alpaca News"),
                    "headline": headline,
                }
        return catalysts
    except Exception as exc:
        log.warning("Alpaca news catalyst feed unavailable: %s", type(exc).__name__)
        return {}


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
        params={"symbols": ",".join(symbols), "feed": ALPACA_FEED},
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
            "feed": ALPACA_FEED,
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
    """Score liquid US equities for speculative momentum; return watchlist and strong setups."""
    trade = snap.get("latestTrade") or {}
    quote = snap.get("latestQuote") or {}
    minute = snap.get("minuteBar") or {}
    daily = snap.get("dailyBar") or {}
    price = _num(trade.get("p"), _num(minute.get("c")))
    daily_open = _num(daily.get("o"))
    daily_volume = _num(daily.get("v"), 0) or 0
    previous_daily = snap.get("prevDailyBar") or {}
    previous_volume = _num(previous_daily.get("v"), 0) or 0
    # Keep prior-session liquid names visible as WATCH/EARLY when IEX has no
    # current-day prints; FAST ENTRY still requires fresh bars and live Bid/Ask.
    if not price or price < 1:
        return None
    if not daily_open and previous_daily.get("c"):
        daily_open = _num(previous_daily.get("c"))
    liquidity_volume = daily_volume if daily_volume > 0 else previous_volume
    if liquidity_volume < 10000:
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
    closes = [x["close"] for x in bars]
    m1 = ((closes[-1] / closes[-2]) - 1) * 100 if len(closes) >= 2 and closes[-2] else None
    m3 = ((closes[-1] / closes[-4]) - 1) * 100 if len(closes) >= 4 and closes[-4] else None
    m5 = ((closes[-1] / closes[-6]) - 1) * 100 if len(closes) >= 6 and closes[-6] else None
    trade_time = trade.get("t") or minute.get("t")
    session = "UNKNOWN"
    if trade_time:
        try:
            et = datetime.fromisoformat(str(trade_time).replace("Z", "+00:00")).astimezone(ZoneInfo("America/New_York"))
            minute_of_day = et.hour * 60 + et.minute
            if et.weekday() < 5 and 240 <= minute_of_day < 570:
                session = "PRE_MARKET"
            elif et.weekday() < 5 and 570 <= minute_of_day < 960:
                session = "CORE"
            elif et.weekday() < 5 and 960 <= minute_of_day < 1200:
                session = "AFTER_HOURS"
            else:
                session = "CLOSED"
        except Exception:
            session = "UNKNOWN"
    previous_volumes = [x["volume"] for x in bars[-6:-1]]
    average_volume = sum(previous_volumes) / len(previous_volumes) if previous_volumes else 0
    rvol = bars[-1]["volume"] / average_volume if average_volume > 0 else None
    prior_highs = [x["high"] for x in bars[-6:-1] if x["high"]]
    breakout = bool(prior_highs and price > max(prior_highs))
    breakout_level = max(prior_highs) if prior_highs else None
    recent_lows = [x["low"] for x in bars[-5:] if x.get("low") and x["low"] > 0]
    recent_swing_low = min(recent_lows) if recent_lows else None
    # A transparent reference plan: current quote as reference, recent 5-bar low as
    # invalidation, and a 2R objective. This is informational, not an order signal.
    risk_stop = recent_swing_low * 0.998 if recent_swing_low and recent_swing_low < price else None
    risk_per_share = price - risk_stop if risk_stop else None
    risk_target_2r = price + (2 * risk_per_share) if risk_per_share else None

    score = 0.0
    reasons = []
    risks = []
    if change is not None:
        if change >= 5:
            score += 2
            reasons.append("ราคาวันนี้บวกตั้งแต่ 5%")
        elif change >= 2:
            score += 1
            reasons.append("ราคาวันนี้มีแรงซื้อ")
        elif change >= 1:
            score += 0.5
            reasons.append("ราคาเริ่มฟื้นตัว")
    if m1 is not None and m1 > 0:
        score += 1
        reasons.append("โมเมนตัม 1 นาทีเป็นบวก")
    if m5 is not None:
        if m5 >= 0.5:
            score += 2
            reasons.append("โมเมนตัม 5 นาทีแข็งแรง")
        elif m5 >= 0.15:
            score += 1
            reasons.append("โมเมนตัม 5 นาทีเป็นบวก")
        elif m5 < 0:
            risks.append("แรงขายระยะ 5 นาที")
    if rvol is not None:
        if rvol >= 2:
            score += 2
            reasons.append("ปริมาณซื้อขายสูงกว่าค่าเฉลี่ย 2 เท่า")
        elif rvol >= 1.3:
            score += 1
            reasons.append("ปริมาณซื้อขายเริ่มหนาแน่น")
    else:
        risks.append("ข้อมูล Relative Volume ยังไม่พอ")
    if breakout:
        score += 2
        reasons.append("ทะลุจุดสูงสุดระยะสั้น")
    if spread is not None:
        if 0 < spread <= 0.25:
            score += 1
            reasons.append("ส่วนต่าง Bid/Ask แคบ")
        elif spread > 0.75:
            risks.append("Spread กว้าง เสี่ยงต้นทุนเข้าออกสูง")
    else:
        risks.append("ไม่มีข้อมูล Bid/Ask สำหรับประเมิน Spread")
    if daily_volume >= 100000:
        score += 1
        reasons.append("มีปริมาณซื้อขายสะสมรองรับ")
    if change is not None and change >= 12:
        risks.append("ราคาขึ้นแรง เสี่ยงไล่ราคา")
    if m1 is not None and m1 < 0:
        risks.append("โมเมนตัมล่าสุดเริ่มอ่อนตัว")

    # Keep a broader, ranked watchlist. Very wide spreads are excluded;
    # strict FAST_ENTRY remains a separate, higher bar.
    # Include EARLY setups from score 3/11 upward so the daily scanner can surface developing moves; FAST_ENTRY remains independently strict.
    if spread is not None and spread > 1.25:
        return None
    # Five parallel momentum playbooks. Each setup has its own trigger;
    # a single shared breakout rule is deliberately NOT required.
    continuation = bool(m3 is not None and m3 >= 0.30)
    prior_closes = closes[-5:-1]
    prior_lows = [x["low"] for x in bars[-5:-1] if x.get("low") and x["low"] > 0]
    pullback_reclaim = bool(
        len(prior_closes) >= 3 and len(prior_lows) >= 3
        and min(prior_closes) < max(prior_closes)
        and price > max(prior_closes[-2:])
        and m1 is not None and m1 > 0
    )
    gap_go = bool(change is not None and 3 <= change <= 30
                  and m1 is not None and m1 > 0
                  and m5 is not None and m5 >= 0.15
                  and rvol is not None and rvol >= 1.3)
    volume_explosion = bool(rvol is not None and rvol >= 2
                            and m1 is not None and m1 > 0
                            and m5 is not None and m5 > 0)
    hod_breakout = bool(breakout and m1 is not None and m1 > 0
                        and rvol is not None and rvol >= 1.3)
    pullback_reversal = bool(pullback_reclaim and rvol is not None and rvol >= 1.0)
    # Catalyst entries are only recognized when a verified news provider supplies
    # a timestamped, sourced catalyst object; price movement alone is not "news".
    catalyst = snap.get("newsCatalyst") if isinstance(snap.get("newsCatalyst"), dict) else {}
    catalyst_time = catalyst.get("publishedAt")
    catalyst_source = catalyst.get("source")
    news_catalyst = bool(catalyst.get("verified") is True and catalyst_time and catalyst_source)
    momentum_setups = []
    if gap_go: momentum_setups.append("GAP_AND_GO")
    if volume_explosion: momentum_setups.append("VOLUME_EXPLOSION")
    if hod_breakout: momentum_setups.append("HIGH_OF_DAY_BREAKOUT")
    if pullback_reversal: momentum_setups.append("PULLBACK_REVERSAL")
    if news_catalyst: momentum_setups.append("NEWS_CATALYST")
    momentum_score = min(100, (28 if gap_go else 0) + (25 if volume_explosion else 0)
                         + (25 if hod_breakout else 0) + (22 if pullback_reversal else 0)
                         + (25 if news_catalyst else 0))
    # Shared execution safety: positive short-term tape, fresh bid/ask, tight spread.
    fast_entry = bool(momentum_setups and momentum_score >= 50
                      and m1 is not None and m1 > 0
                      and m5 is not None and m5 >= -0.10
                      and spread is not None and 0 < spread <= 0.50
                      and change is not None and -5 <= change <= 100)
    fast_entry_reasons = []
    if not momentum_setups: fast_entry_reasons.append("NO_MOMENTUM_SETUP")
    if momentum_score < 50: fast_entry_reasons.append("MOMENTUM_SCORE_BELOW_50")
    if m1 is None or m1 <= 0: fast_entry_reasons.append("M1_NOT_POSITIVE")
    if m5 is None or m5 < -0.10: fast_entry_reasons.append("M5_TREND_WEAK")
    if spread is None or spread <= 0 or spread > 0.50: fast_entry_reasons.append("SPREAD_NOT_TRADABLE")
    if change is None or change < -5 or change > 100: fast_entry_reasons.append("DAILY_MOVE_OUT_OF_RANGE")
    forecast = forecast_10m_from_bars(bars, price)
    setup = "STRONG" if score >= 7 else ("WATCH" if score >= 5 else "EARLY")
    return {
        "symbol": symbol, "exchange": "Alpaca IEX/" + exchange,
        "price": price, "changePct": change, "m1": m1, "m3": m3, "m5": m5,
        "session": session, "volume": int(daily_volume),
        "relativeVolume": rvol, "spreadPct": spread,
        "barsCount": len(bars), "breakout": breakout, "fastEntry": fast_entry,
        "momentumSetups": momentum_setups, "momentumScore": momentum_score,
        "gapAndGo": gap_go, "volumeExplosion": volume_explosion,
        "highOfDayBreakout": hod_breakout, "pullbackReversal": pullback_reversal,
        "newsCatalyst": news_catalyst,
        "newsCatalystSource": catalyst_source if news_catalyst else None,
        "volumeSource": "IEX_CURRENT_SESSION" if daily_volume > 0 else "PREVIOUS_SESSION_REFERENCE",
        "currentSessionVolume": int(daily_volume),
        "fastEntryReasons": [] if fast_entry else fast_entry_reasons,
        "score": round(score, 1), "setup": setup,
        "reasons": reasons, "riskFlags": risks,
        "breakoutLevel": breakout_level,
        "recentSwingLow": recent_swing_low,
        "entryReference": price,
        "riskStop": risk_stop,
        "riskTarget2R": risk_target_2r,
        "riskRewardRatio": 2.0 if risk_stop and risk_target_2r else None,
        "forecast10m": forecast, "receivedAt": datetime.now(timezone.utc).isoformat(),
        "dataFeed": "IEX", "dailyVolumeIex": int(daily_volume),
    }


def _universe_scan_cycle():
    """Scan US equities from Alpaca IEX first; Webull is a fallback only when Alpaca is unavailable."""
    started = time.time()
    source = "Alpaca IEX"
    if ALPACA_KEY and ALPACA_SECRET:
        assets = _load_active_universe(force=False)
        exchange_by_symbol = {x["symbol"]: x.get("exchange", "") for x in assets}
        asset_by_symbol = {x["symbol"]: x for x in assets}
        symbols = [x["symbol"] for x in assets]
        snapshots = {}
        snapshot_errors = 0
        for offset in range(0, len(symbols), UNIVERSE_BATCH_SIZE):
            batch = symbols[offset:offset + UNIVERSE_BATCH_SIZE]
            try:
                snapshots.update(_alpaca_snapshots(batch))
            except Exception:
                snapshot_errors += 1
                log.exception("Alpaca IEX snapshot batch failed at offset=%d", offset)
        if not snapshots:
            raise RuntimeError("ALPACA_IEX_NO_SNAPSHOTS")
        liquid = []
        for symbol, snap in snapshots.items():
            daily = snap.get("dailyBar") or {}
            prev = snap.get("prevDailyBar") or {}
            volume = _num(daily.get("v"), 0) or 0
            price = _num((snap.get("latestTrade") or {}).get("p"),
                          _num((snap.get("minuteBar") or {}).get("c")))
            if price and price >= 1 and volume >= 10000:
                liquid.append((volume * price, symbol))
        liquid.sort(reverse=True)
        selected = [symbol for _, symbol in liquid[:UNIVERSE_LIQUIDITY_LIMIT]]
        bars_by_symbol = {}
        bar_errors = 0
        for offset in range(0, len(selected), UNIVERSE_BATCH_SIZE):
            batch = selected[offset:offset + UNIVERSE_BATCH_SIZE]
            try:
                bars_by_symbol.update(_alpaca_recent_bars(batch))
            except Exception:
                bar_errors += 1
                log.exception("Alpaca IEX minute-bar batch failed at offset=%d", offset)
        news_catalysts = _alpaca_news_catalysts(selected)
        candidates = []
        for symbol in selected:
            candidate_snapshot = dict(snapshots.get(symbol) or {})
            if symbol in news_catalysts:
                candidate_snapshot["newsCatalyst"] = news_catalysts[symbol]
            row = _universe_candidate(symbol, candidate_snapshot,
                                      bars_by_symbol.get(symbol) or [], exchange_by_symbol.get(symbol, ""))
            if row:
                row["exchange"] = exchange_by_symbol.get(symbol, "")
                row["companyName"] = (asset_by_symbol.get(symbol) or {}).get("name")
                row["dataFeed"] = "Alpaca IEX"
                row["volumeSource"] = "ALPACA_IEX_SNAPSHOT"
                candidates.append(row)
        provider = "Alpaca IEX"
        coverage = "Alpaca active US equities universe; real-time IEX venue only, not consolidated SIP"
        batch_count = ((len(symbols) + UNIVERSE_BATCH_SIZE - 1) // UNIVERSE_BATCH_SIZE) + ((len(selected) + UNIVERSE_BATCH_SIZE - 1) // UNIVERSE_BATCH_SIZE)
    else:
        source = "Webull OpenAPI fallback"
        assets = _webull_list_active_universe()
        snapshots_raw = _webull_snapshot_batch(assets)
        exchange_by_symbol = {x["symbol"]: x.get("exchange", "") for x in assets}
        asset_by_symbol = {x["symbol"]: x for x in assets}
        snapshots = {s: _normalize_webull_snapshot(row) for s, row in snapshots_raw.items()}
        liquid = []
        for symbol, snap in snapshots.items():
            daily = snap.get("dailyBar") or {}
            prev = snap.get("prevDailyBar") or {}
            volume = _num(daily.get("v"), 0) or 0
            price = _num((snap.get("latestTrade") or {}).get("p"),
                          _num(daily.get("c"), _num(prev.get("c"))))
            if price and price >= 1 and volume >= 10000:
                liquid.append((volume * price, symbol))
        liquid.sort(reverse=True)
        selected = [symbol for _, symbol in liquid[:UNIVERSE_LIQUIDITY_LIMIT]]
        bars_by_symbol = _webull_bar_batches(selected)
        candidates = []
        for symbol in selected:
            row = _universe_candidate(symbol, snapshots.get(symbol) or {},
                                      bars_by_symbol.get(symbol) or [], exchange_by_symbol.get(symbol, ""))
            if row:
                row["exchange"] = exchange_by_symbol.get(symbol, "")
                row["companyName"] = (asset_by_symbol.get(symbol) or {}).get("name")
                row["dataFeed"] = source
                row["volumeSource"] = "WEBULL_SNAPSHOT"
                candidates.append(row)
        provider = source
        coverage = "Webull US_STOCK + US_ETF catalogue; quote coverage depends on Webull API entitlement"
        snapshot_errors = 0
        bar_errors = 0
        batch_count = ((len(assets) + 99) // 100) + ((len(selected) + 99) // 100)

    candidates.sort(key=lambda x: (
        (x.get("score") or 0) * 2 + (x.get("relativeVolume") or 0)
        + (x.get("m5") or 0) + (x.get("m1") or 0)
        + min((x.get("dailyVolumeIex") or x.get("volume") or 0) / 1000000, 5)
    ), reverse=True)
    rejection_counts = {}
    for candidate in candidates:
        for reason in (candidate.get("fastEntryReasons") or []):
            rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
    now = datetime.now(timezone.utc).isoformat()
    elapsed = round(time.time() - started, 2)
    with UNIVERSE_LOCK:
        UNIVERSE_STATE.update({
            "status": "ok" if snapshots else "no_quotes",
            "provider": provider,
            "universeCount": len(assets), "snapshotCount": len(snapshots),
            "scanned": len(snapshots), "evaluated": len(selected),
            "candidateCount": len(candidates),
            "fastEntryCount": sum(1 for x in candidates if x.get("fastEntry")),
            "fastEntryWatchCount": sum(1 for x in candidates if not x.get("fastEntry") and x.get("score", 0) >= 3),
            "fastEntryRejectionCounts": rejection_counts,
            "candidates": candidates[:UNIVERSE_CANDIDATE_LIMIT],
            "updatedAt": now, "lastError": None, "cycleSeconds": elapsed,
            "batchCount": batch_count, "snapshotBatchErrors": snapshot_errors,
            "barBatchErrors": bar_errors,
            "symbolsWithMinuteBars": len(bars_by_symbol),
            "symbolsWithLiveBidAsk": sum(1 for x in snapshots.values()
                if _num((x.get("latestQuote") or {}).get("bp")) and _num((x.get("latestQuote") or {}).get("ap"))),
            "feed": provider, "coverage": coverage,
            "extendedHoursAvailable": False if provider == "Alpaca IEX" else "depends_on_Webull_entitlement",
        })
    log.info("%s universe scan complete: instruments=%d snapshots=%d evaluated=%d candidates=%d snapshot_errors=%d bar_errors=%d duration=%.1fs",
             provider, len(assets), len(snapshots), len(selected), len(candidates), snapshot_errors, bar_errors, elapsed)


def _universe_scanner_loop():
    while True:
        cycle_started = time.time()
        try:
            if ALPACA_KEY and ALPACA_SECRET:
                _universe_scan_cycle()
            else:
                with _WEBULL_SHARED_API_CLIENT_LOCK:
                    webull_ready = _WEBULL_SHARED_API_CLIENT is not None
                if webull_ready:
                    _universe_scan_cycle()
                else:
                    with UNIVERSE_LOCK:
                        UNIVERSE_STATE.update({"status": "waiting_credentials",
                            "provider": "Alpaca IEX / Webull fallback",
                            "lastError": "NO_MARKET_DATA_PROVIDER_READY"})
        except Exception as exc:
            log.exception("US equity universe scan failed")
            with UNIVERSE_LOCK:
                UNIVERSE_STATE.update({"status": "source_error",
                    "provider": "Alpaca IEX / Webull fallback",
                    "lastError": type(exc).__name__,
                    "updatedAt": datetime.now(timezone.utc).isoformat()})
        cycle_elapsed = max(0.0, time.time() - cycle_started)
        time.sleep(max(15, UNIVERSE_SCAN_INTERVAL - cycle_elapsed))


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
        "ราคาหุ้นและปริมาณซื้อขายจาก Alpaca IEX ครอบคลุมเฉพาะตลาด IEX ไม่ใช่ consolidated SIP ทุกตลาด",
        "ข้อมูลนอกเวลาตลาดและหุ้นที่ไม่มีการซื้อขายบน IEX อาจไม่มีราคาใหม่",
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

def _okx_public_tickers():
    """Display-only public OKX spot snapshot used when Binance/BitMart are unavailable."""
    response = requests.get(
        "https://www.okx.com/api/v5/market/tickers",
        params={"instType": "SPOT"},
        timeout=(5, 15),
        headers={"User-Agent": "BlessingTrade-MarketScanner/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or payload.get("code") != "0":
        raise RuntimeError("OKX_PUBLIC_API_ERROR")
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise RuntimeError("OKX_INVALID_TICKER_RESPONSE")
    normalized = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        inst_id = str(row.get("instId") or "").upper()
        if not inst_id.endswith("-USDT"):
            continue
        pair = inst_id.replace("-", "")
        normalized.append([
            pair, row.get("last") or 0, 0, row.get("volCcy24h") or 0,
            row.get("open24h") or 0, row.get("high24h") or 0,
            row.get("low24h") or 0, 0, row.get("bidPx") or 0,
            row.get("bidSz") or 0, row.get("askPx") or 0,
            row.get("askSz") or 0, row.get("ts") or 0,
        ])
    if not normalized:
        raise RuntimeError("OKX_NO_USDT_SPOT_TICKERS")
    return normalized

def _binance_th_market_headers():
    headers = {"Accept": "application/json", "User-Agent": "BlessingTrade-BinanceTH/1.0"}
    # Public market-data routes do not require a key; attach the owner's key if configured.
    if BINANCE_API_KEY:
        headers["X-MBX-APIKEY"] = BINANCE_API_KEY
    return headers


BINANCE_TH_UNIVERSE_CACHE = {"at": 0.0, "symbols": []}
BINANCE_TH_UNIVERSE_LOCK = threading.Lock()


def _binance_th_all_usdt_symbols():
    """Discover current Binance TH symbols from the documented all-symbol price endpoint."""
    now = time.time()
    with BINANCE_TH_UNIVERSE_LOCK:
        cached = list(BINANCE_TH_UNIVERSE_CACHE.get("symbols") or [])
        if cached and now - float(BINANCE_TH_UNIVERSE_CACHE.get("at") or 0) < 900:
            return cached
    response = requests.get(
        BINANCE_TH_API_BASE + "/api/v1/ticker/price",
        headers=_binance_th_market_headers(), timeout=(4, 12),
    )
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and "data" in payload:
        if payload.get("code") not in (None, 0, "0"):
            raise RuntimeError("BINANCE_TH_ALL_PRICES_API_ERROR:" + str(payload.get("code")))
        payload = payload.get("data")
    if isinstance(payload, dict):
        payload = [payload]
    if not isinstance(payload, list):
        raise RuntimeError("BINANCE_TH_INVALID_ALL_PRICES_RESPONSE")
    symbols = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper()
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            if base and base.isascii() and base.isalnum():
                symbols.add(symbol)
    preferred = [
        "BTCUSDT","ETHUSDT","BNBUSDT","XRPUSDT","SOLUSDT","DOGEUSDT",
        "ADAUSDT","TRXUSDT","LINKUSDT","LTCUSDT","BCHUSDT","DOTUSDT",
        "AVAXUSDT","SHIBUSDT","UNIUSDT","NEARUSDT","SUIUSDT","APTUSDT",
        "ATOMUSDT","ETCUSDT","FILUSDT","ARBUSDT","OPUSDT","PEPEUSDT",
        "XLMUSDT","HBARUSDT","ICPUSDT","AAVEUSDT","INJUSDT","TONUSDT",
    ]
    ordered = [symbol for symbol in preferred if symbol in symbols]
    ordered.extend(sorted(symbols.difference(ordered)))
    with BINANCE_TH_UNIVERSE_LOCK:
        BINANCE_TH_UNIVERSE_CACHE.update({"at": now, "symbols": ordered})
    log.info("Binance TH dynamic Spot universe discovered %d USDT symbols", len(ordered))
    return ordered


def _binance_public_tickers():
    """Build a broad, execution-venue-only universe from Binance TH market data."""
    now = time.time()
    rows = []
    for item in list((state.get("crypto_quotes") or {}).values()):
        if not isinstance(item, dict):
            continue
        try:
            age = now - datetime.fromisoformat(str(item.get("receivedAt")).replace("Z", "+00:00")).timestamp()
        except Exception:
            age = 999999
        if age < 0 or age > 900:
            continue
        pair = str(item.get("symbol") or "").upper()
        if pair.endswith("_USDT"):
            pair = pair[:-5] + "USDT"
        if not pair.endswith("USDT"):
            continue
        base_asset = pair[:-4]
        if not base_asset or not base_asset.isascii() or not base_asset.isalnum():
            continue
        price = _num(item.get("price"), 0) or 0
        change_pct = _num(item.get("change24hPct"), 0) or 0
        open_price = _num(item.get("openPrice"), 0) or (
            price / (1 + change_pct / 100) if price > 0 and change_pct > -99.9 else 0
        )
        try:
            book_age = now - datetime.fromisoformat(
                str(item.get("bookReceivedAt")).replace("Z", "+00:00")
            ).timestamp()
        except Exception:
            book_age = 999999
        bid = _num(item.get("bid"), 0) or 0 if 0 <= book_age <= 120 else 0
        ask = _num(item.get("ask"), 0) or 0 if 0 <= book_age <= 120 else 0
        if price <= 0 or open_price <= 0:
            continue
        rows.append({
            "symbol": pair, "lastPrice": price,
            "quoteVolume": _num(item.get("quoteVolume24h"), 0) or 0,
            "openPrice": open_price, "bidPrice": bid, "askPrice": ask,
            "closeTime": int(_num(item.get("eventTime"), 0) or 0),
            "tickerAgeSeconds": round(age, 1), "bookAgeSeconds": round(book_age, 1),
        })

    # The all-market WebSocket is changed-only, not a complete market snapshot.
    # Discover all current USDT pairs via Binance TH's public all-symbol price
    # endpoint, then enrich with 24h volume and book data. REST enrichment is
    # cached in memory for 15 minutes; candles and entry prices are refreshed.
    if len(rows) < 250:
        try:
            discovered_symbols = _binance_th_all_usdt_symbols()
        except Exception as exc:
            discovered_symbols = []
            log.warning("Binance TH dynamic universe discovery failed: %s", type(exc).__name__)
        existing_symbols = {str(x.get("symbol") or "").upper() for x in rows}
        candidate_symbols = [x for x in discovered_symbols if x not in existing_symbols][:300]

        def fetch_symbol_24h(pair):
            try:
                response = requests.get(
                    BINANCE_TH_API_BASE + "/api/v1/ticker/24hr",
                    params={"symbol": pair},
                    headers=_binance_th_market_headers(), timeout=(3, 8),
                )
                response.raise_for_status()
                item = response.json()
            except Exception:
                return None
            if isinstance(item, dict) and "data" in item:
                if item.get("code") not in (None, 0, "0"):
                    return None
                item = item.get("data")
            if isinstance(item, list):
                item = item[0] if item else None
            if not isinstance(item, dict):
                return None
            symbol = str(item.get("symbol") or pair).upper()
            price = _num(item.get("lastPrice") or item.get("price"), 0) or 0
            open_price = _num(item.get("openPrice"), 0) or 0
            quote_volume = _num(item.get("quoteVolume"), 0) or 0
            bid = _num(item.get("bidPrice"), 0) or 0
            ask = _num(item.get("askPrice"), 0) or 0
            if (symbol != pair or price <= 0 or open_price <= 0
                    or quote_volume < 250000 or bid <= 0 or ask < bid):
                return None
            received_at = datetime.now(timezone.utc).isoformat()
            return {
                "symbol": symbol, "lastPrice": price, "quoteVolume": quote_volume,
                "openPrice": open_price, "bidPrice": bid, "askPrice": ask,
                "closeTime": int(_num(item.get("closeTime"), 0) or 0),
                "receivedAt": received_at,
            }

        recovered = 0
        checked = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            for item in pool.map(fetch_symbol_24h, candidate_symbols):
                checked += 1
                if not item:
                    continue
                symbol = str(item["symbol"]).upper()
                if symbol not in existing_symbols:
                    rows.append(item)
                    existing_symbols.add(symbol)
                    recovered += 1
                received_at = item["receivedAt"]
                state.setdefault("crypto_quotes", {})[symbol] = {
                    "symbol": symbol, "price": item["lastPrice"],
                    "change24hPct": ((item["lastPrice"] / item["openPrice"]) - 1) * 100,
                    "openPrice": item["openPrice"], "quoteVolume24h": item["quoteVolume"],
                    "bid": item["bidPrice"], "ask": item["askPrice"],
                    "receivedAt": received_at, "bookReceivedAt": received_at,
                    "source": "Binance TH official REST ticker",
                }
        log.info(
            "Binance TH universe enrichment discovered=%s checked=%s added=%s cached=%s",
            len(discovered_symbols), checked, recovered, len(rows)
        )
    if not rows:
        quote_cache = state.get("crypto_quotes") or {}
        sample_symbols = list(quote_cache.keys())[:8]
        raise RuntimeError(
            "BINANCE_TH_LIVE_TICKERS_NOT_READY:"
            + "connection=" + str(state.get("cryptoConnection"))
            + ",quotes=" + str(len(quote_cache))
            + ",sample=" + ",".join(str(x) for x in sample_symbols)
            + ",lastMessage=" + str(state.get("lastCryptoMessageAt"))
        )
    return rows


def _binance_1m_klines(symbol: str):
    """Read closed 1-minute candles from Binance TH official Spot REST API."""
    response = requests.get(
        BINANCE_TH_API_BASE + "/api/v1/klines",
        params={"symbol": symbol, "interval": "1m", "limit": 65},
        timeout=(3, 8), headers=_binance_th_market_headers(),
    )
    try:
        response.raise_for_status()
    except requests.HTTPError:
        log.warning("Binance TH klines HTTP error symbol=%s status=%s", symbol, response.status_code)
        raise
    payload = response.json()
    # Binance TH wraps successful REST payloads in {code: 0, data: ...}.
    # Some endpoints/edge responses may still return the raw array.
    if isinstance(payload, dict):
        code = payload.get("code")
        if code not in (None, 0, "0"):
            raise RuntimeError("BINANCE_TH_KLINE_API_ERROR:" + str(code))
        rows = payload.get("data")
    else:
        rows = payload
    if not isinstance(rows, list):
        log.warning("Binance TH klines invalid payload symbol=%s type=%s keys=%s",
                    symbol, type(payload).__name__,
                    sorted(payload.keys()) if isinstance(payload, dict) else None)
        raise RuntimeError("BINANCE_TH_INVALID_KLINE_RESPONSE")
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
                "openTime": int(row[0]), "open": float(row[1]), "high": float(row[2]),
                "low": float(row[3]), "close": float(row[4]), "volume": float(row[5]),
                "closeTime": close_time, "quoteVolume": float(row[7]),
            })
        except (TypeError, ValueError, IndexError):
            continue
    if not bars:
        raise RuntimeError("BINANCE_TH_EMPTY_CANDLE_RESPONSE")
    return bars



def _moderate_crypto_entry_metrics(row: dict):
    """Regime-adaptive Crypto Paper selector; does not authorize LIVE orders."""
    try:
        price = Decimal(str(row.get("price") or row.get("lastPrice") or "0"))
        stop = Decimal(str(row.get("atrStopPct") or "0"))
        spread = Decimal(str(row.get("spreadPct") or "999"))
        m1 = Decimal(str(row.get("m1") if row.get("m1") is not None else "-999"))
        m5 = Decimal(str(row.get("m5") if row.get("m5") is not None else "-999"))
        m15 = Decimal(str(row.get("m15") if row.get("m15") is not None else "-999"))
        rvol = Decimal(str(row.get("rvol") or "0"))
        ema20 = Decimal(str(row.get("ema20") or "0"))
        ema50 = Decimal(str(row.get("ema50") or "0"))
        rsi = Decimal(str(row.get("rsi14") if row.get("rsi14") is not None else "50"))
        range_width = Decimal(str(row.get("rangeWidthPct") or "0"))
        candle_age = Decimal(str(row.get("candleAgeSeconds") if row.get("candleAgeSeconds") is not None else "999999"))
    except Exception:
        return None
    symbol = str(row.get("symbol") or "").upper()
    if price <= 0 or not symbol.endswith("USDT") or str(row.get("exchangeSymbol") or "").upper() != symbol:
        return None
    # A cached 24h ticker may remain in the universe, but entry requires a recent closed candle.
    if candle_age > Decimal("120"):
        return None
    if not (Decimal("0") < spread <= Decimal("0.35")) or not (Decimal("0.8") <= stop <= Decimal("2.0")):
        return None
    btc_bias = str(row.get("btcMarketBias") or "neutral").lower()
    long_trend = row.get("trendAligned") is True
    long_momentum = m5 > Decimal("0.03") and m15 > Decimal("0.08") and m1 > Decimal("-0.25")
    long_confirm = row.get("pullbackReclaim") is True or row.get("breakout") is True or (ema20 > 0 and price > ema20 and m1 > 0)
    direction = None
    setup = None
    if long_trend and long_momentum and long_confirm and rvol >= Decimal("1.0"):
        direction, setup = "long", "PULLBACK" if row.get("pullbackReclaim") else "BREAKOUT" if row.get("breakout") else "TREND"
    ema_gap = abs(ema20 - ema50) / price * Decimal("100") if price > 0 else Decimal("999")
    range_regime = ema20 > 0 and ema50 > 0 and ema_gap <= Decimal("0.25") and abs(m5) <= Decimal("0.35") and abs(m15) <= Decimal("0.60") and Decimal("0.5") <= range_width <= Decimal("3.5")
    if direction is None and range_regime and rvol >= Decimal("1.1"):
        if row.get("rangeLongReversal") is True and rsi <= Decimal("32"):
            direction, setup = "long", "RANGE_SUPPORT_REVERSAL"
    if direction == "long" and btc_bias == "bearish": return None
    if direction and btc_bias == "neutral" and rvol < Decimal("1.2"): return None
    if not direction: return None
    expected_move = abs(m5) + abs(m15)
    estimated_cost = Decimal("0.30") + spread
    if expected_move < estimated_cost * Decimal("1.15"): return None
    trend_points = 25 if long_trend else 15
    momentum_points = 20 if long_momentum else 10
    volume_points = min(15, int(rvol * Decimal("6")))
    spread_points = 10 if spread <= Decimal("0.20") else 6
    risk_points = 10 if stop <= Decimal("1.5") else 6
    score = min(100, trend_points + momentum_points + 20 + volume_points + spread_points + risk_points)
    minimum_score = 70 if setup and setup.startswith("RANGE_") else 75
    if score < minimum_score: return None
    return {"decision":"PAPER_CANDIDATE", "side":direction, "opportunityScore":score,
            "liveEntryScore":0, "liveEntryModel":"PAPER_ONLY_REGIME_ADAPTIVE", "strategy":setup,
            "netEdgeProxyPct":float(expected_move-estimated_cost), "btcMarketBias":btc_bias,
            "estimatedRoundTripCostPct":float(estimated_cost)}

def _crypto_three_strategy_decision(row: dict):
    """Long-only member Crypto Paper selector; short selling is intentionally disabled."""
    decision = _moderate_crypto_entry_metrics(row)
    if not decision:
        return None
    label = {
        "PULLBACK": "Trend Pullback",
        "BREAKOUT": "Momentum Breakout",
        "TREND": "Trend Momentum",
        "RANGE_SUPPORT_REVERSAL": "Range Support Reversal",
    }.get(decision.get("strategy"), "Adaptive Momentum")
    return {
        **decision,
        "strategyLabel": label,
        "entryAllowed": True,
        "paperOnly": True,
        "liveEntryAllowed": False,
        "atrStopPct": row.get("atrStopPct"),
        "note": "Long-only Paper signal; confirmed candle trend, volume, spread and ATR filters required.",
    }


def _altcoin_metrics(row: dict, bars: list[dict]) -> dict:
    """Calculate legacy momentum and Balanced EMA-pullback metrics from closed 1m candles."""
    closes = [float(b["close"]) for b in bars]
    # Use the latest closed Binance TH candle as the decision/fill reference,
    # not a quiet-market ticker snapshot that may be several minutes old.
    price = closes[-1] if closes else float(row.get("price") or row.get("lastPrice") or 0)
    latest_candle_age = max(0.0, (time.time() * 1000 - float(bars[-1].get("closeTime") or 0)) / 1000) if bars else 999999
    latest = bars[-1] if bars else None
    m1 = ((closes[-1] / closes[-2]) - 1) * 100 if len(closes) >= 2 and closes[-2] else None
    m5 = ((closes[-1] / closes[-6]) - 1) * 100 if len(closes) >= 6 and closes[-6] else None
    m15 = ((closes[-1] / closes[-16]) - 1) * 100 if len(closes) >= 16 and closes[-16] else None
    m60 = ((closes[-1] / closes[-61]) - 1) * 100 if len(closes) >= 61 and closes[-61] else None

    previous = bars[-21:-1] if len(bars) >= 21 else []
    avg_quote_volume = sum(b["quoteVolume"] for b in previous) / len(previous) if previous else 0
    rvol = latest["quoteVolume"] / avg_quote_volume if latest and avg_quote_volume > 0 else None
    prior_highs = [b["high"] for b in bars[-21:-1] if b.get("high") is not None]
    prior_lows = [b["low"] for b in bars[-21:-1] if b.get("low") is not None]
    breakout = bool(latest and len(prior_highs) >= 10 and latest["close"] > max(prior_highs))
    breakdown = bool(latest and len(prior_lows) >= 10 and latest["close"] < min(prior_lows))

    def ema(values, period):
        if len(values) < period:
            return None
        alpha = 2.0 / (period + 1.0)
        value = sum(values[:period]) / period
        for item in values[period:]:
            value = item * alpha + value * (1.0 - alpha)
        return value

    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)
    ema20_history = [ema(closes[:i + 1], 20) if i >= 19 else None for i in range(len(closes))]
    pullback_touch = any(
        ema20_history[i] is not None
        and float(bars[i].get("low") or 0) > 0
        and float(bars[i]["low"]) <= ema20_history[i] * 1.003
        for i in range(max(19, len(bars) - 4), max(19, len(bars) - 1))
    )
    trend_aligned = bool(ema20 is not None and ema50 is not None and ema20 > ema50)
    trend_down_aligned = bool(ema20 is not None and ema50 is not None and ema20 < ema50)
    pullback_reclaim = bool(
        latest and ema20 is not None and pullback_touch
        and float(latest["close"]) > ema20
    )
    pullback_reject_touch = any(
        ema20_history[i] is not None
        and float(bars[i].get("high") or 0) > 0
        and float(bars[i]["high"]) >= ema20_history[i] * 0.997
        for i in range(max(19, len(bars) - 4), max(19, len(bars) - 1))
    )
    pullback_reject = bool(
        latest and ema20 is not None and pullback_reject_touch
        and float(latest["close"]) < ema20
    )

    true_ranges = []
    for i in range(1, len(bars)):
        high = float(bars[i].get("high") or 0)
        low = float(bars[i].get("low") or 0)
        prev_close = float(bars[i - 1].get("close") or 0)
        if high > 0 and low > 0 and prev_close > 0:
            true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    atr14 = sum(true_ranges[-14:]) / 14 if len(true_ranges) >= 14 else None
    atr_pct = (atr14 / closes[-1] * 100) if atr14 is not None and closes and closes[-1] > 0 else None
    # Bound the 1m ATR stop distance to avoid microscopic stops or outsized position risk.
    atr_stop_pct = min(2.0, max(0.8, atr_pct * 1.5)) if atr_pct is not None else None

    # RSI and recent range context support low-trend sideways markets.
    rsi14 = 50.0
    if len(closes) >= 15:
        changes = [closes[i] - closes[i - 1] for i in range(len(closes) - 14, len(closes))]
        gains = sum(max(0.0, value) for value in changes) / 14
        losses = sum(max(0.0, -value) for value in changes) / 14
        rsi14 = 100.0 if losses == 0 and gains > 0 else (50.0 if losses == 0 else 100.0 - 100.0 / (1.0 + gains / losses))
    range_bars = bars[-21:-1] if len(bars) >= 21 else []
    range_high = max((float(b.get("high") or 0) for b in range_bars), default=0)
    range_low = min((float(b.get("low") or 0) for b in range_bars if float(b.get("low") or 0) > 0), default=0)
    range_width_pct = ((range_high - range_low) / price * 100) if price > 0 and range_high > range_low > 0 else 0
    latest_open = float(latest.get("open") or 0) if latest else 0
    latest_close = float(latest.get("close") or 0) if latest else 0
    previous_close = closes[-2] if len(closes) >= 2 else 0
    range_long_reversal = bool(range_low > 0 and latest_close > latest_open and latest_close > previous_close and latest_close <= range_low * 1.01)
    range_short_reversal = bool(range_high > 0 and latest_close < latest_open and latest_close < previous_close and latest_close >= range_high * 0.99)

    spread = None

    bid, ask = float(row.get("bidPrice") or row.get("bid") or 0), float(row.get("askPrice") or row.get("ask") or 0)
    if price > 0 and bid > 0 and ask >= bid:
        spread = ((ask - bid) / price) * 100

    momentum_ok = bool(m1 is not None and m1 > 0 and m5 is not None and m5 > 0
                       and m15 is not None and m15 > 0)
    volume_ok = bool(rvol is not None and rvol >= 1.3)
    spread_ok = bool(spread is not None and 0 < spread <= 0.5)
    balanced_score = (
        (25 if trend_aligned else 0)
        + (25 if pullback_reclaim else 0)
        + (20 if momentum_ok else 0)
        + (15 if volume_ok else 0)
        + (15 if spread_ok else 0)
    )
    balanced_entry = bool(
        trend_aligned and pullback_reclaim and momentum_ok
        and volume_ok and spread_ok and atr_stop_pct is not None
    )

    # Separate, moderately wider opportunity signal for discovery/Paper evaluation.
    # It deliberately does NOT replace balancedEntry, which remains the only
    # signal accepted by the existing owner live-order engine.
    close_above_ema20 = bool(latest and ema20 is not None and float(latest["close"]) > ema20)
    opportunity_trend_ok = bool(trend_aligned or close_above_ema20)
    opportunity_momentum_ok = bool(
        m5 is not None and m5 > 0 and m15 is not None and m15 > 0
        and m1 is not None and m1 > -0.15
    )
    opportunity_volume_ok = bool(rvol is not None and rvol >= 1.0)
    opportunity_spread_ok = bool(spread is not None and 0 < spread <= 0.35)
    opportunity_confirmation_ok = bool(
        pullback_reclaim or breakout or (close_above_ema20 and m1 is not None and m1 > 0)
    )
    opportunity_atr_ok = bool(atr_stop_pct is not None and 0.8 <= atr_stop_pct <= 2.0)
    opportunity_score = (
        (20 if opportunity_trend_ok else 0)
        + (25 if opportunity_momentum_ok else 0)
        + (20 if opportunity_confirmation_ok else 0)
        + (15 if opportunity_volume_ok else 0)
        + (10 if opportunity_spread_ok else 0)
        + (10 if opportunity_atr_ok else 0)
    )
    opportunity_entry = bool(
        opportunity_trend_ok and opportunity_momentum_ok
        and opportunity_volume_ok and opportunity_spread_ok
        and opportunity_confirmation_ok and opportunity_atr_ok
    )

    momentum_points = sum(1 for value in (m1, m5, m15, m60) if value is not None and value > 0)
    score = min(100, momentum_points * 15
                + (25 if rvol is not None and rvol >= 2 else 0)
                + (20 if breakout else 0)
                + (15 if spread_ok else 0))
    fast_entry = bool(
        m1 is not None and m1 > 0 and m5 is not None and m5 > 0
        and rvol is not None and rvol >= 2 and breakout and spread_ok and score >= 60
    )
    return {
        "m1": m1, "m5": m5, "m15": m15, "m60": m60,
        "rvol": rvol, "breakout": breakout,
        "volumeSpike": bool(rvol is not None and rvol >= 1.3),
        "spreadPct": spread, "score": score, "fastEntry": fast_entry,
        "ema20": ema20, "ema50": ema50, "trendAligned": trend_aligned,
        "trendDownAligned": trend_down_aligned,
        "rsi14": rsi14, "rangeHigh": range_high, "rangeLow": range_low,
        "rangeWidthPct": range_width_pct,
        "rangeLongReversal": range_long_reversal, "rangeShortReversal": range_short_reversal,
        "pullbackReclaim": pullback_reclaim, "pullbackReject": pullback_reject,
        "breakdown": breakdown, "atr14": atr14,
        "atrPct": atr_pct, "atrStopPct": atr_stop_pct,
        "balancedScore": balanced_score, "balancedEntry": balanced_entry,
        "opportunityScore": opportunity_score, "opportunityEntry": opportunity_entry,
        "opportunityTrendOk": opportunity_trend_ok,
        "opportunityMomentumOk": opportunity_momentum_ok,
        "opportunityVolumeOk": opportunity_volume_ok,
        "opportunitySpreadOk": opportunity_spread_ok,
        "opportunityConfirmationOk": opportunity_confirmation_ok,
        "opportunityAtrOk": opportunity_atr_ok,
        "bid": bid or None, "ask": ask or None,
        "candleCount": len(bars),
        "candleAgeSeconds": round(latest_candle_age, 1),
        "candleTime": latest.get("closeTime") if latest else None,
    }

def _altcoin_scanner_run(refresh: int = 0):
    now = time.time()
    with ALTCOIN_CACHE_LOCK:
        cached = ALTCOIN_CACHE.get("payload")
        if cached and now - ALTCOIN_CACHE["at"] < 30 and not refresh:
            result = dict(cached)
            result["cacheAgeSeconds"] = int(now - ALTCOIN_CACHE["at"])
            return result

    provider = "Binance TH Spot API"
    bitmart_error = None
    try:
        # Binance is the execution venue for Paper Trading; use it as the primary scanner.
        try:
            tickers = _binance_public_tickers()
        except Exception as primary_error:
            raise RuntimeError("BINANCE_TH_LIVE_TICKERS_UNAVAILABLE:" + type(primary_error).__name__ + ":" + str(primary_error)[:180])
    except Exception as binance_error:
        log.error("Binance scanner unavailable: %s", str(binance_error)[:240])
        try:
            tickers = _bitmart_public_tickers()
            provider = "BitMart"
            bitmart_error = type(binance_error).__name__
            log.warning("Using BitMart fallback; Paper entries remain disabled for this feed")
        except Exception as bm_error:
            log.warning("BitMart fallback unavailable: %s", type(bm_error).__name__)
            try:
                tickers = _okx_public_tickers()
                provider = "OKX"
                bitmart_error = type(binance_error).__name__ + "/BitMart:" + type(bm_error).__name__
                log.warning("Using OKX display-only fallback; Paper entries remain disabled without Binance candles")
            except Exception as okx_error:
                log.exception("All crypto market data providers failed")
                return {
                    "status": "source_error", "provider": "Binance/BitMart/OKX Spot",
                    "message": "ไม่สามารถเชื่อมต่อแหล่งข้อมูล Binance, BitMart หรือ OKX ได้",
                    "sourceErrors": {"binance": type(binance_error).__name__, "bitmart": type(bm_error).__name__,
                                     "okx": type(okx_error).__name__},
                    "scanned": 0, "evaluated": 0, "candidateCount": 0, "candidates": [],
                    "fastEntryCount": 0, "updatedAt": datetime.now(timezone.utc).isoformat(),
                }

    eligible = []
    reject = {"not_usdt": 0, "stable_or_leveraged": 0, "low_volume": 0, "invalid_price": 0}
    for item in tickers:
        if provider == "Binance TH Spot API":
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
    # Evaluate the 420 highest-volume eligible USDT pairs per scan; all eligible pairs are counted in scanned.
    selected = eligible[:420]
    candle_errors = 0
    candle_scan_started = time.monotonic()
    if provider == "Binance TH Spot API" and selected:
        # Candle requests are I/O-bound; wider bounded concurrency prevents the
        # 420-pair scan from blocking Paper worker heartbeats for several minutes.
        with concurrent.futures.ThreadPoolExecutor(max_workers=48) as pool:
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
        metrics_source = "Binance TH 1m closed candles; 420 highest-volume eligible USDT pairs"
    else:
        metrics_source = provider + " ticker snapshot only; Paper entries disabled without Binance candles"

    candle_scan_seconds = round(time.monotonic() - candle_scan_started, 2)
    selected.sort(key=lambda x: (
        x.get("balancedEntry", False), x.get("balancedScore", 0),
        x.get("opportunityEntry", False), x.get("opportunityScore", 0),
        x.get("fastEntry", False), x.get("score", 0),
        x.get("rvol") or 0, x["change24h"], x["quoteVolume24h"]
    ), reverse=True)
    # Balanced diagnostics mirror the exact predicates used by the live owner engine.
    for row in selected:
        if row.get("balancedEntry") is True:
            row["rejectReason"] = None
        elif row.get("candleError"):
            row["rejectReason"] = "CANDLE_DATA_UNAVAILABLE"
        elif not row.get("trendAligned"):
            row["rejectReason"] = "EMA_TREND_NOT_ALIGNED"
        elif not row.get("pullbackReclaim"):
            row["rejectReason"] = "PULLBACK_RECLAIM_NOT_CONFIRMED"
        elif any(row.get(key) is None or row.get(key, 0) <= 0 for key in ("m1", "m5", "m15")):
            row["rejectReason"] = "MULTI_TIMEFRAME_MOMENTUM_NOT_POSITIVE"
        elif (row.get("rvol") or 0) < 1.3:
            row["rejectReason"] = "RVOL_BELOW_1_3"
        elif not row.get("spreadPct") or row["spreadPct"] <= 0 or row["spreadPct"] > 0.5:
            row["rejectReason"] = "SPREAD_OUT_OF_RANGE"
        elif row.get("atrStopPct") is None:
            row["rejectReason"] = "ATR_STOP_UNAVAILABLE"
        else:
            row["rejectReason"] = "BALANCED_SCORE_NOT_QUALIFIED"

    reject_signals = {}
    paper_strategy_counts = {"TREND": 0, "BREAKOUT": 0, "PULLBACK": 0, "PULLBACK_REJECT": 0, "BREAKDOWN": 0, "DOWNTREND": 0, "RANGE_SUPPORT_REVERSAL": 0, "RANGE_RESISTANCE_REVERSAL": 0}
    for row in selected:
        reason = row.get("rejectReason") or "BALANCED_ENTRY"
        reject_signals[reason] = reject_signals.get(reason, 0) + 1
        paper_decision = _crypto_three_strategy_decision(row)
        if paper_decision:
            row["paperStrategy"] = paper_decision["strategy"]
            row["paperStrategyScore"] = paper_decision["opportunityScore"]
            paper_strategy_counts[paper_decision["strategy"]] += 1
        else:
            row["paperStrategy"] = None
            row["paperStrategyScore"] = 0

    payload = {
        "status": "ok" if selected else "empty",
        "provider": provider,
        "exchange": provider,
        "scanned": len(eligible),
        "eligibleTotal": len(eligible),
        "evaluated": len(selected),
        "candidateCount": len(selected),
        "fastEntryCount": sum(1 for x in selected if x.get("fastEntry")),
        "balancedEntryCount": sum(1 for x in selected if x.get("balancedEntry") is True),
        "opportunityEntryCount": sum(1 for x in selected if x.get("opportunityEntry") is True),
        "opportunityWatchCount": sum(1 for x in selected if not x.get("opportunityEntry") and (x.get("opportunityScore") or 0) >= 50),
        "paperStrategyCandidateCount": sum(paper_strategy_counts.values()),
        "paperStrategyCounts": paper_strategy_counts,
        "candleErrors": candle_errors,
        "candleScanSeconds": candle_scan_seconds,
        "filterCounts": reject,
        "signalDiagnostics": reject_signals,
        "fallbackFrom": bitmart_error,
        "metricsSource": metrics_source,
        "candidates": selected,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "coverage": provider + " Spot USDT pairs; all eligible pairs counted; top 420 by 24h quote volume evaluated; minimum 250,000 USDT",
        "metricsNote": (
            "BALANCED_EMA_PULLBACK_ATR remains the conservative live-entry gate. OPPORTUNITY_SCAN is a separate wider discovery/Paper signal requiring positive 5m/15m momentum, 1m momentum above -0.15%, RVOL >= 1.0, spread <= 0.35%, confirmation, and ATR stop 0.8-2.0%. Opportunity signals do not authorize live orders."
            if provider == "Binance TH Spot API" else
            "Fallback feed is display-only; no Paper entries are allowed without Binance candle data."
        ),
    }
    log.info("Altcoin scan provider=%s eligible=%s evaluated=%s balanced_entries=%s opportunity_entries=%s paper_strategy_candidates=%s paper_strategies=%s candle_errors=%s candle_scan_seconds=%s balanced_rejects=%s",
             provider, len(eligible), len(selected), payload["balancedEntryCount"],
             payload["opportunityEntryCount"], payload["paperStrategyCandidateCount"],
             paper_strategy_counts, candle_errors, candle_scan_seconds, reject_signals)
    with ALTCOIN_CACHE_LOCK:
        ALTCOIN_CACHE.update({"at": time.time(), "payload": payload})
    return payload


# A single-flight guard prevents the owner engine, member Paper engine and UI
# from launching duplicate 200+ symbol candle scans at the same time.
ALTCOIN_SCAN_LOCK = threading.Lock()


def altcoin_scanner(refresh: int = 0):
    now = time.time()
    with ALTCOIN_CACHE_LOCK:
        cached = ALTCOIN_CACHE.get("payload")
        cache_age = now - ALTCOIN_CACHE.get("at", 0)
        if cached and cache_age < 30 and not refresh:
            result = dict(cached)
            result["cacheAgeSeconds"] = int(cache_age)
            return result

    if not ALTCOIN_SCAN_LOCK.acquire(blocking=False):
        with ALTCOIN_CACHE_LOCK:
            cached = ALTCOIN_CACHE.get("payload")
            cache_age = now - ALTCOIN_CACHE.get("at", 0)
            if cached:
                result = dict(cached)
                result["cacheAgeSeconds"] = int(cache_age)
                result["scanInProgress"] = True
                return result
        return {
            "status": "scanning", "provider": "Binance TH Spot API",
            "exchange": "Binance TH Spot API", "scanned": 0, "evaluated": 0,
            "candidateCount": 0, "candidates": [], "scanInProgress": True,
            "updatedAt": None,
        }

    try:
        # Re-check after taking the lock; another caller may have completed
        # the scan between the first cache check and lock acquisition.
        with ALTCOIN_CACHE_LOCK:
            cached = ALTCOIN_CACHE.get("payload")
            cache_age = time.time() - ALTCOIN_CACHE.get("at", 0)
            if cached and cache_age < 30 and not refresh:
                result = dict(cached)
                result["cacheAgeSeconds"] = int(cache_age)
                return result
        return _altcoin_scanner_run(refresh=1)
    finally:
        ALTCOIN_SCAN_LOCK.release()


@app.get("/altcoin-scanner")
def altcoin_scanner_endpoint(refresh: int = 0):
    return altcoin_scanner(refresh=refresh)


@app.get("/health")
async def health():
    return {"ok": True, "service": "blessing-webull-bridge"}


@app.get("/status")
async def status():
    return public_status()


@app.get("/quotes")
def quotes(symbols: str = ""):
    """Single dashboard quote route; stream-first with Yahoo fallback and a stable response shape."""
    return quotes_fallback(symbols or "AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA,AMD")


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
    # Legacy service_role keys are JWTs and belong in both headers.
    # New sb_secret_* keys are opaque API keys, not JWTs: send them only
    # in apikey or Supabase rejects the Authorization bearer as Invalid JWT.
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Content-Type": "application/json",
    }
    if not SUPABASE_SERVICE_ROLE_KEY.startswith("sb_secret_"):
        headers["Authorization"] = "Bearer " + SUPABASE_SERVICE_ROLE_KEY
    if prefer:
        headers["Prefer"] = prefer
    return headers


# Reuse pooled HTTP connections for Supabase REST calls. This avoids a new TLS
# handshake for every small query, especially on the admin/member dashboards.
_SB_HTTP = requests.Session()
_SB_ADAPTER = HTTPAdapter(pool_connections=32, pool_maxsize=64, max_retries=0)
_SB_HTTP.mount("https://", _SB_ADAPTER)
_SB_HTTP.mount("http://", _SB_ADAPTER)


def _sb_rest(table, params=None, method="GET", body=None):
    if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
        raise RuntimeError("SUPABASE_WORKER_CONFIG_MISSING")
    response = _SB_HTTP.request(method, SUPABASE_URL + "/rest/v1/" + table,
                                params=params or {}, json=body, headers=_sb_headers("return=representation"),
                                timeout=(3, 10))
    response.raise_for_status()
    return response.json() if response.content else []


def _sb_rpc(name, payload):
    response = _SB_HTTP.post(SUPABASE_URL + "/rest/v1/rpc/" + name, json=payload,
                             headers=_sb_headers(), timeout=(3, 10))
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


def _binance_th_owner_auth(request: Request):
    if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
        raise HTTPException(status_code=503, detail="Render ยังไม่ได้ตั้งค่า Supabase")
    authorization = request.headers.get("authorization", "")
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="กรุณาเข้าสู่ระบบก่อน")
    token = authorization.split(" ", 1)[1].strip()
    response = requests.get(
        SUPABASE_URL + "/auth/v1/user",
        headers={"apikey": SUPABASE_SERVICE_ROLE_KEY, "Authorization": "Bearer " + token},
        timeout=(5, 12),
    )
    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="เซสชันหมดอายุ กรุณาเข้าสู่ระบบใหม่")
    user = response.json()
    profiles = _sb_rest("profiles", {
        "select": "role,status", "user_id": "eq." + str(user.get("id")), "limit": "1"
    })
    if not profiles or profiles[0].get("role") != "super_admin" or profiles[0].get("status") != "active":
        raise HTTPException(status_code=403, detail="ต้องใช้บัญชี Super Admin ที่เปิดใช้งานเท่านั้น")
    return user



@app.post("/admin/members/{member_user_id}/remove")
def admin_remove_member(member_user_id: str, request: Request):
    """Soft-remove a member: revoke login, disable assigned portfolios, preserve all history."""
    _binance_th_owner_auth(request)
    try:
        member_uuid = str(uuid.UUID(member_user_id))
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="รหัสสมาชิกไม่ถูกต้อง")

    profiles = _sb_rest("profiles", {
        "select": "user_id,email,role,status",
        "user_id": "eq." + member_uuid,
        "limit": "1",
    })
    if not profiles or profiles[0].get("role") == "super_admin":
        raise HTTPException(status_code=404, detail="ไม่พบสมาชิก หรือไม่อนุญาตให้ดำเนินการกับบัญชีเจ้าของ")
    if profiles[0].get("status") == "removed":
        return {"ok": True, "removed": True, "message": "สมาชิกถูกนำออกจากระบบแล้ว และเก็บประวัติทั้งหมดไว้"}

    # Ban sign-in at the Auth layer first. Existing profile and trade rows are retained.
    auth_response = _SB_HTTP.put(
        SUPABASE_URL + "/auth/v1/admin/users/" + member_uuid,
        json={"ban_duration": "876000h"},
        headers=_sb_headers(),
        timeout=(5, 12),
    )
    if auth_response.status_code not in (200, 201):
        log.error("Member soft-remove Auth ban failed: status=%s", auth_response.status_code)
        raise HTTPException(status_code=502, detail="ระงับการเข้าสู่ระบบไม่สำเร็จ จึงยังไม่เปลี่ยนสถานะสมาชิก")

    now = datetime.now(timezone.utc).isoformat()
    _sb_rest("profiles", {"user_id": "eq." + member_uuid}, "PATCH", {
        "status": "removed", "updated_at": now,
    })
    _sb_rest("blessing_member_portfolio_registry", {
        "assigned_user_id": "eq." + member_uuid,
    }, "PATCH", {
        "enabled": False, "status": "suspended",
        "live_trading_approved": False, "live_ready": False,
        "updated_at": now,
    })
    _sb_rest("blessing_stock_paper_member_slots", {
        "assigned_user_id": "eq." + member_uuid,
    }, "PATCH", {
        "enabled": False, "status": "suspended", "updated_at": now,
    })
    return {
        "ok": True, "removed": True, "historyPreserved": True,
        "loginRevoked": True, "portfoliosDisabled": True,
        "message": "นำสมาชิกออกจากระบบและปิดพอร์ตที่ผูกไว้แล้ว โดยเก็บประวัติการเทรดและข้อมูลเดิมทั้งหมด",
    }



# Webull Trading API bootstrap is read-only until the owner explicitly enables
# live execution in a later, separately reviewed release.
_WEBULL_TRADE_CLIENT = None
_WEBULL_TRADE_CLIENT_LOCK = threading.Lock()
_WEBULL_TRADING_ACCOUNT_CACHE = None
_WEBULL_TRADING_ACCOUNT_CACHE_AT = 0

def _webull_trade_client():
    global _WEBULL_TRADE_CLIENT
    with _WEBULL_TRADE_CLIENT_LOCK:
        if _WEBULL_TRADE_CLIENT is None:
            with _WEBULL_SHARED_API_CLIENT_LOCK:
                api = _WEBULL_SHARED_API_CLIENT
            if api is None:
                raise RuntimeError("WEBULL_SHARED_AUTH_CLIENT_NOT_READY")
            from webull.trade.trade_client import TradeClient
            _WEBULL_TRADE_CLIENT = TradeClient(api)
        return _WEBULL_TRADE_CLIENT


@app.get("/webull/trading/account")
def webull_trading_account(request: Request, refresh: bool = False):
    """Owner-only, read-only Production Trading API account handshake."""
    global _WEBULL_TRADING_ACCOUNT_CACHE, _WEBULL_TRADING_ACCOUNT_CACHE_AT
    _binance_th_owner_auth(request)
    if not (APP_KEY and APP_SECRET):
        raise HTTPException(status_code=503, detail="WEBULL_CREDENTIALS_MISSING")
    if _WEBULL_TRADING_ACCOUNT_CACHE is not None and not refresh and time.time() - _WEBULL_TRADING_ACCOUNT_CACHE_AT < 60:
        return _WEBULL_TRADING_ACCOUNT_CACHE
    try:
        response = _webull_trade_client().account_v2.get_account_list()
        status_code = getattr(response, "status_code", None)
        payload = response.json() if hasattr(response, "json") else response
        if status_code != 200:
            log.warning("Webull Trading API account-list returned HTTP %s", status_code)
            raise HTTPException(status_code=502, detail={"code":"WEBULL_TRADING_ACCOUNT_LIST_FAILED","upstreamStatus":status_code})
        rows = payload if isinstance(payload, list) else (
            payload.get("data") or payload.get("accounts") or payload.get("account_list") or []
            if isinstance(payload, dict) else []
        )
        if isinstance(rows, dict):
            rows = rows.get("items") or rows.get("accounts") or []
        accounts = []
        for item in rows if isinstance(rows, list) else []:
            if not isinstance(item, dict):
                continue
            account_id = item.get("account_id") or item.get("accountId") or item.get("id")
            if not account_id:
                continue
            accounts.append({
                "accountId": str(account_id),
                "accountType": item.get("account_type") or item.get("accountType") or item.get("type"),
                "status": item.get("status") or item.get("account_status"),
                "currency": item.get("currency") or item.get("base_currency"),
                "market": item.get("market") or "US",
            })
        if not accounts:
            log.warning("Webull Trading API account-list returned no recognized account IDs")
            raise HTTPException(status_code=502, detail={"code":"WEBULL_ACCOUNT_ID_NOT_FOUND","message":"API ตอบกลับแล้ว แต่ยังไม่พบ Account ID ในรูปแบบที่ระบบรู้จัก"})
        result = {
            "ok": True,
            "provider": "Webull OpenAPI Trading API",
            "environment": "production",
            "readOnly": True,
            "liveOrderExecutionEnabled": False,
            "accountCount": len(accounts),
            "accounts": accounts,
            "checkedAt": datetime.now(timezone.utc).isoformat(),
        }
        _WEBULL_TRADING_ACCOUNT_CACHE = result
        _WEBULL_TRADING_ACCOUNT_CACHE_AT = time.time()
        log.info("Webull Production Trading API account handshake succeeded; accounts=%d; live order execution remains disabled", len(accounts))
        return result
    except HTTPException:
        raise
    except Exception as exc:
        log.warning("Webull Trading API account handshake failed (%s)", type(exc).__name__)
        raise HTTPException(status_code=503, detail={"code":"WEBULL_TRADING_API_NOT_READY","errorType":type(exc).__name__,"message":"ตรวจสอบ Trading API ไม่สำเร็จ; ระบบยังไม่ส่งคำสั่งซื้อขายเงินจริง"})


register_member_capital_routes(app, _binance_th_owner_auth, _sb_rest, _sb_rpc)
register_member_access_routes(app, _binance_th_owner_auth, _sb_rest, _sb_rpc, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

@app.post("/admin/member-portfolios/control")
async def admin_control_member_portfolios(request: Request):
    """Owner controls portfolio availability. LIVE readiness never authorizes or submits live orders."""
    _binance_th_owner_auth(request)
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="ข้อมูลคำสั่งไม่ถูกต้อง")
    enabled = payload.get("enabled")
    portfolio_id = payload.get("portfolioId")
    member_no = payload.get("memberNo")
    portfolio_type = payload.get("portfolioType")
    requested_mode = str(payload.get("mode") or "paper").lower()
    if not isinstance(enabled, bool):
        raise HTTPException(status_code=400, detail="ต้องระบุ enabled เป็น true หรือ false")
    if requested_mode not in ("paper", "live", "all"):
        raise HTTPException(status_code=400, detail="mode ต้องเป็น paper, live หรือ all")

    query = {
        "select": "id,member_no,assigned_user_id,portfolio_type,market,mode,paper_slot_id,live_trading_approved,live_ready",
        "order": "member_no.asc,portfolio_type.asc",
        "limit": "100",
    }
    if requested_mode in ("paper", "live"):
        query["mode"] = "eq." + requested_mode
    registry = _sb_rest("blessing_member_portfolio_registry", query) or []
    expected = {"paper": 10, "live": 10, "all": 20}[requested_mode]
    if len(registry) != expected:
        raise HTTPException(status_code=503, detail=f"ทะเบียนพอร์ต {requested_mode} ต้องครบ {expected} พอร์ต")

    if portfolio_id:
        targets = [row for row in registry if str(row.get("id")) == str(portfolio_id)]
    elif member_no is not None and portfolio_type:
        targets = [row for row in registry if int(row.get("member_no")) == int(member_no)
                   and row.get("portfolio_type") == str(portfolio_type)]
    elif member_no is not None:
        targets = [row for row in registry if int(row.get("member_no")) == int(member_no)]
    else:
        targets = registry
    allowed_types = ("crypto_paper", "stock_paper") if requested_mode == "paper" else (
        ("crypto_live", "stock_live") if requested_mode == "live" else
        ("crypto_paper", "stock_paper", "crypto_live", "stock_live")
    )
    targets = [row for row in targets if row.get("portfolio_type") in allowed_types]
    if not targets:
        raise HTTPException(status_code=404, detail="ไม่พบพอร์ตที่ต้องการควบคุม")

    # LIVE uses a separate readiness flag; never set its execution-enabled flag.
    paper_targets = [row for row in targets if row.get("mode") == "paper"]
    live_targets = [row for row in targets if row.get("mode") == "live"]
    if paper_targets:
        paper_ids = [str(row["id"]) for row in paper_targets]
        changed = _sb_rest("blessing_member_portfolio_registry", {
            "id": "in.(" + ",".join(paper_ids) + ")",
        }, method="PATCH", body={"enabled": enabled})
        if len(changed) != len(paper_targets):
            raise HTTPException(status_code=502, detail="บันทึกสถานะ PAPER ไม่ครบ")
    if live_targets:
        live_ids = [str(row["id"]) for row in live_targets]
        changed_live = _sb_rest("blessing_member_portfolio_registry", {
            "id": "in.(" + ",".join(live_ids) + ")",
        }, method="PATCH", body={"live_ready": enabled})
        if len(changed_live) != len(live_targets):
            raise HTTPException(status_code=502, detail="บันทึกสถานะ LIVE READY ไม่ครบ")

    # Only PAPER availability is synchronized to the actual Paper execution slots.
    crypto_slot_ids = [str(row["paper_slot_id"]) for row in paper_targets
                       if row.get("portfolio_type") == "crypto_paper" and row.get("paper_slot_id")]
    stock_slot_nos = [str(row["member_no"]) for row in paper_targets
                      if row.get("portfolio_type") == "stock_paper"]
    if crypto_slot_ids:
        _sb_rest("blessing_paper_member_slots", {"id": "in.(" + ",".join(crypto_slot_ids) + ")"},
                 method="PATCH", body={"enabled": enabled})
    if stock_slot_nos:
        _sb_rest("blessing_stock_paper_member_slots", {"slot_no": "in.(" + ",".join(stock_slot_nos) + ")"},
                 method="PATCH", body={"enabled": enabled})
    crypto_users = list(dict.fromkeys(str(row["assigned_user_id"]) for row in paper_targets
                       if row.get("portfolio_type") == "crypto_paper" and row.get("assigned_user_id")))
    stock_users = list(dict.fromkeys(str(row["assigned_user_id"]) for row in paper_targets
                      if row.get("portfolio_type") == "stock_paper" and row.get("assigned_user_id")))
    if crypto_users:
        _sb_rest("blessing_dual_portfolios", {"user_id": "in.(" + ",".join(crypto_users) + ")",
                 "market": "eq.crypto", "mode": "eq.paper"}, method="PATCH",
                 body={"enabled": enabled, "updated_at": datetime.now(timezone.utc).isoformat()})
    if stock_users:
        _sb_rest("blessing_dual_portfolios", {"user_id": "in.(" + ",".join(stock_users) + ")",
                 "market": "eq.stocks", "mode": "eq.paper"}, method="PATCH",
                 body={"enabled": enabled, "updated_at": datetime.now(timezone.utc).isoformat()})

    updated = [{"memberNo": row.get("member_no"), "portfolioType": row.get("portfolio_type"),
                "enabled": (enabled if row.get("mode") == "paper" else bool(row.get("enabled"))),
                "liveReady": (enabled if row.get("mode") == "live" else False),
                "liveTradingApproved": bool(row.get("live_trading_approved")),
                "liveOrdersStillLocked": row.get("mode") == "live"} for row in targets]
    return {"ok": True, "updatedCount": len(updated), "enabled": enabled, "portfolios": updated,
            "liveTradingEnabled": False,
            "message": "เปลี่ยนสถานะความพร้อมของพอร์ตแล้ว โดย LIVE ยังไม่อนุมัติและไม่ส่งคำสั่งเงินจริง"}

def _member_portfolio_auth(request: Request):
    if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
        raise HTTPException(status_code=503, detail="Render ยังไม่ได้ตั้งค่า Supabase")
    authorization = request.headers.get("authorization", "")
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="กรุณาเข้าสู่ระบบก่อน")
    token = authorization.split(" ", 1)[1].strip()
    response = requests.get(
        SUPABASE_URL + "/auth/v1/user",
        headers={"apikey": SUPABASE_SERVICE_ROLE_KEY, "Authorization": "Bearer " + token},
        timeout=(5, 12),
    )
    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="เซสชันหมดอายุ กรุณาเข้าสู่ระบบใหม่")
    user = response.json()
    profiles = _sb_rest("profiles", {
        "select": "role,status", "user_id": "eq." + str(user.get("id")), "limit": "1"
    })
    if not profiles or profiles[0].get("role") != "member" or profiles[0].get("status") != "active":
        raise HTTPException(status_code=403, detail="บัญชีนี้ไม่มีสิทธิ์ควบคุมพอร์ตสมาชิก")
    return user


@app.get("/member/member-portfolios")
def member_member_portfolios(request: Request):
    """Members can view only portfolio rows explicitly assigned to their authenticated user id."""
    user = _member_portfolio_auth(request)
    assigned = _sb_rest("blessing_member_portfolio_registry", {
        "select": "member_no", "assigned_user_id": "eq." + str(user["id"]), "limit": "4",
    }) or []
    member_nos = list(dict.fromkeys(str(x["member_no"]) for x in assigned if x.get("member_no") is not None))
    rows = _sb_rest("blessing_member_portfolio_registry", {
        "select": "id,member_no,member_label,portfolio_type,market,mode,base_currency,allocated_capital,authorized_limit,status,enabled,live_trading_approved,paper_slot_id",
        "member_no": "in.(" + ",".join(member_nos) + ")",
        "order": "portfolio_type.asc", "limit": "4",
    }) if member_nos else []
    crypto_ids = list(dict.fromkeys(str(x["paper_slot_id"]) for x in rows if x.get("portfolio_type") == "crypto_paper" and x.get("paper_slot_id")))
    crypto_slots = _sb_rest("blessing_paper_member_slots", {
        "select": "id,initial_capital,cash_balance,realized_pnl,fees_paid,updated_at,assigned_user_id",
        "id": "in.(" + ",".join(crypto_ids) + ")",
        "assigned_user_id": "eq." + str(user["id"]), "limit": "4",
    }) if crypto_ids else []
    stock_slots = _sb_rest("blessing_stock_paper_member_slots", {
        "select": "id,slot_no,initial_capital,cash_balance,realized_pnl,fees_paid,updated_at",
        "slot_no": "in.(" + ",".join(member_nos) + ")",
        "assigned_user_id": "eq." + str(user["id"]), "limit": "4",
    }) if member_nos else []
    crypto_slot_map = {str(x.get("id")): x for x in crypto_slots}
    stock_slot_map = {str(x.get("slot_no")): x for x in stock_slots}
    crypto_slot_ids = [str(x["id"]) for x in crypto_slots if x.get("id")]
    stock_slot_ids = [str(x["id"]) for x in stock_slots if x.get("id")]
    crypto_positions = _sb_rest("blessing_paper_slot_positions", {
        "select": "slot_id,symbol,quantity,current_price_thb,cost_basis_thb,unrealized_pnl_thb,status,opened_at",
        "slot_id": "in.(" + ",".join(crypto_slot_ids) + ")", "status": "eq.open", "limit": "500",
    }) if crypto_slot_ids else []
    stock_positions = _sb_rest("blessing_stock_paper_slot_positions", {
        "select": "slot_id,symbol,quantity,current_price_usd,current_fx_thb_per_usd,cost_basis_thb,unrealized_pnl_thb,status,opened_at",
        "slot_id": "in.(" + ",".join(stock_slot_ids) + ")", "status": "eq.open", "limit": "500",
    }) if stock_slot_ids else []
    for row in rows:
        ptype = row.get("portfolio_type")
        slot = crypto_slot_map.get(str(row.get("paper_slot_id"))) if ptype == "crypto_paper" else stock_slot_map.get(str(row.get("member_no"))) if ptype == "stock_paper" else None
        positions = [x for x in (crypto_positions if ptype == "crypto_paper" else stock_positions) if slot and str(x.get("slot_id")) == str(slot.get("id"))] if slot else []
        row["snapshot"] = ({
            "initialCapital": slot.get("initial_capital"), "cashBalance": slot.get("cash_balance"),
            "realizedPnl": slot.get("realized_pnl"), "feesPaid": slot.get("fees_paid"),
            "updatedAt": slot.get("updated_at"), "openPositions": positions,
            "unrealizedPnl": sum(float(x.get("unrealized_pnl_thb") or 0) for x in positions),
        } if slot else {"locked": ptype in ("crypto_live", "stock_live"), "openPositions": []})
    # Live rows are visible as locked placeholders; no member Live execution is enabled.
    return {"ok": True, "portfolioCount": len(rows), "portfolios": rows, "liveTradingEnabled": False}


@app.get("/member/member-portfolios/performance")
def member_member_portfolios_performance(request: Request, period: str = "30d"):
    """Private performance series for the authenticated member's own Paper slots only."""
    user = _member_portfolio_auth(request)
    if period not in ("7d", "30d", "90d", "all"):
        raise HTTPException(status_code=400, detail="INVALID_PERIOD")
    now = datetime.now(timezone.utc)
    span_days = {"7d": 7, "30d": 30, "90d": 90}.get(period)
    cutoff = now - timedelta(days=span_days) if span_days else datetime(2000, 1, 1, tzinfo=timezone.utc)
    assigned = _sb_rest("blessing_member_portfolio_registry", {
        "select": "member_no,portfolio_type,mode,paper_slot_id",
        "assigned_user_id": "eq." + str(user["id"]), "limit": "4",
    }) or []
    paper = [x for x in assigned if x.get("mode") == "paper" and x.get("portfolio_type") in ("crypto_paper", "stock_paper")]
    crypto_ids = list(dict.fromkeys(str(x["paper_slot_id"]) for x in paper if x.get("portfolio_type") == "crypto_paper" and x.get("paper_slot_id")))
    stock_nos = list(dict.fromkeys(str(x["member_no"]) for x in paper if x.get("portfolio_type") == "stock_paper" and x.get("member_no") is not None))
    crypto_slots = _sb_rest("blessing_paper_member_slots", {
        "select": "id,initial_capital,cash_balance,realized_pnl,fees_paid,assigned_user_id",
        "id": "in.(" + ",".join(crypto_ids) + ")", "assigned_user_id": "eq." + str(user["id"]), "limit": "10",
    }) if crypto_ids else []
    stock_slots = _sb_rest("blessing_stock_paper_member_slots", {
        "select": "id,slot_no,initial_capital,cash_balance,realized_pnl,fees_paid,assigned_user_id",
        "slot_no": "in.(" + ",".join(stock_nos) + ")", "assigned_user_id": "eq." + str(user["id"]), "limit": "10",
    }) if stock_nos else []
    crypto_positions = _sb_rest("blessing_paper_slot_positions", {
        "select": "slot_id,quantity,current_price_thb,unrealized_pnl_thb,status",
        "slot_id": "in.(" + ",".join(str(x["id"]) for x in crypto_slots) + ")", "status": "eq.open", "limit": "500",
    }) if crypto_slots else []
    stock_positions = _sb_rest("blessing_stock_paper_slot_positions", {
        "select": "slot_id,quantity,current_price_usd,current_fx_thb_per_usd,unrealized_pnl_thb,status",
        "slot_id": "in.(" + ",".join(str(x["id"]) for x in stock_slots) + ")", "status": "eq.open", "limit": "500",
    }) if stock_slots else []
    reports = []
    for row in paper:
        ptype = row["portfolio_type"]
        is_stock = ptype == "stock_paper"
        slot = next((x for x in (stock_slots if is_stock else crypto_slots)
                     if str(x.get("slot_no") if is_stock else x.get("id")) == str(row.get("member_no") if is_stock else row.get("paper_slot_id"))), None)
        if not slot:
            continue
        sid = str(slot["id"])
        table = "blessing_stock_paper_slot_orders" if is_stock else "blessing_paper_slot_orders"
        order_rows = _sb_rest(table, {
            "select": "side,status,realized_pnl_thb,created_at",
            "slot_id": "eq." + sid, "created_at": "gte." + cutoff.isoformat(),
            "order": "created_at.asc", "limit": "5000",
        }) or []
        daily = {}
        for order in order_rows:
            if str(order.get("status") or "").lower() not in ("filled", "executed"):
                continue
            stamp = order.get("created_at")
            try:
                day = datetime.fromisoformat(str(stamp).replace("Z", "+00:00")).astimezone(ZoneInfo("Asia/Bangkok")).date().isoformat()
            except Exception:
                continue
            daily[day] = daily.get(day, 0.0) + float(order.get("realized_pnl_thb") or 0)
        capital = float(slot.get("initial_capital") or 0)
        realized = float(slot.get("realized_pnl") or 0)
        positions = [x for x in (stock_positions if is_stock else crypto_positions) if str(x.get("slot_id")) == sid]
        unrealized = sum(float(x.get("unrealized_pnl_thb") or 0) for x in positions)
        market_value = sum(
            float(x.get("quantity") or 0) * float(x.get("current_price_usd") or 0) * float(x.get("current_fx_thb_per_usd") or 0)
            if is_stock else
            float(x.get("quantity") or 0) * float(x.get("current_price_thb") or 0)
            for x in positions
        )
        cash = float(slot.get("cash_balance") or 0)
        equity = cash + market_value
        running = capital
        series = [{"date": cutoff.date().isoformat(), "equity": round(capital, 2), "realizedPnl": 0.0}]
        for day, pnl in sorted(daily.items()):
            running += pnl
            series.append({"date": day, "equity": round(running, 2), "realizedPnl": round(pnl, 2)})
        # Final point is the current live valuation; historical points are based on realized fills only.
        today = now.astimezone(ZoneInfo("Asia/Bangkok")).date().isoformat()
        if not series or series[-1]["date"] != today:
            series.append({"date": today, "equity": round(equity, 2), "realizedPnl": round(realized, 2), "current": True})
        else:
            series[-1]["equity"] = round(equity, 2)
            series[-1]["current"] = True
        reports.append({
            "portfolioType": ptype, "capital": capital, "cash": cash, "marketValue": round(market_value, 2),
            "equity": round(equity, 2), "realizedPnl": round(realized, 2),
            "unrealizedPnl": round(unrealized, 2), "netPnl": round(realized + unrealized, 2),
            "returnPct": ((equity - capital) / capital * 100) if capital else 0,
            "fees": float(slot.get("fees_paid") or 0), "openPositions": len(positions), "series": series,
        })
    return {"ok": True, "period": period, "timezone": "Asia/Bangkok", "generatedAt": now.isoformat(),
            "method": "Historical points use realized Paper fills; latest point uses current cash plus current marked positions.",
            "portfolios": reports}


@app.get("/member/member-portfolios/daily-trades")
def member_member_portfolios_daily_trades(request: Request, day: str = ""):
    """Return the authenticated member's own Paper order log for one Bangkok calendar day."""
    user = _member_portfolio_auth(request)
    try:
        report_day = datetime.strptime(day, "%Y-%m-%d").date() if day else datetime.now(ZoneInfo("Asia/Bangkok")).date()
    except ValueError:
        raise HTTPException(status_code=400, detail="รูปแบบวันที่ต้องเป็น YYYY-MM-DD")
    tz = ZoneInfo("Asia/Bangkok")
    start = datetime(report_day.year, report_day.month, report_day.day, tzinfo=tz)
    end = start + timedelta(days=1)
    start_utc = start.astimezone(timezone.utc).isoformat()
    end_utc = end.astimezone(timezone.utc).isoformat()

    assigned = _sb_rest("blessing_member_portfolio_registry", {
        "select": "member_no", "assigned_user_id": "eq." + str(user["id"]), "limit": "4",
    }) or []
    member_nos = list(dict.fromkeys(str(x["member_no"]) for x in assigned if x.get("member_no") is not None))
    registry = _sb_rest("blessing_member_portfolio_registry", {
        "select": "member_no,portfolio_type,mode,paper_slot_id,assigned_user_id",
        "member_no": "in.(" + ",".join(member_nos) + ")", "limit": "4",
    }) if member_nos else []
    paper_registry = [x for x in registry if x.get("mode") == "paper" and x.get("portfolio_type") in ("crypto_paper", "stock_paper")]
    registry_crypto_ids = list(dict.fromkeys(
        str(row["paper_slot_id"]) for row in paper_registry
        if row.get("portfolio_type") == "crypto_paper" and row.get("paper_slot_id")
    ))
    owned_crypto_slots = _sb_rest("blessing_paper_member_slots", {
        "select": "id,assigned_user_id", "id": "in.(" + ",".join(registry_crypto_ids) + ")",
        "assigned_user_id": "eq." + str(user["id"]), "limit": "4",
    }) if registry_crypto_ids else []
    crypto_slot_ids = [str(x["id"]) for x in owned_crypto_slots if x.get("id")]
    stock_member_nos = list(dict.fromkeys(
        str(row["member_no"]) for row in registry
        if row.get("portfolio_type") == "stock_paper" and row.get("member_no") is not None
    ))
    stock_slots = _sb_rest("blessing_stock_paper_member_slots", {
        "select": "id,slot_no,assigned_user_id",
        "slot_no": "in.(" + ",".join(stock_member_nos) + ")",
        "assigned_user_id": "eq." + str(user["id"]), "limit": "4",
    }) if stock_member_nos else []
    slots = []
    for row in paper_registry:
        ptype = row.get("portfolio_type")
        if ptype == "crypto_paper" and row.get("paper_slot_id"):
            slots.append({"slot_id": str(row["paper_slot_id"]), "portfolio_type": ptype})
        elif ptype == "stock_paper":
            slot = next((x for x in stock_slots if str(x.get("slot_no")) == str(row.get("member_no"))), None)
            if slot and slot.get("id"):
                slots.append({"slot_id": str(slot["id"]), "portfolio_type": ptype})
    reports = []
    for slot in slots:
        table = "blessing_stock_paper_slot_orders" if slot["portfolio_type"] == "stock_paper" else "blessing_paper_slot_orders"
        stock_order = slot["portfolio_type"] == "stock_paper"
        fields = (
            "slot_id,side,symbol,status,reason,quantity,price_usd,fx_thb_per_usd,notional_thb,fee_thb,realized_pnl_thb,created_at"
            if stock_order else
            "slot_id,side,symbol,status,reason,quantity,price_thb,notional_thb,fee_thb,realized_pnl_thb,created_at"
        )
        orders = _sb_rest(table, {
            "select": fields,
            "slot_id": "eq." + slot["slot_id"],
            "created_at": "gte." + start_utc,
            "and": "(created_at.lt." + end_utc + ")",
            "order": "created_at.desc", "limit": "1000",
        }) or []
        reports.append({
            "portfolioType": slot["portfolio_type"],
            "orders": [{
                "side": order.get("side"), "symbol": order.get("symbol"),
                "status": order.get("status"), "reason": order.get("reason"),
                "quantity": order.get("quantity"),
                "price": order.get("price_usd") if stock_order else order.get("price_thb"),
                "priceCurrency": "USD" if stock_order else "THB",
                "fxThbPerUsd": order.get("fx_thb_per_usd") if stock_order else None,
                "notionalThb": order.get("notional_thb"),
                "feeThb": order.get("fee_thb"),
                "realizedPnlThb": order.get("realized_pnl_thb"),
                "createdAt": order.get("created_at"),
            } for order in orders],
        })
    # Show the member's two Live portfolio tiles as locked; no member Live order execution is enabled.
    for row in registry:
        if row.get("mode") == "live" and row.get("portfolio_type") in ("crypto_live", "stock_live"):
            reports.append({"portfolioType": row.get("portfolio_type"), "orders": [], "liveTradingLocked": True})
    return {
        "ok": True, "day": report_day.isoformat(), "timezone": "Asia/Bangkok",
        "portfolios": reports, "liveOrdersIncluded": False,
    }


@app.get("/admin/member-portfolios/daily-trades")
def admin_member_portfolios_daily_trades(request: Request, day: str = ""):
    """Super Admin daily Paper order report grouped by member and portfolio (Bangkok time)."""
    _binance_th_owner_auth(request)
    try:
        report_day = datetime.strptime(day, "%Y-%m-%d").date() if day else datetime.now(ZoneInfo("Asia/Bangkok")).date()
    except ValueError:
        raise HTTPException(status_code=400, detail="รูปแบบวันที่ต้องเป็น YYYY-MM-DD")
    tz = ZoneInfo("Asia/Bangkok")
    start = datetime(report_day.year, report_day.month, report_day.day, tzinfo=tz)
    end = start + timedelta(days=1)
    start_utc = start.astimezone(timezone.utc).isoformat()
    end_utc = end.astimezone(timezone.utc).isoformat()

    registry = _sb_rest("blessing_member_portfolio_registry", {
        "select": "member_no,member_label,portfolio_type,mode,paper_slot_id,assigned_user_id",
        "portfolio_type": "in.(crypto_paper,stock_paper)",
        "mode": "eq.paper", "order": "member_no.asc", "limit": "100",
    }) or []
    stock_member_nos = sorted({str(x["member_no"]) for x in registry
                               if x.get("portfolio_type") == "stock_paper" and x.get("member_no") is not None})
    stock_slots = _sb_rest("blessing_stock_paper_member_slots", {
        "select": "id,slot_no,assigned_user_id",
        "slot_no": "in.(" + ",".join(stock_member_nos) + ")", "limit": "20",
    }) if stock_member_nos else []
    stock_by_member = {str(x.get("slot_no")): x for x in (stock_slots or [])}
    slot_rows = []
    for row in registry:
        ptype = row.get("portfolio_type")
        if ptype == "crypto_paper" and row.get("paper_slot_id"):
            slot_rows.append((row, str(row["paper_slot_id"])))
        elif ptype == "stock_paper":
            slot = stock_by_member.get(str(row.get("member_no")))
            if slot and slot.get("id"):
                slot_rows.append((row, str(slot["id"])))

    crypto_ids = list(dict.fromkeys(slot_id for row, slot_id in slot_rows if row.get("portfolio_type") == "crypto_paper"))
    stock_ids = list(dict.fromkeys(slot_id for row, slot_id in slot_rows if row.get("portfolio_type") == "stock_paper"))
    crypto_orders = _sb_rest("blessing_paper_slot_orders", {
        "select": "slot_id,side,symbol,status,reason,quantity,price_thb,notional_thb,fee_thb,realized_pnl_thb,created_at",
        "slot_id": "in.(" + ",".join(crypto_ids) + ")",
        "created_at": "gte." + start_utc, "and": "(created_at.lt." + end_utc + ")",
        "order": "created_at.desc", "limit": "5000",
    }) if crypto_ids else []
    stock_orders = _sb_rest("blessing_stock_paper_slot_orders", {
        "select": "slot_id,side,symbol,status,reason,quantity,price_usd,fx_thb_per_usd,notional_thb,fee_thb,realized_pnl_thb,created_at",
        "slot_id": "in.(" + ",".join(stock_ids) + ")",
        "created_at": "gte." + start_utc, "and": "(created_at.lt." + end_utc + ")",
        "order": "created_at.desc", "limit": "5000",
    }) if stock_ids else []
    orders_by_key = {}
    for ptype, orders in (("crypto_paper", crypto_orders or []), ("stock_paper", stock_orders or [])):
        stock_order = ptype == "stock_paper"
        for order in orders:
            orders_by_key.setdefault((ptype, str(order.get("slot_id"))), []).append({
                "side": order.get("side"), "symbol": order.get("symbol"),
                "status": order.get("status"), "reason": order.get("reason"),
                "quantity": order.get("quantity"),
                "price": order.get("price_usd") if stock_order else order.get("price_thb"),
                "priceCurrency": "USD" if stock_order else "THB",
                "fxThbPerUsd": order.get("fx_thb_per_usd") if stock_order else None,
                "notionalThb": order.get("notional_thb"), "feeThb": order.get("fee_thb"),
                "realizedPnlThb": order.get("realized_pnl_thb"), "createdAt": order.get("created_at"),
            })
    portfolios = []
    for row, slot_id in slot_rows:
        ptype = row.get("portfolio_type")
        portfolios.append({
            "memberNo": row.get("member_no"), "memberLabel": row.get("member_label"),
            "portfolioType": ptype, "orders": orders_by_key.get((ptype, slot_id), []),
        })
    return {
        "ok": True, "day": report_day.isoformat(), "timezone": "Asia/Bangkok",
        "memberCount": len({str(x.get("member_no")) for x in registry}),
        "portfolios": portfolios, "liveOrdersIncluded": False,
    }


@app.post("/member/member-portfolios/control")
async def member_control_own_portfolio(request: Request):
    """A member may toggle only a registry row assigned to their authenticated user id."""
    user = _member_portfolio_auth(request)
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="ข้อมูลคำสั่งไม่ถูกต้อง")
    portfolio_id = str(payload.get("portfolioId") or "").strip()
    enabled = payload.get("enabled")
    if not portfolio_id or not isinstance(enabled, bool):
        raise HTTPException(status_code=400, detail="ต้องระบุ portfolioId และ enabled ให้ถูกต้อง")
    rows = _sb_rest("blessing_member_portfolio_registry", {
        "select": "id,member_no,portfolio_type,mode,paper_slot_id,live_trading_approved",
        "id": "eq." + portfolio_id,
        "assigned_user_id": "eq." + str(user["id"]),
        "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="ไม่พบพอร์ตของคุณ หรือพอร์ตนี้ไม่ได้กำหนดให้บัญชีคุณ")
    row = rows[0]
    changed = _sb_rest("blessing_member_portfolio_registry", {
        "id": "eq." + portfolio_id,
        "assigned_user_id": "eq." + str(user["id"]),
    }, method="PATCH", body={"enabled": enabled})
    if not changed:
        raise HTTPException(status_code=502, detail="บันทึกสถานะพอร์ตไม่สำเร็จ")
    if row.get("mode") == "paper":
        if row.get("portfolio_type") == "crypto_paper" and row.get("paper_slot_id"):
            _sb_rest("blessing_paper_member_slots", {
                "id": "eq." + str(row["paper_slot_id"]),
            }, method="PATCH", body={"enabled": enabled})
        elif row.get("portfolio_type") == "stock_paper":
            _sb_rest("blessing_stock_paper_member_slots", {
                "slot_no": "eq." + str(row["member_no"]),
            }, method="PATCH", body={"enabled": enabled})
    return {
        "ok": True, "portfolioId": portfolio_id, "enabled": enabled,
        "liveTradingApproved": bool(row.get("live_trading_approved")),
        "liveOrdersStillLocked": row.get("mode") == "live",
        "message": "เปลี่ยนสถานะพอร์ตของคุณแล้ว แต่ไม่ได้เป็นการอนุมัติส่งคำสั่งเงินจริง",
    }


@app.get("/admin/member-portfolios")
def admin_member_portfolios(request: Request):
    """Owner-only portfolio inventory. Bulk reads keep the 40-portfolio screen fast."""
    _binance_th_owner_auth(request)
    rows = _sb_rest("blessing_member_portfolio_registry", {
        "select": "id,member_no,member_label,portfolio_type,market,mode,base_currency,assigned_user_id,paper_slot_id,allocated_capital,status,enabled,live_trading_approved,live_ready,created_at,updated_at",
        "order": "member_no.asc,portfolio_type.asc",
        "limit": "100",
    })
    if len(rows) != 20:
        raise HTTPException(status_code=503, detail={
            "code": "PORTFOLIO_REGISTRY_INCOMPLETE",
            "expected": 20, "found": len(rows),
            "message": "ทะเบียนสมาชิกต้องมีพอร์ตครบ 20 พอร์ต (Paper 10 และ Live 10) จึงจะแสดงข้อมูลได้",
        })

    # Enrich the owner-only portfolio list with the actual assigned member
    # identity so the dashboard can group all four portfolios by person.
    assigned_ids = list(dict.fromkeys(
        str(row["assigned_user_id"]) for row in rows if row.get("assigned_user_id")
    ))
    profiles_by_user = {}
    if assigned_ids:
        member_profiles = _sb_rest("profiles", {
            "select": "user_id,email,display_name,role,status",
            "user_id": "in.(" + ",".join(assigned_ids) + ")",
            "limit": "100",
        }) or []
        profiles_by_user = {str(x.get("user_id")): x for x in member_profiles}
    for row in rows:
        profile = profiles_by_user.get(str(row.get("assigned_user_id")), {})
        row["assigned_member_name"] = profile.get("display_name") or row.get("member_label")
        row["assigned_member_email"] = profile.get("email")
        row["assigned_member_status"] = profile.get("status")

    # Fetch all paper slots and open-position counts in batches instead of
    # issuing two additional REST requests for each member (40+ requests -> 5).
    stock_slots = _sb_rest("blessing_stock_paper_member_slots", {
        "select": "id,slot_no,initial_capital,cash_balance,realized_pnl,fees_paid,max_open_positions,enabled,status,assigned_user_id",
        "slot_no": "in.(" + ",".join(str(n) for n in range(1, 11)) + ")",
        "limit": "20",
    })
    stock_ids = [str(x["id"]) for x in stock_slots if x.get("id")]
    stock_positions = _sb_rest("blessing_stock_paper_slot_positions", {
        "select": "slot_id,id", "slot_id": "in.(" + ",".join(stock_ids) + ")",
        "status": "eq.open", "limit": "1000",
    }) if stock_ids else []
    stock_counts = {}
    for pos in stock_positions:
        key = str(pos.get("slot_id"))
        stock_counts[key] = stock_counts.get(key, 0) + 1
    stock_by_member = {}
    for slot in stock_slots:
        slot["open_positions_count"] = stock_counts.get(str(slot.get("id")), 0)
        stock_by_member[str(slot.get("slot_no"))] = slot

    crypto_ids = list(dict.fromkeys(str(x["paper_slot_id"]) for x in rows if x.get("paper_slot_id")))
    crypto_slots = _sb_rest("blessing_paper_member_slots", {
        "select": "id,initial_capital,cash_balance,realized_pnl,fees_paid,max_open_positions,enabled,status,assigned_user_id",
        "id": "in.(" + ",".join(crypto_ids) + ")",
        "limit": "20",
    }) if crypto_ids else []
    crypto_slot_ids = [str(x["id"]) for x in crypto_slots if x.get("id")]
    crypto_positions = _sb_rest("blessing_paper_slot_positions", {
        "select": "slot_id,id", "slot_id": "in.(" + ",".join(crypto_slot_ids) + ")",
        "status": "eq.open", "limit": "1000",
    }) if crypto_slot_ids else []
    crypto_counts = {}
    for pos in crypto_positions:
        key = str(pos.get("slot_id"))
        crypto_counts[key] = crypto_counts.get(key, 0) + 1
    crypto_by_id = {}
    for slot in crypto_slots:
        slot["open_positions_count"] = crypto_counts.get(str(slot.get("id")), 0)
        crypto_by_id[str(slot.get("id"))] = slot

    for row in rows:
        row["paper"] = crypto_by_id.get(str(row.get("paper_slot_id"))) if row.get("paper_slot_id") else None
        row["stock_paper"] = stock_by_member.get(str(row.get("member_no"))) if row.get("portfolio_type") == "stock_paper" else None

    return {
        "ok": True,
        "memberSlots": 10,
        "portfoliosPerMember": 2,
        "portfolioCount": len(rows),
        "liveTradingEnabled": False,
        "portfolios": rows,
    }


@app.get("/admin/member-portfolios/report")
def admin_member_portfolios_report(request: Request):
    """Owner-only consolidated read-only report for all 40 isolated member portfolios."""
    _binance_th_owner_auth(request)
    registry = _sb_rest("blessing_member_portfolio_registry", {
        "select": "id,member_no,member_label,portfolio_type,market,mode,base_currency,assigned_user_id,paper_slot_id,allocated_capital,status,enabled,live_trading_approved,live_ready,updated_at",
        "order": "member_no.asc,portfolio_type.asc",
        "limit": "100",
    }) or []
    if len(registry) != 20:
        raise HTTPException(status_code=503, detail={
            "code": "PORTFOLIO_REGISTRY_INCOMPLETE",
            "expected": 20, "found": len(registry),
            "message": "ทะเบียนสมาชิกต้องมีพอร์ตครบ 20 พอร์ต (Paper 10 และ Live 10)",
        })

    def num(value):
        try:
            result = float(value or 0)
            return result if math.isfinite(result) else 0.0
        except (TypeError, ValueError):
            return 0.0

    stock_slots = _sb_rest("blessing_stock_paper_member_slots", {
        "select": "id,slot_no,initial_capital,cash_balance,realized_pnl,fees_paid,enabled,status,updated_at",
        "slot_no": "in.(1,2,3,4,5,6,7,8,9,10)", "limit": "20",
    }) or []
    crypto_slot_ids = list(dict.fromkeys(
        str(row["paper_slot_id"]) for row in registry
        if row.get("portfolio_type") == "crypto_paper" and row.get("paper_slot_id")
    ))
    crypto_slots = _sb_rest("blessing_paper_member_slots", {
        "select": "id,slot_no,initial_capital,cash_balance,realized_pnl,fees_paid,enabled,status,updated_at",
        "id": "in.(" + ",".join(crypto_slot_ids) + ")", "limit": "20",
    }) if crypto_slot_ids else []

    stock_ids = [str(row["id"]) for row in stock_slots if row.get("id")]
    crypto_ids = [str(row["id"]) for row in crypto_slots if row.get("id")]
    stock_positions = _sb_rest("blessing_stock_paper_slot_positions", {
        "select": "id,slot_id,symbol,quantity,average_entry_price_usd,current_price_usd,entry_fx_thb_per_usd,current_fx_thb_per_usd,cost_basis_thb,unrealized_pnl_thb,status,opened_at",
        "slot_id": "in.(" + ",".join(stock_ids) + ")", "status": "eq.open", "limit": "1000",
    }) if stock_ids else []
    crypto_positions = _sb_rest("blessing_paper_slot_positions", {
        "select": "id,slot_id,symbol,quantity,average_entry_price_thb,current_price_thb,cost_basis_thb,unrealized_pnl_thb,status,opened_at",
        "slot_id": "in.(" + ",".join(crypto_ids) + ")", "status": "eq.open", "limit": "1000",
    }) if crypto_ids else []

    stock_orders = _sb_rest("blessing_stock_paper_slot_orders", {
        "select": "slot_id,symbol,side,status,fee_thb,realized_pnl_thb,created_at",
        "slot_id": "in.(" + ",".join(stock_ids) + ")", "limit": "5000",
        "order": "created_at.desc",
    }) if stock_ids else []
    crypto_orders = _sb_rest("blessing_paper_slot_orders", {
        "select": "slot_id,symbol,side,status,fee_thb,created_at",
        "slot_id": "in.(" + ",".join(crypto_ids) + ")", "limit": "5000",
        "order": "created_at.desc",
    }) if crypto_ids else []

    stock_by_member = {str(row.get("slot_no")): row for row in stock_slots}
    crypto_by_id = {str(row.get("id")): row for row in crypto_slots}
    positions_by_slot = {}
    for pos in (stock_positions or []) + (crypto_positions or []):
        positions_by_slot.setdefault(str(pos.get("slot_id")), []).append(pos)
    orders_by_slot = {}
    for order in (stock_orders or []) + (crypto_orders or []):
        orders_by_slot.setdefault(str(order.get("slot_id")), []).append(order)

    reports = []
    totals = {
        "paperPortfolios": 0, "paperCapital": 0.0, "paperCash": 0.0,
        "paperMarketValue": 0.0, "paperEquity": 0.0, "paperRealizedPnl": 0.0,
        "paperUnrealizedPnl": 0.0, "paperFees": 0.0, "paperNetPnl": 0.0,
        "openPositions": 0, "filledOrders": 0, "openOrders": 0, "liveLocked": 0, "paperActive": 0, "paperOff": 0, "liveStandby": 0, "liveOff": 0,
    }
    for row in registry:
        ptype = row.get("portfolio_type")
        if row.get("mode") == "paper":
            totals["paperActive" if row.get("enabled") else "paperOff"] += 1
        elif row.get("mode") == "live":
            totals["liveStandby" if row.get("live_ready") else "liveOff"] += 1
        is_paper = row.get("mode") == "paper" and ptype in ("crypto_paper", "stock_paper")
        slot = None
        positions = []
        orders = []
        if ptype == "crypto_paper":
            slot = crypto_by_id.get(str(row.get("paper_slot_id"))) if row.get("paper_slot_id") else None
        elif ptype == "stock_paper":
            slot = stock_by_member.get(str(row.get("member_no")))
        if is_paper and slot:
            sid = str(slot.get("id"))
            positions = positions_by_slot.get(sid, [])
            orders = orders_by_slot.get(sid, [])
            cash = num(slot.get("cash_balance"))
            capital = num(slot.get("initial_capital") or row.get("allocated_capital"))
            realized = num(slot.get("realized_pnl"))
            fees = num(slot.get("fees_paid"))
            unrealized = sum(num(p.get("unrealized_pnl_thb")) for p in positions)
            market_value = 0.0
            for p in positions:
                qty = num(p.get("quantity"))
                if ptype == "stock_paper":
                    market_value += qty * num(p.get("current_price_usd")) * num(p.get("current_fx_thb_per_usd"))
                else:
                    market_value += qty * num(p.get("current_price_thb"))
            equity = cash + market_value
            filled_orders = sum(1 for o in orders if o.get("status") in ("filled", "executed"))
            buy_orders = sum(1 for o in orders if o.get("side") == "buy" and o.get("status") in ("filled", "executed"))
            sell_orders = sum(1 for o in orders if o.get("side") == "sell" and o.get("status") in ("filled", "executed"))
            report = {
                **row, "reportStatus": "paper_data", "dataAvailable": True,
                "capital": capital, "cash": cash, "marketValue": market_value,
                "equity": equity, "realizedPnl": realized, "unrealizedPnl": unrealized,
                "netPnl": realized + unrealized, "returnPct": ((equity - capital) / capital * 100) if capital else 0,
                "fees": fees, "openPositions": len(positions), "orderCount": len(orders),
                "openOrders": sum(1 for o in orders if str(o.get("status") or "").lower() in ("new", "open", "pending", "partially_filled", "accepted")),
                "recentOrders": [{ "symbol": o.get("symbol"), "side": o.get("side"), "status": o.get("status"), "createdAt": o.get("created_at"), "portfolioType": ptype } for o in orders[:5]],
                "filledOrders": filled_orders, "buyOrders": buy_orders, "sellOrders": sell_orders,
                "positions": [{
                    "symbol": p.get("symbol"), "quantity": num(p.get("quantity")),
                    "entryPrice": (num(p.get("average_entry_price_usd")) * num(p.get("entry_fx_thb_per_usd"))) if ptype == "stock_paper" else num(p.get("average_entry_price_thb")),
                    "currentPrice": (num(p.get("current_price_usd")) * num(p.get("current_fx_thb_per_usd"))) if ptype == "stock_paper" else num(p.get("current_price_thb")),
                    "unrealizedPnl": num(p.get("unrealized_pnl_thb")), "openedAt": p.get("opened_at"),
                } for p in positions],
                "slotUpdatedAt": slot.get("updated_at"),
            }
            totals["paperPortfolios"] += 1
            for key, value in (
                ("paperCapital", capital), ("paperCash", cash), ("paperMarketValue", market_value),
                ("paperEquity", equity), ("paperRealizedPnl", realized),
                ("paperUnrealizedPnl", unrealized), ("paperFees", fees),
                ("paperNetPnl", realized + unrealized), ("openPositions", len(positions)),
                ("filledOrders", filled_orders),
                ("openOrders", sum(1 for o in orders if str(o.get("status") or "").lower() in ("new", "open", "pending", "partially_filled", "accepted"))),
            ):
                totals[key] += value
        else:
            report = {
                **row, "reportStatus": "live_locked" if row.get("mode") == "live" else "paper_slot_missing",
                "dataAvailable": False, "capital": num(row.get("allocated_capital")),
                "cash": None, "marketValue": None, "equity": None,
                "realizedPnl": None, "unrealizedPnl": None, "netPnl": None,
                "returnPct": None, "fees": None, "openPositions": 0, "orderCount": 0,
                "filledOrders": 0, "buyOrders": 0, "sellOrders": 0, "openOrders": 0, "recentOrders": [], "positions": [],
                "slotUpdatedAt": None,
            }
            if row.get("mode") == "live":
                totals["liveLocked"] += 1
        if is_paper and slot:
            daily_pnl = {}
            for order in orders:
                if str(order.get("status") or "").lower() not in ("filled", "executed"):
                    continue
                try:
                    day = datetime.fromisoformat(str(order.get("created_at")).replace("Z", "+00:00")).astimezone(ZoneInfo("Asia/Bangkok")).date().isoformat()
                except Exception:
                    continue
                daily_pnl[day] = daily_pnl.get(day, 0.0) + num(order.get("realized_pnl_thb"))
            series = [{"date": (datetime.now(ZoneInfo("Asia/Bangkok")).date() - timedelta(days=30)).isoformat(), "equity": round(capital, 2), "realizedPnl": 0.0}]
            running = capital
            for day, day_pnl in sorted(daily_pnl.items()):
                running += day_pnl
                series.append({"date": day, "equity": round(running, 2), "realizedPnl": round(day_pnl, 2)})
            today = datetime.now(ZoneInfo("Asia/Bangkok")).date().isoformat()
            if series[-1]["date"] == today:
                series[-1].update({"equity": round(equity, 2), "current": True})
            else:
                series.append({"date": today, "equity": round(equity, 2), "realizedPnl": round(realized, 2), "current": True})
            report["performanceSeries"] = series
        else:
            report["performanceSeries"] = []
        reports.append(report)

    # Owner report v2: central Paper positions remain isolated from the 40 member slots.
# Also include the separate central dual-paper engine. It is deliberately
    # kept separate from the 40 member slots unless its user is explicitly assigned.
    dual_portfolios = _sb_rest("blessing_dual_portfolios", {
        "select": "id,user_id,market,broker,currency,initial_capital,cash_balance,realized_pnl,fees_paid,mode,enabled,updated_at",
        "mode": "eq.paper", "limit": "100",
    }) or []
    dual_user_labels = {
        str(item.get("assigned_user_id")): item.get("member_label") or ("สมาชิก " + str(item.get("member_no")))
        for item in registry if item.get("assigned_user_id")
    }
    dual_stock_symbols = list(dict.fromkeys(
        str(p.get("symbol")) for dp in dual_portfolios if dp.get("market") == "stocks"
        for p in (_sb_rest("blessing_dual_positions", {
            "select": "symbol", "portfolio_id": "eq." + str(dp["id"]),
            "status": "eq.open", "limit": "500",
        }) or []) if p.get("symbol")
    ))
    dual_stock_snapshots = {}
    if dual_stock_symbols and ALPACA_KEY and ALPACA_SECRET and ALPACA_FEED in ("iex", "sip"):
        try:
            dual_stock_snapshots = _alpaca_snapshots(dual_stock_symbols)
        except Exception:
            log.exception("Owner portfolio report: Alpaca batch snapshot failed")
    dual_reports = []
    dual_totals = {}
    for dp in dual_portfolios:
        market = str(dp.get("market") or "")
        currency = str(dp.get("currency") or ("USD" if market == "stocks" else "USDT"))
        pid = str(dp.get("id"))
        positions = _sb_rest("blessing_dual_positions", {
            "select": "id,symbol,side,quantity,entry_price,current_price,stop_loss_price,take_profit_price,entry_fee,exit_fee,unrealized_pnl,realized_pnl,status,opened_at,closed_at,close_reason",
            "portfolio_id": "eq." + pid, "status": "eq.open", "limit": "500",
        }) or []
        orders = _sb_rest("blessing_dual_orders", {
            "select": "symbol,side,quantity,price,notional,fee,status,realized_pnl,reason,created_at",
            "portfolio_id": "eq." + pid, "order": "created_at.desc", "limit": "25",
        }) or []
        position_reports = []
        market_value = 0.0
        unrealized_total = 0.0
        for pos in positions:
            symbol = str(pos.get("symbol") or "")
            entry = num(pos.get("entry_price"))
            quantity = num(pos.get("quantity"))
            current = num(pos.get("current_price"))
            live_price = None
            price_source = "stored"
            price_at = None
            price_age = None
            if market == "crypto":
                quote = (state.get("crypto_quotes") or {}).get(symbol)
                if isinstance(quote, dict):
                    try:
                        stamp = datetime.fromisoformat(str(quote.get("receivedAt")).replace("Z", "+00:00"))
                        age = (datetime.now(timezone.utc) - stamp).total_seconds()
                        candidate = num(quote.get("price"))
                        if 0 <= age <= 5 and candidate > 0:
                            live_price, price_source, price_at, price_age = candidate, "Binance TH stream", quote.get("receivedAt"), age
                    except Exception:
                        pass
            elif market == "stocks":
                quote = (state.get("quotes") or {}).get(symbol)
                if isinstance(quote, dict):
                    try:
                        stamp = datetime.fromisoformat(str(quote.get("receivedAt")).replace("Z", "+00:00"))
                        age = (datetime.now(timezone.utc) - stamp).total_seconds()
                        candidate = _dual_price_number(quote.get("raw") or quote)
                        if 0 <= age <= 5 and candidate and candidate > 0:
                            live_price, price_source, price_at, price_age = float(candidate), "Alpaca IEX stream", quote.get("receivedAt"), age
                    except Exception:
                        pass
                if live_price is None:
                    snap = dual_stock_snapshots.get(symbol) or {}
                    trade = snap.get("latestTrade") or {}
                    candidate = num(trade.get("p"))
                    trade_at = trade.get("t")
                    if candidate > 0 and trade_at:
                        try:
                            stamp = datetime.fromisoformat(str(trade_at).replace("Z", "+00:00"))
                            age = (datetime.now(timezone.utc) - stamp).total_seconds()
                            if 0 <= age <= 5:
                                live_price, price_source, price_at, price_age = candidate, "Alpaca IEX snapshot", trade_at, age
                        except Exception:
                            pass
            if live_price is not None:
                current = live_price
            cost = entry * quantity
            gross_pnl = ((entry - current) if pos.get("side") == "short" else (current - entry)) * quantity if current > 0 and entry > 0 else None
            net_pnl = (gross_pnl - num(pos.get("entry_fee"))) if gross_pnl is not None else num(pos.get("unrealized_pnl"))
            pnl_pct = (net_pnl / cost * 100) if cost > 0 and net_pnl is not None else None
            value = ((entry * quantity + (net_pnl or 0.0) + float(pos.get("entry_fee") or 0)) if pos.get("side") == "short" else current * quantity) if current > 0 else 0.0
            market_value += value
            unrealized_total += net_pnl or 0.0
            position_reports.append({
                "symbol": symbol, "side": pos.get("side") or "long", "quantity": quantity, "entryPrice": entry,
                "currentPrice": current if current > 0 else None,
                "stopLossPrice": num(pos.get("stop_loss_price")) or None,
                "takeProfitPrice": num(pos.get("take_profit_price")) or None,
                "unrealizedPnl": net_pnl, "grossUnrealizedPnl": gross_pnl,
                "pnlPct": pnl_pct, "marketValue": value, "currency": currency,
                "priceFresh": live_price is not None, "priceSource": price_source,
                "priceUpdatedAt": price_at, "priceAgeSeconds": round(price_age, 2) if price_age is not None else None,
                "openedAt": pos.get("opened_at"),
            })
        cash = num(dp.get("cash_balance"))
        capital = num(dp.get("initial_capital"))
        equity = cash + market_value
        realized = num(dp.get("realized_pnl"))
        fee_total = num(dp.get("fees_paid"))
        order_reports = [{
            "symbol": o.get("symbol"), "side": o.get("side"),
            "quantity": num(o.get("quantity")), "price": num(o.get("price")),
            "notional": num(o.get("notional")), "fee": num(o.get("fee")),
            "status": o.get("status"), "realizedPnl": o.get("realized_pnl"),
            "reason": o.get("reason"), "createdAt": o.get("created_at"),
        } for o in orders]
        dual_reports.append({
            "market": market, "currency": currency, "broker": dp.get("broker"),
            "mode": dp.get("mode"), "enabled": bool(dp.get("enabled")),
            "memberLabel": dual_user_labels.get(str(dp.get("user_id")), "พอร์ต Paper กลาง · ยังไม่ผูกกับสมาชิก 40 พอร์ต"),
            "capital": capital, "cash": cash, "marketValue": market_value,
            "equity": equity, "realizedPnl": realized, "unrealizedPnl": unrealized_total,
            "netPnl": realized + unrealized_total,
            "returnPct": ((equity - capital) / capital * 100) if capital else 0,
            "fees": fee_total, "openPositions": len(position_reports),
            "positions": position_reports, "recentOrders": order_reports,
        })
        bucket = dual_totals.setdefault(currency, {
            "capital": 0.0, "cash": 0.0, "marketValue": 0.0, "equity": 0.0,
            "realizedPnl": 0.0, "unrealizedPnl": 0.0, "fees": 0.0,
            "openPositions": 0,
        })
        for key, value in (
            ("capital", capital), ("cash", cash), ("marketValue", market_value),
            ("equity", equity), ("realizedPnl", realized), ("unrealizedPnl", unrealized_total),
            ("fees", fee_total), ("openPositions", len(position_reports)),
        ):
            bucket[key] += value

    return {
        "ok": True, "reportType": "owner_all_member_portfolios",
        "memberCount": 10, "portfolioCount": len(reports),
        "refreshedAt": datetime.now(timezone.utc).isoformat(),
        "refreshIntervalSeconds": 10, "liveTradingEnabled": False,
        "totals": totals, "portfolios": reports,
        "dualPaperPortfolios": dual_reports, "dualPaperTotals": dual_totals,
        "dataNote": "Paper equity uses saved cash plus current position values. Live portfolios are locked and do not show allocated plans as real balances.",
    }


@app.get("/binance-th/account")
def binance_th_account_read(request: Request):
    """Read-only Binance TH account snapshot. No order endpoints are implemented here."""
    _binance_th_owner_auth(request)
    api_key = BINANCE_API_KEY
    api_secret = BINANCE_API_SECRET
    if not api_key or not api_secret:
        missing = "BINANCE_TH_API_KEY" if not api_key else "BINANCE_TH_API_SECRET"
        return {
            "ok": False, "configured": False, "connected": False,
            "provider": "Binance TH",
            "message": f"ยังไม่ได้ตั้งค่า {missing} ใน Render Environment",
        }
    # Binance TH account endpoint requires a fresh HMAC-SHA256 signature.
    signed_params = {
        "recvWindow": "5000",
        "timestamp": str(int(time.time() * 1000)),
    }
    query_string = urllib.parse.urlencode(signed_params)
    signature = hmac.new(
        api_secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    signed_url = "https://api.binance.th/api/v1/accountV2?" + query_string + "&signature=" + signature
    try:
        response = requests.get(
            signed_url,
            headers={"Accept": "application/json", "X-MBX-APIKEY": api_key},
            timeout=(5, 15),
        )
    except requests.RequestException:
        log.exception("Binance TH account read request failed")
        return {
            "ok": False, "configured": True, "connected": False,
            "provider": "Binance TH",
            "message": "Render ติดต่อ Binance TH ไม่สำเร็จ โปรดลองตรวจสอบเครือข่ายของบริการ",
        }

    try:
        payload = response.json() if response.content else {}
    except ValueError:
        payload = {}
    if not response.ok or (isinstance(payload, dict) and isinstance(payload.get("code"), int) and payload.get("code") != 0):
        status = response.status_code
        message = (
            "Binance TH ตอบ HTTP 403: WAF ปฏิเสธคำขอจาก IP ขาออกของ Render"
            if status == 403 else
            "Binance TH ตอบ HTTP 429: เรียก API ถี่เกินไป กรุณารอก่อนลองใหม่"
            if status == 429 else
            "Binance TH ตอบ HTTP 418: IP ต้นทางถูกระงับชั่วคราว"
            if status == 418 else
            str(payload.get("msg") or "Binance TH ปฏิเสธคำขอ")
            if isinstance(payload, dict) else
            "Binance TH ปฏิเสธคำขอ"
        )
        return {
            "ok": False, "configured": True, "connected": False,
            "provider": "Binance TH", "httpStatus": status,
            "errorCode": payload.get("code") if isinstance(payload, dict) else None,
            "message": message,
        }

    account = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else payload
    balances = account.get("balances", []) if isinstance(account, dict) else []
    assets = [
        {
            "asset": str(item.get("asset") or ""),
            "free": str(item.get("free") or "0"),
            "locked": str(item.get("locked") or "0"),
        }
        for item in balances if isinstance(item, dict)
        if float(item.get("free") or 0) > 0 or float(item.get("locked") or 0) > 0
    ]
    return {
        "ok": True, "configured": True, "connected": True,
        "provider": "Binance TH",
        "accountCanTrade": bool(account.get("canTrade", False)),
        "assetCount": len(assets), "assets": assets,
        "checkedAt": datetime.now(timezone.utc).isoformat(),
        "tradingEnabledInBlessing": False,
        "message": "ตรวจสอบบัญชีสำเร็จ · อ่านข้อมูลบัญชีเท่านั้น · ยังไม่ส่งคำสั่งซื้อขาย",
    }


_BINANCE_TH_TIME_OFFSET_MS = 0
_BINANCE_TH_TIME_SYNCED_MONOTONIC = 0.0
_BINANCE_TH_TIME_SYNC_LOCK = threading.Lock()


def _binance_th_sync_server_time(force: bool = False) -> int:
    """Keep signed requests aligned with Binance TH server time."""
    global _BINANCE_TH_TIME_OFFSET_MS, _BINANCE_TH_TIME_SYNCED_MONOTONIC
    now_mono = time.monotonic()
    if not force and now_mono - _BINANCE_TH_TIME_SYNCED_MONOTONIC < 60:
        return _BINANCE_TH_TIME_OFFSET_MS
    with _BINANCE_TH_TIME_SYNC_LOCK:
        now_mono = time.monotonic()
        if not force and now_mono - _BINANCE_TH_TIME_SYNCED_MONOTONIC < 60:
            return _BINANCE_TH_TIME_OFFSET_MS
        before_ms = int(time.time() * 1000)
        response = requests.get(
            BINANCE_TH_API_BASE + "/api/v1/time",
            headers={"Accept": "application/json"},
            timeout=(3, 5),
        )
        response.raise_for_status()
        payload = response.json()
        server_ms = int(payload["serverTime"])
        after_ms = int(time.time() * 1000)
        midpoint_ms = (before_ms + after_ms) // 2
        _BINANCE_TH_TIME_OFFSET_MS = server_ms - midpoint_ms
        _BINANCE_TH_TIME_SYNCED_MONOTONIC = time.monotonic()
        log.info("Binance TH server clock synchronized; offset_ms=%s", _BINANCE_TH_TIME_OFFSET_MS)
        return _BINANCE_TH_TIME_OFFSET_MS


def _binance_th_signed_request(method: str, path: str, params: dict, api_key: str, api_secret: str):
    def send_signed():
        signed = dict(params)
        signed["recvWindow"] = "5000"
        signed["timestamp"] = str(int(time.time() * 1000) + _BINANCE_TH_TIME_OFFSET_MS)
        query = urllib.parse.urlencode(signed)
        signature = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        url = BINANCE_TH_API_BASE + path + "?" + query + "&signature=" + signature
        response = requests.request(
            method, url, headers={"Accept": "application/json", "X-MBX-APIKEY": api_key},
            timeout=(5, 15),
        )
        try:
            payload = response.json() if response.content else {}
        except ValueError:
            payload = {"msg": "Binance TH returned a non-JSON response"}
        return response, payload

    _binance_th_sync_server_time()
    response, payload = send_signed()
    if isinstance(payload, dict) and payload.get("code") == -1021:
        _binance_th_sync_server_time(force=True)
        response, payload = send_signed()
    if not response.ok:
        status = response.status_code
        message = "Binance TH WAF blocked this server (HTTP 403)" if status == 403 else str(payload.get("msg") or "Binance TH rejected the request")
        raise HTTPException(status_code=502, detail=message)
    if isinstance(payload, dict) and isinstance(payload.get("code"), int) and payload.get("code") < 0:
        raise HTTPException(status_code=502, detail=str(payload.get("msg") or "Binance TH API error"))
    return payload


register_owner_auto_trade(
    app, _binance_th_owner_auth, _sb_rest, _sb_rpc,
    _binance_th_signed_request, BINANCE_API_KEY, BINANCE_API_SECRET,
    BINANCE_TH_API_BASE, lambda: altcoin_scanner(refresh=0), state,
    start_worker=False,
)


@app.get("/binance-th/live-readiness")
def binance_th_live_readiness(request: Request):
    """Owner-only preflight for Binance TH Spot. Never places an order."""
    _binance_th_owner_auth(request)
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        return {"ok": False, "provider": "Binance TH", "ready": False,
                "blockers": ["API_CREDENTIALS_MISSING"], "liveOrdersEnabled": False}
    try:
        account_payload = _binance_th_signed_request(
            "GET", "/api/v1/accountV2", {}, BINANCE_API_KEY, BINANCE_API_SECRET
        )
        account = account_payload.get("data") if isinstance(account_payload, dict) and isinstance(account_payload.get("data"), dict) else account_payload
        if not isinstance(account, dict):
            raise ValueError("ACCOUNT_RESPONSE_INVALID")
        exchange_payload = requests.get(
            BINANCE_TH_API_BASE + "/api/v1/exchangeInfo",
            headers={"Accept": "application/json", "X-MBX-APIKEY": BINANCE_API_KEY},
            timeout=(5, 15),
        )
        exchange_payload.raise_for_status()
        exchange = exchange_payload.json()
        symbols = exchange.get("symbols", []) if isinstance(exchange, dict) else []
        active_symbols = sum(1 for item in symbols if isinstance(item, dict) and item.get("status") == "TRADING")
        can_trade = bool(account.get("canTrade", False))
        blockers = []
        if not can_trade:
            blockers.append("BINANCE_ACCOUNT_TRADING_DISABLED")
        if active_symbols <= 0:
            blockers.append("EXCHANGE_SYMBOL_RULES_UNAVAILABLE")
        switch_on = os.getenv("BLESSING_OWNER_LIVE_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
        if not switch_on:
            blockers.append("OWNER_LIVE_MASTER_SWITCH_OFF")
        balances = account.get("balances", [])
        free_assets = [
            {"asset": str(item.get("asset") or ""), "free": str(item.get("free") or "0"),
             "locked": str(item.get("locked") or "0")}
            for item in balances if isinstance(item, dict)
            and (float(item.get("free") or 0) > 0 or float(item.get("locked") or 0) > 0)
        ]
        return {
            "ok": True, "provider": "Binance TH", "accountCanTrade": can_trade,
            "exchangeTradingSymbols": active_symbols, "assets": free_assets,
            "ready": len(blockers) == 0, "blockers": blockers,
            "liveOrdersEnabled": switch_on,
            "manualMarketOrderEndpointImplemented": True,
            "automatedLiveTradingImplemented": True,
            "checkedAt": datetime.now(timezone.utc).isoformat(),
            "message": "ตรวจสอบความพร้อมเท่านั้น ไม่มีการส่งคำสั่งซื้อขาย"
        }
    except Exception as exc:
        log.warning("Binance TH live readiness check failed (%s)", type(exc).__name__)
        return {"ok": False, "provider": "Binance TH", "ready": False,
                "blockers": ["BINANCE_TH_PREFLIGHT_FAILED"],
                "liveOrdersEnabled": False,
                "message": "ตรวจสอบบัญชีหรือกฎการซื้อขายไม่สำเร็จ จึงปิดการส่งคำสั่งไว้"}



def _binance_th_order_decimal(value, field_name):
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number <= 0:
            raise InvalidOperation()
        return number
    except (InvalidOperation, TypeError, ValueError):
        raise HTTPException(status_code=422, detail=f"{field_name} ต้องเป็นตัวเลขมากกว่า 0")


def _binance_th_step_floor(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value // step) * step


@app.post("/binance-th/live-order/market")
async def binance_th_owner_market_order(request: Request):
    """Owner-only Binance TH Spot market order. Requires explicit confirmation and a server-side kill switch."""
    owner = _binance_th_owner_auth(request)
    if os.getenv("BLESSING_OWNER_LIVE_ENABLED", "false").strip().lower() not in {"1", "true", "yes", "on"}:
        raise HTTPException(status_code=423, detail={"code":"OWNER_LIVE_MASTER_SWITCH_OFF","liveTradingEnabled":False,
            "message":"ระบบเงินจริงยังถูกล็อกโดยสวิตช์ความปลอดภัยบนเซิร์ฟเวอร์"})
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        raise HTTPException(status_code=503, detail="ยังไม่ได้ตั้งค่า API Key Binance TH")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="ข้อมูลคำสั่งไม่ถูกต้อง")
    if body.get("confirmLiveOrder") is not True:
        raise HTTPException(status_code=428, detail={"code":"EXPLICIT_CONFIRMATION_REQUIRED","message":"ต้องยืนยันคำสั่งเงินจริงอย่างชัดเจน"})
    symbol = str(body.get("symbol") or "").strip().upper()
    side = str(body.get("side") or "").strip().upper()
    if not symbol or len(symbol) > 30 or not symbol.isalnum():
        raise HTTPException(status_code=422, detail="รหัสคู่เหรียญไม่ถูกต้อง")
    if side not in ("BUY","SELL"):
        raise HTTPException(status_code=422, detail="รองรับเฉพาะ BUY หรือ SELL")
    if body.get("type", "MARKET") != "MARKET":
        raise HTTPException(status_code=422, detail="ระยะแรกเปิดเฉพาะคำสั่ง MARKET Spot")
    if body.get("portfolioMode", "owner") != "owner":
        raise HTTPException(status_code=403, detail="คำสั่งเงินจริงอนุญาตเฉพาะพอร์ตเจ้าของระบบ")
    try:
        account_payload = _binance_th_signed_request("GET","/api/v1/accountV2",{},BINANCE_API_KEY,BINANCE_API_SECRET)
        account = account_payload.get("data") if isinstance(account_payload,dict) and isinstance(account_payload.get("data"),dict) else account_payload
        if not isinstance(account,dict) or not account.get("canTrade"):
            raise HTTPException(status_code=423, detail="บัญชี Binance TH ยังไม่อนุญาตให้ซื้อขาย")
        exchange_payload = requests.get(BINANCE_TH_API_BASE+"/api/v1/exchangeInfo",headers={"Accept":"application/json","X-MBX-APIKEY":BINANCE_API_KEY},timeout=(5,15))
        exchange_payload.raise_for_status()
        exchange = exchange_payload.json()
        if isinstance(exchange,dict) and isinstance(exchange.get("data"),dict):
            exchange = exchange["data"]
        symbol_info = next((x for x in exchange.get("symbols",[]) if isinstance(x,dict) and x.get("symbol")==symbol),None)
        if not symbol_info or symbol_info.get("status") != "TRADING":
            raise HTTPException(status_code=422, detail="คู่เหรียญนี้ไม่มีสถานะ TRADING บน Binance TH")
        if "MARKET" not in (symbol_info.get("orderTypes") or []):
            raise HTTPException(status_code=422, detail="คู่เหรียญนี้ไม่รองรับคำสั่ง MARKET")
        base_asset = str(symbol_info.get("baseAsset") or "").upper()
        quote_asset = str(symbol_info.get("quoteAsset") or "").upper()
        if not base_asset or not quote_asset or quote_asset not in ("THB","USDT"):
            raise HTTPException(status_code=422, detail="ระยะแรกจำกัดคู่ Spot เฉพาะตลาด THB และ USDT")
        filters = {str(x.get("filterType")):x for x in symbol_info.get("filters",[]) if isinstance(x,dict)}
        market_lot = filters.get("MARKET_LOT_SIZE") or {}
        lot = market_lot if Decimal(str(market_lot.get("stepSize") or "0")) > 0 else (filters.get("LOT_SIZE") or market_lot)
        step = Decimal(str(lot.get("stepSize") or "0"))
        min_qty = Decimal(str(lot.get("minQty") or "0"))
        max_qty = Decimal(str(lot.get("maxQty") or "0"))
        balances = {str(x.get("asset") or "").upper():Decimal(str(x.get("free") or "0")) for x in account.get("balances",[]) if isinstance(x,dict)}
        client_order_id = "bls" + uuid.uuid4().hex[:30]
        if side == "BUY":
            requested_quote = _binance_th_order_decimal(body.get("quoteAmount"),"quoteAmount")
            quote_free = balances.get(quote_asset,Decimal("0"))
            ticker_resp = requests.get(BINANCE_TH_API_BASE+"/api/v1/ticker/price",params={"symbol":symbol},timeout=(5,12))
            ticker_resp.raise_for_status()
            ticker = ticker_resp.json()
            if isinstance(ticker,dict) and isinstance(ticker.get("data"),dict): ticker=ticker["data"]
            price = Decimal(str(ticker.get("price") or "0"))
            if price <= 0: raise HTTPException(status_code=503,detail="ไม่สามารถยืนยันราคาตลาดล่าสุดได้")
            estimated_qty = requested_quote / price
            notional_filter = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}
            min_notional = Decimal(str(notional_filter.get("minNotional") or "0"))
            if requested_quote < min_notional:
                raise HTTPException(status_code=422,detail=f"มูลค่าคำสั่งต่ำกว่าขั้นต่ำของตลาด ({min_notional} {quote_asset})")
            if requested_quote > quote_free:
                raise HTTPException(status_code=422,detail="ยอดเงิน Quote ที่ขอซื้อเกินยอด Free ใน Binance TH")
            reserved_asset, reserved_amount = quote_asset, requested_quote
            requested_qty_value, requested_quote_value = None, requested_quote
            order_params = {"symbol":symbol,"side":"BUY","type":"MARKET","quoteOrderQty":format(requested_quote,"f"),"newClientOrderId":client_order_id,"newOrderRespType":"FULL"}
            estimated = {"referencePrice":str(price),"estimatedBaseQuantity":str(estimated_qty),"quoteAmount":str(requested_quote)}
        else:
            requested_qty = _binance_th_order_decimal(body.get("quantity"),"quantity")
            requested_qty = _binance_th_step_floor(requested_qty,step)
            if requested_qty <= 0 or requested_qty < min_qty or (max_qty > 0 and requested_qty > max_qty):
                raise HTTPException(status_code=422,detail="จำนวนเหรียญไม่ผ่านกฎ MARKET_LOT_SIZE/LOT_SIZE")
            base_free = balances.get(base_asset,Decimal("0"))
            if requested_qty > base_free:
                raise HTTPException(status_code=422,detail="จำนวนเหรียญที่ขอขายเกินยอด Free ใน Binance TH")
            ticker_resp = requests.get(BINANCE_TH_API_BASE+"/api/v1/ticker/price",params={"symbol":symbol},timeout=(5,12))
            ticker_resp.raise_for_status()
            ticker = ticker_resp.json()
            if isinstance(ticker,dict) and isinstance(ticker.get("data"),dict): ticker=ticker["data"]
            price = Decimal(str(ticker.get("price") or "0"))
            if price <= 0: raise HTTPException(status_code=503,detail="ไม่สามารถยืนยันราคาตลาดล่าสุดได้")
            notional_filter = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}
            min_notional = Decimal(str(notional_filter.get("minNotional") or "0"))
            if requested_qty * price < min_notional:
                raise HTTPException(status_code=422,detail=f"มูลค่าคำสั่งต่ำกว่าขั้นต่ำของตลาด ({min_notional} {quote_asset})")
            reserved_asset, reserved_amount = base_asset, requested_qty
            requested_qty_value, requested_quote_value = requested_qty, None
            order_params = {"symbol":symbol,"side":"SELL","type":"MARKET","quantity":format(requested_qty,"f"),"newClientOrderId":client_order_id,"newOrderRespType":"FULL"}
            estimated = {"referencePrice":str(price),"estimatedQuoteValue":str(requested_qty*price),"baseQuantity":str(requested_qty)}
        reservation = _sb_rpc("blessing_owner_reserve_live_order",{
            "p_owner_user_id":owner["id"],"p_client_order_id":client_order_id,"p_symbol":symbol,"p_side":side,"p_order_type":"MARKET",
            "p_quote_asset":quote_asset,"p_base_asset":base_asset,"p_reserved_asset":reserved_asset,
            "p_reserved_amount":str(reserved_amount),"p_exchange_free":str(balances.get(reserved_asset,Decimal("0"))),
            "p_requested_quantity":str(requested_qty_value) if requested_qty_value is not None else None,
            "p_requested_quote_amount":str(requested_quote_value) if requested_quote_value is not None else None,
            "p_limit_price":None,"p_request_payload":{"symbol":symbol,"side":side,"type":"MARKET","estimated":estimated}
        })
        intent = reservation[0] if isinstance(reservation,list) and reservation else reservation
        intent_id = intent.get("id") if isinstance(intent,dict) else None
        if not intent_id:
            raise HTTPException(status_code=503,detail="ไม่สามารถสร้างรายการจองยอดเงินได้ จึงไม่ส่งคำสั่ง")
        _sb_rest("blessing_owner_live_order_intents",{"id":"eq."+str(intent_id)}, "PATCH", {"status":"SUBMITTING","updated_at":datetime.now(timezone.utc).isoformat()})
        try:
            exchange_result = _binance_th_signed_request("POST","/api/v1/order",order_params,BINANCE_API_KEY,BINANCE_API_SECRET)
        except Exception as submit_error:
            # Never blindly retry an order after an ambiguous submission result.
            try:
                lookup = _binance_th_signed_request("GET","/api/v1/order",{"symbol":symbol,"origClientOrderId":client_order_id},BINANCE_API_KEY,BINANCE_API_SECRET)
                if isinstance(lookup,dict) and lookup.get("data") and isinstance(lookup["data"],dict): lookup=lookup["data"]
                exchange_result = lookup
            except Exception:
                _sb_rest("blessing_owner_live_order_intents",{"id":"eq."+str(intent_id)},"PATCH",{
                    "status":"RECONCILIATION_REQUIRED","error_code":"SUBMIT_RESULT_AMBIGUOUS",
                    "error_message":type(submit_error).__name__,"updated_at":datetime.now(timezone.utc).isoformat()})
                _sb_rest("blessing_owner_live_order_events",{},"POST",{"order_intent_id":intent_id,"event_type":"RECONCILIATION_REQUIRED","event_data":{"reason":"submit_and_lookup_failed"}})
                raise HTTPException(status_code=503,detail={"code":"RECONCILIATION_REQUIRED","clientOrderId":client_order_id,"message":"ผลการส่งคำสั่งไม่ชัดเจน ระบบไม่ส่งซ้ำและพักยอดเงินไว้รอตรวจสอบ"})
        if isinstance(exchange_result,dict) and isinstance(exchange_result.get("data"),dict): exchange_result=exchange_result["data"]
        exchange_status = str(exchange_result.get("status") or "UNKNOWN").upper()
        status_map = {"NEW":"NEW","PARTIALLY_FILLED":"PARTIALLY_FILLED","FILLED":"FILLED","CANCELED":"CANCELED","REJECTED":"REJECTED","EXPIRED":"EXPIRED"}
        stored_status = status_map.get(exchange_status,"RECONCILIATION_REQUIRED")
        executed_qty = str(exchange_result.get("executedQty") or "0")
        cumulative_quote = str(exchange_result.get("cummulativeQuoteQty") or exchange_result.get("cumulativeQuoteQty") or "0")
        _sb_rest("blessing_owner_live_order_intents",{"id":"eq."+str(intent_id)},"PATCH",{
            "exchange_order_id":str(exchange_result.get("orderId") or "" ) or None,"exchange_status":exchange_status,
            "executed_quantity":executed_qty,"cumulative_quote_quantity":cumulative_quote,"status":stored_status,
            "remaining_reserved_amount":str(
                Decimal("0") if stored_status in ("FILLED","CANCELED","REJECTED","EXPIRED") else
                max(Decimal("0"), (Decimal(str(requested_quote_value)) - Decimal(cumulative_quote)) if side == "BUY"
                    else (Decimal(str(requested_qty_value)) - Decimal(executed_qty)))
            ),
            "exchange_response":exchange_result if isinstance(exchange_result,dict) else {},
            "finalized_at":datetime.now(timezone.utc).isoformat() if stored_status in ("FILLED","CANCELED","REJECTED","EXPIRED") else None,
            "updated_at":datetime.now(timezone.utc).isoformat()})
        _sb_rest("blessing_owner_live_order_events",{},"POST",{"order_intent_id":intent_id,"event_type":stored_status,"event_data":{"exchangeStatus":exchange_status,"executedQuantity":executed_qty,"cumulativeQuoteQuantity":cumulative_quote}})
        return {"ok":True,"provider":"Binance TH","clientOrderId":client_order_id,"symbol":symbol,"side":side,
            "status":stored_status,"exchangeStatus":exchange_status,"executedQuantity":executed_qty,
            "cumulativeQuoteQuantity":cumulative_quote,"estimate":estimated,"liveOrderSent":True,
            "message":"ส่งคำสั่งไปยัง Binance TH แล้ว; ตรวจสอบสถานะและยอดจับคู่จากรายการนี้"}
    except HTTPException:
        raise
    except Exception as exc:
        log.warning("Binance TH owner order failed (%s)",type(exc).__name__)
        raise HTTPException(status_code=503,detail="ตรวจสอบหรือส่งคำสั่งไม่สำเร็จ ระบบปฏิเสธคำสั่งเพื่อความปลอดภัย")



@app.get("/binance-th/live-orders")
def binance_th_owner_live_orders(request: Request, limit: int = 25):
    owner = _binance_th_owner_auth(request)
    limit = max(1,min(int(limit),100))
    rows = _sb_rest("blessing_owner_live_order_intents",{
        "select":"id,client_order_id,symbol,side,order_type,quote_asset,base_asset,reserved_asset,reserved_amount,requested_quantity,requested_quote_amount,exchange_order_id,exchange_status,executed_quantity,cumulative_quote_quantity,status,error_code,created_at,updated_at,finalized_at",
        "owner_user_id":"eq."+str(owner["id"]),"order":"created_at.desc","limit":str(limit)
    })
    return {"ok":True,"provider":"Binance TH","orders":rows,"count":len(rows),"liveOrdersEnabled":os.getenv("BLESSING_OWNER_LIVE_ENABLED","false").strip().lower() in {"1","true","yes","on"}}


@app.post("/binance-th/live-orders/reconcile")
async def binance_th_owner_reconcile_order(request: Request):
    owner = _binance_th_owner_auth(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400,detail="ข้อมูลคำขอไม่ถูกต้อง")
    client_order_id = str(body.get("clientOrderId") or "").strip()
    if not client_order_id or len(client_order_id)>36:
        raise HTTPException(status_code=422,detail="clientOrderId ไม่ถูกต้อง")
    rows = _sb_rest("blessing_owner_live_order_intents",{
        "select":"id,owner_user_id,client_order_id,symbol,status,requested_quantity,requested_quote_amount","owner_user_id":"eq."+str(owner["id"]),
        "client_order_id":"eq."+client_order_id,"limit":"1"
    })
    if not rows:
        raise HTTPException(status_code=404,detail="ไม่พบรายการคำสั่งของเจ้าของระบบ")
    row=rows[0]
    try:
        result=_binance_th_signed_request("GET","/api/v1/order",{"symbol":row["symbol"],"origClientOrderId":client_order_id},BINANCE_API_KEY,BINANCE_API_SECRET)
        if isinstance(result,dict) and isinstance(result.get("data"),dict): result=result["data"]
        exchange_status=str(result.get("status") or "UNKNOWN").upper()
        status_map={"NEW":"NEW","PARTIALLY_FILLED":"PARTIALLY_FILLED","FILLED":"FILLED","CANCELED":"CANCELED","REJECTED":"REJECTED","EXPIRED":"EXPIRED"}
        status=status_map.get(exchange_status,"RECONCILIATION_REQUIRED")
        executed=str(result.get("executedQty") or "0")
        cumulative=str(result.get("cummulativeQuoteQty") or result.get("cumulativeQuoteQty") or "0")
        now=datetime.now(timezone.utc).isoformat()
        _sb_rest("blessing_owner_live_order_intents",{"id":"eq."+str(row["id"])},"PATCH",{
            "exchange_order_id":str(result.get("orderId") or "") or None,"exchange_status":exchange_status,
            "executed_quantity":executed,"cumulative_quote_quantity":cumulative,"status":status,
            "remaining_reserved_amount":str(
                Decimal("0") if status in ("FILLED","CANCELED","REJECTED","EXPIRED") else
                max(Decimal("0"), (Decimal(str(rows[0].get("requested_quote_amount") or "0")) - Decimal(cumulative)) if rows[0].get("requested_quote_amount") is not None
                    else (Decimal(str(rows[0].get("requested_quantity") or "0")) - Decimal(executed)))
            ),
            "exchange_response":result,"error_code":None,"error_message":None,
            "finalized_at":now if status in ("FILLED","CANCELED","REJECTED","EXPIRED") else None,"updated_at":now
        })
        _sb_rest("blessing_owner_live_order_events",{},"POST",{"order_intent_id":row["id"],"event_type":"RECONCILED_"+status,
            "event_data":{"exchangeStatus":exchange_status,"executedQuantity":executed,"cumulativeQuoteQuantity":cumulative}})
        return {"ok":True,"clientOrderId":client_order_id,"status":status,"exchangeStatus":exchange_status,
            "executedQuantity":executed,"cumulativeQuoteQuantity":cumulative}
    except Exception as exc:
        log.warning("Binance TH order reconciliation failed (%s)",type(exc).__name__)
        _sb_rest("blessing_owner_live_order_intents",{"id":"eq."+str(row["id"])},"PATCH",{
            "status":"RECONCILIATION_REQUIRED","error_code":"ORDER_LOOKUP_FAILED",
            "error_message":type(exc).__name__,"updated_at":datetime.now(timezone.utc).isoformat()
        })
        raise HTTPException(status_code=503,detail="ตรวจสอบสถานะกับ Binance TH ไม่สำเร็จ จึงคงยอดจองไว้และไม่ส่งคำสั่งซ้ำ")


@app.post("/binance-th/order")
async def binance_th_place_order(request: Request):
    """Fail closed: the legacy manual order route bypasses member THB sub-ledgers."""
    _binance_th_owner_auth(request)
    raise HTTPException(
        status_code=423,
        detail={
            "code": "MEMBER_PORTFOLIO_ENGINE_NOT_READY",
            "message": "คำสั่งซื้อขายแบบเดิมถูกล็อกไว้ชั่วคราว เพื่อป้องกันการส่งคำสั่งที่ไม่ผ่านพอร์ตสมาชิกและบัญชีจัดสรร THB",
            "liveTradingEnabled": False,
        },
    )


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



def _dual_stock_market_price(symbol: str) -> tuple[float, str]:
    provider = str(state.get("provider") or "").lower()
    if provider != "webull":
        raise ValueError("STOCK_MARKET_DATA_NOT_CONNECTED")
    quote = state.get("quotes", {}).get(symbol)
    if not isinstance(quote, dict):
        raise ValueError("STOCK_QUOTE_NOT_AVAILABLE")
    received = quote.get("receivedAt")
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(str(received).replace("Z", "+00:00"))).total_seconds()
    except Exception:
        raise ValueError("STOCK_QUOTE_TIMESTAMP_INVALID")
    if age < 0 or age > 5:
        raise ValueError("STOCK_QUOTE_STALE")
    raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
    keys = {"price", "last_price", "lastprice", "trade_price", "last"}
    def find_price(value, depth=0):
        if depth > 5 or not isinstance(value, dict):
            return None
        for key, item in value.items():
            if str(key).lower() in keys:
                candidate = _num(item, 0)
                if candidate and candidate > 0:
                    return candidate
        for item in value.values():
            found = find_price(item, depth + 1)
            if found:
                return found
        return None
    price = find_price(raw)
    if not price:
        bid = _num(raw.get("bid"), 0) or 0
        ask = _num(raw.get("ask"), 0) or 0
        if bid > 0 and ask >= bid:
            price = (bid + ask) / 2
    if not price or price <= 0:
        raise ValueError("STOCK_QUOTE_PRICE_MISSING")
    return float(price), provider + "_stream"


def _binance_th_public_price(symbol: str) -> float:
    response = requests.get(
        "https://api.binance.th/api/v1/ticker/price",
        params={"symbol": symbol},
        headers={"Accept": "application/json", "User-Agent": "BLESSING-DualPaper/1.0"},
        timeout=(4, 10),
    )
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and "code" in payload:
        if payload.get("code") not in (0, "0", None):
            raise ValueError("BINANCE_TH_API_ERROR:" + str(payload.get("code")))
        payload = payload.get("data") or payload
    if isinstance(payload, list):
        payload = next((row for row in payload if isinstance(row, dict) and row.get("symbol") == symbol), None)
    if not isinstance(payload, dict) or str(payload.get("symbol") or symbol).upper() != symbol:
        raise ValueError("BINANCE_TH_INVALID_PRICE_RESPONSE")
    price = _dual_paper_number(payload.get("price"), "market_price")
    return price


# Independent crypto/US-equity paper portfolios. These endpoints are admin-only,
# authenticate the Supabase session server-side, and never call a broker order API.
def _dual_paper_number(value, field):
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail=field + " ต้องเป็นตัวเลข")
    if not math.isfinite(number) or number <= 0:
        raise HTTPException(status_code=422, detail=field + " ต้องมากกว่า 0")
    return number


def _dual_paper_portfolio_rows(user_id):
    return _sb_rest("blessing_dual_portfolios", {
        "select": "*", "user_id": "eq." + str(user_id), "order": "market.asc"
    })


@app.get("/ai/portfolio-overview")
def dual_paper_overview(request: Request):
    _paper_auth_user(request)
    return portfolio_overview()


@app.post("/ai/dual-paper/initialize")
def dual_paper_initialize(request: Request):
    user = _paper_auth_user(request)
    uid = str(user["id"])
    existing = _dual_paper_portfolio_rows(uid)
    by_market = {row.get("market"): row for row in existing}
    created = []
    configs = {
        "crypto": {"broker": "binance_th", "currency": "USDT", "capital": 100},
        "stocks": {"broker": "webull", "currency": "USD", "capital": 100},
    }
    for market, config in configs.items():
        if market in by_market:
            continue
        payload = {
            "user_id": uid, "market": market, "broker": config["broker"],
            "currency": config["currency"], "initial_capital": config["capital"],
            "cash_balance": config["capital"], "mode": "paper", "enabled": False,
            "reserve_cash_pct": 20 if market == "crypto" else 0,
            "risk_per_trade_pct": 0.5 if market == "crypto" else 1,
            "max_daily_loss_pct": 2 if market == "crypto" else 3, "max_weekly_loss_pct": 6,
            "max_drawdown_pct": 5 if market == "crypto" else 10,
            "max_open_positions": 3 if market == "crypto" else 5,
            "peak_equity": config["capital"],
        }
        rows = _sb_rest("blessing_dual_portfolios", {}, "POST", payload)
        if not rows:
            raise HTTPException(status_code=502, detail="สร้างพอร์ตทดลองไม่สำเร็จ")
        created.append(market)
    return {
        "ok": True, "created": created, "alreadyExisted": [m for m in configs if m in by_market],
        "enabled": False, "mode": "paper",
        "message": "สร้างพอร์ตทดลองแยก 100 USDT และ 100 USD แล้ว โดยยังปิดการซื้อขายไว้",
    }


@app.get("/ai/dual-paper/status")
def dual_paper_status(request: Request):
    user = _paper_auth_user(request)
    uid = str(user["id"])
    portfolios = _dual_paper_portfolio_rows(uid)
    result = {}
    for portfolio in portfolios:
        pid = str(portfolio["id"])
        positions = _sb_rest("blessing_dual_positions", {
            "select": "id,market,symbol,side,quantity,entry_price,current_price,best_price,stop_loss_price,take_profit_price,entry_fee,unrealized_pnl,status,opened_at",
            "portfolio_id": "eq." + pid, "user_id": "eq." + uid, "status": "eq.open",
            "order": "opened_at.desc", "limit": "50",
        })
        orders = _sb_rest("blessing_dual_orders", {
            "select": "id,market,symbol,side,quantity,price,notional,fee,status,reason,realized_pnl,created_at",
            "portfolio_id": "eq." + pid, "user_id": "eq." + uid,
            "order": "created_at.desc", "limit": "1000",
        })
        unrealized = sum(float(p.get("unrealized_pnl") or 0) for p in positions)
        position_value = sum(
            (float(p.get("entry_price") or 0) * float(p.get("quantity") or 0)
             + float(p.get("unrealized_pnl") or 0) + float(p.get("entry_fee") or 0))
            if p.get("side") == "short"
            else float(p.get("quantity") or 0) * float(p.get("current_price") or p.get("entry_price") or 0)
            for p in positions
        )
        cash = float(portfolio.get("cash_balance") or 0)
        realized = float(portfolio.get("realized_pnl") or 0)
        long_closed = [o for o in orders if o.get("side") == "sell" and o.get("realized_pnl") is not None]
        short_closed = [o for o in orders if o.get("side") == "cover" and o.get("realized_pnl") is not None]
        long_pnl = sum(float(o.get("realized_pnl") or 0) for o in long_closed)
        short_pnl = sum(float(o.get("realized_pnl") or 0) for o in short_closed)
        result[portfolio["market"]] = {
            "id": pid, "broker": portfolio["broker"], "currency": portfolio["currency"],
            "initialCapital": float(portfolio["initial_capital"]), "cashBalance": cash,
            "equity": cash + position_value, "realizedPnl": realized,
            "unrealizedPnl": unrealized, "feesPaid": float(portfolio.get("fees_paid") or 0),
            "enabled": bool(portfolio.get("enabled")), "configured": True, "mode": "paper",
            "marketValue": position_value, "totalPnl": realized + unrealized,
            "directionStats": {
                "longClosedTrades": len(long_closed), "shortClosedTrades": len(short_closed),
                "longRealizedPnl": long_pnl, "shortRealizedPnl": short_pnl,
            },
            "riskLimits": {
                "reserveCashPct": float(portfolio.get("reserve_cash_pct") or 0),
                "riskPerTradePct": float(portfolio.get("risk_per_trade_pct") or 0),
                "dailyLossLimitPct": float(portfolio.get("max_daily_loss_pct") or 0),
                "weeklyLossLimitPct": float(portfolio.get("max_weekly_loss_pct") or 0),
                "maxDrawdownPct": float(portfolio.get("max_drawdown_pct") or 0),
                "dailyLossLimitEnabled": portfolio["market"] == "crypto",
                "weeklyLossLimitEnabled": True,
                "drawdownStopEnabled": True,
                "maxOpenPositions": int(portfolio.get("max_open_positions") or 0),
            },
            "positions": positions, "orders": orders,
            "openPositions": positions, "recentOrders": orders[:30],
        }
    return {"ok": True, "mode": "paper", "liveOrdersEnabled": False,
            "crossPortfolioTransfersEnabled": False, "portfolios": result,
            "worker": {**dict(DUAL_PAPER_WORKER_STATE),
                       "intervalSeconds": DUAL_PAPER_WORKER_INTERVAL,
                       "automaticTakeProfitStopLoss": True}}


@app.post("/ai/dual-paper/refresh")
async def dual_paper_refresh(request: Request):
    user = _paper_auth_user(request)
    uid = str(user["id"])
    portfolios = _dual_paper_portfolio_rows(uid)
    updated = []
    closed = []
    unavailable = []
    for portfolio in portfolios:
        market = portfolio["market"]
        positions = _sb_rest("blessing_dual_positions", {
            "select": "id,symbol,side,quantity,entry_price,entry_fee",
            "portfolio_id": "eq." + str(portfolio["id"]),
            "user_id": "eq." + uid, "status": "eq.open", "limit": "100",
        })
        for position in positions:
            symbol = str(position.get("symbol") or "")
            try:
                price = _dual_market_price(market, symbol)
                result = _sb_rpc("blessing_dual_mark_position", {
                    "p_user_id": uid, "p_market": market,
                    "p_position_id": str(position["id"]), "p_price": price,
                })
                if isinstance(result, dict) and result.get("closed"):
                    closed.append({
                        "market": market, "symbol": symbol,
                        "side": position.get("side"), "result": result,
                    })
                else:
                    updated.append({
                        "market": market, "symbol": symbol, "price": price,
                        "side": position.get("side"),
                        "unrealizedPnl": result.get("unrealizedPnl") if isinstance(result, dict) else None,
                    })
            except Exception as exc:
                unavailable.append({"market": market, "symbol": symbol, "reason": type(exc).__name__})
    return {
        "ok": True, "updatedCount": len(updated), "updated": updated,
        "closedCount": len(closed), "closed": closed,
        "unavailableCount": len(unavailable), "unavailable": unavailable,
        "automaticTakeProfitStopLoss": True,
        "trailingStopEnabled": True,
        "liveOrdersEnabled": False,
    }


@app.post("/ai/dual-paper/start")
@app.post("/ai/dual-paper/enable")
async def dual_paper_enable(request: Request):
    user = _paper_auth_user(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="ข้อมูลคำขอไม่ถูกต้อง")
    market = str(body.get("market") or "").lower()
    if market not in ("crypto", "stocks"):
        raise HTTPException(status_code=422, detail="market ต้องเป็น crypto หรือ stocks")
    rows = _sb_rest("blessing_dual_portfolios", {
        "select": "id,enabled", "user_id": "eq." + str(user["id"]),
        "market": "eq." + market, "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="ยังไม่ได้สร้างพอร์ตทดลอง")
    _sb_rest("blessing_dual_portfolios", {
        "id": "eq." + str(rows[0]["id"]), "user_id": "eq." + str(user["id"]),
    }, "PATCH", {"enabled": True, "updated_at": datetime.now(timezone.utc).isoformat()})
    return {"ok": True, "market": market, "enabled": True, "mode": "paper"}


@app.post("/ai/dual-paper/stop")
@app.post("/ai/dual-paper/disable")
async def dual_paper_disable(request: Request):
    user = _paper_auth_user(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="ข้อมูลคำขอไม่ถูกต้อง")
    market = str(body.get("market") or "").lower()
    if market not in ("crypto", "stocks"):
        raise HTTPException(status_code=422, detail="market ต้องเป็น crypto หรือ stocks")
    rows = _sb_rest("blessing_dual_portfolios", {
        "select": "id", "user_id": "eq." + str(user["id"]),
        "market": "eq." + market, "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="ไม่พบพอร์ตทดลอง")
    _sb_rest("blessing_dual_portfolios", {
        "id": "eq." + str(rows[0]["id"]), "user_id": "eq." + str(user["id"]),
    }, "PATCH", {"enabled": False, "updated_at": datetime.now(timezone.utc).isoformat()})
    return {"ok": True, "market": market, "enabled": False, "mode": "paper"}


@app.get("/ai/dual-paper/quote")
def dual_paper_quote(request: Request, market: str, symbol: str):
    _paper_auth_user(request)
    market = str(market or "").lower()
    symbol = str(symbol or "").strip().upper()
    if market == "crypto":
        if not symbol.endswith("USDT") or not symbol[:-4].isalnum():
            raise HTTPException(status_code=422, detail="พอร์ตคริปโตรับเฉพาะคู่ Spot USDT")
        try:
            price = _dual_market_price(market, symbol)
            source = "Binance TH WebSocket live ticker"
            price_source = "binance_th_public"
            currency = "USDT"
        except Exception as exc:
            log.warning("Binance TH quote unavailable for %s: %s", symbol, type(exc).__name__)
            raise HTTPException(status_code=503, detail="Binance TH ไม่มีราคาสดสำหรับคู่นี้ ขณะนี้จึงไม่รับคำสั่ง")
    elif market == "stocks":
        if not symbol or len(symbol) > 10 or not symbol[0].isalpha() or not all(c.isalnum() or c in ".-" for c in symbol):
            raise HTTPException(status_code=422, detail="สัญลักษณ์หุ้นไม่ถูกต้อง")
        try:
            price = _dual_market_price("stocks", symbol)
            price_source = "alpaca_iex_verified_live"
            source = "Alpaca IEX verified live trade/quote"
            currency = "USD"
        except Exception as exc:
            log.warning("Stock quote unavailable for %s: %s", symbol, type(exc).__name__)
            raise HTTPException(status_code=503, detail="ไม่มีราคาหุ้นจริงที่อัปเดตภายใน 5 วินาที จึงไม่รับคำสั่ง")
    else:
        raise HTTPException(status_code=422, detail="market ต้องเป็น crypto หรือ stocks")
    return {"ok": True, "market": market, "symbol": symbol, "price": price,
            "currency": currency, "source": source, "priceSource": price_source,
            "liveOrdersEnabled": False}



def _binance_public_price(symbol: str):
    """Use Binance TH Spot ticker for Paper marks; never route to Binance Global."""
    last_error = None
    try:
        response = requests.get(
            BINANCE_TH_API_BASE + "/api/v1/ticker/24hr",
            params={"symbol": str(symbol or "").upper()},
            timeout=(4, 10), headers=_binance_th_market_headers(),
        )
        response.raise_for_status()
        payload = response.json()
        price = _num(payload.get("lastPrice"), 0) if isinstance(payload, dict) else 0
        if price and price > 0:
            return float(price)
        last_error = RuntimeError("BINANCE_TH_INVALID_24H_PRICE_RESPONSE")
    except Exception as exc:
        last_error = exc
        log.warning("Binance TH Paper mark-price failed for %s (%s)", symbol, type(exc).__name__)
    # OKX is a mark-only fallback for an existing Paper position, never for new entries.
    try:
        base = str(symbol or "").upper()
        if base.endswith("USDT"):
            response = requests.get(
                "https://www.okx.com/api/v5/market/ticker",
                params={"instId": base[:-4] + "-USDT"}, timeout=(4, 8),
                headers={"User-Agent": "BlessingPaperWorker/1.0"},
            )
            response.raise_for_status()
            payload = response.json()
            rows = payload.get("data") if isinstance(payload, dict) and payload.get("code") == "0" else []
            price = _num(rows[0].get("last"), 0) if rows else 0
            if price and price > 0:
                log.info("Paper position mark price fallback provider=OKX symbol=%s", symbol)
                return float(price)
    except Exception as exc:
        last_error = exc
        log.warning("OKX Paper mark-price fallback failed for %s (%s)", symbol, type(exc).__name__)
    if last_error:
        raise last_error
    return None

def _paper_worker_cycle():
    settings_rows = _sb_rest("paper_bot_settings", {"select":"user_id,enabled,mode,starting_balance,order_budget,max_open_positions,take_profit_pct,stop_loss_pct,daily_loss_limit_pct,allowed_exchanges",
                                                     "mode":"eq.paper", "limit":"100"})
    setting_user_ids = list(dict.fromkeys(str(row.get("user_id")) for row in settings_rows if row.get("user_id")))
    active_member_rows = _sb_rest("profiles", {
        "select":"user_id", "user_id":"in.(" + ",".join(setting_user_ids) + ")",
        "role":"eq.member", "status":"eq.active", "limit":"200"
    }) if setting_user_ids else []
    active_member_ids = {str(row.get("user_id")) for row in (active_member_rows or [])}
    for setting in settings_rows:
        uid = setting["user_id"]
        if str(uid) not in active_member_ids:
            continue
        positions = _sb_rest("paper_bot_positions", {"select":"*", "user_id":"eq." + uid, "status":"eq.open", "limit":"20"})
        if not setting.get("enabled") and not positions:
            continue
        market = altcoin_scanner(0)
        PAPER_WORKER_STATE["scanner"] = {
            "status": market.get("status"),
            "provider": market.get("exchange") or market.get("provider"),
            "scanned": market.get("scanned", 0),
            "evaluated": market.get("evaluated", 0),
            "fastEntryCount": market.get("fastEntryCount", 0),
            "candleErrors": market.get("candleErrors", 0),
            "updatedAt": market.get("updatedAt"),
            "metricsSource": market.get("metricsSource"),
        }
        updated_at = market.get("updatedAt")
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(updated_at.replace("Z","+00:00"))).total_seconds() if updated_at else 999999
        except Exception:
            age = 999999
        fresh = market.get("status") == "ok" and age <= 90
        candidates = {x.get("symbol"): x for x in (market.get("candidates") or [])
                      if x.get("symbol") and x.get("source") == "Binance TH Spot API"}
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
            position_side = str(pos.get("position_side") or "long").lower()
            # Adaptive ATR trailing stop: only tighten protection, never widen risk.
            if row and fresh:
                atr_stop_pct = min(2.0, max(0.8, _num(row.get("atrStopPct"), 1.5) or 1.5))
                entry_price = _num(pos.get("entry_price"), 0)
                old_stop = _num(pos.get("stop_loss_price"), 0)
                if entry_price > 0 and position_side == "short" and price < entry_price:
                    proposed_stop = price * (1 + atr_stop_pct / 100)
                    if old_stop <= 0 or proposed_stop < old_stop:
                        _sb_rest("paper_bot_positions", {
                            "id":"eq." + str(pos["id"]), "user_id":"eq." + str(uid), "status":"eq.open"
                        }, "PATCH", {"stop_loss_price":proposed_stop})
                elif entry_price > 0 and position_side != "short" and price > entry_price:
                    proposed_stop = price * (1 - atr_stop_pct / 100)
                    if old_stop <= 0 or proposed_stop > old_stop:
                        _sb_rest("paper_bot_positions", {
                            "id":"eq." + str(pos["id"]), "user_id":"eq." + str(uid), "status":"eq.open"
                        }, "PATCH", {"stop_loss_price":proposed_stop})
            if position_side == "short":
                reason = "TAKE_PROFIT" if price <= float(pos["take_profit_price"]) else (
                         "STOP_LOSS" if price >= float(pos["stop_loss_price"]) else None)
                close_action = "cover"
            else:
                reason = "TAKE_PROFIT" if price >= float(pos["take_profit_price"]) else (
                         "STOP_LOSS" if price <= float(pos["stop_loss_price"]) else None)
                close_action = "sell"
            if reason:
                result = _sb_rpc("paper_bot_worker_trade", {"p_user_id":uid,"p_action":close_action,"p_symbol":pos["symbol"],
                                 "p_exchange":"Binance","p_price":price,"p_position_id":pos["id"],"p_reason":reason,"p_signal_score":None})
                PAPER_WORKER_STATE["actions"].append({"at":datetime.now(timezone.utc).isoformat(),"action":close_action,
                                                       "symbol":pos["symbol"],"reason":reason,"result":result})
                PAPER_WORKER_STATE["actions"] = PAPER_WORKER_STATE["actions"][-20:]
        # Crypto markets trade continuously; stock extended-hours entitlements do not apply.
        if not setting.get("enabled") or not fresh:
            continue
        # Refresh open positions after exits and enforce the persisted position cap.
        positions = _sb_rest("paper_bot_positions", {"select":"id,symbol,status,position_side,entry_price,quantity", "user_id":"eq." + uid, "status":"eq.open", "limit":"20"})
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
                direction = -1 if str(p.get("position_side") or "long").lower() == "short" else 1
                open_pnl += direction * (_num(q.get("price"),0) - _num(p.get("entry_price"),0)) * _num(p.get("quantity"),0)
        total_pnl = float(account.get("realized_pnl") or 0) + open_pnl
        start_balance = float(account.get("starting_balance") or setting.get("starting_balance") or 88.4173298)
        day_start = datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0).isoformat()
        daily_orders = _sb_rest("paper_bot_orders", {
            "select":"realized_pnl","user_id":"eq." + uid,"side":"in.(sell,cover)",
            "created_at":"gte." + day_start,"status":"eq.filled","limit":"500"
        })
        daily_pnl = sum(_num(x.get("realized_pnl"),0) or 0 for x in daily_orders) + open_pnl
        if daily_pnl <= -(start_balance * 0.02):
            continue
        if total_pnl <= -(start_balance * 0.05):
            continue
        held = {p.get("symbol") for p in positions}
        btc = candidates.get("BTCUSDT") or {}
        btc_m15 = _num(btc.get("m15"), 0) or 0
        btc_m60 = _num(btc.get("m60"), 0) or 0
        if btc.get("trendDownAligned") is True and btc_m15 < -0.08 and btc_m60 < 0:
            btc_market_bias = "bearish"
        elif btc.get("trendAligned") is True and btc_m15 > 0.08 and btc_m60 > 0:
            btc_market_bias = "bullish"
        else:
            btc_market_bias = "neutral"
        ranked = []
        for candidate in candidates.values():
            if candidate.get("symbol") in held or candidate.get("exchangeSymbol") != candidate.get("symbol"):
                continue
            if not _num(candidate.get("price"),0):
                continue
            candidate_context = {**candidate, "btcMarketBias": btc_market_bias}
            decision = _crypto_three_strategy_decision(candidate_context)
            if decision:
                ranked.append((candidate,decision))
        ranked.sort(key=lambda pair:pair[1].get("opportunityScore",0),reverse=True)
        allowed = setting.get("allowed_exchanges") or ["Binance"]
        if "Binance" not in allowed:
            continue
        for candidate,decision in ranked:
            if len(positions) >= int(setting.get("max_open_positions") or 3):
                break
            cooldown_key = uid + ":" + candidate["symbol"]
            if time.time() - PAPER_ENTRY_COOLDOWNS.get(cooldown_key,0) < 180:
                continue
            try:
                entry_action = "short" if decision.get("side") == "short" else "buy"
                result = _sb_rpc("paper_bot_worker_trade", {"p_user_id":uid,"p_action":entry_action,"p_symbol":candidate["symbol"],
                                  "p_exchange":"Binance","p_price":float(candidate["price"]),"p_position_id":None,
                                  "p_reason":"ADAPTIVE_"+entry_action.upper()+"_"+str(decision.get("strategy") or "MOMENTUM"),
                                  "p_signal_score":int(min(100,decision.get("opportunityScore") or 0))})
                if isinstance(result, dict) and result.get("position_id"):
                    atr_stop_pct = min(2.0, max(0.8, _num(candidate.get("atrStopPct"), 1.5) or 1.5))
                    target_pct = max(3.0, atr_stop_pct * 2.0)
                    fill_price = float(candidate["price"])
                    is_short = entry_action == "short"
                    _sb_rest("paper_bot_positions", {
                        "id":"eq." + str(result["position_id"]), "user_id":"eq." + str(uid), "status":"eq.open"
                    }, "PATCH", {
                        "stop_loss_price":fill_price * (1 + atr_stop_pct / 100 if is_short else 1 - atr_stop_pct / 100),
                        "take_profit_price":fill_price * (1 - target_pct / 100 if is_short else 1 + target_pct / 100),
                    })
                PAPER_ENTRY_COOLDOWNS[cooldown_key] = time.time()
                PAPER_WORKER_STATE["actions"].append({"at":datetime.now(timezone.utc).isoformat(),"action":entry_action,
                                                       "symbol":candidate["symbol"],"strategy":decision.get("strategy"),"result":result})
                PAPER_WORKER_STATE["actions"] = PAPER_WORKER_STATE["actions"][-20:]
                positions.append({"symbol":candidate["symbol"]})
            except Exception as exc:
                log.warning("Paper worker entry skipped for %s: %s detail=%s", candidate["symbol"], type(exc).__name__, str(getattr(exc, "detail", None) or exc)[:180])
                continue


def _paper_worker_loop():
    while True:
        try:
            if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
                PAPER_WORKER_STATE.update({"status":"waiting_config","lastError":"SUPABASE_WORKER_CONFIG_MISSING"})
                STOCK_PAPER_WORKER_STATE.update({"status":"waiting_config","lastError":"SUPABASE_WORKER_CONFIG_MISSING"})
                time.sleep(30)
                continue
            try:
                _paper_worker_cycle()
                PAPER_WORKER_STATE.update({"status":"running","lastCycleAt":datetime.now(timezone.utc).isoformat(),
                                           "lastError":None,"cycles":PAPER_WORKER_STATE.get("cycles",0)+1})
            except Exception as crypto_exc:
                PAPER_WORKER_STATE.update({"status":"error","lastError":type(crypto_exc).__name__,
                                           "lastCycleAt":datetime.now(timezone.utc).isoformat()})
                log.exception("Crypto Paper Trading worker cycle failed")
            try:
                _stock_paper_worker_cycle()
            except Exception as stock_exc:
                STOCK_PAPER_WORKER_STATE.update({"status":"error","lastError":type(stock_exc).__name__,
                                                 "lastCycleAt":datetime.now(timezone.utc).isoformat()})
                log.exception("Stock Paper Trading worker cycle failed")
        except Exception as exc:
            PAPER_WORKER_STATE.update({"status":"error","lastError":type(exc).__name__,
                                       "lastCycleAt":datetime.now(timezone.utc).isoformat()})
            log.exception("Persistent Paper Trading worker cycle failed")
        time.sleep(PAPER_WORKER_INTERVAL)


@app.on_event("startup")
async def start_paper_worker():
    global PAPER_WORKER_THREAD
    # Pause background crypto/stock Paper polling by default to protect Supabase.
    # This does not affect the owner LIVE auto-trading engine.
    if os.getenv("PAPER_WORKER_ENABLED", "false").strip().lower() not in {"1", "true", "yes", "on"}:
        PAPER_WORKER_STATE.update({"status":"paused","lastError":"DISABLED_TO_PROTECT_DATABASE"})
        STOCK_PAPER_WORKER_STATE.update({"status":"paused","lastError":"DISABLED_TO_PROTECT_DATABASE"})
        log.warning("Crypto/Stock Paper worker paused; set PAPER_WORKER_ENABLED=true to re-enable")
        return
    if PAPER_WORKER_THREAD is None or not PAPER_WORKER_THREAD.is_alive():
        PAPER_WORKER_THREAD = threading.Thread(target=_paper_worker_loop, name="paper-trading-worker", daemon=True)
        PAPER_WORKER_THREAD.start()



# Separate US-equity Paper Trading engine. It shares market data only; its
# accounts, settings, positions, orders and risk limits are isolated from crypto.
STOCK_PAPER_WORKER_STATE = {
    "status": "waiting_credentials" if not (ALPACA_KEY and ALPACA_SECRET) else "starting",
    "lastCycleAt": None, "lastError": None, "cycles": 0, "actions": []
}
STOCK_PAPER_ENTRY_COOLDOWNS = {}


def _stock_paper_engine_user(request: Request):
    return _paper_auth_user(request)


def _stock_paper_ensure_user(user_id: str):
    settings = _sb_rest("stock_paper_settings", {
        "select":"user_id,enabled,mode,starting_balance,order_budget,max_open_positions,take_profit_pct,stop_loss_pct,daily_loss_limit_pct,fee_pct,slippage_bps",
        "user_id":"eq." + user_id, "limit":"1"
    })
    if not settings:
        settings = _sb_rest("stock_paper_settings", {}, "POST", {
            "user_id": user_id, "enabled": False, "mode": "paper",
            "starting_balance": 3000, "order_budget": 100,
            "max_open_positions": 5, "take_profit_pct": 3,
            "stop_loss_pct": 1.5, "daily_loss_limit_pct": 3,
            "fee_pct": 0.10, "slippage_bps": 5
        })
        settings = settings or _sb_rest("stock_paper_settings", {
            "select":"user_id,enabled,mode,starting_balance,order_budget,max_open_positions,take_profit_pct,stop_loss_pct,daily_loss_limit_pct,fee_pct,slippage_bps",
            "user_id":"eq." + user_id, "limit":"1"
        })
    accounts = _sb_rest("stock_paper_accounts", {
        "select":"user_id", "user_id":"eq." + user_id, "limit":"1"
    })
    if not accounts:
        _sb_rest("stock_paper_accounts", {}, "POST", {
            "user_id": user_id, "starting_balance": 3000,
            "cash_balance": 3000, "status": "active"
        })
    return settings[0] if settings else {}


@app.post("/stock-paper-engine/start")
def stock_paper_engine_start(request: Request):
    user = _stock_paper_engine_user(request)
    if not (ALPACA_KEY and ALPACA_SECRET):
        raise HTTPException(status_code=503, detail="ยังไม่ได้ตั้งค่า Alpaca API สำหรับข้อมูลหุ้น จึงยังเริ่ม Stock Paper Engine ไม่ได้")
    setting = _stock_paper_ensure_user(str(user["id"]))
    _sb_rest("stock_paper_settings", {"user_id":"eq." + str(user["id"])}, "PATCH", {
        "enabled": True, "updated_at": datetime.now(timezone.utc).isoformat()
    })
    return {"ok": True, "enabled": True, "mode": "paper", "settings": setting,
            "message": "Stock Paper Engine enabled; no live orders are sent."}


@app.post("/stock-paper-engine/stop")
def stock_paper_engine_stop(request: Request):
    user = _stock_paper_engine_user(request)
    _stock_paper_ensure_user(str(user["id"]))
    _sb_rest("stock_paper_settings", {"user_id":"eq." + str(user["id"])}, "PATCH", {
        "enabled": False, "updated_at": datetime.now(timezone.utc).isoformat()
    })
    return {"ok": True, "enabled": False,
            "message": "หยุดเปิดสถานะหุ้นใหม่แล้ว; ระบบยังติดตาม TP/SL ของสถานะที่เปิดอยู่"}


@app.get("/stock-paper-engine/status")
def stock_paper_engine_status(request: Request):
    user = _stock_paper_engine_user(request)
    uid = str(user["id"])
    _stock_paper_ensure_user(uid)
    settings = _sb_rest("stock_paper_settings", {
        "select":"enabled,mode,starting_balance,order_budget,max_open_positions,take_profit_pct,stop_loss_pct,daily_loss_limit_pct,fee_pct,slippage_bps,updated_at",
        "user_id":"eq." + uid, "limit":"1"
    })
    accounts = _sb_rest("stock_paper_accounts", {
        "select":"starting_balance,cash_balance,realized_pnl,fees_paid,status",
        "user_id":"eq." + uid, "limit":"1"
    })
    positions = _sb_rest("stock_paper_positions", {
        "select":"id,symbol,quantity,entry_price,current_price,take_profit_price,stop_loss_price,unrealized_pnl,opened_at,status",
        "user_id":"eq." + uid, "status":"eq.open", "order":"opened_at.desc", "limit":"50"
    })
    account = accounts[0] if accounts else {}
    portfolio_base = float(account.get("starting_balance") or (settings[0].get("starting_balance") if settings else 0) or 0)
    allocated_30 = portfolio_base * 0.30
    open_exposure = sum(
        (_num(pos.get("quantity"),0) or 0) * (_num(pos.get("entry_price"),0) or 0)
        for pos in positions
    )
    allocation = {
        "strategy":"PREMARKET_MOMENTUM_PAPER",
        "allocationPct":30,
        "reservePct":70,
        "portfolioBase":portfolio_base,
        "allocationCap":round(allocated_30,2),
        "used":round(open_exposure,2),
        "available":round(max(0,allocated_30-open_exposure),2)
    }
    with UNIVERSE_LOCK:
        scanner_state = dict(UNIVERSE_STATE)
    scanner_updated = scanner_state.get("updatedAt")
    scanner_age = None
    if scanner_updated:
        try:
            scanner_age = max(0, int((datetime.now(timezone.utc) - datetime.fromisoformat(
                scanner_updated.replace("Z", "+00:00")
            )).total_seconds()))
        except Exception:
            scanner_age = None
    scanner_fresh = (
        scanner_state.get("status") == "ok" and scanner_age is not None
        and scanner_age <= max(180, UNIVERSE_SCAN_INTERVAL * 3)
    )
    return {"ok": True, "configured": bool(settings), "enabled": bool(settings and settings[0].get("enabled")),
            "mode":"paper", "settings": settings[0] if settings else None,
            "account": accounts[0] if accounts else None, "openPositions": positions,
            "premarketAllocation": allocation,
            "worker": dict(STOCK_PAPER_WORKER_STATE),
            "scanner": {
                "status": scanner_state.get("status") if scanner_fresh else ("stale" if scanner_updated else "waiting_scan"),
                "updatedAt": scanner_updated,
                "staleSeconds": scanner_age,
                "fresh": scanner_fresh,
                "candidateCount": scanner_state.get("candidateCount", len(scanner_state.get("candidates") or [])),
                "fastEntryCount": scanner_state.get("fastEntryCount", 0),
                "premarketFeedReady": scanner_state.get("extendedHoursAvailable") is True,
                "extendedHoursFeedReady": scanner_state.get("extendedHoursAvailable") is True,
                "momentumSessions": {
                    "premarket": {"enabled": True, "feedReady": scanner_state.get("extendedHoursAvailable") is True},
                    "regular": {"enabled": True, "feedReady": scanner_fresh},
                    "afterHours": {"enabled": True, "feedReady": scanner_state.get("extendedHoursAvailable") is True}
                },
                "feed": scanner_state.get("feed") or scanner_state.get("provider") or "waiting",
                "coverage": scanner_state.get("coverage"),
                "lastError": scanner_state.get("lastError"),
            }}


def _stock_snapshot_from_payload(snap: dict):
    """Choose the freshest valid Alpaca IEX trade/minute price; reject stale or future data."""
    if not isinstance(snap, dict):
        return None, None
    now = datetime.now(timezone.utc)
    valid = []
    latest_stamp = None
    for field, price_key in (("latestTrade", "p"), ("minuteBar", "c")):
        item = snap.get(field) or {}
        price = _num(item.get(price_key), 0) or 0
        stamp = item.get("t")
        if not stamp:
            continue
        latest_stamp = stamp
        if price <= 0:
            continue
        try:
            parsed = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            age = (now - parsed.astimezone(timezone.utc)).total_seconds()
        except Exception:
            continue
        if -30 <= age <= 180:
            valid.append((age, price, stamp))
    if not valid:
        return None, latest_stamp
    _, price, stamp = min(valid, key=lambda item: item[0])
    return price, stamp


def _stock_snapshot_price(symbol: str):
    try:
        snapshots = _alpaca_snapshots([str(symbol).upper()])
        return _stock_snapshot_from_payload(snapshots.get(str(symbol).upper()) or {})
    except Exception as exc:
        log.warning("Stock Paper quote lookup failed for %s (%s)", symbol, type(exc).__name__)
        return None, None


def _stock_paper_worker_cycle():
    # Paper-only stock engine uses the same Alpaca IEX feed as the US-equity scanner.
    if not (ALPACA_KEY and ALPACA_SECRET):
        STOCK_PAPER_WORKER_STATE.update({"status":"waiting_credentials","lastError":"ALPACA_CREDENTIALS_MISSING"})
        return
    # Keep every active member on an isolated stock Paper account; never include removed/suspended users.
    active_member_rows = _sb_rest("profiles", {
        "select":"user_id", "role":"eq.member", "status":"eq.active", "limit":"1000"
    })
    active_member_ids = list(dict.fromkeys(str(row.get("user_id")) for row in (active_member_rows or []) if row.get("user_id")))
    if not active_member_ids:
        STOCK_PAPER_WORKER_STATE.update({"status":"running","lastCycleAt":datetime.now(timezone.utc).isoformat(),
                                         "enabledUsers":0,"buyEntries":0,"sellExits":0})
        return
    existing_rows = _sb_rest("stock_paper_settings", {
        "select":"user_id", "user_id":"in.(" + ",".join(active_member_ids) + ")", "mode":"eq.paper", "limit":"1000"
    })
    existing_ids = {str(row.get("user_id")) for row in (existing_rows or [])}
    for member_id in active_member_ids:
        if member_id not in existing_ids:
            _stock_paper_ensure_user(member_id)
            _sb_rest("stock_paper_settings", {"user_id":"eq." + member_id}, "PATCH", {
                "enabled":True, "mode":"paper", "updated_at":datetime.now(timezone.utc).isoformat()
            })
    settings_rows = _sb_rest("stock_paper_settings", {
        "select":"user_id,enabled,mode,starting_balance,order_budget,max_open_positions,take_profit_pct,stop_loss_pct,daily_loss_limit_pct,fee_pct,slippage_bps",
        "user_id":"in.(" + ",".join(active_member_ids) + ")", "mode":"eq.paper", "limit":"1000"
    })
    with UNIVERSE_LOCK:
        scanner = dict(UNIVERSE_STATE)
    updated = scanner.get("updatedAt")
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(updated.replace("Z","+00:00"))).total_seconds() if updated else 999999
    except Exception:
        age = 999999
    fresh = scanner.get("status") == "ok" and age <= max(180, UNIVERSE_SCAN_INTERVAL * 3)
    # One independent 30% Momentum sleeve trades across all US equity sessions.
    # CORE entries use the verified regular-session scanner feed. Premarket and
    # After-hours entries require an explicitly verified extended-hours entitlement.
    extended_hours_ready = scanner.get("extendedHoursAvailable") is True
    allowed_sessions = {"CORE"}
    if extended_hours_ready:
        allowed_sessions.update({"PRE_MARKET", "AFTER_HOURS"})
    candidates = {x.get("symbol"):x for x in (scanner.get("candidates") or [])
                  if x.get("symbol") and x.get("session") in allowed_sessions
                  and x.get("fastEntry") is True and _num(x.get("price"),0)}
    session_candidate_counts = {
        session: sum(1 for x in candidates.values() if x.get("session") == session)
        for session in ("PRE_MARKET", "CORE", "AFTER_HOURS")
    }
    enabled_users = sum(1 for row in settings_rows if row.get("enabled"))
    buy_entries = 0
    sell_exits = 0
    for setting in settings_rows:
        uid = str(setting["user_id"])
        positions = _sb_rest("stock_paper_positions", {
            "select":"*", "user_id":"eq." + uid, "status":"eq.open", "limit":"1000"
        })
        if not setting.get("enabled") and not positions:
            continue
        # Manage open stock positions from current IEX snapshots, independently of new entries.
        prices = {}
        if positions:
            symbols = list(dict.fromkeys(p["symbol"] for p in positions))
            for offset in range(0, len(symbols), 100):
                batch = symbols[offset:offset+100]
                try:
                    snaps = _alpaca_snapshots(batch)
                except Exception as quote_error:
                    log.warning("Stock Paper price refresh failed: %s", type(quote_error).__name__)
                    snaps = {}
                for symbol, snap in snaps.items():
                    px, quote_stamp = _stock_snapshot_from_payload(snap)
                    if px is not None:
                        prices[symbol] = px
        for pos in positions:
            price = prices.get(pos["symbol"])
            if not price:
                continue
            _sb_rpc("stock_paper_worker_trade", {
                "p_user_id":uid,"p_action":"mark","p_symbol":pos["symbol"],
                "p_price":price,"p_position_id":pos["id"],"p_reason":"MARK_TO_MARKET","p_signal_score":None
            })
            reason = "TAKE_PROFIT" if price >= float(pos["take_profit_price"]) else (
                     "STOP_LOSS" if price <= float(pos["stop_loss_price"]) else None)
            if reason:
                result = _sb_rpc("stock_paper_worker_trade", {
                    "p_user_id":uid,"p_action":"sell","p_symbol":pos["symbol"],
                    "p_price":price,"p_position_id":pos["id"],"p_reason":reason,"p_signal_score":None
                })
                sell_exits += 1
                STOCK_PAPER_WORKER_STATE["actions"].append({
                    "at":datetime.now(timezone.utc).isoformat(),"action":"sell",
                    "symbol":pos["symbol"],"reason":reason,"result":result
                })
                STOCK_PAPER_WORKER_STATE["actions"] = STOCK_PAPER_WORKER_STATE["actions"][-20:]
        if not setting.get("enabled") or not fresh:
            continue
        positions = _sb_rest("stock_paper_positions", {
            "select":"id,symbol,quantity,entry_price","user_id":"eq." + uid,
            "status":"eq.open","limit":"30"
        })
        if len(positions) >= int(setting.get("max_open_positions") or 5):
            continue
        account_rows = _sb_rest("stock_paper_accounts", {
            "select":"cash_balance,starting_balance","user_id":"eq." + uid,"limit":"1"
        })
        if not account_rows:
            continue
        account = account_rows[0]
        day_start = datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0).isoformat()
        todays = _sb_rest("stock_paper_orders", {
            "select":"side,realized_pnl","user_id":"eq." + uid,
            "created_at":"gte." + day_start,"side":"eq.sell","limit":"500"
        })
        daily_realized = sum(_num(x.get("realized_pnl"),0) or 0 for x in todays)
        open_pnl = 0.0
        for pos in positions:
            px = prices.get(pos["symbol"]) or _num((candidates.get(pos["symbol"]) or {}).get("price"),0) or 0
            if px > 0:
                open_pnl += (px-float(pos["entry_price"]))*float(pos["quantity"])
        daily_loss = daily_realized + open_pnl
        loss_limit = float(account.get("starting_balance") or setting.get("starting_balance") or 3000) * float(setting.get("daily_loss_limit_pct") or 3)/100
        if daily_loss <= -loss_limit:
            continue
        held = {p["symbol"] for p in positions}
        ranked = [x for x in candidates.values() if x["symbol"] not in held
                  and _num(x.get("spreadPct"),0) is not None
                  and 0 < float(x.get("spreadPct") or 0) <= 0.5
                  and _num(x.get("m1"),0) > 0
                  and _num(x.get("m5"),0) >= -0.10
                  and _num(x.get("momentumScore"),0) >= 50
                  and bool(x.get("momentumSetups"))]
        ranked.sort(key=lambda x:(
            float(x.get("momentumScore") or 0)
            + min(float(x.get("relativeVolume") or 0)*5,20)
            + max(0,float(x.get("m5") or 0))*2
            + max(0,float(x.get("m1") or 0))
            + min(float(x.get("dailyVolumeIex") or 0)/1000000,5)
        ),reverse=True)
        for candidate in ranked:
            if len(positions) >= int(setting.get("max_open_positions") or 5):
                break
            cooldown_key = uid + ":" + candidate["symbol"]
            if time.time() - STOCK_PAPER_ENTRY_COOLDOWNS.get(cooldown_key,0) < 180:
                continue
            try:
                result = _sb_rpc("stock_paper_worker_trade", {
                    "p_user_id":uid,"p_action":"buy","p_symbol":candidate["symbol"],
                    "p_price":float(candidate["price"]),"p_position_id":None,
                    "p_reason":"MOMENTUM_" + str(candidate.get("session") or "UNKNOWN") + "_" + "_".join(candidate.get("momentumSetups") or ["SETUP"])[:90],
                    "p_signal_score":min(100,int(candidate.get("momentumScore") or 0))
                })
                STOCK_PAPER_ENTRY_COOLDOWNS[cooldown_key] = time.time()
                buy_entries += 1
                STOCK_PAPER_WORKER_STATE["actions"].append({
                    "at":datetime.now(timezone.utc).isoformat(),"action":"buy",
                    "symbol":candidate["symbol"],"result":result
                })
                STOCK_PAPER_WORKER_STATE["actions"] = STOCK_PAPER_WORKER_STATE["actions"][-20:]
                positions.append({"symbol":candidate["symbol"]})
            except Exception as exc:
                log.warning("Stock Paper buy skipped for %s: %s",candidate["symbol"],type(exc).__name__)
    STOCK_PAPER_WORKER_STATE.update({
        "status":"running","lastCycleAt":datetime.now(timezone.utc).isoformat(),
        "lastError":None,"cycles":STOCK_PAPER_WORKER_STATE.get("cycles",0)+1,
        "scannerAgeSeconds":int(age),"scannerFresh":fresh,"candidateCount":len(candidates),
        "strategy":"PREMARKET_REGULAR_AFTER_HOURS_MOMENTUM",
        "allocationPct":30,"reservePct":70,
        "extendedHoursReady":extended_hours_ready,
        "sessionCandidateCounts":session_candidate_counts,
        "momentumSetupCounts":{
            setup:sum(1 for x in candidates.values() if setup in (x.get("momentumSetups") or []))
            for setup in ("GAP_AND_GO","VOLUME_EXPLOSION","HIGH_OF_DAY_BREAKOUT","PULLBACK_REVERSAL","NEWS_CATALYST")
        },
        "enabledUsers":enabled_users,"buyEntries":buy_entries,"sellExits":sell_exits
    })
    log.info(
        "Stock Paper worker cycle complete scanner_fresh=%s scanner_age=%ss fast_entry_candidates=%d enabled_users=%d buys=%d sells=%d",
        fresh, int(age), len(candidates), enabled_users, buy_entries, sell_exits
    )




def _stock_paper_report_user(request: Request, admin_only: bool = False):
    if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
        raise HTTPException(status_code=503, detail="Supabase ยังไม่ได้ตั้งค่าบน Render")
    authorization = request.headers.get("authorization", "")
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="กรุณาเข้าสู่ระบบก่อน")
    token = authorization.split(" ", 1)[1].strip()
    response = requests.get(
        SUPABASE_URL + "/auth/v1/user",
        headers={"apikey": SUPABASE_SERVICE_ROLE_KEY, "Authorization": "Bearer " + token},
        timeout=(5, 12),
    )
    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="เซสชันหมดอายุ กรุณาเข้าสู่ระบบใหม่")
    user = response.json()
    profiles = _sb_rest("profiles", {
        "select":"role,status", "user_id":"eq." + str(user.get("id")), "limit":"1"
    })
    if not profiles or profiles[0].get("status") != "active":
        raise HTTPException(status_code=403, detail="บัญชีไม่ได้รับอนุญาตให้ดูรายงาน")
    role = profiles[0].get("role")
    allowed = ("super_admin", "admin") if admin_only else ("super_admin", "admin", "member", "user")
    if (admin_only and role != "super_admin") or role not in allowed:
        raise HTTPException(status_code=403, detail="รายงานพอร์ตกลางอนุญาตเฉพาะ Super Admin")
    return user


def _stock_paper_member_snapshot(user_id: str):
    accounts = _sb_rest("stock_paper_accounts", {
        "select":"user_id,starting_balance,cash_balance,realized_pnl,fees_paid,status",
        "user_id":"eq." + user_id, "limit":"1"
    }) or []
    settings = _sb_rest("stock_paper_settings", {
        "select":"enabled,mode,starting_balance,order_budget,max_open_positions,take_profit_pct,stop_loss_pct,daily_loss_limit_pct",
        "user_id":"eq." + user_id, "limit":"1"
    }) or []
    positions = _sb_rest("stock_paper_positions", {
        "select":"id,symbol,quantity,entry_price,current_price,unrealized_pnl,realized_pnl,opened_at,status,take_profit_price,stop_loss_price",
        "user_id":"eq." + user_id, "order":"opened_at.desc", "limit":"1000"
    }) or []
    account = accounts[0] if accounts else {}
    setting = settings[0] if settings else {}
    opened = [p for p in positions if p.get("status") == "open"]
    closed = [p for p in positions if p.get("status") == "closed"]
    start = float(account.get("starting_balance") or setting.get("starting_balance") or 0)
    cash = float(account.get("cash_balance") or 0)
    market_value = sum(float(p.get("quantity") or 0) * float(p.get("current_price") or p.get("entry_price") or 0) for p in opened)
    unrealized = sum(float(p.get("unrealized_pnl") or 0) for p in opened)
    realized = float(account.get("realized_pnl") or sum(float(p.get("realized_pnl") or 0) for p in closed))
    pnl = realized + unrealized
    cap = start * 0.30
    exposure = sum(float(p.get("quantity") or 0) * float(p.get("entry_price") or 0) for p in opened)
    return {
        "memberId":user_id,
        "enabled":bool(setting.get("enabled")),
        "mode":"paper",
        "startingBalance":round(start,2),
        "cashBalance":round(cash,2),
        "marketValue":round(market_value,2),
        "equity":round(cash+market_value,2),
        "realizedPnl":round(realized,2),
        "unrealizedPnl":round(unrealized,2),
        "totalPnl":round(pnl,2),
        "returnPct":round((pnl/start*100) if start else 0,2),
        "momentumAllocationCap":round(cap,2),
        "allocationUsed":round(exposure,2),
        "allocationUsedPct":round((exposure/cap*100) if cap else 0,2),
        "allocationReturnPct":round((pnl/cap*100) if cap else 0,2),
        "openPositionCount":len(opened),
        "closedTradeCount":len(closed),
        "feesPaid":round(float(account.get("fees_paid") or 0),2),
        "openPositions":[{
            "symbol":p.get("symbol"),"quantity":float(p.get("quantity") or 0),
            "entryPrice":float(p.get("entry_price") or 0),
            "currentPrice":float(p.get("current_price") or 0),
            "marketValue":round(float(p.get("quantity") or 0)*float(p.get("current_price") or p.get("entry_price") or 0),2),
            "unrealizedPnl":round(float(p.get("unrealized_pnl") or 0),2),
            "returnPct":round((float(p.get("unrealized_pnl") or 0)/(float(p.get("quantity") or 0)*float(p.get("entry_price") or 0))*100) if float(p.get("quantity") or 0)*float(p.get("entry_price") or 0) else 0,2),
            "takeProfitPrice":p.get("take_profit_price"),"stopLossPrice":p.get("stop_loss_price"),
            "openedAt":p.get("opened_at")
        } for p in opened],
        "strategy":"Momentum 5 Setups",
        "allocationPct":30,
        "reservePct":70
    }


@app.get("/stock-paper-momentum/report")
def stock_paper_momentum_member_report(request: Request):
    """Member's own Momentum Paper report only; no other member data is returned."""
    user = _stock_paper_report_user(request, admin_only=False)
    return {
        "ok":True,"reportType":"member","generatedAt":datetime.now(timezone.utc).isoformat(),
        "strategy":"Momentum 5 Setups","currency":"USD",
        "portfolio":_stock_paper_member_snapshot(str(user["id"]))
    }


@app.get("/admin/stock-paper-momentum/report")
def stock_paper_momentum_central_report(request: Request):
    """Super-admin-only aggregate report across all member Momentum Paper portfolios."""
    user = _stock_paper_report_user(request, admin_only=True)
    accounts = _sb_rest("stock_paper_accounts", {
        "select":"user_id","limit":"1000"
    }) or []
    member_ids = list(dict.fromkeys(str(x.get("user_id")) for x in accounts if x.get("user_id")))
    members = []
    for uid in member_ids:
        profile = _sb_rest("profiles", {
            "select":"user_id,display_name,role,status", "user_id":"eq." + uid, "limit":"1"
        }) or []
        if not profile or profile[0].get("role") != "member" or profile[0].get("status") != "active":
            continue
        snapshot = _stock_paper_member_snapshot(uid)
        snapshot["memberLabel"] = profile[0].get("display_name") or ("สมาชิก " + uid[:8])
        members.append(snapshot)
    totals = {}
    for key in ("startingBalance","cashBalance","marketValue","equity","realizedPnl","unrealizedPnl",
                "totalPnl","momentumAllocationCap","allocationUsed","feesPaid"):
        totals[key] = round(sum(float(m.get(key) or 0) for m in members),2)
    totals["returnPct"] = round(totals["totalPnl"]/totals["startingBalance"]*100,2) if totals["startingBalance"] else 0
    totals["allocationReturnPct"] = round(totals["totalPnl"]/totals["momentumAllocationCap"]*100,2) if totals["momentumAllocationCap"] else 0
    totals["allocationUsedPct"] = round(totals["allocationUsed"]/totals["momentumAllocationCap"]*100,2) if totals["momentumAllocationCap"] else 0
    totals["openPositionCount"] = sum(int(m.get("openPositionCount") or 0) for m in members)
    totals["closedTradeCount"] = sum(int(m.get("closedTradeCount") or 0) for m in members)
    return {
        "ok":True,"reportType":"central_aggregate","generatedAt":datetime.now(timezone.utc).isoformat(),
        "strategy":"Momentum 5 Setups","currency":"USD",
        "policy":{"allocationPct":30,"reservePct":70,"mode":"paper_only"},
        "totals":totals,"members":members,
        "openPositions":[{"memberId":m["memberId"],**p} for m in members for p in m["openPositions"]]
    }


@app.get("/paper-reports/{market}")
def paper_trading_report(market: str, request: Request, period: str = "30d"):
    """Authenticated, read-only performance report for one isolated paper market."""
    user = _paper_auth_user(request)
    uid = str(user["id"])
    market_key = market.lower()
    if market_key not in ("crypto", "stocks"):
        raise HTTPException(status_code=404, detail="UNKNOWN_PAPER_MARKET")
    if period not in ("24h", "7d", "30d", "all"):
        raise HTTPException(status_code=400, detail="INVALID_REPORT_PERIOD")

    prefix = "paper_bot" if market_key == "crypto" else "stock_paper"
    settings_rows = _sb_rest(prefix + "_settings", {
        "select":"enabled,mode,starting_balance,order_budget,max_open_positions,take_profit_pct,stop_loss_pct,daily_loss_limit_pct,updated_at",
        "user_id":"eq." + uid, "limit":"1"
    })
    account_select = ("starting_balance,cash_balance,realized_pnl,fees_paid,status,starting_balance_thb,initial_fx_rate"
                      if market_key == "crypto" else
                      "starting_balance,cash_balance,realized_pnl,fees_paid,status")
    account_rows = _sb_rest(prefix + "_accounts", {
        "select":account_select, "user_id":"eq." + uid, "limit":"1"
    })
    positions = _sb_rest(prefix + "_positions", {
        "select":"id,symbol,position_side,quantity,entry_price,current_price,entry_fee,exit_fee,realized_pnl,unrealized_pnl,status,opened_at,closed_at,close_reason,signal_score",
        "user_id":"eq." + uid, "order":"opened_at.desc", "limit":"1000"
    })
    orders = _sb_rest(prefix + "_orders", {
        "select":"id,position_id,symbol,side,quantity,price,notional,fee,status,reason,realized_pnl,created_at",
        "user_id":"eq." + uid, "order":"created_at.desc", "limit":"1000"
    })
    positions = positions or []
    orders = orders or []
    account = account_rows[0] if account_rows else {}
    setting = settings_rows[0] if settings_rows else {}

    now = datetime.now(timezone.utc)
    span_seconds = {"24h":86400, "7d":7*86400, "30d":30*86400}.get(period)
    cutoff = now.timestamp() - span_seconds if span_seconds else 0
    def in_period(value):
        if not span_seconds:
            return True
        try:
            return datetime.fromisoformat(str(value).replace("Z","+00:00")).timestamp() >= cutoff
        except Exception:
            return False

    closed = [p for p in positions if p.get("status") == "closed"]
    opened = [p for p in positions if p.get("status") == "open"]
    closed_period = [p for p in closed if in_period(p.get("closed_at"))]
    open_pnl = sum(float(p.get("unrealized_pnl") or 0) for p in opened)
    realized_total = sum(float(p.get("realized_pnl") or 0) for p in closed)
    realized_period = sum(float(p.get("realized_pnl") or 0) for p in closed_period)
    winning = [p for p in closed_period if float(p.get("realized_pnl") or 0) > 0]
    losing = [p for p in closed_period if float(p.get("realized_pnl") or 0) < 0]
    gross_profit = sum(float(p.get("realized_pnl") or 0) for p in winning)
    gross_loss = abs(sum(float(p.get("realized_pnl") or 0) for p in losing))
    win_rate = (len(winning) / len(closed_period) * 100) if closed_period else 0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (None if gross_profit <= 0 else "infinite")
    market_value = sum(float(p.get("quantity") or 0) * float(p.get("current_price") or p.get("entry_price") or 0) for p in opened)
    cash = float(account.get("cash_balance") or 0)
    equity = cash + market_value
    start_balance = float(account.get("starting_balance") or setting.get("starting_balance") or 0)
    recent_orders = [o for o in orders if in_period(o.get("created_at"))]
    buys = sum(1 for o in recent_orders if o.get("side") == "buy" and o.get("status") == "filled")
    sells = sum(1 for o in recent_orders if o.get("side") == "sell" and o.get("status") == "filled")
    fees_period = sum(float(o.get("fee") or 0) for o in recent_orders)
    daily = {}
    for p in closed_period:
        stamp = p.get("closed_at") or p.get("opened_at")
        try:
            day = datetime.fromisoformat(str(stamp).replace("Z","+00:00")).astimezone(timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            day = "unknown"
        daily[day] = daily.get(day, 0) + float(p.get("realized_pnl") or 0)
    daily_series = [{"date":k,"realizedPnl":round(v,8)} for k,v in sorted(daily.items())]
    report = {
        "ok": True, "market":market_key, "currency":"USDT" if market_key == "crypto" else "USD",
        "period":period, "generatedAt":now.isoformat(), "source":"paper trading database",
        "engine":{"enabled":bool(setting.get("enabled")),"mode":"paper"},
        "account":{
            "startingBalance":start_balance, "cashBalance":cash,
            "marketValue":market_value, "equity":equity,
            "realizedPnlAllTime":realized_total, "unrealizedPnl":open_pnl,
            "totalPnl":realized_total+open_pnl, "feesPaidAllTime":float(account.get("fees_paid") or 0),
            "returnPct":((realized_total+open_pnl)/start_balance*100) if start_balance else 0
        },
        "performance":{
            "closedTrades":len(closed_period), "wins":len(winning), "losses":len(losing),
            "winRatePct":win_rate, "grossProfit":gross_profit, "grossLoss":gross_loss,
            "profitFactor":profit_factor, "realizedPnl":realized_period,
            "unrealizedPnl":open_pnl, "fees":fees_period,
            "buyOrders":buys, "sellOrders":sells, "openPositions":len(opened)
        },
        "dailySeries":daily_series,
        "openPositions":opened[:100],
        "closedTrades":closed_period[:100],
        "recentOrders":recent_orders[:100],
        "settings":setting,
        "limits":{"positionsReturned":min(len(opened),100),"tradesReturned":min(len(closed_period),100),
                  "ordersReturned":min(len(recent_orders),100),"databaseRowsScanned":len(positions)+len(orders)}
    }
    return report




DUAL_PAPER_PORTFOLIOS = {
    "crypto": {"market":"crypto","broker":"binance_th","currency":"USDT","initial_capital":100},
    "stocks": {"market":"stocks","broker":"webull","currency":"USD","initial_capital":100},
}


def _dual_paper_ensure_portfolio(user_id: str, config: dict) -> dict:
    """Create a fresh isolated $100 Paper portfolio only when it does not exist.

    Existing legacy paper accounts are never edited, reset, or merged.
    """
    rows = _sb_rest("blessing_dual_portfolios", {
        "select":"id,user_id,market,broker,currency,initial_capital,cash_balance,realized_pnl,fees_paid,mode,enabled,reserve_cash_pct,risk_per_trade_pct,max_daily_loss_pct,max_weekly_loss_pct,max_drawdown_pct,max_open_positions,peak_equity,created_at,updated_at",
        "user_id":"eq." + user_id, "market":"eq." + config["market"], "limit":"1"
    })
    if rows:
        return rows[0]
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "user_id":user_id, "market":config["market"], "broker":config["broker"],
        "currency":config["currency"], "initial_capital":config["initial_capital"],
        "cash_balance":config["initial_capital"], "realized_pnl":0, "fees_paid":0,
        "mode":"paper", "enabled":False,
        "reserve_cash_pct":20 if config["market"] == "crypto" else 0,
        "risk_per_trade_pct":0.5 if config["market"] == "crypto" else 1,
        "max_daily_loss_pct":2 if config["market"] == "crypto" else 3,
        "max_weekly_loss_pct":6,
        "max_drawdown_pct":5 if config["market"] == "crypto" else 10,
        "max_open_positions":3 if config["market"] == "crypto" else 2147483647,
        "peak_equity":config["initial_capital"], "updated_at":now
    }
    try:
        inserted = _sb_rest("blessing_dual_portfolios", {}, "POST", payload)
        if inserted:
            return inserted[0]
    except requests.HTTPError:
        # Another request may have created this unique (user_id, market) row.
        rows = _sb_rest("blessing_dual_portfolios", {
            "select":"id,user_id,market,broker,currency,initial_capital,cash_balance,realized_pnl,fees_paid,mode,enabled,reserve_cash_pct,risk_per_trade_pct,max_daily_loss_pct,max_weekly_loss_pct,max_drawdown_pct,max_open_positions,peak_equity,created_at,updated_at",
            "user_id":"eq." + user_id, "market":"eq." + config["market"], "limit":"1"
        })
        if rows:
            return rows[0]
        raise
    rows = _sb_rest("blessing_dual_portfolios", {
        "select":"id,user_id,market,broker,currency,initial_capital,cash_balance,realized_pnl,fees_paid,mode,enabled,reserve_cash_pct,risk_per_trade_pct,max_daily_loss_pct,max_weekly_loss_pct,max_drawdown_pct,max_open_positions,peak_equity,created_at,updated_at",
        "user_id":"eq." + user_id, "market":"eq." + config["market"], "limit":"1"
    })
    if not rows:
        raise HTTPException(status_code=503, detail="สร้างบัญชี Paper แยกพอร์ตไม่สำเร็จ")
    return rows[0]


@app.post("/ai/dual-paper/initialize")
def dual_paper_initialize(request: Request):
    """Create two separate $100-equivalent Paper portfolios; never touch legacy accounts."""
    user = _paper_auth_user(request)
    uid = str(user["id"])
    portfolios = {
        market: _dual_paper_ensure_portfolio(uid, config)
        for market, config in DUAL_PAPER_PORTFOLIOS.items()
    }
    return {
        "ok":True, "mode":"paper", "liveOrdersEnabled":False,
        "crossPortfolioTransfersEnabled":False,
        "portfolios":{
            market:{
                "portfolioId":row["id"], "market":row["market"], "broker":row["broker"],
                "currency":row["currency"], "initialCapital":float(row["initial_capital"]),
                "cashBalance":float(row["cash_balance"]), "enabled":bool(row["enabled"]),
                "mode":row["mode"]
            } for market,row in portfolios.items()
        },
        "message":"สร้างพอร์ตจำลองแยกกันแล้ว; ไม่มีการแก้ไขบัญชี Paper เดิม และไม่มีคำสั่งซื้อขายจริง"
    }


@app.get("/ai/dual-paper/status")
def dual_paper_status(request: Request):
    """Admin-only status for isolated new dual portfolios, including independent PnL."""
    user = _paper_auth_user(request)
    uid = str(user["id"])
    result = {}
    for market, config in DUAL_PAPER_PORTFOLIOS.items():
        rows = _sb_rest("blessing_dual_portfolios", {
            "select":"id,market,broker,currency,initial_capital,cash_balance,realized_pnl,fees_paid,mode,enabled,reserve_cash_pct,risk_per_trade_pct,max_daily_loss_pct,max_weekly_loss_pct,max_drawdown_pct,max_open_positions,peak_equity,created_at",
            "user_id":"eq." + uid, "market":"eq." + market, "limit":"1"
        })
        if not rows:
            result[market] = {"configured":False,"currency":config["currency"],"targetCapital":100,"mode":"paper","liveOrdersEnabled":False}
            continue
        row = rows[0]
        positions = _sb_rest("blessing_dual_positions", {
            "select":"id,symbol,side,quantity,entry_price,current_price,stop_loss_price,take_profit_price,entry_fee,unrealized_pnl,status,opened_at",
            "user_id":"eq." + uid, "portfolio_id":"eq." + row["id"], "status":"eq.open",
            "order":"opened_at.desc","limit":"100"
        }) or []
        orders = _sb_rest("blessing_dual_orders", {
            "select":"id,symbol,side,quantity,price,notional,fee,status,realized_pnl,created_at",
            "user_id":"eq." + uid, "portfolio_id":"eq." + row["id"],
            "order":"created_at.desc","limit":"100"
        }) or []
        market_value = sum((float(p.get("entry_price") or 0) * float(p.get("quantity") or 0) + float(p.get("unrealized_pnl") or 0) + float(p.get("entry_fee") or 0)) if p.get("side") == "short" else float(p.get("quantity") or 0) * float(p.get("current_price") or p.get("entry_price") or 0) for p in positions)
        cash = float(row["cash_balance"])
        realized = float(row["realized_pnl"])
        unrealized = sum(float(p.get("unrealized_pnl") or 0) for p in positions)
        result[market] = {
            "configured":True,"portfolioId":row["id"],"broker":row["broker"],
            "currency":row["currency"],"initialCapital":float(row["initial_capital"]),
            "cashBalance":cash,"marketValue":market_value,"equity":cash+market_value,
            "realizedPnl":realized,"unrealizedPnl":unrealized,
            "feesPaid":float(row["fees_paid"]),"totalPnl":realized+unrealized,
            "mode":row["mode"],"enabled":bool(row["enabled"]),
            "riskLimits":{"maxPositionLossPct":25,"maxTotalAllocationPct":100,
                "reserveCashPct":0,"dailyLossLimitEnabled":False,
                "weeklyLossLimitEnabled":False,"cumulativeLossLimitEnabled":False,
                "drawdownStopEnabled":False,"maxOpenPositions":5 if market == "stocks" else None},
            "openPositions":positions,"recentOrders":orders,
            "tradingRules":({"stopLossPct":25,"takeProfitMinPct":5,"takeProfitMaxPct":15,
                             "maxOpenPositions":5,"maxTotalAllocationPct":100,"automaticEntry":True,"stopByUserOnly":True,
                             "feePctPerSide":0.10,"slippageBpsPerSide":5,
                             "source":"server_enforced"} if market == "stocks" else {"source":"per_order"}),
            "liveOrdersEnabled":False
        }
    return {"ok":True,"mode":"paper","liveOrdersEnabled":False,
            "crossPortfolioTransfersEnabled":False,"portfolios":result,
            "worker":dict(DUAL_PAPER_WORKER_STATE)}





# A full 420-symbol candle scan can take about a minute; keep one bounded cycle per 90s.
DUAL_PAPER_WORKER_INTERVAL = max(60, int(os.getenv("DUAL_PAPER_WORKER_INTERVAL", "90")))
DUAL_ADAPTIVE_ENTRY_COOLDOWNS = {}  # per-user cooldown shared across crypto and equities
DUAL_PAPER_WORKER_STATE = {
    "status": "waiting_config" if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY) else "starting",
    "lastCycleAt": None, "lastCycleStartedAt": None, "lastError": None, "cycles": 0,
    "positionsChecked": 0, "automaticExits": 0, "cryptoCandidates": 0, "stocksCandidates": 0,
    "cryptoScanDiagnostics": {}, "entrySkips": [], "entriesOpened": 0
}
DUAL_PAPER_WORKER_THREAD = None
DUAL_PAPER_WORKER_LOCK = threading.Lock()
DUAL_STOCK_BUY_LOCKS = {}
DUAL_STOCK_BUY_LOCKS_GUARD = threading.Lock()


def _dual_stock_buy_lock(user_id: str):
    with DUAL_STOCK_BUY_LOCKS_GUARD:
        return DUAL_STOCK_BUY_LOCKS.setdefault(str(user_id), threading.Lock())





def _binance_th_adaptive_candidates():
    """Build candidates from Binance TH's live WebSocket history; fail closed on stale/incomplete data."""
    with CRYPTO_TICKER_HISTORY_LOCK:
        history = {symbol: list(rows) for symbol, rows in CRYPTO_TICKER_HISTORY.items()}
    output = build_binance_th_candidates(history)
    return [{
        **row, "symbol": str(row.get("symbol") or "").upper(),
        "price": float(row.get("price") or 0),
        "source": "Binance TH live WebSocket · 60-minute history",
        "relativeVolume": row.get("rvol"),
    } for row in output if str(row.get("symbol") or "").upper().endswith("USDT")
       and float(row.get("price") or 0) > 0]

def _dual_equity_snapshot(experiment: dict, portfolio: dict, positions: list):
    """Persist a net-of-fees-and-estimated-slippage equity point for the 14-day run."""
    market = portfolio["market"]
    user_id = str(portfolio["user_id"])
    cash = float(portfolio.get("cash_balance") or 0)
    market_value = sum(float(p.get("quantity") or 0) * float(p.get("current_price") or p.get("entry_price") or 0)
                       for p in positions)
    equity = cash + market_value
    orders = _sb_rest("blessing_dual_orders", {
        "select":"notional", "portfolio_id":"eq." + str(portfolio["id"]),
        "status":"eq.filled", "limit":"5000"
    }) or []
    estimated_slippage = sum(float(o.get("notional") or 0) * 0.0005 for o in orders)
    fees_paid = float(portfolio.get("fees_paid") or 0)
    initial = float(portfolio.get("initial_capital") or 0)
    net_equity = equity - estimated_slippage
    previous = _sb_rest("blessing_dual_equity_marks", {
        "select":"peak_equity,marked_at", "experiment_id":"eq." + str(experiment["id"]),
        "market":"eq." + market, "order":"marked_at.desc", "limit":"1"
    }) or []
    if previous:
        prev_at = datetime.fromisoformat(str(previous[0]["marked_at"]).replace("Z","+00:00"))
        if (datetime.now(timezone.utc)-prev_at).total_seconds() < 60:
            return None
        peak = max(float(previous[0].get("peak_equity") or initial), net_equity)
    else:
        peak = max(initial, net_equity)
    drawdown = max(0.0, (peak-net_equity)/peak*100) if peak > 0 else 0.0
    net_pnl = net_equity - initial
    mark = {
        "experiment_id":experiment["id"],"user_id":user_id,"market":market,
        "cash_balance":cash,"market_value":market_value,"equity":equity,
        "realized_pnl":float(portfolio.get("realized_pnl") or 0),
        "unrealized_pnl":sum(float(p.get("unrealized_pnl") or 0) for p in positions),
        "fees_paid":fees_paid,"estimated_slippage":estimated_slippage,
        "net_pnl_after_costs":net_pnl,
        "net_return_pct":(net_pnl/initial*100) if initial else 0,
        "peak_equity":peak,"drawdown_pct":drawdown
    }
    _sb_rest("blessing_dual_equity_marks", {}, "POST", mark)
    # Daily, cumulative-loss and drawdown stop limits are disabled by owner policy.
    return mark


def _dual_try_adaptive_entry(experiment: dict, portfolio: dict, candidate: dict, market: str):
    user_id = str(portfolio["user_id"])
    symbol = str(candidate.get("symbol") or "").upper()
    if not symbol:
        return None
    direction = str(candidate.get("side") or "long").lower() if market == "crypto" else "long"
    if direction != "long":
        return None
    try:
        market_price = _dual_market_price(market, symbol)
    except Exception as exc:
        log.info("Dual Paper entry skipped %s/%s: price unavailable (%s: %s)", market, symbol, type(exc).__name__, getattr(exc, "detail", str(exc))[:160])
        return None

    positions = _sb_rest("blessing_dual_positions", {
        "select":"id,symbol,side,quantity,entry_price,current_price,stop_loss_price,entry_fee,unrealized_pnl",
        "portfolio_id":"eq."+str(portfolio["id"]), "user_id":"eq."+user_id,
        "status":"eq.open", "limit":"1000"
    }) or []
    # Hard cap at three concurrent positions per member/market, while respecting
    # any stricter portfolio setting already stored in the database.
    max_open_positions = max(1, min(3, int(portfolio.get("max_open_positions") or 3)))
    if len(positions) >= max_open_positions:
        return None
    position_value = sum(
        (float(p.get("entry_price") or 0) * float(p.get("quantity") or 0)
         + float(p.get("unrealized_pnl") or 0) + float(p.get("entry_fee") or 0))
        if p.get("side") == "short"
        else float(p.get("quantity") or 0) * float(p.get("current_price") or p.get("entry_price") or 0)
        for p in positions
    )
    equity = float(portfolio.get("cash_balance") or 0) + position_value
    if any(str(p.get("symbol") or "").upper() == symbol for p in positions):
        return None

    # Enforce a 2% maximum daily loss using Bangkok calendar days. Include both
    # closed P&L today and the current unrealized P&L before allowing another entry.
    bangkok_now = datetime.now(ZoneInfo("Asia/Bangkok"))
    day_start_utc = bangkok_now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()
    closed_today = _sb_rest("blessing_dual_positions", {
        "select":"realized_pnl", "portfolio_id":"eq."+str(portfolio["id"]),
        "user_id":"eq."+user_id, "status":"eq.closed",
        "closed_at":"gte."+day_start_utc, "limit":"1000"
    }) or []
    realized_today = sum(float(row.get("realized_pnl") or 0) for row in closed_today)
    unrealized_now = sum(float(p.get("unrealized_pnl") or 0) for p in positions)
    daily_pnl = realized_today + unrealized_now
    configured_daily_limit = float(portfolio.get("max_daily_loss_pct") or 2.0)
    daily_loss_limit_pct = max(0.25, min(2.0, configured_daily_limit))
    if equity > 0 and daily_pnl <= -(equity * daily_loss_limit_pct / 100.0):
        return None

    strategy_config = experiment.get("config") if isinstance(experiment.get("config"), dict) else {}
    if market == "stocks":
        stop_pct = 25.0
        target_pct = float(strategy_config.get("stocks_take_profit_pct", 20.0))
        if not 5.0 <= target_pct <= 30.0:
            target_pct = 20.0
    else:
        stop_pct = float(candidate.get("atrStopPct") or strategy_config.get("crypto_stop_loss_pct", 1.5))
        stop_pct = min(2.0, max(0.8, stop_pct))
        # Long-only target: at least +3% or 2R, capped at +5%.
        target_pct = min(5.0, max(3.0, stop_pct * 2.0))

    all_portfolios = _sb_rest("blessing_dual_portfolios", {
        "select":"id,market,cash_balance,initial_capital",
        "user_id":"eq."+user_id,"mode":"eq.paper","limit":"2"
    }) or []
    portfolio_ids = [str(row["id"]) for row in all_portfolios]
    all_positions = _sb_rest("blessing_dual_positions", {
        "select":"portfolio_id,side,quantity,current_price,entry_price,entry_fee,unrealized_pnl",
        "user_id":"eq."+user_id,"status":"eq.open","limit":"1000"
    }) or []
    cash_balance = float(portfolio.get("cash_balance") or 0)
    total_cash = sum(float(row.get("cash_balance") or 0) for row in all_portfolios)
    total_exposure = sum(
        (float(p.get("entry_price") or 0) * float(p.get("quantity") or 0)
         + float(p.get("unrealized_pnl") or 0) + float(p.get("entry_fee") or 0))
        if p.get("side") == "short"
        else float(p.get("quantity") or 0) * float(p.get("current_price") or p.get("entry_price") or 0)
        for p in all_positions if str(p.get("portfolio_id")) in portfolio_ids
    )
    total_equity = max(0.0, total_cash + total_exposure)
    remaining_allocation = max(0.0, total_equity - total_exposure)
    reserve_pct = max(20.0, float(portfolio.get("reserve_cash_pct") or 0.0)) if market == "crypto" else float(portfolio.get("reserve_cash_pct") or 0.0)
    deployable_cash = max(0.0, cash_balance - equity * reserve_pct / 100.0)
    if market == "crypto":
        # Risk 0.5% of equity per trade; total open stop risk may not exceed 1.5%.
        open_risk = sum(
            abs(float(p.get("entry_price") or 0) - float(p.get("stop_loss_price") or p.get("entry_price") or 0))
            * float(p.get("quantity") or 0) for p in positions
        )
        aggregate_risk_remaining = max(0.0, equity * 0.015 - open_risk)
        risk_budget = min(equity * 0.005, aggregate_risk_remaining)
        if risk_budget <= 0:
            return None
        risk_sized_notional = risk_budget / (stop_pct / 100.0)
        notional = min(deployable_cash, remaining_allocation, risk_sized_notional, total_equity)
    else:
        notional = min(cash_balance, remaining_allocation, total_equity)
    if notional < 1.0:
        return None

    is_short = False
    fill_price = market_price * 1.0005
    quantity = round(notional / fill_price, 10 if market == "crypto" else 6)
    if quantity <= 0:
        return None
    stop_price = fill_price * (1 - stop_pct / 100)
    target_price = fill_price * (1 + target_pct / 100)
    order_side = "buy"

    result = _dual_rpc_order({
        "p_user_id":user_id,"p_market":market,"p_symbol":symbol,"p_side":order_side,
        "p_quantity":quantity,"p_price":fill_price,"p_stop_loss_price":stop_price,
        "p_take_profit_price":target_price,"p_position_id":None,
        "p_idempotency_key":"ADAPT14-"+str(experiment["id"])[:8]+"-"+market+"-"+symbol+"-"+direction+"-"+str(int(time.time()//60))
    })
    DUAL_PAPER_WORKER_STATE.setdefault("actions",[]).append({
        "at":datetime.now(timezone.utc).isoformat(),"action":order_side,"direction":direction,
        "market":market,"symbol":symbol,"strategy":candidate.get("strategy"),"result":result
    })
    DUAL_PAPER_WORKER_STATE["actions"]=DUAL_PAPER_WORKER_STATE["actions"][-30:]
    return result


def _dual_paper_worker_cycle():
    if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
        DUAL_PAPER_WORKER_STATE.update({"status":"waiting_config","lastError":"SUPABASE_CONFIG_MISSING"})
        return
    if not DUAL_PAPER_WORKER_LOCK.acquire(blocking=False):
        return
    try:
        DUAL_PAPER_WORKER_STATE["lastCycleStartedAt"] = datetime.now(timezone.utc).isoformat()
        # Always manage existing positions, even if an experiment is stopped.
        open_rows = _sb_rest("blessing_dual_positions", {
            "select":"id,user_id,market,symbol","status":"eq.open","order":"opened_at.asc","limit":"1000"
        }) or []
        checked, exits = 0, 0
        for position in open_rows:
            try:
                price = _dual_market_price(position["market"],position["symbol"])
                result = _sb_rpc("blessing_dual_mark_position",{
                    "p_user_id":str(position["user_id"]),"p_market":position["market"],
                    "p_position_id":str(position["id"]),"p_price":price
                })
                checked += 1
                if isinstance(result,dict) and result.get("closed"):
                    exits += 1
            except Exception as exc:
                log.info("Dual paper monitor skipped %s/%s (%s: %s)",position.get("market"),position.get("symbol"),type(exc).__name__,getattr(exc,"detail",str(exc))[:180])

        now = datetime.now(timezone.utc)
        experiments = _sb_rest("blessing_dual_experiments",{
            "select":"id,user_id,started_at,ends_at,status,markets,config",
            "status":"eq.active","limit":"100"
        }) or []

        # Initialize cycle counters before either market loop. Crypto entries run first.
        entries_opened = 0
        selected_markets = {}
        crypto_candidates = {}
        crypto_scan = altcoin_scanner(0)
        try:
            crypto_age = (now - datetime.fromisoformat(str(crypto_scan.get("updatedAt")).replace("Z","+00:00"))).total_seconds()
        except Exception:
            crypto_age = 999999
        crypto_fresh = (
            crypto_scan.get("status") == "ok"
            and crypto_scan.get("provider") == "Binance TH Spot API"
            and 0 <= crypto_age <= 90
        )
        DUAL_PAPER_WORKER_STATE["cryptoScanDiagnostics"] = {
            "status": crypto_scan.get("status"), "provider": crypto_scan.get("provider"),
            "updatedAt": crypto_scan.get("updatedAt"), "ageSeconds": round(crypto_age, 1),
            "evaluated": int(crypto_scan.get("evaluated") or 0),
            "paperStrategyCandidateCount": int(crypto_scan.get("paperStrategyCandidateCount") or 0),
            "opportunityEntryCount": int(crypto_scan.get("opportunityEntryCount") or 0),
            "signalDiagnostics": crypto_scan.get("signalDiagnostics") or {},
            "paperStrategyCounts": crypto_scan.get("paperStrategyCounts") or {},
            "fresh": bool(crypto_fresh)
        }
        if crypto_fresh:
            btc_row = next((item for item in (crypto_scan.get("candidates") or []) if item.get("symbol") == "BTCUSDT"), {})
            btc_m15 = _num(btc_row.get("m15"), 0) or 0
            btc_m60 = _num(btc_row.get("m60"), 0) or 0
            if btc_row.get("trendDownAligned") is True and btc_m15 < -0.08 and btc_m60 < 0:
                btc_market_bias = "bearish"
            elif btc_row.get("trendAligned") is True and btc_m15 > 0.08 and btc_m60 > 0:
                btc_market_bias = "bullish"
            else:
                btc_market_bias = "neutral"
            DUAL_PAPER_WORKER_STATE["cryptoScanDiagnostics"]["btcMarketBias"] = btc_market_bias
            for row in (crypto_scan.get("candidates") or []):
                row_context = {**row, "btcMarketBias": btc_market_bias}
                decision = _crypto_three_strategy_decision(row_context)
                if decision:
                    crypto_candidates[row["symbol"]] = (row_context, decision)
        with UNIVERSE_LOCK:
            stock_scan = dict(UNIVERSE_STATE)
        try:
            stock_age = (now-datetime.fromisoformat(str(stock_scan.get("updatedAt")).replace("Z","+00:00"))).total_seconds()
        except Exception:
            stock_age = 999999
        stock_fresh = stock_scan.get("status")=="ok" and stock_age <= max(180,UNIVERSE_SCAN_INTERVAL*3)
        stock_candidates = {}
        if stock_fresh:
            for row in (stock_scan.get("candidates") or []):
                if row.get("symbol") and _num(row.get("price"),0):
                    normalized=dict(row); normalized["rvol"]=normalized.get("relativeVolume")
                    decision=adaptive_decision("stocks",normalized,fee_bps=10,slippage_bps=5)
                    if decision["decision"]=="PAPER_CANDIDATE":
                        stock_candidates[row["symbol"]]=(row,decision)
        DUAL_PAPER_WORKER_STATE["stocksScanDiagnostics"] = {
            "status":stock_scan.get("status"), "updatedAt":stock_scan.get("updatedAt"),
            "ageSeconds":round(stock_age,1), "fresh":bool(stock_fresh),
            "provider":stock_scan.get("provider"), "lastError":stock_scan.get("lastError"),
            "webullConnection":state.get("webullConnection"),
            "webullConfigured":bool(APP_KEY and APP_SECRET),
            "alpacaConfigured":bool(ALPACA_KEY and ALPACA_SECRET),
            "evaluated":len(stock_scan.get("candidates") or []),
            "paperStrategyCandidateCount":len(stock_candidates)
        }
        # All enabled member portfolios run independently; no experiment-row gate.
            # Crypto Paper: every enabled member portfolio runs automatically without an experiment gate.
        enabled_crypto_portfolios = _sb_rest("blessing_dual_portfolios", {
            "select":"id,user_id,market,initial_capital,cash_balance,realized_pnl,fees_paid,mode,enabled,reserve_cash_pct,risk_per_trade_pct,max_daily_loss_pct,max_weekly_loss_pct,max_drawdown_pct,max_open_positions,peak_equity",
            "market":"eq.crypto","mode":"eq.paper","enabled":"eq.true","limit":"1000"
        }) or []
        if crypto_fresh and crypto_candidates:
            ranked_crypto_candidates = sorted(crypto_candidates.items(),
                key=lambda item: float(item[1][1].get("opportunityScore") or 0), reverse=True)
            for portfolio in enabled_crypto_portfolios:
                uid = str(portfolio["user_id"])
                crypto_experiment = {"id":"AUTO-CRYPTO", "config":{"crypto_stop_loss_pct":1.5,
                    "crypto_take_profit_pct":3,"max_notional_pct":100,"max_total_allocation_pct":100}}
                for symbol,(raw,decision) in ranked_crypto_candidates:
                    cooldown_key = uid + ":crypto:" + symbol
                    if time.time() - DUAL_ADAPTIVE_ENTRY_COOLDOWNS.get(cooldown_key, 0) < 300:
                        continue
                    try:
                        result = _dual_try_adaptive_entry(crypto_experiment, portfolio, {**raw, **decision}, "crypto")
                        if result:
                            entries_opened += 1
                            selected_markets.setdefault(uid, {}).setdefault("crypto", []).append({
                                "symbol":symbol,"opportunityScore":float(decision.get("opportunityScore") or 0),
                                "netEdgeProxyPct":decision.get("netEdgeProxyPct")})
                            DUAL_ADAPTIVE_ENTRY_COOLDOWNS[cooldown_key] = time.time()
                    except Exception as exc:
                        log.info("Automatic crypto entry skipped %s (%s) detail=%s", symbol, type(exc).__name__, str(getattr(exc, "detail", None) or exc)[:180])

        # Stock Paper automation is controlled only by the user's portfolio START/STOP button.
        # The worker keeps scanning while enabled, enters only qualifying candidates, and
        # always monitors existing positions above even after STOP is pressed.
        if stock_fresh and stock_candidates:
            enabled_stock_portfolios = _sb_rest("blessing_dual_portfolios", {
                "select":"id,user_id,market,initial_capital,cash_balance,realized_pnl,fees_paid,mode,enabled,reserve_cash_pct,risk_per_trade_pct,max_daily_loss_pct,max_weekly_loss_pct,max_drawdown_pct,max_open_positions,peak_equity",
                "market":"eq.stocks","mode":"eq.paper","enabled":"eq.true","limit":"1000"
            }) or []
            for portfolio in enabled_stock_portfolios:
                uid = str(portfolio["user_id"])
                stock_experiment = {"id":"AUTO-STOCK", "config":{"stocks_stop_loss_pct":25,
                    "stocks_take_profit_pct":20,"max_notional_pct":100,"max_total_allocation_pct":100}}
                ranked_stock_candidates = sorted(
                    stock_candidates.items(),
                    key=lambda item: float(item[1][1].get("opportunityScore") or 0),
                    reverse=True
                )
                for symbol, (raw, decision) in ranked_stock_candidates:
                    cooldown_key = uid + ":stocks:" + symbol
                    if time.time() - DUAL_ADAPTIVE_ENTRY_COOLDOWNS.get(cooldown_key, 0) < 300:
                        continue
                    try:
                        result = _dual_try_adaptive_entry(
                            stock_experiment, portfolio, {**raw, **decision}, "stocks"
                        )
                        if result:
                            entries_opened += 1
                            selected_markets.setdefault(uid, {}).setdefault("stocks", []).append({
                                "symbol":symbol,
                                "opportunityScore":float(decision.get("opportunityScore") or 0),
                                "netEdgeProxyPct":decision.get("netEdgeProxyPct")
                            })
                            DUAL_ADAPTIVE_ENTRY_COOLDOWNS[cooldown_key] = time.time()
                    except Exception as exc:
                        log.info("Automatic stock entry skipped %s (%s: %s)", symbol, type(exc).__name__, getattr(exc, "detail", str(exc))[:180])
                    # Re-read open positions; the entry helper enforces the two-position cap.
                    open_now = _sb_rest("blessing_dual_positions", {
                        "select":"id","portfolio_id":"eq."+str(portfolio["id"]),
                        "user_id":"eq."+uid,"status":"eq.open","limit":"6"
                    }) or []
                    # No artificial max-open-position cutoff.
        # The automatic per-portfolio crypto and stock loops above are the sole
        # entry paths. Do not re-run the removed experiment-ranked legacy path.
        DUAL_PAPER_WORKER_STATE.update({
            "status":"running","lastCycleAt":datetime.now(timezone.utc).isoformat(),"lastError":None,
            "cycles":DUAL_PAPER_WORKER_STATE.get("cycles",0)+1,
            "positionsChecked":checked,"automaticExits":exits,
            "activeExperiments":len(experiments),
            "cryptoCandidates":len(crypto_candidates),"stocksCandidates":len(stock_candidates),
            "entriesOpened":entries_opened,"selectedMarkets":selected_markets
        })
        log.info("Dual Paper cycle complete crypto_candidates=%s stock_candidates=%s entries=%s crypto_rejects=%s",
                 len(crypto_candidates), len(stock_candidates), entries_opened,
                 DUAL_PAPER_WORKER_STATE.get("cryptoScanDiagnostics", {}).get("signalDiagnostics", {}))
    except Exception as exc:
        DUAL_PAPER_WORKER_STATE.update({"status":"error","lastError":type(exc).__name__})
        log.exception("Dual paper monitor cycle failed")
    finally:
        DUAL_PAPER_WORKER_LOCK.release()


def _dual_paper_worker_loop():
    while True:
        _dual_paper_worker_cycle()
        time.sleep(DUAL_PAPER_WORKER_INTERVAL)


@app.on_event("startup")
async def dual_paper_worker_startup():
    global DUAL_PAPER_WORKER_THREAD
    # The monitor runs one bounded scan cycle at a time; paper-only execution never calls broker order APIs.
    if os.getenv("DUAL_PAPER_WORKER_ENABLED", "false").strip().lower() not in {"1", "true", "yes", "on"}:
        DUAL_PAPER_WORKER_STATE.update({"status":"paused","lastError":"DISABLED_TO_PROTECT_DATABASE"})
        log.warning("Dual Paper monitor paused; set DUAL_PAPER_WORKER_ENABLED=true to re-enable")
        return
    if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
        DUAL_PAPER_WORKER_STATE["status"] = "waiting_config"
        return
    if DUAL_PAPER_WORKER_THREAD and DUAL_PAPER_WORKER_THREAD.is_alive():
        return
    DUAL_PAPER_WORKER_THREAD = threading.Thread(
        target=_dual_paper_worker_loop, name="dual-paper-monitor", daemon=True
    )
    DUAL_PAPER_WORKER_THREAD.start()


def _dual_price_number(value):
    """Find a plausible last price in a nested broker quote without inventing one."""
    keys = {"price", "last_price", "lastprice", "last", "close", "trade_price", "p"}
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).lower() in keys:
                try:
                    number = float(item)
                    if math.isfinite(number) and number > 0:
                        return number
                except (TypeError, ValueError):
                    pass
        for item in value.values():
            found = _dual_price_number(item)
            if found:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _dual_price_number(item)
            if found:
                return found
    return None


def _dual_market_price(market: str, symbol: str) -> float:
    """Return a fresh server-side quote for Paper execution; never trust browser prices."""
    if market == "crypto":
        if not symbol.endswith("USDT"):
            raise HTTPException(status_code=422, detail="คู่เหรียญต้องเป็น Spot/USDT")
        quote = state.get("crypto_quotes", {}).get(symbol)
        if isinstance(quote, dict):
            try:
                stamp = datetime.fromisoformat(str(quote.get("receivedAt")).replace("Z", "+00:00"))
                age = (datetime.now(timezone.utc) - stamp).total_seconds()
            except Exception:
                age = 999999
            price = _num(quote.get("price"), 0)
            if 0 <= age <= 5 and price > 0:
                return float(price)
        # Use a fresh Binance TH server-side ticker when the local websocket is stale.
        try:
            price = _binance_th_public_price(symbol)
            if price > 0:
                return float(price)
        except Exception as exc:
            log.info("Paper crypto price unavailable for %s (%s)", symbol, type(exc).__name__)
        raise HTTPException(status_code=503, detail="ไม่มีราคา Binance TH สดสำหรับส่งคำสั่ง Paper")
    if market == "stocks":
        now = datetime.now(timezone.utc)
        # Independent provider caches: prefer fresh Webull, then fresh Alpaca.
        for provider, cache in (("Webull", state.get("quotes", {})), ("Alpaca", state.get("alpaca_quotes", {}))):
            quote = cache.get(symbol) if isinstance(cache, dict) else None
            if not isinstance(quote, dict):
                continue
            try:
                received = datetime.fromisoformat(str(quote.get("receivedAt")).replace("Z", "+00:00"))
                age = (now - received).total_seconds()
            except Exception:
                age = 999999
            raw = quote.get("raw") or quote
            price = _dual_price_number(raw)
            if 0 <= age <= 8 and price and price > 0:
                return float(price)
        # Reuse a recently scanned candidate only when its scan timestamp is valid.
        try:
            with UNIVERSE_LOCK:
                universe = dict(UNIVERSE_STATE)
            updated = universe.get("updatedAt")
            if updated:
                scan_stamp = datetime.fromisoformat(str(updated).replace("Z", "+00:00"))
                scan_age = (now - scan_stamp).total_seconds()
                scan_limit = max(180, UNIVERSE_SCAN_INTERVAL * 3)
                if 0 <= scan_age <= scan_limit:
                    match = next((row for row in (universe.get("candidates") or [])
                                  if str(row.get("symbol") or "").upper() == symbol), None)
                    if match:
                        scanned_price = _num(match.get("price"), 0)
                        if scanned_price > 0:
                            return float(scanned_price)
        except Exception as exc:
            log.info("Paper stock scanner cache ignored for %s (%s)", symbol, type(exc).__name__)
        if not (ALPACA_KEY and ALPACA_SECRET):
            raise HTTPException(status_code=503, detail="ยังไม่มีราคาหุ้นสดจาก Webull/Alpaca สำหรับ Paper Trading")
        try:
            snapshot = _alpaca_snapshots([symbol]).get(symbol) or {}
            trade = snapshot.get("latestTrade") or {}
            latest_quote = snapshot.get("latestQuote") or {}
            minute = snapshot.get("minuteBar") or {}
            # Choose the newest available market timestamp; don't let an old last
            # trade hide a fresh bid/ask update during premarket or after-hours.
            stamps = []
            for item in (trade, latest_quote, minute):
                value = item.get("t")
                if value:
                    try:
                        stamps.append(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
                    except Exception:
                        pass
            stamp = max(stamps) if stamps else None
            age = (now - stamp).total_seconds() if stamp else 999999
            price = _num(trade.get("p"), 0)
            if price <= 0:
                bid, ask = _num(latest_quote.get("bp"), 0), _num(latest_quote.get("ap"), 0)
                if bid > 0 and ask >= bid:
                    price = (bid + ask) / 2
            if price <= 0:
                price = _num(minute.get("c"), 0)
            if 0 <= age <= 90 and price > 0:
                return float(price)
        except Exception as exc:
            log.info("Paper stock Alpaca snapshot unavailable for %s (%s)", symbol, type(exc).__name__)
        raise HTTPException(status_code=503, detail="ราคาหุ้นยังไม่สดพอ ระบบจึงงดส่งคำสั่ง Paper อย่างปลอดภัย")
    raise HTTPException(status_code=422, detail="เลือกตลาด crypto หรือ stocks เท่านั้น")


def _dual_rpc_order(payload: dict):
    try:
        return _sb_rpc("blessing_dual_execute_order", payload)
    except requests.HTTPError as exc:
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", 400)
        detail = "คำสั่งจำลองไม่ผ่านการตรวจสอบความเสี่ยง"
        if response is not None:
            try:
                raw = response.json()
                detail = str(raw.get("message") or raw.get("details") or raw.get("hint") or detail)[:240]
            except Exception:
                pass
        raise HTTPException(status_code=400 if status < 500 else 503, detail=detail)


@app.post("/ai/dual-paper/start")
async def dual_paper_start(request: Request):
    user = _paper_auth_user(request)
    body = await request.json()
    market = str(body.get("market") or "crypto").lower()
    if market not in DUAL_PAPER_PORTFOLIOS:
        raise HTTPException(status_code=422, detail="ตลาดไม่ถูกต้อง")
    row = _dual_paper_ensure_portfolio(str(user["id"]), DUAL_PAPER_PORTFOLIOS[market])
    if row.get("mode") != "paper":
        raise HTTPException(status_code=409, detail="อนุญาตเฉพาะ Paper mode")
    _sb_rest("blessing_dual_portfolios", {
        "id": "eq." + str(row["id"]), "user_id": "eq." + str(user["id"])
    }, "PATCH", {"enabled": True, "updated_at": datetime.now(timezone.utc).isoformat()})
    return {"ok": True, "market": market, "enabled": True, "mode": "paper", "liveOrdersEnabled": False}


@app.post("/ai/dual-paper/stop")
async def dual_paper_stop(request: Request):
    user = _paper_auth_user(request)
    body = await request.json()
    market = str(body.get("market") or "").lower()
    if market not in DUAL_PAPER_PORTFOLIOS:
        raise HTTPException(status_code=422, detail="ตลาดไม่ถูกต้อง")
    rows = _sb_rest("blessing_dual_portfolios", {
        "select": "id", "user_id": "eq." + str(user["id"]),
        "market": "eq." + market, "limit": "1"
    })
    if not rows:
        raise HTTPException(status_code=404, detail="ยังไม่ได้สร้างพอร์ตทดลอง")
    _sb_rest("blessing_dual_portfolios", {
        "id": "eq." + str(rows[0]["id"]), "user_id": "eq." + str(user["id"])
    }, "PATCH", {"enabled": False, "updated_at": datetime.now(timezone.utc).isoformat()})
    return {"ok": True, "market": market, "enabled": False, "mode": "paper", "message": "หยุดเปิดสถานะใหม่แล้ว; ระบบยังติดตาม Stop Loss/Take Profit ของสถานะเดิม"}


@app.post("/ai/dual-paper/order")
async def dual_paper_order(request: Request):
    user = _paper_auth_user(request)
    body = await request.json()
    market = str(body.get("market") or "").lower()
    side = str(body.get("side") or "").lower()
    symbol = str(body.get("symbol") or "").strip().upper()
    if market not in DUAL_PAPER_PORTFOLIOS or side not in {"buy", "sell"}:
        raise HTTPException(status_code=422, detail="ข้อมูลตลาดหรือด้านคำสั่งไม่ถูกต้อง")
    if market == "crypto" and (not symbol.endswith("USDT") or not symbol[:-4].isalnum() or len(symbol[:-4]) < 2):
        raise HTTPException(status_code=422, detail="คู่เหรียญต้องเป็น Spot/USDT")
    if market == "stocks" and (not symbol or not symbol[0].isalpha() or any(not (c.isalnum() or c in ".-") for c in symbol)):
        raise HTTPException(status_code=422, detail="สัญลักษณ์หุ้นไม่ถูกต้อง")

    stock_buy_lock = _dual_stock_buy_lock(str(user["id"])) if market == "stocks" and side == "buy" else None
    if stock_buy_lock and not stock_buy_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="กำลังประมวลผล BUY หุ้นอยู่ กรุณาลองอีกครั้ง")
    try:
        rows = _sb_rest("blessing_dual_portfolios", {
            "select": "id,mode,enabled", "user_id": "eq." + str(user["id"]),
            "market": "eq." + market, "limit": "1"
        })
        if not rows:
            raise HTTPException(status_code=404, detail="ยังไม่ได้สร้างพอร์ตทดลอง")
        portfolio = rows[0]
        if portfolio.get("mode") != "paper" or not portfolio.get("enabled"):
            raise HTTPException(status_code=409, detail="กรุณาเริ่มพอร์ต Paper ก่อนส่งคำสั่งจำลอง")
        if market == "stocks" and side == "buy":
            open_stocks = _sb_rest("blessing_dual_positions", {
                "select":"id,symbol", "portfolio_id":"eq." + str(portfolio["id"]),
                "user_id":"eq." + str(user["id"]), "status":"eq.open", "limit":"3"
            }) or []
            if len(open_stocks) >= 5:
                raise HTTPException(status_code=409, detail="พอร์ตหุ้นเปิดครบ 5 สถานะแล้ว")
            if any(str(row.get("symbol") or "").upper() == symbol for row in open_stocks):
                raise HTTPException(status_code=409, detail="หุ้นนี้มีสถานะเปิดอยู่แล้ว")

        if side == "buy":
            try:
                notional = float(body.get("notional"))
                if market == "stocks":
                    stop_pct = 25.0
                    target_pct = float(body.get("takeProfitPct", 10.0))
                else:
                    stop_pct = float(body.get("stopLossPct", 1.0))
                    target_pct = float(body.get("takeProfitPct", 2.0))
            except (TypeError, ValueError):
                raise HTTPException(status_code=422, detail="กรุณาระบุเงินลงทุนและระดับ Stop/Take Profit")
            target_valid = (5.0 <= target_pct <= 15.0) if market == "stocks" else (0.1 <= target_pct <= 50.0)
            if not all(math.isfinite(x) for x in (notional, stop_pct, target_pct)) or notional <= 0 or not (0.1 <= stop_pct <= (25 if market == "stocks" else 20)) or not target_valid:
                detail = "หุ้นต้องกำหนด Take Profit ระหว่าง 5% ถึง 15% และ Stop Loss คงที่ -25%" if market == "stocks" else "ค่าการลงทุนหรือ Stop/Take Profit ไม่อยู่ในช่วงที่อนุญาต"
                raise HTTPException(status_code=422, detail=detail)
        else:
            position_id = str(body.get("positionId") or "")
            positions = _sb_rest("blessing_dual_positions", {
                "select": "id,symbol,quantity,side", "id": "eq." + position_id,
                "portfolio_id": "eq." + str(portfolio["id"]),
                "user_id": "eq." + str(user["id"]), "status": "eq.open", "limit": "1"
            })
            if not positions:
                raise HTTPException(status_code=404, detail="ไม่พบสถานะเปิดที่ต้องการปิด")
            if positions[0]["symbol"] != symbol:
                raise HTTPException(status_code=422, detail="สัญลักษณ์ไม่ตรงกับสถานะที่เลือก")
            quantity = float(positions[0]["quantity"])
            rpc_side = "cover" if positions[0].get("side") == "short" else "sell"

        market_price = _dual_market_price(market, symbol)
        # Short covers are buys; longs are sold to close.
        is_buy_fill = side == "buy" or (side == "sell" and rpc_side == "cover")
        fill_price = market_price * (1.0005 if is_buy_fill else 0.9995)
        position_id = None if side == "buy" else position_id
        stop_price = fill_price * (1 - stop_pct / 100) if side == "buy" else None
        target_price = fill_price * (1 + target_pct / 100) if side == "buy" else None
        if side == "buy":
            quantity = round(notional / fill_price, 10 if market == "crypto" else 6)

        result = _dual_rpc_order({
            "p_user_id": str(user["id"]), "p_market": market, "p_symbol": symbol,
            "p_side": side if side == "buy" else rpc_side, "p_quantity": quantity, "p_price": fill_price,
            "p_stop_loss_price": stop_price, "p_take_profit_price": target_price,
            "p_position_id": position_id, "p_idempotency_key": str(body.get("idempotencyKey") or uuid.uuid4())
        })
        return {"ok": True, "mode": "paper", "liveOrdersEnabled": False,
                "marketPrice": market_price, "simulatedFillPrice": fill_price, "result": result}
    finally:
        if stock_buy_lock:
            stock_buy_lock.release()


@app.post("/ai/dual-paper/refresh")
async def dual_paper_refresh(request: Request):
    user = _paper_auth_user(request)
    uid = str(user["id"])
    checked = 0
    exits = []
    unavailable = []
    for market in DUAL_PAPER_PORTFOLIOS:
        portfolios = _sb_rest("blessing_dual_portfolios", {
            "select": "id", "user_id": "eq." + uid, "market": "eq." + market, "limit": "1"
        })
        if not portfolios:
            continue
        positions = _sb_rest("blessing_dual_positions", {
            "select": "id,symbol", "user_id": "eq." + uid,
            "portfolio_id": "eq." + str(portfolios[0]["id"]),
            "status": "eq.open", "limit": "100"
        }) or []
        for position in positions:
            try:
                price = _dual_market_price(market, position["symbol"])
                result = _sb_rpc("blessing_dual_mark_position", {
                    "p_user_id": uid, "p_market": market,
                    "p_position_id": str(position["id"]), "p_price": price
                })
                checked += 1
                if isinstance(result, dict) and result.get("closed"):
                    exits.append(result)
            except HTTPException as exc:
                unavailable.append({"market": market, "symbol": position["symbol"], "reason": exc.detail})
            except Exception as exc:
                log.warning("Dual paper mark failed for %s %s (%s)", market, position["symbol"], type(exc).__name__)
                unavailable.append({"market": market, "symbol": position["symbol"], "reason": "price_or_database_unavailable"})
    return {"ok": True, "checked": checked, "automaticExits": exits, "unavailable": unavailable,
            "liveOrdersEnabled": False}


@app.get("/ai/portfolio-overview")
def ai_portfolio_overview(request: Request):
    """Admin-only view of isolated crypto and stock Paper portfolio policies."""
    _paper_auth_user(request)
    return portfolio_overview()


@app.get("/broker/read-only")
def broker_readonly(request: Request):
    """Admin-only account snapshot; read-only adapter has no order functions."""
    _paper_auth_user(request)
    return brokerage_readonly_snapshot()


@app.get("/ai/decision-preview")
def ai_decision_preview(request: Request, refresh: int = 0):
    """Admin-only explainable signal preview. Does not place or queue orders."""
    _paper_auth_user(request)
    market = altcoin_scanner(refresh=1 if refresh else 0)
    result = build_decision_preview(market, open_positions=0, daily_loss_pct=0.0, drawdown_pct=0.0)
    result["riskMetricsSource"] = "preview_defaults_not_live_account_metrics"
    result["entryGateOpen"] = False
    result["liveOrdersEnabled"] = False
    return result



@app.get("/ai/adaptive-paper/preview")
def adaptive_paper_preview(request: Request):
    """Admin-only adaptive market-regime preview; deliberately does not place orders."""
    _paper_auth_user(request)
    crypto = altcoin_scanner(refresh=0)
    with UNIVERSE_LOCK:
        stock_snapshot = dict(UNIVERSE_STATE)
    crypto_rows = []
    crypto_fresh = crypto.get("status") == "ok" and crypto.get("exchange") == "Binance"
    if crypto_fresh:
        for row in (crypto.get("candidates") or [])[:100]:
            if row.get("source") != "Binance":
                continue
            crypto_rows.append(adaptive_decision("crypto", row))
    stock_rows = []
    try:
        stamp = stock_snapshot.get("updatedAt")
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))).total_seconds() if stamp else 999999
    except Exception:
        age = 999999
    stocks_fresh = stock_snapshot.get("status") == "ok" and age <= max(180, UNIVERSE_SCAN_INTERVAL * 3)
    if stocks_fresh:
        for row in (stock_snapshot.get("candidates") or [])[:150]:
            normalized = dict(row)
            normalized["rvol"] = normalized.get("relativeVolume")
            stock_rows.append(adaptive_decision("stocks", normalized))
    crypto_rows.sort(key=lambda row: (row["decision"] == "PAPER_CANDIDATE", row["momentumProxyPct"]), reverse=True)
    stock_rows.sort(key=lambda row: (row["decision"] == "PAPER_CANDIDATE", row["momentumProxyPct"]), reverse=True)
    ranked_opportunities = sorted(
        [row for row in crypto_rows + stock_rows if row.get("decision") == "PAPER_CANDIDATE"],
        key=lambda row: (row.get("opportunityScore", 0), row.get("netEdgeProxyPct", 0)),
        reverse=True
    )
    return {
        "ok": True, "mode": "paper_only", "liveOrdersEnabled": False,
        "automaticEntryEnabled": False,
        "experiment": experiment_window(14),
        "policy": {
            "adaptiveRegimeSelection": True,
            "cryptoDirections": ["long", "short"],
            "stocksDirections": ["long"],
            "cryptoRiskPerTradePct": 0.5,
            "cryptoMaxAggregateOpenRiskPct": 1.5,
            "cryptoMaxDailyLossPct": 2.0,
            "cryptoMaxOpenPositions": 3,
            "cryptoReserveCashPct": 20,
            "cryptoShortMode": "paper_simulation_only_no_leverage",
            "markets": ["crypto", "stocks"],
            "cryptoSource": "Binance TH scanner/candles; Binance-only candidates",
            "stockSource": "disabled_pending_Webull_stock_scanner",
            "staleOrFallbackData": "block new entries",
            "costModel": "estimated round-trip fees, slippage and quoted spread; verify actual fills in reports",
            "stockStopLossPct": -25, "stockTakeProfitMinPct": 5, "stockTakeProfitMaxPct": 15,
            "stockEntryMode": "manual_buy_only",
            "note": "This endpoint is a signal preview only; it does not execute paper orders."
        },
        "marketData": {
            "crypto": {"fresh": crypto_fresh, "status": crypto.get("status"),
                       "provider": crypto.get("exchange") or crypto.get("provider"),
                       "updatedAt": crypto.get("updatedAt"), "candidateCount": len(crypto_rows)},
            "stocks": {"fresh": stocks_fresh, "status": stock_snapshot.get("status"),
                       "provider": stock_snapshot.get("provider"), "updatedAt": stock_snapshot.get("updatedAt"),
                       "candidateCount": len(stock_rows)}
        },
        "decisions": {"crypto": crypto_rows[:30], "stocks": stock_rows[:30]},
        "crossMarketRanking": ranked_opportunities[:20],
        "bestCurrentMarket": ranked_opportunities[0]["market"] if ranked_opportunities else None,
        "disclaimer": "ผลลัพธ์เป็นการคัดกรองสัญญาณ ไม่ใช่คำสั่งซื้อขาย และไม่รับประกันกำไร"
    }


@app.post("/ai/adaptive-paper/start")
def adaptive_paper_start(request: Request):
    """Start continuous adaptive Paper Trading until the owner manually stops it."""
    user=_paper_auth_user(request)
    uid=str(user["id"])
    active=_sb_rest("blessing_dual_experiments",{
        "select":"id,started_at,ends_at,status","user_id":"eq."+uid,"status":"eq.active","limit":"1"
    }) or []
    if active:
        return {"ok":True,"alreadyActive":True,"experiment":active[0],"mode":"paper","liveOrdersEnabled":False}
    portfolios={}
    for market,config in DUAL_PAPER_PORTFOLIOS.items():
        row=_dual_paper_ensure_portfolio(uid,config)
        if row.get("mode")!="paper":
            raise HTTPException(status_code=409,detail="อนุญาตเฉพาะ Paper Trading")
        portfolios[market]=row
    started=datetime.now(timezone.utc)
    ends=started+timedelta(days=36500)
    exp=_sb_rest("blessing_dual_experiments",{}, "POST",{
        "user_id":uid,"started_at":started.isoformat(),"ends_at":ends.isoformat(),
        "status":"active","markets":["crypto","stocks"],
        "config":{"strategy":"shared_adaptive_momentum","selection":"independent_per_market",
                  "risk_per_trade_pct":25,"max_position_loss_pct":25,
                  "max_total_allocation_pct":100,"max_notional_pct":100,
                  "reserve_cash_pct":0,"daily_loss_limit_enabled":False,
                  "weekly_loss_limit_enabled":False,"cumulative_loss_limit_enabled":False,
                  "drawdown_stop_enabled":False,
                  "crypto_stop_loss_pct":1.5,"crypto_take_profit_pct":3.0,
                  "stocks_stop_loss_pct":25.0,"stocks_take_profit_pct":10.0,
                  "stocks_take_profit_min_pct":5.0,"stocks_take_profit_max_pct":15.0,
                  "stocks_manual_entry_only":False,"stocks_automatic_entry":True,"stocks_max_open_positions":5,
                  "fee_bps_per_side":10,"slippage_bps_per_side":5,
                  "entry_cooldown_seconds":300,"max_entries_per_stocks_cycle":5,
                  "live_orders_enabled":False}
    })
    if not exp:
        raise HTTPException(status_code=503,detail="เริ่มการทดลองไม่สำเร็จ")
    for market,row in portfolios.items():
        _sb_rest("blessing_dual_portfolios",{"id":"eq."+str(row["id"]),"user_id":"eq."+uid},
                 "PATCH",{"enabled":True,"updated_at":started.isoformat()})
    return {"ok":True,"mode":"paper","liveOrdersEnabled":False,"automaticEntriesEnabled":True,
            "automaticCryptoEntriesEnabled":True,"automaticStockEntriesEnabled":False,
            "experiment":exp[0],"markets":{"crypto":"auto-entry","stocks":"manual-buy-only"},
            "message":"คริปโตยังใช้ Auto Paper ได้ ส่วนหุ้นสหรัฐฯ ต้องกด BUY เองทุกครั้ง และ Backend ติดตามปิด TP/SL อัตโนมัติ"}


@app.post("/ai/adaptive-paper/stop")
def adaptive_paper_stop(request: Request):
    user=_paper_auth_user(request)
    uid=str(user["id"])
    rows=_sb_rest("blessing_dual_experiments",{
        "select":"id","user_id":"eq."+uid,"status":"eq.active","limit":"1"
    }) or []
    for row in rows:
        _sb_rest("blessing_dual_experiments",{"id":"eq."+str(row["id"])},
                 "PATCH",{"status":"stopped","updated_at":datetime.now(timezone.utc).isoformat()})
    _sb_rest("blessing_dual_portfolios",{"user_id":"eq."+uid},
             "PATCH",{"enabled":False,"updated_at":datetime.now(timezone.utc).isoformat()})
    return {"ok":True,"stopped":True,"mode":"paper","message":"หยุดเปิดสถานะใหม่แล้ว ระบบยังติดตามและปิดสถานะเดิมตาม TP/SL"}


@app.get("/ai/adaptive-paper/report")
def adaptive_paper_report(request: Request):
    user=_paper_auth_user(request)
    uid=str(user["id"])
    experiments=_sb_rest("blessing_dual_experiments",{
        "select":"id,started_at,ends_at,status,markets,config","user_id":"eq."+uid,
        "order":"created_at.desc","limit":"5"
    }) or []
    if not experiments:
        return {"ok":True,"experiment":None,"markets":{},"mode":"paper","liveOrdersEnabled":False}
    exp=experiments[0]
    report={}
    for market in ("crypto","stocks"):
        marks=_sb_rest("blessing_dual_equity_marks",{
            "select":"marked_at,cash_balance,market_value,equity,realized_pnl,unrealized_pnl,fees_paid,estimated_slippage,net_pnl_after_costs,net_return_pct,peak_equity,drawdown_pct",
            "experiment_id":"eq."+str(exp["id"]),"market":"eq."+market,
            "order":"marked_at.asc","limit":"10000"
        }) or []
        latest=marks[-1] if marks else None
        max_dd=max([float(x.get("drawdown_pct") or 0) for x in marks] or [0])
        report[market]={"markCount":len(marks),"latest":latest,"maxDrawdownPct":max_dd,
                        "equityCurve":marks[-500:]}
    return {"ok":True,"mode":"paper","liveOrdersEnabled":False,
            "automaticEntriesEnabled":True,"automaticCryptoEntriesEnabled":True,
            "automaticStockEntriesEnabled":False,"experiment":exp,"markets":report,
            "measurement":{"netProfit":"equity minus starting capital and estimated slippage; exchange fees already deducted from cash",
                           "drawdown":"peak-to-trough based on net equity marks","slippageModel":"5 bps per filled order side"}}

# Keep the dual paper monitor and verified Alpaca IEX fallback in the deployed backend.


# Independent automated Paper engine for the ten unassigned member slots.
# Each slot has its own THB cash ledger, positions and order history.
# This engine never references user portfolios and never submits live orders.
# Leveraged/inverse ETFs are excluded from automated stock Paper entries.
# Explicit denylist is intentionally fail-closed for known daily-reset products.
STOCK_PAPER_BLOCKED_LEVERAGED_ETFS = {
    "SOXL", "SOXS", "TQQQ", "SQQQ", "UPRO", "SPXU", "TECL", "TECS",
    "FAS", "FAZ", "LABU", "LABD", "NUGT", "DUST", "JNUG", "JDST",
    "DRN", "DRV", "CURE", "SDOW", "UDOW", "TNA", "TZA", "URTY", "SRTY",
    "UCO", "SCO", "BOIL", "KOLD", "GUSH", "DRIP", "UVXY", "SVXY",
    "VXX", "AGQ", "ZSL", "YINN", "YANG", "BITX", "CONL", "MSTU", "MSTZ"
}

MEMBER_SLOT_ENGINE_INTERVAL = max(20, int(os.getenv("MEMBER_SLOT_ENGINE_INTERVAL", "30")))
MEMBER_SLOT_ENGINE_THREAD = None
MEMBER_SLOT_ENGINE_LOCK = threading.Lock()
MEMBER_SLOT_ENGINE_COOLDOWNS = {}
MEMBER_SLOT_ENGINE_STATE = {
    "status": "starting" if (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY) else "waiting_config",
    "lastCycleAt": None, "lastError": None, "cycles": 0,
    "cryptoCandidates": 0, "stockCandidates": 0, "actions": [],
    "liveTradingEnabled": False
}


def _member_slot_usd_thb():
    # Use a current public FX quote; fail closed rather than inventing an exchange rate.
    response = requests.get("https://api.frankfurter.app/latest",
                            params={"from": "USD", "to": "THB"},
                            timeout=(4, 8),
                            headers={"User-Agent": "BlessingMemberPaper/1.0"})
    response.raise_for_status()
    rate = _num((response.json().get("rates") or {}).get("THB"), 0)
    if not rate or rate <= 0:
        raise RuntimeError("USD_THB_RATE_UNAVAILABLE")
    return float(rate)


def _member_slot_engine_cycle():
    if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
        MEMBER_SLOT_ENGINE_STATE.update({"status":"waiting_config","lastError":"SUPABASE_WORKER_CONFIG_MISSING"})
        return
    if not MEMBER_SLOT_ENGINE_LOCK.acquire(blocking=False):
        return
    try:
        now = datetime.now(timezone.utc)
        # Refresh health first; a temporary FX-provider outage must not make a healthy
        # Paper engine appear offline. Trading still fails closed until FX is available.
        heartbeat = now.isoformat()
        _sb_rest("blessing_paper_member_slots", {"enabled":"eq.true","mode":"eq.paper"},
                 method="PATCH", body={"updated_at":heartbeat})
        _sb_rest("blessing_stock_paper_member_slots", {"enabled":"eq.true"},
                 method="PATCH", body={"updated_at":heartbeat})
        fx = _member_slot_usd_thb()
        crypto_slots = _sb_rest("blessing_paper_member_slots", {
            "select":"id,slot_no,initial_capital,cash_balance,realized_pnl,max_open_positions,enabled,status",
            "enabled":"eq.true", "mode":"eq.paper", "limit":"20"
        }) or []
        stock_slots = _sb_rest("blessing_stock_paper_member_slots", {
            "select":"id,slot_no,initial_capital,cash_balance,max_open_positions,enabled,status",
            "enabled":"eq.true", "limit":"20"
        }) or []

        # Paper entries use the candle scanner, not ticker-only WebSocket rows:
        # EMA/ATR are mandatory inputs for the three setup types.
        crypto_scan = altcoin_scanner(0)
        try:
            crypto_age = (now-datetime.fromisoformat(str(crypto_scan.get("updatedAt")).replace("Z","+00:00"))).total_seconds()
        except Exception:
            crypto_age = 999999
        crypto_fresh = (
            crypto_scan.get("status") == "ok"
            and crypto_scan.get("provider") == "Binance TH Spot API"
            and 0 <= crypto_age <= 90
        )
        crypto_candidates = {}
        if crypto_fresh:
            for row in (crypto_scan.get("candidates") or []):
                symbol = str(row.get("symbol") or "").upper()
                price = _num(row.get("price"), 0)
                if not symbol or not price or not symbol.endswith("USDT"):
                    continue
                decision = _crypto_three_strategy_decision(row)
                if decision:
                    crypto_candidates[symbol] = (price, decision)

        # Stock entries use the current Alpaca IEX universe scan and current USD/THB FX.
        with UNIVERSE_LOCK:
            stock_scan = dict(UNIVERSE_STATE)
        stock_stamp = stock_scan.get("updatedAt")
        try:
            stock_age = (now-datetime.fromisoformat(str(stock_stamp).replace("Z","+00:00"))).total_seconds() if stock_stamp else 999999
        except Exception:
            stock_age = 999999
        stock_fresh = stock_scan.get("status") == "ok" and stock_age <= max(180, UNIVERSE_SCAN_INTERVAL * 3)
        stock_candidates = {}
        if stock_fresh:
            for raw in (stock_scan.get("candidates") or []):
                symbol = str(raw.get("symbol") or "").upper().strip()
                price = _num(raw.get("price"), 0)
                # Keep leveraged and inverse ETFs out of member-slot stock Paper.
                # These products amplify daily moves and can create outsized losses.
                if not symbol or not price or symbol in STOCK_PAPER_BLOCKED_LEVERAGED_ETFS:
                    continue
                normalized = dict(raw)
                normalized["rvol"] = normalized.get("relativeVolume")
                decision = adaptive_decision("stocks", normalized, fee_bps=10, slippage_bps=5)
                if decision.get("decision") == "PAPER_CANDIDATE":
                    stock_candidates[symbol] = (price, decision)

        actions = []
        # Mark and manage all open crypto positions independently for each slot.
        for slot in crypto_slots:
            positions = _sb_rest("blessing_paper_slot_positions", {
                "select":"id,symbol,quantity,average_entry_price_thb,current_price_thb,unrealized_pnl_thb,position_side,stop_loss_price_thb,take_profit_price_thb,strategy,status",
                "slot_id":"eq."+str(slot["id"]), "status":"eq.open", "limit":"100"
            }) or []
            for pos in positions:
                quote = crypto_candidates.get(pos["symbol"])
                price = quote[0] if quote else None
                if not price:
                    try:
                        price = _binance_public_price(pos["symbol"])
                    except Exception:
                        continue
                try:
                    _sb_rpc("blessing_member_slot_paper_trade", {
                        "p_market":"crypto","p_slot_id":str(slot["id"]),"p_action":"mark",
                        "p_symbol":pos["symbol"],"p_price":price,"p_fx":fx
                    })
                    current_thb = price * fx
                    entry_thb = float(pos.get("average_entry_price_thb") or 0)
                    position_side = str(pos.get("position_side") or "long").lower()
                    target_thb = _num(pos.get("take_profit_price_thb"),
                                      entry_thb * (0.97 if position_side == "short" else 1.03))
                    stop_thb = _num(pos.get("stop_loss_price_thb"),
                                    entry_thb * (1.015 if position_side == "short" else 0.985))
                    if position_side == "short":
                        reason = "TAKE_PROFIT" if current_thb <= target_thb else (
                                 "STOP_LOSS" if current_thb >= stop_thb else None)
                        close_action = "cover"
                    else:
                        reason = "TAKE_PROFIT" if current_thb >= target_thb else (
                                 "STOP_LOSS" if current_thb <= stop_thb else None)
                        close_action = "sell"
                    if reason:
                        result = _sb_rpc("blessing_member_slot_paper_trade", {
                            "p_market":"crypto","p_slot_id":str(slot["id"]),"p_action":close_action,
                            "p_symbol":pos["symbol"],"p_price":price,"p_fx":fx,
                            "p_reason":reason,"p_position_side":position_side,
                            "p_idempotency_key":"slot-exit-"+str(slot["id"])+"-"+str(pos["id"])
                        })
                        actions.append({"slotNo":slot["slot_no"],"market":"crypto","action":close_action,
                                        "positionSide":position_side,"symbol":pos["symbol"],"reason":reason,"result":result})
                except Exception as exc:
                    log.warning("Member crypto slot %s position monitor skipped (%s)",slot["slot_no"],type(exc).__name__)

            if not crypto_fresh:
                continue
            open_now = _sb_rest("blessing_paper_slot_positions", {
                "select":"symbol,unrealized_pnl_thb","slot_id":"eq."+str(slot["id"]),"status":"eq.open","limit":"100"
            }) or []
            held = {x["symbol"] for x in open_now}
            if len(open_now) >= int(slot.get("max_open_positions") or 3):
                continue
            day_start = now.replace(hour=0,minute=0,second=0,microsecond=0).isoformat()
            daily_rows = _sb_rest("blessing_paper_slot_orders", {
                "select":"realized_pnl_thb","slot_id":"eq."+str(slot["id"]),
                "side":"in.(sell,cover)","status":"eq.filled","created_at":"gte."+day_start,"limit":"500"
            }) or []
            daily_pnl = sum(_num(x.get("realized_pnl_thb"),0) or 0 for x in daily_rows) + sum(
                _num(x.get("unrealized_pnl_thb"),0) or 0 for x in open_now
            )
            initial_capital = _num(slot.get("initial_capital"),100000) or 100000
            total_pnl = (_num(slot.get("realized_pnl"),0) or 0) + sum(
                _num(x.get("unrealized_pnl_thb"),0) or 0 for x in open_now
            )
            if daily_pnl <= -(initial_capital*0.02) or total_pnl <= -(initial_capital*0.05):
                continue
            ranked = sorted(crypto_candidates.items(),
                            key=lambda item: float(item[1][1].get("opportunityScore") or 0), reverse=True)
            for symbol, (price, decision) in ranked:
                if symbol in held:
                    continue
                key = str(slot["id"])+":crypto:"+symbol
                if time.time()-MEMBER_SLOT_ENGINE_COOLDOWNS.get(key,0) < 300:
                    continue
                try:
                    entry_action = "short" if decision.get("side") == "short" else "buy"
                    position_side = "short" if entry_action == "short" else "long"
                    result = _sb_rpc("blessing_member_slot_paper_trade", {
                        "p_market":"crypto","p_slot_id":str(slot["id"]),"p_action":entry_action,
                        "p_symbol":symbol,"p_price":price,"p_fx":fx,"p_position_side":position_side,
                        "p_reason":"ADAPTIVE_"+entry_action.upper()+"_"+str(decision.get("strategy") or "MOMENTUM"),
                        "p_idempotency_key":"slot-"+entry_action+"-"+str(slot["id"])+"-"+symbol+"-"+str(int(time.time()//300))
                    })
                    if isinstance(result,dict) and result.get("positionId"):
                        atr_stop_pct = min(2.0,max(0.8,_num(decision.get("atrStopPct"),1.5) or 1.5))
                        target_pct = max(3.0,atr_stop_pct*2.0)
                        entry_thb = _num(result.get("priceTHB"),price*fx) or price*fx
                        stop_thb = entry_thb*(1+atr_stop_pct/100 if position_side=="short" else 1-atr_stop_pct/100)
                        target_thb = entry_thb*(1-target_pct/100 if position_side=="short" else 1+target_pct/100)
                        _sb_rest("blessing_paper_slot_positions",{
                            "id":"eq."+str(result["positionId"]),"slot_id":"eq."+str(slot["id"]),"status":"eq.open"
                        },"PATCH",{
                            "stop_loss_price_thb":stop_thb,"take_profit_price_thb":target_thb,
                            "strategy":str(decision.get("strategy") or "ADAPTIVE")
                        })
                    MEMBER_SLOT_ENGINE_COOLDOWNS[key] = time.time()
                    actions.append({"slotNo":slot["slot_no"],"market":"crypto","action":entry_action,
                                    "positionSide":position_side,"symbol":symbol,"strategy":decision.get("strategy"),
                                    "score":decision.get("opportunityScore"),"result":result})
                    break
                except Exception as exc:
                    log.info("Member crypto slot %s entry skipped %s (%s)",slot["slot_no"],symbol,type(exc).__name__)

        # Mark, exit and open stock positions using Alpaca IEX USD quotes.
        for slot in stock_slots:
            positions = _sb_rest("blessing_stock_paper_slot_positions", {
                "select":"id,symbol,quantity,average_entry_price_usd,current_price_usd,entry_fx_thb_per_usd,status",
                "slot_id":"eq."+str(slot["id"]), "status":"eq.open", "limit":"100"
            }) or []
            prices = {}
            if positions:
                symbols = list(dict.fromkeys(x["symbol"] for x in positions))
                for offset in range(0, len(symbols), 100):
                    try:
                        snaps = _alpaca_snapshots(symbols[offset:offset+100])
                    except Exception as exc:
                        log.warning("Member stock slot quote refresh skipped (%s)",type(exc).__name__)
                        snaps = {}
                    for symbol, snap in snaps.items():
                        trade = snap.get("latestTrade") or {}
                        minute = snap.get("minuteBar") or {}
                        px = _num(trade.get("p"), _num(minute.get("c"), 0)) or 0
                        if px > 0:
                            prices[symbol] = px
            for pos in positions:
                price = prices.get(pos["symbol"])
                if not price:
                    continue
                try:
                    _sb_rpc("blessing_member_slot_paper_trade", {
                        "p_market":"stocks","p_slot_id":str(slot["id"]),"p_action":"mark",
                        "p_symbol":pos["symbol"],"p_price":price,"p_fx":fx
                    })
                    entry_thb = float(pos.get("average_entry_price_usd") or 0) * float(pos.get("entry_fx_thb_per_usd") or fx)
                    current_thb = price * fx
                    reason = "TAKE_PROFIT" if current_thb >= entry_thb * 1.05 else (
                             "STOP_LOSS" if current_thb <= entry_thb * 0.97 else None)
                    if reason:
                        result = _sb_rpc("blessing_member_slot_paper_trade", {
                            "p_market":"stocks","p_slot_id":str(slot["id"]),"p_action":"sell",
                            "p_symbol":pos["symbol"],"p_price":price,"p_fx":fx,"p_reason":reason,
                            "p_idempotency_key":"slot-stock-exit-"+str(slot["id"])+"-"+str(pos["id"])
                        })
                        actions.append({"slotNo":slot["slot_no"],"market":"stocks","action":"sell",
                                        "symbol":pos["symbol"],"reason":reason,"result":result})
                except Exception as exc:
                    log.warning("Member stock slot %s position monitor skipped (%s)",slot["slot_no"],type(exc).__name__)

            if not stock_fresh:
                continue
            open_now = _sb_rest("blessing_stock_paper_slot_positions", {
                "select":"symbol","slot_id":"eq."+str(slot["id"]),"status":"eq.open","limit":"100"
            }) or []
            held = {x["symbol"] for x in open_now}
            if len(open_now) >= int(slot.get("max_open_positions") or 5):
                continue
            ranked = sorted(stock_candidates.items(),
                            key=lambda item: float(item[1][1].get("opportunityScore") or 0), reverse=True)
            for symbol, (price, decision) in ranked:
                if symbol in held:
                    continue
                key = str(slot["id"])+":stocks:"+symbol
                if time.time()-MEMBER_SLOT_ENGINE_COOLDOWNS.get(key,0) < 600:
                    continue
                try:
                    result = _sb_rpc("blessing_member_slot_paper_trade", {
                        "p_market":"stocks","p_slot_id":str(slot["id"]),"p_action":"buy",
                        "p_symbol":symbol,"p_price":price,"p_fx":fx,
                        "p_reason":"ALPACA_IEX_ADAPTIVE_PAPER",
                        "p_idempotency_key":"slot-stock-buy-"+str(slot["id"])+"-"+symbol+"-"+str(int(time.time()//600))
                    })
                    MEMBER_SLOT_ENGINE_COOLDOWNS[key] = time.time()
                    actions.append({"slotNo":slot["slot_no"],"market":"stocks","action":"buy",
                                    "symbol":symbol,"score":decision.get("opportunityScore"),"result":result})
                    break
                except Exception as exc:
                    log.info("Member stock slot %s entry skipped %s (%s)",slot["slot_no"],symbol,type(exc).__name__)

        MEMBER_SLOT_ENGINE_STATE.update({
            "status":"running","lastCycleAt":now.isoformat(),"lastError":None,
            "cycles":MEMBER_SLOT_ENGINE_STATE.get("cycles",0)+1,
            "cryptoCandidates":len(crypto_candidates),"stockCandidates":len(stock_candidates),
            "cryptoStreamFresh":crypto_fresh,"stockScanFresh":stock_fresh,
            "usdThb":fx,"cryptoSlots":len(crypto_slots),"stockSlots":len(stock_slots),
            "actions":(MEMBER_SLOT_ENGINE_STATE.get("actions",[])+actions)[-50:],
            "liveTradingEnabled":False
        })
    except Exception as exc:
        MEMBER_SLOT_ENGINE_STATE.update({"status":"error","lastError":type(exc).__name__,
                                         "lastCycleAt":datetime.now(timezone.utc).isoformat(),
                                         "liveTradingEnabled":False})
        log.exception("Member slot Paper engine cycle failed")
    finally:
        MEMBER_SLOT_ENGINE_LOCK.release()


def _member_slot_engine_loop():
    while True:
        try:
            _member_slot_engine_cycle()
        except Exception:
            log.exception("Member slot Paper engine loop error")
        time.sleep(MEMBER_SLOT_ENGINE_INTERVAL)


@app.on_event("startup")
async def member_slot_engine_startup():
    global MEMBER_SLOT_ENGINE_THREAD
    # Keep the always-on member Paper worker disabled by default. It repeatedly
    # polls Supabase for every slot and can exhaust the shared connection pool.
    # Re-enable explicitly with MEMBER_SLOT_ENGINE_ENABLED=true after load testing.
    if os.getenv("MEMBER_SLOT_ENGINE_ENABLED", "false").strip().lower() not in {"1", "true", "yes", "on"}:
        MEMBER_SLOT_ENGINE_STATE.update({"status":"paused","lastError":"DISABLED_TO_PROTECT_DATABASE"})
        log.warning("Member slot Paper engine is paused; set MEMBER_SLOT_ENGINE_ENABLED=true to re-enable")
        return
    if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
        MEMBER_SLOT_ENGINE_STATE["status"] = "waiting_config"
        return
    if MEMBER_SLOT_ENGINE_THREAD and MEMBER_SLOT_ENGINE_THREAD.is_alive():
        return
    MEMBER_SLOT_ENGINE_THREAD = threading.Thread(
        target=_member_slot_engine_loop, name="member-slot-paper-engine", daemon=True
    )
    MEMBER_SLOT_ENGINE_THREAD.start()
    log.info("Member slot Paper engine thread started; interval=%ss", MEMBER_SLOT_ENGINE_INTERVAL)


@app.get("/admin/member-slot-engine/status")
def admin_member_slot_engine_status(request: Request):
    _binance_th_owner_auth(request)
    crypto = _sb_rest("blessing_paper_member_slots", {
        "select":"id,slot_no,display_label,assigned_user_id,mode,initial_capital,cash_balance,realized_pnl,fees_paid,enabled,status,updated_at,started_at,stopped_at",
        "order":"slot_no.asc","limit":"10"
    }) or []
    stocks = _sb_rest("blessing_stock_paper_member_slots", {
        "select":"id,slot_no,display_label,assigned_user_id,initial_capital,cash_balance,realized_pnl,fees_paid,enabled,status,updated_at",
        "order":"slot_no.asc","limit":"10"
    }) or []
    crypto_orders = _sb_rest("blessing_paper_slot_orders", {
        "select":"slot_id,side,symbol,status,reason,realized_pnl_thb,created_at",
        "order":"created_at.desc","limit":"20"
    }) or []
    stock_orders = _sb_rest("blessing_stock_paper_slot_orders", {
        "select":"slot_id,side,symbol,status,reason,realized_pnl_thb,created_at",
        "order":"created_at.desc","limit":"20"
    }) or []
    crypto_enabled = sum(1 for row in crypto if row.get("enabled"))
    stock_enabled = sum(1 for row in stocks if row.get("enabled"))
    return {
        "ok":True,
        "engine":dict(MEMBER_SLOT_ENGINE_STATE),
        "cryptoPaperSlots":crypto,
        "stockPaperSlots":stocks,
        "summary":{
            "cryptoEnabled":crypto_enabled,"cryptoTotal":len(crypto),
            "stocksEnabled":stock_enabled,"stocksTotal":len(stocks),
            "latestCryptoOrderAt":crypto_orders[0].get("created_at") if crypto_orders else None,
            "latestStockOrderAt":stock_orders[0].get("created_at") if stock_orders else None,
        },
        "recentCryptoOrders":crypto_orders,
        "recentStockOrders":stock_orders,
        "liveTradingEnabled":False
    }


# OWNER LIVE AUTO ENGINE — owner-only Binance TH Spot automation.
# Entries are allowed only after the Super Admin explicitly starts LIVE mode.
# STOP blocks new entries but keeps monitoring and closing already-owned positions.
# TEMPORARY SAFETY PAUSE approved by the owner while database authentication is repaired.
OWNER_LIVE_ENGINE_PAUSED = False
OWNER_LIVE_ENGINE_INTERVAL = max(10, int(os.getenv("OWNER_LIVE_ENGINE_INTERVAL", "15")))
OWNER_LIVE_ENGINE_STATE = {"status":"waiting_config","lastCycleAt":None,"lastError":None,
                           "cycles":0,"entries":0,"exits":0,"candidates":0,"actions":[]}
OWNER_LIVE_ENGINE_LOCK = threading.Lock()
OWNER_LIVE_ENTRY_COOLDOWNS = {}
OWNER_LIVE_MASTER_ON = lambda: os.getenv("BLESSING_OWNER_LIVE_ENABLED","false").strip().lower() in {"1","true","yes","on"}


def _owner_live_control(owner_id):
    rows = _sb_rest("blessing_owner_auto_trade_control", {
        "select":"owner_user_id,run_state,run_mode,live_approved,live_enabled,strategy_config,control_version",
        "owner_user_id":"eq."+str(owner_id),"limit":"1"
    }) or []
    return rows[0] if rows else None


def _owner_live_account():
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        raise RuntimeError("BINANCE_TH_API_CREDENTIALS_MISSING")
    payload = _binance_th_signed_request("GET","/api/v1/accountV2",{},BINANCE_API_KEY,BINANCE_API_SECRET)
    account = payload.get("data") if isinstance(payload,dict) and isinstance(payload.get("data"),dict) else payload
    if not isinstance(account,dict) or not account.get("canTrade"):
        raise RuntimeError("BINANCE_TH_ACCOUNT_CANNOT_TRADE")
    return account


def _owner_live_exchange_info(symbol=None):
    response = requests.get(BINANCE_TH_API_BASE+"/api/v1/exchangeInfo",
        headers={"Accept":"application/json","X-MBX-APIKEY":BINANCE_API_KEY},timeout=(5,15))
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload,dict) and isinstance(payload.get("data"),dict):
        payload = payload["data"]
    symbols = payload.get("symbols",[]) if isinstance(payload,dict) else []
    if symbol:
        return next((x for x in symbols if isinstance(x,dict) and x.get("symbol")==symbol),None)
    return symbols


def _owner_live_submit_market(owner_id, symbol, side, amount):
    """Submit one market order through the reserved-funds intent ledger; never retries ambiguous submits."""
    side = str(side).upper()
    symbol = str(symbol).upper()
    if side not in ("BUY","SELL") or not symbol.endswith("USDT"):
        raise RuntimeError("OWNER_LIVE_MARKET_SCOPE_REJECTED")
    account = _owner_live_account()
    info = _owner_live_exchange_info(symbol)
    if not info or info.get("status") != "TRADING" or "MARKET" not in (info.get("orderTypes") or []):
        raise RuntimeError("SYMBOL_NOT_MARKET_TRADING")
    base_asset = str(info.get("baseAsset") or "").upper()
    quote_asset = str(info.get("quoteAsset") or "").upper()
    if not base_asset or quote_asset != "USDT":
        raise RuntimeError("ONLY_USDT_SPOT_PAIRS_ALLOWED")
    filters = {str(x.get("filterType")):x for x in info.get("filters",[]) if isinstance(x,dict)}
    market_lot = filters.get("MARKET_LOT_SIZE") or {}
    lot = market_lot if Decimal(str(market_lot.get("stepSize") or "0")) > 0 else (filters.get("LOT_SIZE") or market_lot)
    step = Decimal(str(lot.get("stepSize") or "0"))
    min_qty = Decimal(str(lot.get("minQty") or "0"))
    max_qty = Decimal(str(lot.get("maxQty") or "0"))
    balances = {str(x.get("asset") or "").upper():Decimal(str(x.get("free") or "0"))
                for x in account.get("balances",[]) if isinstance(x,dict)}
    ticker_resp = requests.get(BINANCE_TH_API_BASE+"/api/v1/ticker/price",
        params={"symbol":symbol},headers={"Accept":"application/json"},timeout=(5,12))
    ticker_resp.raise_for_status()
    ticker = ticker_resp.json()
    if isinstance(ticker,dict) and isinstance(ticker.get("data"),dict):
        ticker = ticker["data"]
    price = Decimal(str(ticker.get("price") or "0"))
    if price <= 0:
        raise RuntimeError("MARKET_PRICE_UNAVAILABLE")
    client_id = "blsauto" + uuid.uuid4().hex[:24]
    if side == "BUY":
        quote_amount = _binance_th_order_decimal(amount,"quoteAmount")
        quote_free = balances.get("USDT",Decimal("0"))
        notional_filter = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}
        min_notional = Decimal(str(notional_filter.get("minNotional") or "0"))
        if quote_amount < min_notional or quote_amount > quote_free:
            raise RuntimeError("BUY_AMOUNT_OUTSIDE_FREE_BALANCE_OR_MIN_NOTIONAL")
        reserved_asset, reserved_amount = "USDT", quote_amount
        requested_qty, requested_quote = None, quote_amount
        params = {"symbol":symbol,"side":"BUY","type":"MARKET","quoteOrderQty":format(quote_amount,"f"),
                  "newClientOrderId":client_id,"newOrderRespType":"FULL"}
    else:
        quantity = _binance_th_step_floor(_binance_th_order_decimal(amount,"quantity"),step)
        if quantity <= 0 or quantity < min_qty or (max_qty > 0 and quantity > max_qty):
            raise RuntimeError("SELL_QUANTITY_OUTSIDE_MARKET_LOT")
        base_free = balances.get(base_asset,Decimal("0"))
        notional_filter = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}
        min_notional = Decimal(str(notional_filter.get("minNotional") or "0"))
        if quantity > base_free or quantity*price < min_notional:
            raise RuntimeError("SELL_QUANTITY_OUTSIDE_FREE_BALANCE_OR_MIN_NOTIONAL")
        reserved_asset, reserved_amount = base_asset, quantity
        requested_qty, requested_quote = quantity, None
        params = {"symbol":symbol,"side":"SELL","type":"MARKET","quantity":format(quantity,"f"),
                  "newClientOrderId":client_id,"newOrderRespType":"FULL"}
    reservation = _sb_rpc("blessing_owner_reserve_live_order",{
        "p_owner_user_id":str(owner_id),"p_client_order_id":client_id,"p_symbol":symbol,"p_side":side,
        "p_order_type":"MARKET","p_quote_asset":quote_asset,"p_base_asset":base_asset,
        "p_reserved_asset":reserved_asset,"p_reserved_amount":str(reserved_amount),
        "p_exchange_free":str(balances.get(reserved_asset,Decimal("0"))),
        "p_requested_quantity":str(requested_qty) if requested_qty is not None else None,
        "p_requested_quote_amount":str(requested_quote) if requested_quote is not None else None,
        "p_limit_price":None,"p_request_payload":{"origin":"owner_auto_engine","symbol":symbol,"side":side}
    })
    intent = reservation[0] if isinstance(reservation,list) and reservation else reservation
    intent_id = intent.get("id") if isinstance(intent,dict) else None
    if not intent_id:
        raise RuntimeError("LIVE_ORDER_RESERVATION_FAILED")
    _sb_rest("blessing_owner_live_order_intents",{"id":"eq."+str(intent_id)},"PATCH",
             {"status":"SUBMITTING","updated_at":datetime.now(timezone.utc).isoformat()})
    try:
        result = _binance_th_signed_request("POST","/api/v1/order",params,BINANCE_API_KEY,BINANCE_API_SECRET)
    except Exception as submit_error:
        try:
            result = _binance_th_signed_request("GET","/api/v1/order",
                {"symbol":symbol,"origClientOrderId":client_id},BINANCE_API_KEY,BINANCE_API_SECRET)
        except Exception:
            now = datetime.now(timezone.utc).isoformat()
            _sb_rest("blessing_owner_live_order_intents",{"id":"eq."+str(intent_id)},"PATCH",{
                "status":"RECONCILIATION_REQUIRED","error_code":"SUBMIT_RESULT_AMBIGUOUS",
                "error_message":type(submit_error).__name__,"updated_at":now})
            _sb_rest("blessing_owner_live_order_events",{},"POST",{
                "order_intent_id":intent_id,"event_type":"RECONCILIATION_REQUIRED",
                "event_data":{"origin":"owner_auto_engine","reason":"submit_and_lookup_failed"}})
            raise RuntimeError("ORDER_RESULT_AMBIGUOUS; NO_RETRY")
    if isinstance(result,dict) and isinstance(result.get("data"),dict):
        result = result["data"]
    status = str(result.get("status") or "UNKNOWN").upper()
    executed = Decimal(str(result.get("executedQty") or "0"))
    cumulative = Decimal(str(result.get("cummulativeQuoteQty") or result.get("cumulativeQuoteQty") or "0"))
    mapped = status if status in {"NEW","PARTIALLY_FILLED","FILLED","CANCELED","REJECTED","EXPIRED"} else "RECONCILIATION_REQUIRED"
    terminal = mapped in {"FILLED","CANCELED","REJECTED","EXPIRED"}
    remaining = Decimal("0") if terminal else max(Decimal("0"),
        (requested_quote-cumulative) if side=="BUY" else (requested_qty-executed))
    now = datetime.now(timezone.utc).isoformat()
    _sb_rest("blessing_owner_live_order_intents",{"id":"eq."+str(intent_id)},"PATCH",{
        "exchange_order_id":str(result.get("orderId") or "") or None,"exchange_status":status,
        "executed_quantity":str(executed),"cumulative_quote_quantity":str(cumulative),"status":mapped,
        "remaining_reserved_amount":str(remaining),"exchange_response":result if isinstance(result,dict) else {},
        "finalized_at":now if terminal else None,"updated_at":now})
    _sb_rest("blessing_owner_live_order_events",{},"POST",{
        "order_intent_id":intent_id,"event_type":"AUTO_"+mapped,
        "event_data":{"symbol":symbol,"side":side,"executedQuantity":str(executed),"quoteQuantity":str(cumulative)}})
    if mapped != "FILLED" or executed <= 0 or cumulative <= 0:
        raise RuntimeError("LIVE_ORDER_NOT_FULLY_FILLED; CHECK_RECONCILIATION")
    avg_price = cumulative/executed
    fee_quote = sum((Decimal(str(fill.get("commission") or "0")) for fill in (result.get("fills") or [])
                     if str(fill.get("commissionAsset") or "").upper()=="USDT"),Decimal("0"))
    if fee_quote <= 0:
        fee_quote = cumulative*Decimal("0.001")
    return {"clientOrderId":client_id,"intentId":intent_id,"symbol":symbol,"side":side,
            "quantity":str(executed),"quoteAmount":str(cumulative),"averagePrice":str(avg_price),
            "feeQuote":str(fee_quote),"status":mapped}


def _owner_live_mark_and_exit(owner_id, positions):
    exits = 0
    for position in positions:
        symbol = str(position.get("symbol") or "")
        if not symbol:
            continue
        response = requests.get(BINANCE_TH_API_BASE+"/api/v1/ticker/price",
            params={"symbol":symbol},headers={"Accept":"application/json"},timeout=(4,10))
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload,dict) and isinstance(payload.get("data"),dict):
            payload = payload["data"]
        price = Decimal(str(payload.get("price") or "0"))
        if price <= 0:
            continue
        entry = Decimal(str(position.get("entry_price") or "0"))
        qty = Decimal(str(position.get("quantity") or "0"))
        stop = Decimal(str(position.get("stop_loss_price") or "0"))
        target = Decimal(str(position.get("take_profit_price") or "0"))
        pnl = (price-entry)*qty-Decimal(str(position.get("entry_fee_quote") or "0"))
        _sb_rest("blessing_owner_live_positions",{"id":"eq."+str(position["id"])},"PATCH",{
            "last_price":str(price),"unrealized_pnl":str(pnl),"updated_at":datetime.now(timezone.utc).isoformat()})
        reason = "STOP_LOSS" if stop > 0 and price <= stop else ("TAKE_PROFIT" if target > 0 and price >= target else None)
        if not reason:
            continue
        _sb_rest("blessing_owner_live_positions",{"id":"eq."+str(position["id"]),"status":"eq.OPEN"},"PATCH",
                 {"status":"CLOSING","updated_at":datetime.now(timezone.utc).isoformat()})
        try:
            result = _owner_live_submit_market(owner_id,symbol,"SELL",qty)
            sold = Decimal(result["quantity"])
            sell_value = Decimal(result["quoteAmount"])
            fee = Decimal(result["feeQuote"])
            realized = sell_value-(entry*sold)-Decimal(str(position.get("entry_fee_quote") or "0"))-fee
            left = max(Decimal("0"),qty-sold)
            _sb_rest("blessing_owner_live_positions",{"id":"eq."+str(position["id"])},"PATCH",{
                "quantity":str(left),"exit_price":result["averagePrice"],"exit_fee_quote":str(fee),
                "realized_pnl":str(realized),"exit_client_order_id":result["clientOrderId"],
                "status":"CLOSED" if left==0 else "OPEN",
                "closed_at":datetime.now(timezone.utc).isoformat() if left==0 else None,
                "updated_at":datetime.now(timezone.utc).isoformat()})
            exits += 1
            _sb_rest("blessing_owner_auto_trade_events",{},"POST",{
                "owner_user_id":str(owner_id),"event_type":reason,"run_mode":"LIVE",
                "event_data":{"symbol":symbol,"price":str(price),"realizedPnl":str(realized),
                              "clientOrderId":result["clientOrderId"]}})
        except Exception:
            _sb_rest("blessing_owner_live_positions",{"id":"eq."+str(position["id"])},"PATCH",
                     {"status":"RECONCILIATION_REQUIRED","updated_at":datetime.now(timezone.utc).isoformat()})
            raise
    return exits


def _owner_live_convert_one_asset_to_usdt(owner_id, account, positions):
    """Convert one unreserved free asset with a liquid direct USDT market; never touch locked or held funds."""
    held_bases = {str(p.get("base_asset") or "").upper() for p in positions
                  if p.get("status") in ("OPEN", "CLOSING", "RECONCILIATION_REQUIRED")}
    active_intents = _sb_rest("blessing_owner_live_order_intents", {
        "select": "reserved_asset,remaining_reserved_amount,status",
        "owner_user_id": "eq." + str(owner_id),
        "status": "in.(RESERVED,SUBMITTING,NEW,PARTIALLY_FILLED,UNKNOWN,RECONCILIATION_REQUIRED)",
        "limit": "1000"
    }) or []
    reserved = {}
    for row in active_intents:
        asset = str(row.get("reserved_asset") or "").upper()
        if asset:
            reserved[asset] = reserved.get(asset, Decimal("0")) + Decimal(str(row.get("remaining_reserved_amount") or "0"))
    symbols = _owner_live_exchange_info()
    by_symbol = {str(x.get("symbol") or "").upper(): x for x in symbols if isinstance(x, dict)}
    choices = []
    for row in account.get("balances", []):
        if not isinstance(row, dict):
            continue
        asset = str(row.get("asset") or "").upper()
        free = Decimal(str(row.get("free") or "0"))
        available = max(Decimal("0"), free - reserved.get(asset, Decimal("0")))
        if not asset or asset == "USDT" or asset in held_bases or available <= 0:
            continue
        symbol = asset + "USDT"
        info = by_symbol.get(symbol)
        if not info or info.get("status") != "TRADING" or "MARKET" not in (info.get("orderTypes") or []):
            continue
        if str(info.get("baseAsset") or "").upper() != asset or str(info.get("quoteAsset") or "").upper() != "USDT":
            continue
        filters = {str(x.get("filterType")): x for x in info.get("filters", []) if isinstance(x, dict)}
        lot = filters.get("MARKET_LOT_SIZE") or filters.get("LOT_SIZE") or {}
        step = Decimal(str(lot.get("stepSize") or "0"))
        qty = _binance_th_step_floor(available, step) if step > 0 else available
        min_qty = Decimal(str(lot.get("minQty") or "0"))
        max_qty = Decimal(str(lot.get("maxQty") or "0"))
        notional_filter = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}
        min_notional = Decimal(str(notional_filter.get("minNotional") or "0"))
        if qty <= 0 or qty < min_qty or (max_qty > 0 and qty > max_qty):
            continue
        try:
            ticker_resp = requests.get(BINANCE_TH_API_BASE + "/api/v1/ticker/24hr",
                params={"symbol": symbol}, timeout=(4, 8))
            ticker_resp.raise_for_status()
            ticker = ticker_resp.json()
            if isinstance(ticker, dict) and isinstance(ticker.get("data"), dict):
                ticker = ticker["data"]
            book_resp = requests.get(BINANCE_TH_API_BASE + "/api/v1/ticker/bookTicker",
                params={"symbol": symbol}, timeout=(4, 8))
            book_resp.raise_for_status()
            book = book_resp.json()
            if isinstance(book, dict) and isinstance(book.get("data"), dict):
                book = book["data"]
            bid = Decimal(str(book.get("bidPrice") or "0"))
            ask = Decimal(str(book.get("askPrice") or "0"))
            volume = Decimal(str(ticker.get("quoteVolume") or "0"))
            if bid <= 0 or ask < bid or volume < Decimal("100000"):
                continue
            spread_pct = (ask - bid) / bid * Decimal("100")
            value = qty * bid
            if spread_pct > Decimal("0.30") or value < min_notional:
                continue
            choices.append((value, -spread_pct, volume, asset, symbol, qty))
        except Exception:
            continue
    choices.sort(reverse=True)
    for value, neg_spread, volume, asset, symbol, qty in choices:
        try:
            result = _owner_live_submit_market(owner_id, symbol, "SELL", qty)
            _sb_rest("blessing_owner_auto_trade_events", {}, "POST", {
                "owner_user_id": str(owner_id), "event_type": "AUTO_CONVERT_TO_USDT", "run_mode": "LIVE",
                "event_data": {"asset": asset, "symbol": symbol, "soldQuantity": result["quantity"],
                               "receivedUSDT": result["quoteAmount"], "spreadPct": str(-neg_spread),
                               "clientOrderId": result["clientOrderId"]}
            })
            return result
        except Exception as exc:
            log.warning("Owner live conversion skipped %s (%s)", symbol, type(exc).__name__)
    return None


def _owner_live_engine_cycle():
    if OWNER_LIVE_ENGINE_PAUSED:
        OWNER_LIVE_ENGINE_STATE.update({"status":"paused","lastError":"TEMPORARY_OWNER_APPROVED_PAUSE","liveOrdersEnabled":False})
        return
    if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
        OWNER_LIVE_ENGINE_STATE.update({"status":"waiting_config","lastError":"SUPABASE_CONFIG_MISSING"})
        return
    if not OWNER_LIVE_ENGINE_LOCK.acquire(blocking=False):
        return
    try:
        owners = _sb_rest("blessing_owner_auto_trade_control",{
            "select":"owner_user_id,run_state,run_mode,live_approved,live_enabled,strategy_config,control_version",
            "limit":"5","order":"updated_at.desc"
        }) or []
        if not owners:
            OWNER_LIVE_ENGINE_STATE.update({"status":"waiting_owner_start","lastError":None})
            return
        control = owners[0]
        owner_id = str(control["owner_user_id"])
        live_allowed = (control.get("run_mode")=="LIVE" and bool(control.get("live_approved"))
                        and bool(control.get("live_enabled")) and OWNER_LIVE_MASTER_ON())
        if not live_allowed:
            OWNER_LIVE_ENGINE_STATE.update({"status":"locked","lastError":"LIVE_MASTER_OR_OWNER_APPROVAL_OFF"})
            return
        account = _owner_live_account()
        balances = {str(x.get("asset") or "").upper():Decimal(str(x.get("free") or "0"))
                    for x in account.get("balances",[]) if isinstance(x,dict)}
        usdt_row = next((x for x in account.get("balances",[]) if str(x.get("asset") or "").upper()=="USDT"),{})
        free_usdt = Decimal(str(usdt_row.get("free") or "0"))
        locked_usdt = Decimal(str(usdt_row.get("locked") or "0"))
        positions = _sb_rest("blessing_owner_live_positions",{
            "select":"id,symbol,base_asset,quantity,entry_price,entry_fee_quote,stop_loss_price,take_profit_price,status",
            "owner_user_id":"eq."+owner_id,"status":"in.(OPEN,CLOSING,RECONCILIATION_REQUIRED)","limit":"100"
        }) or []
        if any(p.get("status") in ("CLOSING","RECONCILIATION_REQUIRED") for p in positions):
            OWNER_LIVE_ENGINE_STATE.update({"status":"reconciliation_required","lastError":"OPEN_ORDER_REQUIRES_RECONCILIATION"})
            return
        active_intents = _sb_rest("blessing_owner_live_order_intents", {
            "select":"id,status", "owner_user_id":"eq."+owner_id,
            "status":"in.(RESERVED,SUBMITTING,NEW,PARTIALLY_FILLED,UNKNOWN,RECONCILIATION_REQUIRED)",
            "limit":"100"
        }) or []
        if active_intents:
            OWNER_LIVE_ENGINE_STATE.update({"status":"reconciliation_required","lastError":"LIVE_ORDER_INTENT_REQUIRES_RECONCILIATION"})
            return
        exits = _owner_live_mark_and_exit(owner_id,[p for p in positions if p.get("status")=="OPEN"])
        positions = _sb_rest("blessing_owner_live_positions",{
            "select":"id,symbol,quantity,entry_price,last_price,status",
            "owner_user_id":"eq."+owner_id,"status":"eq.OPEN","limit":"100"
        }) or []
        exposure = Decimal("0")
        for p in positions:
            exposure += Decimal(str(p.get("quantity") or "0"))*Decimal(str(p.get("last_price") or p.get("entry_price") or "0"))
        equity = free_usdt+locked_usdt+exposure
        held_bases = {str(p.get("base_asset") or "").upper() for p in positions}
        for balance in account.get("balances", []):
            if not isinstance(balance, dict):
                continue
            asset = str(balance.get("asset") or "").upper()
            free_amount = Decimal(str(balance.get("free") or "0"))
            if not asset or asset == "USDT" or asset in held_bases or free_amount <= 0:
                continue
            try:
                price_response = requests.get(
                    BINANCE_TH_API_BASE + "/api/v1/ticker/price", params={"symbol": asset + "USDT"},
                    headers={"Accept": "application/json"}, timeout=(4, 8)
                )
                price_response.raise_for_status()
                price_payload = price_response.json()
                if isinstance(price_payload, dict) and isinstance(price_payload.get("data"), dict):
                    price_payload = price_payload["data"]
                asset_price = Decimal(str(price_payload.get("price") or "0"))
                if asset_price > 0:
                    equity += free_amount * asset_price
            except Exception:
                pass
        today = datetime.now(timezone.utc).date().isoformat()
        risk_rows = _sb_rest("blessing_owner_live_daily_risk",{
            "select":"risk_date,start_equity_usdt,peak_equity_usdt,realized_pnl_usdt,halted",
            "owner_user_id":"eq."+owner_id,"risk_date":"eq."+today,"limit":"1"
        }) or []
        if not risk_rows:
            _sb_rest("blessing_owner_live_daily_risk",{}, "POST",{
                "owner_user_id":owner_id,"risk_date":today,"start_equity_usdt":str(equity),
                "peak_equity_usdt":str(equity),"realized_pnl_usdt":"0","halted":False})
            risk = {"start_equity_usdt":str(equity),"peak_equity_usdt":str(equity),"realized_pnl_usdt":"0","halted":False}
        else:
            risk = risk_rows[0]
        start_equity = Decimal(str(risk.get("start_equity_usdt") or "0"))
        daily_loss_pct = Decimal(str((start_equity-equity)/start_equity*100 if start_equity>0 else 0))
        cfg = control.get("strategy_config") if isinstance(control.get("strategy_config"),dict) else {}
        daily_limit = Decimal(str(cfg.get("max_daily_loss_pct",2)))
        halted = bool(risk.get("halted")) or daily_loss_pct >= daily_limit
        if halted and not risk.get("halted"):
            _sb_rest("blessing_owner_live_daily_risk",{
                "owner_user_id":"eq."+owner_id,"risk_date":"eq."+today
            },"PATCH",{"halted":True,"halt_reason":"MAX_DAILY_LOSS","updated_at":datetime.now(timezone.utc).isoformat()})
            _sb_rest("blessing_owner_auto_trade_events",{},"POST",{
                "owner_user_id":owner_id,"event_type":"DAILY_RISK_HALT","run_mode":"LIVE",
                "event_data":{"equity":str(equity),"startEquity":str(start_equity),"lossPct":str(daily_loss_pct)}})
        run_state = str(control.get("run_state") or "STOPPED")
        entries = 0
        candidates_count = 0
        if run_state=="RUNNING" and not halted:
            now = datetime.now(timezone.utc)
            stamp = state.get("lastCryptoMessageAt")
            try:
                age = (now-datetime.fromisoformat(str(stamp).replace("Z","+00:00"))).total_seconds() if stamp else 999999
            except Exception:
                age = 999999
            scan = altcoin_scanner(refresh=0)
            scan_age = 999999
            try:
                scan_stamp = scan.get("updatedAt")
                scan_age = (now-datetime.fromisoformat(str(scan_stamp).replace("Z","+00:00"))).total_seconds() if scan_stamp else 999999
            except Exception:
                scan_age = 999999
            scan_fresh = (scan.get("provider")=="Binance TH Spot API" and scan.get("status")=="ok"
                          and 0<=scan_age<=90)
            candidates = []
            if scan_fresh:
                open_symbols = {str(p.get("symbol") or "").upper() for p in positions}
                for row in (scan.get("candidates") or []):
                    symbol = str(row.get("symbol") or "").upper()
                    price_value = Decimal(str(row.get("price") or "0"))
                    stop_value = Decimal(str(row.get("atrStopPct") or "0"))
                    spread_value = Decimal(str(row.get("spreadPct") or "999"))
                    m1_value = Decimal(str(row.get("m1") or "-999"))
                    m5_value = Decimal(str(row.get("m5") or "-999"))
                    m15_value = Decimal(str(row.get("m15") or "-999"))
                    rvol_value = Decimal(str(row.get("rvol") or "0"))
                    ema20_value = Decimal(str(row.get("ema20") or "0"))
                    trend_ok = row.get("trendAligned") is True or row.get("opportunityTrendOk") is True
                    momentum_ok = m5_value > 0 and m15_value > 0 and m1_value > Decimal("-0.20")
                    confirmation_ok = (row.get("pullbackReclaim") is True or row.get("breakout") is True
                                       or (ema20_value > 0 and price_value > ema20_value and m1_value > 0))
                    # Moderate live gate: preserve 5m/15m direction and confirmation,
                    # while allowing mild 1m pullback and RVOL >= 0.8 to avoid
                    # requiring every strict balanced predicate at the same instant.
                    moderate_entry = bool(
                        trend_ok and momentum_ok and confirmation_ok
                        and rvol_value >= Decimal("0.8")
                        and Decimal("0") < spread_value <= Decimal("0.5")
                        and Decimal("0.8") <= stop_value <= Decimal("2.0")
                    )
                    if (moderate_entry and symbol.endswith("USDT")
                            and row.get("exchangeSymbol")==symbol and price_value > 0
                            and symbol not in open_symbols):
                        candidate = dict(row)
                        candidate["liveEntryModel"] = "MODERATE_EMA_MOMENTUM_CONFIRMATION"
                        candidate["liveEntryScore"] = (
                            (25 if trend_ok else 0) + (25 if momentum_ok else 0)
                            + (20 if confirmation_ok else 0)
                            + (10 if rvol_value >= Decimal("0.8") else 0)
                            + (10 if spread_value <= Decimal("0.5") else 0)
                            + (10 if Decimal("0.8") <= stop_value <= Decimal("2.0") else 0)
                        )
                        candidates.append(candidate)
            candidates.sort(key=lambda row:(float(row.get("liveEntryScore") or 0),
                                             float(row.get("balancedScore") or 0),
                                             float(row.get("rvol") or 0)),reverse=True)
            candidates_count = len(candidates)
            reject_counts = scan.get("signalDiagnostics") or {}
            log.info("Owner live moderate scan fresh=%s provider=%s evaluated=%s moderate_candidates=%s rejects=%s",
                     scan_fresh, scan.get("provider"), scan.get("evaluated"),
                     candidates_count, reject_counts)

            cfg = control.get("strategy_config") if isinstance(control.get("strategy_config"),dict) else {}
            max_positions = max(1,min(2,int(cfg.get("max_open_positions",2))))
            if len(positions)<max_positions and candidates:
                row = candidates[0]
                symbol = str(row["symbol"]).upper()
                risk_pct = max(0.05,min(1.0,float(cfg.get("risk_per_trade_pct",1.0))))
                stop_pct = max(0.8,min(2.0,float(row.get("atrStopPct") or 0)))
                target_pct = stop_pct*1.8
                cap_pct = max(1.0,min(50.0,float(cfg.get("max_position_pct",50))))
                risk_budget = equity*Decimal(str(risk_pct))/Decimal("100")
                risk_sized_notional = risk_budget/(Decimal(str(stop_pct))/Decimal("100"))
                allocation_room = max(Decimal("0"),equity*Decimal("0.95")-exposure)
                target_notional = min(risk_sized_notional,equity*Decimal(str(cap_pct))/Decimal("100"),allocation_room)
                notional = min(free_usdt,target_notional)
                if notional<target_notional and cfg.get("auto_convert_to_usdt") is True:
                    converted = _owner_live_convert_one_asset_to_usdt(owner_id,account,positions)
                    if converted:
                        account = _owner_live_account()
                        balances = {str(x.get("asset") or "").upper():Decimal(str(x.get("free") or "0"))
                                    for x in account.get("balances",[]) if isinstance(x,dict)}
                        free_usdt = balances.get("USDT",Decimal("0"))
                        notional = min(free_usdt,target_notional)
                if notional>0 and time.time()-OWNER_LIVE_ENTRY_COOLDOWNS.get(symbol,0)>=300:
                    try:
                        result = _owner_live_submit_market(owner_id,symbol,"BUY",notional)
                        qty = Decimal(result["quantity"])
                        avg = Decimal(result["averagePrice"])
                        stop = avg*(Decimal("1")-Decimal(str(stop_pct))/Decimal("100"))
                        target = avg*(Decimal("1")+Decimal(str(target_pct))/Decimal("100"))
                        _sb_rest("blessing_owner_live_positions",{},"POST",{
                            "owner_user_id":owner_id,"symbol":symbol,"base_asset":symbol[:-4],"quote_asset":"USDT",
                            "quantity":str(qty),"entry_price":str(avg),"entry_quote_amount":result["quoteAmount"],
                            "entry_fee_quote":result["feeQuote"],"stop_loss_price":str(stop),"take_profit_price":str(target),
                            "status":"OPEN","last_price":str(avg),"entry_client_order_id":result["clientOrderId"]})
                        _sb_rest("blessing_owner_auto_trade_events",{},"POST",{
                            "owner_user_id":owner_id,"event_type":"AUTO_BUY_FILLED","run_mode":"LIVE",
                            "event_data":{"symbol":symbol,"quantity":str(qty),"entryPrice":str(avg),
                                          "quoteAmount":result["quoteAmount"],"stopLoss":str(stop),
                                          "takeProfit":str(target),"strategy":"BALANCED_EMA_PULLBACK_ATR",
                                          "balancedScore":row.get("balancedScore"),
                                          "riskPerTradePct":risk_pct,"atrStopPct":stop_pct,
                                          "clientOrderId":result["clientOrderId"]}})
                        OWNER_LIVE_ENTRY_COOLDOWNS[symbol]=time.time()
                        entries += 1
                    except Exception as exc:
                        log.warning("Owner live balanced entry skipped %s (%s)",symbol,type(exc).__name__)
                        OWNER_LIVE_ENGINE_STATE["lastError"]=type(exc).__name__
        now_iso = datetime.now(timezone.utc).isoformat()
        _sb_rest("blessing_owner_auto_trade_control",{"owner_user_id":"eq."+owner_id},"PATCH",{
            "heartbeat_at":now_iso,"updated_at":now_iso,
            "last_error":OWNER_LIVE_ENGINE_STATE.get("lastError")})
        OWNER_LIVE_ENGINE_STATE.update({
            "status":"running" if run_state=="RUNNING" else "monitoring_stopped",
            "lastCycleAt":now_iso,"lastError":None if not halted else "DAILY_RISK_HALT",
            "cycles":OWNER_LIVE_ENGINE_STATE.get("cycles",0)+1,
            "entries":OWNER_LIVE_ENGINE_STATE.get("entries",0)+entries,
            "exits":OWNER_LIVE_ENGINE_STATE.get("exits",0)+exits,
            "candidates":candidates_count,"openPositions":len(positions),
            "dailyLossPct":float(daily_loss_pct),"dailyHalted":halted,
            "freeUSDT":str(free_usdt),"lockedUSDT":str(locked_usdt),
            "liveOrdersEnabled":True})
    except Exception as exc:
        OWNER_LIVE_ENGINE_STATE.update({"status":"error","lastError":type(exc).__name__,
                                        "lastCycleAt":datetime.now(timezone.utc).isoformat()})
        log.exception("Owner live auto engine cycle failed")
    finally:
        OWNER_LIVE_ENGINE_LOCK.release()


def _owner_live_engine_loop():
    while True:
        try:
            _owner_live_engine_cycle()
        except Exception:
            log.exception("Owner live engine loop failed")
        time.sleep(OWNER_LIVE_ENGINE_INTERVAL)


@app.on_event("startup")
async def owner_live_engine_startup():
    global OWNER_LIVE_ENGINE_THREAD
    if OWNER_LIVE_ENGINE_PAUSED:
        OWNER_LIVE_ENGINE_STATE.update({"status":"paused","lastError":"TEMPORARY_OWNER_APPROVED_PAUSE","liveOrdersEnabled":False})
        log.warning("Owner live auto engine startup skipped: temporary owner-approved pause")
        return
    if not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY):
        OWNER_LIVE_ENGINE_STATE["status"]="waiting_config"
        return
    if globals().get("OWNER_LIVE_ENGINE_THREAD") and OWNER_LIVE_ENGINE_THREAD.is_alive():
        return
    OWNER_LIVE_ENGINE_THREAD = threading.Thread(target=_owner_live_engine_loop,
        name="owner-live-auto-engine",daemon=True)
    OWNER_LIVE_ENGINE_THREAD.start()
    log.info("Owner live auto engine worker started; interval=%ss",OWNER_LIVE_ENGINE_INTERVAL)


@app.get("/binance-th/auto-trade/status")
def owner_live_auto_status(request: Request):
    owner = _binance_th_owner_auth(request)
    control = _owner_live_control(owner["id"])
    positions = _sb_rest("blessing_owner_live_positions",{
        "select":"id,symbol,quantity,entry_price,last_price,stop_loss_price,take_profit_price,unrealized_pnl,status,opened_at",
        "owner_user_id":"eq."+str(owner["id"]),"order":"opened_at.desc","limit":"100"
    }) or []
    return {"ok":True,"control":control,"engine":dict(OWNER_LIVE_ENGINE_STATE),
            "worker":dict(OWNER_LIVE_ENGINE_STATE),"positions":positions,"masterSwitch":OWNER_LIVE_MASTER_ON(),
            "liveOrdersEnabled":(not OWNER_LIVE_ENGINE_PAUSED) and OWNER_LIVE_MASTER_ON() and bool(control and control.get("live_enabled"))}


@app.post("/binance-th/auto-trade/start")
async def owner_live_auto_start(request: Request):
    if OWNER_LIVE_ENGINE_PAUSED:
        raise HTTPException(status_code=423,detail="ระบบ Owner Live Engine ถูกพักชั่วคราวเพื่อซ่อมระบบฐานข้อมูลและล็อกอิน")
    owner = _binance_th_owner_auth(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400,detail="ข้อมูลคำสั่งไม่ถูกต้อง")
    if body.get("confirmLiveTrading") is not True:
        raise HTTPException(status_code=428,detail="ต้องยืนยันเริ่มระบบเงินจริงอย่างชัดเจน")
    if not OWNER_LIVE_MASTER_ON():
        raise HTTPException(status_code=423,detail="สวิตช์เงินจริงฝั่งเซิร์ฟเวอร์ยังปิดอยู่")
    try:
        account = _owner_live_account()
        symbols = _owner_live_exchange_info()
        usdt_free = next((Decimal(str(x.get("free") or "0")) for x in account.get("balances",[])
                          if str(x.get("asset") or "").upper()=="USDT"),Decimal("0"))
        convertible_bases = {str(x.get("baseAsset") or "").upper() for x in symbols
                             if isinstance(x,dict) and x.get("status")=="TRADING"
                             and str(x.get("quoteAsset") or "").upper()=="USDT"
                             and "MARKET" in (x.get("orderTypes") or [])}
        has_convertible_asset = any(
            isinstance(x,dict) and str(x.get("asset") or "").upper() in convertible_bases
            and Decimal(str(x.get("free") or "0")) > 0
            for x in account.get("balances",[])
        )
        if usdt_free <= 0 and not has_convertible_asset:
            raise HTTPException(status_code=423,detail="ไม่พบ USDT Free หรือสินทรัพย์ Free ที่มีคู่ USDT Spot สำหรับแปลง")
        if not any(x.get("status")=="TRADING" and str(x.get("quoteAsset"))=="USDT"
                   and "MARKET" in (x.get("orderTypes") or []) for x in symbols if isinstance(x,dict)):
            raise HTTPException(status_code=503,detail="ไม่พบคู่ USDT Spot ที่รองรับ Market Order")
        active_intents = _sb_rest("blessing_owner_live_order_intents", {
            "select":"id,status", "owner_user_id":"eq."+str(owner["id"]),
            "status":"in.(RESERVED,SUBMITTING,NEW,PARTIALLY_FILLED,UNKNOWN,RECONCILIATION_REQUIRED)",
            "limit":"100"
        }) or []
        if active_intents:
            raise HTTPException(status_code=423,detail="มีคำสั่งซื้อขายเดิมที่ยังต้องตรวจสอบหรือรอปิดรายการ กรุณาเคลียร์ก่อนเริ่ม")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503,detail="ตรวจสอบบัญชี Binance TH ไม่ผ่าน: "+type(exc).__name__)
    uid = str(owner["id"])
    existing = _owner_live_control(uid)
    now = datetime.now(timezone.utc).isoformat()
    config = {"strategy_model":"BALANCED_EMA_PULLBACK_ATR","universe":"BINANCE_TH_SPOT_USDT",
              "max_open_positions":2,"risk_per_trade_pct":1.0,"max_position_pct":50,
              "max_daily_loss_pct":2,"stop_loss_pct":1.2,"take_profit_pct":2.16,
              "auto_convert_to_usdt":True,"min_usdt_reserve":0,
              "scan_interval_seconds":OWNER_LIVE_ENGINE_INTERVAL}
    if existing:
        _sb_rest("blessing_owner_auto_trade_control",{"owner_user_id":"eq."+uid},"PATCH",{
            "run_state":"RUNNING","run_mode":"LIVE","live_approved":True,"live_enabled":True,
            "strategy_config":config,"requested_by":uid,"requested_at":now,"started_at":now,
            "stopped_at":None,"last_error":None,"control_version":int(existing.get("control_version") or 0)+1,
            "updated_at":now})
    else:
        _sb_rest("blessing_owner_auto_trade_control",{},"POST",{
            "owner_user_id":uid,"run_state":"RUNNING","run_mode":"LIVE","live_approved":True,
            "live_enabled":True,"strategy_config":config,"requested_by":uid,
            "requested_at":now,"started_at":now,"control_version":1})
    _sb_rest("blessing_owner_auto_trade_events",{},"POST",{
        "owner_user_id":uid,"event_type":"OWNER_LIVE_STARTED","run_mode":"LIVE",
        "event_data":{"source":"owner_confirmed_start","freeUSDT":str(usdt_free)}})
    return {"ok":True,"runState":"RUNNING","runMode":"LIVE",
            "freeUSDT":str(usdt_free),"strategyModel":"BALANCED_EMA_PULLBACK_ATR",
            "maxOpenPositions":2,"riskPerTradePct":1.0,"maxPositionPct":50,
            "maxDailyLossPct":2,"stopLossPct":"ATR 0.8-2.0%","takeProfitR":1.8,
            "autoConvertToUSDT":True,
            "message":"ระบบเงินจริงเริ่มทำงานแล้ว; ระบบจะแปลงเฉพาะยอด Free ที่มีคู่ USDT และเปิดสถานะเมื่อพบสัญญาณผ่านตัวกรองเท่านั้น"}


@app.post("/binance-th/auto-trade/stop")
def owner_live_auto_stop(request: Request):
    owner = _binance_th_owner_auth(request)
    uid = str(owner["id"])
    control = _owner_live_control(uid)
    if not control:
        raise HTTPException(status_code=404,detail="ยังไม่มีการตั้งค่าระบบ Auto Trade")
    now = datetime.now(timezone.utc).isoformat()
    _sb_rest("blessing_owner_auto_trade_control",{"owner_user_id":"eq."+uid},"PATCH",{
        "run_state":"STOPPED","stopped_at":now,"requested_by":uid,"requested_at":now,
        "control_version":int(control.get("control_version") or 0)+1,"updated_at":now})
    _sb_rest("blessing_owner_auto_trade_events",{},"POST",{
        "owner_user_id":uid,"event_type":"OWNER_LIVE_STOPPED","run_mode":"LIVE",
        "event_data":{"source":"owner_confirmed_stop","openPositionsRemainMonitored":True}})
    return {"ok":True,"runState":"STOPPED",
            "message":"หยุดเปิดสถานะใหม่แล้ว; ระบบยังติดตามและปิดสถานะที่เปิดอยู่ตาม Stop Loss/Take Profit"}



# Webull REST market-data client: used for the complete US instrument universe.
_WEBULL_DATA_CLIENT = None
_WEBULL_DATA_CLIENT_LOCK = threading.Lock()

def _webull_data_client():
    global _WEBULL_DATA_CLIENT
    with _WEBULL_DATA_CLIENT_LOCK:
        if _WEBULL_DATA_CLIENT is None:
            with _WEBULL_SHARED_API_CLIENT_LOCK:
                api = _WEBULL_SHARED_API_CLIENT
            if api is None:
                raise RuntimeError("WEBULL_SHARED_AUTH_CLIENT_NOT_READY")
            from webull.data.data_client import DataClient
            _WEBULL_DATA_CLIENT = DataClient(api)
        return _WEBULL_DATA_CLIENT

def _webull_json(response):
    if hasattr(response, "raise_for_status"):
        response.raise_for_status()
    if hasattr(response, "json"):
        return response.json()
    return response


def _webull_overnight_permission_denied(exc):
    message = str(exc).upper()
    return ("MARKET_DATA_NOT_SUBSCRIBED" in message and
            ("US_STOCK OVERNIGHT" in message or "NIGHT TRADING STOCK QUOTES" in message))


def _webull_list_active_universe(force=False):
    global UNIVERSE_ASSETS, UNIVERSE_ASSETS_AT
    if UNIVERSE_ASSETS and not force and time.time() - UNIVERSE_ASSETS_AT < UNIVERSE_CACHE_SECONDS:
        return UNIVERSE_ASSETS
    if not (APP_KEY and APP_SECRET):
        raise RuntimeError("WEBULL_CREDENTIALS_MISSING")
    client = _webull_data_client()
    assets, seen = [], set()
    for category in ("US_STOCK", "US_ETF"):
        page_key = None
        for _ in range(500):
            response = client.instrument.list_instruments(category=category, pagination_key=page_key)
            payload = _webull_json(response)
            rows = payload.get("data", []) if isinstance(payload, dict) else payload if isinstance(payload, list) else []
            for item in rows:
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get("symbol") or "").strip().upper()
                if not symbol or symbol in seen or not symbol.replace(".", "").replace("-", "").isalnum():
                    continue
                seen.add(symbol)
                assets.append({"symbol":symbol,"exchange":str(item.get("exchange_code") or ""),
                               "name":item.get("name") or "", "category":category,
                               "instrumentId":item.get("instrument_id")})
            page_key = payload.get("pagination_key") if isinstance(payload, dict) else None
            if not page_key:
                break
            time.sleep(1.05)
    if not assets:
        raise RuntimeError("WEBULL_INSTRUMENT_UNIVERSE_EMPTY")
    UNIVERSE_ASSETS, UNIVERSE_ASSETS_AT = assets, time.time()
    return assets

def _webull_snapshot_batch(assets):
    """Fetch snapshots without allowing one unsupported symbol to poison a whole batch."""
    client = _webull_data_client()
    result = {}

    def fetch_safe_batch(batch, category):
        if not batch:
            return
        try:
            try:
                response = client.market_data.get_snapshot(
                    batch, category,
                    extend_hour_required=True,
                    overnight_required=True,
                )
            except Exception as overnight_exc:
                if not _webull_overnight_permission_denied(overnight_exc):
                    raise
                state["overnightMarketDataAvailable"] = False
                state["marketDataEntitlement"] = "regular_session_fallback"
                log.warning("Webull %s snapshot overnight entitlement unavailable; retrying without overnight data", category)
                response = client.market_data.get_snapshot(
                    batch, category,
                    extend_hour_required=True,
                    overnight_required=False,
                )
            payload = _webull_json(response)
            rows = payload if isinstance(payload, list) else payload.get("data", payload.get("snapshots", [])) if isinstance(payload, dict) else []
            for row in rows if isinstance(rows, list) else []:
                if not isinstance(row, dict):
                    continue
                symbol = str(row.get("symbol") or "").upper()
                if symbol:
                    result[symbol] = row
            time.sleep(0.25)
        except Exception as exc:
            error_text = str(exc)
            invalid_symbol = (
                "INVALID_SYMBOL" in error_text
                or "symbols does not exist in the category" in error_text.lower()
            )
            if not invalid_symbol:
                log.exception("Webull %s snapshot batch failed (%s)", category, type(exc).__name__)
                raise
            if len(batch) == 1:
                log.warning("Skipping unsupported Webull %s symbol %s", category, batch[0])
                return
            midpoint = len(batch) // 2
            log.warning(
                "Webull %s snapshot batch contains unsupported symbols; splitting %d symbols",
                category, len(batch)
            )
            fetch_safe_batch(batch[:midpoint], category)
            fetch_safe_batch(batch[midpoint:], category)

    for category in ("US_STOCK", "US_ETF"):
        symbols = list(dict.fromkeys(
            str(item.get("symbol") or "").strip().upper()
            for item in assets
            if item.get("category") == category and item.get("symbol")
        ))
        for offset in range(0, len(symbols), 100):
            fetch_safe_batch(symbols[offset:offset + 100], category)
    return result

def _webull_bar_batches(symbols):
    client = _webull_data_client()
    result = {}
    categories = {x["symbol"]:x.get("category","US_STOCK") for x in UNIVERSE_ASSETS}
    for category in ("US_STOCK","US_ETF"):
        category_symbols = [s for s in symbols if categories.get(s,"US_STOCK") == category]
        for offset in range(0,len(category_symbols),100):
            batch = category_symbols[offset:offset+100]
            if not batch:
                continue
            try:
                try:
                    response = client.market_data.get_batch_history_bar(
                        batch, category, "M1", count="15",
                        real_time_required=True, trading_sessions="ALL"
                    )
                except Exception as overnight_exc:
                    if not _webull_overnight_permission_denied(overnight_exc):
                        raise
                    state["overnightMarketDataAvailable"] = False
                    state["marketDataEntitlement"] = "regular_session_fallback"
                    log.warning("Webull %s ALL-session bars not entitled; retrying CORE session only", category)
                    response = client.market_data.get_batch_history_bar(
                        batch, category, "M1", count="15",
                        real_time_required=True, trading_sessions="CORE"
                    )
                payload = _webull_json(response)
                data = payload.get("data",payload.get("bars",payload)) if isinstance(payload,dict) else payload
                if isinstance(data,dict):
                    for symbol,bars in data.items():
                        if isinstance(bars,dict):
                            bars = bars.get("bars") or bars.get("data") or []
                        result[str(symbol).upper()] = bars if isinstance(bars,list) else []
                elif isinstance(data,list):
                    for row in data:
                        if isinstance(row,dict) and row.get("symbol"):
                            result[str(row["symbol"]).upper()] = row.get("bars") or row.get("data") or []
            except Exception as exc:
                log.warning("Webull %s minute-bar batch failed (%s)",category,type(exc).__name__)
            time.sleep(1.05)
    return result

def _normalize_webull_snapshot(row):
    price = _num(row.get("price"), _num(row.get("extend_hour_last_price"), _num(row.get("ovn_price"))))
    open_price = _num(row.get("open"), _num(row.get("pre_close")))
    volume = _num(row.get("volume"), _num(row.get("extend_hour_volume"), _num(row.get("ovn_volume"), 0))) or 0
    bid, ask = _num(row.get("bid")), _num(row.get("ask"))
    stamp = row.get("last_trade_time") or row.get("extend_hour_last_trade_time") or row.get("ovn_last_trade_time")
    if isinstance(stamp, (int, float)):
        stamp = datetime.fromtimestamp(stamp/1000, timezone.utc).isoformat()
    return {
        "latestTrade":{"p":price,"t":stamp},
        "latestQuote":{"bp":bid,"ap":ask},
        "minuteBar":{"c":price,"t":stamp},
        "dailyBar":{"o":open_price,"c":price,"v":volume},
        "prevDailyBar":{"c":_num(row.get("pre_close")),"v":0},
    }


# === SET/mai market-data bridge ===
# Thailand listed-equity market data is provided by the Stock Exchange of Thailand,
# not Webull OpenAPI. This endpoint is read-only and does not place orders.
SET_MARKET_DATA_API_KEY = (os.getenv("SET_MARKET_DATA_API_KEY") or "").strip()
SET_MARKET_DATA_MODE = (os.getenv("SET_MARKET_DATA_MODE") or "realtime").strip().lower()
if SET_MARKET_DATA_MODE not in {"realtime", "delay"}:
    SET_MARKET_DATA_MODE = "realtime"
SET_MARKET_DATA_CACHE_SECONDS = max(10, min(120, int(os.getenv("SET_MARKET_DATA_CACHE_SECONDS", "30"))))
SET_MARKET_DATA_STATE: dict[str, Any] = {
    "provider": "Stock Exchange of Thailand",
    "status": "waiting_credentials" if not SET_MARKET_DATA_API_KEY else "not_checked",
    "market": ["SET", "mai"],
    "universeCount": 0,
    "updatedAt": None,
    "lastError": None,
    "mode": SET_MARKET_DATA_MODE,
}
SET_MARKET_DATA_CACHE: dict[str, Any] = {"at": 0.0, "payload": None}
SET_MARKET_DATA_LOCK = threading.Lock()


def _set_market_data_fetch(force: bool = False) -> dict[str, Any]:
    """Fetch the official SET/mai all-listed-stock quote feed; never fabricate prices."""
    if not SET_MARKET_DATA_API_KEY:
        return {
            "status": "waiting_credentials",
            "provider": "Stock Exchange of Thailand",
            "market": ["SET", "mai"],
            "message": "ต้องตั้งค่า SET_MARKET_DATA_API_KEY ใน Render ก่อนเปิดข้อมูลหุ้นไทย",
            "quotes": [],
            "universeCount": 0,
            "readOnly": True,
        }
    now = time.time()
    with SET_MARKET_DATA_LOCK:
        cached = SET_MARKET_DATA_CACHE.get("payload")
        if cached and not force and now - SET_MARKET_DATA_CACHE.get("at", 0) < SET_MARKET_DATA_CACHE_SECONDS:
            return {**cached, "cached": True}
    mode_path = "realtime-data" if SET_MARKET_DATA_MODE == "realtime" else "delay-data"
    url = f"https://marketplace.set.or.th/api/public/{mode_path}/stock"
    try:
        response = requests.get(
            url,
            params={"market": "SET,mai", "oddLotFlag": "false"},
            headers={"api-key": SET_MARKET_DATA_API_KEY, "Accept": "application/json"},
            timeout=(5, 20),
        )
        response.raise_for_status()
        body = response.json()
        if isinstance(body, list):
            rows = body
        elif isinstance(body, dict):
            rows = body.get("data") or body.get("result") or body.get("stocks") or []
            if isinstance(rows, dict):
                rows = rows.get("data") or rows.get("items") or []
        else:
            rows = []
        quotes = []
        newest = None
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            stamp = row.get("time")
            if stamp and (newest is None or str(stamp) > str(newest)):
                newest = stamp
            last = row.get("last")
            prior = row.get("prior")
            try:
                change = float(last) - float(prior) if last is not None and prior is not None else None
                change_pct = (change / float(prior) * 100.0) if change is not None and float(prior) else None
            except (TypeError, ValueError, ZeroDivisionError):
                change, change_pct = None, None
            quotes.append({
                "symbol": symbol,
                "name": row.get("fullName"),
                "market": row.get("market"),
                "securityType": row.get("securityType"),
                "industry": row.get("industry"),
                "sector": row.get("sector"),
                "price": last,
                "priorClose": prior,
                "change": change,
                "changePercent": change_pct,
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "volume": row.get("totalVolume", row.get("aomVolume")),
                "value": row.get("totalValue", row.get("aomValue")),
                "bid": row.get("bid") or [],
                "offer": row.get("offer") or [],
                "timestamp": stamp,
                "source": "SET official Market Data API",
                "currency": "THB",
            })
        quotes.sort(key=lambda x: (x.get("changePercent") is not None, x.get("changePercent") or 0), reverse=True)
        payload = {
            "status": "ok",
            "provider": "Stock Exchange of Thailand",
            "market": ["SET", "mai"],
            "mode": SET_MARKET_DATA_MODE,
            "updatedAt": datetime.now(timezone.utc).isoformat(),
            "dataTime": newest,
            "universeCount": len(quotes),
            "quotes": quotes,
            "cached": False,
            "readOnly": True,
            "message": "ข้อมูลหุ้น SET/mai จาก API ทางการ; ระบบนี้ยังไม่ส่งคำสั่งซื้อขาย",
        }
        with SET_MARKET_DATA_LOCK:
            SET_MARKET_DATA_CACHE.update({"at": now, "payload": payload})
            SET_MARKET_DATA_STATE.update({
                "status": "ok", "universeCount": len(quotes),
                "updatedAt": payload["updatedAt"], "lastError": None,
                "dataTime": newest, "mode": SET_MARKET_DATA_MODE,
            })
        return payload
    except Exception as exc:
        error = type(exc).__name__
        with SET_MARKET_DATA_LOCK:
            SET_MARKET_DATA_STATE.update({"status": "source_error", "lastError": error})
        log.exception("SET/mai market-data fetch failed (%s)", error)
        return {
            "status": "source_error",
            "provider": "Stock Exchange of Thailand",
            "market": ["SET", "mai"],
            "lastError": error,
            "quotes": [],
            "universeCount": 0,
            "readOnly": True,
            "message": "ดึงข้อมูลจาก SET ไม่สำเร็จ; ไม่ใช้ข้อมูลจากแหล่งอื่นแทน",
        }


@app.get("/th-stock-market")
def th_stock_market(refresh: int = 0):
    """Read-only all-listed-stock universe and quote feed for SET and mai."""
    return _set_market_data_fetch(force=bool(refresh))


@app.get("/th-stock-market/health")
def th_stock_market_health():
    with SET_MARKET_DATA_LOCK:
        return {
            **SET_MARKET_DATA_STATE,
            "configured": bool(SET_MARKET_DATA_API_KEY),
            "readOnly": True,
        }
