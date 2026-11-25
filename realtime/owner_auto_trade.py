"""Owner-only Binance TH Spot auto-trading engine.

The engine is deliberately fail-closed: it starts only after an authenticated
Super Admin START request, a server-side master switch, account preflight, and
explicit live approval. It never uses member portfolio credentials or balances.
"""
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN

from fastapi import HTTPException, Request

log = logging.getLogger("blessing-owner-auto-trade")
OWNER_ID = "62835b65-13eb-4d99-9647-c4b22bd61ef3"
# Temporary owner-approved pause during authentication/database recovery.
OWNER_LIVE_ENGINE_PAUSED = False
ACTIVE_STATES = {"RUNNING", "STOPPING"}
TERMINAL = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}
_worker_lock = threading.Lock()
_worker_started = False
_worker_state = {"status": "starting", "lastCycleAt": None, "lastError": None, "cycles": 0, "entries": 0, "exits": 0, "freeUSDT": None, "dailyLossPct": None, "lastAction": None}


def _d(value, default="0"):
    try:
        x = Decimal(str(value if value is not None else default))
        return x if x.is_finite() else Decimal(default)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _fmt(value):
    return format(value, "f")


def _master_on():
    return os.getenv("BLESSING_OWNER_LIVE_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}


def register_owner_auto_trade(app, owner_auth, sb_rest, sb_rpc, signed_request,
                              api_key, api_secret, api_base, scanner, market_state):
    def account_snapshot():
        if not api_key or not api_secret:
            raise HTTPException(status_code=503, detail="ยังไม่ได้ตั้งค่า Binance TH API Key/Secret บน Render")
        payload = signed_request("GET", "/api/v1/accountV2", {}, api_key, api_secret)
        account = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else payload
        if not isinstance(account, dict) or not account.get("canTrade"):
            raise HTTPException(status_code=423, detail="บัญชี Binance TH ยังไม่อนุญาตให้ซื้อขาย")
        return account

    def owner_control(uid):
        rows = sb_rest("blessing_owner_auto_trade_control", {
            "select": "*", "owner_user_id": "eq." + str(uid), "limit": "1"
        }) or []
        if rows:
            return rows[0]
        created = sb_rest("blessing_owner_auto_trade_control", {}, "POST", {
            "owner_user_id": str(uid), "run_state": "STOPPED", "run_mode": "PAPER",
            "live_approved": False, "live_enabled": False
        }) or []
        return created[0] if created else {}

    def record_event(uid, event, mode, data=None):
        sb_rest("blessing_owner_auto_trade_events", {}, "POST", {
            "owner_user_id": str(uid), "event_type": event, "run_mode": mode,
            "event_data": data or {}
        })

    def fresh_price(symbol):
        quote = (market_state.get("crypto_quotes") or {}).get(symbol)
        if isinstance(quote, dict):
            try:
                stamp = datetime.fromisoformat(str(quote.get("receivedAt")).replace("Z", "+00:00"))
                age = (datetime.now(timezone.utc) - stamp).total_seconds()
                price = _d(quote.get("price"))
                if 0 <= age <= 8 and price > 0:
                    return price
            except Exception:
                pass
        import requests
        resp = requests.get(api_base + "/api/v1/ticker/price", params={"symbol": symbol}, timeout=(4, 8))
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            data = data["data"]
        price = _d(data.get("price"))
        if price <= 0:
            raise RuntimeError("LIVE_PRICE_UNAVAILABLE")
        return price

    def exchange_rules(symbol):
        import requests
        resp = requests.get(api_base + "/api/v1/exchangeInfo",
                            headers={"Accept": "application/json", "X-MBX-APIKEY": api_key},
                            timeout=(5, 12))
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            data = data["data"]
        info = next((x for x in data.get("symbols", []) if x.get("symbol") == symbol), None)
        if not info or info.get("status") != "TRADING" or "MARKET" not in (info.get("orderTypes") or []):
            raise RuntimeError("SYMBOL_NOT_MARKET_TRADABLE")
        quote_asset = str(info.get("quoteAsset") or "").upper()
        # Strategy entries remain USDT-quoted. The sole THB exception is
        # USDTTHB BUY, used only to convert free THB into the engine's USDT cash.
        if quote_asset != "USDT" and not (
            symbol == "USDTTHB" and str(info.get("baseAsset") or "").upper() == "USDT"
            and quote_asset == "THB"
        ):
            raise RuntimeError("AUTO_ENGINE_UNSUPPORTED_QUOTE_ASSET")
        filters = {str(x.get("filterType")): x for x in info.get("filters", []) if isinstance(x, dict)}
        lot = filters.get("MARKET_LOT_SIZE") or {}
        if _d(lot.get("stepSize")) <= 0:
            lot = filters.get("LOT_SIZE") or {}
        notional = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}
        return info, lot, _d(notional.get("minNotional"))

    def market_order(uid, symbol, side, quote_amount=None, quantity=None, position_id=None):
        account = account_snapshot()
        balances = {str(x.get("asset") or "").upper(): _d(x.get("free")) for x in account.get("balances", []) if isinstance(x, dict)}
        info, lot, min_notional = exchange_rules(symbol)
        base = str(info.get("baseAsset") or "").upper()
        quote = str(info.get("quoteAsset") or "").upper()
        price = fresh_price(symbol)
        step = _d(lot.get("stepSize"))
        min_qty = _d(lot.get("minQty"))
        max_qty = _d(lot.get("maxQty"))
        client_id = "blsa" + uuid.uuid4().hex[:28]
        if side == "BUY":
            amount = _d(quote_amount)
            if amount <= 0 or amount < min_notional or amount > balances.get(quote, Decimal("0")):
                raise RuntimeError("BUY_AMOUNT_OUTSIDE_FREE_BALANCE_OR_MIN_NOTIONAL")
            reserve_asset, reserve_amount = quote, amount
            params = {"symbol": symbol, "side": "BUY", "type": "MARKET",
                      "quoteOrderQty": _fmt(amount), "newClientOrderId": client_id, "newOrderRespType": "FULL"}
            requested_quote, requested_qty = amount, None
        else:
            qty = min(_d(quantity), balances.get(base, Decimal("0")))
            if step > 0:
                qty = (qty / step).to_integral_value(rounding=ROUND_DOWN) * step
            if qty <= 0 or qty < min_qty or (max_qty > 0 and qty > max_qty):
                raise RuntimeError("SELL_QUANTITY_OUTSIDE_MARKET_LOT")
            if qty > balances.get(base, Decimal("0")) or qty * price < min_notional:
                raise RuntimeError("SELL_AMOUNT_OUTSIDE_FREE_BALANCE_OR_MIN_NOTIONAL")
            reserve_asset, reserve_amount = base, qty
            params = {"symbol": symbol, "side": "SELL", "type": "MARKET",
                      "quantity": _fmt(qty), "newClientOrderId": client_id, "newOrderRespType": "FULL"}
            requested_quote, requested_qty = None, qty

        reserved = sb_rpc("blessing_owner_reserve_live_order", {
            "p_owner_user_id": uid, "p_client_order_id": client_id, "p_symbol": symbol,
            "p_side": side, "p_order_type": "MARKET", "p_quote_asset": quote,
            "p_base_asset": base, "p_reserved_asset": reserve_asset,
            "p_reserved_amount": _fmt(reserve_amount), "p_exchange_free": _fmt(balances.get(reserve_asset, Decimal("0"))),
            "p_requested_quantity": _fmt(requested_qty) if requested_qty is not None else None,
            "p_requested_quote_amount": _fmt(requested_quote) if requested_quote is not None else None,
            "p_limit_price": None, "p_request_payload": {"strategy": "BLESSING_AUTO", "positionId": position_id}
        })
        intent = reserved[0] if isinstance(reserved, list) and reserved else reserved
        intent_id = intent.get("id") if isinstance(intent, dict) else None
        if not intent_id:
            raise RuntimeError("LIVE_ORDER_RESERVATION_FAILED")
        sb_rest("blessing_owner_live_order_intents", {"id": "eq." + str(intent_id)}, "PATCH",
                {"status": "SUBMITTING", "updated_at": datetime.now(timezone.utc).isoformat()})
        try:
            result = signed_request("POST", "/api/v1/order", params, api_key, api_secret)
            if isinstance(result, dict) and isinstance(result.get("data"), dict):
                result = result["data"]
        except Exception as submit_exc:
            try:
                result = signed_request("GET", "/api/v1/order",
                                        {"symbol": symbol, "origClientOrderId": client_id}, api_key, api_secret)
                if isinstance(result, dict) and isinstance(result.get("data"), dict):
                    result = result["data"]
            except Exception:
                sb_rest("blessing_owner_live_order_intents", {"id": "eq." + str(intent_id)}, "PATCH", {
                    "status": "RECONCILIATION_REQUIRED", "error_code": "SUBMIT_RESULT_AMBIGUOUS",
                    "error_message": type(submit_exc).__name__, "updated_at": datetime.now(timezone.utc).isoformat()
                })
                record_event(uid, "RECONCILIATION_REQUIRED", "LIVE", {"clientOrderId": client_id})
                raise RuntimeError("ORDER_SUBMIT_REQUIRES_RECONCILIATION")
        exchange_status = str(result.get("status") or "UNKNOWN").upper()
        status = exchange_status if exchange_status in {"NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "EXPIRED"} else "RECONCILIATION_REQUIRED"
        executed = _d(result.get("executedQty"))
        cumulative = _d(result.get("cummulativeQuoteQty") or result.get("cumulativeQuoteQty"))
        remaining = Decimal("0") if status in TERMINAL else max(
            Decimal("0"), (requested_quote - cumulative) if side == "BUY" else (requested_qty - executed)
        )
        now = datetime.now(timezone.utc).isoformat()
        sb_rest("blessing_owner_live_order_intents", {"id": "eq." + str(intent_id)}, "PATCH", {
            "exchange_order_id": str(result.get("orderId") or "") or None,
            "exchange_status": exchange_status, "executed_quantity": _fmt(executed),
            "cumulative_quote_quantity": _fmt(cumulative), "status": status,
            "remaining_reserved_amount": _fmt(remaining), "exchange_response": result if isinstance(result, dict) else {},
            "updated_at": now, "finalized_at": now if status in TERMINAL else None
        })
        sb_rest("blessing_owner_live_order_events", {}, "POST", {
            "order_intent_id": intent_id, "event_type": "AUTO_" + status,
            "event_data": {"symbol": symbol, "side": side, "executedQty": _fmt(executed), "quoteQty": _fmt(cumulative)}
        })
        if status not in {"FILLED", "PARTIALLY_FILLED"} or executed <= 0 or cumulative <= 0:
            raise RuntimeError("ORDER_NOT_FILLED_OR_PARTIAL")
        return {
            "clientOrderId": client_id, "intentId": intent_id, "status": status,
            "executedQty": executed, "quoteQty": cumulative,
            "avgPrice": cumulative / executed, "baseAsset": base, "quoteAsset": quote,
            "exchangeResponse": result,
        }


    def convert_best_asset_to_usdt(uid, account, positions, config):
        """Convert one suitable, unreserved altcoin balance to USDT when trading cash is low."""
        if config.get("auto_convert_to_usdt") is not True:
            return None
        held = {str(p.get("base_asset") or "").upper() for p in positions
                if p.get("status") in ("OPEN", "CLOSING", "RECONCILIATION_REQUIRED")}
        active = sb_rest("blessing_owner_live_order_intents", {
            "select": "reserved_asset,remaining_reserved_amount",
            "owner_user_id": "eq." + str(uid),
            "status": "in.(RESERVED,SUBMITTING,NEW,PARTIALLY_FILLED,UNKNOWN,RECONCILIATION_REQUIRED)",
            "limit": "1000"
        }) or []
        reserved = {}
        for row in active:
            asset = str(row.get("reserved_asset") or "").upper()
            reserved[asset] = reserved.get(asset, Decimal("0")) + _d(row.get("remaining_reserved_amount"))
        protected = {"USDT"}  # Keep USDT as quote cash; other eligible free assets may be converted.

        # THB is the account's fiat quote balance, not an altcoin. Convert it
        # through the Binance TH USDTTHB market, if that market is live and
        # the free amount meets its current filters and liquidity checks.
        thb_free = max(Decimal("0"), _d(next(
            (x.get("free") for x in account.get("balances", [])
             if str(x.get("asset") or "").upper() == "THB"), 0
        )) - reserved.get("THB", Decimal("0")))
        if thb_free > 0:
            try:
                info, lot, min_notional = exchange_rules("USDTTHB")
                if (str(info.get("baseAsset") or "").upper() == "USDT"
                        and str(info.get("quoteAsset") or "").upper() == "THB"
                        and thb_free >= min_notional):
                    import requests
                    book_resp = requests.get(api_base + "/api/v1/ticker/bookTicker",
                                             params={"symbol": "USDTTHB"}, timeout=(4, 8))
                    book_resp.raise_for_status()
                    book = book_resp.json()
                    if isinstance(book, dict) and isinstance(book.get("data"), dict):
                        book = book["data"]
                    bid, ask = _d(book.get("bidPrice")), _d(book.get("askPrice"))
                    price = fresh_price("USDTTHB")
                    if bid > 0 and ask >= bid and price > 0:
                        spread_pct = (ask - bid) / price * Decimal("100")
                        if spread_pct <= Decimal("0.30"):
                            fill = market_order(uid, "USDTTHB", "BUY", quote_amount=thb_free)
                            record_event(uid, "AUTO_CONVERT_THB_TO_USDT", "LIVE", {
                                "symbol": "USDTTHB", "spentTHB": _fmt(fill["quoteQty"]),
                                "receivedUSDT": _fmt(fill["executedQty"]),
                                "spreadPct": _fmt(spread_pct),
                                "clientOrderId": fill["clientOrderId"]
                            })
                            _worker_state["lastAction"] = {
                                "action": "CONVERT_THB_TO_USDT", "spentTHB": _fmt(fill["quoteQty"]),
                                "receivedUSDT": _fmt(fill["executedQty"])
                            }
                            return fill
            except Exception as exc:
                log.info("THB-to-USDT conversion skipped: %s", type(exc).__name__)

        import requests
        choices = []
        for row in account.get("balances", []):
            asset = str(row.get("asset") or "").upper()
            free = _d(row.get("free"))
            available = max(Decimal("0"), free - reserved.get(asset, Decimal("0")))
            if not asset or asset in protected or asset in held or available <= 0:
                continue
            symbol = asset + "USDT"
            try:
                info, lot, min_notional = exchange_rules(symbol)
                if str(info.get("baseAsset") or "").upper() != asset or str(info.get("quoteAsset") or "").upper() != "USDT":
                    continue
                ticker_resp = requests.get(api_base + "/api/v1/ticker/24hr",
                                           params={"symbol": symbol}, timeout=(4, 8))
                ticker_resp.raise_for_status()
                ticker = ticker_resp.json()
                if isinstance(ticker, dict) and isinstance(ticker.get("data"), dict):
                    ticker = ticker["data"]
                book_resp = requests.get(api_base + "/api/v1/ticker/bookTicker",
                                          params={"symbol": symbol}, timeout=(4, 8))
                book_resp.raise_for_status()
                book = book_resp.json()
                if isinstance(book, dict) and isinstance(book.get("data"), dict):
                    book = book["data"]
                bid, ask = _d(book.get("bidPrice")), _d(book.get("askPrice"))
                price = _d(ticker.get("lastPrice"))
                volume = _d(ticker.get("quoteVolume"))
                change = _d(ticker.get("priceChangePercent"))
                if bid <= 0 or ask < bid or price <= 0 or volume < Decimal("100000"):
                    continue
                spread_pct = (ask - bid) / price * Decimal("100")
                value = available * bid
                if spread_pct > Decimal("0.30") or value < min_notional:
                    continue
                # AI-style selection: prioritize weaker 24h assets with liquid, tight books.
                choices.append((change, spread_pct, -volume, asset, symbol, available, value))
            except Exception:
                continue
        if not choices:
            return None
        choices.sort(key=lambda x: (x[0], x[1], x[2]))
        change, spread, neg_volume, asset, symbol, quantity, value = choices[0]
        fill = market_order(uid, symbol, "SELL", quantity=quantity)
        record_event(uid, "AUTO_CONVERT_TO_USDT", "LIVE", {
            "asset": asset, "symbol": symbol, "soldQuantity": _fmt(fill["executedQty"]),
            "receivedUSDT": _fmt(fill["quoteQty"]), "change24hPct": _fmt(change),
            "spreadPct": _fmt(spread), "clientOrderId": fill["clientOrderId"],
            "selection": "weakest_24h_performance_with_liquidity_and_spread_filters"
        })
        _worker_state["lastAction"] = {
            "action": "CONVERT_TO_USDT", "asset": asset, "symbol": symbol,
            "receivedUSDT": _fmt(fill["quoteQty"])
        }
        return fill

    def close_position(uid, pos, price, reason, config):
        pid = str(pos["id"])
        qty = _d(pos.get("quantity"))
        if qty <= 0:
            return
        sb_rest("blessing_owner_live_positions", {"id": "eq." + pid}, "PATCH", {
            "status": "CLOSING", "last_price": _fmt(price),
            "last_checked_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        })
        try:
            fill = market_order(uid, str(pos["symbol"]), "SELL", quantity=qty, position_id=pid)
            sold = fill["executedQty"]
            remaining = max(Decimal("0"), qty - sold)
            pnl = (fill["avgPrice"] - _d(pos.get("average_entry_price"))) * sold
            new_status = "CLOSED" if remaining <= 0 else "OPEN"
            sb_rest("blessing_owner_live_positions", {"id": "eq." + pid}, "PATCH", {
                "quantity": _fmt(remaining) if remaining > 0 else _fmt(sold),
                "status": new_status, "exit_client_order_id": fill["clientOrderId"],
                "close_price": _fmt(fill["avgPrice"]) if remaining <= 0 else None,
                "realized_pnl": _fmt(pnl), "closed_at": datetime.now(timezone.utc).isoformat() if remaining <= 0 else None,
                "last_price": _fmt(price), "updated_at": datetime.now(timezone.utc).isoformat(),
                "metadata": {**(pos.get("metadata") or {}), "lastExitReason": reason}
            })
            record_event(uid, "POSITION_" + new_status, "LIVE", {
                "symbol": pos["symbol"], "reason": reason, "realizedPnl": _fmt(pnl),
                "exitPrice": _fmt(fill["avgPrice"]), "clientOrderId": fill["clientOrderId"]
            })
            _worker_state["exits"] += 1
            _worker_state["lastAction"] = {"action": "SELL", "symbol": pos["symbol"], "reason": reason, "pnl": _fmt(pnl)}
        except Exception:
            sb_rest("blessing_owner_live_positions", {"id": "eq." + pid}, "PATCH", {
                "status": "RECONCILIATION_REQUIRED", "updated_at": datetime.now(timezone.utc).isoformat()
            })
            raise

    def run_cycle(control):
        uid = str(control["owner_user_id"])
        config = control.get("strategy_config") or {}
        mode = control.get("run_mode")
        if mode != "LIVE" or not control.get("live_approved") or not control.get("live_enabled") or not _master_on():
            return
        account = account_snapshot()
        balances = {str(x.get("asset") or "").upper(): _d(x.get("free")) for x in account.get("balances", []) if isinstance(x, dict)}
        free_usdt = balances.get("USDT", Decimal("0"))
        positions = sb_rest("blessing_owner_live_positions", {
            "select": "*", "owner_user_id": "eq." + uid, "status": "in.(OPEN,CLOSING)", "limit": "100"
        }) or []
        # Always manage exits before considering new entries.
        for pos in positions:
            price = fresh_price(str(pos["symbol"]))
            stop = _d(pos.get("stop_loss_price"))
            target = _d(pos.get("take_profit_price"))
            sb_rest("blessing_owner_live_positions", {"id": "eq." + str(pos["id"])}, "PATCH", {
                "last_price": _fmt(price), "last_checked_at": datetime.now(timezone.utc).isoformat()
            })
            entry = _d(pos.get("average_entry_price"))
            initial_risk = entry - stop
            if pos.get("status") == "OPEN" and entry > 0 and initial_risk > 0 and price >= entry + initial_risk:
                # At +1R, protect the position above break-even after estimated fees.
                protected_stop = max(stop, entry * Decimal("1.001"))
                if price >= entry + initial_risk * Decimal("1.5"):
                    # Trail the stop 0.75R behind the current price after +1.5R.
                    protected_stop = max(protected_stop, price - initial_risk * Decimal("0.75"))
                if protected_stop > stop:
                    metadata = dict(pos.get("metadata") or {})
                    metadata.update({"balancedBreakEvenArmed": True, "balancedTrailingStop": True})
                    sb_rest("blessing_owner_live_positions", {"id": "eq." + str(pos["id"])}, "PATCH", {
                        "stop_loss_price": _fmt(protected_stop), "metadata": metadata,
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    })
                    stop = protected_stop
            if pos.get("status") == "CLOSING" or price <= stop or price >= target:
                reason = "STOP_LOSS" if price <= stop else "TAKE_PROFIT" if price >= target else "STOP_REQUEST"
                close_position(uid, pos, price, reason, config)

        refreshed = sb_rest("blessing_owner_live_positions", {
            "select": "id,symbol,status", "owner_user_id": "eq." + uid,
            "status": "in.(OPEN,CLOSING,RECONCILIATION_REQUIRED)", "limit": "100"
        }) or []
        if control.get("run_state") == "STOPPING":
            if not refreshed:
                sb_rest("blessing_owner_auto_trade_control", {"owner_user_id": "eq." + uid}, "PATCH", {
                    "run_state": "STOPPED", "run_mode": "LIVE", "live_enabled": False,
                    "stopped_at": datetime.now(timezone.utc).isoformat(), "heartbeat_at": datetime.now(timezone.utc).isoformat()
                })
                record_event(uid, "STOPPED", "LIVE", {"reason": "all_positions_closed"})
            return
        if control.get("run_state") != "STOPPING" and config.get("auto_convert_to_usdt") is True:
            # Convert one eligible free non-USDT asset per cycle, even when some USDT is already available.
            conversion = convert_best_asset_to_usdt(uid, account, positions, config)
            if conversion:
                account = account_snapshot()
                balances = {str(x.get("asset") or "").upper(): _d(x.get("free")) for x in account.get("balances", []) if isinstance(x, dict)}
                free_usdt = balances.get("USDT", Decimal("0"))
        if len(refreshed) >= int(config.get("max_open_positions", 3)):
            return
        if free_usdt <= 0:
            return
        # Enforce realized daily drawdown from the starting free USDT snapshot.
        today = datetime.now(timezone.utc).date().isoformat()
        if config.get("risk_day") != today:
            config["risk_day"] = today
            config["risk_day_start_usdt"] = _fmt(free_usdt)
            sb_rest("blessing_owner_auto_trade_control", {"owner_user_id": "eq." + uid}, "PATCH", {
                "strategy_config": config, "updated_at": datetime.now(timezone.utc).isoformat()
            })
        day_start = _d(config.get("risk_day_start_usdt"))
        closed_today = sb_rest("blessing_owner_live_positions", {
            "select": "realized_pnl", "owner_user_id": "eq." + uid,
            "status": "eq.CLOSED", "closed_at": "gte." + today + "T00:00:00Z", "limit": "1000"
        }) or []
        realized_today = sum((_d(x.get("realized_pnl")) for x in closed_today), Decimal("0"))
        max_loss = day_start * _d(config.get("max_daily_loss_pct", 2)) / Decimal("100")
        if day_start > 0:
            _worker_state["dailyLossPct"] = _fmt(max(Decimal("0"), -realized_today / day_start * Decimal("100")))
        if day_start > 0 and realized_today <= -max_loss:
            sb_rest("blessing_owner_auto_trade_control", {"owner_user_id": "eq." + uid}, "PATCH", {
                "run_state": "STOPPING", "last_error": "MAX_DAILY_LOSS_REACHED",
                "updated_at": datetime.now(timezone.utc).isoformat()
            })
            record_event(uid, "RISK_STOP_MAX_DAILY_LOSS", "LIVE", {
                "realizedToday": _fmt(realized_today), "maxDailyLoss": _fmt(max_loss)
            })
            return

        # Scanner must be Binance TH, and every entry must pass the strict FAST_ENTRY rule.
        scan = scanner()
        if scan.get("provider") != "Binance TH Spot API" or scan.get("status") != "ok":
            raise RuntimeError("BINANCE_TH_SCANNER_NOT_READY")
        open_symbols = {str(x.get("symbol")) for x in refreshed}
        candidates = [x for x in (scan.get("candidates") or [])
                      if x.get("balancedEntry") is True and x.get("symbol") not in open_symbols
                      and _d(x.get("price")) > 0 and _d(x.get("spreadPct"), "999") <= Decimal("0.5")
                      and Decimal("0.8") <= _d(x.get("atrStopPct")) <= Decimal("2.0")]
        candidates.sort(key=lambda x: (_d(x.get("balancedScore")), _d(x.get("rvol"))), reverse=True)
        if not candidates:
            return
        risk_pct = _d(config.get("risk_per_trade_pct", 1.0))
        candidate = candidates[0]
        stop_pct = _d(candidate.get("atrStopPct"))
        cap_pct = _d(config.get("max_position_pct", 50))
        if stop_pct <= 0 or risk_pct <= 0:
            raise RuntimeError("INVALID_RISK_CONFIGURATION")
        risk_budget = free_usdt * risk_pct / Decimal("100")
        notional = min(risk_budget / (stop_pct / Decimal("100")), free_usdt * cap_pct / Decimal("100"))
        # Owner requested no idle USDT reserve: allow exposure up to 100% of current free USDT.
        # Exchange fees, lot sizes, locked funds, and active reservations can still leave unavoidable dust.
        used = sum((_d(x.get("entry_quote_amount")) for x in positions if x.get("status") in ("OPEN", "CLOSING")), Decimal("0"))
        exposure_cap = free_usdt
        notional = min(notional, max(Decimal("0"), exposure_cap - used))
        if notional <= 0:
            return
        symbol = str(candidate["symbol"])
        fill = market_order(uid, symbol, "BUY", quote_amount=notional)
        avg = fill["avgPrice"]
        qty = fill["executedQty"]
        stop_price = avg * (Decimal("1") - stop_pct / Decimal("100"))
        take_price = avg * (Decimal("1") + (stop_pct * Decimal("1.8")) / Decimal("100"))
        sb_rest("blessing_owner_live_positions", {}, "POST", {
            "owner_user_id": uid, "symbol": symbol, "base_asset": fill["baseAsset"],
            "quote_asset": fill["quoteAsset"], "quantity": _fmt(qty),
            "average_entry_price": _fmt(avg), "entry_quote_amount": _fmt(fill["quoteQty"]),
            "stop_loss_price": _fmt(stop_price), "take_profit_price": _fmt(take_price),
            "status": "OPEN", "entry_client_order_id": fill["clientOrderId"],
            "last_price": _fmt(avg), "metadata": {"strategy": "BALANCED_EMA_PULLBACK", "balancedScore": candidate.get("balancedScore"),
            "ema20": candidate.get("ema20"), "ema50": candidate.get("ema50"),
            "atrStopPct": candidate.get("atrStopPct"), "rvol": candidate.get("rvol"),
            "spreadPct": candidate.get("spreadPct"), "initialRiskPct": _fmt(stop_pct)}
        })
        record_event(uid, "POSITION_OPENED", "LIVE", {
            "symbol": symbol, "quantity": _fmt(qty), "entryPrice": _fmt(avg),
            "stopLoss": _fmt(stop_price), "takeProfit": _fmt(take_price),
            "strategy": "BALANCED_EMA_PULLBACK", "riskPerTradePct": _fmt(risk_pct),
            "atrStopPct": _fmt(stop_pct), "quoteAmount": _fmt(fill["quoteQty"]),
            "clientOrderId": fill["clientOrderId"]
        })
        _worker_state["entries"] += 1
        _worker_state["lastAction"] = {"action": "BUY", "symbol": symbol, "quantity": _fmt(qty), "quoteAmount": _fmt(fill["quoteQty"])}

    def worker_loop():
        if OWNER_LIVE_ENGINE_PAUSED:
            _worker_state.update({"status": "paused", "lastError": "TEMPORARY_OWNER_APPROVED_PAUSE"})
            log.warning("Owner live auto-trade worker startup skipped: temporary pause")
            return
        _worker_state["status"] = "running"
        while True:
            try:
                rows = sb_rest("blessing_owner_auto_trade_control", {
                    "select": "*", "run_state": "in.(RUNNING,STOPPING)", "run_mode": "eq.LIVE",
                    "live_approved": "eq.true", "live_enabled": "eq.true", "limit": "10"
                }) or []
                for control in rows:
                    try:
                        run_cycle(control)
                    except Exception as exc:
                        log.exception("Owner live auto-trade cycle failed")
                        sb_rest("blessing_owner_auto_trade_control", {
                            "owner_user_id": "eq." + str(control["owner_user_id"])
                        }, "PATCH", {
                            "run_state": "ERROR", "last_error": type(exc).__name__,
                            "heartbeat_at": datetime.now(timezone.utc).isoformat(),
                            "updated_at": datetime.now(timezone.utc).isoformat()
                        })
                        record_event(str(control["owner_user_id"]), "ENGINE_ERROR", "LIVE", {"error": type(exc).__name__})
                _worker_state.update({"lastCycleAt": datetime.now(timezone.utc).isoformat(),
                                      "lastError": None, "cycles": _worker_state["cycles"] + 1})
            except Exception as exc:
                _worker_state.update({"lastError": type(exc).__name__})
                log.exception("Owner live auto-trade worker poll failed")
            time.sleep(15)

    global _worker_started
    with _worker_lock:
        if not _worker_started and not OWNER_LIVE_ENGINE_PAUSED:
            threading.Thread(target=worker_loop, name="blessing-owner-live-auto-trade", daemon=True).start()
            _worker_started = True
        elif OWNER_LIVE_ENGINE_PAUSED:
            _worker_state.update({"status": "paused", "lastError": "TEMPORARY_OWNER_APPROVED_PAUSE"})


    @app.get("/binance-th/auto-trade/portfolio")
    def owner_auto_trade_portfolio(request: Request):
        owner = owner_auth(request)
        uid = str(owner["id"])
        if uid != OWNER_ID:
            raise HTTPException(status_code=403, detail="เฉพาะเจ้าของระบบเท่านั้น")
        account = account_snapshot()
        now = datetime.now(timezone.utc).isoformat()
        balances = [x for x in account.get("balances", []) if isinstance(x, dict)]
        active = sb_rest("blessing_owner_live_order_intents", {
            "select": "client_order_id,symbol,side,status,reserved_asset,remaining_reserved_amount",
            "owner_user_id": "eq." + uid,
            "status": "in.(RESERVED,SUBMITTING,NEW,PARTIALLY_FILLED,UNKNOWN,RECONCILIATION_REQUIRED)",
            "limit": "1000"
        }) or []
        reserved = {}
        for row in active:
            asset = str(row.get("reserved_asset") or "").upper()
            if asset:
                reserved[asset] = reserved.get(asset, Decimal("0")) + _d(row.get("remaining_reserved_amount"))
        positions = sb_rest("blessing_owner_live_positions", {
            "select": "*", "owner_user_id": "eq." + uid,
            "status": "in.(OPEN,CLOSING,RECONCILIATION_REQUIRED)",
            "order": "opened_at.desc", "limit": "100"
        }) or []
        held_bases = {str(p.get("base_asset") or "").upper() for p in positions}
        import requests
        assets = []
        total_usdt = Decimal("0")
        unpriced = []
        stable = {"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "USDP"}
        # Fetch the THB/USDT conversion once so THB balances and THB-quoted
        # assets are included in the estimated portfolio total.
        usdt_thb = None
        try:
            usdt_thb = fresh_price("USDTTHB")
        except Exception:
            pass
        for row in balances:
            asset = str(row.get("asset") or "").upper()
            free = _d(row.get("free"))
            locked = _d(row.get("locked"))
            amount = free + locked
            if not asset or amount <= 0:
                continue
            price_usdt = None
            price_source = None
            if asset in stable:
                price_usdt = Decimal("1")
                price_source = "stablecoin_estimate"
            elif asset == "THB" and usdt_thb is not None and usdt_thb > 0:
                price_usdt = Decimal("1") / usdt_thb
                price_source = "THB_USDT_inverse"
            else:
                try:
                    price_usdt = fresh_price(asset + "USDT")
                    price_source = asset + "USDT"
                except Exception:
                    # Binance TH may list an asset against THB but not USDT.
                    # Convert that quote through the live USDTTHB market rate.
                    if usdt_thb is not None and usdt_thb > 0:
                        try:
                            price_thb = fresh_price(asset + "THB")
                            price_usdt = price_thb / usdt_thb
                            price_source = asset + "THB / USDTTHB"
                        except Exception:
                            price_usdt = None
            value_usdt = amount * price_usdt if price_usdt is not None else None
            if value_usdt is not None:
                total_usdt += value_usdt
            else:
                unpriced.append(asset)
            reserved_amount = reserved.get(asset, Decimal("0"))
            available = max(Decimal("0"), free - reserved_amount)
            assets.append({
                "asset": asset, "free": _fmt(free), "locked": _fmt(locked),
                "reserved": _fmt(reserved_amount), "available": _fmt(available),
                "total": _fmt(amount), "priceUSDT": _fmt(price_usdt) if price_usdt is not None else None,
                "valueUSDT": _fmt(value_usdt) if value_usdt is not None else None,
                "priceSource": price_source, "inOpenPosition": asset in held_bases
            })
        priced_positions = []
        for pos in positions:
            item = dict(pos)
            try:
                px = fresh_price(str(pos.get("symbol") or ""))
                qty = _d(pos.get("quantity"))
                entry = _d(pos.get("average_entry_price"))
                item["markPrice"] = _fmt(px)
                item["unrealizedPnl"] = _fmt((px - entry) * qty)
                item["marketValue"] = _fmt(px * qty)
            except Exception:
                item["markPrice"] = None
                item["unrealizedPnl"] = None
                item["marketValue"] = None
            priced_positions.append(item)
        unrealized_total = sum((_d(x.get("unrealizedPnl")) for x in priced_positions), Decimal("0"))
        closed_positions = sb_rest("blessing_owner_live_positions", {
            "select": "realized_pnl", "owner_user_id": "eq." + uid,
            "status": "eq.CLOSED", "limit": "1000"
        }) or []
        realized_total = sum((_d(x.get("realized_pnl")) for x in closed_positions), Decimal("0"))
        orders = sb_rest("blessing_owner_live_order_intents", {
            "select": "client_order_id,symbol,side,status,executed_quantity,cumulative_quote_quantity,updated_at",
            "owner_user_id": "eq." + uid, "order": "created_at.desc", "limit": "20"
        }) or []
        return {
            "ok": True, "asOf": now, "quoteAsset": "USDT",
            "totalValueUSDT": _fmt(total_usdt),
            "totalValueTHB": _fmt(total_usdt * usdt_thb) if usdt_thb is not None else None,
            "usdtThb": _fmt(usdt_thb) if usdt_thb is not None else None,
            "assets": assets, "positions": priced_positions, "orders": orders,
            "unrealizedPnlUSDT": _fmt(unrealized_total), "realizedPnlUSDT": _fmt(realized_total),
            "reservedByAsset": {k: _fmt(v) for k, v in reserved.items()},
            "unpricedAssets": unpriced,
            "conversionPolicy": {
                "enabled": bool((owner_control(uid).get("strategy_config") or {}).get("auto_convert_to_usdt", False)),
                "minUSDTReserve": (owner_control(uid).get("strategy_config") or {}).get("min_usdt_reserve", 0),
                "selection": "AI เลือกเหรียญ Altcoin ที่อ่อนกว่า โดยต้องมีคู่ USDT, สภาพคล่องและสเปรดผ่านเกณฑ์; งดเหรียญหลักและเหรียญที่มีโพซิชัน/ยอดจอง"
            },
            "valuationNote": "มูลค่าประเมินจากคู่ USDT ที่มีราคา; สินทรัพย์ที่ไม่มีคู่ตรงจะแสดงเป็นยังไม่ประเมิน"
        }

    @app.get("/binance-th/auto-trade/legacy-status")
    def owner_auto_trade_status(request: Request):
        owner = owner_auth(request)
        uid = str(owner["id"])
        control = owner_control(uid)
        positions = sb_rest("blessing_owner_live_positions", {
            "select": "*", "owner_user_id": "eq." + uid,
            "status": "in.(OPEN,CLOSING,RECONCILIATION_REQUIRED)", "order": "opened_at.desc", "limit": "100"
        }) or []
        events = sb_rest("blessing_owner_auto_trade_events", {
            "select": "event_type,run_mode,event_data,created_at",
            "owner_user_id": "eq." + uid, "order": "created_at.desc", "limit": "30"
        }) or []
        return {"ok": True, "control": control, "worker": dict(_worker_state),
                "engine": dict(_worker_state), "masterSwitch": _master_on(),
                "positions": positions, "events": events,
                "serverMasterSwitch": _master_on(), "liveOrdersEnabled": _master_on()}

    @app.post("/binance-th/auto-trade/legacy-start")
    async def owner_auto_trade_start(request: Request):
        if OWNER_LIVE_ENGINE_PAUSED:
            raise HTTPException(status_code=423, detail="ระบบ Owner Live Engine ถูกพักชั่วคราวเพื่อซ่อมระบบฐานข้อมูลและล็อกอิน")
        owner = owner_auth(request)
        uid = str(owner["id"])
        if uid != OWNER_ID:
            raise HTTPException(status_code=403, detail="เฉพาะเจ้าของระบบเท่านั้น")
        if not _master_on():
            raise HTTPException(status_code=423, detail="สวิตช์ Live Trading บน Render ยังปิดอยู่")
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="ข้อมูลคำสั่งไม่ถูกต้อง")
        if body.get("confirmLiveTrading") is not True:
            raise HTTPException(status_code=428, detail="ต้องยืนยันเริ่มเทรดเงินจริง")
        account = account_snapshot()
        free_usdt = sum((_d(x.get("free")) for x in account.get("balances", [])
                         if isinstance(x, dict) and str(x.get("asset") or "").upper() == "USDT"), Decimal("0"))
        protected_assets = {"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "USDP", "DAI",
                            "BTC", "ETH", "BNB", "SOL", "XRP"}
        has_convertible_balance = any(
            isinstance(x, dict)
            and str(x.get("asset") or "").upper() not in protected_assets
            and _d(x.get("free")) > 0
            for x in account.get("balances", [])
        )
        if free_usdt <= 0 and not has_convertible_balance:
            raise HTTPException(status_code=423, detail="ไม่พบยอด USDT Free หรือ Altcoin ที่เข้าเกณฑ์สำหรับแปลงเป็น USDT")
        control = owner_control(uid)
        if control.get("run_state") in ("RUNNING", "STARTING", "STOPPING"):
            raise HTTPException(status_code=409, detail="ระบบกำลังทำงานหรือกำลังหยุดอยู่แล้ว")
        cfg = control.get("strategy_config") or {}
        cfg.setdefault("universe", "BINANCE_TH_SPOT")
        cfg.setdefault("max_open_positions", 2)
        cfg.setdefault("risk_per_trade_pct", 1.0)
        cfg.setdefault("max_daily_loss_pct", 2)
        cfg.setdefault("stop_loss_pct", 1.2)
        cfg.setdefault("take_profit_pct", 2.16)
        cfg.setdefault("scan_interval_seconds", 15)
        cfg.setdefault("max_position_pct", 50)
        cfg["strategy_model"] = "BALANCED_EMA_PULLBACK_ATR"
        cfg.setdefault("auto_convert_to_usdt", True)
        cfg.setdefault("min_usdt_reserve", 0)
        now = datetime.now(timezone.utc).isoformat()
        sb_rest("blessing_owner_auto_trade_control", {"owner_user_id": "eq." + uid}, "PATCH", {
            "run_state": "RUNNING", "run_mode": "LIVE", "live_approved": True,
            "live_enabled": True, "strategy_config": cfg, "requested_by": uid,
            "requested_at": now, "started_at": now, "heartbeat_at": now,
            "last_error": None, "control_version": int(control.get("control_version") or 0) + 1,
            "updated_at": now
        })
        record_event(uid, "OWNER_STARTED_LIVE_AUTO_TRADE", "LIVE", {
            "freeUSDTAtStart": _fmt(free_usdt), "riskPerTradePct": cfg["risk_per_trade_pct"],
            "maxDailyLossPct": cfg["max_daily_loss_pct"], "stopLossPct": cfg["stop_loss_pct"],
            "takeProfitPct": cfg["take_profit_pct"], "maxOpenPositions": cfg["max_open_positions"]
        })
        return {"ok": True, "runState": "RUNNING", "runMode": "LIVE",
                "freeUSDT": _fmt(free_usdt), "strategyConfig": cfg,
                "message": "เริ่ม Auto Trading จริงแล้ว; จะเปิดสถานะเมื่อสัญญาณผ่านเกณฑ์และความเสี่ยงอนุญาต"}

    @app.post("/binance-th/auto-trade/legacy-stop")
    async def owner_auto_trade_stop(request: Request):
        owner = owner_auth(request)
        uid = str(owner["id"])
        control = owner_control(uid)
        if control.get("run_state") not in ("RUNNING", "STARTING", "STOPPING"):
            return {"ok": True, "runState": "STOPPED", "message": "ระบบหยุดอยู่แล้ว"}
        now = datetime.now(timezone.utc).isoformat()
        sb_rest("blessing_owner_auto_trade_control", {"owner_user_id": "eq." + uid}, "PATCH", {
            "run_state": "STOPPING", "live_enabled": True, "stopped_at": None,
            "heartbeat_at": now, "updated_at": now
        })
        record_event(uid, "OWNER_REQUESTED_STOP", "LIVE", {
            "message": "หยุดเปิดสถานะใหม่และดูแลปิดสถานะคงค้างตาม SL/TP ก่อนเข้าสู่ STOPPED"
        })
        return {"ok": True, "runState": "STOPPING",
                "message": "หยุดเปิดสถานะใหม่แล้ว; ระบบจะดูแล SL/TP ของสถานะคงค้างจนปิดครบ"}

