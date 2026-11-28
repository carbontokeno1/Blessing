#!/usr/bin/env python3
"""60-day chronological walk-forward replay; no orders are sent."""
import csv, importlib.util, json, os, time
from datetime import datetime, timezone

spec = importlib.util.spec_from_file_location("precision_bt", "realtime/backtest_precision_momentum.py")
bt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bt)

def main():
    days = int(os.getenv("BACKTEST_DAYS", "60"))
    end = int(time.time() * 1000)
    start = end - days * 86400000
    split = start + (end - start) * 0.5
    symbols = list(dict.fromkeys(x.strip().upper() for x in bt.SYMBOLS if x.strip()))
    data = {}
    for symbol in symbols:
        try:
            data[symbol] = bt.fetch_klines(symbol, start, end)
            print(f"loaded {symbol}: {len(data[symbol])} 1m candles", flush=True)
        except Exception as exc:
            print(f"DATA_ERROR {symbol}: {type(exc).__name__}: {exc}", flush=True)
    if "BTCUSDT" not in data or len(data["BTCUSDT"]) < 2000:
        raise SystemExit("Insufficient BTCUSDT history; refusing to publish a result.")
    btc = data["BTCUSDT"]
    btc_index = {b["t"]: i for i, b in enumerate(btc)}
    btc_flags = {}
    for bars in data.values():
        for bar in bars:
            k = btc_index.get(bar["t"])
            bearish = False
            if k is not None and k >= 60:
                bf = bt.features(btc, k, False)
                bearish = bool(bf and (bf["m15"] < 0 or bf["m60"] < 0))
            btc_flags[bar["t"]] = bearish

    results, all_trades = {}, {}
    for mode in ("adaptive_baseline", "precision", "precision_selective"):
        candidates = []
        for symbol, bars in data.items():
            candidates.extend(bt.simulate(symbol, bars, btc_flags, mode))
        # Training candidates must also close before the boundary. Test entries
        # begin at the boundary; indicator history is naturally prior-only.
        train = [t for t in candidates if t["entry_time"] < split and t["exit_time"] <= split]
        test = [t for t in candidates if t["entry_time"] >= split]
        results[mode] = {
            "train": {k:v for k,v in bt.portfolio_simulate(train).items() if k not in ("daily","closed_positions")},
            "test_out_of_sample": {k:v for k,v in bt.portfolio_simulate(test).items() if k not in ("daily","closed_positions")},
            "candidate_signals_train": len(train),
            "candidate_signals_test": len(test)
        }
        all_trades[mode] = [("train", t) for t in bt.portfolio_simulate(train)["closed_positions"]] + [("test_out_of_sample", t) for t in bt.portfolio_simulate(test)["closed_positions"]]

    os.makedirs("backtest-output", exist_ok=True)
    summary = {
        "period_days": days, "split": "first half train / second half untouched out-of-sample",
        "train_start_utc": datetime.fromtimestamp(start/1000, timezone.utc).isoformat(),
        "split_utc": datetime.fromtimestamp(split/1000, timezone.utc).isoformat(),
        "end_utc": datetime.fromtimestamp(end/1000, timezone.utc).isoformat(),
        "timezone_for_daily_limits": "Asia/Bangkok",
        "symbols_loaded": {k:len(v) for k,v in data.items()},
        "assumptions": {
            "fee_per_side_pct": bt.FEE_SIDE_PCT, "slippage_per_side_pct": bt.SLIPPAGE_SIDE_PCT,
            "spread_proxy_pct": bt.SPREAD_PROXY_PCT, "entry": "next 1m candle open",
            "same_bar_sl_tp": "stop first", "max_hold_minutes": 240,
            "position_sizing": "0.5% equity risk per trade, max 3 positions, 20% reserve",
            "daily_guards": "2% realized loss stop; 3% realized profit pause",
            "capital": "100,000 THB-equivalent units; simulated percentage returns, not a live forecast",
            "method": "signals computed chronologically from prior candles; train trades crossing split excluded; test uses second half only",
            "limitations": "feature approximation, not tick-level execution; spread is a fixed proxy; results are not a live forecast"
        },
        "results": results
    }
    with open("backtest-output/walk-forward-summary.json","w") as f:
        json.dump(summary, f, indent=2)
    with open("backtest-output/walk-forward-trades.csv","w",newline="") as f:
        fields=["period","mode","symbol","entry_time","exit_time","entry_price","stop_pct","gross_pct","net_pct","notional","pnl_cash","reason"]
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader()
        for mode, rows in all_trades.items():
            for period, trade in rows:
                w.writerow({"period":period,"mode":mode,**{k:trade.get(k) for k in fields[2:]}})
    print(json.dumps(results, indent=2), flush=True)
    if not all(data.get(s) for s in symbols[:min(5,len(symbols))]):
        raise SystemExit("Partial primary-universe data; inspect artifact and do not treat as conclusive.")

if __name__ == "__main__":
    main()
