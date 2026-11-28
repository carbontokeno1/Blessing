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

    # Build independent candidate streams. Test replay gets only an 80-bar
    # pre-split warm-up window; no pre-split trade can occupy a test slot or
    # cause the per-symbol simulator to skip a post-split signal.
    first_test_index = {
        symbol: next((i for i, bar in enumerate(bars) if bar["t"] >= split), len(bars))
        for symbol, bars in data.items()
    }
    results, all_trades = {}, {}
    for mode in ("adaptive_baseline", "precision", "precision_selective"):
        train_candidates, test_candidates = [], []
        for symbol, bars in data.items():
            full_stream = bt.simulate(symbol, bars, btc_flags, mode)
            train_candidates.extend(
                t for t in full_stream
                if t["entry_time"] < split and t["exit_time"] <= split
            )
            cut = first_test_index[symbol]
            warm_start = max(0, cut - 80)
            test_bars = bars[warm_start:]
            test_stream = bt.simulate(symbol, test_bars, btc_flags, mode)
            test_candidates.extend(t for t in test_stream if t["entry_time"] >= split)

        train_replay = bt.portfolio_simulate(train_candidates)
        test_replay = bt.portfolio_simulate(test_candidates)
        results[mode] = {
            "train": {k:v for k,v in train_replay.items() if k not in ("daily","closed_positions")},
            "test_out_of_sample": {k:v for k,v in test_replay.items() if k not in ("daily","closed_positions")},
            "candidate_signals_train": len(train_candidates),
            "candidate_signals_test": len(test_candidates)
        }
        all_trades[mode] = (
            [("train", t) for t in train_replay["closed_positions"]]
            + [("test_out_of_sample", t) for t in test_replay["closed_positions"]]
        )

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
            "method": "independent train/test portfolio replays; test starts with 80 pre-split candles for indicator warm-up only; no train positions or per-symbol trade-skip state carry into test",
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
