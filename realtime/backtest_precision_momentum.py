#!/usr/bin/env python3
"""Historical, long-only Binance TH spot backtest. No orders are sent."""
import csv, json, math, os, statistics, time
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE = os.getenv("BINANCE_TH_API_BASE", "https://api.binance.th").rstrip("/")
DAYS = int(os.getenv("BACKTEST_DAYS", "14"))
SYMBOLS = os.getenv("BACKTEST_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,LTCUSDT,AVAXUSDT").split(",")
FEE_SIDE_PCT = float(os.getenv("FEE_SIDE_PCT", "0.10"))
SLIPPAGE_SIDE_PCT = float(os.getenv("SLIPPAGE_SIDE_PCT", "0.05"))
SPREAD_PROXY_PCT = float(os.getenv("SPREAD_PROXY_PCT", "0.10"))
ROUND_TRIP_COST = 2 * (FEE_SIDE_PCT + SLIPPAGE_SIDE_PCT) + SPREAD_PROXY_PCT

def fetch_klines(symbol, start_ms, end_ms):
    rows, cursor = [], start_ms
    while cursor < end_ms:
        params = urlencode({"symbol":symbol,"interval":"1m","startTime":cursor,"endTime":end_ms,"limit":1000})
        req = Request(BASE + "/api/v1/klines?" + params, headers={"Accept":"application/json","User-Agent":"BLESSING-paper-backtest/1.0"})
        with urlopen(req, timeout=20) as response:
            payload = json.loads(response.read().decode())
        data = payload.get("data") if isinstance(payload,dict) else payload
        if not isinstance(data,list) or not data: break
        batch=[]
        for r in data:
            if isinstance(r,list) and len(r)>=8:
                batch.append({"t":int(r[0]),"o":float(r[1]),"h":float(r[2]),"l":float(r[3]),"c":float(r[4]),"v":float(r[5]),"ct":int(r[6]),"qv":float(r[7])})
        if not batch: break
        rows.extend(batch)
        nxt=batch[-1]["t"]+60000
        if nxt<=cursor: break
        cursor=nxt
        if len(batch)<1000: break
        time.sleep(0.08)
    # Deduplicate and keep only fully closed candles.
    now=int(time.time()*1000)
    return [x for _,x in sorted({x["t"]:x for x in rows}.items()) if x["ct"]<now]

def ema(values, period):
    if len(values)<period: return None
    alpha=2/(period+1); value=sum(values[:period])/period
    for x in values[period:]: value=x*alpha+value*(1-alpha)
    return value

def features(bars, idx, btc_bearish):
    w=bars[max(0,idx-80):idx+1]
    if len(w)<61: return None
    c=[x["c"] for x in w]; last=w[-1]; price=c[-1]
    e20,e50=ema(c,20),ema(c,50)
    m=lambda n:(price/c[-1-n]-1)*100 if c[-1-n] else -999
    m1,m5,m15,m60=m(1),m(5),m(15),m(60)
    prev=w[-21:-1]; avg=sum(x["qv"] for x in prev)/len(prev) if prev else 0
    rvol=last["qv"]/avg if avg>0 else 0
    tr=[]
    for j in range(1,len(w)):
        p=w[j-1]["c"]; b=w[j]
        tr.append(max(b["h"]-b["l"],abs(b["h"]-p),abs(b["l"]-p)))
    atr=sum(tr[-14:])/14 if len(tr)>=14 else 0
    atr_pct=atr/price*100 if price else 0
    stop=min(2,max(.8,atr_pct*1.5))
    changes=[c[j]-c[j-1] for j in range(len(c)-14,len(c))]
    gains=sum(max(0,x) for x in changes)/14; losses=sum(max(0,-x) for x in changes)/14
    rsi=100 if losses==0 and gains>0 else (50 if losses==0 else 100-100/(1+gains/losses))
    hist=[]
    for k in range(max(19,len(w)-4),max(19,len(w)-1)):
        ev=ema(c[:k+1],20)
        if ev is not None: hist.append((w[k],ev))
    pullback=any(b["l"]<=ev*1.003 for b,ev in hist) and price>e20
    prior=w[-26:-6]; highs=[x["h"] for x in prior]
    level=max(highs) if len(highs)>=10 else 0
    retest_touch=level>0 and any(level*.997<=x["l"]<=level*1.004 for x in w[-6:-1])
    breakout=retest_touch and price>level*1.001 and w[-2]["c"]<=level*1.004
    spread=SPREAD_PROXY_PCT
    return dict(price=price,ema20=e20,ema50=e50,m1=m1,m5=m5,m15=m15,m60=m60,rvol=rvol,
                rsi=rsi,stop=stop,spread=spread,trend=e20>e50,pullback=pullback,breakout=breakout,
                btc_bearish=btc_bearish)

def signal(f, mode):
    if not f or f["btc_bearish"] or not (0<f["spread"]<=.20) or not (.8<=f["stop"]<=1.5): return False
    if not f["trend"] or f["price"]<=f["ema20"] or (f["price"]-f["ema20"])/f["price"]*100>1.25: return False
    if mode=="precision":
        if not (f["m1"]>0 and f["m5"]>=.05 and f["m15"]>=.12 and f["m60"]>0): return False
        if not (48<=f["rsi"]<=68 and 1.15<=f["rvol"]<=3.5 and (f["pullback"] or f["breakout"])): return False
        edge=max(f["m5"],0)*.55+max(f["m15"],0)*.30+max(f["m60"],0)*.15
        cost=.30+f["spread"]
        return edge>=cost*1.20
    # Existing adaptive trend-pullback branch, applied on the same historical candles.
    return (f["pullback"] and 44<=f["rsi"]<=66 and .85<=f["rvol"]<=2.8
            and f["m5"]>0 and f["m15"]>.08
            and max(0,f["m5"])+max(0,f["m15"]) >= (.30+f["spread"])*1.35)

def simulate(symbol, bars, btc_flags, mode):
    trades=[]; i=80
    while i<len(bars)-1:
        f=features(bars,i,btc_flags.get(bars[i]["t"],False))
        if not signal(f,mode): i+=1; continue
        entry_bar=bars[i+1]; entry=entry_bar["o"]*(1+SLIPPAGE_SIDE_PCT/100)
        stop_pct=f["stop"]; tp_pct=min(5.0,max(3.0,2*stop_pct))
        sl=entry*(1-stop_pct/100); tp=entry*(1+tp_pct/100)
        exit_price=None; exit_time=None; reason="time_exit"; j=i+1
        # Maximum 4h hold. If both levels hit in one candle, assume stop first.
        for j in range(i+1,min(len(bars),i+241)):
            b=bars[j]
            if b["l"]<=sl:
                exit_price=sl*(1-SLIPPAGE_SIDE_PCT/100); reason="stop"; exit_time=b["ct"]; break
            if b["h"]>=tp:
                exit_price=tp*(1-SLIPPAGE_SIDE_PCT/100); reason="take_profit"; exit_time=b["ct"]; break
        if exit_price is None:
            b=bars[min(j,len(bars)-1)]
            exit_price=b["c"]*(1-SLIPPAGE_SIDE_PCT/100); exit_time=b["ct"]
        gross=(exit_price/entry-1)*100
        net=gross-2*FEE_SIDE_PCT
        trades.append({"symbol":symbol,"entry_time":entry_bar["t"],"exit_time":exit_time,"gross_pct":gross,"net_pct":net,"reason":reason})
        i=max(i+1,j+1)
    return trades

def stats(trades):
    vals=[x["net_pct"] for x in trades]
    equity=peak=1.0; maxdd=0.0; daily={}
    for t in sorted(trades,key=lambda x:x["exit_time"]):
        equity*=1+t["net_pct"]/100; peak=max(peak,equity); maxdd=max(maxdd,(peak-equity)/peak*100)
        day=datetime.fromtimestamp(t["exit_time"]/1000,timezone.utc).strftime("%Y-%m-%d")
        daily[day]=daily.get(day,0)+t["net_pct"]
    wins=[x for x in vals if x>0]; losses=[x for x in vals if x<=0]
    gp=sum(wins); gl=abs(sum(losses))
    return {"trades":len(vals),"win_rate_pct":round(len(wins)/len(vals)*100,2) if vals else 0,
            "net_compounded_pct":round((equity-1)*100,3),"profit_factor":round(gp/gl,3) if gl else (None if not gp else "inf"),
            "max_drawdown_pct":round(maxdd,3),"avg_trade_net_pct":round(statistics.mean(vals),4) if vals else 0,
            "avg_daily_net_pct":round(statistics.mean(daily.values()),4) if daily else 0,
            "worst_day_net_pct":round(min(daily.values()),4) if daily else 0,
            "daily_observations":len(daily)}

def main():
    end=int(time.time()*1000); start=end-DAYS*86400000
    universe=list(dict.fromkeys([x.strip().upper() for x in SYMBOLS if x.strip()]))
    data={}
    for sym in universe:
        try:
            data[sym]=fetch_klines(sym,start,end)
            print(f"loaded {sym}: {len(data[sym])} 1m candles",flush=True)
        except Exception as exc:
            print(f"DATA_ERROR {sym}: {type(exc).__name__}: {exc}",flush=True)
    if "BTCUSDT" not in data or len(data["BTCUSDT"])<2000:
        raise SystemExit("Insufficient BTCUSDT history; refusing to publish a backtest result.")
    btc=data["BTCUSDT"]; btc_by_time={b["t"]:i for i,b in enumerate(btc)}
    btc_flags={}
    for symbars in data.values():
        for b in symbars:
            k=btc_by_time.get(b["t"])
            bearish=False
            if k is not None and k>=60:
                bf=features(btc,k,False)
                bearish=bool(bf and (bf["m15"]<0 or bf["m60"]<0))
            btc_flags[b["t"]]=bearish
    results={}
    all_trades={}
    for mode in ("adaptive_baseline","precision"):
        combined=[]
        for sym,bars in data.items():
            ts=simulate(sym,bars,btc_flags,mode); combined.extend(ts)
        results[mode]=stats(combined); all_trades[mode]=combined
    os.makedirs("backtest-output",exist_ok=True)
    with open("backtest-output/summary.json","w") as f: json.dump({"period_days":DAYS,"symbols_loaded":{k:len(v) for k,v in data.items()},"assumptions":{"fee_per_side_pct":FEE_SIDE_PCT,"slippage_per_side_pct":SLIPPAGE_SIDE_PCT,"spread_proxy_pct":SPREAD_PROXY_PCT,"round_trip_cost_pct":ROUND_TRIP_COST,"entry":"next 1m candle open","same_bar_sl_tp":"stop first","max_hold_minutes":240,"tp_pct":"clamp 3% to 5%, at least 2R","btc_bias":"historical BTC 15m or 60m momentum negative"},"results":results},f,indent=2)
    with open("backtest-output/trades.csv","w",newline="") as f:
        w=csv.DictWriter(f,fieldnames=["mode","symbol","entry_time","exit_time","gross_pct","net_pct","reason"]);w.writeheader()
        for mode,ts in all_trades.items():
            for t in ts:w.writerow({"mode":mode,**t})
    print(json.dumps(results,indent=2),flush=True)
    if not all(data.get(s) for s in universe[:min(5,len(universe))]):
        raise SystemExit("Partial universe data; inspect summary and do not treat as conclusive.")

if __name__=="__main__": main()
