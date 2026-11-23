import http from "node:http";
import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || 10000);
const STATE_FILE = process.env.STATE_FILE || "/var/data/fast-profit-state.json";
const CONTROL_TOKEN = process.env.FAST_PROFIT_CONTROL_TOKEN || "";
const DASHBOARD_ORIGIN = process.env.DASHBOARD_ORIGIN || "https://blessing.carbontokeno1.workers.dev";
const USD_THB_FX = Number(process.env.USD_THB_FX || 34);
const SCAN_INTERVAL_MS = Math.max(30, Number(process.env.SCAN_INTERVAL_SECONDS || 60)) * 1000;
const PAPER_ONLY = true;

export const LIMITS = Object.freeze({
  initialCapitalTHB: 10000, maxPositionTHB: 2000, maxRiskTHB: 50,
  maxOpenPositions: 3, maxDailyLossTHB: 200,
  maxConsecutiveLosses: 3, feeRate: 0.001, slippageRate: 0.0005
});

export const UNIVERSE = Object.freeze({
  stocks: ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","AMD","AVGO","NFLX","COST","JPM"],
  crypto: ["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","ADAUSDT","LINKUSDT","AVAXUSDT","LTCUSDT","TRXUSDT","DOTUSDT"]
});

export const STRATEGIES = Object.freeze({
  trend: { label: "Trend Follow", stopAtr: 1.5, rewardRisk: 1.8, test: f => f.ema9 > f.ema21 && f.rsi >= 50 && f.rsi <= 68 && f.macdHist > 0 && f.relVol >= 1.0 },
  momentum: { label: "Momentum", stopAtr: 1.4, rewardRisk: 2.0, test: f => f.ema9 > f.ema21 && f.rsi >= 54 && f.rsi <= 72 && f.macdHist > 0 && f.relVol >= 1.25 },
  breakout: { label: "Volume Breakout", stopAtr: 1.6, rewardRisk: 2.2, test: f => f.breakout && f.rsi >= 52 && f.rsi <= 72 && f.relVol >= 1.4 && f.macdHist > 0 }
});

const nowIso = () => new Date().toISOString();
const TIME_ZONE = process.env.TRADING_TIME_ZONE || "Asia/Bangkok";
const dayKey = (d = new Date()) => new Intl.DateTimeFormat("en-CA", { timeZone: TIME_ZONE, year: "numeric", month: "2-digit", day: "2-digit" }).format(d);
const openedDayKey = value => dayKey(new Date(value));
const round = (n, p = 4) => Number(Number(n).toFixed(p));
const safeNumber = n => Number.isFinite(Number(n)) ? Number(n) : 0;
const newId = () => crypto.randomUUID();

export function ema(values, period) {
  if (!values.length) return [];
  const k = 2 / (period + 1), out = [values[0]];
  for (let i = 1; i < values.length; i++) out.push(values[i] * k + out[i - 1] * (1 - k));
  return out;
}
export function rsi(values, period = 14) {
  if (values.length <= period) return 50;
  let gains = 0, losses = 0;
  for (let i = values.length - period; i < values.length; i++) {
    const d = values[i] - values[i - 1];
    if (d > 0) gains += d; else losses -= d;
  }
  if (losses === 0) return 100;
  return 100 - 100 / (1 + gains / losses);
}
export function atr(bars, period = 14) {
  if (bars.length < period + 1) return 0;
  const trs = [];
  for (let i = 1; i < bars.length; i++) {
    const b = bars[i], prev = bars[i - 1];
    trs.push(Math.max(b.high - b.low, Math.abs(b.high - prev.close), Math.abs(b.low - prev.close)));
  }
  return trs.slice(-period).reduce((a,b) => a+b, 0) / period;
}
export function features(bars) {
  if (!Array.isArray(bars) || bars.length < 35) return null;
  const closes = bars.map(b => b.close);
  const e9 = ema(closes, 9), e21 = ema(closes, 21), e12 = ema(closes, 12), e26 = ema(closes, 26);
  const macd = e12.map((v,i) => v - e26[i]), signal = ema(macd, 9);
  const last = bars.at(-1), prior = bars.slice(-21, -1);
  const avgVol = prior.reduce((s,b) => s + b.volume, 0) / Math.max(1, prior.length);
  const priorHigh = Math.max(...prior.map(b => b.high));
  return {
    close: last.close, ema9: e9.at(-1), ema21: e21.at(-1),
    rsi: rsi(closes, 14), macdHist: macd.at(-1) - signal.at(-1),
    atr: atr(bars, 14), relVol: avgVol > 0 ? last.volume / avgVol : 0,
    breakout: last.close > priorHigh, barTime: last.time
  };
}
export function evaluateSignal(bars, strategyName) {
  const f = features(bars), strategy = STRATEGIES[strategyName];
  if (!f || !strategy || !(f.atr > 0)) return null;
  return strategy.test(f) ? { strategy: strategyName, label: strategy.label, ...f } : null;
}
function evaluateBacktest(bars, strategyName) {
  const strategy = STRATEGIES[strategyName], trades = [];
  if (!strategy || bars.length < 60) return { count: 0, winRate: 0, profitFactor: 0, netR: 0, maxDrawdownR: 0, score: -Infinity };
  let equity = 0, peak = 0, maxDD = 0;
  for (let i = 35; i < bars.length - 2; i++) {
    const f = features(bars.slice(0, i + 1));
    if (!f || !strategy.test(f) || !(f.atr > 0)) continue;
    const entry = bars[i + 1].open;
    const risk = f.atr * strategy.stopAtr;
    const stop = entry - risk, target = entry + risk * strategy.rewardRisk;
    let exit = bars[Math.min(i + 13, bars.length - 1)].close, r = (exit-entry)/risk;
    for (let j = i + 1; j < Math.min(i + 13, bars.length); j++) {
      const b = bars[j];
      if (b.low <= stop) { exit = stop; r = -1; break; }
      if (b.high >= target) { exit = target; r = strategy.rewardRisk; break; }
    }
    const costR = ((LIMITS.feeRate + LIMITS.slippageRate) * 2 * entry) / risk;
    r -= costR;
    trades.push(r);
    equity += r; peak = Math.max(peak, equity); maxDD = Math.max(maxDD, peak-equity);
    i += 1;
  }
  const wins = trades.filter(x => x > 0), losses = trades.filter(x => x < 0);
  const grossWin = wins.reduce((a,b)=>a+b,0), grossLoss = Math.abs(losses.reduce((a,b)=>a+b,0));
  const pf = grossLoss ? grossWin/grossLoss : (grossWin > 0 ? 99 : 0);
  const winRate = trades.length ? wins.length/trades.length : 0;
  const score = trades.length >= 8 ? (trades.reduce((a,b)=>a+b,0) + Math.min(pf,4)*2 + winRate*2 - maxDD*0.5) : -Infinity;
  return { count: trades.length, winRate, profitFactor: pf, netR: trades.reduce((a,b)=>a+b,0), maxDrawdownR: maxDD, score };
}
export function chooseStrategy(barSets) {
  const results = Object.keys(STRATEGIES).map(name => {
    const metrics = (barSets || []).map(bars => evaluateBacktest(bars, name));
    const usable = metrics.filter(m => m.count > 0);
    const count = usable.reduce((s,m)=>s+m.count,0);
    const netR = usable.reduce((s,m)=>s+m.netR,0);
    const wins = usable.reduce((s,m)=>s+m.winRate*m.count,0);
    const avgPf = count ? usable.reduce((s,m)=>s+m.profitFactor*m.count,0)/count : 0;
    const maxDd = usable.reduce((s,m)=>Math.max(s,m.maxDrawdownR),0);
    const winRate = count ? wins/count : 0;
    const score = count >= 8 ? netR + Math.min(avgPf,4)*2 + winRate*2 - maxDd*0.5 : -Infinity;
    return { name, label: STRATEGIES[name].label, sampleTrades: count, netR: round(netR,2), winRate: round(winRate,3), profitFactor: round(avgPf,2), maxDrawdownR: round(maxDd,2), score };
  }).sort((a,b)=>b.score-a.score);
  const selected = Number.isFinite(results[0]?.score) ? results[0].name : "trend";
  return { selected, adaptive: Number.isFinite(results[0]?.score), reason: Number.isFinite(results[0]?.score) ? "เลือกจากผลทดสอบย้อนหลังแบบ rolling window; ต้องมีอย่างน้อย 8 สัญญาณปิด" : "ข้อมูลย้อนหลังยังไม่พอ ใช้ Trend Follow แบบอนุรักษ์นิยม", candidates: results };
}
export function positionSizing({ price, stop, fx = USD_THB_FX, cashTHB = LIMITS.initialCapitalTHB }) {
  if (!(price > 0 && stop > 0 && stop < price && fx > 0)) return null;
  const riskPct = (price-stop)/price;
  const allInRiskPct = riskPct + 2*(LIMITS.feeRate+LIMITS.slippageRate);
  const notionalTHB = Math.min(LIMITS.maxPositionTHB, LIMITS.maxRiskTHB/allInRiskPct, cashTHB/(1+LIMITS.feeRate));
  if (!(notionalTHB >= 100)) return null;
  const qty = notionalTHB/(price*fx);
  const riskTHB = (price-stop)*qty*fx + notionalTHB*(LIMITS.feeRate*2+LIMITS.slippageRate*2);
  if (riskTHB > LIMITS.maxRiskTHB + 1e-6) return null;
  return { notionalTHB: round(notionalTHB,2), qty: round(qty,8), riskTHB: round(riskTHB,2) };
}

function emptyState() {
  return {
    version: 1, mode: "PAPER_ONLY", running: false, startedAt: null, createdAt: nowIso(),
    lastScanAt: null, lastError: null, scanCount: 0,
    portfolios: {
      stocks: { initialTHB: LIMITS.initialCapitalTHB, cashTHB: LIMITS.initialCapitalTHB, realizedTHB: 0, peakEquityTHB: LIMITS.initialCapitalTHB, maxDrawdownTHB: 0 },
      crypto: { initialTHB: LIMITS.initialCapitalTHB, cashTHB: LIMITS.initialCapitalTHB, realizedTHB: 0, peakEquityTHB: LIMITS.initialCapitalTHB, maxDrawdownTHB: 0 }
    },
    positions: [], trades: [], scans: [], strategyByMarket: { stocks: null, crypto: null }, dayStats: {}
  };
}
let state = emptyState();
let scanBusy = false, loop = null, server;

async function loadState() {
  try {
    const raw = JSON.parse(await fs.readFile(STATE_FILE, "utf8"));
    if (raw?.version === 1 && raw.portfolios && Array.isArray(raw.positions) && Array.isArray(raw.trades)) state = raw;
  } catch (e) {
    if (e.code !== "ENOENT") console.error("STATE_READ_ERROR", e.message);
  }
}
async function persist() {
  await fs.mkdir(path.dirname(STATE_FILE), { recursive: true });
  const tmp = STATE_FILE + ".tmp";
  await fs.writeFile(tmp, JSON.stringify(state, null, 2), { mode: 0o600 });
  await fs.rename(tmp, STATE_FILE);
}
function dayTrades(market) { const today = dayKey(); return [...state.trades, ...state.positions].filter(t => t.market === market && openedDayKey(t.openedAt) === today); }
function dailyRealizedLoss(market) { return dayTrades(market).filter(t=>t.closedAt).reduce((s,t)=>s+Math.max(0,-safeNumber(t.netTHB)),0); }
function lossStreak(market) {
  const rows = dayTrades(market).filter(t=>t.closedAt).sort((a,b)=>b.closedAt.localeCompare(a.closedAt));
  let n=0; for (const t of rows) { if (t.netTHB < 0) n++; else break; } return n;
}
function openRisk(market) { return state.positions.filter(p=>p.market===market).reduce((s,p)=>s+p.riskTHB,0); }
function portfolioEquity(market) {
  const p=state.portfolios[market];
  const value=state.positions.filter(x=>x.market===market).reduce((s,x)=>s+x.qty*x.markPrice*USD_THB_FX,0);
  return round(p.cashTHB+value,2);
}
function guardEntry(market, risk) {
  const open=state.positions.filter(p=>p.market===market).length;
  if (open >= LIMITS.maxOpenPositions) return "MAX_OPEN_POSITIONS";
  if (dailyRealizedLoss(market)+openRisk(market)+risk > LIMITS.maxDailyLossTHB) return "DAILY_RISK_LIMIT";
  if (lossStreak(market) >= LIMITS.maxConsecutiveLosses) return "LOSS_STREAK_LIMIT";
  if (state.portfolios[market].cashTHB < 100) return "INSUFFICIENT_PAPER_CASH";
  return null;
}

async function fetchJson(url, headers = {}) {
  const response = await fetch(url, { headers: { accept: "application/json", ...headers }, signal: AbortSignal.timeout(10000) });
  if (!response.ok) throw new Error("HTTP_" + response.status + " " + url.split("?")[0]);
  return response.json();
}
function normalizeBinanceBars(rows) {
  return rows.map(x=>({ time:Number(x[0]), open:Number(x[1]), high:Number(x[2]), low:Number(x[3]), close:Number(x[4]), volume:Number(x[5]) }));
}
async function scanCrypto() {
  const tickers = await fetchJson("https://api.binance.com/api/v3/ticker/24hr");
  const exclude = new Set(["USDC","FDUSD","TUSD","USDP","DAI","BUSD","USDE","USTC"]);
  const leveraged = ["UP","DOWN","BULL","BEAR","3L","3S","5L","5S"];
  const rows = tickers.filter(t=>{const base=t.symbol.replace(/USDT$/,"");return t.symbol.endsWith("USDT") && !exclude.has(base) && !leveraged.some(s=>base.endsWith(s)) && Number(t.quoteVolume)>5000000 && Number(t.lastPrice)>0;})
    .sort((a,b)=>Number(b.quoteVolume)-Number(a.quoteVolume)).slice(0,18);
  const bars = await Promise.all(rows.map(async t=>{
    try {
      const [klines, book] = await Promise.all([
        fetchJson("https://api.binance.com/api/v3/klines?symbol="+encodeURIComponent(t.symbol)+"&interval=5m&limit=160"),
        fetchJson("https://api.binance.com/api/v3/ticker/bookTicker?symbol="+encodeURIComponent(t.symbol))
      ]);
      const b=normalizeBinanceBars(klines), spread=(Number(book.askPrice)-Number(book.bidPrice))/Number(t.lastPrice);
      return { market:"crypto", symbol:t.symbol, price:Number(t.lastPrice), spread, bars:b, change24h:Number(t.priceChangePercent), quoteVolume:Number(t.quoteVolume) };
    } catch (e) { console.warn("BINANCE_SYMBOL_SKIP",t.symbol,e.message); return null; }
  })).then(rows=>rows.filter(Boolean));
  return bars;
}
async function scanStocks() {
  if (!process.env.ALPACA_PAPER_KEY || !process.env.ALPACA_PAPER_SECRET) return { unavailable:true, reason:"ALPACA_PAPER_DATA_CREDENTIALS_MISSING", rows:[] };
  const symbols=UNIVERSE.stocks.join(",");
  const url="https://data.alpaca.markets/v2/stocks/bars?symbols="+encodeURIComponent(symbols)+"&timeframe=5Min&limit=160&feed="+encodeURIComponent(process.env.ALPACA_DATA_FEED||"iex");
  const payload=await fetchJson(url, { "APCA-API-KEY-ID":process.env.ALPACA_PAPER_KEY, "APCA-API-SECRET-KEY":process.env.ALPACA_PAPER_SECRET });
  const clock=await fetchJson("https://paper-api.alpaca.markets/v2/clock", { "APCA-API-KEY-ID":process.env.ALPACA_PAPER_KEY, "APCA-API-SECRET-KEY":process.env.ALPACA_PAPER_SECRET });
  const rows=Object.entries(payload.bars||{}).map(([symbol,arr])=>({
    market:"stocks",symbol,price:Number(arr.at(-1)?.c||0),spread:0.001,
    bars:arr.map(x=>({time:Date.parse(x.t),open:Number(x.o),high:Number(x.h),low:Number(x.l),close:Number(x.c),volume:Number(x.v)})),
    marketOpen:Boolean(clock.is_open)
  })).filter(x=>x.price>0);
  return { unavailable:false, marketOpen:Boolean(clock.is_open), rows };
}

function closePosition(position, rawPrice, reason) {
  const exitPrice=rawPrice*(1-LIMITS.slippageRate);
  const exitNotional=position.qty*exitPrice*USD_THB_FX;
  const exitFee=exitNotional*LIMITS.feeRate;
  const proceeds=exitNotional-exitFee;
  const net=proceeds-position.entryCostTHB;
  const p=state.portfolios[position.market];
  p.cashTHB+=proceeds;
  p.realizedTHB+=net;
  const trade={...position, exitPrice:round(exitPrice,8), closedAt:nowIso(), netTHB:round(net,2), exitReason:reason};
  state.trades.unshift(trade);
  state.positions=state.positions.filter(x=>x.id!==position.id);
}
function managePositions(marketRows) {
  const byKey=new Map(marketRows.map(r=>[r.symbol,r]));
  for (const p of [...state.positions]) {
    const row=byKey.get(p.symbol);
    if (!row || !(row.price>0)) continue;
    p.markPrice=row.price;
    if (row.price<=p.stopPrice) closePosition(p,Math.min(row.price,p.stopPrice),"STOP_LOSS");
    else if (row.price>=p.targetPrice) closePosition(p,p.targetPrice,"TAKE_PROFIT");
    else if (Date.now()-Date.parse(p.openedAt)>24*60*60*1000) closePosition(p,row.price,"TIME_EXIT_24H");
  }
}
function maybeOpen(market, candidate, selectedStrategy) {
  if (candidate.spread>0.003 || candidate.price<=0) return null;
  const signal=evaluateSignal(candidate.bars,selectedStrategy);
  if (!signal) return null;
  if (state.positions.some(p=>p.market===market && p.symbol===candidate.symbol)) return null;
  const strategy=STRATEGIES[selectedStrategy], fillEstimate=candidate.price*(1+LIMITS.slippageRate), stop=candidate.price-signal.atr*strategy.stopAtr;
  const size=positionSizing({price:fillEstimate,stop,fx:USD_THB_FX,cashTHB:state.portfolios[market].cashTHB});
  if (!size) return null;
  const reason=guardEntry(market,size.riskTHB);
  if (reason) return { skipped:true, reason, market, symbol:candidate.symbol, score:signal.rsi };
  const fill=fillEstimate;
  const notional=size.qty*fill*USD_THB_FX, fee=notional*LIMITS.feeRate;
  if (notional+fee>state.portfolios[market].cashTHB) return null;
  const risk=round((fill-stop)*size.qty*USD_THB_FX+notional*(LIMITS.feeRate*2+LIMITS.slippageRate*2),2);
  if (risk>LIMITS.maxRiskTHB) return null;
  const position={
    id:newId(),market,symbol:candidate.symbol,side:"LONG",strategy:selectedStrategy,
    entryPrice:round(fill,8),markPrice:candidate.price,stopPrice:round(stop,8),
    targetPrice:round(fill+(fill-stop)*strategy.rewardRisk,8),qty:size.qty,
    notionalTHB:round(notional,2),riskTHB:risk,entryFeeTHB:round(fee,2),
    entryCostTHB:round(notional+fee,2),openedAt:nowIso()
  };
  state.portfolios[market].cashTHB-=position.entryCostTHB;
  state.positions.push(position);
  return { opened:true, market, symbol:candidate.symbol, strategy:selectedStrategy, entry:position.entryPrice, stop:position.stopPrice, target:position.targetPrice, riskTHB:risk };
}

async function runScan() {
  if (scanBusy || !state.running) return;
  scanBusy=true;
  try {
    const [cryptoResult, stockResult]=await Promise.allSettled([scanCrypto(),scanStocks()]);
    const cryptoRows=cryptoResult.status==="fulfilled"?cryptoResult.value:[];
    if(cryptoResult.status==="rejected") console.error("BINANCE_SCAN_ERROR",cryptoResult.reason?.message||"unknown");
    const stocks=stockResult.status==="fulfilled"?stockResult.value:{unavailable:true,reason:"ALPACA_SCAN_FAILED",rows:[]};
    if(stockResult.status==="rejected") console.error("ALPACA_SCAN_ERROR",stockResult.reason?.message||"unknown");
    const stockRows=stocks.rows||[];
    if (!state.running) return;
    managePositions(cryptoRows);
    managePositions(stockRows);
    const cryptoChoice=chooseStrategy(cryptoRows.map(x=>x.bars));
    const stockChoice=chooseStrategy(stockRows.map(x=>x.bars));
    state.strategyByMarket={crypto:cryptoChoice,stocks:stockChoice};
    const events=[];
    for (const row of cryptoRows.sort((a,b)=>b.quoteVolume-a.quoteVolume)) {
      const e=maybeOpen("crypto",row,cryptoChoice.selected); if(e) events.push(e);
    }
    if (stocks.marketOpen) {
      for (const row of stockRows) { const e=maybeOpen("stocks",row,stockChoice.selected); if(e) events.push(e); }
    }
    const scan={
      at:nowIso(), crypto:{scanned:cryptoRows.length,selected:cryptoChoice.selected,adaptive:cryptoChoice.adaptive},
      stocks:{scanned:stockRows.length,marketOpen:Boolean(stocks.marketOpen),selected:stockChoice.selected,adaptive:stockChoice.adaptive,unavailable:Boolean(stocks.unavailable),reason:stocks.reason||null},
      events
    };
    state.scans.unshift(scan); state.scans=state.scans.slice(0,100);
    state.scanCount++; state.lastScanAt=scan.at; state.lastError=null;
  } catch (e) {
    state.lastError=e.message||"SCAN_FAILED";
    console.error("SCAN_ERROR",state.lastError);
  } finally {
    scanBusy=false;
    await persist();
  }
}
function startLoop() {
  if (loop) clearInterval(loop);
  loop=setInterval(()=>{runScan().catch(e=>console.error(e));},SCAN_INTERVAL_MS);
  runScan().catch(e=>console.error(e));
}
function stopLoop() { if(loop) clearInterval(loop); loop=null; }

function snapshot() {
  const portfolios={};
  for(const market of ["stocks","crypto"]) {
    const p=state.portfolios[market];
    const open=state.positions.filter(x=>x.market===market);
    const equity=portfolioEquity(market);
    p.peakEquityTHB=Math.max(p.peakEquityTHB,equity);
    p.maxDrawdownTHB=Math.max(p.maxDrawdownTHB,p.peakEquityTHB-equity);
    portfolios[market]={...p,equityTHB:equity,unrealizedTHB:round(equity-p.cashTHB-open.reduce((s,x)=>s+x.entryCostTHB,0),2),openCount:open.length,openRiskTHB:round(openRisk(market),2),dailyRealizedLossTHB:round(dailyRealizedLoss(market),2),lossStreak:lossStreak(market)};
  }
  return { mode:"PAPER_ONLY", liveTradingEnabled:false, running:state.running, startedAt:state.startedAt, lastScanAt:state.lastScanAt, lastError:state.lastError, scanCount:state.scanCount, scanIntervalSeconds:SCAN_INTERVAL_MS/1000, fxUsdThb:USD_THB_FX, limits:LIMITS, portfolios, positions:state.positions, trades:state.trades.slice(0,100), scans:state.scans.slice(0,20), strategyByMarket:state.strategyByMarket };
}

function authorized(req) {
  const supplied=req.headers["x-fast-profit-token"]||"";
  if(!CONTROL_TOKEN||!supplied) return false;
  const a=Buffer.from(supplied), b=Buffer.from(CONTROL_TOKEN);
  return a.length===b.length && crypto.timingSafeEqual(a,b);
}
function send(res,status,body) { res.writeHead(status,{"content-type":"application/json; charset=utf-8","cache-control":"no-store"});res.end(JSON.stringify(body)); }
function parseBody(req) { return new Promise((resolve,reject)=>{let raw="";req.on("data",c=>{raw+=c;if(raw.length>4096)reject(new Error("BODY_TOO_LARGE"));});req.on("end",()=>{try{resolve(raw?JSON.parse(raw):{});}catch{reject(new Error("INVALID_JSON"));}});req.on("error",reject);}); }

async function route(req,res) {
  const url=new URL(req.url,"http://localhost");
  if(req.method==="OPTIONS") {
    const origin=req.headers.origin;
    if(origin===DASHBOARD_ORIGIN) res.writeHead(204,{"access-control-allow-origin":origin,"access-control-allow-methods":"GET,POST,OPTIONS","access-control-allow-headers":"Content-Type,X-Fast-Profit-Token","vary":"Origin","access-control-max-age":"600"});
    else res.writeHead(403);
    return res.end();
  }
  if(url.pathname==="/health" && req.method==="GET") return send(res,200,{ok:true,mode:"PAPER_ONLY",liveTradingEnabled:false,running:state.running,uptimeSeconds:Math.floor(process.uptime())});
  const origin=req.headers.origin;
  if(origin && origin!==DASHBOARD_ORIGIN) return send(res,403,{error:"ORIGIN_NOT_ALLOWED"});
  if(!authorized(req)) return send(res,401,{error:"UNAUTHORIZED"});
  res.setHeader("access-control-allow-origin",DASHBOARD_ORIGIN);
  res.setHeader("vary","Origin");
  if(url.pathname==="/api/status"&&req.method==="GET") return send(res,200,snapshot());
  if(url.pathname==="/api/control/start"&&req.method==="POST") {
    if(!state.running){state.running=true;state.startedAt=nowIso();await persist();startLoop();}
    return send(res,200,{ok:true,running:true,mode:"PAPER_ONLY"});
  }
  if(url.pathname==="/api/control/stop"&&req.method==="POST") {
    state.running=false;stopLoop();await persist();return send(res,200,{ok:true,running:false,openPositionsRemain:true,mode:"PAPER_ONLY"});
  }
  if(url.pathname==="/api/control/close-all"&&req.method==="POST") {
    state.running=false;stopLoop();
    for(const p of [...state.positions]) closePosition(p,p.markPrice,"MANUAL_EMERGENCY_CLOSE");
    await persist();return send(res,200,{ok:true,running:false,closed:true,openPositions:0});
  }
  if(url.pathname==="/api/scan"&&req.method==="POST") { if(!state.running)return send(res,409,{error:"ENGINE_STOPPED"});await runScan();return send(res,200,{ok:true,lastScanAt:state.lastScanAt}); }
  return send(res,404,{error:"NOT_FOUND"});
}

async function main() {
  if(!CONTROL_TOKEN) throw new Error("FAST_PROFIT_CONTROL_TOKEN is required. Set a long random secret in the service environment.");
  await loadState();
  server=http.createServer((req,res)=>route(req,res).catch(e=>{console.error("API_ERROR",e);send(res,500,{error:"INTERNAL_ERROR"});}));
  server.listen(PORT,"0.0.0.0",()=>console.log("FAST PROFIT PAPER ENGINE listening on",PORT));
  if(state.running) startLoop();
  const shutdown=async()=>{state.running=false;stopLoop();await persist();server.close(()=>process.exit(0));setTimeout(()=>process.exit(1),10000).unref();};
  process.on("SIGTERM",shutdown);process.on("SIGINT",shutdown);
}
if (process.argv[1] && path.resolve(process.argv[1])===path.resolve(fileURLToPath(import.meta.url))) main().catch(e=>{console.error(e);process.exit(1);});
