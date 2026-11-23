const json = (body, status = 200, headers = {}) => new Response(JSON.stringify(body), {
  status,
  headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", "access-control-allow-origin": "*", ...headers }
});

const EXCHANGES = {
  Binance: {
    tickers: "https://data-api.binance.vision/api/v3/ticker/24hr",
    candles: (s) => "https://data-api.binance.vision/api/v3/klines?symbol=" + s + "&interval=1m&limit=61",
    parseTickers: (a) => a.filter(x => String(x.symbol || "").endsWith("USDT")).map(x => ({
      symbol: x.symbol, price: Number(x.lastPrice), change24h: Number(x.priceChangePercent),
      quoteVolume24h: Number(x.quoteVolume), bid: Number(x.bidPrice), ask: Number(x.askPrice)
    })),
    parseBars: (a) => a.map(x => ({ time: Number(x[0]), open: Number(x[1]), high: Number(x[2]), low: Number(x[3]), close: Number(x[4]), quoteVolume: Number(x[7]) }))
  },
  Bybit: {
    tickers: "https://api.bybit.com/v5/market/tickers?category=spot",
    candles: (s) => "https://api.bybit.com/v5/market/kline?category=spot&symbol=" + s + "&interval=1&limit=61",
    parseTickers: (p) => (p.result?.list || []).filter(x => String(x.symbol || "").endsWith("USDT")).map(x => ({
      symbol: x.symbol, price: Number(x.lastPrice), change24h: Number(x.price24hPcnt) * 100,
      quoteVolume24h: Number(x.turnover24h), bid: Number(x.bid1Price), ask: Number(x.ask1Price)
    })),
    parseBars: (p) => (p.result?.list || []).slice().reverse().map(x => ({ time: Number(x[0]), open: Number(x[1]), high: Number(x[2]), low: Number(x[3]), close: Number(x[4]), quoteVolume: Number(x[6]) }))
  },
  OKX: {
    tickers: "https://www.okx.com/api/v5/market/tickers?instType=SPOT",
    candles: (s) => "https://www.okx.com/api/v5/market/candles?instId=" + s.replace("USDT", "-USDT") + "&bar=1m&limit=61",
    parseTickers: (p) => (p.data || []).filter(x => String(x.instId || "").endsWith("-USDT")).map(x => ({
      symbol: x.instId.replace("-", ""), price: Number(x.last), change24h: Number(x.open24h) > 0 ? (Number(x.last) / Number(x.open24h) - 1) * 100 : NaN,
      quoteVolume24h: Number(x.volCcy24h), bid: Number(x.bidPx), ask: Number(x.askPx)
    })),
    parseBars: (p) => (p.data || []).slice().reverse().map(x => ({ time: Number(x[0]), open: Number(x[1]), high: Number(x[2]), low: Number(x[3]), close: Number(x[4]), quoteVolume: Number(x[7]) }))
  }
};

const EXCLUDED = new Set(["BTC","ETH","BNB","SOL","XRP","DOGE","USDC","FDUSD","TUSD","USDP","DAI","EUR","USDE","USTC","BUSD","USDT"]);
const LEVERAGED = ["UP","DOWN","BULL","BEAR","3L","3S","5L","5S"];

async function getJson(url) {
  const r = await fetch(url, { headers: { "accept": "application/json", "user-agent": "BlessingTrade-AltcoinHunter/2.0" }, signal: AbortSignal.timeout(6000) });
  if (!r.ok) throw new Error("HTTP_" + r.status);
  return r.json();
}

async function mapLimit(items, limit, fn) {
  const out = new Array(items.length);
  let cursor = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (cursor < items.length) {
      const i = cursor++;
      try { out[i] = await fn(items[i]); } catch (e) { out[i] = { error: e?.message || "request_failed" }; }
    }
  }));
  return out;
}

function metrics(row, bars) {
  const valid = bars.filter(x => x.close > 0 && x.high > 0);
  if (valid.length < 21) return { ...row, m1: null, m5: null, m15: null, m60: null, rvol: null, breakout: false, volumeSpike: false, momentum: false, score: 0, signal: "INSUFFICIENT_DATA", barsAvailable: valid.length };
  const last = valid[valid.length - 1], close = last.close;
  const move = (n) => valid.length > n && valid[valid.length - 1 - n].close > 0 ? (close / valid[valid.length - 1 - n].close - 1) * 100 : null;
  const priorVol = valid.slice(-21, -1).map(x => x.quoteVolume).filter(x => x > 0);
  const avgVol = priorVol.length ? priorVol.reduce((a,b) => a+b, 0) / priorVol.length : 0;
  const rvol = avgVol > 0 ? last.quoteVolume / avgVol : null;
  const priorHigh = Math.max(...valid.slice(-21, -1).map(x => x.high));
  const breakout = close > priorHigh;
  const spreadPct = row.bid > 0 && row.ask >= row.bid && row.price > 0 ? (row.ask-row.bid)/row.price*100 : null;
  const m1=move(1), m5=move(5), m15=move(15), m60=move(60);
  const volumeSpike = rvol !== null && rvol >= 2;
  const momentum = m1 !== null && m5 !== null && m1 > 0 && m5 > 0 && (m15 === null || m15 > 0);
  let score = 0;
  score += Math.max(0, Math.min(15, row.change24h * 1.5));
  score += Math.max(0, Math.min(20, (m1 || 0) * 18));
  score += Math.max(0, Math.min(20, (m5 || 0) * 8));
  score += Math.max(0, Math.min(12, (m15 || 0) * 3));
  score += Math.max(0, Math.min(8, (m60 || 0)));
  score += rvol === null ? 0 : Math.min(15, Math.max(0, (rvol - 1) * 7.5));
  if (breakout) score += 10;
  if (spreadPct !== null && spreadPct <= 0.2) score += 5;
  else if (spreadPct === null || spreadPct > 0.5) score -= 10;
  score = Math.max(0, Math.min(100, Math.round(score)));
  const fastEntry = momentum && volumeSpike && breakout && spreadPct !== null && spreadPct <= 0.5 && score >= 60;
  return { ...row, m1, m5, m15, m60, rvol, breakout, volumeSpike, momentum, spreadPct, score, fastEntry, signal: fastEntry ? "FAST_ENTRY" : breakout ? "BREAKOUT" : volumeSpike ? "VOLUME_SPIKE" : momentum ? "MOMENTUM" : "WATCH", barsAvailable: valid.length };
}

async function scanExchange(name, cfg) {
  try {
    const payload = await getJson(cfg.tickers);
    if (name === "Bybit" && payload.retCode !== 0) throw new Error(payload.retMsg || "BYBIT_API_ERROR");
    if (name === "OKX" && payload.code !== "0") throw new Error(payload.msg || "OKX_API_ERROR");
    const tickers = cfg.parseTickers(payload);
    const eligible = tickers.filter(x => {
      const base = x.symbol.replace(/USDT$/, "");
      return base && !EXCLUDED.has(base) && !LEVERAGED.some(s => base.endsWith(s)) &&
        x.price > 0 && Number.isFinite(x.change24h) && x.quoteVolume24h >= 250000;
    }).sort((a,b) => b.quoteVolume24h-a.quoteVolume24h).slice(0, 25);
    const evaluated = await mapLimit(eligible, 10, async row => {
      const barsPayload = await getJson(cfg.candles(row.symbol));
      const bars = cfg.parseBars(barsPayload);
      return metrics({ ...row, exchange: name, source: name, symbol: row.symbol }, bars);
    });
    const candidates = evaluated.filter(x => x && !x.error);
    return { exchange: name, status: "ok", scanned: tickers.length, eligible: eligible.length, evaluated: candidates.length, candidates };
  } catch (e) {
    return { exchange: name, status: "source_error", scanned: 0, eligible: 0, evaluated: 0, candidates: [], error: e?.message || "source_error" };
  }
}

async function altcoinScanner(request) {
  const key = new Request(new URL("/__cache/altcoin-multi-v2", request.url).toString());
  const force = new URL(request.url).searchParams.get("refresh") === "1";
  if (!force) {
    const cached = await caches.default.match(key);
    if (cached) return cached;
  }
  const names = Object.keys(EXCHANGES);
  const exchanges = await Promise.all(names.map(name => scanExchange(name, EXCHANGES[name])));
  const candidates = exchanges.flatMap(x => x.candidates).sort((a,b) => b.score-a.score || b.quoteVolume24h-a.quoteVolume24h).slice(0, 60);
  const good = exchanges.filter(x => x.status === "ok");
  const result = json({
    status: good.length ? (candidates.length ? "ok" : "empty") : "source_error",
    provider: "Binance · Bybit · OKX public Spot APIs",
    exchanges: exchanges.map(x => ({ exchange: x.exchange, status: x.status, scanned: x.scanned, eligible: x.eligible, evaluated: x.evaluated, error: x.error || null })),
    exchangeCount: good.length, scanned: exchanges.reduce((n,x) => n+x.scanned, 0),
    evaluated: exchanges.reduce((n,x) => n+x.evaluated, 0),
    candidateCount: candidates.length, candidates,
    updatedAt: new Date().toISOString(), refreshSeconds: 60,
    criteria: { market: "Spot/USDT", minQuoteVolume24h: 250000, momentum: "1m/5m/15m/60m", rvol: "current 1m quote volume / prior 20-bar average", breakout: "close above previous 20 one-minute highs", maxSpreadPct: 0.5, fastEntryScore: 60, takeProfitPct: 3, stopLossPct: 1.5 },
    metricsNote: "All exchange results use the same formula. Scores rank candidates; FAST_ENTRY requires positive momentum, RVOL >= 2, breakout, spread <= 0.5%, and score >= 60. Not a trade instruction."
  });
  const cached = new Response(result.body, { status: result.status, headers: result.headers });
  cached.headers.set("cache-control", "public, max-age=0, s-maxage=15");
  await caches.default.put(key, cached.clone());
  return cached;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname.replace(/\/+$/, "") || "/";
    if (pathname === "/api/altcoin-scanner") {
      if (request.method === "OPTIONS") {
        return new Response(null, { status: 204, headers: {
          "access-control-allow-origin": "*",
          "access-control-allow-methods": "GET, OPTIONS",
          "access-control-allow-headers": "Accept, Content-Type",
          "access-control-max-age": "86400"
        }});
      }
      if (request.method !== "GET" && request.method !== "HEAD") {
        return json({ status: "method_not_allowed", message: "GET only" }, 405, { "allow": "GET, HEAD, OPTIONS" });
      }
      try {
        const response = await altcoinScanner(request);
        const headers = new Headers(response.headers);
        headers.set("content-type", "application/json; charset=utf-8");
        headers.set("x-blessing-api", "altcoin-scanner-v2");
        headers.set("cache-control", "no-store");
        return new Response(request.method === "HEAD" ? null : response.body, { status: response.status, headers });
      } catch (error) {
        return json({ status: "source_error", message: error?.message || "scanner_failed", candidates: [], updatedAt: new Date().toISOString() }, 502);
      }
    }
    if (pathname === "/admin-members" || pathname === "/member") {
      return Response.redirect(new URL("/", request.url), 302);
    }
    if (pathname === "/paper-reports") {
      const reportUrl = new URL("/paper-reports.html", request.url);
      return env.ASSETS.fetch(new Request(reportUrl, request));
    }
    return env.ASSETS.fetch(request);
  }
};
