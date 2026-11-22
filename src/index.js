const json = (body, status = 200) => new Response(JSON.stringify(body), {
  status,
  headers: {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "access-control-allow-origin": "*"
  }
});

async function altcoinScanner(request) {
  const cacheKey = new Request(new URL("/__cache/binance-altcoin-v1", request.url).toString(), { method: "GET" });
  const cache = caches.default;
  const force = new URL(request.url).searchParams.get("refresh") === "1";
  if (!force) {
    const cached = await cache.match(cacheKey);
    if (cached) return cached;
  }

  const endpoints = [
    "https://data-api.binance.vision/api/v3/ticker/24hr",
    "https://api.binance.com/api/v3/ticker/24hr",
    "https://api-gcp.binance.com/api/v3/ticker/24hr"
  ];
  let raw = null;
  let lastError = "ไม่มีแหล่งข้อมูลตอบกลับ";
  let provider = "Binance Spot";
  for (const endpoint of endpoints) {
    try {
      const upstream = await fetch(endpoint, {
        method: "GET",
        headers: { "accept": "application/json", "user-agent": "BlessingTrade-AltcoinHunter/1.0" },
        signal: AbortSignal.timeout(9000)
      });
      if (!upstream.ok) {
        lastError = "HTTP " + upstream.status + " จาก " + new URL(endpoint).hostname;
        continue;
      }
      const payload = await upstream.json();
      if (!Array.isArray(payload)) {
        lastError = "รูปแบบข้อมูลไม่ถูกต้องจาก " + new URL(endpoint).hostname;
        continue;
      }
      raw = payload;
      provider = "Binance Spot (" + new URL(endpoint).hostname + ")";
      break;
    } catch (error) {
      lastError = (error && error.name ? error.name : "ConnectionError") + " จาก " + new URL(endpoint).hostname;
    }
  }

  if (!raw) {
    return json({
      status: "source_error", provider: "Binance Spot",
      message: "เชื่อมต่อแหล่งข้อมูลตลาดไม่สำเร็จ: " + lastError,
      scanned: 0, evaluated: 0, candidateCount: 0, candidates: [],
      updatedAt: new Date().toISOString()
    }, 502);
  }

  const excluded = new Set(["BTC","ETH","BNB","SOL","XRP","DOGE","USDC","FDUSD","TUSD","USDP","DAI","EUR","USDE","USTC","BUSD","USDT"]);
  const suffixes = ["UP","DOWN","BULL","BEAR","3L","3S","5L","5S"];
  const eligible = [];
  for (const row of raw) {
    const pair = String(row.symbol || "").toUpperCase();
    if (!pair.endsWith("USDT")) continue;
    const base = pair.slice(0, -4);
    if (!base || excluded.has(base) || suffixes.some(s => base.endsWith(s))) continue;
    const price = Number(row.lastPrice);
    const quoteVolume = Number(row.quoteVolume);
    const change = Number(row.priceChangePercent);
    const open = Number(row.openPrice);
    const bid = Number(row.bidPrice);
    const ask = Number(row.askPrice);
    if (!(price > 0) || !(quoteVolume >= 250000) || !Number.isFinite(change) || !(open > 0)) continue;
    eligible.push({
      symbol: pair, exchangeSymbol: pair, price,
      change24h: change, quoteVolume24h: quoteVolume,
      m1: null, m5: null, rvol: null, breakout: false,
      volumeSpike: false, momentum: change > 0,
      spreadPct: bid > 0 && ask >= bid ? (ask - bid) / price * 100 : null,
      source: "Binance", marketTimestamp: row.closeTime || Date.now()
    });
  }
  eligible.sort((a, b) => (b.change24h - a.change24h) || (b.quoteVolume24h - a.quoteVolume24h));
  const candidates = eligible.slice(0, 40);
  const result = json({
    status: candidates.length ? "ok" : "empty",
    provider, exchange: "Binance", scanned: raw.length,
    evaluated: candidates.length, candidateCount: candidates.length,
    candidates, updatedAt: new Date().toISOString(),
    coverage: "Binance Spot USDT pairs; quote volume >= 250,000 USDT",
    metricsNote: "24h ticker snapshot; 1m/5m candles and RVOL are not included."
  });
  const cacheResponse = new Response(result.body, result);
  cacheResponse.headers.set("cache-control", "public, max-age=0, s-maxage=20");
  await cache.put(cacheKey, cacheResponse.clone());
  return cacheResponse;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/api/altcoin-scanner" && request.method === "GET") {
      return altcoinScanner(request);
    }
    return env.ASSETS.fetch(request);
  }
};
