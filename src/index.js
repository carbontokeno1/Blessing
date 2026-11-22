const json = (body, status = 200) => new Response(JSON.stringify(body), {
  status,
  headers: {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "access-control-allow-origin": "*"
  }
});

async function altcoinScanner(request) {
  const cacheKey = new Request(new URL("/__cache/bitmart-altcoin-v1", request.url).toString(), { method: "GET" });
  const cache = caches.default;
  const force = new URL(request.url).searchParams.get("refresh") === "1";
  if (!force) {
    const cached = await cache.match(cacheKey);
    if (cached) return cached;
  }

  try {
    const upstream = await fetch("https://api-cloud.bitmart.com/spot/quotation/v3/tickers", {
      method: "GET",
      headers: { "accept": "application/json", "user-agent": "BlessingTrade-AltcoinHunter/1.0" },
      cf: { cacheTtl: 20, cacheEverything: true }
    });
    if (!upstream.ok) {
      return json({ status: "source_error", provider: "BitMart Spot", message: "BitMart ตอบกลับ HTTP " + upstream.status, scanned: 0, candidates: [] }, 502);
    }
    const raw = await upstream.json();
    if (!raw || raw.code !== 1000 || !Array.isArray(raw.data)) {
      return json({ status: "source_error", provider: "BitMart Spot", message: "รูปแบบข้อมูลจาก BitMart ไม่ถูกต้อง", scanned: 0, candidates: [] }, 502);
    }

    const excluded = new Set(["BTC","ETH","BNB","SOL","XRP","DOGE","USDC","FDUSD","TUSD","USDP","DAI","EUR","USDE","USTC","BUSD"]);
    const suffixes = ["UP","DOWN","BULL","BEAR","3L","3S","5L","5S"];
    const eligible = [];
    for (const row of raw.data) {
      if (!Array.isArray(row) || row.length < 13) continue;
      const pair = String(row[0] || "").toUpperCase();
      if (!pair.endsWith("_USDT")) continue;
      const base = pair.slice(0, -5);
      if (!base || excluded.has(base) || suffixes.some(s => base.endsWith(s))) continue;
      const price = Number(row[1]), quoteVolume = Number(row[3]), open = Number(row[4]);
      const bid = Number(row[8]), ask = Number(row[10]), stamp = Number(row[12]);
      if (!(price > 0) || !(quoteVolume > 0) || !(open > 0)) continue;
      const change = (price / open - 1) * 100;
      eligible.push({
        symbol: base + "USDT", exchangeSymbol: pair, price,
        change24h: change, quoteVolume24h: quoteVolume,
        m1: null, m5: null, rvol: null, breakout: false,
        volumeSpike: false, momentum: change > 0,
        spreadPct: bid > 0 && ask >= bid ? (ask - bid) / price * 100 : null,
        source: "BitMart", marketTimestamp: stamp
      });
    }
    eligible.sort((a, b) => (b.change24h - a.change24h) || (b.quoteVolume24h - a.quoteVolume24h));
    const candidates = eligible.slice(0, 40);
    const result = json({
      status: candidates.length ? "ok" : "empty",
      provider: "BitMart Spot public market data (Cloudflare edge)",
      exchange: "BitMart", scanned: eligible.length,
      evaluated: candidates.length, candidateCount: candidates.length,
      candidates, updatedAt: new Date().toISOString(),
      coverage: "BitMart Spot USDT pairs; filtered by 24h quote volume >= 250,000 USDT",
      metricsNote: "24h ticker snapshot; 1m/5m candles and RVOL are not included."
    });
    const cacheResponse = new Response(result.body, result);
    cacheResponse.headers.set("cache-control", "public, max-age=0, s-maxage=20");
    await cache.put(cacheKey, cacheResponse.clone());
    return cacheResponse;
  } catch (error) {
    return json({
      status: "source_error", provider: "BitMart Spot (Cloudflare edge)",
      message: "เชื่อมต่อ BitMart ไม่สำเร็จ: " + (error && error.name ? error.name : "network error"),
      scanned: 0, evaluated: 0, candidateCount: 0, candidates: [],
      updatedAt: new Date().toISOString()
    }, 502);
  }
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
