const json = (body, status = 200, headers = {}) => new Response(JSON.stringify(body), {
  status,
  headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", "access-control-allow-origin": "*", ...headers }
});


// Binance TH private account read: credentials are Cloudflare Worker secrets only.
const SUPABASE_URL = "https://dyzumqcqvnznlkugimxo.supabase.co";
const SUPABASE_ANON_KEY = "sb_publishable_6RthgpRI_bdqxWjMjLVYkQ_nJzw1YWZ";

async function isBlessingOwner(request) {
  const auth = request.headers.get("authorization") || "";
  if (!auth.startsWith("Bearer ")) return false;
  const headers = { apikey: SUPABASE_ANON_KEY, authorization: auth };
  const userRes = await fetch(SUPABASE_URL + "/auth/v1/user", { headers });
  if (!userRes.ok) return false;
  const user = await userRes.json();
  if (!user?.id) return false;
  const profileUrl = new URL(SUPABASE_URL + "/rest/v1/profiles");
  profileUrl.searchParams.set("select", "role,status");
  profileUrl.searchParams.set("user_id", "eq." + user.id);
  const profileRes = await fetch(profileUrl, { headers });
  if (!profileRes.ok) return false;
  const profiles = await profileRes.json();
  return profiles?.[0]?.role === "super_admin" && profiles?.[0]?.status === "active";
}

async function memberBackendProxy(request, backendPath) {
  const authorization = request.headers.get("authorization") || "";
  if (!authorization.startsWith("Bearer ")) {
    return json({ ok: false, message: "กรุณาเข้าสู่ระบบก่อน" }, 401);
  }
  try {
    const response = await fetch("https://blessing-webull-realtime-djfm.onrender.com" + backendPath, {
      method: request.method,
      headers: { "accept": "application/json", "authorization": authorization },
      signal: AbortSignal.timeout(25000),
      cache: "no-store"
    });
    const payload = await response.json().catch(() => ({ ok: false, message: "Backend returned invalid JSON" }));
    return json(payload, response.status);
  } catch (error) {
    console.error("member_backend_proxy_failed", backendPath, error?.message || error);
    return json({
      ok: false,
      code: "MEMBER_BACKEND_UNAVAILABLE",
      message: "ระบบกำลังเชื่อมต่อเซิร์ฟเวอร์ข้อมูลสมาชิกไม่สำเร็จ กรุณาลองใหม่อีกครั้ง"
    }, 502);
  }
}

async function binanceThAccount(request) {
  // Forward the authenticated, read-only request to the existing Render backend.
  // Binance credentials remain only in Render secrets; never expose them to the browser.
  const authorization = request.headers.get("authorization") || "";
  if (!authorization.startsWith("Bearer ")) {
    return json({ ok: false, configured: false, connected: false, provider: "Binance TH", message: "กรุณาเข้าสู่ระบบก่อน" }, 401);
  }
  try {
    const response = await fetch("https://blessing-webull-realtime-djfm.onrender.com/binance-th/account", {
      method: "GET",
      headers: { "accept": "application/json", "authorization": authorization },
      signal: AbortSignal.timeout(20000),
      cache: "no-store"
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      return json({
        ok: false, configured: false, connected: false, provider: "Binance TH",
        message: payload.detail || payload.message || `Render backend ตอบ HTTP ${response.status}`
      }, response.status);
    }
    return json(payload, payload.ok ? 200 : 502);
  } catch (error) {
    return json({
      ok: false, configured: false, connected: false, provider: "Binance TH",
      message: "เชื่อมต่อ Render backend ไม่สำเร็จ: " + (error?.message || "network_error")
    }, 502);
  }
}

async function binanceThPlaceOrder(request) {
  const authorization = request.headers.get("authorization") || "";
  if (!authorization.startsWith("Bearer ")) {
    return json({ ok: false, message: "กรุณาเข้าสู่ระบบก่อน" }, 401);
  }
  try {
    const body = await request.json();
    const response = await fetch("https://blessing-webull-realtime-djfm.onrender.com/binance-th/order", {
      method: "POST",
      headers: { "accept": "application/json", "content-type": "application/json", "authorization": authorization },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(25000),
      cache: "no-store"
    });
    const payload = await response.json().catch(() => ({}));
    return json(payload, response.status);
  } catch (error) {
    return json({ ok: false, message: "ส่งคำขอไป Render ไม่สำเร็จ: " + (error?.message || "network_error") }, 502);
  }
}



async function binanceThLiveProxy(request, backendPath, method = "GET", body = undefined) {
  const authorization = request.headers.get("authorization") || "";
  if (!authorization.startsWith("Bearer ")) return json({ ok:false, message:"กรุณาเข้าสู่ระบบก่อน" },401);
  if (!(await isBlessingOwner(request))) return json({ ok:false, message:"ต้องเข้าสู่ระบบด้วยบัญชี Super Admin ของ BLESSING" },403);
  try {
    const response = await fetch("https://blessing-webull-realtime-djfm.onrender.com" + backendPath, {
      method,
      headers: { "accept":"application/json", "authorization":authorization, ...(body !== undefined ? {"content-type":"application/json"} : {}) },
      body: body !== undefined ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(30000),
      cache:"no-store"
    });
    const payload = await response.json().catch(()=>({}));
    return json(payload,response.status);
  } catch(error) {
    return json({ok:false,message:"เชื่อมต่อ Render backend ไม่สำเร็จ: "+(error?.message||"network_error")},502);
  }
}

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
  // Scheduled keep-alive: wake the existing Render Free web service before its
  // 15-minute idle window expires. This is health-only; it never touches trading.
  async scheduled(controller, env, ctx) {
    const startedAt = Date.now();
    try {
      const response = await fetch("https://blessing-webull-realtime-djfm.onrender.com/health", {
        method: "GET",
        headers: { "accept": "application/json", "user-agent": "BLESSING-KeepAlive/1.0" },
        signal: AbortSignal.timeout(20000),
        cache: "no-store"
      });
      const body = await response.text();
      console.log(JSON.stringify({
        event: "render_keepalive",
        ok: response.ok,
        status: response.status,
        elapsedMs: Date.now() - startedAt,
        response: body.slice(0, 300),
        scheduledAt: new Date(controller.scheduledTime).toISOString()
      }));
      if (!response.ok) throw new Error("Render health returned HTTP " + response.status);
    } catch (error) {
      console.error(JSON.stringify({
        event: "render_keepalive_failed",
        elapsedMs: Date.now() - startedAt,
        error: error?.message || "request_failed"
      }));
    }
  },
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname.replace(/\/+$/, "") || "/";
    // Same-origin member API proxy: avoids browser CORS/network failures
    // while preserving backend authentication and per-user ownership checks.
    // Same-origin owner-only member access management proxy.
    if (pathname.startsWith("/api/admin/members/") && pathname.endsWith("/remove")) {
      if (request.method !== "POST") return json({ok:false,message:"POST only"},405,{allow:"POST"});
      const match = pathname.match(/^\/api\/admin\/members\/([0-9a-fA-F-]{36})\/remove$/);
      if (!match) return json({ok:false,message:"รหัสสมาชิกไม่ถูกต้อง"},400);
      return binanceThLiveProxy(request,"/admin/members/"+match[1]+"/remove","POST");
    }
    if (pathname === "/api/admin/member-access" || pathname === "/api/admin/member-access/portfolio-limit") {
      const isLimit = pathname.endsWith("/portfolio-limit");
      const allowedMethod = isLimit ? "PUT" : (request.method === "GET" ? "GET" : "PUT");
      if (request.method !== allowedMethod) return json({ok:false,message:allowedMethod+" only"},405,{allow:allowedMethod});
      const target = isLimit ? "/admin/member-access/portfolio-limit" : "/admin/member-access";
      try {
        const authorization = request.headers.get("authorization") || "";
        if (!authorization.startsWith("Bearer ")) return json({ok:false,message:"กรุณาเข้าสู่ระบบก่อน"},401);
        const init = {method:request.method,headers:{"accept":"application/json","authorization":authorization},signal:AbortSignal.timeout(25000),cache:"no-store"};
        if (request.method !== "GET") { init.headers["content-type"]="application/json"; init.body=await request.text(); }
        const response=await fetch("https://blessing-webull-realtime-djfm.onrender.com"+target,init);
        const payload=await response.json().catch(()=>({ok:false,message:"Backend returned invalid JSON"}));
        return json(payload,response.status);
      } catch(error) {
        console.error("admin_member_access_proxy_failed",target,error?.message||error);
        return json({ok:false,code:"ADMIN_MEMBER_ACCESS_UNAVAILABLE",message:"เชื่อมต่อระบบกำหนดสิทธิ์สมาชิกไม่สำเร็จ กรุณาลองใหม่"},502);
      }
    }
    if (pathname === "/api/member/member-portfolios") {
      if (request.method !== "GET") return json({ok:false,message:"GET only"},405,{allow:"GET"});
      return memberBackendProxy(request, "/member/member-portfolios");
    }
    if (pathname === "/api/member/member-portfolios/performance") {
      if (request.method !== "GET") return json({ok:false,message:"GET only"},405,{allow:"GET"});
      const period = url.searchParams.get("period") || "30d";
      if (!["7d","30d","90d","all"].includes(period)) return json({ok:false,message:"INVALID_PERIOD"},400);
      return memberBackendProxy(request, "/member/member-portfolios/performance?period=" + encodeURIComponent(period));
    }
    if (pathname === "/api/member/member-portfolios/daily-trades") {
      if (request.method !== "GET") return json({ok:false,message:"GET only"},405,{allow:"GET"});
      const day = url.searchParams.get("day") || "";
      return memberBackendProxy(request, "/member/member-portfolios/daily-trades" + (day ? "?day=" + encodeURIComponent(day) : ""));
    }
    if (pathname === "/api/member/member-portfolios/control") {
      if (request.method !== "POST") return json({ok:false,message:"POST only"},405,{allow:"POST"});
      try {
        const body = await request.json();
        const authorization = request.headers.get("authorization") || "";
        if (!authorization.startsWith("Bearer ")) return json({ok:false,message:"กรุณาเข้าสู่ระบบก่อน"},401);
        const response = await fetch("https://blessing-webull-realtime-djfm.onrender.com/member/member-portfolios/control", {
          method:"POST", headers:{"accept":"application/json","content-type":"application/json","authorization":authorization},
          body:JSON.stringify(body), signal:AbortSignal.timeout(25000), cache:"no-store"
        });
        return json(await response.json().catch(()=>({ok:false,message:"Backend returned invalid JSON"})),response.status);
      } catch(error) {
        return json({ok:false,code:"MEMBER_BACKEND_UNAVAILABLE",message:"บันทึกการตั้งค่าพอร์ตไม่สำเร็จ กรุณาลองใหม่อีกครั้ง"},502);
      }
    }
    if (pathname === "/api/member/member-access") {
      if (request.method !== "GET") return json({ok:false,message:"GET only"},405,{allow:"GET"});
      return memberBackendProxy(request, "/member/member-access");
    }
    if (pathname.startsWith("/api/paper-reports/")) {
      if (request.method !== "GET") return json({ok:false,message:"GET only"},405,{allow:"GET"});
      const market = pathname.slice("/api/paper-reports/".length).toLowerCase();
      if (!["crypto","stocks"].includes(market)) return json({ok:false,message:"UNKNOWN_PAPER_MARKET"},404);
      const period = url.searchParams.get("period") || "24h";
      if (!["24h","7d","30d","all"].includes(period)) return json({ok:false,message:"INVALID_REPORT_PERIOD"},400);
      const authorization = request.headers.get("authorization") || "";
      if (!authorization.startsWith("Bearer ")) return json({ok:false,message:"กรุณาเข้าสู่ระบบก่อน"},401);
      try {
        const target = "https://blessing-webull-realtime-djfm.onrender.com/paper-reports/" +
          market + "?period=" + encodeURIComponent(period);
        const response = await fetch(target, {
          method:"GET",
          headers:{"accept":"application/json","authorization":authorization},
          signal:AbortSignal.timeout(30000),
          cache:"no-store"
        });
        const payload = await response.json().catch(()=>({ok:false,message:"เซิร์ฟเวอร์รายงานส่งข้อมูลกลับมาไม่ถูกต้อง"}));
        return json(payload,response.status,{"x-blessing-report-proxy":"same-origin-v1"});
      } catch(error) {
        console.error("paper_report_proxy_failed",market,error?.message||error);
        return json({
          ok:false,
          code:"PAPER_REPORT_BACKEND_UNAVAILABLE",
          message:"เชื่อมต่อเซิร์ฟเวอร์รายงานไม่สำเร็จ กรุณากดรีเฟรชอีกครั้ง"
        },502,{"x-blessing-report-proxy":"same-origin-v1"});
      }
    }
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
    if (pathname === "/api/admin/member-portfolios") {
      if (request.method !== "GET") return json({ok:false,message:"GET only"},405,{allow:"GET"});
      return binanceThLiveProxy(request,"/admin/member-portfolios","GET");
    }
    if (pathname === "/api/admin/member-portfolios/report") {
      if (request.method !== "GET") return json({ok:false,message:"GET only"},405,{allow:"GET"});
      return binanceThLiveProxy(request,"/admin/member-portfolios/report","GET");
    }
    if (pathname === "/api/admin/member-portfolios/control") {
      if (request.method !== "POST") return json({ok:false,message:"POST only"},405,{allow:"POST"});
      try { return binanceThLiveProxy(request,"/admin/member-portfolios/control","POST",await request.json()); }
      catch(error) { return json({ok:false,message:"ข้อมูลคำสั่งไม่ถูกต้อง"},400); }
    }
    if (pathname === "/api/webull/trading/account") {
      if (request.method !== "GET") return json({ok:false,message:"GET only"},405,{allow:"GET"});
      const refresh = new URL(request.url).searchParams.get("refresh") === "true";
      return binanceThLiveProxy(request,"/webull/trading/account" + (refresh ? "?refresh=true" : ""),"GET");
    }
    if (pathname === "/api/binance-th/auto-trade/portfolio") {
      return binanceThLiveProxy(request,"/binance-th/auto-trade/portfolio","GET");
    }
    if (pathname === "/api/binance-th/auto-trade/status") {
      if (request.method !== "GET") return json({ok:false,message:"GET only"},405,{allow:"GET"});
      return binanceThLiveProxy(request,"/binance-th/auto-trade/status","GET");
    }
    if (pathname === "/api/binance-th/auto-trade/start") {
      if (request.method !== "POST") return json({ok:false,message:"POST only"},405,{allow:"POST"});
      try { return binanceThLiveProxy(request,"/binance-th/auto-trade/start","POST",await request.json()); }
      catch(error) { return json({ok:false,message:"ข้อมูลคำสั่งไม่ถูกต้อง"},400); }
    }
    if (pathname === "/api/binance-th/auto-trade/stop") {
      if (request.method !== "POST") return json({ok:false,message:"POST only"},405,{allow:"POST"});
      return binanceThLiveProxy(request,"/binance-th/auto-trade/stop","POST",{});
    }
    if (pathname === "/api/binance-th/live-readiness") {
      if (request.method !== "GET") return json({ok:false,message:"GET only"},405,{allow:"GET"});
      return binanceThLiveProxy(request,"/binance-th/live-readiness","GET");
    }
    if (pathname === "/api/binance-th/live-order/market") {
      if (request.method !== "POST") return json({ok:false,message:"POST only"},405,{allow:"POST"});
      try {
        const body=await request.json();
        return binanceThLiveProxy(request,"/binance-th/live-order/market","POST",body);
      } catch(error) { return json({ok:false,message:"ข้อมูลคำสั่งไม่ถูกต้อง"},400); }
    }
    if (pathname === "/api/binance-th/live-orders") {
      if (request.method !== "GET") return json({ok:false,message:"GET only"},405,{allow:"GET"});
      const limit=new URL(request.url).searchParams.get("limit")||"25";
      return binanceThLiveProxy(request,"/binance-th/live-orders?limit="+encodeURIComponent(limit),"GET");
    }
    if (pathname === "/api/binance-th/live-orders/reconcile") {
      if (request.method !== "POST") return json({ok:false,message:"POST only"},405,{allow:"POST"});
      try {
        const body=await request.json();
        return binanceThLiveProxy(request,"/binance-th/live-orders/reconcile","POST",body);
      } catch(error) { return json({ok:false,message:"ข้อมูลคำสั่งไม่ถูกต้อง"},400); }
    }
    if (pathname === "/api/binance-th/order") {
      if (request.method !== "POST") return json({ ok: false, message: "POST only" }, 405, { "allow": "POST" });
      try {
        if (!(await isBlessingOwner(request))) return json({ ok: false, message: "ต้องเข้าสู่ระบบด้วยบัญชี Super Admin ของ BLESSING" }, 403);
        return await binanceThPlaceOrder(request);
      } catch (error) {
        return json({ ok: false, message: error?.message || "Binance TH live order failed" }, 502);
      }
    }
    if (pathname === "/api/binance-th/account") {
      if (request.method !== "GET") return json({ ok: false, message: "GET only" }, 405, { "allow": "GET" });
      try {
        if (!(await isBlessingOwner(request))) return json({ ok: false, message: "ต้องเข้าสู่ระบบด้วยบัญชี Super Admin ของ BLESSING" }, 403);
        return await binanceThAccount(request);
      } catch (error) {
        return json({ ok: false, configured: !!(env.BINANCE_TH_API_KEY && String(env.BINANCE_TH_API_KEY).trim()), connected: false, message: error?.message || "Binance TH connection failed" }, 502);
      }
    }
    if (pathname === "/admin-members") {
      return env.ASSETS.fetch(new Request(new URL("/admin-members.html", request.url), request));
    }
    if (pathname === "/member") {
      return Response.redirect(new URL("/", request.url), 302);
    }
    if (pathname === "/paper-reports") {
      const reportUrl = new URL("/paper-reports.html", request.url);
      return env.ASSETS.fetch(new Request(reportUrl, request));
    }
    const assetResponse = await env.ASSETS.fetch(request);
    // Always serve the app shell fresh. A stale cached index.html can load an older
    // classic script without configureBlessingSession while the newer auth module
    // still calls it after login.
    if (pathname === "/" || pathname === "/index.html") {
      const headers = new Headers(assetResponse.headers);
      headers.set("cache-control", "no-store, no-cache, must-revalidate, max-age=0");
      headers.set("pragma", "no-cache");
      headers.set("x-blessing-build", "owner-live-portfolio-proxy-2026-11-25");
      return new Response(request.method === "HEAD" ? null : assetResponse.body, {
        status: assetResponse.status,
        statusText: assetResponse.statusText,
        headers
      });
    }
    return assetResponse;
  }
};
