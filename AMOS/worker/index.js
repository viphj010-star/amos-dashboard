/**
 * AMOS Cloudflare Worker — Binance/CoinGecko 프록시 + 엣지 캐시
 *
 * 설계서(AMOS-Cloudflare-최적화-설계서 v1.1) 3~5장 기준 구현.
 * - 스팟 호출은 Binance 공식 미러(api/api1/api2)로 순차 페일오버
 * - 엔드포인트별 TTL로 Cache API 캐싱 (KV는 쓰지 않음 — 설계서 4.3절 참고)
 * - CoinGecko 데모 키는 여기(env)에만 두고 클라이언트에는 절대 노출하지 않음
 * - 업스트림 상태코드를 그대로 전달 → 클라이언트가 이미 갖고 있는 백오프 로직이
 *   429/5xx를 정확히 구분해서 반응할 수 있도록 함 (Worker가 상태를 대신 들고
 *   있진 않음 — 상태 유지가 필요해지면 그때 KV/DO 도입 검토)
 *
 * 배포: 같은 zone에서 Pages와 함께 쓰는 걸 전제로 함(wrangler.toml의 routes 참고).
 * 그래야 프론트엔드의 상대경로(/api/...) 호출이 별도 CORS 설정 없이 그대로 동작함.
 */

const FAPI = 'https://fapi.binance.com';
const SPOT_MIRRORS = [
  'https://api.binance.com',
  'https://api1.binance.com',
  'https://api2.binance.com',
];
const COINGECKO = 'https://api.coingecko.com';

// 라우트별 캐시 TTL(초) — 설계서 5장 표와 동일
const TTL = {
  oi: 45,
  lsratio: 45,
  usdtd: 45,
  exchangeinfo: 300,
  premium: 10,
  klines_backfill: 60, // limit이 큰(완결봉 위주) 요청
  klines_recent: 4, // limit이 작은(갭 채우기) 요청
};

function corsHeaders(env) {
  const origin = env.ALLOWED_ORIGIN || '*';
  return {
    'Access-Control-Allow-Origin': origin,
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };
}

function jsonResponse(body, status, ttl, env) {
  const headers = {
    'Content-Type': 'application/json',
    ...corsHeaders(env),
  };
  if (ttl && status === 200) {
    headers['Cache-Control'] = `public, max-age=${ttl}`;
  } else {
    headers['Cache-Control'] = 'no-store';
  }
  return new Response(body, { status, headers });
}

// 캐시 우선 조회 → 없으면 fetcher() 실행 → 성공 시 캐시에 적재(응답은 기다리게 하지 않음)
async function withEdgeCache(request, ttl, fetcher, env, ctx) {
  const cache = caches.default;
  const cacheKey = new Request(new URL(request.url).toString(), request);
  if (ttl) {
    const cached = await cache.match(cacheKey);
    if (cached) return cached;
  }
  const res = await fetcher();
  if (ttl && res.status === 200) {
    // 캐시 저장은 백그라운드로 — 응답을 늦추지 않음
    ctx.waitUntil(cache.put(cacheKey, res.clone()));
  }
  return res;
}

// 스팟 미러 순차 페일오버: 앞 미러가 실패(네트워크 오류/5xx)하면 다음 미러 시도
async function fetchWithMirrorFailover(path) {
  let lastErr = null;
  for (const base of SPOT_MIRRORS) {
    try {
      const r = await fetch(base + path);
      if (r.ok) return r;
      lastErr = new Error(`upstream ${base} status ${r.status}`);
      // 4xx(요청 자체 문제)는 다른 미러를 시도해도 의미 없으니 바로 반환
      if (r.status >= 400 && r.status < 500) return r;
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr || new Error('all mirrors failed');
}

async function handleOI(url, env) {
  const symbol = url.searchParams.get('symbol');
  if (!symbol) return jsonResponse(JSON.stringify({ error: 'symbol required' }), 400, 0, env);
  const path = `/fapi/v1/openInterest?symbol=${encodeURIComponent(symbol)}`;
  try {
    const r = await fetch(FAPI + path);
    const body = await r.text();
    return jsonResponse(body, r.status, TTL.oi, env);
  } catch (e) {
    return jsonResponse(JSON.stringify({ error: String(e) }), 502, 0, env);
  }
}

// 클라이언트의 fetchFutsRest가 원래 2번(global+top) 호출하던 걸 1번으로 합침.
// Promise.all이 아니라 allSettled를 쓰는 이유: 한쪽이 네트워크 오류로 reject되면
// Promise.all은 즉시 전체를 실패시켜버려서, 의도했던 "한쪽만 실패해도 나머지는 살림"이
// 깨진다. allSettled로 각자 독립적으로 성공/실패를 판정해야 실제로 부분실패를 허용할 수 있음.
async function handleLsRatio(url, env) {
  const symbol = url.searchParams.get('symbol');
  if (!symbol) return jsonResponse(JSON.stringify({ error: 'symbol required' }), 400, 0, env);
  const qs = `symbol=${encodeURIComponent(symbol)}&period=5m&limit=1`;
  const [gSettled, tSettled] = await Promise.allSettled([
    fetch(`${FAPI}/futures/data/globalLongShortAccountRatio?${qs}`).then((r) => (r.ok ? r.json() : null)),
    fetch(`${FAPI}/futures/data/topLongShortPositionRatio?${qs}`).then((r) => (r.ok ? r.json() : null)),
  ]);
  const global = gSettled.status === 'fulfilled' ? gSettled.value : null;
  const top = tSettled.status === 'fulfilled' ? tSettled.value : null;
  if (global == null && top == null) {
    return jsonResponse(JSON.stringify({ error: 'both upstream calls failed' }), 502, 0, env);
  }
  return jsonResponse(JSON.stringify({ global, top }), 200, TTL.lsratio, env);
}

async function handleUsdtDominance(url, env) {
  const key = env.COINGECKO_API_KEY; // wrangler secret put COINGECKO_API_KEY 로 주입
  const qs = key ? `?x_cg_demo_api_key=${encodeURIComponent(key)}` : '';
  try {
    const r = await fetch(`${COINGECKO}/api/v3/global${qs}`);
    const body = await r.text();
    return jsonResponse(body, r.status, TTL.usdtd, env);
  } catch (e) {
    return jsonResponse(JSON.stringify({ error: String(e) }), 502, 0, env);
  }
}

async function handlePremium(url, env) {
  const symbol = url.searchParams.get('symbol');
  if (!symbol) return jsonResponse(JSON.stringify({ error: 'symbol required' }), 400, 0, env);
  try {
    const r = await fetch(`${FAPI}/fapi/v1/premiumIndex?symbol=${encodeURIComponent(symbol)}`);
    const body = await r.text();
    return jsonResponse(body, r.status, TTL.premium, env);
  } catch (e) {
    return jsonResponse(JSON.stringify({ error: String(e) }), 502, 0, env);
  }
}

async function handleExchangeInfo(url, env) {
  const symbol = url.searchParams.get('symbol');
  if (!symbol) return jsonResponse(JSON.stringify({ error: 'symbol required' }), 400, 0, env);
  try {
    const r = await fetchWithMirrorFailover(`/api/v3/exchangeInfo?symbol=${encodeURIComponent(symbol)}`);
    const body = await r.text();
    return jsonResponse(body, r.status, TTL.exchangeinfo, env);
  } catch (e) {
    return jsonResponse(JSON.stringify({ error: String(e) }), 502, 0, env);
  }
}

function klinesTtl(url) {
  const limit = Number(url.searchParams.get('limit') || '500');
  return limit >= 100 ? TTL.klines_backfill : TTL.klines_recent;
}

async function handleSpotKlines(url, env) {
  const symbol = url.searchParams.get('symbol');
  const interval = url.searchParams.get('interval') || '5m';
  const limit = url.searchParams.get('limit') || '500';
  if (!symbol) return jsonResponse(JSON.stringify({ error: 'symbol required' }), 400, 0, env);
  try {
    const r = await fetchWithMirrorFailover(
      `/api/v3/klines?symbol=${encodeURIComponent(symbol)}&interval=${interval}&limit=${limit}`
    );
    const body = await r.text();
    return jsonResponse(body, r.status, klinesTtl(url), env);
  } catch (e) {
    return jsonResponse(JSON.stringify({ error: String(e) }), 502, 0, env);
  }
}

async function handleFuturesKlines(url, env) {
  const symbol = url.searchParams.get('symbol');
  const interval = url.searchParams.get('interval') || '5m';
  const limit = url.searchParams.get('limit') || '500';
  if (!symbol) return jsonResponse(JSON.stringify({ error: 'symbol required' }), 400, 0, env);
  try {
    const r = await fetch(
      `${FAPI}/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}&interval=${interval}&limit=${limit}`
    );
    const body = await r.text();
    return jsonResponse(body, r.status, klinesTtl(url), env);
  } catch (e) {
    return jsonResponse(JSON.stringify({ error: String(e) }), 502, 0, env);
  }
}

async function handleDepth(url, env) {
  const symbol = url.searchParams.get('symbol');
  const limit = url.searchParams.get('limit') || '20';
  if (!symbol) return jsonResponse(JSON.stringify({ error: 'symbol required' }), 400, 0, env);
  try {
    const r = await fetch(`${FAPI}/fapi/v1/depth?symbol=${encodeURIComponent(symbol)}&limit=${limit}`);
    const body = await r.text();
    // 실시간성이 중요한 엔드포인트라 캐시하지 않음(설계서 5장)
    return jsonResponse(body, r.status, 0, env);
  } catch (e) {
    return jsonResponse(JSON.stringify({ error: String(e) }), 502, 0, env);
  }
}

const ROUTES = {
  '/api/futures/oi': handleOI,
  '/api/futures/lsratio': handleLsRatio,
  '/api/usdtd': handleUsdtDominance,
  '/api/futures/premium': handlePremium,
  '/api/spot/exchangeinfo': handleExchangeInfo,
  '/api/spot/klines': handleSpotKlines,
  '/api/futures/klines': handleFuturesKlines,
  '/api/futures/depth': handleDepth,
};

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // CORS Preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: corsHeaders(env),
      });
    }

    // GET만 지원
    if (request.method !== 'GET') {
      return jsonResponse(
        JSON.stringify({ error: 'method not allowed' }),
        405,
        0,
        env
      );
    }

    const handler = ROUTES[url.pathname];

    // API가 아니면 정적 파일 제공
    if (!handler) {
      return env.ASSETS.fetch(request);
    }

    const isKlines =
      url.pathname === '/api/spot/klines' ||
      url.pathname === '/api/futures/klines';

    const ttl = isKlines
      ? klinesTtl(url)
      : {
          '/api/futures/oi': TTL.oi,
          '/api/futures/lsratio': TTL.lsratio,
          '/api/usdtd': TTL.usdtd,
          '/api/futures/premium': TTL.premium,
          '/api/spot/exchangeinfo': TTL.exchangeinfo,
          '/api/futures/depth': 0,
        }[url.pathname];

    return withEdgeCache(
      request,
      ttl,
      () => handler(url, env),
      env,
      ctx
    );
  },
};
