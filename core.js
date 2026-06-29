/* ============================================================================
 * PolyFomo core logic — 1:1 port of the ATH analysis from wallet_bot.py
 * Works in the browser AND in Node (for parity testing).
 * Network functions live in API.* so tests can inject mocks.
 * ========================================================================== */

const GAMMA_API = "https://gamma-api.polymarket.com";
const CLOB_API  = "https://clob.polymarket.com";
const DATA_API  = "https://data-api.polymarket.com";

/* ---------- tiny helpers that mirror Python semantics ---------- */
function num(x) {                      // float(x or 0)
  if (x === null || x === undefined || x === "") return 0;
  const n = Number(x);
  return isNaN(n) ? 0 : n;
}
function up(x) { return String(x || "").toUpperCase(); }

function normalize_ts(raw) {           // ms -> s if needed
  const v = Math.trunc(num(raw));
  return v > 10000000000 ? Math.trunc(v / 1000) : v;
}
function fmt_ts(ts) {
  if (!ts) return "unknown";
  try {
    const d = new Date(ts * 1000);
    const p = (n) => String(n).padStart(2, "0");
    return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} ` +
           `${p(d.getUTCHours())}:${p(d.getUTCMinutes())}`;
  } catch (e) { return "unknown"; }
}
function pyRound1(x) {                  // python round(x, 1) (banker's rounding)
  const v = x * 10;
  const fl = Math.floor(v);
  const frac = v - fl;
  let r;
  if (Math.abs(frac - 0.5) < 1e-9) r = (fl % 2 === 0) ? fl : fl + 1;
  else r = Math.round(v);
  return r / 10;
}
function parseMaybeJSON(raw) {
  if (typeof raw !== "string") return raw;
  try { return JSON.parse(raw); } catch (e) { return null; }
}

/* ============================================================================
 *  NETWORK  (overridable for tests)
 * ========================================================================== */
const _sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Whole-wallet activity, indexed by market (filled by API.prefetch_activity if used).
let _activityIndex = null;
let _activityWallet = "";

// Cache of COMPUTED position details (not raw responses), keyed per position and
// capped, so memory stays small. The scan itself caches nothing. Cleared per scan.
let _detailCache = new Map();
const _DETAIL_CAP = 40;
function _clearCache() { _detailCache = new Map(); }

let _reqStats = { total: 0, slow: 0, aborted: 0, gaveup: 0, inflight: 0 };
function _dbg() { return (typeof window !== "undefined") && window.__PFDEBUG !== false; }
const _now = () => (typeof performance !== "undefined" ? performance.now() : Date.now());
function _short(url){ return url.replace(/^https?:\/\//,"").replace(/\?.*$/, m=>m.length>50?m.slice(0,50)+"…":m).slice(0,90); }

async function _getJSON(url, retries = 4) {
  _reqStats.total++; _reqStats.inflight++;
  try {
    for (let attempt = 0; attempt <= retries; attempt++) {
      const ctrl = (typeof AbortController !== "undefined") ? new AbortController() : null;
      const timer = ctrl ? setTimeout(() => ctrl.abort(), 12000) : null;   // 12s hard cap per request
      const t0 = _now();
      try {
        const r = await fetch(url, ctrl ? { signal: ctrl.signal } : undefined);
        if (timer) clearTimeout(timer);
        const dt = (_now() - t0) | 0;
        if (dt > 3000) { _reqStats.slow++; if (_dbg()) console.warn(`[REQ ${dt}ms a${attempt}] ${_short(url)}`); }
        // throttled or transient server error → wait and retry
        if (r.status === 429 || r.status >= 500) {
          if (_dbg()) console.warn(`[REQ ${r.status} a${attempt}] ${_short(url)}`);
          const ra = parseFloat(r.headers.get("retry-after")) || 0;
          await _sleep(ra ? ra * 1000 : 500 * Math.pow(2, attempt) + Math.random() * 250);
          continue;
        }
        if (!r.ok && r.status !== 200) return { __status: r.status };
        try { return await r.json(); } catch (e) { if (_dbg()) console.warn(`[REQ json-fail] ${_short(url)}`); return null; }
      } catch (e) {
        if (timer) clearTimeout(timer);
        const dt = (_now() - t0) | 0;
        const aborted = e && e.name === "AbortError";
        if (aborted) _reqStats.aborted++;
        if (_dbg()) console.warn(`[REQ ${aborted ? "ABORTED@12s" : "ERR"} ${dt}ms a${attempt}] ${_short(url)} ${aborted ? "" : (e && e.message || "")}`);
        await _sleep(500 * Math.pow(2, attempt) + Math.random() * 250);
      }
    }
    _reqStats.gaveup++;
    if (_dbg()) console.warn(`[REQ GAVE-UP after ${retries + 1} tries] ${_short(url)}`);
    return null;  // gave up after retries
  } finally {
    _reqStats.inflight--;
  }
}

const API = {
  async get_all_positions(wallet) {
    const seen = new Set();
    const positions = [];
    const add = (batch) => {
      for (const p of batch) {
        const cid = p.conditionId || p.market || "";
        const aid = p.asset || "";
        const key = cid + "|" + aid;
        if (cid && !seen.has(key)) { seen.add(key); positions.push(p); }
      }
    };
    // active
    let offset = 0;
    while (true) {
      const data = await _getJSON(`${DATA_API}/positions?user=${wallet}&limit=500&offset=${offset}&sizeThreshold=0`);
      const batch = Array.isArray(data) ? data : (data && (data.data || data.positions)) || [];
      if (!batch.length) break;
      add(batch);
      if (batch.length < 500) break;
      offset += 500;
    }
    // closed (API caps page at 50)
    offset = 0;
    while (true) {
      const data = await _getJSON(`${DATA_API}/closed-positions?user=${wallet}&limit=50&offset=${offset}`);
      const batch = Array.isArray(data) ? data : (data && data.data) || [];
      if (!batch.length) break;
      add(batch);
      if (batch.length < 50) break;
      offset += 50;
    }
    return positions;
  },

  // Normalize one raw /activity item into our trade shape (or null if not a trade).
  _normTrade(item) {
    const t = up(item.type);
    if (t !== "TRADE" && t !== "SPLIT" && t !== "CONVERSION") return null;
    let side = item.side || "", price = item.price || 0;
    if (t === "SPLIT") { side = "BUY"; price = 0.5; }
    else if (t === "CONVERSION") { side = "BUY"; }
    return {
      asset: item.asset || "", side: side || "BUY", size: item.size || 0, price,
      outcome: item.outcome || "", timestamp: item.timestamp || 0,
      conditionId: item.conditionId || "", type: t,
    };
  },

  // Fetch the WHOLE wallet's activity once and index it by market. After this,
  // get_trades() serves each position from memory (0 requests) instead of one
  // request per position — the main thing that kept us under the rate limit.
  async prefetch_activity(wallet) {
    const idx = new Map();
    let offset = 0;
    while (offset <= 200000) {   // safety cap
      const data = await _getJSON(`${DATA_API}/activity?user=${wallet}&limit=500&offset=${offset}`);
      const batch = Array.isArray(data) ? data : (data && (data.data || data.activity)) || [];
      if (!batch.length) break;
      for (const item of batch) {
        const tr = this._normTrade(item);
        if (!tr) continue;
        const k = (tr.conditionId || "").toLowerCase();
        if (!k) continue;
        if (!idx.has(k)) idx.set(k, []);
        idx.get(k).push(tr);
      }
      if (batch.length < 500) break;
      offset += 500;
    }
    _activityIndex = idx;
    _activityWallet = (wallet || "").toLowerCase();
  },
  clear_activity() { _activityIndex = null; _activityWallet = ""; },

  async get_trades(wallet, condition_id) {
    // served from the prefetched index when available (same wallet)
    if (_activityIndex && _activityWallet === (wallet || "").toLowerCase()) {
      const key = (condition_id || "").toLowerCase();
      if (_activityIndex.has(key)) return _activityIndex.get(key);
      // tolerate id length differences (some positions carry a truncated conditionId)
      for (const [k, v] of _activityIndex) {
        if (k && key && (k.startsWith(key) || key.startsWith(k))) return v;
      }
      return [];
    }
    // fallback: per-market fetch (used by /pos and detail view)
    const trades = [];
    let offset = 0;
    while (offset <= 2000) {
      const data = await _getJSON(`${DATA_API}/activity?user=${wallet}&market=${condition_id}&limit=100&offset=${offset}`);
      const batch = Array.isArray(data) ? data : (data && (data.data || data.activity)) || [];
      if (!batch.length) break;
      for (const item of batch) {
        const tr = this._normTrade(item);
        if (tr) trades.push(tr);
      }
      if (batch.length < 100) break;
      offset += 100;
    }
    return trades;
  },

  async get_price_history(token_id, condition_id = "") {
    if (!token_id) return [];
    // Cover BOTH short sports markets (need fine fidelity=60) and long-running markets
    // like the Elon tweet markets (need coarse fidelity=1000, else CLOB returns empty).
    // First non-empty result wins, so sports hit 60 immediately and long markets fall to 1000.
    for (const [interval, fidelity] of [["max", 60], ["max", 1000], ["max", 1], ["1d", 100], ["1w", 50]]) {
      const data = await _getJSON(`${CLOB_API}/prices-history?market=${token_id}&interval=${interval}&fidelity=${fidelity}`);
      const history = (data && data.history) || [];
      if (history.length) return history;
    }
    return [];
  },

  async get_price_history_from_trades(condition_id, outcome, first_buy_ts) {
    if (!condition_id || !first_buy_ts) return [];
    const all_trades = [];
    let offset = 0;
    while (offset <= 10000) {
      const data = await _getJSON(`${DATA_API}/trades?market=${condition_id}&limit=500&offset=${offset}`);
      const batch = Array.isArray(data) ? data : [];
      if (!batch.length) break;
      for (const t of batch) {
        if (up(t.outcome) === up(outcome)) {
          const ts = Math.trunc(num(t.timestamp));
          if (ts >= first_buy_ts) all_trades.push(t);
        }
      }
      const oldest = Math.min(...batch.map((t) => Math.trunc(num(t.timestamp))));
      if (oldest < first_buy_ts) break;
      offset += 500;
    }
    if (!all_trades.length) return [];
    const byMinute = {};
    for (const t of all_trades) {
      const ts = Math.trunc(num(t.timestamp));
      const minute = Math.trunc(ts / 60) * 60;
      if (!(minute in byMinute)) byMinute[minute] = num(t.price);
    }
    return Object.keys(byMinute)
      .map((k) => parseInt(k, 10))
      .sort((a, b) => a - b)
      .filter((ts) => byMinute[ts] > 0)
      .map((ts) => ({ t: ts, p: String(byMinute[ts]) }));
  },

  async get_market_info(condition_id) {
    if (!condition_id) return null;
    const want = condition_id.toLowerCase();
    // Only the plural `condition_ids=` is a valid Gamma filter (fast, tester-proven).
    // The singular `conditionId=` is NOT a recognized param → Gamma returns a huge
    // unfiltered list → 12s hangs. So we don't use it.
    const data = await _getJSON(`${GAMMA_API}/markets?condition_ids=${condition_id}`);
    const candidates = Array.isArray(data) ? data : (data && typeof data === "object" ? [data] : []);
    for (const m of candidates) {
      const mcid = String(m.conditionId || m.condition_id || "").toLowerCase();
      if (mcid && (mcid === want || mcid.startsWith(want))) return m;
    }
    return null;
  },
};

/* ============================================================================
 *  PURE LOGIC  (identical math to Python — these are parity-tested)
 * ========================================================================== */
// Sport positions: detected by league prefix OR sport slug structure (match date /
// bet-type markers). Sports markets are multi-outcome (negRisk) and get falsely flagged
// as hedges, so they are excluded from hedge filtering entirely.
const SPORT_LEAGUE_RE = /^(fifwc|fifa?|wwc\w*|epl|laliga|seriea|bundesliga|ligue1|uefa|ucl|uel|mls|nba|wnba|nfl|ncaa\w*|nhl|mlb|ufc|mma|atp|wta|f1|nascar|cricket|ipl|soccer|tennis|golf|boxing|euro\w*)[-_]/i;
const SPORT_MARKER_RE = /(-more-markets|-total-corners|-halftime-result|-second-half-result|-first-to-score|-team-total-|-spread-home|-spread-away|-btts|-1st-half|-2nd-half|-corners|-\d{4}-\d{2}-\d{2})/i;
function is_sport_position(p) {
  const es = p.eventSlug || "", sl = p.slug || "";
  return SPORT_LEAGUE_RE.test(es) || SPORT_LEAGUE_RE.test(sl) || SPORT_MARKER_RE.test(es) || SPORT_MARKER_RE.test(sl);
}

function filter_hedged(positions) {
  const cidOut = {};
  for (const p of positions) {
    if (is_sport_position(p)) continue;          // sports never count toward hedging
    const cid = p.conditionId || p.market || "";
    const out = up(p.outcome);
    if (cid && out) (cidOut[cid] = cidOut[cid] || new Set()).add(out);
  }
  const hedged = new Set(Object.keys(cidOut).filter((c) => cidOut[c].size > 1));
  if (hedged.size) return positions.filter((p) => is_sport_position(p) || !hedged.has(p.conditionId || p.market || ""));
  return positions;
}

function filter_trades_by_token(trades, token_id, pos_outcome) {
  if (!token_id || !trades.length) return trades;
  const special = trades.filter((t) => t.type === "SPLIT" || t.type === "CONVERSION");
  const regular = trades.filter((t) => t.type !== "SPLIT" && t.type !== "CONVERSION");
  const byAsset = regular.filter((t) => t.asset === token_id);
  if (byAsset.length) return byAsset.concat(special);
  if (pos_outcome) {
    const byOut = regular.filter((t) => up(t.outcome) === up(pos_outcome));
    if (byOut.length) return byOut.concat(special);
  }
  return trades;
}

function get_token_final_price(market, token_id) {
  if (!market) return null;
  try {
    const pricesRaw = market.outcomePrices;
    const tokensRaw = market.clobTokenIds || market.tokens || "[]";
    if (!pricesRaw) return null;
    const prices = parseMaybeJSON(pricesRaw);
    const tokens = parseMaybeJSON(tokensRaw);
    if (!Array.isArray(tokens) || !Array.isArray(prices)) return null;
    for (let i = 0; i < tokens.length; i++) {
      const tok = tokens[i];
      const tokId = typeof tok === "string" ? tok : (tok.token_id || tok.id || "");
      if (tokId === token_id && i < prices.length) return num(prices[i]);
    }
  } catch (e) {}
  return null;
}

function get_outcome_token_id(market, outcome) {
  if (!market) return "";
  try {
    const tokens = parseMaybeJSON(market.clobTokenIds || market.tokens || "[]");
    const outcomes = parseMaybeJSON(market.outcomes || "[]");
    if (!Array.isArray(tokens) || !Array.isArray(outcomes)) return "";
    for (let i = 0; i < outcomes.length; i++) {
      if (up(outcomes[i]) === up(outcome) && i < tokens.length) {
        const tok = tokens[i];
        return typeof tok === "string" ? tok : (tok.token_id || tok.id || "");
      }
    }
  } catch (e) {}
  return "";
}

function get_event_url(market) {
  if (!market) return "";
  let events = market.events || [];
  if (typeof events === "string") events = parseMaybeJSON(events) || [];
  if (Array.isArray(events) && events.length) {
    const first = events[0];
    if (first && typeof first === "object") {
      const slug = first.slug || "";
      if (slug) return "https://polymarket.com/event/" + slug;
    }
  }
  const slug = market.slug || "";
  return slug ? "https://polymarket.com/event/" + slug : "";
}

function build_event_url(pos, market) {
  const eventSlug = String(pos.eventSlug || "").trim();
  if (eventSlug) return "https://polymarket.com/event/" + eventSlug;
  const marketSlug = String(pos.slug || "").trim();
  if (marketSlug) return "https://polymarket.com/event/" + marketSlug;
  return get_event_url(market);
}

function get_title(pos, market) {
  let title = pos.title || "";
  if (!title && market) title = market.question || "";
  return title || "Unknown";
}

function calc_shares_stats(buys, sells) {
  let total_shares = 0, total_spent = 0;
  for (const t of buys) { total_shares += num(t.size); total_spent += num(t.size) * num(t.price); }
  const avg_entry = total_shares > 0 ? total_spent / total_shares : 0;
  const ops = [];
  for (const t of buys) ops.push([normalize_ts(t.timestamp), "BUY", num(t.size)]);
  for (const t of sells) ops.push([normalize_ts(t.timestamp), "SELL", num(t.size)]);
  ops.sort((a, b) => a[0] - b[0]);
  let max_simul = 0, running = 0;
  for (const [, side, size] of ops) {
    if (side === "BUY") running += size;
    else { running -= size; if (running < 0) running = 0; }
    if (running > max_simul) max_simul = running;
  }
  if (max_simul === 0) max_simul = total_shares;
  return [total_shares, total_spent, avg_entry, max_simul];
}

function calc_timeline_ath(buys, sells, history, first_buy_ts, last_sell_ts) {
  const tradeEvents = [];
  for (const t of buys) tradeEvents.push([normalize_ts(t.timestamp), "BUY", num(t.size)]);
  for (const t of sells) tradeEvents.push([normalize_ts(t.timestamp), "SELL", num(t.size)]);
  tradeEvents.sort((a, b) => a[0] - b[0]);

  const pricePoints = [];
  for (const h of history) {
    const tts = normalize_ts(h.t);
    const p = num(h.p);
    if (p <= 0) continue;
    if (first_buy_ts > 0 && tts <= first_buy_ts) continue;
    if (last_sell_ts > 0 && tts > last_sell_ts) continue;
    pricePoints.push([tts, p]);
  }

  const timeline = [];
  for (const [ts, side, size] of tradeEvents) timeline.push([ts, 0, side, size]);
  for (const [ts, p] of pricePoints) timeline.push([ts, 1, "PRICE", p]);
  timeline.sort((a, b) => (a[0] - b[0]) || (a[1] - b[1]));

  let running = 0, best_val = 0, best_price = 0, best_ts = 0;
  for (const [ts, , type, value] of timeline) {
    if (type === "BUY") running += value;
    else if (type === "SELL") { running -= value; if (running < 0) running = 0; }
    else if (type === "PRICE") {
      const pv = running * value;
      if (pv > best_val) { best_val = pv; best_price = value; best_ts = ts; }
    }
  }
  return [best_val, best_price, best_ts, pricePoints.length];
}

function calc_post_exit_ath(history, last_sell_ts, max_simul, gamma_price) {
  let best_val = 0, best_price = 0, best_ts = 0;
  for (const h of history) {
    const tts = normalize_ts(h.t);
    const p = num(h.p);
    if (p <= 0 || tts <= last_sell_ts) continue;
    const val = max_simul * p;
    if (val > best_val) { best_val = val; best_price = p; best_ts = tts; }
  }
  if (gamma_price !== null && gamma_price !== undefined) {
    const gv = max_simul * gamma_price;
    if (gv > best_val) {
      best_val = gv; best_price = gamma_price;
      if (best_ts === 0) {
        for (const h of history) { if (num(h.p) >= gamma_price * 0.99) { best_ts = normalize_ts(h.t); break; } }
        if (best_ts === 0) best_ts = last_sell_ts;
      }
    }
  }
  return [best_val, best_price, best_ts];
}

/* ---------- cache fingerprint helpers (same as Python) ---------- */
function pos_key(p) { return (p.conditionId || p.market || "") + "|" + (p.asset || ""); }
function pos_fingerprint(p) { return `${p.totalBought}|${p.size}|${p.realizedPnl}|${p.outcome}`; }
function pos_resolved(p) { const cp = num(p.curPrice); return cp <= 0.05 || cp >= 0.95; }

/* ============================================================================
 *  ATH compute core — pure part of process_ath (after data is fetched).
 *  Takes already-resolved history/market/gamma_price. Mutates `history`
 *  for the inversion exactly like Python does.
 * ========================================================================== */
function ath_compute_core(pos, buys, sells, history, market, gamma_price, condition_id, token_id) {
  const pos_outcome = pos.outcome || "";
  sells = sells || [];

  const [total_shares, total_spent, avg_entry, _max_simul] = calc_shares_stats(buys, sells);
  let max_simul = _max_simul;

  // position lost?
  const cur_price_raw = num(pos.curPrice);
  let position_lost = false;
  // curPrice is always the HELD outcome's price. If it's ~0, the held side lost —
  // regardless of YES/NO. (A NO position at price ~1 means NO WON, not lost.)
  if (cur_price_raw > 0 && cur_price_raw <= 0.05) position_lost = true;

  // timestamps
  const buy_ts = buys.filter((t) => t.timestamp).map((t) => normalize_ts(t.timestamp));
  const sell_ts = sells.filter((t) => t.timestamp).map((t) => normalize_ts(t.timestamp));
  const first_buy = buy_ts.length ? Math.min(...buy_ts) : 0;
  const last_sell = sell_ts.length ? Math.max(...sell_ts) : 0;

  // ---- history inversion for NO (heuristic) ----
  let history_inverted = false;
  if (history.length && up(pos_outcome) === "NO" && avg_entry > 0 && avg_entry < 0.95) {
    let pricesNearBuy = [];
    for (const h of history) {
      const tts = normalize_ts(h.t); const p = num(h.p);
      if (p > 0 && first_buy > 0 && Math.abs(tts - first_buy) < 86400 * 3) pricesNearBuy.push(p);
    }
    if (!pricesNearBuy.length)
      pricesNearBuy = history.slice(0, 10).map((h) => num(h.p)).filter((p) => p > 0);
    if (pricesNearBuy.length) {
      const avgHist = pricesNearBuy.reduce((a, b) => a + b, 0) / pricesNearBuy.length;
      const invAvg = 1.0 - avgHist;
      if (Math.abs(invAvg - avg_entry) < Math.abs(avgHist - avg_entry)) {
        for (const h of history) { const p = num(h.p); if (p > 0) h.p = String(1.0 - p); }
        history_inverted = true;
      }
    }
  }
  // (No forced inversion: get_price_history fetches the HELD token's own price
  // history, so it's already correct for both YES and NO. Inverting it turned a
  // winning NO position into a fake loss.)

  // no data at all -> curPrice fallback
  if (!history.length && (gamma_price === null || gamma_price === undefined)) {
    const cp = num(pos.curPrice);
    if (cp > 0) gamma_price = cp;
    else return null;
  }

  // fallback max_simul from position data
  const pos_size = Math.max(num(pos.size), num(pos.totalBought));
  if (pos_size > max_simul) max_simul = pos_size;

  // 1) ATH while holding
  const [best_during, best_during_price, best_during_ts] =
    calc_timeline_ath(buys, sells, history, first_buy, last_sell);

  // 2) ATH after full exit
  let total_sold = 0;
  for (const t of sells) total_sold += num(t.size);
  let fully_exited = last_sell > 0 && (total_shares - total_sold) < 0.01;
  if (!fully_exited) {
    const cur_size = (pos.size !== undefined || pos.currentValue !== undefined)
      ? num(pos.size !== undefined ? pos.size : pos.currentValue) : -1;
    if (cur_size === 0 && last_sell > 0) fully_exited = true;
  }

  let best_after = 0, best_after_price = 0, best_after_ts = 0;
  if (fully_exited && max_simul > 0)
    [best_after, best_after_price, best_after_ts] = calc_post_exit_ath(history, last_sell, max_simul, gamma_price);

  // choose best
  let ath_price, ath_ts, value_at_ath;
  if (best_after > best_during) { ath_price = best_after_price; ath_ts = best_after_ts; value_at_ath = best_after; }
  else { ath_price = best_during_price; ath_ts = best_during_ts; value_at_ath = best_during; }

  // gamma fallback for still-holding
  if (gamma_price !== null && gamma_price !== undefined && !fully_exited && !position_lost) {
    const net_rem = Math.max(0, total_shares - total_sold);
    const gv = net_rem * gamma_price;
    if (gv > value_at_ath) {
      value_at_ath = gv; ath_price = gamma_price; ath_ts = 0;
      for (const h of history) { if (num(h.p) >= gamma_price * 0.99) { ath_ts = normalize_ts(h.t); break; } }
      if (ath_ts === 0 && last_sell > 0) ath_ts = last_sell;
    }
  }

  // last-resort fallback
  if (value_at_ath <= 0 && history.length) {
    for (const h of history) {
      const tts = normalize_ts(h.t); const p = num(h.p);
      if (p > ath_price && (first_buy === 0 || tts >= first_buy)) { ath_price = p; ath_ts = tts; }
    }
    if (ath_price > 0) value_at_ath = max_simul * ath_price;
  }

  // curPrice may beat history (resolved at $1); curPrice is held-outcome price, no inversion
  const cur_price_raw_2 = num(pos.curPrice);
  const cur_price = cur_price_raw_2;
  if (!position_lost && cur_price_raw_2 > 0 && cur_price > 0 && max_simul > 0) {
    const cur_value = max_simul * cur_price;
    if (cur_value > value_at_ath && cur_price > avg_entry) {
      ath_price = cur_price; value_at_ath = cur_value;
      if (ath_ts === 0) ath_ts = normalize_ts(pos.timestamp) || last_sell;
    }
  }

  if (ath_price <= 0 || value_at_ath <= 0) return null;
  if (ath_price <= avg_entry) return null;

  const ath_mult = avg_entry > 0 ? pyRound1(ath_price / avg_entry) : 0;

  // what user actually received
  let sells_val = 0;
  for (const t of sells) sells_val += num(t.size) * num(t.price);
  const net_remaining = total_shares - total_sold;
  let final_check = position_lost ? null : gamma_price;
  if ((final_check === null || final_check === undefined) && history.length)
    final_check = num(history[history.length - 1].p);

  const cur_price_pos = num(pos.curPrice);
  if (!position_lost && cur_price_pos > 0 &&
      (final_check === null || final_check === undefined || cur_price_pos > final_check))
    final_check = cur_price_pos;

  let resolution_received = 0;
  if (position_lost) resolution_received = 0;
  else if (final_check !== null && final_check !== undefined && final_check >= 0.95 && net_remaining > 0.01)
    resolution_received = net_remaining * 1.0;

  // value of shares still held (open position) marked to market
  let open_value = 0;
  const cur_price_held = num(pos.curPrice);
  if (!position_lost && resolution_received === 0 && net_remaining > 0.01 && cur_price_held > 0)
    open_value = net_remaining * cur_price_held;

  const total_received = sells_val + resolution_received + open_value;
  const actual_pnl = total_received - total_spent;
  const missed = Math.max(0, value_at_ath - total_received);

  if (missed < 0.5) return null;

  return {
    title: get_title(pos, market),
    url: build_event_url(pos, market),
    avg_entry, ath_price, ath_mult,
    shares: total_shares, spent: total_spent,
    value_at_ath, missed, actual_pnl,
    sold_price: total_sold > 0 ? sells_val / total_sold : 0,
    sold_value: sells_val,
    sold_shares: total_sold,
    last_sell_ts: last_sell,
    buy_date: first_buy ? fmt_ts(first_buy) : fmt_ts(normalize_ts(pos.timestamp)),
    buy_date_ts: first_buy ? first_buy : normalize_ts(pos.timestamp),
    ath_date: fmt_ts(ath_ts), ath_date_ts: ath_ts,
    conditionId: condition_id, asset: token_id,
    outcome_side: pos_outcome,
  };
}

/* ---------- extract_buys_sells (needs network) ---------- */
async function extract_buys_sells(wallet, pos) {
  const condition_id = pos.conditionId || pos.market || "";
  const token_id = pos.asset || "";
  const pos_outcome = pos.outcome || "";

  const raw_trades = condition_id ? await API.get_trades(wallet, condition_id) : [];

  let full_cid = condition_id;
  for (const t of raw_trades) {
    const tc = t.conditionId || "";
    if (tc.length > full_cid.length) { full_cid = tc; break; }
  }
  const trades = filter_trades_by_token(raw_trades, token_id, pos_outcome);
  let buys = trades.filter((t) => up(t.side) === "BUY");
  const sells = trades.filter((t) => up(t.side) === "SELL");

  if (!buys.length) {
    const avg_price = num(pos.avgPrice || pos.curPrice);
    const size = Math.max(num(pos.size), num(pos.totalBought), num(pos.initialValue));
    const realized = num(pos.realizedPnl);
    if (avg_price <= 0 || size <= 0 || (realized === 0 && !raw_trades.length))
      return [null, null, pos_outcome, full_cid];
    buys = [{ size, price: avg_price, timestamp: 0 }];
  }
  return [buys, sells, pos_outcome, full_cid];
}

/* ---------- process_ath (full orchestration, mirrors Python) ---------- */
async function process_ath(wallet, pos) {
  let condition_id = pos.conditionId || pos.market || "";
  let token_id = pos.asset || "";
  if (!condition_id && !token_id) return null;

  const [buys, sells, pos_outcome, full_cid] = await extract_buys_sells(wallet, pos);
  if (buys === null) return null;

  if (full_cid.length > condition_id.length) condition_id = full_cid;
  for (const t of buys) {
    const ta = t.asset || "";
    if (ta.length > token_id.length) { token_id = ta; break; }
  }

  // timestamps for the trades-history fetch decision
  const buy_ts = buys.filter((t) => t.timestamp).map((t) => normalize_ts(t.timestamp));
  const first_buy = buy_ts.length ? Math.min(...buy_ts) : 0;

  let history = token_id ? await API.get_price_history(token_id, condition_id) : [];
  const market = condition_id ? await API.get_market_info(condition_id) : null;
  const gamma_price = (market && token_id) ? get_token_final_price(market, token_id) : null;

  if (history.length < 20 && condition_id && first_buy > 0) {
    const th = await API.get_price_history_from_trades(condition_id, pos_outcome, first_buy);
    if (th.length > history.length) history = th;
  }

  return ath_compute_core(pos, buys, sells, history, market, gamma_price, condition_id, token_id);
}

/* ---------- process_resolution (Mode 2, full orchestration, mirrors Python) ----------
 * No positions are treated exactly like Yes: gamma / history / curPrice are all
 * already the held outcome's price, so there is no inversion. */
async function process_resolution(wallet, pos) {
  let condition_id = pos.conditionId || pos.market || "";
  let token_id = pos.asset || "";
  if (!condition_id && !token_id) return null;

  const [buys, sells, pos_outcome, full_cid] = await extract_buys_sells(wallet, pos);
  if (buys === null) return null;

  if (full_cid.length > condition_id.length) condition_id = full_cid;
  for (const t of buys) {
    const ta = t.asset || "";
    if (ta.length > token_id.length) { token_id = ta; break; }
  }

  const [total_shares, total_spent, avg_entry] = calc_shares_stats(buys, sells || []);
  const _fb = buys.filter((t) => t.timestamp).map((t) => normalize_ts(t.timestamp));
  const first_buy_ts = _fb.length ? Math.min(..._fb) : normalize_ts(pos.timestamp);
  // shares sold before the market resolved, and what they were sold for
  let total_sold = 0, sells_value = 0;
  for (const t of (sells || [])) { total_sold += num(t.size); sells_value += num(t.size) * num(t.price); }
  const net_held = Math.max(0, total_shares - total_sold);

  const history = token_id ? await API.get_price_history(token_id, condition_id) : [];
  const market = condition_id ? await API.get_market_info(condition_id) : null;
  const gamma = (market && token_id) ? get_token_final_price(market, token_id) : null;

  // gamma is the final price of the HELD token, history[-1] is the held outcome's
  // last price, and curPrice is the held outcome's price too — so No is treated
  // exactly like Yes here (no inversion).
  let final_price;
  if (gamma !== null && gamma !== undefined) final_price = gamma;
  else if (history.length) final_price = num(history[history.length - 1].p);
  else {
    const cur_price = num(pos.curPrice);
    if (cur_price > 0) final_price = cur_price;
    else return null;
  }

  let outcome, max_return, missed;
  if (final_price >= 0.95) {
    outcome = "WON";
    // Only the shares you SOLD before the win could have been worth $1 each.
    // The shares you HELD to resolution already received $1 — they miss nothing.
    // missed = (shares sold) * $1 - (what you sold them for) = sum size*(1 - price).
    missed = Math.max(0, total_sold * 1.0 - sells_value);
    max_return = total_shares * 1.0;
  } else if (final_price <= 0.05) {
    outcome = "LOST"; max_return = 0; missed = 0;
  } else {
    outcome = "UNRESOLVED"; max_return = net_held * final_price; missed = 0;
  }

  return {
    title: get_title(pos, market), url: build_event_url(pos, market),
    outcome, avg_entry, shares: total_shares, spent: total_spent,
    sold_price: total_sold > 0 ? sells_value / total_sold : 0,
    sold_value: sells_value,
    sold_shares: total_sold,
    max_return, missed,
    conditionId: condition_id, asset: token_id, outcome_side: pos.outcome || "",
    buy_date_ts: first_buy_ts,
  };
}

/* ---------- get_position_detail (drill-down: operations + chart data) ----------
 * Ports analyze_detail's data prep (not the matplotlib drawing). Works for both
 * modes; the ATH star/point only appears when the result carries ath_price. */
async function get_position_detail(wallet, result) {
  const cache_key = (result.conditionId || "") + "|" + (result.asset || "");
  if (cache_key !== "|" && _detailCache.has(cache_key)) return _detailCache.get(cache_key);

  const promise = _compute_position_detail(wallet, result);
  if (cache_key !== "|") {
    _detailCache.set(cache_key, promise);
    promise.catch(() => _detailCache.delete(cache_key));   // don't keep failed loads
    if (_detailCache.size > _DETAIL_CAP) {                 // evict oldest
      const oldest = _detailCache.keys().next().value;
      if (oldest !== cache_key) _detailCache.delete(oldest);
    }
  }
  return promise;
}

async function _compute_position_detail(wallet, result) {
  const condition_id = result.conditionId || "";
  const token_id = result.asset || "";
  const pos_outcome = result.outcome_side || "";

  let trades = condition_id ? await API.get_trades(wallet, condition_id) : [];
  trades = filter_trades_by_token(trades, token_id, pos_outcome);

  const all_ops = [];
  for (const t of trades) {
    const side = up(t.side);
    if (side !== "BUY" && side !== "SELL") continue;
    all_ops.push({ side, ts: normalize_ts(t.timestamp), size: num(t.size), price: num(t.price) });
  }
  all_ops.sort((a, b) => a.ts - b.ts);

  // which op reaches the ATH share count (star)
  const ath_price = num(result.ath_price);
  const ath_shares = ath_price > 0 ? pyRound1(num(result.value_at_ath) / ath_price) : 0;
  let running_check = 0, ath_op_idx = -1;
  for (let i = 0; i < all_ops.length; i++) {
    const o = all_ops[i];
    if (o.side === "BUY") running_check += o.size;
    else { running_check -= o.size; if (running_check < 0) running_check = 0; }
    if (ath_price > 0 && Math.abs(running_check - ath_shares) < 0.5) ath_op_idx = i;
  }

  // operations list with cumulative total
  const operations = [];
  const sell_markers = [];   // for chart: each sell with remaining-after
  if (!all_ops.length) {
    operations.push({ side: "BUY", date: result.buy_date || "unknown", size: num(result.shares),
                      price: num(result.avg_entry), total: num(result.shares), star: false, noData: true });
  } else {
    let running = 0;
    for (let i = 0; i < all_ops.length; i++) {
      const o = all_ops[i];
      if (o.side === "BUY") running += o.size;
      else { running -= o.size; if (running < 0) running = 0; }
      operations.push({ side: o.side, date: fmt_ts(o.ts), size: o.size, price: o.price,
                        total: running, star: i === ath_op_idx });
      if (o.side === "SELL")
        sell_markers.push({ ts: o.ts, price_c: o.price * 100, size: o.size,
                            received: o.size * o.price, remaining: running });
    }
  }

  // ---- chart data ----
  const first_buy_ts = all_ops.length ? all_ops[0].ts : normalize_ts(result.buy_date_ts || 0);
  let history = token_id ? await API.get_price_history(token_id, condition_id) : [];
  if (history.length < 20 && condition_id && first_buy_ts > 0) {
    const th = await API.get_price_history_from_trades(condition_id, pos_outcome, first_buy_ts);
    if (th.length > history.length) history = th;
  }

  let chart = null;
  if (history.length) {
    const price_line = []; const seen = new Set();
    for (const h of history) {
      const t = normalize_ts(h.t), p = num(h.p);
      if (p > 0 && t > 0 && !seen.has(t)) { price_line.push([t, p]); seen.add(t); }
    }
    for (const o of all_ops) {
      if (o.ts > 0 && o.price > 0 && !seen.has(o.ts)) { price_line.push([o.ts, o.price]); seen.add(o.ts); }
    }
    const ath_ts = num(result.ath_date_ts);
    if (ath_ts > 0 && ath_price > 0 && !seen.has(ath_ts)) { price_line.push([ath_ts, ath_price]); seen.add(ath_ts); }
    price_line.sort((a, b) => a[0] - b[0]);

    let pl = price_line;
    if (first_buy_ts > 0) { const f = price_line.filter(([t]) => t >= first_buy_ts); if (f.length) pl = f; }

    if (pl.length) {
      const trade_steps = [];
      let run = 0;
      for (const o of all_ops) {
        if (o.side === "BUY") run += o.size; else { run -= o.size; if (run < 0) run = 0; }
        trade_steps.push([o.ts, run]);
      }
      if (!trade_steps.length && result.shares) trade_steps.push([first_buy_ts, num(result.shares)]);
      const get_shares_at = (ts) => { let cur = 0; for (const [t, sh] of trade_steps) { if (t <= ts) cur = sh; else break; } return cur; };

      const points = pl.map(([ts, p]) => ({ ts, price: p * 100, shares: get_shares_at(ts) }));

      let ath_index = -1;
      if (ath_ts > 0) { let md = Infinity; for (let i = 0; i < pl.length; i++) { const d = Math.abs(pl[i][0] - ath_ts); if (d < md) { md = d; ath_index = i; } } }
      chart = { points, ath_index, ath_price_c: ath_price * 100, ath_value: num(result.value_at_ath), sells: sell_markers };
    }
  }

  return { operations, chart, ath_shares };
}

/* ---------- /pos : single-market lookup by wallet + Polymarket link ---------- */
API.get_event_by_slug = async function (slug) {
  const data = await _getJSON(`${GAMMA_API}/events?slug=${encodeURIComponent(slug)}&limit=1`);
  const arr = Array.isArray(data) ? data : [];
  return arr.length ? arr[0] : null;
};
API.get_markets_by_slug = async function (slug) {
  const data = await _getJSON(`${GAMMA_API}/markets?slug=${encodeURIComponent(slug)}&limit=5`);
  return Array.isArray(data) ? data : [];
};
API.get_event_by_slug_path = async function (slug) {
  const data = await _getJSON(`${GAMMA_API}/events/slug/${encodeURIComponent(slug)}`);
  return (data && data.markets) ? data : null;
};

// the bet-type events a sports match is split into (each shares the match slug prefix)
const SPORT_SUFFIXES = ["", "-more-markets", "-halftime-result", "-total-corners",
                        "-second-half-result", "-first-to-score"];

// concurrency-limited map (keeps order)
async function _mapPool(items, worker, concurrency = 10) {
  const results = new Array(items.length);
  let idx = 0;
  async function lane() {
    while (idx < items.length) {
      const i = idx++;
      try { results[i] = await worker(items[i], i); } catch (e) { results[i] = null; }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length || 1) }, lane));
  return results;
}

async function get_pos_report(wallet, query, onProgress) {
  query = (query || "").trim();
  let markets = [];           // [{conditionId, question}]
  let title = "";
  let isSports = false;

  // 1) direct conditionId
  if (query.startsWith("0x") && query.length > 20 && !query.includes(" ")) {
    const m = await API.get_market_info(query);
    title = (m && m.question) || query.slice(0, 20);
    markets = [{ conditionId: query, question: title }];

  // 2) Polymarket URL
  } else if (query.includes("polymarket.com")) {
    let slug = "", market_slug = "";
    const path = query.split("polymarket.com").pop().split("?")[0].split("#")[0].replace(/^\/+|\/+$/g, "");
    const parts = path.split("/").filter(Boolean);
    const head = parts[0] || "";
    if (head === "event") { slug = parts[1] || ""; market_slug = parts[2] || ""; }
    else if (head === "market") { slug = parts[1] || ""; market_slug = slug; }
    else if (head === "sports") { slug = parts[parts.length - 1] || ""; }   // /sports/<league>/<game>
    else { slug = parts[parts.length - 1] || ""; }                          // generic: last path segment
    if (!slug) return { error: "Could not read the market from that link." };

    // Sports match: each bet type (Moneyline / Spreads / Totals / Corners / ...) is a
    // SEPARATE Gamma event sharing the match's slug prefix. We fetch those events directly
    // by known suffixes (fast, no need to download the whole wallet) and collect their markets.
    if (head === "sports") {
      isSports = true;
      const base = slug;
      const events = await _mapPool(SPORT_SUFFIXES, (sfx) => API.get_event_by_slug_path(base + sfx), SPORT_SUFFIXES.length);
      const seen = {};
      for (const ev of events) {
        if (!ev) continue;
        if (!title && ev.title) title = ev.title.replace(/ - .*$/, "");   // "Panama vs. England"
        for (const m of (ev.markets || [])) {
          const cid = m.conditionId || m.condition_id || "";
          if (cid && !seen[cid]) { seen[cid] = true; markets.push({ conditionId: cid, question: m.question || m.groupItemTitle || ev.title || "" }); }
        }
      }
      if (!markets.length) return { error: `No markets found for this match (${base}).` };

    } else {
      let found = [];
      const ev = await API.get_event_by_slug(slug);
      if (ev) { title = ev.title || ""; for (const m of (ev.markets || [])) found.push(m); }
      if (market_slug && found.length) {
        const specific = found.filter((m) => m.slug === market_slug);
        if (specific.length) found = specific;
      }
      if (!found.length) {
        for (const ts of (market_slug ? [market_slug, slug] : [slug])) {
          if (!ts) continue;
          const data = await API.get_markets_by_slug(ts);
          if (data.length) { found = data; break; }
        }
      }
      if (!found.length) return { error: `No market found for "${slug}". If this is a sports/category listing page, open a specific game and use that link.` };
      markets = found.map((m) => ({ conditionId: m.conditionId || m.condition_id || "", question: m.question || title || "" }));
    }

  } else {
    return { error: "Paste a Polymarket link (or a conditionId)." };
  }

  // build per-market trades + summary, in parallel (fast even for ~76 markets)
  let _done = 0; const _total = markets.length;
  const built = await _mapPool(markets, async (mk) => {
    if (!mk.conditionId) { _done++; if (onProgress) onProgress(_done, _total); return null; }
    const raw = await API.get_trades(wallet, mk.conditionId);
    const trades = raw.slice().sort((a, b) => normalize_ts(a.timestamp) - normalize_ts(b.timestamp));
    const running = {};
    const tlist = trades.map((t) => {
      const side = up(t.side), size = num(t.size), price = num(t.price), ts = normalize_ts(t.timestamp);
      const outcome = t.outcome || "?", type = t.type || "TRADE";
      if (!(outcome in running)) running[outcome] = 0;
      if (side === "BUY") running[outcome] += size;
      else if (side === "SELL") { running[outcome] -= size; if (running[outcome] < 0) running[outcome] = 0; }
      return { side, date: fmt_ts(ts), size, price, outcome, total: running[outcome], type };
    });
    const byOut = {};
    for (const t of trades) { const o = t.outcome || "Unknown"; (byOut[o] = byOut[o] || []).push(t); }
    const summary = Object.keys(byOut).map((o) => {
      const list = byOut[o];
      const buys = list.filter((t) => up(t.side) === "BUY");
      const sells = list.filter((t) => up(t.side) === "SELL");
      const tb = buys.reduce((s, t) => s + num(t.size), 0);
      const tsold = sells.reduce((s, t) => s + num(t.size), 0);
      const spent = buys.reduce((s, t) => s + num(t.size) * num(t.price), 0);
      const recv = sells.reduce((s, t) => s + num(t.size) * num(t.price), 0);
      return { outcome: o, bought: tb, spent, sold: tsold, received: recv, net: tb - tsold };
    });
    _done++; if (onProgress) onProgress(_done, _total);
    const asset = (trades.find((t) => t.asset) || {}).asset || "";
    const outcome_side = trades.length ? (trades[0].outcome || "") : "";
    return { conditionId: mk.conditionId, asset, outcome_side, question: mk.question, trades: tlist, summary, hasTrades: trades.length > 0 };
  }, 10);

  let out = built.filter(Boolean);
  if (isSports) out = out.filter((m) => m.hasTrades);   // sports: only bet types actually traded
  const eventUrl = query.includes("polymarket.com") ? query : "";
  return { title, eventUrl, markets: out };
}

/* ---------- exports ---------- */
const CORE = {
  API, num, normalize_ts, fmt_ts, pyRound1,
  filter_hedged, filter_trades_by_token, get_token_final_price, get_outcome_token_id,
  get_event_url, build_event_url, get_title,
  calc_shares_stats, calc_timeline_ath, calc_post_exit_ath,
  pos_key, pos_fingerprint, pos_resolved,
  ath_compute_core, extract_buys_sells, process_ath, process_resolution, get_position_detail,
  get_pos_report, clearCache: _clearCache,
  _reqStats, _resetStats: () => { Object.assign(_reqStats, { total: 0, slow: 0, aborted: 0, gaveup: 0, inflight: 0 }); return _reqStats; },
};
// Node (parity tests)
if (typeof module !== "undefined" && module.exports) module.exports = CORE;
// Browser — expose as window.core so index.html can call core.*
if (typeof window !== "undefined") window.core = CORE;
