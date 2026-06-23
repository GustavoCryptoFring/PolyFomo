"""
Polymarket Wallet Analyzer Bot v2 — Clean rewrite
Architecture:
  1. /positions + /closed-positions → full list of unique positions
  2. /activity?user=X&market=conditionId (TRADE only) → per-market trades
  3. /prices-history → price history for ATH calculation
  4. Timeline ATH: tracks shares × price over time (not just max price)
  5. Post-exit ATH: checks price after full exit using max simultaneous shares
"""

import time
import json
import uuid
import os
import sqlite3
import logging
import threading
import requests
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timezone

# ═════════════════════════════════════════════
#  CONFIG
# ═════════════════════════════════════════════

# Bot token is loaded from the environment variable TELEGRAM_BOT_TOKEN,
# or from a local "token.txt" file next to this script (kept OUT of git via .gitignore).
#   export TELEGRAM_BOT_TOKEN="123456:abcdef"      # option A
#   echo "123456:abcdef" > token.txt               # option B
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
if not TELEGRAM_BOT_TOKEN:
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "token.txt")) as _f:
            TELEGRAM_BOT_TOKEN = _f.read().strip()
    except FileNotFoundError:
        pass

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"
DATA_API  = "https://data-api.polymarket.com"

PAGE_SIZE = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

user_state = {}
state_lock = threading.Lock()


# ═════════════════════════════════════════════
#  TELEGRAM HELPERS
# ═════════════════════════════════════════════

def tg_post(method: str, **kwargs) -> dict:
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",
            timeout=kwargs.pop("timeout", 15),
            **kwargs,
        )
        return resp.json() if resp.status_code == 200 else {}
    except Exception as e:
        log.error(f"TG {method} error: {e}")
        return {}


def send_message(chat_id: int, text: str, reply_markup: dict = None) -> dict:
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            send_message(chat_id, text[i:i+4000])
            time.sleep(0.3)
        return {}
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    return tg_post("sendMessage", json=payload)


def edit_message(chat_id: int, message_id: int, text: str) -> None:
    tg_post("editMessageText", json={
        "chat_id": chat_id, "message_id": message_id,
        "text": text, "disable_web_page_preview": True,
    }, timeout=10)


def send_typing(chat_id: int) -> None:
    tg_post("sendChatAction", json={"chat_id": chat_id, "action": "typing"}, timeout=5)


def answer_callback(cq_id: str) -> None:
    tg_post("answerCallbackQuery", json={"callback_query_id": cq_id}, timeout=5)


def send_photo(chat_id: int, photo_path: str, caption: str = "") -> None:
    try:
        with open(photo_path, "rb") as f:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
                files={"photo": f}, timeout=30,
            )
    except Exception as e:
        log.error(f"Send photo error: {e}")
    finally:
        try:
            os.remove(photo_path)
        except Exception:
            pass


def mode_keyboard() -> dict:
    return {"inline_keyboard": [[
        {"text": "📈 Mode 1 — ATH Analysis", "callback_data": "mode_1"},
        {"text": "✅ Mode 2 — Resolution",   "callback_data": "mode_2"},
    ]]}


def page_keyboard(page: int, total_pages: int, session_id: str) -> dict:
    buttons = []
    if page > 0:
        buttons.append({"text": "◀ Назад", "callback_data": f"page_{session_id}_{page - 1}"})
    buttons.append({"text": f"{page + 1}/{total_pages}", "callback_data": "noop"})
    if page < total_pages - 1:
        buttons.append({"text": "Далее ▶", "callback_data": f"page_{session_id}_{page + 1}"})
    return {"inline_keyboard": [buttons]}


# ═════════════════════════════════════════════
#  UTILS
# ═════════════════════════════════════════════

def normalize_ts(raw) -> int:
    v = int(raw or 0)
    return v // 1000 if v > 10_000_000_000 else v


def fmt_ts(ts: int) -> str:
    if not ts:
        return "unknown"
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "unknown"


def get_batch_size(total: int) -> int:
    if total <= 50:
        return 8
    elif total <= 200:
        return 15
    elif total <= 1000:
        return 30
    return 50


# ═════════════════════════════════════════════
#  CACHE (SQLite, single file: cache.db)
# ═════════════════════════════════════════════
# Only wallets with more than CACHE_MIN_POSITIONS positions are cached.
# Re-analysis recomputes ONLY positions whose fingerprint changed; resolved
# positions whose fingerprint matches are served straight from the cache.
# TTL is a sliding window: every access sets expiry to now + CACHE_TTL_DAYS.
# A hard size cap evicts the least-recently-used wallets first.

CACHE_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache.db")
CACHE_MIN_POSITIONS = 1000
CACHE_TTL_DAYS = 20
CACHE_MAX_BYTES = 10 * 1024 ** 3   # 10 GB
_cache_lock = threading.Lock()


def cache_init():
    """Create the cache database if it doesn't exist."""
    try:
        with _cache_lock:
            con = sqlite3.connect(CACHE_DB, timeout=30)
            # auto_vacuum=FULL so deleted rows shrink the file on disk (size cap works).
            con.execute("PRAGMA auto_vacuum=FULL")
            con.execute("""CREATE TABLE IF NOT EXISTS wallets(
                wallet TEXT PRIMARY KEY, last_access INTEGER, expires_at INTEGER)""")
            con.execute("""CREATE TABLE IF NOT EXISTS positions(
                wallet TEXT, mode TEXT, pos_key TEXT, fingerprint TEXT,
                is_skip INTEGER, result_json TEXT,
                PRIMARY KEY(wallet, mode, pos_key))""")
            con.commit()
            con.close()
        log.info(f"Cache ready: {CACHE_DB} (min {CACHE_MIN_POSITIONS} pos, "
                 f"TTL {CACHE_TTL_DAYS}d, cap {CACHE_MAX_BYTES // 1024**3}GB)")
    except Exception as e:
        log.error(f"cache_init failed: {e}")


def _pos_key(p) -> str:
    return (p.get("conditionId") or p.get("market") or "") + "|" + (p.get("asset") or "")


def _pos_fingerprint(p) -> str:
    # Cheap fields from the bulk positions list — catch buys, sells, resolution.
    return f"{p.get('totalBought')}|{p.get('size')}|{p.get('realizedPnl')}|{p.get('outcome')}"


def _pos_resolved(p) -> bool:
    # Market settled → result is final and safe to cache forever.
    cp = float(p.get("curPrice") or 0)
    return cp <= 0.05 or cp >= 0.95


def cache_load(wallet: str, mode: str) -> dict:
    """Return {pos_key: (fingerprint, is_skip, result_dict_or_None)} for this wallet+mode."""
    out = {}
    try:
        with _cache_lock:
            con = sqlite3.connect(CACHE_DB, timeout=30)
            cur = con.execute(
                "SELECT pos_key, fingerprint, is_skip, result_json FROM positions "
                "WHERE wallet=? AND mode=?", (wallet, mode))
            for pk, fp, skip, rj in cur.fetchall():
                out[pk] = (fp, bool(skip), json.loads(rj) if rj else None)
            con.close()
    except Exception as e:
        log.error(f"cache_load failed: {e}")
    return out


def cache_store(wallet: str, mode: str, rows: list):
    """Upsert position rows, refresh the wallet's sliding TTL, evict expired + over-cap.
    rows = list of (pos_key, fingerprint, is_skip_bool, result_dict_or_None)."""
    now = int(time.time())
    exp = now + CACHE_TTL_DAYS * 86400
    try:
        with _cache_lock:
            con = sqlite3.connect(CACHE_DB, timeout=30)
            if rows:
                con.executemany(
                    "INSERT OR REPLACE INTO positions(wallet,mode,pos_key,fingerprint,is_skip,result_json) "
                    "VALUES(?,?,?,?,?,?)",
                    [(wallet, mode, pk, fp, 1 if skip else 0, json.dumps(r) if r else None)
                     for (pk, fp, skip, r) in rows])
            # sliding TTL — every access resets expiry to now + TTL
            con.execute("INSERT OR REPLACE INTO wallets(wallet,last_access,expires_at) VALUES(?,?,?)",
                        (wallet, now, exp))
            # drop expired wallets
            dead = [w[0] for w in con.execute("SELECT wallet FROM wallets WHERE expires_at < ?", (now,)).fetchall()]
            for w in dead:
                con.execute("DELETE FROM positions WHERE wallet=?", (w,))
                con.execute("DELETE FROM wallets WHERE wallet=?", (w,))
            con.commit()
            # enforce hard size cap — evict least-recently-used wallets first
            try:
                if os.path.getsize(CACHE_DB) > CACHE_MAX_BYTES:
                    order = [w[0] for w in con.execute(
                        "SELECT wallet FROM wallets ORDER BY last_access ASC").fetchall()]
                    for w in order:
                        if w == wallet:
                            continue  # never evict the wallet we just analyzed
                        con.execute("DELETE FROM positions WHERE wallet=?", (w,))
                        con.execute("DELETE FROM wallets WHERE wallet=?", (w,))
                        con.commit()
                        if os.path.getsize(CACHE_DB) <= CACHE_MAX_BYTES * 0.9:
                            break
            except Exception as e:
                log.error(f"cache size enforce failed: {e}")
            con.close()
    except Exception as e:
        log.error(f"cache_store failed: {e}")


def cache_split(wallet: str, mode: str, positions: list):
    """Split positions into (reused_results, to_compute) using the cache.
    Returns (reused_results, to_compute, use_cache)."""
    use_cache = len(positions) > CACHE_MIN_POSITIONS
    if not use_cache:
        return [], list(positions), False
    cached = cache_load(wallet, mode)
    reused, to_compute = [], []
    for p in positions:
        row = cached.get(_pos_key(p))
        if row and row[0] == _pos_fingerprint(p) and _pos_resolved(p):
            fp, is_skip, res = row
            if not is_skip and res:
                reused.append(res)
            # a cached skip contributes nothing — and costs no API call
        else:
            to_compute.append(p)
    return reused, to_compute, True


def cache_collect_rows(to_compute: list, outcomes: dict) -> list:
    """Build cache rows for the freshly computed positions — only the resolved (final) ones.
    outcomes = {pos_key: result_dict_or_None}."""
    rows = []
    for p in to_compute:
        if _pos_resolved(p):
            r = outcomes.get(_pos_key(p))
            rows.append((_pos_key(p), _pos_fingerprint(p), r is None, r))
    return rows


# ═════════════════════════════════════════════
#  POLYMARKET API
# ═════════════════════════════════════════════

def get_all_positions(wallet: str) -> list:
    """Fetch active + closed positions, deduplicated by (conditionId, asset)."""
    seen = set()
    positions = []

    def _add(batch):
        for p in batch:
            cid = p.get("conditionId") or p.get("market") or ""
            aid = p.get("asset") or ""
            key = (cid, aid)
            if key not in seen and cid:
                seen.add(key)
                positions.append(p)

    # Active
    offset = 0
    while True:
        try:
            r = requests.get(f"{DATA_API}/positions",
                             params={"user": wallet, "limit": 500, "offset": offset, "sizeThreshold": "0"},
                             timeout=15)
            if r.status_code != 200:
                break
            batch = r.json() if isinstance(r.json(), list) else (r.json().get("data") or r.json().get("positions") or [])
        except Exception as e:
            log.error(f"/positions error: {e}")
            break
        if not batch:
            break
        _add(batch)
        if len(batch) < 500:
            break
        offset += 500

    active_count = len(positions)

    # Closed
    offset = 0
    closed_fetched = 0
    CLOSED_LIMIT = 50
    while True:
        try:
            r = requests.get(f"{DATA_API}/closed-positions",
                             params={"user": wallet, "limit": CLOSED_LIMIT, "offset": offset}, timeout=30)
            if r.status_code != 200:
                time.sleep(1)
                r = requests.get(f"{DATA_API}/closed-positions",
                                 params={"user": wallet, "limit": CLOSED_LIMIT, "offset": offset}, timeout=30)
                if r.status_code != 200:
                    log.warning(f"/closed-positions status {r.status_code} at offset {offset}, stopping")
                    break
            batch = r.json() if isinstance(r.json(), list) else (r.json().get("data") or [])
        except Exception as e:
            log.error(f"/closed-positions error at offset {offset}: {e}")
            time.sleep(2)
            try:
                r = requests.get(f"{DATA_API}/closed-positions",
                                 params={"user": wallet, "limit": CLOSED_LIMIT, "offset": offset}, timeout=30)
                if r.status_code != 200:
                    break
                batch = r.json() if isinstance(r.json(), list) else (r.json().get("data") or [])
            except Exception:
                break
        if not batch:
            break
        _add(batch)
        closed_fetched += len(batch)
        if closed_fetched % 500 == 0:
            log.info(f"Fetched {closed_fetched} closed positions so far...")
        if len(batch) < CLOSED_LIMIT:
            break
        offset += CLOSED_LIMIT
        time.sleep(0.1)

    log.info(f"Positions: {len(positions)} (active={active_count}, closed={len(positions)-active_count})")
    return positions


def filter_hedged(positions: list) -> list:
    """Remove positions where user has both Yes and No on same conditionId."""
    cid_outcomes = {}
    for p in positions:
        cid = p.get("conditionId") or p.get("market") or ""
        out = (p.get("outcome") or "").upper()
        if cid and out:
            cid_outcomes.setdefault(cid, set()).add(out)

    hedged = {c for c, outs in cid_outcomes.items() if len(outs) > 1}
    if hedged:
        before = len(positions)
        positions = [p for p in positions if (p.get("conditionId") or p.get("market") or "") not in hedged]
        log.info(f"Filtered {before - len(positions)} hedged positions")
    return positions


def get_trades(wallet: str, condition_id: str) -> list:
    """Fetch TRADE-only records for a specific market."""
    trades = []
    offset = 0
    while offset <= 2000:
        try:
            r = requests.get(f"{DATA_API}/activity",
                             params={"user": wallet, "market": condition_id, "limit": 100, "offset": offset},
                             timeout=8)
            if r.status_code != 200:
                break
            batch = r.json() if isinstance(r.json(), list) else (r.json().get("data") or r.json().get("activity") or [])
        except Exception:
            break
        if not batch:
            break
        for item in batch:
            item_type = (item.get("type") or "").upper()
            # TRADE = regular buy/sell, SPLIT = buy via split, CONVERSION = convert between outcomes
            if item_type not in ("TRADE", "SPLIT", "CONVERSION"):
                continue
            side = item.get("side") or ""
            size = item.get("size") or 0
            price = item.get("price") or 0
            # SPLIT creates both Yes and No shares — treat as BUY at 0.50
            if item_type == "SPLIT":
                side = "BUY"
                price = 0.5
            # CONVERSION: treat as BUY for the received outcome
            elif item_type == "CONVERSION":
                side = "BUY"
            trades.append({
                "asset":       item.get("asset") or "",
                "side":        side if side else "BUY",
                "size":        size,
                "price":       price,
                "outcome":     item.get("outcome") or "",
                "timestamp":   item.get("timestamp") or 0,
                "conditionId": item.get("conditionId") or "",
                "type":        item_type,
            })
        if len(batch) < 100:
            break
        offset += 100
    return trades


def filter_trades_by_token(trades: list, token_id: str, pos_outcome: str) -> list:
    """Filter trades to match specific Yes/No token. SPLIT/CONVERSION always pass."""
    if not token_id or not trades:
        return trades

    # SPLIT/CONVERSION apply to all outcomes — never filter them out
    special = [t for t in trades if t.get("type") in ("SPLIT", "CONVERSION")]
    regular = [t for t in trades if t.get("type") not in ("SPLIT", "CONVERSION")]

    # Try by asset first
    by_asset = [t for t in regular if t.get("asset") == token_id]
    if by_asset:
        return by_asset + special
    # Fallback by outcome
    if pos_outcome:
        by_outcome = [t for t in regular if (t.get("outcome") or "").upper() == pos_outcome.upper()]
        if by_outcome:
            return by_outcome + special
    return trades


def get_price_history(token_id: str, condition_id: str = "") -> list:
    """Try market=token_id, market=conditionId, token_id=token_id."""
    ids_to_try = [token_id]
    if condition_id and condition_id != token_id:
        ids_to_try.append(condition_id)

    for tid in ids_to_try:
        if not tid:
            continue
        for interval, fidelity in [("max", 1000), ("1d", 100), ("1w", 50)]:
            for param in ("market", "token_id"):
                try:
                    r = requests.get(f"{CLOB_API}/prices-history",
                                     params={param: tid, "interval": interval, "fidelity": fidelity},
                                     timeout=8)
                    if r.status_code != 200:
                        continue
                    history = r.json().get("history") or []
                    if history:
                        return history
                except Exception:
                    pass
    return []


def get_price_history_from_trades(condition_id: str, outcome: str, first_buy_ts: int) -> list:
    """Build price history from market trades after first_buy_ts, grouped by minute."""
    if not condition_id or not first_buy_ts:
        return []

    all_trades = []
    offset = 0
    # Trades are returned newest-first, paginate until we pass first_buy_ts
    while offset <= 10000:
        try:
            r = requests.get(f"{DATA_API}/trades",
                             params={"market": condition_id, "limit": 500, "offset": offset},
                             timeout=10)
            if r.status_code != 200:
                break
            batch = r.json() if isinstance(r.json(), list) else []
        except Exception:
            break
        if not batch:
            break
        # Filter by outcome and after first_buy_ts
        for t in batch:
            if (t.get("outcome") or "").upper() == outcome.upper():
                ts = int(t.get("timestamp") or 0)
                if ts >= first_buy_ts:
                    all_trades.append(t)
        # Check if we've gone past first_buy_ts
        oldest_ts = min(int(t.get("timestamp") or 0) for t in batch)
        if oldest_ts < first_buy_ts:
            break
        offset += 500
        time.sleep(0.05)

    if not all_trades:
        return []

    # Group by minute, take first trade per minute
    by_minute = {}
    for t in all_trades:
        ts = int(t.get("timestamp") or 0)
        minute = (ts // 60) * 60  # round down to minute
        if minute not in by_minute:
            by_minute[minute] = float(t.get("price") or 0)

    # Return as history format [{t, p}] sorted by time
    history = [{"t": ts, "p": str(price)} for ts, price in sorted(by_minute.items()) if price > 0]
    log.info(f"[TRADES HISTORY] {condition_id[:20]}.. outcome={outcome}: {len(history)} minute-points from {len(all_trades)} trades")
    return history


def get_market_info(condition_id: str) -> dict:
    if not condition_id:
        return None
    want = condition_id.lower()
    for param in ({"condition_ids": condition_id}, {"conditionId": condition_id}):
        try:
            r = requests.get(f"{GAMMA_API}/markets", params=param, timeout=6)
            if r.status_code != 200:
                continue
            data = r.json()
            candidates = data if isinstance(data, list) else ([data] if isinstance(data, dict) and data else [])
            for m in candidates:
                mcid = (m.get("conditionId") or m.get("condition_id") or "").lower()
                # Only trust a market whose conditionId actually matches the one we asked for.
                # Gamma can return unrelated default markets for an unknown/truncated id, and
                # blindly taking data[0] is exactly what produced wrong links (e.g. "GTA VI").
                # startswith handles truncated ids from /closed-positions (prefix of the real id).
                if mcid and (mcid == want or mcid.startswith(want)):
                    return m
        except Exception:
            pass
    return None


def get_token_final_price(market: dict, token_id: str):
    if not market:
        return None
    try:
        prices_raw = market.get("outcomePrices")
        tokens_raw = market.get("clobTokenIds") or market.get("tokens") or "[]"
        if not prices_raw:
            return None
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
        tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
        if not isinstance(tokens, list) or not isinstance(prices, list):
            return None
        for idx, tok in enumerate(tokens):
            tok_id = tok if isinstance(tok, str) else (tok.get("token_id") or tok.get("id") or "")
            if tok_id == token_id and idx < len(prices):
                return float(prices[idx])
    except Exception:
        pass
    return None


def get_outcome_token_id(market: dict, outcome: str) -> str:
    """Get the correct token_id for a specific outcome (Yes/No) from gamma market."""
    if not market:
        return ""
    try:
        tokens_raw = market.get("clobTokenIds") or market.get("tokens") or "[]"
        outcomes_raw = market.get("outcomes") or "[]"
        tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
        outcomes_list = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
        if not isinstance(tokens, list) or not isinstance(outcomes_list, list):
            return ""
        for idx, out in enumerate(outcomes_list):
            if out.upper() == outcome.upper() and idx < len(tokens):
                tok = tokens[idx]
                return tok if isinstance(tok, str) else (tok.get("token_id") or tok.get("id") or "")
    except Exception:
        pass
    return ""


def get_event_url(market: dict) -> str:
    if not market:
        return ""
    events = market.get("events") or []
    if isinstance(events, str):
        try:
            events = json.loads(events)
        except Exception:
            events = []
    if isinstance(events, list) and events:
        first = events[0]
        if isinstance(first, dict):
            slug = first.get("slug") or ""
            if slug:
                return "https://polymarket.com/event/" + slug
    slug = market.get("slug") or ""
    return ("https://polymarket.com/event/" + slug) if slug else ""


def build_event_url(pos, market) -> str:
    """Build the Polymarket event URL, preferring the position's OWN slug data.
    The position always carries the correct eventSlug/slug for the user's market,
    whereas a Gamma lookup can fail or return a wrong market for resolved markets."""
    event_slug = (pos.get("eventSlug") or "").strip()
    if event_slug:
        return "https://polymarket.com/event/" + event_slug
    market_slug = (pos.get("slug") or "").strip()
    if market_slug:
        return "https://polymarket.com/event/" + market_slug
    return get_event_url(market)


# ═════════════════════════════════════════════
#  SHARED ANALYSIS HELPERS
# ═════════════════════════════════════════════

def extract_buys_sells(wallet, pos):
    """Get filtered buys/sells for a position. Returns (buys, sells, pos_outcome, full_cid)."""
    condition_id = pos.get("conditionId") or pos.get("market") or ""
    token_id = pos.get("asset") or ""
    pos_outcome = pos.get("outcome") or ""

    raw_trades = get_trades(wallet, condition_id) if condition_id else []

    # Grab full conditionId from raw trades (closed-positions may truncate)
    full_cid = condition_id
    for t in raw_trades:
        tc = t.get("conditionId") or ""
        if len(tc) > len(full_cid):
            full_cid = tc
            break
    trades = filter_trades_by_token(raw_trades, token_id, pos_outcome)

    buys = [t for t in trades if (t.get("side") or "").upper() == "BUY"]
    sells = [t for t in trades if (t.get("side") or "").upper() == "SELL"]

    # Fallback to position data if no trades
    if not buys:
        avg_price = float(pos.get("avgPrice") or pos.get("curPrice") or 0)
        size = max(float(pos.get("size") or 0), float(pos.get("totalBought") or 0),
                   float(pos.get("initialValue") or 0))
        realized = float(pos.get("realizedPnl") or 0)
        # Skip phantom positions (no trades, no realized PnL)
        if (avg_price <= 0 or size <= 0) or (realized == 0 and not raw_trades):
            return None, None, pos_outcome, full_cid
        buys = [{"size": size, "price": avg_price, "timestamp": 0}]

    return buys, sells, pos_outcome, full_cid


def calc_shares_stats(buys, sells):
    """Calculate total_shares, total_spent, avg_entry, max_simultaneous_shares."""
    total_shares = sum(float(t.get("size") or 0) for t in buys)
    total_spent = sum(float(t.get("size") or 0) * float(t.get("price") or 0) for t in buys)
    avg_entry = total_spent / total_shares if total_shares > 0 else 0

    # Max simultaneous shares
    ops = []
    for t in buys:
        ops.append((normalize_ts(t.get("timestamp") or 0), "BUY", float(t.get("size") or 0)))
    for t in sells:
        ops.append((normalize_ts(t.get("timestamp") or 0), "SELL", float(t.get("size") or 0)))
    ops.sort(key=lambda x: x[0])

    max_simul = 0.0
    running = 0.0
    for _, side, size in ops:
        if side == "BUY":
            running += size
        else:
            running -= size
            if running < 0:
                running = 0
        if running > max_simul:
            max_simul = running

    if max_simul == 0:
        max_simul = total_shares

    return total_shares, total_spent, avg_entry, max_simul


def calc_timeline_ath(buys, sells, history, first_buy_ts, last_sell_ts):
    """Calculate best portfolio value while holding position using timeline."""
    trade_events = []
    for t in buys:
        trade_events.append((normalize_ts(t.get("timestamp") or 0), "BUY", float(t.get("size") or 0)))
    for t in sells:
        trade_events.append((normalize_ts(t.get("timestamp") or 0), "SELL", float(t.get("size") or 0)))
    trade_events.sort(key=lambda x: x[0])

    price_points = []
    for h in history:
        t_ts = normalize_ts(h.get("t") or 0)
        p = float(h.get("p") or 0)
        if p <= 0:
            continue
        if first_buy_ts > 0 and t_ts <= first_buy_ts:
            continue
        if last_sell_ts > 0 and t_ts > last_sell_ts:
            continue
        price_points.append((t_ts, p))

    # Merge into timeline: trades (priority 0) before prices (priority 1) at same ts
    timeline = []
    for ts, side, size in trade_events:
        timeline.append((ts, 0, "BUY" if side == "BUY" else "SELL", size))
    for ts, p in price_points:
        timeline.append((ts, 1, "PRICE", p))
    timeline.sort(key=lambda x: (x[0], x[1]))

    running = 0.0
    best_val = 0.0
    best_price = 0.0
    best_ts = 0

    for ts, _, event_type, value in timeline:
        if event_type == "BUY":
            running += value
        elif event_type == "SELL":
            running -= value
            if running < 0:
                running = 0
        elif event_type == "PRICE":
            pv = running * value
            if pv > best_val:
                best_val = pv
                best_price = value
                best_ts = ts

    return best_val, best_price, best_ts, len(price_points)


def calc_post_exit_ath(history, last_sell_ts, max_simul, gamma_price):
    """Calculate best value after full exit using max simultaneous shares."""
    best_val = 0.0
    best_price = 0.0
    best_ts = 0

    for h in history:
        t_ts = normalize_ts(h.get("t") or 0)
        p = float(h.get("p") or 0)
        if p <= 0 or t_ts <= last_sell_ts:
            continue
        val = max_simul * p
        if val > best_val:
            best_val = val
            best_price = p
            best_ts = t_ts

    if gamma_price is not None:
        gamma_val = max_simul * gamma_price
        if gamma_val > best_val:
            best_val = gamma_val
            best_price = gamma_price
            if best_ts == 0:
                # Try to find ts from history
                for h in history:
                    if float(h.get("p") or 0) >= gamma_price * 0.99:
                        best_ts = normalize_ts(h.get("t") or 0)
                        break
                if best_ts == 0:
                    best_ts = last_sell_ts  # approximate

    return best_val, best_price, best_ts


def get_title(pos, market):
    """Get title: prefer position data, fallback to gamma."""
    title = pos.get("title") or ""
    if not title and market:
        title = market.get("question") or ""
    return title or "Unknown"


# ═════════════════════════════════════════════
#  PROGRESS BAR HELPER
# ═════════════════════════════════════════════

def run_with_progress(chat_id, positions, worker_fn, label="Analyzing"):
    """Run worker_fn on positions in parallel batches with progress bar."""
    total = len(positions)
    results = []
    results_lock = threading.Lock()
    progress = {"done": 0}
    progress_lock = threading.Lock()

    def prog_bar(done, tot):
        pct = int(done / tot * 20) if tot else 0
        return f"⏳ {label}... [{'█' * pct}{'░' * (20 - pct)}] {done}/{tot}"

    prog_resp = send_message(chat_id, prog_bar(0, total))
    prog_msg_id = (prog_resp.get("result") or {}).get("message_id") if isinstance(prog_resp, dict) else None
    reported = {"val": 0}

    def update_progress():
        while True:
            time.sleep(2)
            with progress_lock:
                done = progress["done"]
            if done >= total:
                break
            pct = int(done / total * 100) if total else 0
            milestone = (pct // 25) * 25
            if milestone > reported["val"] and milestone < 100:
                reported["val"] = milestone
                if prog_msg_id:
                    edit_message(chat_id, prog_msg_id, prog_bar(done, total))

    threading.Thread(target=update_progress, daemon=True).start()

    def wrapped_worker(pos):
        try:
            result = worker_fn(pos)
        except Exception as e:
            log.error(f"Worker error: {e}")
            result = None
        with progress_lock:
            progress["done"] += 1
        if result:
            with results_lock:
                results.append(result)

    BATCH = get_batch_size(total)
    log.info(f"{label}: batch={BATCH} for {total} positions")

    for i in range(0, total, BATCH):
        batch = positions[i:i + BATCH]
        threads = [threading.Thread(target=wrapped_worker, args=(p,), daemon=True) for p in batch]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=45)
        stuck = sum(1 for th in threads if th.is_alive())
        if stuck:
            log.warning(f"{label}: {stuck} threads timed out")
            with progress_lock:
                progress["done"] += stuck
        time.sleep(0.1)

    with progress_lock:
        progress["done"] = total

    log.info(f"{label}: completed. {len(results)} results from {total} positions")
    return results


# ═════════════════════════════════════════════
#  MODE 1 — ATH ANALYSIS
# ═════════════════════════════════════════════

def process_ath(wallet, pos):
    """Analyze a single position for ATH missed profit."""
    condition_id = pos.get("conditionId") or pos.get("market") or ""
    token_id = pos.get("asset") or ""
    pos_title = pos.get("title") or pos.get("slug") or condition_id[:12]
    if not condition_id and not token_id:
        return None

    buys, sells, pos_outcome, full_cid = extract_buys_sells(wallet, pos)
    if buys is None:
        log.info(f"[SKIP] {pos_title[:40]}: no buys/phantom")
        return None

    # Use full conditionId from trades if available
    if len(full_cid) > len(condition_id):
        condition_id = full_cid

    # Update IDs from trades — /closed-positions may truncate them
    for t in buys:
        t_asset = t.get("asset") or ""
        if len(t_asset) > len(token_id):
            token_id = t_asset
            break

    total_shares, total_spent, avg_entry, max_simul = calc_shares_stats(buys, sells or [])

    # Detect if position lost (resolved against user)
    cur_price_raw = float(pos.get("curPrice") or 0)
    position_lost = False
    if cur_price_raw > 0:
        if pos_outcome.upper() == "NO" and cur_price_raw >= 0.95:
            position_lost = True  # Event resolved Yes → No = $0
        elif pos_outcome.upper() == "YES" and cur_price_raw <= 0.05:
            position_lost = True  # Event resolved No → Yes = $0

    if "Lebanon" in pos_title or "Lebanon" in (pos.get("title") or ""):
        log.info(f"[DBG-LB] curPrice_raw={cur_price_raw} outcome={pos_outcome} position_lost={position_lost}")

    # Timestamps
    buy_ts = [normalize_ts(t.get("timestamp") or 0) for t in buys if t.get("timestamp")]
    sell_ts = [normalize_ts(t.get("timestamp") or 0) for t in (sells or []) if t.get("timestamp")]
    first_buy = min(buy_ts) if buy_ts else 0
    last_sell = max(sell_ts) if sell_ts else 0

    # Price history & market info
    history = get_price_history(token_id, condition_id) if token_id else []
    market = get_market_info(condition_id) if condition_id else None
    gamma_price = get_token_final_price(market, token_id) if market and token_id else None

    # If CLOB history is too sparse (< 20 points), build from market trades
    if len(history) < 20 and condition_id and first_buy > 0:
        trades_history = get_price_history_from_trades(condition_id, pos_outcome, first_buy)
        if len(trades_history) > len(history):
            log.info(f"[TRADES HISTORY] {pos_title[:40]}: using trades history ({len(trades_history)} pts) over CLOB ({len(history)} pts)")
            history = trades_history

    # TOKEN FIX removed — pos.get("asset") is already the correct Yes/No token

    # If No position — check if price history is for wrong outcome
    # Compare history prices near buy time with entry price
    history_inverted = False
    if history and pos_outcome.upper() == "NO" and avg_entry > 0 and avg_entry < 0.95:
        prices_near_buy = []
        for h in history:
            t_ts = normalize_ts(h.get("t") or 0)
            p = float(h.get("p") or 0)
            if p > 0 and first_buy > 0 and abs(t_ts - first_buy) < 86400 * 3:
                prices_near_buy.append(p)
        if not prices_near_buy:
            prices_near_buy = [float(h.get("p") or 0) for h in history[:10] if float(h.get("p") or 0) > 0]
        if prices_near_buy:
            avg_hist = sum(prices_near_buy) / len(prices_near_buy)
            inverted_avg = 1.0 - avg_hist
            if abs(inverted_avg - avg_entry) < abs(avg_hist - avg_entry):
                log.info(f"[INVERT] {pos_title[:40]}: inverting history for No "
                         f"(entry={avg_entry:.3f}, hist={avg_hist:.3f}, inv={inverted_avg:.3f})")
                for h in history:
                    p = float(h.get("p") or 0)
                    if p > 0:
                        h["p"] = str(1.0 - p)
                history_inverted = True

    # Force inversion for confirmed losing positions if not already inverted
    # A lost NO position means YES won → price history is for YES (goes to 1.0) → must invert
    if history and position_lost and not history_inverted and pos_outcome.upper() == "NO":
        last_prices = [float(h.get("p") or 0) for h in history[-5:] if float(h.get("p") or 0) > 0]
        log.info(f"[INVERT-LOST] {pos_title[:40]}: forcing inversion for lost No position "
                 f"(last_prices={[f'{p:.2f}' for p in last_prices[-3:]]})")
        for h in history:
            p = float(h.get("p") or 0)
            if p > 0:
                h["p"] = str(1.0 - p)
        history_inverted = True

    if "280-299" in pos_title:
        log.info(f"[DEBUG2 280-299] outcome={pos_outcome} tid={token_id[:20]}.. cid={condition_id[:20]}.. "
                 f"history={len(history)} gamma={gamma_price} buys={len(buys)} avg={avg_entry:.4f}")

    if "Lebanon" in pos_title:
        fh = history[0] if history else {}
        lh = history[-1] if history else {}
        log.info(f"[DBG-LB2] history={len(history)} inverted={history_inverted} "
                 f"first=({fmt_ts(normalize_ts(fh.get('t',0)))},{float(fh.get('p',0)):.3f}) "
                 f"last=({fmt_ts(normalize_ts(lh.get('t',0)))},{float(lh.get('p',0)):.3f}) "
                 f"first_buy={fmt_ts(first_buy)} token_id={token_id} cid={condition_id}")

    if not history and gamma_price is None:
        # Last fallback: use curPrice from position data.
        # curPrice is already the price of the held outcome (e.g. the No price for a No
        # position), so it must NOT be inverted.
        cur_price = float(pos.get("curPrice") or 0)
        if cur_price > 0:
            gamma_price = cur_price
            log.info(f"[FALLBACK] {pos_title[:40]}: using curPrice={cur_price} as gamma (outcome={pos_outcome})")
        else:
            log.info(f"[SKIP] {pos_title[:40]}: no history, no gamma, no curPrice")
            return None

    # Fallback max_simul from position data
    pos_size = max(float(pos.get("size") or 0), float(pos.get("totalBought") or 0))
    if pos_size > max_simul:
        max_simul = pos_size

    # 1) ATH while holding
    best_during, best_during_price, best_during_ts, price_pts = \
        calc_timeline_ath(buys, sells or [], history, first_buy, last_sell)

    # 2) ATH after full exit
    total_sold = sum(float(t.get("size") or 0) for t in (sells or []))
    fully_exited = last_sell > 0 and (total_shares - total_sold) < 0.01
    if not fully_exited:
        cur_size = float(pos.get("size") or pos.get("currentValue") or -1)
        if cur_size == 0 and last_sell > 0:
            fully_exited = True

    best_after = best_after_price = 0.0
    best_after_ts = 0
    if fully_exited and max_simul > 0:
        best_after, best_after_price, best_after_ts = \
            calc_post_exit_ath(history, last_sell, max_simul, gamma_price)

    # Choose best
    if best_after > best_during:
        ath_price, ath_ts, value_at_ath = best_after_price, best_after_ts, best_after
    else:
        ath_price, ath_ts, value_at_ath = best_during_price, best_during_ts, best_during

    # Gamma fallback for still-holding (skip if position confirmed lost)
    if gamma_price is not None and not fully_exited and not position_lost:
        net_rem = max(0, total_shares - total_sold)
        gv = net_rem * gamma_price
        if gv > value_at_ath:
            value_at_ath = gv
            ath_price = gamma_price
            ath_ts = 0
            for h in history:
                if float(h.get("p") or 0) >= gamma_price * 0.99:
                    ath_ts = normalize_ts(h.get("t") or 0)
                    break
            if ath_ts == 0 and last_sell > 0:
                ath_ts = last_sell

    # Last-resort fallback
    if value_at_ath <= 0 and history:
        for h in history:
            t_ts = normalize_ts(h.get("t") or 0)
            p = float(h.get("p") or 0)
            if p > ath_price and (first_buy == 0 or t_ts >= first_buy):
                ath_price = p
                ath_ts = t_ts
        if ath_price > 0:
            value_at_ath = max_simul * ath_price

    # Check curPrice — may be higher than ATH from history (e.g. resolved at $1)
    # Skip for position_lost, or if curPrice_raw=0 (no data from API)
    cur_price_raw_2 = float(pos.get("curPrice") or 0)
    cur_price = cur_price_raw_2  # curPrice is already the held-outcome price — no inversion
    if "Lebanon" in pos_title:
        log.info(f"[DBG-LB] ath_price={ath_price:.4f} val={value_at_ath:.2f} cur_price={cur_price:.4f} position_lost={position_lost} best_during={best_during:.2f} best_after={best_after:.2f}")
    if not position_lost and cur_price_raw_2 > 0 and cur_price > 0 and max_simul > 0:
        cur_value = max_simul * cur_price
        if cur_value > value_at_ath and cur_price > avg_entry:
            ath_price = cur_price
            value_at_ath = cur_value
            if ath_ts == 0:
                ath_ts = normalize_ts(pos.get("timestamp") or 0) or last_sell

    if ath_price <= 0 or value_at_ath <= 0:
        log.info(f"[SKIP] {pos_title[:40]}: ath_price={ath_price:.4f} val={value_at_ath:.2f}")
        return None
    if ath_price <= avg_entry:
        log.info(f"[SKIP] {pos_title[:40]}: ATH {ath_price:.4f} <= entry {avg_entry:.4f}")
        return None

    ath_mult = round(ath_price / avg_entry, 1) if avg_entry > 0 else 0

    # What user actually received
    sells_val = sum(float(t.get("size") or 0) * float(t.get("price") or 0) for t in (sells or []))
    net_remaining = total_shares - total_sold
    # For lost positions, gamma_price may reflect the winning (YES) side — don't use it as final_check
    final_check = gamma_price if not position_lost else None
    if final_check is None and history:
        final_check = float(history[-1].get("p") or 0)

    # Also check curPrice from position — it may indicate resolution ($1).
    # curPrice is the price of the held outcome, so it is used directly (no inversion).
    cur_price_pos = float(pos.get("curPrice") or 0)
    if not position_lost and cur_price_pos > 0 and (final_check is None or cur_price_pos > final_check):
        final_check = cur_price_pos

    resolution_received = 0.0
    if position_lost:
        # Position lost — user received $0 from resolution
        resolution_received = 0.0
    elif final_check is not None and final_check >= 0.95 and net_remaining > 0.01:
        resolution_received = net_remaining * 1.0

    # Value of shares STILL HELD in an open (unresolved) position, marked to market.
    # curPrice is the current price of the held outcome, i.e. what these shares could be
    # sold for right now. Without this, an open position counts as $0 received and looks
    # like a total loss (P&L = -spent, missed = full ATH value), which is wrong.
    open_value = 0.0
    cur_price_held = float(pos.get("curPrice") or 0)
    if not position_lost and resolution_received == 0.0 and net_remaining > 0.01 and cur_price_held > 0:
        open_value = net_remaining * cur_price_held

    total_received = sells_val + resolution_received + open_value
    actual_pnl = total_received - total_spent
    missed = max(0.0, value_at_ath - total_received)

    if "280-299" in pos_title:
        log.info(f"[DEBUG 280-299] val_ath=${value_at_ath:.2f} sells_val=${sells_val:.2f} "
                 f"res_recv=${resolution_received:.2f} total_recv=${total_received:.2f} "
                 f"missed=${missed:.2f} shares={total_shares:.0f} max_simul={max_simul:.0f} "
                 f"ath_price={ath_price:.4f} avg={avg_entry:.4f} exited={fully_exited}")

    if missed < 0.5:
        log.info(f"[SKIP] {pos_title[:40]}: missed ${missed:.2f} < $0.50")
        return None

    return {
        "title":        get_title(pos, market),
        "url":          build_event_url(pos, market),
        "avg_entry":    avg_entry,
        "ath_price":    ath_price,
        "ath_mult":     ath_mult,
        "shares":       total_shares,
        "spent":        total_spent,
        "value_at_ath": value_at_ath,
        "missed":       missed,
        "actual_pnl":   actual_pnl,
        "buy_date":     fmt_ts(first_buy) if first_buy else fmt_ts(normalize_ts(pos.get("timestamp") or 0)),
        "buy_date_ts":  first_buy if first_buy else normalize_ts(pos.get("timestamp") or 0),
        "ath_date":     fmt_ts(ath_ts),
        "ath_date_ts":  ath_ts,
        "conditionId":  condition_id,
        "asset":        token_id,
        "outcome_side": pos_outcome,
    }


def build_ath_page(results, wallet, total_missed, page):
    total_pages = max(1, (len(results) + PAGE_SIZE - 1) // PAGE_SIZE)
    chunk = results[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    nl = "\n"
    text = (
        f"📈 ATH Analysis for <code>{wallet[:10]}...</code>{nl}"
        f"Positions: {len(results)} | Total missed: ~${total_missed:.2f}{nl}"
        f"Page {page + 1}/{total_pages}{nl}"
        f"─── Missed upside ───{nl}{nl}"
    )
    for i, r in enumerate(chunk, page * PAGE_SIZE + 1):
        pnl = r["actual_pnl"]
        pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        side = f" [{r.get('outcome_side', '')}]" if r.get("outcome_side") else ""
        text += (
            f"{i}. {r['title']}{side}{nl}"
            f"   Entry: {r['avg_entry']*100:.1f}¢ → ATH: {r['ath_price']*100:.1f}¢ (x{r['ath_mult']}){nl}"
            f"   Shares: {r['shares']:.0f} | Spent: ${r['spent']:.2f}{nl}"
            f"   Value at ATH: ${r['value_at_ath']:.2f} | P&L: {pnl_str}{nl}"
            f"   Missed: ~${r['missed']:.2f}{nl}"
            f"   🕐 Bought: {r['buy_date']}{nl}"
            f"   📈 ATH on: {r['ath_date']}"
            + (f"{nl}   <a href=\"{r['url']}?via=Xview\">market link</a>" if r["url"] else "")
            + f"{nl}{nl}"
        )
    return text, total_pages


def send_ath_page(chat_id, session_id, results, wallet, total_missed, page, msg_id=None):
    text, total_pages = build_ath_page(results, wallet, total_missed, page)
    kb = page_keyboard(page, total_pages, session_id)
    if msg_id:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText",
                json={"chat_id": chat_id, "message_id": msg_id, "text": text,
                      "parse_mode": "HTML", "disable_web_page_preview": True,
                      "reply_markup": json.dumps(kb)}, timeout=15)
        except Exception:
            send_message(chat_id, text, reply_markup=kb)
    else:
        resp = send_message(chat_id, text, reply_markup=kb)
        new_id = (resp.get("result") or {}).get("message_id") if isinstance(resp, dict) else None
        if new_id:
            with state_lock:
                st = user_state.get(chat_id, {})
                if st.get("session_id") == session_id:
                    st["page_msg_id"] = new_id


def analyze_ath(wallet, chat_id):
    try:
        send_message(chat_id, "🔍 Fetching all positions...")
        positions = get_all_positions(wallet)
        if not positions:
            send_message(chat_id, "❌ No positions found.")
            return

        positions = filter_hedged(positions)
        total = len(positions)

        # Cache: reuse resolved positions whose fingerprint is unchanged, compute the rest.
        reused_results, to_compute, use_cache = cache_split(wallet, "ath", positions)
        if use_cache:
            send_message(chat_id, f"📊 Found {total} positions. {total - len(to_compute)} from cache, "
                                  f"analyzing {len(to_compute)} new/changed...")
        else:
            send_message(chat_id, f"📊 Found {total} positions. Analyzing ATH...")

        outcomes = {}
        oc_lock = threading.Lock()

        def ath_worker(pos):
            r = process_ath(wallet, pos)
            with oc_lock:
                outcomes[_pos_key(pos)] = r
            return r

        fresh = run_with_progress(chat_id, to_compute, ath_worker, "Analyzing") if to_compute else []

        if use_cache:
            cache_store(wallet, "ath", cache_collect_rows(to_compute, outcomes))

        results = reused_results + fresh

        log.info(f"ATH analysis done: {len(results)} results "
                 f"({len(reused_results)} cached, {len(fresh)} fresh)")

        if not results:
            send_message(chat_id, "❌ No missed profit found.")
            return

        results.sort(key=lambda x: x["missed"], reverse=True)
        total_missed = sum(r["missed"] for r in results)

        log.info(f"ATH results: {len(results)} positions, total missed: ${total_missed:.2f}")

        sid = uuid.uuid4().hex[:8]
        with state_lock:
            user_state[chat_id] = {
                "step": "paging_ath", "session_id": sid,
                "results": results, "wallet": wallet, "total_missed": total_missed,
            }
        send_ath_page(chat_id, sid, results, wallet, total_missed, 0)
    except Exception as e:
        log.error(f"analyze_ath crashed: {e}", exc_info=True)
        send_message(chat_id, f"❌ Analysis error: {e}")


# ═════════════════════════════════════════════
#  MODE 2 — RESOLUTION ANALYSIS
# ═════════════════════════════════════════════

def process_resolution(wallet, pos):
    """Analyze a single position for resolution missed profit."""
    condition_id = pos.get("conditionId") or pos.get("market") or ""
    token_id = pos.get("asset") or ""
    if not condition_id and not token_id:
        return None

    buys, sells, pos_outcome, full_cid = extract_buys_sells(wallet, pos)
    if buys is None:
        return None

    if len(full_cid) > len(condition_id):
        condition_id = full_cid

    # Update token_id from trades — /closed-positions may truncate IDs
    for t in buys:
        t_asset = t.get("asset") or ""
        if len(t_asset) > len(token_id):
            token_id = t_asset
            break

    total_shares, total_spent, avg_entry, max_simul = calc_shares_stats(buys, sells or [])

    # Fallback max_simul from position data
    pos_size = max(float(pos.get("size") or 0), float(pos.get("totalBought") or 0))
    if pos_size > max_simul:
        max_simul = pos_size

    history = get_price_history(token_id, condition_id) if token_id else []
    market = get_market_info(condition_id) if condition_id else None
    gamma = get_token_final_price(market, token_id) if market and token_id else None

    # TOKEN FIX removed — pos.get("asset") is already the correct Yes/No token

    if gamma is not None:
        final_price = gamma
    elif history:
        final_price = float(history[-1].get("p") or 0)
    else:
        cur_price = float(pos.get("curPrice") or 0)
        pos_out = pos.get("outcome") or ""
        if pos_out.upper() == "NO":
            cur_price = 1.0 - cur_price
        if cur_price > 0:
            final_price = cur_price
        else:
            return None

    # For NO positions: gamma/history may be for the YES token — invert
    pos_out_upper = (pos.get("outcome") or "").upper()
    if pos_out_upper == "NO":
        # Use curPrice as ground truth: curPrice is always YES price
        cur_price_raw = float(pos.get("curPrice") or 0)
        if cur_price_raw > 0:
            # final_price should be NO price = 1 - YES price
            final_price = 1.0 - cur_price_raw

    if final_price >= 0.95:
        outcome = "WON"
        max_return = max_simul * 1.0
        total_received = sum(float(t.get("size") or 0) * float(t.get("price") or 0) for t in (sells or []))
        missed = max(0.0, max_return - total_received)
    elif final_price <= 0.05:
        outcome = "LOST"
        max_return = missed = 0.0
    else:
        outcome = "UNRESOLVED"
        max_return = max_simul * final_price
        missed = 0.0

    return {
        "title": get_title(pos, market), "url": build_event_url(pos, market),
        "outcome": outcome, "avg_entry": avg_entry, "shares": max_simul,
        "spent": total_spent, "max_return": max_return, "missed": missed,
    }


def build_res_page(results, wallet, missed_total, won, lost, total_count, page):
    total_pages = max(1, (len(results) + PAGE_SIZE - 1) // PAGE_SIZE)
    chunk = results[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    nl = "\n"
    text = (
        f"✅ Resolution Analysis for <code>{wallet[:10]}...</code>{nl}"
        f"Total: {total_count} | Won: {won} | Lost: {lost}{nl}"
        f"💰 Total missed profit: ~${missed_total:.2f}{nl}"
        f"Page {page + 1}/{total_pages}{nl}"
        f"─── Winning positions ───{nl}{nl}"
    )
    for i, r in enumerate(chunk, page * PAGE_SIZE + 1):
        text += (
            f"{i}. ✅ {r['title']}{nl}"
            f"   Bought: {r['avg_entry']*100:.1f}¢ | Shares: {r['shares']:.0f}{nl}"
            f"   Spent: ${r['spent']:.2f} | Max return: ${r['max_return']:.2f}{nl}"
            f"   Profit if held: ~+${r['missed']:.2f}"
            + (f"{nl}   <a href=\"{r['url']}?via=Xview\">market link</a>" if r["url"] else "")
            + f"{nl}{nl}"
        )
    return text, total_pages


def send_res_page(chat_id, session_id, results, wallet, missed_total, won, lost, total_count, page, msg_id=None):
    text, total_pages = build_res_page(results, wallet, missed_total, won, lost, total_count, page)
    kb = page_keyboard(page, total_pages, session_id)
    if msg_id:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText",
                json={"chat_id": chat_id, "message_id": msg_id, "text": text,
                      "parse_mode": "HTML", "disable_web_page_preview": True,
                      "reply_markup": json.dumps(kb)}, timeout=15)
        except Exception:
            send_message(chat_id, text, reply_markup=kb)
    else:
        resp = send_message(chat_id, text, reply_markup=kb)
        new_id = (resp.get("result") or {}).get("message_id") if isinstance(resp, dict) else None
        if new_id:
            with state_lock:
                st = user_state.get(chat_id, {})
                if st.get("session_id") == session_id:
                    st["page_msg_id"] = new_id


def analyze_resolution(wallet, chat_id):
    try:
        send_message(chat_id, "🔍 Fetching all positions...")
        positions = get_all_positions(wallet)
        if not positions:
            send_message(chat_id, "❌ No positions found.")
            return

        positions = filter_hedged(positions)
        total = len(positions)

        # Cache: reuse resolved positions whose fingerprint is unchanged, compute the rest.
        reused_results, to_compute, use_cache = cache_split(wallet, "res", positions)
        if use_cache:
            send_message(chat_id, f"📊 Found {total} positions. {total - len(to_compute)} from cache, "
                                  f"checking {len(to_compute)} new/changed...")
        else:
            send_message(chat_id, f"📊 Found {total} positions. Checking resolutions...")

        outcomes = {}
        oc_lock = threading.Lock()

        def res_worker(pos):
            r = process_resolution(wallet, pos)
            with oc_lock:
                outcomes[_pos_key(pos)] = r
            return r

        fresh = run_with_progress(chat_id, to_compute, res_worker, "Analyzing") if to_compute else []

        if use_cache:
            cache_store(wallet, "res", cache_collect_rows(to_compute, outcomes))

        results = reused_results + fresh
        won = [r for r in results if r.get("outcome") == "WON"]
        lost_list = [r for r in results if r.get("outcome") == "LOST"]
        won_count = {"val": len(won)}
        lost_count = {"val": len(lost_list)}

        if not won:
            send_message(chat_id, "❌ No winning positions with missed profit found.")
            return

        won.sort(key=lambda x: x["missed"], reverse=True)
        missed_total = sum(r["missed"] for r in won)

        log.info(f"Resolution results: {len(won)} won, {len(lost_list)} lost, total missed: ${missed_total:.2f}")

        sid = uuid.uuid4().hex[:8]
        with state_lock:
            user_state[chat_id] = {
                "step": "paging_res", "session_id": sid,
                "results": won, "wallet": wallet, "total_missed": missed_total,
                "won_count": won_count["val"], "lost_count": lost_count["val"],
                "total_count": len(results),
            }
        send_res_page(chat_id, sid, won, wallet, missed_total,
                      won_count["val"], lost_count["val"], len(results), 0)
    except Exception as e:
        log.error(f"analyze_resolution crashed: {e}", exc_info=True)
        send_message(chat_id, f"❌ Analysis error: {e}")


# ═════════════════════════════════════════════
#  DETAIL VIEW — single position deep dive
# ═════════════════════════════════════════════

def analyze_detail(chat_id, wallet, result):
    condition_id = result.get("conditionId") or ""
    token_id = result.get("asset") or ""
    title = result.get("title") or "Unknown"
    pos_outcome = result.get("outcome_side") or ""

    send_typing(chat_id)

    trades = get_trades(wallet, condition_id) if condition_id else []
    trades = filter_trades_by_token(trades, token_id, pos_outcome)

    buys = sorted([t for t in trades if (t.get("side") or "").upper() == "BUY"],
                  key=lambda t: normalize_ts(t.get("timestamp") or 0))
    sells = sorted([t for t in trades if (t.get("side") or "").upper() == "SELL"],
                   key=lambda t: normalize_ts(t.get("timestamp") or 0))

    all_ops = []
    for t in buys:
        all_ops.append(("BUY", normalize_ts(t.get("timestamp") or 0),
                        float(t.get("size") or 0), float(t.get("price") or 0)))
    for t in sells:
        all_ops.append(("SELL", normalize_ts(t.get("timestamp") or 0),
                        float(t.get("size") or 0), float(t.get("price") or 0)))
    all_ops.sort(key=lambda x: x[1])

    # Find ATH shares for star marker
    ath_shares = round(result["value_at_ath"] / result["ath_price"], 1) if result.get("ath_price", 0) > 0 else 0
    running_check = 0.0
    ath_op_idx = -1
    for i, (side, ts, size, price) in enumerate(all_ops):
        if side == "BUY":
            running_check += size
        else:
            running_check -= size
            if running_check < 0:
                running_check = 0
        if abs(running_check - ath_shares) < 0.5:
            ath_op_idx = i

    nl = "\n"
    text = f"🔍 <b>{title}</b>{nl}{nl}─── Operations ───{nl}"

    if not all_ops:
        # No trades found — show position data
        text += f"⚠️ No trade details available from API{nl}"
        text += f"🟢 BUY | {result.get('buy_date', 'unknown')} | {result['shares']:.1f} @ {result['avg_entry']*100:.1f}¢ | Total: {result['shares']:.1f} shares{nl}"
    else:
        running = 0.0
        for i, (side, ts, size, price) in enumerate(all_ops):
            if side == "BUY":
                running += size
                emoji = "🟢"
            else:
                running -= size
                if running < 0:
                    running = 0
                emoji = "🔴"
            if i == ath_op_idx:
                emoji = "⭐"
            text += f"{emoji} {side} | {fmt_ts(ts)} | {size:.1f} @ {price*100:.1f}¢ | Total: {running:.1f} shares{nl}"

    text += (
        f"{nl}📊 Avg entry: {result['avg_entry']*100:.1f}¢ | Total spent: ${result['spent']:.2f}{nl}"
        f"📈 ATH price: {result['ath_price']*100:.1f}¢ | Value at ATH: ${result['value_at_ath']:.2f}{nl}"
        f"🕐 ATH on: {result.get('ath_date', 'unknown')}"
        f" | Shares at ATH: {ath_shares:.0f}{nl}"
        f"💰 Missed: ~${result['missed']:.2f}"
    )
    send_message(chat_id, text)

    # ── Chart ────────────────────────────────────────────────────────────
    try:
        history = get_price_history(token_id, condition_id) if token_id else []
        if not history:
            log.info(f"[CHART] No price history for {title[:40]} tid={token_id[:20]}.. cid={condition_id[:20]}..")
            return

        # First buy timestamp — chart starts here
        if all_ops:
            first_buy_ts = all_ops[0][1]
        else:
            first_buy_ts = normalize_ts(result.get("buy_date_ts") or 0)

        # Build combined price line: API history + trade points + ATH point
        price_line = []  # [(ts, price)]
        seen_ts = set()

        # 1) API price history
        for h in history:
            t_ts = normalize_ts(h.get("t") or 0)
            p = float(h.get("p") or 0)
            if p > 0 and t_ts > 0:
                price_line.append((t_ts, p))
                seen_ts.add(t_ts)

        # 2) Insert trade points with their actual prices
        for side, ts, size, price in all_ops:
            if ts > 0 and price > 0 and ts not in seen_ts:
                price_line.append((ts, price))
                seen_ts.add(ts)

        # 3) Insert ATH point
        ath_ts = result.get("ath_date_ts") or 0
        ath_price = result.get("ath_price", 0)
        if ath_ts > 0 and ath_price > 0 and ath_ts not in seen_ts:
            price_line.append((ath_ts, ath_price))
            seen_ts.add(ath_ts)

        price_line.sort(key=lambda x: x[0])

        # Filter: start from first buy
        if first_buy_ts > 0:
            filtered = [(ts, p) for ts, p in price_line if ts >= first_buy_ts]
            if filtered:
                price_line = filtered

        if not price_line:
            return

        # Build shares step function from all_ops
        trade_steps = []  # [(ts, running_shares)]
        running = 0.0
        for side, ts, size, price in all_ops:
            if side == "BUY":
                running += size
            else:
                running -= size
                if running < 0:
                    running = 0
            trade_steps.append((ts, running))

        if not trade_steps and result.get("shares"):
            trade_steps = [(first_buy_ts, float(result.get("shares") or 0))]

        def get_shares_at(ts):
            cur = 0.0
            for t_ts, sh in trade_steps:
                if t_ts <= ts:
                    cur = sh
                else:
                    break
            return cur

        # Build arrays
        price_times = [datetime.fromtimestamp(ts, tz=timezone.utc) for ts, _ in price_line]
        price_vals = [p * 100 for _, p in price_line]
        shares_tl = [get_shares_at(ts) for ts, _ in price_line]

        # Find ATH index on chart
        ath_value = result.get("value_at_ath", 0)
        ath_price_val = ath_price * 100
        ath_shares_count = round(ath_value / ath_price, 0) if ath_price > 0 else 0

        best_pi = 0
        if ath_ts > 0:
            min_diff = float("inf")
            for i, (ts, _) in enumerate(price_line):
                diff = abs(ts - ath_ts)
                if diff < min_diff:
                    min_diff = diff
                    best_pi = i
        else:
            best_pv = 0
            for i, (ts, p) in enumerate(price_line):
                pv = get_shares_at(ts) * p
                if pv > best_pv:
                    best_pv = pv
                    best_pi = i

        # ── Plot ──
        fig, ax1 = plt.subplots(figsize=(10, 5))
        fig.patch.set_facecolor("#1a1a2e")
        ax1.set_facecolor("#1a1a2e")

        ax1.plot(price_times, price_vals, color="#00d4ff", linewidth=1.5)
        ax1.set_ylabel("Price (¢)", color="#00d4ff", fontsize=11)
        ax1.tick_params(axis="y", labelcolor="#00d4ff")

        ax2 = ax1.twinx()
        ax2.fill_between(price_times, shares_tl, alpha=0.25, color="#4ade80", step="post")
        ax2.step(price_times, shares_tl, color="#4ade80", linewidth=1, alpha=0.7, where="post")
        ax2.set_ylabel("Shares", color="#4ade80", fontsize=11)
        ax2.tick_params(axis="y", labelcolor="#4ade80")
        ax2.set_ylim(bottom=0)

        # ATH marker — use actual ATH price from result, not from price_line
        if best_pi < len(price_times):
            ax1.scatter([price_times[best_pi]], [ath_price_val],
                        color="#ff6b6b", s=120, zorder=5, edgecolors="white", linewidths=1.5)
            ax1.annotate(
                f"ATH ${ath_value:.2f}\n{ath_shares_count:.0f} shares @ {ath_price_val:.1f}¢",
                xy=(price_times[best_pi], ath_price_val),
                xytext=(-15, -45), textcoords="offset points", fontsize=8, color="white",
                bbox=dict(boxstyle="round,pad=0.3", facecolor="#ff6b6b", alpha=0.85),
                arrowprops=dict(arrowstyle="->", color="white"), ha="center")

        # Buy/Sell markers at their actual prices
        for side, ts, size, price in all_ops:
            t_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            color = "#4ade80" if side == "BUY" else "#ff6b6b"
            marker = "^" if side == "BUY" else "v"
            ax1.scatter([t_dt], [price * 100], color=color, s=60, zorder=6,
                        marker=marker, edgecolors="white", linewidths=0.5)

        ax1.set_title(title[:60], color="white", fontsize=12, pad=15)

        # Adaptive date format
        if len(price_times) >= 2:
            span = (price_times[-1] - price_times[0]).total_seconds() / 86400
            if span < 0.01:
                ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            elif span <= 3:
                ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %d %H:%M"))
                ax1.xaxis.set_major_locator(mdates.HourLocator(interval=max(1, int(span * 24 / 6))))
            elif span <= 30:
                ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
                ax1.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, int(span / 6))))
            else:
                ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        else:
            ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %d %H:%M"))
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=25, ha="right")
        ax1.tick_params(axis="x", colors="white")

        for ax in (ax1, ax2):
            ax.spines["top"].set_visible(False)
            for s in ("bottom", "left", "right"):
                ax.spines[s].set_color("#444")

        fig.tight_layout(rect=[0, 0, 1, 0.93])
        path = f"/tmp/chart_{chat_id}_{uuid.uuid4().hex[:6]}.png"
        fig.savefig(path, dpi=130, facecolor=fig.get_facecolor())
        plt.close(fig)
        send_photo(chat_id, path)
    except Exception as e:
        log.error(f"[CHART] Error building chart for {title[:40]}: {e}")
        try:
            plt.close("all")
        except Exception:
            pass


# ═════════════════════════════════════════════
#  LOOKUP — single position raw data
# ═════════════════════════════════════════════

def search_markets(query: str) -> list:
    """Search markets by name via Gamma API."""
    # 1) Try /public-search (text search)
    try:
        r = requests.get(f"{GAMMA_API}/public-search",
                         params={"query": query, "limit": 10}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            # May return {markets: [...], events: [...]} or just a list
            if isinstance(data, dict):
                markets = data.get("markets") or []
                events = data.get("events") or []
                # Extract markets from events
                for ev in events:
                    for m in (ev.get("markets") or []):
                        m["_event_title"] = ev.get("title") or ""
                        markets.append(m)
                if markets:
                    return markets
            elif isinstance(data, list) and data:
                return data
    except Exception as e:
        log.error(f"search_markets public-search error: {e}")

    # 2) Try /events?slug_filter
    try:
        slug_query = query.lower().replace(",", "").replace(" ", "-")[:50]
        r = requests.get(f"{GAMMA_API}/events",
                         params={"slug_filter": slug_query, "limit": 10}, timeout=10)
        if r.status_code == 200:
            events = r.json()
            if isinstance(events, list) and events:
                results = []
                for ev in events:
                    for m in (ev.get("markets") or []):
                        m["_event_title"] = ev.get("title") or ""
                        results.append(m)
                if results:
                    return results
    except Exception as e:
        log.error(f"search_markets events error: {e}")

    # 3) Try /markets?slug_filter
    try:
        slug_query = query.lower().replace(",", "").replace(" ", "-")[:50]
        r = requests.get(f"{GAMMA_API}/markets",
                         params={"slug_filter": slug_query, "limit": 10}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                return data
    except Exception as e:
        log.error(f"search_markets slug error: {e}")

    return []


def handle_pos(chat_id, args):
    """Show all trades for a wallet + Polymarket event URL or search text."""
    try:
        parts = args.strip().split(maxsplit=1)
        if len(parts) < 2:
            send_message(chat_id,
                "Usage: /pos 0xWALLET polymarket_link\n"
                "Example: /pos 0x1234... https://polymarket.com/event/ethereum-up-or-down-february-20-3am-et")
            return

        wallet = parts[0]
        query = parts[1].strip()

        if not wallet.startswith("0x") or len(wallet) < 10:
            send_message(chat_id, "❌ Invalid wallet address.")
            return

        send_typing(chat_id)

        condition_id = None
        title = None

        # 1) Direct conditionId (starts with 0x, long hex)
        if query.startswith("0x") and len(query) > 20 and " " not in query:
            condition_id = query
            market = get_market_info(condition_id)
            title = (market.get("question") or condition_id[:20]) if market else condition_id[:20]

        # 2) Polymarket URL — extract slug
        elif "polymarket.com" in query:
            # Extract slug from URL like https://polymarket.com/event/slug-here or /event/slug-here?tid=...
            slug = ""
            market_slug = ""
            if "/event/" in query:
                path = query.split("/event/")[-1].split("?")[0].split("#")[0].strip("/")
                # URL can be /event/event-slug or /event/event-slug/market-slug
                path_parts = path.split("/")
                slug = path_parts[0] if path_parts else ""
                market_slug = path_parts[1] if len(path_parts) > 1 else ""
            elif "/market/" in query:
                slug = query.split("/market/")[-1].split("?")[0].split("#")[0].strip("/")
                market_slug = slug

            if not slug:
                send_message(chat_id, "❌ Could not extract slug from URL.")
                return

            log.info(f"[POS] Extracted slug: {slug}, market_slug: {market_slug}")

            # Try /events?slug=
            found_markets = []
            try:
                r = requests.get(f"{GAMMA_API}/events", params={"slug": slug, "limit": 1}, timeout=10)
                if r.status_code == 200:
                    events = r.json()
                    if isinstance(events, list) and events:
                        ev = events[0]
                        title = ev.get("title") or ""
                        for m in (ev.get("markets") or []):
                            found_markets.append(m)
            except Exception as e:
                log.error(f"[POS] events slug error: {e}")

            # If market_slug provided, try to narrow down to specific market
            if market_slug and found_markets:
                specific = [m for m in found_markets if m.get("slug") == market_slug]
                if specific:
                    found_markets = specific

            if not found_markets:
                # Try /markets?slug= with market_slug or event slug
                for try_slug in ([market_slug, slug] if market_slug else [slug]):
                    if not try_slug:
                        continue
                    try:
                        r = requests.get(f"{GAMMA_API}/markets", params={"slug": try_slug, "limit": 5}, timeout=10)
                        if r.status_code == 200:
                            data = r.json()
                            if isinstance(data, list) and data:
                                found_markets = data
                                break
                    except Exception as e:
                        log.error(f"[POS] markets slug error: {e}")

            if not found_markets:
                send_message(chat_id, f"❌ No market found for slug: {slug}")
                return

            # If multiple markets (multi-outcome event) — show all
            if len(found_markets) == 1:
                condition_id = found_markets[0].get("conditionId") or ""
                title = found_markets[0].get("question") or title or condition_id[:20]
            else:
                # Show trades for ALL markets in this event
                nl = "\n"
                event_title = title or slug
                text = f"🔍 <b>{event_title}</b>{nl}"
                text += f"Wallet: <code>{wallet[:10]}...</code>{nl}"
                text += f"Markets in event: {len(found_markets)}{nl}{nl}"

                any_trades = False
                for m in found_markets:
                    cid = m.get("conditionId") or ""
                    q = m.get("question") or cid[:20]
                    if not cid:
                        continue

                    raw_trades = get_trades(wallet, cid)
                    if not raw_trades:
                        continue

                    any_trades = True
                    text += f"📊 <b>{q}</b>{nl}"
                    text += f"ConditionId: <code>{cid[:20]}...</code>{nl}"

                    running_by_outcome = {}
                    for t in sorted(raw_trades, key=lambda x: normalize_ts(x.get("timestamp") or 0)):
                        side = (t.get("side") or "?").upper()
                        size = float(t.get("size") or 0)
                        price = float(t.get("price") or 0)
                        ts = normalize_ts(t.get("timestamp") or 0)
                        outcome = t.get("outcome") or "?"
                        t_type = t.get("type") or "TRADE"

                        if outcome not in running_by_outcome:
                            running_by_outcome[outcome] = 0.0
                        if side == "BUY":
                            running_by_outcome[outcome] += size
                            emoji = "🟢"
                        elif side == "SELL":
                            running_by_outcome[outcome] -= size
                            if running_by_outcome[outcome] < 0:
                                running_by_outcome[outcome] = 0
                            emoji = "🔴"
                        else:
                            emoji = "⚪"

                        type_tag = f" [{t_type}]" if t_type != "TRADE" else ""
                        text += (f"{emoji} {side} | {fmt_ts(ts)} | {size:.1f} @ {price*100:.1f}¢ | "
                                 f"{outcome} | Total: {running_by_outcome[outcome]:.1f}{type_tag}{nl}")

                    text += nl

                if not any_trades:
                    text += "⚠️ No trades found for this wallet on any market in this event."

                send_message(chat_id, text)
                return

        # 3) Text search fallback
        else:
            markets = search_markets(query)
            log.info(f"[POS] Search '{query}' returned {len(markets)} results")

            if not markets:
                send_message(chat_id, f"❌ No markets found for \"{query}\"")
                return

            search_lower = query.lower()
            best = None
            for m in markets:
                q = (m.get("question") or "").lower()
                words = [w for w in search_lower.split() if len(w) > 2]
                if words and all(w in q for w in words):
                    best = m
                    break
            if not best:
                nl = "\n"
                text = f"⚠️ No exact match for \"{query}\".{nl}Found:{nl}"
                for i, m in enumerate(markets[:5], 1):
                    text += f"{i}. {m.get('question', '?')}{nl}"
                text += f"{nl}Try using a Polymarket link instead."
                send_message(chat_id, text)
                return

            condition_id = best.get("conditionId") or best.get("condition_id") or ""
            title = best.get("question") or condition_id[:20]

        if not condition_id:
            send_message(chat_id, "❌ Could not find conditionId.")
            return

        # Fetch trades for single market
        raw_trades = get_trades(wallet, condition_id)

        nl = "\n"
        text = f"🔍 <b>{title}</b>{nl}"
        text += f"Wallet: <code>{wallet[:10]}...</code>{nl}"
        text += f"ConditionId: <code>{condition_id[:20]}...</code>{nl}{nl}"

        if not raw_trades:
            text += "⚠️ No trades found for this wallet on this market."
            send_message(chat_id, text)
            return

        outcomes = {}
        for t in raw_trades:
            out = t.get("outcome") or "Unknown"
            outcomes.setdefault(out, []).append(t)

        text += f"Total trades: {len(raw_trades)} | Outcomes: {', '.join(outcomes.keys())}{nl}"
        text += f"─── Trades ───{nl}"

        running_by_outcome = {}
        for t in sorted(raw_trades, key=lambda x: normalize_ts(x.get("timestamp") or 0)):
            side = (t.get("side") or "?").upper()
            size = float(t.get("size") or 0)
            price = float(t.get("price") or 0)
            ts = normalize_ts(t.get("timestamp") or 0)
            outcome = t.get("outcome") or "?"
            t_type = t.get("type") or "TRADE"

            if outcome not in running_by_outcome:
                running_by_outcome[outcome] = 0.0
            if side == "BUY":
                running_by_outcome[outcome] += size
                emoji = "🟢"
            elif side == "SELL":
                running_by_outcome[outcome] -= size
                if running_by_outcome[outcome] < 0:
                    running_by_outcome[outcome] = 0
                emoji = "🔴"
            else:
                emoji = "⚪"

            type_tag = f" [{t_type}]" if t_type != "TRADE" else ""
            text += (f"{emoji} {side} | {fmt_ts(ts)} | {size:.1f} @ {price*100:.1f}¢ | "
                     f"{outcome} | Total: {running_by_outcome[outcome]:.1f}{type_tag}{nl}")

        text += f"{nl}─── Summary ───{nl}"
        for out, trades_list in outcomes.items():
            buys = [t for t in trades_list if (t.get("side") or "").upper() == "BUY"]
            sells = [t for t in trades_list if (t.get("side") or "").upper() == "SELL"]
            total_bought = sum(float(t.get("size") or 0) for t in buys)
            total_sold = sum(float(t.get("size") or 0) for t in sells)
            spent = sum(float(t.get("size") or 0) * float(t.get("price") or 0) for t in buys)
            received = sum(float(t.get("size") or 0) * float(t.get("price") or 0) for t in sells)
            text += (f"{out}: bought {total_bought:.1f} (${spent:.2f}) | "
                     f"sold {total_sold:.1f} (${received:.2f}) | "
                     f"net: {total_bought - total_sold:.1f} shares{nl}")

        send_message(chat_id, text)

    except Exception as e:
        log.error(f"handle_pos error: {e}", exc_info=True)
        send_message(chat_id, f"❌ Error: {e}")


# ═════════════════════════════════════════════
#  COMMAND HANDLERS
# ═════════════════════════════════════════════

def handle_start(chat_id, username):
    send_message(chat_id,
        f"👋 Welcome{', ' + username if username else ''}!\n\n"
        "Are you ready to feel FOMO?\n\n"
        "Commands:\n"
        "/check 0x... — analyze a wallet\n"
        "/help — show help\n\n"
        "Example:\n"
        "<code>/check 0x1234...abcd</code>")


def handle_help(chat_id):
    send_message(chat_id,
        "📋 How to use:\n\n"
        "/check 0x... — enter any Polymarket wallet address\n\n"
        "📈 Mode 1 — ATH Analysis\n"
        "Shows the highest price each token reached AFTER your first buy.\n\n"
        "✅ Mode 2 — Resolution Analysis\n"
        "Shows resolved events — did your token win or lose?\n\n"
        "/pos 0xWALLET event name — view trades for a specific position\n"
        "Example: /pos 0x1234... Bitcoin Up or Down - February 20\n\n"
        "📊 Uses /positions (active+closed) as source.")


def handle_check(chat_id, wallet):
    wallet = wallet.strip()
    if not wallet.startswith("0x") or len(wallet) < 10:
        send_message(chat_id, "❌ Invalid wallet address. Example: /check 0x1234...abcd")
        return
    with state_lock:
        user_state[chat_id] = {"wallet": wallet, "step": "mode_select"}
    send_message(chat_id,
        f"✅ Wallet set:\n<code>{wallet}</code>\n\nChoose analysis mode:",
        reply_markup=mode_keyboard())


def handle_callback(chat_id, data, cq_id):
    answer_callback(cq_id)

    if data == "noop":
        return

    if data.startswith("page_"):
        parts = data.split("_")
        if len(parts) == 3:
            sid, page = parts[1], int(parts[2])
            with state_lock:
                st = user_state.get(chat_id, {})
            if st.get("session_id") == sid:
                if st.get("step") == "paging_ath":
                    send_ath_page(chat_id, sid, st["results"], st["wallet"],
                                 st["total_missed"], page, st.get("page_msg_id"))
                elif st.get("step") == "paging_res":
                    send_res_page(chat_id, sid, st["results"], st["wallet"],
                                 st["total_missed"], st["won_count"], st["lost_count"],
                                 st["total_count"], page, st.get("page_msg_id"))
        return

    with state_lock:
        st = user_state.get(chat_id)
    if not st or st.get("step") != "mode_select":
        send_message(chat_id, "Please use /check 0x... first.")
        return
    wallet = st.get("wallet")

    if data == "mode_1":
        send_message(chat_id, f"📈 Starting ATH Analysis for\n<code>{wallet}</code>\n\nThis may take a few minutes...")
        threading.Thread(target=analyze_ath, args=(wallet, chat_id), daemon=True).start()
    elif data == "mode_2":
        send_message(chat_id, f"✅ Starting Resolution Analysis for\n<code>{wallet}</code>\n\nThis may take a few minutes...")
        threading.Thread(target=analyze_resolution, args=(wallet, chat_id), daemon=True).start()

    with state_lock:
        user_state.pop(chat_id, None)


# ═════════════════════════════════════════════
#  MAIN POLLING LOOP
# ═════════════════════════════════════════════

def main():
    log.info("=" * 50)
    log.info("  Polymarket Wallet Analyzer Bot v2")
    log.info("=" * 50)

    if not TELEGRAM_BOT_TOKEN or "YOUR" in TELEGRAM_BOT_TOKEN:
        log.error("❌ TELEGRAM_BOT_TOKEN is not set! Put it in token.txt next to the script, "
                  "or run: export TELEGRAM_BOT_TOKEN=\"...\"")
        return

    cache_init()

    last_id = None
    log.info("Bot started. Polling...")

    while True:
        try:
            params = {"timeout": 30, "limit": 100}
            if last_id is not None:
                params["offset"] = last_id + 1

            resp = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params=params, timeout=35)
            resp.raise_for_status()
            updates = resp.json().get("result", [])

            for upd in updates:
                last_id = upd["update_id"]

                if "message" in upd:
                    msg = upd["message"]
                    cid = msg["chat"]["id"]
                    text = msg.get("text", "").strip()
                    uname = msg.get("from", {}).get("first_name", "")
                    log.info(f"[MSG] {cid} ({uname}): {text[:60]}")

                    if text.startswith("/start"):
                        handle_start(cid, uname)
                    elif text.startswith("/help"):
                        handle_help(cid)
                    elif text.startswith("/check"):
                        parts = text.split(maxsplit=1)
                        if len(parts) < 2:
                            send_message(cid, "Usage: /check 0x...")
                        else:
                            handle_check(cid, parts[1])
                    elif text.startswith("/pos"):
                        parts = text.split(maxsplit=1)
                        if len(parts) < 2:
                            send_message(cid, "Usage: /pos 0xWALLET event name\nExample: /pos 0x1234... Bitcoin Up or Down")
                        else:
                            threading.Thread(target=handle_pos, args=(cid, parts[1]), daemon=True).start()
                    elif text.isdigit():
                        num = int(text)
                        with state_lock:
                            st = user_state.get(cid, {})
                        if st.get("step") in ("paging_ath", "paging_res") and st.get("results"):
                            if 1 <= num <= len(st["results"]):
                                threading.Thread(
                                    target=analyze_detail,
                                    args=(cid, st.get("wallet", ""), st["results"][num - 1]),
                                    daemon=True).start()
                            else:
                                send_message(cid, f"❌ Position #{num} not found. Range: 1-{len(st['results'])}")
                        else:
                            send_message(cid, "Use /check 0x... to analyze a wallet.\n/help for more info.")
                    else:
                        send_message(cid, "Use /check 0x... to analyze a wallet.\n/help for more info.")

                elif "callback_query" in upd:
                    cq = upd["callback_query"]
                    cid = cq["message"]["chat"]["id"]
                    threading.Thread(
                        target=handle_callback,
                        args=(cid, cq.get("data", ""), cq["id"]),
                        daemon=True).start()

        except requests.RequestException as e:
            log.error(f"Polling error: {e}")
            time.sleep(5)
        except Exception as e:
            log.error(f"Unexpected error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()