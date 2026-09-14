import json
import os
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import quote
from urllib.request import Request, urlopen

from flask import Flask, jsonify, request

try:
    import websocket
except Exception:
    websocket = None

# ============================================================
# SCREENER WORKER — Render
# ============================================================
# This file is intentionally standalone. It does NOT import app274.py.
# It contains only the server-side market/screener engine required by Render.
# ============================================================

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

BUFFER_HOURS = float(os.getenv("SCREENER_BUFFER_HOURS", "25"))
BUFFER_SECONDS = int(BUFFER_HOURS * 3600)
SNAPSHOT_INTERVAL = max(15, int(os.getenv("SCREENER_SNAPSHOT_INTERVAL", "60")))
BUFFER_MAXLEN = int(BUFFER_SECONDS // SNAPSHOT_INTERVAL) + 5
HISTORY_CACHE_TTL = int(os.getenv("SCREENER_HISTORY_CACHE_TTL", "300"))
HISTORY_CACHE_MAX = int(os.getenv("SCREENER_HISTORY_CACHE_MAX", "5000"))
OI_WORKERS = max(1, min(24, int(os.getenv("SCREENER_OI_WORKERS", "24"))))

BINANCE_WS_URL = "wss://fstream.binance.com/market/ws"
BINANCE_REST_URLS = (
    "https://fapi.binance.com/fapi/v1/ticker/24hr",
    "https://fapi1.binance.com/fapi/v1/ticker/24hr",
    "https://fapi2.binance.com/fapi/v1/ticker/24hr",
)
BINANCE_DATA_TIMEOUT = 20
BINANCE_REST_INTERVAL = 10

app = Flask(__name__)

STATE = {
    "status": "STARTING",
    "last_error": "",
    "last_update": None,
    "last_message": None,
    "connection_count": 0,
    "reconnect_attempts": 0,
    "coins": {},
    "screener_engine": "worker",
    "screener_buffer_started_at": None,
    "screener_last_snapshot": None,
    "screener_historical_cache_size": 0,
}

state_lock = threading.RLock()
buffer_lock = threading.Lock()
history_cache_lock = threading.Lock()

# symbol -> deque[(timestamp, price, rolling_24h_turnover, base_volume_24h, trades, oi)]
SCREENER_BUFFER = {}
# (symbol, period, target_minute_bucket) -> {cached_at, point}
SCREENER_HISTORY_CACHE = {}


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(message, level="INFO"):
    print(f"[{utc_now()}] [{level}] {message}", flush=True)


def set_status(status):
    with state_lock:
        STATE["status"] = status


def add_error(message):
    with state_lock:
        STATE["last_error"] = str(message)
    log(str(message), "ERROR")


def normalize_ticker(item):
    if not isinstance(item, dict):
        return None
    symbol = item.get("s")
    if not symbol:
        return None
    try:
        price = float(item.get("c", 0))
        change_pct = float(item.get("P", 0))
        volume_quote = float(item.get("q", 0))
        volume_base = float(item.get("v", 0))
        trades = int(item.get("n", 0))
        bid = float(item.get("b", 0))
        ask = float(item.get("a", 0))
    except (TypeError, ValueError):
        return None
    return {
        "symbol": str(symbol).upper(),
        "price": price,
        "change_pct": change_pct,
        "volume_24h": volume_quote,
        "volume_base_24h": volume_base,
        "trades": trades,
        "bid": bid,
        "ask": ask,
        "exchange": "BINANCE FUTURES",
        "updated_at": utc_now(),
    }


def process_tickers(payload):
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        payload = payload["data"]
    elif isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        payload = [payload["data"]]
    elif isinstance(payload, dict) and payload.get("e") == "24hrTicker":
        payload = [payload]
    if not isinstance(payload, list):
        return 0

    normalized = []
    with state_lock:
        for raw in payload:
            item = normalize_ticker(raw)
            if item is None or not item["symbol"].endswith("USDT"):
                continue
            old = STATE["coins"].get(item["symbol"], {})
            for key in ("oi_value",):
                if key in old:
                    item[key] = old[key]
            STATE["coins"][item["symbol"]] = item
            normalized.append(item)
        if normalized:
            STATE["last_update"] = utc_now()
            STATE["last_message"] = utc_now()
            STATE["last_error"] = ""
    return len(normalized)


def fetch_binance_rest_tickers():
    errors = []
    for url in BINANCE_REST_URLS:
        try:
            req = Request(url, headers={"User-Agent": "Crypto-Screener-Worker/1.0", "Accept": "application/json"})
            with urlopen(req, timeout=8) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if not isinstance(payload, list) or not payload:
                raise ValueError("empty or invalid ticker response")
            return payload, url
        except Exception as exc:
            errors.append(f"{url}: {exc}")
    raise ConnectionError("; ".join(errors))


def binance_ws_worker():
    if websocket is None:
        set_status("ERROR")
        add_error("websocket-client is not available")
        return

    while True:
        ws = None
        try:
            set_status("CONNECTING")
            ws = websocket.create_connection(
                BINANCE_WS_URL,
                timeout=10,
                enable_multithread=True,
                ping_interval=20,
                ping_timeout=10,
            )
            with state_lock:
                STATE["connection_count"] += 1
                STATE["reconnect_attempts"] = 0
            ws.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": ["!ticker@arr"],
                "id": int(time.time() * 1000) % 2147483647,
            }))
            set_status("LIVE")
            log("Binance Futures WebSocket connected: !ticker@arr")

            while True:
                raw = ws.recv()
                if raw is None:
                    raise ConnectionError("WebSocket returned no data")
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                try:
                    payload = json.loads(raw)
                except Exception:
                    continue
                if isinstance(payload, dict) and payload.get("result") is None and payload.get("id"):
                    continue
                changed = process_tickers(payload)
                if changed:
                    set_status("LIVE")
        except Exception as exc:
            with state_lock:
                STATE["reconnect_attempts"] += 1
                STATE["status"] = "RECONNECTING"
            log(f"Binance WebSocket disconnected: {exc}", "WARNING")
            try:
                if ws is not None:
                    ws.close()
            except Exception:
                pass
            time.sleep(min(30, max(2, STATE["reconnect_attempts"])))


def rest_fallback_worker():
    while True:
        try:
            with state_lock:
                last_update = STATE["last_update"]
                coin_count = len(STATE["coins"])
            stale = True
            if last_update:
                try:
                    dt = datetime.fromisoformat(last_update)
                    stale = (datetime.now(timezone.utc) - dt).total_seconds() > BINANCE_DATA_TIMEOUT
                except (TypeError, ValueError):
                    stale = True
            if coin_count == 0 or stale:
                payload, source = fetch_binance_rest_tickers()
                count = process_tickers(payload)
                if count:
                    set_status("LIVE")
                    log(f"REST bootstrap/recovery: {count} USDT symbols from {source}")
        except Exception as exc:
            with state_lock:
                fresh = False
                last_update = STATE.get("last_update")
                if last_update:
                    try:
                        dt = datetime.fromisoformat(last_update)
                        fresh = (datetime.now(timezone.utc) - dt).total_seconds() <= BINANCE_DATA_TIMEOUT
                    except (TypeError, ValueError):
                        pass
                if not fresh and not STATE["coins"]:
                    STATE["status"] = "RECONNECTING"
                    STATE["last_error"] = str(exc)
            if not fresh:
                log(f"REST fallback failed: {exc}", "WARNING")
        time.sleep(BINANCE_REST_INTERVAL)


def fetch_current_oi(symbol):
    url = "https://fapi.binance.com/fapi/v1/openInterest?symbol=" + quote(symbol.upper())
    req = Request(url, headers={"User-Agent": "Crypto-Screener-Worker/1.0", "Accept": "application/json"})
    with urlopen(req, timeout=6) as response:
        payload = json.loads(response.read().decode("utf-8"))
    try:
        return float(payload.get("openInterest"))
    except (TypeError, ValueError):
        return None


def current_oi(symbol):
    with state_lock:
        value = STATE["coins"].get(symbol, {}).get("oi_value")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def buffer_upsert(symbol, coin, oi_value, timestamp):
    symbol = str(symbol).upper()
    bucket_ts = int(timestamp // SNAPSHOT_INTERVAL) * SNAPSHOT_INTERVAL
    point = (
        float(bucket_ts),
        float(coin.get("price", 0) or 0),
        float(coin.get("volume_24h", 0) or 0),
        float(coin.get("volume_base_24h", 0) or 0),
        int(coin.get("trades", 0) or 0),
        float(oi_value) if oi_value is not None else None,
    )
    with buffer_lock:
        dq = SCREENER_BUFFER.setdefault(symbol, deque(maxlen=BUFFER_MAXLEN))
        if dq and dq[-1][0] == point[0]:
            dq[-1] = point
        else:
            dq.append(point)
        cutoff = timestamp - BUFFER_SECONDS
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        if not dq:
            SCREENER_BUFFER.pop(symbol, None)


def snapshot_market_state():
    with state_lock:
        coins = {s: dict(c) for s, c in STATE["coins"].items() if s.endswith("USDT")}
    if not coins:
        return 0

    oi_values = {}
    workers = min(OI_WORKERS, len(coins))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_current_oi, symbol): symbol for symbol in coins}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                oi_values[symbol] = future.result()
            except Exception:
                oi_values[symbol] = current_oi(symbol)

    now_ts = time.time()
    with state_lock:
        for symbol, oi in oi_values.items():
            if oi is not None and symbol in STATE["coins"]:
                STATE["coins"][symbol]["oi_value"] = oi
        STATE["screener_last_snapshot"] = utc_now()
        if STATE["screener_buffer_started_at"] is None:
            STATE["screener_buffer_started_at"] = utc_now()

    for symbol, coin in coins.items():
        buffer_upsert(symbol, coin, oi_values.get(symbol, coin.get("oi_value")), now_ts)
    return len(coins)


def buffer_worker():
    while True:
        started = time.time()
        try:
            count = snapshot_market_state()
            if count:
                log(f"Rolling buffer snapshot: {count} symbols")
        except Exception as exc:
            log(f"Rolling buffer snapshot failed: {exc}", "WARNING")
        time.sleep(max(1, SNAPSHOT_INTERVAL - (time.time() - started)))


def period_seconds(period):
    aliases = {
        "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "4h": 14400, "12h": 43200,
        "24h": 86400, "25h": 90000, "1d": 86400,
    }
    return aliases.get(str(period or "").strip().lower())


def buffer_reference(symbol, target_ts):
    with buffer_lock:
        dq = SCREENER_BUFFER.get(str(symbol).upper())
        if not dq:
            return None
        for point in reversed(dq):
            if point[0] <= target_ts:
                return point
    return None


def history_cache_get(symbol, period, target_ts):
    bucket = int(float(target_ts) // 60)
    key = (str(symbol).upper(), str(period), bucket)
    with history_cache_lock:
        item = SCREENER_HISTORY_CACHE.get(key)
        if item and time.time() - item["cached_at"] <= HISTORY_CACHE_TTL:
            return dict(item["point"]), True
    return None, False


def history_cache_put(symbol, period, target_ts, point):
    bucket = int(float(target_ts) // 60)
    key = (str(symbol).upper(), str(period), bucket)
    with history_cache_lock:
        SCREENER_HISTORY_CACHE[key] = {"cached_at": time.time(), "point": dict(point)}
        while len(SCREENER_HISTORY_CACHE) > HISTORY_CACHE_MAX:
            SCREENER_HISTORY_CACHE.pop(next(iter(SCREENER_HISTORY_CACHE)))
        size = len(SCREENER_HISTORY_CACHE)
    with state_lock:
        STATE["screener_historical_cache_size"] = size


def fetch_historical_point(symbol, target_ts):
    """Fetch only one historical reference point; never download a range."""
    symbol = str(symbol).upper()
    target_ms = int(float(target_ts) * 1000)
    point = {"timestamp": float(target_ts), "price": None, "oi": None}

    try:
        url = (
            "https://fapi.binance.com/fapi/v1/klines?symbol=" + quote(symbol)
            + "&interval=1m&limit=1&endTime=" + str(target_ms + 60000)
        )
        req = Request(url, headers={"User-Agent": "Crypto-Screener-Worker/1.0"})
        with urlopen(req, timeout=8) as response:
            rows = json.loads(response.read().decode("utf-8"))
        if rows:
            row = rows[-1]
            point["timestamp"] = int(row[0]) / 1000.0
            point["price"] = float(row[4])
    except Exception as exc:
        log(f"Historical price point failed for {symbol}: {exc}", "WARNING")

    try:
        url = (
            "https://fapi.binance.com/futures/data/openInterestHist?symbol=" + quote(symbol)
            + "&period=5m&limit=1&endTime=" + str(target_ms + 300000)
        )
        req = Request(url, headers={"User-Agent": "Crypto-Screener-Worker/1.0"})
        with urlopen(req, timeout=8) as response:
            rows = json.loads(response.read().decode("utf-8"))
        if rows:
            row = rows[-1]
            point["oi"] = float(row.get("sumOpenInterest", 0))
    except Exception as exc:
        log(f"Historical OI point failed for {symbol}: {exc}", "WARNING")

    return point


def historical_reference(symbol, period, target_ts):
    cached, ok = history_cache_get(symbol, period, target_ts)
    if ok:
        return cached
    point = fetch_historical_point(symbol, target_ts)
    history_cache_put(symbol, period, target_ts, point)
    return point


def rolling_market_metrics(symbol, period):
    seconds = period_seconds(period)
    if seconds is None:
        return {}
    symbol = str(symbol or "").upper()
    with state_lock:
        coin = dict(STATE["coins"].get(symbol, {}))
    if not coin:
        return {}

    now_ts = time.time()
    target_ts = now_ts - seconds
    result = {
        "price": float(coin.get("price", 0) or 0),
        "volume_24h": float(coin.get("volume_24h", 0) or 0),
        "turnover_usd": float(coin.get("volume_24h", 0) or 0),
        "trades": float(coin.get("trades", 0) or 0),
    }

    if seconds <= BUFFER_SECONDS:
        ref = buffer_reference(symbol, target_ts)
        result["reference_source"] = "buffer"
        result["reference_ready"] = bool(ref)
        if not ref:
            return result
        ref_price = float(ref[1] or 0)
        if result["price"] > 0 and ref_price > 0:
            result["change_pct"] = (result["price"] / ref_price - 1.0) * 100.0
        ref_turnover = ref[2]
        if ref_turnover not in (None, 0):
            result["turnover_change_pct"] = (result["turnover_usd"] / float(ref_turnover) - 1.0) * 100.0
        current_oi = current_oi_value = current_oi_for_symbol(symbol)
        ref_oi = ref[5]
        if current_oi_value is not None and ref_oi not in (None, 0):
            result["oi_change_pct"] = (current_oi_value / float(ref_oi) - 1.0) * 100.0
            result["oi_change_usd"] = current_oi_value - float(ref_oi)
        result["reference_timestamp"] = ref[0]
        return result

    ref = historical_reference(symbol, period, target_ts)
    result["reference_source"] = "historical_point"
    result["reference_ready"] = bool(ref)
    ref_price = float(ref.get("price") or 0)
    if result["price"] > 0 and ref_price > 0:
        result["change_pct"] = (result["price"] / ref_price - 1.0) * 100.0
    current_oi_value = current_oi_for_symbol(symbol)
    ref_oi = ref.get("oi")
    if current_oi_value is not None and ref_oi not in (None, 0):
        result["oi_change_pct"] = (current_oi_value / float(ref_oi) - 1.0) * 100.0
        result["oi_change_usd"] = current_oi_value - float(ref_oi)
    result["reference_timestamp"] = ref.get("timestamp")
    return result


def current_oi_for_symbol(symbol):
    with state_lock:
        value = STATE["coins"].get(str(symbol).upper(), {}).get("oi_value")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def build_screener_snapshot():
    with state_lock:
        coins = [dict(c) for c in STATE["coins"].values()]
    coins.sort(key=lambda x: x.get("volume_24h", 0), reverse=True)
    return coins


def apply_filters(coins, payload):
    filters = payload.get("filters") if isinstance(payload, dict) else None
    if not isinstance(filters, dict):
        return coins
    result = []
    rolling_keys = {"change_pct", "volume_24h", "trades", "price", "oi_change_usd", "oi_change_pct", "delta_volume_usd"}
    for coin in coins:
        ok = True
        for key, cfg in filters.items():
            if not isinstance(cfg, dict):
                continue
            try:
                minimum = float(cfg["min"]) if cfg.get("min") not in (None, "") else None
            except (TypeError, ValueError):
                minimum = None
            try:
                maximum = float(cfg["max"]) if cfg.get("max") not in (None, "") else None
            except (TypeError, ValueError):
                maximum = None
            if minimum is None and maximum is None:
                continue
            timeframe = str(cfg.get("timeframe") or "1m")
            metrics = rolling_market_metrics(coin["symbol"], timeframe) if key in rolling_keys else {}
            value = metrics.get(key)
            if key == "volume_24h":
                value = metrics.get("turnover_usd")
            if value is None:
                value = coin.get(key)
            try:
                value = float(value)
            except (TypeError, ValueError):
                ok = False
                break
            if key == "change_pct" and minimum is not None and minimum >= 0 and (maximum is None or maximum >= 0):
                value = abs(value)
            if minimum is not None and value < minimum:
                ok = False
                break
            if maximum is not None and value > maximum:
                ok = False
                break
        if ok:
            result.append(coin)
    return result


def screener_response(coins=None):
    if coins is None:
        coins = build_screener_snapshot()
    with state_lock:
        return {
            "status": STATE["status"],
            "last_error": STATE["last_error"],
            "last_update": STATE["last_update"],
            "connection_count": STATE["connection_count"],
            "reconnect_attempts": STATE["reconnect_attempts"],
            "last_message": STATE["last_message"],
            "coins": coins,
            "count": len(coins),
            "screener_engine": "worker",
            "buffer_hours": BUFFER_HOURS,
            "buffer_last_snapshot": STATE["screener_last_snapshot"],
            "historical_cache_size": len(SCREENER_HISTORY_CACHE),
            "timestamp": utc_now(),
        }


@app.after_request
def worker_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Max-Age"] = "600"
    return response


@app.get("/health")
def health():
    with state_lock:
        return jsonify({
            "ok": True,
            "service": "binance-screener-worker",
            "role": "worker",
            "status": STATE["status"],
            "symbols": len(STATE["coins"]),
            "last_update": STATE["last_update"],
            "last_snapshot": STATE["screener_last_snapshot"],
            "timestamp": utc_now(),
        })


@app.get("/status")
@app.get("/api/screener/status")
def status():
    with state_lock:
        return jsonify({
            "ok": True,
            "engine": "worker",
            "status": STATE["status"],
            "symbols": len(STATE["coins"]),
            "buffer_symbols": len(SCREENER_BUFFER),
            "buffer_max_snapshots": BUFFER_MAXLEN,
            "buffer_hours": BUFFER_HOURS,
            "last_snapshot": STATE["screener_last_snapshot"],
            "historical_cache_size": len(SCREENER_HISTORY_CACHE),
            "timestamp": utc_now(),
        })


@app.route("/api/screener", methods=["GET", "POST", "OPTIONS"])
def screener():
    if request.method == "OPTIONS":
        return ("", 204)
    coins = build_screener_snapshot()
    if request.method == "POST":
        coins = apply_filters(coins, request.get_json(silent=True) or {})
    return jsonify(screener_response(coins))


def main():
    print("=" * 60, flush=True)
    print("Binance Screener Worker — Render", flush=True)
    print("=" * 60, flush=True)
    print(f"HTTP: http://{HOST}:{PORT}", flush=True)
    print(f"Rolling buffer: {BUFFER_HOURS:g}h", flush=True)
    print("Market data: Binance Futures !ticker@arr + REST fallback", flush=True)
    print("Role: Screener Worker only — no UI, no chart, no browser", flush=True)
    print("=" * 60, flush=True)

    threading.Thread(target=binance_ws_worker, daemon=True, name="binance-ws").start()
    threading.Thread(target=rest_fallback_worker, daemon=True, name="binance-rest-fallback").start()
    threading.Thread(target=buffer_worker, daemon=True, name="screener-buffer").start()

    app.run(host=HOST, port=PORT, debug=False, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
