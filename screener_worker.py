#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance Crypto Screener — Screener Worker 275
Standalone Render worker. No import from app273/app274/app275.

Architecture:
    Binance Futures -> Market State -> 25h RAM Buffer -> Calculation Plan -> Screener

Only real Binance Futures data is used. The worker has no UI, chart, browser,
Telegram or trading logic.
"""
import json
import math
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

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
BUFFER_HOURS = 25.0
BUFFER_SECONDS = int(BUFFER_HOURS * 3600)
SNAPSHOT_INTERVAL = max(30, int(os.getenv("SCREENER_SNAPSHOT_INTERVAL", "60")))
BUFFER_MAXLEN = int(BUFFER_SECONDS // SNAPSHOT_INTERVAL) + 5
HISTORY_CACHE_TTL = int(os.getenv("SCREENER_HISTORY_CACHE_TTL", "300"))
HISTORY_CACHE_MAX = int(os.getenv("SCREENER_HISTORY_CACHE_MAX", "5000"))
HISTORICAL_TOLERANCE_SECONDS = max(60, int(os.getenv("SCREENER_HISTORY_TOLERANCE_SECONDS", "90")))
BUFFER_REFERENCE_TOLERANCE_SECONDS = max(60, int(os.getenv("SCREENER_BUFFER_TOLERANCE_SECONDS", str(max(90, SNAPSHOT_INTERVAL * 2)))))
CANDLE_CACHE_TTL = int(os.getenv("SCREENER_CANDLE_CACHE_TTL", "20"))
OI_WORKERS = max(1, min(24, int(os.getenv("SCREENER_OI_WORKERS", "24"))))
MAX_SYMBOLS = max(1, int(os.getenv("SCREENER_MAX_SYMBOLS", "700")))

BINANCE_WS_URL = "wss://fstream.binance.com/market/ws"
BINANCE_REST_URLS = (
    "https://fapi.binance.com/fapi/v1/ticker/24hr",
    "https://fapi1.binance.com/fapi/v1/ticker/24hr",
    "https://fapi2.binance.com/fapi/v1/ticker/24hr",
    "https://fapi3.binance.com/fapi/v1/ticker/24hr",
)
BINANCE_DATA_TIMEOUT = 20
BINANCE_REST_INTERVAL = 10

ROLLING_PERIODS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800,
                   "1h": 3600, "4h": 14400, "12h": 43200,
                   "24h": 86400, "25h": 90000}
ALERT_TIMEFRAMES = ("1m", "5m", "15m", "30m", "1h", "4h", "12h", "24h", "25h", "1D")
OI_TIMEFRAMES = ("5m", "15m", "30m", "1h", "4h", "1D")
FILTER_KEYS = {
    "trades", "change_pct", "volume_24h", "natr", "btc_corr",
    "volume_spike", "volume_expansion_x", "spread_pct", "funding_pct",
    "oi_change_usd", "oi_change_pct", "delta_volume_usd", "price",
}

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
    "screener_snapshot_duration_ms": None,
    "screener_snapshot_symbols": 0,
    "historical_cache_size": 0,
    "candle_cache_size": 0,
    "calculation_count": 0,
    "calculation_errors": 0,
}
state_lock = threading.RLock()
buffer_lock = threading.Lock()
cache_lock = threading.Lock()

# symbol -> deque[(timestamp, price, rolling_24h_turnover, base_volume_24h, trades, oi_usd)]
SCREENER_BUFFER = {}
# (symbol, period, target_minute_bucket) -> {cached_at, point}
SCREENER_HISTORY_CACHE = {}
# (symbol, interval, bucket) -> {cached_at, candles}
CANDLE_CACHE = {}


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(message, level="INFO"):
    print(f"[{utc_now()}] [{level}] {message}", flush=True)


def set_status(status):
    with state_lock:
        STATE["status"] = status


def buffer_has_data():
    with buffer_lock:
        return any(bool(dq) for dq in SCREENER_BUFFER.values())


def set_ready_status():
    set_status("LIVE" if buffer_has_data() else "RECOVERING")


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
    symbol = str(symbol).upper()
    if not symbol.endswith("USDT"):
        return None
    return {
        "symbol": symbol,
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
            if item is None:
                continue
            old = STATE["coins"].get(item["symbol"], {})
            for key in ("oi_value", "oi_value_usd"):
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
            req = Request(url, headers={"User-Agent": "Crypto-Screener-Worker/275", "Accept": "application/json"})
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
                BINANCE_WS_URL, timeout=10, enable_multithread=True,
                ping_interval=20, ping_timeout=10,
            )
            with state_lock:
                STATE["connection_count"] += 1
                STATE["reconnect_attempts"] = 0
            ws.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": ["!ticker@arr"],
                "id": int(time.time() * 1000) % 2147483647,
            }))
            set_ready_status()
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
                if process_tickers(payload):
                    if buffer_has_data():
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
                    pass
            if coin_count == 0 or stale:
                payload, source = fetch_binance_rest_tickers()
                count = process_tickers(payload)
                if count:
                    set_ready_status()
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
    req = Request(url, headers={"User-Agent": "Crypto-Screener-Worker/275", "Accept": "application/json"})
    with urlopen(req, timeout=6) as response:
        payload = json.loads(response.read().decode("utf-8"))
    try:
        return float(payload.get("openInterest"))
    except (TypeError, ValueError):
        return None


def current_oi(symbol):
    with state_lock:
        value = STATE["coins"].get(str(symbol).upper(), {}).get("oi_value")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def buffer_upsert(symbol, coin, oi_value, timestamp):
    try:
        point = (
            float(timestamp),
            float(coin.get("price", 0) or 0),
            float(coin.get("volume_24h", 0) or 0),
            float(coin.get("volume_base_24h", 0) or 0),
            int(coin.get("trades", 0) or 0),
            float(oi_value) if oi_value is not None else None,
        )
    except (TypeError, ValueError):
        return
    symbol = str(symbol).upper()
    bucket_ts = int(timestamp // SNAPSHOT_INTERVAL) * SNAPSHOT_INTERVAL
    point = (float(bucket_ts),) + point[1:]
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
    started = time.time()
    with state_lock:
        symbols = sorted(
            [s for s in STATE["coins"] if s.endswith("USDT")],
            key=lambda s: STATE["coins"].get(s, {}).get("volume_24h", 0),
            reverse=True,
        )[:MAX_SYMBOLS]
        coins = {s: dict(STATE["coins"][s]) for s in symbols}
    if not coins:
        return 0

    oi_values = {}
    with ThreadPoolExecutor(max_workers=min(OI_WORKERS, len(coins))) as executor:
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
            coin = STATE["coins"].get(symbol)
            if coin is not None and oi is not None:
                coin["oi_value"] = oi
                price = float(coin.get("price", 0) or 0)
                coin["oi_value_usd"] = oi * price if price > 0 else None
        STATE["screener_last_snapshot"] = utc_now()
        if STATE["screener_buffer_started_at"] is None:
            STATE["screener_buffer_started_at"] = utc_now()
        STATE["screener_snapshot_duration_ms"] = round((time.time() - started) * 1000, 1)
        STATE["screener_snapshot_symbols"] = len(coins)

    for symbol, coin in coins.items():
        oi = oi_values.get(symbol, coin.get("oi_value"))
        oi_usd = (float(oi) * float(coin.get("price", 0) or 0)) if oi is not None else None
        buffer_upsert(symbol, coin, oi_usd, now_ts)
    if coins:
        set_status("LIVE")
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


def buffer_reference(symbol, target_ts, tolerance=BUFFER_REFERENCE_TOLERANCE_SECONDS):
    """Return the closest RAM-buffer point only when it is close enough to target."""
    with buffer_lock:
        dq = SCREENER_BUFFER.get(str(symbol).upper())
        if not dq:
            return None
        best = None
        best_distance = None
        for point in dq:
            distance = abs(float(point[0]) - float(target_ts))
            if best_distance is None or distance < best_distance:
                best = point
                best_distance = distance
        if best is None or best_distance > float(tolerance):
            return None
        return best


def period_seconds(period):
    return ROLLING_PERIODS.get(str(period or "").strip().lower())


def cache_get(key):
    now = time.time()
    with cache_lock:
        item = SCREENER_HISTORY_CACHE.get(key)
        if item and now - item["cached_at"] <= HISTORY_CACHE_TTL:
            return dict(item["point"]), True
    return None, False


def cache_put(key, point):
    with cache_lock:
        SCREENER_HISTORY_CACHE[key] = {"cached_at": time.time(), "point": dict(point)}
        while len(SCREENER_HISTORY_CACHE) > HISTORY_CACHE_MAX:
            SCREENER_HISTORY_CACHE.pop(next(iter(SCREENER_HISTORY_CACHE)))
        size = len(SCREENER_HISTORY_CACHE)
    with state_lock:
        STATE["historical_cache_size"] = size


def fetch_historical_point(symbol, target_ts):
    symbol = str(symbol).upper()
    target_ms = int(float(target_ts) * 1000)
    point = {"timestamp": float(target_ts), "price": None, "oi_usd": None}
    try:
        end_ms = target_ms + 60_000
        url = ("https://fapi.binance.com/fapi/v1/klines?symbol=" + quote(symbol)
               + "&interval=1m&limit=1&endTime=" + str(end_ms))
        with urlopen(Request(url, headers={"User-Agent": "Crypto-Screener-Worker/275"}), timeout=8) as response:
            rows = json.loads(response.read().decode("utf-8"))
        if rows:
            row = rows[-1]
            row_ts = int(row[0]) / 1000.0
            if abs(row_ts - float(target_ts)) <= HISTORICAL_TOLERANCE_SECONDS:
                point["timestamp"] = row_ts
                point["price"] = float(row[4])
    except Exception as exc:
        log(f"Historical price point failed for {symbol}: {exc}", "WARNING")
    try:
        end_ms = target_ms + 300_000
        url = ("https://fapi.binance.com/futures/data/openInterestHist?symbol=" + quote(symbol)
               + "&period=5m&limit=1&endTime=" + str(end_ms))
        with urlopen(Request(url, headers={"User-Agent": "Crypto-Screener-Worker/275"}), timeout=8) as response:
            rows = json.loads(response.read().decode("utf-8"))
        if rows:
            row = rows[-1]
            oi_ts = int(row.get("timestamp", target_ms)) / 1000.0
            if abs(oi_ts - float(target_ts)) <= max(300, HISTORICAL_TOLERANCE_SECONDS):
                point["oi_usd"] = float(row.get("sumOpenInterestValue", 0) or 0)
                point["oi_timestamp"] = oi_ts
    except Exception as exc:
        log(f"Historical OI point failed for {symbol}: {exc}", "WARNING")
    return point


def historical_reference(symbol, period, target_ts):
    bucket = int(float(target_ts) // 60)
    key = (str(symbol).upper(), str(period), bucket)
    cached, ok = cache_get(key)
    if ok:
        return cached
    point = fetch_historical_point(symbol, target_ts)
    cache_put(key, point)
    return point


def rolling_metrics(symbol, timeframe):
    seconds = period_seconds(timeframe)
    if seconds is None:
        return {}
    symbol = str(symbol).upper()
    with state_lock:
        coin = dict(STATE["coins"].get(symbol, {}))
    if not coin:
        return {}
    now_ts = time.time()
    target_ts = now_ts - seconds
    current_price = float(coin.get("price", 0) or 0)
    current_turnover = float(coin.get("volume_24h", 0) or 0)
    current_trades = float(coin.get("trades", 0) or 0)
    current_oi_usd = coin.get("oi_value_usd")
    try:
        current_oi_usd = float(current_oi_usd) if current_oi_usd is not None else None
    except (TypeError, ValueError):
        current_oi_usd = None

    if seconds <= BUFFER_SECONDS:
        ref = buffer_reference(symbol, target_ts)
        result = {"reference_source": "buffer", "reference_ready": bool(ref),
                  "price": current_price, "volume_24h": current_turnover,
                  "turnover_usd": current_turnover, "trades": current_trades}
        if not ref:
            return result
        ref_price = float(ref[1] or 0)
        if current_price > 0 and ref_price > 0:
            result["change_pct"] = (current_price / ref_price - 1.0) * 100.0
        ref_turnover = ref[2]
        if ref_turnover not in (None, 0):
            # This is change of Binance's rolling 24h turnover indicator,
            # not accumulated turnover during the requested period.
            result["turnover_change_pct"] = (current_turnover / float(ref_turnover) - 1.0) * 100.0
        ref_oi_usd = ref[5]
        if current_oi_usd is not None and ref_oi_usd not in (None, 0):
            result["oi_change_pct"] = (current_oi_usd / float(ref_oi_usd) - 1.0) * 100.0
            result["oi_change_usd"] = current_oi_usd - float(ref_oi_usd)
        result["reference_timestamp"] = ref[0]
        return result

    ref = historical_reference(symbol, timeframe, target_ts)
    result = {"reference_source": "historical_point", "reference_ready": bool(ref),
              "price": current_price, "volume_24h": current_turnover,
              "turnover_usd": current_turnover, "trades": current_trades}
    ref_price = float(ref.get("price") or 0)
    if current_price > 0 and ref_price > 0:
        result["change_pct"] = (current_price / ref_price - 1.0) * 100.0
    ref_oi_usd = ref.get("oi_usd")
    if current_oi_usd is not None and ref_oi_usd not in (None, 0):
        result["oi_change_pct"] = (current_oi_usd / float(ref_oi_usd) - 1.0) * 100.0
        result["oi_change_usd"] = current_oi_usd - float(ref_oi_usd)
    result["reference_timestamp"] = ref.get("timestamp")
    return result


def fetch_klines(symbol, interval, limit):
    symbol = str(symbol).upper()
    limit = max(1, min(int(limit), 1500))
    bucket = int(time.time() // max(60, interval_seconds(interval)))
    key = (symbol, interval, bucket, limit)
    with cache_lock:
        item = CANDLE_CACHE.get(key)
        if item and time.time() - item["cached_at"] <= CANDLE_CACHE_TTL:
            return [dict(x) for x in item["candles"]]
    url = ("https://fapi.binance.com/fapi/v1/klines?symbol=" + quote(symbol)
           + "&interval=" + quote(interval) + "&limit=" + str(limit))
    req = Request(url, headers={"User-Agent": "Crypto-Screener-Worker/275", "Accept": "application/json"})
    with urlopen(req, timeout=10) as response:
        rows = json.loads(response.read().decode("utf-8"))
    candles = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, list) or len(row) < 9:
            continue
        candles.append({
            "open_time": int(row[0]), "close_time": int(row[6]),
            "open": float(row[1]), "high": float(row[2]), "low": float(row[3]),
            "close": float(row[4]), "volume": float(row[5]),
            "quote_volume": float(row[7]), "trades_count": int(row[8]),
        })
    with cache_lock:
        CANDLE_CACHE[key] = {"cached_at": time.time(), "candles": candles}
        # Keep cache bounded even if many symbols/timeframes are requested.
        while len(CANDLE_CACHE) > 5000:
            CANDLE_CACHE.pop(next(iter(CANDLE_CACHE)))
        size = len(CANDLE_CACHE)
    with state_lock:
        STATE["candle_cache_size"] = size
    return [dict(x) for x in candles]


def interval_seconds(interval):
    aliases = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
               "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600,
               "12h": 43200, "1d": 86400, "1D": 86400}
    return aliases.get(str(interval), 60)


def closed_candles(rows):
    now_ms = int(time.time() * 1000)
    return [r for r in rows if int(r.get("close_time", 0) or 0) <= now_ms]


def calculate_natr(rows, period=14):
    rows = closed_candles(rows)
    if len(rows) < period + 1:
        return None
    tr = []
    prev_close = None
    for row in rows:
        high, low, close = float(row["high"]), float(row["low"]), float(row["close"])
        current_tr = high - low if prev_close is None else max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr.append(max(0.0, current_tr))
        prev_close = close
    atr = sum(tr[:period]) / period
    for value in tr[period:]:
        atr = ((atr * (period - 1)) + value) / period
    close = float(rows[-1]["close"])
    return atr / close * 100.0 if close > 0 else None


def calculate_correlation(symbol_rows, btc_rows, period=50):
    symbol_rows = closed_candles(symbol_rows)
    btc_rows = closed_candles(btc_rows)
    sm = {int(x["open_time"]): float(x["close"]) for x in symbol_rows}
    bm = {int(x["open_time"]): float(x["close"]) for x in btc_rows}
    times = sorted(set(sm) & set(bm))
    if len(times) < period + 1:
        return None
    times = times[-(period + 1):]
    sr = [(sm[b] / sm[a] - 1.0) if sm[a] else 0.0 for a, b in zip(times, times[1:])]
    br = [(bm[b] / bm[a] - 1.0) if bm[a] else 0.0 for a, b in zip(times, times[1:])]
    ma, mb = sum(sr) / len(sr), sum(br) / len(br)
    va = sum((x - ma) ** 2 for x in sr)
    vb = sum((x - mb) ** 2 for x in br)
    den = math.sqrt(va * vb)
    if den == 0:
        return None
    return sum((a - ma) * (b - mb) for a, b in zip(sr, br)) / den


def candle_metrics(symbol, timeframe, volume_base=100, volume_growth=20, need=None):
    # 1D is candle-based and intentionally is NOT converted to 24h rolling.
    interval = "1d" if timeframe == "1D" else timeframe
    need = set(need or FILTER_KEYS)
    limit = max(60, int(volume_base) + int(volume_growth) + 3, 55)
    rows = fetch_klines(symbol, interval, min(limit, 1500))
    rows = closed_candles(rows)
    if not rows:
        return {}
    current = rows[-1]
    previous = rows[-2] if len(rows) >= 2 else None
    result = {"price": float(current["close"]), "trades": float(current["trades_count"]),
              "volume_24h": float(current["quote_volume"]),
              "turnover_usd": float(current["quote_volume"])}
    if previous:
        prev_close = float(previous["close"])
        if prev_close > 0:
            result["change_pct"] = (float(current["close"]) / prev_close - 1.0) * 100.0
        result["delta_volume_usd"] = float(current["quote_volume"]) - float(previous["quote_volume"])
    if "natr" in need:
        result["natr"] = calculate_natr(rows)
    if "volume_expansion_x" in need:
        base = max(1, min(int(volume_base), 500))
        growth = max(1, min(int(volume_growth), 200))
        if len(rows) >= base + growth + 1:
            base_slice = rows[-(base + growth + 1):- (growth + 1)]
            growth_slice = rows[-(growth + 1):-1]
            base_avg = sum(float(x["quote_volume"]) for x in base_slice) / len(base_slice)
            growth_avg = sum(float(x["quote_volume"]) for x in growth_slice) / len(growth_slice)
            if base_avg > 0:
                result["volume_expansion_x"] = growth_avg / base_avg
                result["volume_expansion_direction"] = "up" if growth_avg >= base_avg else "down"
    if "volume_spike" in need and previous:
        pv = float(previous["quote_volume"])
        result["volume_spike"] = ((float(current["quote_volume"]) / pv) - 1.0) * 100.0 if pv > 0 else None
    return result


def metric_for_filter(symbol, filter_cfg, calc_cache, btc_rows=None):
    """Calculate one filter metric using the shared per-request calculation cache.

    Cheap Market-State values are resolved before any candle/history REST work.
    """
    key = str(filter_cfg.get("key") or "").strip()
    tf = str(filter_cfg.get("timeframe") or "1h").strip()
    if key not in FILTER_KEYS or tf not in ALERT_TIMEFRAMES:
        return None
    symbol = str(symbol).upper()
    volume_base = max(1, min(500, int(filter_cfg.get("volumeBase") or 100)))
    volume_growth = max(1, min(200, int(filter_cfg.get("volumeGrowth") or 20)))

    # 1) Cheap values already present in the shared Market State.
    if key in {"price", "volume_24h", "trades"}:
        with state_lock:
            coin = dict(STATE["coins"].get(symbol, {}))
        if key == "price":
            return number_or_none(coin.get("price"))
        if key == "volume_24h":
            return number_or_none(coin.get("volume_24h"))
        return number_or_none(coin.get("trades"))

    if key == "spread_pct":
        with state_lock:
            coin = dict(STATE["coins"].get(symbol, {}))
        bid, ask = number_or_none(coin.get("bid")), number_or_none(coin.get("ask"))
        if bid is None or ask is None:
            return None
        mid = (bid + ask) / 2.0
        return (ask - bid) / mid * 100.0 if mid > 0 else None

    if key == "funding_pct":
        return funding_rate(symbol, calc_cache)

    # 2) Rolling values use NOW vs NOW-PERIOD and RAM first.
    rolling_keys = {"change_pct", "oi_change_usd", "oi_change_pct", "delta_volume_usd"}
    if key in rolling_keys and tf != "1D":
        cache_key = ("rolling", symbol, tf)
        if cache_key not in calc_cache:
            calc_cache[cache_key] = rolling_metrics(symbol, tf)
        return calc_cache[cache_key].get(key)

    # 3) OI is independent from candles. 1D uses NOW vs NOW-1D reference,
    # not the latest-two OI samples and not a candle shortcut.
    if key in {"oi_change_pct", "oi_change_usd"}:
        return oi_change_metric(symbol, tf, key, calc_cache)

    # 4) Candle-based metrics use the last fully closed candle only.
    cache_key = ("candle", symbol, tf, volume_base, volume_growth)
    if cache_key not in calc_cache:
        calc_cache[cache_key] = candle_metrics(symbol, tf, volume_base, volume_growth, {key, "natr", "btc_corr"} & FILTER_KEYS)
    metrics = calc_cache[cache_key]
    if key == "btc_corr":
        btc_key = ("btc", tf)
        if btc_key not in calc_cache:
            calc_cache[btc_key] = fetch_klines("BTCUSDT", "1d" if tf == "1D" else tf, 60)
        return calculate_correlation(
            fetch_klines(symbol, "1d" if tf == "1D" else tf, 60),
            calc_cache[btc_key]
        )
    return metrics.get(key)


def funding_rate(symbol, calc_cache):
    key = ("funding", str(symbol).upper())
    if key in calc_cache:
        return calc_cache[key]
    try:
        url = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=" + quote(str(symbol).upper())
        with urlopen(Request(url, headers={"User-Agent": "Crypto-Screener-Worker/275"}), timeout=6) as response:
            data = json.loads(response.read().decode("utf-8"))
        value = float(data.get("lastFundingRate", 0)) * 100.0
    except Exception as exc:
        log(f"Funding request failed for {symbol}: {exc}", "WARNING")
        value = None
    calc_cache[key] = value
    return value


def oi_change_metric(symbol, timeframe, key, calc_cache):
    """OI change against the requested target time, independent of candles."""
    symbol = str(symbol).upper()
    tf = str(timeframe or "1h")
    seconds = period_seconds(tf)
    if seconds is None:
        return None
    cache_key = ("oi_target", symbol, tf)
    if cache_key in calc_cache:
        return calc_cache[cache_key].get(key)

    with state_lock:
        coin = dict(STATE["coins"].get(symbol, {}))
    current = number_or_none(coin.get("oi_value_usd"))
    if current is None:
        current = number_or_none(coin.get("oi_value"))
    if current is None:
        calc_cache[cache_key] = {}
        return None

    target_ts = time.time() - seconds
    ref = None
    if seconds <= BUFFER_SECONDS:
        ref_point = buffer_reference(symbol, target_ts)
        if ref_point is not None:
            ref = {"timestamp": ref_point[0], "oi_usd": ref_point[5]}
    if ref is None:
        ref = historical_reference(symbol, tf, target_ts)

    previous = number_or_none(ref.get("oi_usd") if ref else None)
    result = {}
    if previous is not None and previous > 0:
        result["oi_change_pct"] = (current / previous - 1.0) * 100.0
        result["oi_change_usd"] = current - previous
    calc_cache[cache_key] = result
    return result.get(key)


def normalize_filter(filter_cfg):
    if not isinstance(filter_cfg, dict):
        return None
    key = str(filter_cfg.get("key") or "")
    if key not in FILTER_KEYS:
        return None
    tf = str(filter_cfg.get("timeframe") or "1h")
    if key == "oi_change_pct" and tf not in OI_TIMEFRAMES:
        tf = "1h"
    elif tf not in ALERT_TIMEFRAMES:
        tf = "1h"
    try:
        base = max(1, min(500, int(filter_cfg.get("volumeBase") or 100)))
    except (TypeError, ValueError):
        base = 100
    try:
        growth = max(1, min(200, int(filter_cfg.get("volumeGrowth") or 20)))
    except (TypeError, ValueError):
        growth = 20
    direction = str(filter_cfg.get("direction") or "any")
    if direction not in {"any", "up", "down"}:
        direction = "any"
    return {"key": key, "timeframe": tf,
            "min": filter_cfg.get("min", ""), "max": filter_cfg.get("max", ""),
            "direction": direction, "volumeBase": base, "volumeGrowth": growth}


def number_or_none(value):
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def filter_pass(value, cfg):
    if value is None:
        return False
    try:
        value = float(value)
    except (TypeError, ValueError):
        return False
    minimum = number_or_none(cfg.get("min"))
    maximum = number_or_none(cfg.get("max"))
    if minimum is None and maximum is None:
        return True
    if cfg.get("key") == "change_pct" and minimum is not None and minimum >= 0 and (maximum is None or maximum >= 0):
        value = abs(value)
    if minimum is not None and value < minimum:
        return False
    if maximum is not None and value > maximum:
        return False
    return True


def direction_pass(value, direction):
    if direction == "up":
        return value is not None and float(value) > 0
    if direction == "down":
        return value is not None and float(value) < 0
    return True


def filter_cost_rank(cfg):
    """Lower rank = cheaper. Market State checks always precede REST/history work."""
    key = cfg.get("key")
    return {
        "price": 0, "trades": 0, "volume_24h": 0, "spread_pct": 0,
        "change_pct": 1, "oi_change_pct": 2, "oi_change_usd": 2,
        "delta_volume_usd": 2, "funding_pct": 3,
        "volume_spike": 4, "volume_expansion_x": 4,
        "natr": 5, "btc_corr": 6,
    }.get(key, 9)


def ordered_filters(filters):
    return sorted(filters, key=lambda cfg: (filter_cost_rank(cfg), cfg["key"], cfg["timeframe"], cfg["volumeBase"], cfg["volumeGrowth"]))


def evaluate_alert(alert, symbols, calc_cache, precomputed=None):
    filters = [normalize_filter(x) for x in (alert.get("filters") or [])]
    filters = ordered_filters([x for x in filters if x])
    if not filters:
        return {}
    selected = {str(s).upper() for s in (alert.get("symbols") or []) if str(s).strip()}
    candidates = [s for s in symbols if not selected or s in selected]
    matches = {}
    for symbol in candidates:
        ok = True
        values = {}
        try:
            for cfg in filters:
                cache_key = ("filter", symbol, cfg["key"], cfg["timeframe"], cfg["volumeBase"], cfg["volumeGrowth"])
                if precomputed is not None and cache_key in precomputed:
                    value = precomputed[cache_key]
                else:
                    value = metric_for_filter(symbol, cfg, calc_cache)
                    if precomputed is not None:
                        precomputed[cache_key] = value
                values[cfg["key"]] = value
                if not filter_pass(value, cfg):
                    ok = False
                    break
                if cfg["key"] in {"oi_change_pct", "volume_expansion_x"} and cfg["direction"] != "any":
                    if cfg["key"] == "oi_change_pct":
                        direction_value = value
                    else:
                        direction_value = 1 if value is not None and value >= 1 else -1
                    if not direction_pass(direction_value, cfg["direction"]):
                        ok = False
                        break
            if ok:
                matches[symbol] = values
        except Exception as exc:
            with state_lock:
                STATE["calculation_errors"] += 1
            log(f"Alert calculation failed for {symbol}: {exc}", "WARNING")
    return matches


def build_snapshot():
    with state_lock:
        coins = [dict(c) for c in STATE["coins"].values() if str(c.get("symbol", "")).endswith("USDT")]
    coins.sort(key=lambda x: x.get("volume_24h", 0), reverse=True)
    return coins[:MAX_SYMBOLS]


def screener_filter_payload(payload):
    # Supports the 275 API contract: filters may be a dict keyed by metric or an array.
    raw = payload.get("filters") if isinstance(payload, dict) else None
    if isinstance(raw, dict):
        filters = []
        for key, cfg in raw.items():
            if isinstance(cfg, dict):
                item = dict(cfg)
                item["key"] = key
                filters.append(item)
    elif isinstance(raw, list):
        filters = raw
    else:
        filters = []
    return ordered_filters([x for x in (normalize_filter(f) for f in filters) if x])


def evaluate_screener(payload):
    coins = build_snapshot()
    symbols = [c["symbol"] for c in coins]
    filters = screener_filter_payload(payload)
    calc_cache = {}
    precomputed = {}
    result = []
    for coin in coins:
        symbol = coin["symbol"]
        ok = True
        for cfg in filters:
            try:
                cache_key = ("filter", symbol, cfg["key"], cfg["timeframe"], cfg["volumeBase"], cfg["volumeGrowth"])
                if cache_key not in precomputed:
                    precomputed[cache_key] = metric_for_filter(symbol, cfg, calc_cache)
                value = precomputed[cache_key]
                if not filter_pass(value, cfg):
                    ok = False
                    break
                if cfg["key"] == "oi_change_pct" and cfg["direction"] != "any" and not direction_pass(value, cfg["direction"]):
                    ok = False
                    break
            except Exception as exc:
                with state_lock:
                    STATE["calculation_errors"] += 1
                log(f"Screener calculation failed for {symbol}: {exc}", "WARNING")
                ok = False
                break
        if ok:
            result.append(coin)
    with state_lock:
        STATE["calculation_count"] += len(symbols)
    return result, calc_cache


def batch_evaluate(alerts):
    if not isinstance(alerts, list):
        raise ValueError("alerts must be a list")
    coins = build_snapshot()
    symbols = [c["symbol"] for c in coins]
    calc_cache = {}
    precomputed = {}
    results = []
    # Normalize and pre-plan all unique filter requests once. Evaluation then reuses
    # the same Market State/calculation result across all alerts.
    plans = []
    for raw in alerts:
        alert = raw if isinstance(raw, dict) else {}
        filters = ordered_filters([x for x in (normalize_filter(f) for f in (alert.get("filters") or [])) if x])
        plans.append((alert, filters))
    for alert, filters in plans:
        if not filters:
            matches = {}
        else:
            selected = {str(s).upper() for s in (alert.get("symbols") or []) if str(s).strip()}
            candidates = [s for s in symbols if not selected or s in selected]
            matches = {}
            for symbol in candidates:
                ok = True
                values = {}
                try:
                    for cfg in filters:
                        key = ("filter", symbol, cfg["key"], cfg["timeframe"], cfg["volumeBase"], cfg["volumeGrowth"])
                        if key not in precomputed:
                            precomputed[key] = metric_for_filter(symbol, cfg, calc_cache)
                        value = precomputed[key]
                        values[cfg["key"]] = value
                        if not filter_pass(value, cfg):
                            ok = False
                            break
                        if cfg["key"] in {"oi_change_pct", "volume_expansion_x"} and cfg["direction"] != "any":
                            direction_value = value if cfg["key"] == "oi_change_pct" else (1 if value is not None and value >= 1 else -1)
                            if not direction_pass(direction_value, cfg["direction"]):
                                ok = False
                                break
                    if ok:
                        matches[symbol] = values
                except Exception as exc:
                    with state_lock:
                        STATE["calculation_errors"] += 1
                    log(f"Batch calculation failed for {symbol}: {exc}", "WARNING")
        results.append({"id": alert.get("id"), "name": alert.get("name", ""), "matches": matches, "count": len(matches)})
    with state_lock:
        STATE["calculation_count"] += len(symbols) * max(1, len(alerts))
    return results


def diagnostics():
    now = time.time()
    with buffer_lock:
        buffer_symbols = len(SCREENER_BUFFER)
        points = sum(len(x) for x in SCREENER_BUFFER.values())
        oldest = None
        newest = None
        for dq in SCREENER_BUFFER.values():
            if dq:
                oldest = dq[0][0] if oldest is None else min(oldest, dq[0][0])
                newest = dq[-1][0] if newest is None else max(newest, dq[-1][0])
    with state_lock, cache_lock:
        last_update = STATE["last_update"]
        return {
            "status": STATE["status"], "symbols": len(STATE["coins"]),
            "buffer_symbols": buffer_symbols, "buffer_points": points,
            "buffer_hours": BUFFER_HOURS, "buffer_max_snapshots": BUFFER_MAXLEN,
            "buffer_reference_tolerance_seconds": BUFFER_REFERENCE_TOLERANCE_SECONDS,
            "historical_reference_tolerance_seconds": HISTORICAL_TOLERANCE_SECONDS,
            "buffer_oldest_timestamp": datetime.fromtimestamp(oldest, timezone.utc).isoformat() if oldest else None,
            "buffer_newest_timestamp": datetime.fromtimestamp(newest, timezone.utc).isoformat() if newest else None,
            "last_update": last_update, "last_snapshot": STATE["screener_last_snapshot"],
            "snapshot_duration_ms": STATE["screener_snapshot_duration_ms"],
            "snapshot_symbols": STATE["screener_snapshot_symbols"],
            "historical_cache_size": len(SCREENER_HISTORY_CACHE),
            "candle_cache_size": len(CANDLE_CACHE),
            "calculation_count": STATE["calculation_count"],
            "calculation_errors": STATE["calculation_errors"],
            "timestamp": utc_now(),
        }


@app.after_request
def worker_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Max-Age"] = "600"
    return response


@app.route("/health", methods=["GET"])
def health():
    with state_lock:
        return jsonify({
            "ok": True, "service": "binance-screener-worker-275", "role": "worker",
            "status": STATE["status"], "symbols": len(STATE["coins"]),
            "last_update": STATE["last_update"], "last_snapshot": STATE["screener_last_snapshot"],
            "timestamp": utc_now(),
        })


@app.route("/status", methods=["GET"])
@app.route("/api/screener/status", methods=["GET"])
def status():
    return jsonify({"ok": True, "engine": "worker", **diagnostics()})


@app.route("/api/screener", methods=["GET", "POST", "OPTIONS"])
def screener():
    if request.method == "OPTIONS":
        return ("", 204)
    if request.method == "GET":
        coins = build_snapshot()
    else:
        payload = request.get_json(silent=True) or {}
        coins, _ = evaluate_screener(payload)
    d = diagnostics()
    return jsonify({
        "ok": True, "status": d["status"], "timestamp": d["timestamp"],
        "engine": "worker", "screener_engine": "worker", "coins": coins,
        "count": len(coins), "buffer_hours": BUFFER_HOURS,
        "buffer_last_snapshot": d["last_snapshot"],
        "historical_cache_size": d["historical_cache_size"],
        "last_error": STATE["last_error"], "connection_count": STATE["connection_count"],
        "reconnect_attempts": STATE["reconnect_attempts"], "last_message": STATE["last_message"],
    })


@app.route("/api/state", methods=["GET"])
def state_compat():
    # Compatibility with the Local screener response shape. Chart data is not served here.
    d = diagnostics()
    return jsonify({
        "status": d["status"], "last_error": STATE["last_error"],
        "last_update": STATE["last_update"], "connection_count": STATE["connection_count"],
        "reconnect_attempts": STATE["reconnect_attempts"], "last_message": STATE["last_message"],
        "coins": build_snapshot(), "levels": [], "signals": [], "screener_engine": "worker",
        "buffer_hours": BUFFER_HOURS, "buffer_last_snapshot": d["last_snapshot"],
        "historical_cache_size": d["historical_cache_size"],
    })


@app.route("/api/alert_metrics", methods=["POST", "OPTIONS"])
def alert_metrics():
    if request.method == "OPTIONS":
        return ("", 204)
    payload = request.get_json(silent=True) or {}
    raw_symbols = payload.get("symbols") or []
    if not isinstance(raw_symbols, list):
        return jsonify({"error": "symbols must be a list"}), 400
    symbols = list(dict.fromkeys(str(s).upper().strip() for s in raw_symbols if str(s).upper().strip().endswith("USDT")))[:MAX_SYMBOLS]
    timeframe = str(payload.get("timeframe") or "5m")
    if timeframe not in ALERT_TIMEFRAMES:
        return jsonify({"error": "Invalid alert timeframe"}), 400
    need_oi = bool(payload.get("need_oi"))
    need_volume = bool(payload.get("need_volume"))
    need_market = bool(payload.get("need_market"))
    volume_base = max(1, min(500, int(payload.get("volume_base") or 100)))
    volume_growth = max(1, min(200, int(payload.get("volume_growth") or 20)))
    calc_cache = {}
    metrics = {}
    for symbol in symbols:
        values = {}
        try:
            if need_market:
                # Compute all common market values once for this symbol/timeframe.
                if timeframe != "1D":
                    values.update(rolling_metrics(symbol, timeframe))
                else:
                    values.update(candle_metrics(symbol, timeframe, volume_base, volume_growth, FILTER_KEYS))
                with state_lock:
                    coin = dict(STATE["coins"].get(symbol, {}))
                values["spread_pct"] = ((float(coin.get("ask", 0)) - float(coin.get("bid", 0))) / ((float(coin.get("ask", 0)) + float(coin.get("bid", 0))) / 2) * 100) if float(coin.get("ask", 0) or 0) + float(coin.get("bid", 0) or 0) > 0 else None
                values["funding_pct"] = funding_rate(symbol, calc_cache)
                if "btc_corr" not in values:
                    rows = fetch_klines(symbol, "1d" if timeframe == "1D" else timeframe, 60)
                    btc = fetch_klines("BTCUSDT", "1d" if timeframe == "1D" else timeframe, 60)
                    values["btc_corr"] = calculate_correlation(rows, btc)
            if need_oi:
                values["oi_change_pct"] = oi_change_metric(symbol, timeframe, "oi_change_pct", calc_cache)
                values["oi_change_usd"] = oi_change_metric(symbol, timeframe, "oi_change_usd", calc_cache)
            if need_volume:
                vm = candle_metrics(symbol, timeframe, volume_base, volume_growth, {"volume_expansion_x"})
                values["volume_expansion_x"] = vm.get("volume_expansion_x")
                values["volume_expansion_direction"] = vm.get("volume_expansion_direction", "any")
            metrics[symbol] = {k: (round(float(v), 8) if isinstance(v, (int, float)) and math.isfinite(float(v)) else v) for k, v in values.items()}
        except Exception as exc:
            with state_lock:
                STATE["calculation_errors"] += 1
            log(f"Alert metrics failed for {symbol}: {exc}", "WARNING")
    return jsonify({"metrics": metrics, "cached": False, "timestamp": utc_now()})


@app.route("/api/screener/batch", methods=["POST", "OPTIONS"])
def screener_batch():
    if request.method == "OPTIONS":
        return ("", 204)
    payload = request.get_json(silent=True) or {}
    alerts = payload.get("alerts") if isinstance(payload, dict) else None
    if alerts is None and isinstance(payload, list):
        alerts = payload
    if not isinstance(alerts, list):
        return jsonify({"error": "alerts must be a list"}), 400
    try:
        results = batch_evaluate(alerts)
        return jsonify({"ok": True, "engine": "worker", "results": results, "count": len(results), "timestamp": utc_now()})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


def main():
    print("=" * 60, flush=True)
    print("Binance Screener Worker 275 — Render", flush=True)
    print("=" * 60, flush=True)
    print(f"HTTP: http://{HOST}:{PORT}", flush=True)
    print(f"Rolling buffer: {BUFFER_HOURS:g}h / snapshot target: {SNAPSHOT_INTERVAL}s", flush=True)
    print("Market data: Binance Futures !ticker@arr + REST fallback", flush=True)
    print("Architecture: Market State -> 25h Buffer -> Rolling/Closed Candle -> Screener", flush=True)
    print("Role: Screener Worker only — no UI, no chart, no browser", flush=True)
    print("=" * 60, flush=True)
    threading.Thread(target=binance_ws_worker, daemon=True, name="binance-ws").start()
    threading.Thread(target=rest_fallback_worker, daemon=True, name="binance-rest-fallback").start()
    threading.Thread(target=buffer_worker, daemon=True, name="screener-buffer").start()
    app.run(host=HOST, port=PORT, debug=False, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
