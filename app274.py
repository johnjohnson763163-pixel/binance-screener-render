#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Crypto Screener - Stage 1
Single-file local development version.

Run:
    python app263.py

Then open:
    http://127.0.0.1:8000

Requirements:
    Python 3.9+
    Flask

Install Flask once if necessary:
    python -m pip install flask
"""

import json
import math
import os
import threading
import time
import webbrowser
from collections import deque
from datetime import datetime, timezone
from urllib.parse import quote, urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen

from flask import Flask, jsonify, render_template_string, request

try:
    import websocket
except ImportError:
    websocket = None


# ============================================================
# CONFIG
# ============================================================

# Screener Engine can run inside the same single-file app as a Render worker.
# Local UI remains the default process; Render deployment sets SCREENER_WORKER=1.
SCREENER_WORKER_MODE = str(os.environ.get("SCREENER_WORKER", "0")).strip().lower() in {"1", "true", "yes", "on"}
SCREENER_RENDER_URL = str(os.environ.get("SCREENER_RENDER_URL", "")).strip().rstrip("/")

HOST = os.environ.get("HOST", "0.0.0.0" if SCREENER_WORKER_MODE else "127.0.0.1")
PORT = int(os.environ.get("PORT", "8000"))
SCREENER_BUFFER_HOURS = 25
SCREENER_BUFFER_SECONDS = SCREENER_BUFFER_HOURS * 3600
SCREENER_SNAPSHOT_INTERVAL = 60
SCREENER_BUFFER_MAXLEN = SCREENER_BUFFER_HOURS * 60 + 5
SCREENER_HISTORY_CACHE_TTL = 300
SCREENER_HISTORY_TARGET_BUCKET_MS = 60_000

BINANCE_WS_URLS = [
    "wss://fstream.binance.com/stream?streams=!ticker@arr",
    "wss://fstream.binance.com/market/ws/!ticker@arr",
    "wss://fstream1.binance.com/stream?streams=!ticker@arr",
    "wss://fstream2.binance.com/stream?streams=!ticker@arr",
    "wss://fstream3.binance.com/stream?streams=!ticker@arr",
]
BINANCE_REST_URLS = [
    "https://fapi.binance.com/fapi/v1/ticker/24hr",
    "https://fapi1.binance.com/fapi/v1/ticker/24hr",
    "https://fapi2.binance.com/fapi/v1/ticker/24hr",
    "https://fapi3.binance.com/fapi/v1/ticker/24hr",
]
BINANCE_REST_INTERVAL = 3
BINANCE_DATA_TIMEOUT = 8

# Screener analytics.
# NATR: 14-period ATR normalized by the latest close.
# BTC correlation: Pearson correlation of candle-to-candle returns over 50 candles.
ANALYTICS_INTERVAL = "5m"
ANALYTICS_PERIOD = 14
# NATR uses a 14-period ATR on closed 5-minute candles. Extra candles are
# loaded to warm up Wilder smoothing instead of calculating from only 14 bars.
ANALYTICS_KLINE_LIMIT = 60
CORRELATION_PERIOD = 50
ANALYTICS_REFRESH = 30
ANALYTICS_MAX_SYMBOLS = 200
ANALYTICS_INITIAL_READY = threading.Event()

MAX_COINS = None
UI_UPDATE_INTERVAL = 0.25
RECONNECT_DELAY = 3

# ============================================================
# DENSITY / ORDER BOOK DATA LAYER — STAGE 1
# ============================================================
# Stage 1 only: collect and normalize real order-book data.
# Density detection/visualization is intentionally NOT implemented here.
ORDERBOOK_ENABLED = True
ORDERBOOK_SYMBOL_LIMIT = 8
ORDERBOOK_REFRESH_SYMBOLS = 30
ORDERBOOK_DEPTH_BYBIT = 50
ORDERBOOK_CHANNEL_OKX = "books"
ORDERBOOK_BINANCE_DEPTH = 20
ORDERBOOK_BINANCE_SPEED = "100ms"
ORDERBOOK_STALE_AFTER = 10
ORDERBOOK_EXCHANGES = ("binance", "bybit", "okx")
ORDERBOOK_DEFAULT_SYMBOLS = (
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
    "XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT",
)

# ============================================================
# DENSITY ENGINE — STAGE 2
# ============================================================
# Density is a live cluster of nearby order-book levels. The engine keeps
# lifecycle state server-side and exposes only compact density objects.
DENSITY_ENABLED = True
DENSITY_SCAN_INTERVAL = 0.50
DENSITY_MIN_USD = 400_000.0
DENSITY_MAX_DISTANCE_PCT = 3.0
DENSITY_MIN_DISTANCE_PCT = 0.05
DENSITY_CLUSTER_GAP_PCT = 0.05
DENSITY_MIN_CHANGE_PCT = 1.0
DENSITY_MAX_PER_SIDE = 40
DENSITY_SYMBOL_LIMIT = ORDERBOOK_SYMBOL_LIMIT
# Interest-filter infrastructure. Disabled by default so the current density
# universe is not silently narrowed. The "filter all" mode is reserved for
# the future full-universe engine and currently does not expand subscriptions.
DENSITY_INTEREST_FILTER_ENABLED = False
DENSITY_FILTER_ALL_SYMBOLS = False
DENSITY_INTEREST_MIN_VOLUME_USD = 0.0
DENSITY_SHOW_BUY = True
DENSITY_SHOW_SELL = True

# Stage 4 — Density presentation
DENSITY_MAP_MAX_ITEMS = 80
DENSITY_CHART_MAX_ITEMS = 12
DENSITY_UI_REFRESH_MS = 500

# Stage 5 — Runtime Density settings
# Stage 6 — Multi-exchange integration / health / aggregation
DENSITY_AGGREGATION_GAP_PCT = 0.08
DENSITY_HEALTH_STALE_SEC = 10.0
DENSITY_AGGREGATION_MAX_ITEMS = 60

DENSITY_SETTINGS = {
    "enabled": True,
    "min_usd": DENSITY_MIN_USD,
    "min_distance_percent": DENSITY_MIN_DISTANCE_PCT,
    "max_distance_percent": DENSITY_MAX_DISTANCE_PCT,
    "approach": "adaptive_cluster",
    "show_on_chart": True,
    "show_in_screener": True,
    "show_consumed": True,
    "show_remaining": True,
    "show_lifetime": True,
    "show_spot": True,
    "show_futures": True,
    "strength_min": 0,
    "exchanges": ["binance", "bybit", "okx"],
    "blacklist": [],
    "interest_filter_enabled": DENSITY_INTEREST_FILTER_ENABLED,
    "filter_all_symbols": DENSITY_FILTER_ALL_SYMBOLS,
    "interest_min_volume_usd": DENSITY_INTEREST_MIN_VOLUME_USD,
}

# ============================================================
# PERFORMANCE / SUBSCRIPTION ROUTER — STAGE 3
# ============================================================
ORDERBOOK_LOAD_MODE = "balanced"  # economy | balanced | maximum
ORDERBOOK_MODE_LIMITS = {"economy": 4, "balanced": 8, "maximum": 16}
ORDERBOOK_RECONCILE_INTERVAL = 5
ORDERBOOK_ACTIVE_SYMBOL_TTL = 45
ORDERBOOK_BOOK_RETENTION = 60
ORDERBOOK_MAX_LEVELS = {
    "binance": ORDERBOOK_BINANCE_DEPTH,
    "bybit": ORDERBOOK_DEPTH_BYBIT,
    "okx": 400,
    "binance_spot": ORDERBOOK_BINANCE_DEPTH,
    "bybit_spot": ORDERBOOK_DEPTH_BYBIT,
    "okx_spot": 400,
}

# Signal levels are supplied by the Screener and synchronized to the
# external alert monitor. There are no hard-coded/test symbols or prices.
LEVEL_TOUCH_TOLERANCE = 0.0001
SIGNAL_COOLDOWN = 10

# Render alert monitor. The Screener sends the complete current set of
# signal levels here; Render persists them to Supabase and monitors exactly
# those symbols on Binance Futures.
ALERT_SERVER_URL = "https://binance-render-test-w2xk.onrender.com"

app = Flask(__name__)

# Render Worker is consumed directly by the local browser, so the worker API
# must allow cross-origin requests. No external CORS dependency is required.
@app.after_request
def add_screener_cors_headers(response):
    if SCREENER_WORKER_MODE:
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        response.headers["Access-Control-Max-Age"] = "600"
    return response

# ============================================================
# STATE
# ============================================================

state_lock = threading.Lock()

# Density pipeline is started/stopped on demand. When runtime_enabled is False,
# no Density order-book sockets, scanner loop, or symbol-refresh worker exist.
DENSITY_RUNTIME_ENABLED = False
DENSITY_PIPELINE_LOCK = threading.Lock()
DENSITY_PIPELINE_THREADS = {}
DENSITY_PIPELINE_WS = {}

STATE = {
    "status": "STARTING",
    "last_error": "",
    "last_update": None,
    "last_message": None,
    "connection_count": 0,
    "reconnect_attempts": 0,
    "coins": {},
    "levels": {},
    "signals": deque(maxlen=100),
    "last_signal_by_level": {},
    "order_books": {},
    "orderbook_status": {
        "binance": "DISCONNECTED",
        "bybit": "DISCONNECTED",
        "okx": "DISCONNECTED",
    },
    "orderbook_last_update": {},
    "orderbook_symbols": list(ORDERBOOK_DEFAULT_SYMBOLS[:ORDERBOOK_SYMBOL_LIMIT]),
    "densities": {},
    "density_last_scan": None,
    "orderbook_active_symbols": {},
    "orderbook_subscription_symbols": {"binance": [], "bybit": [], "okx": [], "binance_spot": [], "bybit_spot": [], "okx_spot": []},
    "orderbook_sockets": {},
    "screener_engine": "worker" if SCREENER_WORKER_MODE else "local",
    "screener_buffer_started_at": None,
    "screener_last_snapshot": None,
    "screener_historical_cache_size": 0,
}


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def density_market_from_exchange(exchange):
    e = str(exchange or "").lower()
    return "spot" if e.endswith("_spot") or e == "spot" else "futures"


def density_exchange_base(exchange):
    e = str(exchange or "").lower()
    return e[:-5] if e.endswith("_spot") else e


def density_source_label(exchange):
    base = density_exchange_base(exchange)
    market = density_market_from_exchange(exchange)
    code = {"binance": "B", "bybit": "BY", "okx": "O"}.get(base, base.upper()[:3])
    return f"{code}-{'S' if market == 'spot' else 'F'}"


def add_error(message):
    with state_lock:
        STATE["last_error"] = str(message)
        STATE["signals"].appendleft({
            "time": utc_now(),
            "type": "ERROR",
            "message": str(message),
        })


def set_status(status):
    with state_lock:
        STATE["status"] = status


# ============================================================
# BINANCE CONNECTION
# ============================================================

def log(message, level="INFO"):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{level}] {message}", flush=True)


def initialize_levels():
    # No local hard-coded levels. Runtime levels come from the Screener's
    # chart drawings and are synchronized to the Render monitor.
    with state_lock:
        STATE["levels"].clear()


def generate_signal(symbol, price, level):
    now = time.time()
    key = symbol

    with state_lock:
        last_signal = STATE["last_signal_by_level"].get(key, 0)
        if now - last_signal < SIGNAL_COOLDOWN:
            return

        STATE["last_signal_by_level"][key] = now
        signal = {
            "time": utc_now(),
            "type": "SIGNAL",
            "symbol": symbol,
            "signal_type": "PRICE_TOUCHED_LEVEL",
            "price": price,
            "level": level["price"],
            "message": f"{symbol} price touched signal level {level['price']}"
        }
        STATE["signals"].appendleft(signal)

    log(f"Signal: {symbol} touched level {level['price']} at {price}")


def check_levels(tickers):
    with state_lock:
        levels = list(STATE["levels"].values())

    for item in tickers:
        symbol = item.get("symbol")
        price = item.get("price")
        if not symbol or price is None:
            continue

        for level in levels:
            if level["symbol"] != symbol or level["status"] != "ACTIVE":
                continue

            distance = abs(price - level["price"]) / level["price"] if level["price"] else 1
            if distance <= LEVEL_TOUCH_TOLERANCE:
                with state_lock:
                    live_level = STATE["levels"].get(symbol)
                    if live_level is not None:
                        live_level["touch_count"] += 1
                        live_level["last_touch"] = utc_now()
                        snapshot = dict(live_level)
                    else:
                        snapshot = level
                generate_signal(symbol, price, snapshot)


def normalize_ticker(item):
    """
    Convert Binance 24h ticker payload into the common internal format.

    The rest of the application should use this normalized structure,
    rather than depending directly on Binance field names.
    """
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
    # Binance normally sends !ticker@arr as a plain list. Accept the
    # wrapped {"data": [...]} form as well so the data engine is resilient
    # to stream/proxy wrappers.
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        payload = payload["data"]
    elif isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        payload = [payload["data"]]
    elif isinstance(payload, dict) and payload.get("e") == "24hrTicker":
        payload = [payload]
    if not isinstance(payload, list):
        return

    changed = 0
    normalized = []

    with state_lock:
        for raw in payload:
            item = normalize_ticker(raw)
            if item is None:
                continue
            # The screener is intentionally USDT-only. Ignore every other
            # quote asset at the data-engine boundary so non-USDT symbols
            # never enter Market State or the client-side search/screener.
            if not str(item.get("symbol") or "").upper().endswith("USDT"):
                continue

            # Preserve calculated analytics when a fresh ticker update arrives.
            # WebSocket/REST ticker updates must not erase NATR/correlation fields.
            old_coin = STATE["coins"].get(item["symbol"], {})
            for key in ("natr", "btc_corr", "volume_spike", "natr_updated_at", "btc_corr_updated_at"):
                if key in old_coin:
                    item[key] = old_coin[key]

            STATE["coins"][item["symbol"]] = item
            normalized.append(item)
            changed += 1

        if changed:
            STATE["last_update"] = utc_now()
            STATE["last_message"] = utc_now()
            # A successful live ticker snapshot supersedes an older REST
            # fallback error. Do not keep showing a stale red error banner
            # while the WebSocket is delivering fresh market data.
            STATE["last_error"] = ""

    if normalized:
        check_levels(normalized)



# ============================================================
# ORDER BOOK DATA LAYER — STAGE 1
# ============================================================


def _density_pipeline_active():
    """Single runtime gate for the resource-consuming Density pipeline.

    UI visibility and backend collection are intentionally tied together: when
    the density map is hidden/disabled, no density order-book subscriptions
    are maintained and the density scanner does not run.
    """
    return bool(DENSITY_ENABLED and DENSITY_RUNTIME_ENABLED and DENSITY_SETTINGS.get("enabled", True))


def _density_interest_score(coin):
    """Cheap deterministic score used by the future interest-filter stage.

    It is deliberately side-effect free and inexpensive. With the filter off,
    this function is never used to exclude symbols.
    """
    try:
        volume = max(0.0, float(coin.get("quoteVolume", 0) or 0))
    except (TypeError, ValueError):
        volume = 0.0
    try:
        change = abs(float(coin.get("priceChangePercent", 0) or 0))
    except (TypeError, ValueError):
        change = 0.0
    try:
        natr = max(0.0, float(coin.get("natr", 0) or 0))
    except (TypeError, ValueError):
        natr = 0.0
    return math.log10(volume + 1.0) * 10.0 + min(change, 50.0) + min(natr, 20.0)


def _orderbook_interest_filter_symbols(coins):
    """Return symbols for the optional future interest-filter stage.

    The stage is intentionally conservative: it only filters when explicitly
    enabled and never invents a symbol outside the screener universe.
    """
    if not bool(DENSITY_SETTINGS.get("interest_filter_enabled", False)):
        return None
    min_volume = max(0.0, float(DENSITY_SETTINGS.get("interest_min_volume_usd", 0) or 0))
    ranked = []
    for coin in coins:
        symbol = str(coin.get("symbol", "")).upper()
        if not symbol.endswith("USDT"):
            continue
        try:
            volume = float(coin.get("quoteVolume", 0) or 0)
        except (TypeError, ValueError):
            volume = 0.0
        if volume < min_volume:
            continue
        ranked.append((_density_interest_score(coin), symbol))
    ranked.sort(reverse=True)
    return [symbol for _, symbol in ranked]


def _orderbook_mode_limit():
    mode = str(ORDERBOOK_LOAD_MODE or "balanced").lower()
    return int(ORDERBOOK_MODE_LIMITS.get(mode, ORDERBOOK_MODE_LIMITS["balanced"]))


def _orderbook_active_symbols():
    now=time.time(); active=[]
    with state_lock:
        items=dict(STATE.get("orderbook_active_symbols", {}))
    for symbol, seen_at in items.items():
        try:
            if now-float(seen_at) <= ORDERBOOK_ACTIVE_SYMBOL_TTL:
                active.append(str(symbol).upper())
        except (TypeError,ValueError):
            continue
    return active


def _orderbook_background_symbols(limit=None):
    limit=int(limit or _orderbook_mode_limit())
    with state_lock:
        coins=list(STATE.get("coins", {}).values())
    filtered_symbols=_orderbook_interest_filter_symbols(coins)
    if filtered_symbols is not None:
        ranked_symbols=filtered_symbols
    else:
        ranked=[]
        for coin in coins:
            symbol=str(coin.get("symbol","")).upper()
            if not symbol.endswith("USDT"): continue
            try: volume=float(coin.get("quoteVolume",0) or 0)
            except (TypeError,ValueError): volume=0.0
            ranked.append((volume,symbol))
        ranked.sort(reverse=True)
        ranked_symbols=[symbol for _,symbol in ranked]
    result=[]
    for symbol in ranked_symbols:
        if symbol not in result: result.append(symbol)
        if len(result)>=limit: break
    for symbol in ORDERBOOK_DEFAULT_SYMBOLS:
        if len(result)>=limit: break
        if symbol not in result: result.append(symbol)
    return result[:limit]


def _orderbook_symbol_candidates():
    if not _density_pipeline_active():
        return []
    limit=_orderbook_mode_limit(); result=[]
    for symbol in _orderbook_active_symbols()+_orderbook_background_symbols(limit):
        symbol=str(symbol).upper()
        if symbol.endswith("USDT") and symbol not in result: result.append(symbol)
        if len(result)>=limit: break
    return result[:limit]


def _orderbook_prune_book(book):
    exchange=str(book.get("exchange") or "").lower()
    max_levels=int(ORDERBOOK_MAX_LEVELS.get(exchange,100))
    bids=book.get("bids",{}); asks=book.get("asks",{})
    if len(bids)>max_levels:
        book["bids"]=dict(sorted(bids.items(),key=lambda x:x[0],reverse=True)[:max_levels])
    if len(asks)>max_levels:
        book["asks"]=dict(sorted(asks.items(),key=lambda x:x[0])[:max_levels])


def _orderbook_cleanup_unsubscribed():
    now=time.time(); desired=set(_orderbook_symbol_candidates())
    with state_lock:
        remove=[]
        for key,book in STATE.get("order_books",{}).items():
            symbol=str(book.get("symbol") or "").upper(); last=book.get("last_update")
            if symbol in desired or not last: continue
            try:
                age=max(0.0,(datetime.now(timezone.utc)-datetime.fromisoformat(last)).total_seconds())
            except (TypeError,ValueError): age=ORDERBOOK_BOOK_RETENTION+1
            if age>ORDERBOOK_BOOK_RETENTION: remove.append(key)
        for key in remove:
            STATE["order_books"].pop(key,None); STATE["orderbook_last_update"].pop(key,None)


def _orderbook_set_active_symbol(symbol):
    symbol=str(symbol or "").upper()
    if not symbol.endswith("USDT"): return
    # IMPORTANT: calculate candidates before taking state_lock.
    # _orderbook_symbol_candidates() itself reads state_lock; calling it while
    # holding the non-reentrant Lock deadlocks the Flask request thread.
    with state_lock:
        STATE.setdefault("orderbook_active_symbols",{})[symbol]=time.time()
    candidates=_orderbook_symbol_candidates()
    with state_lock:
        STATE["orderbook_symbols"]=candidates


def _orderbook_reconcile_socket(ws,exchange,subscribed):
    desired=set(_orderbook_symbol_candidates()); subscribed=set(subscribed or set())
    add=sorted(desired-subscribed); remove=sorted(subscribed-desired)
    if exchange in {"binance", "binance_spot"}:
        if add: ws.send(json.dumps({"method":"SUBSCRIBE","params":[f"{s.lower()}@depth{ORDERBOOK_BINANCE_DEPTH}@{ORDERBOOK_BINANCE_SPEED}" for s in add],"id":int(time.time()*1000)%2147483647}))
        if remove: ws.send(json.dumps({"method":"UNSUBSCRIBE","params":[f"{s.lower()}@depth{ORDERBOOK_BINANCE_DEPTH}@{ORDERBOOK_BINANCE_SPEED}" for s in remove],"id":int(time.time()*1000)%2147483647}))
    elif exchange in {"bybit", "bybit_spot"}:
        if add: ws.send(json.dumps({"op":"subscribe","args":[f"orderbook.{ORDERBOOK_DEPTH_BYBIT}.{s}" for s in add]}))
        if remove: ws.send(json.dumps({"op":"unsubscribe","args":[f"orderbook.{ORDERBOOK_DEPTH_BYBIT}.{s}" for s in remove]}))
    elif exchange in {"okx", "okx_spot"}:
        if add:
            suffix = "-USDT-SWAP" if exchange == "okx" else "-USDT"
            ws.send(json.dumps({"op":"subscribe","args":[{"channel":ORDERBOOK_CHANNEL_OKX,"instId":f"{s[:-4]}{suffix}"} for s in add]}))
        if remove:
            suffix = "-USDT-SWAP" if exchange == "okx" else "-USDT"
            ws.send(json.dumps({"op":"unsubscribe","args":[{"channel":ORDERBOOK_CHANNEL_OKX,"instId":f"{s[:-4]}{suffix}"} for s in remove]}))
    with state_lock:
        STATE["orderbook_subscription_symbols"][exchange]=sorted(desired)
    return desired


def _orderbook_ensure(exchange, symbol):
    key = f"{exchange}:{symbol}"
    with state_lock:
        book = STATE["order_books"].get(key)
        if book is None:
            book = {
                "exchange": exchange,
                "symbol": symbol,
                "bids": {},
                "asks": {},
                "last_update": None,
                "update_id": None,
                "sequence": None,
                "depth": 0,
                "status": "CONNECTING",
            }
            STATE["order_books"][key] = book
        return key


def _orderbook_set_status(exchange, status, message=None):
    with state_lock:
        STATE["orderbook_status"][exchange] = status
        # Optional Spot feeds must not take down the core Futures UI or turn a
        # temporary Spot reconnect into the global red error banner.
        if message and not str(exchange).endswith("_spot"):
            STATE["last_error"] = message


def _orderbook_apply_snapshot(exchange, symbol, bids, asks, update_id=None, sequence=None, depth=0):
    key = _orderbook_ensure(exchange, symbol)
    bid_map = {}
    ask_map = {}
    for row in bids or []:
        try:
            price = float(row[0])
            qty = float(row[1])
        except (TypeError, ValueError, IndexError):
            continue
        if price > 0 and qty > 0:
            bid_map[price] = qty
    for row in asks or []:
        try:
            price = float(row[0])
            qty = float(row[1])
        except (TypeError, ValueError, IndexError):
            continue
        if price > 0 and qty > 0:
            ask_map[price] = qty

    now = utc_now()
    with state_lock:
        book = STATE["order_books"][key]
        book["bids"] = bid_map
        book["asks"] = ask_map
        book["last_update"] = now
        book["update_id"] = update_id
        book["sequence"] = sequence
        book["depth"] = depth or max(len(bid_map), len(ask_map))
        book["status"] = "LIVE"
        _orderbook_prune_book(book)
        STATE["orderbook_last_update"][key] = now


def _orderbook_apply_delta(exchange, symbol, bids, asks, update_id=None, sequence=None):
    key = _orderbook_ensure(exchange, symbol)
    now = utc_now()
    with state_lock:
        book = STATE["order_books"][key]
        for row in bids or []:
            try:
                price = float(row[0])
                qty = float(row[1])
            except (TypeError, ValueError, IndexError):
                continue
            if price <= 0:
                continue
            if qty <= 0:
                book["bids"].pop(price, None)
            else:
                book["bids"][price] = qty

        for row in asks or []:
            try:
                price = float(row[0])
                qty = float(row[1])
            except (TypeError, ValueError, IndexError):
                continue
            if price <= 0:
                continue
            if qty <= 0:
                book["asks"].pop(price, None)
            else:
                book["asks"][price] = qty

        book["last_update"] = now
        if update_id is not None:
            book["update_id"] = update_id
        if sequence is not None:
            book["sequence"] = sequence
        book["status"] = "LIVE"
        _orderbook_prune_book(book)
        STATE["orderbook_last_update"][key] = now


def _orderbook_touch(exchange, symbol, update_id=None, sequence=None):
    key = _orderbook_ensure(exchange, symbol)
    now = utc_now()
    with state_lock:
        book = STATE["order_books"][key]
        book["last_update"] = now
        if update_id is not None:
            book["update_id"] = update_id
        if sequence is not None:
            book["sequence"] = sequence
        book["status"] = "LIVE"
        STATE["orderbook_last_update"][key] = now


def _orderbook_public_snapshot():
    """Return compact normalized books for API/debugging/UI integration.

    Raw books stay server-side. Only a bounded number of top levels are
    exposed so the browser never receives the full exchange feed.
    """
    now = time.time()
    result = []
    with state_lock:
        books = list(STATE["order_books"].values())
        statuses = dict(STATE["orderbook_status"])

    for book in books:
        last_update = book.get("last_update")
        stale = True
        age = None
        if last_update:
            try:
                dt = datetime.fromisoformat(last_update)
                age = max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())
                stale = age > ORDERBOOK_STALE_AFTER
            except (TypeError, ValueError):
                pass

        bids = sorted(book.get("bids", {}).items(), key=lambda x: x[0], reverse=True)[:20]
        asks = sorted(book.get("asks", {}).items(), key=lambda x: x[0])[:20]
        result.append({
            "exchange": book.get("exchange"),
            "symbol": book.get("symbol"),
            "status": "STALE" if stale else book.get("status", "DISCONNECTED"),
            "last_update": last_update,
            "age_seconds": round(age, 3) if age is not None else None,
            "update_id": book.get("update_id"),
            "sequence": book.get("sequence"),
            "bids": [[price, qty] for price, qty in bids],
            "asks": [[price, qty] for price, qty in asks],
        })

    return {"exchanges": statuses, "symbols": _orderbook_symbol_candidates(), "books": result}


def _run_orderbook_ws(exchange,url,subscribe_payload_factory,parser):
    if websocket is None:
        _orderbook_set_status(exchange,"ERROR","Модуль websocket-client не установлен; Order Book отключён."); return
    while True:
        if not _density_pipeline_active():
            _orderbook_set_status(exchange, "DISABLED")
            return
        ws=None; subscribed=set(); next_reconcile=0.0
        try:
            _orderbook_set_status(exchange,"CONNECTING")
            ws=websocket.create_connection(url,timeout=2,enable_multithread=True,ping_interval=20,ping_timeout=10)
            with state_lock:
                STATE.setdefault("orderbook_sockets", {})[exchange] = ws
                DENSITY_PIPELINE_WS[exchange] = ws
            _orderbook_set_status(exchange,"CONNECTED")
            subscribed=_orderbook_reconcile_socket(ws,exchange,set())
            next_reconcile=time.time()+ORDERBOOK_RECONCILE_INTERVAL
            silent_since=time.time()
            while True:
                if not _density_pipeline_active():
                    break
                now=time.time()
                if now>=next_reconcile:
                    subscribed=_orderbook_reconcile_socket(ws,exchange,subscribed)
                    _orderbook_cleanup_unsubscribed()
                    next_reconcile=now+ORDERBOOK_RECONCILE_INTERVAL
                try: raw=ws.recv()
                except websocket.WebSocketTimeoutException:
                    if time.time()-silent_since>=ORDERBOOK_STALE_AFTER: raise TimeoutError(f"{exchange}: Order Book data is stale")
                    continue
                if raw is None: raise ConnectionError(f"{exchange}: WebSocket closed")
                silent_since=time.time()
                try: payload=json.loads(raw)
                except json.JSONDecodeError: continue
                parser(payload)
        except Exception as exc:
            if not _density_pipeline_active():
                _orderbook_set_status(exchange, "DISABLED")
                return
            _orderbook_set_status(exchange,"RECONNECTING",f"{exchange} Order Book: {exc}")
            log(f"{exchange} Order Book error: {exc}","ERROR"); time.sleep(RECONNECT_DELAY)
        finally:
            with state_lock:
                if STATE.get("orderbook_sockets", {}).get(exchange) is ws:
                    STATE["orderbook_sockets"].pop(exchange, None)
                DENSITY_PIPELINE_WS.pop(exchange, None)
            try:
                if ws is not None: ws.close()
            except Exception: pass


def _binance_orderbook_subscribe_payload():
    symbols = _orderbook_symbol_candidates()
    params = [
        f"{symbol.lower()}@depth{ORDERBOOK_BINANCE_DEPTH}@{ORDERBOOK_BINANCE_SPEED}"
        for symbol in symbols
    ]
    return {
        "method": "SUBSCRIBE",
        "params": params,
        "id": int(time.time() * 1000) % 2147483647,
    }


def _parse_binance_orderbook(payload):
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        payload = payload["data"]
    if not isinstance(payload, dict):
        return
    if payload.get("e") != "depthUpdate":
        return
    symbol = str(payload.get("s", "")).upper()
    if not symbol:
        return
    # Partial-depth messages are bounded top-of-book snapshots/updates.
    # Keep only what Binance actually supplies; no synthetic levels are made.
    _orderbook_apply_snapshot(
        "binance",
        symbol,
        payload.get("b", []),
        payload.get("a", []),
        update_id=payload.get("u"),
        sequence=payload.get("pu"),
        depth=ORDERBOOK_BINANCE_DEPTH,
    )


def _parse_binance_spot_orderbook(payload):
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        payload = payload["data"]
    if not isinstance(payload, dict) or payload.get("e") != "depthUpdate":
        return
    symbol = str(payload.get("s", "")).upper()
    if not symbol:
        return
    _orderbook_apply_snapshot("binance_spot", symbol, payload.get("b", []), payload.get("a", []),
                              update_id=payload.get("u"), sequence=payload.get("U"), depth=ORDERBOOK_BINANCE_DEPTH)


def _bybit_orderbook_subscribe_payload():
    symbols = _orderbook_symbol_candidates()
    return {
        "op": "subscribe",
        "args": [f"orderbook.{ORDERBOOK_DEPTH_BYBIT}.{symbol}" for symbol in symbols],
    }


def _parse_bybit_orderbook(payload):
    if not isinstance(payload, dict):
        return
    topic = str(payload.get("topic", ""))
    if not topic.startswith("orderbook."):
        return
    data = payload.get("data") or {}
    symbol = str(data.get("s", "")).upper()
    if not symbol:
        parts = topic.split(".")
        symbol = parts[-1].upper() if parts else ""
    if not symbol:
        return
    message_type = payload.get("type")
    bids = data.get("b", [])
    asks = data.get("a", [])
    if message_type == "snapshot":
        _orderbook_apply_snapshot(
            "bybit", symbol, bids, asks,
            update_id=data.get("u"),
            sequence=data.get("seq"),
            depth=ORDERBOOK_DEPTH_BYBIT,
        )
    elif message_type == "delta":
        _orderbook_apply_delta(
            "bybit", symbol, bids, asks,
            update_id=data.get("u"),
            sequence=data.get("seq"),
        )


def _parse_bybit_spot_orderbook(payload):
    if not isinstance(payload, dict):
        return
    topic = str(payload.get("topic", ""))
    if not topic.startswith("orderbook."):
        return
    data = payload.get("data") or {}
    symbol = str(data.get("s", "")).upper()
    if not symbol:
        parts = topic.split(".")
        symbol = parts[-1].upper() if parts else ""
    if not symbol:
        return
    if payload.get("type") == "snapshot":
        _orderbook_apply_snapshot("bybit_spot", symbol, data.get("b", []), data.get("a", []),
                                   update_id=data.get("u"), sequence=data.get("seq"), depth=ORDERBOOK_DEPTH_BYBIT)
    elif payload.get("type") == "delta":
        _orderbook_apply_delta("bybit_spot", symbol, data.get("b", []), data.get("a", []),
                                update_id=data.get("u"), sequence=data.get("seq"))


def _okx_orderbook_subscribe_payload():
    symbols = _orderbook_symbol_candidates()
    return {
        "op": "subscribe",
        "args": [
            {"channel": ORDERBOOK_CHANNEL_OKX, "instId": f"{symbol[:-4]}-USDT-SWAP"}
            for symbol in symbols
        ],
    }


def _parse_okx_orderbook(payload):
    if not isinstance(payload, dict):
        return
    arg = payload.get("arg") or {}
    if arg.get("channel") not in {"books", "books5", "books-rpi"}:
        return
    data_list = payload.get("data") or []
    for data in data_list:
        inst_id = str(data.get("instId", ""))
        if not inst_id:
            continue
        symbol = inst_id.replace("-SWAP", "").replace("-", "")
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        seq_id = data.get("seqId")
        prev_seq_id = data.get("prevSeqId")
        action = data.get("action")
        # OKX books sends a full snapshot first, then incremental changes.
        # A missing/unknown action is treated as a snapshot for robustness.
        if action == "snapshot" or action is None:
            _orderbook_apply_snapshot(
                "okx", symbol, bids, asks,
                update_id=seq_id,
                sequence=prev_seq_id,
                depth=max(len(bids), len(asks)),
            )
        else:
            _orderbook_apply_delta(
                "okx", symbol, bids, asks,
                update_id=seq_id,
                sequence=prev_seq_id,
            )


def _parse_okx_spot_orderbook(payload):
    if not isinstance(payload, dict):
        return
    arg = payload.get("arg") or {}
    if arg.get("channel") not in {"books", "books5"}:
        return
    for data in payload.get("data") or []:
        inst_id = str(data.get("instId", ""))
        if not inst_id or not inst_id.endswith("-USDT"):
            continue
        symbol = inst_id.replace("-", "")
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        seq_id = data.get("seqId")
        prev_seq_id = data.get("prevSeqId")
        if data.get("action") in {"snapshot", None}:
            _orderbook_apply_snapshot("okx_spot", symbol, bids, asks, update_id=seq_id, sequence=prev_seq_id, depth=max(len(bids), len(asks)))
        else:
            _orderbook_apply_delta("okx_spot", symbol, bids, asks, update_id=seq_id, sequence=prev_seq_id)


def orderbook_worker_bybit():
    if not ORDERBOOK_ENABLED or "bybit" not in ORDERBOOK_EXCHANGES:
        return
    _run_orderbook_ws(
        "bybit",
        "wss://stream.bybit.com/v5/public/linear",
        _bybit_orderbook_subscribe_payload,
        _parse_bybit_orderbook,
    )


def orderbook_worker_okx():
    if not ORDERBOOK_ENABLED or "okx" not in ORDERBOOK_EXCHANGES:
        return
    _run_orderbook_ws(
        "okx",
        "wss://ws.okx.com:8443/ws/v5/public",
        _okx_orderbook_subscribe_payload,
        _parse_okx_orderbook,
    )


def orderbook_worker_binance_spot():
    if not ORDERBOOK_ENABLED:
        return
    _run_orderbook_ws("binance_spot", "wss://stream.binance.com:9443/ws",
                      lambda: {"method":"SUBSCRIBE","params":[f"{s.lower()}@depth{ORDERBOOK_BINANCE_DEPTH}@{ORDERBOOK_BINANCE_SPEED}" for s in _orderbook_symbol_candidates()],"id":int(time.time()*1000)%2147483647},
                      _parse_binance_spot_orderbook)


def orderbook_worker_bybit_spot():
    if not ORDERBOOK_ENABLED:
        return
    _run_orderbook_ws("bybit_spot", "wss://stream.bybit.com/v5/public/spot",
                      lambda: {"op":"subscribe","args":[f"orderbook.{ORDERBOOK_DEPTH_BYBIT}.{s}" for s in _orderbook_symbol_candidates()]},
                      _parse_bybit_spot_orderbook)


def orderbook_worker_okx_spot():
    if not ORDERBOOK_ENABLED:
        return
    _run_orderbook_ws("okx_spot", "wss://ws.okx.com:8443/ws/v5/public",
                      lambda: {"op":"subscribe","args":[{"channel":ORDERBOOK_CHANNEL_OKX,"instId":f"{s[:-4]}-USDT"} for s in _orderbook_symbol_candidates()]},
                      _parse_okx_spot_orderbook)


def orderbook_symbol_refresh_worker():
    """Publish the currently selected symbol set for later dynamic routing.

    Stage 1 keeps existing subscriptions stable; this worker only updates the
    shared state. Stage 3 will use it for live subscribe/unsubscribe routing.
    """
    while _density_pipeline_active():
        try:
            symbols = _orderbook_symbol_candidates()
            with state_lock:
                STATE["orderbook_symbols"] = list(symbols)
        except Exception as exc:
            log(f"Order Book symbol discovery: {exc}", "ERROR")
        if not _density_pipeline_active():
            break
        time.sleep(ORDERBOOK_REFRESH_SYMBOLS)



# ============================================================
# DENSITY ENGINE — STAGE 2
# ============================================================

def _density_price_key(price):
    """Stable price key for matching the same cluster between scans."""
    return round(float(price), 8)


def _density_current_price(symbol, exchange=None):
    with state_lock:
        # For density distance, always prefer the midpoint of the exact
        # exchange/product order book. Using Binance ticker for a Bybit/OKX
        # density would make the distance drift away from that book's spread.
        if exchange:
            book = STATE.get("order_books", {}).get(f"{exchange}:{symbol}")
            if book:
                bids = book.get("bids", {})
                asks = book.get("asks", {})
                if bids and asks:
                    try:
                        return (max(bids) + min(asks)) / 2.0
                    except (TypeError, ValueError):
                        pass

        market = density_market_from_exchange(exchange)
        if market == "futures":
            coin = STATE.get("coins", {}).get(symbol)
            if coin:
                try:
                    return float(coin.get("price", 0) or 0)
                except (TypeError, ValueError):
                    pass

        # Last-resort fallback to another same-product book.
        preferred = [f"{x}_{market}" if market == "spot" else x for x in ORDERBOOK_EXCHANGES]
        for ex in preferred:
            book = STATE.get("order_books", {}).get(f"{ex}:{symbol}")
            if not book:
                continue
            bids = book.get("bids", {})
            asks = book.get("asks", {})
            if bids and asks:
                try:
                    return (max(bids) + min(asks)) / 2.0
                except (TypeError, ValueError):
                    pass
    return None


def _density_book_snapshot():
    with state_lock:
        books = []
        for book in STATE.get("order_books", {}).values():
            books.append({
                "exchange": book.get("exchange"),
                "symbol": book.get("symbol"),
                "bids": dict(book.get("bids", {})),
                "asks": dict(book.get("asks", {})),
                "last_update": book.get("last_update"),
                "status": book.get("status"),
            })
    return books


def _density_cluster_levels(levels, current_price):
    """Cluster adjacent price levels using a bounded adaptive price gap.

    The gap is expressed as a percentage of the local price, so the same
    approach works across BTC and lower-priced altcoins. We intentionally
    keep this deterministic and cheap: sorting the already bounded order book
    is the expensive part, while clustering is linear after sorting.
    """
    if not levels:
        return []

    levels = sorted(levels, key=lambda row: row[0])
    clusters = []
    current = [levels[0]]

    for row in levels[1:]:
        prev_price = current[-1][0]
        price = row[0]
        local_gap = abs(price - prev_price) / max(abs(prev_price), 1e-12) * 100.0

        # Slightly adapt the configured gap to distance from market. Farther
        # levels may be clustered a little more aggressively, but never more
        # than 2x the configured base gap.
        distance_pct = abs(price - current_price) / current_price * 100.0 if current_price else 0
        adaptive_gap = DENSITY_CLUSTER_GAP_PCT * min(2.0, 1.0 + distance_pct / max(DENSITY_MAX_DISTANCE_PCT, 0.1))

        if local_gap <= adaptive_gap:
            current.append(row)
        else:
            clusters.append(current)
            current = [row]

    clusters.append(current)
    return clusters


def _density_make_object(exchange, symbol, side, cluster, current_price, now_ts):
    if not cluster or not current_price or current_price <= 0:
        return None

    price_low = min(row[0] for row in cluster)
    price_high = max(row[0] for row in cluster)
    total_usd = sum(row[0] * row[1] for row in cluster)
    total_coin = sum(row[1] for row in cluster)
    anchor_price = (price_low + price_high) / 2.0

    if total_usd < float(DENSITY_SETTINGS.get("min_usd", DENSITY_MIN_USD)):
        return None

    distance_pct = (anchor_price - current_price) / current_price * 100.0
    if abs(distance_pct) > float(DENSITY_SETTINGS.get("max_distance_percent", DENSITY_MAX_DISTANCE_PCT)):
        return None
    if abs(distance_pct) < float(DENSITY_SETTINGS.get("min_distance_percent", DENSITY_MIN_DISTANCE_PCT)):
        return None

    if side == "BUY" and anchor_price >= current_price:
        return None
    if side == "SELL" and anchor_price <= current_price:
        return None

    return {
        "exchange": exchange,
        "symbol": symbol,
        "side": side,
        "price": anchor_price,
        "price_low": price_low,
        "price_high": price_high,
        "current_usd": total_usd,
        "current_coin": total_coin,
        "distance_price": anchor_price - current_price,
        "distance_percent": distance_pct,
        "initial_usd": total_usd,
        "initial_coin": total_coin,
        "consumed_usd": 0.0,
        "consumed_percent": 0.0,
        "created_at_ts": now_ts,
        "last_update_ts": now_ts,
        "status": "ACTIVE",
        "change_percent": 0.0,
        "strength": None,
    }


def _density_match(previous, candidate):
    """Return True when candidate is the same live density zone."""
    if not previous or not candidate:
        return False
    if previous.get("exchange") != candidate.get("exchange"):
        return False
    if previous.get("symbol") != candidate.get("symbol"):
        return False
    if previous.get("side") != candidate.get("side"):
        return False

    old_mid = float(previous.get("price", 0) or 0)
    new_mid = float(candidate.get("price", 0) or 0)
    if old_mid <= 0 or new_mid <= 0:
        return False

    gap_pct = abs(new_mid - old_mid) / old_mid * 100.0
    return gap_pct <= max(DENSITY_CLUSTER_GAP_PCT * 2.0, 0.10)


def _density_update_lifecycle(candidates, now_ts):
    """Merge the current scan with the previous live density state."""
    with state_lock:
        previous = list(STATE.get("densities", {}).values())

    used_previous = set()
    updated = {}

    for candidate in candidates:
        best_index = None
        best_gap = None

        for index, old in enumerate(previous):
            if index in used_previous or not _density_match(old, candidate):
                continue
            gap = abs(float(old.get("price", 0)) - float(candidate.get("price", 0)))
            if best_gap is None or gap < best_gap:
                best_gap = gap
                best_index = index

        if best_index is not None:
            old = previous[best_index]
            used_previous.add(best_index)

            initial_usd = float(old.get("initial_usd", old.get("current_usd", 0)) or 0)
            old_current = float(old.get("current_usd", 0) or 0)
            current_usd = float(candidate.get("current_usd", 0) or 0)

            consumed_usd = max(0.0, initial_usd - current_usd)
            consumed_percent = (consumed_usd / initial_usd * 100.0) if initial_usd > 0 else 0.0
            change_percent = ((current_usd - old_current) / old_current * 100.0) if old_current > 0 else 0.0

            candidate["initial_usd"] = initial_usd
            candidate["initial_coin"] = float(old.get("initial_coin", candidate.get("initial_coin", 0)) or 0)
            candidate["consumed_usd"] = consumed_usd
            candidate["consumed_percent"] = consumed_percent
            candidate["change_percent"] = change_percent
            candidate["created_at_ts"] = float(old.get("created_at_ts", now_ts))
            candidate["last_update_ts"] = now_ts
            candidate["status"] = "REDUCED" if current_usd < old_current else (
                "STRENGTHENED" if current_usd > old_current else "ACTIVE"
            )
            candidate["strength"] = old.get("strength")

        density_id = (
            f"{candidate['exchange']}:{candidate['symbol']}:"
            f"{candidate['side']}:{_density_price_key(candidate['price'])}"
        )
        updated[density_id] = candidate

    # Anything not matched disappeared from the current book. We deliberately
    # remove it from ACTIVE state rather than keeping ghost densities.
    with state_lock:
        STATE["densities"] = updated
        STATE["density_last_scan"] = utc_now()

    return list(updated.values())



def _density_aggregate_cross_exchange(densities):
    """Presentation-only aggregation of nearby densities across exchanges."""
    groups = []
    ordered = sorted(
        densities,
        key=lambda d: (
            str(d.get("symbol", "")),
            str(d.get("side", "")),
            float(d.get("price", 0) or 0),
        ),
    )

    for density in ordered:
        symbol = str(density.get("symbol", "")).upper()
        side = str(density.get("side", "")).upper()
        price = float(density.get("price", 0) or 0)
        if not symbol or not side or price <= 0:
            continue

        target = None
        for group in reversed(groups):
            if group["symbol"] != symbol or group["side"] != side:
                continue
            if abs(price - group["price"]) / price * 100.0 <= DENSITY_AGGREGATION_GAP_PCT:
                target = group
                break
            if group["price"] < price:
                break

        if target is None:
            target = {
                "symbol": symbol,
                "side": side,
                "price": price,
                "price_low": float(density.get("price_low", price) or price),
                "price_high": float(density.get("price_high", price) or price),
                "current_usd": 0.0,
                "exchange_count": 0,
                "exchanges": [],
                "nearest_distance_percent": density.get("distance_percent"),
                "consumed_percent": 0.0,
            }
            groups.append(target)

        target["current_usd"] += float(density.get("current_usd", 0) or 0)
        target["price_low"] = min(target["price_low"], float(density.get("price_low", price) or price))
        target["price_high"] = max(target["price_high"], float(density.get("price_high", price) or price))

        exchange = str(density.get("exchange", ""))
        if exchange and exchange not in target["exchanges"]:
            target["exchanges"].append(exchange)
            target["exchange_count"] += 1

        distance = density.get("distance_percent")
        if distance is not None:
            distance = abs(float(distance))
            if target["nearest_distance_percent"] is None or distance < abs(float(target["nearest_distance_percent"])):
                target["nearest_distance_percent"] = distance

        target["consumed_percent"] = max(
            target["consumed_percent"],
            float(density.get("consumed_percent", 0) or 0),
        )

    for group in groups:
        group["exchanges"].sort()
        group["strength"] = "MULTI_EXCHANGE" if group["exchange_count"] >= 2 else "SINGLE_EXCHANGE"

    groups.sort(
        key=lambda x: (
            -float(x["current_usd"]),
            x["nearest_distance_percent"] if x["nearest_distance_percent"] is not None else 999999,
        )
    )
    return groups[:DENSITY_AGGREGATION_MAX_ITEMS]


def _density_scan_once():
    if not _density_pipeline_active():
        with state_lock:
            STATE["density_last_scan"] = time.time()
            STATE["densities"] = {}
        return []

    books = _density_book_snapshot()
    candidates = []

    for book in books:
        exchange = str(book.get("exchange") or "")
        symbol = str(book.get("symbol") or "").upper()
        if not exchange or not symbol:
            continue

        if symbol in {str(x).upper() for x in DENSITY_SETTINGS.get("blacklist", [])}:
            continue

        market = density_market_from_exchange(exchange)
        if market == "spot" and not bool(DENSITY_SETTINGS.get("show_spot", True)):
            continue
        if market == "futures" and not bool(DENSITY_SETTINGS.get("show_futures", True)):
            continue
        base_exchange = density_exchange_base(exchange)
        enabled_exchanges = {str(x).lower() for x in DENSITY_SETTINGS.get("exchanges", ["binance", "bybit", "okx"])}
        if base_exchange not in enabled_exchanges:
            continue

        current_price = _density_current_price(symbol, exchange)
        if not current_price or current_price <= 0:
            continue

        for side, levels_map in (
            ("BUY", book.get("bids", {}) if DENSITY_SHOW_BUY else {}),
            ("SELL", book.get("asks", {}) if DENSITY_SHOW_SELL else {}),
        ):
            levels = []
            for price, qty in levels_map.items():
                try:
                    price = float(price)
                    qty = float(qty)
                except (TypeError, ValueError):
                    continue
                if price <= 0 or qty <= 0:
                    continue

                distance_pct = abs(price - current_price) / current_price * 100.0
                if distance_pct < DENSITY_MIN_DISTANCE_PCT:
                    continue
                if distance_pct > DENSITY_MAX_DISTANCE_PCT:
                    continue

                levels.append((price, qty))

            clusters = _density_cluster_levels(levels, current_price)

            side_candidates = []
            for cluster in clusters:
                density = _density_make_object(
                    exchange, symbol, side, cluster, current_price, time.time()
                )
                if density is not None:
                    side_candidates.append(density)

            side_candidates.sort(key=lambda item: item["current_usd"], reverse=True)
            candidates.extend(side_candidates[:DENSITY_MAX_PER_SIDE])

    return _density_update_lifecycle(candidates, time.time())


def _density_public_snapshot():
    now = time.time()
    with state_lock:
        densities = list(STATE.get("densities", {}).values())
        last_scan = STATE.get("density_last_scan")

    result = []
    for item in densities:
        lifetime_seconds = max(0.0, now - float(item.get("created_at_ts", now)))
        market = density_market_from_exchange(item.get("exchange"))
        consumed_percent = max(0.0, min(100.0, float(item.get("consumed_percent", 0) or 0)))
        remaining_percent = max(0.0, 100.0 - consumed_percent)
        initial_usd = max(0.0, float(item.get("initial_usd", 0) or 0))
        current_usd = max(0.0, float(item.get("current_usd", 0) or 0))
        lifetime_minutes = lifetime_seconds / 60.0
        size_score = min(50.0, 10.0 * math.log10(max(initial_usd, 1.0) / 100000.0 + 1.0))
        life_score = min(25.0, lifetime_minutes * 3.0)
        stability_score = min(15.0, remaining_percent * 0.15)
        distance_score = max(0.0, 10.0 - abs(float(item.get("distance_percent", 0) or 0)) * 2.0)
        strength_score = round(max(0.0, min(100.0, size_score + life_score + stability_score + distance_score)), 1)
        strength_label = "LOW" if strength_score < 35 else ("MEDIUM" if strength_score < 65 else "HIGH")
        result.append({
            "id": (
                f"{item.get('exchange')}:{item.get('symbol')}:"
                f"{item.get('side')}:{_density_price_key(item.get('price', 0))}"
            ),
            "exchange": item.get("exchange"),
            "exchange_label": density_source_label(item.get("exchange")),
            "market": market,
            "symbol": item.get("symbol"),
            "side": item.get("side"),
            "price": round(float(item.get("price", 0)), 10),
            "price_low": round(float(item.get("price_low", 0)), 10),
            "price_high": round(float(item.get("price_high", 0)), 10),
            "current_usd": round(current_usd, 2),
            "current_coin": round(float(item.get("current_coin", 0)), 10),
            "initial_usd": round(float(item.get("initial_usd", 0)), 2),
            "initial_coin": round(float(item.get("initial_coin", 0)), 10),
            "consumed_usd": round(float(item.get("consumed_usd", 0)), 2),
            "consumed_percent": round(consumed_percent, 2),
            "remaining_percent": round(remaining_percent, 2),
            "change_percent": round(float(item.get("change_percent", 0)), 2),
            "distance_price": round(float(item.get("distance_price", 0)), 10),
            "distance_percent": round(float(item.get("distance_percent", 0)), 4),
            "lifetime_seconds": round(lifetime_seconds, 1),
            "lifetime_label": "< 1 мин" if lifetime_seconds < 60 else f"{int(lifetime_seconds // 60)} мин",
            "status": item.get("status", "ACTIVE"),
            "strength": strength_label,
            "strength_score": strength_score,
        })

    result.sort(key=lambda item: (item["symbol"], item["side"], -item["current_usd"]))
    return {
        "enabled": DENSITY_ENABLED,
        "last_scan": last_scan,
        "count": len(result),
        "densities": result,
        "aggregated": _density_aggregate_cross_exchange(result),
    }


def density_engine_worker():
    while _density_pipeline_active():
        try:
            _density_scan_once()
        except Exception as exc:
            log(f"Density Engine failed: {exc}", "ERROR")
        if not _density_pipeline_active():
            break
        time.sleep(DENSITY_SCAN_INTERVAL)
    with state_lock:
        STATE["densities"] = {}


def _density_pipeline_thread_targets():
    return {
        "bybit": orderbook_worker_bybit,
        "okx": orderbook_worker_okx,
        "binance_spot": orderbook_worker_binance_spot,
        "bybit_spot": orderbook_worker_bybit_spot,
        "okx_spot": orderbook_worker_okx_spot,
        "symbol_refresh": orderbook_symbol_refresh_worker,
        "engine": density_engine_worker,
    }


def start_density_pipeline():
    """Start all resource-consuming Density workers only when requested."""
    global DENSITY_RUNTIME_ENABLED
    with DENSITY_PIPELINE_LOCK:
        DENSITY_RUNTIME_ENABLED = True
        targets = _density_pipeline_thread_targets()
        for name, target in targets.items():
            thread = DENSITY_PIPELINE_THREADS.get(name)
            if thread is not None and thread.is_alive():
                continue
            thread = threading.Thread(target=target, daemon=True, name=f"density-{name}")
            DENSITY_PIPELINE_THREADS[name] = thread
            thread.start()
    return True


def stop_density_pipeline():
    """Stop Density collection immediately and release its network resources."""
    global DENSITY_RUNTIME_ENABLED
    with DENSITY_PIPELINE_LOCK:
        DENSITY_RUNTIME_ENABLED = False
        with state_lock:
            sockets = list(STATE.get("orderbook_sockets", {}).values())
            binance_ws = STATE.get("binance_market_socket")
            binance_subscribed = list(STATE.get("orderbook_subscription_symbols", {}).get("binance", []))
            STATE["orderbook_subscription_symbols"] = {
                "binance": [], "bybit": [], "okx": [],
                "binance_spot": [], "bybit_spot": [], "okx_spot": [],
            }
            STATE["orderbook_symbols"] = []
            STATE["densities"] = {}
        # Binance Futures shares the core ticker socket. Remove only its depth
        # subscriptions; the normal ticker stream remains alive.
        if binance_ws is not None and binance_subscribed:
            try:
                binance_ws.send(json.dumps({
                    "method": "UNSUBSCRIBE",
                    "params": [f"{s.lower()}@depth{ORDERBOOK_BINANCE_DEPTH}@{ORDERBOOK_BINANCE_SPEED}" for s in binance_subscribed],
                    "id": int(time.time() * 1000) % 2147483647,
                }))
            except Exception:
                pass
        for ws in sockets:
            try:
                ws.close()
            except Exception:
                pass
    return True


def binance_worker():
    if websocket is None:
        message = "Модуль websocket-client не установлен."
        set_status("ERROR")
        add_error(message)
        log(message, "ERROR")
        return

    # Connect to the Futures WebSocket endpoint and explicitly subscribe to
    # the documented all-market ticker stream. This avoids relying on URL
    # combined-stream parsing through a proxy/network filter.
    ws_url = "wss://fstream.binance.com/market/ws"

    while True:
        ws = None
        try:
            set_status("CONNECTING")
            log(f"Connecting to Binance WebSocket: {ws_url}")

            ws = websocket.create_connection(
                ws_url,
                timeout=10,
                enable_multithread=True,
                ping_interval=20,
                ping_timeout=10,
            )

            with state_lock:
                STATE["connection_count"] += 1
                STATE["reconnect_attempts"] = 0
                STATE["binance_market_socket"] = ws

            subscribe = {
                "method": "SUBSCRIBE",
                "params": ["!ticker@arr"],
                "id": int(time.time() * 1000) % 2147483647,
            }
            ws.send(json.dumps(subscribe))
            subscribed_depth = set()
            if _density_pipeline_active():
                subscribed_depth = _orderbook_reconcile_socket(ws,"binance",set())
            next_depth_reconcile = time.time() + ORDERBOOK_RECONCILE_INTERVAL
            log(
                "Binance WebSocket connected; subscribed to !ticker@arr "
                + (f"+ {len(subscribed_depth)} adaptive order-book streams" if subscribed_depth else "(Density order book OFF)")
            )

            silent_since = time.time()

            while True:
                if time.time() >= next_depth_reconcile:
                    if _density_pipeline_active():
                        subscribed_depth = _orderbook_reconcile_socket(ws,"binance",subscribed_depth)
                        _orderbook_cleanup_unsubscribed()
                    elif subscribed_depth:
                        if subscribed_depth:
                            ws.send(json.dumps({"method":"UNSUBSCRIBE","params":[f"{s.lower()}@depth{ORDERBOOK_BINANCE_DEPTH}@{ORDERBOOK_BINANCE_SPEED}" for s in subscribed_depth],"id":int(time.time()*1000)%2147483647}))
                        subscribed_depth = set()
                        with state_lock:
                            STATE["orderbook_subscription_symbols"]["binance"] = []
                    next_depth_reconcile = time.time() + ORDERBOOK_RECONCILE_INTERVAL
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    if time.time() - silent_since >= BINANCE_DATA_TIMEOUT:
                        raise TimeoutError("WebSocket подключён, но ticker-данные не приходят")
                    continue

                if raw is None:
                    raise ConnectionError("Binance closed the WebSocket connection")

                silent_since = time.time()
                with state_lock:
                    STATE["last_message"] = utc_now()

                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Subscription acknowledgement: {"result": null, "id": ...}
                if (
                    isinstance(payload, dict)
                    and "id" in payload
                    and "result" in payload
                    and "stream" not in payload
                    and "data" not in payload
                ):
                    if payload.get("result") is None:
                        log("Binance subscription confirmed")
                    else:
                        raise RuntimeError(f"Binance subscription error: {payload}")
                    continue

                # Accept a combined wrapper too.
                if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
                    payload = payload["data"]

                # Stage 1 order-book stream shares the existing Binance market
                # connection; no second Binance WebSocket is opened.
                if isinstance(payload, dict) and payload.get("e") == "depthUpdate":
                    _parse_binance_orderbook(payload)
                    continue

                if not (
                    isinstance(payload, list)
                    or (isinstance(payload, dict) and payload.get("e") == "24hrTicker")
                ):
                    continue

                process_tickers(payload)
                with state_lock:
                    after = len(STATE.get("coins", {}))
                    received_at = STATE.get("last_update")

                if received_at and after > 0:
                    set_status("LIVE")
                    log(f"Binance WebSocket market data received: {after} tickers") if after and after < 2 else None

        except Exception as exc:
            with state_lock:
                STATE["reconnect_attempts"] += 1
                attempt = STATE["reconnect_attempts"]
                has_data = bool(STATE.get("coins")) and bool(STATE.get("last_update"))

            set_status("LIVE" if has_data else "RECONNECTING")
            add_error(f"Binance WebSocket: {exc}")
            log(f"Binance WebSocket error: {exc}", "ERROR")
            log(f"Reconnect attempt #{attempt}")
            time.sleep(RECONNECT_DELAY)

        finally:
            with state_lock:
                if STATE.get("binance_market_socket") is ws:
                    STATE["binance_market_socket"] = None
            try:
                if ws is not None:
                    ws.close()
            except Exception:
                pass


def fetch_binance_rest_tickers():
    """Fetch a real Binance USDⓈ-M Futures 24h ticker snapshot.

    Try the documented Futures REST host first, then Binance alternate API
    hosts if the first hostname is unavailable from the user's network.
    """
    errors = []
    for url in BINANCE_REST_URLS:
        try:
            request = Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 CryptoScreener/1.0",
                    "Accept": "application/json",
                },
            )
            with urlopen(request, timeout=8) as response:
                payload = json.loads(response.read().decode("utf-8"))

            if not isinstance(payload, list):
                raise ValueError(
                    f"unexpected payload: {type(payload).__name__}"
                )
            if not payload:
                raise ValueError("empty ticker list")
            return payload, url
        except Exception as exc:
            errors.append(f"{url}: {exc}")

    raise ConnectionError("; ".join(errors))



def binance_rest_fallback_worker():
    """Bootstrap market data and recover it when WebSocket data is stale.

    WebSocket remains the realtime source. REST is used only to make sure the
    screener has a real initial snapshot and to recover if the stream becomes
    silent. Empty Min/Max settings never affect this data acquisition layer.
    """
    while True:
        try:
            with state_lock:
                last_update = STATE.get("last_update")
                coin_count = len(STATE.get("coins", {}))

            stale = True
            if last_update:
                try:
                    last_dt = datetime.fromisoformat(last_update)
                    stale = (datetime.now(timezone.utc) - last_dt).total_seconds() > BINANCE_DATA_TIMEOUT
                except (TypeError, ValueError):
                    stale = True

            # Always bootstrap if no market snapshot exists.
            if coin_count == 0 or stale:
                tickers, source_url = fetch_binance_rest_tickers()
                if tickers:
                    process_tickers(tickers)
                    with state_lock:
                        received = len(STATE.get("coins", {}))
                        STATE["last_message"] = utc_now()
                    if received:
                        set_status("LIVE")
                        log(f"Binance REST snapshot: received {received} tickers from {source_url}")
                else:
                    message = "Binance REST returned an empty ticker list"
                    with state_lock:
                        STATE["last_error"] = message
                    log(message, "ERROR")
        except Exception as exc:
            message = f"Binance REST fallback failed: {exc}"
            with state_lock:
                current_last_update = STATE.get("last_update")
                current_coin_count = len(STATE.get("coins", {}))
                ws_data_fresh = False
                if current_last_update:
                    try:
                        current_dt = datetime.fromisoformat(current_last_update)
                        ws_data_fresh = (datetime.now(timezone.utc) - current_dt).total_seconds() <= BINANCE_DATA_TIMEOUT
                    except (TypeError, ValueError):
                        ws_data_fresh = False
                # REST is only a fallback. If the WebSocket is healthy, a
                # REST timeout must not become a user-facing market error.
                if not (current_coin_count > 0 and ws_data_fresh):
                    STATE["last_error"] = message
                    if current_coin_count == 0:
                        STATE["status"] = "RECONNECTING"
            log(message, "WARNING" if ws_data_fresh else "ERROR")

        time.sleep(BINANCE_REST_INTERVAL)


# ============================================================
# SCREENER MARKET STATE / ROLLING BUFFER / HISTORICAL POINT CACHE
# ============================================================
# The browser-facing screener consumes Market State. The buffer is intentionally
# compact: one snapshot per minute, only the fields needed for rolling filters.
SCREENER_BUFFER = {}
SCREENER_BUFFER_LOCK = threading.Lock()
SCREENER_HISTORY_CACHE = {}
SCREENER_HISTORY_CACHE_LOCK = threading.Lock()


def _period_seconds(period):
    value = str(period or "1h").strip().lower()
    aliases = {
        "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "4h": 14400, "12h": 43200,
        "24h": 86400, "25h": 90000, "1d": 86400,
    }
    return aliases.get(value)


def _buffer_prune_locked(now_ts=None):
    now_ts = float(now_ts or time.time())
    cutoff = now_ts - SCREENER_BUFFER_SECONDS
    empty = []
    for symbol, dq in SCREENER_BUFFER.items():
        while dq and float(dq[0].get("timestamp", 0)) < cutoff:
            dq.popleft()
        if not dq:
            empty.append(symbol)
    for symbol in empty:
        SCREENER_BUFFER.pop(symbol, None)


def _buffer_upsert_snapshot(symbol, coin, oi_value=None, timestamp=None):
    symbol = str(symbol or "").upper()
    if not symbol.endswith("USDT"):
        return
    timestamp = float(timestamp or time.time())
    try:
        price = float(coin.get("price", 0) or 0)
        turnover = float(coin.get("volume_24h", 0) or 0)
        volume_base = float(coin.get("volume_base_24h", 0) or 0)
        trades = int(coin.get("trades", 0) or 0)
    except (TypeError, ValueError):
        return
    try:
        oi = float(oi_value) if oi_value is not None else None
    except (TypeError, ValueError):
        oi = None
    point = (timestamp, price, turnover, volume_base, trades, oi)
    with SCREENER_BUFFER_LOCK:
        dq = SCREENER_BUFFER.setdefault(symbol, deque(maxlen=SCREENER_BUFFER_MAXLEN))
        # Keep exactly one point for the current minute bucket. This prevents
        # the high-frequency ticker stream from multiplying RAM usage.
        bucket = int(timestamp // SCREENER_SNAPSHOT_INTERVAL)
        if dq and int(float(dq[-1][0]) // SCREENER_SNAPSHOT_INTERVAL) == bucket:
            dq[-1] = point
        else:
            dq.append(point)
        _buffer_prune_locked(timestamp)


def _buffer_reference(symbol, target_ts):
    with SCREENER_BUFFER_LOCK:
        dq = SCREENER_BUFFER.get(str(symbol or "").upper())
        if not dq:
            return None
        # deque is chronological. A short linear scan is cheap at <=1505 points.
        best = None
        for point in reversed(dq):
            try:
                ts = float(point[0])
            except (TypeError, ValueError, IndexError):
                continue
            if ts <= target_ts:
                best = point
                break
        return best


def _current_oi(symbol):
    with state_lock:
        coin = STATE["coins"].get(str(symbol or "").upper(), {})
        value = coin.get("oi_value")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _fetch_current_oi(symbol):
    url = "https://fapi.binance.com/fapi/v1/openInterest?symbol=" + quote(str(symbol).upper())
    req = Request(url, headers={"User-Agent": "Crypto-Screener/1.0", "Accept": "application/json"})
    with urlopen(req, timeout=6) as response:
        payload = json.loads(response.read().decode("utf-8"))
    try:
        return float(payload.get("openInterest", 0))
    except (TypeError, ValueError):
        return None


def _snapshot_market_state():
    with state_lock:
        coins = {symbol: dict(coin) for symbol, coin in STATE.get("coins", {}).items() if str(symbol).endswith("USDT")}
    if not coins:
        return 0

    # OI is independent market data. Fetch current OI in bounded parallelism;
    # there is one shared snapshot cycle rather than one request per filter.
    symbols = list(coins.keys())
    max_workers = min(24, max(1, len(symbols)))
    oi_values = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_current_oi, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                oi_values[symbol] = future.result()
            except Exception:
                oi_values[symbol] = _current_oi(symbol)

    now_ts = time.time()
    with state_lock:
        for symbol, oi in oi_values.items():
            coin = STATE["coins"].get(symbol)
            if coin is not None and oi is not None:
                coin["oi_value"] = oi
        STATE["screener_last_snapshot"] = utc_now()
        if STATE.get("screener_buffer_started_at") is None:
            STATE["screener_buffer_started_at"] = utc_now()

    for symbol, coin in coins.items():
        oi = oi_values.get(symbol)
        if oi is None:
            oi = coin.get("oi_value")
        _buffer_upsert_snapshot(symbol, coin, oi_value=oi, timestamp=now_ts)
    with state_lock:
        STATE["screener_historical_cache_size"] = len(SCREENER_HISTORY_CACHE)
    return len(coins)


def screener_buffer_worker():
    # The same worker is used in Local and Render modes. It never touches chart
    # endpoints and therefore cannot block chart loading.
    while True:
        started = time.time()
        try:
            count = _snapshot_market_state()
            if count:
                log(f"Screener rolling snapshot: {count} symbols")
        except Exception as exc:
            log(f"Screener buffer snapshot failed: {exc}", "WARNING")
        elapsed = time.time() - started
        time.sleep(max(1.0, SCREENER_SNAPSHOT_INTERVAL - elapsed))


def _history_cache_get(symbol, period, target_ts):
    bucket = int((float(target_ts) * 1000) // SCREENER_HISTORY_TARGET_BUCKET_MS)
    key = (str(symbol).upper(), str(period), bucket)
    now = time.time()
    with SCREENER_HISTORY_CACHE_LOCK:
        item = SCREENER_HISTORY_CACHE.get(key)
        if item and now - float(item.get("cached_at", 0)) <= SCREENER_HISTORY_CACHE_TTL:
            return dict(item.get("point") or {}), True
    return None, False


def _history_cache_put(symbol, period, target_ts, point):
    bucket = int((float(target_ts) * 1000) // SCREENER_HISTORY_TARGET_BUCKET_MS)
    key = (str(symbol).upper(), str(period), bucket)
    with SCREENER_HISTORY_CACHE_LOCK:
        SCREENER_HISTORY_CACHE[key] = {"cached_at": time.time(), "point": dict(point or {})}
        # Bound cache growth. Oldest insertion order entries are discarded first.
        while len(SCREENER_HISTORY_CACHE) > 5000:
            SCREENER_HISTORY_CACHE.pop(next(iter(SCREENER_HISTORY_CACHE)))
    with state_lock:
        STATE["screener_historical_cache_size"] = len(SCREENER_HISTORY_CACHE)


def _fetch_historical_point(symbol, target_ts):
    """Fetch only the reference point needed for a >25h rolling comparison.

    Price/volume are represented by one 1-minute candle around target. OI uses
    one 5-minute historical OI record. We deliberately do not download a range.
    Historical rolling-24h turnover is not a single Binance point API, so it is
    left unavailable rather than replacing it with an incorrect large range.
    """
    symbol = str(symbol or "").upper()
    target_ms = int(float(target_ts) * 1000)
    point = {"timestamp": float(target_ts), "price": None, "turnover": None, "volume": None, "trades": None, "oi": None}

    try:
        end_ms = target_ms + 60_000
        url = ("https://fapi.binance.com/fapi/v1/klines?symbol=" + quote(symbol)
               + "&interval=1m&limit=1&endTime=" + str(end_ms))
        with urlopen(Request(url, headers={"User-Agent": "Crypto-Screener/1.0"}), timeout=8) as response:
            rows = json.loads(response.read().decode("utf-8"))
        if isinstance(rows, list) and rows:
            row = rows[-1]
            if isinstance(row, list) and len(row) >= 9:
                point["timestamp"] = int(row[0]) / 1000.0
                point["price"] = float(row[4])
                point["volume"] = float(row[5])
                point["turnover_period"] = float(row[7])
                point["trades"] = int(row[8])
    except Exception as exc:
        log(f"Historical price point failed for {symbol}: {exc}", "WARNING")

    try:
        end_ms = target_ms + 300_000
        url = ("https://fapi.binance.com/futures/data/openInterestHist?symbol=" + quote(symbol)
               + "&period=5m&limit=1&endTime=" + str(end_ms))
        with urlopen(Request(url, headers={"User-Agent": "Crypto-Screener/1.0"}), timeout=8) as response:
            rows = json.loads(response.read().decode("utf-8"))
        if isinstance(rows, list) and rows:
            item = rows[-1]
            point["oi"] = float(item.get("sumOpenInterest", 0))
            point["oi_timestamp"] = int(item.get("timestamp", target_ms)) / 1000.0
    except Exception as exc:
        log(f"Historical OI point failed for {symbol}: {exc}", "WARNING")
    return point


def _historical_reference(symbol, period, target_ts):
    cached, ok = _history_cache_get(symbol, period, target_ts)
    if ok:
        return cached
    point = _fetch_historical_point(symbol, target_ts)
    _history_cache_put(symbol, period, target_ts, point)
    return point


def rolling_market_metrics(symbol, period):
    seconds = _period_seconds(period)
    if seconds is None:
        return {}
    symbol = str(symbol or "").upper()
    with state_lock:
        coin = dict(STATE["coins"].get(symbol, {}))
    if not coin:
        return {}
    now_ts = time.time()
    target_ts = now_ts - seconds

    if seconds <= SCREENER_BUFFER_SECONDS:
        reference = _buffer_reference(symbol, target_ts)
        if not reference:
            return {"reference_source": "buffer", "reference_ready": False}
        current_price = float(coin.get("price", 0) or 0)
        current_turnover = float(coin.get("volume_24h", 0) or 0)
        current_trades = float(coin.get("trades", 0) or 0)
        current_oi = _current_oi(symbol)
        result = {"reference_source": "buffer", "reference_ready": True}
        ref_price = float(reference[1] or 0)
        if ref_price > 0 and current_price > 0:
            result["change_pct"] = (current_price / ref_price - 1.0) * 100.0
        ref_turnover = reference[2]
        if ref_turnover is not None:
            result["turnover_usd"] = current_turnover
            result["turnover_change_pct"] = ((current_turnover / float(ref_turnover) - 1.0) * 100.0) if float(ref_turnover) > 0 else None
        result["volume_24h"] = current_turnover
        result["trades"] = current_trades
        result["price"] = current_price
        ref_oi = reference[5]
        if current_oi is not None and ref_oi not in (None, 0):
            result["oi_change_pct"] = (current_oi / float(ref_oi) - 1.0) * 100.0
            result["oi_change_usd"] = current_oi - float(ref_oi)
        result["reference_timestamp"] = reference[0]
        return result

    reference = _historical_reference(symbol, period, target_ts)
    result = {"reference_source": "historical_point", "reference_ready": bool(reference)}
    current_price = float(coin.get("price", 0) or 0)
    ref_price = float(reference.get("price", 0) or 0)
    if current_price > 0 and ref_price > 0:
        result["change_pct"] = (current_price / ref_price - 1.0) * 100.0
    result["price"] = current_price
    result["volume_24h"] = float(coin.get("volume_24h", 0) or 0)
    result["turnover_usd"] = result["volume_24h"]
    result["trades"] = float(coin.get("trades", 0) or 0)
    current_oi = _current_oi(symbol)
    ref_oi = reference.get("oi")
    if current_oi is not None and ref_oi not in (None, 0):
        result["oi_change_pct"] = (current_oi / float(ref_oi) - 1.0) * 100.0
        result["oi_change_usd"] = current_oi - float(ref_oi)
    return result


# ============================================================
# SCREENER ANALYTICS
# ============================================================

def fetch_klines(symbol, interval=ANALYTICS_INTERVAL, limit=60):
    url = (
        "https://fapi.binance.com/fapi/v1/klines"
        f"?symbol={quote(symbol)}&interval={quote(interval)}&limit={limit}"
    )
    req = Request(url, headers={"User-Agent": "Crypto-Screener/1.0"})
    with urlopen(req, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))

    if not isinstance(payload, list):
        return []

    return [
        {
            "open_time": int(row[0]),
            "close_time": int(row[6]) if len(row) >= 7 else 0,
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]) if len(row) >= 6 else 0.0,
            "quote_volume": float(row[7]) if len(row) >= 8 else 0.0,
            "trades_count": int(row[8]) if len(row) >= 9 else 0,
        }
        for row in payload
        if isinstance(row, list) and len(row) >= 5
    ]


def calculate_natr(klines, period=ANALYTICS_PERIOD):
    """Return standard NATR (%) using Wilder ATR on completed candles.

    The screener timeframe is 5 minutes and the period is 14 candles. The
    current unfinished candle is excluded. True Range is used rather than a
    plain High-Low average, because NATR/ATR is based on:
        max(high-low, abs(high-prev_close), abs(low-prev_close))
    Extra historical candles are supplied by the worker to warm up Wilder's
    smoothing and make the result stable.
    """
    if period <= 0 or not klines:
        return None

    now_ms = int(time.time() * 1000)
    completed = []
    for candle in klines:
        close_time = int(candle.get("close_time", 0) or 0)
        if close_time and close_time > now_ms:
            continue
        try:
            high = float(candle["high"])
            low = float(candle["low"])
            close = float(candle["close"])
        except (KeyError, TypeError, ValueError):
            continue
        if high < low or close <= 0:
            continue
        completed.append((high, low, close))

    if len(completed) < period + 1:
        return None

    true_ranges = []
    previous_close = None
    for high, low, close in completed:
        if previous_close is None:
            true_range = high - low
        else:
            true_range = max(
                high - low,
                abs(high - previous_close),
                abs(low - previous_close),
            )
        true_ranges.append(max(0.0, true_range))
        previous_close = close

    if len(true_ranges) < period:
        return None

    # Wilder ATR: seed with the first period average, then smooth all later
    # completed candles. This is the conventional ATR used by NATR(14).
    atr = sum(true_ranges[:period]) / period
    for true_range in true_ranges[period:]:
        atr = ((atr * (period - 1)) + true_range) / period

    last_close = completed[-1][2]
    return atr / last_close * 100.0 if last_close > 0 else None


def calculate_correlation(symbol_klines, btc_klines, period=CORRELATION_PERIOD):
    """Pearson correlation of candle-to-candle returns on matching candle times."""
    if not symbol_klines or not btc_klines or period <= 0:
        return None

    # Align candles by their opening time. This prevents a small timing
    # difference between two REST requests from pairing different candles.
    symbol_by_time = {
        int(candle["open_time"]): candle["close"]
        for candle in symbol_klines
        if candle.get("open_time") is not None
    }
    btc_by_time = {
        int(candle["open_time"]): candle["close"]
        for candle in btc_klines
        if candle.get("open_time") is not None
    }

    common_times = sorted(set(symbol_by_time) & set(btc_by_time))
    if len(common_times) < period + 1:
        return None

    common_times = common_times[-(period + 1):]

    symbol_closes = [symbol_by_time[t] for t in common_times]
    btc_closes = [btc_by_time[t] for t in common_times]

    symbol_returns = [
        (b / a - 1.0) if a else 0.0
        for a, b in zip(symbol_closes, symbol_closes[1:])
    ]
    btc_returns = [
        (b / a - 1.0) if a else 0.0
        for a, b in zip(btc_closes, btc_closes[1:])
    ]

    if len(symbol_returns) != len(btc_returns) or not symbol_returns:
        return None

    mean_a = sum(symbol_returns) / len(symbol_returns)
    mean_b = sum(btc_returns) / len(btc_returns)

    covariance = sum(
        (a - mean_a) * (b - mean_b)
        for a, b in zip(symbol_returns, btc_returns)
    )
    variance_a = sum((a - mean_a) ** 2 for a in symbol_returns)
    variance_b = sum((b - mean_b) ** 2 for b in btc_returns)

    denominator = math.sqrt(variance_a * variance_b)
    if denominator == 0:
        return None

    return covariance / denominator


def analytics_worker():
    # Calculate all symbols concurrently. A small thread pool keeps the
    # initial screener fill fast without creating hundreds of simultaneous
    # HTTP connections.
    max_workers = 20

    while True:
        try:
            with state_lock:
                symbols = [
                    symbol
                    for symbol, coin in STATE["coins"].items()
                    if symbol.endswith("USDT")
                ]

            symbols.sort(
                key=lambda symbol: STATE["coins"].get(symbol, {}).get("volume_24h", 0),
                reverse=True,
            )
            if ANALYTICS_MAX_SYMBOLS:
                symbols = symbols[:ANALYTICS_MAX_SYMBOLS]

            if not symbols:
                time.sleep(1)
                continue

            btc_klines = fetch_klines("BTCUSDT", ANALYTICS_INTERVAL, ANALYTICS_KLINE_LIMIT)

            def calculate_for_symbol(symbol):
                klines = fetch_klines(symbol, ANALYTICS_INTERVAL, ANALYTICS_KLINE_LIMIT)
                natr = calculate_natr(klines)
                if symbol == "BTCUSDT":
                    btc_corr = 1.0
                elif btc_klines:
                    btc_corr = calculate_correlation(klines, btc_klines)
                else:
                    btc_corr = None
                return symbol, natr, btc_corr

            # Fetch/calculation runs in parallel, so one slow symbol no longer
            # delays every symbol behind it.
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(calculate_for_symbol, symbol): symbol
                    for symbol in symbols
                }

                for future in as_completed(futures):
                    symbol = futures[future]
                    try:
                        _, natr, btc_corr = future.result()
                        with state_lock:
                            coin = STATE["coins"].get(symbol)
                            if coin is not None:
                                coin["natr"] = round(natr, 4) if natr is not None else None
                                coin["btc_corr"] = round(btc_corr, 4) if btc_corr is not None else None
                    except Exception as exc:
                        log(f"Analytics failed for {symbol}: {exc}", "ERROR")

            # The first complete batch is ready before the browser is opened.
            ANALYTICS_INITIAL_READY.set()

        except Exception as exc:
            log(f"Analytics worker failed: {exc}", "ERROR")

        time.sleep(ANALYTICS_REFRESH)

def build_screener_snapshot():
    """
    Stage 1 common analytics boundary.

    Later indicators, filters, alerts and smart groups should consume
    normalized market data here rather than parsing Binance payloads
    independently.
    """
    with state_lock:
        coins = list(STATE["coins"].values())

    coins.sort(
        key=lambda x: x.get("volume_24h", 0),
        reverse=True,
    )

    if MAX_COINS is None:
        return coins

    return coins[:MAX_COINS]


# ============================================================
# FLASK ROUTES
# ============================================================

@app.get("/")
def index():
    return render_template_string(HTML_PAGE)


@app.get("/api/klines")
def api_klines():
    symbol = (request.args.get("symbol") or "").upper().strip()
    interval = (request.args.get("interval") or "15m").strip()
    limit = min(max(int(request.args.get("limit", 500)), 1), 1500)
    end_time = request.args.get("endTime")
    if end_time is not None:
        try:
            end_time = int(end_time)
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid endTime"}), 400

    if not symbol:
        return jsonify({"error": "Symbol is required"}), 400

    url = (
        "https://fapi.binance.com/fapi/v1/klines"
        f"?symbol={quote(symbol)}&interval={quote(interval)}&limit={limit}"
        + (f"&endTime={end_time}" if end_time is not None else "")
    )

    try:
        req = Request(url, headers={"User-Agent": "Crypto-Screener/1.0"})
        with urlopen(req, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))

        if not isinstance(payload, list):
            return jsonify({"error": "Invalid Binance klines response"}), 502

        candles = []
        for item in payload:
            if len(item) < 6:
                continue
            candles.append({
                "time": int(item[0]) // 1000,
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
            })

        return jsonify({"symbol": symbol, "interval": interval, "candles": candles})
    except Exception as exc:
        log(f"Klines request failed for {symbol}: {exc}", "ERROR")
        return jsonify({"error": str(exc)}), 502


@app.get("/api/open_interest")
def api_open_interest():
    symbol = (request.args.get("symbol") or "").upper().strip()
    period = (request.args.get("period") or "5m").strip()
    limit = min(max(int(request.args.get("limit", 300)), 1), 500)
    end_time = request.args.get("endTime")
    if end_time is not None:
        try:
            end_time = int(end_time)
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid endTime"}), 400

    allowed_periods = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}
    if not symbol or not symbol.isalnum():
        return jsonify({"error": "Invalid symbol"}), 400
    if period not in allowed_periods:
        return jsonify({"error": "Invalid open interest period"}), 400

    url = (
        "https://fapi.binance.com/futures/data/openInterestHist"
        f"?symbol={quote(symbol)}&period={quote(period)}&limit={limit}"
        + (f"&endTime={end_time}" if end_time is not None else "")
    )
    try:
        req = Request(url, headers={"User-Agent": "Crypto-Screener/1.0"})
        with urlopen(req, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, list):
            return jsonify({"error": "Invalid Binance open interest response"}), 502

        points = []
        for item in payload:
            try:
                points.append({
                    "time": int(item["timestamp"]) // 1000,
                    "oi": float(item.get("sumOpenInterest", 0)),
                    "oiValue": float(item.get("sumOpenInterestValue", 0)),
                })
            except (KeyError, TypeError, ValueError):
                continue
        return jsonify({"symbol": symbol, "period": period, "points": points})
    except Exception as exc:
        log(f"Open interest request failed for {symbol}: {exc}", "ERROR")
        return jsonify({"error": str(exc), "points": []}), 502

@app.get("/api/current_open_interest")
def api_current_open_interest():
    symbol = (request.args.get("symbol") or "").upper().strip()
    if not symbol or not symbol.isalnum():
        return jsonify({"error": "Invalid symbol"}), 400
    try:
        url = "https://fapi.binance.com/fapi/v1/openInterest?symbol=" + quote(symbol)
        with urlopen(Request(url, headers={"User-Agent": "Crypto-Screener/1.0"}), timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return jsonify({
            "symbol": symbol,
            "time": int(payload.get("time", time.time() * 1000)) // 1000,
            "oi": float(payload.get("openInterest", 0)),
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


# ============================================================
# ALERT METRICS — live OI change % and volume expansion x
# ============================================================
ALERT_METRIC_CACHE = {}
ALERT_METRIC_CACHE_LOCK = threading.Lock()
ALERT_METRIC_CACHE_TTL = 20


def _alert_oi_period(timeframe):
    return {
        "5m": "5m", "15m": "15m", "30m": "30m", "1h": "1h",
        "2h": "2h", "4h": "4h", "6h": "6h", "12h": "12h", "1D": "1d",
    }.get(str(timeframe), "5m")


def _alert_fetch_open_interest_history(symbol, period, limit):
    url = (
        "https://fapi.binance.com/futures/data/openInterestHist"
        f"?symbol={quote(symbol)}&period={quote(period)}&limit={limit}"
    )
    req = Request(url, headers={"User-Agent": "Crypto-Screener/1.0"})
    with urlopen(req, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload if isinstance(payload, list) else []


def _alert_volume_expansion(symbol, timeframe, base_candles, growth_candles):
    base_candles = max(1, min(int(base_candles), 500))
    growth_candles = max(1, min(int(growth_candles), 200))
    total = base_candles + growth_candles + 1
    url = (
        "https://fapi.binance.com/fapi/v1/klines"
        f"?symbol={quote(symbol)}&interval={quote(timeframe)}&limit={total}"
    )
    req = Request(url, headers={"User-Agent": "Crypto-Screener/1.0"})
    with urlopen(req, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return _volume_expansion_latest_from_rows(payload, base_candles, growth_candles)


def _alert_timeframe_market_metrics(symbol, timeframe):
    """Return screener metrics using rolling Market State first.

    <=25h uses the RAM snapshot buffer. >25h uses a cached single historical
    reference point. Existing NATR/BTC correlation remain on their established
    analytics path because they are indicator calculations, not simple rolling
    point comparisons.
    """
    tf_raw = str(timeframe or "1h").strip()
    tf = "24h" if tf_raw.lower() == "1d" else tf_raw
    rolling_periods = {"1m", "5m", "15m", "30m", "1h", "4h", "12h", "24h", "25h", "1d"}
    with state_lock:
        coin = dict(STATE["coins"].get(str(symbol).upper(), {}))
    if not coin:
        return {}

    if tf in rolling_periods:
        result = rolling_market_metrics(symbol, tf)
        if result:
            result.setdefault("natr", coin.get("natr"))
            result.setdefault("btc_corr", coin.get("btc_corr"))
            result.setdefault("volume_spike", coin.get("volume_spike"))
            result.setdefault("spread_pct", None)
            result.setdefault("funding_pct", coin.get("funding_pct"))
            result.setdefault("delta_volume_usd", coin.get("delta_volume_usd"))
            return result

    rows = fetch_klines(symbol, interval=tf_raw, limit=32)
    if not rows:
        return {}
    current = rows[-1]
    previous = rows[-2] if len(rows) >= 2 else None
    current_close = float(current.get("close", 0) or 0)
    previous_close = float(previous.get("close", 0) or 0) if previous else 0.0
    change = ((current_close - previous_close) / previous_close * 100.0) if previous_close > 0 else None
    quote_volume = current.get("quote_volume")
    trades = current.get("trades_count")
    previous_quote = float(previous.get("quote_volume", 0) or 0) if previous else 0.0
    delta_volume = (float(quote_volume) - previous_quote) if quote_volume is not None else None
    return {
        "change_pct": change,
        "turnover_usd": quote_volume,
        "volume_24h": quote_volume,
        "trades": trades,
        "price": current_close,
        "natr": calculate_natr(rows),
        "btc_corr": coin.get("btc_corr"),
        "volume_spike": coin.get("volume_spike"),
        "spread_pct": None,
        "funding_pct": coin.get("funding_pct"),
        "oi_change_usd": coin.get("oi_change_usd"),
        "delta_volume_usd": delta_volume,
    }


def _alert_metric_for_symbol(symbol, timeframe, need_oi, need_volume, need_market, volume_base, volume_growth):
    result = {}
    if need_market:
        result.update(_alert_timeframe_market_metrics(symbol, timeframe))
    if need_oi:
        period = _alert_oi_period(timeframe)
        points = _alert_fetch_open_interest_history(symbol, period, 2)
        if len(points) >= 2:
            previous = float(points[-2].get("sumOpenInterestValue", 0) or 0)
            current = float(points[-1].get("sumOpenInterestValue", 0) or 0)
            if previous > 0:
                result["oi_change_pct"] = (current - previous) / previous * 100.0
    if need_volume:
        value = _alert_volume_expansion(symbol, timeframe, volume_base, volume_growth)
        if value is not None:
            result["volume_expansion_x"] = value["ratio"]
            result["volume_expansion_direction"] = value["direction"]
    return result


@app.post("/api/alert_metrics")
def api_alert_metrics():
    payload = request.get_json(silent=True) or {}
    raw_symbols = payload.get("symbols") or []
    if not isinstance(raw_symbols, list):
        return jsonify({"error": "symbols must be a list"}), 400
    # The Screener can cover the full Binance Futures universe. Keep the
    # existing 200-symbol limit only for unrelated heavy alert workloads;
    # this shared metrics endpoint is intentionally allowed to serve the
    # approximately 600-symbol screener universe.
    symbols = list(dict.fromkeys(
        str(symbol).upper().strip() for symbol in raw_symbols
        if str(symbol).upper().strip().endswith("USDT")
    ))[:600]
    timeframe = str(payload.get("timeframe") or "5m")
    allowed_volume_tfs = {"1m", "5m", "15m", "30m", "1h", "4h", "1D"}
    if timeframe not in allowed_volume_tfs:
        return jsonify({"error": "Invalid alert timeframe"}), 400
    need_oi = bool(payload.get("need_oi"))
    need_volume = bool(payload.get("need_volume"))
    need_market = bool(payload.get("need_market"))
    volume_base = int(payload.get("volume_base") or 100)
    volume_growth = int(payload.get("volume_growth") or 20)
    if volume_base < 1 or volume_base > 500 or volume_growth < 1 or volume_growth > 200:
        return jsonify({"error": "Invalid volume candle settings"}), 400
    if not symbols or not (need_oi or need_volume or need_market):
        return jsonify({"metrics": {}})

    cache_key = (tuple(symbols), timeframe, need_oi, need_volume, need_market, volume_base, volume_growth)
    now = time.time()
    with ALERT_METRIC_CACHE_LOCK:
        cached = ALERT_METRIC_CACHE.get(cache_key)
        if cached and now - cached[0] < ALERT_METRIC_CACHE_TTL:
            return jsonify({"metrics": cached[1], "cached": True})

    metrics = {}
    max_workers = min(12, max(1, len(symbols)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _alert_metric_for_symbol, symbol, timeframe, need_oi, need_volume, need_market,
                volume_base, volume_growth
            ): symbol
            for symbol in symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                value = future.result()
                if value:
                    metrics[symbol] = {k: round(v, 6) for k, v in value.items()}
            except Exception as exc:
                log(f"Alert metrics failed for {symbol}: {exc}", "ERROR")

    with ALERT_METRIC_CACHE_LOCK:
        ALERT_METRIC_CACHE[cache_key] = (now, metrics)
        if len(ALERT_METRIC_CACHE) > 32:
            oldest = min(ALERT_METRIC_CACHE.items(), key=lambda item: item[1][0])[0]
            ALERT_METRIC_CACHE.pop(oldest, None)
    return jsonify({"metrics": metrics, "cached": False})


@app.get("/api/aggtrades")
def api_aggtrades():
    symbol = request.args.get("symbol", "").upper().strip()
    if not symbol or not symbol.isalnum():
        return jsonify({"error": "Invalid symbol"}), 400
    try:
        url = "https://fapi.binance.com/fapi/v1/aggTrades?symbol=" + quote(symbol) + "&limit=1000"
        with urlopen(Request(url, headers={"User-Agent": "CryptoScreener/1.0"}), timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
        trades = [{"timestamp": int(t[5]), "price": float(t[1])} for t in payload]
        return jsonify({"trades": trades})
    except Exception as exc:
        return jsonify({"error": str(exc), "trades": []}), 502

@app.get("/api/instruments")
def api_instruments():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        req = Request(url, headers={"User-Agent": "Crypto-Screener/1.0"})
        with urlopen(req, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))

        symbols = []
        stock_underlying_types = {
            "EQUITY", "HK_EQUITY", "KR_EQUITY", "CN_EQUITY", "PREMARKET"
        }
        for item in payload.get("symbols", []):
            if item.get("status") != "TRADING":
                continue
            symbol = item.get("symbol")
            if symbol and str(item.get("quoteAsset") or "").upper() == "USDT":
                underlying_type = str(item.get("underlyingType") or "").upper()
                contract_type = str(item.get("contractType") or "").upper()
                is_stock = (
                    contract_type == "TRADIFI_PERPETUAL"
                    and underlying_type in stock_underlying_types
                )
                symbols.append({
                    "symbol": symbol,
                    "baseAsset": item.get("baseAsset", ""),
                    "quoteAsset": item.get("quoteAsset", ""),
                    "instrumentType": "stock" if is_stock else "crypto",
                    "underlyingType": underlying_type,
                })

        symbols.sort(key=lambda x: x["symbol"])
        return jsonify({"symbols": symbols})
    except Exception as exc:
        log(f"Instrument catalog request failed: {exc}", "ERROR")
        return jsonify({"error": str(exc)}), 502


@app.post("/api/signal-levels/sync")
def api_signal_levels_sync():
    """Synchronize the Screener's actual signal levels to Render.

    The payload contains the complete current set of chart signal levels:
    symbol + price (+ stable drawing id when available). Render stores this
    set in Supabase and monitors exactly these symbols on Binance Futures.
    """
    try:
        payload = request.get_json(silent=True) or {}
        levels = payload.get("levels") if isinstance(payload.get("levels"), list) else []
        if len(levels) > 5000:
            return jsonify({"ok": False, "error": "Too many signal levels"}), 400

        clean_levels = []
        for item in levels:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").strip().upper()
            try:
                price = float(item.get("price"))
            except (TypeError, ValueError):
                continue
            if not symbol or not symbol.isalnum() or price <= 0:
                continue
            clean_levels.append({
                "id": str(item.get("id") or "").strip(),
                "symbol": symbol,
                "price": price,
                "active": item.get("active") is not False,
            })

        worker_url = ALERT_SERVER_URL.rstrip("/") + "/api/signal-levels/sync"
        body = json.dumps({"levels": clean_levels}).encode("utf-8")
        req = Request(
            worker_url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Crypto-Screener/1.0",
            },
            method="POST",
        )
        with urlopen(req, timeout=12) as response:
            raw = response.read().decode("utf-8")
            result = json.loads(raw) if raw else {}

        if not result.get("ok"):
            return jsonify({
                "ok": False,
                "error": str(result.get("error") or "Render signal level sync failed"),
            }), 502

        synced = int(result.get("levels") or 0)
        log(f"Signal levels synchronized to Render: {synced}")
        return jsonify({"ok": True, "levels": synced})
    except Exception as exc:
        log(f"Render signal level sync failed: {exc}", "ERROR")
        return jsonify({"ok": False, "error": str(exc)}), 502


@app.post("/api/telegram/cloudflare")
def api_telegram_cloudflare():
    try:
        payload = request.get_json(silent=True) or {}
        text = str(payload.get("text") or payload.get("message") or "").strip()
        if not text:
            return jsonify({"ok": False, "error": "Telegram message is required"}), 400
        if len(text) > 4096:
            return jsonify({"ok": False, "error": "Telegram message is too large"}), 400
        worker_url = "https://crypto-alert-telegram.johnjohnson763163.workers.dev/"
        body = json.dumps({"message": text}).encode("utf-8")
        req = Request(worker_url, data=body, headers={"Content-Type": "application/json", "User-Agent": "Crypto-Screener/1.0"}, method="POST")
        with urlopen(req, timeout=8) as response:
            raw = response.read().decode("utf-8")
            result = json.loads(raw) if raw else {}
        if not result.get("ok"):
            return jsonify({"ok": False, "error": str(result.get("error") or "Cloudflare Worker error")}), 502
        return jsonify({"ok": True})
    except Exception as exc:
        log(f"Cloudflare Telegram send failed: {exc}", "ERROR")
        return jsonify({"ok": False, "error": str(exc)}), 502


@app.post("/api/telegram/send")
def api_telegram_send():
    try:
        payload = request.get_json(silent=True) or {}
        token = str(payload.get("token") or "").strip()
        chat_id = str(payload.get("chat_id") or "").strip()
        text = str(payload.get("text") or "").strip()
        if not token or not chat_id or not text:
            return jsonify({"ok": False, "error": "Telegram token, chat ID and text are required"}), 400
        if len(token) > 256 or len(chat_id) > 256 or len(text) > 4096:
            return jsonify({"ok": False, "error": "Telegram request is too large"}), 400
        url = f"https://api.telegram.org/bot{quote(token, safe='')}/sendMessage"
        body = urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
        req = Request(url, data=body, headers={"Content-Type": "application/x-www-form-urlencoded", "User-Agent": "Crypto-Screener/1.0"}, method="POST")
        with urlopen(req, timeout=5) as response:
            result = json.loads(response.read().decode("utf-8"))
        if not result.get("ok"):
            return jsonify({"ok": False, "error": str(result.get("description") or "Telegram API error")}), 502
        return jsonify({"ok": True})
    except Exception as exc:
        log(f"Telegram send failed: {exc}", "ERROR")
        return jsonify({"ok": False, "error": str(exc)}), 502


@app.post("/api/telegram/validate")
def api_telegram_validate():
    data = request.get_json(silent=True) or {}
    token = str(data.get("token") or "").strip()
    if not token:
        return jsonify({"ok": False, "error": "Telegram Bot Token обязателен"}), 400
    try:
        url = f"https://api.telegram.org/bot{quote(token, safe='')}/getMe"
        req = Request(url, headers={"User-Agent": "Crypto-Screener/1.0"}, method="GET")
        with urlopen(req, timeout=5) as response:
            result = json.loads(response.read().decode("utf-8"))
        if not result.get("ok"):
            return jsonify({"ok": False, "error": str(result.get("description") or "Неверный Telegram Bot Token")}), 400
        bot = result.get("result") or {}
        return jsonify({"ok": True, "bot": {"id": bot.get("id"), "username": bot.get("username"), "first_name": bot.get("first_name")}})
    except Exception as exc:
        log(f"Telegram validation failed: {exc}", "ERROR")
        error_text = str(exc)
        if "timed out" in error_text.lower() or "timeout" in error_text.lower():
            error_text = "Не удалось подключиться к Telegram API: превышено время ожидания. Проверьте доступ к https://api.telegram.org."
        return jsonify({"ok": False, "error": error_text}), 502


@app.post("/api/telegram/test")
def api_telegram_test():
    data = request.get_json(silent=True) or {}
    token = str(data.get("token") or "").strip()
    chat_id = str(data.get("chat_id") or "").strip()
    if not token or not chat_id:
        return jsonify({"ok": False, "error": "Telegram token и chat ID обязательны"}), 400
    try:
        url = f"https://api.telegram.org/bot{quote(token, safe='')}/sendMessage"
        body = urlencode({"chat_id": chat_id, "text": "Crypto Screener: Telegram уведомления подключены."}).encode("utf-8")
        req = Request(url, data=body, headers={"Content-Type": "application/x-www-form-urlencoded", "User-Agent": "Crypto-Screener/1.0"}, method="POST")
        with urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode("utf-8"))
        if not result.get("ok"):
            return jsonify({"ok": False, "error": str(result.get("description") or "Telegram API error")}), 502
        return jsonify({"ok": True})
    except Exception as exc:
        log(f"Telegram test failed: {exc}", "ERROR")
        error_text = str(exc)
        if "timed out" in error_text.lower() or "timeout" in error_text.lower():
            error_text = "Не удалось подключиться к Telegram API: превышено время ожидания. Проверьте доступ к https://api.telegram.org."
        return jsonify({"ok": False, "error": error_text}), 502


# ============================================================
# MARKET STRUCTURE / PATTERN ENGINE
# ============================================================

PATTERN_INTERVALS = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "3h", "4h", "6h", "8h", "12h", "1d", "3d", "1w"}

# Stage 8: keep the heavy formation calculation cached per symbol/timeframe.
# The cache is invalidated automatically when a new candle opens. During the
# current candle, repeated requests reuse the last completed structure result
# instead of rebuilding pivots/trendlines on every request.
PATTERN_ANALYSIS_CACHE = {}
PATTERN_ANALYSIS_CACHE_LOCK = threading.Lock()

def _pattern_analysis_cache_key(symbol, interval, settings=None):
    settings = settings or {}
    relevant = {
        key: settings.get(key)
        for key in (
            "horizontalLevelSearchPeriod", "horizontalLevelSearchStrict",
            "horizontalLevelTouches", "horizontalLevelTouchTolerancePct",
            "horizontalLevelLifetimeHours", "horizontalLevelNoCross", "horizontalLevelAllowMinorPierce",
            "trendlineSearchPeriod", "trendlineSearchStrict", "trendlineTouches",
            "trendlineTouchTolerancePct", "trendlineLifetimeHours", "trendlineNoCross", "trendlineAllowMinorPierce",
        )
    }
    signature = json.dumps(relevant, sort_keys=True, separators=(",", ":"))
    return f"{str(symbol).upper()}|{str(interval).lower()}|{signature}"

def _cached_pattern_analysis(symbol, interval, klines, settings=None):
    if not klines:
        return None
    candle_open_time = int(klines[-1].get("open_time", 0))
    key = _pattern_analysis_cache_key(symbol, interval, settings)
    with PATTERN_ANALYSIS_CACHE_LOCK:
        cached = PATTERN_ANALYSIS_CACHE.get(key)
        if cached and cached.get("candle_open_time") == candle_open_time:
            return cached.get("result")
    result = pattern_engine(klines, interval, settings=settings)
    with PATTERN_ANALYSIS_CACHE_LOCK:
        PATTERN_ANALYSIS_CACHE[key] = {
            "candle_open_time": candle_open_time,
            "result": result,
        }
    return result

# ---------------------------------------------------------------------------
# Market structure engine
#
# This block deliberately stays independent from the UI.  The important
# difference from the previous implementation is that levels/trendlines are
# now candidates which must survive structural validation instead of being
# averages/regressions of the last few extrema.
# ---------------------------------------------------------------------------

STRUCTURE_ATR_PERIOD = 14
STRUCTURE_SMALL_PIVOT = 5
STRUCTURE_LARGE_PIVOT = 25
STRUCTURE_MIN_LINE_SPAN = 8
STRUCTURE_MAX_ANCHOR_AGE = 220
STRUCTURE_TOUCH_ATR = 0.25
STRUCTURE_BREAK_ATR = 0.35
STRUCTURE_MAX_VIOLATION_ATR = 0.40
STRUCTURE_BREAK_CLOSES = 3
STRUCTURE_MIN_LINE_TOUCHES = 3
STRUCTURE_MAX_LINES_PER_SIDE = 3
STRUCTURE_MAX_SLOPE_ATR_PER_BAR = 0.90
STRUCTURE_NEAR_MISS_ATR = 0.55
STRUCTURE_MAX_VIOLATION_RATE = 0.08
STRUCTURE_STRONG_PIVOT_STRENGTH = 8
STRUCTURE_MAX_PIVOTS_FOR_LINES = 16
STRUCTURE_MAX_STRONG_PIVOTS_FOR_LINES = 8

# Horizontal-level interaction engine. These thresholds are ATR-adaptive so
# interaction classes remain meaningful across symbols and timeframes.
LEVEL_INTERACTION_TOUCH_ATR = 0.25
LEVEL_INTERACTION_NEAR_ATR = 0.55
LEVEL_INTERACTION_PIERCE_ATR = 0.35
LEVEL_INTERACTION_BREAK_ATR = 0.35
LEVEL_INTERACTION_BREAK_CLOSES = 3
LEVEL_INTERACTION_RETEST_ATR = 0.35


def _linreg_slope(values):
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    den = sum((i - x_mean) ** 2 for i in range(n)) or 1.0
    return sum((i - x_mean) * (y - y_mean) for i, y in enumerate(values)) / den


def _atr_series(klines, period=STRUCTURE_ATR_PERIOD):
    """Return ATR-like values aligned 1:1 with klines."""
    if not klines:
        return []
    tr = []
    prev_close = None
    for k in klines:
        high = float(k["high"])
        low = float(k["low"])
        close = float(k["close"])
        if prev_close is None:
            value = high - low
        else:
            value = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr.append(value)
        prev_close = close
    atr = []
    for i in range(len(tr)):
        window = tr[max(0, i - period + 1):i + 1]
        atr.append(sum(window) / len(window) if window else 0.0)
    return atr


def _filter_extrema_by_spacing(points, min_distance, strict=False):
    """Apply the configured minimum spacing between same-type extrema.

    In strict mode the spacing is a hard requirement. In non-strict mode we
    prefer spaced extrema, but fall back to the original set when enforcing
    the minimum would leave fewer than two candidates.
    """
    if not points:
        return []
    try:
        minimum = int(float(min_distance))
    except (TypeError, ValueError):
        return list(points)
    if minimum <= 1:
        return list(points)

    ordered = sorted(points, key=lambda p: p["index"])
    selected = [ordered[0]]
    for point in ordered[1:]:
        if point["index"] - selected[-1]["index"] >= minimum:
            selected.append(point)

    if strict:
        return selected if len(selected) >= 2 else []
    return selected if len(selected) >= 2 else ordered


def _filter_extrema_by_lifetime(points, klines, lifetime_hours):
    if not points or lifetime_hours is None:
        return list(points)
    try:
        hours = float(lifetime_hours)
    except (TypeError, ValueError):
        return list(points)
    if hours <= 0 or not klines:
        return list(points)
    cutoff = int(klines[-1]["open_time"]) - int(hours * 3600 * 1000)
    return [p for p in points if int(klines[p["index"]]["open_time"]) >= cutoff]


def _pivot_points(klines, atrs, radius):
    """Confirmed pivots with an ATR prominence filter and pivot strength."""
    points_high, points_low = [], []
    if len(klines) < radius * 2 + 3:
        return points_high, points_low

    for i in range(radius, len(klines) - radius):
        h = float(klines[i]["high"])
        l = float(klines[i]["low"])
        left = klines[i - radius:i]
        right = klines[i + 1:i + radius + 1]
        left_high = max(float(x["high"]) for x in left)
        right_high = max(float(x["high"]) for x in right)
        left_low = min(float(x["low"]) for x in left)
        right_low = min(float(x["low"]) for x in right)
        atr = max(float(atrs[i] or 0.0), h * 0.0005, 1e-9)

        if h >= left_high and h >= right_high:
            prominence = h - max(left_high, right_high)
            # A pivot must represent a real move, not just a one-tick wiggle.
            neighbourhood_low = min(float(x["low"]) for x in klines[max(0, i-radius):min(len(klines), i+radius+1)])
            if (h - neighbourhood_low) >= 0.75 * atr:
                strength = max(1, min(radius, int(round((h - min(left_low, right_low)) / atr))))
                points_high.append({
                    "index": i, "time": int(klines[i]["open_time"] / 1000),
                    "price": h, "strength": strength, "atr": atr, "radius": radius,
                })

        if l <= left_low and l <= right_low:
            prominence = min(left_low, right_low) - l
            neighbourhood_high = max(float(x["high"]) for x in klines[max(0, i-radius):min(len(klines), i+radius+1)])
            if (neighbourhood_high - l) >= 0.75 * atr:
                strength = max(1, min(radius, int(round((max(left_high, right_high) - l) / atr))))
                points_low.append({
                    "index": i, "time": int(klines[i]["open_time"] / 1000),
                    "price": l, "strength": strength, "atr": atr, "radius": radius,
                })
    return points_high, points_low


def _merge_pivots(primary, secondary, atrs):
    """Keep large pivots and add small pivots that are not duplicates."""
    result = list(primary)
    for p in secondary:
        duplicate = False
        for q in result:
            tol = max(0.20 * max(p.get("atr", 0.0), q.get("atr", 0.0)), p["price"] * 0.0008)
            if abs(p["price"] - q["price"]) <= tol and abs(p["index"] - q["index"]) <= 4:
                duplicate = True
                if p.get("strength", 0) > q.get("strength", 0):
                    q.update({"strength": p["strength"], "radius": max(q.get("radius", 0), p.get("radius", 0))})
                break
        if not duplicate:
            result.append(p)
    result.sort(key=lambda x: x["index"])
    return result


def _cluster_levels(points, current_price, touch_tolerance_pct=0.1):
    """Build price clusters instead of averaging unrelated extrema."""
    if not points:
        return []
    ordered = sorted(points, key=lambda p: p["price"])
    clusters = []
    for p in ordered:
        if not clusters:
            clusters.append([p])
            continue
        c = clusters[-1]
        center = sum(x["price"] for x in c) / len(c)
        tol = max(
            0.35 * max(float(p.get("atr", 0.0)), 0.0),
            0.35 * max(float(c[-1].get("atr", 0.0)), 0.0),
            center * max(float(touch_tolerance_pct or 0.1), 0.0) / 100.0,
        )
        if abs(p["price"] - center) <= tol:
            c.append(p)
        else:
            clusters.append([p])

    out = []
    last_index = max(p["index"] for p in points)
    for c in clusters:
        prices = [p["price"] for p in c]
        weights = []
        for p in c:
            recency = 1.0 + 1.5 * max(0.0, 1.0 - (last_index - p["index"]) / max(1.0, last_index))
            weights.append((1.0 + 0.20 * p.get("strength", 1)) * recency)
        center = sum(p * w for p, w in zip(prices, weights)) / sum(weights)
        touches = len(c)
        strength = sum(p.get("strength", 1) for p in c)
        out.append({
            "price": center,
            "touches": touches,
            "strength": strength,
            "points": c,
            "distance_pct": abs(center - current_price) / current_price * 100 if current_price else None,
        })
    return out


def _pick_level(clusters, current_price, side):
    if not clusters or current_price <= 0:
        return None
    if side == "support":
        candidates = [c for c in clusters if c["price"] <= current_price * 1.003]
        candidates.sort(key=lambda c: (-c["touches"], c["distance_pct"] if c["distance_pct"] is not None else 999, -c["strength"]))
    else:
        candidates = [c for c in clusters if c["price"] >= current_price * 0.997]
        candidates.sort(key=lambda c: (-c["touches"], c["distance_pct"] if c["distance_pct"] is not None else 999, -c["strength"]))
    return candidates[0] if candidates else None


def _line_price(line, index):
    return line["p1"] + line["slope"] * (index - line["i1"])


def _validate_trendline(klines, atrs, pivots, p1, p2, side, min_touches=3, touch_tolerance_pct=0.1, require_no_cross=False, allow_minor_pierce=False):
    """Validate a trendline against meaningful pivots and the full candle span."""
    i1, i2 = p1["index"], p2["index"]
    if i2 <= i1 or i2 - i1 < STRUCTURE_MIN_LINE_SPAN:
        return None
    if len(klines) - 1 - i1 > STRUCTURE_MAX_ANCHOR_AGE:
        return None

    slope = (p2["price"] - p1["price"]) / float(i2 - i1)
    base_atr = sum(float(x or 0.0) for x in atrs[i1:i2 + 1]) / max(1, i2 - i1 + 1)
    base_atr = max(base_atr, p1["price"] * 0.0005, 1e-9)

    # Optional clean-line rule: after the second anchor, price may not cross
    # the projected trendline. A small wick pierce can be explicitly allowed.
    if require_no_cross:
        crossed, _, _ = _structure_line_crossing(
            klines, lambda idx: p1["price"] + slope * (idx - i1), i2, side,
            allow_minor_pierce=allow_minor_pierce, base_atr=base_atr,
            tolerance_pct=touch_tolerance_pct
        )
        if crossed:
            return None

    # Normalize slope by volatility instead of using a screen-space angle.
    normalized_slope = abs(slope) / base_atr
    if normalized_slope > STRUCTURE_MAX_SLOPE_ATR_PER_BAR:
        return None

    touch_pct = max(float(touch_tolerance_pct or 0.1), 0.0) / 100.0
    touch_tol = max(STRUCTURE_TOUCH_ATR * base_atr, p1["price"] * touch_pct)
    near_tol = max(STRUCTURE_NEAR_MISS_ATR * base_atr, p1["price"] * max(touch_pct * 1.5, 0.0012))
    violation_tol = max(STRUCTURE_MAX_VIOLATION_ATR * base_atr, p1["price"] * max(touch_pct * 2.0, 0.0010))

    # The anchors are two confirmed structural interactions. Additional pivots
    # count only once; this prevents the old double-counting of the anchors.
    touches = 2
    near_misses = 0
    minor_violations = 0
    max_violation = 0.0
    max_miss_atr = 0.0
    strong_pivot_touches = 0
    break_run = 0
    broken_at = None

    # Validate every candle between the anchors. A line may tolerate a small
    # wick pierce, but repeated/large structural violations invalidate it.
    for i in range(i1 + 1, i2 + 1):
        lp = _line_price({"p1": p1["price"], "i1": i1, "slope": slope}, i)
        atr = max(float(atrs[i] or base_atr), base_atr * 0.5, 1e-9)
        if side == "resistance":
            distance = lp - float(klines[i]["high"])
            penetration = -distance
            close_beyond = float(klines[i]["close"]) > lp + STRUCTURE_BREAK_ATR * atr
        else:
            distance = float(klines[i]["low"]) - lp
            penetration = -distance
            close_beyond = float(klines[i]["close"]) < lp - STRUCTURE_BREAK_ATR * atr

        if distance >= 0 and distance <= near_tol:
            # Price came close without actually touching the line.
            if distance > touch_tol:
                near_misses += 1
                max_miss_atr = max(max_miss_atr, distance / atr)
        elif penetration > touch_tol:
            minor_violations += 1
            max_violation = max(max_violation, penetration / atr)
            if penetration > violation_tol:
                return None

        if close_beyond:
            break_run += 1
            if break_run >= STRUCTURE_BREAK_CLOSES and broken_at is None:
                broken_at = i - STRUCTURE_BREAK_CLOSES + 1
        else:
            break_run = 0

    # Count independent structural pivot confirmations. Prefer strong pivots;
    # a line made from one huge extreme plus one weak pivot is not promoted.
    for p in pivots:
        idx = p["index"]
        if idx <= i1 or idx >= i2:
            continue
        lp = _line_price({"p1": p1["price"], "i1": i1, "slope": slope}, idx)
        atr = max(float(p.get("atr", 0.0) or 0.0), base_atr)
        tol = max(STRUCTURE_TOUCH_ATR * atr, p["price"] * 0.0007)
        distance = abs(p["price"] - lp)
        if distance <= tol:
            touches += 1
            if p.get("radius", 0) >= STRUCTURE_LARGE_PIVOT or p.get("strength", 0) >= STRUCTURE_STRONG_PIVOT_STRENGTH:
                strong_pivot_touches += 1
        elif distance <= max(STRUCTURE_NEAR_MISS_ATR * atr, p["price"] * 0.0012):
            near_misses += 1

    if touches < max(2, int(min_touches or STRUCTURE_MIN_LINE_TOUCHES)):
        return None

    # A valid line should have at least one additional meaningful confirmation
    # unless both anchors are large/strong structural pivots.
    anchor_strength = p1.get("strength", 1) + p2.get("strength", 1)
    anchors_strong = (
        p1.get("radius", 0) >= STRUCTURE_LARGE_PIVOT
        and p2.get("radius", 0) >= STRUCTURE_LARGE_PIVOT
    )
    if touches == 2 and not anchors_strong:
        return None

    span = i2 - i1
    violation_rate = minor_violations / max(1.0, span)
    if violation_rate > STRUCTURE_MAX_VIOLATION_RATE:
        return None

    # Long, clean, multi-touch lines rank above short mathematical fits.
    cleanliness = max(0.0, 1.0 - violation_rate * 4.0)
    recency = max(0.0, 1.0 - (len(klines) - 1 - i2) / max(1.0, STRUCTURE_MAX_ANCHOR_AGE))
    miss_quality = max(0.0, 1.0 - min(1.0, near_misses / max(1.0, span * 0.12)))
    slope_quality = max(0.0, 1.0 - normalized_slope / max(STRUCTURE_MAX_SLOPE_ATR_PER_BAR, 1e-9))

    score = (
        touches * 20.0
        + strong_pivot_touches * 7.0
        + min(25.0, span / 5.0)
        + min(20.0, anchor_strength * 1.5)
        + cleanliness * 25.0
        + recency * 10.0
        + miss_quality * 8.0
        + slope_quality * 7.0
        - min(18.0, near_misses * 1.5)
        - min(20.0, max_violation * 8.0)
    )

    if broken_at is not None and broken_at <= i2:
        return None

    return {
        "start": {"time": p1["time"], "price": p1["price"]},
        "end": {"time": p2["time"], "price": p2["price"]},
        "i1": i1, "i2": i2, "p1": p1["price"], "p2": p2["price"], "slope": slope,
        "touches": touches, "near_misses": near_misses,
        "score": round(score, 2), "violations": minor_violations,
        "max_violation_atr": round(max_violation, 3),
        "max_miss_atr": round(max_miss_atr, 3),
        "normalized_slope": round(normalized_slope, 4),
        "strength": anchor_strength, "side": side, "span": span, "base_atr": base_atr,
    }


def _trendline_candidates(klines, atrs, pivots, side, min_extrema_distance=1, strict_spacing=False, min_touches=3, touch_tolerance_pct=0.1, require_no_cross=False, allow_minor_pierce=False):
    if len(pivots) < 2:
        return []

    pivots = _filter_extrema_by_spacing(pivots, min_extrema_distance, strict_spacing)
    if len(pivots) < 2:
        return []

    # Work from the strongest/recent structural pivots, not every local wiggle.
    ordered = sorted(
        pivots,
        key=lambda p: (p.get("strength", 1), p.get("radius", 0), p["index"]),
        reverse=True,
    )
    strong = [p for p in ordered if p.get("radius", 0) >= STRUCTURE_LARGE_PIVOT]
    recent = sorted(pivots[-STRUCTURE_MAX_PIVOTS_FOR_LINES:], key=lambda p: p["index"])
    pool = []
    seen = set()
    for p in strong[:STRUCTURE_MAX_STRONG_PIVOTS_FOR_LINES] + recent:
        key = (p["index"], round(p["price"], 12))
        if key not in seen:
            seen.add(key)
            pool.append(p)
    pool.sort(key=lambda p: p["index"])

    candidates = []
    for a in range(len(pool) - 1):
        for b in range(a + 1, len(pool)):
            p1, p2 = pool[a], pool[b]
            candidate = _validate_trendline(klines, atrs, pivots, p1, p2, side, min_touches=min_touches, touch_tolerance_pct=touch_tolerance_pct)
            if candidate:
                candidates.append(candidate)

    candidates.sort(
        key=lambda x: (x["score"], x["touches"], x["span"], x["strength"]),
        reverse=True,
    )
    return candidates


def _lines_overlap(a, b, klines):
    left = max(a["i1"], b["i1"])
    right = min(a["i2"], b["i2"])
    if right <= left:
        return False
    overlap = right - left
    shorter = max(1, min(a["i2"] - a["i1"], b["i2"] - b["i1"]))
    if overlap / shorter < 0.55:
        return False

    pa = _line_price(a, left)
    pb = _line_price(b, left)
    mid = max(0, min(len(klines) - 1, left))
    candle_range = abs(float(klines[mid]["high"]) - float(klines[mid]["low"])) if klines else 0.0
    atr = max(candle_range, a.get("base_atr", 0.0), b.get("base_atr", 0.0), 1e-9)
    return abs(pa - pb) <= max(0.5 * atr, pa * 0.002)


def _select_lines(candidates, klines):
    selected = []
    for candidate in candidates:
        if any(_lines_overlap(candidate, chosen, klines) for chosen in selected):
            continue
        selected.append(candidate)
        if len(selected) >= STRUCTURE_MAX_LINES_PER_SIDE:
            break
    return selected


def _filter_cascade_points(klines, points, side, allow_minor_pierce=False, tolerance_pct=0.1):
    """Keep only cascade extrema that remain valid after their own touch.

    A cascade is a chain of still-defended horizontal extrema. An old pivot
    must not remain a cascade vertex after price has subsequently crossed its
    exact level.  The check starts after the pivot candle itself, so the pivot
    candle is allowed to make the intended touch.  When minor pierces are
    explicitly allowed, the same tolerance is used as for clean horizontal
    levels; a close beyond the level still invalidates the vertex.
    """
    if not klines or not points:
        return []

    result = []
    for point in sorted(points, key=lambda p: p["index"]):
        try:
            anchor_index = int(point["index"])
            price = float(point["price"])
        except (TypeError, ValueError, KeyError):
            continue
        if price <= 0 or anchor_index < 0 or anchor_index >= len(klines):
            continue

        base_atr = float(klines[anchor_index].get("atr", 0.0) or 0.0)
        crossed, _, _ = _structure_line_crossing(
            klines,
            lambda _i, level=price: level,
            anchor_index,
            side,
            allow_minor_pierce=allow_minor_pierce,
            base_atr=base_atr,
            tolerance_pct=tolerance_pct,
        )
        if not crossed:
            result.append(point)

    return result


def _cluster_cascade(cluster):
    points = sorted(cluster.get("points", []), key=lambda p: p["index"])
    if len(points) < 2:
        return "none", len(points), None
    gaps = []
    for a, b in zip(points, points[1:]):
        if a["price"]:
            gaps.append(abs(b["price"] - a["price"]) / a["price"] * 100.0)
    return "exists", len(points), max(gaps) if gaps else None


def _level_interaction_engine(klines, level_price, side):
    """Classify the latest interaction with a horizontal S/R level.

    Events are deliberately mutually prioritized: confirmed breakout/retest
    outranks a simple touch, while a wick/body pierce remains distinct from a
    clean touch. The engine uses candle structure plus ATR and returns only the
    latest relevant event so it does not flood the UI with historical repeats.
    """
    if level_price is None or level_price <= 0 or len(klines) < 4:
        return {"type": "NONE", "price": level_price, "side": side, "time": None,
                "distance_atr": None, "pierce_type": None, "confirmed": False}

    atrs = _atr_series(klines)
    level = float(level_price)
    events = []

    def candle_event(i):
        k = klines[i]
        atr = max(float(atrs[i] or 0.0), level * 0.0005, 1e-9)
        o = float(k["open"]); h = float(k["high"]); l = float(k["low"]); c = float(k["close"])
        if side == "resistance":
            approach = max(0.0, level - h)
            penetration = max(0.0, h - level)
            body_penetration = max(0.0, c - level) if o > level or c > level else 0.0
            close_beyond = c > level + LEVEL_INTERACTION_BREAK_ATR * atr
            rejection = h >= level - LEVEL_INTERACTION_TOUCH_ATR * atr and c < level - LEVEL_INTERACTION_TOUCH_ATR * atr
        else:
            approach = max(0.0, l - level)
            penetration = max(0.0, level - l)
            body_penetration = max(0.0, level - c) if o < level or c < level else 0.0
            close_beyond = c < level - LEVEL_INTERACTION_BREAK_ATR * atr
            rejection = l <= level + LEVEL_INTERACTION_TOUCH_ATR * atr and c > level + LEVEL_INTERACTION_TOUCH_ATR * atr

        near = approach > LEVEL_INTERACTION_TOUCH_ATR * atr and approach <= LEVEL_INTERACTION_NEAR_ATR * atr
        touched = approach <= LEVEL_INTERACTION_TOUCH_ATR * atr
        pierce = penetration > LEVEL_INTERACTION_TOUCH_ATR * atr
        strong_pierce = penetration > LEVEL_INTERACTION_PIERCE_ATR * atr

        return {
            "atr": atr, "approach": approach, "penetration": penetration,
            "body_penetration": body_penetration, "close_beyond": close_beyond,
            "rejection": rejection, "near": near, "touched": touched,
            "pierce": pierce, "strong_pierce": strong_pierce,
            "time": int(k["open_time"] / 1000), "close": c,
        }

    # Inspect a short recent window. Heavy historical analysis is intentionally
    # avoided; the interaction engine is cheap enough for the active timeframe.
    start = max(1, len(klines) - 6)
    for i in range(start, len(klines)):
        e = candle_event(i)
        prev = candle_event(i - 1)

        event_type = "NONE"
        pierce_type = None
        confirmed = False
        distance_atr = None

        # Confirmed breakout: three consecutive closes beyond the level.
        if e["close_beyond"]:
            run = 1
            j = i - 1
            while j >= 0 and run < LEVEL_INTERACTION_BREAK_CLOSES and candle_event(j)["close_beyond"]:
                run += 1
                j -= 1
            if run >= LEVEL_INTERACTION_BREAK_CLOSES:
                event_type = "BREAKOUT"
                confirmed = True

        # Retest: price was already beyond the level, returns to it, and closes
        # back in the breakout direction without invalidating the break.
        if event_type == "NONE" and i >= 2:
            before = candle_event(i - 2)
            if side == "resistance":
                was_broken = before["close_beyond"] or prev["close"] > level
                retest = e["touched"] and e["close"] >= level
            else:
                was_broken = before["close_beyond"] or prev["close"] < level
                retest = e["touched"] and e["close"] <= level
            if was_broken and retest:
                event_type = "RETEST"
                confirmed = True

        # A body crossing the level is a body pierce; a wick-only crossing is a
        # wick pierce. Neither is automatically a breakout.
        if event_type == "NONE" and e["pierce"]:
            if e["body_penetration"] > LEVEL_INTERACTION_TOUCH_ATR * e["atr"]:
                pierce_type = "BODY"
            else:
                pierce_type = "WICK"
            event_type = "PIERCE"
            confirmed = e["strong_pierce"]

        # Bounce requires an approach/touch followed by rejection away from the
        # level, rather than merely seeing the price inside the tolerance band.
        if event_type == "NONE" and e["rejection"] and (prev["touched"] or prev["pierce"]):
            event_type = "BOUNCE"
            confirmed = True

        # Near miss is deliberately separate from touch.
        if event_type == "NONE" and e["near"]:
            event_type = "NEAR_MISS"
            distance_atr = round(e["approach"] / e["atr"], 3)

        if event_type == "NONE" and e["touched"]:
            event_type = "TOUCH"
            confirmed = True

        if event_type != "NONE":
            if distance_atr is None:
                distance_atr = round(
                    (e["penetration"] if e["penetration"] > 0 else e["approach"]) / e["atr"], 3
                )
            events.append({
                "type": event_type,
                "price": level,
                "side": side,
                "time": e["time"],
                "distance_atr": distance_atr,
                "pierce_type": pierce_type,
                "confirmed": confirmed,
            })

    return events[-1] if events else {
        "type": "NONE", "price": level, "side": side, "time": None,
        "distance_atr": None, "pierce_type": None, "confirmed": False,
    }


def _structure_line_crossing(klines, line_price_fn, start_index, side, allow_minor_pierce=False, base_atr=0.0, tolerance_pct=0.1):
    """Check candles after a level/line becomes active.

    side='resistance' rejects highs above the line; side='support' rejects
    lows below it. A small wick pierce may be allowed when explicitly enabled.
    The anchor candle(s) are excluded because they are the intended touches.
    """
    if not klines or start_index >= len(klines) - 1:
        return False, 0.0, None
    pct_tol = max(0.0, float(tolerance_pct or 0.0)) / 100.0
    base_atr = max(float(base_atr or 0.0), 1e-12)
    minor_tol = max(base_atr * STRUCTURE_TOUCH_ATR, abs(float(klines[start_index].get("close", 0.0))) * pct_tol) if allow_minor_pierce else 0.0
    max_penetration = 0.0
    crossed_at = None
    for i in range(start_index + 1, len(klines)):
        lp = float(line_price_fn(i))
        atr = max(base_atr, float(klines[i].get("atr", 0.0) or 0.0))
        if side == "resistance":
            penetration = float(klines[i]["high"]) - lp
        else:
            penetration = lp - float(klines[i]["low"])
        if penetration > 0:
            max_penetration = max(max_penetration, penetration)
            allowed = minor_tol if allow_minor_pierce else 0.0
            # A close beyond the line is always a real break, even when a
            # small wick pierce is allowed.
            close_beyond = (float(klines[i]["close"]) > lp + allowed) if side == "resistance" else (float(klines[i]["close"]) < lp - allowed)
            if penetration > allowed or close_beyond:
                crossed_at = i
                return True, max_penetration, crossed_at
    return False, max_penetration, crossed_at


def _horizontal_level_crossed_after_anchor(klines, anchor_index, price, side, allow_minor_pierce=False, tolerance_pct=0.1):
    if price is None or anchor_index is None:
        return False, 0.0, None
    base_atr = 0.0
    if 0 <= anchor_index < len(klines):
        base_atr = float(klines[anchor_index].get("atr", 0.0) or 0.0)
    return _structure_line_crossing(
        klines, lambda _i: float(price), anchor_index, side,
        allow_minor_pierce=allow_minor_pierce, base_atr=base_atr,
        tolerance_pct=tolerance_pct
    )


def market_structure_engine(klines, settings=None):
    """ATR-adaptive S/R + validated pivot trendlines; no UI assumptions."""
    settings = settings or {}
    level_distance = settings.get("horizontalLevelSearchPeriod", 50)
    level_strict = bool(settings.get("horizontalLevelSearchStrict", False))
    level_touches_min = max(2, int(settings.get("horizontalLevelTouches", 2) or 2))
    level_tolerance_pct = max(0.0, float(settings.get("horizontalLevelTouchTolerancePct", 0.1) or 0.1))
    level_lifetime = settings.get("horizontalLevelLifetimeHours")
    level_allow_minor_pierce = bool(settings.get("horizontalLevelAllowMinorPierce", False))
    level_no_cross = bool(settings.get("horizontalLevelNoCross", False) or level_allow_minor_pierce)
    trend_distance = settings.get("trendlineSearchPeriod", 50)
    trend_strict = bool(settings.get("trendlineSearchStrict", False))
    trend_touches_min = max(2, int(settings.get("trendlineTouches", 2) or 2))
    trend_tolerance_pct = max(0.0, float(settings.get("trendlineTouchTolerancePct", 0.1) or 0.1))
    trend_lifetime = settings.get("trendlineLifetimeHours")
    trend_allow_minor_pierce = bool(settings.get("trendlineAllowMinorPierce", False))
    trend_no_cross = bool(settings.get("trendlineNoCross", False) or trend_allow_minor_pierce)
    if len(klines) < 40:
        return {
            "swing_highs": [], "swing_lows": [], "support": None, "resistance": None,
            "touches": 0, "level_type": "", "level_price": None, "level_touches": 0,
            "level_cascade": "none", "cascade_vertices": 0, "cascade_distance_pct": None,
            "cascade_levels": [],
            "consolidation_duration_hours": 0.0, "breakout_status": "none",
            "support_touches": 0, "resistance_touches": 0, "upper_trendlines": [], "lower_trendlines": [],
            "support_interaction": {"type": "NONE"}, "resistance_interaction": {"type": "NONE"},
            "upper": None, "lower": None,
        }

    atrs = _atr_series(klines)
    small_h, small_l = _pivot_points(klines, atrs, STRUCTURE_SMALL_PIVOT)
    large_h, large_l = _pivot_points(klines, atrs, STRUCTURE_LARGE_PIVOT)
    highs_all = _merge_pivots(large_h, small_h, atrs)
    lows_all = _merge_pivots(large_l, small_l, atrs)

    level_highs = _filter_extrema_by_lifetime(highs_all, klines, level_lifetime)
    level_lows = _filter_extrema_by_lifetime(lows_all, klines, level_lifetime)
    level_highs = _filter_extrema_by_spacing(level_highs, level_distance, level_strict)
    level_lows = _filter_extrema_by_spacing(level_lows, level_distance, level_strict)
    trend_highs = _filter_extrema_by_lifetime(highs_all, klines, trend_lifetime)
    trend_lows = _filter_extrema_by_lifetime(lows_all, klines, trend_lifetime)

    current_price = float(klines[-1]["close"])
    support_clusters = _cluster_levels(level_lows, current_price, level_tolerance_pct)
    resistance_clusters = _cluster_levels(level_highs, current_price, level_tolerance_pct)
    support_candidates = [c for c in support_clusters if c.get("touches", 0) >= level_touches_min]
    resistance_candidates = [c for c in resistance_clusters if c.get("touches", 0) >= level_touches_min]
    support_cluster = _pick_level(support_candidates or support_clusters, current_price, "support")
    resistance_cluster = _pick_level(resistance_candidates or resistance_clusters, current_price, "resistance")

    # The displayed horizontal level must be anchored to the actual structural
    # extremum, not to the weighted center of the cluster. Support uses the
    # lowest confirmed low in the cluster; resistance uses the highest high.
    support_anchor = min((float(p["price"]) for p in (support_cluster or {}).get("points", [])), default=None) if support_cluster else None
    resistance_anchor = max((float(p["price"]) for p in (resistance_cluster or {}).get("points", [])), default=None) if resistance_cluster else None
    support = support_cluster["price"] if support_cluster else None
    resistance = resistance_cluster["price"] if resistance_cluster else None
    st = support_cluster["touches"] if support_cluster else 0
    rt = resistance_cluster["touches"] if resistance_cluster else 0

    # Optional clean-level filter: once the last anchor/touch is formed, price
    # must not cross the level. This is disabled by default to preserve the
    # existing detector behaviour.
    if level_no_cross and support is not None and support_cluster and support_cluster.get("points"):
        last_support_anchor = max(int(p["index"]) for p in support_cluster["points"])
        crossed, _, _ = _horizontal_level_crossed_after_anchor(
            klines, last_support_anchor, support_anchor if support_anchor is not None else support,
            "support", allow_minor_pierce=level_allow_minor_pierce, tolerance_pct=level_tolerance_pct
        )
        if crossed:
            support = None; st = 0; support_anchor = None; support_cluster = None
    if level_no_cross and resistance is not None and resistance_cluster and resistance_cluster.get("points"):
        last_resistance_anchor = max(int(p["index"]) for p in resistance_cluster["points"])
        crossed, _, _ = _horizontal_level_crossed_after_anchor(
            klines, last_resistance_anchor, resistance_anchor if resistance_anchor is not None else resistance,
            "resistance", allow_minor_pierce=level_allow_minor_pierce, tolerance_pct=level_tolerance_pct
        )
        if crossed:
            resistance = None; rt = 0; resistance_anchor = None; resistance_cluster = None

    support_interaction = _level_interaction_engine(klines, support, "support") if support is not None else {"type": "NONE"}
    resistance_interaction = _level_interaction_engine(klines, resistance, "resistance") if resistance is not None else {"type": "NONE"}

    trend_highs = _filter_extrema_by_spacing(trend_highs, trend_distance, trend_strict)
    trend_lows = _filter_extrema_by_spacing(trend_lows, trend_distance, trend_strict)
    upper_candidates = _trendline_candidates(klines, atrs, trend_highs, "resistance", trend_distance, trend_strict, trend_touches_min, trend_tolerance_pct, trend_no_cross, trend_allow_minor_pierce)
    lower_candidates = _trendline_candidates(klines, atrs, trend_lows, "support", trend_distance, trend_strict, trend_touches_min, trend_tolerance_pct, trend_no_cross, trend_allow_minor_pierce)
    upper_lines = _select_lines(upper_candidates, klines)
    lower_lines = _select_lines(lower_candidates, klines)

    # Prefer the strongest currently relevant line as the active boundary.
    def active_line(lines, side):
        if not lines:
            return None
        candidates = []
        for line in lines:
            projected = _line_price(line, len(klines) - 1)
            if side == "resistance" and projected >= current_price * 0.995:
                distance = abs(projected - current_price) / current_price
                candidates.append((line, distance))
            elif side == "support" and projected <= current_price * 1.005:
                distance = abs(projected - current_price) / current_price
                candidates.append((line, distance))
        if not candidates:
            return lines[0]
        candidates.sort(key=lambda x: (-x[0]["score"], x[1]))
        return candidates[0][0]

    upper = active_line(upper_lines, "resistance")
    lower = active_line(lower_lines, "support")

    # For the formation engine the validated trendlines take precedence over
    # crude averages. Horizontal clusters remain the fallback S/R source.
    active_resistance = resistance
    active_support = support
    if upper:
        projected = _line_price(upper, len(klines) - 1)
        if projected >= current_price * 0.98:
            active_resistance = projected
    if lower:
        projected = _line_price(lower, len(klines) - 1)
        if projected <= current_price * 1.02:
            active_support = projected

    breakout = "none"
    if active_resistance and current_price > active_resistance:
        breakout = "up"
    elif active_support and current_price < active_support:
        breakout = "down"

    if active_support is not None and active_resistance is not None:
        ds = abs(current_price - active_support) / current_price if current_price else 999
        dr = abs(active_resistance - current_price) / current_price if current_price else 999
        if ds <= dr:
            level_type, level_price, level_touches = "long", active_support, st
        else:
            level_type, level_price, level_touches = "short", active_resistance, rt
    elif active_support is not None:
        level_type, level_price, level_touches = "long", active_support, st
    elif active_resistance is not None:
        level_type, level_price, level_touches = "short", active_resistance, rt
    else:
        level_type, level_price, level_touches = "", None, 0

    cascade_cluster = support_cluster if level_type == "long" else resistance_cluster if level_type == "short" else None
    cascade_side = "support" if level_type == "long" else "resistance" if level_type == "short" else None
    if cascade_cluster and cascade_side:
        # Cascade vertices are independently validated. A historical extremum
        # that was later crossed is removed from the cascade even when the
        # general level filter is not enabled. This prevents old/broken pivots
        # from being presented as current stop-liquidity levels.
        cascade_points = _filter_cascade_points(
            klines,
            cascade_cluster.get("points", []),
            cascade_side,
            allow_minor_pierce=level_allow_minor_pierce,
            tolerance_pct=level_tolerance_pct,
        )
        cascade_cluster = dict(cascade_cluster)
        cascade_cluster["points"] = cascade_points
        cascade_cluster["touches"] = len(cascade_points)
    level_cascade, cascade_vertices, cascade_distance_pct = _cluster_cascade(cascade_cluster) if cascade_cluster else ("none", 0, None)
    # A cascade is meaningful only when at least two still-valid vertices
    # participate. Never expose a one-point "cascade 1/1" to the chart.
    valid_cascade_points = (
        sorted((cascade_cluster or {}).get("points", []), key=lambda p: p["index"])
        if level_cascade == "exists" and cascade_vertices >= 2
        else []
    )

    start_i = min(
        [x["index"] for x in (highs_all[-8:] + lows_all[-8:])],
        default=len(klines) - 1,
    )
    start_time = int(klines[start_i]["open_time"] / 1000)
    end_time = int(klines[-1]["open_time"] / 1000)
    duration_hours = max(0.0, (end_time - start_time) / 3600.0)

    # Keep overlay readable: only the latest structurally relevant pivots are shown.
    display_highs = highs_all[-8:]
    display_lows = lows_all[-8:]
    clean_point = lambda p: {"index": p["index"], "time": p["time"], "price": p["price"], "strength": p.get("strength", 1)}

    return {
        "swing_highs": [clean_point(x) for x in display_highs],
        "swing_lows": [clean_point(x) for x in display_lows],
        "support": support,
        "resistance": resistance,
        "support_touches": st,
        "resistance_touches": rt,
        "support_anchor": support_anchor,
        "resistance_anchor": resistance_anchor,
        "support_interaction": support_interaction,
        "resistance_interaction": resistance_interaction,
        "touches": st + rt,
        "level_type": level_type,
        "level_price": level_price,
        "level_touches": level_touches,
        "level_cascade": level_cascade,
        "cascade_vertices": cascade_vertices,
        "cascade_distance_pct": cascade_distance_pct,
        # Expose every extremum participating in the detected cascade so the
        # chart can show the actual horizontal levels that form the cascade.
        "cascade_levels": [clean_point(x) for x in valid_cascade_points],
        "consolidation_duration_hours": duration_hours,
        "breakout_status": breakout,
        "upper_trendlines": upper_lines,
        "lower_trendlines": lower_lines,
        "upper": upper,
        "lower": lower,
    }


def _line_slope_pct(line, price):
    if not line or not price:
        return 0.0
    return (line["slope"] / price) * 100.0


def _formation_interaction_engine(klines, upper=None, lower=None, support=None, resistance=None):
    """Classify the latest interaction of price with a formation boundary.

    Formation state is intentionally separate from horizontal-level interaction:
    ACTIVE, NEAR_BREAKOUT, PIERCED, BROKEN, INVALIDATED.
    """
    if not klines or len(klines) < 4:
        return {"state": "ACTIVE", "boundary": None, "side": None,
                "time": None, "distance_atr": None, "confirmed": False}

    atrs = _atr_series(klines)
    last_i = len(klines) - 1

    boundaries = []
    if upper:
        boundaries.append(("upper", "resistance", lambda i: _line_price(upper, i)))
    elif resistance is not None:
        boundaries.append(("resistance", "resistance", lambda i: float(resistance)))
    if lower:
        boundaries.append(("lower", "support", lambda i: _line_price(lower, i)))
    elif support is not None:
        boundaries.append(("support", "support", lambda i: float(support)))

    if not boundaries:
        return {"state": "ACTIVE", "boundary": None, "side": None,
                "time": None, "distance_atr": None, "confirmed": False}

    def boundary_event(boundary, side, price_fn, i):
        k = klines[i]
        level = float(price_fn(i))
        atr = max(float(atrs[i] or 0.0), abs(level) * 0.0005, 1e-9)
        o = float(k["open"]); h = float(k["high"]); l = float(k["low"]); c = float(k["close"])
        near = LEVEL_INTERACTION_NEAR_ATR * atr
        break_dist = LEVEL_INTERACTION_BREAK_ATR * atr

        if side == "resistance":
            distance = max(0.0, level - c)
            wick_pierced = h > level + LEVEL_INTERACTION_TOUCH_ATR * atr and c <= level + LEVEL_INTERACTION_TOUCH_ATR * atr
            close_beyond = c > level + break_dist
            inside = c <= level + LEVEL_INTERACTION_TOUCH_ATR * atr
        else:
            distance = max(0.0, c - level)
            wick_pierced = l < level - LEVEL_INTERACTION_TOUCH_ATR * atr and c >= level - LEVEL_INTERACTION_TOUCH_ATR * atr
            close_beyond = c < level - break_dist
            inside = c >= level - LEVEL_INTERACTION_TOUCH_ATR * atr

        return {
            "boundary": boundary, "side": side, "level": level, "atr": atr,
            "near": distance <= near, "wick_pierced": wick_pierced,
            "close_beyond": close_beyond, "inside": inside,
            "distance_atr": round(abs(distance) / atr, 3),
            "time": int(k["open_time"] / 1000),
            "open": o, "close": c,
        }

    recent = []
    for boundary, side, price_fn in boundaries:
        latest = boundary_event(boundary, side, price_fn, last_i)
        previous = boundary_event(boundary, side, price_fn, max(0, last_i - 1))

        run = 1 if latest["close_beyond"] else 0
        j = last_i - 1
        while j >= 0 and run < LEVEL_INTERACTION_BREAK_CLOSES:
            e = boundary_event(boundary, side, price_fn, j)
            if not e["close_beyond"]:
                break
            run += 1
            j -= 1

        if run >= LEVEL_INTERACTION_BREAK_CLOSES:
            latest["state"] = "BROKEN"
            latest["confirmed"] = True
        elif latest["wick_pierced"] and latest["inside"]:
            latest["state"] = "PIERCED"
            latest["confirmed"] = True
        elif latest["near"]:
            latest["state"] = "NEAR_BREAKOUT"
            latest["confirmed"] = False
        else:
            latest["state"] = "ACTIVE"
            latest["confirmed"] = False
        recent.append(latest)

    # A formation is invalidated when both boundaries have been decisively lost;
    # this is stronger than a single-side breakout and prevents over-classifying
    # ordinary breakouts as invalid formations.
    broken = [e for e in recent if e["state"] == "BROKEN"]
    if len(broken) >= 2:
        chosen = broken[-1]
        return {
            "state": "INVALIDATED", "boundary": chosen["boundary"],
            "side": chosen["side"], "time": chosen["time"],
            "distance_atr": chosen["distance_atr"], "confirmed": True,
        }

    priority = {"ACTIVE": 0, "NEAR_BREAKOUT": 1, "PIERCED": 2, "BROKEN": 3}
    chosen = max(recent, key=lambda e: (priority[e["state"]], e["time"]))
    return {
        "state": chosen["state"], "boundary": chosen["boundary"],
        "side": chosen["side"], "time": chosen["time"],
        "distance_atr": chosen["distance_atr"], "confirmed": chosen["confirmed"],
    }


def pattern_engine(klines, timeframe, settings=None):
    structure = market_structure_engine(klines, settings=settings)
    highs = structure["swing_highs"]
    lows = structure["swing_lows"]
    upper = structure.get("upper")
    lower = structure.get("lower")
    support = structure.get("support")
    resistance = structure.get("resistance")
    close = float(klines[-1]["close"])

    if len(highs) < 2 or len(lows) < 2:
        return {
            "pattern": None, "pattern_type": None, "timeframe": timeframe, **structure,
            "formation_duration_hours": 0.0, "stage": "none", "distance_to_breakout": None,
            "formation_interaction": {"state": "ACTIVE", "boundary": None, "side": None, "time": None, "distance_atr": None, "confirmed": False},
            "score": 0,
            "overlay": {"support": None, "resistance": None, "upper": None, "lower": None,
                        "swing_highs": [], "swing_lows": [], "level": {"type": "", "price": None, "touches": 0}}
        }

    # Formation recognition is geometry-first: it requires validated boundaries.
    hs = _line_slope_pct(upper, close) if upper else 0.0
    ls = _line_slope_pct(lower, close) if lower else 0.0
    horizontal_tol = 0.06  # % price slope per bar, intentionally conservative.
    pattern, ptype = None, None

    if upper and lower:
        converging = (upper["slope"] > lower["slope"] and upper["p2"] > lower["p2"]) or (upper["slope"] < lower["slope"] and upper["p2"] < lower["p2"])
        if abs(hs) <= horizontal_tol and ls > horizontal_tol:
            pattern, ptype = "Triangle", "Ascending"
        elif abs(ls) <= horizontal_tol and hs < -horizontal_tol:
            pattern, ptype = "Triangle", "Descending"
        elif converging and hs < -horizontal_tol and ls > horizontal_tol:
            pattern, ptype = "Triangle", "Symmetrical"
        elif hs > horizontal_tol and ls > horizontal_tol and abs(hs - ls) > horizontal_tol:
            pattern, ptype = "Wedge", "Rising"
        elif hs < -horizontal_tol and ls < -horizontal_tol and abs(hs - ls) > horizontal_tol:
            pattern, ptype = "Wedge", "Falling"
        elif abs(hs - ls) <= horizontal_tol:
            if abs(hs) <= horizontal_tol and abs(ls) <= horizontal_tol:
                pattern, ptype = "Channel", "Horizontal"
            elif hs > horizontal_tol:
                pattern, ptype = "Channel", "Ascending"
            elif hs < -horizontal_tol:
                pattern, ptype = "Channel", "Descending"

    # Horizontal levels can still form a valid double/multiple top or bottom.
    if not pattern:
        if resistance and structure.get("resistance_touches", 0) >= 2:
            pattern, ptype = "Multiple Top", "Horizontal Resistance"
        elif support and structure.get("support_touches", 0) >= 2:
            pattern, ptype = "Multiple Bottom", "Horizontal Support"

    formation_interaction = _formation_interaction_engine(
        klines, upper=upper, lower=lower, support=support, resistance=resistance
    ) if pattern else {"state": "ACTIVE", "boundary": None, "side": None, "time": None, "distance_atr": None, "confirmed": False}

    distances = []
    if upper:
        distances.append(abs(_line_price(upper, len(klines)-1) - close) / close * 100.0)
    if lower:
        distances.append(abs(_line_price(lower, len(klines)-1) - close) / close * 100.0)
    if resistance and close > 0:
        distances.append(abs(resistance - close) / close * 100.0)
    if support and close > 0:
        distances.append(abs(support - close) / close * 100.0)
    distance = min(distances) if distances else None

    touches = max(
        upper.get("touches", 0) if upper else 0,
        lower.get("touches", 0) if lower else 0,
        structure.get("touches", 0),
    )
    stage = "none"
    if pattern:
        if formation_interaction.get("state") == "INVALIDATED":
            stage = "invalidated"
        elif formation_interaction.get("state") == "BROKEN":
            stage = "breakout"
        elif formation_interaction.get("state") == "PIERCED":
            stage = "pierced"
        elif formation_interaction.get("state") == "NEAR_BREAKOUT":
            stage = "near_breakout"
        elif touches >= 5:
            stage = "mature"
        elif touches >= 3:
            stage = "forming"
        else:
            stage = "early"

    first_indices = [x["index"] for x in (highs[-1] if highs else None, lows[-1] if lows else None) if x is not None]
    formation_start = min(first_indices) if first_indices else len(klines) - 1
    formation_duration_hours = max(
        0.0,
        (int(klines[-1]["open_time"]) - int(klines[formation_start]["open_time"])) / 3600000.0,
    )

    def line_from_candidate(candidate):
        if not candidate:
            return None
        return {
            "start": candidate["start"], "end": candidate["end"],
            "touches": candidate.get("touches", 0),
            "score": candidate.get("score", 0),
            "violations": candidate.get("violations", 0),
            "slope": candidate.get("slope", 0.0),
        }

    def first_cluster_time(cluster):
        points = (cluster or {}).get("points", []) if isinstance(cluster, dict) else []
        times = [int(p.get("time", 0)) for p in points if p.get("time") is not None and int(p.get("time", 0)) > 0]
        return min(times) if times else None

    overlay = {
        "support": {"price": structure.get("support_anchor", support), "time": first_cluster_time(support_cluster), "cluster_price": support, "touches": structure.get("support_touches", 0)} if support is not None else None,
        "resistance": {"price": structure.get("resistance_anchor", resistance), "time": first_cluster_time(resistance_cluster), "cluster_price": resistance, "touches": structure.get("resistance_touches", 0)} if resistance is not None else None,
        "upper": line_from_candidate(upper),
        "lower": line_from_candidate(lower),
        "upper_lines": [line_from_candidate(x) for x in structure.get("upper_trendlines", [])],
        "lower_lines": [line_from_candidate(x) for x in structure.get("lower_trendlines", [])],
        "cascade_levels": [
            {
                "index": int(p.get("index", 0)),
                "time": int(p.get("time", 0)),
                "price": float(p.get("price")),
                "side": "long" if structure.get("level_type") == "long" else "short"
            }
            for p in structure.get("cascade_levels", [])
            if p.get("price") is not None and p.get("time") is not None
        ],
        "swing_highs": highs[-8:],
        "swing_lows": lows[-8:],
        "level": {
            "type": structure.get("level_type", ""),
            "price": (structure.get("support_anchor") if structure.get("level_type") == "long" else structure.get("resistance_anchor") if structure.get("level_type") == "short" else structure.get("level_price")),
            "cluster_price": structure.get("level_price"),
            "touches": structure.get("level_touches", 0),
            "interaction": (structure.get("support_interaction") if structure.get("level_type") == "long"
                             else structure.get("resistance_interaction") if structure.get("level_type") == "short" else {"type": "NONE"}),
        },
    }

    score = 0
    if pattern:
        score += 30
    if upper:
        score += min(25, upper.get("touches", 0) * 6)
        score += min(15, upper.get("score", 0) / 10)
    if lower:
        score += min(25, lower.get("touches", 0) * 6)
        score += min(15, lower.get("score", 0) / 10)
    if distance is not None:
        score += max(0, 15 - int(distance * 8))
    if structure.get("breakout_status") != "none":
        score += 15

    return {
        "pattern": pattern, "pattern_type": ptype, "timeframe": timeframe, **structure,
        "formation_duration_hours": formation_duration_hours,
        "stage": stage, "distance_to_breakout": distance,
        "formation_interaction": formation_interaction,
        "score": min(100, int(round(score))),
        "overlay": overlay,
    }


def _volume_direction_from_values(values):
    """Classify a completed volume window as up/down/any using a smoothed path."""
    if len(values) < 4 or any(float(v) <= 0 for v in values):
        return "any"
    sm = []
    for j in range(len(values)):
        lo = max(0, j - 1); hi = min(len(values), j + 2)
        window = sorted(float(v) for v in values[lo:hi])
        mid = len(window) // 2
        sm.append(window[mid] if len(window) % 2 else (window[mid-1] + window[mid]) / 2.0)
    import math as _math
    logs = [_math.log(max(v, 1e-12)) for v in sm]
    n = len(logs)
    xbar = (n - 1) / 2.0
    ybar = sum(logs) / n
    denom = sum((i - xbar) ** 2 for i in range(n))
    slope = sum((i - xbar) * (logs[i] - ybar) for i in range(n)) / denom if denom else 0.0
    edge = max(3, min(5, n // 4))
    first = sum(sm[:edge]) / edge
    last = sum(sm[-edge:]) / edge
    recent_n = max(3, min(6, n // 3))
    recent_logs = logs[-recent_n:]
    rxbar = (recent_n - 1) / 2.0
    rybar = sum(recent_logs) / recent_n
    rden = sum((i-rxbar) ** 2 for i in range(recent_n))
    recent_slope = sum((i-rxbar)*(recent_logs[i]-rybar) for i in range(recent_n)) / rden if rden else 0.0
    positive_steps = sum(1 for a,b in zip(sm, sm[1:]) if b > a * 1.002)
    negative_steps = sum(1 for a,b in zip(sm, sm[1:]) if b < a * 0.998)
    step_count = max(1, len(sm) - 1)
    if slope > 0 and recent_slope > -0.002 and last > first * 1.05 and positive_steps >= step_count * 0.40:
        return "up"
    if slope < 0 and recent_slope < 0.002 and last < first * 0.95 and negative_steps >= step_count * 0.40:
        return "down"
    return "any"


def _volume_expansion_latest_from_rows(rows, base_candles=100, growth_candles=20):
    base_candles = max(1, min(int(base_candles), 500))
    growth_candles = max(1, min(int(growth_candles), 200))
    candles = [r for r in (rows or []) if isinstance(r, (list, tuple)) and len(r) >= 6]
    if len(candles) > 1:
        candles = candles[:-1]
    if len(candles) < base_candles + growth_candles:
        return None
    volumes = []
    for row in candles:
        try: volumes.append(float(row[5]))
        except (TypeError, ValueError, IndexError): return None
    growth = volumes[-growth_candles:]
    base = volumes[-growth_candles-base_candles:-growth_candles]
    if len(base) < base_candles or not growth:
        return None
    base_avg = sum(base) / len(base)
    if base_avg <= 0:
        return None
    return {"ratio": sum(growth) / len(growth) / base_avg,
            "direction": _volume_direction_from_values(growth)}


def _volume_expansion_windows_from_rows(rows, base_candles=100, growth_candles=20,
                                        min_ratio=None, max_ratio=None, direction="any",
                                        oi_points=None, price_min=None, price_max=None,
                                        oi_min=None, oi_max=None, oi_direction="any"):
    """Find historical volume-expansion zones with a robust directional check.

    The ratio is still average(volume of the latest M completed candles) /
    average(volume of the preceding N completed candles), but direction is
    determined from a smoothed volume path inside those M candles.  This keeps
    a single abnormal candle from being interpreted as a sustained rise.
    """
    base_candles = max(1, min(int(base_candles), 500))
    growth_candles = max(1, min(int(growth_candles), 200))
    direction = str(direction or "any").lower()
    if direction not in {"any", "up", "down"}:
        direction = "any"
    oi_direction = str(oi_direction or "any").lower()
    if oi_direction not in {"any", "up", "down"}:
        oi_direction = "any"
    candles = [r for r in (rows or []) if isinstance(r, (list, tuple)) and len(r) >= 6]
    if len(candles) > 1:
        candles = candles[:-1]  # ignore the still-forming Binance candle
    if len(candles) < base_candles + growth_candles:
        return []

    volumes, closes, times = [], [], []
    for row in candles:
        try:
            volumes.append(float(row[5]))
            closes.append(float(row[4]))
            times.append(int(row[0]) // 1000)
        except (TypeError, ValueError, IndexError):
            return []

    # Small rolling median removes isolated volume spikes without flattening
    # the overall 20-candle direction.
    def smooth(values):
        out = []
        n = len(values)
        for j in range(n):
            lo = max(0, j - 1); hi = min(n, j + 2)
            window = sorted(values[lo:hi])
            mid = len(window) // 2
            out.append(window[mid] if len(window) % 2 else (window[mid-1] + window[mid]) / 2.0)
        return out

    def directional_match(values, wanted):
        return wanted == "any" or _volume_direction_from_values(values) == wanted


    def nearest_oi_value(ts):
        if not oi_points:
            return None
        # Use the latest OI observation at or before the candle timestamp.
        best = None
        for point in oi_points:
            pt = int(point.get("time", 0))
            if pt <= ts:
                best = point
            else:
                break
        return float(best.get("oiValue")) if best and best.get("oiValue") is not None else None

    qualifying = []
    start_i = base_candles + growth_candles - 1
    for i in range(start_i, len(volumes)):
        growth = volumes[i - growth_candles + 1:i + 1]
        base_end = i - growth_candles + 1
        base = volumes[base_end - base_candles:base_end]
        if len(base) < base_candles or not growth:
            continue
        base_avg = sum(base) / len(base)
        growth_avg = sum(growth) / len(growth)
        if base_avg <= 0:
            continue
        ratio = growth_avg / base_avg
        if min_ratio is not None and ratio < min_ratio:
            continue
        if max_ratio is not None and ratio > max_ratio:
            continue
        if not directional_match(growth, direction):
            continue

        growth_start = i - growth_candles + 1
        price_start = closes[growth_start]
        price_end = closes[i]
        price_change = ((price_end / price_start) - 1.0) * 100.0 if price_start > 0 else None
        oi_start = nearest_oi_value(times[growth_start])
        oi_end = nearest_oi_value(times[i])
        oi_change = ((oi_end / oi_start) - 1.0) * 100.0 if oi_start and oi_start > 0 and oi_end is not None else None
        if price_min is not None and (price_change is None or price_change < price_min):
            continue
        if price_max is not None and (price_change is None or price_change > price_max):
            continue
        if oi_min is not None and (oi_change is None or oi_change < oi_min):
            continue
        if oi_max is not None and (oi_change is None or oi_change > oi_max):
            continue
        if oi_direction == "up" and (oi_change is None or oi_change <= 0):
            continue
        if oi_direction == "down" and (oi_change is None or oi_change >= 0):
            continue
        qualifying.append((i, ratio, price_change, oi_change))

    if not qualifying:
        return []
    zones = []
    zone_start = zone_end = qualifying[0][0]
    best_i, best_ratio, best_price, best_oi = qualifying[0]
    for i, ratio, price_change, oi_change in qualifying[1:]:
        if i <= zone_end + 1:
            zone_end = i
            if ratio > best_ratio:
                best_i, best_ratio, best_price, best_oi = i, ratio, price_change, oi_change
        else:
            zones.append((zone_start, zone_end, best_i, best_ratio, best_price, best_oi))
            zone_start = zone_end = i
            best_i, best_ratio, best_price, best_oi = i, ratio, price_change, oi_change
    zones.append((zone_start, zone_end, best_i, best_ratio, best_price, best_oi))

    result = []
    for a, b, peak_i, peak_ratio, peak_price, peak_oi in zones:
        actual_start_i = max(0, a - growth_candles + 1)
        actual_end_i = min(len(times) - 1, b)
        result.append({
            "start_time": times[actual_start_i], "end_time": times[actual_end_i],
            "time": times[peak_i], "ratio": peak_ratio,
            "base_candles": base_candles, "growth_candles": growth_candles,
            "direction": direction, "price_change_pct": peak_price,
            "oi_change_pct": peak_oi,
        })
    return result


@app.post("/api/volume_expansion_history")
def api_volume_expansion_history():
    payload = request.get_json(silent=True) or {}
    raw_symbols = payload.get("symbols") or []
    if not isinstance(raw_symbols, list):
        return jsonify({"error": "symbols must be a list"}), 400
    symbols = list(dict.fromkeys(str(x).upper().strip() for x in raw_symbols
                                 if str(x).upper().strip().endswith("USDT")))[:200]
    timeframe = str(payload.get("timeframe") or "5m").strip()
    allowed = {"1m", "5m", "15m", "30m", "1h", "4h", "1D"}
    if timeframe not in allowed:
        return jsonify({"error": "Invalid timeframe"}), 400
    try:
        base_candles = max(1, min(500, int(payload.get("baseCandles", 100))))
        growth_candles = max(1, min(200, int(payload.get("growthCandles", 20))))
        min_ratio = payload.get("min")
        max_ratio = payload.get("max")
        min_ratio = None if min_ratio in (None, "") else float(min_ratio)
        max_ratio = None if max_ratio in (None, "") else float(max_ratio)
        price_min = payload.get("priceMin")
        price_max = payload.get("priceMax")
        price_min = None if price_min in (None, "") else float(price_min)
        price_max = None if price_max in (None, "") else float(price_max)
        oi_min = payload.get("oiMin")
        oi_max = payload.get("oiMax")
        oi_min = None if oi_min in (None, "") else float(oi_min)
        oi_max = None if oi_max in (None, "") else float(oi_max)
        direction = str(payload.get("direction") or "any").lower()
        oi_direction = str(payload.get("oiDirection") or "any").lower()
        if direction not in {"any", "up", "down"} or oi_direction not in {"any", "up", "down"}:
            raise ValueError("Invalid direction")
        limit = max(1, min(200, int(payload.get("limit", 50))))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid volume expansion settings"}), 400
    if not symbols:
        return jsonify({"results": [], "scanned": 0})

    def one(symbol):
        try:
            candle_limit = min(1500, base_candles + growth_candles + 1 + 1000)
            url = ("https://fapi.binance.com/fapi/v1/klines"
                   f"?symbol={quote(symbol)}&interval={quote(timeframe)}&limit={candle_limit}")
            req = Request(url, headers={"User-Agent": "Crypto-Screener/1.0"})
            with urlopen(req, timeout=12) as response:
                rows = json.loads(response.read().decode("utf-8"))
            oi_points = []
            try:
                oi_period = _alert_oi_period(timeframe)
                oi_points = _alert_fetch_open_interest_history(symbol, oi_period, 500)
                oi_points = sorted(
                    [{"time": int(x.get("timestamp", 0)) // 1000, "oiValue": float(x.get("sumOpenInterestValue", 0) or 0)}
                     for x in oi_points if x.get("timestamp") is not None],
                    key=lambda x: x["time"]
                )
            except Exception:
                oi_points = []
            events = _volume_expansion_windows_from_rows(
                rows, base_candles, growth_candles, min_ratio, max_ratio, direction,
                oi_points, price_min, price_max, oi_min, oi_max, oi_direction
            )
            return [{"symbol": symbol, **event} for event in events]
        except Exception as exc:
            log(f"Volume expansion history failed for {symbol}: {exc}", "ERROR")
            return []

    results = []
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = [pool.submit(one, symbol) for symbol in symbols]
        for future in as_completed(futures):
            results.extend(future.result())
    results.sort(key=lambda x: (float(x.get("ratio", 0)), int(x.get("time", 0))), reverse=True)
    return jsonify({"results": results[:limit], "scanned": len(symbols), "timeframe": timeframe,
                    "baseCandles": base_candles, "growthCandles": growth_candles,
                    "direction": direction, "oiDirection": oi_direction})


@app.get("/api/template_market_analysis")
def api_template_market_analysis():
    """
    Supply template market metrics using the same timeframe-aware market
    metric path as Custom Alerts. The selected timeframe is authoritative
    for every timeframe-sensitive metric; 1D uses the live 24h Market State.
    """
    symbols_raw = (request.args.get("symbols") or "").upper().strip()
    timeframes_raw = (request.args.get("timeframes") or "").lower().strip()
    allowed = set(PATTERN_INTERVALS)
    timeframes = [x.strip() for x in timeframes_raw.split(",") if x.strip() in allowed]
    symbols = [x.strip() for x in symbols_raw.split(",") if x.strip() and x.strip().isalnum()][:100]
    try:
        volume_base = max(1, min(500, int(request.args.get("volume_base", 100))))
        volume_growth = max(1, min(200, int(request.args.get("volume_growth", 20))))
    except (TypeError, ValueError):
        volume_base, volume_growth = 100, 20
    if not symbols or not timeframes:
        return jsonify({"results": []})

    with state_lock:
        state_coins = {str(symbol).upper(): dict(coin) for symbol, coin in STATE["coins"].items()}

    def one(symbol):
        coin = state_coins.get(symbol.upper(), {})
        result = {"symbol": symbol, "metrics": {}}
        for interval in timeframes:
            try:
                # Use the exact same timeframe-aware metric source as Custom
                # Alerts. Do not reuse the 24h Market State values for an
                # intraday template timeframe.
                market_metrics = _alert_timeframe_market_metrics(symbol, interval)
                change = market_metrics.get("change_pct")
                turnover = market_metrics.get("turnover_usd")
                natr = market_metrics.get("natr")
                btc_corr = market_metrics.get("btc_corr")
                trades = market_metrics.get("trades")
                price = market_metrics.get("price")
                volume_spike = market_metrics.get("volume_spike")

                oi_change_pct = None
                try:
                    oi_period = _alert_oi_period(interval if interval != "1d" else "1D")
                    oi_points = _alert_fetch_open_interest_history(symbol, oi_period, 2)
                    if len(oi_points) >= 2:
                        previous_oi = float(oi_points[-2].get("sumOpenInterestValue", 0) or 0)
                        current_oi = float(oi_points[-1].get("sumOpenInterestValue", 0) or 0)
                        if previous_oi > 0:
                            oi_change_pct = (current_oi - previous_oi) / previous_oi * 100.0
                except Exception:
                    oi_change_pct = None

                expansion = None
                expansion_direction = "any"
                # Volume expansion is genuinely history-dependent and has no
                # ready equivalent in the shared Market State. Keep that one
                # calculation on the dynamic path.
                klines = fetch_klines(symbol, interval=interval, limit=min(1500, max(32, volume_base + volume_growth + 1)))
                volume_rows = [[c.get("open_time", 0), 0, 0, 0, c.get("close", 0), c.get("volume", 0)] for c in klines]
                expansion_latest = _volume_expansion_latest_from_rows(volume_rows, volume_base, volume_growth)
                if expansion_latest:
                    expansion = expansion_latest.get("ratio")
                    expansion_direction = expansion_latest.get("direction", "any")

                result["metrics"][interval] = {
                    "change_pct": round(float(change), 6) if change is not None else None,
                    "turnover_usd": round(float(turnover), 2) if turnover is not None else None,
                    "natr": round(float(natr), 6) if natr is not None else None,
                    "trades": int(trades) if trades is not None else None,
                    "btc_corr": round(float(btc_corr), 6) if btc_corr is not None else None,
                    "price": round(float(price), 12) if price is not None else None,
                    "volume_spike": round(float(volume_spike), 6) if volume_spike is not None else None,
                    "volume_expansion_x": round(float(expansion), 6) if expansion is not None else None,
                    "volume_expansion_direction": expansion_direction,
                    "oi_change_pct": round(float(oi_change_pct), 6) if oi_change_pct is not None else None,
                    "spread_pct": market_metrics.get("spread_pct"),
                    "funding_pct": market_metrics.get("funding_pct"),
                    "oi_change_usd": market_metrics.get("oi_change_usd"),
                    "delta_volume_usd": market_metrics.get("delta_volume_usd"),
                }
            except Exception as exc:
                result["metrics"][interval] = {
                    "change_pct": None, "turnover_usd": None, "natr": None,
                    "trades": None, "btc_corr": None, "price": None,
                    "volume_spike": None, "volume_expansion_x": None,
                    "volume_expansion_direction": "any", "oi_change_pct": None,
                    "spread_pct": None, "funding_pct": None, "oi_change_usd": None,
                    "delta_volume_usd": None,
                    "error": str(exc)
                }
        return result

    results = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(one, symbol) for symbol in symbols]
        for future in as_completed(futures):
            results.append(future.result())
    order = {sym:i for i, sym in enumerate(symbols)}
    results.sort(key=lambda x: order.get(x.get("symbol"), 999))
    return jsonify({"timeframes": timeframes, "results": results})


@app.get("/api/pattern_analysis")
def api_pattern_analysis():
    symbols_raw = (request.args.get("symbols") or "").upper().strip()
    interval = (request.args.get("interval") or "1h").lower().strip()
    if interval not in PATTERN_INTERVALS:
        return jsonify({"error": "Unsupported timeframe"}), 400
    symbols = [s.strip() for s in symbols_raw.split(",") if s.strip() and s.strip().isalnum()][:100]
    if not symbols:
        return jsonify({"results": []})

    def qnum(name, default=None):
        raw = request.args.get(name)
        if raw in (None, ""):
            return default
        try:
            return float(raw)
        except (TypeError, ValueError):
            return default

    analysis_settings = {
        "horizontalLevelSearchPeriod": qnum("level_distance", 50),
        "horizontalLevelSearchStrict": str(request.args.get("level_strict", "0")).lower() in ("1", "true", "yes", "on"),
        "horizontalLevelTouches": qnum("level_touches", 2),
        "horizontalLevelTouchTolerancePct": qnum("level_tolerance", 0.1),
        "horizontalLevelLifetimeHours": qnum("level_lifetime"),
        "horizontalLevelNoCross": str(request.args.get("level_no_cross", "0")).lower() in ("1", "true", "yes", "on"),
        "horizontalLevelAllowMinorPierce": str(request.args.get("level_minor_pierce", "0")).lower() in ("1", "true", "yes", "on"),
        "trendlineSearchPeriod": qnum("trend_distance", 50),
        "trendlineSearchStrict": str(request.args.get("trend_strict", "0")).lower() in ("1", "true", "yes", "on"),
        "trendlineTouches": qnum("trend_touches", 2),
        "trendlineTouchTolerancePct": qnum("trend_tolerance", 0.1),
        "trendlineLifetimeHours": qnum("trend_lifetime"),
        "trendlineNoCross": str(request.args.get("trend_no_cross", "0")).lower() in ("1", "true", "yes", "on"),
        "trendlineAllowMinorPierce": str(request.args.get("trend_minor_pierce", "0")).lower() in ("1", "true", "yes", "on"),
    }


    def one(symbol):
        try:
            klines = fetch_klines(symbol, interval=interval, limit=750)
            result = _cached_pattern_analysis(symbol, interval, klines, settings=analysis_settings)
            result = dict(result or {})
            result["symbol"] = symbol
            # Expose the exact candle boundary used by the cached pattern
            # calculation. The browser must not infer this from the visible
            # chart timeframe because overlay analysis may use another TF.
            result["candle_open_time"] = int(klines[-1].get("open_time", 0)) if klines else 0
            return result
        except Exception as exc:
            return {"symbol": symbol, "error": str(exc), "pattern": None, "score": 0}

    results = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(one, symbol) for symbol in symbols]
        for future in as_completed(futures):
            results.append(future.result())
    order = {s:i for i,s in enumerate(symbols)}
    results.sort(key=lambda x: order.get(x.get("symbol"), 999))
    return jsonify({"interval": interval, "results": results})


@app.get("/api/orderbooks")
def api_orderbooks():
    return jsonify(_orderbook_public_snapshot())


@app.get("/api/densities")
def api_densities():
    return jsonify(_density_public_snapshot())





@app.get("/api/density/health")
def api_density_health():
    now = time.time()
    by_exchange = {}
    with state_lock:
        books = list(STATE.get("orderbooks", {}).items())

    for _, book in books:
        exchange = str(book.get("exchange", ""))
        updated = float(book.get("updated_at", 0) or 0)
        age = max(0.0, now - updated) if updated else None
        row = by_exchange.setdefault(exchange, {
            "exchange": exchange,
            "symbols": 0,
            "fresh": 0,
            "stale": 0,
            "max_age_sec": 0.0,
        })
        row["symbols"] += 1
        if age is None or age > DENSITY_HEALTH_STALE_SEC:
            row["stale"] += 1
        else:
            row["fresh"] += 1
        if age is not None:
            row["max_age_sec"] = max(row["max_age_sec"], age)

    return jsonify({
        "ok": True,
        "exchanges": list(by_exchange.values()),
        "stale_after_sec": DENSITY_HEALTH_STALE_SEC,
    })


@app.get("/api/density/settings")
def api_density_settings_get():
    result = dict(DENSITY_SETTINGS)
    result["runtime_enabled"] = bool(DENSITY_RUNTIME_ENABLED)
    return jsonify(result)


@app.post("/api/density/settings")
def api_density_settings_set():
    payload = request.get_json(silent=True) or {}
    changed = {}

    for key in ("enabled", "show_on_chart", "show_in_screener", "show_consumed", "show_remaining", "show_lifetime", "show_spot", "show_futures", "interest_filter_enabled", "filter_all_symbols"):
        if key in payload:
            changed[key] = bool(payload[key])

    for key in ("min_usd", "min_distance_percent", "max_distance_percent", "strength_min", "interest_min_volume_usd"):
        if key in payload:
            try:
                changed[key] = max(0.0, float(payload[key]))
            except (TypeError, ValueError):
                pass

    if "min_distance_percent" in changed and "max_distance_percent" in changed:
        changed["max_distance_percent"] = max(
            changed["max_distance_percent"],
            changed["min_distance_percent"],
        )

    if "approach" in payload:
        approach = str(payload.get("approach") or "").strip()
        if approach in {"adaptive_cluster", "exact_level", "fixed_cluster"}:
            changed["approach"] = approach

    if "exchanges" in payload:
        raw_exchanges = payload["exchanges"] if isinstance(payload["exchanges"], list) else str(payload["exchanges"]).split(",")
        allowed_exchanges = {"binance", "bybit", "okx"}
        changed["exchanges"] = sorted({str(x).strip().lower() for x in raw_exchanges if str(x).strip().lower() in allowed_exchanges})

    if "blacklist" in payload:
        raw = payload["blacklist"]
        if isinstance(raw, list):
            values = raw
        else:
            values = str(raw).split(",")
        changed["blacklist"] = sorted({
            str(x).strip().upper() for x in values if str(x).strip()
        })

    runtime_requested = payload.get("runtime_enabled") if "runtime_enabled" in payload else None
    DENSITY_SETTINGS.update(changed)
    if runtime_requested is True:
        start_density_pipeline()
    elif runtime_requested is False:
        stop_density_pipeline()
    elif changed.get("enabled") is False:
        stop_density_pipeline()
    return jsonify({"ok": True, "settings": dict(DENSITY_SETTINGS), "runtime_enabled": bool(DENSITY_RUNTIME_ENABLED)})


@app.get("/api/densities/screener")
def api_densities_screener():
    densities = list(_density_public_snapshot().get("densities", []))
    blacklist = {str(x).upper() for x in DENSITY_SETTINGS.get("blacklist", [])}
    if blacklist:
        densities = [
            d for d in densities
            if str(d.get("symbol", "")).upper() not in blacklist
        ]

    by_symbol = {}
    for d in densities:
        symbol = str(d.get("symbol", "")).upper()
        if not symbol:
            continue
        row = by_symbol.setdefault(symbol, {
            "symbol": symbol,
            "density_count": 0,
            "buy_count": 0,
            "sell_count": 0,
            "nearest_distance_percent": None,
            "nearest_price": None,
            "max_usd": 0.0,
            "total_usd": 0.0,
            "max_consumed_percent": 0.0,
        })

        row["density_count"] += 1
        side = str(d.get("side", "")).upper()
        if side == "BUY":
            row["buy_count"] += 1
        elif side == "SELL":
            row["sell_count"] += 1

        distance = abs(float(d.get("distance_percent", 0) or 0))
        if row["nearest_distance_percent"] is None or distance < row["nearest_distance_percent"]:
            row["nearest_distance_percent"] = distance
            row["nearest_price"] = d.get("price")

        usd = float(d.get("current_usd", 0) or 0)
        row["max_usd"] = max(row["max_usd"], usd)
        row["total_usd"] += usd
        row["max_consumed_percent"] = max(
            row["max_consumed_percent"],
            float(d.get("consumed_percent", 0) or 0),
        )

    rows = sorted(
        by_symbol.values(),
        key=lambda r: (
            r["nearest_distance_percent"] if r["nearest_distance_percent"] is not None else 999999,
            -r["max_usd"],
        ),
    )

    return jsonify({
        "enabled": bool(
            DENSITY_SETTINGS.get("enabled", True)
            and DENSITY_SETTINGS.get("show_in_screener", True)
        ),
        "rows": rows,
    })


@app.get("/api/densities/presentation")
def api_densities_presentation():
    snapshot = _density_public_snapshot()
    densities = list(snapshot.get("densities", []))
    blacklist = {str(x).upper() for x in DENSITY_SETTINGS.get("blacklist", [])}
    strength_min = float(DENSITY_SETTINGS.get("strength_min", 0) or 0)
    if blacklist:
        densities = [d for d in densities if str(d.get("symbol", "")).upper() not in blacklist]
    if strength_min > 0:
        densities = [d for d in densities if float(d.get("strength_score", 0) or 0) >= strength_min]
    densities.sort(key=lambda d: (
        str(d.get("symbol", "")),
        abs(float(d.get("distance_percent", 0) or 0)),
        -float(d.get("current_usd", 0) or 0),
    ))
    return jsonify({
        "enabled": bool(snapshot.get("enabled")),
        "last_scan": snapshot.get("last_scan"),
        "map": densities[:DENSITY_MAP_MAX_ITEMS],
        "chart": densities[:DENSITY_CHART_MAX_ITEMS],
        "aggregated": snapshot.get("aggregated", [])[:DENSITY_AGGREGATION_MAX_ITEMS],
    })


@app.post("/api/orderbook/active")
def api_orderbook_active():
    payload=request.get_json(silent=True) or {}
    symbol=str(payload.get("symbol") or "").upper()
    if not symbol.endswith("USDT"): return jsonify({"ok":False,"error":"Invalid symbol"}),400
    _orderbook_set_active_symbol(symbol)
    return jsonify({"ok":True,"symbol":symbol,"symbols":_orderbook_symbol_candidates()})


@app.get("/health")
def screener_health():
    with state_lock:
        last_update = STATE.get("last_update")
        last_snapshot = STATE.get("screener_last_snapshot")
        coins = len(STATE.get("coins", {}))
    return jsonify({
        "ok": True,
        "service": "binance-crypto-screener",
        "role": "worker" if SCREENER_WORKER_MODE else "local",
        "status": STATE.get("status", "STARTING"),
        "symbols": coins,
        "last_update": last_update,
        "last_snapshot": last_snapshot,
        "timestamp": utc_now(),
    })


@app.get("/api/screener/status")
def api_screener_status():
    with state_lock:
        return jsonify({
            "engine": "worker" if SCREENER_WORKER_MODE else "local",
            "status": STATE.get("status", "STARTING"),
            "symbols": len(STATE.get("coins", {})),
            "buffer_symbols": len(SCREENER_BUFFER),
            "buffer_max_snapshots": SCREENER_BUFFER_MAXLEN,
            "buffer_hours": SCREENER_BUFFER_HOURS,
            "last_snapshot": STATE.get("screener_last_snapshot"),
            "historical_cache_size": len(SCREENER_HISTORY_CACHE),
            "timestamp": utc_now(),
        })


def _screener_server_filter_coins(coins, payload):
    """Apply only cheap/current/rolling filters server-side.

    The browser still owns presentation/sorting. Render/Local Engine owns the
    market-data metric calculation and can therefore do the expensive rolling
    reference work once per request instead of once per browser filter.
    """
    filters = payload.get("filters") if isinstance(payload, dict) else None
    if not isinstance(filters, dict):
        return coins
    result = []
    for coin in coins:
        ok = True
        for key, cfg in filters.items():
            if not isinstance(cfg, dict):
                continue
            min_value = cfg.get("min")
            max_value = cfg.get("max")
            try:
                min_value = float(min_value) if min_value not in (None, "") else None
            except (TypeError, ValueError):
                min_value = None
            try:
                max_value = float(max_value) if max_value not in (None, "") else None
            except (TypeError, ValueError):
                max_value = None
            if min_value is None and max_value is None:
                continue
            timeframe = str(cfg.get("timeframe") or "1m")
            metrics = rolling_market_metrics(coin.get("symbol"), timeframe) if key in {
                "change_pct", "volume_24h", "trades", "price", "oi_change_usd", "oi_change_pct", "delta_volume_usd"
            } else {}
            value = metrics.get(key)
            if key == "volume_24h": value = metrics.get("turnover_usd")
            if key == "oi_change_usd": value = metrics.get("oi_change_usd")
            if key == "change_pct": value = metrics.get("change_pct")
            if value is None:
                value = coin.get(key)
            try: value = float(value)
            except (TypeError, ValueError):
                ok = False; break
            if key == "change_pct" and min_value is not None and min_value >= 0 and (max_value is None or max_value >= 0):
                value = abs(value)
            if min_value is not None and value < min_value: ok = False; break
            if max_value is not None and value > max_value: ok = False; break
        if ok: result.append(coin)
    return result


@app.route("/api/screener", methods=["GET", "POST"])
def api_screener_snapshot():
    if request.method == "GET":
        return api_state()
    payload = request.get_json(silent=True) or {}
    snapshot = build_screener_snapshot()
    filtered = _screener_server_filter_coins(snapshot, payload)
    return jsonify({
        "status": STATE.get("status", "STARTING"),
        "timestamp": utc_now(),
        "engine": "worker" if SCREENER_WORKER_MODE else "local",
        "coins": filtered,
        "count": len(filtered),
        "buffer_hours": SCREENER_BUFFER_HOURS,
        "historical_cache_size": len(SCREENER_HISTORY_CACHE),
    })


@app.get("/api/state")
def api_state():
    with state_lock:
        status = STATE["status"]
        last_error = STATE["last_error"]
        last_update = STATE["last_update"]
        connection_count = STATE["connection_count"]
        reconnect_attempts = STATE["reconnect_attempts"]
        last_message = STATE["last_message"]
        levels = list(STATE["levels"].values())
        signal_items = list(STATE["signals"])[:50]

    return jsonify({
        "status": status,
        "last_error": last_error,
        "last_update": last_update,
        "connection_count": connection_count,
        "reconnect_attempts": reconnect_attempts,
        "last_message": last_message,
        "coins": build_screener_snapshot(),
        "levels": levels,
        "signals": signal_items,
        "screener_engine": "worker" if SCREENER_WORKER_MODE else "local",
        "buffer_hours": SCREENER_BUFFER_HOURS,
        "buffer_last_snapshot": STATE.get("screener_last_snapshot"),
        "historical_cache_size": len(SCREENER_HISTORY_CACHE),
    })


# ============================================================
# HTML / CSS / JAVASCRIPT
# ============================================================

HTML_PAGE = r"""
<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Crypto Screener — Stage 1</title>

<style>
    :root {
        --bg: #0d1117;
        --panel: #151b23;
        --panel2: #11161d;
        --border: #28313d;
        --text: #e6edf3;
        --muted: #8b949e;
        --green: #3fb950;
        --red: #f85149;
        --yellow: #d29922;
        --blue: #58a6ff;
    }

    * {
        box-sizing: border-box;
    }

    body {
        margin: 0;
        background: var(--bg);
        color: var(--text);
        font-family: Arial, Helvetica, sans-serif;
        font-size: 13px;
    }

    .topbar {
        height: 56px;
        display: flex;
        align-items: center;
        gap: 18px;
        padding: 0 18px;
        border-bottom: 1px solid var(--border);
        background: var(--panel);
    }

    .title {
        font-size: 18px;
        font-weight: 700;
        white-space: nowrap;
    }

    .stage {
        color: var(--muted);
        font-size: 12px;
    }

    .status {
        margin-left: 0;
        display: flex;
        align-items: center;
        gap: 7px;
        color: var(--muted);
    }

    .dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: var(--yellow);
    }

    .dot.live {
        background: var(--green);
    }

    .dot.error {
        background: var(--red);
    }

    .top-actions {
        margin-left: 0;
        display: flex;
        align-items: center;
        gap: 6px;
    }

    .top-action {
        width: 34px;
        height: 34px;
        padding: 0;
        border: 1px solid var(--border);
        border-radius: 6px;
        background: var(--panel);
        color: var(--text);
        display: inline-flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
        font-size: 17px;
        line-height: 1;
        box-sizing: border-box;
    }

    .settings-button {
        font-size: 17px;
    }


    .top-action:hover {
        background: #1a212b;
        border-color: var(--blue);
    }

    .top-search {
        position: fixed;
        inset: 0;
        z-index: 200;
        display: none;
        align-items: center;
        justify-content: center;
        background: rgba(0, 0, 0, 0.72);
        padding: 20px;
    }

    .top-search.open {
        display: flex;
    }

    .settings-overlay {
        position: fixed;
        inset: 0;
        z-index: 300;
        display: none;
        align-items: center;
        justify-content: center;
        background: rgba(0, 0, 0, 0.72);
        padding: 20px;
        box-sizing: border-box;
    }

    .settings-overlay.open {
        display: flex;
    }

    .settings-modal {
        width: min(720px, 94vw);
        max-height: min(760px, 90vh);
        overflow: auto;
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 7px;
        box-shadow: 0 16px 50px rgba(0,0,0,.45);
    }

    .settings-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 12px 14px;
        border-bottom: 1px solid var(--border);
        font-weight: 600;
    }

    .settings-close {
        width: 30px;
        height: 30px;
        padding: 0;
        border: 1px solid var(--border);
        border-radius: 5px;
        background: transparent;
        color: var(--muted);
        cursor: pointer;
        font-size: 18px;
    }

    .settings-close:hover {
        color: var(--text);
        background: #1a212b;
    }

    .settings-section {
        padding: 14px;
    }

    .settings-section-title {
        margin: 0;
        padding: 10px 12px;
        color: var(--text);
        font-size: 13px;
        font-weight: 600;
        cursor: pointer;
        border: 1px solid var(--border);
        border-radius: 6px;
        background: var(--panel2);
        user-select: none;
    }

    .settings-section-title::after {
        content: "▸";
        float: right;
        color: var(--muted);
    }

    .settings-section.open .settings-section-title::after {
        content: "▾";
    }

    .settings-section-content {
        display: none;
        padding-top: 10px;
    }

    .settings-section.open .settings-section-content {
        display: block;
    }

    .telegram-connections { display:flex; flex-direction:column; gap:10px; }
    .telegram-connection-row, .notification-route-row { border:1px solid var(--border); border-radius:6px; background:var(--panel2); padding:10px; }
    .telegram-connection-head, .notification-route-head { display:flex; align-items:center; justify-content:space-between; gap:10px; }
    .telegram-connection-name, .notification-route-name { font-size:12px; font-weight:600; color:var(--text); }
    .telegram-connection-meta, .notification-route-meta { color:var(--muted); font-size:10px; margin-top:3px; }
    .telegram-connection-actions, .notification-route-controls { display:flex; align-items:center; gap:7px; flex-wrap:wrap; margin-top:8px; }
    .telegram-input { width:100%; height:32px; border:1px solid var(--border); border-radius:5px; background:#11171e; color:var(--text); padding:0 9px; }
    .telegram-add-grid { display:grid; grid-template-columns:1fr 1.5fr 1fr; gap:8px; align-items:end; }
    .telegram-add-grid label { display:flex; flex-direction:column; gap:5px; color:var(--muted); font-size:11px; }
    .telegram-small-button { height:30px; padding:0 10px; border:1px solid var(--border); border-radius:5px; background:#11171e; color:var(--text); cursor:pointer; font:600 11px/1 inherit; }
    .telegram-small-button:hover { border-color:var(--blue); background:#1a212b; }
    .notification-routes { display:flex; flex-direction:column; gap:8px; }
    .notification-route-controls select { height:30px; min-width:170px; border:1px solid var(--border); border-radius:5px; background:#11171e; color:var(--text); padding:0 8px; }
    .notification-route-toggle { display:flex; align-items:center; gap:7px; color:var(--text); font-size:11px; }
    .notification-route-empty { color:var(--muted); font-size:11px; padding:8px 2px; }
    @media (max-width:700px) { .telegram-add-grid { grid-template-columns:1fr; } }
    .terminal-connection { display:flex; flex-direction:column; gap:10px; }
    .terminal-connection-row { display:grid; grid-template-columns:110px minmax(0,1fr) 110px; gap:8px; align-items:end; }
    .terminal-connection-field { display:flex; flex-direction:column; gap:5px; }
    .terminal-connection-field label { color:var(--muted); font-size:11px; }
    .terminal-connection-field input, .terminal-connection-field select { width:100%; height:32px; border:1px solid var(--border); border-radius:5px; background:#11171e; color:var(--text); padding:0 9px; }
    .terminal-connection-actions { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
    .terminal-connection-button { height:32px; padding:0 12px; border:1px solid var(--border); border-radius:5px; background:#11171e; color:var(--text); cursor:pointer; font:600 11px/1 inherit; }
    .terminal-connection-button:hover { border-color:var(--blue); background:#1a212b; }
    .terminal-connection-button.primary { border-color:#3a6ea5; }
    .terminal-connection-status { display:inline-flex; align-items:center; gap:6px; color:var(--muted); font-size:11px; }
    .terminal-connection-status-dot { width:8px; height:8px; border-radius:50%; background:var(--yellow); flex:0 0 auto; }
    .terminal-connection-status-dot.connected { background:var(--green); }
    .terminal-connection-status-dot.error { background:var(--red); }
    .terminal-connection-hint { color:var(--muted); font-size:10px; line-height:1.45; }
    @media (max-width:700px) { .terminal-connection-row { grid-template-columns:1fr; } }

    .hotkeys-table {
        width: 100%;
        border-collapse: collapse;
    }

    .hotkeys-table th,
    .hotkeys-table td {
        padding: 9px 10px;
        border-bottom: 1px solid var(--border);
        text-align: left;
        font-size: 12px;
    }

    .hotkeys-table th {
        color: var(--muted);
        font-weight: 500;
    }

    .hotkey-action {
        min-width: 58px;
        padding: 5px 9px;
        border: 1px solid var(--border);
        border-radius: 4px;
        background: var(--panel2);
        color: var(--text);
        font: inherit;
        font-weight: 600;
        cursor: pointer;
    }

    .hotkey-action:hover,
    .hotkey-action.listening {
        border-color: var(--blue);
        background: #1a212b;
    }

    .hotkey-description {
        color: var(--muted);
    }


    .notification-sounds-list { display:flex; flex-direction:column; gap:8px; }
    .notification-sound-row { border:1px solid var(--border); border-radius:6px; background:var(--panel2); padding:10px; }
    .notification-sound-row-head { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:8px; }
    .notification-sound-name { font-size:12px; font-weight:600; color:var(--text); }
    .notification-sound-meta { color:var(--muted); font-size:10px; margin-top:2px; }
    .notification-sound-controls { display:grid; grid-template-columns:minmax(0,1fr) auto auto; gap:8px; align-items:center; }
    .notification-sound-select { width:100%; min-width:0; height:30px; border:1px solid var(--border); border-radius:4px; background:#11171e; color:var(--text); padding:0 8px; }
    .notification-sound-button { height:30px; padding:0 10px; border:1px solid var(--border); border-radius:4px; background:#11171e; color:var(--text); cursor:pointer; font:600 11px/1 inherit; }
    .notification-sound-button:hover { border-color:var(--blue); background:#1a212b; }
    .notification-volume { display:flex; align-items:center; gap:6px; min-width:150px; }
    .notification-volume input { width:90px; }
    .notification-volume strong { min-width:38px; text-align:right; }
    .notification-sound-upload { display:flex; gap:8px; align-items:center; margin-top:9px; flex-wrap:wrap; }
    .notification-sound-hint { margin-top:9px; color:var(--muted); font-size:10px; line-height:1.45; }
    @media (max-width:700px) { .notification-sound-controls { grid-template-columns:1fr; } .notification-volume { min-width:0; } }

    .settings-hint {
        margin-top: 10px;
        color: var(--muted);
        font-size: 11px;
        line-height: 1.5;
    }

    .search-modal {
        width: min(560px, 90vw);
    }

    .search-wrap {
        position: relative;
        width: 100%;
    }

    .search {
        width: 100%;
        height: 48px;
        box-sizing: border-box;
        padding: 0 16px;
        border: 1px solid var(--border);
        border-radius: 7px;
        outline: none;
        background: var(--panel);
        color: var(--text);
        font-size: 16px;
    }

    .search:focus {
        border-color: var(--blue);
    }

    .suggestions {
        position: absolute;
        top: 54px;
        left: 0;
        width: 100%;
        max-height: 360px;
        overflow-y: auto;
        z-index: 20;
        box-sizing: border-box;
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 7px;
        box-shadow: 0 10px 30px rgba(0,0,0,.45);
        display: none;
    }

    .toolbar {
        padding: 10px 18px;
        border-bottom: 1px solid var(--border);
        background: var(--panel2);
        display: flex;
        align-items: center;
        gap: 10px;
    }

    /* app200: align template search status under the first letter of the active template name. */
    .market-template-dock {
        margin-left: auto;
        position: relative;
        min-width: 0;
        width: 330px;
    }
    .market-template-dock .market-template-header {
        width: 100%;
        height: 40px;
        box-sizing: border-box;
        border-bottom: 0;
    }
    .market-template-dock .market-template-menu {
        right: 0;
        top: 42px;
    }
    .market-template-dock .market-template-search-wrap {
        margin-left: 10px;
        width: calc(100% - 10px);
    }
    .market-template-dock .template-search-status {
        position: absolute;
        left: 27px;
        right: 0;
        top: 42px;
        min-height: 18px;
        padding: 6px 2px 2px;
        background: var(--panel2);
        z-index: 40;
    }

    .search {
        width: 260px;
        height: 34px;
        padding: 0 11px;
        border: 1px solid var(--border);
        border-radius: 5px;
        outline: none;
        background: var(--panel);
        color: var(--text);
    }

    .search:focus {
        border-color: var(--blue);
    }

    .search-wrap {
        position: relative;
    }

    .suggestions {
        position: absolute;
        top: 38px;
        left: 0;
        width: 300px;
        max-height: 320px;
        overflow-y: auto;
        z-index: 20;
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 6px;
        box-shadow: 0 10px 30px rgba(0,0,0,.35);
        display: none;
    }

    .suggestion {
        padding: 9px 11px;
        cursor: pointer;
        display: flex;
        justify-content: space-between;
        gap: 10px;
    }

    .suggestion:hover, .suggestion.active {
        background: #1a212b;
    }

    .suggestion-price {
        color: var(--muted);
    }

    .clickable-row {
        cursor: pointer;
    }

    /* app271: selected market row + tactile ticker click feedback */
    .selected-market-row td {
        background: rgba(37, 99, 235, 0.24) !important;
    }
    .selected-market-row:hover td {
        background: rgba(37, 99, 235, 0.34) !important;
    }
    .ticker-click-feedback {
        animation: tickerClickFeedback 220ms ease-out;
    }
    @keyframes tickerClickFeedback {
        0% { transform: scale(1); filter: brightness(1); }
        45% { transform: scale(0.94); filter: brightness(1.45); }
        100% { transform: scale(1); filter: brightness(1); }
    }

    /* app28: chart workspace grid — added without replacing the legacy chart */
    .chart-grid-workspace {
        position:absolute; left:0; right:0; top:46px; bottom:34px;
        min-width:0; min-height:0; display:none; gap:6px;
        background:#0b0f14; overflow:hidden; z-index:5;
    }
    .chart-grid-slot {
        position: relative;
        min-width: 0;
        min-height: 0;
        display: flex;
        flex-direction: column;
        background: #0b0f14;
        border: 1px solid #202a35;
        border-radius: 4px;
        overflow: hidden;
    }
    .chart-grid-slot.active-slot { border-color: #44566b; }
    .chart-grid-slot-head {
        height: 30px;
        flex: 0 0 30px;
        display: flex;
        align-items: center;
        gap: 6px;
        padding: 0 6px;
        border-bottom: 1px solid #202a35;
        background: #11171e;
        color: var(--text);
        font-size: 11px;
        font-weight: 700;
        user-select: none;
    }
    .chart-grid-slot.maximized .chart-grid-slot-head { display:none; }
    .chart-grid-flag-wrap { position:relative; flex:0 0 24px; margin-left:auto; }
    .chart-grid-flag {
        width:24px; height:24px; padding:0; border:0; border-radius:4px;
        background:transparent; color:#59636f; cursor:pointer; position:relative;
        display:inline-flex; align-items:center; justify-content:center; appearance:none;
    }
    .chart-grid-flag:hover { background:#1a212b; }
    .chart-grid-flag svg { display:block; width:18px; height:18px; overflow:visible; }
    .chart-grid-flag .flag-pole { fill:none; stroke:#59636f; stroke-width:1.5; stroke-linecap:round; }
    .chart-grid-flag .flag-cloth { fill:none; stroke:#59636f; stroke-width:1.5; stroke-linejoin:round; }
    .chart-grid-flag.has-color .flag-pole { stroke:var(--chart-grid-flag-color); }
    .chart-grid-flag.has-color .flag-cloth { fill:var(--chart-grid-flag-color); stroke:var(--chart-grid-flag-color); }
    .chart-grid-flag-palette {
        position:absolute; right:0; top:28px; z-index:80;
        display:flex; align-items:center; gap:6px; padding:5px 7px;
        border:1px solid var(--border); border-radius:6px; background:#171c23;
        box-shadow:0 8px 24px rgba(0,0,0,.45); white-space:nowrap;
    }
    .chart-grid-flag-palette[hidden] { display:none !important; }
    .chart-grid-flag-palette .watchlist-color-option,
    .chart-grid-flag-palette .watchlist-color-clear { flex:0 0 14px; width:14px; height:14px; }
    .chart-grid-title-dot {
        width: 8px;
        height: 8px;
        flex: 0 0 8px;
        border-radius: 50%;
        background: #6b7280;
        border: 1px solid rgba(255,255,255,.25);
        box-sizing: border-box;
    }
    .chart-grid-title { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .chart-grid-intervals { margin-left:4px; display:flex; align-items:center; gap:2px; min-width:0; }
    .chart-grid-intervals button {
        border: 1px solid transparent;
        background: transparent;
        color: var(--muted);
        border-radius: 3px;
        padding: 2px 3px;
        font: 600 9px/1 inherit;
        cursor: pointer;
    }
    .chart-grid-intervals button:hover, .chart-grid-intervals button.active { color: var(--text); background:#1a212b; border-color:var(--border); }
    .chart-grid-card-body { flex: 1 1 auto; min-width:0; min-height:0; position:relative; overflow:hidden; }
    .chart-grid-price, .chart-grid-oi { min-width:0; min-height:0; position:absolute; inset:0; }
    .chart-grid-price { border-bottom:0; }
    .chart-grid-oi { display:none !important; }
    /* Цвет графика теперь находится в левой панели инструментов, как на референсе. */
    .chart-grid-colorbar, .chart-single-colorbar { display:none !important; }
    .chart-grid-color-palette {
        display:flex; flex-direction:column; align-items:center; gap:4px;
        width:28px; margin:2px 0 1px; padding:3px 0;
        border-top:1px solid var(--border); border-bottom:1px solid var(--border);
    }
    .chart-grid-color-dot {
        width:14px; height:14px; padding:0; flex:0 0 14px; border-radius:50%;
        border:1px solid rgba(255,255,255,.30); cursor:pointer; box-sizing:border-box;
    }
    .chart-grid-color-dot:hover, .chart-grid-color-dot.selected { transform:scale(1.08); border-color:#fff; box-shadow:0 0 0 2px rgba(255,255,255,.18); }
    .chart-grid-expand { position:absolute; right:4px; bottom:4px; z-index:18; width:26px; height:24px; border:1px solid var(--border); border-radius:4px; background:rgba(17,22,29,.88); color:var(--muted); cursor:pointer; font-size:17px; line-height:22px; padding:0; }
    .chart-grid-expand:hover { color:var(--text); }
    .chart-grid-expand.is-maximized { transform:rotate(180deg); color:var(--text); }
    .chart-grid-slot.maximized { position:absolute; inset:0; z-index:50; border-color:#59697b; border-radius:0; }
    .chart-grid-workspace.has-maximized { position:absolute; }
    .chart-grid-workspace.has-maximized > .chart-grid-slot:not(.maximized) { display:none; }
    .chart-grid-pagination {
        position:absolute; left:0; right:0; bottom:0; height:34px; z-index:20;
        display:none; align-items:center; justify-content:center; gap:8px;
        border-top:1px solid #202a35; background:#0b0f14;
        color:var(--muted); font-size:11px; user-select:none;
    }
    .chart-grid-pagination.visible { display:flex; }
    .chart-grid-pagination button {
        width:28px; height:24px; padding:0; border:1px solid var(--border);
        border-radius:4px; background:#11171e; color:var(--text); cursor:pointer;
        font:600 14px/22px inherit;
    }
    .chart-grid-pagination button:hover:not(:disabled) { background:#1a212b; }
    .chart-grid-pagination button:disabled { opacity:.35; cursor:default; }
    .chart-grid-pagination-info { min-width:82px; text-align:center; }
    .chart-content { position:relative; flex:1; min-width:0; min-height:0; overflow:hidden; }
    .chart-content.grid-active .own-chart, .chart-content.grid-active .chart-single-colorbar { display:none !important; }

    .grid-layout-settings { padding: 2px 0 8px; }
    .grid-layout-hint { color:var(--muted); font-size:11px; margin-bottom:8px; }
    .grid-layout-picker { display:grid; grid-template-columns:repeat(7, 28px); grid-template-rows:repeat(7, 24px); gap:2px; width:max-content; padding:6px; background:#11171e; border:1px solid var(--border); border-radius:5px; }
    .grid-layout-cell { border:0; padding:0; background:#171d25; cursor:pointer; border-radius:2px; }
    .grid-layout-cell:hover, .grid-layout-cell.preview, .grid-layout-cell.selected { background:#59636f; }
    .grid-layout-cell.preview-after { background:#343d49; }
    .grid-layout-value { margin-top:7px; color:var(--text); font-size:11px; min-height:16px; }

    .chart-modal {
        position: fixed;
        inset: 0;
        z-index: 100;
        display: none;
        align-items: center;
        justify-content: center;
        background: rgba(0,0,0,.72);
        padding: 24px;
    }

    .chart-modal.open {
        display: flex;
    }

    .chart-box {
        position: relative;
        width: min(1200px, 96vw);
        height: min(760px, 88vh);
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 8px;
        overflow: hidden;
        display: flex;
        flex-direction: column;
    }

    .chart-header {
        position: relative;
        min-height: 46px;
        padding: 0 12px;
        display: flex;
        align-items: center;
        gap: 10px;
        border-bottom: 1px solid var(--border);
        font-weight: 700;
    }

    .chart-intervals {
        position: absolute;
        left: 50%;
        top: 50%;
        transform: translate(-50%, -50%);
        display: flex;
        align-items: center;
        gap: 5px;
        margin: 0;
    }

    .chart-interval {
        border: 1px solid var(--border);
        border-radius: 5px;
        background: var(--panel2);
        color: var(--muted);
        padding: 5px 8px;
        font-size: 12px;
        cursor: pointer;
    }

    .chart-interval:hover, .chart-interval.active {
        color: var(--text);
        background: #1a212b;
    }

    .chart-modal #chartTitleDot,
    .chart-modal #chartTitle,
    .chart-modal #chartFlagWrap { display:none; }
    .chart-modal.large-chart-mode #chartTitleDot,
    .chart-modal.large-chart-mode #chartTitle,
    .chart-modal.large-chart-mode #chartFlagWrap { display:inline-flex; align-items:center; }
    .chart-modal.large-chart-mode #chartTitle { white-space:nowrap; }
    .chart-flag-wrap { position:relative; flex:0 0 30px; margin-left:-4px; }

    .chart-header-actions {
        margin-left:auto; display:flex; align-items:center; gap:6px;
        position:relative; z-index:2;
    }
    .chart-back {
        display:none;
        height:30px; padding:0 10px; border:1px solid var(--border); border-radius:5px;
        background:var(--panel2); color:var(--text); cursor:pointer; font-size:12px; font-weight:700;
    }
    .chart-modal.large-chart-mode .chart-back {
        display:inline-flex; align-items:center; justify-content:center;
    }
    .chart-back:hover { background:#1a212b; }
    /* Флажок графика: ровно вертикальный силуэт — стойка сверху вниз,
       полотнище вправо и V-вырез снизу. Рисуется SVG, чтобы контур не ломался. */
    .chart-flag {
        width:30px; height:30px; padding:0; margin:0; flex:0 0 30px;
        border:0; border-radius:5px; background:transparent; color:#59636f;
        cursor:pointer; position:relative; display:inline-flex; align-items:center;
        justify-content:center; appearance:none;
    }
    .chart-flag:hover { background:#1a212b; }
    .chart-flag svg { display:block; width:22px; height:22px; overflow:visible; }
    .chart-flag .flag-pole { fill:none; stroke:#59636f; stroke-width:1.6; stroke-linecap:round; }
    .chart-flag .flag-cloth { fill:none; stroke:#59636f; stroke-width:1.6; stroke-linejoin:round; }
    .chart-flag.has-color .flag-pole { stroke:var(--chart-flag-color); }
    .chart-flag.has-color .flag-cloth { fill:var(--chart-flag-color); stroke:var(--chart-flag-color); }
    .chart-flag-palette {
        position:absolute; right:calc(100% + 6px); top:50%; transform:translateY(-50%); z-index:120;
        display:flex; align-items:center; gap:6px; padding:5px 7px; border:1px solid var(--border);
        border-radius:6px; background:#171c23; box-shadow:0 8px 24px rgba(0,0,0,.45); white-space:nowrap;
    }
    .chart-flag-palette[hidden] { display:none !important; }
    .chart-flag-palette .watchlist-color-option,
    .chart-flag-palette .watchlist-color-clear {
        flex:0 0 14px; width:14px; height:14px;
    }

    .chart-close {
        margin-left: auto;
        width: 30px;
        height: 30px;
        border: 1px solid var(--border);
        border-radius: 5px;
        background: var(--panel2);
        color: var(--text);
        cursor: pointer;
        font-size: 18px;
        position: relative;
        z-index: 2;
    }

    .chart-content {
        position: relative;
        flex: 1 1 auto;
        width: 100%;
        min-width: 0;
        min-height: 0;
        display: flex;
        flex-direction: column;
        overflow: hidden;
    }

    .drawing-toolbar {
        position: absolute;
        left: 7px;
        top: 50%;
        transform: translateY(-50%);
        z-index: 30;
        width: 34px;
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 5px;
        padding: 5px 0;
        border: 1px solid var(--border);
        border-radius: 6px;
        background: rgba(17, 22, 29, .94);
        box-shadow: 0 6px 18px rgba(0,0,0,.3);
    }

    .drawing-tool {
        width: 28px;
        height: 28px;
        padding: 0;
        border: 0;
        border-radius: 4px;
        background: transparent;
        color: var(--muted);
        display: inline-flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
    }

    .drawing-tool:hover,
    .drawing-tool.active {
        color: var(--text);
        background: #1a212b;
    }

    .drawing-toolbar-separator {
        width: 20px;
        height: 1px;
        margin: 2px 0;
        background: var(--border);
    }

    .position-glyph {
        font-size: 12px;
        font-weight: 700;
    }

    .drawing-tool svg {
        width: 17px;
        height: 17px;
        fill: none;
        stroke: currentColor;
        stroke-width: 1.7;
        stroke-linecap: round;
        stroke-linejoin: round;
    }

    .chart-hover-tooltip {
        position: absolute;
        display: none;
        z-index: 30;
        pointer-events: none;
        min-width: 190px;
        padding: 7px 9px;
        border: 1px solid #34404d;
        border-radius: 5px;
        background: rgba(17, 22, 29, .96);
        color: var(--text);
        box-shadow: 0 6px 18px rgba(0,0,0,.35);
        font-size: 11px;
        line-height: 1.5;
        white-space: nowrap;
    }

    .chart-hover-tooltip .tooltip-title {
        color: var(--muted);
        margin-bottom: 2px;
    }

    .drawing-canvas {
        position: absolute;
        z-index: 20;
        pointer-events: none;
        display: block;
        touch-action: none;
    }

    .drawing-canvas.active {
        pointer-events: auto;
        cursor: crosshair;
    }

    .drawing-status {
        position: absolute;
        left: 48px;
        top: 10px;
        z-index: 25;
        display: none;
        padding: 4px 7px;
        border: 1px solid var(--border);
        border-radius: 4px;
        background: rgba(17,22,29,.9);
        color: var(--muted);
        font-size: 10px;
        pointer-events: none;
    }

    .drawing-status.open { display: block; }

    .own-chart {
        position: absolute;
        inset: 0;
        width: 100%;
        height: 100%;
        min-width: 0;
        min-height: 0;
        display: block;
        background: #0b0f14;
        overflow: hidden;
    }

    .own-chart-main,
    .own-chart-oi {
        width: 100%;
        min-width: 0;
        min-height: 0;
        position: relative;
    }

    .own-chart-main {
        position: absolute;
        inset: 0;
        border-bottom: 0;
    }

    .chart-loading-indicator {
        position: absolute;
        left: 10px;
        top: 8px;
        z-index: 8;
        display: none;
        padding: 5px 8px;
        border: 1px solid var(--border);
        border-radius: 4px;
        background: rgba(11,15,20,.88);
        color: #aab4bf;
        font-size: 11px;
        pointer-events: none;
    }

    .chart-loading-indicator.open { display: block; }

    /* Dim only the actual price-chart canvas while its history is loading.
       The loading badge stays above the dim layer so the state is obvious. */
    .own-chart-main.chart-is-loading::after {
        content: "";
        position: absolute;
        inset: 0;
        z-index: 7;
        background: rgba(18, 23, 30, .34);
        pointer-events: none;
    }

    /* Centered loading spinner over the actual chart area.  It stays above
       the dim layer, so chart loading is immediately visible even when the
       chart is also showing formation/level overlays. */
    .own-chart-main.chart-is-loading::before {
        content: "";
        position: absolute;
        left: 50%;
        top: 50%;
        width: 28px;
        height: 28px;
        margin-left: -14px;
        margin-top: -14px;
        z-index: 9;
        border: 3px solid rgba(255,255,255,.18);
        border-top-color: #b36cff;
        border-radius: 50%;
        animation: chartLoadingSpin .8s linear infinite;
        pointer-events: none;
        box-sizing: border-box;
    }

    @keyframes chartLoadingSpin {
        to { transform: rotate(360deg); }
    }

    .own-chart-oi {
        display: none !important;
    }

    .chart-indicator-label {
        position: absolute;
        left: 12px;
        top: 8px;
        z-index: 5;
        color: #7f8b98;
        font-size: 11px;
        font-weight: 600;
        pointer-events: none;
        text-transform: uppercase;
        letter-spacing: .04em;
    }

    .volume-label {
        top: auto;
        bottom: 22px;
    }

    .info {
        color: var(--muted);
    }

    .layout {
        display: grid;
        grid-template-columns: minmax(0, 1fr) 330px;
        gap: 10px;
        padding: 10px;
    }

    .panel {
        border: 1px solid var(--border);
        border-radius: 6px;
        overflow: hidden;
        background: var(--panel);
    }

    .panel-header {
        height: 40px;
        display: flex;
        align-items: center;
        padding: 0 12px;
        border-bottom: 1px solid var(--border);
        font-weight: 700;
    }

    .table-wrap {
        overflow: auto;
        max-height: calc(100vh - 128px);
    }

    table {
        width: 100%;
        border-collapse: collapse;
    }

    th {
        position: sticky;
        top: 0;
        z-index: 10;
        background: #1a212b;
        color: var(--muted);
        text-align: left;
        font-weight: 600;
        padding: 9px 10px;
        border-bottom: 1px solid var(--border);
    }

    td {
        padding: 8px 10px;
        border-bottom: 1px solid #202731;
        white-space: nowrap;
    }

    tr:hover td {
        background: #1a212b;
    }

    .symbol {
        font-weight: 700;
    }

    /* app2: Watchlist color groups */
    .market-symbol-cell {
        display: flex;
        align-items: center;
        gap: 7px;
        min-width: 0;
    }

    /* The hit area is the whole compact flag zone; only the small flag
       silhouette is painted inside it. */
    .watchlist-color-trigger {
        width: 30px;
        height: 30px;
        padding: 0;
        margin: 0 -2px 0 -4px;
        flex: 0 0 30px;
        border: 0;
        background: transparent;
        box-sizing: border-box;
        cursor: pointer;
        position: relative;
        z-index: 2;
        outline: none;
        appearance: none;
    }

    /* Unselected flag: outline only, empty inside. The right edge keeps the
       V-shaped cut-out silhouette. */
    .watchlist-color-trigger::after {
        content: "";
        position: absolute;
        left: 8px;
        top: 10px;
        width: 13px;
        height: 9px;
        background: transparent;
        border: 1.5px solid #59636f;
        clip-path: polygon(0 0, 100% 0, 72% 50%, 100% 100%, 0 100%);
        box-sizing: border-box;
        transition: none;
    }

    /* Hover paints the empty flag dark gray, without changing its saved
       state. The whole 30x30 hit area is responsible for this hover. */
    .watchlist-color-trigger:not(.has-color):hover::after {
        background: #59636f;
        border-color: #59636f;
    }

    .watchlist-color-trigger.has-color::after {
        background: var(--watchlist-flag-color);
        border-color: var(--watchlist-flag-color);
    }

    /* The palette begins immediately to the right of the flag and may cover
       the coin name, but it can never cover the flag itself. */
    .watchlist-color-palette {
        position: absolute;
        left: 18px;
        top: 50%;
        transform: translateY(-50%);
        z-index: 3;
        display: flex;
        align-items: center;
        gap: 6px;
        padding: 5px 7px;
        border: 1px solid var(--border);
        border-radius: 6px;
        background: #171c23;
        box-shadow: 0 8px 24px rgba(0,0,0,.45);
        white-space: nowrap;
    }

    .watchlist-color-option {
        width: 14px;
        height: 14px;
        padding: 0;
        border-radius: 50%;
        border: 1px solid rgba(255,255,255,.35);
        cursor: pointer;
    }

    .watchlist-color-option:hover {
        transform: scale(1.2);
        border-color: #fff;
    }

    .watchlist-color-clear {
        width: 14px;
        height: 14px;
        padding: 0;
        border-radius: 50%;
        border: 1px solid #6f7885;
        background: transparent;
        cursor: pointer;
        position: relative;
    }

    .watchlist-color-clear::after {
        content: "";
        position: absolute;
        width: 16px;
        height: 1px;
        left: -2px;
        top: 6px;
        background: #aeb6c1;
        transform: rotate(-45deg);
    }

    .watchlist-colored-row td:first-child {
        box-shadow: inset 2px 0 0 var(--watchlist-row-color);
    }

    .positive {
        color: var(--green);
    }

    .negative {
        color: var(--red);
    }

    .muted {
        color: var(--muted);
    }

    .right {
        text-align: right;
    }

    .signal-list {
        max-height: calc(100vh - 128px);
        overflow: auto;
    }

    .signal {
        padding: 10px 12px;
        border-bottom: 1px solid var(--border);
    }

    .signal-time {
        color: var(--muted);
        font-size: 11px;
        margin-bottom: 4px;
    }

    .level-list {
        max-height: 260px;
        overflow: auto;
    }

    .level {
        padding: 10px 12px;
        border-bottom: 1px solid var(--border);
    }

    .level-price {
        font-weight: 700;
    }

    .connection-meta {
        padding: 10px 12px;
        border-top: 1px solid var(--border);
        color: var(--muted);
        line-height: 1.6;
    }

    .error-box {
        margin: 10px 18px 0;
        padding: 9px 11px;
        border: 1px solid #5a2525;
        background: #261417;
        color: #ffb4b0;
        border-radius: 5px;
        display: none;
    }

    @media (max-width: 700px) {
        .chart-intervals {
            gap: 2px;
        }

        .chart-interval {
            padding: 4px 5px;
            font-size: 11px;
        }
    }

    @media (max-width: 1000px) {
        .layout {
            grid-template-columns: 1fr;
        }

        .signal-list {
            max-height: 300px;
        }
    }

/* app35: controls upper-right; order remains search -> settings -> LIVE */
.topbar {
  position: relative;
}
.top-actions {
  margin-left: 0 !important;
  display: flex;
  align-items: center;
}
.status {
  margin-left: 0 !important;
}

/* app13: compact horizontal workspace visibility controls */
.top-view-controls {
  margin-left: auto;
  display: inline-flex;
  align-items: center;
  gap: 12px;
  white-space: nowrap;
}
.top-view-check {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  color: var(--muted);
  font-size: 11px;
  cursor: pointer;
  user-select: none;
}
.top-view-check input {
  width: 13px;
  height: 13px;
  margin: 0;
  accent-color: var(--blue);
  cursor: pointer;
}
.top-view-check:hover { color: var(--text); }

/* app35: search input at the very top, centered horizontally */
.top-search,
#topSearch,
.search-field {
  position: fixed !important;
  top: 0 !important;
  left: 50% !important;
  right: auto !important;
  bottom: auto !important;
  transform: translateX(-50%) !important;
  z-index: 10000 !important;
}


/* app44: search overlay — always above every chart layout */
#topSearch {
  position: fixed !important;
  inset: 0 !important;
  z-index: 100000 !important;
  display: none !important;
  align-items: flex-start !important;
  justify-content: center !important;
  background: transparent !important;
  padding: 18px 20px !important;
  box-sizing: border-box !important;
  transform: none !important;
}
#topSearch.open {
  display: flex !important;
}
#topSearch .search-modal {
  width: min(560px, 90vw) !important;
  margin: 0 !important;
}
#topSearch .search-wrap {
  width: 100% !important;
}
#topSearch .search {
  width: 100% !important;
}

/* app36: persistent workspace — large chart left, realtime coin list right */
.layout {
  grid-template-columns: minmax(0, 1fr) 390px;
  gap: 10px;
  padding: 10px;
  height: calc(100vh - 56px - 45px);
  min-height: 0;
}

.persistent-chart.chart-modal {
  position: relative;
  inset: auto;
  z-index: auto;
  display: flex;
  align-items: stretch;
  justify-content: stretch;
  background: var(--panel);
  padding: 0;
  min-width: 0;
  min-height: 0;
  overflow: hidden;
}

.persistent-chart .chart-box {
  width: 100%;
  height: 100%;
  min-width: 0;
  min-height: 0;
  border-radius: 6px;
}

.persistent-chart .chart-close {
  display: none;
}

.market-panel {
  min-width: 0;
  width: 100%;
  min-height: 0;
  height: 100%;
  overflow: hidden;
}
.market-panel.workspace-hidden {
  display: none !important;
}
.layout.market-hidden {
  grid-template-columns: minmax(0, 1fr);
}
.workspace-divider.workspace-hidden {
  display: none !important;
}

.market-panel .table-wrap {
  max-height: none;
  width: 100%;
  min-width: 0;
  height: calc(100% - 40px);
  overflow: auto;
}

.market-panel table {
  min-width: 760px;
}

@media (max-width: 1000px) {
  .layout {
    grid-template-columns: minmax(0, 1fr) 330px;
  }
}


/* app37: draggable vertical divider between chart and market screener */
.layout {
  grid-template-columns: minmax(320px, 1fr) 8px 420px;
}
.workspace-divider {
  width: 8px;
  height: 100%;
  min-height: 0;
  cursor: col-resize;
  position: relative;
  z-index: 50;
  touch-action: none;
}
.workspace-divider::after {
  content: "";
  position: absolute;
  top: 0;
  bottom: 0;
  left: 3px;
  width: 2px;
  background: #2a3440;
  border-radius: 2px;
}
.workspace-divider:hover::after,
.workspace-divider.dragging::after {
  width: 4px;
  left: 2px;
  background: #8b949e;
}
body.workspace-resizing,
body.workspace-resizing * {
  cursor: col-resize !important;
  user-select: none !important;
}
body.density-workspace-resizing,
body.density-workspace-resizing * {
  cursor: row-resize !important;
  user-select: none !important;
}


.broker-card-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;max-height:420px;overflow-y:auto;padding:4px}
.broker-card{border:1px solid #303744;border-radius:8px;padding:12px;display:flex;flex-direction:column;gap:8px;background:#121821}
.broker-card label{display:flex;flex-direction:column;gap:4px;font-size:12px}
.broker-card select,.broker-card input{width:100%;box-sizing:border-box}
.broker-checkboxes{display:grid;grid-template-columns:repeat(2,1fr);gap:8px}
.market-sort-header{cursor:pointer;user-select:none;white-space:nowrap}
.market-sort-header:hover{background:#1b2430}


/* app7: Alert constructor — stage 1 */
.alert-top-action {
    position: relative;
    font-size: 16px;
}
.alert-top-badge {
    position: absolute;
    top: -5px;
    right: -5px;
    min-width: 15px;
    height: 15px;
    padding: 0 3px;
    border-radius: 8px;
    background: var(--red);
    color: #fff;
    font-size: 9px;
    line-height: 15px;
    text-align: center;
    box-sizing: border-box;
    display: none;
}
.alert-overlay {
    position: fixed;
    inset: 0;
    z-index: 450;
    display: none;
    align-items: center;
    justify-content: center;
    background: rgba(0,0,0,.72);
    padding: 20px;
    box-sizing: border-box;
}
.alert-overlay.open { display: flex; }
.alert-modal {
    width: min(860px, 96vw);
    max-height: min(820px, 92vh);
    overflow: hidden;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 8px;
    box-shadow: 0 16px 50px rgba(0,0,0,.5);
    display: flex;
    flex-direction: column;
}
.alert-header {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 12px 14px;
    border-bottom: 1px solid var(--border);
    font-weight: 700;
}
.alert-header-icon {
    width: 28px;
    height: 28px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 6px;
    background: #202938;
    color: #9cc8ff;
    font-size: 15px;
}
.alert-header-title { flex: 1; }
.alert-close {
    width: 30px;
    height: 30px;
    padding: 0;
    border: 1px solid var(--border);
    border-radius: 5px;
    background: transparent;
    color: var(--muted);
    cursor: pointer;
    font-size: 18px;
}
.alert-close:hover { color: var(--text); background: #1a212b; }
.alert-body {
    overflow: auto;
    padding: 14px;
}
.alert-stepper {
    display: grid;
    grid-template-columns: 1fr 1.4fr 1fr;
    gap: 0;
    margin-bottom: 16px;
}
.alert-step {
    position: relative;
    display: flex;
    align-items: center;
    gap: 7px;
    color: var(--muted);
    font-size: 12px;
    padding-right: 14px;
}
.alert-step:not(:last-child)::after {
    content: "";
    position: absolute;
    top: 50%;
    left: 30px;
    right: 8px;
    height: 1px;
    background: var(--border);
    z-index: 0;
    pointer-events: none;
}
.alert-step-label {
    position: relative;
    z-index: 2;
    background: var(--panel);
    padding-right: 5px;
    white-space: nowrap;
}
.alert-step-number {
    position: relative;
    z-index: 1;
    width: 22px;
    height: 22px;
    flex: 0 0 22px;
    border-radius: 50%;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    background: #69717d;
    color: #fff;
    font-size: 11px;
    font-weight: 700;
}
.alert-step.active .alert-step-number { background: #2d8cff; }
.alert-step.done .alert-step-number { background: #78bfff; color: #0e1722; }
.alert-step.active .alert-step-label { color: var(--text); font-weight: 600; }
.alert-step.done .alert-step-label { color: var(--text); }
.alert-step-content { display: none; }
.alert-step-content.active { display: block; }
.alert-field { margin-bottom: 13px; }
.alert-field > label,
.alert-section-title {
    display: block;
    margin-bottom: 6px;
    color: var(--muted);
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
}
.alert-input,
.alert-select,
.alert-search-input {
    width: 100%;
    min-height: 34px;
    box-sizing: border-box;
    border: 1px solid var(--border);
    border-radius: 5px;
    background: var(--panel2);
    color: var(--text);
    padding: 7px 9px;
    outline: none;
}
.alert-input:focus,
.alert-select:focus,
.alert-search-input:focus { border-color: var(--blue); }
.alert-market-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 8px;
}
.alert-market-option {
    display: flex;
    align-items: center;
    gap: 7px;
    padding: 9px 10px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--panel2);
    cursor: pointer;
    font-size: 12px;
}
.alert-market-option:hover { background: #1a212b; }
.alert-market-option input { accent-color: #2d8cff; }
.alert-coin-box {
    border: 1px solid var(--border);
    border-radius: 6px;
    overflow: hidden;
}
.alert-coin-search { padding: 8px; border-bottom: 1px solid var(--border); }
.alert-coin-list {
    max-height: 190px;
    overflow: auto;
    padding: 6px 8px;
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 3px 8px;
}
.alert-coin-item {
    display: flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
    padding: 4px 3px;
    font-size: 11px;
    cursor: pointer;
}
.alert-coin-item:hover { background: #1a212b; }
.alert-coin-item span { overflow: hidden; text-overflow: ellipsis; }
.alert-coin-actions {
    display: flex;
    gap: 7px;
    margin-top: 7px;
}
.alert-small-button,
.alert-footer button {
    border: 1px solid var(--border);
    border-radius: 5px;
    background: var(--panel2);
    color: var(--text);
    padding: 7px 11px;
    cursor: pointer;
    font-size: 11px;
}
.alert-small-button:hover,
.alert-footer button:hover { background: #1a212b; }
.alert-filter-list {
    display: flex;
    flex-direction: column;
    gap: 8px;
}
.alert-filter-row {
    display: grid;
    grid-template-columns: minmax(150px, 1fr) 86px 92px 92px 34px 30px;
    gap: 7px;
    align-items: center;
    padding: 8px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: #121821;
}
.alert-filter-row .alert-select,
.alert-filter-row .alert-input { min-height: 31px; font-size: 11px; }
.alert-filter-spacer { width: 0; height: 0; }
.alert-filter-settings-button { width: 30px; height: 30px; border: 1px solid var(--border); border-radius: 5px; background: transparent; color: var(--muted); cursor: pointer; font-size: 14px; line-height: 1; }
.alert-filter-settings-button:hover, .alert-filter-settings-button.active { color: var(--text); border-color: var(--blue); background: #1a212b; }
.alert-filter-settings-popover { grid-column: 1 / -1; display: none; align-items: center; gap: 10px; padding: 9px 10px; border-top: 1px solid var(--border); color: var(--muted); font-size: 10px; }
.alert-filter-settings-popover.open { display: flex; }
.alert-filter-settings-popover label { display: flex; align-items: center; gap: 6px; }
.alert-filter-settings-popover .alert-input { width: 82px; }
.alert-filter-settings-popover .alert-select { min-width: 100px; }
.alert-filter-remove {
    width: 30px;
    height: 30px;
    border: 1px solid var(--border);
    border-radius: 5px;
    background: transparent;
    color: var(--muted);
    cursor: pointer;
}
.alert-filter-remove:hover { color: var(--red); border-color: var(--red); }
.alert-filter-add {
    margin-top: 8px;
    width: 100%;
    border: 1px dashed #46505e;
    border-radius: 6px;
    background: transparent;
    color: var(--muted);
    padding: 8px;
    cursor: pointer;
    font-size: 11px;
}
.alert-filter-add:hover { color: var(--text); border-color: var(--blue); }
.alert-filter-hint {
    margin: 7px 0 12px;
    color: var(--muted);
    font-size: 10px;
}
.alert-condition-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 10px;
}
.alert-channel-grid { display: grid; gap: 7px; }
.alert-channel {
    display: flex;
    align-items: center;
    gap: 9px;
    padding: 9px 10px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--panel2);
    cursor: pointer;
}
.alert-channel input { accent-color: #2d8cff; }
.alert-channel-text { display: flex; flex-direction: column; gap: 2px; }
.alert-channel-text b { font-size: 12px; }
.alert-channel-text span { color: var(--muted); font-size: 10px; }
.alert-summary {
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 10px;
    background: #121821;
    font-size: 11px;
    line-height: 1.55;
}
.alert-summary-row { display: flex; justify-content: space-between; gap: 12px; }
.alert-summary-row + .alert-summary-row { margin-top: 4px; }
.alert-summary-key { color: var(--muted); }
.alert-footer {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    padding: 11px 14px;
    border-top: 1px solid var(--border);
}
.alert-footer-left,
.alert-footer-right { display: flex; gap: 7px; }
.alert-footer .primary {
    background: #2d8cff;
    border-color: #2d8cff;
    color: #fff;
}
.alert-footer .primary:hover { background: #4b9cff; }
.alert-footer .danger:hover { border-color: var(--red); color: var(--red); }
.alert-saved-list {
    display: flex;
    flex-direction: column;
    gap: 7px;
    margin-top: 12px;
}
.alert-saved-item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    padding: 9px 10px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--panel2);
}
.alert-saved-name { font-weight: 600; font-size: 12px; }
.alert-saved-meta { color: var(--muted); font-size: 10px; margin-top: 2px; }
.alert-empty { color: var(--muted); font-size: 11px; padding: 8px 0; }
@media (max-width: 720px) {
    .alert-market-grid { grid-template-columns: 1fr; }
    .alert-coin-list { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .alert-filter-row { grid-template-columns: 1fr 1fr 1fr 1fr 34px 30px; }
    .alert-condition-grid { grid-template-columns: 1fr; }
}


/* app10: external alert delivery settings */
.alert-telegram-settings { margin-top:12px; padding:10px; border:1px solid var(--border); border-radius:7px; background:#12171e; }
.alert-telegram-grid { display:grid; grid-template-columns:1fr 1fr; gap:10px; }
.alert-input { width:100%; box-sizing:border-box; padding:8px 9px; border:1px solid var(--border); border-radius:5px; background:#0f141a; color:var(--text); outline:none; }
.alert-input:focus { border-color:var(--blue); }
.alert-telegram-actions { display:flex; align-items:center; gap:9px; margin-top:8px; }
.alert-telegram-status { font-size:10px; color:var(--muted); }
@media (max-width:700px) { .alert-telegram-grid { grid-template-columns:1fr; } }

/* app12: alert center tabs + active switches */
.alert-center-tabs {
    display:grid;
    grid-template-columns:1fr 1fr;
    gap:4px;
    padding:7px 8px 0;
    border-bottom:1px solid var(--border);
}
.alert-center-tab {
    border:0;
    border-bottom:2px solid transparent;
    background:transparent;
    color:var(--muted);
    padding:7px 8px 8px;
    cursor:pointer;
    font-size:11px;
    font-weight:600;
}
.alert-center-tab.active {
    color:var(--text);
    border-bottom-color:#2d8cff;
}
.alert-center-tab:hover { color:var(--text); }
.alert-center-tab.alert-tab-hidden { display:none; }
.alert-center-view { display:none; min-height:0; flex:1; overflow:hidden; }
.alert-center-view.active { display:flex; flex-direction:column; }
.alert-saved-center-list {
    overflow:auto;
    padding:8px;
    display:flex;
    flex-direction:column;
    gap:7px;
}
.alert-saved-center-item {
    display:flex;
    align-items:center;
    gap:8px;
    padding:9px 10px;
    border:1px solid var(--border);
    border-radius:6px;
    background:var(--panel2);
}
.alert-saved-center-info { flex:1; min-width:0; cursor:pointer; }
.alert-saved-center-name { font-weight:700; font-size:12px; }
.alert-saved-center-meta { color:var(--muted); font-size:9px; margin-top:3px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.alert-saved-center-actions { display:flex; align-items:center; gap:5px; flex:0 0 auto; }
.alert-saved-center-edit {
    border:1px solid var(--border); border-radius:5px; background:transparent; color:var(--muted);
    padding:5px 7px; cursor:pointer; font-size:10px;
}
.alert-saved-center-edit:hover { color:var(--text); background:#1a212b; }
.alert-toggle {
    position:relative; width:30px; height:17px; padding:0; border:0; border-radius:9px;
    background:#59636f; cursor:pointer; flex:0 0 30px; transition:background .12s ease;
}
.alert-toggle::after {
    content:""; position:absolute; top:3px; left:3px; width:11px; height:11px; border-radius:50%;
    background:#fff; transition:transform .12s ease; box-shadow:0 1px 2px rgba(0,0,0,.3);
}
.alert-toggle.on { background:#2d8cff; }
.alert-toggle.on::after { transform:translateX(13px); }
.alert-saved-item .alert-saved-actions { display:flex; align-items:center; gap:6px; }
.alert-saved-item .alert-toggle { margin-left:2px; }
.alert-saved-item.is-disabled { opacity:.62; }
.alert-center-empty { color:var(--muted); text-align:center; padding:30px 12px; font-size:11px; }

.stock-market-icon {
    display:inline-flex;
    align-items:center;
    justify-content:center;
    width:15px;
    height:15px;
    margin-left:5px;
    color:var(--muted);
    opacity:.92;
    vertical-align:-2px;
    box-sizing:border-box;
    flex:0 0 15px;
}
.stock-market-icon svg {
    width:15px;
    height:15px;
    display:block;
    fill:none;
    stroke:currentColor;
    stroke-width:1.35;
    stroke-linecap:round;
    stroke-linejoin:round;
}
.stock-market-icon:hover {
    color:var(--text);
    opacity:1;
}

/* app9: in-site alert notification center */
.alert-notification-panel {
    position: fixed;
    top: 50px;
    right: 12px;
    width: min(390px, calc(100vw - 24px));
    max-height: min(620px, calc(100vh - 70px));
    z-index: 520;
    display: none;
    flex-direction: column;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 8px;
    box-shadow: 0 16px 50px rgba(0,0,0,.55);
    overflow: hidden;
}
.alert-notification-panel.open { display: flex; }
.alert-notification-header {
    display:flex;
    align-items:center;
    gap:8px;
    padding:10px 12px;
    border-bottom:1px solid var(--border);
}
.alert-notification-title { flex:1; font-size:13px; font-weight:700; }
.alert-notification-unread {
    min-width:18px;
    height:18px;
    padding:0 5px;
    border-radius:10px;
    background:var(--red);
    color:#fff;
    font-size:10px;
    line-height:18px;
    text-align:center;
    box-sizing:border-box;
}
.alert-notification-actions { display:flex; gap:5px; }
.alert-notification-actions button,
.alert-notification-create {
    border:1px solid var(--border);
    border-radius:5px;
    background:var(--panel2);
    color:var(--text);
    cursor:pointer;
    font-size:10px;
    padding:6px 8px;
}
.alert-notification-actions button:hover,
.alert-notification-create:hover { background:#1a212b; }
.alert-notification-list {
    overflow:auto;
    padding:8px;
    display:flex;
    flex-direction:column;
    gap:7px;
}
.alert-notification-empty {
    color:var(--muted);
    text-align:center;
    padding:28px 12px;
    font-size:11px;
}
.alert-notification-item {
    padding:8px 9px;
    border-bottom:1px solid var(--border);
    cursor:pointer;
    transition:background .12s ease, opacity .12s ease;
}
.alert-notification-item:hover { background:#1a212b; }
.alert-notification-item.unread { border-left:2px solid var(--blue); padding-left:7px; }
.alert-notification-item.read { opacity:.72; }
.alert-notification-item-title {
    font-size:11px;
    font-weight:700;
    color:var(--text);
    line-height:1.35;
}
.alert-notification-item-meta {
    display:flex;
    align-items:center;
    gap:8px;
    margin-top:3px;
    color:var(--muted);
    font-size:9px;
    line-height:1.3;
}
.alert-notification-item-symbol {
    color:var(--text);
    font-weight:700;
}

.alert-notification-card {
    position:relative;
    padding:9px 10px;
    border:1px solid var(--border);
    border-radius:6px;
    background:var(--panel2);
    cursor:pointer;
    transition:background .12s ease, border-color .12s ease, opacity .12s ease;
}
.alert-notification-card:hover { background:#1a212b; border-color:#46505e; }
.alert-notification-card.unread { border-left:3px solid var(--blue); }
.alert-notification-card.read { opacity:.72; }
.alert-notification-card-head {
    display:flex;
    align-items:center;
    gap:7px;
    margin-bottom:4px;
}
.alert-notification-name { flex:1; font-weight:700; font-size:12px; }
.alert-notification-time { color:var(--muted); font-size:9px; white-space:nowrap; }
.alert-notification-symbol { font-size:15px; font-weight:700; line-height:1.2; }
.alert-notification-market { color:var(--muted); font-size:9px; margin-top:1px; }
.alert-notification-values {
    display:flex;
    flex-wrap:wrap;
    gap:4px 8px;
    margin-top:6px;
}
.alert-notification-value {
    font-size:10px;
    color:var(--text);
}
.alert-notification-value b { font-weight:700; }
.alert-notification-muted {
    width:25px;
    height:25px;
    padding:0;
    border:1px solid var(--border);
    border-radius:5px;
    background:transparent;
    color:var(--muted);
    cursor:pointer;
    font-size:12px;
}
.alert-notification-muted:hover { color:var(--text); background:#1a212b; }
.alert-notification-footer {
    padding:8px 10px;
    border-top:1px solid var(--border);
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:8px;
}
.alert-notification-footer-text { color:var(--muted); font-size:9px; }

/* In-site realtime notification toasts. */
.alert-toast-stack {
    position:fixed; right:14px; bottom:14px; z-index:900;
    width:min(360px, calc(100vw - 28px));
    max-height:calc(100vh - 28px);
    display:flex; flex-direction:column; gap:8px;
    pointer-events:none;
    overflow:hidden;
}
.alert-toast {
    pointer-events:auto; position:relative; padding:10px 11px 10px 13px;
    border:1px solid var(--border); border-left:3px solid var(--blue);
    border-radius:7px; background:var(--panel); color:var(--text);
    box-shadow:0 14px 40px rgba(0,0,0,.48);
    animation:alertToastIn .16s ease-out;
}
.alert-toast.signal { border-left-color:#2bd576; }
.alert-toast-head { display:flex; align-items:center; gap:7px; margin-bottom:4px; }
.alert-toast-name { flex:1; font-size:11px; font-weight:700; }
.alert-toast-time { color:var(--muted); font-size:9px; white-space:nowrap; }
.alert-toast-close {
    width:22px; height:22px; padding:0; border:1px solid var(--border);
    border-radius:4px; background:transparent; color:var(--muted); cursor:pointer;
}
.alert-toast-close:hover { color:var(--text); background:#1a212b; }
.alert-toast-symbol { font-size:15px; font-weight:700; line-height:1.2; }
.alert-toast-market { color:var(--muted); font-size:9px; margin-top:1px; }
.alert-toast-message { font-size:10px; line-height:1.4; margin-top:6px; }
.alert-toast-values { display:flex; flex-wrap:wrap; gap:4px 8px; margin-top:6px; }
.alert-toast-value { font-size:10px; }
.alert-toast-footer { display:flex; align-items:center; justify-content:space-between; gap:8px; margin-top:7px; }
.alert-toast-unread { color:var(--muted); font-size:9px; }
.alert-toast-mute {
    border:1px solid var(--border); border-radius:4px; background:transparent;
    color:var(--muted); cursor:pointer; font-size:11px; padding:3px 6px;
}
@keyframes alertToastIn { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:translateY(0); } }


/* app62: coin-list / independent template workspace */
.market-panel .panel-header { display:flex; align-items:center; gap:8px; }
#marketTemplateButton { border:0; background:transparent; color:var(--text); font:inherit; font-weight:600; padding:4px 6px; border-radius:5px; cursor:pointer; }
#marketTemplateButton:hover { background:#202832; }
.market-template-menu { position:absolute; right:8px; top:42px; z-index:500; width:min(360px, calc(100vw - 24px)); background:#151b23; border:1px solid #2b3542; border-radius:8px; box-shadow:0 16px 40px rgba(0,0,0,.35); padding:8px; display:none; }
.market-template-header{min-width:0}.market-template-search-wrap{display:flex;align-items:center;min-width:180px;max-width:330px;flex:1;border:1px solid #303b48;border-radius:6px;background:#0d1117}.market-template-search{width:100%;min-width:0;border:0!important;outline:0;background:transparent;color:var(--text);font:inherit;font-size:12px;padding:6px 8px}.market-template-search::placeholder{color:#697786}.market-template-search-button{width:27px;height:27px;border:0;border-left:1px solid #303b48;background:transparent;color:#9daab8;cursor:pointer}.market-template-search-button:hover{background:#202832;color:#fff}.market-template-search-wrap:focus-within{border-color:#526579}.market-template-header > #activeTemplateLabel{display:none}.market-template-menu.open { display:block; }
.market-template-item { display:flex; align-items:center; gap:8px; padding:9px 10px; border-radius:6px; cursor:pointer; }
.market-template-item:hover { background:#202832; }
.market-template-item.active { background:#1c2834; }
.market-template-item.keyboard-active { background:#202832; outline:1px solid #3b4b5c; }
.market-template-item .name { flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.market-template-item .edit { opacity:.65; border:0; background:transparent; color:var(--text); cursor:pointer; }
.market-template-add { width:100%; margin-top:6px; border:1px dashed #3a4654; background:transparent; color:#a9b7c6; border-radius:6px; padding:8px; cursor:pointer; }
.market-template-add:hover { background:#1b232d; color:#fff; }
.template-overlay { position:fixed; inset:0; background:rgba(0,0,0,.58); z-index:1200; display:none; align-items:center; justify-content:center; }
.template-overlay.open { display:flex; }
.template-modal { width:min(820px,94vw); max-height:90vh; overflow:auto; background:#11161d; border:1px solid #2d3845; border-radius:10px; box-shadow:0 24px 70px rgba(0,0,0,.5); }
.template-header,.template-footer { display:flex; align-items:center; justify-content:space-between; padding:14px 16px; border-bottom:1px solid #27313d; }
.template-footer { border-top:1px solid #27313d; border-bottom:0; gap:8px; justify-content:flex-end; }
.template-title { font-weight:700; font-size:15px; }
.template-close { border:0; background:transparent; color:#9aa7b5; font-size:22px; cursor:pointer; }
.template-body { padding:16px; display:grid; gap:14px; }
.template-section { border:1px solid #27313d; border-radius:8px; padding:12px; }
.template-section h4 { margin:0 0 10px; font-size:12px; color:#aebbc9; text-transform:uppercase; letter-spacing:.04em; }
.template-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; }
.template-field { display:grid; gap:5px; }
.template-field label { color:#aeb8c5; font-size:12px; }
.template-field input,.template-field select { width:100%; background:#0d1117; color:#e6edf3; border:1px solid #303b48; border-radius:5px; padding:7px 8px; }
.template-check { display:flex; align-items:center; gap:7px; color:#c5d0db; }
.template-actions { display:flex; gap:8px; flex-wrap:wrap; }
.template-btn { border:1px solid #354252; background:#18212b; color:#e6edf3; border-radius:6px; padding:8px 11px; cursor:pointer; }
.template-btn.primary { background:#245b8f; border-color:#3275b4; }
.template-btn.danger { background:#3a1d21; border-color:#71333a; }
.template-muted { color:#7f8c99; font-size:11px; }
.template-structure-results { margin-top:10px; color:#9aa7b5; font-size:11px; }
.template-structure-search-choice { grid-column: 1 / -1; }
.template-structure-search-options { display:flex; flex-wrap:wrap; gap:10px 18px; margin-top:7px; }
.template-structure-search-options > label { display:flex; align-items:center; gap:7px; min-height:30px; padding:6px 10px; border:1px solid #34404d; border-radius:6px; background:#10161d; cursor:pointer; font-weight:600; }
.template-structure-search-options > label:has(input:checked) { border-color:#5d8fd3; background:#172334; }
.template-structure-search-options input { margin:0; }

@media (max-width:700px) { .template-grid { grid-template-columns:1fr; } }.template-timeframe-card{position:relative}.template-timeframe-head{display:flex;align-items:center;justify-content:space-between;gap:10px}.template-timeframe-head .template-market-card-title{margin:0}.template-timeframe-strict{display:flex!important;flex-direction:row!important;align-items:center;gap:6px;color:#c5d0db;font-size:11px!important;white-space:nowrap}.template-timeframe-strict input{width:auto!important;margin:0}.template-timeframe-controls{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:9px}.template-timeframe-controls.strict{grid-template-columns:1fr}.template-timeframe-controls label{display:flex;flex-direction:column;gap:4px;color:#aeb8c5;font-size:10px}.template-timeframe-controls input{width:100%;box-sizing:border-box}.template-timeframe-hint{margin-top:7px;color:#7f8c99;font-size:10px}
.template-metric-row{display:grid;grid-template-columns:1.05fr .75fr .75fr .7fr .7fr;gap:6px;align-items:center}.template-metric-row select,.template-metric-row input{min-width:0}.template-metric-block{grid-column:1/-1}.template-market-card-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}.template-market-card{border:1px solid #27313d;border-radius:8px;padding:11px;background:#11171f}.template-market-card-title{font-weight:700;font-size:12px;margin-bottom:9px;color:#e7edf4}.template-market-card-head{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:9px}.template-market-card-head .template-market-card-title{margin:0}.template-market-strict{display:flex!important;flex-direction:row!important;align-items:center;gap:5px!important;white-space:nowrap;font-size:10px!important;color:#9daab8}.template-market-strict input{width:auto!important}.template-market-card label{display:flex;flex-direction:column;gap:4px;font-size:10px;color:#9daab8}.template-market-card select,.template-market-card input{width:100%;box-sizing:border-box}.template-market-range{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px}.template-market-card .template-market-range:first-of-type{margin-top:0}.template-market-card .template-market-range label{min-width:0}.template-market-card .template-market-range.is-strict{grid-template-columns:1fr}.template-market-card .template-market-range.is-hidden{display:none}.template-market-card .template-volume-expansion-inline{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px}.template-market-card .template-volume-expansion-inline label{display:grid;grid-template-columns:minmax(0,1fr) auto;grid-template-rows:auto auto;align-items:center;column-gap:5px;row-gap:4px}.template-market-card .template-volume-expansion-inline label span{grid-column:1/-1;white-space:nowrap}.template-market-card .template-volume-expansion-inline label input{grid-column:1}.template-market-card .template-volume-expansion-inline label em{font-style:normal;white-space:nowrap;color:#9daab8;font-size:10px}.template-oi-direction-row{grid-template-columns:minmax(0,1fr)}.template-oi-direction-row label{max-width:50%}.template-market-card .template-oi-direction-row select{height:30px}.template-volume-direction-row{margin-top:8px}.template-volume-direction-row label{max-width:50%}.template-market-card .template-volume-direction-row select{height:30px}.market-panel .market-template-dock{width:100%;margin:0;position:relative;box-sizing:border-box}.market-panel .market-template-header{height:40px;box-sizing:border-box}.market-panel .market-template-dock .market-template-search-wrap{margin-left:10px;width:calc(100% - 10px);max-width:none}.market-panel .market-template-dock .market-template-menu{right:8px;top:42px}.market-panel .market-template-dock .template-search-status{left:27px;right:0;top:42px;background:var(--panel2)}.template-timeframe-card{position:relative}.template-timeframe-head{display:flex;align-items:center;justify-content:space-between;gap:10px}.template-timeframe-head .template-market-card-title{margin:0}.template-timeframe-strict{display:flex;align-items:center;gap:6px;color:#c5d0db;font-size:11px;white-space:nowrap}.template-timeframe-strict input{margin:0}.template-timeframe-controls{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:9px}.template-timeframe-controls.strict{grid-template-columns:1fr}.template-timeframe-controls label{display:flex;flex-direction:column;gap:4px;color:#aeb8c5;font-size:10px}.template-timeframe-controls select{width:100%;box-sizing:border-box}.template-timeframe-hint{margin-top:7px;color:#7f8c99;font-size:10px}.template-search-status{min-height:18px;padding:6px 2px 2px;color:#8e9aaa;font-size:11px}.chart-overlay-toggle{height:26px;padding:0 9px;border:1px solid #303b48;border-radius:5px;background:#11171f;color:#9daab8;font-size:11px;cursor:pointer}.chart-overlay-toggle.active{color:#eef3f8;border-color:#59697c;background:#1a222c}.chart-overlay-toggle:not(.active){opacity:.55}@media(max-width:900px){.template-market-card-grid{grid-template-columns:1fr}}
.volume-history-modal{width:min(900px,calc(100vw - 24px));max-height:calc(100vh - 40px);overflow:hidden}.volume-history-results{display:grid;gap:6px;max-height:360px;overflow:auto}.volume-history-result{display:grid;grid-template-columns:1.05fr .8fr .65fr .8fr .8fr 1fr;gap:8px;align-items:center;border:1px solid #27313d;border-radius:6px;padding:8px 10px;background:#0f141b;color:#dbe4ed;font-size:11px;cursor:pointer}.volume-history-result:hover{background:#151d26;border-color:#3a4858}.volume-history-result-head{color:#7f8c99;font-size:10px}.volume-history-result-main{font-weight:700}.volume-history-result-x{font-weight:700}.volume-history-result-meta{color:#9daab8}.volume-history-empty{padding:10px;text-align:center;color:#7f8c99;font-size:11px}@media(max-width:700px){.volume-history-result{grid-template-columns:1fr 1fr}.volume-history-modal .template-grid{grid-template-columns:1fr 1fr}}
/* Density presentation — Stage 4 */
.density-workspace{grid-column:auto;grid-row:auto;min-width:0;min-height:120px;width:100%;height:360px;flex:0 0 360px;position:relative;border:1px solid #27313d;border-radius:7px;background:#0f141b;overflow:hidden;display:flex;flex-direction:column}
.density-workspace[hidden]{display:none!important}
.density-workspace-resize{position:absolute;left:0;right:0;top:-4px;height:8px;cursor:row-resize;z-index:60;touch-action:none}
.density-workspace-resize:after{content:"";position:absolute;left:35%;right:35%;top:3px;height:2px;border-radius:2px;background:#2f3b49}
.density-workspace-resize:hover:after,.density-workspace-resize.dragging:after{left:30%;right:30%;height:3px;background:#718096}
.density-workspace-head{height:31px;min-height:31px;display:flex;align-items:center;justify-content:space-between;gap:10px;padding:0 10px;border-bottom:1px solid #27313d;background:#131a22;font-size:11px}
 .density-workspace-title{display:flex;align-items:center;gap:7px;font-weight:700;color:#e5edf5;white-space:nowrap}
.density-map-status{display:inline-flex;align-items:center;gap:5px;font-weight:600;margin-left:4px}
.density-map-status-dot{width:7px;height:7px;border-radius:50%;display:inline-block;background:#778395}
.density-map-status.loading .density-map-status-dot{background:#f59e0b;box-shadow:0 0 0 2px rgba(245,158,11,.12)}
.density-map-status.ready .density-map-status-dot{background:#22c55e;box-shadow:0 0 0 2px rgba(34,197,94,.12)}
.density-map-status.error .density-map-status-dot{background:#ef4444;box-shadow:0 0 0 2px rgba(239,68,68,.12)}
.density-map-status.empty .density-map-status-dot{background:#7b8796}
.density-workspace-dot{width:7px;height:7px;border-radius:50%;background:#f59e0b;box-shadow:0 0 0 2px rgba(245,158,11,.12)}
.density-workspace-scale{color:#8492a3;white-space:nowrap;font-variant-numeric:tabular-nums}
.density-map-panel{margin:0;border:0;border-radius:0;background:#0e141b;overflow:hidden;min-height:0;flex:1;position:relative}
.density-map-list{background:#0e141b}
.density-map-column.small{background:#111923}.density-map-column.medium{background:#101821}.density-map-column.large{background:#0f171f}
.density-map-column.small .density-map-column-head{color:#91a0af}.density-map-column.medium .density-map-column-head{color:#c2ccd7}.density-map-column.large .density-map-column-head{color:#eef3f8}
.density-map-list{display:grid;grid-template-columns:22px repeat(3,minmax(0,1fr));gap:5px;padding:5px;height:100%;box-sizing:border-box;overflow:hidden;position:relative}
.density-map-axis{position:relative;height:100%;font-size:8px;color:#687687;text-align:center;display:flex;flex-direction:column;justify-content:space-between;padding:10px 0 12px;z-index:3}
.density-axis-ticks{position:absolute;inset:0;display:flex;flex-direction:column;justify-content:space-between;padding:8px 0 10px;pointer-events:none}.density-axis-tick{font-size:8px;line-height:1;color:#738093;white-space:nowrap}.density-axis-tick.zero{color:#d5dde6;font-weight:800}.density-map-axis .density-axis-top,.density-map-axis .density-axis-zero,.density-map-axis .density-axis-bottom{position:absolute;left:0;right:0}.density-map-axis .density-axis-top{top:2px}.density-map-axis .density-axis-zero{top:50%;transform:translateY(-50%)}.density-map-axis .density-axis-bottom{bottom:2px}
.density-axis-top{color:#ef7676}.density-axis-zero{color:#c4ccd6;font-weight:700}.density-axis-bottom{color:#62c68b}
.density-map-column{min-width:0;border:1px solid #26313e;border-radius:5px;background:#101720;display:flex;flex-direction:column;overflow:hidden}
.density-map-column-head{height:25px;min-height:25px;display:flex;align-items:center;justify-content:space-between;padding:0 7px;border-bottom:1px solid #27313d;color:#b9c5d2;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.03em}
.density-map-column-body{position:relative;flex:1;min-height:0;overflow:hidden}
.density-map-zero{position:absolute;left:0;right:0;top:50%;height:1px;background:#657181;opacity:.85;z-index:1}
.density-map-grid-lines{position:absolute;inset:0;pointer-events:none;z-index:0}.density-map-grid-lines i{position:absolute;left:0;right:0;height:1px;background:rgba(255,255,255,.055)}.density-map-grid-lines i:nth-child(1){top:15%}.density-map-grid-lines i:nth-child(2){top:32.5%}.density-map-grid-lines i:nth-child(3){top:50%}.density-map-grid-lines i:nth-child(4){top:67.5%}.density-map-grid-lines i:nth-child(5){top:85%}
.density-map-card{position:absolute;transform:translateY(-50%);left:7px;right:7px;display:flex;flex-direction:column;gap:1px;padding:4px 7px;border:1px solid #3a4654;border-radius:6px;background:#18212b;color:#eef3f8;font-size:9px;line-height:1.14;min-width:0;cursor:pointer;text-align:left;z-index:2;box-shadow:0 1px 4px rgba(0,0,0,.28);transition:filter .12s,border-color .12s,transform .12s}
.density-map-card.buy{border-color:#35a96d;background:linear-gradient(90deg,rgba(36,170,103,.28),rgba(25,37,47,.96) 72%)}
.density-map-card.sell{border-color:#e05b5b;background:linear-gradient(90deg,rgba(226,70,70,.30),rgba(31,36,45,.96) 72%)}
.density-map-card:hover{filter:brightness(1.18);border-color:#d7e0ea;transform:translateY(-50%) scale(1.015);z-index:5}
.density-map-card-title{font-weight:700}.density-map-card-title b{font-weight:900;margin-right:3px}.density-map-card-price{font-weight:800}.density-map-card-size{font-weight:700;color:#dbe5ee}.density-map-card-extra{color:#aebbc8;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.density-map-card:focus-visible{outline:2px solid #dce6ef;outline-offset:1px}
.density-map-card-title{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.density-map-card-title b{font-weight:800}.density-map-card-price{font-weight:700}.density-map-card-size{color:#d0dae4}.density-map-card-extra{color:#8e9baa;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.density-map-empty{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#637182;font-size:9px}
.market-panel{display:flex;flex-direction:column}
.market-panel .table-wrap{flex:1 1 auto;min-height:0;height:auto;max-height:none;overflow:auto}
.market-panel .density-workspace{order:3}.market-panel.density-only .density-workspace{height:100%;flex:1}.market-panel.density-only .market-template-header,.market-panel.density-only .market-template-menu,.market-panel.density-only .template-search-status{display:none}.density-source-hidden{display:none!important}
/* The density map is the primary visual workspace. Diagnostic/radar/screener blocks stay backend/API-only so they cannot collapse the map. */
#densityExchangeHealth,#densityExchangeRadar,#densityScreenerPanel{display:none!important}
@media(max-width:900px){.density-map-list{grid-template-columns:1fr}.density-workspace{min-height:150px;height:280px;flex-basis:280px}}
.density-chart-overlay{position:absolute;inset:0;pointer-events:none;z-index:24;overflow:hidden}
.density-chart-pill{position:absolute;right:8px;transform:translateY(-50%);min-width:145px;max-width:260px;padding:4px 7px;border-radius:5px;background:rgba(255,255,255,.94);box-shadow:0 1px 5px rgba(0,0,0,.28);color:#151515;font-size:10px;line-height:1.25;white-space:nowrap}
.density-chart-pill.buy{border-left:3px solid #16a34a}.density-chart-pill.sell{border-left:3px solid #dc2626}
.density-chart-pill .density-main{font-weight:700}.density-chart-pill .density-sub{opacity:.72}

/* Density settings / screener — Stage 5 */
.density-settings-form{display:flex;flex-direction:column;gap:0}
.density-setting-main{padding:4px 0 12px;border-bottom:1px solid #27313d;font-weight:700}
.density-setting-block{padding:13px 0;border-bottom:1px solid #27313d}
.density-setting-heading{font-size:12px;font-weight:700;color:#e5edf5;margin-bottom:9px;display:flex;align-items:center;gap:5px}
.density-setting-row{display:grid;grid-template-columns:1fr 22px;gap:12px;align-items:center;min-height:29px;font-size:12px;color:#cbd5df}
.density-setting-row input[type="checkbox"]{justify-self:center}
.density-setting-field{display:grid;grid-template-columns:1fr 170px;gap:12px;align-items:center;font-size:12px;color:#cbd5df;min-height:34px}
.density-setting-input,.density-setting-field input[type="number"],.density-setting-block textarea{width:100%;box-sizing:border-box}
.density-setting-input{height:32px}
.density-setting-block textarea{resize:vertical;min-height:105px;line-height:1.45;padding:8px}
.density-settings-status{font-size:11px;opacity:.75;margin-left:8px}.density-help{display:inline-flex;align-items:center;justify-content:center;width:15px;height:15px;border:1px solid #607083;border-radius:50%;font-size:10px;cursor:help;color:#cbd5df}
.density-screener-panel{margin:8px 0;border:1px solid rgba(255,255,255,.10);border-radius:8px;background:rgba(20,20,24,.82);overflow:hidden;max-height:300px}
.density-screener-list{display:flex;flex-direction:column;gap:3px;padding:6px;overflow:auto;max-height:250px}
.density-screener-row{display:grid;grid-template-columns:90px 70px 115px 115px 85px 120px;gap:7px;padding:6px;font-size:11px;border-radius:5px;background:rgba(255,255,255,.035)}


/* Multi-exchange radar — Stage 6 */
.density-exchange-health,.density-exchange-radar{margin:8px 0;border:1px solid rgba(255,255,255,.10);border-radius:8px;background:rgba(20,20,24,.82);overflow:hidden}
.density-exchange-radar-list{display:flex;flex-direction:column;gap:3px;padding:6px;overflow:auto;max-height:230px}
.density-exchange-radar-row{display:grid;grid-template-columns:90px 65px 110px 85px 1fr 90px;gap:7px;padding:6px;font-size:11px;border-radius:5px;background:rgba(255,255,255,.035)}
.density-exchange-radar-row.multi{border-left:3px solid #f59e0b}

</style>
<script src="https://unpkg.com/lightweight-charts@5.0.8/dist/lightweight-charts.standalone.production.js"></script>
</head>

<body>
<div id="densityChartOverlay" class="density-chart-overlay" aria-hidden="true"></div>

<div class="topbar">
    <div class="title">Crypto Screener</div>
    <div class="stage">Stage 1 • Binance Futures</div>

    <div class="top-view-controls" aria-label="Отображение панелей">
        <label class="top-view-check"><input id="viewCoinList" type="checkbox" checked><span>Список монет</span></label>
        <label class="top-view-check"><input id="viewAlerts" type="checkbox" checked><span>Алерты</span></label>
        <label class="top-view-check"><input id="viewDensity" type="checkbox"><span>Карта плотностей</span></label>
    </div>
    <div class="top-actions">
        <button id="alertsButton" class="top-action alert-top-action" type="button" title="Уведомления" aria-label="Уведомления">🔔<span id="alertCountBadge" class="alert-top-badge"></span></button>
        <button id="searchToggle" class="top-action" type="button" title="Поиск">⌕</button>
        <button id="settingsButton" class="top-action settings-button" type="button" title="Настройки">⚙</button>
    </div>

    <div class="status">
        <span id="statusDot" class="dot"></span>
        <span id="statusText">STARTING</span>
    </div>
</div>

<div id="topSearch" class="top-search">
    <div class="search-modal">
        <div class="search-wrap">
            <input id="search" class="search" type="text" placeholder="Введите монету, например BTCUSDT" autocomplete="off">
            <div id="suggestions" class="suggestions"></div>
        </div>
    </div>
</div>

<div id="alertsOverlay" class="alert-overlay" aria-hidden="true">
    <div class="alert-modal" role="dialog" aria-modal="true" aria-labelledby="alertsTitle">
        <div class="alert-header">
            <span class="alert-header-icon">🔔</span>
            <span id="alertsTitle" class="alert-header-title">Алерты</span>
            <button id="alertsClose" class="alert-close" type="button" title="Закрыть">×</button>
        </div>
        <div class="alert-body">
            <div class="alert-stepper">
                <div class="alert-step active" data-alert-step-indicator="1"><span class="alert-step-number">1</span><span class="alert-step-label">Биржи</span></div>
                <div class="alert-step" data-alert-step-indicator="2"><span class="alert-step-number">2</span><span class="alert-step-label">Фильтры</span></div>
                <div class="alert-step" data-alert-step-indicator="3"><span class="alert-step-number">3</span><span class="alert-step-label">Условия</span></div>
            </div>

            <section id="alertStep1" class="alert-step-content active">
                <div class="alert-field">
                    <label for="alertName">Название алерта</label>
                    <input id="alertName" class="alert-input" type="text" maxlength="80" placeholder="Например: Импульс">
                </div>
                <div class="alert-section-title">Рынок</div>
                <div class="alert-market-grid">
                    <label class="alert-market-option"><input type="radio" name="alertMarket" value="binance_futures" checked> Binance Futures</label>
                    <label class="alert-market-option" style="opacity:.45"><input type="radio" name="alertMarket" value="binance_spot" disabled> Binance Spot <span class="muted">(позже)</span></label>
                    <label class="alert-market-option" style="opacity:.45"><input type="radio" name="alertMarket" value="all" disabled> Все доступные <span class="muted">(позже)</span></label>
                </div>
                <div style="height:14px"></div>
                <div class="alert-section-title">Монеты</div>
                <div class="alert-coin-box">
                    <div class="alert-coin-search"><input id="alertCoinSearch" class="alert-search-input" type="text" placeholder="Найти монету..."></div>
                    <div id="alertCoinList" class="alert-coin-list"></div>
                </div>
                <div class="alert-coin-actions">
                    <button id="alertSelectAllCoins" class="alert-small-button" type="button">Выбрать все</button>
                    <button id="alertClearCoins" class="alert-small-button" type="button">Очистить</button>
                </div>
                <div class="alert-filter-hint">Если монеты не выбраны, алерт применяется ко всему выбранному рынку.</div>
            </section>

            <section id="alertStep2" class="alert-step-content">
                <div class="alert-section-title">Фильтры Market Screener</div>
                <div id="alertFilterList" class="alert-filter-list"></div>
                <button id="alertAddFilter" class="alert-filter-add" type="button">＋ Добавить фильтр</button>
                <div class="alert-filter-hint">Можно добавить несколько условий. Все добавленные условия должны выполняться одновременно.</div>
            </section>

            <section id="alertStep3" class="alert-step-content">
                <div class="alert-condition-grid">
                    <div class="alert-field">
                        <label for="alertRepeat">Повторять не чаще</label>
                        <select id="alertRepeat" class="alert-select">
                            <option value="once">Один раз при появлении сигнала</option>
                            <option value="60">1 минута</option>
                            <option value="300">5 минут</option>
                            <option value="900">15 минут</option>
                            <option value="1800">30 минут</option>
                            <option value="3600" selected>1 час</option>
                            <option value="14400">4 часа</option>
                            <option value="86400">1 день</option>
                        </select>
                    </div>
                    <div class="alert-field">
                        <label for="alertTimezone">Временная зона</label>
                        <select id="alertTimezone" class="alert-select">
                            <option value="local" selected>Локальное время</option>
                            <option value="utc">UTC</option>
                        </select>
                    </div>
                    <div class="alert-field">
                        <label for="alertActive">Статус</label>
                        <select id="alertActive" class="alert-select">
                            <option value="true" selected>Активен</option>
                            <option value="false">Выключен</option>
                        </select>
                    </div>
                </div>
                <div class="alert-section-title">Уведомления</div>
                <div class="alert-channel-grid">
                    <label class="alert-channel"><input id="alertChannelSite" type="checkbox" checked><span class="alert-channel-text"><b>На сайте</b><span>Показывать уведомление в интерфейсе Screener</span></span></label>
                    <label class="alert-channel"><input id="alertChannelDesktop" type="checkbox"><span class="alert-channel-text"><b>На компьютере</b><span>Browser/Desktop Notification при срабатывании</span></span></label>
                    <label class="alert-channel"><input id="alertChannelTelegram" type="checkbox"><span class="alert-channel-text"><b>Telegram</b><span>Отправлять уведомление в Telegram</span></span></label>
                </div>
                <div id="alertTelegramSettings" class="alert-telegram-settings" style="display:none">
                    <div class="alert-section-title">Telegram</div>
                    <div class="alert-field"><label for="alertTelegramDestination">Куда отправлять</label><select id="alertTelegramDestination" class="alert-select"></select></div>
                    <div class="alert-telegram-actions"><button id="alertTelegramTest" type="button" class="alert-small-button">Проверить Telegram</button><span id="alertTelegramStatus" class="alert-telegram-status"></span></div>
                    <div class="alert-filter-hint">Можно отправлять через Cloudflare → Telegram без хранения Bot Token в app173.</div>
                </div>
                <div style="height:14px"></div>
                <div class="alert-section-title">Проверка</div>
                <div id="alertSummary" class="alert-summary"></div>
                <div class="alert-summary-row"><span class="alert-summary-key">Звук</span><select id="alertSoundSelect" class="alert-select"></select><button id="alertSoundTest" type="button" class="alert-small-button">▶ Проверить</button></div>
            
            </section>

            <div id="alertSavedList" class="alert-saved-list"></div>
        </div>
        <div class="alert-footer">
            <div class="alert-footer-left">
                <button id="alertCancel" type="button">Отмена</button>
                <button id="alertDelete" class="danger" type="button" style="display:none">Удалить</button>
            </div>
            <div class="alert-footer-right">
                <button id="alertBack" type="button" style="display:none">← Назад</button>
                <button id="alertNext" class="primary" type="button">Далее →</button>
                <button id="alertSave" class="primary" type="button" style="display:none">Сохранить алерт</button>
            </div>
        </div>
    </div>
</div>

<div id="alertNotificationPanel" class="alert-notification-panel" aria-hidden="true">
    <div class="alert-notification-header">
        <span class="alert-header-icon">🔔</span>
        <span class="alert-notification-title">Алерты</span>
        <span id="alertNotificationUnread" class="alert-notification-unread" style="display:none">0</span>
        <div class="alert-notification-actions">
            <button id="alertNotificationCreate" type="button">＋ Создать</button>
            <button id="alertNotificationClose" type="button" title="Закрыть">×</button>
        </div>
    </div>
    <div class="alert-center-tabs">
        <button type="button" class="alert-center-tab active" data-alert-center-tab="alerts">Алерты</button>
        <button type="button" class="alert-center-tab" data-alert-center-tab="notifications">Уведомления</button>
    </div>
    <div id="alertCenterAlertsView" class="alert-center-view active">
        <div id="alertCenterSavedList" class="alert-saved-center-list"></div>
    </div>
    <div id="alertCenterNotificationsView" class="alert-center-view">
        <div id="alertNotificationList" class="alert-notification-list"></div>
        <div class="alert-notification-footer">
            <span id="alertNotificationFooterText" class="alert-notification-footer-text">Непрочитанных: 0</span>
            <div class="alert-notification-actions">
                <button id="alertNotificationMarkAll" type="button" title="Отметить всё прочитанным">✓</button>
                <button id="alertNotificationClear" class="alert-notification-create" type="button">Очистить</button>
            </div>
        </div>
    </div>
</div>

<div id="alertToastStack" class="alert-toast-stack" aria-live="polite" aria-atomic="false"></div>
<div id="templateOverlay" class="template-overlay" aria-hidden="true">
  <div class="template-modal" role="dialog" aria-modal="true" aria-labelledby="templateTitle">
    <div class="template-header"><span id="templateTitle" class="template-title">Новый обзор</span><button id="templateClose" class="template-close" type="button">×</button></div>
    <div class="template-body">
      <div class="template-section">
        <h4>Обзор</h4>
        <div class="template-grid">
          <div class="template-field"><label>Название</label><input id="templateName" maxlength="80" placeholder="Например: Агрессивные треугольники"></div>
          <div class="template-field template-timeframe-card">
            <div class="template-timeframe-head"><div class="template-market-card-title">Таймфрейм формации</div><label class="template-timeframe-strict"><input id="templateTimeframeStrict" type="checkbox"> Строго</label></div>
            <div id="templateTimeframeControls" class="template-timeframe-controls strict">
              <label id="templateTimeframeSingleWrap">Таймфрейм<input id="templateTimeframeSingle" list="templateTimeframeList" placeholder=""></label>
              <label id="templateTimeframeFromWrap">От<input id="templateTimeframeFrom" list="templateTimeframeList" placeholder=""></label>
              <label id="templateTimeframeToWrap">До<input id="templateTimeframeTo" list="templateTimeframeList" placeholder=""></label>
            </div>
            <datalist id="templateTimeframeList"><option value="1m"><option value="3m"><option value="5m"><option value="15m"><option value="30m"><option value="1h"><option value="2h"><option value="3h"><option value="4h"><option value="6h"><option value="8h"><option value="12h"><option value="1d"><option value="3d"><option value="1w"></datalist>
            <datalist id="templateMarketTimeframeList"></datalist>
            <div id="templateTimeframeHint" class="template-timeframe-hint">Можно выбрать, напечатать таймфрейм или менять его стрелками ↑/↓ и Enter.</div>
          </div>
        </div>
      </div>
      <div class="template-section">
        <h4>Фильтры рынка</h4>
        <div class="template-market-card-grid">
          <div class="template-market-card">
            <div class="template-market-card-head"><div class="template-market-card-title">Изменение, %</div><label class="template-market-strict"><input id="templateChangeStrict" type="checkbox"> Строго</label></div>
            <div class="template-market-range" id="templateChangeTfRange"><label>От<input id="templateChangeFrom" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label><label>До<input id="templateChangeTo" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range is-strict" id="templateChangeTfStrict"><label>TimeFrame<input id="templateChangeSingle" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range"><label>Мин<input id="templateChangeMin" type="number" step="0.1" placeholder=""></label><label>Макс<input id="templateChangeMax" type="number" step="0.1" placeholder=""></label></div>
          </div>
          <div class="template-market-card">
            <div class="template-market-card-head"><div class="template-market-card-title">Оборот, $</div><label class="template-market-strict"><input id="templateTurnoverStrict" type="checkbox"> Строго</label></div>
            <div class="template-market-range" id="templateTurnoverTfRange"><label>От<input id="templateTurnoverFrom" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label><label>До<input id="templateTurnoverTo" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range is-strict" id="templateTurnoverTfStrict"><label>TimeFrame<input id="templateTurnoverSingle" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range"><label>Мин<input id="templateTurnoverMin" type="number" step="1" placeholder=""></label><label>Макс<input id="templateTurnoverMax" type="number" step="1" placeholder=""></label></div>
          </div>
          <div class="template-market-card">
            <div class="template-market-card-head"><div class="template-market-card-title">NATR</div><label class="template-market-strict"><input id="templateNatrStrict" type="checkbox"> Строго</label></div>
            <div class="template-market-range" id="templateNatrTfRange"><label>От<input id="templateNatrFrom" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label><label>До<input id="templateNatrTo" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range is-strict" id="templateNatrTfStrict"><label>TimeFrame<input id="templateNatrSingle" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range"><label>Мин<input id="templateNatrMin" type="number" step="0.1" placeholder=""></label><label>Макс<input id="templateNatrMax" type="number" step="0.1" placeholder=""></label></div>
          </div>
          <div class="template-market-card">
            <div class="template-market-card-head"><div class="template-market-card-title">Сделки</div><label class="template-market-strict"><input id="templateTradesStrict" type="checkbox"> Строго</label></div>
            <div class="template-market-range" id="templateTradesTfRange"><label>От<input id="templateTradesFrom" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label><label>До<input id="templateTradesTo" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range is-strict" id="templateTradesTfStrict"><label>TimeFrame<input id="templateTradesSingle" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range"><label>Мин<input id="templateTradesMin" type="number" step="1" placeholder=""></label><label>Макс<input id="templateTradesMax" type="number" step="1" placeholder=""></label></div>
          </div>
          <div class="template-market-card">
            <div class="template-market-card-head"><div class="template-market-card-title">Корреляция к BTC</div><label class="template-market-strict"><input id="templateBtcCorrStrict" type="checkbox"> Строго</label></div>
            <div class="template-market-range" id="templateBtcCorrTfRange"><label>От<input id="templateBtcCorrFrom" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label><label>До<input id="templateBtcCorrTo" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range is-strict" id="templateBtcCorrTfStrict"><label>TimeFrame<input id="templateBtcCorrSingle" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range"><label>Мин<input id="templateBtcCorrMin" type="number" step="0.01" placeholder=""></label><label>Макс<input id="templateBtcCorrMax" type="number" step="0.01" placeholder=""></label></div>
          </div>
          <div class="template-market-card">
            <div class="template-market-card-head"><div class="template-market-card-title">Всплеск объёма, %</div><label class="template-market-strict"><input id="templateVolumeSpikeStrict" type="checkbox"> Строго</label></div>
            <div class="template-market-range" id="templateVolumeSpikeTfRange"><label>От<input id="templateVolumeSpikeFrom" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label><label>До<input id="templateVolumeSpikeTo" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range is-strict" id="templateVolumeSpikeTfStrict"><label>TimeFrame<input id="templateVolumeSpikeSingle" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range"><label>Мин<input id="templateVolumeSpikeMin" type="number" step="0.1" placeholder=""></label><label>Макс<input id="templateVolumeSpikeMax" type="number" step="0.1" placeholder=""></label></div>
          </div>
          <div class="template-market-card">
            <div class="template-market-card-head"><div class="template-market-card-title">Всплеск объёма, x</div><label class="template-market-strict"><input id="templateVolumeExpansionStrict" type="checkbox"> Строго</label></div>
            <div class="template-market-range" id="templateVolumeExpansionTfRange"><label>От<input id="templateVolumeExpansionFrom" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label><label>До<input id="templateVolumeExpansionTo" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range is-strict" id="templateVolumeExpansionTfStrict"><label>TimeFrame<input id="templateVolumeExpansionSingle" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range"><label>Мин<input id="templateVolumeExpansionMin" type="number" step="0.1" placeholder=""></label><label>Макс<input id="templateVolumeExpansionMax" type="number" step="0.1" placeholder=""></label></div>
            <div class="template-volume-expansion-inline"><label><span>Среднее</span><input id="templateVolumeExpansionBase" type="number" min="1" max="500" step="1" value="100"><em>свечей</em></label><label><span>Период всплеска</span><input id="templateVolumeExpansionGrowth" type="number" min="1" max="200" step="1" value="20"><em>свечей</em></label></div><div class="template-volume-direction-row"><label>Направление<select id="templateVolumeExpansionDirection"><option value="any">Любое</option><option value="up">Рост</option><option value="down">Падение</option></select></label></div>
          </div>
          <div class="template-market-card">
            <div class="template-market-card-head"><div class="template-market-card-title">Спред, %</div><label class="template-market-strict"><input id="templateSpreadStrict" type="checkbox"> Строго</label></div>
            <div class="template-market-range" id="templateSpreadTfRange"><label>От<input id="templateSpreadFrom" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label><label>До<input id="templateSpreadTo" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range is-strict" id="templateSpreadTfStrict"><label>TimeFrame<input id="templateSpreadSingle" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range"><label>Мин<input id="templateSpreadMin" type="number" step="0.001" placeholder=""></label><label>Макс<input id="templateSpreadMax" type="number" step="0.001" placeholder=""></label></div>
          </div>
          <div class="template-market-card">
            <div class="template-market-card-head"><div class="template-market-card-title">Фандинг, %</div><label class="template-market-strict"><input id="templateFundingStrict" type="checkbox"> Строго</label></div>
            <div class="template-market-range" id="templateFundingTfRange"><label>От<input id="templateFundingFrom" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label><label>До<input id="templateFundingTo" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range is-strict" id="templateFundingTfStrict"><label>TimeFrame<input id="templateFundingSingle" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range"><label>Мин<input id="templateFundingMin" type="number" step="0.001" placeholder=""></label><label>Макс<input id="templateFundingMax" type="number" step="0.001" placeholder=""></label></div>
          </div>
          <div class="template-market-card">
            <div class="template-market-card-head"><div class="template-market-card-title">Изм. OI, $</div><label class="template-market-strict"><input id="templateOiChangeStrict" type="checkbox"> Строго</label></div>
            <div class="template-market-range" id="templateOiChangeTfRange"><label>От<input id="templateOiChangeFrom" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label><label>До<input id="templateOiChangeTo" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range is-strict" id="templateOiChangeTfStrict"><label>TimeFrame<input id="templateOiChangeSingle" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range"><label>Мин<input id="templateOiChangeMin" type="number" step="1" placeholder=""></label><label>Макс<input id="templateOiChangeMax" type="number" step="1" placeholder=""></label></div>
          </div>
          <div class="template-market-card">
            <div class="template-market-card-head"><div class="template-market-card-title">Изм. OI, %</div><label class="template-market-strict"><input id="templateOiChangePctStrict" type="checkbox"> Строго</label></div>
            <div class="template-market-range" id="templateOiChangePctTfRange"><label>От<input id="templateOiChangePctFrom" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label><label>До<input id="templateOiChangePctTo" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range is-strict" id="templateOiChangePctTfStrict"><label>TimeFrame<input id="templateOiChangePctSingle" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range"><label>Мин<input id="templateOiChangePctMin" type="number" step="0.1" placeholder=""></label><label>Макс<input id="templateOiChangePctMax" type="number" step="0.1" placeholder=""></label></div>
            <div class="template-market-range template-oi-direction-row"><label>Направление<select id="templateOiChangePctDirection"><option value="any">Любое</option><option value="up">Рост</option><option value="down">Падение</option></select></label></div>
          </div>
          <div class="template-market-card">
            <div class="template-market-card-head"><div class="template-market-card-title">Δ оборота, $</div><label class="template-market-strict"><input id="templateDeltaVolumeStrict" type="checkbox"> Строго</label></div>
            <div class="template-market-range" id="templateDeltaVolumeTfRange"><label>От<input id="templateDeltaVolumeFrom" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label><label>До<input id="templateDeltaVolumeTo" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range is-strict" id="templateDeltaVolumeTfStrict"><label>TimeFrame<input id="templateDeltaVolumeSingle" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range"><label>Мин<input id="templateDeltaVolumeMin" type="number" step="1" placeholder=""></label><label>Макс<input id="templateDeltaVolumeMax" type="number" step="1" placeholder=""></label></div>
          </div>
          <div class="template-market-card">
            <div class="template-market-card-head"><div class="template-market-card-title">Цена</div><label class="template-market-strict"><input id="templatePriceStrict" type="checkbox"> Строго</label></div>
            <div class="template-market-range" id="templatePriceTfRange"><label>От<input id="templatePriceFrom" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label><label>До<input id="templatePriceTo" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range is-strict" id="templatePriceTfStrict"><label>TimeFrame<input id="templatePriceSingle" list="templateMarketTimeframeList" type="text" autocomplete="off" spellcheck="false"></label></div>
            <div class="template-market-range"><label>Мин<input id="templatePriceMin" type="number" step="0.00000001" placeholder=""></label><label>Макс<input id="templatePriceMax" type="number" step="0.00000001" placeholder=""></label></div>
          </div>
        </div>
        <div class="template-muted" style="margin-top:8px">Если «Строго» включён — используется один выбранный TimeFrame. Если выключен — используется диапазон «От — До». Пустые Мин/Макс отключают фильтр показателя.</div>
      </div>
      <div class="template-section">
        <h4>Структура рынка</h4>
        <div class="template-grid">
          <div class="template-field template-structure-search-choice">
            <label>Что искать в этом шаблоне</label>
            <div class="template-structure-search-options">
              <label><input id="templateSearchHorizontal" type="checkbox"> Горизонтальные уровни</label>
              <label><input id="templateSearchTrendline" type="checkbox"> Наклонные линии</label>
              <label><input id="templateSearchCascade" type="checkbox"> Каскад</label>
            </div>
            <div class="template-muted">Отмечено = участвует в поиске и определяет, какие монеты попадут в таблицу. Неотмеченные типы не являются условием отбора. Их отображение на графике остаётся отдельной функцией.</div>
          </div>
          <div class="template-field"><label>Формация</label><select id="templatePattern"><option value="">Без формации</option><option value="Triangle">Треугольник</option><option value="Wedge">Клин</option><option value="Channel">Канал</option><option value="Flag">Флаг</option><option value="Pennant">Пенант</option><option value="Double Top">Двойная вершина</option><option value="Double Bottom">Двойное дно</option><option value="Head and Shoulders">Голова и плечи</option><option value="Inverse Head and Shoulders">Перевёрнутая ГиП</option></select></div>
          <div class="template-field"><label>Тип</label><select id="templatePatternType"></select></div>
          <div class="template-field"><label>Минимум касаний</label><select id="templateTouches"><option value="0">Не учитывать</option><option value="2">≥ 2</option><option value="3">≥ 3</option><option value="4">≥ 4</option><option value="5">≥ 5</option></select></div>
          <div class="template-field"><label>Стадия</label><select id="templateStage"><option value="">Любая</option><option value="early">Начало формирования</option><option value="forming">Формируется</option><option value="middle">Середина формирования</option><option value="mature">Зрелая формация</option><option value="near_breakout">Почти пробой</option><option value="breakout">Пробой</option><option value="retest">Ретест</option><option value="confirmed_breakout">Подтверждённый пробой</option><option value="failed_breakout">Неудачный/ложный пробой</option></select></div>
          <div class="template-field"><label>Проторговка, минимум часов</label><input id="templateConsolidationMin" type="number" step="0.5" min="0" placeholder="например 6"></div>
          <div class="template-field"><label>Расстояние до пробоя, максимум %</label><input id="templateDistanceMax" type="number" step="0.01" min="0" placeholder="например 0.5"></div>
          <div class="template-field"><label>Уровень</label><select id="templateLevelType"><option value="">Любой</option><option value="long">Long уровень</option><option value="short">Short уровень</option></select></div>
          <div class="template-field"><label>Касания уровня</label><select id="templateLevelTouches"><option value="0">Не учитывать</option><option value="1">≥ 1</option><option value="2">≥ 2</option><option value="3">≥ 3</option><option value="4">≥ 4</option><option value="5">≥ 5</option></select></div>
          <div class="template-field"><label>Каскад уровней</label><select id="templateLevelCascade"><option value="">Не учитывать</option><option value="none">Нет</option><option value="exists">Есть</option></select></div>
          <div class="template-field"><label>Вершины каскада</label><select id="templateCascadeVertices"><option value="0">Не учитывать</option><option value="2">≥ 2</option><option value="3">≥ 3</option><option value="4">≥ 4</option><option value="5">≥ 5</option><option value="6">≥ 6</option></select></div>
          <div class="template-field"><label>Расстояние между вершинами, максимум %</label><input id="templateCascadeDistanceMax" type="number" step="0.01" min="0" placeholder="например 1.0"></div>
          <div class="template-field"><label>Уровни: минимальное расстояние между экстремумами, свечей</label><input id="templateHorizontalLevelSearchPeriod" type="number" min="1" step="1" placeholder="например 50"><label class="template-market-strict"><input id="templateHorizontalLevelSearchStrict" type="checkbox"> Строго</label></div>
          <div class="template-field"><label>Уровни: количество касаний</label><input id="templateHorizontalLevelTouches" type="number" min="2" step="1" placeholder="например 2"></div>
          <div class="template-field"><label>Уровни: погрешность касания, %</label><input id="templateHorizontalLevelTolerance" type="number" min="0" step="0.01" placeholder="например 0.1"></div>
          <div class="template-field"><label>Уровни: время жизни, часов</label><input id="templateHorizontalLevelLifetime" type="number" min="0" step="0.5" placeholder="пусто = без ограничения"></div>
          <div class="template-field"><label class="template-market-strict"><input id="templateHorizontalLevelNoCross" type="checkbox"> Уровень не пересекался</label><label class="template-market-strict"><input id="templateHorizontalLevelAllowMinorPierce" type="checkbox"> Допускать небольшой запил</label></div>
          <div class="template-field"><label>Трендовые линии: минимальное расстояние между экстремумами, свечей</label><input id="templateTrendlineSearchPeriod" type="number" min="1" step="1" placeholder="например 50"><label class="template-market-strict"><input id="templateTrendlineSearchStrict" type="checkbox"> Строго</label></div>
          <div class="template-field"><label>Трендовые линии: количество касаний</label><input id="templateTrendlineTouches" type="number" min="2" step="1" placeholder="например 2"></div>
          <div class="template-field"><label>Трендовые линии: погрешность касания, %</label><input id="templateTrendlineTolerance" type="number" min="0" step="0.01" placeholder="например 0.1"></div>
          <div class="template-field"><label>Трендовые линии: время жизни, часов</label><input id="templateTrendlineLifetime" type="number" min="0" step="0.5" placeholder="пусто = без ограничения"></div>
          <div class="template-field"><label class="template-market-strict"><input id="templateTrendlineNoCross" type="checkbox"> Наклонная не пересекалась</label><label class="template-market-strict"><input id="templateTrendlineAllowMinorPierce" type="checkbox"> Допускать небольшой запил</label></div>
        </div>
      </div>
      <div class="template-section">
        <h4>Отображение и сортировка</h4>
        <div class="template-grid">
          <div class="template-field"><label>Режим</label><select id="templateDisplay"><option value="table">Таблица</option><option value="grid4">Сетка · 4 графика</option><option value="grid6">Сетка · 6 графиков</option><option value="grid10">Сетка · 10 графиков</option><option value="grid20">Сетка · 20 графиков</option><option value="combined">Таблица + графики</option></select></div>
          <div class="template-field"><label>Показывать</label><select id="templateLimit"><option value="10">10</option><option value="20">20</option><option value="50">50</option><option value="100">100</option><option value="0">Все</option></select></div>
          <div class="template-field"><label>Сортировка 1</label><select id="templateSort1"><option value="">Без сортировки</option><option value="change_pct">Изменение</option><option value="volume_24h">Оборот</option><option value="natr">Volatility / NATR</option><option value="touches">Касания</option><option value="distance_to_breakout">Distance to Breakout</option><option value="formation_duration_hours">Formation Duration</option><option value="score">Pattern Score</option></select></div>
          <div class="template-field"><label>Направление</label><select id="templateSortDir"><option value="asc">По возрастанию ↑</option><option value="desc">По убыванию ↓</option></select></div>
        </div>
        <div class="template-muted" style="margin-top:8px">Шаблон хранит эти настройки независимо от других обзоров. Многоуровневую сортировку можно расширять без изменения Filter Engine.</div>
      </div>
    </div>
    <div class="template-footer">
      <button id="templateDelete" class="template-btn danger" type="button" style="display:none">Удалить</button>
      <button id="templateSaveAs" class="template-btn" type="button">Сохранить как новый</button>
      <button id="templateSave" class="template-btn primary" type="button">Сохранить</button>
    </div>
  </div>
</div>


<div id="volumeHistoryOverlay" class="template-overlay" aria-hidden="true">
  <div class="template-modal volume-history-modal" role="dialog" aria-modal="true" aria-labelledby="volumeHistoryTitle">
    <div class="template-header"><span id="volumeHistoryTitle" class="template-title">История · Всплеск объёма, x</span><button id="volumeHistoryClose" class="template-close" type="button">×</button></div>
    <div class="template-body">
      <div class="template-section">
        <h4>Поиск всплеска объёма</h4>
        <div class="template-grid">
          <div class="template-field"><label>Монета</label><input id="volumeHistorySymbol" placeholder="Пусто — весь рынок" autocomplete="off" spellcheck="false"></div>
          <div class="template-field"><label>TimeFrame</label><select id="volumeHistoryTimeframe"><option>1m</option><option selected>5m</option><option>15m</option><option>30m</option><option>1h</option><option>4h</option><option>1D</option></select></div>
          <div class="template-field"><label>Мин, x</label><input id="volumeHistoryMin" type="number" step="0.1" value="10"></div>
          <div class="template-field"><label>Макс, x</label><input id="volumeHistoryMax" type="number" step="0.1" placeholder="Без верхнего предела"></div>
          <div class="template-field"><label>Направление объёма</label><select id="volumeHistoryDirection"><option value="any">Любое</option><option value="up">Рост</option><option value="down">Падение</option></select></div>
          <div class="template-field"><label>Среднее, свечей</label><input id="volumeHistoryBase" type="number" min="1" max="500" step="1" value="100"></div>
          <div class="template-field"><label>Период всплеска, свечей</label><input id="volumeHistoryGrowth" type="number" min="1" max="200" step="1" value="20"></div>
          <div class="template-field"><label>Изм. цены, % · Мин</label><input id="volumeHistoryPriceMin" type="number" step="0.1" placeholder="Любое"></div>
          <div class="template-field"><label>Изм. цены, % · Макс</label><input id="volumeHistoryPriceMax" type="number" step="0.1" placeholder="Любое"></div>
          <div class="template-field"><label>Направление OI</label><select id="volumeHistoryOiDirection"><option value="any">Любое</option><option value="up">Рост</option><option value="down">Падение</option></select></div>
          <div class="template-field"><label>Изм. OI, % · Мин</label><input id="volumeHistoryOiMin" type="number" step="0.1" placeholder="Любое"></div>
          <div class="template-field"><label>Изм. OI, % · Макс</label><input id="volumeHistoryOiMax" type="number" step="0.1" placeholder="Любое"></div>
        </div>
        <div class="template-muted" style="margin-top:8px">Расчёт: средний объём последних N свечей / средний объём предыдущих M свечей. Направление определяется по сглаженному ряду внутри периода всплеска. Цена и OI проверяются за тот же период. Анализируются завершённые свечи Binance Futures.</div>
        <div class="template-actions" style="margin-top:10px"><button id="volumeHistorySearch" class="template-btn primary" type="button">Найти</button><span id="volumeHistoryStatus" class="template-search-status"></span></div>
      </div>
      <div class="template-section">
        <h4>Найденные события</h4>
        <div id="volumeHistoryResults" class="volume-history-results"><div class="template-muted">Поиск ещё не запускался.</div></div>
      </div>
    </div>
  </div>
</div>

<div id="settingsOverlay" class="settings-overlay" aria-hidden="true">
    <div class="settings-modal" role="dialog" aria-modal="true" aria-labelledby="settingsTitle">
        <div class="settings-header">
            <span id="settingsTitle">Настройки</span>
            <button id="settingsClose" class="settings-close" type="button" title="Закрыть">×</button>
        </div>
        <div class="settings-section" id="marketOverviewSection">
            <div class="settings-section-title" id="marketOverviewSectionTitle">Обзор рынка</div>
            <div class="settings-section-content market-overview-page">
                <div class="settings-group">
                    <h4>Выберите столбцы</h4>
                    <div class="column-grid broker-checkboxes">
                        <label><input class="market-column-check" data-column="symbol" type="checkbox" checked> Название</label>
                        <label><input class="market-column-check" data-column="trades" type="checkbox" checked> Сделки</label>
                        <label><input class="market-column-check" data-column="volume_24h" type="checkbox" checked> Оборот, $</label>
                        <label><input class="market-column-check" data-column="change_pct" type="checkbox" checked> Изм. цены, %</label>
                        <label><input class="market-column-check" data-column="natr" type="checkbox" checked> NATR</label>
                        <label><input class="market-column-check" data-column="btc_corr" type="checkbox" checked> Корреляция к BTC</label>
                        <label><input class="market-column-check" data-column="volume_spike" type="checkbox" checked> Всплеск объёма, %</label>
                        <label><input class="market-column-check" data-column="spread_pct" type="checkbox"> Спред, %</label>
                        <label><input class="market-column-check" data-column="funding_pct" type="checkbox"> Фандинг, %</label>
                        <label><input class="market-column-check" data-column="oi_change_usd" type="checkbox"> Изм. OI, $</label>
                        <label><input class="market-column-check" data-column="delta_volume_usd" type="checkbox"> Δ оборота, $</label>
                        <label><input class="market-column-check" data-column="price" type="checkbox"> Цена</label>
                    </div>
                </div>

                <div class="settings-group">
                    <h4>Настройки колонок</h4>
                    <div class="broker-card-grid">
                        <div class="broker-card" data-market-card="trades"><b>Сделки</b><label>TimeFrame<select data-market-field="timeframe" data-column="trades"><option>1m</option><option>5m</option><option>15m</option><option>30m</option><option>1h</option><option>4h</option><option>1D</option></select></label><label>Мин<input data-market-field="min" data-column="trades" type="text"></label><label>Макс<input data-market-field="max" data-column="trades" type="text"></label></div>
                        <div class="broker-card" data-market-card="volume_24h"><b>Оборот, $</b><label>TimeFrame<select data-market-field="timeframe" data-column="volume_24h"><option>1m</option><option>5m</option><option>15m</option><option>30m</option><option>1h</option><option>4h</option><option>1D</option></select></label><label>Мин<input data-market-field="min" data-column="volume_24h" type="text"></label><label>Макс<input data-market-field="max" data-column="volume_24h" type="text"></label></div>
                        <div class="broker-card" data-market-card="change_pct"><b>Изм. цены, %</b><label>TimeFrame<select data-market-field="timeframe" data-column="change_pct"><option>1m</option><option>5m</option><option>15m</option><option>30m</option><option>1h</option><option>4h</option><option>1D</option></select></label><label>Мин<input data-market-field="min" data-column="change_pct" type="text"></label><label>Макс<input data-market-field="max" data-column="change_pct" type="text"></label></div>
                        <div class="broker-card" data-market-card="natr"><b>NATR</b><label>TimeFrame<select data-market-field="timeframe" data-column="natr"><option>1m</option><option>5m</option><option>15m</option><option>30m</option><option>1h</option><option>4h</option><option>1D</option></select></label><label>Мин<input data-market-field="min" data-column="natr" type="text"></label><label>Макс<input data-market-field="max" data-column="natr" type="text"></label></div>
                        <div class="broker-card" data-market-card="symbol"><b>Название</b><label>TimeFrame<select data-market-field="timeframe" data-column="symbol"><option>1m</option><option>5m</option><option>15m</option><option>30m</option><option>1h</option><option>4h</option><option>1D</option></select></label><label>Мин<input data-market-field="min" data-column="symbol" type="text"></label><label>Макс<input data-market-field="max" data-column="symbol" type="text"></label></div>
                        <div class="broker-card" data-market-card="btc_corr"><b>Корреляция к BTC</b><label>TimeFrame<select data-market-field="timeframe" data-column="btc_corr"><option>1m</option><option>5m</option><option>15m</option><option>30m</option><option>1h</option><option>4h</option><option>1D</option></select></label><label>Мин<input data-market-field="min" data-column="btc_corr" type="text"></label><label>Макс<input data-market-field="max" data-column="btc_corr" type="text"></label></div>
                        <div class="broker-card" data-market-card="volume_spike"><b>Всплеск объёма, %</b><label>TimeFrame<select data-market-field="timeframe" data-column="volume_spike"><option>1m</option><option>5m</option><option>15m</option><option>30m</option><option>1h</option><option>4h</option><option>1D</option></select></label><label>Мин<input data-market-field="min" data-column="volume_spike" type="text"></label><label>Макс<input data-market-field="max" data-column="volume_spike" type="text"></label></div>
                        <div class="broker-card" data-market-card="spread_pct"><b>Спред, %</b><label>TimeFrame<select data-market-field="timeframe" data-column="spread_pct"><option>1m</option><option>5m</option><option>15m</option><option>30m</option><option>1h</option><option>4h</option><option>1D</option></select></label><label>Мин<input data-market-field="min" data-column="spread_pct" type="text"></label><label>Макс<input data-market-field="max" data-column="spread_pct" type="text"></label></div>
                        <div class="broker-card" data-market-card="funding_pct"><b>Фандинг, %</b><label>TimeFrame<select data-market-field="timeframe" data-column="funding_pct"><option>1m</option><option>5m</option><option>15m</option><option>30m</option><option>1h</option><option>4h</option><option>1D</option></select></label><label>Мин<input data-market-field="min" data-column="funding_pct" type="text"></label><label>Макс<input data-market-field="max" data-column="funding_pct" type="text"></label></div>
                        <div class="broker-card" data-market-card="oi_change_usd"><b>Изм. OI, $</b><label>TimeFrame<select data-market-field="timeframe" data-column="oi_change_usd"><option>1m</option><option>5m</option><option>15m</option><option>30m</option><option>1h</option><option>4h</option><option>1D</option></select></label><label>Мин<input data-market-field="min" data-column="oi_change_usd" type="text"></label><label>Макс<input data-market-field="max" data-column="oi_change_usd" type="text"></label></div>
                        <div class="broker-card" data-market-card="delta_volume_usd"><b>Δ оборота, $</b><label>TimeFrame<select data-market-field="timeframe" data-column="delta_volume_usd"><option>1m</option><option>5m</option><option>15m</option><option>30m</option><option>1h</option><option>4h</option><option>1D</option></select></label><label>Мин<input data-market-field="min" data-column="delta_volume_usd" type="text"></label><label>Макс<input data-market-field="max" data-column="delta_volume_usd" type="text"></label></div>
                        <div class="broker-card" data-market-card="price"><b>Цена</b><label>TimeFrame<select data-market-field="timeframe" data-column="price"><option>1m</option><option>5m</option><option>15m</option><option>30m</option><option>1h</option><option>4h</option><option>1D</option></select></label><label>Мин<input data-market-field="min" data-column="price" type="text"></label><label>Макс<input data-market-field="max" data-column="price" type="text"></label></div>
                    </div>
                </div>

                <div class="settings-group toggles">
                    <label><input id="marketIgnoreSign" type="checkbox"> Игнорировать знак (+/-)</label>
                    <label><input id="marketLinkActive" type="checkbox"> Линковка с активным окном</label>
                    <label><input id="marketFreeze" type="checkbox"> Замораживать</label>
                    <label><input id="marketFreezeAll" type="checkbox"> Замораживать все графики</label>
                </div>

                <div class="settings-actions">
                    <button id="marketResetButton" type="button">Сбросить</button>
                    <button id="marketApplyButton" type="button">Применить</button>
                </div>
            </div>
        </div>



<div class="settings-section" id="screenerEngineSection">
    <div class="settings-section-title" id="screenerEngineSectionTitle">Screener Engine</div>
    <div class="settings-section-content">
        <div class="settings-group">
            <div style="display:flex;flex-direction:column;gap:8px;max-width:520px">
                <label><input type="radio" name="screenerEngineMode" value="local"> Local — расчёты на этом компьютере</label>
                <label><input type="radio" name="screenerEngineMode" value="render"> Render — Screener Worker на Render</label>
                <label><input type="radio" name="screenerEngineMode" value="auto"> Auto — Render при доступности, иначе Local</label>
                <label style="display:flex;align-items:center;gap:8px">Render URL <input id="screenerRenderUrl" type="url" placeholder="https://your-worker.onrender.com" style="flex:1;min-width:240px"></label>
                <div id="screenerEngineStatus" class="settings-hint">Проверка состояния Engine…</div>
            </div>
        </div>
    </div>
</div>

<div class="settings-section" id="densitySettingsSection">
    <div class="settings-section-title" id="densitySettingsSectionTitle">Карта плотностей</div>
    <div class="settings-section-content">
        <div class="settings-group density-settings-form">
            <div class="density-setting-row density-setting-main"><span>Показывать карту плотностей</span><input id="densitySettingEnabled" type="checkbox"></div>

            <div class="density-setting-block">
                <div class="density-setting-heading">Фильтр интереса</div>
                <div class="density-setting-row"><span>Фильтровать по интересу</span><input id="densitySettingInterestFilter" type="checkbox"></div>
                <div class="density-setting-row"><span>Фильтровать все монеты <b class="density-help" title="Архитектура предусмотрена для полного покрытия. Сейчас этот режим не расширяет тяжёлые подписки и не увеличивает нагрузку автоматически.">?</b></span><input id="densitySettingFilterAll" type="checkbox"></div>
                <label class="density-setting-field"><span>Мин. оборот для интереса, $</span><input id="densitySettingInterestVolume" type="number" min="0" step="100000"></label>
            </div>

            <div class="density-setting-block">
                <div class="density-setting-heading">Тип рынка</div>
                <div class="density-setting-row"><span>Спот</span><input id="densitySettingSpot" type="checkbox"></div>
                <div class="density-setting-row"><span>Фьючерс</span><input id="densitySettingFutures" type="checkbox"></div>
            </div>

            <div class="density-setting-block">
                <div class="density-setting-heading">Минимальный размер плотности, $</div>
                <input id="densitySettingMinUsd" class="density-setting-input" type="number" min="0" step="50000">
            </div>

            <div class="density-setting-block">
                <div class="density-setting-heading">Расстояние от спреда</div>
                <label class="density-setting-field"><span>Минимальное расстояние, %</span><input id="densitySettingMinDistance" type="number" min="0" step="0.01"></label>
                <label class="density-setting-field"><span>Максимальное расстояние, %</span><input id="densitySettingMaxDistance" type="number" min="0" step="0.1"></label>
            </div>

            <div class="density-setting-block">
                <div class="density-setting-heading">Минимальная сила плотности <b class="density-help" title="Сила плотности — оценка от 0 до 100, учитывающая размер, время жизни, остаток плотности и близость к спреду.">?</b></div>
                <input id="densitySettingStrengthMin" class="density-setting-input" type="number" min="0" max="100" step="1">
            </div>

            <div class="density-setting-block">
                <div class="density-setting-heading">Отображение</div>
                <div class="density-setting-row"><span>На графике</span><input id="densitySettingChart" type="checkbox"></div>
                <div class="density-setting-row"><span>В скринере</span><input id="densitySettingScreener" type="checkbox"></div>
            </div>

            <div class="density-setting-block">
                <div class="density-setting-heading">Информация на плашке</div>
                <div class="density-setting-row"><span>Показывать, сколько осталось</span><input id="densitySettingRemaining" type="checkbox"></div>
                <div class="density-setting-row"><span>Показывать, сколько разъели</span><input id="densitySettingConsumed" type="checkbox"></div>
                <div class="density-setting-row"><span>Показывать время жизни</span><input id="densitySettingLifetime" type="checkbox"></div>
            </div>

            <div class="density-setting-block">
                <div class="density-setting-heading">Биржи</div>
                <div class="density-setting-row"><span>Binance</span><input id="densitySettingBinance" type="checkbox"></div>
                <div class="density-setting-row"><span>Bybit</span><input id="densitySettingBybit" type="checkbox"></div>
                <div class="density-setting-row"><span>OKX</span><input id="densitySettingOkx" type="checkbox"></div>
            </div>

            <div class="density-setting-block density-blacklist-block">
                <div class="density-setting-heading">Чёрный список монет</div>
                <textarea id="densitySettingBlacklist" rows="5" placeholder="BTCUSDT, ETHUSDT, SOLUSDT
DOGEUSDT"></textarea>
            </div>

            <div class="settings-actions">
                <button id="densitySettingsSave" type="button">Применить плотности</button>
                <span id="densitySettingsStatus" class="density-settings-status"></span>
            </div>
        </div>
    </div>
</div>

<div class="settings-section" id="formationTrendSection">
    <div class="settings-section-title" id="formationTrendSectionTitle">Формации</div>
    <div class="settings-section-content">
        <div class="settings-group">
            <h4>Направление трендовых линий</h4>
            <label style="display:flex;align-items:center;gap:8px;max-width:280px">
                <select id="formationTrendDirection" style="width:100%">
                    <option value="rising">Только растущие</option>
                    <option value="falling">Только падающие</option>
                    <option value="all">Все</option>
                </select>
            </label>
            <div class="settings-hint">Анализируются оба направления. Параметр определяет только отображение линий на графике.</div>
        </div>

        <div class="settings-group">
            <h4>Настройки уровней и трендовых линий</h4>
            <div class="template-grid">
                <div class="template-field"><label>Уровни: минимальное расстояние между экстремумами, свечей</label><input id="formationSettingsHorizontalLevelSearchPeriod" type="number" min="1" step="1" placeholder="например 50"><label class="template-market-strict"><input id="formationSettingsHorizontalLevelSearchStrict" type="checkbox"> Строго</label></div>
                <div class="template-field"><label>Уровни: количество касаний</label><input id="formationSettingsHorizontalLevelTouches" type="number" min="2" step="1" placeholder="например 2"></div>
                <div class="template-field"><label>Уровни: погрешность касания, %</label><input id="formationSettingsHorizontalLevelTolerance" type="number" min="0" step="0.01" placeholder="например 0.1"></div>
                <div class="template-field"><label>Уровни: время жизни, часов</label><input id="formationSettingsHorizontalLevelLifetime" type="number" min="0" step="0.5" placeholder="пусто = без ограничения"></div>
                <div class="template-field"><label class="template-market-strict"><input id="formationSettingsHorizontalLevelNoCross" type="checkbox"> Уровень не пересекался</label><label class="template-market-strict"><input id="formationSettingsHorizontalLevelAllowMinorPierce" type="checkbox"> Допускать небольшой запил</label></div>
                <div class="template-field"><label>Трендовые линии: минимальное расстояние между экстремумами, свечей</label><input id="formationSettingsTrendlineSearchPeriod" type="number" min="1" step="1" placeholder="например 50"><label class="template-market-strict"><input id="formationSettingsTrendlineSearchStrict" type="checkbox"> Строго</label></div>
                <div class="template-field"><label>Трендовые линии: количество касаний</label><input id="formationSettingsTrendlineTouches" type="number" min="2" step="1" placeholder="например 2"></div>
                <div class="template-field"><label>Трендовые линии: погрешность касания, %</label><input id="formationSettingsTrendlineTolerance" type="number" min="0" step="0.01" placeholder="например 0.1"></div>
                <div class="template-field"><label>Трендовые линии: время жизни, часов</label><input id="formationSettingsTrendlineLifetime" type="number" min="0" step="0.5" placeholder="пусто = без ограничения"></div>
                <div class="template-field"><label class="template-market-strict"><input id="formationSettingsTrendlineNoCross" type="checkbox"> Наклонная не пересекалась</label><label class="template-market-strict"><input id="formationSettingsTrendlineAllowMinorPierce" type="checkbox"> Допускать небольшой запил</label></div>
            </div>
            <div class="settings-hint">Параметры пока только сохраняются в настройках активного шаблона и подготовлены для будущего Formation Engine. «Минимальное расстояние» задаётся как значение «от», а «Строго» определяет, нужно ли соблюдать этот минимум буквально. Автоматическое построение линий и формаций ими пока не управляется.</div>
        </div>

        <div class="settings-group">
            <h4>Поиск и фильтрация формаций</h4>
            <div class="template-grid">
                <div class="template-field"><label>Таймфрейм формации</label><select id="formationSettingsTimeframeMode"><option value="all">Все</option><option value="only">Только</option><option value="below">Ниже</option><option value="above">Выше</option><option value="range">Диапазон</option></select></div>
                <div class="template-field" id="formationSettingsTimeframeFromWrap"><label>От</label><select id="formationSettingsTimeframeFrom"></select></div>
                <div class="template-field" id="formationSettingsTimeframeToWrap"><label>До</label><select id="formationSettingsTimeframeTo"></select></div>
                <div class="template-field"><label>Формация</label><select id="formationSettingsPattern"><option value="">Без формации</option><option value="Triangle">Треугольник</option><option value="Wedge">Клин</option><option value="Channel">Канал</option><option value="Flag">Флаг</option><option value="Pennant">Пенант</option><option value="Double Top">Двойная вершина</option><option value="Double Bottom">Двойное дно</option><option value="Head and Shoulders">Голова и плечи</option><option value="Inverse Head and Shoulders">Перевёрнутая ГиП</option></select></div>
                <div class="template-field"><label>Тип</label><select id="formationSettingsPatternType"></select></div>
                <div class="template-field"><label>Минимум касаний</label><select id="formationSettingsTouches"><option value="0">Не учитывать</option><option value="2">≥ 2</option><option value="3">≥ 3</option><option value="4">≥ 4</option><option value="5">≥ 5</option></select></div>
                <div class="template-field"><label>Стадия</label><select id="formationSettingsStage"><option value="">Любая</option><option value="early">Начало формирования</option><option value="forming">Формируется</option><option value="middle">Середина формирования</option><option value="mature">Зрелая формация</option><option value="near_breakout">Почти пробой</option><option value="breakout">Пробой</option><option value="retest">Ретест</option><option value="confirmed_breakout">Подтверждённый пробой</option><option value="failed_breakout">Неудачный/ложный пробой</option></select></div>
                <div class="template-field"><label>Проторговка, минимум часов</label><input id="formationSettingsConsolidationMin" type="number" step="0.5" min="0" placeholder="например 6"></div>
                <div class="template-field"><label>Расстояние до пробоя, максимум %</label><input id="formationSettingsDistanceMax" type="number" step="0.01" min="0" placeholder="например 0.5"></div>
                <div class="template-field"><label>Уровень</label><select id="formationSettingsLevelType"><option value="">Любой</option><option value="long">Long уровень</option><option value="short">Short уровень</option></select></div>
                <div class="template-field"><label>Касания уровня</label><select id="formationSettingsLevelTouches"><option value="0">Не учитывать</option><option value="1">≥ 1</option><option value="2">≥ 2</option><option value="3">≥ 3</option><option value="4">≥ 4</option><option value="5">≥ 5</option></select></div>
                <div class="template-field"><label>Каскад уровней</label><select id="formationSettingsLevelCascade"><option value="">Не учитывать</option><option value="none">Нет</option><option value="exists">Есть</option></select></div>
                <div class="template-field"><label>Вершины каскада</label><select id="formationSettingsCascadeVertices"><option value="0">Не учитывать</option><option value="2">≥ 2</option><option value="3">≥ 3</option><option value="4">≥ 4</option><option value="5">≥ 5</option><option value="6">≥ 6</option></select></div>
                <div class="template-field"><label>Расстояние между вершинами, максимум %</label><input id="formationSettingsCascadeDistanceMax" type="number" step="0.01" min="0" placeholder="например 1.0"></div>
            </div>
            <div class="settings-hint">Эти параметры управляют структурными фильтрами активного обзора. Таймфрейм поддерживает выбор одного, диапазона, всех TF, либо TF ниже/выше заданной границы.</div>
            <div class="settings-actions">
                <button id="formationSettingsReset" type="button">Сбросить</button>
                <button id="formationSettingsApply" type="button">Применить</button>
            </div>
        </div>
    </div>
</div>

<div class="settings-section" id="terminalConnectionSection">
    <div class="settings-section-title" id="terminalConnectionSectionTitle">Подключение</div>
    <div class="settings-section-content">
        <div class="terminal-connection">
            <div class="terminal-connection-row">
                <div class="terminal-connection-field">
                    <label for="terminalType">Терминал</label>
                    <select id="terminalType">
                        <option value="tiger">Tiger</option>
                    </select>
                </div>
                <div class="terminal-connection-field">
                    <label for="terminalHost">Локальный сервер сигналов</label>
                    <input id="terminalHost" type="text" value="127.0.0.1" autocomplete="off" spellcheck="false">
                </div>
                <div class="terminal-connection-field">
                    <label for="terminalPort">Порт</label>
                    <input id="terminalPort" type="number" min="1" max="65535" value="7819">
                </div>
            </div>
            <div class="terminal-connection-actions">
                <button id="terminalConnectButton" type="button" class="terminal-connection-button primary">Подключиться</button>
                <button id="terminalDisconnectButton" type="button" class="terminal-connection-button">Отключиться</button>
                <span class="terminal-connection-status"><span id="terminalConnectionStatusDot" class="terminal-connection-status-dot"></span><span id="terminalConnectionStatusText">Отключено</span></span>
            </div>
            <div class="terminal-connection-hint">После подключения цветные кружки графика используются как группы линковки Tiger: жёлтый — A, красный — B, зелёный — C, синий — D, фиолетовый — E, серый — F.</div>
<div id="telegramConnectionsSection" style="margin-top:14px"><div class="notification-route-name" style="margin-bottom:8px">Telegram</div>
        <div id="telegramConnectionsList" class="telegram-connections"></div>
        <div class="telegram-add-grid" style="margin-top:10px">
            <label>Название<input id="telegramNewName" class="telegram-input" type="text" placeholder="Структура"></label>
            <label>Bot Token<input id="telegramNewToken" class="telegram-input" type="password" autocomplete="off" placeholder="123456:ABC..."></label>
            <label>Chat ID<input id="telegramNewChatId" class="telegram-input" type="text" autocomplete="off" placeholder="-100..."></label>
        </div>
        <div class="telegram-connection-actions">
            <button id="telegramAddButton" type="button" class="telegram-small-button">＋ Добавить бота</button>
            <span id="telegramConnectionsStatus" class="terminal-connection-status"></span>
        </div>
        <div class="terminal-connection-hint">Можно добавить несколько ботов/чатов и назначать разные уведомления на разные Telegram-подключения.</div>
    </div></div>
        </div>
    </div>

<div class="settings-section" id="notificationRoutingSection">
    <div class="settings-section-title" id="notificationRoutingSectionTitle">Уведомления</div>
    <div class="settings-section-content">
        <div id="notificationRoutingList" class="notification-routes"></div>
        <div class="terminal-connection-hint">Настройки применяются отдельно к листингам, пересечениям сигнальных уровней и каждому сохранённому кастомному алерту.</div>
    </div>
</div>

<div class="settings-section" id="chartGridSection">
    <div class="settings-section-title" id="chartGridSectionTitle">Сетка</div>
    <div class="settings-section-content">
        <div class="grid-layout-settings">
            <div class="grid-layout-hint">Наведите на область, чтобы выбрать количество и расположение графиков.</div>
            <div id="chartGridPicker" class="grid-layout-picker" role="grid" aria-label="Раскладка графиков"></div>
            <div id="chartGridValue" class="grid-layout-value">1 × 1 · 1 график</div>
        </div>
    </div>
</div>

<div class="settings-section" id="notificationSoundsSection">
    <div class="settings-section-title" id="notificationSoundsSectionTitle">Звуки уведомлений</div>
    <div class="settings-section-content">
        <div id="notificationSoundsList" class="notification-sounds-list"></div>
        <div class="notification-sound-upload">
            <button id="notificationSoundUploadButton" type="button" class="notification-sound-button">＋ Добавить свой звук</button>
            <input id="notificationSoundFile" type="file" accept="audio/*" style="display:none">
        </div>
        <div class="notification-sound-hint">Громкость по умолчанию 120%. Можно назначить отдельный звук каждому алерту. Свои WAV/MP3/OGG сохраняются локально в браузере.</div>
    </div>
</div>

<div class="settings-section" id="hotkeysSection">
            <div class="settings-section-title" id="hotkeysSectionTitle">Горячие клавиши</div>
            <div class="settings-section-content">
                <table class="hotkeys-table">
                    <thead>
                        <tr><th>Клавиша</th><th>Инструмент</th><th>Пояснение</th></tr>
                    </thead>
                    <tbody id="hotkeysTable"></tbody>
                </table>
                <div class="settings-hint">Нажмите на клавишу в таблице, затем нажмите новую клавишу. Настройки сохраняются автоматически. Для отмены назначения нажмите Esc.</div>
            </div>
        </div>
    </div>
</div>

<div class="toolbar">
    <div class="info">
        Монет: <span id="coinCount">0</span>
    </div>
    <div class="info">
        Обновление: <span id="lastUpdate">—</span>
    </div>

    <div class="market-template-dock">
        <div class="panel-header market-template-header">
            <div class="market-template-search-wrap">
                <input id="marketTemplateSearch" class="market-template-search" type="text" autocomplete="off" spellcheck="false" placeholder="Поиск шаблона…" title="Поиск сохранённых обзоров">
                <button id="marketTemplateButton" class="market-template-search-button" type="button" title="Показать сохранённые обзоры">⌄</button>
            </div>
            <span id="activeTemplateLabel" class="muted"></span>
            <button id="volumeHistoryButton" class="template-btn" type="button" title="Поиск всплесков объёма на истории">История</button>
        </div>
        <div id="marketTemplateMenu" class="market-template-menu" aria-hidden="true"></div>
        <div id="templateSearchStatus" class="template-search-status" aria-live="polite"></div>
    </div>
</div>

<div id="errorBox" class="error-box"></div>

<div class="layout">

<div id="chartModal" class="chart-modal persistent-chart">
    <div class="chart-box">
        <div class="chart-header">
            <span id="chartTitleDot" class="chart-grid-title-dot"></span><span id="chartTitle">Chart</span>
            <div class="chart-intervals" id="chartIntervals">
                <button class="chart-interval active" data-interval="1" type="button">1m</button>
                <button class="chart-interval" data-interval="5" type="button">5m</button>
                <button class="chart-interval" data-interval="15" type="button">15m</button>
                <button class="chart-interval" data-interval="60" type="button">1h</button>
                <button class="chart-interval" data-interval="240" type="button">4h</button>
                <button class="chart-interval" data-interval="1D" type="button">1D</button>
            </div>
            <div class="chart-header-actions">
                <div id="chartFlagWrap" class="chart-flag-wrap"><button id="chartFlag" class="chart-flag" type="button" title="Флажок" aria-label="Флажок"><svg viewBox="0 0 24 24" aria-hidden="true"><path class="flag-pole" d="M6 4v17"/><path class="flag-cloth" d="M7 5h11v12l-5.5-4-5.5 4V5z"/></svg></button><div id="chartFlagPalette" class="chart-flag-palette" hidden></div></div>
                <button id="chartFormationToggle" class="chart-overlay-toggle" type="button" title="Показать/скрыть формации">Формации</button>
                <button id="chartLevelToggle" class="chart-overlay-toggle" type="button" title="Показать/скрыть уровни">Уровни</button>
                <button id="chartDensityToggle" class="chart-overlay-toggle" type="button" title="Показать/скрыть плотности">Плотности</button>
                <button id="chartBack" class="chart-back" type="button" title="Назад">Назад</button>
            </div>
            <button id="chartClose" class="chart-close" type="button">×</button>
        </div>
        <div id="chartGridWorkspace" class="chart-grid-workspace"></div>
        <div id="chartGridPagination" class="chart-grid-pagination" aria-label="Переключение наборов графиков">
            <button id="chartGridPrev" type="button" title="Предыдущий набор" aria-label="Предыдущий набор">‹</button>
            <span id="chartGridPaginationInfo" class="chart-grid-pagination-info">—</span>
            <button id="chartGridNext" type="button" title="Следующий набор" aria-label="Следующий набор">›</button>
        </div>
        <div class="chart-content" id="chartContent">
            <div class="drawing-toolbar" aria-label="Инструменты графика">
                <button class="drawing-tool" type="button" title="Сигнальный уровень" data-tool="signal">
                    <svg viewBox="0 0 24 24"><path d="M7 18h10"/><path d="M9 15h6"/><path d="M8 10a4 4 0 0 1 8 0c0 3 1.5 3.5 1.5 5H6.5c0-1.5 1.5-2 1.5-5Z"/><path d="M10 20h4"/></svg>
                </button>
                <div id="chartColorPalette" class="chart-grid-color-palette" aria-label="Цвет графика"></div>
                <button class="drawing-tool" type="button" title="Наклонная линия" data-tool="trend">
                    <svg viewBox="0 0 24 24"><path d="M5 19 19 5"/><path d="M5 19h4"/><path d="M19 5v4"/></svg>
                </button>
                <button class="drawing-tool" type="button" title="Луч" data-tool="ray">
                    <svg viewBox="0 0 24 24"><path d="M5 19 19 5"/><path d="M14 5h5v5"/></svg>
                </button>
                <button class="drawing-tool" type="button" title="Горизонтальный уровень" data-tool="horizontal">
                    <svg viewBox="0 0 24 24"><path d="M4 12h16"/><path d="M7 9v6"/><path d="M17 9v6"/></svg>
                </button>
                <button class="drawing-tool" type="button" title="Линейка" data-tool="ruler">
                    <svg viewBox="0 0 24 24"><path d="M4 17 17 4l3 3L7 20l-3-3Z"/><path d="m9 15 2 2"/><path d="m12 12 2 2"/><path d="m15 9 2 2"/></svg>
                </button>
                <button class="drawing-tool" type="button" title="Прямоугольник" data-tool="rectangle">
                    <svg viewBox="0 0 24 24"><rect x="5" y="6" width="14" height="12" rx="1"/></svg>
                </button>
                <button class="drawing-tool" type="button" title="Рисование" data-tool="draw">
                    <svg viewBox="0 0 24 24"><path d="m5 17 1-4 9-9 3 3-9 9-4 1Z"/><path d="m14 5 3 3"/><path d="M5 20h14"/></svg>
                </button>
                <span class="drawing-toolbar-separator"></span>
                <button class="drawing-tool position-tool" type="button" title="Long Position — расчёт риска и потенциальной прибыли" data-tool="longPosition">
                    <span class="position-glyph">L</span>
                </button>
                <button class="drawing-tool position-tool" type="button" title="Short Position — расчёт риска и потенциальной прибыли" data-tool="shortPosition">
                    <span class="position-glyph">S</span>
                </button>
            </div>
            <div id="chartHoverTooltip" class="chart-hover-tooltip"></div>
            <canvas id="drawingCanvas" class="drawing-canvas"></canvas>
            <div id="drawingStatus" class="drawing-status"></div>
            <div class="own-chart">
                <div id="priceChart" class="own-chart-main"><div class="chart-indicator-label volume-label">Volume</div><div id="chartLoadingIndicator" class="chart-loading-indicator">Данные загружаются…</div></div>
                <div id="oiChart" class="own-chart-oi"></div>
            </div>
        </div>
    </div>
</div>




    <div id="workspaceDivider" class="workspace-divider" role="separator"
         aria-orientation="vertical" aria-label="Изменить ширину графика и маркет скринера"></div>

    <div class="panel market-panel" style="position:relative">
        <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>COIN</th>
                        <th class="right">PRICE</th>
                        <th class="right">24H %</th>
                        <th class="right">VOLUME 24H</th>
                        <th class="right">BID</th>
                        <th class="right">ASK</th>
                        <th>EXCHANGE</th>
                    </tr>
                </thead>
                <tbody id="coinTable"></tbody>
            </table>
        </div>

    <section id="densityWorkspace" class="density-workspace" hidden aria-label="Карта плотностей">
        <div id="densityWorkspaceResize" class="density-workspace-resize" role="separator" aria-orientation="horizontal" aria-label="Изменить высоту карты плотностей"></div>
        <div class="density-workspace-head">
            <div class="density-workspace-title"><span class="density-workspace-dot"></span><span>Карта плотностей</span><span id="densityMapStatus" class="density-map-status empty"><span class="density-map-status-dot"></span><span class="density-map-status-text">Ожидание</span></span></div>
            <div class="density-workspace-scale" id="densityDistanceScale">—</div>
        </div>
        <div id="densityMapPanel" class="density-map-panel">
            <div id="densityMapList" class="density-map-list"></div>
        </div>
        <div id="densityExchangeHealth" class="density-exchange-health" hidden>
            <div class="density-map-head"><span>Exchange Data Health</span><span id="densityExchangeHealthStatus">—</span></div>
        </div>
        <div id="densityExchangeRadar" class="density-exchange-radar" hidden>
            <div class="density-map-head"><span>Cross-Exchange Radar</span><span id="densityExchangeRadarStatus">—</span></div>
            <div id="densityExchangeRadarList" class="density-exchange-radar-list"></div>
        </div>
        <div id="densityScreenerPanel" class="density-screener-panel" hidden>
            <div class="density-map-head"><span>Плотности в скринере</span><span id="densityScreenerStatus">—</span></div>
            <div id="densityScreenerList" class="density-screener-list"></div>
        </div>
    </section>
    </div>



</div>

<script>
const searchInput = document.getElementById("search");
const coinTable = document.getElementById("coinTable");
let selectedMarketSymbol = "";

function normalizeCopySymbol(symbol) {
    return String(symbol || "").toUpperCase().trim();
}

function syncSelectedMarketRow(symbol = selectedMarketSymbol || selectedChartSymbol || "") {
    const safeSymbol = normalizeCopySymbol(symbol);
    if (safeSymbol) selectedMarketSymbol = safeSymbol;
    if (!coinTable) return;
    coinTable.querySelectorAll("tr[data-symbol]").forEach(row => {
        row.classList.toggle("selected-market-row", normalizeCopySymbol(row.dataset.symbol) === selectedMarketSymbol);
    });
}

function showTickerClickFeedback(element) {
    if (!element) return;
    element.classList.remove("ticker-click-feedback");
    // Force a reflow so repeated fast clicks replay the same short animation.
    void element.offsetWidth;
    element.classList.add("ticker-click-feedback");
    window.setTimeout(() => element.classList.remove("ticker-click-feedback"), 260);
}

async function copyTickerSymbol(symbol, feedbackElement = null) {
    const safeSymbol = normalizeCopySymbol(symbol);
    if (!safeSymbol) return false;
    let copied = false;
    try {
        if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
            await navigator.clipboard.writeText(safeSymbol);
            copied = true;
        }
    } catch (_) {}
    if (!copied) {
        try {
            const textarea = document.createElement("textarea");
            textarea.value = safeSymbol;
            textarea.setAttribute("readonly", "");
            textarea.style.position = "fixed";
            textarea.style.opacity = "0";
            document.body.appendChild(textarea);
            textarea.select();
            copied = document.execCommand("copy");
            textarea.remove();
        } catch (_) {}
    }
    showTickerClickFeedback(feedbackElement);
    return copied;
}

if (coinTable && typeof MutationObserver !== "undefined") {
    const gridTableObserver = new MutationObserver(() => {
        updateChartGridPagination();
        syncSelectedMarketRow();
    });
    gridTableObserver.observe(coinTable, {childList:true});
}
// Robust market-row navigation: delegation is registered immediately, before
// the optional Density/UI initialization below. A later UI error therefore
// cannot prevent ticker clicks from opening the large chart.
coinTable?.addEventListener("click", event => {
    const ticker = event.target.closest(".symbol");
    const row = event.target.closest("tr[data-symbol]");

    if (ticker && row && coinTable.contains(row)) {
        const safeSymbol = normalizeCopySymbol(row.dataset.symbol);
        selectedMarketSymbol = safeSymbol;
        syncSelectedMarketRow(safeSymbol);
        copyTickerSymbol(safeSymbol, ticker);
    }

    if (event.target.closest("button,input,a,select,textarea,[data-watchlist-trigger]")) return;
    if (row && coinTable.contains(row)) {
        selectedMarketSymbol = normalizeCopySymbol(row.dataset.symbol);
        syncSelectedMarketRow(selectedMarketSymbol);
        openChart(row.dataset.symbol);
    }
});

// Chart/search ticker titles use the same small click feedback and copy only
// the pure symbol, never the surrounding timeframe/source text.
document.addEventListener("click", event => {
    const gridTitle = event.target.closest(".chart-grid-title");
    if (gridTitle) {
        const slot = gridTitle.closest(".chart-grid-slot[data-chart-symbol]");
        const symbol = slot?.dataset.chartSymbol || "";
        if (symbol) {
            selectedMarketSymbol = normalizeCopySymbol(symbol);
            syncSelectedMarketRow(selectedMarketSymbol);
            copyTickerSymbol(symbol, gridTitle);
        }
        return;
    }

    const suggestionTitle = event.target.closest(".suggestion[data-symbol] strong");
    if (suggestionTitle) {
        const suggestion = suggestionTitle.closest(".suggestion[data-symbol]");
        if (suggestion?.dataset.symbol) copyTickerSymbol(suggestion.dataset.symbol, suggestionTitle);
        return;
    }

    if (event.target.closest("#chartTitle")) {
        const symbol = normalizeCopySymbol(selectedChartSymbol);
        if (symbol) copyTickerSymbol(symbol, document.getElementById("chartTitle"));
    }
});
const coinCount = document.getElementById("coinCount");
const lastUpdate = document.getElementById("lastUpdate");
const statusText = document.getElementById("statusText");
const statusDot = document.getElementById("statusDot");
const signals = document.getElementById("signals");
const levels = document.getElementById("levels");
const connectionStatus = document.getElementById("connectionStatus");
const connectionCount = document.getElementById("connectionCount");
const reconnectAttempts = document.getElementById("reconnectAttempts");
const lastMessage = document.getElementById("lastMessage");
const errorBox = document.getElementById("errorBox");
const searchToggle = document.getElementById("searchToggle");
const settingsButton = document.getElementById("settingsButton");
const terminalConnectionSection = document.getElementById("terminalConnectionSection");
const terminalConnectionSectionTitle = document.getElementById("terminalConnectionSectionTitle");
const terminalType = document.getElementById("terminalType");
const terminalHost = document.getElementById("terminalHost");
const terminalPort = document.getElementById("terminalPort");
const terminalConnectButton = document.getElementById("terminalConnectButton");
const terminalDisconnectButton = document.getElementById("terminalDisconnectButton");
const terminalConnectionStatusDot = document.getElementById("terminalConnectionStatusDot");
const terminalConnectionStatusText = document.getElementById("terminalConnectionStatusText");
let tigerTerminalSocket = null;
let tigerTerminalManualClose = false;
let tigerTerminalReconnectTimer = null;
const TIGER_TERMINAL_STORAGE = "cryptoScreenerTerminalConnection";
const TIGER_LINK_GROUPS = ["A", "B", "C", "D", "E", "F"];

function loadTerminalConnectionSettings() {
    const fallback = { type: "tiger", host: "127.0.0.1", port: 7819, autoConnect: false };
    try {
        const saved = JSON.parse(localStorage.getItem(TIGER_TERMINAL_STORAGE) || "null");
        if (!saved || typeof saved !== "object") return fallback;
        return {
            type: saved.type === "tiger" ? "tiger" : "tiger",
            host: String(saved.host || fallback.host).trim() || fallback.host,
            port: Math.min(65535, Math.max(1, Number(saved.port) || fallback.port)),
            autoConnect: saved.autoConnect === true
        };
    } catch {
        return fallback;
    }
}

function saveTerminalConnectionSettings(autoConnect = true) {
    localStorage.setItem(TIGER_TERMINAL_STORAGE, JSON.stringify({
        type: terminalType?.value === "tiger" ? "tiger" : "tiger",
        host: String(terminalHost?.value || "127.0.0.1").trim() || "127.0.0.1",
        port: Math.min(65535, Math.max(1, Number(terminalPort?.value) || 7819)),
        autoConnect
    }));
}

function setTerminalConnectionStatus(state, text) {
    if (!terminalConnectionStatusText || !terminalConnectionStatusDot) return;
    terminalConnectionStatusText.textContent = text;
    terminalConnectionStatusDot.classList.toggle("connected", state === "connected");
    terminalConnectionStatusDot.classList.toggle("error", state === "error");
}

function tigerTerminalUrl() {
    const host = String(terminalHost?.value || "127.0.0.1").trim() || "127.0.0.1";
    const port = Math.min(65535, Math.max(1, Number(terminalPort?.value) || 7819));
    return `ws://${host}:${port}`;
}

function disconnectTigerTerminal() {
    tigerTerminalManualClose = true;
    saveTerminalConnectionSettings(false);
    if (tigerTerminalReconnectTimer) { clearTimeout(tigerTerminalReconnectTimer); tigerTerminalReconnectTimer = null; }
    if (tigerTerminalSocket) {
        try { tigerTerminalSocket.close(); } catch {}
    }
    tigerTerminalSocket = null;
    setTerminalConnectionStatus("idle", "Отключено");
}

function connectTigerTerminal(autoReconnect = false) {
    if (typeof WebSocket === "undefined") {
        setTerminalConnectionStatus("error", "WebSocket недоступен");
        return;
    }
    saveTerminalConnectionSettings();
    tigerTerminalManualClose = false;
    if (tigerTerminalReconnectTimer) { clearTimeout(tigerTerminalReconnectTimer); tigerTerminalReconnectTimer = null; }
    if (tigerTerminalSocket && (tigerTerminalSocket.readyState === WebSocket.OPEN || tigerTerminalSocket.readyState === WebSocket.CONNECTING)) return;
    setTerminalConnectionStatus("idle", autoReconnect ? "Переподключение…" : "Подключение…");
    try {
        tigerTerminalSocket = new WebSocket(tigerTerminalUrl());
        tigerTerminalSocket.addEventListener("open", () => {
            saveTerminalConnectionSettings(true);
            setTerminalConnectionStatus("connected", "Подключено к Tiger");
        });
        tigerTerminalSocket.addEventListener("message", event => {
            try {
                const data = JSON.parse(event.data);
                if (data?.response?.type === "error" || data?.title === "Invalid JSON (syntax error)") {
                    setTerminalConnectionStatus("error", data?.response?.error?.message || data?.message || "Ошибка Tiger");
                }
            } catch {}
        });
        tigerTerminalSocket.addEventListener("error", () => {
            setTerminalConnectionStatus("error", "Tiger недоступен");
        });
        tigerTerminalSocket.addEventListener("close", () => {
            tigerTerminalSocket = null;
            if (tigerTerminalManualClose) {
                setTerminalConnectionStatus("idle", "Отключено");
                return;
            }
            setTerminalConnectionStatus("error", "Соединение потеряно");
            tigerTerminalReconnectTimer = setTimeout(() => connectTigerTerminal(true), 2000);
        });
    } catch (error) {
        tigerTerminalSocket = null;
        setTerminalConnectionStatus("error", "Не удалось подключиться");
    }
}

function tigerLinkGroupForChartColor(color) {
    const index = CHART_COLORS.indexOf(color);
    return index >= 0 ? (TIGER_LINK_GROUPS[index] || null) : null;
}

function sendTigerLinkedSymbol(symbol, color) {
    const safeSymbol = String(symbol || "").toUpperCase().trim();
    const linkGroup = tigerLinkGroupForChartColor(color);
    if (!safeSymbol || !linkGroup) return false;
    if (!tigerTerminalSocket || tigerTerminalSocket.readyState !== WebSocket.OPEN) {
        setTerminalConnectionStatus("error", "Tiger не подключён");
        return false;
    }
    const payload = { type: "setLinkSymbol", e: "TigerX Binance", m: "FUTURES", symbol: safeSymbol, linkGroup };
    try {
        tigerTerminalSocket.send(JSON.stringify(payload));
        return true;
    } catch {
        setTerminalConnectionStatus("error", "Ошибка отправки в Tiger");
        return false;
    }
}

function initTerminalConnectionSettings() {
    const saved = loadTerminalConnectionSettings();
    if (terminalType) terminalType.value = saved.type;
    if (terminalHost) terminalHost.value = saved.host;
    if (terminalPort) terminalPort.value = saved.port;
    terminalConnectionSectionTitle?.addEventListener("click", () => terminalConnectionSection?.classList.toggle("open"));
    terminalConnectButton?.addEventListener("click", () => connectTigerTerminal(false));
    terminalDisconnectButton?.addEventListener("click", disconnectTigerTerminal);
    [terminalType, terminalHost, terminalPort].forEach(el => el?.addEventListener("change", () => saveTerminalConnectionSettings(saved.autoConnect)));
    if (saved.autoConnect) {
        setTimeout(() => connectTigerTerminal(true), 250);
    }
}

const topSearch = document.getElementById("topSearch");
const viewCoinList = document.getElementById("viewCoinList");
const viewAlerts = document.getElementById("viewAlerts");
const viewDensity = document.getElementById("viewDensity");
const marketPanel = document.querySelector(".market-panel");
const workspaceDivider = document.getElementById("workspaceDivider");
const workspaceLayout = document.querySelector(".layout");
const settingsOverlay = document.getElementById("settingsOverlay");
const SCREENER_ENGINE_STORAGE = "cryptoScreenerEngineSettings";
const DEFAULT_SCREENER_ENGINE_SETTINGS = { mode: "auto", renderUrl: "" };
let screenerEngineSettings = loadScreenerEngineSettings();
let activeScreenerEngine = "local";
let screenerEngineCheckPromise = null;

function loadScreenerEngineSettings() {
    try {
        const saved = JSON.parse(localStorage.getItem(SCREENER_ENGINE_STORAGE) || "null");
        return {
            mode: ["local", "render", "auto"].includes(saved?.mode) ? saved.mode : DEFAULT_SCREENER_ENGINE_SETTINGS.mode,
            renderUrl: String(saved?.renderUrl || "").trim().replace(/\/$/, "")
        };
    } catch (_) { return {...DEFAULT_SCREENER_ENGINE_SETTINGS}; }
}
function saveScreenerEngineSettings() {
    localStorage.setItem(SCREENER_ENGINE_STORAGE, JSON.stringify(screenerEngineSettings));
}
function normalizeScreenerRenderUrl(value) {
    return String(value || "").trim().replace(/\/$/, "");
}
function screenerApiBase() {
    if (activeScreenerEngine === "render" && screenerEngineSettings.renderUrl) return screenerEngineSettings.renderUrl;
    return "";
}
function screenerApiUrl(path) {
    const base = screenerApiBase();
    return base ? `${base}${path}` : path;
}
async function fetchScreenerJson(path, options = {}, timeoutMs = 3500) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
        const response = await fetch(screenerApiUrl(path), {...options, signal: controller.signal, cache: "no-store"});
        if (!response.ok) throw new Error("HTTP " + response.status);
        return await response.json();
    } finally { clearTimeout(timer); }
}
async function checkRenderHealth() {
    const url = normalizeScreenerRenderUrl(screenerEngineSettings.renderUrl);
    if (!url) return false;
    try {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), 3000);
        const response = await fetch(`${url}/health`, {cache:"no-store", signal:controller.signal});
        clearTimeout(timer);
        if (!response.ok) return false;
        const data = await response.json().catch(() => ({}));
        return data.ok === true;
    } catch (_) { return false; }
}

async function fallbackToLocalIfRenderUnavailable(error) {
    if (activeScreenerEngine !== "render" || !screenerEngineSettings.renderUrl) return false;
    const online = await checkRenderHealth();
    if (online) return false;
    activeScreenerEngine = "local";
    renderScreenerEngineStatus("Render OFFLINE → Local");
    console.warn("Screener Render unavailable; switched to Local", error || "");
    return true;
}
async function resolveScreenerEngine(force = false) {
    if (screenerEngineCheckPromise && !force) return screenerEngineCheckPromise;
    screenerEngineCheckPromise = (async () => {
        const requested = screenerEngineSettings.mode;
        if (requested === "local") {
            activeScreenerEngine = "local";
        } else {
            const online = await checkRenderHealth();
            activeScreenerEngine = online ? "render" : "local";
        }
        renderScreenerEngineStatus();
        return activeScreenerEngine;
    })().finally(() => { screenerEngineCheckPromise = null; });
    return screenerEngineCheckPromise;
}
function renderScreenerEngineStatus(extra = "") {
    const status = document.getElementById("screenerEngineStatus");
    if (!status) return;
    const requested = screenerEngineSettings.mode;
    const label = activeScreenerEngine === "render" ? "Render ONLINE" : (requested === "render" ? "Render OFFLINE → Local" : "Local");
    status.textContent = extra || `Режим: ${requested} · активно: ${label}`;
    document.querySelectorAll('input[name="screenerEngineMode"]').forEach(el => { el.checked = el.value === requested; });
    const input = document.getElementById("screenerRenderUrl");
    if (input && document.activeElement !== input) input.value = screenerEngineSettings.renderUrl;
}
function initScreenerEngineSettings() {
    const section = document.getElementById("screenerEngineSection");
    const title = document.getElementById("screenerEngineSectionTitle");
    const input = document.getElementById("screenerRenderUrl");
    title?.addEventListener("click", () => section?.classList.toggle("open"));
    document.querySelectorAll('input[name="screenerEngineMode"]').forEach(el => el.addEventListener("change", async () => {
        screenerEngineSettings.mode = el.value;
        saveScreenerEngineSettings();
        await resolveScreenerEngine(true);
        loadState();
    }));
    input?.addEventListener("change", async () => {
        screenerEngineSettings.renderUrl = normalizeScreenerRenderUrl(input.value);
        saveScreenerEngineSettings();
        await resolveScreenerEngine(true);
        loadState();
    });
    renderScreenerEngineStatus();
    resolveScreenerEngine();
}


const chartGridSectionTitle = document.getElementById("chartGridSectionTitle");
const chartGridPicker = document.getElementById("chartGridPicker");
const chartGridValue = document.getElementById("chartGridValue");
const chartGridWorkspace = document.getElementById("chartGridWorkspace");
const chartGridPagination = document.getElementById("chartGridPagination");
const chartGridPrev = document.getElementById("chartGridPrev");
const chartGridNext = document.getElementById("chartGridNext");
const chartGridPaginationInfo = document.getElementById("chartGridPaginationInfo");
const chartSingleColorBar = document.getElementById("chartSingleColorBar");
const chartColorPalette = document.getElementById("chartColorPalette");
const CHART_GRID_STORAGE = "cryptoScreenerChartGrid";
const CHART_GRID_RETURN_STORAGE = "cryptoScreenerChartGridReturnState";
const CHART_COLOR_STORAGE = "cryptoScreenerChartColors";
const CHART_COLORS = ["#f2c94c", "#ef5350", "#31c48d", "#3b82f6", "#b36cff", "#6b7280"];
let chartGridRows = 1;
let chartGridCols = 1;
let chartGridMaximized = -1;
let chartGridPage = 0;
const chartGridIntervalsBySymbol = new Map();
let chartLargeMode = false;
let chartGridColors = {};
try {
    const savedGrid = JSON.parse(localStorage.getItem(CHART_GRID_STORAGE) || "null");
    if (savedGrid) {
        chartGridRows = Math.max(1, Math.min(7, Number(savedGrid.rows) || 1));
        chartGridCols = Math.max(1, Math.min(7, Number(savedGrid.cols) || 1));
    }
} catch (_) {}
try { chartGridColors = JSON.parse(localStorage.getItem(CHART_COLOR_STORAGE) || "{}"); } catch (_) { chartGridColors = {}; }

const WORKSPACE_VIEW_STORAGE = "cryptoScreenerWorkspaceViews";
let workspaceViews = { coinList: true, alerts: true, density: false };
window.workspaceViews = workspaceViews;
try {
    const savedViews = JSON.parse(localStorage.getItem(WORKSPACE_VIEW_STORAGE) || "null");
    if (savedViews && typeof savedViews === "object") {
        workspaceViews = {
            coinList: savedViews.coinList !== false,
            alerts: savedViews.alerts !== false,
            density: savedViews.density === true
        };
    }
} catch (_) {}

// Critical UI handlers are registered immediately after the DOM references are available.
// This keeps Settings and Density responsive even if a later optional initialization fails.
function openSettingsSafe() {
    try {
        renderHotkeysSettings();
        renderNotificationSoundSettings();
        renderTelegramConnections();
        renderNotificationRouting();
        updateChartGridPicker(chartGridRows, chartGridCols, false);
    } catch (error) {
        console.warn("Settings content init warning:", error);
    }
    settingsOverlay?.classList.add("open");
    settingsOverlay?.setAttribute("aria-hidden", "false");
}
settingsButton?.addEventListener("click", openSettingsSafe);

function setDensityMapStatus(kind, text) {
    const el = document.getElementById("densityMapStatus");
    if (!el) return;
    el.className = `density-map-status ${kind}`;
    const textEl = el.querySelector(".density-map-status-text");
    if (textEl) textEl.textContent = text;
}

function chartGridCountText(count) {
    if (count === 1) return "1 график";
    if (count >= 2 && count <= 4) return `${count} графика`;
    return `${count} графиков`;
}
function saveChartGrid() {
    try { localStorage.setItem(CHART_GRID_STORAGE, JSON.stringify({rows:chartGridRows, cols:chartGridCols})); } catch (_) {}
}
function saveChartGridReturnState(state) {
    if(!state || !Array.isArray(state.slots) || !state.slots.length) return;
    try { localStorage.setItem(CHART_GRID_RETURN_STORAGE, JSON.stringify(state)); } catch (_) {}
}
function loadChartGridReturnState() {
    try {
        const state=JSON.parse(localStorage.getItem(CHART_GRID_RETURN_STORAGE) || "null");
        if(!state || !Array.isArray(state.slots) || !state.slots.length) return null;
        return state;
    } catch (_) { return null; }
}
function saveChartColors() {
    try { localStorage.setItem(CHART_COLOR_STORAGE, JSON.stringify(chartGridColors)); } catch (_) {}
}
function chartGridColorFor(symbol) { return chartGridColors[String(symbol || "").toUpperCase()] || CHART_COLORS[CHART_COLORS.length-1]; }
function chartDisplaySymbol(symbol) {
    const safe = String(symbol || "").toUpperCase().trim();
    return safe.endsWith("USDT") ? safe.slice(0, -4) : safe;
}
function marketDisplayExchange(exchange) {
    const value = String(exchange || "").toUpperCase();
    if (value.includes("BINANCE") && value.includes("FUTURES")) return "B-F";
    if (value.includes("BINANCE") && value.includes("SPOT")) return "B-S";
    if (value.includes("OKX") && value.includes("FUTURES")) return "OK-F";
    if (value.includes("OKX") && value.includes("SPOT")) return "OK-S";
    if (value.includes("BYBIT") && value.includes("FUTURES")) return "BY-F";
    if (value.includes("BYBIT") && value.includes("SPOT")) return "BY-S";
    return value || "—";
}
function renderChartGridPicker() {
    if (!chartGridPicker) return;
    chartGridPicker.innerHTML = "";
    for (let r=1; r<=7; r++) for (let c=1; c<=7; c++) {
        const cell=document.createElement("button");
        cell.type="button"; cell.className="grid-layout-cell";
        cell.dataset.row=String(r); cell.dataset.col=String(c);
        cell.setAttribute("aria-label",`${r} × ${c}`);
        chartGridPicker.appendChild(cell);
    }
    updateChartGridPicker(chartGridRows, chartGridCols, false);
    chartGridPicker.querySelectorAll(".grid-layout-cell").forEach(cell=>{
        cell.addEventListener("pointerenter",()=>updateChartGridPicker(Number(cell.dataset.row),Number(cell.dataset.col),true));
    });
    chartGridPicker.addEventListener("pointerleave",()=>updateChartGridPicker(chartGridRows,chartGridCols,false));
    chartGridPicker.addEventListener("click",event=>{
        const cell=event.target.closest(".grid-layout-cell"); if(!cell) return;
        chartGridRows=Number(cell.dataset.row); chartGridCols=Number(cell.dataset.col);
        saveChartGrid();
        if(chartGridRows*chartGridCols>1){
            const previous=loadChartGridReturnState();
            saveChartGridReturnState({rows:chartGridRows,cols:chartGridCols,page:0,slots:previous?.slots||[],activeSymbol:selectedChartSymbol||previous?.activeSymbol||""});
        }
        updateChartGridPicker(chartGridRows,chartGridCols,false); applyChartGridLayout();
    });
}
function updateChartGridPicker(rows, cols, preview) {
    chartGridPicker?.querySelectorAll(".grid-layout-cell").forEach(cell=>{
        const r=Number(cell.dataset.row), c=Number(cell.dataset.col);
        const active=r<=rows && c<=cols;
        cell.classList.toggle("selected", !preview && active);
        cell.classList.toggle("preview", preview && active);
        cell.classList.toggle("preview-after", preview && !active && r<=rows && c<=cols);
    });
    if(chartGridValue) chartGridValue.textContent=`${rows} × ${cols} · ${chartGridCountText(rows*cols)}`;
}
function renderChartColorPalette(symbol) {
    if(!chartColorPalette) return;
    chartColorPalette.innerHTML='';
    const selected=chartGridColorFor(symbol);
    CHART_COLORS.forEach(color=>{
        const dot=document.createElement('button');
        dot.type='button'; dot.className='chart-grid-color-dot'; dot.dataset.color=color; dot.style.background=color;
        dot.title=color===CHART_COLORS[CHART_COLORS.length-1]?'Серый':`Цвет ${color}`;
        dot.setAttribute('aria-label', dot.title);
        if(color===selected) dot.classList.add('selected');
        dot.addEventListener('click',e=>{e.stopPropagation();setChartGridColor(symbol,color);});
        chartColorPalette.appendChild(dot);
    });
}
function setChartGridColor(symbol,color) {
    const key=String(symbol||"").toUpperCase(); if(!key || !CHART_COLORS.includes(color)) return;
    // A chart color identifies a group, so the same explicit color belongs to
    // only one instrument. Assigning it to a new instrument transfers it from
    // the previous instrument instead of creating two groups with one color.
    Object.keys(chartGridColors).forEach(otherKey=>{
        if(otherKey!==key && chartGridColors[otherKey]===color) delete chartGridColors[otherKey];
    });
    chartGridColors[key]=color; saveChartColors();
    sendTigerLinkedSymbol(key, color);
    chartGridWorkspace?.querySelectorAll(`.chart-grid-slot[data-chart-symbol="${CSS.escape(key)}"] .chart-grid-title-dot`).forEach(dot=>dot.style.background=color);
    chartGridWorkspace?.querySelectorAll('.chart-grid-slot').forEach(slot=>{
        const other=String(slot.dataset.chartSymbol||"").toUpperCase();
        if(other!==key && !chartGridColors[other]) slot.querySelector('.chart-grid-title-dot')?.style.setProperty('background',CHART_COLORS[CHART_COLORS.length-1]);
    });
    if(selectedChartSymbol===key) chartTitleDot?.style.setProperty("background",color);
    if(selectedChartSymbol===key) renderChartColorPalette(key);
}
function updateChartGridColor(symbol,color){ setChartGridColor(symbol,color); }
function ensureSingleChartColorBar(symbol){ renderChartColorPalette(symbol); }

function renderChartFlagPalette() {
    if (!chartFlagPalette) return;
    const symbol = String(selectedChartSymbol || "").toUpperCase();
    chartFlagPalette.innerHTML = WATCHLIST_COLORS.map(color => `
        <button type="button" class="watchlist-color-option" data-chart-flag-color="${color}" title="${color}" style="background:${color}"></button>
    `).join("") + `<button type="button" class="watchlist-color-clear" data-chart-flag-clear="1" title="Убрать цвет"></button>`;
    chartFlagPalette.hidden = !chartFlagOpen;
    chartFlagPalette.querySelectorAll("[data-chart-flag-color]").forEach(btn => btn.addEventListener("click", event => {
        event.preventDefault(); event.stopPropagation();
        chartFlagPendingColor = btn.dataset.chartFlagColor;
        chartFlagPendingExplicitClear = false;
        setChartFlagVisual(chartFlagPendingColor);
    }));
    chartFlagPalette.querySelector("[data-chart-flag-clear]")?.addEventListener("click", event => {
        event.preventDefault(); event.stopPropagation();
        chartFlagPendingColor = null;
        chartFlagPendingExplicitClear = true;
        setChartFlagVisual(null);
    });
}
function setChartFlagVisual(color) {
    if (!chartFlag) return;
    chartFlag.classList.toggle("has-color", !!color);
    if (color) chartFlag.style.setProperty("--chart-flag-color", color);
    else chartFlag.style.removeProperty("--chart-flag-color");
}
function chartFlagColorFor(symbol) {
    const key = String(symbol || "").toUpperCase();
    if (!key) return null;
    // The chart flag and Market Screener flag are one state.
    const shared = (typeof watchlistColorFor === "function") ? watchlistColorFor(key) : null;
    if (shared) return shared;
    // One-time migration from the old chart-only storage.
    try {
        const saved = JSON.parse(localStorage.getItem("cryptoScreenerChartFlags") || "{}");
        const legacy = saved[key] || null;
        if (legacy && typeof commitWatchlistColor === "function") commitWatchlistColor(key, legacy);
        return legacy;
    } catch (_) { return null; }
}
function saveChartFlagColor(symbol, color) {
    const key = String(symbol || "").toUpperCase();
    if (!key) return;
    // Write to the same storage used by Market Screener.
    if (typeof commitWatchlistColor === "function") {
        commitWatchlistColor(key, color || null);
        if (typeof renderTable === "function") renderTable();
        return;
    }
    // Fallback only during very early initialization.
    let state = {};
    try { state = JSON.parse(localStorage.getItem("cryptoScreenerChartFlags") || "{}") || {}; } catch (_) {}
    if (color) state[key] = color; else delete state[key];
    try { localStorage.setItem("cryptoScreenerChartFlags", JSON.stringify(state)); } catch (_) {}
}
function syncChartFlagForSymbol(symbol) {
    const saved = chartFlagColorFor(symbol);
    chartFlagPendingColor = saved;
    chartFlagPendingExplicitClear = false;
    setChartFlagVisual(saved);
    chartFlagOpen = false;
    if (chartFlagPalette) chartFlagPalette.hidden = true;
}
function commitChartFlag() {
    if (!selectedChartSymbol) return;
    const color = chartFlagPendingExplicitClear ? null : chartFlagPendingColor;
    saveChartFlagColor(selectedChartSymbol, color);
    setChartFlagVisual(color);
}

function setChartLargeMode(enabled){
    chartLargeMode = !!enabled;
    chartModal?.classList.toggle("large-chart-mode", chartLargeMode);
    if (!chartLargeMode) {
        chartFlagOpen = false;
        if (chartFlagPalette) chartFlagPalette.hidden = true;
    } else if (selectedChartSymbol) {
        syncChartFlagForSymbol(selectedChartSymbol);
    }
}

function updateChartGridExpandButtons(){
    chartGridWorkspace?.querySelectorAll('.chart-grid-slot').forEach(slot=>{
        const btn=slot.querySelector('.chart-grid-expand'); if(!btn) return;
        const maximized=slot.classList.contains('maximized');
        btn.classList.toggle('is-maximized',maximized);
        btn.title=maximized?'Вернуть график в сетку':'Развернуть график';
        btn.setAttribute('aria-label',btn.title);
    });
}
function toggleChartGridMaximize(slot) {
    const slots=[...chartGridWorkspace.querySelectorAll('.chart-grid-slot')]; const idx=slots.indexOf(slot); if(idx<0) return;
    if(chartGridMaximized===idx){ chartGridMaximized=-1; slot.classList.remove('maximized'); chartGridWorkspace.classList.remove('has-maximized'); }
    else { slots.forEach(x=>x.classList.remove('maximized')); chartGridMaximized=idx; slot.classList.add('maximized'); chartGridWorkspace.classList.add('has-maximized'); activateGridSlot(slot); }
    document.getElementById('chartModal')?.classList.toggle('grid-maximized', chartGridMaximized >= 0);
    setChartLargeMode(chartGridMaximized >= 0);
    updateChartGridExpandButtons();
    requestAnimationFrame(() => {
        resizeOwnChartsToContainer();
    });
}
function chartGridSymbolsList() {
    const out=[]; const seen=new Set();
    const rows=[...(coinTable?.querySelectorAll('tr[data-symbol]') || [])];
    const source=rows.length
        ? rows.map(row=>row.dataset.symbol)
        : latestCoins.map(c=>c.symbol);
    source.forEach(sym=>{
        const s=String(sym||'').toUpperCase().trim();
        if(!s || !s.endsWith('USDT') || seen.has(s)) return;
        seen.add(s); out.push(s);
    });
    return out;
}
function chartGridPageSize() { return Math.max(1, chartGridRows * chartGridCols); }
function chartGridPageCount() {
    const total=chartGridSymbolsList().length;
    return Math.max(1, Math.ceil(total / chartGridPageSize()));
}
function chartGridPageSymbols() {
    const list=chartGridSymbolsList();
    const size=chartGridPageSize();
    const maxPage=Math.max(0, Math.ceil(list.length / size)-1);
    chartGridPage=Math.max(0, Math.min(chartGridPage, maxPage));
    return list.slice(chartGridPage*size, (chartGridPage+1)*size);
}
function updateChartGridPagination() {
    if(!chartGridPagination) return;
    const total=chartGridSymbolsList().length;
    const size=chartGridPageSize();
    const pages=Math.max(1, Math.ceil(total / size));
    chartGridPage=Math.max(0, Math.min(chartGridPage, pages-1));
    const maxPage=Math.max(0,pages-1);
    if(chartGridPage>maxPage && chartGridRows*chartGridCols>1 && document.getElementById('chartContent')?.classList.contains('grid-active') && chartGridMaximized < 0){
        renderChartGridPage(maxPage);
        return;
    }
    chartGridPage=Math.max(0,Math.min(chartGridPage,maxPage));
    const from=total ? chartGridPage*size+1 : 0;
    const to=total ? Math.min((chartGridPage+1)*size,total) : 0;
    const show=chartGridRows*chartGridCols>1 && total>size && document.getElementById('chartContent')?.classList.contains('grid-active') && chartGridMaximized < 0;
    chartGridPagination.classList.toggle('visible',show);
    if(chartGridPaginationInfo) chartGridPaginationInfo.textContent=total ? `${from}–${to} из ${total}` : '0 из 0';
    if(chartGridPrev) chartGridPrev.disabled=!show || chartGridPage<=0;
    if(chartGridNext) chartGridNext.disabled=!show || chartGridPage>=pages-1;
}
function rememberCurrentGridIntervals() {
    chartGridWorkspace?.querySelectorAll('.chart-grid-slot').forEach(slot=>{
        const symbol=String(slot.dataset.chartSymbol||'').toUpperCase();
        const interval=slot._chartRefs?.interval || slot.dataset.chartInterval || slot.querySelector('.chart-grid-intervals button.active')?.dataset.interval || '1';
        if(symbol) chartGridIntervalsBySymbol.set(symbol, interval);
    });
}
function renderChartGridPage(page) {
    if(chartGridRows*chartGridCols<=1 || !chartGridWorkspace) return;
    rememberCurrentGridIntervals();
    saveFocusedGridDrawingState();
    const total=chartGridSymbolsList().length;
    const size=chartGridPageSize();
    const pages=Math.max(1,Math.ceil(total/size));
    chartGridPage=Math.max(0,Math.min(Number(page)||0,pages-1));
    chartGridMaximized=-1;
    chartGridWorkspace.classList.remove('has-maximized');
    document.getElementById('chartModal')?.classList.remove('grid-maximized');
    setChartLargeMode(false);
    destroyMiniCharts();
    chartGridWorkspace.innerHTML='';
    chartGridPageSymbols().forEach((sym,i)=>{
        const interval=chartGridIntervalsBySymbol.get(sym) || '1';
        buildChartGridSlot(i,sym,interval);
    });
    const first=chartGridWorkspace.querySelector('.chart-grid-slot');
    const preferred=chartGridSymbolsList().includes(selectedChartSymbol)
        ? chartGridWorkspace.querySelector(`.chart-grid-slot[data-chart-symbol="${CSS.escape(selectedChartSymbol)}"]`)
        : null;
    if(preferred) activateGridSlot(preferred); else if(first) activateGridSlot(first);
    updateChartGridPagination();
    requestAnimationFrame(()=>resizeOwnChartsToContainer());
}
chartGridPrev?.addEventListener('click',()=>{ if(chartGridPage>0) renderChartGridPage(chartGridPage-1); });
chartGridNext?.addEventListener('click',()=>{ if(chartGridPage<chartGridPageCount()-1) renderChartGridPage(chartGridPage+1); });

function formatCompactMetric(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) return "—";
    const sign = number < 0 ? "-" : "";
    const abs = Math.abs(number);
    const compact = (scaled, suffix) => {
        let text = scaled.toFixed(2).replace(/\.00$/, "").replace(/(\.[0-9])0$/, "$1");
        return sign + text + suffix;
    };
    if (abs >= 1e9) return compact(abs / 1e9, "B");
    if (abs >= 1e6) return compact(abs / 1e6, "M");
    if (abs >= 1e3) return compact(abs / 1e3, "K");
    return sign + (Number.isInteger(abs) ? String(abs) : abs.toFixed(2).replace(/0+$/, "").replace(/\.$/, ""));
}

function formatCompactOI(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) return "—";
    const sign = number < 0 ? "-" : "";
    const abs = Math.abs(number);
    const compact = (scaled, suffix) => {
        let text = scaled.toFixed(2)
            .replace(/\.00$/, "")
            .replace(/(\.[0-9])0$/, "$1");
        return sign + text + suffix;
    };
    if (abs >= 1e9) return compact(abs / 1e9, "B");
    if (abs >= 1e6) return compact(abs / 1e6, "M");
    if (abs >= 1e3) return compact(abs / 1e3, "K");
    return sign + (Number.isInteger(abs)
        ? String(abs)
        : abs.toFixed(2).replace(/0+$/, "").replace(/\.$/, ""));
}
function clearMiniHistory(refs) {
    if (!refs) return;
    const chunks = miniHistoryChunkSeries.get(refs) || [];
    for (const chunk of chunks) {
        try { refs.price.removeSeries(chunk.candle); } catch (_) {}
        try { refs.price.removeSeries(chunk.vol); } catch (_) {}
        if (chunk.oi) { try { refs.oi.removeSeries(chunk.oi); } catch (_) {} }
    }
    miniHistoryChunkSeries.delete(refs);
    miniHistoryStates.delete(refs);
}

function armMiniHistory(refs, kd, od) {
    if (!refs || !kd) return;
    const candles = (kd.candles || []).map(c => Number(c.time)).filter(Number.isFinite).sort((a,b)=>a-b);
    const oiPoints = (od?.points || []).map(p => Number(p.time)).filter(Number.isFinite).sort((a,b)=>a-b);
    if (!candles.length) return;
    miniHistoryStates.set(refs, {
        token: refs,
        symbol: refs.symbol,
        interval: refs.interval,
        binanceInterval: ownChartIntervalToBinance(refs.interval),
        oiPeriod: ownOiPeriodForInterval(refs.interval),
        oldestKlineTime: candles[0],
        klineTimes: new Set(candles),
        oiTimes: new Set(oiPoints),
        loading: false,
        exhausted: candles.length < 250,
        armed: false,
        lastVisibleFrom: null,
        checkTimer: null
    });
}

function scheduleOlderMiniHistory(slot, refs) {
    const state = miniHistoryStates.get(refs);
    if (!state || state.checkTimer) return;
    state.checkTimer = setTimeout(() => {
        state.checkTimer = null;
        maybeLoadOlderMiniHistory(slot, refs);
    }, 80);
}

async function maybeLoadOlderMiniHistory(slot, refs) {
    const state = miniHistoryStates.get(refs);
    if (!state || state.loading || state.exhausted || slot._chartRefs !== refs) return;
    let logical = null;
    try { logical = refs.price.timeScale().getVisibleLogicalRange() || null; } catch (_) {}
    if (!logical || !Number.isFinite(Number(logical.from))) return;
    const from = Number(logical.from);
    if (!state.armed) {
        if (Number.isFinite(state.lastVisibleFrom) && from < state.lastVisibleFrom - 0.5) state.armed = true;
        state.lastVisibleFrom = from;
        if (!state.armed) return;
    } else state.lastVisibleFrom = from;
    if (from > 45) return;
    const oldest = Number(state.oldestKlineTime);
    if (!Number.isFinite(oldest) || oldest <= 0) return;
    state.loading = true;
    try {
        const endTime = Math.floor(oldest * 1000 - 1);
        const [kr, or] = await Promise.allSettled([
            fetch(`/api/klines?symbol=${encodeURIComponent(state.symbol)}&interval=${encodeURIComponent(state.binanceInterval)}&limit=250&endTime=${endTime}`, {cache:"no-store"}),
            fetch(`/api/open_interest?symbol=${encodeURIComponent(state.symbol)}&period=${encodeURIComponent(state.oiPeriod)}&limit=250&endTime=${endTime}`, {cache:"no-store"})
        ]);
        if (slot._chartRefs !== refs || miniHistoryStates.get(refs) !== state) return;
        if (kr.status !== "fulfilled") throw kr.reason || new Error("Ошибка загрузки истории");
        const kresp = kr.value, kd = await kresp.json();
        if (!kresp.ok) throw new Error(kd?.error || "Ошибка загрузки истории");
        let od = {points:[]};
        if (or.status === "fulfilled") { try { const r=or.value, d=await r.json(); if(r.ok&&Array.isArray(d.points)) od=d; } catch(_){} }
        const older = (kd.candles||[]).map(c=>({time:Number(c.time),open:Number(c.open),high:Number(c.high),low:Number(c.low),close:Number(c.close),volume:Number(c.volume)}))
            .filter(c=>[c.time,c.open,c.high,c.low,c.close,c.volume].every(Number.isFinite))
            .filter(c=>c.time < state.oldestKlineTime && !state.klineTimes.has(c.time)).sort((a,b)=>a.time-b.time);
        if (!older.length) { state.exhausted=true; return; }
        let timeRange=null, logicalRange=null;
        try { timeRange=refs.price.timeScale().getVisibleRange()||null; } catch(_) {}
        try { logicalRange=refs.price.timeScale().getVisibleLogicalRange()||null; } catch(_) {}
        const candle=refs.price.addSeries(LightweightCharts.CandlestickSeries, {upColor:"#26a69a",downColor:"#ef5350",borderVisible:false,wickUpColor:"#26a69a",wickDownColor:"#ef5350",priceLineVisible:false,lastValueVisible:false},0);
        const vol=refs.price.addSeries(LightweightCharts.HistogramSeries, {priceFormat:{type:"volume"},priceScaleId:"volume",color:"#808890",scaleMargins:{top:.86,bottom:0},priceLineVisible:false,lastValueVisible:false},0);
        candle.setData(older.map(c=>({time:c.time,open:c.open,high:c.high,low:c.low,close:c.close})));
        vol.setData(older.map(c=>({time:c.time,value:c.volume,color:"#808890"})));
        const oi=(od.points||[]).map(p=>({time:Number(p.time),value:Number(p.oiValue)})).filter(p=>Number.isFinite(p.time)&&Number.isFinite(p.value)).filter(p=>!state.oiTimes.has(p.time)).sort((a,b)=>a.time-b.time);
        // Do not create a second OI line when older history is loaded. The
        // mini-chart keeps one canonical OI series and merges older points.
        if (oi.length) {
            const merged = [...oi, ...(refs.oiPoints || [])];
            const byTime = new Map();
            for (const point of merged) byTime.set(Number(point.time), point);
            refs.oiPoints = [...byTime.values()].sort((a,b) => a.time - b.time);
            refs.oiSeries.setData(refs.oiPoints);
        }
        const chunks=miniHistoryChunkSeries.get(refs)||[]; chunks.push({candle,vol,oi:null}); miniHistoryChunkSeries.set(refs,chunks);
        older.forEach(c=>state.klineTimes.add(c.time)); oi.forEach(p=>state.oiTimes.add(p.time));
        state.oldestKlineTime=older[0].time; if(older.length<250) state.exhausted=true;
        if(timeRange){
            try{refs.price.timeScale().setVisibleRange(timeRange);}catch(_){}
            if((refs.oiPoints||[]).length){
                syncOiViewportToPrice(refs.price, refs.oi, refs.oiViewportExtensionSeries, timeRange);
            }
        }
        else if(logicalRange){try{refs.price.timeScale().setVisibleLogicalRange({from:Number(logicalRange.from)+older.length,to:Number(logicalRange.to)+older.length});}catch(_){} }
    } catch(e) {
        if (slot._chartRefs===refs) console.warn("Mini chart older history error:",e);
    } finally { if(miniHistoryStates.get(refs)===state) state.loading=false; }
}

async function loadMiniChart(slot,symbol,interval="1") {
    const MINI_HISTORY_LIMIT = 250;
    if(typeof LightweightCharts==="undefined" || !slot) return;
    const priceEl=slot.querySelector(".chart-grid-price"), oiEl=slot.querySelector(".chart-grid-oi"); if(!priceEl||!oiEl)return;
    const nextInterval=String(interval||"1");
    const symbolSafe=String(symbol||slot._chartRefs?.symbol||"").toUpperCase();
    const bi=ownChartIntervalToBinance(nextInterval), oiPeriod=ownOiPeriodForInterval(nextInterval);
    const cacheKey=`${symbolSafe}|${bi}|${oiPeriod}`;
    const existing=slot._chartRefs;
    if (existing?.symbol && existing?.interval && String(selectedChartSymbol || "").toUpperCase() === String(existing.symbol || "").toUpperCase() && String(selectedChartInterval || "") === String(existing.interval || "") && chartDrawingFocus) {
        syncActiveDrawingsToSharedStore();
    }
    const prefetched = !miniChartHistoryCache.has(cacheKey) ? takeChartHistoryPrefetch(cacheKey) : null;
    if (prefetched) miniChartHistoryCache.set(cacheKey, prefetched);

    const renderHistory=(refs,kd,od,fitContent=false,restoreRange=null)=>{
        if(slot._chartRefs!==refs || !kd) return false;
        const candles=(kd.candles||[]).map(c=>({time:Number(c.time),open:Number(c.open),high:Number(c.high),low:Number(c.low),close:Number(c.close)})).filter(c=>[c.time,c.open,c.high,c.low,c.close].every(Number.isFinite));
        const volumes=(kd.candles||[]).map(c=>({time:Number(c.time),value:Number(c.volume),color:"#808890"})).filter(c=>[c.time,c.value].every(Number.isFinite));
        const oiPoints=(od?.points||[]).map(p=>({time:Number(p.time),value:Number(p.oiValue)})).filter(p=>[p.time,p.value].every(Number.isFinite)).sort((a,b)=>a.time-b.time);
        refs.candle.setData(candles);
        refs.vol.setData(volumes);
        ensureDrawingFutureSpace(refs.price, candles, Number(ownChartIntervalToBinance(refs.interval)) > 0 ? ({ "1m":60,"3m":180,"5m":300,"15m":900,"30m":1800,"1h":3600,"4h":14400,"1d":86400 }[String(refs.interval)] || 60) : null);
        refs.oiPoints = oiPoints;
        if(od) refs.oiSeries.setData(oiPoints);
        refs.lastPrice=candles.length?Number(candles[candles.length-1].close):0;
        if(candles.length || oiPoints.length) miniChartHistoryCache.set(cacheKey,{klineData:kd,oiData:od||{points:[]}});
        armMiniHistory(refs, kd, od || {points:[]});
        if(fitContent) restoreChartViewport(refs.price, null, null, true);
        else if(restoreRange && !refs.price._userMovedViewport) restoreChartViewport(refs.price, restoreRange, null, false);
        const miniTimeRange=refs.price.timeScale().getVisibleRange();
        if(oiPoints.length && miniTimeRange) syncOiViewportToPrice(refs.price, refs.oi, refs.oiViewportExtensionSeries, miniTimeRange);
        if(slot.classList.contains('active-slot')) activateGridSlot(slot);
        return true;
    };

    const startLive=(refs)=>{
        const activeSymbol=refs.symbol, activeInterval=refs.interval, activeBi=ownChartIntervalToBinance(activeInterval), activeOiPeriod=ownOiPeriodForInterval(activeInterval);
        const applyLive=k=>{if(slot._chartRefs!==refs||!k)return;const time=Math.floor(Number(k.t)/1000),open=Number(k.o),high=Number(k.h),low=Number(k.l),close=Number(k.c),volume=Number(k.v);if(![time,open,high,low,close,volume].every(Number.isFinite))return;const previousPrice=refs.lastPrice;refs.lastPrice=close;updateSeriesPreservingManualViewport(refs.price, refs.candle, {time,open,high,low,close});checkSignalLevelCrossingsForSymbol(activeSymbol,signalDrawingsForSymbol(activeSymbol),previousPrice,close);updateSeriesPreservingManualViewport(refs.price, refs.vol, {time,value:volume,color:"#808890"});ensureDrawingFutureSpace(refs.price,[{time}], ({ "1":60,"3":180,"5":300,"15":900,"30":1800,"60":3600,"240":14400,"1D":86400 }[String(refs.interval)] || 60));const cached=miniChartHistoryCache.get(cacheKey);if(cached?.klineData?.candles?.length){const last=cached.klineData.candles[cached.klineData.candles.length-1];if(Number(last.time)===time){last.open=open;last.high=high;last.low=low;last.close=close;last.volume=volume;}}};
        try { const ws=new WebSocket(`wss://fstream.binance.com/market/ws/${activeSymbol.toLowerCase()}@kline_${activeBi}`); refs.ws=ws; ws.onmessage=e=>{try{const msg=JSON.parse(e.data);if(msg?.k)applyLive({t:msg.k.t,o:msg.k.o,h:msg.k.h,l:msg.k.l,c:msg.k.c,v:msg.k.v});}catch(_){} }; } catch(_) {}
        const refresh=async()=>{if(slot._chartRefs!==refs)return;try{const requestKey=`live|klines|${activeSymbol}|${activeBi}|1`;const d=await fetchChartJsonShared(`/api/klines?symbol=${encodeURIComponent(activeSymbol)}&interval=${encodeURIComponent(activeBi)}&limit=1`,requestKey,CHART_LIVE_CACHE_TTL_MS);if(d?.candles?.length){const c=d.candles[d.candles.length-1];applyLive({t:Number(c.time)*1000,o:c.open,h:c.high,l:c.low,c:c.close,v:c.volume});}}catch(_) {}};
        refs.timer=setInterval(refresh,3000);
        const refreshOi=async()=>{if(slot._chartRefs!==refs||!refs.lastPrice)return;try{const requestKey=`live|current-oi|${activeSymbol}`;const d=await fetchChartJsonShared(`/api/current_open_interest?symbol=${encodeURIComponent(activeSymbol)}`,requestKey,CHART_LIVE_CACHE_TTL_MS);if(!d?.time)return;const periodSeconds=({"5m":300,"15m":900,"30m":1800,"1h":3600,"4h":14400,"1d":86400})[activeOiPeriod]||300;const bucket=Math.floor(Number(d.time)/periodSeconds)*periodSeconds;const livePoint={time:bucket,value:Number(d.oi)*refs.lastPrice}; const byTime=new Map((refs.oiPoints||[]).map(p=>[Number(p.time),p])); byTime.set(bucket,livePoint); refs.oiPoints=[...byTime.values()].sort((a,b)=>a.time-b.time); refs.oiSeries.update(livePoint);}catch(_) {}};
        refreshOi(); refs.oiTimer=setInterval(refreshOi,3000);
    };

    const loadKlines=async(refs,fitContent=false,restoreRange=null,restoreLogicalRange=null)=>{
        try {
            const requestKey=`history|klines|${refs.symbol}|${bi}|${MINI_HISTORY_LIMIT}`;
            const kd=await fetchChartJsonShared(`/api/klines?symbol=${encodeURIComponent(refs.symbol)}&interval=${encodeURIComponent(bi)}&limit=${MINI_HISTORY_LIMIT}`,requestKey,CHART_HISTORY_CACHE_TTL_MS);
            if(slot._chartRefs!==refs || refs.interval!==nextInterval)return;
            if(!kd) throw new Error("Ошибка загрузки свечей");
            const cached=miniChartHistoryCache.get(cacheKey);
            const currentRange=restoreRange || (cached ? null : null);
            renderHistory(refs,kd,cached?.oiData||{points:[]},fitContent,restoreLogicalRange ? null : currentRange);
            if (restoreLogicalRange) {
                try { refs.price.timeScale().setVisibleLogicalRange(restoreLogicalRange); } catch (_) {}
                if((refs.oiPoints||[]).length) syncOiViewportToPrice(refs.price, refs.oi, refs.oiViewportExtensionSeries);
            }
            if(cached?.oiData?.points?.length){
                const cachedOiPoints=(cached.oiData.points||[])
                    .map(p=>({time:Number(p.time),value:Number(p.oiValue)}))
                    .filter(p=>[p.time,p.value].every(Number.isFinite))
                    .sort((a,b)=>a.time-b.time);
                refs.oiPoints=cachedOiPoints;
                refs.oiSeries.setData(cachedOiPoints);
            }
            // OI is loaded independently below; price/volume become usable immediately.
            return kd;
        } catch(e) {
            if(slot._chartRefs===refs && !miniChartHistoryCache.has(cacheKey)) priceEl.insertAdjacentHTML("beforeend",'<div class="muted" style="position:absolute;left:8px;top:8px;font-size:10px">Ошибка загрузки графика</div>');
            return null;
        }
    };

    const loadOi=async(refs)=>{
        try {
            const requestKey=`history|oi|${refs.symbol}|${oiPeriod}|${MINI_HISTORY_LIMIT}`;
            const od=await fetchChartJsonShared(`/api/open_interest?symbol=${encodeURIComponent(refs.symbol)}&period=${encodeURIComponent(oiPeriod)}&limit=${MINI_HISTORY_LIMIT}`,requestKey,CHART_HISTORY_CACHE_TTL_MS);
            if(slot._chartRefs!==refs || refs.interval!==nextInterval)return;
            if(Array.isArray(od?.points)){
                const oiPoints=od.points
                    .map(p=>({time:Number(p.time),value:Number(p.oiValue)}))
                    .filter(p=>[p.time,p.value].every(Number.isFinite))
                    .sort((a,b)=>a.time-b.time);
                refs.oiPoints=oiPoints;
                refs.oiSeries.setData(oiPoints);
                const cached=miniChartHistoryCache.get(cacheKey)||{klineData:{candles:[]},oiData:{points:[]}}; cached.oiData=od; miniChartHistoryCache.set(cacheKey,cached);
                const range=refs.price.timeScale().getVisibleRange(); if(oiPoints.length && range) syncOiViewportToPrice(refs.price, refs.oi, refs.oiViewportExtensionSeries, range);
            }
        } catch(_) {}
    };

    // Fast path: keep the existing chart/series. Show cached history immediately,
    // preserve the current time window, then refresh Binance data in the background.
    if(existing && existing.price && existing.oi && existing.candle && existing.vol && existing.oiSeries){
        const previousSymbol = String(existing.symbol || symbolSafe).toUpperCase();
        const previousInterval = String(existing.interval || nextInterval);
        if (chartDrawingFocus && String(selectedChartSymbol || "").toUpperCase() === previousSymbol) {
            saveSharedChartDrawings(previousSymbol, previousInterval, chartDrawings);
        }
        const previousRange=existing.price.timeScale().getVisibleRange();
        const previousLogicalRange=existing.price.timeScale().getVisibleLogicalRange();
        try { existing.ws?.close(); } catch(_) {}
        if(existing.timer) clearInterval(existing.timer);
        if(existing.oiTimer) clearInterval(existing.oiTimer);
        existing.ws=null; existing.timer=null; existing.oiTimer=null;
        ensureDrawingFutureSpace(existing.price, null, ({ "1":60,"3":180,"5":300,"15":900,"30":1800,"60":3600,"240":14400,"1D":86400 }[String(existing.interval)] || 60));
        clearMiniHistory(existing);
        existing.interval=nextInterval; existing.symbol=symbolSafe; existing.lastPrice=0; slot.dataset.chartInterval=nextInterval;
        if (String(selectedChartSymbol || "").toUpperCase() === symbolSafe) {
            chartDrawings = cloneDrawings(getSharedChartDrawings(symbolSafe, nextInterval));
            selectedDrawingIndex = -1;
            drawingEditState = null;
            redrawDrawings();
        }
        [...signalLevelStates.keys()].forEach(key=>{if(String(key).startsWith(`${symbolSafe}:`)) signalLevelStates.delete(key);});
        [...signalLevelFired].forEach(key=>{if(String(key).startsWith(`${symbolSafe}:`)) signalLevelFired.delete(key);});
        const cached=miniChartHistoryCache.get(cacheKey);
        if(cached?.klineData) renderHistory(existing,cached.klineData,cached.oiData||{points:[]},false,previousRange);
        startLive(existing);
        prefetchChartHistory(symbolSafe, nextInterval);
        const historyPromise=loadKlines(existing,false,previousRange,previousLogicalRange);
        loadOi(existing);
        await historyPromise;
        return;
    }

    // First creation only. Subsequent timeframe switches reuse these instances.
    slot.dataset.chartInterval=nextInterval;
    // Grid mini-chart: Price, Volume and OI are panes of ONE chart instance.
    const price=LightweightCharts.createChart(priceEl,{...ownChartTheme(),width:Math.max(1,priceEl.clientWidth),height:Math.max(1,priceEl.clientHeight),layout:{...ownChartTheme().layout,panes:{separatorColor:"#28313d",separatorHoverColor:"#28313d",enableResize:false}},handleScale:{mouseWheel:true,pinch:true},handleScroll:{mouseWheel:true,pressedMouseMove:true}});
    const oi=price;
    const futureSeries=price.addSeries(LightweightCharts.LineSeries,{priceScaleId:"",lineVisible:false,priceLineVisible:false,lastValueVisible:false,crosshairMarkerVisible:false,visible:false},0);
    price._drawingFutureSeries=futureSeries;
    const candle=price.addSeries(LightweightCharts.CandlestickSeries,{upColor:"#26a69a",downColor:"#ef5350",borderVisible:false,wickUpColor:"#26a69a",wickDownColor:"#ef5350"},0);
    const vol=price.addSeries(LightweightCharts.HistogramSeries,{priceFormat:{type:"volume"},priceScaleId:"volume",color:"#808890"},0);
    price.priceScale("volume").applyOptions({scaleMargins:{top:.86,bottom:0}});
    const oiSeries=price.addSeries(LightweightCharts.LineSeries,{color:"#8ab4f8",lineWidth:2,priceLineVisible:false,lastValueVisible:false,title:"",priceFormat:{type:"custom",formatter:formatCompactOI,minMove:0.01}},1);
    oiSeries.applyOptions({priceLineVisible:false,lastValueVisible:false,title:""});
    const oiViewportExtensionSeries=null;
    resizeUnifiedChartPanes(price, priceEl, 0.08);
    const refs={price,oi,candle,vol,oiSeries,oiViewportExtensionSeries,futureSeries,oiPoints:[],symbol:symbolSafe,interval:nextInterval,ws:null,timer:null,oiTimer:null,lastPrice:0}; slot._chartRefs=refs;
    installOhlcHover(price, candle, priceEl);
    installLinkedChartViewports(price, oi, refs.oiViewportExtensionSeries);
    try { refs.price.timeScale().subscribeVisibleLogicalRangeChange(() => { if (slot._chartRefs!==refs) return; if (ownPriceChart===refs.price) redrawDrawings(); scheduleOlderMiniHistory(slot, refs); }); } catch (_) {}
    startLive(refs);
    prefetchChartHistory(symbolSafe, nextInterval);
    const cached=miniChartHistoryCache.get(cacheKey);
    if(cached?.klineData) renderHistory(refs,cached.klineData,cached.oiData||{points:[]},true,null);
    const historyPromise=loadKlines(refs,!cached,null);
    loadOi(refs);
    await historyPromise;
    const resize=()=>{if(slot._chartRefs!==refs)return; resizeUnifiedChartPanes(price, priceEl, 0.08);}; refs.resize=resize; window.addEventListener("resize",resize);
}

function observeActiveDrawingGeometry() {
    if (window.__cryptoScreenerDrawingGeometryObserver) {
        try { window.__cryptoScreenerDrawingGeometryObserver.disconnect(); } catch (_) {}
    }
    const target = activeDrawingPriceElement;
    if (!target || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver(() => scheduleStableDrawingsRedraw());
    observer.observe(target);
    window.__cryptoScreenerDrawingGeometryObserver = observer;
}

// Price-scale dragging changes Lightweight Charts' price->pixel transform
// without changing the DOM size. The overlay therefore needs a lightweight
// redraw while the pointer is moving over the active chart. The RAF scheduler
// coalesces rapid pointer events into at most one redraw per frame.
if (!window.__cryptoScreenerPriceScaleRedrawBound) {
    window.__cryptoScreenerPriceScaleRedrawBound = true;
    document.addEventListener("pointermove", () => {
        if (activeDrawingPriceElement && ownPriceChart) scheduleDrawingsRedraw();
    }, {passive:true});
}
function destroyMiniCharts(){
    const slots=[...(chartGridWorkspace?.querySelectorAll(".chart-grid-slot") || [])];
    slots.forEach(slot=>destroyMiniSlotChart(slot));
    if(!chartGridWorkspace?.querySelector(".chart-grid-slot._alive")){
        // No Grid slot remains. Any own* references that pointed into Grid are
        // invalid and must not participate in large-chart reuse decisions.
        if(ownChartContext === "grid"){
            ownChartContext="none";
            ownPriceChart=null; ownOiChart=null;
            ownCandleSeries=null; ownVolumeSeries=null; ownOiSeries=null;
        }
    }
}
function buildChartGridSlot(slotIndex,symbol,initialInterval="1"){
    const slot=document.createElement("div");slot.className="chart-grid-slot";slot.dataset.chartSymbol=symbol;slot.dataset.slotIndex=String(slotIndex);
    const head=document.createElement("div");head.className="chart-grid-slot-head";
    const dot=document.createElement("span");dot.className="chart-grid-title-dot";dot.style.background=chartGridColorFor(symbol);
    const title=document.createElement("span");title.className="chart-grid-title";title.textContent=`${chartDisplaySymbol(symbol)} · B-F`;head.append(dot,title);
    const ints=document.createElement("div");ints.className="chart-grid-intervals";const startInterval=String(initialInterval||"1");["1","5","15","60","240","1D"].forEach(v=>{const b=document.createElement("button");b.type="button";b.dataset.interval=v;b.textContent={"1":"1m","5":"5m","15":"15m","60":"1h","240":"4h","1D":"1D"}[v];if(v===startInterval)b.classList.add("active");b.addEventListener("click",e=>{e.stopPropagation();ints.querySelectorAll("button").forEach(x=>x.classList.remove("active"));b.classList.add("active");selectedChartInterval=v;selectedChartSymbol=symbol;loadMiniChart(slot,symbol,v);if(chartGridMaximized>=0)chartIntervals.querySelectorAll(".chart-interval").forEach(x=>x.classList.toggle("active",x.dataset.interval===v));requestAnimationFrame(()=>resizeOwnChartsToContainer());});ints.appendChild(b);});head.appendChild(ints);
    const flagWrap=document.createElement("div");flagWrap.className="chart-grid-flag-wrap";
    const flag=document.createElement("button");flag.type="button";flag.className="chart-grid-flag";flag.title="Флажок";flag.setAttribute("aria-label","Флажок");
    flag.innerHTML='<svg viewBox="0 0 24 24" aria-hidden="true"><path class="flag-pole" d="M6 4v17"/><path class="flag-cloth" d="M7 5h11v12l-5.5-4-5.5 4V5z"/></svg>';
    const palette=document.createElement("div");palette.className="chart-grid-flag-palette";palette.hidden=true;
    palette.innerHTML=WATCHLIST_COLORS.map(color=>`<button type="button" class="watchlist-color-option" data-grid-flag-color="${color}" title="${color}" style="background:${color}"></button>`).join("")+`<button type="button" class="watchlist-color-clear" data-grid-flag-clear="1" title="Убрать цвет"></button>`;
    flagWrap.append(flag,palette);head.appendChild(flagWrap);slot.appendChild(head);
    const syncMiniFlag=()=>{const c=watchlistColorFor(symbol);flag.classList.toggle("has-color",!!c);if(c)flag.style.setProperty("--chart-grid-flag-color",c);else flag.style.removeProperty("--chart-grid-flag-color");};
    syncMiniFlag();
    flag.addEventListener("pointerdown",e=>{e.preventDefault();e.stopPropagation();});
    flag.addEventListener("click",e=>{
        e.preventDefault();e.stopPropagation();
        const key=String(symbol||"").toUpperCase();
        const saved=watchlistColorFor(key);
        if(saved){ commitWatchlistColor(key,null); syncMiniFlag(); return; }
        if(watchlistOpenPaletteSymbol && watchlistOpenPaletteSymbol!==key) closeWatchlistPalette(true);
        watchlistOpenPaletteSymbol=key; watchlistPendingColor=WATCHLIST_COLORS[0]; watchlistPendingExplicitClear=false;
        flag.classList.add("has-color"); flag.style.setProperty("--chart-grid-flag-color",WATCHLIST_COLORS[0]); palette.hidden=false;
    });
    palette.addEventListener("click",e=>{
        e.preventDefault();e.stopPropagation();
        const option=e.target.closest("[data-grid-flag-color]");
        if(option){ watchlistPendingColor=option.dataset.gridFlagColor; watchlistPendingExplicitClear=false; flag.classList.add("has-color"); flag.style.setProperty("--chart-grid-flag-color",watchlistPendingColor); return; }
        if(e.target.closest("[data-grid-flag-clear]")){ watchlistPendingColor=null; watchlistPendingExplicitClear=true; flag.classList.remove("has-color"); flag.style.removeProperty("--chart-grid-flag-color"); }
    });
    const body=document.createElement("div");body.className="chart-grid-card-body";const pe=document.createElement("div");pe.className="chart-grid-price";const oe=document.createElement("div");oe.className="chart-grid-oi";body.append(pe,oe);slot.appendChild(body);
    const expand=document.createElement("button"); expand.type="button"; expand.className="chart-grid-expand"; expand.textContent="⛶"; expand.title="Развернуть график"; expand.setAttribute("aria-label","Развернуть график");
    expand.addEventListener("click",e=>{e.stopPropagation();toggleChartGridMaximize(slot);});
    slot.appendChild(expand);
    slot.addEventListener("pointerdown",e=>{if(e.target.closest(".chart-grid-flag-wrap,.chart-grid-flag-palette"))return;activateGridSlot(slot);});chartGridWorkspace.appendChild(slot);loadMiniChart(slot,symbol,startInterval);return slot;
}
function destroyMiniSlotChart(slot){
    const r=slot?._chartRefs;
    if(!r)return;
    try{r.ws?.close();}catch(_){}
    if(r.timer)clearInterval(r.timer);
    if(r.oiTimer)clearInterval(r.oiTimer);
    if(r.resize){try{window.removeEventListener("resize",r.resize);}catch(_){} r.resize=null;}
    const wasOwnPrice = ownPriceChart === r.price;
    const wasOwnOi = ownOiChart === r.oi;
    try{r.price?.remove();}catch(_){}
    if (r.oi && r.oi !== r.price) { try{r.oi.remove();}catch(_){} }
    slot._chartRefs=null;
    if(wasOwnPrice) ownPriceChart=null;
    if(wasOwnOi) ownOiChart=null;
    if(wasOwnPrice || !ownPriceChart){ownCandleSeries=null;ownVolumeSeries=null;}
    if(wasOwnOi || !ownOiChart) ownOiSeries=null;
    if(wasOwnPrice || wasOwnOi) ownChartContext="none";
}
function saveFocusedGridDrawingState(){
    if(!chartDrawingFocus || !selectedChartSymbol) return;
    const activeSlot = chartGridWorkspace?.querySelector(`.chart-grid-slot[data-chart-symbol="${CSS.escape(selectedChartSymbol)}"]`);
    const interval = activeSlot?._chartRefs?.interval || activeSlot?.dataset.chartInterval || selectedChartInterval || "1";
    saveSharedChartDrawings(selectedChartSymbol, interval, chartDrawings);
}
function loadFocusedGridDrawingState(symbol, interval = selectedChartInterval){
    chartDrawings = cloneDrawings(getSharedChartDrawings(symbol, interval));
    selectedDrawingIndex = -1;
    drawingEditState = null;
}
async function notifyOrderbookPriority(symbol){
    const s=String(symbol||"").toUpperCase();
    if(!s.endsWith("USDT")) return;
    try{await fetch("/api/orderbook/active",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({symbol:s}),keepalive:true});}catch(_){}
}
function activateGridSlot(slot){
    if(!slot) return; saveFocusedGridDrawingState();
    chartGridWorkspace?.querySelectorAll('.chart-grid-slot').forEach(x=>x.classList.remove('active-slot')); slot.classList.add('active-slot');
    const symbol=slot.dataset.chartSymbol||'', refs=slot._chartRefs; if(!symbol) return;
    selectedChartSymbol=symbol; syncSelectedMarketRow(symbol); selectedChartInterval=refs?.interval || slot.dataset.chartInterval || selectedChartInterval || "1"; chartTitle.textContent=`${chartDisplaySymbol(symbol)} · B-F`; chartTitleDot?.style.setProperty('background',chartGridColorFor(symbol)); notifyOrderbookPriority(symbol);
    renderChartColorPalette(symbol);
    syncChartFlagForSymbol(symbol);
    activeDrawingPriceElement=slot.querySelector('.chart-grid-price')||priceChartEl; chartDrawingFocus=true; observeActiveDrawingGeometry();
    if(refs){ ownChartContext = "grid"; ownPriceChart=refs.price; ownOiChart=refs.oi; ownCandleSeries=refs.candle; ownVolumeSeries=refs.vol; ownOiSeries=refs.oiSeries; ownLastPrice=refs.lastPrice||0; loadFocusedGridDrawingState(symbol, selectedChartInterval); redrawDrawings(); const overlayTf=ownChartIntervalToBinance(selectedChartInterval || refs.interval || slot.dataset.chartInterval || "1");
        if (isChartOverlayAnalysisEnabled()) {
            ensureChartOverlayAnalysis(symbol,overlayTf).then(()=>{
                if (!isChartOverlayAnalysisEnabled()) return;
                if (selectedChartSymbol===symbol) redrawDrawings();
            });
        } }
}
function applyChartGridLayout(){
    if(!chartGridWorkspace) return;
    chartGridMaximized=-1; chartGridWorkspace.classList.remove('has-maximized'); document.getElementById('chartModal')?.classList.remove('grid-maximized'); setChartLargeMode(false);
    const content=document.getElementById('chartContent') || document.querySelector('.chart-content');
    if(chartGridRows===1&&chartGridCols===1){
        const activeMiniSlot = chartGridWorkspace.querySelector('.chart-grid-slot.active-slot');
        const activeMiniRefs = activeMiniSlot?._chartRefs || null;
        if(activeMiniRefs?.price){
            let visibleRange = null;
            try { visibleRange = activeMiniRefs.price.timeScale().getVisibleRange() || null; } catch (_) {}
            if(visibleRange){
                pendingOwnChartOpenRange = {
                    symbol: String(activeMiniRefs.symbol || activeMiniSlot.dataset.chartSymbol || '').toUpperCase(),
                    interval: String(activeMiniRefs.interval || activeMiniSlot.dataset.chartInterval || '1'),
                    range: visibleRange
                };
            }
        }
        destroyMiniCharts(); chartGridWorkspace.innerHTML=''; chartGridWorkspace.style.display='none'; content?.classList.remove('grid-active');
        chartGridPagination?.classList.remove('visible');
        activeDrawingPriceElement=priceChartEl; chartDrawingFocus=true; ownChartContext="none";
        if(selectedChartSymbol) requestAnimationFrame(()=>loadChart(selectedChartSymbol,selectedChartInterval||'1'));
        return;
    }
    content?.classList.add('grid-active'); closeOwnChart(); chartGridWorkspace.style.display='grid';
    chartGridWorkspace.style.gridTemplateColumns=`repeat(${chartGridCols},minmax(0,1fr))`; chartGridWorkspace.style.gridTemplateRows=`repeat(${chartGridRows},minmax(0,1fr))`;
    chartGridPage=0;
    renderChartGridPage(chartGridPage);
}




// Density runtime constants — browser-side values.
const DENSITY_UI_REFRESH_MS = 500;
const DENSITY_CHART_MAX_ITEMS = 12;
let DENSITY_SETTINGS = { enabled:true, show_on_chart:true, show_in_screener:true, show_consumed:true, show_remaining:true, show_lifetime:true, show_spot:true, show_futures:true, exchanges:["binance","bybit","okx"], strength_min:0 };
const DENSITY_CHART_VISIBILITY_STORAGE = "cryptoScreenerDensityChartVisibilityV1";
let densityChartVisible = true;
try {
    const savedDensityChartVisible = localStorage.getItem(DENSITY_CHART_VISIBILITY_STORAGE);
    if (savedDensityChartVisible !== null) densityChartVisible = savedDensityChartVisible === "true";
} catch (_) {}
const DENSITY_MAP_ZOOM_STORAGE = "cryptoScreenerDensityMapZoomV1";
let densityMapZoomMax = 3;
try {
    const savedDensityZoom = Number(localStorage.getItem(DENSITY_MAP_ZOOM_STORAGE));
    if (Number.isFinite(savedDensityZoom) && savedDensityZoom > 0) densityMapZoomMax = savedDensityZoom;
} catch (_) {}
function saveDensityChartVisibility(){ try { localStorage.setItem(DENSITY_CHART_VISIBILITY_STORAGE, densityChartVisible ? "true" : "false"); } catch (_) {} }
function saveDensityMapZoom(){ try { localStorage.setItem(DENSITY_MAP_ZOOM_STORAGE, String(densityMapZoomMax)); } catch (_) {} }

const DENSITY_DOCK_DEFAULT_HEIGHT = 360;
const DENSITY_DOCK_MIN_HEIGHT = 170;
const DENSITY_DOCK_MAX_HEIGHT = 620;
const DENSITY_DOCK_STORAGE = "crypto_screener_density_dock_height_v2";

function initDensityDock(){
    const market=document.querySelector(".market-panel"), handle=document.getElementById("densityWorkspaceResize"), dock=document.getElementById("densityWorkspace");
    if(!market||!handle||!dock)return;
    let saved=DENSITY_DOCK_DEFAULT_HEIGHT;
    try{saved=Math.max(DENSITY_DOCK_MIN_HEIGHT,Math.min(DENSITY_DOCK_MAX_HEIGHT,Number(localStorage.getItem(DENSITY_DOCK_STORAGE))||DENSITY_DOCK_DEFAULT_HEIGHT));}catch(_){}
    dock.style.height=`${saved}px`; dock.style.flexBasis=`${saved}px`;
    let dragging=false,startY=0,startH=saved;
    const move=e=>{
        if(!dragging)return;
        const rect=market.getBoundingClientRect();
        const maxAllowed=Math.min(DENSITY_DOCK_MAX_HEIGHT,Math.max(DENSITY_DOCK_MIN_HEIGHT,rect.height-110));
        const next=Math.max(DENSITY_DOCK_MIN_HEIGHT,Math.min(maxAllowed,startH-(e.clientY-startY)));
        dock.style.height=`${next}px`; dock.style.flexBasis=`${next}px`;
        try{localStorage.setItem(DENSITY_DOCK_STORAGE,String(Math.round(next)));}catch(_){}
    };
    const up=()=>{if(!dragging)return;dragging=false;handle.classList.remove("dragging");document.body.classList.remove("density-workspace-resizing");window.removeEventListener("pointermove",move);window.removeEventListener("pointerup",up);};
    handle.addEventListener("pointerdown",e=>{
        e.preventDefault(); dragging=true; startY=e.clientY; startH=dock.getBoundingClientRect().height;
        handle.classList.add("dragging"); document.body.classList.add("density-workspace-resizing");
        window.addEventListener("pointermove",move); window.addEventListener("pointerup",up);
    });
}
initDensityDock();

// Multi-exchange radar — Stage 6
function renderDensityExchangeRadar(items){
    const panel=document.getElementById("densityExchangeRadar");
    const list=document.getElementById("densityExchangeRadarList");
    if(!panel||!list)return;
    if(!(window.workspaceViews&&window.workspaceViews.density)){panel.hidden=true;return;}
    panel.hidden=true;
    const rows=Array.isArray(items)?items:[];
    const status=document.getElementById("densityExchangeRadarStatus");
    if(status)status.textContent=rows.length?`${rows.length} зон`:"нет";
    list.innerHTML=rows.map(x=>{
        const multi=Number(x.exchange_count||0)>=2;
        return `<div class="density-exchange-radar-row ${multi?"multi":""}">
          <b>${densityEsc(x.symbol)}</b>
          <span>${densityEsc(x.side)}</span>
          <span>${Number(x.price||0).toLocaleString(undefined,{maximumFractionDigits:8})}</span>
          <span>${densityMoney(x.current_usd)}</span>
          <span>${(x.exchanges||[]).map(densityEsc).join(" · ")}</span>
          <span>${multi?"MULTI":"single"}</span>
        </div>`;
    }).join("");
}
async function refreshDensityExchangeHealth(){
    const panel=document.getElementById("densityExchangeHealth");
    if(!panel)return;
    if(!(window.workspaceViews&&window.workspaceViews.density)){panel.hidden=true;return;}
    try{
        const r=await fetch("/api/density/health",{cache:"no-store"});
        if(!r.ok)return;
        const data=await r.json();
        panel.hidden=true;
        const status=document.getElementById("densityExchangeHealthStatus");
        const rows=(data.exchanges||[]).map(x=>`${densityEsc(x.exchange)}: ${x.fresh} fresh / ${x.stale} stale`).join(" · ");
        if(status)status.innerHTML=rows||"нет данных";
    }catch(_){}
}
let densityHealthTimer=null;

// Density settings / screener — Stage 5
async function loadDensitySettings(){
    try{
        const r=await fetch("/api/density/settings",{cache:"no-store"});
        if(!r.ok)return;
        const s=await r.json();
        DENSITY_SETTINGS = Object.assign({}, DENSITY_SETTINGS, s || {});
        const serverChartEnabled = s.show_on_chart !== false;
        if (!localStorage.getItem(DENSITY_CHART_VISIBILITY_STORAGE)) densityChartVisible = serverChartEnabled;
        syncChartOverlayToggleButtons();
        const q=id=>document.getElementById(id);
        if(q("densitySettingEnabled"))q("densitySettingEnabled").checked=!!s.enabled;
        if(q("densitySettingInterestFilter"))q("densitySettingInterestFilter").checked=!!s.interest_filter_enabled;
        if(q("densitySettingFilterAll"))q("densitySettingFilterAll").checked=!!s.filter_all_symbols;
        if(q("densitySettingInterestVolume"))q("densitySettingInterestVolume").value=s.interest_min_volume_usd??0;
        if(q("densitySettingMinUsd"))q("densitySettingMinUsd").value=s.min_usd??400000;
        if(q("densitySettingMinDistance"))q("densitySettingMinDistance").value=s.min_distance_percent??0.05;
        if(q("densitySettingMaxDistance"))q("densitySettingMaxDistance").value=s.max_distance_percent??3;
        if(q("densitySettingChart"))q("densitySettingChart").checked=!!s.show_on_chart;
        if(q("densitySettingScreener"))q("densitySettingScreener").checked=!!s.show_in_screener;
        if(q("densitySettingConsumed"))q("densitySettingConsumed").checked=s.show_consumed !== false;
        if(q("densitySettingRemaining"))q("densitySettingRemaining").checked=s.show_remaining !== false;
        if(q("densitySettingLifetime"))q("densitySettingLifetime").checked=s.show_lifetime !== false;
        if(q("densitySettingSpot"))q("densitySettingSpot").checked=s.show_spot !== false;
        if(q("densitySettingFutures"))q("densitySettingFutures").checked=s.show_futures !== false;
        const ex=new Set((s.exchanges||["binance","bybit","okx"]).map(x=>String(x).toLowerCase()));
        if(q("densitySettingBinance"))q("densitySettingBinance").checked=ex.has("binance");
        if(q("densitySettingBybit"))q("densitySettingBybit").checked=ex.has("bybit");
        if(q("densitySettingOkx"))q("densitySettingOkx").checked=ex.has("okx");
        if(q("densitySettingStrengthMin"))q("densitySettingStrengthMin").value=s.strength_min??0;
        if(q("densitySettingBlacklist"))q("densitySettingBlacklist").value=(s.blacklist||[]).join(", ");
    }catch(_){}
}
async function saveDensitySettings(){
    const q=id=>document.getElementById(id);
    const blacklist=String(q("densitySettingBlacklist")?.value||"").split(",").map(x=>x.trim().toUpperCase()).filter(Boolean);
    const payload={
        enabled:!!q("densitySettingEnabled")?.checked,
        runtime_enabled:!!(window.workspaceViews&&window.workspaceViews.density),
        interest_filter_enabled:!!q("densitySettingInterestFilter")?.checked,
        filter_all_symbols:!!q("densitySettingFilterAll")?.checked,
        interest_min_volume_usd:Number(q("densitySettingInterestVolume")?.value||0),
        min_usd:Number(q("densitySettingMinUsd")?.value||0),
        min_distance_percent:Number(q("densitySettingMinDistance")?.value||0),
        max_distance_percent:Number(q("densitySettingMaxDistance")?.value||0),
        show_on_chart:!!q("densitySettingChart")?.checked,
        show_in_screener:!!q("densitySettingScreener")?.checked,
        show_consumed:!!q("densitySettingConsumed")?.checked,
        show_remaining:!!q("densitySettingRemaining")?.checked,
        show_lifetime:!!q("densitySettingLifetime")?.checked,
        show_spot:!!q("densitySettingSpot")?.checked,
        show_futures:!!q("densitySettingFutures")?.checked,
        strength_min:Math.max(0,Math.min(100,Number(q("densitySettingStrengthMin")?.value||0))),
        exchanges:[q("densitySettingBinance")?.checked?"binance":null,q("densitySettingBybit")?.checked?"bybit":null,q("densitySettingOkx")?.checked?"okx":null].filter(Boolean),
        blacklist
    };
    try{
        const r=await fetch("/api/density/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});
        const data=await r.json();
        if(data && data.ok && data.settings) DENSITY_SETTINGS = Object.assign({}, DENSITY_SETTINGS, data.settings);
        const status=q("densitySettingsStatus");
        if(status)status.textContent=data.ok?"Сохранено":"Ошибка";
        if(data.ok){
            DENSITY_SETTINGS = Object.assign({}, DENSITY_SETTINGS, data.settings || {});
            const minDist=Math.max(0,Number(DENSITY_SETTINGS.min_distance_percent ?? 0.05));
            const maxDist=Math.max(minDist,Number(DENSITY_SETTINGS.max_distance_percent ?? 3));
            densityMapZoomMax=Math.max(minDist,Math.min(maxDist,densityMapZoomMax));
            saveDensityMapZoom();
            refreshDensityPresentation();
            requestDensityChartRender(densityPresentationData);
        }
    }catch(_){
        const status=q("densitySettingsStatus");
        if(status)status.textContent="Ошибка";
    }
}
async function refreshDensityScreener(){
    const panel=document.getElementById("densityScreenerPanel");
    const list=document.getElementById("densityScreenerList");
    if(!panel||!list)return;
    if(!(window.workspaceViews&&window.workspaceViews.density)){panel.hidden=true;return;}
    try{
        const r=await fetch("/api/densities/screener",{cache:"no-store"});
        if(!r.ok)return;
        const data=await r.json();
        panel.hidden=true;
        const rows=Array.isArray(data.rows)?data.rows:[];
        const status=document.getElementById("densityScreenerStatus");
        if(status)status.textContent=rows.length?`${rows.length} монет`:"нет";
        list.innerHTML=rows.map(x=>`<div class="density-screener-row">
          <b>${densityEsc(x.symbol)}</b>
          <span>${x.density_count} плотн.</span>
          <span>BUY ${x.buy_count} / SELL ${x.sell_count}</span>
          <span>ближайшая ${x.nearest_distance_percent==null?"—":x.nearest_distance_percent.toFixed(2)+"%"}</span>
          <span>max ${densityMoney(x.max_usd)}</span>
          <span>разъели ${x.max_consumed_percent.toFixed(1)}%</span>
        </div>`).join("");
    }catch(_){}
}
loadDensitySettings();
document.getElementById("densitySettingsSave")?.addEventListener("click",saveDensitySettings);
let densityScreenerTimer=null;

// Density presentation — Stage 4
let densityPresentationTimer=null;
let densityPresentationData=[];

function densityMoney(v){
    const n=Number(v||0);
    if(!Number.isFinite(n)) return "$0";
    if(Math.abs(n)>=1e9)return "$"+(n/1e9).toFixed(2)+"B";
    if(Math.abs(n)>=1e6)return "$"+(n/1e6).toFixed(2)+"M";
    if(Math.abs(n)>=1e3)return "$"+(n/1e3).toFixed(1)+"K";
    return "$"+n.toFixed(0);
}
function densityLife(d){
    const sec=Number(d?.lifetime_seconds||0);
    return sec<60?"< 1 мин":Math.floor(sec/60)+" мин";
}
function densityEsc(v){
    return String(v??"").replace(/[&<>"']/g,m=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[m]));
}
function renderDensityMap(items){
    const workspace=document.getElementById("densityWorkspace"), list=document.getElementById("densityMapList"), status=document.getElementById("densityMapStatus"), scale=document.getElementById("densityDistanceScale");
    if(!workspace||!list)return;
    const enabled=!!(window.workspaceViews&&window.workspaceViews.density); workspace.hidden=!enabled;
    if(!enabled){list.innerHTML="";return;}
    const minDist=Math.max(0,Number(DENSITY_SETTINGS?.min_distance_percent ?? 0.05));
    const maxDist=Math.max(minDist,Number(DENSITY_SETTINGS?.max_distance_percent ?? 3));
    densityMapZoomMax=Math.max(minDist,Math.min(maxDist,Number(densityMapZoomMax)||maxDist));
    const viewMax=densityMapZoomMax;
    const minStrength=Math.max(0,Number(DENSITY_SETTINGS?.strength_min ?? 0));
    const source=(Array.isArray(items)?items:[]).filter(d=>Number.isFinite(Number(d.distance_percent)) && Math.abs(Number(d.distance_percent))<=viewMax && Math.abs(Number(d.distance_percent))>=minDist && Number(d.strength_score||0)>=minStrength);
    if(scale)scale.textContent=`спред 0% · вид 0–${viewMax.toFixed(2)}% · фильтр ${minDist.toFixed(2)}–${maxDist.toFixed(2)}%`;
    const groups=[
        {name:"Малые",cls:"small",items:[]},
        {name:"Средние",cls:"medium",items:[]},
        {name:"Большие",cls:"large",items:[]}
    ];
    const maxUsd=Math.max(1,...source.map(d=>Number(d.initial_usd||d.current_usd||0)));
    source.forEach(d=>{
        const usd=Number(d.initial_usd||d.current_usd||0);
        const ratio=usd/maxUsd;
        groups[ratio<0.33?0:(ratio<0.66?1:2)].items.push(d);
    });
    const esc=densityEsc;
    const fmtPrice=v=>Number(v||0).toLocaleString(undefined,{maximumFractionDigits:8});
    const card=(d)=>{
        const dist=Math.abs(Number(d.distance_percent||0));
        const side=String(d.side||"").toLowerCase();
        const remaining=Number(d.remaining_percent ?? Math.max(0,100-Number(d.consumed_percent||0)));
        const consumed=Number(d.consumed_percent||0);
        const top=50 + (side==="buy" ? 1 : -1) * Math.min(48,Math.max(0,dist/viewMax*48));
        const consumedText = DENSITY_SETTINGS?.show_consumed === false ? "" : ` · разъели ${consumed.toFixed(0)}%`;
        const remainingText = DENSITY_SETTINGS?.show_remaining === false ? "" : `ост. ${remaining.toFixed(0)}%`;
        const lifetimeText = DENSITY_SETTINGS?.show_lifetime === false ? "" : densityLife(d);
        const sourceLabel=esc(d.exchange_label||densitySourceLabelFallback(d.exchange));
        const direction=side==="buy"?"BUY":"SELL";
        const tooltip=[
            `${sourceLabel} · ${esc(d.symbol)}`,
            `Направление: ${direction}`,
            `Цена: ${fmtPrice(d.price)}`,
            `Размер: ${densityMoney(d.current_usd)}`,
            `Остаток: ${remaining.toFixed(1)}%`,
            `Разъели: ${consumed.toFixed(1)}%`,
            `В стакане: ${densityLife(d)}`,
            `Дистанция от спреда: ${dist.toFixed(2)}%`
        ].join("\n");
        return `<button type="button" class="density-map-card ${side}" data-density-symbol="${esc(d.symbol)}" data-density-title="${tooltip}" style="top:${top.toFixed(2)}%" title="${tooltip}">
            <span class="density-map-card-title"><b>${sourceLabel}</b> ${esc(d.symbol)}</span>
            <span class="density-map-card-price">${fmtPrice(d.price)}</span>
            <span class="density-map-card-size">${densityMoney(d.current_usd)} · ${dist.toFixed(2)}%</span>
            <span class="density-map-card-extra">${remainingText}${consumedText}${lifetimeText ? ` · ${lifetimeText}` : ""}</span>
        </button>`;
    };
    const tickMax=viewMax;
    const tickVals=[tickMax,tickMax*0.5,0,-tickMax*0.5,-tickMax];
    const tickHtml=tickVals.map((v,i)=>`<span class="density-axis-tick ${i===2?"zero":""}">${v>0?"+":""}${v.toFixed(2)}%</span>`).join("");
    list.innerHTML=`<div class="density-map-axis"><span class="density-axis-top">SELL</span><div class="density-axis-ticks">${tickHtml}</div><span class="density-axis-zero">0%</span><span class="density-axis-bottom">BUY</span></div>`+
      groups.map(g=>`<section class="density-map-column ${g.cls}"><div class="density-map-column-head"><span>${g.name}</span><span>${g.items.length}</span></div><div class="density-map-column-body"><div class="density-map-grid-lines"><i></i><i></i><i></i><i></i><i></i></div><div class="density-map-zero"></div>${g.items.map(card).join("")||'<div class="density-map-empty">нет плотностей</div>'}</div></section>`).join("");
    list.querySelectorAll(".density-map-card[data-density-symbol]").forEach(cardEl=>cardEl.addEventListener("click",()=>openChart(cardEl.dataset.densitySymbol)));
}
function densitySourceLabelFallback(exchange){
    const e=String(exchange||"").toLowerCase();
    const base=e.endsWith("_spot")?e.slice(0,-5):e, spot=e.endsWith("_spot");
    return `${base==="binance"?"B":base==="bybit"?"BY":base==="okx"?"OK":base.toUpperCase().slice(0,3)}-${spot?"S":"F"}`;
}

function densityChartRenderEnabled(){
    return !!(window.workspaceViews?.density && densityChartVisible && DENSITY_SETTINGS?.enabled !== false && DENSITY_SETTINGS?.show_on_chart !== false);
}
function requestDensityChartRender(items = densityPresentationData){
    if (!densityChartRenderEnabled()) {
        const host = document.getElementById("densityChartOverlay");
        if (host) host.replaceChildren();
        return;
    }
    renderDensityChartOverlay(items);
}

function renderDensityChartOverlay(items){
    const host=document.getElementById("densityChartOverlay");
    const chartEl=document.getElementById("priceChart");
    if(!host||!chartEl)return;
    if(host.parentElement!==chartEl) chartEl.appendChild(host);
    host.innerHTML="";
    if(!densityChartRenderEnabled()) return;
    const active=String(selectedChartSymbol||"").toUpperCase();
    const filtered=(Array.isArray(items)?items:[]).filter(d=>String(d.symbol||"").toUpperCase()===active);
    const series=ownCandleSeries||null;
    if(!series||typeof series.priceToCoordinate!=="function")return;
    const chartHeight=Math.max(1,chartEl.clientHeight);
    const esc=densityEsc;
    filtered.slice(0,DENSITY_CHART_MAX_ITEMS).forEach(d=>{
        const y=series.priceToCoordinate(Number(d.price||0));
        if(!Number.isFinite(y)||y<0||y>chartHeight)return;
        const side=String(d.side||"").toLowerCase();
        const pill=document.createElement("div");
        pill.className=`density-chart-pill ${side}`;
        pill.style.top=`${Math.max(12,Math.min(chartHeight-12,y))}px`;
        const sourceLabel=esc(d.exchange_label||densitySourceLabelFallback(d.exchange));
        const price=Number(d.price||0).toLocaleString(undefined,{maximumFractionDigits:8});
        const usd=densityMoney(d.current_usd);
        const life=densityLife(d);
        const remaining=Number(d.remaining_percent??(100-Number(d.consumed_percent||0)));
        const infoParts=[price];
        if(DENSITY_SETTINGS?.show_lifetime !== false) infoParts.push(life);
        infoParts.push(`${Math.abs(Number(d.distance_percent||0)).toFixed(2)}%`);
        if(DENSITY_SETTINGS?.show_remaining !== false) infoParts.push(`ост. ${remaining.toFixed(0)}%`);
        if(DENSITY_SETTINGS?.show_consumed !== false) infoParts.push(`разъели ${(Number(d.consumed_percent||0)).toFixed(0)}%`);
        pill.innerHTML=`<div class="density-main">${sourceLabel} · ${usd}</div><div class="density-sub">${infoParts.join(" · ")}</div>`;
        host.appendChild(pill);
    });
}
const densityMapPanelEl = document.getElementById("densityMapPanel");
densityMapPanelEl?.addEventListener("wheel", event => {
    if (!(window.workspaceViews?.density)) return;
    const minDist=Math.max(0,Number(DENSITY_SETTINGS?.min_distance_percent ?? 0.05));
    const maxDist=Math.max(minDist,Number(DENSITY_SETTINGS?.max_distance_percent ?? 3));
    const factor=event.deltaY<0 ? 0.82 : 1.22;
    const next=Math.max(minDist,Math.min(maxDist,densityMapZoomMax*factor));
    if (Math.abs(next-densityMapZoomMax)<0.0001) return;
    event.preventDefault();
    densityMapZoomMax=next;
    saveDensityMapZoom();
    renderDensityMap(densityPresentationData);
},{passive:false});

let densityPresentationFirstLoad = true;
let densityPresentationRequestInFlight = false;
async function refreshDensityPresentation(){
    if (densityPresentationRequestInFlight) return;
    if (!(window.workspaceViews?.density)) return;
    densityPresentationRequestInFlight = true;
    if (densityPresentationFirstLoad) {
        setDensityMapStatus("loading", "Плотности загружаются…");
        const list = document.getElementById("densityMapList");
        if (list && !list.children.length) {
            list.innerHTML = '<div class="density-map-empty">Плотности загружаются…</div>';
        }
    }
    try{
        const r=await fetch("/api/densities/presentation",{cache:"no-store"});
        if(!r.ok) throw new Error(`HTTP ${r.status}`);
        const data=await r.json();
        if (!data || typeof data !== "object") throw new Error("Некорректный ответ сервера");
        densityPresentationData=Array.isArray(data.map)?data.map:[];
        renderDensityMap(densityPresentationData);
        requestDensityChartRender(data.enabled ? (Array.isArray(data.chart)?data.chart:[]) : []);
        renderDensityExchangeRadar(Array.isArray(data.aggregated)?data.aggregated:[]);
        if (data.enabled === false) {
            setDensityMapStatus("empty", "Плотности выключены");
        } else if (densityPresentationData.length) {
            setDensityMapStatus("ready", `Загружено · ${densityPresentationData.length}`);
        } else {
            setDensityMapStatus("empty", "Загружено · плотностей нет");
        }
        densityPresentationFirstLoad = false;
    }catch(error){
        console.warn("Density presentation error:", error);
        setDensityMapStatus("error", "Ошибка загрузки плотностей");
        const list = document.getElementById("densityMapList");
        if (list) list.innerHTML = `<div class="density-map-empty">Ошибка загрузки плотностей: ${densityEsc(error?.message || error)}</div>`;
    }finally{
        densityPresentationRequestInFlight = false;
    }
}
function startDensityPresentation(){
    if(densityPresentationTimer)clearInterval(densityPresentationTimer);
    densityPresentationTimer=setInterval(()=>{
        if(window.workspaceViews?.density) refreshDensityPresentation();
    },DENSITY_UI_REFRESH_MS);
    if(window.workspaceViews?.density) refreshDensityPresentation();
}
function stopDensityPresentation(){
    if(densityPresentationTimer){clearInterval(densityPresentationTimer);densityPresentationTimer=null;}
}
function setDensityUiRuntime(enabled){
    if(enabled){
        startDensityPresentation();
        if(densityHealthTimer)clearInterval(densityHealthTimer);
        densityHealthTimer=setInterval(()=>{if(window.workspaceViews?.density)refreshDensityExchangeHealth();},Math.max(1000,DENSITY_UI_REFRESH_MS*4));
        if(densityScreenerTimer)clearInterval(densityScreenerTimer);
        densityScreenerTimer=setInterval(()=>{if(window.workspaceViews?.density)refreshDensityScreener();},DENSITY_UI_REFRESH_MS);
        refreshDensityExchangeHealth();
        refreshDensityScreener();
    }else{
        stopDensityPresentation();
        if(densityHealthTimer){clearInterval(densityHealthTimer);densityHealthTimer=null;}
        if(densityScreenerTimer){clearInterval(densityScreenerTimer);densityScreenerTimer=null;}
    }
}

function applyWorkspaceViews() {
    window.workspaceViews = workspaceViews;
    workspaceViews.coinList = !!viewCoinList?.checked;
    workspaceViews.alerts = !!viewAlerts?.checked;
    workspaceViews.density = !!viewDensity?.checked;
    const densityWorkspace = document.getElementById("densityWorkspace");
    if (densityWorkspace) densityWorkspace.hidden = !workspaceViews.density;
    const marketNeeded = workspaceViews.coinList || workspaceViews.density;
    marketPanel?.classList.toggle("workspace-hidden", !marketNeeded);
    workspaceDivider?.classList.toggle("workspace-hidden", !marketNeeded);
    workspaceLayout?.classList.toggle("market-hidden", !marketNeeded);
    marketPanel?.classList.toggle("density-only", !workspaceViews.coinList && workspaceViews.density);
    document.querySelector(".market-panel .table-wrap")?.classList.toggle("density-source-hidden", !workspaceViews.coinList);

    // When Market Screener is hidden, the chart workspace must immediately
    // reclaim the entire horizontal area. Lightweight Charts need an explicit
    // resize after the grid column changes; otherwise they keep their old
    // canvas dimensions until another resize event happens.
    requestAnimationFrame(() => {
        resizeOwnChartsToContainer();
    });
    document.querySelectorAll("[data-alert-center-tab=\"alerts\"]").forEach(el => {
        el.classList.toggle("alert-tab-hidden", !workspaceViews.alerts);
    });
    document.getElementById("alertCenterAlertsView")?.classList.toggle("alert-view-hidden", !workspaceViews.alerts);
    if (!workspaceViews.alerts && document.getElementById("alertCenterNotificationsView")) {
        document.getElementById("alertCenterNotificationsView").classList.add("active");
        document.getElementById("alertCenterAlertsView")?.classList.remove("active");
    }
    try { localStorage.setItem(WORKSPACE_VIEW_STORAGE, JSON.stringify(workspaceViews)); } catch (_) {}
}

if (viewCoinList) viewCoinList.checked = workspaceViews.coinList;
if (viewAlerts) viewAlerts.checked = workspaceViews.alerts;
if (viewDensity) viewDensity.checked = workspaceViews.density;
async function setDensityRuntimeEnabled(enabled){
    try{
        const r=await fetch("/api/density/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({runtime_enabled:!!enabled})});
        if(r.ok){
            const data=await r.json();
            if(data?.settings) DENSITY_SETTINGS=Object.assign({},DENSITY_SETTINGS,data.settings);
        }
    }catch(error){
        console.warn("Density runtime switch error:",error);
    }
}
viewDensity?.addEventListener("change", () => {
    workspaceViews.density = !!viewDensity.checked;
    applyWorkspaceViews();
    if(viewDensity.checked){
        setDensityUiRuntime(true);
        setDensityRuntimeEnabled(true);
        densityPresentationFirstLoad = true;
        setDensityMapStatus("loading", "Плотности загружаются…");
        refreshDensityPresentation();
        refreshDensityExchangeHealth();
    } else {
        setDensityUiRuntime(false);
        setDensityRuntimeEnabled(false);
        densityPresentationData=[];
        document.getElementById("densityChartOverlay")?.replaceChildren();
        document.getElementById("densityMapList")?.replaceChildren();
        setDensityMapStatus("empty", "Карта скрыта · сбор плотностей остановлен");
    }
});
viewCoinList?.addEventListener("change", applyWorkspaceViews);
viewCoinList?.addEventListener("change", () => {
    // The checkbox is the Market Screener visibility control. Recalculate
    // chart geometry immediately after its grid track is removed/restored.
    requestAnimationFrame(() => resizeOwnChartsToContainer());
});
viewAlerts?.addEventListener("change", () => {
    applyWorkspaceViews();
    if (viewAlerts.checked && alertNotificationPanel?.classList.contains("open")) {
        setAlertCenterTab("notifications");
    }
});
applyWorkspaceViews();
if (workspaceViews.density) {
    setDensityRuntimeEnabled(true);
    setDensityMapStatus("loading", "Плотности загружаются…");
} else {
    setDensityRuntimeEnabled(false);
}
const settingsClose = document.getElementById("settingsClose");
const marketOverviewSectionTitle = document.getElementById("marketOverviewSectionTitle");
if (marketOverviewSectionTitle) {
    marketOverviewSectionTitle.addEventListener("click", () => {
        document.getElementById("marketOverviewSection")?.classList.toggle("open");
    });
}
const densitySettingsSectionTitle = document.getElementById("densitySettingsSectionTitle");
if (densitySettingsSectionTitle) {
    densitySettingsSectionTitle.addEventListener("click", () => {
        document.getElementById("densitySettingsSection")?.classList.toggle("open");
    });
}
const formationTrendSectionTitle = document.getElementById("formationTrendSectionTitle");
if (formationTrendSectionTitle) {
    formationTrendSectionTitle.addEventListener("click", () => {
        document.getElementById("formationTrendSection")?.classList.toggle("open");
    });
}

const hotkeysTable = document.getElementById("hotkeysTable");

let latestCoins = [];
let instrumentCatalog = [];
let instrumentTypeBySymbol = new Map();
const suggestions = document.getElementById("suggestions");
const chartModal = document.getElementById("chartModal");
const chartContent = document.getElementById("chartContent");
const priceChartEl = document.getElementById("priceChart");
const oiChartEl = document.getElementById("oiChart");
const drawingCanvas = document.getElementById("drawingCanvas");
const drawingStatus = document.getElementById("drawingStatus");
const chartHoverTooltip = document.getElementById("chartHoverTooltip");
const chartLoadingIndicator = document.getElementById("chartLoadingIndicator");
let ownPriceChart = null;
let ownOiChart = null;
let ownCandleSeries = null;
let ownVolumeSeries = null;
let ownOiSeries = null;
let ownOiViewportExtensionSeries = null;
let ownDrawingFutureSeries = null;
let ownWs = null;
let ownOiTimer = null;
let ownChartKlineTimer = null;
let ownChartLoadToken = 0;
const ownChartHistoryCache = new Map();
const ownPriceHistoryCache = new Map();
const ownOiHistoryCache = new Map();
const miniChartHistoryCache = new Map();

// Shared chart-data request/cache layer (Stage 3 performance optimization).
// It is deliberately small and time-limited: history is reused for a few
// seconds, while identical requests that are already in flight are always
// shared. This lets the large chart and grid use the same network result
// without changing the existing chart architecture or realtime behavior.
const chartDataRequestCache = new Map();
const chartDataInFlight = new Map();
const chartRequestControllers = new Map();
const ownActiveHistoryControllers = new Set();
const chartHistoryPrefetchCache = new Map();
const chartPricePrefetchCache = new Map();
const chartOiPrefetchCache = new Map();
// Bounded viewport cache: returning to a symbol/timeframe restores the exact
// large-chart view without keeping any Lightweight Charts instances alive.
const ownChartViewportCache = new Map();
const OWN_CHART_VIEWPORT_CACHE_MAX = 12;
const CHART_HISTORY_PREFETCH_MAX = 12;
const CHART_HISTORY_CACHE_TTL_MS = 4000;
const CHART_LIVE_CACHE_TTL_MS = 700;
// Large-chart history does not need a fresh 250-candle REST response on every
// timeframe switch: the realtime kline/OI paths keep the visible edge current.
// This gate only suppresses redundant background history refreshes for a short
// window; it never delays the cache-first render.
const CHART_ACTIVE_HISTORY_FRESH_MS = 10000;
const CHART_PERFORMANCE_VERSION = "159";
// Tiny per-symbol timeframe preference model. It only remembers the last few
// user-selected transitions so prefetch follows actual usage instead of
// continuously warming unrelated timeframes.
const chartTimeframePreferences = new Map();
const chartTimeframeTransitions = new Map();
const chartLastIntervalBySymbol = new Map();
const CHART_TIMEFRAME_PREFERENCE_MAX = 4;
const CHART_TIMEFRAME_TRANSITION_MAX = 12;

function rememberChartTimeframe(symbol, interval) {
    const symbolSafe = String(symbol || "").toUpperCase();
    const intervalSafe = String(interval || "1");
    if (!symbolSafe) return;

    const previous = chartLastIntervalBySymbol.get(symbolSafe) || null;
    if (previous && previous !== intervalSafe) {
        const byFrom = chartTimeframeTransitions.get(symbolSafe) || new Map();
        const byTo = byFrom.get(previous) || new Map();
        byTo.set(intervalSafe, Number(byTo.get(intervalSafe) || 0) + 1);
        byFrom.set(previous, byTo);
        chartTimeframeTransitions.set(symbolSafe, byFrom);
    }
    chartLastIntervalBySymbol.set(symbolSafe, intervalSafe);

    const recent = chartTimeframePreferences.get(symbolSafe) || [];
    const next = recent.filter(v => v !== intervalSafe);
    next.unshift(intervalSafe);
    chartTimeframePreferences.set(symbolSafe, next.slice(0, CHART_TIMEFRAME_PREFERENCE_MAX));

    // Keep the transition model tiny: only the most useful destinations survive.
    const byFrom = chartTimeframeTransitions.get(symbolSafe);
    if (byFrom && byFrom.size > CHART_TIMEFRAME_TRANSITION_MAX) {
        const oldestFrom = byFrom.keys().next().value;
        if (oldestFrom !== undefined) byFrom.delete(oldestFrom);
    }
}

function preferredChartIntervals(symbol, currentInterval) {
    const symbolSafe = String(symbol || "").toUpperCase();
    const current = String(currentInterval || "1");
    const learned = chartTimeframePreferences.get(symbolSafe) || [];
    const previous = chartLastIntervalBySymbol.get(symbolSafe) || null;
    const transitionMap = previous ? (chartTimeframeTransitions.get(symbolSafe)?.get(previous) || new Map()) : new Map();
    const ranked = [...transitionMap.entries()]
        .filter(([value]) => value !== current)
        .sort((a, b) => Number(b[1]) - Number(a[1]))
        .map(([value]) => value);
    return [...ranked, ...learned.filter(v => v !== current && !ranked.includes(v))];
}

// Per-(symbol,timeframe) UI state. The chart instance itself is still reused;
// this cache only keeps cheap state/data needed to make returning to a view instant.
const chartViewStateCache = new Map();
const CHART_VIEW_STATE_CACHE_MAX = 12;
// app156: bounded hot-state registry. It stores references to already cached
// Price/OI/analysis data, not duplicate candle arrays, so repeat TF switches
// can resolve from one key without extra parsing or network work.
const chartHotStateCache = new Map();

// 159: unified atomic chart snapshot. Keeps one restore source for a TF.
const chartAtomicStateCache = new Map();
const CHART_ATOMIC_STATE_CACHE_MAX = 8;

function saveAtomicChartState(symbol, interval, patch = {}) {
    const key = chartStateKey(symbol, interval);
    if (!key) return;
    const prev = chartAtomicStateCache.get(key) || {};
    chartAtomicStateCache.delete(key);
    chartAtomicStateCache.set(key, { ...prev, ...patch, savedAt: Date.now() });
    while (chartAtomicStateCache.size > CHART_ATOMIC_STATE_CACHE_MAX) {
        const oldest = chartAtomicStateCache.keys().next().value;
        if (oldest === undefined) break;
        chartAtomicStateCache.delete(oldest);
    }
}

function getAtomicChartState(symbol, interval) {
    const key = chartStateKey(symbol, interval);
    const value = chartAtomicStateCache.get(key) || null;
    if (value) {
        chartAtomicStateCache.delete(key);
        chartAtomicStateCache.set(key, value);
    }
    return value;
}

const CHART_HOT_STATE_CACHE_MAX = 8;
const CHART_VIEW_STATE_STORAGE = "cryptoScreenerChartViewStateV153";
let chartViewStatePersistTimer = null;

function chartStateKey(symbol, interval) {
    return `${String(symbol || "").toUpperCase()}|${String(interval || "1")}`;
}

function rememberChartViewState(symbol, interval, state) {
    const key = chartStateKey(symbol, interval);
    if (!key || !state) return;
    chartViewStateCache.delete(key);
    chartViewStateCache.set(key, state);
    while (chartViewStateCache.size > CHART_VIEW_STATE_CACHE_MAX) {
        const oldest = chartViewStateCache.keys().next().value;
        if (oldest === undefined) break;
        chartViewStateCache.delete(oldest);
    }
    // Viewport events can fire frequently while the user zooms/pans. Keep the
    // hot path in memory and persist the bounded snapshot at most a few times
    // per second instead of serializing it on every viewport event.
    if (!chartViewStatePersistTimer) {
        chartViewStatePersistTimer = setTimeout(() => {
            chartViewStatePersistTimer = null;
            try {
                const serializable = {};
                chartViewStateCache.forEach((value, cacheKey) => { serializable[cacheKey] = value; });
                localStorage.setItem(CHART_VIEW_STATE_STORAGE, JSON.stringify(serializable));
            } catch (_) {}
        }, 250);
    }
}

function restoreChartViewStateCache(symbol, interval) {
    const key = chartStateKey(symbol, interval);
    if (!chartViewStateCache.size) {
        try {
            const saved = JSON.parse(localStorage.getItem(CHART_VIEW_STATE_STORAGE) || "null");
            if (saved && typeof saved === "object") {
                Object.entries(saved).slice(-CHART_VIEW_STATE_CACHE_MAX).forEach(([k, v]) => chartViewStateCache.set(k, v));
            }
        } catch (_) {}
    }
    const value = chartViewStateCache.get(key) || null;
    if (value) {
        chartViewStateCache.delete(key);
        chartViewStateCache.set(key, value);
    }
    return value;
}

function rememberChartHotState(symbol, interval) {
    const key = chartStateKey(symbol, interval);
    if (!key) return;
    const safeSymbol = String(symbol||"").toUpperCase();
    const safeInterval = String(interval||"1");
    const dataKey = `${safeSymbol}|${ownChartIntervalToBinance(safeInterval)}|${ownOiPeriodForInterval(safeInterval)}`;
    const analysisKey = `${safeSymbol}|${ownChartIntervalToBinance(safeInterval)}`;
    const existingView = chartViewStateCache.get(chartStateKey(safeSymbol, safeInterval)) || null;
    const state = {
        price: ownPriceHistoryCache.get(dataKey) || null,
        oi: ownOiHistoryCache.get(dataKey) || null,
        analysis: chartAnalysisCache.get(analysisKey) || chartIndicatorLayerCache.get(analysisKey) || null,
        analysisState: chartAnalysisStateCache.get(analysisKey) || null,
        viewport: existingView?.viewport || null,
        drawings: cloneDrawings(getSharedChartDrawings(safeSymbol, safeInterval)),
        selectedDrawingIndex: Number.isInteger(existingView?.selectedDrawingIndex) ? existingView.selectedDrawingIndex : -1,
        layers: existingView?.layers ? {...existingView.layers} : {formations:!!chartShowFormations, levels:!!chartShowLevels, density:!!densityChartVisible},
        savedAt: Date.now()
    };
    saveAtomicChartState(safeSymbol, safeInterval, state);
    chartHotStateCache.delete(key);
    chartHotStateCache.set(key, state);
    while (chartHotStateCache.size > CHART_HOT_STATE_CACHE_MAX) {
        const oldest = chartHotStateCache.keys().next().value;
        if (oldest === undefined) break;
        chartHotStateCache.delete(oldest);
    }
}

function getChartHotState(symbol, interval) {
    const key = chartStateKey(symbol, interval);
    const state = chartHotStateCache.get(key) || null;
    if (state) {
        chartHotStateCache.delete(key);
        chartHotStateCache.set(key, state);
    }
    return state;
}

function captureActiveChartState(symbol, interval) {
    const key = chartStateKey(symbol, interval);
    if (!key) return;
    let viewport = null;
    try {
        if (ownPriceChart) viewport = {
            range: ownPriceChart.timeScale().getVisibleRange() || null,
            logicalRange: ownPriceChart.timeScale().getVisibleLogicalRange() || null
        };
    } catch (_) {}
    rememberChartViewState(symbol, interval, {
        viewport,
        drawings: Array.isArray(chartDrawings) ? JSON.parse(JSON.stringify(chartDrawings)) : [],
        selectedDrawingIndex: Number.isInteger(selectedDrawingIndex) ? selectedDrawingIndex : -1,
        layers: {
            formations: !!chartShowFormations,
            levels: !!chartShowLevels,
            density: !!densityChartVisible
        },
        savedAt: Date.now()
    });
    rememberChartHotState(symbol, interval);
}

function restoreActiveChartState(symbol, interval) {
    const state = getAtomicChartState(symbol, interval) || restoreChartViewStateCache(symbol, interval) || getChartHotState(symbol, interval);
    if (!state) return null;
    if (state.analysis) {
        const analysisKey = `${String(symbol||"").toUpperCase()}|${ownChartIntervalToBinance(interval)}`;
        chartAnalysisCache.set(analysisKey, state.analysis);
        chartIndicatorLayerCache.delete(analysisKey);
        chartIndicatorLayerCache.set(analysisKey, state.analysis);
        const restoredCandleTime = Number(state.analysis?.candle_open_time || 0) || null;
        chartAnalysisCandleCache.set(analysisKey, restoredCandleTime);
        chartAnalysisStateCache.set(analysisKey, {
            symbol: String(symbol || "").toUpperCase(),
            timeframe: ownChartIntervalToBinance(interval),
            last_candle_open: restoredCandleTime,
            last_candle_close: Number(state.analysis?.candle_close || 0) || null,
            candle_count: Array.isArray(state.price?.candles) ? state.price.candles.length : 0,
            indicator_state: state.analysis?.indicator_state || state.analysis?.indicators || null,
            analysis: state.analysis,
            updatedAt: Date.now()
        });
    }
    const sharedDrawings = getSharedChartDrawings(symbol, interval);
    if (sharedDrawings.length || !Array.isArray(state.drawings)) chartDrawings = cloneDrawings(sharedDrawings);
    else { chartDrawings = cloneDrawings(state.drawings); saveSharedChartDrawings(symbol, interval, chartDrawings); }
    selectedDrawingIndex = Number.isInteger(state.selectedDrawingIndex) ? state.selectedDrawingIndex : -1;
    if (state.layers) {
        if (typeof state.layers.formations === "boolean") chartShowFormations = state.layers.formations;
        if (typeof state.layers.levels === "boolean") chartShowLevels = state.layers.levels;
        if (typeof state.layers.density === "boolean") densityChartVisible = state.layers.density;
        syncChartOverlayToggleButtons();
    }
    return state.viewport || null;
}


// Small, bounded cache for likely-neighbor timeframes. Prefetch is strictly
// non-blocking: the active chart never waits for these background requests.
function putChartHistoryPrefetch(cacheKey, klineData, oiData) {
    if (!klineData) return;
    if (chartHistoryPrefetchCache.has(cacheKey)) chartHistoryPrefetchCache.delete(cacheKey);
    chartHistoryPrefetchCache.set(cacheKey, {klineData, oiData: oiData || {points:[]}});
    while (chartHistoryPrefetchCache.size > CHART_HISTORY_PREFETCH_MAX) {
        const oldestKey = chartHistoryPrefetchCache.keys().next().value;
        if (oldestKey === undefined) break;
        chartHistoryPrefetchCache.delete(oldestKey);
    }
}

function takeChartHistoryPrefetch(cacheKey) {
    const cached = chartHistoryPrefetchCache.get(cacheKey);
    if (!cached) return null;
    chartHistoryPrefetchCache.delete(cacheKey);
    return cached;
}

function neighborChartIntervals(interval) {
    const current = String(interval || "1");
    const neighbors = {
        "1": ["3", "5"],
        "3": ["1", "5", "15"],
        "5": ["1", "3", "15"],
        "15": ["5", "30", "60"],
        "30": ["15", "60"],
        "60": ["15", "30", "240"],
        "240": ["60", "1D"],
        "1D": ["240"]
    };
    return (neighbors[current] || []).slice(0, 3);
}

function prefetchChartHistory(symbol, interval) {
    const symbolSafe = String(symbol || "").toUpperCase();
    if (!symbolSafe) return;
    const neighborTargets = neighborChartIntervals(interval);
    const learnedTargets = preferredChartIntervals(symbolSafe, interval);
    // Keep prefetch deliberately tiny: warm only the single most likely next
    // timeframe. The active chart never waits for it, so this improves repeat
    // switches without creating a burst of background requests.
    const targets = [...learnedTargets, ...neighborTargets]
        .filter((value, index, arr) => arr.indexOf(value) === index)
        .slice(0, 1);
    if (!targets.length) return;

    targets.forEach(targetInterval => {
        const bi = ownChartIntervalToBinance(targetInterval);
        const oiPeriod = ownOiPeriodForInterval(targetInterval);
        const cacheKey = `${symbolSafe}|${bi}|${oiPeriod}`;
        const klineRequestKey = `history|klines|${symbolSafe}|${bi}|250`;
        const oiRequestKey = `history|oi|${symbolSafe}|${oiPeriod}|250`;

        if (!ownPriceHistoryCache.has(cacheKey) && !miniChartHistoryCache.has(cacheKey) && !chartPricePrefetchCache.has(cacheKey)) {
            fetchChartJsonShared(`/api/klines?symbol=${encodeURIComponent(symbolSafe)}&interval=${encodeURIComponent(bi)}&limit=250`, klineRequestKey, CHART_HISTORY_CACHE_TTL_MS)
                .then(klineData => {
                    if (!klineData) return;
                    chartPricePrefetchCache.set(cacheKey, klineData);
                    while (chartPricePrefetchCache.size > CHART_HISTORY_PREFETCH_MAX) {
                        const oldestKey = chartPricePrefetchCache.keys().next().value;
                        if (oldestKey === undefined) break;
                        chartPricePrefetchCache.delete(oldestKey);
                    }
                }).catch(() => {});
        }

        if (!ownOiHistoryCache.has(cacheKey) && !chartOiPrefetchCache.has(cacheKey)) {
            fetchChartJsonShared(`/api/open_interest?symbol=${encodeURIComponent(symbolSafe)}&period=${encodeURIComponent(oiPeriod)}&limit=250`, oiRequestKey, CHART_HISTORY_CACHE_TTL_MS)
                .then(oiData => {
                    if (!oiData) return;
                    chartOiPrefetchCache.set(cacheKey, oiData);
                    while (chartOiPrefetchCache.size > CHART_HISTORY_PREFETCH_MAX) {
                        const oldestKey = chartOiPrefetchCache.keys().next().value;
                        if (oldestKey === undefined) break;
                        chartOiPrefetchCache.delete(oldestKey);
                    }
                }).catch(() => {});
        }
    });
}

async function fetchChartJsonShared(url, cacheKey, ttlMs = CHART_HISTORY_CACHE_TTL_MS) {
    const now = Date.now();
    const cached = chartDataRequestCache.get(cacheKey);
    if (cached && now - cached.time < ttlMs) return cached.data;

    const running = chartDataInFlight.get(cacheKey);
    if (running) return running;

    const controller = new AbortController();
    const previousController = chartRequestControllers.get(cacheKey);
    if (previousController) { try { previousController.abort(); } catch (_) {} }
    chartRequestControllers.set(cacheKey, controller);

    const request = (async () => {
        const response = await fetch(url, { cache: "no-store", signal: controller.signal });
        const data = await response.json();
        if (!response.ok) {
            const error = new Error(data?.error || `HTTP ${response.status}`);
            error.status = response.status;
            throw error;
        }
        chartDataRequestCache.set(cacheKey, { time: Date.now(), data });
        return data;
    })();

    chartDataInFlight.set(cacheKey, request);
    try {
        return await request;
    } finally {
        if (chartDataInFlight.get(cacheKey) === request) chartDataInFlight.delete(cacheKey);
        if (chartRequestControllers.get(cacheKey) === controller) chartRequestControllers.delete(cacheKey);
    }
}

// Background refresh deliberately bypasses the short shared-cache TTL. The
// active chart shows cached data first, then this function fetches Binance data
// and updates only changed points.
function chartHistoryRequestIsFresh(cacheKey, maxAge = CHART_ACTIVE_HISTORY_FRESH_MS) {
    const cached = chartDataRequestCache.get(cacheKey);
    return !!(cached && Number.isFinite(cached.time) && (Date.now() - cached.time) < maxAge);
}

async function fetchChartJsonFresh(url, cacheKey, activeController = null) {
    // A shared Grid/prefetch request for the same dataset is safe to reuse.
    // It remains owned by the shared request layer and is therefore never
    // aborted by a large-chart timeframe switch. The load-token below still
    // prevents stale data from becoming visible.
    const running = chartDataInFlight.get(cacheKey);
    if (running) return running;

    const recent = chartDataRequestCache.get(cacheKey);
    if (recent && Number.isFinite(recent.time) &&
        (Date.now() - recent.time) < CHART_ACTIVE_HISTORY_FRESH_MS) {
        return recent.data;
    }

    // Active large-chart requests have an isolated lifecycle. Do NOT register
    // their controller in chartRequestControllers: that map belongs to the
    // shared Grid/prefetch layer and aborting it here can cancel work another
    // chart is still using.
    const controller = activeController || new AbortController();
    const owned = !!activeController;
    if (owned) ownActiveHistoryControllers.add(controller);

    try {
        const response = await fetch(url, { cache: "no-store", signal: controller.signal });
        const data = await response.json();
        if (!response.ok) {
            const error = new Error(data?.error || `HTTP ${response.status}`);
            error.status = response.status;
            throw error;
        }
        chartDataRequestCache.set(cacheKey, { time: Date.now(), data });
        return data;
    } finally {
        if (owned) ownActiveHistoryControllers.delete(controller);
    }
}

let ownChartReuseSymbol = "";
let ownChartReuseKey = "";
// Explicitly separate the lifecycle context of the large chart from Grid.
// Grid slots temporarily reuse the own* references for drawing/focus support,
// but those references must never make loadChart think a large chart is alive.
let ownChartContext = "none";
let pendingOwnChartOpenRange = null;
let pendingOwnChartHistoryAnchor = null;
let volumeExpansionHighlightZones = [];
function volumeExpansionBarColor(time){
    const t=Number(time);
    return Number.isFinite(t) && volumeExpansionHighlightZones.some(z=>t>=Number(z.start)&&t<=Number(z.end)) ? "#ef5350" : "#808890";
}
let ownHistoryChunkSeries = [];
let ownHistoryLoadState = null;
let ownHistoryCheckTimer = null;
let ownChartRenderedCacheKey = "";
let ownChartRenderedKlineSignature = null;
let ownChartRenderedOiSignature = null;
let ownResizeObserver = null;
let ownOiPeriod = "5m";
let ownLastPrice = 0;
const miniHistoryStates = new WeakMap();
const miniHistoryChunkSeries = new WeakMap();

function clearOwnHistoryChunkSeries() {
    if (ownHistoryCheckTimer) { clearTimeout(ownHistoryCheckTimer); ownHistoryCheckTimer = null; }
    for (const chunk of ownHistoryChunkSeries) {
        if (chunk.candle && ownPriceChart) { try { ownPriceChart.removeSeries(chunk.candle); } catch (_) {} }
        if (chunk.volume && ownPriceChart) { try { ownPriceChart.removeSeries(chunk.volume); } catch (_) {} }
        if (chunk.oi && ownOiChart) { try { ownOiChart.removeSeries(chunk.oi); } catch (_) {} }
    }
    ownHistoryChunkSeries = [];
    ownHistoryLoadState = null;
}

function scheduleOlderOwnHistoryCheck() {
    if (ownHistoryCheckTimer) clearTimeout(ownHistoryCheckTimer);
    ownHistoryCheckTimer = setTimeout(() => {
        ownHistoryCheckTimer = null;
        maybeLoadOlderOwnHistory();
    }, 80);
}

async function maybeLoadOlderOwnHistory() {
    const state = ownHistoryLoadState;
    if (!state || state.loading || state.exhausted || state.token !== ownChartLoadToken) return;
    if (!ownPriceChart || !ownCandleSeries || !ownVolumeSeries || !ownOiChart || !ownOiSeries) return;

    let visibleLogical = null;
    try { visibleLogical = ownPriceChart.timeScale().getVisibleLogicalRange() || null; } catch (_) {}
    if (!visibleLogical || !Number.isFinite(Number(visibleLogical.from))) return;

    const currentFrom = Number(visibleLogical.from);
    if (!state.armed) {
        if (Number.isFinite(state.lastVisibleFrom) && currentFrom < state.lastVisibleFrom - 0.5) state.armed = true;
        state.lastVisibleFrom = currentFrom;
        if (!state.armed) return;
    } else {
        state.lastVisibleFrom = currentFrom;
    }

    // Prefetch before the user reaches the left edge. After each prepend the
    // logical indices shift to the right, so this threshold naturally becomes
    // inactive again until the user continues scrolling left.
    if (currentFrom > 90) return;

    const oldestKlineTime = Number(state.oldestKlineTime);
    if (!Number.isFinite(oldestKlineTime) || oldestKlineTime <= 0) return;

    state.loading = true;
    if (chartLoadingIndicator) {
        chartLoadingIndicator.textContent = "Подгружается история…";
        chartLoadingIndicator.classList.add("open");
    }

    try {
        const endTime = Math.floor(oldestKlineTime * 1000 - 1);
        const [klineResult, oiResult] = await Promise.allSettled([
            fetch(`/api/klines?symbol=${encodeURIComponent(state.symbol)}&interval=${encodeURIComponent(state.binanceInterval)}&limit=250&endTime=${endTime}`, { cache: "no-store" }),
            fetch(`/api/open_interest?symbol=${encodeURIComponent(state.symbol)}&period=${encodeURIComponent(state.oiPeriod)}&limit=250&endTime=${endTime}`, { cache: "no-store" })
        ]);

        if (state !== ownHistoryLoadState || state.token !== ownChartLoadToken) return;
        if (klineResult.status !== "fulfilled") throw klineResult.reason || new Error("Ошибка загрузки старых свечей");

        const kr = klineResult.value;
        const kd = await kr.json();
        if (!kr.ok) throw new Error(kd.error || "Ошибка загрузки старых свечей");

        let od = { points: [] };
        if (oiResult.status === "fulfilled") {
            try {
                const or = oiResult.value;
                const parsed = await or.json();
                if (or.ok && parsed && Array.isArray(parsed.points)) od = parsed;
            } catch (_) {}
        }

        const olderCandles = (kd.candles || [])
            .map(c => ({
                time: Number(c.time), open: Number(c.open), high: Number(c.high),
                low: Number(c.low), close: Number(c.close), volume: Number(c.volume)
            }))
            .filter(c => [c.time,c.open,c.high,c.low,c.close,c.volume].every(Number.isFinite))
            .filter(c => c.time < state.oldestKlineTime && !state.klineTimes.has(c.time))
            .sort((a,b) => a.time - b.time);

        if (!olderCandles.length) {
            state.exhausted = true;
            return;
        }

        // Capture the user's CURRENT viewport only after the network request has
        // finished. If the user kept dragging while data was loading, we preserve
        // the latest position, not the position from the beginning of the request.
        let priceTimeRange = null, oiTimeRange = null;
        let priceLogicalRange = null, oiLogicalRange = null;
        try { priceTimeRange = ownPriceChart.timeScale().getVisibleRange() || null; } catch (_) {}
        try { oiTimeRange = ownOiChart.timeScale().getVisibleRange() || null; } catch (_) {}
        try { priceLogicalRange = ownPriceChart.timeScale().getVisibleLogicalRange() || null; } catch (_) {}
        try { oiLogicalRange = ownOiChart.timeScale().getVisibleLogicalRange() || null; } catch (_) {}

        // Lightweight Charts cannot prepend old bars through update().
        // Keep every historical batch in its own series: only the NEW 250 bars
        // receive setData(), while already rendered history remains untouched.
        const candleChunk = ownPriceChart.addSeries(LightweightCharts.CandlestickSeries, {
            upColor: "#26a69a", downColor: "#ef5350", borderVisible: false,
            wickUpColor: "#26a69a", wickDownColor: "#ef5350",
            priceLineVisible: false, lastValueVisible: false
        }, 0);
        const volumeChunk = ownPriceChart.addSeries(LightweightCharts.HistogramSeries, {
            priceFormat: { type: "volume" }, priceScaleId: "volume", color: "#808890",
            scaleMargins: { top: 0.86, bottom: 0 }, priceLineVisible: false, lastValueVisible: false
        }, 0);
        candleChunk.setData(olderCandles.map(c => ({ time:c.time, open:c.open, high:c.high, low:c.low, close:c.close })));
        volumeChunk.setData(olderCandles.map(c => ({ time:c.time, value:c.volume, color:"#808890" })));

        const olderOi = (od.points || [])
            .map(p => ({ time:Number(p.time), value:Number(p.oiValue) }))
            .filter(p => Number.isFinite(p.time) && Number.isFinite(p.value))
            .filter(p => !state.oiTimes.has(p.time))
            .sort((a,b) => a.time - b.time);

        // Keep Open Interest as ONE continuous series. Creating a new OI
        // series for every historical chunk produced the visible parallel
        // lines on a single chart. Merge the older points into the existing
        // series instead.
        if (olderOi.length) {
            const mergedOi = [...olderOi, ...(ownOiPoints || [])];
            const oiByTime = new Map();
            for (const point of mergedOi) oiByTime.set(Number(point.time), point);
            ownOiPoints = [...oiByTime.values()].sort((a,b) => a.time - b.time);
            ownOiSeries.setData(ownOiPoints);
        }
        ownHistoryChunkSeries.push({ candle:candleChunk, volume:volumeChunk, oi:null });

        for (const c of olderCandles) state.klineTimes.add(c.time);
        for (const p of olderOi) state.oiTimes.add(p.time);
        state.oldestKlineTime = olderCandles[0].time;
        if (olderOi.length) state.oldestOiTime = Math.min(state.oldestOiTime || olderOi[0].time, olderOi[0].time);
        if (olderCandles.length < 250) state.exhausted = true;

        // Older bars were added as a new series before the existing data.
        // Restore the CURRENT logical viewport by shifting its indices by the
        // exact number of prepended candles. This is deliberately based on the
        // user's latest position, rather than restoring a stale time range that
        // can be reinterpreted after the chart's data set changes.
        if (priceLogicalRange && !ownPriceChart._userMovedViewport) {
            restoreChartViewport(ownPriceChart, null, {
                from: Number(priceLogicalRange.from) + olderCandles.length,
                to: Number(priceLogicalRange.to) + olderCandles.length
            }, false);
        } else if (priceTimeRange && !ownPriceChart._userMovedViewport) {
            restoreChartViewport(ownPriceChart, priceTimeRange, null, false);
        }
        // Price remains the only horizontal viewport owner. After older OI
        // data is inserted, restore the follower from the current Price range.
        syncOiViewportToPrice(ownPriceChart, ownOiChart, ownOiViewportExtensionSeries);

        let restoredLogical = null;
        try { restoredLogical = ownPriceChart.timeScale().getVisibleLogicalRange() || null; } catch (_) {}
        if (restoredLogical && Number.isFinite(Number(restoredLogical.from))) state.lastVisibleFrom = Number(restoredLogical.from);
        redrawDrawings();
    } catch (e) {
        if (state === ownHistoryLoadState && state.token === ownChartLoadToken) {
            console.warn("Older chart history error:", e);
        }
    } finally {
        if (state === ownHistoryLoadState) state.loading = false;
        if (state.token === ownChartLoadToken && chartLoadingIndicator) chartLoadingIndicator.classList.remove("open");
    }
}

const chartTitle = document.getElementById("chartTitle");
const chartTitleDot = document.getElementById("chartTitleDot");
const chartClose = document.getElementById("chartClose");
const chartBack = document.getElementById("chartBack");
const chartFlag = document.getElementById("chartFlag");
const chartFlagPalette = document.getElementById("chartFlagPalette");
const chartIntervals = document.getElementById("chartIntervals");
let chartFlagOpen = false;
let chartFlagPendingColor = null;
let chartFlagPendingExplicitClear = false;
let selectedChartSymbol = "";
let chartAutoOpenPending = false;
let selectedChartInterval = "1";
const CHART_OVERLAY_VISIBILITY_STORAGE = "cryptoScreenerChartOverlayVisibility";

function loadChartOverlayVisibility() {
    try {
        const saved = JSON.parse(localStorage.getItem(CHART_OVERLAY_VISIBILITY_STORAGE) || "null");
        return {
            formations: saved?.formations !== false,
            levels: saved?.levels !== false
        };
    } catch (e) {
        return {formations: true, levels: true};
    }
}

let chartOverlayVisibility = loadChartOverlayVisibility();
let chartShowFormations = chartOverlayVisibility.formations;
let chartShowLevels = chartOverlayVisibility.levels;

function saveChartOverlayVisibility() {
    try {
        localStorage.setItem(CHART_OVERLAY_VISIBILITY_STORAGE, JSON.stringify({
            formations: chartShowFormations,
            levels: chartShowLevels
        }));
    } catch (e) {}
}

function syncChartOverlayToggleButtons() {
    document.getElementById("chartFormationToggle")?.classList.toggle("active", chartShowFormations);
    document.getElementById("chartLevelToggle")?.classList.toggle("active", chartShowLevels);
    document.getElementById("chartDensityToggle")?.classList.toggle("active", densityChartVisible);
}

const FORMATION_TREND_DIRECTION_STORAGE = "cryptoScreenerFormationTrendDirection";
const DEFAULT_FORMATION_TREND_DIRECTION = "rising";
let formationTrendDirection = loadFormationTrendDirection();
const chartAnalysisCache = new Map();
// app157: deduplicate overlay analysis requests and invalidate only when the
// latest cached candle changes. This keeps formation/structure analysis
// incremental without introducing a second indicator engine.
const chartAnalysisInFlight = new Map();
let chartOverlayAnalysisGeneration = 0;
let chartOverlayAnalysisAbortController = null;
const chartAnalysisCandleCache = new Map();
const CHART_ANALYSIS_CANDLE_CACHE_MAX = 12;
// app160: exact analysis state for the visible chart.  The state is keyed by
// symbol/timeframe and records the candle boundary and indicator snapshot used
// to produce the current overlay.  This lets us distinguish a new candle from
// an ordinary tail update without rebuilding the whole analysis state.
const chartAnalysisStateCache = new Map();
const CHART_ANALYSIS_STATE_CACHE_MAX = 12;

// app153: analysis/overlay cache is intentionally bounded separately from chart
// instances; it is reused when returning to the same symbol/timeframe.
const chartIndicatorLayerCache = new Map();
const CHART_INDICATOR_LAYER_CACHE_MAX = 12;
let chartAnalysisBusyKey = "";
let activeDrawingPriceElement = priceChartEl;
let chartDrawingFocus = true;
// Shared drawing store: drawings belong to the symbol, never to a
// particular Lightweight Charts instance or timeframe. The same drawing
// coordinates (time/price) are therefore rendered on every timeframe for
// that symbol.
const chartDrawingStore = new Map();
function drawingStateKey(symbol, interval) {
    const safeSymbol = String(symbol || "").toUpperCase().trim();
    return safeSymbol || null;
}
function cloneDrawings(drawings) { return Array.isArray(drawings) ? JSON.parse(JSON.stringify(drawings)) : []; }
function getSharedChartDrawings(symbol = selectedChartSymbol, interval = selectedChartInterval) {
    const key = drawingStateKey(symbol, interval);
    return key ? (chartDrawingStore.get(key) || []) : [];
}
function saveSharedChartDrawings(symbol = selectedChartSymbol, interval = selectedChartInterval, drawings = chartDrawings) {
    const key = drawingStateKey(symbol, interval);
    if (key) chartDrawingStore.set(key, cloneDrawings(drawings));
}
function syncActiveDrawingsToSharedStore() {
    if (selectedChartSymbol && selectedChartInterval) saveSharedChartDrawings(selectedChartSymbol, selectedChartInterval, chartDrawings);
}

let ownOiPoints = [];
let activeDrawingTool = null;
let drawingPoints = [];
let drawingInProgress = false;
let drawingPreview = null;
let chartDrawings = [];
let drawingRaf = null;
let selectedDrawingIndex = -1;
let drawingEditState = null;
let signalLevelStates = new Map();
const signalLevelFired = new Set();

function loadFormationTrendDirection() {
    try {
        const value = String(localStorage.getItem(FORMATION_TREND_DIRECTION_STORAGE) || DEFAULT_FORMATION_TREND_DIRECTION);
        return ["rising", "falling", "all"].includes(value) ? value : DEFAULT_FORMATION_TREND_DIRECTION;
    } catch (e) {
        return DEFAULT_FORMATION_TREND_DIRECTION;
    }
}

function saveFormationTrendDirection() {
    try { localStorage.setItem(FORMATION_TREND_DIRECTION_STORAGE, formationTrendDirection); } catch (e) {}
}

function formationTrendLineVisible(line) {
    if (!line || formationTrendDirection === "all") return true;
    const slope = Number(line.slope);
    if (!Number.isFinite(slope) || Math.abs(slope) < 1e-12) return false;
    return formationTrendDirection === "rising" ? slope > 0 : slope < 0;
}

function initFormationTrendSettings() {
    const select = document.getElementById("formationTrendDirection");
    if (!select) return;
    select.value = formationTrendDirection;
    select.addEventListener("change", () => {
        const value = String(select.value || DEFAULT_FORMATION_TREND_DIRECTION);
        formationTrendDirection = ["rising", "falling", "all"].includes(value) ? value : DEFAULT_FORMATION_TREND_DIRECTION;
        saveFormationTrendDirection();
        redrawDrawings();
    });
}

function makeSignalLevelNotification(symbol, drawing, currentPrice, direction) {
    const level = Number(drawing?.price);
    const now = new Date().toISOString();
    const levelId = drawing?.id || "level";
    return {
        id: `signal_${symbol}_${levelId}_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
        alertId: `signal:${symbol}:${levelId}`,
        alertName: "Сигнальный уровень",
        kind: "signal",
        symbol: String(symbol || "").toUpperCase(),
        market: "Binance Futures",
        time: now,
        message: `Сигнальный уровень пересечён ${direction === "up" ? "вверх" : "вниз"}.`,
        direction,
        values: { level, price: Number(currentPrice) },
        read: false
    };
}

function checkSignalLevelCrossingsForSymbol(symbol, drawings, previousPrice, currentPrice) {
    if (!Number.isFinite(previousPrice) || !Number.isFinite(currentPrice) || !symbol || !Array.isArray(drawings)) return;
    drawings.forEach((drawing, index) => {
        if (!drawing || drawing.type !== "signal" || !Number.isFinite(Number(drawing.price))) return;
        const key = `${symbol}:${drawing.id || index}`;
        const level = Number(drawing.price);
        const previous = signalLevelStates.get(key);
        if (!Number.isFinite(previous)) {
            signalLevelStates.set(key, currentPrice);
            return;
        }
        if (signalLevelFired.has(key)) {
            signalLevelStates.set(key, currentPrice);
            return;
        }
        const crossedUp = previous < level && currentPrice >= level;
        const crossedDown = previous > level && currentPrice <= level;
        if (crossedUp || crossedDown) {
            const direction = crossedUp ? "up" : "down";
            signalLevelFired.add(key);
            playSignalLevelSound();
            redrawDrawings();
            window.dispatchEvent(new CustomEvent("crypto-screener-signal-level", {
                detail: makeSignalLevelNotification(symbol, drawing, currentPrice, direction)
            }));
        }
        signalLevelStates.set(key, currentPrice);
    });
}

function signalDrawingsForSymbol(symbol) {
    const safeSymbol = String(symbol || "").toUpperCase();
    if (!safeSymbol) return [];
    const merged = [];
    const seen = new Set();
    const add = drawings => {
        if (!Array.isArray(drawings)) return;
        drawings.forEach((drawing, index) => {
            if (!drawing || drawing.type !== "signal" || !Number.isFinite(Number(drawing.price))) return;
            const id = String(drawing.id || `${safeSymbol}:signal:${index}:${Number(drawing.price)}`);
            if (seen.has(id)) return;
            seen.add(id);
            merged.push(drawing);
        });
    };
    if (safeSymbol === String(selectedChartSymbol || "").toUpperCase()) add(chartDrawings);
    chartDrawingStore.forEach((drawings, key) => {
        if (String(key).startsWith(`${safeSymbol}|`)) add(drawings);
    });
    return merged;
}

async function syncSignalLevelsToAlertServer() {
    const merged = new Map();
    const add = (symbol, drawings) => {
        const safeSymbol = String(symbol || "").toUpperCase();
        if (!safeSymbol || !Array.isArray(drawings)) return;
        drawings.forEach((drawing, index) => {
            if (!drawing || drawing.type !== "signal" || !Number.isFinite(Number(drawing.price))) return;
            const id = String(drawing.id || `${safeSymbol}:signal:${index}:${Number(drawing.price)}`);
            merged.set(`${safeSymbol}:${id}`, { id, symbol: safeSymbol, price: Number(drawing.price), active: true });
        });
    };
    add(selectedChartSymbol, chartDrawings);
    chartDrawingStore.forEach((drawings, key) => {
        const symbol = String(key).split("|")[0];
        add(symbol, drawings);
    });
    try {
        const response = await fetch("/api/signal-levels/sync", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ levels: [...merged.values()] }),
            keepalive: true
        });
        const result = await response.json().catch(() => ({}));
        if (!result.ok) console.warn("Alert server signal level sync failed:", result.error || "unknown error");
    } catch (error) {
        console.warn("Alert server signal level sync failed:", error);
    }
}

function resetDrawingCanvas() {
    const targetPriceElement = activeDrawingPriceElement || priceChartEl;
    if (!drawingCanvas || !targetPriceElement) return null;

    // Lightweight Charts keeps the right price scale inside the chart
    // container. The drawing layer must cover only the plotting pane,
    // otherwise it sits above and hides the native price labels.
    let chartRect = targetPriceElement.getBoundingClientRect();
    try {
        const panes = ownPriceChart?.panes?.();
        const mainPaneEl = panes?.[0]?.getHTMLElement?.();
        if (mainPaneEl && targetPriceElement === priceChartEl) chartRect = mainPaneEl.getBoundingClientRect();
        else if (mainPaneEl && activeDrawingPriceElement === targetPriceElement) chartRect = mainPaneEl.getBoundingClientRect();
    } catch (_) {}
    const hostRect = drawingCanvas.parentElement.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;

    let priceScaleWidth = 0;
    try {
        priceScaleWidth = Number(ownPriceChart?.priceScale("right")?.width?.() || 0);
    } catch (e) {
        priceScaleWidth = 0;
    }

    // v4 exposes the right price-scale width. Keep a conservative fallback
    // so the canvas can never cover the scale if the API is unavailable.
    if (!Number.isFinite(priceScaleWidth) || priceScaleWidth <= 0) {
        priceScaleWidth = 70;
    }

    const width = Math.max(1, Math.round(chartRect.width - priceScaleWidth));
    const height = Math.max(1, Math.round(chartRect.height));

    drawingCanvas.width = Math.max(1, Math.round(width * dpr));
    drawingCanvas.height = Math.max(1, Math.round(height * dpr));
    drawingCanvas.style.left = (chartRect.left - hostRect.left) + "px";
    drawingCanvas.style.top = (chartRect.top - hostRect.top) + "px";
    drawingCanvas.style.right = "auto";
    drawingCanvas.style.bottom = "auto";
    drawingCanvas.style.width = width + "px";
    drawingCanvas.style.height = height + "px";

    const ctx = drawingCanvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    return {ctx, width, height};
}

// Drawings must not extend the Lightweight Charts time series with thousands
// of whitespace points. Doing that makes the chart itself zoom out when the
// timeframe changes. Trader Diary keeps the market series untouched and
// extrapolates only the drawing coordinate when a point is in the future.
function ensureDrawingFutureSpace(chart, candles = null, intervalSeconds = null) {
    if (!chart) return;
    installManualViewportGuard(chart);
    const interval = Number(intervalSeconds) > 0
        ? Number(intervalSeconds)
        : Number(chart._drawingIntervalSeconds) > 0
            ? Number(chart._drawingIntervalSeconds)
            : drawingIntervalSeconds();
    if (Number.isFinite(interval) && interval > 0) {
        chart._drawingIntervalSeconds = interval;
    }
    // Intentionally keep the optional future series empty. Drawing geometry is
    // projected beyond the last real candle without changing the chart scale.
    const futureSeries = chart._drawingFutureSeries || (chart === ownPriceChart ? ownDrawingFutureSeries : null);
    if (futureSeries) {
        try { futureSeries.setData([]); } catch (_) {}
    }
}

function markManualChartViewport(chart) {
    if (!chart) return;
    if (chart._viewportGuardSuspended) return;
    chart._userMovedViewport = true;
    chart._userMovedViewportAt = Date.now();
}

function installManualViewportGuard(chart) {
    if (!chart || chart._manualViewportGuardInstalled) return;
    chart._manualViewportGuardInstalled = true;
    try {
        chart.timeScale().subscribeVisibleLogicalRangeChange(() => markManualChartViewport(chart));
    } catch (_) {}
}

function withViewportGuardSuspended(chart, fn) {
    if (!chart || typeof fn !== "function") return;
    chart._viewportGuardSuspended = true;
    try { fn(); } finally { setTimeout(() => { chart._viewportGuardSuspended = false; }, 0); }
}

function restoreChartViewport(chart, range, logicalRange, fitContent = false) {
    if (!chart) return;
    withViewportGuardSuspended(chart, () => {
        if (fitContent) chart.timeScale().fitContent();
        else if (logicalRange) chart.timeScale().setVisibleLogicalRange(logicalRange);
        else if (range) chart.timeScale().setVisibleRange(range);
    });
}

function drawingIntervalSeconds() {
    const map = {"1":60, "3":180, "5":300, "15":900, "30":1800, "60":3600, "240":14400, "1D":86400};
    if (ownPriceChart && Number.isFinite(Number(ownPriceChart._drawingIntervalSeconds)) && Number(ownPriceChart._drawingIntervalSeconds) > 0) {
        return Number(ownPriceChart._drawingIntervalSeconds);
    }
    return map[selectedChartInterval || "1"] || 900;
}

function drawingChartCandles() {
    const symbol = String(selectedChartSymbol || "").toUpperCase();
    if (!symbol) return [];
    const interval = ownChartIntervalToBinance(selectedChartInterval || "1");
    const oiPeriod = ownOiPeriodForInterval(selectedChartInterval || "1");
    return ownPriceHistoryCache.get(`${symbol}|${interval}|${oiPeriod}`)?.candles || [];
}

function normalizeDrawingScaleTime(value) {
    if (typeof value === "number") return Number(value);
    if (typeof value === "object" && value && value.year) {
        return Math.floor(Date.UTC(value.year, value.month - 1, value.day) / 1000);
    }
    return NaN;
}

// Same coordinate model as Trader Diary: remember the real time under the
// cursor, keep it as anchorTime, and snap only the rendered point to the
// corresponding candle of the currently displayed timeframe. The drawing
// itself remains stored in time/price coordinates and is never tied to a
// candle index.
function snapTimeToCurrentCandle(sec) {
    const candles = drawingChartCandles();
    if (!candles.length || !Number.isFinite(Number(sec))) return null;
    const target = Number(sec);
    let lo = 0, hi = candles.length - 1;
    const first = Number(candles[0]?.time);
    const last = Number(candles[hi]?.time);
    if (!Number.isFinite(first) || !Number.isFinite(last)) return null;
    if (target <= first) return first;
    if (target >= last) return last;
    while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        const t = Number(candles[mid]?.time);
        if (t === target) return t;
        if (t < target) lo = mid + 1;
        else hi = mid - 1;
    }
    return hi >= 0 ? Number(candles[hi].time) : first;
}

function drawingLastCandleInfo() {
    const candles = drawingChartCandles();
    if (candles.length < 2) return null;
    const last = Number(candles[candles.length - 1]?.time);
    const prev = Number(candles[candles.length - 2]?.time);
    if (!Number.isFinite(last) || !Number.isFinite(prev) || last <= prev) return null;
    return {lastTime:last, prevTime:prev, interval:last-prev};
}

function drawingNativeTimeToX(time) {
    if (!ownPriceChart || !Number.isFinite(Number(time))) return null;
    try {
        const x = ownPriceChart.timeScale().timeToCoordinate(Number(time));
        return x != null && Number.isFinite(Number(x)) ? Number(x) : null;
    } catch (_) {
        return null;
    }
}

function drawingTimeToX(time) {
    if (!ownPriceChart || !Number.isFinite(Number(time))) return null;
    const target = Number(time);
    const direct = drawingNativeTimeToX(target);
    if (direct != null) return direct;

    const scale = ownPriceChart.timeScale();
    const info = drawingLastCandleInfo();
    if (!info) return null;

    // Future coordinates are extrapolated from the real last-candle spacing.
    // This is only drawing geometry; no whitespace candles are added to the
    // market series, so the chart itself cannot shrink when TF changes.
    if (target > info.lastTime) {
        const lastX = drawingNativeTimeToX(info.lastTime);
        const prevX = drawingNativeTimeToX(info.prevTime);
        if (lastX != null && prevX != null) {
            return lastX + (target - info.lastTime) / info.interval * (lastX - prevX);
        }
    }

    // For an arbitrary in-between time, use the corresponding current candle
    // exactly as Trader Diary does. This prevents 15m/1h drawings from drifting
    // because the original minute timestamp is not an exact candle timestamp.
    const snapped = snapTimeToCurrentCandle(target);
    if (snapped != null) {
        const x = drawingNativeTimeToX(snapped);
        if (x != null) return x;
    }

    // Keep a native logical-coordinate fallback only for chart initialization.
    // It is never used to manufacture a second visible-range coordinate system.
    try {
        const logical = scale.coordinateToLogical(drawingNativeTimeToX(info.lastTime) ?? 0);
        const lastX = drawingNativeTimeToX(info.lastTime);
        if (logical != null && Number.isFinite(Number(logical)) && lastX != null) {
            const targetLogical = Number(logical) + (target - info.lastTime) / info.interval;
            const x = scale.logicalToCoordinate(targetLogical);
            return x != null && Number.isFinite(Number(x)) ? Number(x) : null;
        }
    } catch (_) {}
    return null;
}

function pointToPixels(point) {
    if (!point) return null;
    const rawTime = point.anchorTime != null ? Number(point.anchorTime) : Number(point.time);
    if (!Number.isFinite(rawTime)) return null;
    let renderTime = snapTimeToCurrentCandle(rawTime);
    const info = drawingLastCandleInfo();
    // A future point must remain future; snapping it to the last candle would
    // collapse the drawing back onto the chart tail.
    if (info && rawTime > info.lastTime) renderTime = rawTime;
    if (renderTime == null) renderTime = Number(point.time);

    const x = drawingTimeToX(renderTime);
    const y = ownCandleSeries?.priceToCoordinate(point.price);
    if (x == null || y == null || !Number.isFinite(x) || !Number.isFinite(y)) return null;
    return {x, y};
}

function pixelToPoint(x, y) {
    if (!ownPriceChart || !ownCandleSeries) return null;
    const scale = ownPriceChart.timeScale();
    const price = ownCandleSeries.coordinateToPrice(y);
    if (price == null || !Number.isFinite(Number(price))) return null;

    let rawTime = null;
    try { rawTime = normalizeDrawingScaleTime(scale.coordinateToTime(Number(x))); } catch (_) {}
    if (!Number.isFinite(rawTime)) {
        const info = drawingLastCandleInfo();
        if (info) {
            const lastX = drawingNativeTimeToX(info.lastTime);
            const prevX = drawingNativeTimeToX(info.prevTime);
            if (lastX != null && prevX != null && Math.abs(lastX-prevX) > 0.0001) {
                rawTime = info.lastTime + ((Number(x)-lastX)/(lastX-prevX))*info.interval;
            }
        }
    }
    if (!Number.isFinite(rawTime)) return null;

    const snappedTime = snapTimeToCurrentCandle(rawTime);
    return {
        time: snappedTime != null ? snappedTime : rawTime,
        anchorTime: rawTime,
        price: Number(price)
    };
}

function formatPrice(value) {
    return Number(value).toLocaleString("en-US", {maximumSignificantDigits: 7});
}

function formatTime(value) {
    return new Date(Number(value) * 1000).toLocaleString();
}

function nearestOiPoint(time) {
    if (!ownOiPoints.length || time == null) return null;
    let lo = 0, hi = ownOiPoints.length - 1;
    while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (ownOiPoints[mid].time < time) lo = mid + 1;
        else hi = mid - 1;
    }
    const a = ownOiPoints[Math.max(0, hi)];
    const b = ownOiPoints[Math.min(ownOiPoints.length - 1, lo)];
    if (!a) return b || null;
    if (!b) return a;
    return Math.abs(a.time - time) <= Math.abs(b.time - time) ? a : b;
}

function updateHoverTooltip() {
    if (chartHoverTooltip && !chartHoverTooltip.__ohlcHoverActive) chartHoverTooltip.style.display = "none";
    document.querySelectorAll(".chart-grid-price .chart-hover-tooltip").forEach(el => {
        if (!el.__ohlcHoverActive) el.style.display = "none";
    });
}

function clearDrawingPreview() {
    drawingPreview = null;
    drawingPoints = [];
    drawingInProgress = false;
}

function setDrawingTool(tool) {
    // In grid mode the drawing layer must always follow the currently active
    // mini-chart. The toolbar lives in the legacy chart container, so make
    // the active slot explicit before enabling pointer input.
    if (tool && document.getElementById("chartContent")?.classList.contains("grid-active")) {
        const activeSlot = chartGridWorkspace?.querySelector(".chart-grid-slot.active-slot");
        if (activeSlot) activateGridSlot(activeSlot);
    }
    activeDrawingTool = activeDrawingTool === tool ? null : tool;
    document.querySelectorAll(".drawing-tool").forEach(item => item.classList.toggle("active", item.dataset.tool === activeDrawingTool));
    drawingCanvas.classList.toggle("active", !!activeDrawingTool);
    if (activeDrawingTool) {
        observeActiveDrawingGeometry();
        requestAnimationFrame(() => resetDrawingCanvas());
    }
    if (!activeDrawingTool) {
        clearDrawingPreview();
        drawingStatus.classList.remove("open");
    } else {
        const names = {signal:"Сигнальный уровень", horizontal:"Горизонтальный уровень", trend:"Трендовая", ray:"Луч", ruler:"Линейка", rectangle:"Прямоугольник", draw:"Рисование", longPosition:"Long Position", shortPosition:"Short Position"};
        drawingStatus.textContent = names[activeDrawingTool] || activeDrawingTool;
        drawingStatus.classList.add("open");
    }
    redrawDrawings();
}

function drawLine(ctx, a, b, options={}) {
    ctx.save();
    ctx.strokeStyle = options.color || "#d8dee9";
    ctx.lineWidth = options.width || 1.5;
    if (options.dash) ctx.setLineDash(options.dash);
    ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke();
    ctx.restore();
}

function drawEditHandle(ctx, point, active=false) {
    if (!point) return;
    ctx.save();
    ctx.beginPath();
    ctx.arc(point.x, point.y, active ? 7 : 6, 0, Math.PI * 2);
    ctx.fillStyle = "#11161d";
    ctx.fill();
    ctx.lineWidth = 2;
    ctx.strokeStyle = "#f1e05a";
    ctx.stroke();
    ctx.restore();
}

function distanceToSegment(px, py, ax, ay, bx, by) {
    const dx = bx - ax, dy = by - ay;
    const len2 = dx * dx + dy * dy;
    if (len2 <= 0.000001) return Math.hypot(px - ax, py - ay);
    let t = ((px - ax) * dx + (py - ay) * dy) / len2;
    t = Math.max(0, Math.min(1, t));
    const x = ax + t * dx, y = ay + t * dy;
    return Math.hypot(px - x, py - y);
}

function drawingPixelGeometry(d, width) {
    if (!d || d.type === "horizontal" || d.type === "signal" || d.type === "free") return null;
    const a = pointToPixels(d.a), b = d.b ? pointToPixels(d.b) : null;
    if (!a) return null;
    if (d.type === "ray" && b) {
        const dx = b.x - a.x, dy = b.y - a.y;
        if (Math.abs(dx) <= 0.001) return {a, b, end: {x:a.x, y:a.y}};
        const endX = dx > 0 ? width : 0;
        return {a, b, end:{x:endX, y:a.y + dy * ((endX - a.x) / dx)}};
    }
    return {a, b};
}

function hitTestDrawing(x, y) {
    const tolerance = 9;
    for (let i = chartDrawings.length - 1; i >= 0; i--) {
        const d = chartDrawings[i];
        if (d.type === "position") {
            const ey = ownCandleSeries?.priceToCoordinate(d.entry);
            const sy = ownCandleSeries?.priceToCoordinate(d.stop);
            const ty = ownCandleSeries?.priceToCoordinate(d.target);
            if (ey != null && Math.abs(y-ey) <= tolerance) return {index:i, part:"entry"};
            if (sy != null && Math.abs(y-sy) <= tolerance) return {index:i, part:"stop"};
            if (ty != null && Math.abs(y-ty) <= tolerance) return {index:i, part:"target"};
            const px = pointToPixels({time:d.time, price:d.entry})?.x ?? 0;
            if (x >= px && x <= (drawingCanvas.clientWidth || width) && ((y >= Math.min(ey,ty) && y <= Math.max(ey,ty)) || (y >= Math.min(ey,sy) && y <= Math.max(ey,sy)))) return {index:i, part:"body"};
            continue;
        }
        if (d.type === "horizontal" || d.type === "signal") {
            const levelY = ownCandleSeries?.priceToCoordinate(d.price);
            if (levelY != null && Math.abs(y - levelY) <= tolerance) {
                return {index:i, part:"body"};
            }
            continue;
        }
        if (d.type === "free") continue;
        const g = drawingPixelGeometry(d, drawingCanvas.clientWidth || drawingCanvas.width || 0);
        if (!g) continue;

        if (g.a && Math.hypot(x - g.a.x, y - g.a.y) <= 10) return {index:i, part:"a"};
        if (g.b && Math.hypot(x - g.b.x, y - g.b.y) <= 10) return {index:i, part:"b"};

        if (d.type === "rectangle" && g.b) {
            const l=Math.min(g.a.x,g.b.x), r=Math.max(g.a.x,g.b.x), t=Math.min(g.a.y,g.b.y), btm=Math.max(g.a.y,g.b.y);
            if (distanceToSegment(x,y,l,t,r,t)<=tolerance ||
                distanceToSegment(x,y,r,t,r,btm)<=tolerance ||
                distanceToSegment(x,y,r,btm,l,btm)<=tolerance ||
                distanceToSegment(x,y,l,btm,l,t)<=tolerance) return {index:i, part:"body"};
        } else if (g.b) {
            const end = d.type === "ray" ? g.end : g.b;
            if (end && distanceToSegment(x,y,g.a.x,g.a.y,end.x,end.y)<=tolerance) return {index:i, part:"body"};
        }
    }
    return null;
}

function translateDrawing(d, dt, dp) {
    const out = JSON.parse(JSON.stringify(d));
    const movePoint = point => {
        if (!point) return;
        point.time = Number(point.time) + dt;
        if (point.anchorTime != null && Number.isFinite(Number(point.anchorTime))) point.anchorTime = Number(point.anchorTime) + dt;
        point.price = Number(point.price) + dp;
    };
    movePoint(out.a);
    movePoint(out.b);
    if (Array.isArray(out.points)) out.points.forEach(movePoint);
    return out;
}

function updateDrawingPoint(d, key, p) {
    if (!d || !p || !d[key]) return;
    d[key] = {time:p.time, anchorTime:p.anchorTime != null ? p.anchorTime : p.time, price:p.price};
}

function drawSelectedDrawingHandles(ctx, d) {
    if (!d || d.type === "horizontal" || d.type === "signal" || d.type === "free") return;
    const a = pointToPixels(d.a);
    const b = d.b ? pointToPixels(d.b) : null;
    drawEditHandle(ctx, a, drawingEditState?.part === "a");
    if (b) drawEditHandle(ctx, b, drawingEditState?.part === "b");
}

function chartOverlayTemplateTimeframe() {
    const t=activeMarketTemplate?.(), st=t?.structure||{};
    const tfs=formationTimeframeRange(st.timeframeMode||"only",st.timeframeFrom,st.timeframeTo||st.timeframe);
    const tf=tfs[0]||st.timeframe||"1h";
    return PATTERN_INTERVALS_CLIENT.has(tf)?tf:"1h";
}
const PATTERN_INTERVALS_CLIENT = new Set(["1m","3m","5m","15m","30m","1h","2h","3h","4h","6h","8h","12h","1d","3d","1w"]);
function chartAnalysisForCurrent() {
    if (!selectedChartSymbol) return null;
    // Overlay visualization is independent from screener filter settings.
    // Always use the timeframe of the chart that is currently visible.
    const tf = ownChartIntervalToBinance(selectedChartInterval || "1");
    return chartAnalysisCache.get(`${selectedChartSymbol}|${tf}`) || null;
}
function updateChartAnalysisTailState(symbol, timeframe, candles) {
    const key = `${String(symbol||"").toUpperCase()}|${String(timeframe||"").toLowerCase()}`;
    const state = chartAnalysisStateCache.get(key);
    if (!state || !Array.isArray(candles) || !candles.length) return null;
    const last = candles[candles.length - 1];
    const lastOpen = Number(last?.time || 0);
    const lastClose = Number(last?.close || 0);
    const tail = {
        open: Number(last?.open || 0),
        high: Number(last?.high || 0),
        low: Number(last?.low || 0),
        close: lastClose,
        volume: Number(last?.volume || 0)
    };
    if (getChartAnalysisUpdateMode(state, candles) === "tail" || getChartAnalysisUpdateMode(state, candles) === "same") {
        state.last_candle_close = lastClose;
        state.last_candle = tail;
        state.candle_count = candles.length;
        // Keep the analysis object alive and update only the live candle tail.
        // Heavy calculations remain untouched until a new candle boundary appears.
        if (state.analysis && typeof state.analysis === "object") {
            state.analysis.candle_close = lastClose;
            state.analysis.live_candle_update_time = Date.now();
        }
        state.updatedAt = Date.now();
        chartAnalysisStateCache.set(key, state);
        return state.analysis || null;
    }
    return null;
}


function getChartAnalysisUpdateMode(state, candles) {
    if (!state || !Array.isArray(candles) || !candles.length) return "full";
    const last = candles[candles.length - 1];
    const open = Number(last?.time || 0);
    if (!open || state.last_candle_open !== open) return "full";
    const old = state.last_candle || {};
    const changed = Number(old.close) !== Number(last.close) ||
        Number(old.high) !== Number(last.high) ||
        Number(old.low) !== Number(last.low) ||
        Number(old.volume) !== Number(last.volume);
    return changed ? "tail" : "same";
}

function cancelChartOverlayAnalysis() {
    // Turning both chart overlay switches off is a hard stop: invalidate every
    // pending result before aborting the request, so a late promise callback
    // cannot schedule analysis redraws or repopulate the active overlay state.
    chartOverlayAnalysisGeneration++;
    if (chartOverlayAnalysisAbortController) {
        try { chartOverlayAnalysisAbortController.abort(); } catch (_) {}
        chartOverlayAnalysisAbortController = null;
    }
    chartAnalysisInFlight.clear();
    chartAnalysisBusyKey = "";
}

function isChartOverlayAnalysisEnabled() {
    // A selected structure template is also an explicit request to show the
    // structures it finds on the active chart.  The separate chart switches
    // continue to control formations/levels when no structure template is active.
    const activeStructure=activeMarketTemplate()?.structure||{};
    return !!(chartShowFormations || chartShowLevels || hasTemplateStructureEngineSettings(activeStructure));
}
function chartStructureOverlayFlags() {
    const activeStructure=activeMarketTemplate()?.structure||{};
    const mode=String(activeStructure.structureSearchMode||"none");
    if (mode === "all") return {horizontal:true,trendline:true,cascade:true};
    if (mode === "both") return {horizontal:true,trendline:true,cascade:false};
    if (mode === "horizontal_cascade") return {horizontal:true,trendline:false,cascade:true};
    if (mode === "trendline_cascade") return {horizontal:false,trendline:true,cascade:true};
    if (mode === "horizontal") return {horizontal:true,trendline:false,cascade:false};
    if (mode === "trendline") return {horizontal:false,trendline:true,cascade:false};
    if (mode === "cascade") return {horizontal:false,trendline:false,cascade:true};
    return chartShowLevels ? {horizontal:true,trendline:true,cascade:true} : {horizontal:false,trendline:false,cascade:false};
}
function isStructureTemplateOverlayEnabled() {
    const f=chartStructureOverlayFlags();
    return f.horizontal || f.trendline || f.cascade;
}

async function ensureChartOverlayAnalysis(symbol, timeframe) {
    if (!isChartOverlayAnalysisEnabled()) return null;
    const safeSymbol = String(symbol||"").toUpperCase();
    const safeTimeframe = String(timeframe||"1h");
    const key = `${safeSymbol}|${safeTimeframe}`;
    const chartTf = ownChartIntervalToBinance(selectedChartInterval || "1");
    const priceKey = `${safeSymbol}|${chartTf}|${ownOiPeriodForInterval(selectedChartInterval || "1")}`;
    const candles = ownPriceHistoryCache.get(priceKey)?.candles || [];
    const latestChartCandle = candles.length ? candles[candles.length - 1] : null;
    const latestChartCandleTime = latestChartCandle ? Number(latestChartCandle.time) : null;
    const latestChartCandleClose = latestChartCandle ? Number(latestChartCandle.close) : null;
    const cached = chartAnalysisCache.get(key) || chartIndicatorLayerCache.get(key);
    const cachedCandleTime = chartAnalysisCandleCache.get(key);
    const cachedState = chartAnalysisStateCache.get(key) || null;
    const tailState = updateChartAnalysisTailState(safeSymbol, safeTimeframe, candles);
    if (tailState) return tailState;
    const atomicState = chartAtomicStateCache.get(chartStateKey(safeSymbol, selectedChartInterval || "1"));

    // Same candle: the heavy formation/indicator calculation is already valid.
    // A changing close/high/low/volume is only a tail update, so keep the
    // existing analysis result and update its local state instead of issuing a
    // second full pattern-analysis request.
    if (cached && safeTimeframe === chartTf &&
        latestChartCandleTime != null && cachedCandleTime === latestChartCandleTime) {
        if (cachedState) {
            cachedState.last_candle_close = latestChartCandleClose;
            cachedState.candle_count = candles.length;
            cachedState.updatedAt = Date.now();
        }
        return cached;
    }
    if (cached && safeTimeframe !== chartTf) return cached;
    if (atomicState?.analysis && atomicState.savedAt && (Date.now() - atomicState.savedAt) < 10000) {
        const restored = atomicState.analysis;
        const restoredCandleTime = Number(restored?.candle_open_time || 0) || null;
        chartAnalysisCache.set(key, restored);
        chartIndicatorLayerCache.delete(key);
        chartIndicatorLayerCache.set(key, restored);
        chartAnalysisCandleCache.set(key, restoredCandleTime);
        chartAnalysisStateCache.set(key, {
            symbol: safeSymbol,
            timeframe: safeTimeframe,
            last_candle_open: restoredCandleTime,
            last_candle_close: Number(restored?.candle_close ?? latestChartCandleClose) || null,
            candle_count: candles.length,
            indicator_state: restored?.indicator_state || restored?.indicators || null,
            analysis: restored,
            updatedAt: Date.now()
        });
        return restored;
    }

    const running = chartAnalysisInFlight.get(key);
    if (running) return running;

    const requestGeneration = chartOverlayAnalysisGeneration;
    const controller = new AbortController();
    chartOverlayAnalysisAbortController = controller;
    const request = (async () => {
        try {
            if (!isChartOverlayAnalysisEnabled() || requestGeneration !== chartOverlayAnalysisGeneration) return null;
            const activeStructure = activeMarketTemplate()?.structure || {};
            const chartParams = new URLSearchParams({
                symbols: safeSymbol, interval: safeTimeframe, limit: "750",
                level_distance: String(Number(activeStructure.horizontalLevelSearchPeriod) || 50),
                level_strict: activeStructure.horizontalLevelSearchStrict ? "1" : "0",
                level_touches: String(Number(activeStructure.horizontalLevelTouches) || 2),
                level_tolerance: String(Number(activeStructure.horizontalLevelTouchTolerancePct) || 0.1),
                level_no_cross: activeStructure.horizontalLevelNoCross ? "1" : "0",
                level_minor_pierce: activeStructure.horizontalLevelAllowMinorPierce ? "1" : "0",
                trend_distance: String(Number(activeStructure.trendlineSearchPeriod) || 50),
                trend_strict: activeStructure.trendlineSearchStrict ? "1" : "0",
                trend_touches: String(Number(activeStructure.trendlineTouches) || 2),
                trend_tolerance: String(Number(activeStructure.trendlineTouchTolerancePct) || 0.1),
                trend_no_cross: activeStructure.trendlineNoCross ? "1" : "0",
                trend_minor_pierce: activeStructure.trendlineAllowMinorPierce ? "1" : "0"
            });
            if (activeStructure.horizontalLevelLifetimeHours != null && activeStructure.horizontalLevelLifetimeHours !== "") chartParams.set("level_lifetime", String(activeStructure.horizontalLevelLifetimeHours));
            if (activeStructure.trendlineLifetimeHours != null && activeStructure.trendlineLifetimeHours !== "") chartParams.set("trend_lifetime", String(activeStructure.trendlineLifetimeHours));
            const r = await fetch(`/api/pattern_analysis?${chartParams.toString()}`, {cache:"no-store", signal: controller.signal});
            const data = await r.json();
            if (!isChartOverlayAnalysisEnabled() || requestGeneration !== chartOverlayAnalysisGeneration) return null;
            const result = data?.results?.[0];
            if (result && isChartOverlayAnalysisEnabled() && requestGeneration === chartOverlayAnalysisGeneration) {
                chartAnalysisCache.set(key, result);
                chartIndicatorLayerCache.delete(key);
                chartIndicatorLayerCache.set(key, result);
                const analysisCandleTime = Number(result.candle_open_time);
                const normalizedCandleTime = Number.isFinite(analysisCandleTime) && analysisCandleTime > 0
                    ? analysisCandleTime : (safeTimeframe === chartTf ? latestChartCandleTime : null);
                chartAnalysisCandleCache.set(key, normalizedCandleTime);
                chartAnalysisStateCache.delete(key);
                chartAnalysisStateCache.set(key, {
                    symbol: safeSymbol,
                    timeframe: safeTimeframe,
                    last_candle_open: normalizedCandleTime,
                    last_candle_close: Number(result.candle_close ?? latestChartCandleClose) || null,
                    candle_count: candles.length,
                    last_candle: latestChartCandle ? {open:Number(latestChartCandle.open||0), high:Number(latestChartCandle.high||0), low:Number(latestChartCandle.low||0), close:Number(latestChartCandle.close||0), volume:Number(latestChartCandle.volume||0)} : null,
                    indicator_state: result.indicator_state || result.indicators || null,
                    analysis: result,
                    updatedAt: Date.now()
                });
                while (chartAnalysisStateCache.size > CHART_ANALYSIS_STATE_CACHE_MAX) {
                    const oldest = chartAnalysisStateCache.keys().next().value;
                    if (oldest === undefined) break;
                    chartAnalysisStateCache.delete(oldest);
                }
                while (chartIndicatorLayerCache.size > CHART_INDICATOR_LAYER_CACHE_MAX) {
                    const oldest = chartIndicatorLayerCache.keys().next().value;
                    if (oldest === undefined) break;
                    chartIndicatorLayerCache.delete(oldest);
                }
                while (chartAnalysisCandleCache.size > CHART_ANALYSIS_CANDLE_CACHE_MAX) {
                    const oldest = chartAnalysisCandleCache.keys().next().value;
                    if (oldest === undefined) break;
                    chartAnalysisCandleCache.delete(oldest);
                }
            }
            return result || null;
        } catch (e) {
            if (e?.name !== "AbortError") console.warn("Chart structure overlay error:", e);
            return null;
        } finally {
            if (chartAnalysisInFlight.get(key) === request) chartAnalysisInFlight.delete(key);
            if (chartAnalysisBusyKey === key) chartAnalysisBusyKey = "";
            if (chartOverlayAnalysisAbortController === controller) chartOverlayAnalysisAbortController = null;
        }
    })();
    chartAnalysisInFlight.set(key, request);
    chartAnalysisBusyKey = key;
    return request;
}

function drawAnalysisOverlay(ctx, width, height) {
    const analysis = chartAnalysisForCurrent();
    if (!analysis?.overlay) return;
    const o = analysis.overlay;
    const point = p => {
        if (!p || !ownPriceChart || !ownCandleSeries) return null;
        const x = ownPriceChart.timeScale().timeToCoordinate(Number(p.time));
        const y = ownCandleSeries.priceToCoordinate(Number(p.price));
        return x == null || y == null ? null : {x, y};
    };
    const line = (a,b,dash=false) => {
        const pa=point(a), pb=point(b); if(!pa||!pb)return;
        ctx.save(); ctx.strokeStyle="#7f8ea3"; ctx.lineWidth=1.25; if(dash)ctx.setLineDash([6,5]);
        ctx.beginPath();ctx.moveTo(pa.x,pa.y);ctx.lineTo(pb.x,pb.y);ctx.stroke();ctx.restore();
    };
    if (isStructureTemplateOverlayEnabled()) {
        const structureFlags=chartStructureOverlayFlags();
        const drawLevel=(obj, active)=>{
            if(!obj || !Number.isFinite(Number(obj.price)))return;
            const y=ownCandleSeries?.priceToCoordinate(Number(obj.price)); if(y==null)return;
            const firstTime=Number(obj.time ?? obj.start?.time ?? obj.first_time ?? 0);
            const firstX=Number.isFinite(firstTime) && firstTime>0 ? ownPriceChart?.timeScale().timeToCoordinate(firstTime) : null;
            const startX=firstX==null ? 0 : Math.max(0, Number(firstX));
            ctx.save();
            ctx.strokeStyle="#ffffff";ctx.lineWidth=1.5;ctx.setLineDash([]);ctx.beginPath();ctx.moveTo(startX,y);ctx.lineTo(width,y);ctx.stroke();
            const text=`${active ? (obj.type==="long"?"LONG":"SHORT") : (obj===o.support?"Support":"Resistance")} · ${formatPrice(obj.price)} · ${obj.touches||0} кас.`;
            ctx.font="10px Arial";const tw=ctx.measureText(text).width+12;const labelX=Math.min(startX+6,Math.max(7,width-tw-7));ctx.fillStyle="rgba(12,16,22,.9)";ctx.fillRect(labelX,y-18,tw,17);ctx.fillStyle="#ffffff";ctx.fillText(text,labelX+6,y-6);
            ctx.beginPath();ctx.arc(startX,y,3.5,0,Math.PI*2);ctx.fillStyle="#ffffff";ctx.fill();
            ctx.restore();
        };
        const cascadeLevels = Array.isArray(o.cascade_levels) ? o.cascade_levels : [];
        if (structureFlags.cascade && cascadeLevels.length) {
            cascadeLevels.forEach((obj, idx)=>{
                if(!obj || !Number.isFinite(Number(obj.price))) return;
                const y=ownCandleSeries?.priceToCoordinate(Number(obj.price));
                if(y==null) return;
                const firstTime=Number(obj.time);
                const firstX=Number.isFinite(firstTime) ? ownPriceChart?.timeScale().timeToCoordinate(firstTime) : null;
                const startX=(firstX==null ? 0 : Math.max(0, Number(firstX)));
                const text=`Каскад ${idx+1}/${cascadeLevels.length} · ${formatPrice(obj.price)}`;
                ctx.save();
                // Cascade levels are structural rays: start exactly at the
                // first detected touch and continue into the future.
                ctx.strokeStyle="#ffffff";
                ctx.lineWidth=1.5;
                ctx.setLineDash([]);
                ctx.beginPath();
                ctx.moveTo(startX,y);
                ctx.lineTo(width,y);
                ctx.stroke();
                ctx.font="10px Arial";
                const labelWidth=ctx.measureText(text).width+12;
                const labelX=Math.min(startX+6, Math.max(7, width-labelWidth-7));
                ctx.fillStyle="rgba(12,16,22,.94)";
                ctx.fillRect(labelX,y+3,labelWidth,16);
                ctx.fillStyle="#ffffff";
                ctx.fillText(text,labelX+6,y+14);
                ctx.beginPath();
                ctx.arc(startX,y,3.5,0,Math.PI*2);
                ctx.fillStyle="#ffffff";
                ctx.fill();
                ctx.restore();
            });
        }
        if (structureFlags.horizontal) {
            drawLevel(o.support,false); drawLevel(o.resistance,false); drawLevel(o.level,true);
        }
        const drawTrend=(obj, label)=>{
            if(!obj?.start || !obj?.end) return;
            const a=point(obj.start), b=point(obj.end);
            if(!a || !b) return;
            const dx=b.x-a.x, dy=b.y-a.y;
            const endX=width;
            const endY=Math.abs(dx)>0.0001 ? a.y + dy*((endX-a.x)/dx) : b.y;
            ctx.save();ctx.strokeStyle="#ffffff";ctx.lineWidth=1.5;ctx.setLineDash([]);
            ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(endX,endY);ctx.stroke();
            ctx.beginPath();ctx.arc(a.x,a.y,3.5,0,Math.PI*2);ctx.fillStyle="#ffffff";ctx.fill();
            const touches=Number(obj.touches||0);
            const text=`${label} · ${touches} кас.`;
            ctx.font="10px Arial";const tw=ctx.measureText(text).width+12;
            const labelX=Math.min(a.x+6,Math.max(7,width-tw-7));
            const labelY=Math.max(18,Math.min(height-5,endY));
            ctx.fillStyle="rgba(12,16,22,.9)";ctx.fillRect(labelX,labelY-17,tw,16);
            ctx.fillStyle="#ffffff";ctx.fillText(text,labelX+6,labelY-6);ctx.restore();
        };
        const upperLines=o.upper_lines||[], lowerLines=o.lower_lines||[];
        if (structureFlags.trendline) {
            upperLines.forEach(x=>drawTrend(x,"Наклонка SHORT"));
            lowerLines.forEach(x=>drawTrend(x,"Наклонка LONG"));
        }
    }
    if (chartShowFormations && analysis.pattern) {
        const formationFlags=chartStructureOverlayFlags();
        if((!hasTemplateStructureEngineSettings(activeMarketTemplate()?.structure||{}) || formationFlags.trendline) && o.upper && formationTrendLineVisible(o.upper)) line(o.upper.start,o.upper.end);
        if((!hasTemplateStructureEngineSettings(activeMarketTemplate()?.structure||{}) || formationFlags.trendline) && o.lower && formationTrendLineVisible(o.lower)) line(o.lower.start,o.lower.end);
        const a=analysis.pattern, st=analysis.stage||"";
        const badge=`${a}${analysis.pattern_type?" · "+analysis.pattern_type:""}${st?" · "+st.replaceAll("_"," "):""}`;
        ctx.save();ctx.font="10px Arial";const w=ctx.measureText(badge).width+14;ctx.fillStyle="rgba(12,16,22,.92)";ctx.fillRect(8,8,w,19);ctx.fillStyle="#e5ebf2";ctx.fillText(badge,15,21);ctx.restore();
    }
}

function redrawDrawings() {
    if (!drawingCanvas || !priceChartEl) return;
    const canvasState = resetDrawingCanvas();
    if (!canvasState) return;
    const {ctx, width, height} = canvasState;
    ctx.clearRect(0, 0, width, height);
    drawAnalysisOverlay(ctx, width, height);

    const DRAWING_YELLOW = "#f1e05a";
    const SIGNAL_PURPLE = "#b36cff";
    const PREVIEW_DASH = [7, 5];

    for (const d of chartDrawings) {
        if (d.type === "horizontal" || d.type === "signal") {
            const y = ownCandleSeries?.priceToCoordinate(d.price);
            if (y == null) continue;

            if (d.type === "signal") {
                const signalKey = `${selectedChartSymbol}:${d.id || chartDrawings.indexOf(d)}`;
                const signalFired = signalLevelFired.has(signalKey);
                drawLine(ctx, {x:0, y}, {x:width, y}, {
                    color: signalFired ? "#cdb4ff" : SIGNAL_PURPLE,
                    dash: [7, 5]
                });
            } else {
                drawLine(ctx, {x:0, y}, {x:width, y}, {
                    color: DRAWING_YELLOW
                });
            }
            continue;
        }

        if (d.type === "position") {
            const entryY = ownCandleSeries?.priceToCoordinate(d.entry);
            const stopY = ownCandleSeries?.priceToCoordinate(d.stop);
            const targetY = ownCandleSeries?.priceToCoordinate(d.target);
            if (entryY == null || stopY == null || targetY == null) continue;

            // Position box is confined to the price-chart area. It never
            // extends into the time axis / OI panel. Width is visual only;
            // the risk/reward geometry is controlled by price levels.
            const anchorX = pointToPixels({time:d.time, price:d.entry})?.x ?? width * 0.35;
            const boxWidth = Math.min(300, Math.max(150, width * 0.28));
            const x1 = Math.max(8, Math.min(width - boxWidth - 8, anchorX));
            const x2 = x1 + boxWidth;
            const profitTop = Math.min(entryY, targetY), profitBottom = Math.max(entryY, targetY);
            const riskTop = Math.min(entryY, stopY), riskBottom = Math.max(entryY, stopY);

            ctx.save();
            ctx.globalAlpha = .22;
            ctx.fillStyle = "#31c48d";
            ctx.fillRect(x1, profitTop, x2-x1, Math.max(1, profitBottom-profitTop));
            ctx.fillStyle = "#ef4444";
            ctx.fillRect(x1, riskTop, x2-x1, Math.max(1, riskBottom-riskTop));
            ctx.restore();

            drawLine(ctx,{x:x1,y:entryY},{x:x2,y:entryY},{color:"#e5e7eb",dash:[5,4]});
            drawLine(ctx,{x:x1,y:targetY},{x:x2,y:targetY},{color:"#31c48d"});
            drawLine(ctx,{x:x1,y:stopY},{x:x2,y:stopY},{color:"#ef4444"});

            const entry = Number(d.entry);
            const stop = Number(d.stop);
            const target = Number(d.target);
            const riskPct = entry ? Math.abs((stop-entry)/entry)*100 : 0;
            const rewardPct = entry ? Math.abs((target-entry)/entry)*100 : 0;
            const rr = riskPct > 0 ? rewardPct / riskPct : 0;
            const stopText = `STOP  ${formatPrice(stop)}  (${riskPct.toFixed(2)}%)`;
            const targetText = `TARGET  ${formatPrice(target)}  (${rewardPct.toFixed(2)}%)`;
            const rrText = `R:R  1:${rr.toFixed(2)}`;

            // Labels stay inside the position box, centered on the price area.
            ctx.save();
            ctx.font = "bold 11px Arial";
            ctx.textAlign = "center";
            ctx.textBaseline = "middle";
            const centerX = (x1+x2)/2;

            const labelBg = (text, y, textColor) => {
                if (y == null || y < 10 || y > height-10) return;
                const w = Math.min(x2-x1-8, ctx.measureText(text).width + 12);
                const h = 18;
                ctx.fillStyle = "rgba(17,22,29,.88)";
                ctx.fillRect(centerX-w/2, y-h/2, w, h);
                ctx.fillStyle = textColor;
                ctx.fillText(text, centerX, y);
            };

            labelBg(stopText, stopY, "#ff6b6b");
            labelBg(rrText, entryY, "#f1f5f9");
            labelBg(targetText, targetY, "#55d6a5");
            ctx.restore();
            continue;
        }

        if (d.type === "free") {
            const pts = d.points.map(pointToPixels).filter(Boolean);
            if (pts.length < 2) continue;
            ctx.save();
            ctx.strokeStyle = DRAWING_YELLOW;
            ctx.lineWidth = 1.5;
            ctx.beginPath();
            ctx.moveTo(pts[0].x, pts[0].y);
            for (let i=1; i<pts.length; i++) ctx.lineTo(pts[i].x, pts[i].y);
            ctx.stroke();
            ctx.restore();
            continue;
        }

        const a = pointToPixels(d.a), b = pointToPixels(d.b);
        if (!a || !b) continue;

        if (d.type === "trend") {
            drawLine(ctx, a, b, {color:DRAWING_YELLOW});
        }

        if (d.type === "ray") {
            const dx = b.x - a.x;
            const dy = b.y - a.y;
            if (Math.abs(dx) > 0.001) {
                const endX = dx > 0 ? width : 0;
                const endY = a.y + dy * ((endX - a.x) / dx);
                drawLine(ctx, a, {x:endX, y:endY}, {color:DRAWING_YELLOW});
            }
        }

        if (d.type === "rectangle") {
            ctx.save();
            ctx.strokeStyle = DRAWING_YELLOW;
            ctx.lineWidth = 1.5;
            ctx.strokeRect(
                Math.min(a.x,b.x),
                Math.min(a.y,b.y),
                Math.abs(b.x-a.x),
                Math.abs(b.y-a.y)
            );
            ctx.restore();
        }

        if (d.type === "ruler") {
            drawLine(ctx, a, b, {color:DRAWING_YELLOW});
            const startPrice = Number(d.a?.price);
            const endPrice = Number(d.b?.price);
            const priceDelta = endPrice - startPrice;
            const pct = Number.isFinite(startPrice) && startPrice !== 0 && Number.isFinite(endPrice)
                ? ((endPrice / startPrice) - 1) * 100
                : 0;
            const label = `${formatPrice(Math.abs(priceDelta))}  (${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%)`;
            const mx=(a.x+b.x)/2, my=(a.y+b.y)/2;
            ctx.save();
            ctx.font="11px Arial";
            const w=ctx.measureText(label).width+12;
            ctx.fillStyle="rgba(17,22,29,.92)";
            ctx.fillRect(mx-w/2,my-22,w,18);
            ctx.fillStyle=DRAWING_YELLOW;
            ctx.fillText(label,mx-w/2+6,my-9);
            ctx.restore();
        }
    }

    if (selectedDrawingIndex >= 0 && chartDrawings[selectedDrawingIndex]) {
        drawSelectedDrawingHandles(ctx, chartDrawings[selectedDrawingIndex]);
    }

    if (drawingPreview) {
        const d = drawingPreview;

        if (d.type === "free") {
            const pts=d.points.map(pointToPixels).filter(Boolean);
            if(pts.length>1){
                ctx.save();
                ctx.strokeStyle=DRAWING_YELLOW;
                ctx.lineWidth=1.5;
                ctx.beginPath();
                ctx.moveTo(pts[0].x,pts[0].y);
                for(let i=1;i<pts.length;i++)ctx.lineTo(pts[i].x,pts[i].y);
                ctx.stroke();
                ctx.restore();
            }
        } else if (d.type === "horizontal" || d.type === "signal") {
            const y=ownCandleSeries?.priceToCoordinate(d.price);
            if(y!=null){
                const signalKey = d.type === "signal" ? `${selectedChartSymbol}:${d.id || chartDrawings.indexOf(d)}` : "";
                const signalFired = d.type === "signal" && signalLevelFired.has(signalKey);
                drawLine(ctx,{x:0,y},{x:width,y},{
                    color:d.type === "signal" ? (signalFired ? "#cdb4ff" : SIGNAL_PURPLE) : DRAWING_YELLOW,
                    dash:d.type === "signal" ? PREVIEW_DASH : undefined
                });
            }
        } else {
            const a=pointToPixels(d.a), b=pointToPixels(d.b);
            if(a&&b){
                if(d.type === "trend") {
                    drawLine(ctx,a,b,{color:DRAWING_YELLOW,dash:PREVIEW_DASH});
                }
                if(d.type === "ray") {
                    const dx=b.x-a.x,dy=b.y-a.y;
                    if(Math.abs(dx)>0.001){
                        const ex=dx > 0 ? width : 0;
                        const ey=a.y+dy*((ex-a.x)/dx);
                        drawLine(ctx,a,{x:ex,y:ey},{color:DRAWING_YELLOW,dash:PREVIEW_DASH});
                    }
                }
                if(d.type === "rectangle") {
                    ctx.save();
                    ctx.strokeStyle=DRAWING_YELLOW;
                    ctx.lineWidth=1.5;
                    ctx.setLineDash(PREVIEW_DASH);
                    ctx.strokeRect(Math.min(a.x,b.x),Math.min(a.y,b.y),Math.abs(b.x-a.x),Math.abs(b.y-a.y));
                    ctx.restore();
                }
                if(d.type === "ruler") drawLine(ctx,a,b,{color:DRAWING_YELLOW,dash:PREVIEW_DASH});
            }
        }
    }
}
    try { requestDensityChartRender(densityPresentationData); } catch (_) {}

function installDrawingEditing() {
    if (!priceChartEl) return;

    const beginEdit = (event, host, isGrid) => {
        if (activeDrawingTool || !ownPriceChart || !ownCandleSeries) return;
        if (isGrid) {
            const slot = event.target.closest?.(".chart-grid-slot");
            const priceHost = event.target.closest?.(".chart-grid-price");
            if (!slot || !priceHost) return;
            activateGridSlot(slot);
            redrawDrawings();
        }
        const rect = (isGrid ? drawingCanvas : host).getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;
        if (x < 0 || y < 0 || x > rect.width || y > rect.height) return;

        const hit = hitTestDrawing(x, y);
        if (!hit) {
            if (selectedDrawingIndex !== -1) {
                selectedDrawingIndex = -1;
                drawingEditState = null;
                redrawDrawings();
            }
            return;
        }

        selectedDrawingIndex = hit.index;
        const d = chartDrawings[hit.index];
        const p = pixelToPoint(x, y);
        if (!p) return;

        drawingEditState = {
            mode: hit.part === "a" || hit.part === "b" ? "handle" : "move",
            part: hit.part,
            pointerId: event.pointerId,
            startPoint: p,
            original: JSON.parse(JSON.stringify(d)),
            grid: !!isGrid,
            host
        };
        host.setPointerCapture?.(event.pointerId);
        event.preventDefault();
        event.stopPropagation();
        redrawDrawings();
    };

    const moveEdit = event => {
        if (!drawingEditState || drawingEditState.pointerId !== event.pointerId) return;
        const host = drawingEditState.grid ? drawingCanvas : priceChartEl;
        const rect = host.getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;
        const p = pixelToPoint(x, y);
        if (!p) return;

        const d = chartDrawings[selectedDrawingIndex];
        if (!d) return;

        if (d.type === "position") {
            if (drawingEditState.part === "entry") {
                const delta = p.price - d.entry; d.entry = p.price; d.stop += delta; d.target += delta;
            } else if (drawingEditState.part === "stop") d.stop = p.price;
            else if (drawingEditState.part === "target") d.target = p.price;
            else {
                const delta = p.price - drawingEditState.startPoint.price;
                d.entry = drawingEditState.original.entry + delta;
                d.stop = drawingEditState.original.stop + delta;
                d.target = drawingEditState.original.target + delta;
            }
        } else if (d.type === "horizontal" || d.type === "signal") {
            d.price = p.price;
        } else if (drawingEditState.mode === "handle") {
            updateDrawingPoint(d, drawingEditState.part, p);
        } else {
            const currentTime = p.anchorTime != null ? Number(p.anchorTime) : Number(p.time);
            const startTime = drawingEditState.startPoint.anchorTime != null ? Number(drawingEditState.startPoint.anchorTime) : Number(drawingEditState.startPoint.time);
            const dt = currentTime - startTime;
            const dp = p.price - drawingEditState.startPoint.price;
            chartDrawings[selectedDrawingIndex] = translateDrawing(drawingEditState.original, dt, dp);
        }
        event.preventDefault();
        event.stopPropagation();
        syncActiveDrawingsToSharedStore();
        redrawDrawings();
    };

    const finishEdit = event => {
        if (!drawingEditState || drawingEditState.pointerId !== event.pointerId) return;
        const state = drawingEditState;
        try { state.host?.releasePointerCapture?.(event.pointerId); } catch (e) {}
        const editedDrawing = chartDrawings[selectedDrawingIndex];
        if (editedDrawing?.type === "signal") { signalLevelStates.delete(`${selectedChartSymbol}:${editedDrawing.id || selectedDrawingIndex}`); signalLevelFired.delete(`${selectedChartSymbol}:${editedDrawing.id || selectedDrawingIndex}`); }
        syncActiveDrawingsToSharedStore();
        drawingEditState = null;
        if (editedDrawing?.type === "signal") syncSignalLevelsToAlertServer();
        redrawDrawings();
        event.preventDefault();
        event.stopPropagation();
    };

    priceChartEl.addEventListener("pointerdown", event => beginEdit(event, priceChartEl, false), true);
    priceChartEl.addEventListener("pointermove", moveEdit, true);
    priceChartEl.addEventListener("pointerup", finishEdit, true);
    priceChartEl.addEventListener("pointercancel", finishEdit, true);

    chartGridWorkspace?.addEventListener("pointerdown", event => beginEdit(event, event.target.closest?.(".chart-grid-price") || chartGridWorkspace, true), true);
    chartGridWorkspace?.addEventListener("pointermove", moveEdit, true);
    chartGridWorkspace?.addEventListener("pointerup", finishEdit, true);
    chartGridWorkspace?.addEventListener("pointercancel", finishEdit, true);
}

installDrawingEditing();

function installDrawingHandlers() {
    if (!drawingCanvas) return;

    const twoClickTool = () => ["trend", "ray", "ruler", "rectangle"].includes(activeDrawingTool);
    const positionTool = () => ["longPosition", "shortPosition"].includes(activeDrawingTool);

    const getPoint = (event) => {
        const rect = drawingCanvas.getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;
        if (x < 0 || y < 0 || x > rect.width || y > rect.height) return null;
        return pixelToPoint(x, y);
    };

    drawingCanvas.addEventListener("pointerdown", event => {
        if (!activeDrawingTool) return;
        const p = getPoint(event);
        if (!p) return;
        event.preventDefault();

        if (positionTool()) {
            const long = activeDrawingTool === "longPosition";
            const direction = long ? 1 : -1;
            const price = Number(p.price);
            const riskPct = 0.005;
            const rewardPct = 0.01;
            chartDrawings.push({
                type: "position", direction: long ? "long" : "short",
                entry: price,
                stop: price * (1 - direction * riskPct),
                target: price * (1 + direction * rewardPct),
                time: p.time
            });
            selectedDrawingIndex = chartDrawings.length - 1;
            drawingEditState = null;
            syncActiveDrawingsToSharedStore();
            redrawDrawings();
            setDrawingTool(null);
            return;
        }

        if (activeDrawingTool === "horizontal" || activeDrawingTool === "signal") {
            chartDrawings.push({type:activeDrawingTool, price:p.price, id: activeDrawingTool === "signal" ? makeAlertId() : undefined});
            selectedDrawingIndex = chartDrawings.length - 1;
            if (activeDrawingTool === "signal" && document.getElementById("chartContent")?.classList.contains("grid-active")) saveFocusedGridDrawingState();
            drawingEditState = null;
            redrawDrawings();
            syncActiveDrawingsToSharedStore();
            if (activeDrawingTool === "signal") syncSignalLevelsToAlertServer();
            setDrawingTool(null);
            return;
        }

        if (twoClickTool()) {
            if (!drawingInProgress) {
                drawingInProgress = true;
                drawingPoints = [p];
                drawingPreview = {type:activeDrawingTool, a:p, b:p};
                redrawDrawings();
            } else {
                const a = drawingPoints[0];
                chartDrawings.push({type:activeDrawingTool, a:a, b:p});
                selectedDrawingIndex = chartDrawings.length - 1;
                drawingEditState = null;
                clearDrawingPreview();
                syncActiveDrawingsToSharedStore();
                redrawDrawings();
                setDrawingTool(null);
            }
            return;
        }

        drawingCanvas.setPointerCapture?.(event.pointerId);
        drawingInProgress = true;
        drawingPoints = [p];
        if (activeDrawingTool === "draw") drawingPreview = {type:"free", points:[p]};
    });

    drawingCanvas.addEventListener("pointermove", event => {
        if (!activeDrawingTool || !drawingInProgress) return;
        const p = getPoint(event);
        if (!p) return;
        event.preventDefault();

        if (twoClickTool()) {
            drawingPreview = {type:activeDrawingTool, a:drawingPoints[0], b:p};
        } else if (activeDrawingTool === "draw") {
            drawingPreview = {type:"free", points:[...drawingPoints, p]};
            drawingPoints.push(p);
        }
        redrawDrawings();
    });

    const finishFreeDrawing = event => {
        if (activeDrawingTool !== "draw" || !drawingInProgress) return;
        const p = getPoint(event);
        if (!p) return;
        event.preventDefault();
        const pts = drawingPreview?.points || drawingPoints;
        if (pts.length > 1) {
            chartDrawings.push({type:"free", points:pts.slice()});
            selectedDrawingIndex = chartDrawings.length - 1;
            drawingEditState = null;
        }
        clearDrawingPreview();
        syncActiveDrawingsToSharedStore();
        redrawDrawings();
        setDrawingTool(null);
    };

    drawingCanvas.addEventListener("pointerup", finishFreeDrawing);
    drawingCanvas.addEventListener("pointercancel", finishFreeDrawing);
}

installDrawingHandlers();

chartContent?.addEventListener('pointerdown',event=>{ if(event.target.closest('.drawing-toolbar,.chart-grid-color-palette,.chart-grid-colorbar,.chart-grid-intervals,.chart-grid-expand,.chart-back,.chart-flag')) return; chartDrawingFocus=true; });

searchToggle.addEventListener("click", event => {
    event.preventDefault();
    event.stopPropagation();
    topSearch.classList.add("open");
    searchInput.focus();
    searchInput.select();
});

topSearch.addEventListener("click", (event) => {
    if (event.target === topSearch) {
        topSearch.classList.remove("open");
        searchInput.blur();
    }
});

function closeSearchModal() {
    topSearch.classList.remove("open");
    searchInput.value = "";
    suggestions.style.display = "none";
    renderTable();
}

topSearch.addEventListener("mousedown", (event) => {
    if (event.target === topSearch) {
        closeSearchModal();
    }
});

document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && topSearch.classList.contains("open")) {
        closeSearchModal();
    }
});
document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && activeDrawingTool) setDrawingTool(null);
});

const DRAWING_HOTKEY_STORAGE = "cryptoScreenerDrawingHotkeys";
const DEFAULT_DRAWING_HOTKEYS = {
    horizontal: "h",
    trend: "l",
    signal: "a",
    ruler: "r",
    ray: "y",
    rectangle: "b",
    draw: "d",
    longPosition: "p",
    shortPosition: "shift+p",
    back: "backspace",
    deleteSelected: "delete",
    clearAll: "ctrl+delete"
};
const DRAWING_HOTKEY_INFO = {
    horizontal: {name: "Горизонтальный уровень", description: "Горизонтальная линия на выбранном уровне цены"},
    trend: {name: "Наклонный уровень", description: "Наклонная линия между двумя точками графика"},
    signal: {name: "Сигнальный уровень", description: "Горизонтальный уровень для отслеживания сигнала"},
    ruler: {name: "Линейка", description: "Измерение расстояния и изменения цены между двумя точками"},
    ray: {name: "Луч", description: "Наклонная линия, продолжающаяся от первой точки"},
    rectangle: {name: "Прямоугольник", description: "Выделение ценовой области на графике"},
    draw: {name: "Свободное рисование", description: "Рисование произвольной линии мышью"},
    longPosition: {name: "Long Position", description: "Расчёт риска, Stop Loss и Take Profit для лонга"},
    shortPosition: {name: "Short Position", description: "Расчёт риска, Stop Loss и Take Profit для шорта"},
    back: {name: "Назад", description: "Вернуться к предыдущему состоянию сетки графиков"},
    deleteSelected: {name: "Удалить выбранный элемент", description: "Удаляет выделенный объект на графике"},
    clearAll: {name: "Очистить всё", description: "Удаляет все нарисованные элементы на графике"}
};
let drawingHotkeys = loadDrawingHotkeys();
let listeningHotkeyTool = null;
const hotkeysSection = document.getElementById("hotkeysSection");
const hotkeysSectionTitle = document.getElementById("hotkeysSectionTitle");
if (hotkeysSectionTitle) {
    hotkeysSectionTitle.addEventListener("click", () => hotkeysSection.classList.toggle("open"));
}

const notificationSoundsSection = document.getElementById("notificationSoundsSection");
const notificationSoundsSectionTitle = document.getElementById("notificationSoundsSectionTitle");
if (notificationSoundsSectionTitle) {
    notificationSoundsSectionTitle.addEventListener("click", () => notificationSoundsSection.classList.toggle("open"));
}

function loadDrawingHotkeys() {
    try {
        const saved = JSON.parse(localStorage.getItem(DRAWING_HOTKEY_STORAGE) || "{}");
        const merged = {...DEFAULT_DRAWING_HOTKEYS};
        Object.keys(merged).forEach(tool => {
            if (typeof saved[tool] === "string" && saved[tool].trim()) merged[tool] = saved[tool].toLowerCase();
        });
        return merged;
    } catch (e) {
        return {...DEFAULT_DRAWING_HOTKEYS};
    }
}

function saveDrawingHotkeys() {
    localStorage.setItem(DRAWING_HOTKEY_STORAGE, JSON.stringify(drawingHotkeys));
}

function formatHotkey(key) {
    return key ? key.toUpperCase() : "—";
}

function renderHotkeysSettings() {
    if (!hotkeysTable) return;
    hotkeysTable.innerHTML = Object.keys(DRAWING_HOTKEY_INFO).map(tool => {
        const info = DRAWING_HOTKEY_INFO[tool];
        return `<tr>
            <td><button class="hotkey-action" type="button" data-hotkey-tool="${tool}">${formatHotkey(drawingHotkeys[tool])}</button></td>
            <td>${info.name}</td>
            <td class="hotkey-description">${info.description}</td>
        </tr>`;
    }).join("");
}

function closeSettings() {
    listeningHotkeyTool = null;
    settingsOverlay.classList.remove("open");
    settingsOverlay.setAttribute("aria-hidden", "true");
}

initTerminalConnectionSettings();
initFormationTrendSettings();
function initChartGridPicker() { renderChartGridPicker(); }
chartGridSectionTitle?.addEventListener("click",()=>document.getElementById("chartGridSection")?.classList.toggle("open"));
initChartGridPicker();

function initTelegramNotificationSettings() {
    document.getElementById("notificationRoutingSectionTitle")?.addEventListener("click", () => document.getElementById("notificationRoutingSection")?.classList.toggle("open"));
    document.getElementById("telegramAddButton")?.addEventListener("click", async () => {
        const name=document.getElementById("telegramNewName")?.value.trim() || "Telegram";
        const token=document.getElementById("telegramNewToken")?.value.trim() || "";
        const chatId=document.getElementById("telegramNewChatId")?.value.trim() || "";
        const status=document.getElementById("telegramConnectionsStatus");
        if(!token || !chatId){ if(status) status.textContent="Укажите Bot Token и Chat ID"; return; }
        if(status) status.textContent="Сохранение...";
        try {
            const connection={id:makeTelegramConnectionId(),name,token,chatId};
            telegramConnections.push(connection); saveTelegramConnections();
            document.getElementById("telegramNewName").value=""; document.getElementById("telegramNewToken").value=""; document.getElementById("telegramNewChatId").value="";
            if(status) status.textContent="✓ Telegram подключён. Нажмите «Проверить», чтобы проверить связь.";
            renderTelegramConnections(); renderNotificationRouting(); renderAlertTelegramDestination();
        } catch(error) { if(status) status.textContent=`Ошибка: ${error.message}`; }
    });
    document.getElementById("telegramConnectionsList")?.addEventListener("click", async event => {
        const test=event.target.closest("[data-telegram-test]");
        if(test){ const c=telegramConnectionById(test.dataset.telegramTest); const status=document.querySelector(`[data-telegram-status="${CSS.escape(test.dataset.telegramTest)}"]`); if(!c) return; if(status) status.textContent="Проверка..."; try{const r=await fetch("/api/telegram/test",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({token:c.token,chat_id:c.chatId})}); const j=await r.json().catch(()=>({})); if(!r.ok||!j.ok) throw new Error(j.error||"Telegram error"); if(status) status.textContent="✓ Работает";}catch(e){if(status)status.textContent=`Ошибка: ${e.message}`;} return; }
        const del=event.target.closest("[data-telegram-delete]");
        if(del){ const id=del.dataset.telegramDelete; telegramConnections=telegramConnections.filter(x=>x.id!==id); saveTelegramConnections(); ["listing","signal"].forEach(k=>{if(notificationRouting[k].telegramDestinationId===id) notificationRouting[k].telegramDestinationId="";}); Object.values(notificationRouting.alerts).forEach(r=>{if(r.telegramDestinationId===id) r.telegramDestinationId="";}); saveNotificationRouting(); renderTelegramConnections(); renderNotificationRouting(); renderAlertTelegramDestination(); }
    });
    document.getElementById("notificationRoutingList")?.addEventListener("change", event => {
        const kind=event.target.dataset.routeKind;
        if(kind){ notificationRouting[kind]=normalizeNotificationRoute({...notificationRouting[kind], [event.target.dataset.routeField]: event.target.type === "checkbox" ? event.target.checked : event.target.value}); saveNotificationRouting(); return; }
        const id=event.target.dataset.alertRouteId;
        if(id){ const current=routeForAlert({id}); notificationRouting.alerts[id]=normalizeNotificationRoute({...current, [event.target.dataset.alertRouteField]: event.target.type === "checkbox" ? event.target.checked : event.target.value}); saveNotificationRouting(); }
    });
    document.getElementById("alertTelegramDestination")?.addEventListener("change", captureAlertTelegramUI);
}

initTelegramNotificationSettings();

settingsClose.addEventListener("click", closeSettings);
settingsOverlay.addEventListener("click", event => {
    if (event.target === settingsOverlay) closeSettings();
});

hotkeysTable.addEventListener("click", event => {
    const button = event.target.closest("[data-hotkey-tool]");
    if (!button) return;
    listeningHotkeyTool = button.dataset.hotkeyTool;
    button.classList.add("listening");
    button.textContent = "Нажмите…";
});

document.addEventListener("keydown", event => {
    if (!listeningHotkeyTool || !settingsOverlay.classList.contains("open")) return;
    event.preventDefault();
    event.stopPropagation();
    if (event.key === "Escape") {
        listeningHotkeyTool = null;
        renderHotkeysSettings();
        return;
    }
    const key = event.key.toLowerCase();
    if (key === "escape") return;
    const normalizedKey = (event.ctrlKey ? "ctrl+" : "") + (event.shiftKey ? "shift+" : "") + (event.altKey ? "alt+" : "") + key;
    const assignableNamedKeys = ["backspace","delete","tab","enter","home","end","pageup","pagedown","arrowup","arrowdown","arrowleft","arrowright"];
    if (key.length !== 1 && !event.ctrlKey && !assignableNamedKeys.includes(key)) return;
    const conflictTool = Object.keys(drawingHotkeys).find(tool => tool !== listeningHotkeyTool && drawingHotkeys[tool] === normalizedKey);
    if (conflictTool) {
        const conflictName = DRAWING_HOTKEY_INFO[conflictTool]?.name || conflictTool;
        const button = hotkeysTable.querySelector(`[data-hotkey-tool="${listeningHotkeyTool}"]`);
        if (button) {
            button.textContent = `Занято: ${conflictName}`;
            setTimeout(() => { if (listeningHotkeyTool) renderHotkeysSettings(); }, 900);
        }
        return;
    }
    drawingHotkeys[listeningHotkeyTool] = normalizedKey;
    saveDrawingHotkeys();
    listeningHotkeyTool = null;
    renderHotkeysSettings();
});

document.addEventListener("keydown", event => {
    if (settingsOverlay.classList.contains("open") || topSearch.classList.contains("open")) return;

    // Backspace is a direct duplicate of the visible «Назад» button.
    // It works even when the chart itself is not the current keyboard target.
    const target = event.target;
    const editingTarget = target && (target.matches("input, textarea, select") || target.isContentEditable);
    // Escape is a direct duplicate of the visible «Назад» action.
    // When a grid chart is maximized, first restore the grid; otherwise
    // restore the previous chart/grid state.
    if (event.key === "Escape" && !editingTarget && chartModal?.classList.contains("open")) {
        event.preventDefault();
        event.stopPropagation();
        goBackFromChart();
        return;
    }

    if (event.key === "Backspace" && !event.ctrlKey && !event.shiftKey && !event.altKey && !editingTarget && chartModal?.classList.contains("open")) {
        event.preventDefault();
        event.stopPropagation();
        goBackFromChart();
        return;
    }

    if (!ownPriceChart || !ownCandleSeries) return;
    if (target && (target.matches("input, textarea, select, button") || target.isContentEditable)) return;
    const physicalLetter = ({KeyA:"a",KeyB:"b",KeyD:"d",KeyH:"h",KeyL:"l",KeyP:"p",KeyR:"r",KeyY:"y"})[event.code] || "";
    const key = String(event.key || "").toLowerCase();
    const modifierPrefix = (event.ctrlKey ? "ctrl+" : "") + (event.shiftKey ? "shift+" : "") + (event.altKey ? "alt+" : "");
    const normalizedKey = modifierPrefix + key;
    const normalizedPhysicalKey = modifierPrefix + physicalLetter;
    const hotkeyMatches = configured => {
        const value = String(configured || "").toLowerCase();
        return value === normalizedKey || (physicalLetter && value === normalizedPhysicalKey);
    };

    if (hotkeyMatches(drawingHotkeys.back)) {
        event.preventDefault();
        goBackFromChart();
        return;
    }

    // Delete removes only the currently selected drawing. Ctrl+Delete clears all drawings.
    if (hotkeyMatches(drawingHotkeys.clearAll)) {
        event.preventDefault();
        chartDrawings = [];
        signalLevelStates.clear();
        signalLevelFired.clear();
        selectedDrawingIndex = -1;
        drawingEditState = null;
        clearDrawingPreview();
        syncActiveDrawingsToSharedStore();
        redrawDrawings();
        syncSignalLevelsToAlertServer();
        return;
    }
    if (hotkeyMatches(drawingHotkeys.deleteSelected)) {
        event.preventDefault();
        if (selectedDrawingIndex >= 0 && selectedDrawingIndex < chartDrawings.length) {
            const deletedDrawing = chartDrawings[selectedDrawingIndex];
            chartDrawings.splice(selectedDrawingIndex, 1);
            selectedDrawingIndex = -1;
            drawingEditState = null;
            syncActiveDrawingsToSharedStore();
            redrawDrawings();
            if (deletedDrawing?.type === "signal") syncSignalLevelsToAlertServer();
        }
        return;
    }

    const tool = Object.keys(drawingHotkeys).find(name =>
        !["deleteSelected", "clearAll"].includes(name) && hotkeyMatches(drawingHotkeys[name])
    );
    if (!tool) return;
    event.preventDefault();
    setDrawingTool(tool);
});

document.querySelectorAll(".drawing-tool").forEach(tool => {
    tool.addEventListener("click", event => {
        event.preventDefault();
        event.stopPropagation();
        setDrawingTool(tool.dataset.tool);
    });
});

function ownChartIntervalToBinance(interval) {
    const map = {"1": "1m", "5": "5m", "15": "15m", "60": "1h", "240": "4h", "1D": "1d"};
    return map[interval] || "15m";
}

function ownOiPeriodForInterval(interval) {
    const map = {"1": "5m", "5": "5m", "15": "15m", "60": "1h", "240": "4h", "1D": "1d"};
    return map[interval] || "5m";
}

function closeOwnChart() {
    if (ownWs) {
        try { ownWs.close(); } catch (e) {}
        ownWs = null;
    }
    if (ownOiTimer) {
        clearInterval(ownOiTimer);
        ownOiTimer = null;
    }
    if (ownChartKlineTimer) {
        clearInterval(ownChartKlineTimer);
        ownChartKlineTimer = null;
    }
    if (ownResizeObserver) {
        try { ownResizeObserver.disconnect(); } catch (e) {}
        ownResizeObserver = null;
    }
    clearOwnHistoryChunkSeries();
    const miniPriceCharts = new Set([...chartGridWorkspace?.querySelectorAll('.chart-grid-slot') || []]
        .map(slot => slot._chartRefs?.price).filter(Boolean));
    const miniOiCharts = new Set([...chartGridWorkspace?.querySelectorAll('.chart-grid-slot') || []]
        .map(slot => slot._chartRefs?.oi).filter(Boolean));
    if (ownPriceChart && !miniPriceCharts.has(ownPriceChart)) {
        try { ownPriceChart.remove(); } catch (e) {}
    } else if (ownOiChart && ownOiChart !== ownPriceChart && !miniOiCharts.has(ownOiChart)) {
        try { ownOiChart.remove(); } catch (e) {}
    }
    ownPriceChart = null;
    ownOiChart = null;
    ownChartContext = "none";
    ownChartReuseSymbol = "";
    ownChartReuseKey = "";
    ownChartRenderedCacheKey = "";
    ownChartRenderedKlineSignature = null;
    ownChartRenderedOiSignature = null;
    ownCandleSeries = null;
    ownVolumeSeries = null;
    ownOiSeries = null;
    ownOiViewportExtensionSeries = null;
    ownOiPoints = [];
    chartHoverTooltip.style.display = "none";
    clearDrawingPreview();
    activeDrawingTool = null;
    selectedDrawingIndex = -1;
    drawingEditState = null;
    if (drawingCanvas) drawingCanvas.classList.remove("active");
    if (drawingStatus) drawingStatus.classList.remove("open");
    document.querySelectorAll(".drawing-tool").forEach(item => item.classList.remove("active"));
    priceChartEl.innerHTML = "";
    oiChartEl.innerHTML = "";
}

function ownChartTheme() {
    return {
        layout: { background: { type: "solid", color: "#0b0f14" }, textColor: "#aab4c0", attributionLogo: false },
        grid: { vertLines: { color: "#151b23" }, horzLines: { color: "#151b23" } },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
        rightPriceScale: { borderColor: "#28313d", scaleMargins: { top: 0.01, bottom: 0.015 }, entireTextOnly: true, ticksVisible: false, minimumWidth: 34 },
        timeScale: { borderColor: "#28313d", timeVisible: true, secondsVisible: false, rightOffset: 0, barSpacing: 5, minBarSpacing: 1, ticksVisible: false, minimumHeight: 16 },
        // Do not install a global priceFormatter here. Lightweight Charts
        // must use each series' own formatter; the OI series has a custom
        // K/M/B formatter and a global formatter would override/conflict with it.
    };
}

function ownOiTheme() {
    return {
        layout: { background: { type: "solid", color: "#0b0f14" }, textColor: "#aab4c0", attributionLogo: false },
        grid: { vertLines: { color: "#151b23" }, horzLines: { color: "#151b23" } },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
        rightPriceScale: { borderColor: "#28313d", scaleMargins: { top: 0.18, bottom: 0.18 }, entireTextOnly: true, ticksVisible: false, minimumWidth: 72 },
        timeScale: { visible: false, borderColor: "#28313d", timeVisible: false, secondsVisible: false, rightOffset: 0, barSpacing: 5, minBarSpacing: 1, ticksVisible: false }
    };
}

function resizeUnifiedChartPanes(chart, el, oiFraction = 0.08) {
    if (!chart || !el) return;
    const w = Math.max(1, el.clientWidth || 1);
    const h = Math.max(1, el.clientHeight || 1);
    try { chart.resize(w, h, false); } catch (_) { try { chart.applyOptions({width:w, height:h}); } catch (_) {} }
    try {
        const panes = chart.panes?.() || [];
        if (panes.length > 1) {
            const oiHeight = Math.max(32, Math.min(Math.round(h * oiFraction), Math.max(32, h - 60)));
            panes[1].setHeight(oiHeight);
        }
    } catch (_) {}
}

// Price and OI now live in panes of ONE Lightweight Charts instance.
// There is deliberately no Price/OI viewport synchronization layer anymore:
// both series use the chart's single timeScale, exactly like Price and Volume.
function extendOiViewport() {}
function syncOiViewportToPrice() {}
function installLinkedChartViewports(priceChart, oiChart) {
    // Kept as a compatibility no-op for existing call sites. The unified chart
    // already provides one shared timeScale for every pane.
    return;
}

function ownSyncCharts() {
    return;
}

function installOhlcHover(chart, candleSeries, host, tooltip = null) {
    if (!chart || !candleSeries || !host) return;
    if (chart.__ohlcHoverInstalled) return;
    chart.__ohlcHoverInstalled = true;

    const tip = tooltip || document.createElement("div");
    if (!tooltip) {
        tip.className = "chart-hover-tooltip";
        host.appendChild(tip);
    } else if (tip.parentElement !== host) {
        // Keep the large-chart OHLC legend inside the actual Price chart.
        host.appendChild(tip);
    }
    tip.style.position = "absolute";
    tip.style.left = "auto";
    tip.style.right = "82px";
    tip.style.top = "6px";
    tip.style.minWidth = "0";
    tip.style.width = "auto";
    tip.style.padding = "0";
    tip.style.border = "0";
    tip.style.borderRadius = "0";
    tip.style.background = "transparent";
    tip.style.boxShadow = "none";
    tip.style.color = "#ffffff";
    tip.style.fontSize = "11px";
    tip.style.fontWeight = "500";
    tip.style.lineHeight = "1.25";
    tip.style.zIndex = "30";
    tip.style.display = "none";
    tip.style.pointerEvents = "none";
    tip.__ohlcHoverActive = false;

    const hide = () => {
        tip.__ohlcHoverActive = false;
        tip.style.display = "none";
    };

    try {
        chart.subscribeCrosshairMove(param => {
            if (!param || !param.point || !param.time) {
                hide();
                return;
            }
            const data = param.seriesData?.get(candleSeries);
            if (!data || ![data.open,data.high,data.low,data.close].every(v => Number.isFinite(Number(v)))) {
                hide();
                return;
            }
            tip.textContent = `O: ${formatPrice(data.open)}   H: ${formatPrice(data.high)}   L: ${formatPrice(data.low)}   C: ${formatPrice(data.close)}`;
            tip.__ohlcHoverActive = true;
            tip.style.display = "block";
        });
    } catch (_) {}
}

let redrawDrawingsRaf = 0;
let chartLayoutSyncRaf = 0;
let chartLayoutSyncRunning = false;

function redrawDrawingsNow() {
    if (!ownPriceChart || !drawingCanvas) return;
    redrawDrawings();
}

function scheduleDrawingsRedraw() {
    scheduleOwnChartVisualSync({drawings:true});
}

let stableDrawingsRedrawRaf = 0;
function scheduleStableDrawingsRedraw() {
    scheduleOwnChartVisualSync({drawings:true});
}

function chartViewState(chart) {
    if (!chart) return {time:null, price:null};
    let time = null, price = null;
    try { time = chart.timeScale().getVisibleRange() || null; } catch (_) {}
    try { price = chart.priceScale('right').getVisibleRange() || null; } catch (_) {}
    return {time, price};
}

function restoreChartViewState(chart, state, preservePrice=true) {
    if (!chart || !state) return;
    if (state.time) {
        try { chart.timeScale().setVisibleRange(state.time); } catch (_) {}
    }
    // Restoring a saved range must not remove the configured future drawing
    // margin. Re-apply the time-scale option after the range restoration.
    ensureDrawingFutureSpace(chart);
    // Do not force the previous Y-range back immediately after resize.
    // Lightweight Charts owns the price-scale/autoscale calculation and a
    // stale range restored during an intermediate layout pass can collapse
    // the visible candles toward an edge. Manual drawings remain anchored by
    // their real time/price coordinates and are redrawn against the final map.
    if (preservePrice && state.price) {
        try { chart.priceScale('right').setVisibleRange(state.price); } catch (_) {}
    }
}

function redrawAfterChartLayout(chart, state) {
    if (!chart) return;
    restoreChartViewState(chart, state);
    if (chart === ownPriceChart) scheduleOwnChartVisualSync({drawings:true,viewport:true});
}

function resizeChartPreservingView(chart, el) {
    if (!chart || !el) return null;
    const state = chartViewState(chart);
    // Preserve horizontal viewport, but let Lightweight Charts recalculate
    // the Y scale for the new geometry. Restoring an old price range here is
    // what could make a chart collapse into a thin band during resize.
    state.price = null;
    const w = Math.max(1, el.clientWidth);
    const h = Math.max(1, el.clientHeight);
    try { chart.resize(w, h, false); } catch (_) {
        try { chart.applyOptions({width:w, height:h}); } catch (_) {}
    }
    return state;
}

function scheduleChartLayoutSync() {
    if (chartLayoutSyncRaf) return;
    chartLayoutSyncRaf = requestAnimationFrame(() => {
        chartLayoutSyncRaf = 0;
        if (chartLayoutSyncRunning) return;
        chartLayoutSyncRunning = true;

        // In grid mode ownPriceChart/ownOiChart point to the active mini-chart,
        // while priceChartEl/oiChartEl belong to the hidden single-chart panes.
        // Resizing the mini-chart instances against those hidden containers can
        // collapse them to a zero/stale size and produces the black-screen bug
        // when the chart/market divider is dragged. In grid mode, resize only
        // against the actual mini-chart containers.
        const gridActive = !!document.getElementById('chartContent')?.classList.contains('grid-active');
        if (gridActive) {
            chartLayoutSyncRunning = false;
            resizeMiniChartsToContainer();
            scheduleOwnChartVisualSync({resize:true,viewport:true,drawings:true});
            return;
        }

        const priceState = resizeChartPreservingView(ownPriceChart, priceChartEl);
        chartLayoutSyncRunning = false;

        // Large chart is now one multi-pane chart; Price and OI share one timeScale.
        restoreChartViewState(ownPriceChart, priceState);
        resizeUnifiedChartPanes(ownPriceChart, priceChartEl);
        scheduleOwnChartVisualSync({viewport:true,drawings:true});
    });
}

function resizeMiniChartsToContainer() {
    if (!chartGridWorkspace) return;
    const pending = [];
    const hasMaximized = chartGridWorkspace.classList.contains('has-maximized');
    chartGridWorkspace.querySelectorAll('.chart-grid-slot').forEach(slot => {
        // Hidden grid slots have zero layout dimensions while another slot is
        // maximized. Never resize their live charts to 1x1; keep their last
        // real geometry until the grid is restored.
        if (hasMaximized && !slot.classList.contains('maximized')) return;
        const refs = slot._chartRefs;
        if (!refs) return;
        const priceEl = slot.querySelector('.chart-grid-price');
        const oiEl = slot.querySelector('.chart-grid-oi');
        if (!priceEl || !oiEl) return;
        pending.push([refs.price, priceEl, chartViewState(refs.price)]);
    });
    pending.forEach(([chart, el, state]) => {
        if (!chart || !el) return;
        const w = Math.max(1, el.clientWidth), h = Math.max(1, el.clientHeight);
        // Keep the time viewport stable while allowing the chart to recalculate
        // its own price scale after the container geometry changes.
        state.price = null;
        resizeUnifiedChartPanes(chart, el, 0.08);
        // Restore is part of the common visual commit path. Do not create
        // nested RAF chains here: the main chart scheduler owns the next frame.
        restoreChartViewState(chart, state, false);
        if (chart === ownPriceChart) scheduleOwnChartVisualSync({drawings:true,viewport:true});
    });
}

function resizeOwnChartsToContainer(fromVisualSync = false) {
    const gridActive = !!document.getElementById('chartContent')?.classList.contains('grid-active');
    if (gridActive) {
        // Grid keeps its own resize path. Large-chart visual synchronization is
        // handled by the single scheduler below.
        if (!fromVisualSync) scheduleChartLayoutSync();
        else resizeMiniChartsToContainer();
        return;
    }
    if (!ownPriceChart || !priceChartEl) return;
    const priceState = resizeChartPreservingView(ownPriceChart, priceChartEl);
    restoreChartViewState(ownPriceChart, priceState);
    resizeUnifiedChartPanes(ownPriceChart, priceChartEl);
}

// app160: one visual scheduler owns the complete large-chart visual commit.
// Events only set dirty flags; exactly one RAF performs resize, viewport restore,
// drawings, overlays and density work. No child RAF is created from this path.
let ownChartVisualSyncScheduled = false;
let ownChartVisualSyncPending = false;
const ownChartVisualDirty = {
    resize: true,
    viewport: true,
    drawings: true,
    overlays: true,
    density: true
};
function scheduleOwnChartVisualSync(dirty = null) {
    ownChartVisualSyncPending = true;
    if (dirty) Object.keys(dirty).forEach(k => { if (k in ownChartVisualDirty) ownChartVisualDirty[k] = true; });
    if (ownChartVisualSyncScheduled) return;
    ownChartVisualSyncScheduled = true;
    requestAnimationFrame(() => {
        ownChartVisualSyncScheduled = false;
        if (!ownChartVisualSyncPending) return;
        ownChartVisualSyncPending = false;
        const needsResize = ownChartVisualDirty.resize;
        ownChartVisualDirty.resize = false;
        ownChartVisualDirty.viewport = false;
        ownChartVisualDirty.drawings = false;
        ownChartVisualDirty.overlays = false;
        ownChartVisualDirty.density = false;
        if (needsResize) resizeOwnChartsToContainer(true);
        updateHoverTooltip();
        redrawDrawingsNow();
        if (densityChartRenderEnabled()) requestDensityChartRender(densityPresentationData);
    });
}

// A live series update is allowed to follow the newest candle only while the
// user is actually at the live edge. If the user has manually scrolled away
// (including into the future/empty area), keep exactly that viewport.
function updateSeriesPreservingManualViewport(chart, series, data) {
    if (!chart || !series) return;
    let range = null, scroll = 0;
    try { range = chart.timeScale().getVisibleLogicalRange() || null; } catch (_) {}
    try { scroll = Number(chart.timeScale().scrollPosition()) || 0; } catch (_) {}
    series.update(data);
    if (range && Math.abs(scroll) > 0.5) {
        try { chart.timeScale().setVisibleLogicalRange(range); } catch (_) {}
    }
}

function updateChartSeriesBatchPreservingManualViewport(chart, updates) {
    if (!chart || !Array.isArray(updates) || !updates.length) return;
    let range = null, scroll = 0;
    try { range = chart.timeScale().getVisibleLogicalRange() || null; } catch (_) {}
    try { scroll = Number(chart.timeScale().scrollPosition()) || 0; } catch (_) {}

    for (const item of updates) {
        if (item && item.series && Array.isArray(item.updates)) {
            for (const update of item.updates) {
                item.series.update(update);
            }
        } else if (item && item.series && item.data) {
            item.series.update(item.data);
        }
    }

    if (range && Math.abs(scroll) > 0.5) {
        try { chart.timeScale().setVisibleLogicalRange(range); } catch (_) {}
    }
}

let ownInitialHistoryPendingToken = 0;
let ownInitialHistoryUserInteracted = false;

function getOwnChartViewport(cacheKey) {
    const state = ownChartViewportCache.get(cacheKey);
    if (!state) return null;
    ownChartViewportCache.delete(cacheKey);
    ownChartViewportCache.set(cacheKey, state);
    return state;
}

function saveOwnChartViewport(cacheKey) {
    if (!cacheKey || !ownPriceChart) return;
    try {
        const range = ownPriceChart.timeScale().getVisibleRange() || null;
        const logicalRange = ownPriceChart.timeScale().getVisibleLogicalRange() || null;
        if (!range && !logicalRange) return;
        ownChartViewportCache.delete(cacheKey);
        ownChartViewportCache.set(cacheKey, { range, logicalRange, savedAt: Date.now() });
        captureActiveChartState(ownChartReuseSymbol, selectedChartInterval);
        while (ownChartViewportCache.size > OWN_CHART_VIEWPORT_CACHE_MAX) {
            const oldestKey = ownChartViewportCache.keys().next().value;
            if (oldestKey === undefined) break;
            ownChartViewportCache.delete(oldestKey);
        }
    } catch (_) {}
}

function updateOwnChartHistoryIncremental(cacheKey, klineData, oiData) {
    if (!ownCandleSeries || !ownVolumeSeries || !klineData) return;
    const incomingCandles = (klineData.candles || []).map(c => ({
        time: Number(c.time), open: Number(c.open), high: Number(c.high), low: Number(c.low), close: Number(c.close), volume: Number(c.volume)
    })).filter(c => [c.time,c.open,c.high,c.low,c.close,c.volume].every(Number.isFinite));
    const previous = ownPriceHistoryCache.get(cacheKey);
    const previousCandles = Array.isArray(previous?.candles) ? previous.candles : [];
    const previousByTime = new Map(previousCandles.map(c => [Number(c.time), c]));
    const changed = [];
    incomingCandles.forEach(c => {
        const old = previousByTime.get(c.time);
        if (!old || Number(old.open)!==c.open || Number(old.high)!==c.high || Number(old.low)!==c.low || Number(old.close)!==c.close || Number(old.volume)!==c.volume) changed.push(c);
    });
    changed.forEach(c => {
        updateSeriesPreservingManualViewport(ownPriceChart, ownCandleSeries, {time:c.time,open:c.open,high:c.high,low:c.low,close:c.close});
        updateSeriesPreservingManualViewport(ownPriceChart, ownVolumeSeries, {time:c.time,value:c.volume,color:"#808890"});
    });
    if (incomingCandles.length) ensureDrawingFutureSpace(ownPriceChart, incomingCandles);

    const incomingOi = (oiData?.points || []).map(p => ({time:Number(p.time),value:Number(p.oiValue)}))
        .filter(p => Number.isFinite(p.time) && Number.isFinite(p.value));
    if (ownOiSeries && incomingOi.length) {
        const previousOi = Array.isArray(ownOiHistoryCache.get(cacheKey)?.points) ? ownOiHistoryCache.get(cacheKey).points : [];
        const previousOiByTime = new Map(previousOi.map(p => [Number(p.time), p]));
        incomingOi.forEach(p => {
            const old = previousOiByTime.get(p.time);
            if (!old || Number(old.oiValue)!==p.value) updateSeriesPreservingManualViewport(ownOiChart, ownOiSeries, p);
        });
    }

    ownPriceHistoryCache.set(cacheKey, klineData);
    if (oiData?.points) ownOiHistoryCache.set(cacheKey, oiData);
    ownChartHistoryCache.set(cacheKey, {klineData, oiData: oiData || ownOiHistoryCache.get(cacheKey) || {points:[]}});
    return changed.length;
}

async function loadOwnChart(symbol, interval) {
    if (ownChartReuseKey) saveOwnChartViewport(ownChartReuseKey);
    // Cancel only history requests owned by the previous large-chart load.
    // Shared Grid/prefetch requests are intentionally left alone.
    ownActiveHistoryControllers.forEach(controller => { try { controller.abort(); } catch (_) {} });
    ownActiveHistoryControllers.clear();
    const loadToken = ++ownChartLoadToken;
    ownInitialHistoryPendingToken = loadToken;
    ownInitialHistoryUserInteracted = false;
    const requestedSymbol = String(symbol || "").toUpperCase();
    const requestedInterval = String(interval || "1");
    const symbolChangedForLoad = !!ownChartReuseSymbol && ownChartReuseSymbol !== requestedSymbol;
    const historyAnchor = (pendingOwnChartHistoryAnchor && pendingOwnChartHistoryAnchor.symbol === requestedSymbol && pendingOwnChartHistoryAnchor.interval === requestedInterval) ? pendingOwnChartHistoryAnchor : null;
    if (historyAnchor) pendingOwnChartHistoryAnchor = null;
    rememberChartTimeframe(requestedSymbol, requestedInterval);
    const binanceInterval = ownChartIntervalToBinance(requestedInterval);
    const requestedOiPeriod = ownOiPeriodForInterval(requestedInterval);
    const cacheKey = requestedSymbol + "|" + binanceInterval + "|" + requestedOiPeriod;
    if (chartLoadingIndicator) {
        chartLoadingIndicator.textContent = "График загружается…";
        chartLoadingIndicator.classList.add("open");
    }
    priceChartEl?.classList.add("chart-is-loading");

    let previousOwnChartRange = null;
    let previousOwnChartLogicalRange = null;
    const savedFullState = restoreActiveChartState(requestedSymbol, requestedInterval);
    const savedViewportFromState = savedFullState || null;
    const savedViewport = getOwnChartViewport(cacheKey);
    if (savedViewportFromState) {
        previousOwnChartRange = savedViewportFromState.range || null;
        previousOwnChartLogicalRange = savedViewportFromState.logicalRange || null;
    } else if (savedViewport) {
        previousOwnChartRange = savedViewport.range || null;
        previousOwnChartLogicalRange = savedViewport.logicalRange || null;
    }
    const hadExistingOwnChart = !!(ownPriceChart && ownOiChart && ownCandleSeries && ownVolumeSeries && ownOiSeries);
    const sameChartContext = ownChartReuseKey === cacheKey;
    const sameSymbolContext = ownChartReuseSymbol === requestedSymbol;
    if (hadExistingOwnChart && sameSymbolContext) {
        try { previousOwnChartRange = ownPriceChart.timeScale().getVisibleRange() || null; } catch (_) {}
        try { previousOwnChartLogicalRange = ownPriceChart.timeScale().getVisibleLogicalRange() || null; } catch (_) {}
    }

    // A newly opened large chart always starts from the full first 250-candle
    // history. A range captured from a mini chart must not collapse that first
    // view to the mini chart's few visible candles. Once the large chart already
    // exists, its own viewport is preserved separately below.
    let openingRange = null;
    if (pendingOwnChartOpenRange &&
        pendingOwnChartOpenRange.symbol === requestedSymbol &&
        String(pendingOwnChartOpenRange.interval) === requestedInterval) {
        openingRange = hadExistingOwnChart ? (pendingOwnChartOpenRange.range || null) : null;
        pendingOwnChartOpenRange = null;
    }

    if (typeof LightweightCharts === "undefined") {
        priceChartEl.innerHTML = '<div style="padding:20px;color:#f85149">Lightweight Charts не загрузился.</div>';
        ownInitialHistoryPendingToken = 0;
        return;
    }

    // The initial 250-candle request is asynchronous. If the user starts
    // panning/zooming before that request finishes, the late history response
    // must not call fitContent() and jump the chart back to the latest candles.
    // Mark only real pointer/wheel/touch interaction; programmatic viewport
    // changes do not fire these DOM events.
    if (!priceChartEl.__ownInitialViewportInteractionBound) {
        const markOwnInitialViewportInteraction = () => {
            if (ownInitialHistoryPendingToken === ownChartLoadToken) {
                ownInitialHistoryUserInteracted = true;
            }
        };
        priceChartEl.addEventListener("pointerdown", markOwnInitialViewportInteraction, {passive:true});
        priceChartEl.addEventListener("wheel", markOwnInitialViewportInteraction, {passive:true});
        priceChartEl.addEventListener("touchstart", markOwnInitialViewportInteraction, {passive:true});
        priceChartEl.__ownInitialViewportInteractionBound = true;
    }

    // Full reuse: keep the Lightweight Charts instances and series alive.
    // Only realtime subscriptions and series data are replaced on TF/symbol change.
    if (ownWs) { try { ownWs.close(); } catch (_) {} ownWs = null; }
    if (ownOiTimer) { clearInterval(ownOiTimer); ownOiTimer = null; }
    if (ownChartKlineTimer) { clearInterval(ownChartKlineTimer); ownChartKlineTimer = null; }

    ownOiPeriod = requestedOiPeriod;
    clearOwnHistoryChunkSeries();

    if (!ownPriceChart || !ownOiChart || !ownCandleSeries || !ownVolumeSeries || !ownOiSeries) {
        priceChartEl.innerHTML = "<div class=\"chart-indicator-label volume-label\">Volume</div><div id=\"chartLoadingIndicator\" class=\"chart-loading-indicator\">Данные загружаются…</div>";
        oiChartEl.innerHTML = "";
        ownPriceChart = LightweightCharts.createChart(priceChartEl, {
            ...ownChartTheme(),
            width: priceChartEl.clientWidth || 900,
            height: priceChartEl.clientHeight || 650,
            layout: { ...ownChartTheme().layout, panes: { separatorColor: "#28313d", separatorHoverColor: "#28313d", enableResize: false } }
        });
        // IMPORTANT: Price and OI are panes of the SAME chart. Therefore they
        // physically share one timeScale; moving Price moves OI exactly like
        // Volume, and moving OI moves the same shared viewport.
        ownOiChart = ownPriceChart;
        ownCandleSeries = ownPriceChart.addSeries(LightweightCharts.CandlestickSeries, {
            upColor: "#26a69a", downColor: "#ef5350", borderVisible: false,
            wickUpColor: "#26a69a", wickDownColor: "#ef5350",
            priceFormat: { type: "custom", formatter: formatPrice, minMove: 0.0000001 }
        }, 0);
        ownDrawingFutureSeries = ownPriceChart.addSeries(LightweightCharts.LineSeries, {
            priceScaleId: "", lineVisible: false, priceLineVisible: false,
            lastValueVisible: false, crosshairMarkerVisible: false, visible: false
        }, 0);
        ownVolumeSeries = ownPriceChart.addSeries(LightweightCharts.HistogramSeries, {
            priceFormat: { type: "volume" }, priceScaleId: "volume", color: "#808890"
        }, 0);
        ownPriceChart.priceScale("volume").applyOptions({ scaleMargins: { top: 0.86, bottom: 0 } });
        ownOiSeries = ownPriceChart.addSeries(LightweightCharts.LineSeries, {
            color: "#8ab4f8", lineWidth: 2, priceLineVisible: false, lastValueVisible: false, title: "",
            priceFormat: { type: "custom", formatter: formatCompactOI, minMove: 0.01 }
        }, 1);
        ownOiSeries.applyOptions({
            priceLineVisible: false,
            lastValueVisible: false,
            title: "",
            priceFormat: { type: "custom", formatter: formatCompactOI, minMove: 0.01 }
        });
        ownOiViewportExtensionSeries = null;
        resizeUnifiedChartPanes(ownPriceChart, priceChartEl);

        installOhlcHover(ownPriceChart, ownCandleSeries, priceChartEl, chartHoverTooltip);
        try { ownPriceChart.timeScale().subscribeVisibleLogicalRangeChange(() => { redrawDrawings(); scheduleOlderOwnHistoryCheck(); }); } catch (_) {}
        ownSyncCharts();
    }
    ownChartContext = "large";
    ownChartReuseSymbol = requestedSymbol;
    ownChartReuseKey = cacheKey;
    scheduleOwnChartVisualSync({resize:true,viewport:true});

    const renderHistory = (klineData, oiData, fitContent, restoreRange = null, restoreLogicalRange = null) => {
        if (loadToken !== ownChartLoadToken || !ownCandleSeries || !ownVolumeSeries) return false;

        const candles = (klineData.candles || []).map(c => ({
            time: Number(c.time), open: Number(c.open), high: Number(c.high),
            low: Number(c.low), close: Number(c.close)
        }));
        const volumes = (klineData.candles || []).map(c => ({
            time: Number(c.time), value: Number(c.volume), color: volumeExpansionBarColor(c.time)
        }));
        const nextOiPoints = (oiData?.points || [])
            .map(p => ({ time: Number(p.time), value: Number(p.oiValue) }))
            .filter(p => Number.isFinite(p.time) && Number.isFinite(p.value))
            .sort((a,b) => a.time - b.time);
        if (nextOiPoints.length || !ownOiPoints.length) ownOiPoints = nextOiPoints;

        ownLastPrice = candles.length ? Number(candles[candles.length - 1].close) : ownLastPrice;
        ensureDrawingFutureSpace(ownPriceChart, candles);

        // Reuse the existing series data when the fetched history is effectively
        // unchanged. If only the newest candle changed/arrived, update just that
        // point instead of rebuilding all 250 points with setData().
        const lastCandle = candles.length ? candles[candles.length - 1] : null;
        const lastVolume = volumes.length ? volumes[volumes.length - 1] : null;
        const lastOiPoint = ownOiPoints.length ? ownOiPoints[ownOiPoints.length - 1] : null;
        const klineSignature = candles.length ? {
            length: candles.length, firstTime: candles[0].time, lastTime: lastCandle.time,
            open: lastCandle.open, high: lastCandle.high, low: lastCandle.low, close: lastCandle.close,
            volume: lastVolume?.value ?? null
        } : {length: 0, firstTime: null, lastTime: null, open: null, high: null, low: null, close: null, volume: null};
        const oiSignature = ownOiPoints.length ? {
            length: ownOiPoints.length, firstTime: ownOiPoints[0].time, lastTime: lastOiPoint.time,
            value: lastOiPoint.value
        } : {length: 0, firstTime: null, lastTime: null, value: null};

        // app160: chart-tail state is explicit. A same-open-time update is a
        // tail mutation; a different lastTime means a new candle and therefore
        // invalidates analysis for the visible timeframe.
        const previousLastCandleTime = ownChartRenderedKlineSignature?.lastTime ?? null;
        const isNewCandle = previousLastCandleTime != null && klineSignature.lastTime !== previousLastCandleTime;
        const isTailUpdate = previousLastCandleTime != null && klineSignature.lastTime === previousLastCandleTime;
        if (isNewCandle && ownChartIntervalToBinance(selectedChartInterval || requestedInterval) === binanceInterval) {
            chartAnalysisCandleCache.delete(`${requestedSymbol}|${binanceInterval}`);
        } else if (isTailUpdate) {
            const analysisKey = `${requestedSymbol}|${binanceInterval}`;
            const analysisState = chartAnalysisStateCache.get(analysisKey);
            if (analysisState) {
                analysisState.last_candle_close = klineSignature.close;
                analysisState.candle_count = candles.length;
                analysisState.updatedAt = Date.now();
            }
        }

        const sameKlineHistory = ownChartRenderedCacheKey === cacheKey &&
            ownChartRenderedKlineSignature &&
            ownChartRenderedKlineSignature.length === klineSignature.length &&
            ownChartRenderedKlineSignature.firstTime === klineSignature.firstTime &&
            ownChartRenderedKlineSignature.lastTime === klineSignature.lastTime &&
            ownChartRenderedKlineSignature.open === klineSignature.open &&
            ownChartRenderedKlineSignature.high === klineSignature.high &&
            ownChartRenderedKlineSignature.low === klineSignature.low &&
            ownChartRenderedKlineSignature.close === klineSignature.close &&
            ownChartRenderedKlineSignature.volume === klineSignature.volume;
        const sameOiHistory = ownChartRenderedCacheKey === cacheKey &&
            ownChartRenderedOiSignature &&
            ownChartRenderedOiSignature.length === oiSignature.length &&
            ownChartRenderedOiSignature.firstTime === oiSignature.firstTime &&
            ownChartRenderedOiSignature.lastTime === oiSignature.lastTime &&
            ownChartRenderedOiSignature.value === oiSignature.value;

        // If the exact cache snapshot is already rendered for this key and no
        // viewport restoration is requested, there is nothing to redraw. Live
        // WebSocket updates will continue to update the current candle.
        const exactSnapshotAlreadyRendered = sameKlineHistory && sameOiHistory &&
            !fitContent && !restoreRange && !restoreLogicalRange;
        if (exactSnapshotAlreadyRendered) {
            return true;
        }

        if (!sameKlineHistory) {
            ownCandleSeries.setData(candles);
            ownVolumeSeries.setData(volumes);
        } else if (candles.length) {
            const lastCandle = candles[candles.length - 1];
            const lastVolume = volumes[volumes.length - 1];
            updateChartSeriesBatchPreservingManualViewport(ownPriceChart, [
                { series: ownCandleSeries, updates: [lastCandle] },
                { series: ownVolumeSeries, updates: [lastVolume] }
            ]);
        }

        if (ownOiSeries) {
            if (!sameOiHistory) {
                ownOiSeries.setData(ownOiPoints);
            } else if (ownOiPoints.length) {
                updateSeriesPreservingManualViewport(ownOiChart, ownOiSeries, ownOiPoints[ownOiPoints.length - 1]);
            }
        }

        // A new symbol must never inherit the previous instrument's vertical
        // price scale (e.g. ETH ~$2500 -> a coin priced near $1). Reset only
        // the Y scale here; the horizontal/time viewport is restored below as
        // before, so existing navigation behavior is preserved.
        if (symbolChangedForLoad) {
            try { ownPriceChart.priceScale("right").applyOptions({autoScale: true}); } catch (_) {}
            requestAnimationFrame(() => {
                if (loadToken !== ownChartLoadToken || !ownPriceChart) return;
                try { ownPriceChart.priceScale("right").applyOptions({autoScale: true}); } catch (_) {}
            });
        }
        syncOiViewportToPrice(ownPriceChart, ownOiChart, ownOiViewportExtensionSeries);

        ownChartRenderedCacheKey = cacheKey;
        ownChartRenderedKlineSignature = klineSignature;
        ownChartRenderedOiSignature = oiSignature;
        const renderedAnalysisKey = `${requestedSymbol}|${binanceInterval}`;
        const renderedAnalysis = chartAnalysisCache.get(renderedAnalysisKey) || chartIndicatorLayerCache.get(renderedAnalysisKey) || null;
        if (renderedAnalysis) {
            const existingAnalysisState = chartAnalysisStateCache.get(renderedAnalysisKey) || {};
            chartAnalysisStateCache.delete(renderedAnalysisKey);
            chartAnalysisStateCache.set(renderedAnalysisKey, {
                ...existingAnalysisState,
                symbol: requestedSymbol,
                timeframe: binanceInterval,
                last_candle_open: Number(renderedAnalysis.candle_open_time || klineSignature.lastTime) || null,
                last_candle_close: klineSignature.close,
                candle_count: candles.length,
                indicator_state: renderedAnalysis.indicator_state || renderedAnalysis.indicators || existingAnalysisState.indicator_state || null,
                analysis: renderedAnalysis,
                updatedAt: Date.now()
            });
        }

        if (fitContent) {
            // One chart / one timeScale: fit the Price pane once. OI follows
            // automatically because it is on pane 1 of the same chart.
            restoreChartViewport(ownPriceChart, null, null, true);
        } else if (restoreLogicalRange && !ownPriceChart._userMovedViewport) {
            try { ownPriceChart.timeScale().setVisibleLogicalRange(restoreLogicalRange); } catch (_) {}
            syncOiViewportToPrice(ownPriceChart, ownOiChart, ownOiViewportExtensionSeries);
        } else if (restoreRange && !ownPriceChart._userMovedViewport) {
            restoreChartViewport(ownPriceChart, restoreRange, null, false);
            syncOiViewportToPrice(ownPriceChart, ownOiChart, ownOiViewportExtensionSeries, restoreRange);
        }

        // History rendering/restoration can replace time-scale options after the
        // chart was created. Re-apply the configured future drawing margin.
        ensureDrawingFutureSpace(ownPriceChart);

        return true;
    };

    // OI arrives independently. Do not touch the Price series when only OI
    // changes; this keeps the fast Price-first path truly independent.
    const renderOiHistory = (oiData, restoreRange = null, restoreLogicalRange = null) => {
        if (loadToken !== ownChartLoadToken || !ownOiChart || !ownOiSeries) return false;
        const nextOiPoints = (oiData?.points || [])
            .map(p => ({ time: Number(p.time), value: Number(p.oiValue) }))
            .filter(p => Number.isFinite(p.time) && Number.isFinite(p.value))
            .sort((a,b) => a.time - b.time);
        ownOiPoints = nextOiPoints;
        const oiSignature = ownOiPoints.length ? {
            length: ownOiPoints.length, firstTime: ownOiPoints[0].time, lastTime: ownOiPoints[ownOiPoints.length - 1].time
        } : {length: 0, firstTime: null, lastTime: null};
        const sameOiHistory = ownChartRenderedCacheKey === cacheKey &&
            ownChartRenderedOiSignature &&
            ownChartRenderedOiSignature.length === oiSignature.length &&
            ownChartRenderedOiSignature.firstTime === oiSignature.firstTime &&
            ownChartRenderedOiSignature.lastTime === oiSignature.lastTime;
        if (!sameOiHistory) {
            ownOiSeries.setData(ownOiPoints);
        } else if (ownOiPoints.length) {
            updateSeriesPreservingManualViewport(ownOiChart, ownOiSeries, ownOiPoints[ownOiPoints.length - 1]);
        }
        ownChartRenderedOiSignature = oiSignature;
        syncOiViewportToPrice(ownPriceChart, ownOiChart, ownOiViewportExtensionSeries, restoreRange || null);
        return true;
    };

    // Render the last successful history immediately when the user returns to a TF.
    // Promote a bounded prefetch entry into the normal large-chart cache on demand.
    if (!ownPriceHistoryCache.has(cacheKey)) {
        const prefetchedPrice = chartPricePrefetchCache.get(cacheKey);
        if (prefetchedPrice) {
            ownPriceHistoryCache.set(cacheKey, prefetchedPrice);
            chartPricePrefetchCache.delete(cacheKey);
        }
    }
    if (!ownOiHistoryCache.has(cacheKey)) {
        const prefetchedOi = chartOiPrefetchCache.get(cacheKey);
        if (prefetchedOi) {
            ownOiHistoryCache.set(cacheKey, prefetchedOi);
            chartOiPrefetchCache.delete(cacheKey);
        }
    }
    const hotState = getChartHotState(requestedSymbol, requestedInterval);
    const cachedPrice = ownPriceHistoryCache.get(cacheKey) || hotState?.price || ownChartHistoryCache.get(cacheKey)?.klineData || null;
    const cachedOi = ownOiHistoryCache.get(cacheKey) || hotState?.oi || ownChartHistoryCache.get(cacheKey)?.oiData || null;
    const cached = cachedPrice || cachedOi ? { klineData: cachedPrice, oiData: cachedOi } : null;
    if (!cachedOi) ownOiPoints = [];
    if (cachedPrice) {
        // Price is independent: render it immediately without waiting for OI.
        const cachedRestoreRange = openingRange || (!previousOwnChartLogicalRange ? previousOwnChartRange : null);
        const cachedRestoreLogicalRange = !openingRange ? previousOwnChartLogicalRange : null;
        renderHistory(
            cachedPrice,
            cachedOi || {points:[]},
            !cachedRestoreRange && !cachedRestoreLogicalRange,
            cachedRestoreRange,
            cachedRestoreLogicalRange
        );
    } else if (cachedOi && ownOiSeries) {
        // OI may already be cached even when Price is not; keep it available for
        // the independent OI request below, but do not block the Price chart.
        renderOiHistory(cachedOi);
    }

    if (loadToken !== ownChartLoadToken || !ownCandleSeries) return;


    function checkSignalLevelCrossings(previousPrice, currentPrice) {
        checkSignalLevelCrossingsForSymbol(selectedChartSymbol, signalDrawingsForSymbol(selectedChartSymbol), previousPrice, currentPrice);
    }

    const applyLiveKline = k => {
        if (loadToken !== ownChartLoadToken || !k || !ownCandleSeries || !ownVolumeSeries) return;
        const time = Math.floor(Number(k.t) / 1000);
        const open = Number(k.o);
        const high = Number(k.h);
        const low = Number(k.l);
        const close = Number(k.c);
        const volume = Number(k.v);
        if (![time, open, high, low, close, volume].every(Number.isFinite)) return;

        const previousLivePrice = ownLastPrice;
        ownLastPrice = close;
        updateSeriesPreservingManualViewport(ownPriceChart, ownCandleSeries, { time, open, high, low, close });
        ensureDrawingFutureSpace(ownPriceChart, [{time}]);
        checkSignalLevelCrossings(previousLivePrice, close);
        updateSeriesPreservingManualViewport(ownPriceChart, ownVolumeSeries, { time, value: volume, color: volumeExpansionBarColor(time) });

        // Keep the cached last candle fresh for instant switching back.
        const cachedState = ownChartHistoryCache.get(cacheKey);
        if (cachedState && Array.isArray(cachedState.klineData.candles) && cachedState.klineData.candles.length) {
            const last = cachedState.klineData.candles[cachedState.klineData.candles.length - 1];
            if (Number(last.time) === time) {
                last.open = open;
                last.high = high;
                last.low = low;
                last.close = close;
                last.volume = volume;
            }
        }
    };

    // Binance sends the still-forming kline through this stream several times per second.
    // The WebSocket is the primary realtime source; REST is only a lightweight fallback.
    const wsUrl = "wss://fstream.binance.com/market/ws/" + symbol.toLowerCase() + "@kline_" + binanceInterval;
    try {
        const ws = new WebSocket(wsUrl);
        ownWs = ws;

        ws.onopen = () => {
            if (loadToken === ownChartLoadToken) {
                console.log("Own chart WebSocket LIVE:", symbol, binanceInterval);
            }
        };

        ws.onmessage = event => {
            if (loadToken !== ownChartLoadToken) return;
            try {
                const msg = JSON.parse(event.data);
                const k = msg && msg.k;
                if (!k) return;
                applyLiveKline({
                    t: Number(k.t),
                    o: k.o,
                    h: k.h,
                    l: k.l,
                    c: k.c,
                    v: k.v
                });
            } catch (e) {
                console.warn("Own chart WebSocket message error:", e);
            }
        };

        ws.onerror = e => {
            if (loadToken === ownChartLoadToken) console.warn("Own chart WebSocket error", e);
        };

        ws.onclose = () => {
            if (loadToken === ownChartLoadToken) {
                console.warn("Own chart WebSocket CLOSED:", symbol, binanceInterval);
            }
        };
    } catch (e) {
        console.warn("Own chart WebSocket create error:", e);
    }

    const refreshCurrentKline = async () => {
        if (loadToken !== ownChartLoadToken) return;
        try {
            const requestKey = `live|klines|${symbol}|${binanceInterval}|1`;
            const data = await fetchChartJsonShared(`/api/klines?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(binanceInterval)}&limit=1`, requestKey, CHART_LIVE_CACHE_TTL_MS);
            if (loadToken !== ownChartLoadToken) return;
            if (!data?.candles || !data.candles.length) return;
            const c = data.candles[data.candles.length - 1];
            applyLiveKline({
                t: Number(c.time) * 1000,
                o: c.open,
                h: c.high,
                l: c.low,
                c: c.close,
                v: c.volume
            });
        } catch (e) {
            if (loadToken === ownChartLoadToken) console.warn("Current kline refresh error:", e);
        }
    };

    // Do not wait for the fallback request before starting realtime updates.
    refreshCurrentKline();
    prefetchChartHistory(requestedSymbol, requestedInterval);
    const ownKlineTimer = setInterval(refreshCurrentKline, 3000);
    ownKlineTimer.__chartTimer = true;
    ownChartKlineTimer = ownKlineTimer;

    // Historical OI is USD value. For live updates use current open interest
    // in contracts multiplied by the latest live chart price.
    const updateLiveOiValue = async () => {
        if (loadToken !== ownChartLoadToken) return;
        try {
            const requestKey = `live|current-oi|${symbol}`;
            const data = await fetchChartJsonShared(`/api/current_open_interest?symbol=${encodeURIComponent(symbol)}`, requestKey, CHART_LIVE_CACHE_TTL_MS);
            if (loadToken !== ownChartLoadToken) return;
            if (!data?.time || !ownLastPrice) return;
            const periodSeconds = ({"5m":300,"15m":900,"30m":1800,"1h":3600,"4h":14400,"1d":86400})[ownOiPeriod] || 300;
            const bucket = Math.floor(Number(data.time) / periodSeconds) * periodSeconds;
            const liveOiPoint = { time: bucket, value: Number(data.oi) * ownLastPrice };
            const oiByTime = new Map((ownOiPoints || []).map(point => [Number(point.time), point]));
            oiByTime.set(bucket, liveOiPoint);
            ownOiPoints = [...oiByTime.values()].sort((a,b) => a.time - b.time);
            updateSeriesPreservingManualViewport(ownOiChart, ownOiSeries, liveOiPoint);
        } catch (e) {}
    };

    updateLiveOiValue();
    ownOiTimer = setInterval(updateLiveOiValue, 3000);


    try {
        // Candles and OI are independent data sources: an OI error must never
        // prevent the price chart from loading (this was blocking ETH and other
        // symbols whenever the OI endpoint/rate-limit failed).
        const klineRequestKey = `history|klines|${symbol}|${binanceInterval}|250`;
        const oiRequestKey = `history|oi|${symbol}|${ownOiPeriod}|250`;
        const activeKlineController = new AbortController();
        const activeOiController = new AbortController();
        const renderFreshPrice = async () => {
            try {
                const historyEndTime = historyAnchor ? Math.floor((Number(historyAnchor.end) + (({ "1":"60", "5":"300", "15":"900", "30":"1800", "60":"3600", "240":"14400", "1D":"86400" }[requestedInterval] || 300) * 60)) * 1000) : null;
                const historyUrl = `/api/klines?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(binanceInterval)}&limit=250${historyEndTime ? `&endTime=${historyEndTime}` : ""}`;
                const klineData = await fetchChartJsonFresh(historyUrl, klineRequestKey + (historyEndTime ? `|${historyEndTime}` : ""), activeKlineController);
                if (loadToken !== ownChartLoadToken || !klineData) return;
                const currentOi = ownOiHistoryCache.get(cacheKey) || cachedOi || {points:[]};
                if (cachedPrice) {
                    updateOwnChartHistoryIncremental(cacheKey, klineData, currentOi);
                } else {
                    ownPriceHistoryCache.set(cacheKey, klineData);
                    ownChartHistoryCache.set(cacheKey, { klineData, oiData: currentOi });
                }

                let lateCurrentRange = null;
                if (ownInitialHistoryUserInteracted) {
                    try { lateCurrentRange = ownPriceChart.timeScale().getVisibleRange() || null; } catch (_) {}
                }
                const historyRestoreRange = historyAnchor ? { from: Math.max(0, Number(historyAnchor.start) - 12 * ({ "1":60, "5":300, "15":900, "30":1800, "60":3600, "240":14400, "1D":86400 }[requestedInterval] || 300)), to: Number(historyAnchor.end) + 12 * ({ "1":60, "5":300, "15":900, "30":1800, "60":3600, "240":14400, "1D":86400 }[requestedInterval] || 300) } : null;
                const finalRestoreRange = ownInitialHistoryUserInteracted ? lateCurrentRange : (historyRestoreRange || openingRange || previousOwnChartRange);
                const finalFitContent = !ownInitialHistoryUserInteracted && !finalRestoreRange;
                if (!cachedPrice) {
                    renderHistory(klineData, currentOi, finalFitContent, finalRestoreRange);
                } else if (finalRestoreRange) {
                    try { ownPriceChart.timeScale().setVisibleRange(finalRestoreRange); } catch (_) {}
                     syncOiViewportToPrice(ownPriceChart, ownOiChart, ownOiViewportExtensionSeries, finalRestoreRange);
                }

                const baseCandles = Array.isArray(klineData.candles) ? klineData.candles : [];
                const baseOiPoints = Array.isArray(currentOi?.points) ? currentOi.points : [];
                let initialLogicalRange = null;
                try { initialLogicalRange = ownPriceChart.timeScale().getVisibleLogicalRange() || null; } catch (_) {}
                ownHistoryLoadState = {
                    token: loadToken, symbol: requestedSymbol, interval: requestedInterval,
                    binanceInterval, oiPeriod: ownOiPeriod, cacheKey, loading: false, exhausted: false, armed: false,
                    oldestKlineTime: baseCandles.length ? Math.min(...baseCandles.map(c => Number(c.time)).filter(Number.isFinite)) : null,
                    oldestOiTime: baseOiPoints.length ? Math.min(...baseOiPoints.map(p => Number(p.time)).filter(Number.isFinite)) : null,
                    klineTimes: new Set(baseCandles.map(c => Number(c.time)).filter(Number.isFinite)),
                    oiTimes: new Set(baseOiPoints.map(p => Number(p.time)).filter(Number.isFinite)),
                    lastVisibleFrom: initialLogicalRange ? Number(initialLogicalRange.from) : null
                };
                ownInitialHistoryPendingToken = 0;
                if (chartLoadingIndicator) chartLoadingIndicator.classList.remove("open");
                if (priceChartEl) priceChartEl.classList.remove("chart-is-loading");
            } catch (e) {
                if (loadToken === ownChartLoadToken) {
                    console.warn("Price history request error:", e);
                    if (chartLoadingIndicator) chartLoadingIndicator.classList.remove("open");
                    if (priceChartEl) priceChartEl.classList.remove("chart-is-loading");
                }
            }
        };

        const renderFreshOi = async () => {
            try {
                const oiData = await fetchChartJsonFresh(`/api/open_interest?symbol=${encodeURIComponent(symbol)}&period=${encodeURIComponent(ownOiPeriod)}&limit=250`, oiRequestKey, activeOiController);
                if (loadToken !== ownChartLoadToken || !oiData || !Array.isArray(oiData.points)) return;
                const cachedOiBeforeRefresh = ownOiHistoryCache.get(cacheKey) || cachedOi;
                ownOiHistoryCache.set(cacheKey, oiData);
                ownChartHistoryCache.set(cacheKey, {
                    klineData: ownPriceHistoryCache.get(cacheKey) || cachedPrice || {candles:[]},
                    oiData
                });
                if (cachedOiBeforeRefresh) {
                    updateOwnChartHistoryIncremental(cacheKey, ownPriceHistoryCache.get(cacheKey) || cachedPrice || {candles:[]}, oiData);
                } else {
                    renderOiHistory(oiData);
                }
            } catch (e) {
                if (loadToken === ownChartLoadToken) console.warn("Open interest history request error:", e);
            }
        };

        // Cache-first is intentionally visible before background network work
        // starts. Yield one browser turn so the restored timeframe can paint
        // immediately. The fresh-fetch helper below skips redundant 250-point
        // REST requests when the same history was fetched moments ago.
        setTimeout(() => {
            if (loadToken !== ownChartLoadToken) return;
            renderFreshPrice();
            renderFreshOi();
        }, 0);

        // Older-history loading is armed from Price as soon as Price is ready;
        // OI is tracked independently and may arrive later.
        const initialPrice = ownPriceHistoryCache.get(cacheKey) || cachedPrice;
        const initialOi = ownOiHistoryCache.get(cacheKey) || cachedOi;
        if (initialPrice) {
            const baseCandles = Array.isArray(initialPrice.candles) ? initialPrice.candles : [];
            const baseOiPoints = Array.isArray(initialOi?.points) ? initialOi.points : [];
            let initialLogicalRange = null;
            try { initialLogicalRange = ownPriceChart.timeScale().getVisibleLogicalRange() || null; } catch (_) {}
            ownHistoryLoadState = {
                token: loadToken, symbol: requestedSymbol, interval: requestedInterval,
                binanceInterval, oiPeriod: ownOiPeriod, cacheKey, loading: false, exhausted: false, armed: false,
                oldestKlineTime: baseCandles.length ? Math.min(...baseCandles.map(c => Number(c.time)).filter(Number.isFinite)) : null,
                oldestOiTime: baseOiPoints.length ? Math.min(...baseOiPoints.map(p => Number(p.time)).filter(Number.isFinite)) : null,
                klineTimes: new Set(baseCandles.map(c => Number(c.time)).filter(Number.isFinite)),
                oiTimes: new Set(baseOiPoints.map(p => Number(p.time)).filter(Number.isFinite)),
                lastVisibleFrom: initialLogicalRange ? Number(initialLogicalRange.from) : null
            };
        }
        ownInitialHistoryUserInteracted = false;

        const overlayTf = chartOverlayTemplateTimeframe();
        if (isChartOverlayAnalysisEnabled()) {
            ensureChartOverlayAnalysis(symbol, overlayTf).then(() => {
                if (!isChartOverlayAnalysisEnabled()) return;
                if (selectedChartSymbol === symbol) scheduleOwnChartVisualSync();
            });
        }
        updateHoverTooltip();
        captureActiveChartState(requestedSymbol, requestedInterval);
        scheduleOwnChartVisualSync();

        try {
            const range = ownPriceChart.timeScale().getVisibleRange();
            if (range && ownOiChart) syncOiViewportToPrice(ownPriceChart, ownOiChart, ownOiViewportExtensionSeries, range);
        } catch (_) {}

    } catch (e) {
        if (loadToken !== ownChartLoadToken) return;
        console.warn("Own chart history error:", e);
        if (!cached) {
            if (chartLoadingIndicator) chartLoadingIndicator.classList.remove("open");
                if (priceChartEl) priceChartEl.classList.remove("chart-is-loading");
            priceChartEl.innerHTML = '<div style="padding:20px;color:#f85149">Ошибка загрузки графика: ' + String(e).replace(/</g, "&lt;") + '</div>';
            return;
        }
        if (chartLoadingIndicator) chartLoadingIndicator.classList.remove("open");
                if (priceChartEl) priceChartEl.classList.remove("chart-is-loading");
    }

    // Main chart resizing is driven explicitly by the workspace/window layout
    // changes. Do not observe the chart panes themselves: Lightweight Charts
    // can internally resize its canvas during a resize, which feeds the
    // ResizeObserver back into the layout and causes visible oscillation.
    if (!window.__cryptoScreenerMainResizeBound) {
        window.__cryptoScreenerMainResizeBound = true;
        window.addEventListener("resize", () => scheduleOwnChartVisualSync());
        priceChartEl?.addEventListener("wheel", () => {
            if (densityChartRenderEnabled()) setTimeout(() => requestDensityChartRender(densityPresentationData), 0);
        }, {passive:true});
    }
    scheduleOwnChartVisualSync({resize:true,viewport:true,drawings:true,overlays:true});
    observeActiveDrawingGeometry();
}

function loadChart(symbol, interval) {
    const safeSymbol = String(symbol || "").toUpperCase().trim();
    if (!safeSymbol) return;

    if (selectedChartSymbol) {
        captureActiveChartState(selectedChartSymbol, selectedChartInterval);
    }

    if (selectedChartSymbol && selectedChartInterval) syncActiveDrawingsToSharedStore();
    if (selectedChartSymbol && selectedChartSymbol !== safeSymbol) {
        chartDrawings = [];
        selectedDrawingIndex = -1;
        drawingEditState = null;
    }
    const nextInterval = interval || selectedChartInterval || "1";
    const maximizedSlot = chartGridMaximized >= 0 ? chartGridWorkspace?.querySelectorAll('.chart-grid-slot')[chartGridMaximized] : null;
    if (maximizedSlot && maximizedSlot.dataset.chartSymbol === safeSymbol) {
        const current = maximizedSlot._chartRefs?.interval || maximizedSlot.dataset.chartInterval || "1";
        if (current !== nextInterval && selectedChartSymbol === safeSymbol) {
            saveSharedChartDrawings(safeSymbol, current, chartDrawings);
        }
        selectedChartSymbol = safeSymbol;
        syncSelectedMarketRow(safeSymbol);
        selectedChartInterval = nextInterval;
        chartDrawings = cloneDrawings(getSharedChartDrawings(safeSymbol, nextInterval));
        selectedDrawingIndex = -1;
        drawingEditState = null;
        redrawDrawings();
        maximizedSlot.querySelectorAll('.chart-grid-intervals button').forEach(button => button.classList.toggle('active', button.dataset.interval === nextInterval));
        if (current !== nextInterval) {
            // Keep the existing mini-chart instances alive. app62 was fast
            // because timeframe switching did not rebuild the whole chart
            // stack; app85 already has the safer reuse mechanism, so use it
            // here too instead of destroying the maximized chart.
            loadMiniChart(maximizedSlot, safeSymbol, nextInterval);
        }
        chartIntervals.querySelectorAll('.chart-interval').forEach(button => button.classList.toggle('active', button.dataset.interval === nextInterval));
        return;
    }
    const hasOwnChart = !!(ownPriceChart && ownOiChart && ownCandleSeries && ownVolumeSeries && ownOiSeries);
    const expectedChartCacheKey = `${safeSymbol}|${ownChartIntervalToBinance(nextInterval)}|${ownOiPeriodForInterval(nextInterval)}`;
    // A chart is considered reusable only when the requested symbol/timeframe
    // is actually rendered in the current Lightweight Charts instance.  This
    // prevents a blank/stale chart from being treated as already loaded.
    const sameChart = ownChartContext === "large" && hasOwnChart &&
        selectedChartSymbol === safeSymbol && selectedChartInterval === nextInterval &&
        ownChartRenderedCacheKey === expectedChartCacheKey;
    const symbolChanged = selectedChartSymbol !== safeSymbol;
    if (selectedChartSymbol === safeSymbol && selectedChartInterval !== nextInterval) { signalLevelStates.clear(); signalLevelFired.clear(); }
    selectedChartSymbol = safeSymbol;
    syncSelectedMarketRow(safeSymbol);
    if (symbolChanged && hasOwnChart) {
        // Do not leave the previous instrument's price data/viewport visible
        // while the new instrument is loading.  Price and volume are cleared
        // together, then the normal cache-first/REST path repopulates them.
        try { ownCandleSeries.setData([]); } catch (_) {}
        try { ownVolumeSeries.setData([]); } catch (_) {}
        try { ownOiSeries?.setData([]); } catch (_) {}
        ownChartRenderedCacheKey = "";
        ownChartRenderedKlineSignature = null;
        ownChartRenderedOiSignature = null;
        ownChartReuseKey = "";
    }
    selectedChartInterval = nextInterval;
    chartDrawings = cloneDrawings(getSharedChartDrawings(safeSymbol, nextInterval));
    selectedDrawingIndex = -1;
    drawingEditState = null;
    restoreActiveChartState(safeSymbol, nextInterval);
    chartTitle.textContent = chartDisplaySymbol(safeSymbol) + " · B-F";
    activeDrawingPriceElement = priceChartEl; chartDrawingFocus = true;
    ensureSingleChartColorBar(safeSymbol);
    syncChartFlagForSymbol(safeSymbol);
    if (chartTitleDot) chartTitleDot.style.background = chartGridColorFor(safeSymbol);
    if (!sameChart) loadOwnChart(safeSymbol, selectedChartInterval);
    scheduleOwnChartVisualSync();

    chartIntervals.querySelectorAll(".chart-interval").forEach(button => {
        button.classList.toggle("active", button.dataset.interval === selectedChartInterval);
    });
}

let chartReturnState = null;

function captureChartReturnState() {
    const slots=[...chartGridWorkspace?.querySelectorAll('.chart-grid-slot') || []];
    if(slots.length){
        const state={
            rows: chartGridRows, cols: chartGridCols, page: chartGridPage,
            slots: slots.map(slot=>({
                symbol: slot.dataset.chartSymbol || '',
                interval: slot.querySelector('.chart-grid-intervals button.active')?.dataset.interval || '1'
            })),
            activeSymbol: selectedChartSymbol || slots.find(s=>s.classList.contains('active-slot'))?.dataset.chartSymbol || slots[0]?.dataset.chartSymbol || ''
        };
        saveChartGridReturnState(state);
        return state;
    }
    const savedReturn=loadChartGridReturnState();
    if(savedReturn && savedReturn.slots?.length){
        return {
            rows:Math.max(1,Math.min(7,Number(savedReturn.rows)||1)),
            cols:Math.max(1,Math.min(7,Number(savedReturn.cols)||1)),
            slots:savedReturn.slots,
            activeSymbol:savedReturn.activeSymbol || savedReturn.slots[0]?.symbol || ''
        };
    }
    if(selectedChartSymbol){
        return {rows:1, cols:1, slots:[{symbol:selectedChartSymbol, interval:selectedChartInterval || '1'}], activeSymbol:selectedChartSymbol};
    }
    return null;
}

function restoreChartReturnState() {
    const state = chartReturnState || loadChartGridReturnState();
    if (!state) return false;
    closeOwnChart();

    chartGridRows = Math.max(1, Math.min(7, Number(state.rows) || 1));
    chartGridCols = Math.max(1, Math.min(7, Number(state.cols) || 1));
    chartGridPage = Math.max(0, Number(state.page) || 0);
    saveChartGrid();

    if (chartGridRows === 1 && chartGridCols === 1) {
        setChartLargeMode(false);
        chartGridWorkspace.querySelectorAll('.chart-grid-slot').forEach(slot => destroyMiniSlotChart(slot));
        chartGridWorkspace.innerHTML = '';
        chartGridWorkspace.style.display = 'none';
        document.getElementById('chartContent')?.classList.remove('grid-active');
        selectedChartSymbol = state.activeSymbol || 'BTCUSDT';
        syncSelectedMarketRow(selectedChartSymbol);
        selectedChartInterval = state.slots?.find(x => x.symbol === selectedChartSymbol)?.interval || '1';
        loadChart(selectedChartSymbol, selectedChartInterval);
        chartModal.classList.add('open');
        chartReturnState = null;
        return true;
    }

    const content = document.getElementById('chartContent') || document.querySelector('.chart-content');
    content?.classList.add('grid-active');
    chartGridWorkspace.style.display = 'grid';
    chartGridWorkspace.style.gridTemplateColumns = `repeat(${chartGridCols},minmax(0,1fr))`;
    chartGridWorkspace.style.gridTemplateRows = `repeat(${chartGridRows},minmax(0,1fr))`;
    chartGridWorkspace.classList.remove('has-maximized');
    chartGridMaximized = -1;
    setChartLargeMode(false);

    // Usually the original grid is still alive. Remove only the temporary
    // maximized slot, so all original charts keep their data/WebSockets.
    chartGridWorkspace.querySelectorAll('.chart-grid-slot[data-temporary-chart="1"]').forEach(slot => {
        destroyMiniSlotChart(slot);
        slot.remove();
    });

    let slots = [...chartGridWorkspace.querySelectorAll('.chart-grid-slot')];
    const matchesState = slots.length === state.slots.length && state.slots.every((item, i) => slots[i]?.dataset.chartSymbol === String(item.symbol || '').toUpperCase());
    if (!matchesState) {
        destroyMiniCharts();
        chartGridWorkspace.innerHTML = '';
        slots = [];
        (state.slots || []).slice(0, chartGridRows * chartGridCols).forEach((item, i) => {
            const slot = buildChartGridSlot(i, item.symbol || 'BTCUSDT');
            slot.querySelectorAll('.chart-grid-intervals button').forEach(b => b.classList.toggle('active', b.dataset.interval === (item.interval || '1')));
            if ((item.interval || '1') !== '1') {
                destroyMiniSlotChart(slot);
                slot.querySelector('.chart-grid-price').innerHTML = '';
                slot.querySelector('.chart-grid-oi').innerHTML = '';
                loadMiniChart(slot, item.symbol || 'BTCUSDT', item.interval || '1');
            }
        });
        slots = [...chartGridWorkspace.querySelectorAll('.chart-grid-slot')];
    }

    const active = slots.find(slot => slot.dataset.chartSymbol === String(state.activeSymbol || '').toUpperCase()) || slots[0];
    if (active) activateGridSlot(active);
    updateChartGridPagination();
    scheduleOwnChartVisualSync();
    chartModal.classList.add('open');
    chartReturnState = null;
    return true;
}

function goBackFromChart(){
    // «Назад» from a maximized grid chart means returning to the grid.
    // Do this before restoring the saved chart state so the button/Escape
    // cannot accidentally reopen the same full-size chart.
    if (chartGridMaximized >= 0) {
        const slots = [...chartGridWorkspace.querySelectorAll('.chart-grid-slot')];
        const slot = slots[chartGridMaximized];
        if (slot) {
            chartGridMaximized = -1;
            slot.classList.remove('maximized');
            chartGridWorkspace.classList.remove('has-maximized');
            setChartLargeMode(false);
            updateChartGridExpandButtons();
            scheduleOwnChartVisualSync();
            return;
        }
    }
    if(!restoreChartReturnState()) return;
}

function parkGridChartInstances() {
    // Keep every Grid mini-chart alive so its loaded history, current state,
    // and WebSocket remain warm while the user views a large chart. The large
    // chart must not reuse those instances because they belong to the Grid
    // containers. Clearing only the global own* aliases cleanly separates the
    // two chart modes without throwing away the warm Grid state.
    const slots = [...(chartGridWorkspace?.querySelectorAll('.chart-grid-slot') || [])];
    if (!slots.length) return;
    const activeSlot = slots.find(slot => slot.classList.contains('active-slot'));
    if (activeSlot?._chartRefs) {
        const refs = activeSlot._chartRefs;
        try {
            if (refs.price) {
                const range = refs.price.timeScale().getVisibleRange() || null;
                if (range) {
                    pendingOwnChartOpenRange = {
                        symbol: String(refs.symbol || activeSlot.dataset.chartSymbol || '').toUpperCase(),
                        interval: String(refs.interval || activeSlot.dataset.chartInterval || '1'),
                        range
                    };
                }
            }
        } catch (_) {}
    }
    ownPriceChart = null;
    ownOiChart = null;
    ownCandleSeries = null;
    ownVolumeSeries = null;
    ownOiSeries = null;
    ownDrawingFutureSeries = null;
    ownChartContext = "none";
    activeDrawingPriceElement = priceChartEl;
}

function openChart(symbol, intervalOverride = null) {
    const safeSymbol = String(symbol || "").toUpperCase().trim();
    if (!safeSymbol || !safeSymbol.endsWith("USDT")) return;
    selectedMarketSymbol = safeSymbol;
    syncSelectedMarketRow(safeSymbol);

    // Preserve the Grid itself instead of destroying its mini-chart instances.
    // Their data and live state stay warm, so opening a large chart for a coin
    // that was already present in Grid can render from cache immediately.
    // Global own* aliases are parked so loadOwnChart always creates the large
    // chart in its own full-size containers rather than reusing a mini-chart.
    const gridSlots = [...(chartGridWorkspace?.querySelectorAll('.chart-grid-slot') || [])];
    const previousState = captureChartReturnState();
    if (previousState && previousState.slots?.length) chartReturnState = previousState;
    if (gridSlots.length) {
        parkGridChartInstances();
        chartGridWorkspace.classList.remove('has-maximized');
        chartGridWorkspace.style.display = 'none';
        document.getElementById('chartContent')?.classList.remove('grid-active');
        chartGridMaximized = -1;
        document.getElementById('chartModal')?.classList.remove('grid-maximized');
    }

    chartModal.classList.add("open");
    document.getElementById('chartContent')?.classList.remove('grid-active');
    if (chartGridWorkspace) {
        chartGridWorkspace.classList.remove('has-maximized');
        chartGridWorkspace.style.display = 'none';
        chartGridMaximized = -1;
        document.getElementById('chartModal')?.classList.remove('grid-maximized');
    }

    activeDrawingPriceElement = priceChartEl;
    chartDrawingFocus = true;
    chartTitle.textContent = chartDisplaySymbol(safeSymbol) + " · B-F";
    chartTitleDot?.style.setProperty('background', chartGridColorFor(safeSymbol));
    ensureSingleChartColorBar(safeSymbol);
    syncChartFlagForSymbol(safeSymbol);
    setChartLargeMode(true);

    const initialChartInterval = intervalOverride || "1";
    chartIntervals.querySelectorAll(".chart-interval").forEach(button => {
        button.classList.toggle("active", button.dataset.interval === initialChartInterval);
    });

    loadChart(safeSymbol, initialChartInterval);
    scheduleOwnChartVisualSync();

    topSearch.classList.remove("open");
    searchInput.blur();
    searchInput.value = "";
    suggestions.style.display = "none";
}

function closeChart() {
    // The chart is now a persistent part of the main workspace.
    closeOwnChart();
}


chartIntervals.addEventListener("click", event => {
    const button = event.target.closest(".chart-interval");
    if (!button || !selectedChartSymbol) return;

    const nextInterval = button.dataset.interval;
    const slots = [...(chartGridWorkspace?.querySelectorAll('.chart-grid-slot') || [])];
    const isGrid = slots.length > 1 && chartGridWorkspace?.classList.contains('has-maximized') ||
        slots.length > 1 && chartGridWorkspace?.style.display !== 'none';

    if (isGrid) {
        // The large toolbar is the master timeframe control for the whole grid.
        // Keep every mini-chart on the same selected timeframe.
        selectedChartInterval = nextInterval;
        slots.forEach(slot => {
            const symbol = String(slot.dataset.chartSymbol || '').toUpperCase();
            if (!symbol) return;
            slot.querySelectorAll('.chart-grid-intervals button').forEach(b =>
                b.classList.toggle('active', b.dataset.interval === nextInterval)
            );
            loadMiniChart(slot, symbol, nextInterval);
        });
        chartIntervals.querySelectorAll('.chart-interval').forEach(b =>
            b.classList.toggle('active', b.dataset.interval === nextInterval)
        );
        scheduleOwnChartVisualSync({resize:true,viewport:true});
        return;
    }

    // In the single large chart, only the active chart changes timeframe.
    // loadChart owns the selected interval state.
    loadChart(selectedChartSymbol, nextInterval);
});

syncChartOverlayToggleButtons();

document.getElementById("chartFormationToggle")?.addEventListener("click", e => {
    chartShowFormations = !chartShowFormations;
    saveChartOverlayVisibility();
    e.currentTarget.classList.toggle("active", chartShowFormations);
    if (isChartOverlayAnalysisEnabled() && selectedChartSymbol) {
        const overlayTf = chartOverlayTemplateTimeframe();
        ensureChartOverlayAnalysis(selectedChartSymbol, overlayTf).then(() => {
            if (!isChartOverlayAnalysisEnabled()) return;
            if (selectedChartSymbol) scheduleOwnChartVisualSync({drawings:true});
        });
    } else {
        if (!isChartOverlayAnalysisEnabled()) cancelChartOverlayAnalysis();
        redrawDrawings();
    }
});
document.getElementById("chartLevelToggle")?.addEventListener("click", e => {
    chartShowLevels = !chartShowLevels;
    saveChartOverlayVisibility();
    e.currentTarget.classList.toggle("active", chartShowLevels);
    if (isChartOverlayAnalysisEnabled() && selectedChartSymbol) {
        const overlayTf = chartOverlayTemplateTimeframe();
        ensureChartOverlayAnalysis(selectedChartSymbol, overlayTf).then(() => {
            if (!isChartOverlayAnalysisEnabled()) return;
            if (selectedChartSymbol) scheduleOwnChartVisualSync({drawings:true});
        });
    } else {
        if (!isChartOverlayAnalysisEnabled()) cancelChartOverlayAnalysis();
        redrawDrawings();
    }
});
document.getElementById("chartDensityToggle")?.addEventListener("click", e => {
    densityChartVisible = !densityChartVisible;
    saveDensityChartVisibility();
    e.currentTarget.classList.toggle("active", densityChartVisible);
    requestDensityChartRender(densityPresentationData);
});

function renderSuggestions() {
    const query = searchInput.value.trim().toUpperCase();
    if (!query) {
        suggestions.style.display = "none";
        suggestions.innerHTML = "";
        return;
    }

    const liveBySymbol = new Map(latestCoins.map(c => [c.symbol, c]));
    const matches = instrumentCatalog
        .filter(c => String(c.symbol || "").toUpperCase().endsWith("USDT"))
        .filter(c => c.symbol.includes(query))
        .slice(0, 25);

    if (!matches.length) {
        const liveMatches = latestCoins
            .filter(c => String(c.symbol || "").toUpperCase().endsWith("USDT"))
            .filter(c => c.symbol.includes(query))
            .slice(0, 25);
        if (liveMatches.length) {
            suggestions.innerHTML = liveMatches.map(c => `
                <div class="suggestion" data-symbol="${c.symbol}">
                    <strong>${chartDisplaySymbol(c.symbol)}</strong>
                    <span class="suggestion-price">${fmtPrice(c.price)}</span>
                </div>
            `).join("");
        } else {
            suggestions.innerHTML = `<div class="suggestion muted">Инструмент не найден</div>`;
        }
        suggestions.style.display = "block";
        suggestions.querySelectorAll(".suggestion[data-symbol]").forEach(el => {
            el.addEventListener("mousedown", event => {
                event.preventDefault();
                openChart(el.dataset.symbol);
            });
        });
        return;
    }

    suggestions.innerHTML = matches.map(item => {
        const live = liveBySymbol.get(item.symbol);
        return `
            <div class="suggestion" data-symbol="${item.symbol}">
                <strong>${item.symbol}</strong>
                <span class="suggestion-price">${live ? fmtPrice(live.price) : "—"}</span>
            </div>
        `;
    }).join("");
    suggestions.style.display = "block";

    suggestions.querySelectorAll(".suggestion[data-symbol]").forEach(el => {
        el.addEventListener("mousedown", event => {
            event.preventDefault();
            openChart(el.dataset.symbol);
        });
    });
}

function fmtPrice(value) {
    if (!Number.isFinite(value)) return "—";

    if (value >= 1000) return value.toLocaleString("en-US", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });

    if (value >= 1) return value.toLocaleString("en-US", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 4
    });

    return value.toLocaleString("en-US", {
        minimumFractionDigits: 4,
        maximumFractionDigits: 8
    });
}

function fmtNumber(value) {
    return formatCompactMetric(value);
}
function fmtTime(value) {
    if (!value) return "—";

    try {
        return new Date(value).toLocaleTimeString();
    } catch {
        return value;
    }
}

const MARKET_SETTINGS_STORAGE = "cryptoScreenerMarketSettings";
const DEFAULT_MARKET_SETTINGS = {
    columns: ["symbol", "trades", "change_pct", "volume_24h", "natr", "btc_corr", "volume_spike"],
    fields: {},
    ignoreSign: false,
    linkActive: false,
    freeze: false,
    freezeAll: false
};

let marketSettings = loadMarketSettings();
let marketSettingsApplied = true;
let marketFrozenCoins = null;

function ensureExtendedMarketTimeframes() {
    const options = ["1m", "5m", "15m", "30m", "1h", "4h", "12h", "24h", "25h", "1D"];
    document.querySelectorAll('select[data-market-field="timeframe"]').forEach(select => {
        const current = select.value;
        options.forEach(tf => {
            if (![...select.options].some(o => o.value === tf || o.textContent === tf)) select.add(new Option(tf, tf));
        });
        if (current) select.value = current;
    });
}

function loadMarketSettings() {
    try {
        const saved = JSON.parse(localStorage.getItem(MARKET_SETTINGS_STORAGE) || "null");
        if (!saved || typeof saved !== "object") return structuredClone(DEFAULT_MARKET_SETTINGS);
        const merged = structuredClone(DEFAULT_MARKET_SETTINGS);
        if (Array.isArray(saved.columns) && saved.columns.length) merged.columns = saved.columns.filter(Boolean);
        if (saved.fields && typeof saved.fields === "object") merged.fields = saved.fields;
        ["ignoreSign", "linkActive", "freeze", "freezeAll"].forEach(k => { if (typeof saved[k] === "boolean") merged[k] = saved[k]; });
        return merged;
    } catch {
        return structuredClone(DEFAULT_MARKET_SETTINGS);
    }
}

function saveMarketSettings() {
    localStorage.setItem(MARKET_SETTINGS_STORAGE, JSON.stringify(marketSettings));
}

function getMarketColumns() {
    const columns = Array.isArray(marketSettings.columns) && marketSettings.columns.length
        ? marketSettings.columns.filter(Boolean)
        : [...DEFAULT_MARKET_SETTINGS.columns];

    // Тикер всегда должен быть первым столбцом.
    const symbolIndex = columns.indexOf("symbol");
    if (symbolIndex > 0) {
        columns.splice(symbolIndex, 1);
        columns.unshift("symbol");
    } else if (symbolIndex === -1) {
        columns.unshift("symbol");
    }

    return columns;
}

function marketValue(c, key) {
    if (key === "symbol") return c.symbol;
    if (key === "trades") return Number.isFinite(Number(c.trades)) ? formatCompactMetric(c.trades) : "—";
    if (key === "change_pct") return `${c.change_pct >= 0 ? "+" : ""}${(c.change_pct || 0).toFixed(2)}%`;
    if (key === "volume_24h") return fmtNumber(c.volume_24h);
    if (key === "natr") return Number.isFinite(Number(c.natr)) ? `${Number(c.natr).toFixed(2)}%` : "—";
    if (key === "btc_corr") return Number.isFinite(Number(c.btc_corr)) ? Number(c.btc_corr).toFixed(2) : "—";
    if (key === "volume_spike") return Number.isFinite(Number(c.volume_spike)) ? formatCompactMetric(c.volume_spike) : "—";
    if (key === "price") return fmtPrice(c.price);
    if (key === "exchange") return marketDisplayExchange(c.exchange);
    if (key === "spread_pct") {
        const mid = (Number(c.bid) + Number(c.ask)) / 2;
        return Number.isFinite(mid) && mid > 0 ? (((Number(c.ask) - Number(c.bid)) / mid) * 100).toFixed(3) + "%" : "—";
    }
    return c[key] ?? "—";
}

function marketFilterValue(c, key) {
    if (key === "spread_pct") {
        const bid = Number(c.bid);
        const ask = Number(c.ask);
        const mid = (bid + ask) / 2;
        return Number.isFinite(mid) && mid > 0 ? (ask - bid) / mid * 100 : NaN;
    }
    const value = Number(c[key]);
    return Number.isFinite(value) ? value : NaN;
}

function parseMarketLimit(value) {
    if (value === null || value === undefined || String(value).trim() === "") return null;
    const normalized = String(value).trim().replace(/\s/g, "").replace(/,/g, ".");
    const number = Number(normalized);
    return Number.isFinite(number) ? number : null;
}

// Timeframe-aware Market Filter cache. It is populated only for configured
// filters, so an unused TimeFrame does not add REST traffic.
const MARKET_TIMEFRAME_KEYS = new Set([
    "trades", "change_pct", "volume_24h", "natr", "btc_corr", "volume_spike",
    "spread_pct", "funding_pct", "oi_change_usd", "oi_change_pct",
    "delta_volume_usd", "price"
]);
let marketTimeframeRefreshAt = 0;
let marketTimeframeRefreshPromise = null;

function marketFilterConfiguredJobs() {
    const jobs = new Map();
    Object.entries(marketSettings.fields || {}).forEach(([key, settings]) => {
        if (!MARKET_TIMEFRAME_KEYS.has(key) || !settings) return;
        const min = parseMarketLimit(settings.min), max = parseMarketLimit(settings.max);
        if (min === null && max === null) return;
        const timeframe = String(settings.timeframe || "1m");
        if (!ALERT_TIMEFRAMES.includes(timeframe)) return;
        jobs.set(timeframe, true);
    });
    return [...jobs.keys()];
}

function marketTimeframeMetricValue(c, key, settings) {
    const timeframe = String(settings?.timeframe || "1m");
    const cached = c?.__marketTimeframeMetrics?.[timeframe];
    if (cached && Object.prototype.hasOwnProperty.call(cached, key)) {
        const value = Number(cached[key]);
        if (Number.isFinite(value)) return value;
    }
    // For intraday filters, never fall back to the 24h Market State value.
    // A fallback here would make the visible TimeFrame selection misleading.
    if (timeframe !== "1D") return NaN;
    return marketFilterValue(c, key);
}

async function refreshMarketTimeframeMetrics(coins, force = false) {
    if (!Array.isArray(coins) || !coins.length) return;
    const timeframes = marketFilterConfiguredJobs();
    if (!timeframes.length) return;
    const now = Date.now();
    if (!force && now - marketTimeframeRefreshAt < 20000) return;
    if (marketTimeframeRefreshPromise) return marketTimeframeRefreshPromise;

    marketTimeframeRefreshPromise = (async () => {
        try {
            for (const timeframe of timeframes) {
                const response = await fetch(screenerApiUrl("/api/alert_metrics"), {
                    method: "POST", headers: {"Content-Type": "application/json"}, cache: "no-store",
                    body: JSON.stringify({
                        symbols: coins.map(c => String(c.symbol || "").toUpperCase()).filter(Boolean).slice(0, 600),
                        timeframe, need_market: true, need_oi: false, need_volume: false
                    })
                });
                if (!response.ok) continue;
                const data = await response.json().catch(() => ({}));
                Object.entries(data.metrics || {}).forEach(([symbol, values]) => {
                    const coin = coins.find(c => String(c.symbol || "").toUpperCase() === symbol);
                    if (!coin) return;
                    coin.__marketTimeframeMetrics = coin.__marketTimeframeMetrics || {};
                    coin.__marketTimeframeMetrics[timeframe] = Object.assign(
                        coin.__marketTimeframeMetrics[timeframe] || {}, values || {}
                    );
                });
            }
            marketTimeframeRefreshAt = Date.now();
            renderTable();
        } catch (error) {
            console.warn("Market timeframe metrics refresh failed:", error);
            await fallbackToLocalIfRenderUnavailable(error);
        } finally {
            marketTimeframeRefreshPromise = null;
        }
    })();
    return marketTimeframeRefreshPromise;
}

// Min/Max are optional filters. Empty Min/Max means "do not filter".
// Selecting a column only controls which column is displayed.
function passesMarketFilters(c, columns) {
    return columns.every(key => {
        const settings = marketSettings.fields?.[key];
        if (!settings) return true;

        const min = parseMarketLimit(settings.min);
        const max = parseMarketLimit(settings.max);
        if (min === null && max === null) return true;

        if ((MARKET_COLUMN_META[key] || {}).type === "text") return true;

        const value = marketTimeframeMetricValue(c, key, settings);
        if (!Number.isFinite(value)) return false;

        // Positive change thresholds use magnitude, so +10% and -10% both
        // satisfy a Min=10 condition. Explicit negative ranges stay directional.
        if (key === "change_pct" && min !== null && min >= 0 && (max === null || max >= 0)) {
            const magnitude = Math.abs(value);
            if (magnitude < min) return false;
            if (max !== null && magnitude > max) return false;
            return true;
        }

        if (min !== null && value < min) return false;
        if (max !== null && value > max) return false;
        return true;
    });
}

const MARKET_COLUMN_META = {
    symbol: { label: "Название", type: "text" },
    trades: { label: "Сделки", type: "number" },
    change_pct: { label: "Изм. цены, %", type: "number" },
    volume_24h: { label: "Оборот, $", type: "number" },
    natr: { label: "NATR", type: "number" },
    btc_corr: { label: "Корреляция к BTC", type: "number" },
    volume_spike: { label: "Всплеск объёма, %", type: "number" },
    volume_expansion_x: { label: "Всплеск объёма, x", type: "number" },
    spread_pct: { label: "Спред, %", type: "number" },
    funding_pct: { label: "Фандинг, %", type: "number" },
    oi_change_usd: { label: "Изм. OI, $", type: "number" },
    oi_change_pct: { label: "Изм. OI, %", type: "number" },
    delta_volume_usd: { label: "Δ оборота, $", type: "number" },
    price: { label: "Цена", type: "number" }
};

// Three-state column sorting: unsorted -> ascending -> descending -> unsorted.
let marketSort = { column: null, direction: 0 };

function marketSortValue(c, key) {
    if (key === "symbol") return String(c.symbol || "");
    if (key === "spread_pct") {
        const bid = Number(c.bid);
        const ask = Number(c.ask);
        const mid = (bid + ask) / 2;
        return Number.isFinite(mid) && mid > 0 ? (ask - bid) / mid * 100 : NaN;
    }
    const value = Number(c[key]);
    return Number.isFinite(value) ? value : NaN;
}

function applyMarketSort(rows) {
    if (!marketSort.column || !marketSort.direction) return rows;
    const key = marketSort.column;
    const direction = marketSort.direction;
    const meta = MARKET_COLUMN_META[key] || { type: "number" };
    return rows.slice().sort((a, b) => {
        let av = marketSortValue(a, key);
        let bv = marketSortValue(b, key);
        if (key === "change_pct" && marketSettings.ignoreSign) {
            av = Math.abs(av);
            bv = Math.abs(bv);
        }
        if (meta.type === "text") {
            return String(av).localeCompare(String(bv), undefined, { numeric: true, sensitivity: "base" }) * direction;
        }
        const aMissing = !Number.isFinite(av);
        const bMissing = !Number.isFinite(bv);
        if (aMissing && bMissing) return 0;
        if (aMissing) return 1;
        if (bMissing) return -1;
        if (av === bv) return 0;
        return (av < bv ? -1 : 1) * direction;
    });
}

function updateMarketSort(column) {
    if (marketSort.column !== column) {
        // При «Игнорировать +/-» для изменения цены первым показываем
        // самые сильные движения: 54%, -48%, 36% и т.д.
        const direction = (column === "change_pct" && marketSettings.ignoreSign) ? -1 : 1;
        marketSort = { column, direction };
    } else if (marketSort.direction === 1) {
        marketSort.direction = -1;
    } else {
        marketSort = { column: null, direction: 0 };
    }
    renderTable();
}

function renderMarketHeaders(columns) {
    const head = document.querySelector(".market-panel thead tr");
    if (!head) return;
    head.innerHTML = columns.map(key => {
        const meta = MARKET_COLUMN_META[key] || { label: key, type: "number" };
        const active = marketSort.column === key && marketSort.direction !== 0;
        const arrow = active ? (marketSort.direction === 1 ? " ↑" : " ↓") : "";
        const aria = active ? (marketSort.direction === 1 ? "ascending" : "descending") : "none";
        return `<th class="market-sort-header" data-sort-column="${key}" aria-sort="${aria}" title="Нажмите для сортировки">${meta.label}${arrow}</th>`;
    }).join("");
    head.querySelectorAll(".market-sort-header").forEach(th => {
        th.addEventListener("click", () => updateMarketSort(th.dataset.sortColumn));
    });
}

function readMarketSettingsFromUI() {
    const columns = Array.from(document.querySelectorAll(".market-column-check:checked"))
        .map(el => el.dataset.column);
    const fields = {};
    document.querySelectorAll("[data-market-field][data-column]").forEach(el => {
        const key = el.dataset.column;
        fields[key] = fields[key] || {};
        fields[key][el.dataset.marketField] = el.value;
    });
    return {
        columns: columns.length ? columns : ["symbol"],
        fields,
        ignoreSign: document.getElementById("marketIgnoreSign")?.checked || false,
        linkActive: document.getElementById("marketLinkActive")?.checked || false,
        freeze: document.getElementById("marketFreeze")?.checked || false,
        freezeAll: document.getElementById("marketFreezeAll")?.checked || false
    };
}

function populateMarketSettingsUI() {
    document.querySelectorAll(".market-column-check").forEach(el => {
        el.checked = marketSettings.columns.includes(el.dataset.column);
    });
    document.querySelectorAll("[data-market-field][data-column]").forEach(el => {
        const value = marketSettings.fields?.[el.dataset.column]?.[el.dataset.marketField];
        el.value = value ?? (el.dataset.marketField === "timeframe" ? "1m" : "");
    });
    const setChecked = (id, value) => { const el = document.getElementById(id); if (el) el.checked = !!value; };
    setChecked("marketIgnoreSign", marketSettings.ignoreSign);
    setChecked("marketLinkActive", marketSettings.linkActive);
    setChecked("marketFreeze", marketSettings.freeze);
    setChecked("marketFreezeAll", marketSettings.freezeAll);
}

function applyMarketSettingsFromUI() {
    const next = readMarketSettingsFromUI();
    marketSettings = next;
    saveMarketSettings();
    marketSettingsApplied = true;
    if (marketSettings.freeze || marketSettings.freezeAll) {
        if (!marketFrozenCoins) marketFrozenCoins = latestCoins.slice();
    } else {
        marketFrozenCoins = null;
    }
    renderTable();
    refreshMarketTimeframeMetrics(latestCoins, true);
}

function resetMarketSettings() {
    marketSettings = structuredClone(DEFAULT_MARKET_SETTINGS);
    marketFrozenCoins = null;
    saveMarketSettings();
    populateMarketSettingsUI();
    renderTable();
    refreshMarketTimeframeMetrics(latestCoins, true);
}

const marketApplyButton = document.getElementById("marketApplyButton");
const marketResetButton = document.getElementById("marketResetButton");
marketApplyButton?.addEventListener("click", applyMarketSettingsFromUI);
marketResetButton?.addEventListener("click", resetMarketSettings);
populateMarketSettingsUI();



function escapeHtml(value) {
    return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/\"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

// ============================================================
// ALERTS — constructor + local persistence
// ============================================================
const ALERTS_STORAGE = "cryptoScreenerAlerts";
const alertsOverlay = document.getElementById("alertsOverlay");
const alertsButton = document.getElementById("alertsButton");
const alertsClose = document.getElementById("alertsClose");
const alertCancel = document.getElementById("alertCancel");
const alertNext = document.getElementById("alertNext");
const alertBack = document.getElementById("alertBack");
const alertSave = document.getElementById("alertSave");
const alertDelete = document.getElementById("alertDelete");
const alertFilterList = document.getElementById("alertFilterList");
const alertCoinList = document.getElementById("alertCoinList");
const alertSavedList = document.getElementById("alertSavedList");
const alertCenterSavedList = document.getElementById("alertCenterSavedList");
const alertSummary = document.getElementById("alertSummary");
const alertCountBadge = document.getElementById("alertCountBadge");

const ALERT_FILTER_KEYS = Object.keys(MARKET_COLUMN_META).filter(key => key !== "symbol");
const ALERT_TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "4h", "12h", "24h", "25h", "1D"];
let alertDraft = null;
let alertStep = 1;
let alertEditingId = null;
let savedAlerts = loadSavedAlerts();

function loadSavedAlerts() {
    try {
        const value = JSON.parse(localStorage.getItem(ALERTS_STORAGE) || "[]");
        return Array.isArray(value) ? value.filter(item => item && typeof item === "object") : [];
    } catch {
        return [];
    }
}

function saveSavedAlerts() {
    localStorage.setItem(ALERTS_STORAGE, JSON.stringify(savedAlerts));
}

function makeAlertId() {
    return `alert_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
}

function defaultAlertDraft() {
    return {
        id: null,
        name: "",
        market: "binance_futures",
        symbols: [],
        filters: [],
        repeat: "3600",
        timezone: "local",
        active: true,
        channels: { site: true, desktop: false, telegram: false, telegramDestinationId: "" },
        soundId: "builtin:terminal",
        createdAt: null,
        updatedAt: null
    };
}

function cloneAlert(value) {
    return JSON.parse(JSON.stringify(value));
}

function normalizeAlertDraft(value) {
    const base = defaultAlertDraft();
    const draft = Object.assign(base, cloneAlert(value || {}));
    draft.name = String(draft.name || "").slice(0, 80);
    draft.market = ["binance_futures", "binance_spot", "all"].includes(draft.market) ? draft.market : base.market;
    draft.symbols = Array.isArray(draft.symbols) ? [...new Set(draft.symbols.map(s => String(s || "").toUpperCase()).filter(Boolean))] : [];
    draft.filters = Array.isArray(draft.filters) ? draft.filters.map(normalizeAlertFilter).filter(Boolean) : [];
    draft.channels = Object.assign({}, base.channels, draft.channels || {});
    draft.channels.telegramDestinationId = String(draft.channels.telegramDestinationId || "");
    draft.repeat = String(draft.repeat ?? base.repeat);
    draft.timezone = draft.timezone === "utc" ? "utc" : "local";
    draft.active = draft.active !== false;
    draft.soundId = String(draft.soundId || "builtin:terminal");
    return draft;
}

function normalizeAlertFilter(filter) {
    if (!filter || typeof filter !== "object") return null;
    const key = ALERT_FILTER_KEYS.includes(filter.key) ? filter.key : ALERT_FILTER_KEYS[0];
    const oiTimeframes = ["5m", "15m", "30m", "1h", "4h", "1D"];
    const allowedTimeframes = key === "oi_change_pct" ? oiTimeframes : ALERT_TIMEFRAMES;
    return {
        key,
        timeframe: allowedTimeframes.includes(filter.timeframe) ? filter.timeframe : (key === "oi_change_pct" ? "1h" : "1h"),
        min: filter.min == null ? "" : String(filter.min),
        max: filter.max == null ? "" : String(filter.max),
        direction: ["any", "up", "down"].includes(filter.direction) ? filter.direction : "any",
        volumeBase: Math.max(1, Math.min(500, Number(filter.volumeBase) || 100)),
        volumeGrowth: Math.max(1, Math.min(200, Number(filter.volumeGrowth) || 20))
    };
}

function alertOpen(editId = null) {
    alertEditingId = editId ? String(editId) : null;
    const existing = alertEditingId ? savedAlerts.find(item => String(item.id) === alertEditingId) : null;
    alertDraft = normalizeAlertDraft(existing || defaultAlertDraft());
    if (alertEditingId) alertDraft.soundId = alertSoundConfigFor(alertEditingId).soundId;
    else alertDraft.soundId = alertSoundSettings.defaultAlert.soundId;
    alertStep = 1;
    alertsOverlay.classList.add("open");
    alertsOverlay.setAttribute("aria-hidden", "false");
    populateAlertDraftUI();
    renderAlertCoins();
    renderAlertFilters();
    renderAlertSavedList();
    renderAlertCenterSavedList();
    renderNotificationRouting();
    updateAlertStepUI();
}

function alertClose() {
    alertsOverlay.classList.remove("open");
    alertsOverlay.setAttribute("aria-hidden", "true");
    alertDraft = null;
    alertEditingId = null;
}

function populateAlertDraftUI() {
    document.getElementById("alertName").value = alertDraft.name;
    document.querySelectorAll("input[name='alertMarket']").forEach(el => { el.checked = el.value === alertDraft.market; });
    document.getElementById("alertRepeat").value = alertDraft.repeat;
    document.getElementById("alertTimezone").value = alertDraft.timezone;
    document.getElementById("alertActive").value = alertDraft.active ? "true" : "false";
    document.getElementById("alertChannelSite").checked = !!alertDraft.channels.site;
    document.getElementById("alertChannelDesktop").checked = !!alertDraft.channels.desktop;
    document.getElementById("alertChannelTelegram").checked = !!alertDraft.channels.telegram;
    renderAlertTelegramDestination();
    syncAlertTelegramUI();
    document.getElementById("alertCoinSearch").value = "";
}

function syncAlertDraftFromUI() {
    if (!alertDraft) return;
    alertDraft.name = document.getElementById("alertName").value.trim().slice(0, 80);
    alertDraft.market = document.querySelector("input[name='alertMarket']:checked")?.value || "binance_futures";
    alertDraft.repeat = document.getElementById("alertRepeat").value;
    alertDraft.timezone = document.getElementById("alertTimezone").value;
    alertDraft.active = document.getElementById("alertActive").value === "true";
    alertDraft.channels = {
        site: document.getElementById("alertChannelSite").checked,
        desktop: document.getElementById("alertChannelDesktop").checked,
        telegram: document.getElementById("alertChannelTelegram").checked,
        telegramDestinationId: String(document.getElementById("alertTelegramDestination")?.value || alertDraft.channels.telegramDestinationId || "")
    };
    alertDraft.symbols = Array.from(alertCoinList.querySelectorAll("input[data-alert-symbol]:checked"))
        .map(el => el.dataset.alertSymbol);
}

function alertCoinUniverse() {
    const symbols = latestCoins.map(c => String(c.symbol || "").toUpperCase()).filter(symbol => symbol.endsWith("USDT"));
    return [...new Set(symbols)].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
}

function renderAlertCoins() {
    if (!alertCoinList || !alertDraft) return;
    const query = document.getElementById("alertCoinSearch").value.trim().toUpperCase();
    const symbols = alertCoinUniverse().filter(symbol => !query || symbol.includes(query));
    if (!symbols.length) {
        alertCoinList.innerHTML = `<div class="alert-empty">Монеты пока не получены из Binance.</div>`;
        return;
    }
    alertCoinList.innerHTML = symbols.map(symbol => `
        <label class="alert-coin-item">
            <input type="checkbox" data-alert-symbol="${symbol}"${alertDraft.symbols.includes(symbol) ? " checked" : ""}>
            <span>${symbol}</span>
        </label>
    `).join("");
}

function renderAlertFilters() {
    if (!alertFilterList || !alertDraft) return;
    if (!alertDraft.filters.length) {
        alertFilterList.innerHTML = `<div class="alert-empty">Фильтры пока не добавлены.</div>`;
        return;
    }
    alertFilterList.innerHTML = alertDraft.filters.map((filter, index) => {
        const options = ALERT_FILTER_KEYS.map(key => {
            const meta = MARKET_COLUMN_META[key] || { label: key };
            return `<option value="${key}"${filter.key === key ? " selected" : ""}>${meta.label}</option>`;
        }).join("");
        const tfs = filter.key === "oi_change_pct" ? ["5m", "15m", "30m", "1h", "4h", "1D"] : ALERT_TIMEFRAMES;
        const timeframes = tfs.map(tf => `<option value="${tf}"${filter.timeframe === tf ? " selected" : ""}>${tf}</option>`).join("");
        const isOi = filter.key === "oi_change_pct";
        const isVolume = filter.key === "volume_expansion_x";
        return `
            <div class="alert-filter-row${isVolume ? " alert-filter-row-volume" : ""}" data-alert-filter-index="${index}">
                <select class="alert-select" data-alert-filter-field="key">${options}</select>
                <select class="alert-select" data-alert-filter-field="timeframe">${timeframes}</select>
                <input class="alert-input" data-alert-filter-field="min" type="text" value="${escapeHtml(filter.min)}" placeholder="Мин">
                <input class="alert-input" data-alert-filter-field="max" type="text" value="${escapeHtml(filter.max)}" placeholder="Макс">
                ${isOi || isVolume ? `<button class="alert-filter-settings-button" data-alert-filter-settings type="button" title="Дополнительные настройки">⚙</button>` : `<span class="alert-filter-spacer"></span>`}
                <button class="alert-filter-remove" data-alert-filter-remove="1" type="button" title="Удалить">×</button>
                ${(isOi || isVolume) ? `<div class="alert-filter-settings-popover" data-alert-filter-settings-popover>
                    ${isOi ? `<label>Направление<select class="alert-select" data-alert-filter-field="direction"><option value="any"${filter.direction === "any" ? " selected" : ""}>Любое</option><option value="up"${filter.direction === "up" ? " selected" : ""}>Рост</option><option value="down"${filter.direction === "down" ? " selected" : ""}>Падение</option></select></label>` : ""}
                    ${isVolume ? `<label>Среднее<input class="alert-input" data-alert-filter-field="volumeBase" type="number" min="1" max="500" value="${filter.volumeBase}"> свечей</label><label>Период всплеска<input class="alert-input" data-alert-filter-field="volumeGrowth" type="number" min="1" max="200" value="${filter.volumeGrowth}"> свечей</label>` : ""}
                </div>` : ""}
            </div>
        `;
    }).join("");
}

function syncAlertFiltersFromUI() {
    if (!alertDraft) return;
    alertDraft.filters = Array.from(alertFilterList.querySelectorAll("[data-alert-filter-index]")).map(row => ({
        key: row.querySelector("[data-alert-filter-field='key']")?.value || ALERT_FILTER_KEYS[0],
        timeframe: row.querySelector("[data-alert-filter-field='timeframe']")?.value || "1h",
        min: row.querySelector("[data-alert-filter-field='min']")?.value || "",
        max: row.querySelector("[data-alert-filter-field='max']")?.value || "",
        direction: row.querySelector("[data-alert-filter-field='direction']")?.value || "any",
        volumeBase: row.querySelector("[data-alert-filter-field='volumeBase']")?.value || 100,
        volumeGrowth: row.querySelector("[data-alert-filter-field='volumeGrowth']")?.value || 20
    })).map(normalizeAlertFilter).filter(Boolean);
}

function addAlertFilter() {
    syncAlertFiltersFromUI();
    alertDraft.filters.push({ key: "change_pct", timeframe: "1h", min: "", max: "", direction: "any", volumeBase: 100, volumeGrowth: 20 });
    renderAlertFilters();
}

function updateAlertStepUI() {
    document.querySelectorAll("[data-alert-step-indicator]").forEach(el => {
        const step = Number(el.dataset.alertStepIndicator);
        el.classList.toggle("active", step === alertStep);
        el.classList.toggle("done", step < alertStep);
    });
    document.querySelectorAll(".alert-step-content").forEach(el => el.classList.remove("active"));
    document.getElementById(`alertStep${alertStep}`)?.classList.add("active");
    alertBack.style.display = alertStep > 1 ? "inline-block" : "none";
    alertNext.style.display = alertStep < 3 ? "inline-block" : "none";
    alertSave.style.display = alertStep === 3 ? "inline-block" : "none";
    alertDelete.style.display = alertEditingId ? "inline-block" : "none";
    if (alertStep === 3) renderAlertSummary();
}

function validateAlertStep(step) {
    syncAlertDraftFromUI();
    syncAlertFiltersFromUI();
    if (step === 1) {
        if (!alertDraft.name) {
            document.getElementById("alertName").focus();
            return false;
        }
        return true;
    }
    if (step === 2) {
        if (!alertDraft.filters.length) {
            alertFilterList?.querySelector(".alert-filter-add")?.focus();
            return false;
        }
        for (const filter of alertDraft.filters) {
            if (!String(filter.min).trim() && !String(filter.max).trim()) return false;
            if (filter.min !== "" && parseMarketLimit(filter.min) === null) return false;
            if (filter.max !== "" && parseMarketLimit(filter.max) === null) return false;
            if (filter.key === "volume_expansion_x") {
                const base = Number(filter.volumeBase);
                const growth = Number(filter.volumeGrowth);
                if (!Number.isInteger(base) || base < 1 || base > 500) return false;
                if (!Number.isInteger(growth) || growth < 1 || growth > 200) return false;
            }
        }
        return true;
    }
    return true;
}

function alertMarketLabel(market) {
    return ({ binance_futures: "Binance Futures", binance_spot: "Binance Spot", all: "Все доступные" })[market] || market;
}

function alertRepeatLabel(value) {
    const map = { once: "Один раз", "60": "1 мин", "300": "5 мин", "900": "15 мин", "1800": "30 мин", "3600": "1 час", "14400": "4 часа", "86400": "1 день" };
    return map[String(value)] || String(value);
}

function renderAlertSummary() {
    if (!alertSummary || !alertDraft) return;
    const selectedSymbols = alertDraft.symbols.length ? `${alertDraft.symbols.length} мон.` : "весь рынок";
    const filters = alertDraft.filters.length
        ? alertDraft.filters.map(f => {
            const label = MARKET_COLUMN_META[f.key]?.label || f.key;
            const limits = [f.min ? `≥ ${f.min}` : "", f.max ? `≤ ${f.max}` : ""].filter(Boolean).join("; ");
            const direction = f.key === "oi_change_pct" ? ({any:"Любое",up:"Рост",down:"Падение"}[f.direction] || "Любое") : "";
            const volume = f.key === "volume_expansion_x" ? `Среднее ${f.volumeBase} / всплеск ${f.volumeGrowth} свечей` : "";
            return [label, f.timeframe, limits, direction, volume].filter(Boolean).join(" · ");
        }).join("<br>")
        : "Без фильтров";
    const channels = [
        alertDraft.channels.site ? "На сайте" : "",
        alertDraft.channels.desktop ? "На компьютере" : "",
        alertDraft.channels.telegram ? "Telegram" : ""
    ].filter(Boolean).join(", ") || "не выбраны";
    alertSummary.innerHTML = `
        <div class="alert-summary-row"><span class="alert-summary-key">Название</span><strong>${escapeHtml(alertDraft.name || "—")}</strong></div>
        <div class="alert-summary-row"><span class="alert-summary-key">Рынок</span><span>${escapeHtml(alertMarketLabel(alertDraft.market))}</span></div>
        <div class="alert-summary-row"><span class="alert-summary-key">Монеты</span><span>${escapeHtml(selectedSymbols)}</span></div>
        <div class="alert-summary-row"><span class="alert-summary-key">Фильтры</span><span style="text-align:right">${filters}</span></div>
        <div class="alert-summary-row"><span class="alert-summary-key">Повтор</span><span>${escapeHtml(alertRepeatLabel(alertDraft.repeat))}</span></div>
        <div class="alert-summary-row"><span class="alert-summary-key">Каналы</span><span>${escapeHtml(channels)}</span></div>
        <div class="alert-summary-row"><span class="alert-summary-key">Статус</span><span>${alertDraft.active ? "Активен" : "Выключен"}</span></div>
    `;
    populateAlertSoundSelect();
}

function toggleSavedAlertActive(id) {
    const alert = savedAlerts.find(item => item.id === id);
    if (!alert) return;
    alert.active = alert.active === false;
    alert.updatedAt = new Date().toISOString();
    saveSavedAlerts();
    notificationRouting.alerts[payload.id] = { telegram: !!payload.channels.telegram, telegramDestinationId: String(payload.channels.telegramDestinationId || "") };
    saveNotificationRouting();
    renderAlertSavedList();
    renderAlertCenterSavedList();
}

function renderAlertCenterSavedList() {
    if (!alertCenterSavedList) return;
    if (!savedAlerts.length) {
        alertCenterSavedList.innerHTML = `<div class="alert-center-empty">Сохранённых алертов пока нет.</div>`;
        return;
    }
    alertCenterSavedList.innerHTML = savedAlerts.map(alert => {
        const active = alert.active !== false;
        const filters = alert.filters?.length || 0;
        const symbols = alert.symbols?.length ? `${alert.symbols.length} мон.` : "весь рынок";
        return `<div class="alert-saved-center-item${active ? "" : " is-disabled"}">
            <div class="alert-saved-center-info" data-alert-center-edit="${escapeHtml(alert.id)}">
                <div class="alert-saved-center-name">${escapeHtml(alert.name || "Без названия")}</div>
                <div class="alert-saved-center-meta">${escapeHtml(alertMarketLabel(alert.market))} · ${filters} фильтр(а) · ${escapeHtml(symbols)} · ${active ? "активен" : "выключен"}</div>
            </div>
            <div class="alert-saved-center-actions">
                <button type="button" class="alert-toggle${active ? " on" : ""}" data-alert-toggle="${escapeHtml(alert.id)}" aria-pressed="${active ? "true" : "false"}" title="${active ? "Выключить алерт" : "Включить алерт"}"></button>
                <button type="button" class="alert-saved-center-edit" data-alert-center-edit="${escapeHtml(alert.id)}">Изменить</button>
            </div>
        </div>`;
    }).join("");
}

function renderAlertSavedList() {
    if (!alertSavedList) return;
    if (!savedAlerts.length) {
        alertSavedList.innerHTML = `<div class="alert-empty">Сохранённых алертов пока нет.</div>`;
        return;
    }
    alertSavedList.innerHTML = savedAlerts.map(alert => `
        <div class="alert-saved-item">
            <div>
                <div class="alert-saved-name">${escapeHtml(alert.name || "Без названия")}</div>
                <div class="alert-saved-meta">${escapeHtml(alertMarketLabel(alert.market))} · ${alert.filters?.length || 0} фильтр(а) · ${alert.active === false ? "выключен" : "активен"}</div>
            </div>
            <div class="alert-saved-actions">
                <button type="button" class="alert-toggle${alert.active === false ? "" : " on"}" data-alert-toggle="${alert.id}" aria-pressed="${alert.active === false ? "false" : "true"}" title="${alert.active === false ? "Включить алерт" : "Выключить алерт"}"></button>
                <button type="button" class="alert-small-button" data-alert-edit="${alert.id}">Изменить</button>
            </div>
        </div>
    `).join("");
    updateAlertNotificationBadge();
}

function saveCurrentAlert() {
    if (!alertDraft) return;
    if (!validateAlertStep(3)) return;
    captureAlertTelegramUI();
    if (alertDraft.channels.desktop) requestDesktopAlertPermission();
    if (alertDraft.channels.telegram && (!alertDraft.channels.telegramDestinationId || (alertDraft.channels.telegramDestinationId !== TELEGRAM_CLOUDFLARE_DESTINATION_ID && !telegramConnectionById(alertDraft.channels.telegramDestinationId)))) { alertStep=3; updateAlertStepUI(); return; }
    const now = new Date().toISOString();
    const payload = normalizeAlertDraft(alertDraft);
    payload.id = alertEditingId || makeAlertId();
    payload.createdAt = alertEditingId ? (savedAlerts.find(item => item.id === alertEditingId)?.createdAt || now) : now;
    payload.updatedAt = now;
    if (payload.id) {
        const soundConfig = ensureAlertSoundConfig(payload.id);
        soundConfig.soundId = payload.soundId || soundConfig.soundId;
        saveAlertSoundSettingsV2();
    }
    if (alertEditingId) {
        savedAlerts = savedAlerts.map(item => item.id === alertEditingId ? payload : item);
    } else {
        savedAlerts.unshift(payload);
    }
    saveSavedAlerts();
    renderAlertSavedList();
    renderAlertCenterSavedList();
    renderNotificationSoundSettings();
    alertClose();
}

// Stage 3: the top bell opens the in-site notification center.
// Alert creation remains available from the center itself.
alertsButton?.addEventListener("click", () => {
    if (alertNotificationPanel?.classList.contains("open")) {
        toggleAlertNotificationPanel(false);
        return;
    }
    if (!alertNotificationPanel) return;
    toggleAlertNotificationPanel(true);
    setAlertCenterTab("notifications");
});
alertsClose?.addEventListener("click", alertClose);
alertCancel?.addEventListener("click", alertClose);
alertsOverlay?.addEventListener("click", event => {
    if (event.target === alertsOverlay) alertClose();
});
alertNext?.addEventListener("click", () => {
    if (!validateAlertStep(alertStep)) return;
    if (alertStep < 3) {
        alertStep += 1;
        updateAlertStepUI();
    }
});
alertBack?.addEventListener("click", () => {
    syncAlertDraftFromUI();
    syncAlertFiltersFromUI();
    if (alertStep > 1) {
        alertStep -= 1;
        updateAlertStepUI();
    }
});
alertSave?.addEventListener("click", saveCurrentAlert);
document.getElementById("alertAddFilter")?.addEventListener("click", addAlertFilter);
document.getElementById("alertCoinSearch")?.addEventListener("input", renderAlertCoins);
document.getElementById("alertSelectAllCoins")?.addEventListener("click", () => {
    if (!alertDraft) return;
    alertDraft.symbols = alertCoinUniverse();
    renderAlertCoins();
});
document.getElementById("alertClearCoins")?.addEventListener("click", () => {
    if (!alertDraft) return;
    alertDraft.symbols = [];
    renderAlertCoins();
});
alertCoinList?.addEventListener("change", event => {
    if (event.target.matches("[data-alert-symbol]") && alertDraft) syncAlertDraftFromUI();
});
alertFilterList?.addEventListener("change", () => syncAlertFiltersFromUI());
alertFilterList?.addEventListener("input", () => syncAlertFiltersFromUI());
document.getElementById("alertChannelTelegram")?.addEventListener("change", () => { captureAlertTelegramUI(); syncAlertTelegramUI(); });
document.getElementById("alertChannelDesktop")?.addEventListener("change", () => { if (document.getElementById("alertChannelDesktop")?.checked) requestDesktopAlertPermission(); });
document.getElementById("alertTelegramTest")?.addEventListener("click", async () => {
    captureAlertTelegramUI(); const status=document.getElementById("alertTelegramStatus"); const destinationId=alertDraft?.channels?.telegramDestinationId;
    if(!destinationId){ if(status) status.textContent="Выберите Telegram-подключение"; return; }
    if(status) status.textContent="Отправка...";
    try {
        if(destinationId === TELEGRAM_CLOUDFLARE_DESTINATION_ID){
            const response=await fetch(TELEGRAM_CLOUDFLARE_WORKER_URL,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({message:"Crypto Screener: Cloudflare → Telegram работает."})});
            const result=await response.json().catch(()=>({}));
            if(!response.ok || !result.ok) throw new Error(result.error || "Cloudflare Worker error");
        } else {
            const c=telegramConnectionById(destinationId);
            if(!c) throw new Error("Telegram-подключение не найдено");
            const response=await fetch("/api/telegram/test",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({token:c.token,chat_id:c.chatId})});
            const result=await response.json().catch(()=>({}));
            if(!response.ok || !result.ok) throw new Error(result.error || "Telegram error");
        }
        if(status) status.textContent="✓ Telegram работает";
    } catch(error){ if(status) status.textContent=`Ошибка: ${error.message}`; }
});

alertFilterList?.addEventListener("change", event => {
    const keySelect = event.target.closest("[data-alert-filter-field='key']");
    if (!keySelect || !alertDraft) return;
    const row = keySelect.closest("[data-alert-filter-index]");
    const index = Number(row?.dataset.alertFilterIndex);
    syncAlertFiltersFromUI();
    if (!Number.isInteger(index) || !alertDraft.filters[index]) return;
    const filter = alertDraft.filters[index];
    if (filter.key === "oi_change_pct") {
        filter.timeframe = ["5m", "15m", "30m", "1h", "4h", "1D"].includes(filter.timeframe) ? filter.timeframe : "1h";
    } else if (!ALERT_TIMEFRAMES.includes(filter.timeframe)) {
        filter.timeframe = "1h";
    }
    renderAlertFilters();
});

alertFilterList?.addEventListener("click", event => {
    const settings = event.target.closest("[data-alert-filter-settings]");
    if (settings) {
        const row = settings.closest("[data-alert-filter-index]");
        const popover = row?.querySelector("[data-alert-filter-settings-popover]");
        if (popover) {
            const willOpen = !popover.classList.contains("open");
            alertFilterList.querySelectorAll("[data-alert-filter-settings-popover].open").forEach(el => el.classList.remove("open"));
            alertFilterList.querySelectorAll("[data-alert-filter-settings].active").forEach(el => el.classList.remove("active"));
            if (willOpen) { popover.classList.add("open"); settings.classList.add("active"); }
        }
        return;
    }
    const remove = event.target.closest("[data-alert-filter-remove]");
    if (!remove || !alertDraft) return;
    syncAlertFiltersFromUI();
    const row = remove.closest("[data-alert-filter-index]");
    const index = Number(row?.dataset.alertFilterIndex);
    if (Number.isInteger(index)) alertDraft.filters.splice(index, 1);
    renderAlertFilters();
});
alertDelete?.addEventListener("click", () => {
    if (!alertEditingId) return;
    const id = alertEditingId;
    savedAlerts = savedAlerts.filter(item => item.id !== id);
    delete alertEngineState[id];
    delete alertSoundSettings.alerts[id];
    delete notificationRouting.alerts[id];
    saveNotificationRouting();
    saveAlertSoundSettingsV2();
    saveSavedAlerts();
    saveAlertEngineState();
    renderAlertSavedList();
    renderAlertCenterSavedList();
    alertClose();
});

document.addEventListener("click", event => {
    const edit = event.target.closest?.("[data-alert-edit], [data-alert-center-edit]");
    if (!edit) return;
    const id = edit.dataset.alertEdit || edit.dataset.alertCenterEdit;
    if (!id || !savedAlerts.some(item => String(item.id) === String(id))) return;
    event.preventDefault();
    event.stopPropagation();
    if (alertNotificationPanel?.classList.contains("open")) toggleAlertNotificationPanel(false);
    alertOpen(String(id));
}, true);

alertSavedList?.addEventListener("click", event => {
    const toggle = event.target.closest("[data-alert-toggle]");
    if (toggle) { event.preventDefault(); event.stopPropagation(); toggleSavedAlertActive(toggle.dataset.alertToggle); return; }
    const edit = event.target.closest("[data-alert-edit]");
    if (edit) { event.preventDefault(); event.stopPropagation(); alertOpen(String(edit.dataset.alertEdit)); }
});

alertCenterSavedList?.addEventListener("click", event => {
    const toggle = event.target.closest("[data-alert-toggle]");
    if (toggle) { event.preventDefault(); event.stopPropagation(); toggleSavedAlertActive(toggle.dataset.alertToggle); return; }
    const edit = event.target.closest("[data-alert-center-edit]");
    if (edit) {
        toggleAlertNotificationPanel(false);
        alertOpen(String(edit.dataset.alertCenterEdit));
    }
});

// ============================================================
// ALERT ENGINE — STAGE 2: evaluate saved rules against Market State
// ============================================================
// Stage 2 deliberately contains no notification UI or external delivery.
// It only decides whether a saved alert has a valid matching signal.
const ALERT_ENGINE_STORAGE = "cryptoScreenerAlertEngineState";
const ALERT_ENGINE_HISTORY_LIMIT = 200;
let alertEngineState = loadAlertEngineState();
let alertEngineEvents = [];

function loadAlertEngineState() {
    try {
        const value = JSON.parse(localStorage.getItem(ALERT_ENGINE_STORAGE) || "{}");
        return value && typeof value === "object" ? value : {};
    } catch {
        return {};
    }
}

function saveAlertEngineState() {
    localStorage.setItem(ALERT_ENGINE_STORAGE, JSON.stringify(alertEngineState));
}

function alertEngineNow() {
    return Date.now();
}

function alertRepeatSeconds(alert) {
    const value = String(alert?.repeat ?? "3600");
    if (value === "once") return null;
    const seconds = Number(value);
    return Number.isFinite(seconds) && seconds >= 0 ? seconds : 3600;
}

// Timeframe-aware metrics are loaded through the same dynamic path for both
// Custom Alerts and Market Filters. The UI timeframe is therefore authoritative.
const ALERT_DYNAMIC_KEYS = new Set([
    "change_pct", "volume_24h", "trades", "natr", "btc_corr",
    "volume_spike", "spread_pct", "funding_pct", "oi_change_usd",
    "oi_change_pct", "delta_volume_usd", "price", "volume_expansion_x"
]);
const alertEngineDynamicMetrics = {};
const alertEngineDynamicDirections = {};
let alertEngineDynamicRefreshAt = 0;
let alertEngineDynamicRefreshPromise = null;

function alertEngineDynamicKey(filter) {
    return JSON.stringify([
        String(filter?.key || ""), String(filter?.timeframe || "1h"),
        Number(filter?.volumeBase) || 100, Number(filter?.volumeGrowth) || 20
    ]);
}

function alertEngineMetricValue(coin, key, filter = null) {
    if (!coin || !key) return NaN;
    if (ALERT_DYNAMIC_KEYS.has(key)) {
        const symbol = String(coin.symbol || "").toUpperCase();
        const cacheKey = filter ? alertEngineDynamicKey(filter) : null;
        const value = cacheKey ? Number(alertEngineDynamicMetrics[symbol]?.[cacheKey]) : NaN;
        return Number.isFinite(value) ? value : NaN;
    }
    if (key === "spread_pct") return marketFilterValue(coin, key);
    const value = Number(coin[key]);
    return Number.isFinite(value) ? value : NaN;
}

function alertEngineFilterPasses(coin, filter) {
    const value = alertEngineMetricValue(coin, filter.key, filter);
    if (!Number.isFinite(value)) return false;

    const min = parseMarketLimit(filter.min);
    const max = parseMarketLimit(filter.max);
    if (min === null && max === null) return true;

    // Price-change thresholds are magnitude filters: Min=10 means +10% or -10%.
    // Explicit negative ranges remain directional.
    if (filter.key === "change_pct" && min !== null && min >= 0 && (max === null || max >= 0)) {
        const magnitude = Math.abs(value);
        if (magnitude < min) return false;
        if (max !== null && magnitude > max) return false;
        return true;
    }

    if (min !== null && value < min) return false;
    if (max !== null && value > max) return false;
    return true;
}

function alertEngineFilterDataAvailable(filter) {
    // The current shared Market State contains these fields directly.
    // Do not invent values for columns that are only UI placeholders today.
    return [
        "trades", "change_pct", "volume_24h", "natr", "btc_corr",
        "volume_spike", "spread_pct", "price", "oi_change_pct", "volume_expansion_x"
    ].includes(filter?.key);
}

function alertEngineMarketSupported(alert) {
    // /api/state currently exposes the Binance Futures Market State.
    // 'all' is therefore compatible with the available source; Spot is not.
    return alert?.market === "binance_futures";
}

function alertEngineSymbols(alert, coins) {
    const selected = Array.isArray(alert?.symbols)
        ? new Set(alert.symbols.map(s => String(s || "").toUpperCase()))
        : new Set();
    if (!selected.size) return coins;
    return coins.filter(coin => selected.has(String(coin.symbol || "").toUpperCase()));
}

function alertEngineFilterTimeframeCompatible(filter) {
    const tf = String(filter?.timeframe || "1h");
    return ALERT_TIMEFRAMES.includes(tf);
}

function alertEngineCoinMatches(alert, coin) {
    if (!alertEngineMarketSupported(alert)) return false;
    const filters = Array.isArray(alert?.filters) ? alert.filters : [];
    if (!filters.length) return false;

    return filters.every(filter => {
        if (!alertEngineFilterDataAvailable(filter)) return false;
        if (!alertEngineFilterTimeframeCompatible(filter)) return false;
        if (filter.key === "oi_change_pct") {
            const value = alertEngineMetricValue(coin, filter.key, filter);
            if (filter.direction === "up" && value <= 0) return false;
            if (filter.direction === "down" && value >= 0) return false;
        }
        if (filter.key === "volume_expansion_x" && filter.direction !== "any") {
            const symbol = String(coin.symbol || "").toUpperCase();
            const cacheKey = alertEngineDynamicKey(filter);
            const direction = alertEngineDynamicDirections[symbol]?.[cacheKey] || "any";
            if (direction !== filter.direction) return false;
        }
        return alertEngineFilterPasses(coin, filter);
    });
}

function alertEngineStateFor(id) {
    if (!alertEngineState[id] || typeof alertEngineState[id] !== "object") {
        alertEngineState[id] = { lastTriggeredAt: 0, symbolLastTriggeredAt: {}, activeMatches: {}, firedOnce: false };
    }
    const state = alertEngineState[id];
    if (!state.symbolLastTriggeredAt || typeof state.symbolLastTriggeredAt !== "object") state.symbolLastTriggeredAt = {};
    if (!state.activeMatches || typeof state.activeMatches !== "object") state.activeMatches = {};
    return state;
}

function alertEngineCanTrigger(alert, state, symbol, now) {
    if (state.firedOnce && String(alert.repeat) === "once") return false;
    const repeatSeconds = alertRepeatSeconds(alert);
    if (repeatSeconds === null) return !state.activeMatches[symbol] && !state.firedOnce;
    const last = Number(state.symbolLastTriggeredAt[symbol] || 0);
    return !last || now - last >= repeatSeconds * 1000;
}

function alertEngineRecordEvent(alert, coin, now) {
    const event = {
        id: makeAlertId(),
        alertId: alert.id,
        alertName: alert.name || "Без названия",
        symbol: String(coin.symbol || "").toUpperCase(),
        market: alertMarketLabel(alert.market),
        time: new Date(now).toISOString(),
        values: {},
        filters: cloneAlert(alert.filters || [])
    };

    (alert.filters || []).forEach(filter => {
        const value = alertEngineMetricValue(coin, filter.key, filter);
        if (Number.isFinite(value)) event.values[filter.key] = value;
        if (filter.key === "volume_expansion_x") {
            event.values.volume_base_candles = filter.volumeBase;
            event.values.volume_growth_candles = filter.volumeGrowth;
            const symbol = String(coin.symbol || "").toUpperCase();
            const cacheKey = alertEngineDynamicKey(filter);
            event.values.volume_direction = alertEngineDynamicDirections[symbol]?.[cacheKey] || "any";
        }
    });

    alertEngineEvents.unshift(event);
    if (alertEngineEvents.length > ALERT_ENGINE_HISTORY_LIMIT) {
        alertEngineEvents.length = ALERT_ENGINE_HISTORY_LIMIT;
    }
    return event;
}

async function refreshAlertDynamicMetrics(coins, force = false) {
    if (!Array.isArray(coins) || !coins.length || !Array.isArray(savedAlerts) || !savedAlerts.length) return;
    const now = Date.now();
    if (!force && now - alertEngineDynamicRefreshAt < 20000) return;
    if (alertEngineDynamicRefreshPromise) return alertEngineDynamicRefreshPromise;

    const jobs = new Map();
    savedAlerts.forEach(alert => {
        if (!alert || alert.active === false || alert.market !== "binance_futures") return;
        const selected = Array.isArray(alert.symbols) && alert.symbols.length
            ? new Set(alert.symbols.map(s => String(s).toUpperCase()))
            : null;
        (alert.filters || []).forEach(filter => {
            if (!ALERT_DYNAMIC_KEYS.has(filter.key)) return;
            const symbols = selected ? coins.filter(c => selected.has(String(c.symbol || "").toUpperCase())) : coins;
            if (!symbols.length) return;
            const needOi = filter.key === "oi_change_pct";
            const needVolume = filter.key === "volume_expansion_x";
            const needMarket = filter.key !== "oi_change_pct" && filter.key !== "volume_expansion_x";
            const key = JSON.stringify([filter.timeframe, needOi, needVolume, needMarket, Number(filter.volumeBase) || 100, Number(filter.volumeGrowth) || 20]);
            if (!jobs.has(key)) jobs.set(key, {
                timeframe: filter.timeframe, needOi, needVolume, needMarket,
                volumeBase: Number(filter.volumeBase) || 100, volumeGrowth: Number(filter.volumeGrowth) || 20,
                symbols: new Set()
            });
            symbols.forEach(c => jobs.get(key).symbols.add(String(c.symbol || "").toUpperCase()));
        });
    });

    if (!jobs.size) { alertEngineDynamicRefreshAt = now; return; }

    alertEngineDynamicRefreshPromise = (async () => {
        try {
            for (const job of jobs.values()) {
                const response = await fetch(screenerApiUrl("/api/alert_metrics"), {
                    method: "POST", headers: {"Content-Type": "application/json"}, cache: "no-store",
                    body: JSON.stringify({
                        symbols: [...job.symbols], timeframe: job.timeframe, need_oi: job.needOi,
                        need_volume: job.needVolume, need_market: job.needMarket,
                        volume_base: job.volumeBase, volume_growth: job.volumeGrowth
                    })
                });
                if (!response.ok) continue;
                const data = await response.json().catch(() => ({}));
                Object.entries(data.metrics || {}).forEach(([symbol, values]) => {
                    alertEngineDynamicMetrics[symbol] = alertEngineDynamicMetrics[symbol] || {};
                    Object.entries(values).forEach(([metricKey, metricValue]) => {
                        if (metricKey === "volume_expansion_direction") {
                            const directionCacheKey = JSON.stringify([
                                "volume_expansion_x", job.timeframe, Number(job.volumeBase) || 100, Number(job.volumeGrowth) || 20
                            ]);
                            alertEngineDynamicDirections[symbol] = alertEngineDynamicDirections[symbol] || {};
                            alertEngineDynamicDirections[symbol][directionCacheKey] = String(metricValue || "any");
                            return;
                        }
                        const metricCacheKey = JSON.stringify([
                            metricKey, job.timeframe, Number(job.volumeBase) || 100, Number(job.volumeGrowth) || 20
                        ]);
                        alertEngineDynamicMetrics[symbol][metricCacheKey] = metricValue;
                    });
                });
            }
            alertEngineDynamicRefreshAt = Date.now();
            evaluateAlertEngine(latestCoins);
        } catch (error) {
            console.warn("Alert dynamic metrics refresh failed:", error);
        } finally {
            alertEngineDynamicRefreshPromise = null;
        }
    })();
    return alertEngineDynamicRefreshPromise;
}

function evaluateAlertEngine(coins) {
    if (!Array.isArray(coins) || !coins.length || !Array.isArray(savedAlerts) || !savedAlerts.length) return;

    const now = alertEngineNow();
    let stateChanged = false;

    savedAlerts.forEach(alert => {
        if (!alert || alert.active === false || !alert.id) return;

        const state = alertEngineStateFor(alert.id);
        const candidates = alertEngineSymbols(alert, coins);
        const currentMatches = {};

        candidates.forEach(coin => {
            const symbol = String(coin.symbol || "").toUpperCase();
            if (!symbol || !alertEngineCoinMatches(alert, coin)) return;
            currentMatches[symbol] = true;

            if (!alertEngineCanTrigger(alert, state, symbol, now)) return;

            const event = alertEngineRecordEvent(alert, coin, now);
            state.lastTriggeredAt = now;
            state.symbolLastTriggeredAt[symbol] = now;
            if (String(alert.repeat) === "once") state.firedOnce = true;
            stateChanged = true;

            // Stage 2 output boundary. Stage 3 will consume this event for
            // the in-site notification center, sound and unread counter.
            window.dispatchEvent(new CustomEvent("crypto-screener-alert", { detail: event }));
        });

        if (JSON.stringify(state.activeMatches) !== JSON.stringify(currentMatches)) {
            state.activeMatches = currentMatches;
            stateChanged = true;
        }
    });

    // Remove engine state belonging to deleted alerts.
    const liveIds = new Set(savedAlerts.map(alert => alert.id));
    Object.keys(alertEngineState).forEach(id => {
        if (!liveIds.has(id)) {
            delete alertEngineState[id];
            stateChanged = true;
        }
    });

    if (stateChanged) saveAlertEngineState();
}

function getAlertEngineEvents() {
    return alertEngineEvents.slice();
}


// ============================================================
// ALERTS — STAGE 4: in-site + desktop + Telegram delivery
// ============================================================
const ALERT_NOTIFICATION_STORAGE = "cryptoScreenerAlertNotifications";
const ALERT_SOUND_STORAGE = "cryptoScreenerAlertMuted";
const ALERT_NOTIFICATION_LIMIT = 100;
let alertNotifications = loadAlertNotifications();
let alertMuted = loadAlertMuted();

const alertNotificationPanel = document.getElementById("alertNotificationPanel");
const alertNotificationList = document.getElementById("alertNotificationList");
const alertNotificationUnread = document.getElementById("alertNotificationUnread");
const alertNotificationFooterText = document.getElementById("alertNotificationFooterText");
const alertNotificationCreate = document.getElementById("alertNotificationCreate");
const alertNotificationMarkAll = document.getElementById("alertNotificationMarkAll");
const alertNotificationClear = document.getElementById("alertNotificationClear");
const alertNotificationClose = document.getElementById("alertNotificationClose");

function setAlertCenterTab(tab) {
    const next = (!workspaceViews.alerts || tab === "notifications") ? "notifications" : "alerts";
    document.querySelectorAll("[data-alert-center-tab]").forEach(button => button.classList.toggle("active", button.dataset.alertCenterTab === next));
    document.getElementById("alertCenterAlertsView")?.classList.toggle("active", next === "alerts");
    document.getElementById("alertCenterNotificationsView")?.classList.toggle("active", next === "notifications");
    const title = document.querySelector(".alert-notification-title");
    if (title) title.textContent = next === "notifications" ? "Уведомления" : "Алерты";
    if (next === "alerts") renderAlertCenterSavedList();
    if (next === "notifications") renderAlertNotifications();
}

document.querySelectorAll("[data-alert-center-tab]").forEach(button => {
    button.addEventListener("click", () => setAlertCenterTab(button.dataset.alertCenterTab));
});

function loadAlertNotifications() {
    try {
        const saved = JSON.parse(localStorage.getItem(ALERT_NOTIFICATION_STORAGE) || "[]");
        return Array.isArray(saved) ? saved.slice(0, ALERT_NOTIFICATION_LIMIT) : [];
    } catch { return []; }
}

function saveAlertNotifications() {
    localStorage.setItem(ALERT_NOTIFICATION_STORAGE, JSON.stringify(alertNotifications.slice(0, ALERT_NOTIFICATION_LIMIT)));
}

function loadAlertMuted() {
    try {
        const saved = JSON.parse(localStorage.getItem(ALERT_SOUND_STORAGE) || "{}");
        return saved && typeof saved === "object" ? saved : {};
    } catch { return {}; }
}

function saveAlertMuted() {
    localStorage.setItem(ALERT_SOUND_STORAGE, JSON.stringify(alertMuted));
}

const TELEGRAM_CONNECTIONS_STORAGE = "cryptoScreenerTelegramConnections";
const NOTIFICATION_ROUTING_STORAGE = "cryptoScreenerNotificationRouting";
const TELEGRAM_CLOUDFLARE_WORKER_URL = "https://crypto-alert-telegram.johnjohnson763163.workers.dev/";
const TELEGRAM_CLOUDFLARE_DESTINATION_ID = "cloudflare_worker";
const SIGNAL_CLOUDFLARE_SYNC_ENABLED = false;

function loadTelegramConnections() {
    try {
        const saved = JSON.parse(localStorage.getItem(TELEGRAM_CONNECTIONS_STORAGE) || "[]");
        if (!Array.isArray(saved)) return [];
        return saved.filter(x => x && typeof x === "object" && x.id && x.token && x.chatId).map(x => ({
            id: String(x.id), name: String(x.name || "Telegram"), token: String(x.token), chatId: String(x.chatId)
        }));
    } catch { return []; }
}
let telegramConnections = loadTelegramConnections();
(function migrateLegacyTelegramSettings(){
    try {
        const legacy=JSON.parse(localStorage.getItem("cryptoScreenerTelegramSettings") || "null");
        if(!telegramConnections.length && legacy?.token && legacy?.chatId){
            telegramConnections=[{id:"tg_legacy",name:"Telegram",token:String(legacy.token),chatId:String(legacy.chatId)}];
            saveTelegramConnections();
        }
    } catch {}
})();
function saveTelegramConnections() { localStorage.setItem(TELEGRAM_CONNECTIONS_STORAGE, JSON.stringify(telegramConnections)); }
function makeTelegramConnectionId() { return "tg_" + Date.now().toString(36) + Math.random().toString(36).slice(2, 7); }
function telegramConnectionById(id) { return telegramConnections.find(x => String(x.id) === String(id)) || null; }

function loadNotificationRouting() {
    const defaults = { listing: { telegram:false, telegramDestinationId:"" }, signal: { telegram:true, telegramDestinationId:TELEGRAM_CLOUDFLARE_DESTINATION_ID }, alerts:{} };
    try {
        const saved = JSON.parse(localStorage.getItem(NOTIFICATION_ROUTING_STORAGE) || "null");
        if (!saved || typeof saved !== "object") return defaults;
        return {
            listing: { telegram: !!saved.listing?.telegram, telegramDestinationId: String(saved.listing?.telegramDestinationId || "") },
            signal: { telegram: saved.signal && typeof saved.signal.telegram === "boolean" ? !!saved.signal.telegram : true, telegramDestinationId: String(saved.signal?.telegramDestinationId || TELEGRAM_CLOUDFLARE_DESTINATION_ID) },
            alerts: saved.alerts && typeof saved.alerts === "object" ? saved.alerts : {}
        };
    } catch { return defaults; }
}
let notificationRouting = loadNotificationRouting();
function saveNotificationRouting() { localStorage.setItem(NOTIFICATION_ROUTING_STORAGE, JSON.stringify(notificationRouting)); }
function normalizeNotificationRoute(route) { return { telegram: !!route?.telegram, telegramDestinationId: String(route?.telegramDestinationId || "") }; }
function routeForAlert(alert) {
    if (!alert?.id) return { telegram:false, telegramDestinationId:"" };
    const saved = notificationRouting.alerts[alert.id];
    if (saved) {
        const route=normalizeNotificationRoute(saved);
        if (route.telegram && !route.telegramDestinationId) route.telegramDestinationId = telegramConnections.length === 1 ? telegramConnections[0].id : TELEGRAM_CLOUDFLARE_DESTINATION_ID;
        return route;
    }
    const route=normalizeNotificationRoute({ telegram: !!alert.channels?.telegram, telegramDestinationId: alert.channels?.telegramDestinationId || "" });
    if (route.telegram && !route.telegramDestinationId) route.telegramDestinationId = telegramConnections.length === 1 ? telegramConnections[0].id : TELEGRAM_CLOUDFLARE_DESTINATION_ID;
    return route;
}
function telegramOptions(selected) {
    const selectedId = String(selected || "");
    const cloudflareSelected = selectedId === TELEGRAM_CLOUDFLARE_DESTINATION_ID || (!selectedId && !telegramConnections.length);
    const cloudflare = `<option value="${TELEGRAM_CLOUDFLARE_DESTINATION_ID}"${cloudflareSelected ? " selected" : ""}>Cloudflare → Telegram</option>`;
    if (!telegramConnections.length) return cloudflare;
    return `<option value="">Не выбрано</option>${cloudflare}` + telegramConnections.map(x => `<option value="${escapeHtml(x.id)}"${selectedId===String(x.id)?" selected":""}>${escapeHtml(x.name)}</option>`).join("");
}
function renderTelegramConnections() {
    const box=document.getElementById("telegramConnectionsList"); if(!box) return;
    if(!telegramConnections.length){ box.innerHTML='<div class="notification-route-empty">Telegram-подключения ещё не добавлены.</div>'; return; }
    box.innerHTML=telegramConnections.map(x=>`<div class="telegram-connection-row">
        <div class="telegram-connection-head"><div><div class="telegram-connection-name">${escapeHtml(x.name)}</div><div class="telegram-connection-meta">Chat ID: ${escapeHtml(x.chatId)} · Bot Token: ••••••••</div></div></div>
        <div class="telegram-connection-actions"><button type="button" class="telegram-small-button" data-telegram-test="${escapeHtml(x.id)}">Проверить</button><button type="button" class="telegram-small-button" data-telegram-delete="${escapeHtml(x.id)}">Удалить</button><span class="telegram-connection-meta" data-telegram-status="${escapeHtml(x.id)}"></span></div>
    </div>`).join("");
}
function renderNotificationRouting() {
    const box=document.getElementById("notificationRoutingList"); if(!box) return;
    const rows=[
        ["listing","Листинги","Новые монеты/листинги"],
        ["signal","Сигнальные уровни","Пересечение уровня ценой"]
    ];
    let html=rows.map(([kind,name,meta])=>{
        const baseRoute=normalizeNotificationRoute(notificationRouting[kind]);
        const r=kind === "signal" && !baseRoute.telegramDestinationId ? {telegram:true, telegramDestinationId:TELEGRAM_CLOUDFLARE_DESTINATION_ID} : baseRoute;
        if(kind === "signal"){ notificationRouting.signal.telegram=r.telegram; notificationRouting.signal.telegramDestinationId=r.telegramDestinationId; }
        return `<div class="notification-route-row"><div class="notification-route-head"><div><div class="notification-route-name">${name}</div><div class="notification-route-meta">${meta}</div></div></div><div class="notification-route-controls"><label class="notification-route-toggle"><input type="checkbox" data-route-kind="${kind}" data-route-field="telegram"${r.telegram?" checked":""}> Отправлять в Telegram</label><select data-route-kind="${kind}" data-route-field="telegramDestinationId">${telegramOptions(r.telegramDestinationId)}</select></div></div>`;
    }).join("");
    html += `<div class="notification-route-row"><div class="notification-route-head"><div><div class="notification-route-name">Кастомные алерты</div><div class="notification-route-meta">Настройка Telegram отдельно для каждого сохранённого алерта</div></div></div><div style="margin-top:8px">${savedAlerts.length ? savedAlerts.map(alert=>{const r=routeForAlert(alert); return `<div class="notification-route-controls" style="justify-content:space-between;border-top:1px solid var(--border);padding-top:8px"><span class="notification-route-name">${escapeHtml(alert.name||"Без названия")}</span><label class="notification-route-toggle"><input type="checkbox" data-alert-route-id="${escapeHtml(alert.id)}" data-alert-route-field="telegram"${r.telegram?" checked":""}> Telegram</label><select data-alert-route-id="${escapeHtml(alert.id)}" data-alert-route-field="telegramDestinationId">${telegramOptions(r.telegramDestinationId)}</select></div>`;}).join("") : '<div class="notification-route-empty">Кастомных алертов пока нет.</div>'}</div></div>`;
    box.innerHTML=html;
}
function renderAlertTelegramDestination() {
    const select=document.getElementById("alertTelegramDestination"); if(!select || !alertDraft) return;
    const effectiveDestination = String(alertDraft.channels.telegramDestinationId || (telegramConnections.length ? "" : TELEGRAM_CLOUDFLARE_DESTINATION_ID));
    select.innerHTML=telegramOptions(effectiveDestination);
    select.value=effectiveDestination;
    select.disabled=false;
    const status=document.getElementById("alertTelegramStatus");
    if(status && select.value===TELEGRAM_CLOUDFLARE_DESTINATION_ID) status.textContent="Cloudflare → Telegram";
}
function syncAlertTelegramUI() {
    const box=document.getElementById("alertTelegramSettings"); const enabled=!!document.getElementById("alertChannelTelegram")?.checked;
    if(box) box.style.display=enabled ? "block" : "none";
    renderAlertTelegramDestination();
}
function captureAlertTelegramUI() {
    if(!alertDraft) return;
    alertDraft.channels.telegramDestinationId=String(document.getElementById("alertTelegramDestination")?.value || "");
}

function requestDesktopAlertPermission() {
    if(!("Notification" in window)) return Promise.resolve("unsupported");
    if(Notification.permission !== "default") return Promise.resolve(Notification.permission);
    return Notification.requestPermission();
}

function alertUnreadCount() {
    return alertNotifications.length;
}

function updateAlertNotificationBadge() {
    const unread = alertUnreadCount();
    if (alertNotificationUnread) {
        alertNotificationUnread.textContent = String(unread);
        alertNotificationUnread.style.display = unread ? "block" : "none";
    }
    if (alertCountBadge) {
        alertCountBadge.textContent = String(unread);
        alertCountBadge.style.display = unread ? "block" : "none";
    }
    if (alertNotificationFooterText) {
        alertNotificationFooterText.textContent = `Непрочитанных: ${unread}`;
    }
}

function formatAlertNotificationTime(value) {
    const date = new Date(value);
    if (!Number.isFinite(date.getTime())) return "—";
    return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function formatAlertNotificationValue(key, value) {
    const meta = key === "level" ? { label: "Уровень" } : ({
        volume_base_candles: { label: "Среднее" },
        volume_growth_candles: { label: "Период всплеска" }
    }[key] || MARKET_COLUMN_META[key] || { label: key });
    let formatted = "";
    const number = Number(value);
    if (!Number.isFinite(number)) return null;
    if (key === "change_pct" || key === "natr" || key === "volume_spike" || key === "spread_pct" || key === "oi_change_pct") {
        formatted = `${number > 0 ? "+" : ""}${number.toFixed(2)}%`;
    } else if (key === "volume_expansion_x") {
        formatted = `${number.toFixed(2)}x`;
    } else if (key === "volume_base_candles" || key === "volume_growth_candles") {
        formatted = `${Math.round(number)} свечей`;
    } else if (key === "level" || key === "price") {
        formatted = formatPrice(number);
    } else if (key === "volume_24h") {
        formatted = fmtCompact(number);
    } else if (key === "trades") {
        formatted = fmtCompact(number);
    } else if (key === "btc_corr") {
        formatted = number.toFixed(2);
    } else if (key === "price") {
        formatted = formatPrice(number);
    } else {
        formatted = String(value);
    }
    return { label: meta.label || key, value: formatted };
}

function alertNotificationValuesHtml(item) {
    const entries = Object.entries(item.values || {});
    if (!entries.length) return "";
    return `<div class="alert-notification-values">${entries.map(([key, value]) => {
        const formatted = formatAlertNotificationValue(key, value);
        if (!formatted) return "";
        return `<span class="alert-notification-value"><b>${escapeHtml(formatted.label)}:</b> ${escapeHtml(formatted.value)}</span>`;
    }).join("")}</div>`;
}

function displayAlertSymbol(symbol) {
    const value = String(symbol || "—").toUpperCase();
    return value.endsWith("USDT") ? value.slice(0, -4) : value;
}

function notificationTitle(item) {
    if (item?.kind === "signal") {
        const level = formatAlertNotificationValue("level", item?.values?.level);
        return level ? `Пересечение уровня · ${level.value}` : "Пересечение уровня";
    }
    return item?.alertName || "Уведомление";
}

function renderAlertNotifications() {
    if (!alertNotificationList) return;
    if (!alertNotifications.length) {
        alertNotificationList.innerHTML = `<div class="alert-notification-empty">Новых уведомлений пока нет.</div>`;
        updateAlertNotificationBadge();
        return;
    }
    // В центре показываем только непрочитанные уведомления.
    // Новейшее всегда сверху: последнее пришедшее — первое в списке.
    alertNotificationList.innerHTML = alertNotifications.map(item => {
        return `<div class="alert-notification-item unread" data-alert-notification-id="${escapeHtml(item.id)}" data-symbol="${escapeHtml(item.symbol)}">
            <div class="alert-notification-item-title">${escapeHtml(notificationTitle(item))}</div>
            <div class="alert-notification-item-meta">
                <span class="alert-notification-item-symbol">${escapeHtml(displayAlertSymbol(item.symbol))}</span>
                <span>${escapeHtml(item.market || "—")}</span>
                <span>${escapeHtml(formatAlertNotificationTime(item.time))}</span>
            </div>
        </div>`;
    }).join("");
    updateAlertNotificationBadge();
}

function toggleAlertNotificationPanel(force) {
    if (!alertNotificationPanel) return;
    const next = typeof force === "boolean" ? force : !alertNotificationPanel.classList.contains("open");
    alertNotificationPanel.classList.toggle("open", next);
    alertNotificationPanel.setAttribute("aria-hidden", next ? "false" : "true");
    if (next) { renderAlertCenterSavedList(); renderAlertNotifications(); }
}

function markAlertNotificationRead(id) {
    const index = alertNotifications.findIndex(entry => entry.id === id);
    if (index < 0) return;
    // Нажатие означает прочтение: уведомление сразу удаляется из непрочитанных.
    alertNotifications.splice(index, 1);
    saveAlertNotifications();
    renderAlertNotifications();
    updateAlertToastUnreadLabels();
}

function markAllAlertNotificationsRead() {
    if (!alertNotifications.length) return;
    // Все текущие уведомления считаются прочитанными и исчезают из очереди.
    alertNotifications = [];
    saveAlertNotifications();
    renderAlertNotifications();
    updateAlertToastUnreadLabels();
}

function clearAlertNotifications() {
    alertNotifications = [];
    saveAlertNotifications();
    renderAlertNotifications();
    updateAlertToastUnreadLabels();
}


const ALERT_SOUND_SETTINGS_STORAGE = "cryptoScreenerAlertSoundSettingsV2";
const ALERT_SOUND_DB = "cryptoScreenerAudioLibraryV2";
const ALERT_SOUND_DB_VERSION = 1;
const BUILTIN_SOUNDS = {
    "builtin:terminal": { name: "Terminal — резкий двойной", meta: "короткий бодрый сигнал" },
    "builtin:beep": { name: "Terminal — короткий beep", meta: "плотный одиночный" },
    "builtin:up": { name: "Signal — вверх", meta: "двойной rising" },
    "builtin:down": { name: "Signal — вниз", meta: "двойной falling" },
    "builtin:alarm": { name: "Alert — тревога", meta: "тройной акцент" },
    "builtin:tick": { name: "Tick — торговый", meta: "сухой терминальный клик" },
    "builtin:ping": { name: "Ping — яркий", meta: "высокий короткий ping" },
    "builtin:triple": { name: "Triple — триггер", meta: "тройной импульс" }
};
let alertSoundSettings = loadAlertSoundSettingsV2();
(function migrateLegacySoundSettings() {
    try {
        const legacy = JSON.parse(localStorage.getItem("cryptoScreenerAlertSoundSettings") || "null");
        if (!legacy || typeof legacy !== "object") return;
        const convert = value => {
            const number = Number(value?.sound);
            const ids = Object.keys(BUILTIN_SOUNDS);
            return ids[Math.max(0, Math.min(ids.length - 1, (Number.isFinite(number) ? number : 1) - 1))] || "builtin:terminal";
        };
        if (legacy.listing) alertSoundSettings.listing = { soundId: convert(legacy.listing), volume: Number(legacy.listing.volume ?? 120) };
        if (legacy.defaultAlert) alertSoundSettings.defaultAlert = { soundId: convert(legacy.defaultAlert), volume: Number(legacy.defaultAlert.volume ?? 120) };
        if (legacy.alerts && typeof legacy.alerts === "object") Object.entries(legacy.alerts).forEach(([id, value]) => {
            alertSoundSettings.alerts[id] = { soundId: convert(value), volume: Number(value?.volume ?? 120) };
        });
        saveAlertSoundSettingsV2();
    } catch (_) {}
}
)();

let alertAudioContext = null;
let alertAudioMaster = null;
let customSoundRecords = [];
let customSoundBuffers = new Map();
let soundDbPromise = null;

function loadAlertSoundSettingsV2() {
    const defaults = { listing: { soundId: "builtin:terminal", volume: 120 }, signal: { soundId: "builtin:up", volume: 120 }, defaultAlert: { soundId: "builtin:terminal", volume: 120 }, alerts: {} };
    try {
        const saved = JSON.parse(localStorage.getItem(ALERT_SOUND_SETTINGS_STORAGE) || "null");
        if (!saved || typeof saved !== "object") return defaults;
        const result = {
            listing: { soundId: String(saved.listing?.soundId || defaults.listing.soundId), volume: Math.max(0, Math.min(250, Number(saved.listing?.volume ?? 120))) },
            signal: { soundId: String(saved.signal?.soundId || defaults.signal.soundId), volume: Math.max(0, Math.min(250, Number(saved.signal?.volume ?? 120))) },
            defaultAlert: { soundId: String(saved.defaultAlert?.soundId || defaults.defaultAlert.soundId), volume: Math.max(0, Math.min(250, Number(saved.defaultAlert?.volume ?? 120))) },
            alerts: {}
        };
        if (saved.alerts && typeof saved.alerts === "object") {
            Object.entries(saved.alerts).forEach(([id, value]) => {
                if (!value || typeof value !== "object") return;
                result.alerts[id] = { soundId: String(value.soundId || result.defaultAlert.soundId), volume: Math.max(0, Math.min(250, Number(value.volume ?? result.defaultAlert.volume))) };
            });
        }
        return result;
    } catch (_) { return defaults; }
}
function saveAlertSoundSettingsV2() {
    localStorage.setItem(ALERT_SOUND_SETTINGS_STORAGE, JSON.stringify(alertSoundSettings));
}
function alertSoundConfigFor(alertId) {
    return alertSoundSettings.alerts[alertId] || alertSoundSettings.defaultAlert;
}
function ensureAlertSoundConfig(alertId) {
    if (!alertId) return alertSoundSettings.defaultAlert;
    if (!alertSoundSettings.alerts[alertId]) alertSoundSettings.alerts[alertId] = { ...alertSoundSettings.defaultAlert };
    return alertSoundSettings.alerts[alertId];
}
function soundLabel(soundId) {
    if (BUILTIN_SOUNDS[soundId]) return BUILTIN_SOUNDS[soundId].name;
    const custom = customSoundRecords.find(item => item.id === soundId);
    return custom ? `Мой звук — ${custom.name}` : "Terminal — резкий двойной";
}
function soundOptions(selected) {
    const builtins = Object.entries(BUILTIN_SOUNDS).map(([id, info]) => `<option value="${escapeHtml(id)}"${selected === id ? " selected" : ""}>${escapeHtml(info.name)}</option>`).join("");
    const custom = customSoundRecords.map(item => `<option value="${escapeHtml(item.id)}"${selected === item.id ? " selected" : ""}>${escapeHtml(`Мой звук — ${item.name}`)}</option>`).join("");
    return builtins + custom;
}
function ensureAlertAudioContext() {
    const AudioCtx = window.AudioContext || window.webkitAudioContext;
    if (!AudioCtx) return null;
    alertAudioContext = alertAudioContext || new AudioCtx();
    if (!alertAudioMaster) {
        // Do not put the notification signal through the old heavy compressor:
        // it was flattening the difference between roughly 120% and 250%.
        // The volume control now directly determines the output gain.
        alertAudioMaster = alertAudioContext.createGain();
        alertAudioMaster.gain.value = 1.0;
        alertAudioMaster.connect(alertAudioContext.destination);
    }
    if (alertAudioContext.state === "suspended") alertAudioContext.resume().catch(() => {});
    return alertAudioContext;
}
function playBuiltinSound(soundId, volumePercent) {
    const ctx = ensureAlertAudioContext();
    if (!ctx || !alertAudioMaster) return;
    const master = Math.max(0, Math.min(2.5, Number(volumePercent) / 100));
    const now = ctx.currentTime;
    const patterns = {
        "builtin:terminal": [[920,.055,"square",0],[1240,.075,"triangle",.075],[920,.045,"square",.165]],
        "builtin:beep": [[880,.13,"square",0]],
        "builtin:up": [[620,.07,"triangle",0],[980,.09,"triangle",.085],[1320,.13,"sine",.19]],
        "builtin:down": [[1320,.07,"triangle",0],[980,.09,"triangle",.085],[620,.13,"sine",.19]],
        "builtin:alarm": [[760,.07,"square",0],[760,.07,"square",.12],[1040,.12,"square",.24]],
        "builtin:tick": [[520,.045,"square",0],[760,.035,"square",.055]],
        "builtin:ping": [[1180,.16,"sine",0]],
        "builtin:triple": [[720,.055,"square",0],[900,.055,"square",.07],[1180,.12,"triangle",.14]]
    };
    const pattern = patterns[soundId] || patterns["builtin:terminal"];
    pattern.forEach(([frequency, duration, type, offset]) => {
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.type = type;
        osc.frequency.setValueAtTime(frequency, now + offset);
        gain.gain.setValueAtTime(0.0001, now + offset);
        gain.gain.exponentialRampToValueAtTime(Math.max(0.0001, 0.72 * master), now + offset + 0.006);
        gain.gain.exponentialRampToValueAtTime(0.0001, now + offset + duration);
        osc.connect(gain);
        gain.connect(alertAudioMaster);
        osc.start(now + offset);
        osc.stop(now + offset + duration + 0.02);
    });
}
async function getCustomSoundBuffer(soundId) {
    if (customSoundBuffers.has(soundId)) return customSoundBuffers.get(soundId);
    const record = customSoundRecords.find(item => item.id === soundId);
    if (!record) return null;
    const ctx = ensureAlertAudioContext();
    if (!ctx) return null;
    try {
        const buffer = await ctx.decodeAudioData(await record.blob.arrayBuffer());
        customSoundBuffers.set(soundId, buffer);
        return buffer;
    } catch (_) { return null; }
}
async function playConfiguredNotificationSound(soundId, volumePercent) {
    if (String(soundId).startsWith("builtin:")) {
        playBuiltinSound(soundId, volumePercent);
        return;
    }
    const ctx = ensureAlertAudioContext();
    const buffer = await getCustomSoundBuffer(soundId);
    if (!ctx || !buffer || !alertAudioMaster) return;
    const source = ctx.createBufferSource();
    const gain = ctx.createGain();
    gain.gain.value = Math.max(0, Math.min(2.5, Number(volumePercent) / 100));
    source.buffer = buffer;
    source.connect(gain);
    gain.connect(alertAudioMaster);
    source.start();
}
function playAlertSound(alertId) {
    if (alertMuted[alertId]) return;
    const config = alertSoundConfigFor(alertId);
    playConfiguredNotificationSound(config.soundId, config.volume);
}
function playSignalLevelSound() {
    playConfiguredNotificationSound(alertSoundSettings.signal.soundId, alertSoundSettings.signal.volume);
}
function playListingSound() {
    playConfiguredNotificationSound(alertSoundSettings.listing.soundId, alertSoundSettings.listing.volume);
}

function openSoundDb() {
    if (soundDbPromise) return soundDbPromise;
    soundDbPromise = new Promise((resolve, reject) => {
        if (!("indexedDB" in window)) { reject(new Error("IndexedDB недоступен")); return; }
        const request = indexedDB.open(ALERT_SOUND_DB, ALERT_SOUND_DB_VERSION);
        request.onupgradeneeded = () => {
            const db = request.result;
            if (!db.objectStoreNames.contains("sounds")) {
                db.createObjectStore("sounds", { keyPath: "id" });
            }
        };
        request.onsuccess = () => {
            const db = request.result;
            db.onversionchange = () => { try { db.close(); } catch (_) {} };
            resolve(db);
        };
        request.onerror = () => reject(request.error || new Error("Ошибка IndexedDB"));
        request.onblocked = () => reject(new Error("Не удалось открыть хранилище звуков: другое окно использует старую версию"));
    });
    return soundDbPromise;
}
function setNotificationSoundStatus(message, isError = false) {
    const hint = document.querySelector("#notificationSoundsSection .notification-sound-hint");
    if (!hint) return;
    hint.textContent = message;
    hint.style.color = isError ? "var(--red, #ff6b6b)" : "";
}
async function loadCustomSounds() {
    try {
        const db = await openSoundDb();
        customSoundRecords = await new Promise((resolve, reject) => {
            const request = db.transaction("sounds", "readonly").objectStore("sounds").getAll();
            request.onsuccess = () => resolve(request.result || []);
            request.onerror = () => reject(request.error || new Error("Не удалось прочитать звуки"));
        });
        renderNotificationSoundSettings();
        populateAlertSoundSelect();
    } catch (error) {
        customSoundRecords = [];
        soundDbPromise = null;
        renderNotificationSoundSettings();
        setNotificationSoundStatus(`Не удалось открыть хранилище звуков: ${error?.message || "неизвестная ошибка"}`, true);
    }
}
async function saveCustomSound(file) {
    const type = String(file?.type || "").toLowerCase();
    const name = String(file?.name || "").toLowerCase();
    const isAudio = type.startsWith("audio/") || /\.(wav|mp3|ogg|oga|m4a|aac|flac)$/i.test(name);
    if (!file || !isAudio) {
        setNotificationSoundStatus("Выберите аудиофайл WAV, MP3, OGG, M4A, AAC или FLAC.", true);
        return;
    }
    try {
        const db = await openSoundDb();
        const blob = file.slice(0, file.size, file.type || "application/octet-stream");
        const record = {
            id: `custom:${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
            name: String(file.name || "Мой звук").slice(0,80),
            type: file.type || "audio/*",
            blob
        };
        await new Promise((resolve, reject) => {
            const request = db.transaction("sounds", "readwrite").objectStore("sounds").put(record);
            request.onsuccess = resolve;
            request.onerror = () => reject(request.error || new Error("Не удалось сохранить файл"));
        });
        customSoundRecords = customSoundRecords.filter(item => item.id !== record.id);
        customSoundRecords.push(record);
        renderNotificationSoundSettings();
        populateAlertSoundSelect();
        setNotificationSoundStatus(`Сохранён: ${record.name}`);
    } catch (error) {
        soundDbPromise = null;
        setNotificationSoundStatus(`Не удалось сохранить звук: ${error?.message || "неизвестная ошибка"}`, true);
    }
}
async function deleteCustomSound(soundId) {
    const id = String(soundId || "");
    if (!id || !id.startsWith("custom:")) return;
    const record = customSoundRecords.find(item => item.id === id);
    if (!record) return;
    try {
        const db = await openSoundDb();
        await new Promise((resolve, reject) => {
            const request = db.transaction("sounds", "readwrite").objectStore("sounds").delete(id);
            request.onsuccess = resolve;
            request.onerror = () => reject(request.error || new Error("Не удалось удалить звук"));
        });

        customSoundRecords = customSoundRecords.filter(item => item.id !== id);
        customSoundBuffers.delete(id);

        let settingsChanged = false;
        const fallback = {
            listing: "builtin:terminal",
            signal: "builtin:up",
            defaultAlert: "builtin:terminal"
        };
        Object.entries(fallback).forEach(([key, fallbackId]) => {
            if (alertSoundSettings[key]?.soundId === id) {
                alertSoundSettings[key].soundId = fallbackId;
                settingsChanged = true;
            }
        });
        Object.values(alertSoundSettings.alerts || {}).forEach(config => {
            if (config?.soundId === id) {
                config.soundId = "builtin:terminal";
                settingsChanged = true;
            }
        });
        if (settingsChanged) saveAlertSoundSettingsV2();

        if (alertDraft?.soundId === id) {
            alertDraft.soundId = alertSoundSettings.defaultAlert.soundId;
            populateAlertSoundSelect();
        }
        renderNotificationSoundSettings();
        populateAlertSoundSelect();
        setNotificationSoundStatus(`Удалён: ${record.name}`);
    } catch (error) {
        setNotificationSoundStatus(`Не удалось удалить звук: ${error?.message || "неизвестная ошибка"}`, true);
    }
}
function renderNotificationSoundRow(name, meta, kind, id, config, customDeleteId = "") {
    const deleteButton = customDeleteId
        ? `<button type="button" class="notification-sound-button" data-notification-sound-delete="${escapeHtml(customDeleteId)}" title="Удалить свой звук">Удалить</button>`
        : "";
    return `<div class="notification-sound-row"><div class="notification-sound-row-head"><div><div class="notification-sound-name">${escapeHtml(name)}</div><div class="notification-sound-meta">${escapeHtml(meta)}</div></div>${deleteButton}</div><div class="notification-sound-controls"><select class="notification-sound-select" data-notification-sound-kind="${kind}" data-notification-sound-id="${escapeHtml(id || "")}">${soundOptions(config.soundId)}</select><button type="button" class="notification-sound-button" data-notification-sound-test-kind="${kind}" data-notification-sound-test-id="${escapeHtml(id || "")}">▶</button><label class="notification-volume"><span>Громкость</span><input type="range" min="0" max="250" step="1" value="${Math.round(config.volume)}" data-notification-volume-kind="${kind}" data-notification-volume-id="${escapeHtml(id || "")}"><strong>${Math.round(config.volume)}%</strong></label></div></div>`;
}
function renderNotificationSoundSettings() {
    const box = document.getElementById("notificationSoundsList");
    if (!box) return;
    let html = renderNotificationSoundRow("Листинги монет", "Звук новых листингов", "listing", "", alertSoundSettings.listing);
    html += renderNotificationSoundRow("Сигнальный уровень", "Срабатывает один раз при пересечении уровня ценой", "signal", "", alertSoundSettings.signal);
    html += renderNotificationSoundRow("Алерты по умолчанию", "Если для алерта не выбран отдельный звук", "alert-default", "", alertSoundSettings.defaultAlert);
    (Array.isArray(savedAlerts) ? savedAlerts : []).forEach(alert => {
        html += renderNotificationSoundRow(alert.name || "Алерт", `${alertMarketLabel(alert.market)} · ${alert.active === false ? "выключен" : "активен"}`, "alert", alert.id, alertSoundConfigFor(alert.id));
    });
    customSoundRecords.forEach(item => {
        html += renderNotificationSoundRow(
            `Мой звук — ${item.name}`,
            `${item.type || "audio"} · сохранён локально`,
            "custom",
            item.id,
            { soundId: item.id, volume: 120 },
            item.id
        );
    });
    box.innerHTML = html;
}
function setNotificationSound(kind, id, soundId) {
    const config = kind === "listing" ? alertSoundSettings.listing : kind === "signal" ? alertSoundSettings.signal : kind === "alert-default" ? alertSoundSettings.defaultAlert : ensureAlertSoundConfig(id);
    config.soundId = soundId;
    saveAlertSoundSettingsV2();
    renderNotificationSoundSettings();
    if (kind === "alert") syncAlertDraftSoundFromSettings(id);
    playConfiguredNotificationSound(config.soundId, config.volume);
}
function setNotificationSoundVolume(kind, id, value) {
    const config = kind === "listing" ? alertSoundSettings.listing : kind === "signal" ? alertSoundSettings.signal : kind === "alert-default" ? alertSoundSettings.defaultAlert : ensureAlertSoundConfig(id);
    config.volume = Math.max(0, Math.min(250, Number(value) || 0));
    saveAlertSoundSettingsV2();
    const strong = Array.from(document.querySelectorAll("[data-notification-volume-kind]")).find(el => el.dataset.notificationVolumeKind === kind && (el.dataset.notificationVolumeId || "") === (id || ""))?.parentElement?.querySelector("strong");
    if (strong) strong.textContent = `${Math.round(config.volume)}%`;
}
document.getElementById("notificationSoundsList")?.addEventListener("change", event => {
    const select = event.target.closest("[data-notification-sound-kind]");
    if (select) setNotificationSound(select.dataset.notificationSoundKind, select.dataset.notificationSoundId || "", select.value);
});
document.getElementById("notificationSoundsList")?.addEventListener("input", event => {
    const input = event.target.closest("[data-notification-volume-kind]");
    if (input) setNotificationSoundVolume(input.dataset.notificationVolumeKind, input.dataset.notificationVolumeId || "", input.value);
});
document.getElementById("notificationSoundsList")?.addEventListener("click", event => {
    const remove = event.target.closest("[data-notification-sound-delete]");
    if (remove) {
        event.preventDefault();
        event.stopPropagation();
        deleteCustomSound(remove.dataset.notificationSoundDelete || "");
        return;
    }
    const test = event.target.closest("[data-notification-sound-test-kind]");
    if (!test) return;
    const kind = test.dataset.notificationSoundTestKind;
    const id = test.dataset.notificationSoundTestId || "";
    if (kind === "custom") {
        playConfiguredNotificationSound(id, 120);
        return;
    }
    const config = kind === "listing" ? alertSoundSettings.listing : kind === "signal" ? alertSoundSettings.signal : kind === "alert-default" ? alertSoundSettings.defaultAlert : ensureAlertSoundConfig(id);
    playConfiguredNotificationSound(config.soundId, config.volume);
});
document.getElementById("notificationSoundUploadButton")?.addEventListener("click", () => document.getElementById("notificationSoundFile")?.click());
document.getElementById("notificationSoundFile")?.addEventListener("change", event => {
    const file = event.target.files?.[0];
    if (file) saveCustomSound(file);
    event.target.value = "";
});
function populateAlertSoundSelect() {
    const select = document.getElementById("alertSoundSelect");
    if (!select || !alertDraft) return;
    select.innerHTML = soundOptions(alertDraft.soundId);
    select.value = alertDraft.soundId;
}
function syncAlertDraftSoundFromSettings(alertId) {
    if (alertDraft && alertEditingId === alertId) {
        alertDraft.soundId = alertSoundConfigFor(alertId).soundId;
        populateAlertSoundSelect();
    }
}
document.getElementById("alertSoundSelect")?.addEventListener("change", event => {
    if (!alertDraft) return;
    alertDraft.soundId = event.target.value;
    if (alertEditingId) {
        const config = ensureAlertSoundConfig(alertEditingId);
        config.soundId = alertDraft.soundId;
        saveAlertSoundSettingsV2();
    }
});
document.getElementById("alertSoundTest")?.addEventListener("click", () => {
    if (!alertDraft) return;
    playConfiguredNotificationSound(alertDraft.soundId, alertEditingId ? alertSoundConfigFor(alertEditingId).volume : alertSoundSettings.defaultAlert.volume);
});
loadCustomSounds();


function signalLevelExternalText(event) {
    const price = Number(event?.values?.price ?? event?.price);
    const priceText = Number.isFinite(price)
        ? price.toLocaleString("en-US", {minimumFractionDigits:2, maximumFractionDigits:2}).replace(/,/g, " ")
        : "—";
    const direction = event?.direction === "up" ? "↑ Пересечение вверх" : "↓ Пересечение вниз";
    const time = formatSignalLevelTime(event?.time);
    return [
        "🔔 Сигнальный уровень",
        "",
        `Монета: ${String(event?.symbol || "—").toUpperCase()}`,
        "Рынок: Binance Futures",
        `Цена: ${priceText} USDT`,
        `Направление: ${direction}`,
        `Время: ${time}`
    ].join("\n");
}
function formatSignalLevelTime(value) {
    const date = new Date(value);
    if (!Number.isFinite(date.getTime())) return "—";
    return date.toLocaleTimeString([], {hour:"2-digit", minute:"2-digit", second:"2-digit"});
}
function alertExternalText(event) {
    if (event?.kind === "signal") return signalLevelExternalText(event);
    const lines=[`🔔 ${event.alertName || "Алерт"}`, `${event.symbol || "—"}`, `Рынок: ${event.market || "—"}`];
    Object.entries(event.values || {}).forEach(([key,value])=>{ const f=formatAlertNotificationValue(key,value); if(f) lines.push(`${f.label}: ${f.value}`); });
    return lines.join("\n");
}
function sendAlertTelegram(event, destinationId) {
    const text=alertExternalText(event);
    if(String(destinationId) === TELEGRAM_CLOUDFLARE_DESTINATION_ID){
        fetch("/api/telegram/cloudflare",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({text}),keepalive:true})
            .then(r=>r.json().catch(()=>({}))).then(result=>{if(!result.ok) console.warn("Cloudflare Telegram alert failed:",result.error||"unknown error");}).catch(error=>console.warn("Cloudflare Telegram alert failed:",error));
        return;
    }
    const connection=telegramConnectionById(destinationId);
    if(!connection) return;
    fetch("/api/telegram/send",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({token:connection.token,chat_id:connection.chatId,text})})
        .then(r=>r.json().catch(()=>({}))).then(result=>{if(!result.ok) console.warn("Telegram alert failed:",result.error||"unknown error");}).catch(error=>console.warn("Telegram alert failed:",error));
}
function sendNotificationToTelegram(event, destinationId) { sendAlertTelegram(event, destinationId); }
function showDesktopAlert(event) {
    if(!("Notification" in window) || Notification.permission !== "granted") return;
    const body=[event.symbol || "—", event.market || "—"];
    Object.entries(event.values || {}).slice(0,4).forEach(([key,value])=>{const f=formatAlertNotificationValue(key,value); if(f) body.push(`${f.label}: ${f.value}`);});
    try { const n=new Notification(event.alertName || "Алерт",{body:body.join(" · "),tag:`crypto-screener-alert-${event.alertId || "default"}`,renotify:true}); n.onclick=()=>{window.focus();copyAlertTicker(event.symbol);if(event.symbol)openChart(event.symbol);n.close();}; } catch(_) {}
}
function deliverAlertExternal(event) {
    const alert=savedAlerts.find(item=>item.id===event.alertId); if(!alert) return;
    if(alert.channels?.desktop) showDesktopAlert(event);
    const route=routeForAlert(alert);
    if(route.telegram) sendNotificationToTelegram(event, route.telegramDestinationId);
}
function deliverSignalTelegram(event) {
    // Signal-level Telegram delivery is now server-side in Cloudflare.
    // The browser keeps the local notification/toast, but does not send a
    // second Telegram message while the server monitor is enabled.
    if (typeof SIGNAL_CLOUDFLARE_SYNC_ENABLED !== "undefined" && SIGNAL_CLOUDFLARE_SYNC_ENABLED) return;
    const route=normalizeNotificationRoute(notificationRouting.signal);
    if(route.telegram) sendNotificationToTelegram(event, route.telegramDestinationId);
}
function deliverListingTelegram(event) {
    const route=normalizeNotificationRoute(notificationRouting.listing);
    if(route.telegram) sendNotificationToTelegram(event, route.telegramDestinationId);
}

let alertToastCollapseTimer = null;
const ALERT_TOAST_MAX_VISIBLE = 3;
const ALERT_TOAST_BURST_MS = 3000;

function updateAlertToastUnreadLabels() {
    const stack = document.getElementById("alertToastStack");
    if (!stack) return;
    const unread = alertUnreadCount();
    stack.querySelectorAll(".alert-toast-unread").forEach(node => {
        node.textContent = `Непрочитанных: ${unread}`;
    });
}

function collapseAlertToasts() {
    const stack = document.getElementById("alertToastStack");
    if (!stack) return;
    alertToastCollapseTimer = null;
    stack.querySelectorAll(".alert-toast").forEach(toast => toast.remove());
    const latest = alertNotifications[0];
    if (!latest) return;
    showAlertToast(latest, true);
}

function scheduleAlertToastCollapse() {
    if (alertToastCollapseTimer) clearTimeout(alertToastCollapseTimer);
    alertToastCollapseTimer = setTimeout(collapseAlertToasts, ALERT_TOAST_BURST_MS);
}

function showAlertToast(event, collapsed = false) {
    const stack = document.getElementById("alertToastStack");
    if (!stack || !event) return;
    if (alertToastCollapseTimer) {
        clearTimeout(alertToastCollapseTimer);
        alertToastCollapseTimer = null;
    }
    stack.querySelector(".alert-toast-summary")?.remove();

    const muted = !!alertMuted[event.alertId];
    const priceValue = event.values?.price ?? event.price;
    const formattedPrice = Number.isFinite(Number(priceValue)) ? formatPrice(Number(priceValue)) : "";
    const title = event.alertName || (event.kind === "signal" ? "Пересечение уровня" : "Уведомление");
    const symbol = displayAlertSymbol(event.symbol);
    const toast = document.createElement("div");
    toast.className = `alert-toast${event.kind === "signal" ? " signal" : ""}`;
    toast.innerHTML = `
        <div class="alert-toast-head">
            <span class="alert-toast-name">${escapeHtml(title)}</span>
            <span class="alert-toast-time">${escapeHtml(formatAlertNotificationTime(event.time))}</span>
            <button type="button" class="alert-toast-close" title="Закрыть">×</button>
        </div>
        <div class="alert-toast-symbol">${escapeHtml(symbol)}</div>
        ${formattedPrice ? `<div class="alert-toast-message">Цена: ${escapeHtml(formattedPrice)}</div>` : ""}
        <div class="alert-toast-market">${escapeHtml(event.market || "Binance Futures")}</div>
        <div class="alert-toast-footer">
            <span class="alert-toast-unread">${alertUnreadCount()}</span>
            <button type="button" class="alert-toast-mute">${muted ? "🔕 Вкл. звук" : "🔔 Выкл. звук"}</button>
        </div>`;
    toast.querySelector(".alert-toast-close")?.addEventListener("click", click => {
        click.stopPropagation();
        toast.remove();
    });
    toast.querySelector(".alert-toast-mute")?.addEventListener("click", click => {
        click.stopPropagation();
        alertMuted[event.alertId] = !alertMuted[event.alertId];
        saveAlertMuted();
        toast.querySelector(".alert-toast-mute").textContent = alertMuted[event.alertId] ? "🔕 Вкл. звук" : "🔔 Выкл. звук";
    });
    toast.addEventListener("click", click => {
        if (click.target.closest("button")) return;
        markAlertNotificationRead(event.id);
        copyAlertTicker(event.symbol);
        if (event.symbol) openChart(event.symbol);
        toast.remove();
        updateAlertToastUnreadLabels();
        if (alertNotifications.length) showAlertToast(alertNotifications[0], true);
    });
    stack.prepend(toast);
    while (stack.querySelectorAll(".alert-toast:not(.alert-toast-summary)").length > ALERT_TOAST_MAX_VISIBLE) {
        const individualToasts = stack.querySelectorAll(".alert-toast:not(.alert-toast-summary)");
        individualToasts[individualToasts.length - 1].remove();
    }
    updateAlertToastUnreadLabels();
    if (!collapsed) scheduleAlertToastCollapse();
}

function addAlertNotification(event) {
    if (!event || !event.id) return;
    const isSignal = event.kind === "signal";
    const alert = isSignal ? null : (event.kind === "listing" ? { channels: { site: true } } : savedAlerts.find(item => item.id === event.alertId));
    if (!isSignal && !alert) return;
    const siteEnabled = isSignal ? true : alert.channels?.site !== false;
    if (siteEnabled) {
        const notification = { ...event, read: false };
        alertNotifications = [notification, ...alertNotifications.filter(item => item.id !== notification.id)].slice(0, ALERT_NOTIFICATION_LIMIT);
        saveAlertNotifications();
        renderAlertNotifications();
        if (!isSignal) playAlertSound(event.alertId);
        showAlertToast(notification);
    }
    if (isSignal) deliverSignalTelegram(event);
    else if (event.kind !== "listing") deliverAlertExternal(event);
}

function copyAlertTicker(symbol) {
    const value = String(symbol || "").toUpperCase();
    if (!value) return Promise.resolve();
    if (navigator.clipboard?.writeText) {
        return navigator.clipboard.writeText(value).catch(() => {});
    }
    return Promise.resolve();
}

alertNotificationList?.addEventListener("click", event => {
    const mute = event.target.closest("[data-alert-mute]");
    if (mute) {
        event.preventDefault();
        event.stopPropagation();
        const alertId = mute.dataset.alertMute;
        alertMuted[alertId] = !alertMuted[alertId];
        saveAlertMuted();
        renderAlertNotifications();
        return;
    }

    const card = event.target.closest("[data-alert-notification-id]");
    if (!card) return;
    const id = card.dataset.alertNotificationId;
    const item = alertNotifications.find(entry => entry.id === id);
    if (!item) return;
    markAlertNotificationRead(id);
    copyAlertTicker(item.symbol);
    if (item.symbol) openChart(item.symbol);
});

alertNotificationCreate?.addEventListener("click", () => {
    toggleAlertNotificationPanel(false);
    alertOpen();
});
alertNotificationMarkAll?.addEventListener("click", markAllAlertNotificationsRead);
alertNotificationClear?.addEventListener("click", clearAlertNotifications);
alertNotificationClose?.addEventListener("click", () => toggleAlertNotificationPanel(false));

document.addEventListener("click", event => {
    if (!alertNotificationPanel?.classList.contains("open")) return;
    if (event.target.closest("#alertNotificationPanel, #alertsButton")) return;
    toggleAlertNotificationPanel(false);
});

window.addEventListener("crypto-screener-alert", event => {
    addAlertNotification(event.detail);
});
window.addEventListener("crypto-screener-signal-level", event => {
    addAlertNotification(event.detail);
});

renderAlertNotifications();
renderAlertCenterSavedList();
renderNotificationSoundSettings();
renderTelegramConnections();
renderNotificationRouting();
applyWorkspaceViews();
setDensityUiRuntime(!!workspaceViews.density);


// Keep the coin selector fresh as Binance symbols arrive, without changing the draft.
const _originalLoadStateForAlerts = loadState;
loadState = async function(...args) {
    const result = await _originalLoadStateForAlerts.apply(this, args);
    if (alertsOverlay?.classList.contains("open") && alertDraft) renderAlertCoins();
    return result;
};

const WATCHLIST_COLORS = [
    "#ef4444", // red
    "#f97316", // orange
    "#facc15", // yellow
    "#22c55e", // green
    "#3b82f6", // blue
    "#8b5cf6", // purple
    "#ec4899"  // pink
];
const WATCHLIST_COLOR_STORAGE = "cryptoScreenerWatchlistColors";
let watchlistColorState = loadWatchlistColorState();
let watchlistOpenPaletteSymbol = null;
let watchlistPendingColor = null;
let watchlistPendingExplicitClear = false;

function loadWatchlistColorState() {
    const empty = { assignments: {}, groupOrder: [] };
    try {
        const saved = JSON.parse(localStorage.getItem(WATCHLIST_COLOR_STORAGE) || "null");
        if (!saved || typeof saved !== "object") return empty;
        const assignments = {};
        if (saved.assignments && typeof saved.assignments === "object") {
            Object.entries(saved.assignments).forEach(([symbol, color]) => {
                const safeSymbol = String(symbol || "").toUpperCase().trim();
                if (safeSymbol && WATCHLIST_COLORS.includes(color)) assignments[safeSymbol] = color;
            });
        }
        const groupOrder = Array.isArray(saved.groupOrder)
            ? saved.groupOrder.filter(color => WATCHLIST_COLORS.includes(color))
            : [];
        Object.values(assignments).forEach(color => {
            if (!groupOrder.includes(color)) groupOrder.push(color);
        });
        return { assignments, groupOrder };
    } catch {
        return empty;
    }
}

function saveWatchlistColorState() {
    localStorage.setItem(WATCHLIST_COLOR_STORAGE, JSON.stringify(watchlistColorState));
}

function watchlistColorFor(symbol) {
    return watchlistColorState.assignments[String(symbol || "").toUpperCase()] || null;
}

function moveWatchlistColorToFront(color) {
    watchlistColorState.groupOrder = [
        color,
        ...watchlistColorState.groupOrder.filter(item => item !== color)
    ];
}

function normalizeWatchlistGroupOrder() {
    const activeColors = new Set(Object.values(watchlistColorState.assignments));
    const ordered = WATCHLIST_COLORS.filter(color => activeColors.has(color));
    watchlistColorState.groupOrder = ordered;
}

function commitWatchlistColor(symbol, color) {
    const safeSymbol = String(symbol || "").toUpperCase().trim();
    if (!safeSymbol) return;

    if (!color) {
        delete watchlistColorState.assignments[safeSymbol];
        watchlistColorState.groupOrder = watchlistColorState.groupOrder.filter(groupColor =>
            Object.values(watchlistColorState.assignments).includes(groupColor)
        );
    } else if (WATCHLIST_COLORS.includes(color)) {
        watchlistColorState.assignments[safeSymbol] = color;
        moveWatchlistColorToFront(color);
    }

    normalizeWatchlistGroupOrder();
    saveWatchlistColorState();
}

function closeWatchlistPalette(commitPending = true) {
    if (watchlistOpenPaletteSymbol && commitPending) {
        // A new/uncolored flag must never remain without a color after
        // leaving the palette. Red is the default fallback.
        const colorToCommit = watchlistPendingExplicitClear
            ? null
            : (watchlistPendingColor || WATCHLIST_COLORS[0]);
        commitWatchlistColor(watchlistOpenPaletteSymbol, colorToCommit);
    }
    watchlistOpenPaletteSymbol = null;
    watchlistPendingColor = null;
    watchlistPendingExplicitClear = false;
    renderTable();
}

function applyWatchlistColorGrouping(rows) {
    const groups = new Map(WATCHLIST_COLORS.map(color => [color, []]));
    const uncolored = [];

    rows.forEach(row => {
        const color = watchlistColorFor(row.symbol);
        if (color && groups.has(color)) groups.get(color).push(row);
        else uncolored.push(row);
    });

    const usedColors = new Set();
    const grouped = [];

    // Stored order is newest color group first.
    watchlistColorState.groupOrder.forEach(color => {
        const items = groups.get(color) || [];
        if (items.length) {
            grouped.push(...items);
            usedColors.add(color);
        }
    });

    // Recover gracefully if storage was created before groupOrder existed.
    WATCHLIST_COLORS.forEach(color => {
        if (usedColors.has(color)) return;
        const items = groups.get(color) || [];
        if (items.length) grouped.push(...items);
    });

    return grouped.concat(uncolored);
}

// Explicit presentation-only classification for stock-like symbols.
// It does not change Binance routing or Market State.
const STOCK_BASE_SYMBOLS = new Set([
    // Major US / global equities already supported by the screener.
    "AAPL", "AMZN", "GOOGL", "GOOG", "MSFT", "META", "NVDA", "TSLA",
    "NFLX", "AMD", "INTC", "AVGO", "QCOM", "ORCL", "CRM", "ADBE",
    "CSCO", "IBM", "UBER", "COIN", "MSTR", "HOOD", "PLTR", "NIO", "BABA",

    // Binance Futures TradFi / equity-linked contracts.
    "SPCX", "SAMSUNG", "SKHYNIX", "SKHY", "HYUNDAI", "SAMSUNGEM",
    "HANMI", "LGELECTRONICS", "NAVER", "KUAISHOU", "MEITUAN", "BYD", "HK0992",
    "ZHONGJI", "LLY", "NVO", "BBX", "NOK", "ASTS", "SHAZ", "SOFI", "PANW",
    "PENG", "BX", "HPE", "AMAT", "CRWD", "CRDO", "AAOI", "AXTI", "DELL",
    "NOW", "IREN", "ONDS", "STRC", "CAT", "TXN", "FLEX", "TER", "TTWO",
    "BSP", "BOT", "WENUS", "INTW", "SNXX", "BNC", "FWDI", "DJT", "MRNA",
    "SKUU", "SKDD", "RAM", "MUU", "SOXS", "TZA", "XBI", "IWM", "EWT",
    "KSTR", "SAMSUNGEM", "KODEX200", "CSOPSKHYNIX2L", "CSOPSAMSUNG2L"
]);

function isStockSymbol(symbol) {
    const safeSymbol = String(symbol || "").toUpperCase();
    const catalogType = instrumentTypeBySymbol.get(safeSymbol);
    if (catalogType) return catalogType === "stock";
    const base = safeSymbol.endsWith("USDT") ? safeSymbol.slice(0, -4) : safeSymbol;
    return STOCK_BASE_SYMBOLS.has(base);
}

function stockIconHtml(symbol) {
    if (!isStockSymbol(symbol)) return "";
    return `<span class="stock-market-icon" title="Акция / TradFi" aria-label="Акция / TradFi">
        <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">
            <path d="M2.25 7.05 8 2.65l5.75 4.4" />
            <path d="M3.55 6.7v6.65h8.9V6.7" />
            <path d="M5.15 13.35V9.4h1.55v3.95M9.3 13.35V9.4h1.55v3.95" />
            <path d="M2.45 13.35h11.1" />
        </svg>
    </span>`;
}

function renderWatchlistSymbolCell(symbol) {
    const safeSymbol = String(symbol || "").toUpperCase();
    const savedColor = watchlistColorFor(safeSymbol);
    const isOpen = watchlistOpenPaletteSymbol === safeSymbol;
    const color = isOpen && watchlistPendingColor ? watchlistPendingColor : savedColor;
    const markerStyle = color ? `--watchlist-flag-color:${color};` : "";
    const markerClass = color ? " has-color" : "";

    const palette = isOpen ? `
        <div class="watchlist-color-palette" data-watchlist-palette="${safeSymbol}">
            ${WATCHLIST_COLORS.map(option => `
                <button type="button" class="watchlist-color-option"
                        data-watchlist-color="${option}" data-symbol="${safeSymbol}"
                        title="${option}" style="background:${option}"></button>
            `).join("")}
            <button type="button" class="watchlist-color-clear"
                    data-watchlist-clear="1" data-symbol="${safeSymbol}"
                    title="Убрать цвет"></button>
        </div>
    ` : "";

    return `<div class="market-symbol-cell" style="position:relative">
        <button type="button" class="watchlist-color-trigger${markerClass}"
                data-watchlist-trigger="1" data-symbol="${safeSymbol}"
                title="Цвет группы" aria-label="Цвет группы" style="${markerStyle}"></button>
        <span class="symbol">${chartDisplaySymbol(safeSymbol)}${stockIconHtml(safeSymbol)}</span>
        ${palette}
    </div>`;
}

function updateWatchlistTriggerVisual(symbol, color) {
    const safeSymbol = String(symbol || "").toUpperCase();
    const trigger = document.querySelector(`[data-watchlist-trigger][data-symbol="${safeSymbol}"]`);
    if (!trigger) return;
    if (color) {
        trigger.classList.add("has-color");
        trigger.style.setProperty("--watchlist-flag-color", color);
    } else {
        trigger.classList.remove("has-color");
        trigger.style.removeProperty("--watchlist-flag-color");
    }
}

function openWatchlistPaletteInPlace(symbol) {
    const safeSymbol = String(symbol || "").toUpperCase();
    const trigger = document.querySelector(`[data-watchlist-trigger][data-symbol="${safeSymbol}"]`);
    if (!trigger) return;
    const cell = trigger.closest(".market-symbol-cell");
    if (!cell) return;

    cell.querySelectorAll("[data-watchlist-palette]").forEach(el => el.remove());
    const palette = document.createElement("div");
    palette.className = "watchlist-color-palette";
    palette.dataset.watchlistPalette = safeSymbol;
    palette.innerHTML = WATCHLIST_COLORS.map(option => `
        <button type="button" class="watchlist-color-option"
                data-watchlist-color="${option}" data-symbol="${safeSymbol}"
                title="${option}" style="background:${option}"></button>
    `).join("") + `
        <button type="button" class="watchlist-color-clear"
                data-watchlist-clear="1" data-symbol="${safeSymbol}"
                title="Убрать цвет"></button>`;
    cell.appendChild(palette);
}

// Watchlist interaction is intentionally handled without renderTable() on the
// first/second flag click. Rebuilding the whole table here used to replace the
// clicked DOM node and caused fast clicks to be lost.
document.addEventListener("pointerdown", event => {
    const trigger = event.target.closest("[data-watchlist-trigger]");
    if (!trigger || event.button !== 0) return;

    event.preventDefault();
    event.stopPropagation();
    const symbol = trigger.dataset.symbol;
    const savedColor = watchlistColorFor(symbol);

    // Already saved color: preserve the existing behavior — one click clears it.
    if (savedColor && watchlistOpenPaletteSymbol !== symbol) {
        if (watchlistOpenPaletteSymbol) closeWatchlistPalette(true);
        commitWatchlistColor(symbol, null);
        renderTable();
        return;
    }

    // Close another open palette before opening this one. The closing commit
    // may move that other coin to its group, so render once for that transition.
    if (watchlistOpenPaletteSymbol && watchlistOpenPaletteSymbol !== symbol) {
        closeWatchlistPalette(true);
    }

    // Second click on the same open flag toggles the red preview off/on, but
    // keeps the same DOM node and palette in place so the click cannot be lost.
    if (watchlistOpenPaletteSymbol === symbol) {
        if (watchlistPendingColor) {
            watchlistPendingColor = null;
            watchlistPendingExplicitClear = true;
        } else {
            watchlistPendingColor = WATCHLIST_COLORS[0];
            watchlistPendingExplicitClear = false;
        }
        updateWatchlistTriggerVisual(symbol, watchlistPendingColor);
        return;
    }

    watchlistOpenPaletteSymbol = symbol;
    watchlistPendingColor = WATCHLIST_COLORS[0];
    watchlistPendingExplicitClear = false;

    // Paint immediately and create the palette without rebuilding the table.
    updateWatchlistTriggerVisual(symbol, WATCHLIST_COLORS[0]);
    openWatchlistPaletteInPlace(symbol);
});

document.addEventListener("click", event => {
    const option = event.target.closest("[data-watchlist-color]");
    if (option) {
        event.preventDefault();
        event.stopPropagation();
        if (watchlistOpenPaletteSymbol === option.dataset.symbol) {
            watchlistPendingColor = option.dataset.watchlistColor;
            watchlistPendingExplicitClear = false;
            updateWatchlistTriggerVisual(option.dataset.symbol, watchlistPendingColor);
        }
        return;
    }

    const clear = event.target.closest("[data-watchlist-clear]");
    if (clear) {
        event.preventDefault();
        event.stopPropagation();
        if (watchlistOpenPaletteSymbol === clear.dataset.symbol) {
            watchlistPendingColor = null;
            watchlistPendingExplicitClear = true;
            updateWatchlistTriggerVisual(clear.dataset.symbol, null);
        }
        return;
    }
});

document.addEventListener("click", event => {
    if (!watchlistOpenPaletteSymbol) return;
    if (event.target.closest("[data-watchlist-trigger], [data-watchlist-palette]")) return;
    closeWatchlistPalette(true);
});

document.addEventListener("click", event => {
    if (event.target.closest(".chart-grid-flag-wrap, .chart-grid-flag-palette")) return;
    chartGridWorkspace?.querySelectorAll(".chart-grid-flag-palette").forEach(p => p.hidden = true);
});


let templateAnalysisInProgress = false;
function renderTable() {
    if (templateAnalysisInProgress) {
        const columns = getMarketColumns();
        coinCount.textContent = 0;
        renderMarketHeaders(columns);
        coinTable.innerHTML = "";
        return;
    }
    const query = searchInput.value.trim().toUpperCase();
    const columns = getMarketColumns();

    const sourceCoins = (marketSettings.freeze || marketSettings.freezeAll) && marketFrozenCoins
        ? marketFrozenCoins
        : latestCoins;

    let rows = sourceCoins.filter(c => {
        if (!String(c.symbol || "").toUpperCase().endsWith("USDT")) return false;
        if (query && !c.symbol.includes(query)) return false;
        return passesMarketFilters(c, columns);
    });

    // When a column sort is active, it has absolute priority. Watchlist
    // grouping is used only in the normal/unsorted table state; otherwise it
    // would destroy the selected ascending/descending order.
    rows = applyMarketSort(rows);
    if (!marketSort.column || !marketSort.direction) {
        rows = applyWatchlistColorGrouping(rows);
    }
    coinCount.textContent = rows.length;
    renderMarketHeaders(columns);

    coinTable.innerHTML = rows.map(c => {
        const changeClass = c.change_pct > 0 ? "positive" : c.change_pct < 0 ? "negative" : "";
        const rowColor = watchlistColorFor(c.symbol);
        return `<tr class="clickable-row${rowColor ? ' watchlist-colored-row' : ''}" data-symbol="${c.symbol}"${rowColor ? ` style="--watchlist-row-color:${rowColor}"` : ""}>` +
            columns.map(k => {
                if (k === "symbol") {
                    return `<td>${renderWatchlistSymbolCell(c.symbol)}</td>`;
                }
                return `<td class="right ${k === 'change_pct' ? changeClass : ''}">${marketValue(c,k)}</td>`;
            }).join("") +
            `</tr>`;
    }).join("");

    if (!rows.length) {
        coinTable.innerHTML = `
            <tr>
                <td colspan="${columns.length || 1}" class="muted" style="text-align:center;padding:25px">
                    Данные пока не получены.
                </td>
            </tr>
        `;
    } else {
    }
}

function renderLevels(items) {
    if (!levels) return;
    if (!items || !items.length) {
        levels.innerHTML = `<div class="level muted">Уровни пока не настроены.</div>`;
        return;
    }

    levels.innerHTML = items.map(level => `
        <div class="level">
            <div><strong>${level.symbol}</strong> · ${level.type}</div>
            <div class="level-price">Level: ${fmtPrice(level.price)}</div>
            <div class="muted">Status: ${level.status} · Touches: ${level.touch_count}</div>
            <div class="muted">Last touch: ${fmtTime(level.last_touch)}</div>
        </div>
    `).join("");
}


function renderSignals(items) {
    if (!signals) return;
    if (!items || !items.length) {
        signals.innerHTML = `
            <div class="signal muted">
                Пока событий нет.
            </div>
        `;
        return;
    }

    signals.innerHTML = items.map(item => `
        <div class="signal">
            <div class="signal-time">${fmtTime(item.time)}</div>
            <div>
                <strong>${item.type}</strong>
                ${item.message ? " — " + item.message : ""}
            </div>
        </div>
    `).join("");
}

function renderStatus(status) {
    statusText.textContent = status;

    statusDot.classList.remove("live", "error");

    if (status === "LIVE") {
        statusDot.classList.add("live");
    } else if (status === "ERROR") {
        statusDot.classList.add("error");
    }
}

async function loadState() {
    try {
        const response = await fetch(screenerApiUrl("/api/screener"), {
            cache: "no-store"
        });

        if (!response.ok) {
            throw new Error("HTTP " + response.status);
        }

        const data = await response.json();

        // Список монет Binance сам по себе не является источником листингов.
        // Отдельный источник листингов будет подключён отдельным этапом.
        latestCoins = data.coins || [];
        evaluateAlertEngine(latestCoins);
        refreshAlertDynamicMetrics(latestCoins);

        if (!selectedChartSymbol && latestCoins.length && !chartAutoOpenPending) {
            chartAutoOpenPending = true;
            requestAnimationFrame(() => {
                chartAutoOpenPending = false;
                if (!selectedChartSymbol && latestCoins.length) {
                    openChart(latestCoins[0].symbol);
                }
            });
        }

        renderStatus(data.status);
        renderTable();
        refreshMarketTimeframeMetrics(latestCoins);
        updateChartGridPagination();
        renderLevels(data.levels || []);
        renderSignals(data.signals || []);

        if (connectionStatus) connectionStatus.textContent = data.status || "—";
        if (connectionCount) connectionCount.textContent = data.connection_count ?? 0;
        if (reconnectAttempts) reconnectAttempts.textContent = data.reconnect_attempts ?? 0;
        if (lastMessage) lastMessage.textContent = fmtTime(data.last_message);
        if (lastUpdate) lastUpdate.textContent = fmtTime(data.last_update);

        if (data.last_error) {
            errorBox.style.display = "block";
            errorBox.textContent = "Последняя ошибка: " + data.last_error;
        } else {
            errorBox.style.display = "none";
        }

    } catch (error) {
        if (await fallbackToLocalIfRenderUnavailable(error)) {
            return loadState();
        }
        renderStatus("WEB ERROR");
        errorBox.style.display = "block";
        errorBox.textContent = "Ошибка получения состояния: " + error;
    }
}

async function loadInstrumentCatalog() {
    try {
        const response = await fetch("/api/instruments", { cache: "no-store" });
        const data = await response.json();
        if (!response.ok || !Array.isArray(data.symbols)) {
            throw new Error(data.error || "Failed to load instruments");
        }
        instrumentCatalog = data.symbols;
        instrumentTypeBySymbol = new Map(
            instrumentCatalog.map(item => [String(item.symbol || "").toUpperCase(), String(item.instrumentType || "").toLowerCase()])
        );
        renderTable();
        if (searchInput.value.trim()) renderSuggestions();
    } catch (error) {
        console.error("Instrument catalog error:", error);
    }
}

loadInstrumentCatalog();

let workspaceDragging = false;

// Normalize only an invalid/stale layout. There is intentionally NO maximum
// width for either pane: the divider may move to either extreme, leaving the
// opposite pane at its minimum usable width.
try {
    const layoutEl = document.querySelector(".layout");
    const rect = layoutEl?.getBoundingClientRect();
    const currentCols = getComputedStyle(layoutEl || document.body).gridTemplateColumns || "";
    if (layoutEl && rect?.width && currentCols) {
        const parts = currentCols.split(/\s+/);
        const chartPx = parseFloat(parts[0]);
        const marketPx = parseFloat(parts[parts.length - 1]);
        if (!Number.isFinite(chartPx) || !Number.isFinite(marketPx) || chartPx < 320 || marketPx < 320) {
            const style = getComputedStyle(layoutEl);
            const gap = parseFloat(style.columnGap) || 0;
            const padLeft = parseFloat(style.paddingLeft) || 0;
            const padRight = parseFloat(style.paddingRight) || 0;
            const available = Math.max(640, rect.width - padLeft - padRight - 8 - (gap * 2));
            const marketDefault = Math.min(420, Math.max(320, available / 2));
            layoutEl.style.gridTemplateColumns = `${available - marketDefault}px 8px ${marketDefault}px`;
        }
    }
} catch (_) {}

if (workspaceDivider) {
    workspaceDivider.addEventListener("pointerdown", event => {
        event.preventDefault();
        workspaceDragging = true;
        workspaceDivider.classList.add("dragging");
        document.body.classList.add("workspace-resizing");
        workspaceDivider.setPointerCapture?.(event.pointerId);
    });

    workspaceDivider.addEventListener("pointermove", event => {
        if (!workspaceDragging) return;

        const layout = document.querySelector(".layout");
        if (!layout) return;

        const rect = layout.getBoundingClientRect();
        const dividerWidth = 8;
        const minChart = 320;
        const minMarket = 320;
        const style = getComputedStyle(layout);
        const padLeft = parseFloat(style.paddingLeft) || 0;
        const padRight = parseFloat(style.paddingRight) || 0;
        const gap = parseFloat(style.columnGap) || 0;
        const available = Math.max(1, rect.width - padLeft - padRight - dividerWidth - (gap * 2));
        const contentLeft = rect.left + padLeft;

        let chartWidth = event.clientX - contentLeft - gap;

        // One continuous clamp is important here. The previous implementation
        // switched between two different branches at available/2, which made
        // the divider suddenly jump to the opposite side when a minimum width
        // was reached. Keep the divider exactly under the pointer until a real
        // minimum is reached, then stop it there.
        const minTotal = minChart + minMarket;
        if (available >= minTotal) {
            // No artificial maximum: the chart may shrink to minChart while
            // the market screener expands to the full remaining width, and
            // vice versa.
            const maxChart = available - minMarket;
            chartWidth = Math.max(minChart, Math.min(maxChart, chartWidth));
        } else {
            // Extremely narrow window: both preferred minimums cannot fit.
            // Keep both panes usable and prevent either column from collapsing
            // to 1px while dragging.
            const safeHalf = Math.max(1, available / 2);
            chartWidth = Math.max(1, Math.min(available - 1, Math.max(1, Math.min(safeHalf, chartWidth))));
        }
        const marketWidth = Math.max(1, available - chartWidth);

        const nextColumns = `${chartWidth}px ${dividerWidth}px ${marketWidth}px`;
        if (layout.style.gridTemplateColumns !== nextColumns) {
            layout.style.gridTemplateColumns = nextColumns;
            // Layout is changing continuously while dragging. The sync engine
            // coalesces these calls into one frame and preserves both price/time
            // view ranges, so the chart cannot accumulate zoom changes.
            scheduleChartLayoutSync();
            scheduleStableDrawingsRedraw();
        }
    });

    const stopWorkspaceDrag = () => {
        if (!workspaceDragging) return;
        workspaceDragging = false;
        workspaceDivider.classList.remove("dragging");
        document.body.classList.remove("workspace-resizing");
        scheduleChartLayoutSync();
        scheduleStableDrawingsRedraw();
    };

    workspaceDivider.addEventListener("pointerup", stopWorkspaceDrag);
    workspaceDivider.addEventListener("pointercancel", stopWorkspaceDrag);
}

searchInput.addEventListener("input", () => {
    renderTable();
    renderSuggestions();
});

searchInput.addEventListener("keydown", event => {
    if (event.key === "Enter") {
        const query = searchInput.value.trim().toUpperCase();
        const liveMatch = latestCoins.find(c => c.symbol === query) ||
                          latestCoins.find(c => c.symbol.includes(query));
        const catalogMatch = instrumentCatalog.find(c => c.symbol === query) ||
                             instrumentCatalog.find(c => c.symbol.includes(query));
        const match = liveMatch || catalogMatch;
        if (match) {
            openChart(match.symbol);
            closeSearchModal();
        }
    } else if (event.key === "Escape") {
        suggestions.style.display = "none";
        closeChart();
    }
});

suggestions.addEventListener("click", event => {
    const item = event.target.closest(".suggestion");
    if (!item) return;
    const symbol = item.dataset.symbol || item.getAttribute("data-symbol");
    if (symbol) {
        openChart(symbol);
        closeSearchModal();
    }
});

document.addEventListener("click", event => {
    if (!event.target.closest(".search-wrap")) {
        suggestions.style.display = "none";
        if (topSearch.classList.contains("open")) {
            closeSearchModal();
        }
    }
});

chartClose.addEventListener("click", closeChart);
chartBack?.addEventListener("click", event => {
    event.preventDefault(); event.stopPropagation();
    if (chartGridMaximized >= 0) {
        const slots = [...chartGridWorkspace.querySelectorAll('.chart-grid-slot')];
        const slot = slots[chartGridMaximized];
        if (slot) {
            chartGridMaximized = -1;
            slot.classList.remove('maximized');
            chartGridWorkspace.classList.remove('has-maximized');
            chartModal?.classList.remove('grid-maximized');
            setChartLargeMode(false);
            updateChartGridExpandButtons();
            scheduleOwnChartVisualSync({resize:true,viewport:true});
            return;
        }
    }
    goBackFromChart();
});
let chartFlagLastPointerDown = 0;
chartFlag?.addEventListener("pointerdown", event => {
    if (event.button !== 0) return;
    event.preventDefault(); event.stopPropagation();
    const now = performance.now();
    const isFastDouble = (now - chartFlagLastPointerDown) <= 320;
    chartFlagLastPointerDown = now;
    if (isFastDouble) {
        chartFlagPendingColor = null;
        chartFlagPendingExplicitClear = true;
        setChartFlagVisual(null);
        saveChartFlagColor(selectedChartSymbol, null);
        chartFlagOpen = false;
        if (chartFlagPalette) chartFlagPalette.hidden = true;
        return;
    }
    chartFlagOpen = true;
    chartFlagPendingColor = WATCHLIST_COLORS[0];
    chartFlagPendingExplicitClear = false;
    setChartFlagVisual(chartFlagPendingColor);
    renderChartFlagPalette();
});
document.addEventListener("pointerdown", event => {
    if (!chartFlagOpen) return;
    if (event.target.closest("#chartFlag, #chartFlagPalette")) return;
    commitChartFlag();
    chartFlagOpen = false;
    if (chartFlagPalette) chartFlagPalette.hidden = true;
});
document.addEventListener("click", event => {
    if (!chartFlagOpen) return;
    if (event.target.closest("#chartFlag, #chartFlagPalette")) return;
    commitChartFlag();
    chartFlagOpen = false;
    if (chartFlagPalette) chartFlagPalette.hidden = true;
});
chartModal.addEventListener("click", event => {
    // Persistent chart workspace: do not close the chart on background clicks.
});

setInterval(() => {
    if (tigerTerminalSocket && tigerTerminalSocket.readyState === WebSocket.OPEN) {
        try { tigerTerminalSocket.send("ping"); } catch {}
    }
}, 15000);


// ============================================================
// app63: independent Market View / Filter / Pattern / Level / Sort / Display layer
// ============================================================
const TEMPLATE_STORAGE = "cryptoScreenerMarketTemplatesV1";
const TEMPLATE_ACTIVE_STORAGE = "cryptoScreenerActiveTemplateV1";
const DEFAULT_TEMPLATE = {
    id: "market-default",
    name: "Общий рынок",
    marketFilters: {
        change: { mode: "off", from: "", to: "", min: null, max: null },
        turnover: { mode: "off", from: "", to: "", min: null, max: null },
        natr: { mode: "off", from: "", to: "", min: null, max: null },
        trades: { mode: "off", from: "", to: "", min: null, max: null },
        btcCorr: { mode: "off", from: "", to: "", min: null, max: null },
        volumeSpike: { mode: "off", from: "", to: "", min: null, max: null },
        volumeExpansion: { mode: "off", from: "", to: "", min: null, max: null, baseCandles: 100, growthCandles: 20 },
        spread: { mode: "off", from: "", to: "", min: null, max: null },
        funding: { mode: "off", from: "", to: "", min: null, max: null },
        oiChange: { mode: "off", from: "", to: "", min: null, max: null },
        oiChangePct: { mode: "off", from: "", to: "", min: null, max: null, direction: "any" },
        deltaVolume: { mode: "off", from: "", to: "", min: null, max: null },
        price: { mode: "off", from: "", to: "", min: null, max: null }
    },
    structure: { pattern: "", patternType: "", timeframe: "", timeframeMode: "all", timeframeFrom: null, timeframeTo: null, minTouches: 0, stage: "", consolidationMin: null, distanceMax: null, structureSearchMode: "none",
        levelType: "", levelMinTouches: 0, levelCascade: "", cascadeMinVertices: 0, cascadeDistanceMax: null,
        horizontalLevelSearchPeriod: 50, horizontalLevelSearchStrict: false, horizontalLevelTouches: 2, horizontalLevelTouchTolerancePct: 0.1, horizontalLevelLifetimeHours: null, horizontalLevelNoCross: false, horizontalLevelAllowMinorPierce: false,
        trendlineSearchPeriod: 50, trendlineSearchStrict: false, trendlineTouches: 2, trendlineTouchTolerancePct: 0.1, trendlineLifetimeHours: null, trendlineNoCross: false, trendlineAllowMinorPierce: false },
    display: { mode: "table", limit: 0 },
    sort: [{ column: "", direction: "asc" }]
};
let marketTemplates = loadMarketTemplates();
let activeMarketTemplateId = localStorage.getItem(TEMPLATE_ACTIVE_STORAGE) || "market-default";
let templateEditingId = null;
let templateAnalysisBusy = false;
let templateAnalysisCache = new Map();
let templateApplyGeneration = 0;

function cloneTemplate(value) { return JSON.parse(JSON.stringify(value)); }
function normalizeMetricFilter(src, legacyMin=null, legacyMax=null) {
    const x = src && typeof src === "object" ? src : {};
    const mode = ["off","strict","from","to","between"].includes(x.mode) ? x.mode : (legacyMin!=null || legacyMax!=null ? "from" : "off");
    return { mode, from: String(x.from || ""), to: String(x.to || x.from || ""), min: x.min ?? legacyMin ?? null, max: x.max ?? legacyMax ?? null };
}
function normalizeVolumeExpansionFilter(src){
    const base=normalizeMetricFilter(src);
    const raw=src&&typeof src==="object"?src:{};
    const b=Number(raw.baseCandles), g=Number(raw.growthCandles);
    const direction=raw&&["any","up","down"].includes(raw.direction)?raw.direction:"any";
    return {...base,baseCandles:Number.isFinite(b)&&b>=1?Math.min(500,Math.round(b)):100,growthCandles:Number.isFinite(g)&&g>=1?Math.min(200,Math.round(g)):20,direction};
}
function normalizeOiChangePctFilter(src){
    const base=normalizeMetricFilter(src);
    const direction=src&&["any","up","down"].includes(src.direction)?src.direction:"any";
    return {...base,direction};
}
function getTemplateMetricFilter(t, key) {
    const f = t?.marketFilters || {};
    if (key === "change") return normalizeMetricFilter(f.change, f.changeMin, null);
    if (key === "volumeExpansion") return normalizeVolumeExpansionFilter(f.volumeExpansion);
    if (key === "oiChangePct") return normalizeOiChangePctFilter(f.oiChangePct);
    if (key === "turnover") return normalizeMetricFilter(f.turnover, f.volumeMin, null);
    if (key === "natr") return normalizeMetricFilter(f.natr, f.natrMin, f.natrMax);
    const map={trades:"trades",btcCorr:"btcCorr",volumeSpike:"volumeSpike",volumeExpansion:"volumeExpansion",spread:"spread",funding:"funding",oiChange:"oiChange",oiChangePct:"oiChangePct",deltaVolume:"deltaVolume",price:"price"};
    return normalizeMetricFilter(f[map[key]] || f[key]);
}
function timeframeRank(tf) {
    const map={"1m":1,"3m":3,"5m":5,"15m":15,"30m":30,"1h":60,"2h":120,"3h":180,"4h":240,"6h":360,"8h":480,"12h":720,"1d":1440,"3d":4320,"1w":10080};
    return map[String(tf||"").toLowerCase()] || 0;
}
function timeframeRange(mode, from, to) {
    const all=["1m","3m","5m","15m","30m","1h","2h","3h","4h","6h","8h","12h","1d","3d","1w"];
    if(mode === "strict") return [from || "1h"];
    if(mode === "from") return all.filter(x=>timeframeRank(x)>=timeframeRank(from||"1h"));
    if(mode === "to") return all.filter(x=>timeframeRank(x)<=timeframeRank(to||from||"1h"));
    if(mode === "between") { const a=timeframeRank(from||"1h"), b=timeframeRank(to||from||"1h"); return all.filter(x=>timeframeRank(x)>=Math.min(a,b)&&timeframeRank(x)<=Math.max(a,b)); }
    return [];
}
function metricPassesRange(metricMap, cfg, field) {
    if(!cfg || cfg.mode === "off") return true;
    const tfs=timeframeRange(cfg.mode,cfg.from,cfg.to);
    if(!tfs.length) return true;
    const min=cfg.min==null?null:Number(cfg.min), max=cfg.max==null?null:Number(cfg.max);
    return tfs.some(tf=>{
        const value=Number(metricMap?.[tf]?.[field]);
        if(!Number.isFinite(value)) return false;

        // For the market "Изменение, %" filter, a positive threshold
        // means the magnitude of the move: +10% and -10% both qualify.
        // Directional ranges (for example -20 .. -10) remain directional.
        if(field === "change_pct" && min !== null && min >= 0 && (max === null || max >= 0)){
            const magnitude=Math.abs(value);
            if(magnitude < min) return false;
            if(max !== null && magnitude > max) return false;
            return true;
        }

        if(min!==null && value<min) return false;
        if(max!==null && value>max) return false;
        return true;
    });
}
function normalizeTemplate(t) {
    const base = cloneTemplate(DEFAULT_TEMPLATE);
    const src = t && typeof t === "object" ? t : {};
    base.id = String(src.id || base.id);
    base.name = String(src.name || "Без названия").slice(0,80);
    base.marketFilters = Object.assign(base.marketFilters, src.marketFilters || {});
    base.marketFilters.change = normalizeMetricFilter(src.marketFilters?.change, src.marketFilters?.changeMin, null);
    base.marketFilters.turnover = normalizeMetricFilter(src.marketFilters?.turnover, src.marketFilters?.volumeMin, null);
    base.marketFilters.natr = normalizeMetricFilter(src.marketFilters?.natr, src.marketFilters?.natrMin, src.marketFilters?.natrMax);
    ["trades","btcCorr","volumeSpike","spread","funding","oiChange","deltaVolume","price"].forEach(k=>{
        base.marketFilters[k] = normalizeMetricFilter(src.marketFilters?.[k]);
    });
    base.marketFilters.oiChangePct = normalizeOiChangePctFilter(src.marketFilters?.oiChangePct);
    base.marketFilters.volumeExpansion = normalizeVolumeExpansionFilter(src.marketFilters?.volumeExpansion);
    base.structure = Object.assign(base.structure, src.structure || {});
    // Structure search is controlled only by the saved structure settings.
    // Never infer a search command from the template name.
    if (!Object.prototype.hasOwnProperty.call(src.structure || {}, "structureSearchMode")) {
        base.structure.structureSearchMode = "none";
    }
    if (!["none","horizontal","trendline","cascade","both","horizontal_cascade","trendline_cascade","all"].includes(base.structure.structureSearchMode)) base.structure.structureSearchMode = "none";
    if (!Object.prototype.hasOwnProperty.call(src.structure || {}, "timeframeMode")) {
        base.structure.timeframeMode = "only";
        base.structure.timeframeFrom = String(base.structure.timeframe || "1h");
        base.structure.timeframeTo = String(base.structure.timeframe || "1h");
    }
    if (!["all","only","below","above","range"].includes(base.structure.timeframeMode)) base.structure.timeframeMode = "all";
    base.structure.timeframe = String(base.structure.timeframe || base.structure.timeframeFrom || "");
    base.structure.timeframeFrom = base.structure.timeframeFrom ? String(base.structure.timeframeFrom) : null;
    base.structure.timeframeTo = base.structure.timeframeTo ? String(base.structure.timeframeTo) : null;
    base.display = Object.assign(base.display, src.display || {});
    base.sort = Array.isArray(src.sort) && src.sort.length ? src.sort.slice(0,3) : base.sort;
    return base;
}
function loadMarketTemplates() {
    try {
        const saved = JSON.parse(localStorage.getItem(TEMPLATE_STORAGE) || "null");
        if (!Array.isArray(saved)) return [cloneTemplate(DEFAULT_TEMPLATE)];
        const list = saved.map(normalizeTemplate).filter(x => x.id && x.name);
        if (!list.some(x => x.id === "market-default")) list.unshift(cloneTemplate(DEFAULT_TEMPLATE));
        return list;
    } catch { return [cloneTemplate(DEFAULT_TEMPLATE)]; }
}
function saveMarketTemplates() { localStorage.setItem(TEMPLATE_STORAGE, JSON.stringify(marketTemplates)); }
function activeMarketTemplate() { return marketTemplates.find(t => t.id === activeMarketTemplateId) || marketTemplates[0]; }
function setActiveMarketTemplate(id) {
    if (!marketTemplates.some(t => t.id === id)) return;
    activeMarketTemplateId = id;
    localStorage.setItem(TEMPLATE_ACTIVE_STORAGE, id);
    templateAnalysisCache.clear();
    applyMarketTemplate(activeMarketTemplate());
    
setTemplateTimeframeUI({timeframeMode:"all"});
renderMarketTemplateMenu();
if (marketTemplateSearch) marketTemplateSearch.value = activeMarketTemplate()?.name || "";
}
const TEMPLATE_TIMEFRAMES=["1m","3m","5m","15m","30m","1h","2h","3h","4h","6h","8h","12h","1d","3d","1w"];
function formationTimeframeRange(mode, from, to){
    const all=TEMPLATE_TIMEFRAMES;
    if(mode === "only") return from ? [from] : [];
    if(mode === "below") return all.filter(x=>timeframeRank(x)<=timeframeRank(to||from));
    if(mode === "above") return all.filter(x=>timeframeRank(x)>=timeframeRank(from||to));
    if(mode === "range") { const a=timeframeRank(from||to), b=timeframeRank(to||from); return (from||to) ? all.filter(x=>timeframeRank(x)>=Math.min(a,b)&&timeframeRank(x)<=Math.max(a,b)) : []; }
    return all.slice();
}
function fillTemplateFormationTimeframes(){
    const list=document.getElementById("templateTimeframeList");
    if(list) list.innerHTML=TEMPLATE_TIMEFRAMES.map(tf=>`<option value="${tf}">`).join("");
}
function bindTemplateTimeframeKeyboard(){
    ["templateTimeframeSingle","templateTimeframeFrom","templateTimeframeTo"].forEach(id=>{
        const el=document.getElementById(id); if(!el || el.dataset.tfKeyboardBound)return;
        el.dataset.tfKeyboardBound="1";
        el.addEventListener("keydown",e=>{
            if(!["ArrowUp","ArrowDown","Enter"].includes(e.key))return;
            const cur=String(el.value||"").toLowerCase(), idx=TEMPLATE_TIMEFRAMES.indexOf(cur);
            if(e.key==="Enter"){ if(idx>=0){e.preventDefault();el.value=TEMPLATE_TIMEFRAMES[idx];el.dispatchEvent(new Event("change",{bubbles:true}));el.blur();} return; }
            e.preventDefault(); const next=Math.max(0,Math.min(TEMPLATE_TIMEFRAMES.length-1,(idx<0?0:idx)+(e.key==="ArrowUp"?-1:1))); el.value=TEMPLATE_TIMEFRAMES[next]; el.dispatchEvent(new Event("change",{bubbles:true}));
        });
    });
}
function setTemplateTimeframeUI(s){
    const mode=s.timeframeMode||"all", strict=mode==="only";
    let single=strict?(s.timeframeFrom||s.timeframe||""):"", from=s.timeframeFrom||"", to=s.timeframeTo||"";
    if(mode==="below"){from="";to=s.timeframeTo||s.timeframe||"";}
    if(mode==="above"){from=s.timeframeFrom||s.timeframe||"";to="";}
    const cb=document.getElementById("templateTimeframeStrict"), se=document.getElementById("templateTimeframeSingle"), f=document.getElementById("templateTimeframeFrom"), t=document.getElementById("templateTimeframeTo");
    if(cb)cb.checked=strict;if(se)se.value=single;if(f)f.value=from;if(t)t.value=to;
    document.getElementById("templateTimeframeControls")?.classList.toggle("strict",strict);
    document.getElementById("templateTimeframeSingleWrap")?.style.setProperty("display",strict?"":"none");
    document.getElementById("templateTimeframeFromWrap")?.style.setProperty("display",strict?"none":"");
    document.getElementById("templateTimeframeToWrap")?.style.setProperty("display",strict?"none":"");
}
function readTemplateTimeframeUI(){
    const strict=!!document.getElementById("templateTimeframeStrict")?.checked;
    const single=document.getElementById("templateTimeframeSingle")?.value.trim()||"";
    const from=document.getElementById("templateTimeframeFrom")?.value.trim()||"";
    const to=document.getElementById("templateTimeframeTo")?.value.trim()||"";
    if(strict){const tf=single;return {timeframe:tf,timeframeMode:"only",timeframeFrom:tf||null,timeframeTo:tf||null};}
    if(from||to)return {timeframe:from||to||"",timeframeMode:"range",timeframeFrom:from||null,timeframeTo:to||null};
    return {timeframe:"",timeframeMode:"all",timeframeFrom:null,timeframeTo:null};
}
function fillTemplateMetricTimeframes(){
    const list=document.getElementById("templateMarketTimeframeList");
    if(list) list.innerHTML=TEMPLATE_TIMEFRAMES.map(tf=>`<option value="${tf}">`).join("");
}
function bindTemplateMetricKeyboard(){
    ["Change","Turnover","Natr","Trades","BtcCorr","VolumeSpike","VolumeExpansion","Spread","Funding","OiChange","OiChangePct","DeltaVolume","Price"].forEach(prefix=>["Single","From","To"].forEach(side=>{
        const el=document.getElementById(`template${prefix}${side}`); if(!el||el.dataset.tfKeyboardBound)return;
        el.dataset.tfKeyboardBound="1";
        el.addEventListener("keydown",e=>{
            if(!["ArrowUp","ArrowDown","Enter"].includes(e.key))return;
            const cur=String(el.value||"").toLowerCase(), idx=TEMPLATE_TIMEFRAMES.indexOf(cur);
            if(e.key==="Enter"){ if(idx>=0){e.preventDefault();el.value=TEMPLATE_TIMEFRAMES[idx];el.dispatchEvent(new Event("change",{bubbles:true}));} return; }
            e.preventDefault();
            const next=Math.max(0,Math.min(TEMPLATE_TIMEFRAMES.length-1,(idx<0?0:idx)+(e.key==="ArrowUp"?-1:1)));
            el.value=TEMPLATE_TIMEFRAMES[next];
        });
    }));
}
function setTemplateMetricUI(prefix,cfg){
    const oldMode=cfg.mode||"off";
    const strict=oldMode==="strict";
    let from=cfg.from||"", to=cfg.to||"";
    if(oldMode==="from"){to="";}
    if(oldMode==="to"){from="";}
    const strictEl=document.getElementById(`template${prefix}Strict`);
    const singleEl=document.getElementById(`template${prefix}Single`);
    const fromEl=document.getElementById(`template${prefix}From`);
    const toEl=document.getElementById(`template${prefix}To`);
    if(strictEl)strictEl.checked=strict;
    if(singleEl)singleEl.value=strict?(cfg.from||""):"";
    if(fromEl)fromEl.value=from;
    if(toEl)toEl.value=to;
    document.getElementById(`template${prefix}Min`).value=cfg.min??"";
    document.getElementById(`template${prefix}Max`).value=cfg.max??"";
    if(prefix === "VolumeExpansion"){ const b=document.getElementById("templateVolumeExpansionBase"), g=document.getElementById("templateVolumeExpansionGrowth"), d=document.getElementById("templateVolumeExpansionDirection"); if(b)b.value=cfg.baseCandles ?? 100; if(g)g.value=cfg.growthCandles ?? 20; if(d)d.value=cfg.direction || "any"; }
    if(prefix === "OiChangePct"){ const d=document.getElementById("templateOiChangePctDirection"); if(d)d.value=cfg.direction || "any"; }
    updateTemplateMetricTimeframeUI(prefix);
}
function updateTemplateMetricTimeframeUI(prefix){
    const strict=!!document.getElementById(`template${prefix}Strict`)?.checked;
    const range=document.getElementById(`template${prefix}TfRange`), single=document.getElementById(`template${prefix}TfStrict`);
    if(range)range.style.display=strict?"none":"";
    if(single)single.style.display=strict?"":"none";
}
function readTemplateMetric(prefix){
    const num=id=>{const v=document.getElementById(id).value.trim();return v===""?null:Number(v)};
    const min=num(`template${prefix}Min`), max=num(`template${prefix}Max`);
    const extra = prefix === "VolumeExpansion" ? { baseCandles: Math.max(1, Math.min(500, Math.round(Number(document.getElementById("templateVolumeExpansionBase")?.value || 100)))), growthCandles: Math.max(1, Math.min(200, Math.round(Number(document.getElementById("templateVolumeExpansionGrowth")?.value || 20)))), direction: ["any","up","down"].includes(document.getElementById("templateVolumeExpansionDirection")?.value) ? document.getElementById("templateVolumeExpansionDirection").value : "any" } : prefix === "OiChangePct" ? { direction: ["any","up","down"].includes(document.getElementById("templateOiChangePctDirection")?.value) ? document.getElementById("templateOiChangePctDirection").value : "any" } : {};
    if(min===null && max===null) return {mode:"off",from:"",to:"",min:null,max:null,...extra};
    const strict=!!document.getElementById(`template${prefix}Strict`)?.checked;
    if(strict){
        const tf=document.getElementById(`template${prefix}Single`)?.value||"";
        return {mode:"strict",from:tf,to:tf,min,max,...extra};
    }
    const from=document.getElementById(`template${prefix}From`)?.value||"";
    const to=document.getElementById(`template${prefix}To`)?.value||"";
    return {mode:"between",from:from||to||"",to:to||from||"",min,max,...extra};
}
function openTemplateEditor(id=null) {
    templateEditingId = id;
    const source = id ? marketTemplates.find(t => t.id === id) : cloneTemplate(DEFAULT_TEMPLATE);
    const t = normalizeTemplate(source);
    document.getElementById("templateTitle").textContent = id ? "Изменить шаблон" : "Новый шаблон";
    document.getElementById("templateName").value = id ? t.name : "";
    const structureSearchMode = String(t.structure.structureSearchMode || "none");
    const searchHorizontal = document.getElementById("templateSearchHorizontal");
    const searchTrendline = document.getElementById("templateSearchTrendline");
    const searchCascade = document.getElementById("templateSearchCascade");
    const searchFlags = templateStructureSearchFlags(t.structure);
    if (searchHorizontal) searchHorizontal.checked = searchFlags.horizontal;
    if (searchTrendline) searchTrendline.checked = searchFlags.trendline;
    if (searchCascade) searchCascade.checked = searchFlags.cascade;
    fillTemplateFormationTimeframes();
    bindTemplateTimeframeKeyboard();
    fillTemplateMetricTimeframes();
    bindTemplateMetricKeyboard();
    setTemplateTimeframeUI(t.structure);
    setTemplateMetricUI("Change", getTemplateMetricFilter(t,"change"));
    setTemplateMetricUI("Turnover", getTemplateMetricFilter(t,"turnover"));
    setTemplateMetricUI("Natr", getTemplateMetricFilter(t,"natr"));
    ["Trades","BtcCorr","VolumeSpike","VolumeExpansion","Spread","Funding","OiChange","OiChangePct","DeltaVolume","Price"].forEach(prefix=>{ const key={Trades:"trades",BtcCorr:"btcCorr",VolumeSpike:"volumeSpike",VolumeExpansion:"volumeExpansion",Spread:"spread",Funding:"funding",OiChange:"oiChange",OiChangePct:"oiChangePct",DeltaVolume:"deltaVolume",Price:"price"}[prefix]; setTemplateMetricUI(prefix,getTemplateMetricFilter(t,key)); });
    document.getElementById("templatePattern").value = t.structure.pattern || "";
    updateTemplatePatternTypes(t.structure.pattern || "", t.structure.patternType || "");
    document.getElementById("templateTouches").value = String(t.structure.minTouches ?? 0);
    document.getElementById("templateStage").value = t.structure.stage || "";
    document.getElementById("templateConsolidationMin").value = t.structure.consolidationMin ?? "";
    document.getElementById("templateDistanceMax").value = t.structure.distanceMax ?? "";
    document.getElementById("templateLevelType").value = t.structure.levelType || "";
    document.getElementById("templateLevelTouches").value = String(t.structure.levelMinTouches ?? 0);
    document.getElementById("templateLevelCascade").value = t.structure.levelCascade || "";
    document.getElementById("templateCascadeVertices").value = String(t.structure.cascadeMinVertices ?? 0);
    document.getElementById("templateCascadeDistanceMax").value = t.structure.cascadeDistanceMax ?? "";
    document.getElementById("templateHorizontalLevelSearchPeriod").value = t.structure.horizontalLevelSearchPeriod ?? 50;
    document.getElementById("templateHorizontalLevelSearchStrict").checked = !!t.structure.horizontalLevelSearchStrict;
    document.getElementById("templateHorizontalLevelTouches").value = t.structure.horizontalLevelTouches ?? 2;
    document.getElementById("templateHorizontalLevelTolerance").value = t.structure.horizontalLevelTouchTolerancePct ?? 0.1;
    document.getElementById("templateHorizontalLevelLifetime").value = t.structure.horizontalLevelLifetimeHours ?? "";
    document.getElementById("templateHorizontalLevelNoCross").checked = !!t.structure.horizontalLevelNoCross;
    document.getElementById("templateHorizontalLevelAllowMinorPierce").checked = !!t.structure.horizontalLevelAllowMinorPierce;
    document.getElementById("templateTrendlineSearchPeriod").value = t.structure.trendlineSearchPeriod ?? 50;
    document.getElementById("templateTrendlineSearchStrict").checked = !!t.structure.trendlineSearchStrict;
    document.getElementById("templateTrendlineTouches").value = t.structure.trendlineTouches ?? 2;
    document.getElementById("templateTrendlineTolerance").value = t.structure.trendlineTouchTolerancePct ?? 0.1;
    document.getElementById("templateTrendlineLifetime").value = t.structure.trendlineLifetimeHours ?? "";
    document.getElementById("templateTrendlineNoCross").checked = !!t.structure.trendlineNoCross;
    document.getElementById("templateTrendlineAllowMinorPierce").checked = !!t.structure.trendlineAllowMinorPierce;
    document.getElementById("templateDisplay").value = t.display.mode || "table";
    document.getElementById("templateLimit").value = String(t.display.limit ?? 0);
    const sort0 = t.sort?.[0] || {};
    document.getElementById("templateSort1").value = sort0.column || "";
    document.getElementById("templateSortDir").value = sort0.direction || "asc";
    document.getElementById("templateDelete").style.display = id && id !== "market-default" ? "inline-block" : "none";
    document.getElementById("templateOverlay").classList.add("open");
    document.getElementById("templateOverlay").setAttribute("aria-hidden","false");
}
function closeTemplateEditor() {
    document.getElementById("templateOverlay").classList.remove("open");
    document.getElementById("templateOverlay").setAttribute("aria-hidden","true");
    templateEditingId = null;
}
function readTemplateEditor() {
    const num = id => { const v=document.getElementById(id).value.trim(); return v==="" ? null : Number(v); };
    const name = document.getElementById("templateName").value.trim() || "Мои настройки";
    return normalizeTemplate({
        id: templateEditingId || "template_" + Date.now(), name,
        marketFilters: { change:readTemplateMetric("Change"), turnover:readTemplateMetric("Turnover"), natr:readTemplateMetric("Natr"), trades:readTemplateMetric("Trades"), btcCorr:readTemplateMetric("BtcCorr"), volumeSpike:readTemplateMetric("VolumeSpike"), volumeExpansion:readTemplateMetric("VolumeExpansion"), spread:readTemplateMetric("Spread"), funding:readTemplateMetric("Funding"), oiChange:readTemplateMetric("OiChange"), oiChangePct:readTemplateMetric("OiChangePct"), deltaVolume:readTemplateMetric("DeltaVolume"), price:readTemplateMetric("Price") },
        structure: { pattern:document.getElementById("templatePattern").value, patternType:document.getElementById("templatePatternType").value, structureSearchMode:(document.getElementById("templateSearchHorizontal")?.checked && document.getElementById("templateSearchTrendline")?.checked && document.getElementById("templateSearchCascade")?.checked) ? "all" : (document.getElementById("templateSearchHorizontal")?.checked && document.getElementById("templateSearchTrendline")?.checked) ? "both" : (document.getElementById("templateSearchHorizontal")?.checked && document.getElementById("templateSearchCascade")?.checked) ? "horizontal_cascade" : (document.getElementById("templateSearchTrendline")?.checked && document.getElementById("templateSearchCascade")?.checked) ? "trendline_cascade" : document.getElementById("templateSearchHorizontal")?.checked ? "horizontal" : document.getElementById("templateSearchTrendline")?.checked ? "trendline" : document.getElementById("templateSearchCascade")?.checked ? "cascade" : "none", ...readTemplateTimeframeUI(), minTouches:Number(document.getElementById("templateTouches").value||0), stage:document.getElementById("templateStage").value, consolidationMin:num("templateConsolidationMin"), distanceMax:num("templateDistanceMax"),
            levelType:document.getElementById("templateLevelType").value, levelMinTouches:Number(document.getElementById("templateLevelTouches").value||0), levelCascade:document.getElementById("templateLevelCascade").value, cascadeMinVertices:Number(document.getElementById("templateCascadeVertices").value||0), cascadeDistanceMax:num("templateCascadeDistanceMax"),
            horizontalLevelSearchPeriod:Number(document.getElementById("templateHorizontalLevelSearchPeriod").value||50), horizontalLevelSearchStrict:!!document.getElementById("templateHorizontalLevelSearchStrict")?.checked, horizontalLevelTouches:Number(document.getElementById("templateHorizontalLevelTouches").value||2), horizontalLevelTouchTolerancePct:num("templateHorizontalLevelTolerance") ?? 0.1, horizontalLevelLifetimeHours:num("templateHorizontalLevelLifetime"), horizontalLevelNoCross:!!document.getElementById("templateHorizontalLevelNoCross")?.checked, horizontalLevelAllowMinorPierce:!!document.getElementById("templateHorizontalLevelAllowMinorPierce")?.checked,
            trendlineSearchPeriod:Number(document.getElementById("templateTrendlineSearchPeriod").value||50), trendlineSearchStrict:!!document.getElementById("templateTrendlineSearchStrict")?.checked, trendlineTouches:Number(document.getElementById("templateTrendlineTouches").value||2), trendlineTouchTolerancePct:num("templateTrendlineTolerance") ?? 0.1, trendlineLifetimeHours:num("templateTrendlineLifetime"), trendlineNoCross:!!document.getElementById("templateTrendlineNoCross")?.checked, trendlineAllowMinorPierce:!!document.getElementById("templateTrendlineAllowMinorPierce")?.checked },
        display: { mode:document.getElementById("templateDisplay").value, limit:Number(document.getElementById("templateLimit").value||0) },
        sort: [{ column:document.getElementById("templateSort1").value, direction:document.getElementById("templateSortDir").value }]
    });
}
function fillFormationSettingsTimeframes() {
    const options = TEMPLATE_TIMEFRAMES.map(tf => `<option value="${tf}">${tf}</option>`).join("");
    ["formationSettingsTimeframeFrom", "formationSettingsTimeframeTo"].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.innerHTML = `<option value="">—</option>${options}`;
    });
}

function setFormationSettingsTimeframeUI(s) {
    const mode = s.timeframeMode || "all";
    const from = s.timeframeFrom || (mode === "only" ? s.timeframe : "") || "";
    const to = s.timeframeTo || (mode === "only" ? s.timeframe : "") || "";
    const m = document.getElementById("formationSettingsTimeframeMode");
    const f = document.getElementById("formationSettingsTimeframeFrom");
    const t = document.getElementById("formationSettingsTimeframeTo");
    if (m) m.value = mode;
    if (f) f.value = from;
    if (t) t.value = to;
    const fw = document.getElementById("formationSettingsTimeframeFromWrap");
    const tw = document.getElementById("formationSettingsTimeframeToWrap");
    if (fw) fw.style.display = (mode === "all" || mode === "below") ? "none" : "";
    if (tw) tw.style.display = (mode === "range" || mode === "below") ? "" : "none";
    if (f) f.disabled = mode === "all";
    if (t) t.disabled = mode === "all" || mode === "only" || mode === "above";
}

function readFormationSettingsTimeframeUI() {
    const mode = document.getElementById("formationSettingsTimeframeMode")?.value || "all";
    const from = document.getElementById("formationSettingsTimeframeFrom")?.value || "";
    const to = document.getElementById("formationSettingsTimeframeTo")?.value || "";
    return {
        timeframe: mode === "only" ? from : (from || to || "1h"),
        timeframeMode: mode,
        timeframeFrom: from || null,
        timeframeTo: to || null
    };
}

function loadFormationSettingsFromActiveTemplate() {
    const t = normalizeTemplate(activeMarketTemplate());
    const s = t.structure || {};
    fillFormationSettingsTimeframes();
    setFormationSettingsTimeframeUI(s);
    const pattern = document.getElementById("formationSettingsPattern");
    if (pattern) pattern.value = s.pattern || "";
    updateTemplatePatternTypes(s.pattern || "", s.patternType || "");
    const pt = document.getElementById("formationSettingsPatternType");
    if (pt) pt.value = s.patternType || pt.value || "";
    const set = (id, value) => { const el=document.getElementById(id); if (el) el.value = String(value ?? ""); };
    set("formationSettingsTouches", s.minTouches ?? 0);
    set("formationSettingsStage", s.stage || "");
    set("formationSettingsConsolidationMin", s.consolidationMin ?? "");
    set("formationSettingsDistanceMax", s.distanceMax ?? "");
    set("formationSettingsLevelType", s.levelType || "");
    set("formationSettingsLevelTouches", s.levelMinTouches ?? 0);
    set("formationSettingsLevelCascade", s.levelCascade || "");
    set("formationSettingsCascadeVertices", s.cascadeMinVertices ?? 0);
    set("formationSettingsCascadeDistanceMax", s.cascadeDistanceMax ?? "");
    set("formationSettingsHorizontalLevelSearchPeriod", s.horizontalLevelSearchPeriod ?? 50);
    document.getElementById("formationSettingsHorizontalLevelSearchStrict").checked = !!s.horizontalLevelSearchStrict;
    set("formationSettingsHorizontalLevelTouches", s.horizontalLevelTouches ?? 2);
    set("formationSettingsHorizontalLevelTolerance", s.horizontalLevelTouchTolerancePct ?? 0.1);
    set("formationSettingsHorizontalLevelLifetime", s.horizontalLevelLifetimeHours ?? "");
    document.getElementById("formationSettingsHorizontalLevelNoCross").checked = !!s.horizontalLevelNoCross;
    document.getElementById("formationSettingsHorizontalLevelAllowMinorPierce").checked = !!s.horizontalLevelAllowMinorPierce;
    set("formationSettingsTrendlineSearchPeriod", s.trendlineSearchPeriod ?? 50);
    document.getElementById("formationSettingsTrendlineSearchStrict").checked = !!s.trendlineSearchStrict;
    set("formationSettingsTrendlineTouches", s.trendlineTouches ?? 2);
    set("formationSettingsTrendlineTolerance", s.trendlineTouchTolerancePct ?? 0.1);
    set("formationSettingsTrendlineLifetime", s.trendlineLifetimeHours ?? "");
    document.getElementById("formationSettingsTrendlineNoCross").checked = !!s.trendlineNoCross;
    document.getElementById("formationSettingsTrendlineAllowMinorPierce").checked = !!s.trendlineAllowMinorPierce;
}

function readFormationSettingsStructure() {
    const num = id => {
        const el = document.getElementById(id);
        const v = el ? el.value.trim() : "";
        return v === "" ? null : Number(v);
    };
    return {
        pattern: document.getElementById("formationSettingsPattern")?.value || "",
        patternType: document.getElementById("formationSettingsPatternType")?.value || "",
        ...readFormationSettingsTimeframeUI(),
        minTouches: Number(document.getElementById("formationSettingsTouches")?.value || 0),
        stage: document.getElementById("formationSettingsStage")?.value || "",
        consolidationMin: num("formationSettingsConsolidationMin"),
        distanceMax: num("formationSettingsDistanceMax"),
        levelType: document.getElementById("formationSettingsLevelType")?.value || "",
        levelMinTouches: Number(document.getElementById("formationSettingsLevelTouches")?.value || 0),
        levelCascade: document.getElementById("formationSettingsLevelCascade")?.value || "",
        cascadeMinVertices: Number(document.getElementById("formationSettingsCascadeVertices")?.value || 0),
        cascadeDistanceMax: num("formationSettingsCascadeDistanceMax"),
        horizontalLevelSearchPeriod: Number(document.getElementById("formationSettingsHorizontalLevelSearchPeriod")?.value || 50),
        horizontalLevelSearchStrict: !!document.getElementById("formationSettingsHorizontalLevelSearchStrict")?.checked,
        horizontalLevelTouches: Number(document.getElementById("formationSettingsHorizontalLevelTouches")?.value || 2),
        horizontalLevelTouchTolerancePct: num("formationSettingsHorizontalLevelTolerance") ?? 0.1,
        horizontalLevelLifetimeHours: num("formationSettingsHorizontalLevelLifetime"),
        horizontalLevelNoCross: !!document.getElementById("formationSettingsHorizontalLevelNoCross")?.checked,
        horizontalLevelAllowMinorPierce: !!document.getElementById("formationSettingsHorizontalLevelAllowMinorPierce")?.checked,
        trendlineSearchPeriod: Number(document.getElementById("formationSettingsTrendlineSearchPeriod")?.value || 50),
        trendlineSearchStrict: !!document.getElementById("formationSettingsTrendlineSearchStrict")?.checked,
        trendlineTouches: Number(document.getElementById("formationSettingsTrendlineTouches")?.value || 2),
        trendlineTouchTolerancePct: num("formationSettingsTrendlineTolerance") ?? 0.1,
        trendlineLifetimeHours: num("formationSettingsTrendlineLifetime"),
        trendlineNoCross: !!document.getElementById("formationSettingsTrendlineNoCross")?.checked,
        trendlineAllowMinorPierce: !!document.getElementById("formationSettingsTrendlineAllowMinorPierce")?.checked
    };
}

function applyFormationSettingsToActiveTemplate() {
    const active = activeMarketTemplate();
    if (!active) return;
    active.structure = {...active.structure, ...readFormationSettingsStructure()};
    marketTemplates = marketTemplates.map(t => t.id === active.id ? active : t);
    saveMarketTemplates();
    templateAnalysisCache.clear();
    applyMarketTemplate(active);
}

function initFormationFilterSettings() {
    if (!document.getElementById("formationSettingsPattern")) return;
    document.getElementById("formationSettingsTimeframeMode")?.addEventListener("change", () => setFormationSettingsTimeframeUI(readFormationSettingsTimeframeUI()));
    document.getElementById("formationSettingsPattern")?.addEventListener("change", e => updateTemplatePatternTypes(e.target.value));
    document.getElementById("formationSettingsApply")?.addEventListener("click", applyFormationSettingsToActiveTemplate);
    document.getElementById("formationSettingsReset")?.addEventListener("click", loadFormationSettingsFromActiveTemplate);
    loadFormationSettingsFromActiveTemplate();
}

function saveTemplate(asNew=false) {
    const next = readTemplateEditor();
    if (asNew || !templateEditingId) { next.id = "template_" + Date.now() + "_" + Math.random().toString(36).slice(2,7); marketTemplates.push(next); }
    else { const i=marketTemplates.findIndex(x=>x.id===templateEditingId); if(i>=0) marketTemplates[i]=next; }
    saveMarketTemplates(); setActiveMarketTemplate(next.id); closeTemplateEditor();
}
function deleteTemplate() {
    if (!templateEditingId || templateEditingId === "market-default") return;
    marketTemplates = marketTemplates.filter(x=>x.id!==templateEditingId);
    saveMarketTemplates(); setActiveMarketTemplate("market-default"); closeTemplateEditor();
}
function renderMarketTemplateMenu() {
    const menu=document.getElementById("marketTemplateMenu"); if(!menu) return;
    const active=activeMarketTemplateId;
    menu.innerHTML = marketTemplates.map(t=>`<div class="market-template-item${t.id===active?' active':''}" data-template-id="${t.id}"><span class="name">${escapeHtml(t.name)}</span><button class="edit" type="button" data-template-edit="${t.id}" title="Изменить">⚙</button></div>`).join("") + `<button id="marketTemplateAdd" class="market-template-add" type="button">＋ Новый шаблон</button>`;
    if (marketTemplateSearch && document.activeElement !== marketTemplateSearch) {
        marketTemplateSearch.value = activeMarketTemplate()?.name || "";
    }
    document.getElementById("activeTemplateLabel").textContent = activeMarketTemplate()?.name || "";
}
function updateTemplatePatternTypes(pattern, selected="") {
    const el=document.getElementById("templatePatternType"); if(!el) return;
    const map={Triangle:["Ascending","Descending","Symmetrical"],Wedge:["Rising","Falling"],Channel:["Ascending","Descending","Horizontal"],Flag:["Bullish","Bearish"],Pennant:["Bullish","Bearish"],"Double Top":["Classic"],"Double Bottom":["Classic"],"Head and Shoulders":["Classic"],"Inverse Head and Shoulders":["Classic"]};
    const options=map[pattern]||[]; el.innerHTML=options.map(v=>`<option value="${v}">${v}</option>`).join(""); el.value=options.includes(selected)?selected:(options[0]||"");
}
function applyTemplateMarketFilters(rows, t) {
    const metricMap = {
        change: "change_pct",
        turnover: "turnover_usd",
        natr: "natr",
        trades: "trades",
        btcCorr: "btc_corr",
        volumeSpike: "volume_spike",
        volumeExpansion: "volume_expansion_x",
        spread: "spread_pct",
        funding: "funding_pct",
        oiChange: "oi_change_usd",
        oiChangePct: "oi_change_pct",
        deltaVolume: "delta_volume_usd",
        price: "price"
    };
    return rows.filter(c=>{
        const metrics=c.__templateMetrics||{};
        for(const [filterKey, field] of Object.entries(metricMap)){
            const cfg=getTemplateMetricFilter(t,filterKey);
            if(cfg.mode==="off") continue;
            if(!metricPassesRange(metrics,cfg,field)) return false;
        }

        const volumeExpansion=getTemplateMetricFilter(t,"volumeExpansion");
        if(volumeExpansion.mode !== "off" && volumeExpansion.direction !== "any"){
            const tfs=timeframeRange(volumeExpansion.mode,volumeExpansion.from,volumeExpansion.to);
            if(!tfs.some(tf=>String(metrics?.[tf]?.volume_expansion_direction||"any")===volumeExpansion.direction)) return false;
        }

        const oiPct=getTemplateMetricFilter(t,"oiChangePct");
        if(oiPct.mode !== "off"){
            const tfs=timeframeRange(oiPct.mode,oiPct.from,oiPct.to);
            const min=oiPct.min==null?null:Number(oiPct.min), max=oiPct.max==null?null:Number(oiPct.max);
            const direction=oiPct.direction||"any";
            const matched=tfs.some(tf=>{
                const value=Number(metrics?.[tf]?.oi_change_pct);
                if(!Number.isFinite(value)) return false;
                if(min!==null && value<min) return false;
                if(max!==null && value>max) return false;
                if(direction==="up" && value<=0) return false;
                if(direction==="down" && value>=0) return false;
                return true;
            });
            if(!matched) return false;
        }
        return true;
    });
}

function templateStageMatches(actual, wanted) {
    if(!wanted) return true;
    if(actual===wanted) return true;
    if(wanted==="middle") return actual==="forming";
    if(wanted==="retest") return actual==="breakout";
    if(wanted==="confirmed_breakout") return actual==="breakout";
    if(wanted==="failed_breakout") return actual==="none";
    return false;
}
function applyTemplateAnalysis(rows,t) {
    const s=t.structure||{};
    const hasFormationCriteria=hasTemplateFormationCriteria(s);
    const hasEngineSettings=hasTemplateStructureEngineSettings(s);
    if(!hasFormationCriteria && !hasEngineSettings) return rows;
    const tfs=formationTimeframeRange(s.timeframeMode||"only",s.timeframeFrom,s.timeframeTo||s.timeframe);
    return rows.filter(c=>{
        const sym=String(c.symbol||"").toUpperCase();
        const analyses=tfs.map(tf=>templateAnalysisCache.get(`${sym}|${tf}`)).filter(Boolean);
        return analyses.some(a=>{
            // A structure-engine template is a real screener: keep only coins
            // where the engine found at least one requested structure.
            // Horizontal support/resistance and trendlines are independent;
            // finding either one is enough for the coin to enter the result set.
            if(hasEngineSettings){
                const structure=a?.structure || a?.overlay || a || {};
                const flags=templateStructureSearchFlags(s);
                const requiredLevelTouches=Math.max(2,Number(s.horizontalLevelTouches||2));
                const hasSupport=Number.isFinite(Number(a?.support)) && Number(a?.support_touches||0)>=requiredLevelTouches;
                const hasResistance=Number.isFinite(Number(a?.resistance)) && Number(a?.resistance_touches||0)>=requiredLevelTouches;
                const hasHorizontal=hasSupport || hasResistance;
                const hasTrendline = !!(a?.upper || a?.lower ||
                    (Array.isArray(a?.upper_trendlines) && a.upper_trendlines.length) ||
                    (Array.isArray(a?.lower_trendlines) && a.lower_trendlines.length) ||
                    (Array.isArray(structure?.upper_lines) && structure.upper_lines.length) ||
                    (Array.isArray(structure?.lower_lines) && structure.lower_lines.length));
                const cascadeLevels=Array.isArray(a?.cascade_levels) ? a.cascade_levels : Array.isArray(structure?.cascade_levels) ? structure.cascade_levels : [];
                const hasCascade=String(a?.level_cascade||structure?.level_cascade||"") === "exists" && cascadeLevels.length >= 2;
                const matched = (flags.horizontal && hasHorizontal) || (flags.trendline && hasTrendline) || (flags.cascade && hasCascade);
                if(!matched) return false;
            }
            if(!hasFormationCriteria) return true;
            if(s.pattern && a.pattern!==s.pattern) return false;
            if(s.patternType && a.pattern_type!==s.patternType) return false;
            if(Number(s.minTouches||0)>0 && Number(a.touches||0)<Number(s.minTouches)) return false;
            if(s.consolidationMin!=null && Number(a.consolidation_duration_hours||0)<Number(s.consolidationMin)) return false;
            if(s.distanceMax!=null && (a.distance_to_breakout==null || Number(a.distance_to_breakout)>Number(s.distanceMax)) ) return false;
            if(s.levelType && a.level_type!==s.levelType) return false;
            if(Number(s.levelMinTouches||0)>0 && Number(a.level_touches||0)<Number(s.levelMinTouches)) return false;
            if(s.levelCascade && a.level_cascade!==s.levelCascade) return false;
            if(Number(s.cascadeMinVertices||0)>0 && Number(a.cascade_vertices||0)<Number(s.cascadeMinVertices)) return false;
            if(s.cascadeDistanceMax!=null && (a.cascade_distance_pct==null || Number(a.cascade_distance_pct)>Number(s.cascadeDistanceMax)) ) return false;
            if(!templateStageMatches(a.stage,s.stage)) return false;
            return true;
        });
    });
}

function applyTemplateSort(rows,t) {
    const sort=(t.sort||[]).filter(x=>x.column);
    if(!sort.length) return rows;
    return rows.slice().sort((a,b)=>{
        for(const rule of sort){
            const key=rule.column, dir=rule.direction==="desc"?-1:1;
            const getAnalysis=coin=>{const sym=String(coin?.symbol||"").toUpperCase(), st=t.structure||{}, tfs=formationTimeframeRange(st.timeframeMode||"only",st.timeframeFrom,st.timeframeTo||st.timeframe); return tfs.map(tf=>templateAnalysisCache.get(`${sym}|${tf}`)).find(Boolean)||null;};
            const av=key in a?Number(a[key]):Number(getAnalysis(a)?.[key]);
            const bv=key in b?Number(b[key]):Number(getAnalysis(b)?.[key]);
            if(Number.isFinite(av)&&Number.isFinite(bv)&&av!==bv) return (av<bv?-1:1)*dir;
        }
        return 0;
    });
}
async function ensureTemplateAnalysis(rows,t) {
    const s=t.structure||{};
    const metricKeys=["change","turnover","natr","trades","btcCorr","volumeSpike","volumeExpansion","spread","funding","oiChange","oiChangePct","deltaVolume","price"];
    const metricCfg=metricKeys.map(key=>getTemplateMetricFilter(t,key));
    const metricTfs=[...new Set(metricCfg.flatMap(x=>timeframeRange(x.mode,x.from,x.to)))];
    const needMetrics=metricTfs.length>0;
    const needPattern=templateAnalysisNeeded(s);
    const symbols=rows.map(x=>x.symbol).filter(Boolean).slice(0,100); if(!symbols.length)return;
    if(needMetrics){
        // Keep 1D values from the shared Market State. For other timeframes,
        // do not silently reuse 24h values: the backend must supply the actual
        // selected timeframe metric. Instantaneous fields remain usable directly.
        symbols.forEach(sym=>{
            const coin=latestCoins.find(x=>String(x.symbol).toUpperCase()===String(sym).toUpperCase());
            if(!coin)return;
            coin.__templateMetrics=coin.__templateMetrics||{};
            metricTfs.forEach(tf=>{
                const current=coin.__templateMetrics[tf]||{};
                const bid=Number(coin.bid), ask=Number(coin.ask), mid=(bid+ask)/2;
                const values={
                    price:coin.price,
                    spread_pct:Number.isFinite(mid)&&mid>0 ? (ask-bid)/mid*100 : null,
                    funding_pct:coin.funding_pct,
                    oi_change_usd:coin.oi_change_usd
                };
                if(tf === "1D") Object.assign(values,{
                    change_pct:coin.change_pct, turnover_usd:coin.volume_24h, natr:coin.natr,
                    trades:coin.trades, btc_corr:coin.btc_corr, volume_spike:coin.volume_spike,
                    oi_change_pct:coin.oi_change_pct, delta_volume_usd:coin.delta_volume_usd
                });
                Object.entries(values).forEach(([key,value])=>{
                    if(value!==undefined && value!==null && Number.isFinite(Number(value))) current[key]=Number(value);
                });
                coin.__templateMetrics[tf]=current;
            });
        });
        const metricFieldMap={change:"change_pct",turnover:"turnover_usd",natr:"natr",trades:"trades",btcCorr:"btc_corr",volumeSpike:"volume_spike",volumeExpansion:"volume_expansion_x",spread:"spread_pct",funding:"funding_pct",oiChange:"oi_change_usd",oiChangePct:"oi_change_pct",deltaVolume:"delta_volume_usd",price:"price"};
        const requiredMetricFields=metricKeys.map((key,i)=>({key,field:metricFieldMap[key],cfg:metricCfg[i]})).filter(x=>x.cfg.mode!=="off");
        const metricMissing=symbols.filter(sym=>{
            const c=latestCoins.find(x=>String(x.symbol).toUpperCase()===String(sym).toUpperCase());
            return !c?.__templateMetrics || requiredMetricFields.some(item=>{
                const tfs=timeframeRange(item.cfg.mode,item.cfg.from,item.cfg.to);
                return tfs.some(tf=>!Number.isFinite(Number(c.__templateMetrics?.[tf]?.[item.field])));
            });
        });
        if(metricMissing.length){
            try{
                const r=await fetch(`/api/template_market_analysis?symbols=${encodeURIComponent(metricMissing.join(","))}&timeframes=${encodeURIComponent(metricTfs.join(","))}&volume_base=${encodeURIComponent(Number(getTemplateMetricFilter(t,"volumeExpansion").baseCandles)||100)}&volume_growth=${encodeURIComponent(Number(getTemplateMetricFilter(t,"volumeExpansion").growthCandles)||20)}`,{cache:"no-store"});
                const data=await r.json();
                (data.results||[]).forEach(x=>{
                    const coin=latestCoins.find(c=>String(c.symbol).toUpperCase()===String(x.symbol).toUpperCase());
                    if(coin)coin.__templateMetrics=Object.assign(coin.__templateMetrics||{},x.metrics||{});
                });
            }catch(e){console.error("Template market metrics error:",e);}
        }
    }
    if(needPattern){
        const tfs=formationTimeframeRange(s.timeframeMode||"only",s.timeframeFrom,s.timeframeTo||s.timeframe);
        for(const tf of tfs){
            const missing=symbols.filter(sym=>!templateAnalysisCache.has(`${String(sym).toUpperCase()}|${tf}`)); if(!missing.length)continue;
            if(templateAnalysisBusy)break; templateAnalysisBusy=true;
            try{const r=await fetch(`/api/pattern_analysis?symbols=${encodeURIComponent(missing.join(","))}&interval=${encodeURIComponent(tf)}&limit=750&level_distance=${encodeURIComponent(Number(s.horizontalLevelSearchPeriod)||50)}&level_strict=${s.horizontalLevelSearchStrict?1:0}&level_touches=${encodeURIComponent(Number(s.horizontalLevelTouches)||2)}&level_tolerance=${encodeURIComponent(Number(s.horizontalLevelTouchTolerancePct)||0.1)}&level_lifetime=${s.horizontalLevelLifetimeHours==null?"":encodeURIComponent(s.horizontalLevelLifetimeHours)}&level_no_cross=${s.horizontalLevelNoCross?1:0}&level_minor_pierce=${s.horizontalLevelAllowMinorPierce?1:0}&trend_distance=${encodeURIComponent(Number(s.trendlineSearchPeriod)||50)}&trend_strict=${s.trendlineSearchStrict?1:0}&trend_touches=${encodeURIComponent(Number(s.trendlineTouches)||2)}&trend_tolerance=${encodeURIComponent(Number(s.trendlineTouchTolerancePct)||0.1)}&trend_lifetime=${s.trendlineLifetimeHours==null?"":encodeURIComponent(s.trendlineLifetimeHours)}&trend_no_cross=${s.trendlineNoCross?1:0}&trend_minor_pierce=${s.trendlineAllowMinorPierce?1:0}`,{cache:"no-store"});const data=await r.json();(data.results||[]).forEach(x=>templateAnalysisCache.set(`${String(x.symbol).toUpperCase()}|${tf}`,x));}
            catch(e){console.error("Pattern analysis error",e);}
            finally{templateAnalysisBusy=false;}
        }
    }
}

function setTemplateSearchStatus(text="", busy=false){
    const el=document.getElementById("templateSearchStatus"); if(!el)return;
    el.textContent=text; el.classList.toggle("busy",!!busy);
}
function hasTemplateFormationCriteria(s){
    return !!(s?.pattern || s?.patternType || s?.minTouches || s?.stage || s?.consolidationMin!=null || s?.distanceMax!=null || s?.levelType || s?.levelMinTouches || s?.levelCascade || s?.cascadeMinVertices || s?.cascadeDistanceMax!=null);
}
function templateStructureSearchFlags(s){
    const mode=String(s?.structureSearchMode||"none");
    if(mode === "all") return {horizontal:true,trendline:true,cascade:true};
    if(mode === "both") return {horizontal:true,trendline:true,cascade:false};
    if(mode === "horizontal_cascade") return {horizontal:true,trendline:false,cascade:true};
    if(mode === "trendline_cascade") return {horizontal:false,trendline:true,cascade:true};
    if(mode === "horizontal") return {horizontal:true,trendline:false,cascade:false};
    if(mode === "trendline") return {horizontal:false,trendline:true,cascade:false};
    if(mode === "cascade") return {horizontal:false,trendline:false,cascade:true};
    return {horizontal:false,trendline:false,cascade:false};
}
function hasTemplateStructureEngineSettings(s){
    const f=templateStructureSearchFlags(s);
    return f.horizontal || f.trendline || f.cascade;
}
function hasTemplateMarketFilters(t){
    const keys=["change","turnover","natr","trades","btcCorr","volumeSpike","volumeExpansion","spread","funding","oiChange","oiChangePct","deltaVolume","price"];
    return keys.some(key=>getTemplateMetricFilter(t,key).mode!=="off");
}
function templateAnalysisNeeded(s){
    return hasTemplateFormationCriteria(s) || hasTemplateStructureEngineSettings(s);
}
async function applyMarketTemplate(t) {
    if(!t) return;
    const generation=++templateApplyGeneration;
    const s=t.structure||{};
    const hasFormationCriteria=hasTemplateFormationCriteria(s);
    const hasEngineSettings=hasTemplateStructureEngineSettings(s);
    const hasMarketFilters=hasTemplateMarketFilters(t);
    const hasAnalysis=hasFormationCriteria || hasEngineSettings;
    const searchFlags=templateStructureSearchFlags(s);
    const structureLabel=[searchFlags.horizontal?"горизонтальные уровни":"",searchFlags.trendline?"наклонные линии":"",searchFlags.cascade?"каскад":""].filter(Boolean).join(" + ");
    const searchParts=[];
    if(hasMarketFilters) searchParts.push("фильтры рынка");
    if(hasFormationCriteria) searchParts.push("формации");
    if(hasEngineSettings) searchParts.push(structureLabel || "структура");
    setTemplateSearchStatus(searchParts.length ? `Ищем: ${searchParts.join(" + ")}…` : "", !!searchParts.length);
    templateAnalysisInProgress = !!(hasMarketFilters || hasAnalysis);
    if (templateAnalysisInProgress) { activeTemplateRows = []; renderTable(); }

    const base=(marketSettings.freeze||marketSettings.freezeAll)&&marketFrozenCoins?marketFrozenCoins:latestCoins;
    let rows=base.filter(c=>String(c.symbol||"").toUpperCase().endsWith("USDT"));
    await ensureTemplateAnalysis(rows,t);

    // A newer template selection owns the UI. Never let an older async
    // calculation replace its result.
    if(generation!==templateApplyGeneration || activeMarketTemplateId!==t.id) return;

    rows=applyTemplateMarketFilters(rows,t);
    rows=applyTemplateAnalysis(rows,t);
    rows=applyTemplateSort(rows,t);
    if(t.display?.limit>0) rows=rows.slice(0,Number(t.display.limit));
    activeTemplateRows=rows;
    templateAnalysisInProgress = false;
    renderTable();

    if(hasAnalysis) {
        if(selectedChartSymbol && isChartOverlayAnalysisEnabled()) {
            const chartTf=ownChartIntervalToBinance(selectedChartInterval || "1");
            const chartKey=`${String(selectedChartSymbol).toUpperCase()}|${chartTf}`;
            chartAnalysisCache.delete(chartKey);
            chartIndicatorLayerCache.delete(chartKey);
            chartAnalysisCandleCache.delete(chartKey);
            chartAnalysisStateCache.delete(chartKey);
            ensureChartOverlayAnalysis(selectedChartSymbol, chartTf).then(()=>{
                if(selectedChartSymbol) scheduleOwnChartVisualSync({drawings:true});
            });
        }
    }

    setTemplateSearchStatus(
        rows.length
            ? `Найдено монет: ${rows.length}`
            : "Ничего не найдено по заданным условиям.",
        false
    );
}
let activeTemplateRows=null;
function renderTemplateRows(){
    const t=activeMarketTemplate();
    if(!t || !activeTemplateRows) return;
    const rows=activeTemplateRows;
    coinCount.textContent=rows.length;
    renderMarketHeaders(getMarketColumns());
    coinTable.innerHTML=rows.map(c=>`<tr class="clickable-row" data-symbol="${c.symbol}">${getMarketColumns().map(k=>k==="symbol"?`<td>${renderWatchlistSymbolCell(c.symbol)}</td>`:`<td class="right">${marketValue(c,k)}</td>`).join("")}</tr>`).join("") || `<tr><td colspan="${getMarketColumns().length}" class="muted" style="text-align:center;padding:25px">Нет монет, соответствующих условиям.</td></tr>`;
}
const _renderTableBase=renderTable;
renderTable=function(){
    if (templateAnalysisInProgress) {
        const columns = getMarketColumns();
        coinCount.textContent = 0;
        renderMarketHeaders(columns);
        coinTable.innerHTML = "";
        return;
    }
    const t=activeMarketTemplate();
    if(!t || t.id==="market-default" && !["change","turnover","natr","volumeExpansion","oiChangePct"].some(k=>getTemplateMetricFilter(t,k).mode!=="off") && !t.structure?.pattern && !hasTemplateStructureEngineSettings(t.structure)) return _renderTableBase();
    // Keep search and existing column settings, but use the template result set.
    const query=searchInput.value.trim().toUpperCase();
    const columns=getMarketColumns();
    let rows=(activeTemplateRows||latestCoins).filter(c=>!query||String(c.symbol).includes(query));
    rows=applyTemplateMarketFilters(rows,t);
    // activeTemplateRows is already the result of the complete template
    // evaluation (market filters + structure analysis). Running the structure
    // filter a second time can make the table disagree with the result count.
    rows=applyTemplateSort(rows,t);
    if(t.display?.limit>0) rows=rows.slice(0,Number(t.display.limit));
    coinCount.textContent=rows.length;
    renderMarketHeaders(columns);
    coinTable.innerHTML=rows.map(c=>`<tr class="clickable-row" data-symbol="${c.symbol}">${columns.map(k=>k==="symbol"?`<td>${renderWatchlistSymbolCell(c.symbol)}</td>`:`<td class="right">${marketValue(c,k)}</td>`).join("")}</tr>`).join("") || `<tr><td colspan="${columns.length}" class="muted" style="text-align:center;padding:25px">Нет монет, соответствующих условиям.</td></tr>`;
};

const marketTemplateSearch=document.getElementById("marketTemplateSearch");
const volumeHistoryOverlay = document.getElementById("volumeHistoryOverlay");
const volumeHistorySymbol = document.getElementById("volumeHistorySymbol");
const volumeHistoryTimeframe = document.getElementById("volumeHistoryTimeframe");
const volumeHistoryMin = document.getElementById("volumeHistoryMin");
const volumeHistoryMax = document.getElementById("volumeHistoryMax");
const volumeHistoryBase = document.getElementById("volumeHistoryBase");
const volumeHistoryGrowth = document.getElementById("volumeHistoryGrowth");
const volumeHistoryDirection = document.getElementById("volumeHistoryDirection");
const volumeHistoryPriceMin = document.getElementById("volumeHistoryPriceMin");
const volumeHistoryPriceMax = document.getElementById("volumeHistoryPriceMax");
const volumeHistoryOiDirection = document.getElementById("volumeHistoryOiDirection");
const volumeHistoryOiMin = document.getElementById("volumeHistoryOiMin");
const volumeHistoryOiMax = document.getElementById("volumeHistoryOiMax");
const volumeHistoryStatus = document.getElementById("volumeHistoryStatus");
const volumeHistoryResults = document.getElementById("volumeHistoryResults");
const VOLUME_HISTORY_STORAGE_KEY = "cryptoScreenerVolumeHistoryResults";
let volumeHistorySavedResults = [];
let volumeHistorySavedMeta = null;

function volumeHistoryRestoreSaved(){
    try{
        const raw=localStorage.getItem(VOLUME_HISTORY_STORAGE_KEY);
        if(!raw)return false;
        const saved=JSON.parse(raw);
        if(!saved || !Array.isArray(saved.results))return false;
        volumeHistorySavedResults=saved.results;
        volumeHistorySavedMeta=saved.meta||null;
        return true;
    }catch(_){ return false; }
}
function volumeHistoryPersistSaved(results, meta){
    volumeHistorySavedResults=Array.isArray(results)?results:[];
    volumeHistorySavedMeta=meta||null;
    try{
        localStorage.setItem(VOLUME_HISTORY_STORAGE_KEY, JSON.stringify({
            results:volumeHistorySavedResults,
            meta:volumeHistorySavedMeta
        }));
    }catch(_){}
}
function renderVolumeHistoryResults(results){
    const list=Array.isArray(results)?results:[];
    if(!volumeHistoryResults)return;
    if(!list.length){
        volumeHistoryResults.innerHTML='<div class="volume-history-empty">Событий не найдено.</div>';
        return;
    }
    volumeHistoryResults.innerHTML=list.map((x,i)=>`<div class="volume-history-result" data-volume-history-index="${i}"><div><div class="volume-history-result-head">Монета</div><div class="volume-history-result-main">${escapeHtml(displayAlertSymbol(x.symbol))}</div></div><div><div class="volume-history-result-head">Время</div><div class="volume-history-result-meta">${escapeHtml(volumeHistoryFormatTime(x.time))}</div></div><div><div class="volume-history-result-head">Всплеск</div><div class="volume-history-result-x">${Number(x.ratio).toFixed(2)}x</div></div><div><div class="volume-history-result-head">Объём</div><div class="volume-history-result-meta">${x.direction==="up"?"Рост":x.direction==="down"?"Падение":"Любое"}</div></div><div><div class="volume-history-result-head">Цена</div><div class="volume-history-result-meta">${Number.isFinite(Number(x.price_change_pct))?(Number(x.price_change_pct)>=0?"+":"")+Number(x.price_change_pct).toFixed(2)+"%":"—"}</div></div><div><div class="volume-history-result-head">OI</div><div class="volume-history-result-meta">${Number.isFinite(Number(x.oi_change_pct))?(Number(x.oi_change_pct)>=0?"+":"")+Number(x.oi_change_pct).toFixed(2)+"%":"—"}</div></div></div>`).join("");
    volumeHistoryResults.querySelectorAll("[data-volume-history-index]").forEach(el=>el.addEventListener("click",()=>{
        const x=list[Number(el.dataset.volumeHistoryIndex)];
        if(!x)return;
        volumeExpansionHighlightZones=[{start:Number(x.start_time),end:Number(x.end_time)}];
        pendingOwnChartHistoryAnchor={symbol:String(x.symbol).toUpperCase(),interval:volumeHistoryInterval(x.timeframe || volumeHistoryTimeframe?.value || "5m"),start:Number(x.start_time),end:Number(x.end_time),time:Number(x.time)};
        volumeHistoryClose();
        openChart(x.symbol,volumeHistoryInterval(x.timeframe || volumeHistoryTimeframe?.value || "5m"));
    }));
}
function volumeHistoryOpen(){
    const cfg=getTemplateMetricFilter(activeMarketTemplate(),"volumeExpansion");
    if(volumeHistoryTimeframe) volumeHistoryTimeframe.value=cfg.from || cfg.to || "5m";
    if(volumeHistoryMin) volumeHistoryMin.value=cfg.min ?? 10;
    if(volumeHistoryMax) volumeHistoryMax.value=cfg.max ?? "";
    if(volumeHistoryBase) volumeHistoryBase.value=cfg.baseCandles ?? 100;
    if(volumeHistoryGrowth) volumeHistoryGrowth.value=cfg.growthCandles ?? 20;
    if(volumeHistoryDirection) volumeHistoryDirection.value="any";
    if(volumeHistoryPriceMin) volumeHistoryPriceMin.value="";
    if(volumeHistoryPriceMax) volumeHistoryPriceMax.value="";
    if(volumeHistoryOiDirection) volumeHistoryOiDirection.value="any";
    if(volumeHistoryOiMin) volumeHistoryOiMin.value="";
    if(volumeHistoryOiMax) volumeHistoryOiMax.value="";
    if(volumeHistorySymbol) volumeHistorySymbol.value="";
    volumeHistoryRestoreSaved();
    if(volumeHistorySavedResults.length){
        renderVolumeHistoryResults(volumeHistorySavedResults);
        if(volumeHistoryStatus){
            const meta=volumeHistorySavedMeta;
            volumeHistoryStatus.textContent=meta?.status || `Сохранено: ${volumeHistorySavedResults.length}.`;
        }
    }else{
        if(volumeHistoryStatus) volumeHistoryStatus.textContent="";
        if(volumeHistoryResults) volumeHistoryResults.innerHTML='<div class="template-muted">Поиск ещё не запускался.</div>';
    }
    volumeHistoryOverlay?.classList.add("open"); volumeHistoryOverlay?.setAttribute("aria-hidden","false");
}
function volumeHistoryClose(){volumeHistoryOverlay?.classList.remove("open");volumeHistoryOverlay?.setAttribute("aria-hidden","true");}
function volumeHistoryFormatTime(sec){const d=new Date(Number(sec)*1000);return Number.isFinite(d.getTime())?d.toLocaleString([], {day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"}):"—";}
function volumeHistoryInterval(tf){return ({"1m":"1","5m":"5","15m":"15","30m":"30","1h":"60","4h":"240","1D":"1D"})[tf]||"5";}
async function searchVolumeHistory(){
    const symbolText=String(volumeHistorySymbol?.value||"").trim().toUpperCase();
    let symbols=[];
    if(symbolText){
        symbols=[symbolText.endsWith("USDT")?symbolText:symbolText+"USDT"];
    }else{
        symbols=latestCoins.map(c=>String(c.symbol||"").toUpperCase()).filter(x=>x.endsWith("USDT")).slice(0,200);
    }
    const payload={symbols,timeframe:volumeHistoryTimeframe?.value||"5m",min:volumeHistoryMin?.value||"",max:volumeHistoryMax?.value||"",direction:volumeHistoryDirection?.value||"any",priceMin:volumeHistoryPriceMin?.value||"",priceMax:volumeHistoryPriceMax?.value||"",oiDirection:volumeHistoryOiDirection?.value||"any",oiMin:volumeHistoryOiMin?.value||"",oiMax:volumeHistoryOiMax?.value||"",baseCandles:Number(volumeHistoryBase?.value||100),growthCandles:Number(volumeHistoryGrowth?.value||20),limit:200};
    if(!symbols.length){if(volumeHistoryStatus)volumeHistoryStatus.textContent="Нет доступных монет.";return;}
    if(volumeHistoryStatus) volumeHistoryStatus.textContent=`Ищем по ${symbols.length} монетам…`;
    try{
        const response=await fetch("/api/volume_expansion_history",{method:"POST",headers:{"Content-Type":"application/json"},cache:"no-store",body:JSON.stringify(payload)});
        const data=await response.json().catch(()=>({}));
        if(!response.ok) throw new Error(data.error||"Ошибка поиска");
        const results=Array.isArray(data.results)?data.results:[];
        const resultStatus=`Проверено: ${data.scanned||symbols.length}. Найдено: ${results.length}.`;
        if(volumeHistoryStatus) volumeHistoryStatus.textContent=resultStatus;
        volumeHistoryPersistSaved(results,{status:resultStatus,time:Date.now(),timeframe:payload.timeframe});
        renderVolumeHistoryResults(results);
    }catch(error){
        if(volumeHistoryStatus) volumeHistoryStatus.textContent="Ошибка поиска";
        if(volumeHistoryResults) volumeHistoryResults.innerHTML=`<div class="volume-history-empty">${escapeHtml(error?.message||"Ошибка поиска")}</div>`;
    }
}
document.getElementById("volumeHistoryButton")?.addEventListener("click",volumeHistoryOpen);
document.getElementById("volumeHistoryClose")?.addEventListener("click",volumeHistoryClose);
document.getElementById("volumeHistorySearch")?.addEventListener("click",searchVolumeHistory);
volumeHistoryOverlay?.addEventListener("click",e=>{if(e.target===volumeHistoryOverlay)volumeHistoryClose();});

let marketTemplateSearchIndex=-1;
let marketTemplateSearchOriginal="";
let marketTemplateSearchCommitted=false;
let marketTemplateSearchEditing=false;
let marketTemplateSearchRestoreTimer=null;
function filterMarketTemplates(query){const q=String(query||"").trim().toLowerCase();return marketTemplates.filter(t=>!q||String(t.name||"").toLowerCase().includes(q));}
function renderMarketTemplateSearchResults(){
    const menu=document.getElementById("marketTemplateMenu"), matches=filterMarketTemplates(marketTemplateSearch?.value||"");
    if(!menu)return;
    if(marketTemplateSearchIndex>=matches.length) marketTemplateSearchIndex=matches.length-1;
    menu.innerHTML=matches.map((t,i)=>`<div class="market-template-item ${t.id===activeMarketTemplateId?"active ":""}${i===marketTemplateSearchIndex?"keyboard-active":""}" data-template-id="${t.id}"><span class="name">${escapeHtml(t.name)}</span><button class="edit" type="button" data-template-edit="${t.id}" title="Изменить">⚙</button></div>`).join("")||`<div class="template-muted" style="padding:9px">Обзор не найден</div>`;
    const add=document.createElement("button"); add.id="marketTemplateAdd"; add.className="market-template-add"; add.type="button"; add.textContent="＋ Новый шаблон"; menu.appendChild(add);
    menu.classList.add("open");
    const active=menu.querySelector(".keyboard-active"); if(active) active.scrollIntoView({block:"nearest"});
}
function restoreMarketTemplateSearch(){
    if(!marketTemplateSearch) return;
    marketTemplateSearch.value=activeMarketTemplate()?.name || marketTemplateSearchOriginal || "";
    marketTemplateSearchCommitted=false;
    marketTemplateSearchEditing=false;
    marketTemplateSearchIndex=-1;
}
function beginMarketTemplateSearchEdit(){
    if(!marketTemplateSearch) return;
    if(marketTemplateSearchRestoreTimer) { clearTimeout(marketTemplateSearchRestoreTimer); marketTemplateSearchRestoreTimer=null; }
    marketTemplateSearchOriginal=activeMarketTemplate()?.name || marketTemplateSearch.value || "";
    marketTemplateSearchCommitted=false;
    marketTemplateSearchEditing=true;
    marketTemplateSearch.value="";
    marketTemplateSearchIndex=-1;
    renderMarketTemplateSearchResults();
    requestAnimationFrame(()=>marketTemplateSearch?.focus());
}
marketTemplateSearch?.addEventListener("focus",()=>{
    if(!marketTemplateSearchEditing) beginMarketTemplateSearchEdit();
});
marketTemplateSearch?.addEventListener("input",()=>{marketTemplateSearchIndex=-1;renderMarketTemplateSearchResults();});
marketTemplateSearch?.addEventListener("keydown",e=>{
    if(!["ArrowDown","ArrowUp","Enter","Escape"].includes(e.key))return;
    const matches=filterMarketTemplates(marketTemplateSearch.value);
    if(e.key==="Escape"){e.preventDefault();restoreMarketTemplateSearch();document.getElementById("marketTemplateMenu")?.classList.remove("open");marketTemplateSearch.blur();return;}
    if(!matches.length)return;
    e.preventDefault();
    if(e.key==="ArrowDown") marketTemplateSearchIndex=(marketTemplateSearchIndex+1)%matches.length;
    else if(e.key==="ArrowUp") marketTemplateSearchIndex=(marketTemplateSearchIndex-1+matches.length)%matches.length;
    else if(e.key==="Enter"){
        if(marketTemplateSearchIndex<0) marketTemplateSearchIndex=0;
        const selected=matches[marketTemplateSearchIndex];
        setActiveMarketTemplate(selected.id);
        marketTemplateSearch.value=selected.name;
        marketTemplateSearchOriginal=selected.name;
        marketTemplateSearchCommitted=true;
        marketTemplateSearchEditing=false;
        document.getElementById("marketTemplateMenu")?.classList.remove("open");
        marketTemplateSearch.blur();
        return;
    }
    renderMarketTemplateSearchResults();
});
marketTemplateSearch?.addEventListener("blur",()=>{
    marketTemplateSearchRestoreTimer=setTimeout(()=>{
        marketTemplateSearchRestoreTimer=null;
        if(!marketTemplateSearchCommitted) restoreMarketTemplateSearch();
        document.getElementById("marketTemplateMenu")?.classList.remove("open");
    },120);
});
document.getElementById("marketTemplateButton")?.addEventListener("click",e=>{
    e.stopPropagation();
    beginMarketTemplateSearchEdit();
});
document.getElementById("marketTemplateButton")?.addEventListener("keydown",e=>{
    if(!["ArrowDown","ArrowUp","Enter"].includes(e.key))return;
    e.preventDefault();
    marketTemplateSearch?.focus();
    marketTemplateSearch?.dispatchEvent(new KeyboardEvent("keydown",{key:e.key,bubbles:true}));
});
document.getElementById("marketTemplateMenu")?.addEventListener("click",e=>{
    const edit=e.target.closest("[data-template-edit]"); if(edit){e.stopPropagation();openTemplateEditor(edit.dataset.templateEdit);return;}
    const item=e.target.closest("[data-template-id]");
    if(item){
        setActiveMarketTemplate(item.dataset.templateId);
        const selected=activeMarketTemplate();
        marketTemplateSearchOriginal=selected?.name || "";
        marketTemplateSearchCommitted=true;
        marketTemplateSearchEditing=false;
        if(marketTemplateSearch) marketTemplateSearch.value=selected?.name || "";
        document.getElementById("marketTemplateMenu").classList.remove("open");
        return;
    }
    if(e.target.closest("#marketTemplateAdd")){openTemplateEditor();}
});
document.addEventListener("click",e=>{
    if(!e.target.closest("#marketTemplateSearch,#marketTemplateButton,#marketTemplateMenu")){
        document.getElementById("marketTemplateMenu")?.classList.remove("open");
        if(document.activeElement!==marketTemplateSearch) restoreMarketTemplateSearch();
    }
});
// Template timeframe strict toggles: one field in strict mode, From/To otherwise.
document.getElementById("templateTimeframeStrict")?.addEventListener("change",()=>{
    const cb=document.getElementById("templateTimeframeStrict"), single=document.getElementById("templateTimeframeSingle"), from=document.getElementById("templateTimeframeFrom"), to=document.getElementById("templateTimeframeTo");
    if(cb.checked){ single.value=from.value||to.value||single.value||""; }
    else { from.value=single.value||from.value||""; to.value=single.value||to.value||""; }
    setTemplateTimeframeUI({timeframeMode:cb.checked?"only":"range",timeframe:single.value,timeframeFrom:from.value,timeframeTo:to.value});
});
["Change","Turnover","Natr","Trades","BtcCorr","VolumeSpike","VolumeExpansion","Spread","Funding","OiChange","OiChangePct","DeltaVolume","Price"].forEach(prefix=>{
    document.getElementById(`template${prefix}Strict`)?.addEventListener("change",()=>{
        const cb=document.getElementById(`template${prefix}Strict`), single=document.getElementById(`template${prefix}Single`), from=document.getElementById(`template${prefix}From`), to=document.getElementById(`template${prefix}To`);
        if(cb.checked){ single.value=from.value||to.value||single.value||""; }
        else { from.value=single.value||from.value||""; to.value=single.value||to.value||""; }
        updateTemplateMetricTimeframeUI(prefix);
    });
});
document.addEventListener("click",e=>{if(!e.target.closest("#marketTemplateSearch,#marketTemplateButton,#marketTemplateMenu"))document.getElementById("marketTemplateMenu")?.classList.remove("open");});
document.getElementById("templateClose")?.addEventListener("click",closeTemplateEditor);
document.getElementById("templateOverlay")?.addEventListener("click",e=>{if(e.target.id==="templateOverlay")closeTemplateEditor();});
document.getElementById("templateSave")?.addEventListener("click",()=>saveTemplate(false));
document.getElementById("templateSaveAs")?.addEventListener("click",()=>saveTemplate(true));
initFormationFilterSettings();
document.getElementById("templateDelete")?.addEventListener("click",deleteTemplate);
document.getElementById("templatePattern")?.addEventListener("change",e=>updateTemplatePatternTypes(e.target.value));
renderMarketTemplateMenu();
if (marketTemplateSearch) marketTemplateSearch.value = activeMarketTemplate()?.name || "";
// Activate a saved non-default template after the existing page has initialized.
if(activeMarketTemplateId!=="market-default") setTimeout(()=>applyMarketTemplate(activeMarketTemplate()),100);

ensureExtendedMarketTimeframes();
initScreenerEngineSettings();
resolveScreenerEngine().then(() => loadState()).catch(() => loadState());
// Reconcile the complete current Screener signal-level set on startup.
// This clears stale server rows from previous tests (for example an old
// SOLUSDT level) when the current Screener has no such signal level.
setTimeout(() => syncSignalLevelsToAlertServer(), 1500);
setInterval(async () => {
    if (!screenerEngineSettings.renderUrl || screenerEngineSettings.mode === "local") return;
    const online = await checkRenderHealth();
    if (online && activeScreenerEngine !== "render") {
        await resolveScreenerEngine(true);
        loadState();
    } else if (!online && activeScreenerEngine === "render") {
        await fallbackToLocalIfRenderUnavailable("health check");
    }
}, 10000);
setInterval(loadState, 500);
</script>

</body>
</html>
"""


# ============================================================
# STARTUP
# ============================================================

def open_browser():
    time.sleep(1.0)
    webbrowser.open(f"http://{HOST}:{PORT}")


def main():
    initialize_levels()

    print("=" * 60)
    print("Crypto Screener — Density Render Gate + Chart Workspace Stage 9")
    print("=" * 60)
    print(f"Web: http://{HOST}:{PORT}")
    print("Market data: Adaptive Order Book + Density Engine + Density Presentation (Binance / Bybit / OKX)")
    print("Press Ctrl+C to stop.")
    print("=" * 60)

    thread = threading.Thread(
        target=binance_worker,
        daemon=True,
    )
    thread.start()

    rest_thread = threading.Thread(
        target=binance_rest_fallback_worker,
        daemon=True,
    )
    rest_thread.start()

    # Density is strictly on-demand. Do NOT create any Density/order-book
    # worker here. The UI runtime toggle starts the pipeline only when the
    # user actually enables the Density map.

    buffer_thread = threading.Thread(
        target=screener_buffer_worker,
        daemon=True,
        name="screener-buffer",
    )
    buffer_thread.start()

    analytics_thread = threading.Thread(
        target=analytics_worker,
        daemon=True,
    )
    analytics_thread.start()

    if SCREENER_WORKER_MODE:
        print("Screener Worker mode: Render API enabled; browser auto-open disabled.")
    # Do not open the UI while NATR/correlation are still filling in.
    # The first analytics batch runs in parallel; after it is complete, the
    # browser opens with the metrics already populated.

    if not SCREENER_WORKER_MODE:
        browser_thread = threading.Thread(
            target=open_browser,
            daemon=True,
        )
        browser_thread.start()

    app.run(
        host=HOST,
        port=PORT,
        debug=False,
        threaded=True,
        use_reloader=False,
    )


if __name__ == "__main__":
    main()
