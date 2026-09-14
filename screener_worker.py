"""Render Screener Worker entrypoint.

Runs only the server-side market/screener engine from app274.py:
Binance market data, fallback, 25h buffer, rolling metrics and HTTP API.
The local UI/chart runtime is not started.
"""

import threading

from app274 import (
    app,
    HOST,
    PORT,
    binance_worker,
    binance_rest_fallback_worker,
    screener_buffer_worker,
)


def main():
    print("=" * 60)
    print("Crypto Screener — Render Screener Worker")
    print("=" * 60)
    print(f"API: http://{HOST}:{PORT}")
    print("Mode: server-side Screener Engine only")
    print("UI/browser/chart runtime: disabled")
    print("=" * 60)

    threading.Thread(
        target=binance_worker,
        daemon=True,
        name="binance-market-ws",
    ).start()

    threading.Thread(
        target=binance_rest_fallback_worker,
        daemon=True,
        name="binance-rest-fallback",
    ).start()

    threading.Thread(
        target=screener_buffer_worker,
        daemon=True,
        name="screener-buffer",
    ).start()

    app.run(
        host=HOST,
        port=PORT,
        debug=False,
        threaded=True,
        use_reloader=False,
    )


if __name__ == "__main__":
    main()
