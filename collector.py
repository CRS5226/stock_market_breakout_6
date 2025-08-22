# collector.py (tick collector + 1min candle builder with per-minute volume)
import os
import json
import logging
import argparse
from kiteconnect import KiteTicker
from dotenv import load_dotenv
from redis_utils import (
    get_redis,
    floor_minute,
    set_current_candle,
    finalize_and_roll_new_candle,
)

load_dotenv()

API_KEY = os.getenv("KITE_API_KEY")
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

CONFIG_FILE = "config400.json"  # 🔑 All 400 stocks here
r = get_redis()


def load_config():
    try:
        with open(CONFIG_FILE, "r") as f:
            data = json.load(f)
            return data.get("stocks", [])
    except Exception as e:
        print(f"[Config Error] Failed to load {CONFIG_FILE}: {e}")
        return []


def start_collector(stock_configs, shard_index=0, shard_count=1):
    logging.basicConfig(level=logging.INFO, force=True)

    # split work by shard
    stock_configs = [
        s for i, s in enumerate(stock_configs) if i % shard_count == shard_index
    ]
    instrument_map = {
        s["instrument_token"]: s["stock_code"].upper() for s in stock_configs
    }

    # in-memory candle state per stock
    candles_state = {
        code: {"prev_minute": None, "current": None, "last_tick_volume": None}
        for code in instrument_map.values()
    }

    kws = KiteTicker(API_KEY, ACCESS_TOKEN)

    def on_ticks(ws, ticks):
        for tick in ticks:
            token = tick.get("instrument_token")
            code = instrument_map.get(token)
            if not code:
                continue

            ts = tick.get("exchange_timestamp")
            if not ts:
                continue

            price = float(tick.get("last_price"))
            cum_vol = int(tick.get("volume_traded"))  # cumulative daily volume
            minute_key = floor_minute(ts)

            state = candles_state[code]

            # calculate delta volume
            if state["last_tick_volume"] is None:
                delta_vol = 0
            else:
                delta_vol = max(0, cum_vol - state["last_tick_volume"])
            state["last_tick_volume"] = cum_vol

            # new candle
            if state["prev_minute"] is None or minute_key != state["prev_minute"]:
                if state["current"]:
                    # finalize previous candle into Redis
                    finalize_and_roll_new_candle(
                        r, code, state["current"], max_candles=100
                    )

                # start new candle
                state["current"] = {
                    "minute": str(minute_key),
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": delta_vol,
                }
                state["prev_minute"] = minute_key
            else:
                # update current candle
                state["current"]["high"] = max(state["current"]["high"], price)
                state["current"]["low"] = min(state["current"]["low"], price)
                state["current"]["close"] = price
                state["current"]["volume"] += delta_vol

            # save snapshot in Redis
            set_current_candle(
                r,
                code,
                minute_key,
                state["current"]["open"],
                state["current"]["high"],
                state["current"]["low"],
                state["current"]["close"],
                state["current"]["volume"],
            )

            print(f"[{code}] Tick saved")

    def on_connect(ws, response):
        tokens = list(instrument_map.keys())
        print(
            f"✅ Shard {shard_index + 1}/{shard_count}: Connected — Subscribing to {len(tokens)} tokens"
        )
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)

    def on_close(ws, code, reason):
        print(f"❌ Shard {shard_index + 1}: Disconnected {code} - {reason}")

    def on_error(ws, code, reason):
        print(f"⚠️ Shard {shard_index + 1}: Error {code} - {reason}")

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error

    kws.connect(threaded=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--shards", type=int, default=2, help="Number of shards")
    parser.add_argument("--index", type=int, default=0, help="Shard index (0-based)")
    args = parser.parse_args()

    stock_list = load_config()
    if not stock_list:
        print("❌ No stocks loaded. Exiting.")
        exit(1)

    print(
        f"🚀 Collector starting with {len(stock_list)} stocks... Shard {args.index + 1}/{args.shards}"
    )
    start_collector(stock_list, shard_index=args.index, shard_count=args.shards)
