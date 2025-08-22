import redis
import json
import pandas as pd
import os


# --- Redis connection ---
def get_redis():
    return redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def fetch_candles(stock_code: str, n: int = 2):
    """Fetch last N candles from Redis (chronological order)."""
    r = get_redis()
    key = f"candles:{stock_code.upper()}"
    items = r.lrange(key, 0, n - 1)  # newest-first
    if not items:
        print(f"[⚠️] No candles found in Redis for {stock_code}")
        return None
    rows = [json.loads(x) for x in items]
    df = pd.DataFrame(rows)
    df["minute"] = pd.to_datetime(df["minute"])
    return df.sort_values("minute").reset_index(drop=True)


def fetch_indicators(stock_code: str, n: int = 2):
    """Fetch last N indicators from Redis (newest first)."""
    r = get_redis()
    key = f"indicators:{stock_code.upper()}"
    items = r.lrange(key, 0, n - 1)
    if not items:
        print(f"[⚠️] No indicators found in Redis for {stock_code}")
        return None
    rows = [json.loads(x) for x in items]
    return pd.DataFrame(rows)


def fetch_historical(stock_code: str, folder="historical_data"):
    """Load the latest historical CSV for a stock."""
    if not os.path.isdir(folder):
        print(f"[⚠️] No historical_data folder found: {folder}")
        return None

    hist_csv = None
    for file in os.listdir(folder):
        if file.startswith(f"{stock_code}_historical_") and file.endswith(".csv"):
            hist_csv = os.path.join(folder, file)
            break

    if hist_csv and os.path.exists(hist_csv):
        try:
            df = pd.read_csv(hist_csv)
            return df
        except Exception as e:
            print(f"[❌] Error loading historical CSV {hist_csv}: {e}")
    else:
        print(f"[⚠️] No historical file found for {stock_code} in {folder}")
    return None


if __name__ == "__main__":
    stock = "MMFL"

    # df_candles = fetch_candles(stock, n=5)
    # if df_candles is not None:
    #     print(f"[🕒] Redis Candles columns for {stock}: {list(df_candles.columns)}")

    df_indicators = fetch_indicators(stock, n=5)
    if df_indicators is not None:
        print(
            f"[📈] Redis Indicators columns for {stock}: {list(df_indicators.columns)}",
            df_indicators.head(),
        )

    # df_hist = fetch_historical(stock)
    # if df_hist is not None:
    #     print(f"[📚] Historical CSV columns for {stock}: {list(df_hist.columns)}")
