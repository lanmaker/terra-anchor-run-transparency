import io
import os
import zipfile
from datetime import datetime

import pandas as pd
import requests

from src.config import RAW_DIR, WINDOW_END, WINDOW_START

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/{id}/market_chart/range"
COINCAP_URL = "https://api.coincap.io/v2/assets/{id}/history"
COINPAPRIKA_URL = "https://api.coinpaprika.com/v1/tickers/{id}/historical"
CRYPTOCOMPARE_URL = "https://min-api.cryptocompare.com/data/v2/histohour"
BINANCE_URL = "https://api.binance.com/api/v3/klines"
BINANCE_DATA_BASE = "https://data.binance.vision/data/spot/monthly/klines"


def _to_unix(dt: str) -> int:
    return int(datetime.fromisoformat(dt).timestamp())


def _fetch_coincap(start_ts: int, end_ts: int) -> pd.DataFrame:
    asset_id = os.environ.get("COINCAP_ID", "terrausd")
    params = {
        "interval": "h1",
        "start": start_ts * 1000,
        "end": end_ts * 1000,
    }
    resp = requests.get(COINCAP_URL.format(id=asset_id), params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json().get("data", [])
    if not data:
        raise RuntimeError("No data returned from CoinCap")
    df = pd.DataFrame(data)
    df["hour"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.floor("h")
    df["price"] = pd.to_numeric(df["priceUsd"], errors="coerce")
    return df[["hour", "price"]].dropna()


def _fetch_coinpaprika(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    ticker_id = os.environ.get("COINPAPRIKA_ID", "ust-terrausd")
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    params = {"start": start_iso, "end": end_iso, "interval": "1h"}
    resp = requests.get(COINPAPRIKA_URL.format(id=ticker_id), params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        raise RuntimeError("No data returned from CoinPaprika")
    df = pd.DataFrame(data)
    df["hour"] = pd.to_datetime(df["timestamp"], utc=True).dt.floor("h")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    return df[["hour", "price"]].dropna()


def _fetch_coingecko(start_ts: int, end_ts: int) -> pd.DataFrame:
    coin_id = os.environ.get("COINGECKO_ID", "terrausd")
    vs = os.environ.get("COINGECKO_VS", "usd")
    params = {"vs_currency": vs, "from": start_ts, "to": end_ts}
    headers = {}
    api_key = os.environ.get("COINGECKO_API_KEY")
    if api_key:
        headers["x_cg_pro_api_key"] = api_key
    url = COINGECKO_URL.format(id=coin_id)
    resp = requests.get(url, params=params, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    prices = data.get("prices")
    if not prices:
        raise RuntimeError("No prices returned from Coingecko")
    df = pd.DataFrame(prices, columns=["timestamp", "price"])
    df["hour"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.floor("h")
    return df[["hour", "price"]].dropna()


def _fetch_cryptocompare(start_ts: int, end_ts: int) -> pd.DataFrame:
    fsym = os.environ.get("CRYPTOCOMPARE_FSYM", "USTC")
    tsym = os.environ.get("CRYPTOCOMPARE_TSYM", "USD")
    all_rows = []
    remaining_end = end_ts
    while remaining_end >= start_ts:
        hours = int((remaining_end - start_ts) / 3600)
        limit = min(hours, 2000)
        params = {"fsym": fsym, "tsym": tsym, "limit": limit, "toTs": remaining_end}
        resp = requests.get(CRYPTOCOMPARE_URL, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("Response") != "Success":
            raise RuntimeError(payload.get("Message", "CryptoCompare error"))
        data = payload.get("Data", {}).get("Data", [])
        if not data:
            break
        all_rows.extend(data)
        earliest = min(row["time"] for row in data)
        remaining_end = earliest - 3600
        if limit < 2000:
            break
    if not all_rows:
        raise RuntimeError("No data returned from CryptoCompare")
    df = pd.DataFrame(all_rows)
    df["hour"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.floor("h")
    df["price"] = pd.to_numeric(df["close"], errors="coerce")
    return df[["hour", "price"]].dropna()


def _fetch_binance(start_ts: int, end_ts: int) -> pd.DataFrame:
    try:
        return _fetch_binance_api(start_ts, end_ts)
    except Exception:
        return _fetch_binance_vision(start_ts, end_ts)


def _fetch_binance_api(start_ts: int, end_ts: int) -> pd.DataFrame:
    symbol = os.environ.get("BINANCE_SYMBOL", "USTUSDT")
    interval = os.environ.get("BINANCE_INTERVAL", "1h")
    start_ms = start_ts * 1000
    end_ms = end_ts * 1000
    limit = 1000
    rows = []
    current = start_ms
    while current <= end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current,
            "endTime": end_ms,
            "limit": limit,
        }
        resp = requests.get(BINANCE_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        rows.extend(data)
        last_open = data[-1][0]
        next_time = last_open + 60 * 60 * 1000
        if next_time <= current:
            break
        current = next_time
        if len(data) < limit:
            break
    if not rows:
        raise RuntimeError("No data returned from Binance")
    df = pd.DataFrame(
        rows,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_base",
            "taker_quote",
            "ignore",
        ],
    )
    df["hour"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.floor("h")
    df["price"] = pd.to_numeric(df["close"], errors="coerce")
    return df[["hour", "price"]].dropna()


def _fetch_binance_vision(start_ts: int, end_ts: int) -> pd.DataFrame:
    symbol = os.environ.get("BINANCE_SYMBOL", "USTUSDT")
    interval = os.environ.get("BINANCE_INTERVAL", "1h")
    base = os.environ.get("BINANCE_DATA_BASE", BINANCE_DATA_BASE)
    start_dt = datetime.fromtimestamp(start_ts)
    end_dt = datetime.fromtimestamp(end_ts)
    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trades",
        "taker_base",
        "taker_quote",
        "ignore",
    ]
    frames = []
    year, month = start_dt.year, start_dt.month
    while (year, month) <= (end_dt.year, end_dt.month):
        file_name = f"{symbol}-{interval}-{year:04d}-{month:02d}.zip"
        url = f"{base}/{symbol}/{interval}/{file_name}"
        resp = requests.get(url, timeout=60)
        if resp.status_code == 404:
            raise RuntimeError(f"Missing Binance data file: {file_name}")
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_name = zf.namelist()[0]
            data = pd.read_csv(zf.open(csv_name), header=None, names=columns)
        frames.append(data)
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    df = pd.concat(frames, ignore_index=True)
    start_ms = start_ts * 1000
    end_ms = end_ts * 1000
    df = df[(df["open_time"] >= start_ms) & (df["open_time"] <= end_ms)]
    df["hour"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.floor("h")
    df["price"] = pd.to_numeric(df["close"], errors="coerce")
    return df[["hour", "price"]].dropna()


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    start_ts = _to_unix(WINDOW_START)
    end_ts = _to_unix(WINDOW_END)
    start_dt = datetime.fromisoformat(WINDOW_START)
    end_dt = datetime.fromisoformat(WINDOW_END)

    sources = os.environ.get(
        "PRICE_SOURCES", "binance,cryptocompare,coincap,coinpaprika,coingecko"
    ).split(",")
    fetchers = {
        "binance": lambda: _fetch_binance(start_ts, end_ts),
        "cryptocompare": lambda: _fetch_cryptocompare(start_ts, end_ts),
        "coincap": lambda: _fetch_coincap(start_ts, end_ts),
        "coinpaprika": lambda: _fetch_coinpaprika(start_dt, end_dt),
        "coingecko": lambda: _fetch_coingecko(start_ts, end_ts),
    }

    df = None
    errors = []
    for source in sources:
        source = source.strip().lower()
        if not source:
            continue
        fetch = fetchers.get(source)
        if not fetch:
            errors.append(f"{source}: unknown source")
            continue
        try:
            df = fetch()
            if df.empty:
                raise RuntimeError("empty response")
            break
        except Exception as exc:
            errors.append(f"{source}: {exc}")
            df = None

    if df is None:
        raise RuntimeError("All price sources failed: " + "; ".join(errors))

    hourly = df.groupby("hour", as_index=False)["price"].mean()
    hourly = hourly.sort_values("hour")
    hourly = hourly[
        (hourly["hour"] >= pd.to_datetime(WINDOW_START, utc=True))
        & (hourly["hour"] <= pd.to_datetime(WINDOW_END, utc=True))
    ]

    out_path = RAW_DIR / "ust_prices.csv"
    hourly.to_csv(out_path, index=False)
    print("Saved:", out_path)


if __name__ == "__main__":
    main()
