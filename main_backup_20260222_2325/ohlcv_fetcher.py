# -*- coding: utf-8 -*-
"""
OHLCV 조회: 로컬 캐시(SQLite) -> 1차 yfinance -> 2차 Alpha Vantage 폴백.
- 캐시 hit 시 API 호출 없음.
- 1차 실패/부족 시 2차 Alpha Vantage 시도 (ALPHA_VANTAGE_API_KEY 필요).
"""
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd


def _db_path(base_dir: Optional[str] = None) -> str:
    base = base_dir or os.path.dirname(os.path.abspath(__file__))
    cache_dir = os.path.join(base, "cache")
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, "ohlcv.db")


def _init_db(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv (
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL, high REAL, low REAL, close REAL, volume REAL,
                PRIMARY KEY (ticker, date)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_ticker_date ON ohlcv(ticker, date)")


def get_cached_ohlcv(ticker: str, start_date, end_date, db_path: Optional[str] = None) -> Optional[pd.DataFrame]:
    """캐시에서 ticker의 [start_date, end_date] 구간 OHLCV 조회. 없거나 부족하면 None."""
    path = db_path or _db_path()
    if not os.path.exists(path):
        return None
    start_s = start_date if isinstance(start_date, str) else pd.Timestamp(start_date).strftime("%Y-%m-%d")
    end_s = end_date if isinstance(end_date, str) else pd.Timestamp(end_date).strftime("%Y-%m-%d")
    try:
        with sqlite3.connect(path) as conn:
            df = pd.read_sql_query(
                "SELECT date, open AS Open, high AS High, low AS Low, close AS Close, volume AS Volume "
                "FROM ohlcv WHERE ticker = ? AND date >= ? AND date <= ? ORDER BY date",
                conn,
                params=(ticker.upper(), start_s, end_s),
                index_col="date",
            )
        if df is None or df.empty:
            return None
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        for c in ["Open", "High", "Low", "Close", "Volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["Open", "High", "Low", "Close"])
        return df
    except Exception:
        return None


def save_cached_ohlcv(ticker: str, df: pd.DataFrame, db_path: Optional[str] = None) -> None:
    """DataFrame을 캐시 DB에 merge (ticker, date 기준 REPLACE)."""
    if df is None or df.empty:
        return
    path = db_path or _db_path()
    _init_db(path)
    ticker = ticker.upper()
    df = df.copy()
    df.index = pd.to_datetime(df.index)
    df = df[~df.index.isna()]
    need = ["Open", "High", "Low", "Close", "Volume"]
    for c in need:
        if c not in df.columns:
            return
    df = df[need].dropna(how="all")
    df["ticker"] = ticker
    df["date"] = df.index.strftime("%Y-%m-%d")
    df = df.reset_index(drop=True)
    try:
        with sqlite3.connect(path) as conn:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO ohlcv (ticker, date, open, high, low, close, volume)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        ticker,
                        row["date"],
                        float(row["Open"]) if np.isfinite(row["Open"]) else None,
                        float(row["High"]) if np.isfinite(row["High"]) else None,
                        float(row["Low"]) if np.isfinite(row["Low"]) else None,
                        float(row["Close"]) if np.isfinite(row["Close"]) else None,
                        int(row["Volume"]) if np.isfinite(row.get("Volume", 0)) else None,
                    ),
                )
    except Exception:
        pass


def fetch_yfinance(ticker: str, start_date, end_date) -> Optional[pd.DataFrame]:
    """yfinance로 일봉 조회. 실패 시 None."""
    try:
        import yfinance as yf
        end = pd.Timestamp(end_date)
        start = pd.Timestamp(start_date)
        df = yf.download(
            tickers=ticker,
            start=start.strftime("%Y-%m-%d"),
            end=(end + timedelta(days=1)).strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
            group_by="ticker",
        )
        if df is None or len(df) == 0:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            if ticker in df.columns.get_level_values(0):
                df = df[ticker].copy()
            else:
                df.columns = df.columns.get_level_values(-1)
        for c in ["Open", "High", "Low", "Close", "Volume"]:
            if c not in df.columns:
                return None
        df = df[["Open", "High", "Low", "Close", "Volume"]].dropna(how="all")
        df.index = pd.to_datetime(df.index)
        return df
    except Exception:
        return None


def fetch_alpha_vantage(ticker: str, start_date, end_date, api_key: str) -> Optional[pd.DataFrame]:
    """Alpha Vantage TIME_SERIES_DAILY로 일봉 조회. outputsize=full."""
    if not api_key or not api_key.strip():
        return None
    try:
        import urllib.request
        import json
        url = (
            "https://www.alphavantage.co/query?"
            "function=TIME_SERIES_DAILY&"
            f"symbol={ticker.upper()}&"
            "outputsize=full&"
            f"apikey={api_key.strip()}"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        ts = data.get("Time Series (Daily)")
        if not ts:
            return None
        rows = []
        for date_str, v in ts.items():
            try:
                rows.append({
                    "date": date_str,
                    "Open": float(v.get("1. open", 0)),
                    "High": float(v.get("2. high", 0)),
                    "Low": float(v.get("3. low", 0)),
                    "Close": float(v.get("4. close", 0)),
                    "Volume": int(float(v.get("5. volume", 0))),
                })
            except (TypeError, ValueError):
                continue
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        df = df.loc[(df.index >= start_ts) & (df.index <= end_ts)]
        if df.empty:
            return None
        return df
    except Exception:
        return None


def fetch_ohlcv_with_fallback(
    ticker: str,
    start_date,
    end_date,
    min_rows: int = 0,
    base_dir: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """
    1) 캐시 조회 -> min_rows 이상이면 반환
    2) 캐시 미스/부족 -> yfinance
    3) yfinance 실패/부족 -> Alpha Vantage (ALPHA_VANTAGE_API_KEY 환경변수)
    성공 시 캐시에 저장 후 반환.
    """
    start_s = pd.Timestamp(start_date).strftime("%Y-%m-%d")
    end_s = pd.Timestamp(end_date).strftime("%Y-%m-%d")
    db_path = _db_path(base_dir)

    # 1) 캐시
    cached = get_cached_ohlcv(ticker, start_s, end_s, db_path)
    # 갭 채우기: 캐시 마지막 날이 end_date보다 이전이면 부족 구간만 yf로 조회 후 저장·재조회
    if cached is not None and not cached.empty:
        end_d = pd.Timestamp(end_date).date()
        last_cached = cached.index.max()
        last_d = pd.Timestamp(last_cached).date() if last_cached is not pd.NaT else None
        if last_d is not None and last_d < end_d:
            fill_start = (pd.Timestamp(last_d) + timedelta(days=1)).strftime("%Y-%m-%d")
            fill_df = fetch_yfinance(ticker, fill_start, end_s)
            if fill_df is not None and not fill_df.empty:
                save_cached_ohlcv(ticker, fill_df, db_path)
                cached = get_cached_ohlcv(ticker, start_s, end_s, db_path)
    if cached is not None and len(cached) >= min_rows:
        return cached

    # 2) yfinance
    df = fetch_yfinance(ticker, start_s, end_s)
    if df is not None and len(df) >= min_rows:
        save_cached_ohlcv(ticker, df, db_path)
        return df

    # 3) Alpha Vantage
    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY", "").strip()
    if api_key:
        df = fetch_alpha_vantage(ticker, start_s, end_s, api_key)
        if df is not None and len(df) >= min_rows:
            save_cached_ohlcv(ticker, df, db_path)
            return df

    # 캐시가 있으면 min_rows 미만이어도 반환 (재시도에서 더 긴 구간 요청될 수 있음)
    if cached is not None and not cached.empty:
        return cached
    if df is not None and not df.empty:
        return df
    return None
