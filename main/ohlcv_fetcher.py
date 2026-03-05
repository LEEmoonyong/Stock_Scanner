# -*- coding: utf-8 -*-
"""
OHLCV 조회: 로컬 캐시(SQLite) -> 1차 yfinance -> 2차 Alpha Vantage 폴백.
- 캐시 hit 시 API 호출 없음.
- 1차 실패/부족 시 2차 Alpha Vantage 시도 (ALPHA_VANTAGE_API_KEY 필요).
- 한국 종목(.KS/.KQ): 15:30 KST 장마감 이후엔 FinanceDataReader 우선 (종가 반영 빠름).
"""
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None


def _is_kr_market_closed() -> bool:
    """한국 장 15:30 KST 마감 이후인지. True면 종가 반영 시점."""
    # ZoneInfo가 없으면 보수적으로 "장 마감 후"로 간주해서 FDR 우선 사용
    if ZoneInfo is None:
        return True
    try:
        kst = datetime.now(ZoneInfo("Asia/Seoul"))
        if kst.weekday() >= 5:
            return False
        return (kst.hour > 15) or (kst.hour == 15 and kst.minute >= 30)
    except Exception:
        return False


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


def fetch_financedatareader_kr(ticker: str, start_date, end_date) -> Optional[pd.DataFrame]:
    """한국 주식/지수(.KS/.KQ, ^KS11/^KQ11 등) FinanceDataReader로 일봉 조회."""
    if not ticker:
        return None
    try:
        import FinanceDataReader as fdr
        t = ticker.upper().strip()
        # 1) 코스피/코스닥 지수(^KS11, ^KQ11 등)
        if t.startswith("^KS") or t.startswith("^KQ"):
            code = t.replace("^", "").replace(".KS", "").replace(".KQ", "").strip()
        else:
            # 2) 개별 종목(.KS/.KQ)
            code = t.replace(".KS", "").replace(".KQ", "").strip()
            if not code or not code.isdigit():
                return None
        start_s = pd.Timestamp(start_date).strftime("%Y-%m-%d")
        end_s = pd.Timestamp(end_date).strftime("%Y-%m-%d")
        df = fdr.DataReader(code, start_s, end_s)
        if df is None or df.empty:
            return None
        df.index = pd.to_datetime(df.index)
        renames = {"시가": "Open", "고가": "High", "저가": "Low", "종가": "Close", "거래량": "Volume"}
        df = df.rename(columns=renames)
        need = ["Open", "High", "Low", "Close", "Volume"]
        for c in need:
            if c not in df.columns:
                return None
        return df[need].dropna(how="all")
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

    t_upper = ticker.upper()
    is_kr = (".KS" in t_upper) or (".KQ" in t_upper) or t_upper.startswith("^K")
    kr_prefer_fdr = is_kr and _is_kr_market_closed()  # 15:30 KST 이후 종가 반영
    end_d = pd.Timestamp(end_date).date()

    # KR 15:30 이후 + 오늘 데이터 필요 시: 캐시 스킵하고 FDR 직접 조회 (종가 정확 반영)
    if kr_prefer_fdr and ZoneInfo:
        try:
            today_kst = datetime.now(ZoneInfo("Asia/Seoul")).date()
            if end_d >= today_kst and today_kst.weekday() < 5:
                df_fdr_first = fetch_financedatareader_kr(ticker, start_s, end_s)
                if df_fdr_first is not None and len(df_fdr_first) >= min_rows:
                    save_cached_ohlcv(ticker, df_fdr_first, db_path)
                    return df_fdr_first
        except Exception:
            pass

    # 1) 캐시
    cached = get_cached_ohlcv(ticker, start_s, end_s, db_path)
    # 갭 채우기: 캐시 마지막 날이 end_date보다 이전이면 부족 구간만 조회 후 저장·재조회
    if cached is not None and not cached.empty:
        last_cached = cached.index.max()
        last_d = pd.Timestamp(last_cached).date() if last_cached is not pd.NaT else None
        if last_d is not None and last_d < end_d:
            fill_start = (pd.Timestamp(last_d) + timedelta(days=1)).strftime("%Y-%m-%d")
            fill_df = None
            if kr_prefer_fdr:
                fill_df = fetch_financedatareader_kr(ticker, fill_start, end_s)
            if fill_df is None or fill_df.empty:
                fill_df = fetch_yfinance(ticker, fill_start, end_s)
            if fill_df is not None and not fill_df.empty:
                save_cached_ohlcv(ticker, fill_df, db_path)
                cached = get_cached_ohlcv(ticker, start_s, end_s, db_path)
            elif kr_prefer_fdr:
                # KR 15:30 이후 캐시 갭 채우기 실패 시 → 캐시 반환 스킵, FDR 직접 조회로 진행
                cached = None
    if cached is not None and len(cached) >= min_rows:
        return cached

    # 2) 한국 15:30 이후: FinanceDataReader 우선 (종가 반영 빠름)
    if kr_prefer_fdr:
        df_fdr = fetch_financedatareader_kr(ticker, start_s, end_s)
        if df_fdr is not None and len(df_fdr) >= min_rows:
            save_cached_ohlcv(ticker, df_fdr, db_path)
            return df_fdr

    # 3) yfinance
    df = fetch_yfinance(ticker, start_s, end_s)
    if df is not None and len(df) >= min_rows:
        save_cached_ohlcv(ticker, df, db_path)
        return df

    # 4) 한국 주식: FinanceDataReader 폴백 (yf 실패 시)
    if is_kr:
        df_fdr = fetch_financedatareader_kr(ticker, start_s, end_s)
        if df_fdr is not None and len(df_fdr) >= min_rows:
            save_cached_ohlcv(ticker, df_fdr, db_path)
            return df_fdr
        if df_fdr is not None and not df_fdr.empty:
            df = df_fdr

    # 5) Alpha Vantage
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
