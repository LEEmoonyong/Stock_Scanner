import os
import json
import math
import pandas as pd
import numpy as np
import yfinance as yf
import scanner_config as cfg
import requests


from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ============================================================
# ✅ [호환/안전] 기존 코드가 전역 상수를 직접 참조하는 경우 방지
# - 노란줄(미정의) + 런타임 예외로 "전 티커 스킵"되는 현상 예방
# ============================================================
LOOKBACK_DAYS     = getattr(cfg, "LOOKBACK_DAYS", 2000)
MIN_RR            = getattr(cfg, "MIN_RR", 1.8)
MAX_BUY_PER_DAY   = getattr(cfg, "MAX_BUY_PER_DAY", 5)
RISK_PER_TRADE    = getattr(cfg, "RISK_PER_TRADE", 0.005)
ACCOUNT_EQUITY_USD = getattr(cfg, "ACCOUNT_EQUITY_USD", 10000)

from tickers_universe import TICKERS
from tickers_blacklist import TICKER_BLACKLIST

# ✅ 블랙리스트 반영(반드시 TICKERS가 정의된 이후에 적용)
TICKERS = [t for t in TICKERS if t not in TICKER_BLACKLIST]



# -----------------------------
# 캐시
# -----------------------------
_mktcap_cache = {}
_sector_cache = {}

# -----------------------------
# 메타 캐시 CSV (시총/섹터) — 속도 핵심
# -----------------------------
_meta_df = None  # lazy load

def _load_meta_cache():
    global _meta_df
    if _meta_df is not None:
        return _meta_df

    path = getattr(cfg, "META_CACHE_PATH", "meta_cache.csv")
    if os.path.exists(path):
        try:
            _meta_df = pd.read_csv(path)
            if "Ticker" in _meta_df.columns:
                _meta_df["Ticker"] = _meta_df["Ticker"].astype(str).str.upper()
                _meta_df = _meta_df.drop_duplicates(subset=["Ticker"]).set_index("Ticker")
            else:
                _meta_df = pd.DataFrame().set_index(pd.Index([], name="Ticker"))
        except Exception:
            _meta_df = pd.DataFrame().set_index(pd.Index([], name="Ticker"))
    else:
        _meta_df = pd.DataFrame().set_index(pd.Index([], name="Ticker"))
    return _meta_df

def _save_meta_cache():
    global _meta_df
    if _meta_df is None:
        return
    path = getattr(cfg, "META_CACHE_PATH", "meta_cache.csv")
    out = _meta_df.reset_index()
    out.to_csv(path, index=False, encoding="utf-8-sig")

def _get_meta_from_cache(ticker: str):
    dfm = _load_meta_cache()
    t = ticker.upper()
    if t in dfm.index:
        row = dfm.loc[t]
        mkt = row["MarketCap"] if "MarketCap" in dfm.columns else None
        sec = row["Sector"] if "Sector" in dfm.columns else "Unknown"
        try:
            mkt = None if pd.isna(mkt) else float(mkt)
        except Exception:
            mkt = None
        sec = "Unknown" if (sec is None or (isinstance(sec, float) and np.isnan(sec))) else str(sec)
        return mkt, sec
    return None, None

def _set_meta_cache(ticker: str, market_cap: float, sector: str):
    global _meta_df
    dfm = _load_meta_cache()
    t = ticker.upper()
    if _meta_df is None or _meta_df.empty:
        _meta_df = dfm.copy()

    for col in ["MarketCap", "Sector"]:
        if col not in _meta_df.columns:
            _meta_df[col] = np.nan if col == "MarketCap" else "Unknown"

    _meta_df.loc[t, "MarketCap"] = market_cap if market_cap is not None else np.nan
    _meta_df.loc[t, "Sector"] = sector or "Unknown"

# -----------------------------
# 지표
# -----------------------------
def sma(s, n): return s.rolling(n).mean()
def ema(s, n): return s.ewm(span=n, adjust=False).mean()

def atr(df, n=14):
    high, low, close = df["High"], df["Low"], df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(n).mean()


def adx(df, n=14):
    """ADX(추세 강도). 20 미만이면 추세 없음, 25 이상이면 추세 있음."""
    high, low, close = df["High"], df["Low"], df["Close"]
    prev_high, prev_low = high.shift(1), low.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_high).abs(),
        (low - prev_low).abs(),
    ], axis=1).max(axis=1)
    up = high - prev_high
    down = prev_low - low
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    plus_dm = pd.Series(plus_dm, index=df.index)
    minus_dm = pd.Series(minus_dm, index=df.index)
    atr_ser = tr.rolling(n).mean()
    plus_di = 100.0 * (plus_dm.rolling(n).mean() / atr_ser.replace(0, np.nan))
    minus_di = 100.0 * (minus_dm.rolling(n).mean() / atr_ser.replace(0, np.nan))
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx_ser = dx.rolling(n).mean()
    return adx_ser


def macd_hist(close, fast=12, slow=26, signal=9):
    macd = ema(close, fast) - ema(close, slow)
    sig = ema(macd, signal)
    return macd - sig

def macd_all(close, fast=12, slow=26, signal=9):
    macd_line = ema(close, fast) - ema(close, slow)
    sig_line = ema(macd_line, signal)
    hist = macd_line - sig_line
    return macd_line, sig_line, hist


def rsi(close, n=14):
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(n).mean()
    loss = (-delta.clip(upper=0)).rolling(n).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))
def _get_single_df_from_download(data, ticker: str):
    try:
        if isinstance(data.columns, pd.MultiIndex):
            if ticker not in data.columns.get_level_values(0):
                return None
            df = data[ticker].copy()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
                        # ✅ 장중 변동 방지: 오늘(진행중) 일봉은 신호 계산에서 제외
            today = datetime.utcnow().date()
            try:
                last_dt = pd.to_datetime(df.index[-1]).date()
                if last_dt >= today:
                    df = df.iloc[:-1].copy()
            except Exception:
                pass

            return df
        else:
            return data.copy()
    except Exception:
        return None


def compute_market_state_from_data(data):
    df_spy = _get_single_df_from_download(data, "SPY")
    if df_spy is None or df_spy.empty:
        return {"regime": "UNKNOWN", "spy_sma50": None, "spy_sma200": None}

    df_spy = df_spy.dropna(subset=["Close"]).copy()
    if len(df_spy) < 220:
        return {"regime": "UNKNOWN", "spy_sma50": None, "spy_sma200": None}

    close = df_spy["Close"]
    sma50 = float(sma(close, 50).iloc[-1])
    sma200 = float(sma(close, 200).iloc[-1])
    last_close = float(close.iloc[-1])

    if (last_close < sma50) and (last_close < sma200):
        regime = "RISK_OFF"
    elif last_close < sma200:
        regime = "CAUTION"
    else:
        regime = "RISK_ON"

    return {"regime": regime, "spy_sma50": round(sma50, 2), "spy_sma200": round(sma200, 2)}


def data_quality_check(df: pd.DataFrame, end_date: datetime.date, max_stale_days: int = 5):
    """
    반환: (ok: bool, reason: str)
    """
    if df is None or df.empty:
        return False, "empty df"

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # 필수 컬럼
    need = {"Open", "High", "Low", "Close", "Volume"}
    if not need.issubset(set(df.columns)):
        return False, f"missing columns: {need - set(df.columns)}"

    # 인덱스 날짜 최신성
    try:
        last_dt = pd.to_datetime(df.index[-1]).date()
    except Exception:
        return False, "bad index datetime"

    stale_days = (end_date - last_dt).days
    if stale_days > max_stale_days:
        return False, f"stale data: last={last_dt} ({stale_days}d old)"

    # 최근 60봉 결측/이상치 체크
    d = df.tail(60).copy()

    # 숫자 변환/결측
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        d[c] = pd.to_numeric(d[c], errors="coerce")

    if d[["Open","High","Low","Close","Volume"]].isna().mean().mean() > 0.05:
        return False, "too many NaNs (last 60 bars)"

    # OHLC 관계
    bad_ohlc = (d["High"] < d["Low"]).sum()
    if bad_ohlc > 0:
        return False, f"bad OHLC: High<Low count={bad_ohlc}"

    # 0 거래량 너무 많으면 스킵(ETF/지수 일부 예외가 있지만 일단 안정성 우선)
    zero_vol = (d["Volume"] <= 0).sum()
    if zero_vol >= 3:
        return False, f"too many zero volumes (last 60): {zero_vol}"

    return True, "ok"

def _should_drop_today_bar_us() -> bool:
    try:
        et = datetime.now(ZoneInfo("America/New_York"))
        if et.weekday() >= 5:
            return False
        if (et.hour > 16) or (et.hour == 16 and et.minute >= 20):
            return False
        return True
    except Exception:
        return True


def _drop_today_bar_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    try:
        if not _should_drop_today_bar_us():
            return df

        et_today = datetime.now(ZoneInfo("America/New_York")).date()
        last_dt = pd.to_datetime(df.index[-1]).date()
        if last_dt >= et_today:
            return df.iloc[:-1].copy()
    except Exception:
        pass

    return df

# -----------------------------
# 메타 (시총/섹터) — 캐시 우선
# -----------------------------
def get_market_cap_usd(ticker: str):
    # 1) CSV 캐시 우선
    mc, _ = _get_meta_from_cache(ticker)
    if mc is not None:
        return mc

    # 2) 메모리 캐시
    if ticker in _mktcap_cache:
        return _mktcap_cache[ticker]

    # 3) 없으면(옵션) 최초에만 info로 채움
    if not getattr(cfg, "ALLOW_META_FETCH_IF_MISSING", True):
        _mktcap_cache[ticker] = None
        return None

    try:
        t = yf.Ticker(ticker)
        mc = None
        if hasattr(t, "fast_info") and t.fast_info:
            mc = t.fast_info.get("market_cap")
        if mc is None:
            mc = t.info.get("marketCap")

        _mktcap_cache[ticker] = mc

        # sector도 같이 캐시(한 번에)
        sec = None
        try:
            sec = t.info.get("sector") or "Unknown"
        except Exception:
            sec = "Unknown"

        _set_meta_cache(ticker, mc, sec)
        _save_meta_cache()
        return mc
    except Exception:
        _mktcap_cache[ticker] = None
        return None

def get_sector(ticker: str):
    # 1) CSV 캐시 우선
    _, sec = _get_meta_from_cache(ticker)
    if sec is not None:
        return sec

    # 2) 메모리 캐시
    if ticker in _sector_cache:
        return _sector_cache[ticker]

    # 3) 없으면(옵션) 최초에만 info로 채움
    if not getattr(cfg, "ALLOW_META_FETCH_IF_MISSING", True):
        _sector_cache[ticker] = "Unknown"
        return "Unknown"

    try:
        t = yf.Ticker(ticker)
        sector = (t.info.get("sector") if hasattr(t, "info") else None) or "Unknown"
        _sector_cache[ticker] = sector

        # marketCap도 같이 캐시(한 번에)
        mc = None
        try:
            if hasattr(t, "fast_info") and t.fast_info:
                mc = t.fast_info.get("market_cap")
            if mc is None:
                mc = t.info.get("marketCap")
        except Exception:
            mc = None

        _set_meta_cache(ticker, mc, sector)
        _save_meta_cache()
        return sector
    except Exception:
        _sector_cache[ticker] = "Unknown"
        return "Unknown"
 
   
def _breakout_quality(df2, high20_prev: float):
    """
    가짜 돌파 방지:
    - 종가 확인(고점 위에서 마감)
    - 윗꼬리 과다(매도압력) 배제
    - 갭 과다(추격 위험) 배제(→ BUY 대신 WATCH로 내릴 때 사용)
    반환: (ok_buy: bool, ok_watch: bool, reason: str)
      ok_buy=True  => BUY 허용
      ok_watch=True => BUY는 아니어도 WATCH로는 OK
    """
    last = df2.iloc[-1]
    prev = df2.iloc[-2] if len(df2) >= 2 else last

    o = float(last["Open"]); h = float(last["High"]); l = float(last["Low"]); c = float(last["Close"])
    prev_c = float(prev["Close"])

    # 설정값(없으면 기본)
    close_buf = float(getattr(cfg, "BREAKOUT_CLOSE_BUFFER", 0.001))
    max_wick = float(getattr(cfg, "BREAKOUT_MAX_WICK_RATIO", 0.60))
    max_gap_atr = float(getattr(cfg, "BREAKOUT_MAX_GAP_ATR", 1.20))

    # 1) 종가가 전 20고점(high20_prev) 위에서 마감했는가?
    close_confirm = (c >= high20_prev * (1 + close_buf))

    # 2) 윗꼬리 비율: (High - max(Open,Close)) / (High-Low)
    rng = max(1e-9, (h - l))
    upper_wick = (h - max(o, c)) / rng
    wick_ok = (upper_wick <= max_wick)

    # 3) 갭 과다: (오늘 시가 - 전일 종가) / ATR
    atr14 = float(last["ATR14"]) if ("ATR14" in last and np.isfinite(float(last["ATR14"]))) else np.nan
    gap_atr = ((o - prev_c) / atr14) if (np.isfinite(atr14) and atr14 > 0) else 0.0
    gap_ok = (gap_atr <= max_gap_atr)

    # BUY는 3개 다 통과해야
    if close_confirm and wick_ok and gap_ok:
        return True, True, "ok"

    # WATCH는 약간 관대(갭/윗꼬리는 WATCH 허용, 종가 미확인은 WATCH로)
    reasons = []
    if not close_confirm:
        reasons.append(f"close_not_confirm(need>{high20_prev*(1+close_buf):.2f})")
    if not wick_ok:
        reasons.append(f"upper_wick_high({upper_wick:.2f})")
    if not gap_ok:
        reasons.append(f"gap_atr_high({gap_atr:.2f})")

    # 종가 미확인 or 갭 과다 or 꼬리 과다는 BUY 금지, WATCH는 허용
    return False, True, " / ".join(reasons)


# -----------------------------
# 진입 판단
# -----------------------------
def _pullback_quality(df2):
    """
    눌림매수(BUY_PULLBACK) 가짜 반등 필터
    반환: (ok: bool, reason: str)
    """
    if df2 is None or len(df2) < 60:
        return False, "insufficient bars"

    last = df2.iloc[-1]
    prev = df2.iloc[-2]
    prev2 = df2.iloc[-3] if len(df2) >= 3 else prev

    close = float(last["Close"])
    close_prev = float(prev["Close"])
    close_prev2 = float(prev2["Close"])

    open_ = float(last["Open"])
    high = float(last["High"])
    low = float(last["Low"])

    sma20 = float(last.get("SMA20", np.nan))
    sma50 = float(last.get("SMA50", np.nan))

    vol = float(last["Volume"])
    vol20 = float(df2["Volume"].tail(20).mean())
    vol_ratio = (vol / vol20) if vol20 > 0 else np.nan

    # 1) 반등 캔들 기본: 양봉 + 종가가 전일 종가보다 높아야 함
    bullish = (close > open_) and (close > close_prev)
    if not bullish:
        return False, "no bullish rebound candle"

    # 2) “가짜 반등” 흔한 패턴: 윗꼬리만 긴 경우(종가가 고가에서 너무 멀다)
    #    종가가 (고가-저가)의 상단 60% 이상에서 마감해야 통과
    rng = (high - low)
    if rng > 0:
        pos = (close - low) / rng
        if pos < 0.60:
            return False, "weak close (upper-wick heavy)"

    # 3) 거래량 붕괴면 가짜 반등 확률↑ → 최소 0.9x는 요구(원하면 1.0~1.2로 강화 가능)
    if np.isfinite(vol_ratio) and vol_ratio < 0.90:
        return False, f"volume too weak ({vol_ratio:.2f}x)"

    # 4) 반등인데도 SMA20을 회복 못하면 “추세 복귀”가 아니라 “죽은고양이”일 수 있음
    if np.isfinite(sma20) and close < sma20:
        return False, "close below SMA20 (no reclaim)"

    # 5) 급락 연속 후 첫 반등(칼날) 방지: 최근 3일 중 2일 이상 하락이면 보수적으로 WATCH
    downs = int(close_prev < close_prev2) + int(close < close_prev)  # last는 bullish라 보통 0
    # prev<prev2만 봐도 되지만, 완화형으로 최근 3봉 흐름 체크
    if int(close_prev < close_prev2) >= 1 and (close_prev < close_prev2) and (close_prev < float(prev["Open"])):
        # 전일도 음봉이고 하락이면 위험↑
        return False, "still in short-term selloff"

    return True, "ok"

def decide_entry(df2):
    last = df2.iloc[-1]
    prev = df2.iloc[-2] if len(df2) >= 2 else last

    close = float(last["Close"])
    high = float(last["High"])
    vol = float(last["Volume"])
    vol20 = float(df2["Volume"].tail(20).mean())

    sma20 = float(last["SMA20"])
    sma50 = float(last["SMA50"])
    sma200 = float(last["SMA200"])
    atr14 = float(last["ATR14"]) if np.isfinite(float(last["ATR14"])) else np.nan
    rsi14 = float(last["RSI14"]) if np.isfinite(float(last["RSI14"])) else np.nan

    high20 = float(df2["High"].iloc[-20:-1].max()) if len(df2) >= 21 else float(df2["High"].tail(20).max())
    near_high = close / high20 if high20 > 0 else np.nan
    vol_ratio = (vol / vol20) if vol20 > 0 else np.nan

    stop = close - cfg.ATR_STOP_MULT * atr14 if np.isfinite(atr14) and atr14 > 0 else np.nan

    # BUY_BREAKOUT (가짜 돌파 필터 적용: 종가확인/윗꼬리/갭)
    buy_vol_x = getattr(cfg, "BUY_BREAKOUT_VOL_X", cfg.BREAKOUT_VOL_X)

    # ✅ "전 20일 고점" (오늘 봉 제외)
    high20_prev = float(df2["High"].iloc[-20:-1].max()) if len(df2) >= 21 else float(df2["High"].tail(20).max())

    # 기본 후보: 오늘 고점이 전고점 터치/돌파 + 거래량
    is_breakout_touch = (high >= high20_prev)
    vol_ok = (np.isfinite(vol_ratio) and vol_ratio >= buy_vol_x)

    if is_breakout_touch and vol_ok:
        ok_buy, ok_watch, q_reason = _breakout_quality(df2, high20_prev)

        if ok_buy:
            return ("BUY_BREAKOUT",
                    f"20D High {high20_prev:.2f} 돌파 + Vol {vol_ratio:.2f}x (기준 {buy_vol_x}x)",
                    f"{max(close, high20_prev):.2f} 위에서 분할(돌파 유지 시)",
                    f"SMA50({sma50:.2f}) 이탈 또는 Stop {stop:.2f}",
                    "종가확인/윗꼬리/갭 통과 → 진짜 돌파 진입")

        # ✅ BUY는 아니고 WATCH로만 내림(가짜 돌파 방지)
        if ok_watch:
            return ("WATCH_BREAKOUT",
                    f"트리거: {high20_prev:.2f} 상향 돌파(20D High) | 품질: {q_reason}",
                    f"{high20_prev:.2f} 위 '종가확인' + 거래량 유지 시",
                    f"SMA50({sma50:.2f}) 이탈 또는 Stop {stop:.2f}",
                    f"현재 Vol {vol_ratio:.2f}x (BUY 기준 {buy_vol_x}x)")


    # WATCH_BREAKOUT
    if np.isfinite(near_high) and near_high >= cfg.BREAKOUT_NEAR:
        return ("WATCH_BREAKOUT",
                f"트리거: {high20:.2f} 상향 돌파(20D High)",
                f"{high20:.2f} 돌파+거래량 동반 시",
                f"SMA50({sma50:.2f}) 이탈 또는 Stop {stop:.2f}",
                f"현재 Vol {vol_ratio:.2f}x")

    # PULLBACK (상승추세 전제: SMA50 > SMA200 & Close > SMA200*0.98)
    uptrend = (sma50 > sma200) and (close > sma200 * 0.98)
    if uptrend:
        near_sma20 = abs(close / sma20 - 1) <= cfg.PULLBACK_BAND if sma20 > 0 else False
        near_sma50 = abs(close / sma50 - 1) <= (cfg.PULLBACK_BAND * 1.5) if sma50 > 0 else False
        if (near_sma20 or near_sma50) and (np.isnan(rsi14) or rsi14 <= 72):

            rebound = float(last["Close"]) > float(prev["Close"])

            # ============================
            # 가짜 반등 필터 (config로 ON/OFF)
            # ============================
            if getattr(cfg, "USE_PULLBACK_QUALITY_FILTER", True):

                body = abs(last["Close"] - last["Open"])
                range_ = last["High"] - last["Low"]
                bullish_close = last["Close"] > (last["Low"] + range_ * 0.6) if range_ > 0 else False
                strong_body = body > (range_ * 0.4) if range_ > 0 else False

                # 약한 반등 제거
                if not (rebound and bullish_close and strong_body):
                    return ("WATCH_PULLBACK",
                            "약한 반등(가짜 반등 필터)",
                            "강한 양봉 확인 후",
                            f"SMA50({sma50:.2f}) 이탈 또는 Stop {stop:.2f}",
                            f"RSI {rsi14:.1f}")

            ma_name = "SMA20" if near_sma20 else "SMA50"
            ma_val = sma20 if near_sma20 else sma50


            if rebound:
                ok_pb, pb_reason = _pullback_quality(df2)
                if not ok_pb:
                    # ✅ 가짜 반등이면 BUY 대신 WATCH로 내려서 "확인"만 하게
                    return ("WATCH_PULLBACK",
                            f"{ma_name}({ma_val:.2f}) 눌림 후 반등(확인필요)",
                            f"{close:.2f} 재돌파/거래량 동반 확인 후",
                            f"{ma_name}({ma_val:.2f}) 이탈 또는 Stop {stop:.2f}",
                            f"FILTER: {pb_reason}")

                return ("BUY_PULLBACK",
                        f"{ma_name}({ma_val:.2f}) 눌림 후 반등(필터 통과)",
                        f"{close:.2f} 부근 분할 또는 직전 고가 상향 시",
                        f"{ma_name}({ma_val:.2f}) 이탈 또는 Stop {stop:.2f}",
                        f"RSI {rsi14:.1f}")
            else:
                return ("WATCH_PULLBACK",
                        f"{ma_name}({ma_val:.2f}) 부근 눌림 진행",
                        "반등 캔들 확인 후",
                        f"{ma_name}({ma_val:.2f}) 이탈 또는 Stop {stop:.2f}",
                    f"RSI {rsi14:.1f}")


    return ("SKIP", "진입 타이밍 불명확", "-", f"Stop {stop:.2f}" if np.isfinite(stop) else "-", "대기")

def _dynamic_floor_atr(df2: pd.DataFrame, entry: float, atr14: float, entry_type: str) -> float:
    """
    ✅ RR 고정 방지용: floor_atr(최소 목표 ATR배수)를 종목별로 동적으로 조정
    - 최근 High60(또는 High20)까지 남은 거리를 ATR 단위로 계산
    - 고점이 '너무 가까우면' +3ATR 타겟은 의미가 없어져 RR이 고정되기 쉬움 → floor_atr를 올림
    """
    e = str(entry_type or "").upper().strip()

    base = float(getattr(cfg, "TARGET_ATR_FLOOR_PULLBACK", 2.5)) if e == "BUY_PULLBACK" \
        else float(getattr(cfg, "TARGET_ATR_FLOOR_BREAKOUT", 3.0))

    # 최근 고점 후보(high60 우선, 없으면 high20)
    high60 = np.nan
    high20 = np.nan
    try:
        if "High" in df2.columns and len(df2) >= 60:
            high60 = float(df2["High"].tail(60).max())
        if "High" in df2.columns and len(df2) >= 20:
            high20 = float(df2["High"].tail(20).max())
    except Exception:
        pass

    ref_high = high60 if np.isfinite(high60) and high60 > 0 else high20
    if not (np.isfinite(ref_high) and ref_high > 0 and np.isfinite(atr14) and atr14 > 0):
        return base

    # 고점까지 남은 거리(ATR 단위)
    dist_atr = (ref_high - entry) / atr14

    # dist_atr가 작으면(고점이 가깝거나 이미 근접/돌파) 3ATR 타겟이 너무 뻔해서 RR이 고정되기 쉬움
    # → 부족분의 일부를 floor_atr에 더해 “종목마다” 다르게 만든다.
    # (가까울수록 더 올림)
    if dist_atr < base:
        bump = (base - dist_atr) * float(getattr(cfg, "TARGET_ATR_DYNAMIC_BUMP_K", 0.6))
    else:
        bump = 0.0

    # 너무 과하지 않게 상한(기본 6.0)
    cap = float(getattr(cfg, "TARGET_ATR_DYNAMIC_CAP", 6.0))
    floor_atr = min(cap, base + max(0.0, bump))

    # 안전망: 최소 base 보장
    return max(base, floor_atr)

def debug_trade_plan_rr(df2: pd.DataFrame, entry_type: str):
    if df2 is None or df2.empty or len(df2) < 140:
        print("df2 not ready")
        return

    last = df2.iloc[-1]
    entry = float(last["Close"])
    atr14 = float(last["ATR14"])
    atr_stop_mult = float(getattr(cfg, "ATR_STOP_MULT", 2.0))
    stop = entry - atr_stop_mult * atr14

    # base_target 계산(너 calc_trade_plan과 동일)
    lb1 = int(getattr(cfg, "TARGET_LOOKBACK_1", 20))
    lb2 = int(getattr(cfg, "TARGET_LOOKBACK_2", 60))
    high20 = float(df2["High"].tail(lb1).max())
    high60 = float(df2["High"].tail(lb2).max())
    buf20 = float(getattr(cfg, "TARGET_HIGHBUF_20", 0.02))
    buf60 = float(getattr(cfg, "TARGET_HIGHBUF_60", 0.03))
    base_target = max(high20 * (1 + buf20), high60 * (1 + buf60))

    floor_atr = _dynamic_floor_atr(df2, entry, atr14, entry_type)
    min_target = entry + atr14 * float(floor_atr)

    target = max(float(base_target), float(min_target))
    picked = "BASE_TARGET" if base_target >= min_target else "MIN_TARGET"

    rr = (target - entry) / (entry - stop)

    # 고점까지 거리(ATR)
    ref_high = high60
    dist_atr = (ref_high - entry) / atr14 if atr14 > 0 else float("nan")

    print(f"[DEBUG] entry={entry:.2f} atr={atr14:.2f} stop={stop:.2f}")
    print(f"[DEBUG] high60={high60:.2f} dist_atr={dist_atr:.2f}ATR")
    print(f"[DEBUG] floor_atr={floor_atr:.2f} (cap={getattr(cfg,'TARGET_ATR_DYNAMIC_CAP',6.0)})")
    print(f"[DEBUG] base_target={base_target:.2f} min_target={min_target:.2f} -> picked={picked}")
    print(f"[DEBUG] target={target:.2f} RR={rr:.2f}")

# -----------------------------
# 트레이드 플랜 + 포지션 사이징
# -----------------------------
def calc_trade_plan(df2: pd.DataFrame, entry_type: str):
    """
    Trade plan 계산:
      - EntryPrice / StopPrice / TargetPrice / RR
      - Shares / PosValue (계좌 리스크 기반)

    ✅ RR 고정 방지:
      - min_target의 floor_atr를 종목별로 동적으로 조정(_dynamic_floor_atr)
      - Target은 base_target vs min_target 중 큰 값
      - RR은 결과값(=target/stop 결과)로만 계산
    """
    if df2 is None or df2.empty or len(df2) < 140:
        return None

    try:
        last = df2.iloc[-1]
        close = float(last["Close"])
        atr14 = float(last["ATR14"])
    except Exception:
        return None

    if not np.isfinite(close) or not np.isfinite(atr14) or close <= 0 or atr14 <= 0:
        return None

    # ----------------------------
    # 1) EntryPrice
    # ----------------------------
    entry = close

    # ----------------------------
    # 2) StopPrice (ATR 기반)
    # ----------------------------
    atr_stop_mult = float(getattr(cfg, "ATR_STOP_MULT", 2.0))
    stop = entry - atr_stop_mult * atr14
    if not np.isfinite(stop) or stop <= 0 or stop >= entry:
        return None

    # ----------------------------
    # 3) TargetPrice (최근 고점 + ATR 바닥)
    # ----------------------------
    lb1 = int(getattr(cfg, "TARGET_LOOKBACK_1", 20))
    lb2 = int(getattr(cfg, "TARGET_LOOKBACK_2", 60))

    high20 = np.nan
    high60 = np.nan
    try:
        if "High" in df2.columns and len(df2) >= lb1:
            high20 = float(df2["High"].tail(lb1).max())
        if "High" in df2.columns and len(df2) >= lb2:
            high60 = float(df2["High"].tail(lb2).max())
    except Exception:
        pass

    # base_target = 고점 + 버퍼
    buf20 = float(getattr(cfg, "TARGET_HIGHBUF_20", 0.02))
    buf60 = float(getattr(cfg, "TARGET_HIGHBUF_60", 0.03))

    candidates = []
    if np.isfinite(high20) and high20 > 0:
        candidates.append(high20 * (1.0 + buf20))
    if np.isfinite(high60) and high60 > 0:
        candidates.append(high60 * (1.0 + buf60))

    base_target = max(candidates) if candidates else (entry + atr14 * 3.0)

    # ✅ 동적 floor_atr (여기가 RR 고정 깨는 핵심)
    floor_atr = _dynamic_floor_atr(df2, entry, atr14, entry_type)
    min_target = entry + atr14 * float(floor_atr)

    # 최종 Target
    target = max(float(base_target), float(min_target))

    # 너무 가까우면 한 번 더 띄움
    if target <= entry * 1.002:
        target = entry + atr14 * max(float(floor_atr), 3.0)

    if not np.isfinite(target) or target <= entry:
        return None

    # ----------------------------
    # 4) RR (결과값)
    # ----------------------------
    risk_per_share = entry - stop
    reward_per_share = target - entry
    if risk_per_share <= 0 or reward_per_share <= 0:
        return None

    rr = reward_per_share / risk_per_share
    if not np.isfinite(rr) or rr <= 0:
        return None

    # ----------------------------
    # 5) Position sizing
    # ----------------------------
    equity = float(getattr(cfg, "ACCOUNT_EQUITY_USD", 0))
    risk_pct = float(getattr(cfg, "RISK_PER_TRADE", 0.0))
    one_r_dollars = equity * risk_pct

    if one_r_dollars <= 0:
        shares = 0
        pos_value = 0.0
    else:
        shares = int(one_r_dollars / risk_per_share)
        shares = max(0, shares)
        pos_value = shares * entry

    def _r(v, n=2):
        try:
            return round(float(v), n)
        except Exception:
            return v

    return {
        "EntryPrice": _r(entry, 2),
        "StopPrice": _r(stop, 2),
        "TargetPrice": _r(target, 2),
        "RR": _r(rr, 2),
        "Shares": int(shares),
        "PosValue": _r(pos_value, 0),
    }

def debug_trade_plan_rr(df2: pd.DataFrame, entry_type: str, ticker: str = ""):
    """
    ✅ RR이 왜 특정 값으로 몰리는지/고정처럼 보이는지 즉시 확인용 디버그
    - Stop 구성(ATR_STOP_MULT)
    - Target이 base_target vs min_target 중 무엇을 택했는지
    - RR이 floor_atr/stop_mult에 가까운지 확인
    """
    try:
        if df2 is None or df2.empty or len(df2) < 140:
            print(f"[DEBUG][{ticker}] df2 too short/empty")
            return

        last = df2.iloc[-1]
        entry = float(last["Close"])
        atr14 = float(last["ATR14"])
        if not (np.isfinite(entry) and np.isfinite(atr14) and entry > 0 and atr14 > 0):
            print(f"[DEBUG][{ticker}] bad entry/atr: entry={entry}, atr={atr14}")
            return

        atr_stop_mult = float(getattr(cfg, "ATR_STOP_MULT", 2.0))
        stop = entry - atr_stop_mult * atr14

        lb1 = int(getattr(cfg, "TARGET_LOOKBACK_1", 20))
        lb2 = int(getattr(cfg, "TARGET_LOOKBACK_2", 60))

        high20 = float(df2["High"].tail(lb1).max()) if ("High" in df2.columns and len(df2) >= lb1) else np.nan
        high60 = float(df2["High"].tail(lb2).max()) if ("High" in df2.columns and len(df2) >= lb2) else np.nan

        buf20 = float(getattr(cfg, "TARGET_HIGHBUF_20", 0.02))
        buf60 = float(getattr(cfg, "TARGET_HIGHBUF_60", 0.03))

        candidates = []
        if np.isfinite(high20) and high20 > 0:
            candidates.append(high20 * (1.0 + buf20))
        if np.isfinite(high60) and high60 > 0:
            candidates.append(high60 * (1.0 + buf60))

        base_target = max(candidates) if candidates else (entry + atr14 * 3.0)

        e = str(entry_type or "").upper().strip()
        if e == "BUY_PULLBACK":
            floor_atr = float(getattr(cfg, "TARGET_ATR_FLOOR_PULLBACK", 2.5))
        else:
            floor_atr = float(getattr(cfg, "TARGET_ATR_FLOOR_BREAKOUT", 3.0))

        min_target = entry + atr14 * floor_atr
        target = max(float(base_target), float(min_target))

        chosen = "BASE_TARGET" if (target == float(base_target)) else "MIN_TARGET"
        risk = entry - stop
        reward = target - entry
        rr = (reward / risk) if (risk > 0 and reward > 0) else np.nan

        approx_rr = (floor_atr / atr_stop_mult) if atr_stop_mult > 0 else np.nan

        print("\n" + "=" * 70)
        print(f"[DEBUG][{ticker}] entry_type={e}")
        print(f"Entry={entry:.2f} | ATR14={atr14:.4f}")
        print(f"ATR_STOP_MULT={atr_stop_mult:.2f} => Stop={stop:.2f} | Risk/Share={risk:.4f}")
        print(f"High20={high20:.2f} (buf {buf20:.2%}) | High60={high60:.2f} (buf {buf60:.2%})")
        print(f"base_target={base_target:.2f}")
        print(f"floor_atr={floor_atr:.2f} => min_target={min_target:.2f}")
        print(f"✅ chosen={chosen} => Target={target:.2f} | Reward/Share={reward:.4f}")
        print(f"RR={rr:.4f} | (floor_atr/stop_mult ≈ {approx_rr:.4f})")
        print("=" * 70 + "\n")

    except Exception as ex:
        print(f"[DEBUG][{ticker}] error: {ex}")


# -----------------------------
# 보유 포지션: 청산/트레일링 스탑 추천
# -----------------------------
# -----------------------------
# 보유 포지션: 청산/익절/트레일링 스탑 추천 (개선버전)
# -----------------------------
def load_positions(path="positions.csv"):
    if not os.path.exists(path):
        return pd.DataFrame(columns=["Ticker","Shares","AvgPrice"])
    df = pd.read_csv(path)
    for c in ["Ticker","Shares","AvgPrice"]:
        if c not in df.columns:
            raise ValueError("positions.csv는 Ticker,Shares,AvgPrice 컬럼이 필요합니다.")
    df["Ticker"] = df["Ticker"].astype(str).str.upper()
    return df

# -----------------------------
# positions.csv 저장/추가/삭제 유틸
# -----------------------------
def save_positions(df: pd.DataFrame, path="positions.csv"):
    # 컬럼 순서 고정
    cols = ["Ticker", "Shares", "AvgPrice"]
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan

    df = df[cols].copy()
    df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
    df["Shares"] = pd.to_numeric(df["Shares"], errors="coerce")
    df["AvgPrice"] = pd.to_numeric(df["AvgPrice"], errors="coerce")
    df = df.dropna(subset=["Ticker", "Shares", "AvgPrice"])
    df = df[df["Ticker"] != ""]
    df.to_csv(path, index=False, encoding="utf-8-sig")


def add_or_update_position(ticker: str, shares: float, avg_price: float, path="positions.csv", mode="merge"):
    """
    mode:
      - "merge": 동일 티커 있으면 수량 가중평단으로 합침(추천)
      - "replace": 동일 티커 있으면 shares/avg_price로 덮어씀
      - "add_row": 동일 티커 있더라도 새 행으로 추가(비추천: 중복 생김)
    """
    t = str(ticker).upper().strip()
    if not t:
        raise ValueError("Ticker가 비어있습니다.")
    if shares <= 0:
        raise ValueError("Shares는 0보다 커야 합니다.")
    if avg_price <= 0:
        raise ValueError("AvgPrice는 0보다 커야 합니다.")

    df = load_positions(path)
    if df.empty:
        df = pd.DataFrame(columns=["Ticker", "Shares", "AvgPrice"])

    df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()

    if mode == "add_row":
        df = pd.concat([df, pd.DataFrame([{"Ticker": t, "Shares": shares, "AvgPrice": avg_price}])], ignore_index=True)
        save_positions(df, path)
        return df

    if t in df["Ticker"].values:
        idx = df.index[df["Ticker"] == t][0]
        if mode == "replace":
            df.at[idx, "Shares"] = float(shares)
            df.at[idx, "AvgPrice"] = float(avg_price)
        else:
            # merge: 가중평단
            old_sh = float(df.at[idx, "Shares"])
            old_ap = float(df.at[idx, "AvgPrice"])
            new_sh = float(shares)
            new_ap = float(avg_price)

            tot_sh = old_sh + new_sh
            wavg = (old_sh * old_ap + new_sh * new_ap) / tot_sh if tot_sh > 0 else new_ap

            df.at[idx, "Shares"] = tot_sh
            df.at[idx, "AvgPrice"] = round(wavg, 6)
        save_positions(df, path)
        return df

    # 신규 티커
    df = pd.concat([df, pd.DataFrame([{"Ticker": t, "Shares": float(shares), "AvgPrice": float(avg_price)}])], ignore_index=True)
    save_positions(df, path)
    return df


def remove_position(ticker: str, path="positions.csv"):
    t = str(ticker).upper().strip()
    if not t:
        raise ValueError("Ticker가 비어있습니다.")

    df = load_positions(path)
    if df.empty:
        return df

    df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
    before = len(df)
    df = df[df["Ticker"] != t].copy()
    after = len(df)

    save_positions(df, path)
    return before - after  # 삭제된 행 수


def print_positions(path="positions.csv"):
    df = load_positions(path)
    if df.empty:
        print("(positions.csv 비어있음)")
        return
    df = df.copy()
    df["Ticker"] = df["Ticker"].astype(str).str.upper()
    print(df.to_string(index=False))



def holding_risk_review(df2, ticker, shares, avg_price, days_held=None, max_hold_days=None, skip_earnings_warning=False, skip_holding_days_warning=False):
    """
    보유 포지션 매도/익절 후보 룰(스윙용):
    - TAKE_PROFIT: 종가가 60D High 근접/도달 시 (익절 고려)
    - SELL_TRAIL: 종가 < trailing_stop (트레일링 스탑 이탈)
      trailing_stop = max(SMA20, close - 2ATR)
    - SELL_TREND: 추세 꺾임 확인(보수적) — 2일 연속 종가<SMA20 또는 종가<SMA50 + 컨펌
    - SELL_STRUCTURE_BREAK: 구조 붕괴(최근 N봉 스윙 로우 아래로 종가 이탈)
    - 그 외: HOLD
    - skip_earnings_warning=True: 실적일 yfinance 조회 생략(백테스트 등 속도용)
    - skip_holding_days_warning=True: 보유기간 경고 블록 생략(백테스트용)
    """
    last = df2.iloc[-1]
    prev = df2.iloc[-2] if len(df2) >= 2 else last

    close = float(last["Close"])
    sma20 = float(last["SMA20"]) if "SMA20" in last else np.nan
    sma50 = float(last["SMA50"]) if "SMA50" in last else np.nan
    atr14 = float(last["ATR14"]) if ("ATR14" in last and np.isfinite(float(last["ATR14"]))) else np.nan
    
    # =========================
    # ✅ SELL/TP 컨펌(보수화) 신호 계산
    # =========================
    # MACD 계열(SELL/TP 공용)
    macd_dead = False
    macd_weak = False
    try:
        c = df2["Close"].astype(float)
        macd_line, sig_line, hist = macd_all(c)
        m = macd_line.dropna()
        s = sig_line.dropna()
        h = hist.dropna()

        if len(h) >= 2 and len(m) >= 2 and len(s) >= 2:
            m_now, m_prev = float(m.iloc[-1]), float(m.iloc[-2])
            s_now, s_prev = float(s.iloc[-1]), float(s.iloc[-2])
            h_now, h_prev = float(h.iloc[-1]), float(h.iloc[-2])

            # 데드크로스 / 히스토 0 하향
            macd_dead = (m_prev >= s_prev) and (m_now < s_now)
            hist_0_down = (h_prev >= 0) and (h_now < 0)

            # "약화"는 TP에서도 쓰기 위해 조금 넓게:
            # - 데드크로스 or 히스토0하향 or 최근 3봉 히스토 감소(모멘텀 둔화)
            if len(h) >= 4:
                h1, h2, h3 = float(h.iloc[-1]), float(h.iloc[-2]), float(h.iloc[-3])
                hist_falling = (h1 < h2) and (h2 < h3)
            else:
                hist_falling = False

            macd_weak = macd_dead or hist_0_down or hist_falling
    except Exception:
        macd_dead = False
        macd_weak = False

    # 거래량 계열
    vol_confirm_sell = False   # SELL용: 하락봉 + 거래량 스파이크
    vol_fade_tp = False        # TP용: 거래량 둔화(분배 가능성)
    try:
        vol = float(last["Volume"]) if "Volume" in last else np.nan
        vol20 = float(df2["Volume"].tail(20).mean()) if "Volume" in df2.columns else np.nan
        vol_ratio = (vol / vol20) if (np.isfinite(vol) and np.isfinite(vol20) and vol20 > 0) else np.nan

        prev_close = float(prev["Close"]) if "Close" in prev else close
        down_day = close < prev_close

        sell_vol_x = float(getattr(cfg, "SELL_CONFIRM_VOL_X", 1.5))
        vol_confirm_sell = down_day and np.isfinite(vol_ratio) and (vol_ratio >= sell_vol_x)

        fade_x = float(getattr(cfg, "TP_CONFIRM_VOL_FADE_X", 0.8))
        vol_fade_tp = np.isfinite(vol_ratio) and (vol_ratio <= fade_x)
    except Exception:
        vol_confirm_sell = False
        vol_fade_tp = False

    # 2일 연속 확인(휩쏘 방지) — SELL용
    two_day_confirm = False
    try:
        prev_close = float(prev["Close"])
        prev_sma20 = float(prev["SMA20"]) if "SMA20" in prev else np.nan
        two_day_confirm = (
            np.isfinite(sma20) and np.isfinite(prev_sma20) and
            (close < sma20) and (prev_close < prev_sma20)
        )
    except Exception:
        two_day_confirm = False

    # 반전 캔들(윗꼬리/음봉 등) — TP용
    reversal_tp = False
    try:
        o = float(last["Open"]); h_ = float(last["High"]); l_ = float(last["Low"]); c_ = float(last["Close"])
        rng = max(1e-9, (h_ - l_))
        upper_wick = (h_ - max(o, c_)) / rng
        max_w = float(getattr(cfg, "TP_CONFIRM_MAX_UPPER_WICK", 0.60))

        # (1) 윗꼬리 과다 + (2) 음봉 or 종가가 시가보다 크게 못 올라감
        bearish = (c_ < o) or (c_ < (l_ + rng * 0.55))
        reversal_tp = (upper_wick >= max_w) and bearish
    except Exception:
        reversal_tp = False

    # ---- 컨펌 합성 함수들 ----
    def _sell_confirm_ok() -> bool:
        if not bool(getattr(cfg, "SELL_CONFIRM_ENABLED", True)):
            return True
        mode = str(getattr(cfg, "SELL_CONFIRM_MODE", "any")).lower().strip()
        checks = []
        if bool(getattr(cfg, "SELL_CONFIRM_USE_MACD", True)): checks.append(bool(macd_dead))
        if bool(getattr(cfg, "SELL_CONFIRM_USE_VOL", True)):  checks.append(bool(vol_confirm_sell))
        if bool(getattr(cfg, "SELL_CONFIRM_USE_2D", True)):   checks.append(bool(two_day_confirm))
        if not checks:
            return True
        return all(checks) if mode == "all" else any(checks)

    def _sell_confirm_tag() -> str:
        tags = []
        if bool(getattr(cfg, "SELL_CONFIRM_USE_MACD", True)) and macd_dead:
            tags.append("MACD_DEAD")
        if bool(getattr(cfg, "SELL_CONFIRM_USE_VOL", True)) and vol_confirm_sell:
            tags.append("VOL_SPIKE")
        if bool(getattr(cfg, "SELL_CONFIRM_USE_2D", True)) and two_day_confirm:
            tags.append("2D_CONFIRM")
        return ",".join(tags) if tags else "NO_CONFIRM"

    def _tp_confirm_ok() -> bool:
        if not bool(getattr(cfg, "TAKE_PROFIT_CONFIRM_ENABLED", True)):
            return True
        mode = str(getattr(cfg, "TAKE_PROFIT_CONFIRM_MODE", "any")).lower().strip()
        checks = []
        if bool(getattr(cfg, "TP_CONFIRM_USE_MACD_WEAK", True)):           checks.append(bool(macd_weak))
        if bool(getattr(cfg, "TP_CONFIRM_USE_VOL_FADE", True)):           checks.append(bool(vol_fade_tp))
        if bool(getattr(cfg, "TP_CONFIRM_USE_REVERSAL_CANDLE", True)):    checks.append(bool(reversal_tp))
        if not checks:
            return True
        return all(checks) if mode == "all" else any(checks)

    def _tp_confirm_tag() -> str:
        tags = []
        if bool(getattr(cfg, "TP_CONFIRM_USE_MACD_WEAK", True)) and macd_weak:
            tags.append("MACD_WEAK")
        if bool(getattr(cfg, "TP_CONFIRM_USE_VOL_FADE", True)) and vol_fade_tp:
            tags.append("VOL_FADE")
        if bool(getattr(cfg, "TP_CONFIRM_USE_REVERSAL_CANDLE", True)) and reversal_tp:
            tags.append("REVERSAL")
        return ",".join(tags) if tags else "NO_CONFIRM"


    # 2) 하락봉 + 거래량 스파이크
    vol_confirm = False
    try:
        vol = float(last["Volume"]) if "Volume" in last else np.nan
        vol20 = float(df2["Volume"].tail(20).mean()) if "Volume" in df2.columns else np.nan
        vol_ratio = (vol / vol20) if (np.isfinite(vol) and np.isfinite(vol20) and vol20 > 0) else np.nan

        prev_close = float(prev["Close"]) if "Close" in prev else close
        down_day = close < prev_close
        vol_x = float(getattr(cfg, "SELL_CONFIRM_VOL_X", 1.5))
        vol_confirm = down_day and np.isfinite(vol_ratio) and (vol_ratio >= vol_x)
    except Exception:
        vol_confirm = False

    # 3) 2일 연속 확인(휩쏘 방지)
    two_day_confirm = False
    try:
        prev_close = float(prev["Close"])
        prev_sma20 = float(prev["SMA20"]) if "SMA20" in prev else np.nan
        # 기본: 2일 연속 SMA20 아래면 확인
        two_day_confirm = (
            np.isfinite(sma20) and np.isfinite(prev_sma20) and
            (close < sma20) and (prev_close < prev_sma20)
        )
    except Exception:
        two_day_confirm = False

    # 컨펌을 최종적으로 묶는 함수
    def _sell_confirm_ok() -> bool:
        if not bool(getattr(cfg, "SELL_CONFIRM_ENABLED", True)):
            return True

        mode = str(getattr(cfg, "SELL_CONFIRM_MODE", "any")).lower().strip()
        use_macd = bool(getattr(cfg, "SELL_CONFIRM_USE_MACD", True))
        use_vol = bool(getattr(cfg, "SELL_CONFIRM_USE_VOL", True))
        use_2d = bool(getattr(cfg, "SELL_CONFIRM_USE_2D", True))

        checks = []
        if use_macd: checks.append(bool(macd_dead or macd_weak))
        if use_vol:  checks.append(bool(vol_confirm))
        if use_2d:   checks.append(bool(two_day_confirm))

        if not checks:
            return True  # 컨펌을 전부 꺼둔 경우엔 통과

        if mode == "all":
            return all(checks)
        return any(checks)

    def _sell_confirm_tag() -> str:
        tags = []
        if bool(getattr(cfg, "SELL_CONFIRM_USE_MACD", True)) and (macd_dead or macd_weak):
            tags.append("MACD_DEAD")
        if bool(getattr(cfg, "SELL_CONFIRM_USE_VOL", True)) and vol_confirm:
            tags.append("VOL_SPIKE")
        if bool(getattr(cfg, "SELL_CONFIRM_USE_2D", True)) and two_day_confirm:
            tags.append("2D_CONFIRM")
        return ",".join(tags) if tags else "NO_CONFIRM"


    # trailing stop
    if np.isfinite(atr14) and np.isfinite(sma20):
        buf = float(getattr(cfg, "SELL_TRAIL_ATR_BUFFER", 0.0))
        trailing = max(sma20, close - (2.0 + buf) * atr14)
    elif np.isfinite(sma20):
        trailing = sma20
    else:
        trailing = np.nan

    # PnL
    pnl = (close - float(avg_price)) * float(shares)
    pnl_pct = (close / float(avg_price) - 1) * 100 if float(avg_price) > 0 else np.nan

    # 목표가(익절 후보): 최근 60일 고가(High60) 근접/도달
    high60 = float(df2["High"].tail(60).max()) if "High" in df2.columns and len(df2) >= 60 else np.nan
    near_h60 = float(getattr(cfg, "TAKE_PROFIT_NEAR_H60", 0.995))
    take_profit = (np.isfinite(high60) and high60 > 0 and close >= high60 * near_h60)


    # 트렌드 꺾임(확인): 2일 연속 SMA20 아래 또는 SMA50 아래
    prev_close = float(prev["Close"])
    prev_sma20 = float(prev["SMA20"]) if "SMA20" in prev else np.nan

    two_days_below_sma20 = (
        np.isfinite(sma20) and np.isfinite(prev_sma20) and
        (close < sma20) and (prev_close < prev_sma20)
    )
    below_sma50 = (np.isfinite(sma50) and close < sma50)

    # 트레일링 이탈
    below_trailing = (np.isfinite(trailing) and close < trailing)

    # 구조 붕괴(스윙 로우 이탈): 최근 N봉(오늘 제외) 스윙 로우 아래로 종가 이탈
    swing_low_lookback = int(getattr(cfg, "SELL_SWING_LOW_LOOKBACK", 20))
    structure_break = False
    swing_low_val = np.nan
    if len(df2) >= swing_low_lookback + 1:
        swing_low_val = float(df2["Low"].iloc[-(swing_low_lookback + 1):-1].min())
        if np.isfinite(swing_low_val) and swing_low_val > 0 and close < swing_low_val:
            structure_break = True

    action = "HOLD"
    reason = f"Trail {trailing:.2f} | SMA20 {sma20:.2f} | SMA50 {sma50:.2f}"

    # 우선순위: 1) 트레일 이탈  2) 추세 이탈(SMA)  3) 구조 붕괴(스윙 로우)  4) 익절
    if below_trailing:
        if _sell_confirm_ok():
            action = "SELL_TRAIL"
            reason = f"TrailingStop 하회(컨펌:{_sell_confirm_tag()}) → 추세 이탈 | {reason}"
        else:
            action = "HOLD"
            reason = f"TrailingStop 하회(컨펌부족:{_sell_confirm_tag()}) → HOLD | {reason}"

    elif below_sma50 or two_days_below_sma20:
        why = "종가<SMA50" if below_sma50 else "2일 연속 SMA20 아래 마감"
        if _sell_confirm_ok():
            action = "SELL_TREND"
            reason = f"{why}(컨펌:{_sell_confirm_tag()}) → 추세 꺾임 확인 | {reason}"
        else:
            action = "HOLD"
            reason = f"{why}(컨펌부족:{_sell_confirm_tag()}) → HOLD | {reason}"

    elif structure_break:
        action = "SELL_STRUCTURE_BREAK"
        reason = f"구조붕괴(스윙로우 {swing_low_val:.2f} 이탈) | {reason}"

    elif take_profit:
        if _tp_confirm_ok():
            action = "TAKE_PROFIT"
            reason = f"High60 {high60:.2f} 근접 + 컨펌({_tp_confirm_tag()}) → 부분/익절 고려 | {reason}"
        else:
            action = "HOLD"
            reason = f"High60 근접(컨펌부족:{_tp_confirm_tag()}) → HOLD | {reason}"

    # 분배(Distribution) 경고: 참고용. 하락일+거래량확대가 최근 N일 중 M일 이상이면 Reason에 추가
    distribution_warning = False
    distribution_reason = ""
    lb = int(getattr(cfg, "DISTRIBUTION_LOOKBACK", 10))
    min_days = int(getattr(cfg, "DISTRIBUTION_MIN_DAYS", 4))
    if len(df2) >= lb + 20:
        vol20 = float(df2["Volume"].tail(20).mean())
        if np.isfinite(vol20) and vol20 > 0:
            window = df2.iloc[-(lb + 1):-1]
            count = 0
            for i in range(1, len(window)):
                c_cur = float(window["Close"].iloc[i])
                c_prev = float(window["Close"].iloc[i - 1])
                v_cur = float(window["Volume"].iloc[i])
                if c_cur < c_prev and v_cur >= vol20:
                    count += 1
            if count >= min_days:
                distribution_warning = True
                distribution_reason = f"⚠ 분배 경고(최근{lb}일 중 {count}일 하락+거래량확대)"
    if distribution_warning:
        reason = (reason + " | " + distribution_reason).strip()

    # 추세약화(Slope) 경고: SMA20 5봉 기울기 음전 + 상승률 감소 시 참고용
    trend_weakness_warning = False
    trend_weakness_reason = ""
    slope_lb = int(getattr(cfg, "SLOPE_SMA20_LOOKBACK", 5))
    if len(df2) >= slope_lb * 2 + 1 and "SMA20" in df2.columns:
        sma20_series = df2["SMA20"].astype(float)
        s_now = float(sma20_series.iloc[-1])
        s_5ago = float(sma20_series.iloc[-(slope_lb + 1)])
        s_10ago = float(sma20_series.iloc[-(slope_lb * 2 + 1)])
        slope_now = (s_now - s_5ago) / slope_lb if slope_lb > 0 else 0.0
        slope_prev = (s_5ago - s_10ago) / slope_lb if slope_lb > 0 else 0.0
        if np.isfinite(slope_now) and np.isfinite(slope_prev) and slope_now < 0 and slope_now < slope_prev:
            trend_weakness_warning = True
            trend_weakness_reason = f"⚠ 추세약화 경고(SMA20 {slope_lb}봉 기울기 음전)"
    if trend_weakness_warning:
        reason = (reason + " | " + trend_weakness_reason).strip()

    # 변동성 붕괴(ATR Compression) 경고: ATR%가 N일 전 대비 X% 이상 감소 시 참고용
    atr_compression_warning = False
    atr_compression_reason = ""
    atr_lb = int(getattr(cfg, "ATR_COMPRESSION_LOOKBACK", 10))
    atr_drop = float(getattr(cfg, "ATR_COMPRESSION_PCT_DROP", 0.20))
    if len(df2) >= atr_lb + 1 and "ATR14" in df2.columns and "Close" in df2.columns:
        atr_now = float(df2["ATR14"].iloc[-1])
        close_now = float(df2["Close"].iloc[-1])
        atr_ago = float(df2["ATR14"].iloc[-(atr_lb + 1)])
        close_ago = float(df2["Close"].iloc[-(atr_lb + 1)])
        if close_now > 0 and close_ago > 0 and np.isfinite(atr_now) and np.isfinite(atr_ago):
            atr_pct_now = (atr_now / close_now) * 100.0
            atr_pct_ago = (atr_ago / close_ago) * 100.0
            if atr_pct_ago > 1e-6 and atr_pct_now <= atr_pct_ago * (1.0 - atr_drop):
                atr_compression_warning = True
                atr_compression_reason = "⚠ 변동성 붕괴 경고"
    if atr_compression_warning:
        reason = (reason + " | " + atr_compression_reason).strip()

    # 손실 확대 경고: 진입가 대비 손실이 N% 이상이면 참고용 경고 (실제 청산은 기존 매도 신호만)
    loss_warning = False
    loss_warning_reason = ""
    loss_thresh = float(getattr(cfg, "LOSS_WARNING_PCT", 7))
    if float(avg_price) > 0 and np.isfinite(pnl_pct) and pnl_pct <= -loss_thresh:
        loss_warning = True
        loss_warning_reason = f"⚠ 손실 확대(진입가 대비 {pnl_pct:.1f}%)"
    if loss_warning:
        reason = (reason + " | " + loss_warning_reason).strip()

    # 보유 기간 경고: days_held가 만료 D일 전 이상이면 참고용 (caller가 days_held 전달 시에만)
    holding_warning = False
    holding_warning_reason = ""
    if not skip_holding_days_warning:
        _max_hold = int(max_hold_days) if max_hold_days is not None else int(getattr(cfg, "MAX_HOLD_DAYS_DEFAULT", 15))
        _near = int(getattr(cfg, "HOLDING_DAYS_WARNING_NEAR", 2))
        if days_held is not None and int(days_held) >= _max_hold - _near:
            holding_warning = True
            holding_warning_reason = f"⚠ 보유 기간 {int(days_held)}일(만료 {_max_hold}일 근접)"
    if holding_warning:
        reason = (reason + " | " + holding_warning_reason).strip()

    # 실적/이벤트 전 경고: 다음 실적 발표가 N일 이내면 참고용 (skip_earnings_warning 시 yfinance 호출 없음)
    earnings_warning = False
    earnings_warning_reason = ""
    if not skip_earnings_warning:
        try:
            last_dt = df2.index[-1]
            ref_date = last_dt.date() if hasattr(last_dt, "date") else (pd.Timestamp(last_dt).date() if hasattr(pd, "Timestamp") else datetime.utcnow().date())
        except Exception:
            ref_date = datetime.utcnow().date()
        warn_days = int(getattr(cfg, "EARNINGS_WARNING_DAYS", 10))
        try:
            t = yf.Ticker(ticker)
            future_dates = []
            if hasattr(t, "get_earnings_dates"):
                ed_df = t.get_earnings_dates(limit=8)
                if ed_df is not None and not ed_df.empty and hasattr(ed_df.index, "tolist"):
                    for d in ed_df.index.tolist():
                        try:
                            earn_d = pd.Timestamp(d).date()
                            if earn_d >= ref_date:
                                future_dates.append(earn_d)
                        except Exception:
                            continue
            if not future_dates and hasattr(t, "calendar"):
                cal = t.calendar
                if isinstance(cal, dict) and "Earnings Date" in cal:
                    ed = cal["Earnings Date"]
                    if isinstance(ed, (list, tuple)) and len(ed) > 0:
                        for x in ed:
                            try:
                                d = pd.Timestamp(x).date()
                                if d >= ref_date:
                                    future_dates.append(d)
                            except Exception:
                                continue
                    elif ed is not None:
                        try:
                            d = pd.Timestamp(ed).date()
                            if d >= ref_date:
                                future_dates.append(d)
                        except Exception:
                            pass
            if not future_dates and hasattr(t, "info"):
                info = t.info or {}
                for key in ("earningsDate", "nextEarningsDate"):
                    ed = info.get(key)
                    if ed is not None:
                        try:
                            if isinstance(ed, (list, tuple)) and len(ed) > 0:
                                ed = ed[0]
                            d = pd.Timestamp(ed).date()
                            if d >= ref_date:
                                future_dates.append(d)
                            break
                        except Exception:
                            continue
            next_earnings = min(future_dates) if future_dates else None
            if next_earnings is not None:
                delta = (next_earnings - ref_date).days
                if 0 <= delta <= warn_days:
                    earnings_warning = True
                    earnings_warning_reason = f"⚠ 실적 발표 {delta}일 전"
        except Exception:
            pass
    if earnings_warning:
        reason = (reason + " | " + earnings_warning_reason).strip()

    return {
        "Ticker": ticker,
        "Action": action,
        "Close": round(close, 2),
        "SMA20": round(sma20, 2) if np.isfinite(sma20) else None,
        "SMA50": round(sma50, 2) if np.isfinite(sma50) else None,
        "TrailingStop": round(trailing, 2) if np.isfinite(trailing) else None,
        "High60": round(high60, 2) if np.isfinite(high60) else None,
        "PnL": round(pnl, 2),
        "PnL%": round(pnl_pct, 2) if np.isfinite(pnl_pct) else None,
        "Reason": reason,
        "DistributionWarning": distribution_warning,
        "DistributionReason": distribution_reason or None,
        "TrendWeaknessWarning": trend_weakness_warning,
        "TrendWeaknessReason": trend_weakness_reason or None,
        "ATRCompressionWarning": atr_compression_warning,
        "ATRCompressionReason": atr_compression_reason or None,
        "LossWarning": loss_warning,
        "LossWarningReason": loss_warning_reason or None,
        "HoldingDaysWarning": holding_warning,
        "HoldingDaysReason": holding_warning_reason or None,
        "EarningsWarning": earnings_warning,
        "EarningsWarningReason": earnings_warning_reason or None
    }


def backtest_signal_dates(df2: pd.DataFrame, ticker: str):
    """
    과거 각 일자에 대해 decide_entry / holding_risk_review를 적용해
    매수 신호 날짜·매도 신호 날짜·매도 시 진입가를 반환.
    - buy_dates, sell_dates: 해당 날짜 인덱스(타임스탬프) 리스트.
    - sell_entry_prices: sell_dates와 동일 순서로, 각 매도에 대응하는 진입가 리스트.
    """
    if df2 is None or len(df2) < 260:
        return [], [], []
    buy_dates = []
    sell_dates = []
    sell_entry_prices = []
    max_hold = int(getattr(cfg, "MAX_HOLD_DAYS_DEFAULT", 15))
    start_i = 200  # SMA200 등 지표 유효 구간
    in_position = False
    entry_price = None
    entry_date = None

    for i in range(start_i, len(df2)):
        df_slice = df2.iloc[: i + 1]
        try:
            idx_date = df2.index[i]
            if hasattr(idx_date, "date"):
                cur_date = idx_date.date()
            else:
                cur_date = pd.Timestamp(idx_date).date()
        except Exception:
            cur_date = None

        if not in_position:
            try:
                entry, *_ = decide_entry(df_slice)
                if str(entry).startswith("BUY_"):
                    buy_dates.append(df2.index[i])
                    entry_price = float(df_slice.iloc[-1]["Close"])
                    entry_date = cur_date
                    in_position = True
            except Exception:
                pass
            continue

        # 보유 중: 매도 조건 검사
        try:
            if entry_date is None:
                in_position = False
                continue
            days_held = (cur_date - entry_date).days if cur_date else 0
            risk = holding_risk_review(
                df_slice, ticker, 1.0, entry_price,
                days_held=days_held, max_hold_days=max_hold,
                skip_earnings_warning=True,
                skip_holding_days_warning=True,
            )
            action = risk.get("Action", "HOLD")
            if action in ("SELL_TRAIL", "SELL_TREND", "SELL_STRUCTURE_BREAK", "TAKE_PROFIT"):
                sell_dates.append(df2.index[i])
                sell_entry_prices.append(entry_price)
                in_position = False
                entry_price = None
                entry_date = None
        except Exception:
            pass

    return buy_dates, sell_dates, sell_entry_prices


def _round_up_to_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.ceil(x / step) * step

def _tp_step_by_price(price: float) -> float:
    # 가격대별 '보기 좋은' 목표가 단위
    if price >= 500:
        return 50
    if price >= 200:
        return 25
    if price >= 100:
        return 10
    if price >= 50:
        return 5
    return 1

def build_partial_tp_plan(df2: pd.DataFrame, boost: bool = False) -> str:
    """
    ATR 기반 동적 부분익절 플랜:
    - HOLD: 기본 ATR 배수
    - boost=True(ADD_BUY): 목표가 배수 상향
    """
    if df2 is None or df2.empty:
        return ""

    last = df2.iloc[-1]
    close = float(last["Close"]) if "Close" in last else np.nan
    atr14 = float(last["ATR14"]) if "ATR14" in last else np.nan

    if not (np.isfinite(close) and close > 0 and np.isfinite(atr14) and atr14 > 0):
        return ""

    step = _tp_step_by_price(close)

    # ✅ 설정값이 있으면 우선 사용 (없으면 기본값)
    base_m1 = float(getattr(cfg, "TP_ATR_M1", 1.0))
    base_m2 = float(getattr(cfg, "TP_ATR_M2", 2.0))
    base_m3 = float(getattr(cfg, "TP_ATR_M3", 3.0))

    boost_m1 = float(getattr(cfg, "TP_ATR_BOOST_M1", 1.3))
    boost_m2 = float(getattr(cfg, "TP_ATR_BOOST_M2", 2.6))
    boost_m3 = float(getattr(cfg, "TP_ATR_BOOST_M3", 4.0))

    if boost:
        m1, m2, m3 = boost_m1, boost_m2, boost_m3
        tag = "목표가(상향, ATR)"
    else:
        m1, m2, m3 = base_m1, base_m2, base_m3
        tag = "목표가(ATR)"

    # ✅ ATR 기반 목표가
    t1_raw = close + atr14 * m1
    t2_raw = close + atr14 * m2
    t3_raw = close + atr14 * m3

    t1 = _round_up_to_step(t1_raw, step)
    t2 = _round_up_to_step(t2_raw, step)
    t3 = _round_up_to_step(t3_raw, step)
    
        # =========================
    # ✅ High60 캡(현실화) + ATR 바닥(최소 보장)
    # =========================
    use_cap = bool(getattr(cfg, "TP_USE_HIGH60_CAP", True))

    # 최근 60일 고점(High60)
    high60 = np.nan
    try:
        if "High" in df2.columns and len(df2) >= 60:
            high60 = float(df2["High"].tail(60).max())
    except Exception:
        high60 = np.nan

    # 2차/3차 최소 바닥(ATR 기반 최소 보장)
    floor_m2 = float(getattr(cfg, "TP_FLOOR_ATR_M2", 3.0))
    floor_m3 = float(getattr(cfg, "TP_FLOOR_ATR_M3", 4.5))
    t2_floor = close + atr14 * floor_m2
    t3_floor = close + atr14 * floor_m3

    # 2차/3차 High60 상한 캡
    cap2_mult = float(getattr(cfg, "TP_CAP_H60_MULT_2", 1.02))
    cap3_mult = float(getattr(cfg, "TP_CAP_H60_MULT_3", 1.05))

    if use_cap and np.isfinite(high60) and high60 > 0:

        # ✅ High60 돌파(종가 확인) 이후에는 캡 자동 해제
        if bool(getattr(cfg, "TP_CAP_DISABLE_ON_BREAKOUT", True)):
            buf = float(getattr(cfg, "TP_CAP_DISABLE_BUFFER", 0.002))
            if close >= high60 * (1 + buf):
                use_cap = False

        # 캡이 유지되는 경우에만 적용
        if use_cap:
            cap2 = high60 * cap2_mult
            cap3 = high60 * cap3_mult

            t2_raw = min(t2_raw, cap2)
            t3_raw = min(t3_raw, cap3)


    # 바닥 보정(캡 때문에 과도하게 낮아지는 경우 방지)
    t2_raw = max(t2_raw, t2_floor)
    t3_raw = max(t3_raw, t3_floor)


    # 단조 증가 보장(라운딩 때문에 같아질 수 있어 방어)
    if t2 <= t1:
        t2 = t1 + step
    if t3 <= t2:
        t3 = t2 + step

    return (f"{tag}: 1차 {t1:.0f} 1/3익절, "
            f"2차 {t2:.0f} 1/2 익절, "
            f"3차 {t3:.0f} 전량 익절")

# -----------------------------
# 보유 종목 추천(추가매수/매도/보유) 생성
# -----------------------------
def recommend_for_holding(df2, ticker, shares, avg_price):
    """
    출력:
      Reco: SELL / SELL(부분/익절) / ADD_BUY / HOLD
      Why: 근거 문자열
      AddPlan: 추가매수 시 플랜(Entry/Stop/Target/RR/Shares)
    """
    # 1) 보유 리스크(매도/익절) 판단
    risk = holding_risk_review(df2, ticker, shares, avg_price)

    # 2) 추가매수 판단: 지금도 진입 신호가 뜨는지 + RR 통과
    entry, trigger, entry_hint, invalid, note = decide_entry(df2)
    # ✅ 미네르비니 필터(보유 추가매수에도 동일 적용)
    last_row = df2.iloc[-1]
    last_close_h = float(last_row["Close"])
    if str(entry).startswith("BUY_") and getattr(cfg, "MINERVINI_52WK_ENABLED", False):
        high52 = float(df2["High"].tail(252).max()) if len(df2) >= 252 else np.nan
        if np.isfinite(high52) and high52 > 0:
            pct_off = (1.0 - last_close_h / high52) * 100.0
            max_off = (float(getattr(cfg, "MINERVINI_52WK_MAX_PCT_OFF_PULLBACK", 0.35)) * 100.0
                       if entry == "BUY_PULLBACK" else float(getattr(cfg, "MINERVINI_52WK_MAX_PCT_OFF", 0.25)) * 100.0)
            if pct_off > max_off:
                entry = "WATCH_52WK"
    if str(entry).startswith("BUY_") and getattr(cfg, "MINERVINI_MA_STACK_ENABLED", False):
        s50 = float(last_row.get("SMA50", np.nan))
        s150 = float(last_row.get("SMA150", np.nan))
        s200 = float(last_row.get("SMA200", np.nan))
        if all(np.isfinite([s50, s150, s200])):
            stack_ok = (s50 > s150 and s150 > s200)
            if entry == "BUY_PULLBACK":
                if not stack_ok:
                    entry = "WATCH_MA_STACK"
            else:
                if not (last_close_h > s50 and stack_ok):
                    entry = "WATCH_MA_STACK"
    if str(entry).startswith("BUY_") and getattr(cfg, "MINERVINI_FUNDAMENTAL_ENABLED", False):
        try:
            t = yf.Ticker(ticker)
            info = t.info if hasattr(t, "info") else {}
            min_eps = getattr(cfg, "MINERVINI_EPS_GROWTH_MIN_PCT", 20) / 100.0
            min_rev = getattr(cfg, "MINERVINI_REVENUE_GROWTH_MIN_PCT", 15) / 100.0
            min_roe = getattr(cfg, "MINERVINI_ROE_MIN_PCT", 17) / 100.0
            if (info.get("earningsGrowth") is not None and info["earningsGrowth"] < min_eps) or \
               (info.get("revenueGrowth") is not None and info["revenueGrowth"] < min_rev) or \
               (info.get("returnOnEquity") is not None and info["returnOnEquity"] < min_roe):
                entry = "WATCH_FUNDAMENTAL"
        except Exception:
            pass
    # ✅ RR 디버그(원하면 True/False로 끄기)
    if bool(getattr(cfg, "DEBUG_RR", True)):
        debug_trade_plan_rr(df2, entry, ticker=ticker)


    plan = None
    add_ok = False
    if str(entry).startswith("BUY_"):
        plan = calc_trade_plan(df2, entry)
        if plan is not None and plan["RR"] >= cfg.MIN_RR and plan["Shares"] > 0:
            add_ok = True
        else:
            entry = "SKIP"
            plan = None

    # 3) 최종 추천(우선순위: 방어매도 > 익절 > 추가매수 > 보유)
    if risk["Action"] in ("SELL_TRAIL", "SELL_TREND", "SELL_STRUCTURE_BREAK"):
        return {
            "Ticker": ticker,
            "Reco": "SELL",
            "SellSignal": risk["Action"],
            "AddSignal": entry,
            "Close": risk["Close"],
            "PnL": risk["PnL"],
            "PnL%": risk["PnL%"],
            "Why": risk.get("Reason", ""),
            "AddPlan": None
        }

    if risk["Action"] == "TAKE_PROFIT":
        return {
            "Ticker": ticker,
            "Reco": "SELL(부분/익절)",
            "SellSignal": risk["Action"],
            "AddSignal": entry,
            "Close": risk["Close"],
            "PnL": risk["PnL"],
            "PnL%": risk["PnL%"],
            "Why": risk.get("Reason", ""),
            "AddPlan": None
        }

    if add_ok:
        why = (f"{trigger} | RR {plan['RR']} | "
               f"Add {int(plan['Shares'])}sh (~${plan['PosValue']}) | "
               f"Inval: {invalid}")
        
        tp_up = build_partial_tp_plan(df2, boost=True)
        if tp_up:
            why = (why + " | " + tp_up).strip()

        return {
            "Ticker": ticker,
            "Reco": "ADD_BUY",
            "SellSignal": risk["Action"],
            "AddSignal": entry,
            "Close": risk["Close"],
            "PnL": risk["PnL"],
            "PnL%": risk["PnL%"],
            "Why": why,
            "AddPlan": plan
        }
    tp_text = build_partial_tp_plan(df2, boost=False)

    base_why = (risk.get("Reason", "") + " | 추가매수 신호 없음").strip()
    if tp_text:
        base_why = (base_why + " | " + tp_text).strip()
        
    # 기본: 보유 유지
    return {
        "Ticker": ticker,
        "Reco": "HOLD",
        "SellSignal": risk["Action"],
        "AddSignal": entry,
        "Close": risk["Close"],
        "PnL": risk["PnL"],
        "PnL%": risk["PnL%"],
        "Why": base_why,
        "AddPlan": None
    }

def analyze_single_ticker(ticker: str, shares: float = 0.0, avg_price: float = 0.0):
    global MARKET_STATE  # ✅ 반드시 첫 줄 (SyntaxError 방지)

    ticker = ticker.upper().strip()
    end = datetime.utcnow().date()
    start = end - timedelta(days=cfg.LOOKBACK_DAYS)

    # -------------------------
    # 가격 데이터 다운로드
    # -------------------------
    df = yf.download(
        ticker,
        start=str(start),
        end=str(end + timedelta(days=1)),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=True
    )

    if df is None or df.empty:
        return {"error": "No price data"}

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.dropna(subset=["Open", "High", "Low", "Close", "Volume"]).copy()
    if len(df) < 140:
        return {"error": "Not enough candles (need 140+)"}

    # -------------------------
    # ✅ 시장 레짐 계산 (스캔과 동일 환경 만들기)
    # -------------------------
    regime_data = None
    try:
        regime_data = yf.download(
            ["SPY"],
            start=str(start),
            end=str(end + timedelta(days=1)),
            interval="1d",
            group_by="ticker",   # 중요: multi-index 구조
            auto_adjust=False,
            progress=False,
            threads=True
        )

        ms = compute_market_state_from_data(regime_data)

    except Exception:
        ms = MARKET_STATE  # fallback

    # ★ 여기서 레짐 주입
    MARKET_STATE = ms

    # -------------------------
    # 지표 계산
    # -------------------------
    close = df["Close"]
    df["SMA20"] = sma(close, 20)
    df["SMA50"] = sma(close, 50)
    df["SMA150"] = sma(close, 150)
    df["SMA200"] = sma(close, 200)
    df["ATR14"] = atr(df, 14)
    df["ADX14"] = adx(df, 14)
    df["MACD_H"] = macd_hist(close)
    df["RSI14"] = rsi(close, 14)

    df2 = df.dropna().copy()
    if len(df2) < 140:
        return {"error": "Indicator warmup failed"}

    # -------------------------
    # 보유 분석
    # -------------------------
    last_close = float(df2.iloc[-1]["Close"])
    if shares is None:
        shares = 0.0
    use_avg = float(avg_price) if (avg_price is not None and float(avg_price) > 0) else float(last_close)

    risk = holding_risk_review(df2, ticker, shares, use_avg)

    # -------------------------
    # 진입 신호 분석
    # -------------------------
    entry, trigger, entry_hint, invalid, note = decide_entry(df2)
    # ✅ ADX(추세 강도) 필터: 추세 없으면 BUY → WATCH_ADX
    min_adx = float(getattr(cfg, "MIN_ADX", 20))
    if str(entry).startswith("BUY_"):
        last_row = df2.iloc[-1]
        adx14 = float(last_row.get("ADX14", np.nan)) if "ADX14" in last_row.index else np.nan
        if np.isfinite(adx14) and adx14 < min_adx:
            entry = "WATCH_ADX"
            note = (str(note) + f" | 추세없음(ADX {adx14:.1f} < {min_adx})").strip(" |")
    # ✅ Relative Strength (시장 대비 강도): SPY보다 약하면 BUY → WATCH_RS (ADD_BUY 경로)
    rs_lookback = int(getattr(cfg, "RS_LOOKBACK_DAYS", 20))
    if str(entry).startswith("BUY_") and regime_data is not None:
        try:
            if isinstance(regime_data.columns, pd.MultiIndex) and "SPY" in regime_data.columns.get_level_values(0):
                spy_close = regime_data["SPY"]["Close"].copy()
            else:
                spy_close = regime_data["Close"].copy() if "Close" in regime_data.columns else None
        except Exception:
            spy_close = None
        if spy_close is not None:
            stock_ret, spy_ret = compute_rs_vs_spy(df2, spy_close, rs_lookback)
            if stock_ret is not None and spy_ret is not None and stock_ret <= spy_ret:
                entry = "WATCH_RS"
                note = (str(note) + f" | 시장대비 약함(RS {rs_lookback}일: 종목 {stock_ret*100:.1f}% vs SPY {spy_ret*100:.1f}%)").strip(" |")
    # ✅ 미네르비니 1) 52주 고점 근접
    if str(entry).startswith("BUY_") and getattr(cfg, "MINERVINI_52WK_ENABLED", False):
        high52 = float(df2["High"].tail(252).max()) if len(df2) >= 252 else np.nan
        if np.isfinite(high52) and high52 > 0:
            pct_off = (1.0 - last_close / high52) * 100.0
            max_off = (float(getattr(cfg, "MINERVINI_52WK_MAX_PCT_OFF_PULLBACK", 0.35)) * 100.0
                       if entry == "BUY_PULLBACK" else float(getattr(cfg, "MINERVINI_52WK_MAX_PCT_OFF", 0.25)) * 100.0)
            if pct_off > max_off:
                entry = "WATCH_52WK"
                note = (str(note) + f" | 52주고점에서 {pct_off:.1f}% 아래(>{max_off:.0f}% 허용)").strip(" |")
    # ✅ 미네르비니 2) 이평 정렬. BUY_PULLBACK은 SMA 근처이므로 가격>SMA50 생략
    if str(entry).startswith("BUY_") and getattr(cfg, "MINERVINI_MA_STACK_ENABLED", False):
        last_row = df2.iloc[-1]
        s50 = float(last_row.get("SMA50", np.nan))
        s150 = float(last_row.get("SMA150", np.nan))
        s200 = float(last_row.get("SMA200", np.nan))
        if all(np.isfinite([s50, s150, s200])):
            stack_ok = (s50 > s150 and s150 > s200)
            if entry == "BUY_PULLBACK":
                if not stack_ok:
                    entry = "WATCH_MA_STACK"
                    note = (str(note) + " | 이평정렬 미충족(SMA50>SMA150>SMA200)").strip(" |")
            else:
                if not (last_close > s50 and stack_ok):
                    entry = "WATCH_MA_STACK"
                    note = (str(note) + " | 이평정렬 미충족(가격>SMA50>SMA150>SMA200)").strip(" |")
    # ✅ 미네르비니 3) 펀더멘털(실적): 단일종목만. yfinance info 1회 호출.
    if str(entry).startswith("BUY_") and getattr(cfg, "MINERVINI_FUNDAMENTAL_ENABLED", False):
        try:
            t = yf.Ticker(ticker)
            info = t.info if hasattr(t, "info") else {}
            eps_g = info.get("earningsGrowth")
            rev_g = info.get("revenueGrowth")
            roe = info.get("returnOnEquity")
            min_eps = getattr(cfg, "MINERVINI_EPS_GROWTH_MIN_PCT", 20) / 100.0
            min_rev = getattr(cfg, "MINERVINI_REVENUE_GROWTH_MIN_PCT", 15) / 100.0
            min_roe = getattr(cfg, "MINERVINI_ROE_MIN_PCT", 17) / 100.0
            fail = []
            if eps_g is not None and eps_g < min_eps:
                fail.append(f"EPS성장{eps_g*100:.0f}%<{min_eps*100:.0f}%")
            if rev_g is not None and rev_g < min_rev:
                fail.append(f"매출성장{rev_g*100:.0f}%<{min_rev*100:.0f}%")
            if roe is not None and roe < min_roe:
                fail.append(f"ROE{roe*100:.0f}%<{min_roe*100:.0f}%")
            if fail:
                entry = "WATCH_FUNDAMENTAL"
                note = (str(note) + " | 실적미달(" + ", ".join(fail) + ")").strip(" |")
        except Exception:
            pass
    # ✅ DEBUG: RR 계산 바로 확인 (entry가 정의된 이후에만!)
    try:
        if str(entry).startswith("BUY_"):
            debug_trade_plan_rr(df2, entry)
    except Exception as _e:
        print("[DEBUG] debug_trade_plan_rr failed:", _e)


    add_ok = False
    plan = None

    if str(entry).startswith("BUY_"):
        plan = calc_trade_plan(df2, entry)
        if plan is not None and plan["RR"] >= cfg.MIN_RR and plan["Shares"] > 0:
            add_ok = True
        else:
            entry = "SKIP"
            plan = None

    # -------------------------
    # 최종 추천 결정
    # -------------------------
    if risk["Action"] in ("SELL_TRAIL", "SELL_TREND", "SELL_STRUCTURE_BREAK"):
        reco = "SELL"
        why = risk.get("Reason", "")

    elif risk["Action"] == "TAKE_PROFIT":
        reco = "SELL(부분/익절)"
        why = risk.get("Reason", "")

    elif add_ok:
        reco = "ADD_BUY"
        why = f"{trigger} | RR {plan['RR']} | Inval: {invalid}"

    else:
        reco = "HOLD"
        why = f"{risk.get('Reason','')} | 추가매수 신호 없음"

    # -------------------------
    # 반환
    # -------------------------
    return {
        "ticker": ticker,
        "reco": reco,
        "why": why,
        "close": risk.get("Close"),
        "sell_signal": risk.get("Action"),
        "add_signal": entry,
        "plan": plan,
        "risk": risk,
        "market_state": ms,
        "df_tail": df2.tail(30).copy()
    }



def query_loop():
    """
    터미널 '검색창' 모드:
    - 티커 입력하면 즉시 ADD/SELL/HOLD와 근거 출력
    - avg_price, shares는 선택(엔터면 생략)
    """
    print("\n=== Ticker Search Mode (ADD/SELL/HOLD) ===")
    print("종료: q 또는 quit 입력\n")

    while True:
        t = input("Ticker (예: AAPL): ").strip()
        if t.lower() in ("q", "quit", "exit"):
            break
        if not t:
            continue

        ap = input("AvgPrice (없으면 엔터): ").strip()
        sh = input("Shares (없으면 엔터=1): ").strip()

        try:
            avg_price = float(ap) if ap != "" else None
        except:
            avg_price = None

        try:
            shares = float(sh) if sh != "" else 1.0
        except:
            shares = 1.0

        res = analyze_single_ticker(t, shares=shares, avg_price=avg_price)

        if res is None:
            print("분석 실패\n")
            continue

        # ✅ analyze_single_ticker는 소문자 키를 쓰는 버전이 있고,
        # query_loop는 대문자 키를 기대하는 버전이 있음 → 여기서 통일
        Ticker = res.get("Ticker", res.get("ticker", t))
        Reco   = res.get("Reco",   res.get("reco", ""))
        Close  = res.get("Close",  res.get("close", None))
        SellSignal = res.get("SellSignal", res.get("sell_signal", None))
        AddSignal  = res.get("AddSignal",  res.get("add_signal", None))
        Why    = res.get("Why",    res.get("why", ""))
        AddPlan = res.get("AddPlan", res.get("plan", None))

        # error 키도 대/소문자 혼재 방어
        err = res.get("Error", res.get("error", None))
        if err:
            print(f"❌ {Ticker} - {err}\n")
            continue

        print(f"\n[{Ticker}]  ✅ RECO: {Reco}")
        print(f"- Close: {Close} | SellSignal: {SellSignal} | AddSignal: {AddSignal}")
        print(f"- Why: {Why}")

        # ADD_BUY 플랜 출력(키 혼재 방어)
        if (str(Reco).upper() == "ADD_BUY") and AddPlan:
            entry_p = AddPlan.get("EntryPrice")
            stop_p  = AddPlan.get("StopPrice")
            targ_p  = AddPlan.get("TargetPrice")
            rr      = AddPlan.get("RR")
            sh_     = AddPlan.get("Shares")
            pv_     = AddPlan.get("PosValue")

            print(f"- AddPlan: Entry {entry_p} | Stop {stop_p} | Target {targ_p} | RR {rr}")
            print(f"- Size(1R): {sh_} sh (~${pv_})")

        print("")


def portfolio_edit_loop(path="positions.csv"):
    print("\n=== Portfolio Edit Mode (positions.csv) ===")
    print("명령: add / del / list / back\n")
    print("ADD 입력 예시: AAPL,10,190.2")
    print("DEL 입력 예시: AAPL\n")

    while True:
        cmd = input("Command (add/del/list/back): ").strip().lower()
        if cmd in ("back", "b", "q", "quit", "exit"):
            break

        if cmd == "list":
            print_positions(path)
            print("")
            continue

        if cmd == "add":
            line = input("Input (Ticker,Shares,AvgPrice): ").strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) != 3:
                print("❌ 형식 오류. 예: AAPL,10,190.2\n")
                continue
            t, sh, ap = parts
            try:
                add_or_update_position(t, float(sh), float(ap), path=path, mode="merge")
                print(f"✅ 추가/업데이트 완료: {t.upper()} (merge)\n")
            except Exception as e:
                print(f"❌ 실패: {e}\n")
            continue

        if cmd == "del":
            df = load_positions(path)
            if df.empty:
                print("⚠️ 포트폴리오가 비어있습니다.\n")
                continue

            tickers = df["Ticker"].astype(str).str.upper().tolist()
            print("\n현재 보유 종목:")
            for i, tk in enumerate(tickers, 1):
                print(f"{i}. {tk}")
            print("")

            t = input("삭제할 Ticker 입력 (번호 또는 티커): ").strip().upper()
            if not t:
                continue

            if t.isdigit():
                idx = int(t) - 1
                if 0 <= idx < len(tickers):
                    t = tickers[idx]
                else:
                    print("❌ 잘못된 번호입니다.\n")
                    continue

            try:
                n = remove_position(t, path=path)
                if n > 0:
                    print(f"🗑️ {t} 제거 완료\n")
                else:
                    print(f"⚠️ 해당 티커 없음: {t}\n")
            except Exception as e:
                print(f"❌ 실패: {e}\n")
            continue

        print("❌ 알 수 없는 명령입니다. add/del/list/back 중 선택하세요.\n")




def embed_for_portfolio(recos_df, run_date):
    """
    recos_df: columns = [Ticker, Reco, Close, PnL, PnL%, Why]
    """
    if recos_df is None or recos_df.empty:
        desc = "보유 종목 추천 결과 없음 (positions.csv 없거나 데이터 부족)"
    else:
        # 정렬: SELL -> 익절 -> ADD -> HOLD
        order = {"SELL": 0, "SELL(부분/익절)": 1, "ADD_BUY": 2, "HOLD": 9}
        df = recos_df.copy()
        df["P"] = df["Reco"].map(order).fillna(9).astype(int)
        df = df.sort_values(["P", "PnL%"], ascending=[True, True]).drop(columns=["P"])

        lines = []
        for _, r in df.iterrows():
            lines.append(
                f"**{r['Ticker']}** `{r['Reco']}` | Close {r['Close']} | PnL ${r['PnL']} ({r['PnL%']}%)\n"
                f"{r['Why']}"
            )
        desc = "\n\n".join(lines)[:3900]

    return {
        "title": f"🟣 PORTFOLIO (ADD/SELL/HOLD) — {run_date}",
        "description": desc,
        "color": 0x9B59B6  # purple
    }


# -----------------------------
# Relative Strength (시장 대비 강도) vs SPY
# -----------------------------
def compute_rs_vs_spy(df2, spy_close_series, lookback=20):
    """
    df2: 종목 OHLC (최소 lookback+1 행). index=날짜.
    spy_close_series: SPY 종가 시리즈 (날짜 인덱스). df2.index에 맞춰 reindex됨.
    반환: (stock_ret, spy_ret) 소수(0.05=5%) 또는 데이터 부족 시 (None, None)
    """
    if df2 is None or df2.empty or len(df2) < lookback + 1:
        return None, None
    try:
        spy_aligned = spy_close_series.reindex(df2.index).ffill().bfill()
        if spy_aligned.isna().any() or len(spy_aligned) < lookback + 1:
            return None, None
        close = df2["Close"]
        stock_ret = (float(close.iloc[-1]) / float(close.iloc[-1 - lookback])) - 1.0
        spy_ret = (float(spy_aligned.iloc[-1]) / float(spy_aligned.iloc[-1 - lookback])) - 1.0
        if not (np.isfinite(stock_ret) and np.isfinite(spy_ret)):
            return None, None
        return stock_ret, spy_ret
    except Exception:
        return None, None


# -----------------------------
# 스코어/필터 (그대로)
# -----------------------------
def score_stock(df, ticker, market_state=None, data=None):
    if df is None or df.empty:
        return None
     # ✅ 데이터 품질 체크(신뢰도)
    end_date = datetime.utcnow().date()
    ok, q_reason = data_quality_check(df, end_date, max_stale_days=getattr(cfg, "MAX_DATA_STALE_DAYS", 5))
    
    if not ok:
        # 필요하면 로그 찍고 싶을 때만 print
        # print(f"[SKIP][{ticker}] data quality fail: {q_reason}")
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.dropna(subset=["Open","High","Low","Close","Volume"]).copy()
    if len(df) < 140:
        return None

    close = df["Close"]
    df["SMA20"] = sma(close, 20)
    df["SMA50"] = sma(close, 50)
    df["SMA150"] = sma(close, 150)
    df["SMA200"] = sma(close, 200)
    df["ATR14"] = atr(df, 14)
    df["ADX14"] = adx(df, 14)
    macd_line, sig_line, hist = macd_all(close)
    df["MACD"] = macd_line
    df["MACD_SIG"] = sig_line
    df["MACD_H"] = hist

    df["RSI14"] = rsi(close, 14)

    df2 = df.dropna().copy()
    if len(df2) < 140:
        return None

    last = df2.iloc[-1]
    last_close = float(last["Close"])

    # 유동성
    dollar_vol_20 = float((df2["Close"].tail(20) * df2["Volume"].tail(20)).mean())
    if dollar_vol_20 < cfg.MIN_DOLLAR_VOL:
        return None

    # 시총(선호/태그는 main에서 처리, 여기선 숫자만 가져오되 필터는 하지 않게 유지 가능)
    mktcap_usd = get_market_cap_usd(ticker)
    mktcap_krw = (mktcap_usd * cfg.KRW_PER_USD) if (mktcap_usd is not None) else None
    if cfg.USE_MKT_CAP_FILTER:
        if mktcap_krw is None or not (cfg.MKT_CAP_MIN_KRW <= mktcap_krw <= cfg.MKT_CAP_MAX_KRW):
            return None

    # 기본 점수
    score = 0
    reasons = []

    high20 = float(df2["High"].tail(20).max())
    near_high = last_close / high20 if high20 > 0 else np.nan
    if np.isfinite(near_high) and near_high >= 0.99:
        score += 25; reasons.append("20D high 근접")

    vol_ratio = float(df2["Volume"].iloc[-1] / df2["Volume"].tail(20).mean())

# ✅ 볼륨 점수 계단형(신뢰도 상승)
    if vol_ratio >= 2.0:
        score += 18; reasons.append(f"Vol {vol_ratio:.1f}x (강함)")
    elif vol_ratio >= 1.5:
        score += 14; reasons.append(f"Vol {vol_ratio:.1f}x")
    elif vol_ratio >= 1.2:
        score += 8; reasons.append(f"Vol {vol_ratio:.1f}x (약)")


    # ✅ MACD 트리거(개선이 아니라 실제 신호로)
    # 1) MACD 라인 상향 크로스: MACD가 시그널을 위로 돌파
    macd_now = float(last["MACD"])
    sig_now = float(last["MACD_SIG"])
    macd_prev = float(df2["MACD"].iloc[-2]) if len(df2) >= 2 else macd_now
    sig_prev = float(df2["MACD_SIG"].iloc[-2]) if len(df2) >= 2 else sig_now

    macd_bull_cross = (macd_prev <= sig_prev) and (macd_now > sig_now)

    # 2) 히스토그램 0선 상향 돌파
    h_now = float(last["MACD_H"])
    h_prev = float(df2["MACD_H"].iloc[-2]) if len(df2) >= 2 else h_now
    hist_zero_cross = (h_prev <= 0) and (h_now > 0)

    macd_buy_signal = macd_bull_cross or hist_zero_cross
    # ✅ MACD 트리거 문자열(Embed 표시용)
    macd_trigger = ""
    if macd_bull_cross:
        macd_trigger = "CROSS_UP"
    elif hist_zero_cross:
        macd_trigger = "HIST_0_UP"


    if macd_bull_cross:
        score += 12
        reasons.append("MACD 상향크로스")
    elif hist_zero_cross:
        score += 10
        reasons.append("MACD 히스토 0선 돌파")
    else:
        # 약한 개선(옵션): 완전 삭제해도 됨
        # 최근 5봉 평균 대비 히스토가 상승하면 +3 정도
        try:
            h_mean_prev5 = float(df2["MACD_H"].iloc[-7:-2].mean())
            if np.isfinite(h_mean_prev5) and h_now > h_mean_prev5:
                score += 3
                reasons.append("MACD 완만개선")
        except Exception:
            pass


    rsi_now = float(last["RSI14"])
    if np.isfinite(rsi_now) and 40 <= rsi_now <= 72:
        score += 10; reasons.append(f"RSI {rsi_now:.0f}")

    entry, trigger, entry_hint, invalid, note = decide_entry(df2)

    # MACD 필터
    if str(entry).startswith("BUY_") and getattr(cfg, "REQUIRE_MACD_CONFIRM_FOR_BUY", False):
        if not macd_buy_signal:
            entry = "SKIP"
            note = "MACD 확인(상향크로스/0선돌파) 미충족 → 매수 금지"

    # ===== 시장 레짐 필터 =====
    entry_raw = entry
    ms = market_state or MARKET_STATE  # ✅ 주입값 우선, 없으면 기존 전역 fallback
    regime = ms.get("regime", "UNKNOWN")
    spy50 = ms.get("spy_sma50")
    spy200 = ms.get("spy_sma200")

    if str(entry).startswith("BUY_"):
        if regime == "RISK_OFF":
            entry = "SKIP"
            note = (str(note) + f" | 🧯 MARKET RISK_OFF: SPY<SMA50&SMA200 ({spy50}/{spy200})").strip(" |")
        elif regime == "CAUTION":
            if entry == "BUY_BREAKOUT":
                entry = "WATCH_BREAKOUT"
                note = (str(note) + f" | ⚠️ MARKET CAUTION: SPY<SMA200 ({spy200})").strip(" |")

    # ✅ ADX(추세 강도) 필터: 추세 없으면 BUY → WATCH_ADX
    min_adx = float(getattr(cfg, "MIN_ADX", 20))
    if str(entry).startswith("BUY_"):
        adx14 = float(last.get("ADX14", np.nan)) if "ADX14" in last.index else np.nan
        if np.isfinite(adx14) and adx14 < min_adx:
            entry = "WATCH_ADX"
            note = (str(note) + f" | 추세없음(ADX {adx14:.1f} < {min_adx})").strip(" |")

    # ✅ Relative Strength (시장 대비 강도): SPY보다 약하면 BUY → WATCH_RS
    rs_txt = None
    rs_lookback = int(getattr(cfg, "RS_LOOKBACK_DAYS", 20))
    if str(entry).startswith("BUY_") and data is not None:
        spy_close = None
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if "SPY" in data.columns.get_level_values(0):
                    spy_close = data["SPY"]["Close"].copy()
            else:
                spy_close = data["Close"].copy() if "Close" in data.columns else None
        except Exception:
            spy_close = None
        if spy_close is not None:
            stock_ret, spy_ret = compute_rs_vs_spy(df2, spy_close, rs_lookback)
            if stock_ret is not None and spy_ret is not None:
                rs_txt = f"종목 {stock_ret*100:.1f}% vs SPY {spy_ret*100:.1f}%"
                if stock_ret <= spy_ret:
                    entry = "WATCH_RS"
                    note = (str(note) + f" | 시장대비 약함(RS {rs_lookback}일: {rs_txt})").strip(" |")

    # ✅ 미네르비니 1) 52주 고점 근접: 고점에서 max_pct_off 이상 멀면 WATCH_52WK (주의: 브레이크아웃 직후는 걸릴 수 있음 → WATCH로 완화)
    pct_off_52h = None
    if len(df2) >= 252:
        high52 = float(df2["High"].tail(252).max())
        if np.isfinite(high52) and high52 > 0:
            pct_off_52h = (1.0 - last_close / high52) * 100.0
            if str(entry).startswith("BUY_") and getattr(cfg, "MINERVINI_52WK_ENABLED", False):
                if entry == "BUY_PULLBACK":
                    max_off = float(getattr(cfg, "MINERVINI_52WK_MAX_PCT_OFF_PULLBACK", 0.35)) * 100.0
                else:
                    max_off = float(getattr(cfg, "MINERVINI_52WK_MAX_PCT_OFF", 0.25)) * 100.0
                if pct_off_52h > max_off:
                    entry = "WATCH_52WK"
                    note = (str(note) + f" | 52주고점에서 {pct_off_52h:.1f}% 아래(>{max_off:.0f}% 허용)").strip(" |")

    # ✅ 미네르비니 2) 이평 정렬(50>150>200) + 가격 위. 눌림매수(BUY_PULLBACK)는 정의상 SMA 근처이므로 "가격>SMA50"은 돌파만 적용.
    if str(entry).startswith("BUY_") and getattr(cfg, "MINERVINI_MA_STACK_ENABLED", False):
        s50 = float(last.get("SMA50", np.nan))
        s150 = float(last.get("SMA150", np.nan))
        s200 = float(last.get("SMA200", np.nan))
        if all(np.isfinite([s50, s150, s200])):
            stack_ok = (s50 > s150 and s150 > s200)
            if entry == "BUY_PULLBACK":
                if not stack_ok:
                    entry = "WATCH_MA_STACK"
                    note = (str(note) + " | 이평정렬 미충족(SMA50>SMA150>SMA200)").strip(" |")
            else:
                if not (last_close > s50 and stack_ok):
                    entry = "WATCH_MA_STACK"
                    note = (str(note) + " | 이평정렬 미충족(가격>SMA50>SMA150>SMA200)").strip(" |")

    # BUY는 트레이드 플랜 + RR 필터
    plan = None


    if str(entry).startswith("BUY_"):
        plan = calc_trade_plan(df2, entry)

        # 플랜 자체가 없으면 완전 SKIP
        if plan is None:
            entry = "SKIP"
            note = "플랜 계산 불가(ATR/데이터 문제) → SKIP"
        else:
            # RR/사이징이 부족하면 완전 SKIP로 죽이지 말고 후보로 보관
            if plan["RR"] < cfg.MIN_RR or plan["Shares"] <= 0:
                entry = "CANDIDATE_BUY"
                note = f"BUY 후보(미달): RR {plan['RR']} < {cfg.MIN_RR} 또는 Shares {plan['Shares']} → SMART 승격 후보"


    # 실행 가산점
    if str(entry).startswith("BUY_"): score += 10
    elif str(entry).startswith("WATCH_"): score += 5

    sector = get_sector(ticker)
    atr_pct = float(last["ATR14"] / last_close) * 100 if float(last["ATR14"]) > 0 else np.nan

    # ✅ 디스코드와 동일한 Prob/EV 계산(Top Pick과 같은 공식)
    prob = None
    ev = None
    try:
        row_like = {
            "Score": score,
            "RR": (plan["RR"] if plan else np.nan),
            "VolRatio": vol_ratio,
            "RSI": (rsi_now if np.isfinite(rsi_now) else np.nan),
            "ATR%": (atr_pct if np.isfinite(atr_pct) else np.nan),
            "Entry": entry,
            "MACDTrigger": macd_trigger,
        }
        prob, ev = calc_prob_ev_like_discord(row_like, market_state=ms)
    except Exception:
        prob, ev = None, None

    return {
        "Ticker": ticker,
        "Sector": sector,
        "Entry": entry,
        "EntryRaw": entry_raw,
        "Score": round(score, 1),
        "Close": round(last_close, 2),
        "MktCap_KRW_T": round(mktcap_krw / 1e12, 1) if mktcap_krw else None,
        "Avg$Vol": int(dollar_vol_20),
        "VolRatio": round(vol_ratio, 2),
        "RSI": round(rsi_now, 1) if np.isfinite(rsi_now) else None,
        "ATR%": round(atr_pct, 2) if np.isfinite(atr_pct) else None,
        "ADX": round(float(last.get("ADX14", np.nan)), 1) if np.isfinite(float(last.get("ADX14", np.nan))) else None,
        "RS_vs_SPY": rs_txt,
        "PctOff52H": round(pct_off_52h, 1) if pct_off_52h is not None and np.isfinite(pct_off_52h) else None,
        "Trigger": trigger,
        "EntryHint": entry_hint,
        "Invalidation": invalid,
        "Reasons": ", ".join(reasons),
        "MACDTrigger": macd_trigger,
        "Note": note,
        "EntryPrice": plan["EntryPrice"] if plan else None,
        "StopPrice": plan["StopPrice"] if plan else None,
        "TargetPrice": plan["TargetPrice"] if plan else None,
        "RR": plan["RR"] if plan else None,
        "EV": round(ev, 2) if ev is not None else None,
        "Prob": round(prob, 2) if prob is not None else None,
        "Shares": plan["Shares"] if plan else None,
        "PosValue": plan["PosValue"] if plan else None,
        
    }
# =========================
# 시장 상태(레짐) 필터용 전역
# =========================
MARKET_STATE = {
    "regime": "UNKNOWN",   # RISK_ON / CAUTION / RISK_OFF / UNKNOWN
    "spy_sma50": None,
    "spy_sma200": None
}


# -----------------------------
# 디스코드 Embed 전송 (RISK embed 포맷 개선 포함)
# -----------------------------
COLOR_BUY = 0x2ECC71     # green
COLOR_WATCH = 0xF1C40F   # yellow
COLOR_RISK = 0xE74C3C    # red

def discord_webhook_send(embeds, content=None):
    url = cfg.DISCORD_WEBHOOK_URL.strip()
    if not url.startswith("https://discord.com/api/webhooks"):
        print("❌ DISCORD_WEBHOOK_URL 설정 필요")
        return False

    payload = {"embeds": embeds}
    if content:
        payload["content"] = content

    try:
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code in (200, 204):
            print("[DISCORD] sent")
            return True
        print("[DISCORD] fail:", r.status_code, r.text[:200])
        return False
    except Exception as e:
        print("[DISCORD] error::", e)
        return False

def _embed_text_len(e: dict) -> int:
    # Discord limit: combined characters across all embeds in ONE message <= 6000
    # We only use title/description mostly, but count safely.
    n = 0
    n += len(str(e.get("title", "") or ""))
    n += len(str(e.get("description", "") or ""))
    for f in (e.get("fields") or []):
        n += len(str(f.get("name", "") or ""))
        n += len(str(f.get("value", "") or ""))
    footer = e.get("footer") or {}
    n += len(str(footer.get("text", "") or ""))
    author = e.get("author") or {}
    n += len(str(author.get("name", "") or ""))
    return n


def discord_webhook_send_chunked(embeds: list[dict], content: str | None = None,
                                max_embeds_per_msg: int = 10, max_chars_per_msg: int = 5800):
    """
    - 디스코드: 한 메시지당 embeds 최대 10개
    - 한 메시지의 모든 embed 텍스트 합계 최대 6000자 (여유로 5800 사용)
    """
    if not embeds:
        return True

    chunks = []
    cur = []
    cur_chars = 0

    for e in embeds:
        elen = _embed_text_len(e)
        # 너무 큰 단일 embed 방어: description을 강제로 줄임
        if elen > max_chars_per_msg:
            desc = str(e.get("description", "") or "")
            # title/기타 제외하고 description을 강제로 줄여서 맞춤
            e = e.copy()
            e["description"] = desc[:2000]
            elen = _embed_text_len(e)

        if (len(cur) >= max_embeds_per_msg) or (cur and (cur_chars + elen > max_chars_per_msg)):
            chunks.append(cur)
            cur = []
            cur_chars = 0

        cur.append(e)
        cur_chars += elen

    if cur:
        chunks.append(cur)

    ok_all = True
    for i, ch in enumerate(chunks):
        # content는 첫 메시지에만 붙임(원하면 매번 붙여도 됨)
        c = content if (i == 0) else None
        ok = discord_webhook_send(ch, content=c)
        ok_all = ok_all and ok

    return ok_all

def save_scan_snapshot(path: str,
                       run_date: str,
                       market_state: dict,
                       df_all: pd.DataFrame,
                       buy_df: pd.DataFrame,
                       watch_df: pd.DataFrame,
                       top_picks: pd.DataFrame,
                       risk_df: pd.DataFrame,
                       recos_df: pd.DataFrame,
                       out_csv: str):
    """
    ✅ 디스코드 기준(=scanner.py가 만든 결과) 스냅샷 저장
    Streamlit은 이 파일만 읽어서 표시(재계산 금지)
    """
    def _df_to_records(df: pd.DataFrame):
        if df is None or df.empty:
            return []
        d = df.copy()
        # numpy 타입/NaN json 호환
        d = d.replace({np.nan: None})
        return d.to_dict(orient="records")

    snap = {
        "run_date": run_date,
        "market_state": market_state or {},
        "out_csv": out_csv,
        "counts": {
            "df_all": 0 if df_all is None else int(len(df_all)),
            "buy_df": 0 if buy_df is None else int(len(buy_df)),
            "watch_df": 0 if watch_df is None else int(len(watch_df)),
            "top_picks": 0 if top_picks is None else int(len(top_picks)),
            "risk_df": 0 if risk_df is None else int(len(risk_df)),
            "recos_df": 0 if recos_df is None else int(len(recos_df)),
        },
        "df_all": _df_to_records(df_all),
        "buy_df": _df_to_records(buy_df),
        "watch_df": _df_to_records(watch_df),
        "top_picks": _df_to_records(top_picks),
        "risk_df": _df_to_records(risk_df),
        "recos_df": _df_to_records(recos_df), 
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, indent=2)

def embed_for_market(run_date):
    reg = MARKET_STATE.get("regime", "UNKNOWN")
    s50 = MARKET_STATE.get("spy_sma50")
    s200 = MARKET_STATE.get("spy_sma200")

    if reg == "RISK_ON":
        emoji = "🟢"
    elif reg == "CAUTION":
        emoji = "🟡"
    elif reg == "RISK_OFF":
        emoji = "🔴"
    else:
        emoji = "⚪"

    risk_dollar = cfg.ACCOUNT_EQUITY_USD * cfg.RISK_PER_TRADE
    total_risk = risk_dollar * cfg.MAX_BUY_PER_DAY

    desc = (
        f"**Regime:** {emoji} {reg}\n"
        f"SPY SMA50: {s50} | SMA200: {s200}\n\n"
        f"계좌: **${cfg.ACCOUNT_EQUITY_USD:,}**\n"
        f"1R: **${risk_dollar:,.0f}** | Max Risk Today: **${total_risk:,.0f}**"
    )

    return {
        "title": f"📊 MARKET DASHBOARD — {run_date}",
        "description": desc,
        "color": 0x3498DB
    }

# =========================
# UI: Top Picks + Risk Meter + Ticker Card
# =========================
def _bar(level: float, max_level: float = 100.0, width: int = 12) -> str:
    """0~max_level을 width칸 막대로"""
    try:
        x = float(level)
    except Exception:
        x = 0.0
    x = max(0.0, min(max_level, x))
    filled = int(round((x / max_level) * width))
    return "█" * filled + "░" * (width - filled)

def _risk_label_rsi(rsi: float) -> str:
    if rsi >= 75: return "🔴 Overbought"
    if rsi >= 65: return "🟡 Hot"
    if rsi >= 40: return "🟢 Healthy"
    if rsi > 0:   return "🟠 Weak"
    return "⚪ N/A"

def _risk_label_atr(atr_pct: float) -> str:
    # 스윙 기준: ATR%가 너무 크면 변동성 과다(슬리피지/갭 위험)
    if atr_pct >= 7.0: return "🔴 Very High"
    if atr_pct >= 5.0: return "🟡 High"
    if atr_pct >= 2.5: return "🟢 Normal"
    if atr_pct > 0:    return "🟠 Low"
    return "⚪ N/A"

def pick_top_picks(df_all: pd.DataFrame, n: int = 3, allow_watch_fallback: bool = False) -> pd.DataFrame:
    """
    TOP PICK 우선순위:
      1) BUY_* / BUY_SMART 먼저 뽑고
      2) (옵션) 남는 자리는 WATCH_*로 채운다  ✅ (디스코드/Streamlit 동일)
    """
    if df_all is None or df_all.empty:
        return pd.DataFrame()

    d = df_all.copy()

    # 1) BUY 계열 먼저
    buy_like = d[d["Entry"].astype(str).isin(["BUY_BREAKOUT", "BUY_PULLBACK", "BUY_SMART"])].copy()
    picks = buy_like.head(n).copy()

    # 2) 남는 자리만 WATCH로 채우기 (핵심)
    if allow_watch_fallback and len(picks) < n:
        used = set(picks["Ticker"].astype(str).str.upper().tolist()) if ("Ticker" in picks.columns and not picks.empty) else set()
        watch_like = d[d["Entry"].astype(str).str.startswith("WATCH_")].copy()

        add_rows = []
        for _, r in watch_like.iterrows():
            t = str(r.get("Ticker", "")).upper()
            if not t or t in used:
                continue
            add_rows.append(r)
            used.add(t)
            if len(picks) + len(add_rows) >= n:
                break

        if add_rows:
            picks = pd.concat([picks, pd.DataFrame(add_rows)], ignore_index=True)

    return picks.head(n).copy()

def _clamp(x, lo, hi):
    try:
        x = float(x)
    except Exception:
        return lo
    return max(lo, min(hi, x))

def _sigmoid(z: float) -> float:
    try:
        z = float(z)
    except Exception:
        z = 0.0
    # 너무 큰 값 방지
    z = _clamp(z, -8, 8)
    return 1.0 / (1.0 + math.exp(-z))

def ev_rank_top_picks(buy_df: pd.DataFrame, n: int = 3) -> pd.DataFrame:
    """
    ✅ Top Pick 3를 EV(기대값) 기반으로 뽑는다.
    - BUY_* 는 '타이밍' (진입 규칙 통과)
    - Score/Vol/RSI/ATR/MACD/MarketRegime 는 '확률(승률 추정)'
    - RR은 '보상 크기'
    - BUY_SMART는 확률 가중치에서 약간 페널티(승격이므로)
    """
    if buy_df is None or buy_df.empty:
        return pd.DataFrame()

    d = buy_df.copy()

    # 필요한 컬럼 방탄
    for col in ["Score","RR","VolRatio","RSI","ATR%","Entry","MACDTrigger"]:
        if col not in d.columns:
            d[col] = np.nan

    # ---- 1) 승률 추정 p_win (0~1) ----
    # Score 정규화: 대략 20~80을 0~1로 매핑
    score_n = d["Score"].apply(lambda x: _clamp((float(x) - 20.0) / 60.0, 0.0, 1.0) if pd.notna(x) else 0.3)

    # 볼륨: 1.0x~2.0x를 0~1로
    vol_n = d["VolRatio"].apply(lambda x: _clamp((float(x) - 1.0) / 1.0, 0.0, 1.0) if pd.notna(x) else 0.3)

    # RSI: 스윙은 보통 45~65가 “건강”, 72+는 과열 페널티
    def _rsi_n(x):
        if not pd.notna(x):
            return 0.5
        r = float(x)
        if r >= 75: return 0.20
        if r >= 72: return 0.30
        if 45 <= r <= 65: return 0.75
        if 40 <= r < 45: return 0.60
        if r < 40: return 0.35
        return 0.55
    rsi_n = d["RSI"].apply(_rsi_n)

    # ATR%: 너무 크면(갭/슬리피지) 페널티, 너무 작아도(움직임 부족) 약간 페널티
    def _atr_n(x):
        if not pd.notna(x):
            return 0.55
        a = float(x)
        if a >= 7.0: return 0.25
        if a >= 5.0: return 0.40
        if 2.5 <= a < 5.0: return 0.65
        if 1.2 <= a < 2.5: return 0.60
        return 0.45
    atr_n = d["ATR%"].apply(_atr_n)

    # MACD 트리거는 약간의 보너스(확률 보정)
    def _macd_bonus(x):
        s = str(x or "")
        if s == "CROSS_UP": return 0.10
        if s == "HIST_0_UP": return 0.07
        return 0.0
    macd_b = d["MACDTrigger"].apply(_macd_bonus)

    # 레짐 보정(전역 MARKET_STATE 사용)
    reg = str(MARKET_STATE.get("regime", "UNKNOWN"))
    if reg == "RISK_ON":
        regime_b = 0.08
    elif reg == "CAUTION":
        regime_b = -0.05
    elif reg == "RISK_OFF":
        regime_b = -0.15
    else:
        regime_b = 0.0

    # BUY_SMART는 확률 약간 페널티(승격이므로)
    def _smart_penalty(e):
        e = str(e or "")
        return -0.08 if e == "BUY_SMART" else 0.0
    smart_p = d["Entry"].apply(_smart_penalty)

    # 승률 p_win 계산(로지스틱)
    # 핵심: “점수/볼륨/RSI/ATR”로 확률을 잡고, MACD/레짐/SMART로 미세 조정
    z = (
        -0.20
        + 1.10 * score_n
        + 0.90 * vol_n
        + 0.90 * rsi_n
        + 0.70 * atr_n
        + macd_b
        + regime_b
        + smart_p
    )
    d["Pwin"] = z.apply(_sigmoid)
    d["Prob"] = d["Pwin"]


    # ---- 2) EV(기대값) ----
    # 1R 손실을 1로 두고, 기대값 EV = p*RR - (1-p)*1
    # RR이 없으면 큰 페널티
    def _rr_safe(x):
        if not pd.notna(x):
            return 0.0
        try:
            return float(x)
        except Exception:
            return 0.0

    d["RR_s"] = d["RR"].apply(_rr_safe)
    d["EV"] = d["Pwin"] * d["RR_s"] - (1.0 - d["Pwin"]) * 1.0

    # ---- 3) 타이밍 우선순위(동점일 때) ----
    # breakout/pullback이 smart보다 우선, 대신 EV가 더 중요
    def _timing_rank(e):
        e = str(e or "")
        if e == "BUY_BREAKOUT": return 0
        if e == "BUY_PULLBACK": return 1
        if e == "BUY_SMART": return 2
        return 9

    d["T"] = d["Entry"].apply(_timing_rank)

    # 정렬: EV 최우선, 그다음 타이밍, 그다음 Score, 유동성
    # 정렬용 컬럼 방탄
    if "Avg$Vol" not in d.columns:
        d["Avg$Vol"] = 0

    # ✅ 1) 진짜 BUY 먼저 (BREAKOUT/PULLBACK)
    real_buy = d[d["Entry"].astype(str).isin(["BUY_BREAKOUT", "BUY_PULLBACK"])].copy()
    real_buy = real_buy.sort_values(
        ["EV", "Score", "Avg$Vol"],
        ascending=[False, False, False]
    )

    picks = real_buy.head(n).copy()

    # ✅ 2) 부족한 자리만 BUY_SMART로 채움
    if len(picks) < n:
        need = n - len(picks)
        smart = d[d["Entry"].astype(str).eq("BUY_SMART")].copy()
        smart = smart.sort_values(
            ["EV", "Score", "Avg$Vol"],
            ascending=[False, False, False]
        )

        if not smart.empty:
            picks = pd.concat([picks, smart.head(need)], ignore_index=True)

    return picks.head(n).copy()


def calc_prob_ev_like_discord(row: dict, market_state: dict | None = None):
    """
    ✅ 디스코드 ev_rank_top_picks()와 동일한 Prob/EV 계산(행 단위)
    입력 row: Score, RR, VolRatio, RSI, ATR%, Entry, MACDTrigger
    출력: (prob, ev)
    """
    ms = market_state or MARKET_STATE or {}
    reg = str(ms.get("regime", "UNKNOWN"))

    # ---- 1) 승률 추정 p_win ----
    def _clamp(x, lo, hi):
        try:
            x = float(x)
        except Exception:
            return lo
        return max(lo, min(hi, x))

    def _sigmoid(z: float) -> float:
        z = _clamp(z, -8, 8)
        return 1.0 / (1.0 + math.exp(-z))

    score = row.get("Score", np.nan)
    rr = row.get("RR", np.nan)
    volr = row.get("VolRatio", np.nan)
    rsi = row.get("RSI", np.nan)
    atrp = row.get("ATR%", np.nan)
    entry = str(row.get("Entry", "") or "")
    macdtr = str(row.get("MACDTrigger", "") or "")

    # Score 정규화: 20~80 -> 0~1
    score_n = _clamp((float(score) - 20.0) / 60.0, 0.0, 1.0) if pd.notna(score) else 0.3

    # 볼륨: 1.0~2.0 -> 0~1
    vol_n = _clamp((float(volr) - 1.0) / 1.0, 0.0, 1.0) if pd.notna(volr) else 0.3

    # RSI 점수
    def _rsi_n(x):
        if not pd.notna(x):
            return 0.5
        r = float(x)
        if r >= 75: return 0.20
        if r >= 72: return 0.30
        if 45 <= r <= 65: return 0.75
        if 40 <= r < 45: return 0.60
        if r < 40: return 0.35
        return 0.55
    rsi_n = _rsi_n(rsi)

    # ATR% 점수
    def _atr_n(x):
        if not pd.notna(x):
            return 0.55
        a = float(x)
        if a >= 7.0: return 0.25
        if a >= 5.0: return 0.40
        if 2.5 <= a < 5.0: return 0.65
        if 1.2 <= a < 2.5: return 0.60
        return 0.45
    atr_n = _atr_n(atrp)

    # MACD 보너스
    macd_b = 0.10 if macdtr == "CROSS_UP" else (0.07 if macdtr == "HIST_0_UP" else 0.0)

    # 레짐 보정
    if reg == "RISK_ON":
        regime_b = 0.08
    elif reg == "CAUTION":
        regime_b = -0.05
    elif reg == "RISK_OFF":
        regime_b = -0.15
    else:
        regime_b = 0.0

    # BUY_SMART 페널티
    smart_p = -0.08 if entry == "BUY_SMART" else 0.0

    z = (
        -0.20
        + 1.10 * score_n
        + 0.90 * vol_n
        + 0.90 * rsi_n
        + 0.70 * atr_n
        + macd_b
        + regime_b
        + smart_p
    )
    prob = _sigmoid(z)

    # ---- 2) EV ----
    def _rr_safe(x):
        if not pd.notna(x):
            return 0.0
        try:
            return float(x)
        except Exception:
            return 0.0

    rr_s = _rr_safe(rr)
    ev = prob * rr_s - (1.0 - prob) * 1.0

    return prob, ev

def embed_for_top_picks_summary(picks_df: pd.DataFrame, run_date: str):
    if picks_df is None or picks_df.empty:
        return {
            "title": f"🔥 TOP PICKS — {run_date}",
            "description": "Top Pick 후보 없음",
            "color": 0xE67E22
        }

    lines = []
    for i, (_, row) in enumerate(picks_df.iterrows(), 1):
        t = row.get("Ticker", "?")
        e = row.get("Entry", "?")
        sc = row.get("Score", "")
        rr = row.get("RR", None)
        vr = row.get("VolRatio", None)
        rsi = row.get("RSI", None)
        atrp = row.get("ATR%", None)

        # ✅ 여기: Pwin → Prob로 '표시만' 바꿈 (데이터는 Pwin을 그대로 씀)
        prob = row.get("Prob", None)
        if prob is None:
            prob = row.get("Pwin", None)

        ev = row.get("EV", None)

        rr_txt = f"RR {rr}" if pd.notna(rr) else "RR -"
        vr_txt = f"Vol {vr}x" if pd.notna(vr) else "Vol -"
        rsi_txt = f"RSI {rsi}" if pd.notna(rsi) else "RSI -"
        atr_txt = f"ATR% {atrp}" if pd.notna(atrp) else "ATR% -"

        prob_txt = f"Prob {float(prob):.2f}" if pd.notna(prob) else "Prob -"
        ev_txt = f"EV {float(ev):.2f}" if pd.notna(ev) else "EV -"

        lines.append(
            f"**#{i} {t}** `{e}` | {ev_txt} | {prob_txt} | Score {sc} | {rr_txt} | {vr_txt} | {rsi_txt} | {atr_txt}"
        )

    return {
        "title": f"🔥 TOP PICK 3 — {run_date}",
        "description": ("\n".join(lines))[:3900],
        "color": 0xE67E22
    }


def embed_for_risk_meter(picks_df: pd.DataFrame, run_date: str):
    """
    '막대그래프' 느낌: RSI / ATR%를 막대로 표시하고 위험 라벨을 붙임
    """
    if picks_df is None or picks_df.empty:
        return {
            "title": f"📈 RISK METER — {run_date}",
            "description": "표시할 Top Pick 없음",
            "color": 0x95A5A6
        }

    lines = []
    for _, row in picks_df.iterrows():
        t = row.get("Ticker", "?")
        rsi = row.get("RSI", np.nan)
        atrp = row.get("ATR%", np.nan)

        # RSI는 0~100
        rsi_bar = _bar(float(rsi) if pd.notna(rsi) else 0, 100.0, 12)
        rsi_lab = _risk_label_rsi(float(rsi)) if pd.notna(rsi) else "⚪ N/A"

        # ATR%는 0~10을 100스케일로 매핑(10%면 풀바)
        atr_val = float(atrp) if pd.notna(atrp) else 0.0
        atr_bar = _bar(min(atr_val, 10.0) * 10.0, 100.0, 12)
        atr_lab = _risk_label_atr(atr_val) if pd.notna(atrp) else "⚪ N/A"

        lines.append(
            f"**{t}**\n"
            f"RSI  {rsi_bar}  {rsi_lab}\n"
            f"ATR% {atr_bar}  {atr_lab}"
        )

    desc = "\n\n".join(lines)
    return {
        "title": f"📈 RISK METER (RSI / ATR%) — {run_date}",
        "description": desc[:3900],
        "color": 0x1ABC9C
    }

def embed_for_ticker_card(row: dict, run_date: str, rank: int = 1):
    """
    종목 카드 1장(Top Pick용)
    """
    t = row.get("Ticker", "?")
    e = row.get("Entry", "?")
    sec = row.get("Sector", "Unknown")
    close = row.get("Close", None)
    score = row.get("Score", None)
    reasons = row.get("Reasons", "")
    note = row.get("Note", "")

    entry_p = row.get("EntryPrice", None)
    stop_p  = row.get("StopPrice", None)
    targ_p  = row.get("TargetPrice", None)
    rr      = row.get("RR", None)
    sh      = row.get("Shares", None)
    pv      = row.get("PosValue", None)

    vr = row.get("VolRatio", None)
    rsi = row.get("RSI", None)
    atrp = row.get("ATR%", None)
    trig = row.get("Trigger", "")

    # 카드 본문(보기 좋게)
    lines = []
    lines.append(f"**{t}** ({sec})  `#{rank}  {e}`")
    if pd.notna(close): lines.append(f"Close **{close}** | Score **{score}**")
    if pd.notna(vr) or pd.notna(rsi) or pd.notna(atrp):
        lines.append(f"Vol **{vr}x** | RSI **{rsi}** | ATR% **{atrp}**")

    # 플랜(없으면 대시)
    if pd.notna(entry_p) and pd.notna(stop_p) and pd.notna(targ_p):
        lines.append(f"Entry **{entry_p}** | Stop **{stop_p}** | Target **{targ_p}** | RR **{rr}**")
    else:
        lines.append("Entry/Stop/Target: **-** (WATCH 또는 플랜 없음)")

    if pd.notna(sh) and pd.notna(pv) and float(sh) > 0:
        lines.append(f"Size **{int(sh)} sh** (~${pv})")

    if trig: lines.append(f"Trigger: {trig}")
    if reasons: lines.append(f"Reasons: {reasons}")
    if note: lines.append(f"Note: {note}")

    # 색상(상태별)
    et = str(e)
    if et.startswith("BUY_") or et == "BUY_SMART":
        color = COLOR_BUY
    elif et.startswith("WATCH_"):
        color = COLOR_WATCH
    else:
        color = 0x95A5A6

    return {
        "title": f"🧠 TICKER CARD — {t} — {run_date}",
        "description": ("\n".join(lines))[:3900],
        "color": color
    }


def embed_for_buy(buy_df, run_date):
    if buy_df.empty:
        desc = f"BUY 없음 (RR≥{cfg.MIN_RR} 또는 신호 부족)"
    else:
        lines = []
        for _, r in buy_df.iterrows():
            promo_val = r.get("PromoTag", "")
            promo_txt = f" {promo_val}" if pd.notna(promo_val) and str(promo_val) != "" else ""

            if promo_txt == "":               
                promoted = bool(r.get("Promoted", False))
                promo_txt = " 🟣✅ PROMOTED" if promoted else ""

            mt = r.get("MACDTrigger", "")
            mt_txt = ""
            if mt == "CROSS_UP":
                mt_txt = " | MACD ✅CROSS"
            elif mt == "HIST_0_UP":
                mt_txt = " | MACD ✅0UP"

            # 🔥 추가되는 부분
            sh = r.get("Shares")
            pv = r.get("PosValue")
            if pd.notna(sh) and pd.notna(pv) and float(sh) > 0:
                size_text = f"Size: **{int(sh)} sh** (~${pv})"
            else:
                size_text = "Size: 계획 계산중"

            # RR 표시 방식 변경 (SMART는 확률 후보)
            if str(r['Entry']) == "BUY_SMART":
                rr_text = "PROBABLE"
            else:
                rr_text = f"RR {r['RR']}"

            lines.append(
                f"**{r['Ticker']}**{promo_txt} ({r['Sector']})  `{r['Entry']}`{mt_txt}\n"
                f"Entry **{r['EntryPrice']}** | Stop **{r['StopPrice']}** | Target **{r['TargetPrice']}** | {rr_text}\n"
                f"{size_text} | {r['Trigger']}"
            )



            
        desc = "\n\n".join(lines)

    return {
        "title": f"🟢 BUY (max {cfg.MAX_BUY_PER_DAY}) — {run_date}",
        "description": desc[:3900],
        "color": COLOR_BUY
    }


def embed_for_watch(watch_df, run_date):
    if watch_df.empty:
        desc = "WATCH 없음"
    else:
        lines = []
        for _, r in watch_df.head(cfg.WATCH_LIMIT).iterrows():
            mt = r.get("MACDTrigger", "")
            mt_txt = ""
            if mt == "CROSS_UP":
                mt_txt = " | MACD✅CROSS"
            elif mt == "HIST_0_UP":
                mt_txt = " | MACD✅0UP"

            lines.append(f"**{r['Ticker']}** ({r['Sector']}) `{r['Entry']}`{mt_txt} — {r['Trigger']}")

        desc = "\n".join(lines)

    return {
        "title": f"🟡 WATCH — {run_date}",
        "description": desc[:3900],
        "color": COLOR_WATCH
    }


def embed_for_risk(risk_df, run_date):
    if risk_df.empty:
        desc = "보유 포지션 매도/익절 신호 없음"
    else:
        lines = []
        for _, r in risk_df.iterrows():
            lines.append(
                f"**{r['Ticker']}** `{r['Action']}`  Close {r['Close']} | "
                f"SMA20 {r.get('SMA20')} | SMA50 {r.get('SMA50')} | "
                f"Trail {r.get('TrailingStop')} | High60 {r.get('High60')}\n"
                f"PnL ${r['PnL']} ({r['PnL%']}%)\n"
                f"{r.get('Reason','')}"
            )
        desc = "\n\n".join(lines)

    return {
        "title": f"🔴 SELL / TAKE PROFIT 후보 — {run_date}",
        "description": desc[:3900],
        "color": COLOR_RISK
    }


# -----------------------------
# 메인 (속도 개선 + WATCH 별도 + 시총 선호 가산점 + 보유 매도/익절 신호)
# -----------------------------
def main():
    # ✅ 안전 초기화 (먼저 선언부터)
    run_date = ""
    skip_reasons = []  # (ticker, reason)
    end = datetime.utcnow().date()
    start = end - timedelta(days=cfg.LOOKBACK_DAYS)
    run_date = str(end)

    results = []

    # ✅ 보유 포지션 로드
    pos = load_positions("positions.csv")
    pos_tickers = []
    if not pos.empty:
        pos_tickers = pos["Ticker"].astype(str).str.upper().tolist()

    # ✅ 전체 티커(스캔 + 보유 + 시장지수)
    bench = ["SPY", "QQQ"]
    scan_universe = [t.upper() for t in TICKERS if t.upper() not in TICKER_BLACKLIST]
    all_tickers = sorted(list(set(scan_universe + pos_tickers + bench)))

    if not all_tickers:
        return


    # ✅ 한 번에 다운로드
    data = yf.download(
        all_tickers,
        start=str(start),
        end=str(end + timedelta(days=1)),
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=True
    )
    
    global MARKET_STATE
    MARKET_STATE = compute_market_state_from_data(data)

    reg = MARKET_STATE.get("regime")
    s50 = MARKET_STATE.get("spy_sma50")
    s200 = MARKET_STATE.get("spy_sma200")
    print(f"[MARKET] SPY regime={reg} | SMA50={s50} | SMA200={s200}")


    # ✅ 스캔 결과
    for t in TICKERS:
        # 🚫 블랙리스트는 가장 먼저 컷
        if t in TICKER_BLACKLIST:
            continue
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if t not in data.columns.get_level_values(0):
                    continue
                df = data[t].copy()
            else:
                df = data.copy()

            if df is None or df.empty:
                continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            # 🔥 장중에는 오늘 진행중 봉 제거 (핵심)
            df = _drop_today_bar_if_needed(df)
            
            # ✅ SKIP 사유 수집(데이터 품질/기본 요건)
            end_date = datetime.utcnow().date()
            ok, q_reason = data_quality_check(df, end_date, max_stale_days=getattr(cfg, "MAX_DATA_STALE_DAYS", 5))
            if not ok:
                skip_reasons.append((t, f"DATA_QUALITY: {q_reason}"))
                continue

            # 최소 데이터 길이(지표용)
            if len(df) < 140:
                skip_reasons.append((t, f"TOO_SHORT: len={len(df)} (<140)"))
                continue

            # OHLCV 결측 과다(한 번 더 안전망)
            tmp = df.tail(60)
            if tmp[["Open","High","Low","Close","Volume"]].isna().mean().mean() > 0.05:
                skip_reasons.append((t, "NAN_TOO_MUCH(last60)"))
                continue


            r = score_stock(df, t, market_state=MARKET_STATE, data=data)
            if r:
                results.append(r)
        except Exception:
            continue

    df_all = pd.DataFrame(results)
    if df_all.empty:
        discord_webhook_send([{
            "title": f"US Swing Scanner — {run_date}",
            "description": "조건을 만족하는 후보가 없습니다.",
            "color": COLOR_WATCH
        }])
        return

    # -----------------------------
    # ✅ 50~100조는 '필터'가 아니라 '선호(가산점)'로 반영
    # -----------------------------
    df_all["MktPref"] = 0
    if "MktCap_KRW_T" in df_all.columns:
        df_all["MktPref"] = df_all["MktCap_KRW_T"].apply(
            lambda x: 1 if (pd.notna(x) and 50 <= float(x) <= 100) else 0
        )
        df_all.loc[df_all["MktPref"] == 1, "Score"] = df_all.loc[df_all["MktPref"] == 1, "Score"] + 8

        def _tag_note(row):
            if row.get("MktPref", 0) == 1:
                return (str(row.get("Note", "")) + " | ✅ 50~100조 선호").strip(" |")
            return row.get("Note", "")
        df_all["Note"] = df_all.apply(_tag_note, axis=1)

    # -----------------------------
    # 정렬: BUY/WATCH 우선순위 + (선호 시총 가산된) Score + 유동성
    # -----------------------------
    priority = {
        "BUY_BREAKOUT": 0,
        "BUY_PULLBACK": 1,
        "BUY_SMART": 2,
        "WATCH_BREAKOUT": 3,
        "WATCH_PULLBACK": 4,
        "WATCH_ADX": 4,
        "WATCH_RS": 4,
        "WATCH_52WK": 4,
        "WATCH_MA_STACK": 4,
        "WATCH_FUNDAMENTAL": 4,
        "CANDIDATE_BUY": 5,
        "SKIP": 9
    }

    df_all["P"] = df_all["Entry"].map(priority).fillna(9).astype(int)
    df_all["RR_sort"] = df_all["RR"].fillna(-1.0)

    df_all = df_all.sort_values(
        ["P", "RR_sort", "Score", "Avg$Vol"],
        ascending=[True, False, False, False]
    ).drop(columns=["RR_sort"])

    # -----------------------------
    # ✅ BUY 후보는 섹터 분산 + 상위 30에서 뽑기
    # -----------------------------
    if cfg.USE_SECTOR_CAP:
        picked = []
        counts = {}
        for _, row in df_all.iterrows():
            sec = row.get("Sector", "Unknown") or "Unknown"
            if counts.get(sec, 0) >= cfg.MAX_PER_SECTOR:
                continue
            picked.append(row)
            counts[sec] = counts.get(sec, 0) + 1
            if len(picked) >= 30:
                break
        df_out = pd.DataFrame(picked)
    else:
        df_out = df_all.head(30)

    buy_df = df_out[df_out["Entry"].astype(str).str.startswith("BUY_")].head(cfg.MAX_BUY_PER_DAY).copy()

    # -----------------------------
    # ✅ WATCH는 df_all에서 따로 뽑아서 cfg.WATCH_LIMIT까지 보여주기
    # -----------------------------
    # ✅ (안전) Promoted 기본값
    if "Promoted" not in df_out.columns:
        df_out["Promoted"] = False

    # =========================
    # ✅ SMART RELAX: BUY가 너무 적을 때만 후보 승격
    # =========================
    if getattr(cfg, "SMART_RELAX_ENABLED", False):
        min_buy = int(getattr(cfg, "SMART_MIN_BUY", 3))

        # buy_df는 여기서 "진짜 BUY_"만 포함
        if len(buy_df) < min_buy:
            relaxed_rr = float(getattr(cfg, "SMART_MIN_RR", max(1.2, cfg.MIN_RR - 0.3)))
            relaxed_vol = float(getattr(cfg, "SMART_BUY_BREAKOUT_VOL_X", cfg.BREAKOUT_VOL_X))

            need = int(cfg.MAX_BUY_PER_DAY - len(buy_df))
            if need > 0:
                # ✅ 후보 풀 확장: CANDIDATE_BUY + WATCH_* 에서도 승격 가능
                # (BUY가 적을 때 "WATCH에서 몇 종목 승격"이 실제로 되게 함)
                if (df_all is None) or (not isinstance(df_all, pd.DataFrame)) or df_all.empty:
                    cand = pd.DataFrame()
                else:
                    cand = df_all[df_all["Entry"].astype(str).isin([
                        "CANDIDATE_BUY", "WATCH_BREAKOUT", "WATCH_PULLBACK",
                        "WATCH_52WK", "WATCH_MA_STACK"
                    ])].copy()

                # ✅ (방탄) 컬럼 깨짐 방어: Ticker 없으면 승격 스킵
                if cand is None or (not isinstance(cand, pd.DataFrame)) or ("Ticker" not in cand.columns):
                    cand = pd.DataFrame(columns=df_all.columns)

                # ✅ 완화 RR 조건: RR이 있는 것만 필터링 (WATCH는 RR이 없을 수 있음)
                # WATCH_*는 RR이 None일 수 있으니 RR 필터는 CANDIDATE_BUY에만 적용
                if not cand.empty:
                    is_candbuy = cand["Entry"].astype(str) == "CANDIDATE_BUY"
                    candbuy = cand[is_candbuy].copy()
                    watch   = cand[~is_candbuy].copy()

                    candbuy = candbuy[candbuy["RR"].fillna(-1) >= relaxed_rr]
                    cand = pd.concat([candbuy, watch], ignore_index=True)

                # ✅ 돌파 계열은 볼륨 완화 조건 적용 (WATCH_BREAKOUT 포함)
                def _vol_ok(row):
                    e = str(row.get("Entry", ""))
                    raw = str(row.get("EntryRaw", ""))

                    # breakout 계열(원신호 or watch breakout)은 relaxed_vol 이상만
                    if (raw == "BUY_BREAKOUT") or (e == "WATCH_BREAKOUT"):
                        return float(row.get("VolRatio", 0) or 0) >= relaxed_vol

                    # pullback 계열은 볼륨 강제 X
                    return True

                if not cand.empty:
                    cand = cand[cand.apply(_vol_ok, axis=1)]

                # 이미 뽑힌 BUY와 중복 제거 (buy_df도 방탄)
                if (buy_df is None) or (not isinstance(buy_df, pd.DataFrame)) or buy_df.empty or ("Ticker" not in buy_df.columns):
                    already = set()
                else:
                    already = set(buy_df["Ticker"].astype(str).tolist())

                # ✅ cand에 Ticker가 있을 때만 중복 제거
                if not cand.empty and ("Ticker" in cand.columns):
                    cand = cand[~cand["Ticker"].astype(str).isin(already)]

                # ✅ 점수/유동성 상위부터 승격 (need만큼)
                if not cand.empty:
                    cand = cand.sort_values(["Score", "Avg$Vol"], ascending=[False, False]).head(need).copy()

                    # ✅ 승격 처리
                    # ✅ 승격 처리
                    def _tag(row):
                        row["Entry"] = "BUY_SMART"
                        row["Promoted"] = True
                        row["PromoTag"] = "🟣✅ PROMOTED"

                        # 🔥 승격 시에도 트레이드 플랜 생성 (안정 버전)
                        try:
                            tkr = row.get("Ticker", None)
                            if not tkr:
                                return row

                            if isinstance(data.columns, pd.MultiIndex) and tkr in data.columns.get_level_values(0):
                                dfp = data[tkr].copy()
                            else:
                                return row

                            if isinstance(dfp.columns, pd.MultiIndex):
                                dfp.columns = dfp.columns.get_level_values(0)

                            dfp = dfp.dropna(subset=["Open","High","Low","Close","Volume"]).copy()

                            close = dfp["Close"]
                            dfp["SMA20"] = sma(close, 20)
                            dfp["SMA50"] = sma(close, 50)
                            dfp["SMA200"] = sma(close, 200)
                            dfp["ATR14"] = atr(dfp, 14)
                            dfp["ADX14"] = adx(dfp, 14)

                            dfp2 = dfp.dropna().copy()
                            if len(dfp2) < 140:
                                return row

                            raw = row.get("EntryRaw", None)

                            # ✅ WATCH에서 승격된 종목도 플랜 계산 가능하게 매핑
                            if raw == "WATCH_BREAKOUT":
                                raw = "BUY_BREAKOUT"
                            elif raw == "WATCH_PULLBACK":
                                raw = "BUY_PULLBACK"

                            if raw not in ("BUY_BREAKOUT", "BUY_PULLBACK"):
                                return row


                            plan = calc_trade_plan(dfp2, raw)
                            if not plan:
                                return row
                            
                            # ✅ PROMOTED 방탄: Target이 Entry보다 의미있게 위 + RR 기준 충족해야만 승격
                            relaxed_rr = float(getattr(cfg, "SMART_MIN_RR", max(1.2, cfg.MIN_RR - 0.3)))
                            if (plan["TargetPrice"] <= plan["EntryPrice"] * 1.002) or (plan["RR"] < relaxed_rr):
                                # 승격 실패 처리(이 row는 PROMOTED로 쓰지 않게 됨)
                                row["Promoted"] = False
                                row["PromoTag"] = ""
                                return row

                            row["EntryPrice"] = plan["EntryPrice"]
                            row["StopPrice"] = plan["StopPrice"]
                            row["TargetPrice"] = plan["TargetPrice"]
                            row["RR"] = plan["RR"]
                            row["Shares"] = plan["Shares"]
                            row["PosValue"] = plan["PosValue"]

                        except Exception:
                            return row

                        row["Note"] = (str(row.get("Note", "")) + " | 🟣✅ PROMOTED (SMART)").strip(" |")
                        return row


                    cand = cand.apply(_tag, axis=1)
                    # ✅ 승격 성공한 것만 사용
                    cand = cand[cand.get("Promoted", False) == True].copy()

                    # buy_df에 합치기
                    buy_df = pd.concat([buy_df, cand], ignore_index=True)
                    buy_df = buy_df.head(cfg.MAX_BUY_PER_DAY)
                    # ✅ (방탄) BUY 리스트에 플랜 없는 행 제거 (NaN Shares 방지)
                    if buy_df is not None and isinstance(buy_df, pd.DataFrame) and not buy_df.empty:
                        for col in ["Shares", "PosValue", "EntryPrice", "StopPrice", "TargetPrice"]:
                            if col not in buy_df.columns:
                                buy_df[col] = np.nan

                        buy_df = buy_df.dropna(subset=["Shares", "PosValue", "EntryPrice", "StopPrice", "TargetPrice"]).copy()

                    

    watch_df_all = df_all[df_all["Entry"].astype(str).str.startswith("WATCH_")].copy()

    if cfg.USE_SECTOR_CAP:
        picked_w = []
        counts_w = {}
        for _, row in watch_df_all.iterrows():
            sec = row.get("Sector", "Unknown") or "Unknown"
            if counts_w.get(sec, 0) >= cfg.MAX_PER_SECTOR:
                continue
            picked_w.append(row)
            counts_w[sec] = counts_w.get(sec, 0) + 1
            if len(picked_w) >= cfg.WATCH_LIMIT:
                break
        watch_df = pd.DataFrame(picked_w)
    else:
        watch_df = watch_df_all.head(cfg.WATCH_LIMIT).copy()

    # -----------------------------
    # ✅ 보유 포지션 매도/익절 신호 (이미 받은 data 재사용)
    # -----------------------------
    risk_rows = []
    if not pos.empty:
        for _, p in pos.iterrows():
            t = str(p["Ticker"]).upper()
            shares = float(p["Shares"])
            avgp = float(p["AvgPrice"])

            try:
                if isinstance(data.columns, pd.MultiIndex):
                    if t not in data.columns.get_level_values(0):
                        continue
                    df = data[t].copy()
                else:
                    df = data.copy()

                if df is None or df.empty:
                    continue

                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                    
                # 🔥 장중 진행봉 제거 (보유 포지션도 동일 기준)
                df = _drop_today_bar_if_needed(df)


                df = df.dropna(subset=["Open", "High", "Low", "Close", "Volume"]).copy()
                close = df["Close"]
                df["SMA20"] = sma(close, 20)
                df["SMA50"] = sma(close, 50)  # ✅ 추가(SELL_TREND에서 사용)
                df["ATR14"] = atr(df, 14)
                df["ADX14"] = adx(df, 14)

                df2 = df.dropna().copy()
                if len(df2) < 60:
                    continue

                review = holding_risk_review(df2, t, shares, avgp)
                if review["Action"] != "HOLD":   # ✅ HOLD만 제외하고 전송
                    risk_rows.append(review)
            except Exception:
                continue

    risk_df = pd.DataFrame(risk_rows)
    
        # -----------------------------
    # ✅ 보유 종목 추천(추가매수/매도/보유) 생성
    # -----------------------------
    portfolio_recos = []
    if not pos.empty:
        for _, p in pos.iterrows():
            t = str(p["Ticker"]).upper()
            shares = float(p["Shares"])
            avgp = float(p["AvgPrice"])

            try:
                if isinstance(data.columns, pd.MultiIndex):
                    if t not in data.columns.get_level_values(0):
                        continue
                    df = data[t].copy()
                else:
                    df = data.copy()

                if df is None or df.empty:
                    continue

                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)

                df = df.dropna(subset=["Open","High","Low","Close","Volume"]).copy()

                # 지표 세팅(보유 추천용, 미네르비니 MA 스택용 SMA150 포함)
                if len(df) < 140:
                    continue
                close = df["Close"]
                df["SMA20"] = sma(close, 20)
                df["SMA50"] = sma(close, 50)
                df["SMA150"] = sma(close, 150)
                df["SMA200"] = sma(close, 200)
                df["ATR14"] = atr(df, 14)
                df["ADX14"] = adx(df, 14)
                df["MACD_H"] = macd_hist(close)
                df["RSI14"] = rsi(close, 14)

                df2 = df.dropna().copy()
                if len(df2) < 140:
                    continue

                rec = recommend_for_holding(df2, t, shares, avgp)
                portfolio_recos.append(rec)

            except Exception:
                continue

    recos_df = pd.DataFrame([{
        "Ticker": r["Ticker"],
        "Reco": r["Reco"],
        "Close": r["Close"],
        "PnL": r["PnL"],
        "PnL%": r["PnL%"],
        "Why": r["Why"]
    } for r in portfolio_recos])


    # CSV 저장(기록)
    outname = f"us_swing_scanner_{run_date}.csv"
    df_out.to_csv(outname, index=False, encoding="utf-8-sig")

    # 디스코드 Embed 3장 전송
    if "Promoted" not in buy_df.columns:
        buy_df["Promoted"] = False
    else:
        buy_df["Promoted"] = buy_df["Promoted"].fillna(False)


    # ✅ Top Pick 3는 EV(기대값) 기반으로 선정
    top_picks = ev_rank_top_picks(buy_df, n=3)

    # ==============================
    # 🔥 여기 추가 (Streamlit 동기화 핵심)
    # ==============================
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    snapshot_path = os.path.join(BASE_DIR, f"scan_snapshot_{run_date}.json")

    # ✅ 실제 계산된 MARKET_STATE를 넣어야 Streamlit에서 market_state가 보임
    ms_for_snap = MARKET_STATE if isinstance(MARKET_STATE, dict) else {}

    # ✅ out_csv는 실제 생성한 CSV 파일명(outname)
    out_csv_for_snap = outname  # 예: us_swing_scanner_2026-02-13.csv

    save_scan_snapshot(
        snapshot_path,
        run_date,
        ms_for_snap,
        df_all,
        buy_df,
        watch_df,
        top_picks,
        risk_df,
        recos_df,
        out_csv_for_snap,
    )
    print(f"[SNAPSHOT] saved: {snapshot_path}")


    embeds = [
        embed_for_market(run_date),
        embed_for_top_picks_summary(top_picks, run_date),
        embed_for_risk_meter(top_picks, run_date),
    ]


    # ✅ 종목 카드 3장(Top Pick만 카드로)
    if top_picks is not None and not top_picks.empty:
        for i, (_, r) in enumerate(top_picks.iterrows(), 1):
            embeds.append(embed_for_ticker_card(r.to_dict(), run_date, rank=i))

    # ✅ 기존 섹션들
    embeds += [
        embed_for_buy(buy_df, run_date),
        embed_for_watch(watch_df, run_date),
        embed_for_risk(risk_df, run_date),
        embed_for_portfolio(recos_df, run_date),
    ]

    # ✅ 디스코드 Embed는 최대 10개 제한(안전장치)
    embeds = embeds[:10]

    # ==============================
    # SKIP 리스트 콘솔 출력
    # ==============================
    if skip_reasons:
        print("\n==============================")
        print(f"[SKIP LIST] 총 {len(skip_reasons)}개")
        skip_reasons_sorted = sorted(skip_reasons, key=lambda x: x[1])

        lim = int(getattr(cfg, "SKIP_PRINT_LIMIT", 50))

        for tk, rs in skip_reasons_sorted[:lim]:
            print(f"- {tk}: {rs}")

        if len(skip_reasons_sorted) > lim:
            print(f"... (총 {len(skip_reasons_sorted)}개 중 {lim}개만 표시)")

        print("==============================\n")

    discord_webhook_send_chunked(
        embeds,
        content=f"계좌 ${cfg.ACCOUNT_EQUITY_USD:,} | 1R={cfg.RISK_PER_TRADE*100:.2f}% | CSV: {outname}"
    )



if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["scan", "query", "portfolio", "menu"],
        default="menu",
        help="scan=스캔 실행, query=티커검색, portfolio=포트폴리오편집, menu=대화형 메뉴"
    )
    args = parser.parse_args()

    # ✅ Streamlit(subprocess) 환경: stdin이 없으면(input 불가) 자동으로 scan 실행
    non_interactive = (not sys.stdin) or (not sys.stdin.isatty())

    if args.mode == "scan" or (args.mode == "menu" and non_interactive):
        main()
        raise SystemExit

    if args.mode == "query":
        query_loop()
        raise SystemExit

    if args.mode == "portfolio":
        portfolio_edit_loop("positions.csv")
        raise SystemExit

    # 여기부터는 "진짜 터미널에서 직접 실행"할 때만 메뉴 띄움
    print("모드 선택:")
    print("  1) 스캔 실행 (BUY/WATCH/SELL + 디스코드)")
    print("  2) 티커 검색 (ADD/SELL/HOLD 즉시 조회)")
    print("  3) 포트폴리오 편집 (positions.csv add/del/list)")
    mode = input(">> ").strip()

    if mode == "2":
        query_loop()
    elif mode == "3":
        portfolio_edit_loop("positions.csv")
    else:
        main()



