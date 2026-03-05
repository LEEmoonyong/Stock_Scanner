 # scanner_kr.py
# -----------------------------
# 한국 증시 전용 스캐너 (코스피 100 유니버스)
# ticker_universe_kr.TICKERS 사용, kr_scan_snapshot_*.json 출력
# -----------------------------

import os
import sys
import types

# ✅ scanner가 KR 유니버스를 사용하도록 tickers_universe 오버라이드
import ticker_universe_kr
from ticker_universe_kr import TICKER_TO_NAME, TICKER_TO_SECTOR
_tickers_universe = types.ModuleType("tickers_universe")
_tickers_universe.TICKERS = ticker_universe_kr.TICKERS
try:
    from tickers_blacklist import TICKER_BLACKLIST
except ImportError:
    TICKER_BLACKLIST = set()
_tickers_universe.TICKER_BLACKLIST = TICKER_BLACKLIST
sys.modules["tickers_universe"] = _tickers_universe

# 이제 scanner import (tickers_universe에서 KR TICKERS 로드됨)
import scanner_config as cfg
import scanner as sc

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

SCANNER_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# KR 전용 경로
POSITIONS_KR_PATH = os.path.join(SCANNER_BASE_DIR, "positions_kr.csv")
SNAPSHOTS_KR_DIR = os.path.join(SCANNER_BASE_DIR, "snapshots")
SCAN_CSV_KR_DIR = os.path.join(SCANNER_BASE_DIR, "scan_csv")
os.makedirs(SNAPSHOTS_KR_DIR, exist_ok=True)
os.makedirs(SCAN_CSV_KR_DIR, exist_ok=True)

# 한국 시장 벤치마크 (KOSPI, KOSDAQ) + KODEX 200(VKOSPI/거래량비 대체용)
KR_BENCH = ["^KS11", "^KQ11", "069500.KS"]

# 원화 거래대금 최소 (15M USD ≈ 22B KRW, 완화하여 5B KRW)
MIN_KRW_VOL = 5_000_000_000


def _get_kr_df(data, ticker: str):
    """MultiIndex 데이터에서 ticker 추출"""
    try:
        if not isinstance(data.columns, pd.MultiIndex) or ticker not in data.columns.get_level_values(0):
            return None
        df = data[ticker].copy()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df.dropna(subset=["Close"]).copy() if "Close" in df.columns else None
    except Exception:
        return None


def compute_market_state_kr(data):
    """
    한국 증시 전용 시장 상태 (KOSPI/KOSDAQ 기반).
    - KOSPI/KOSDAQ: close, sma50, sma200, above_sma50/200, adx, vol_ratio, return_5d, return_20d
    - KOSPI vs KOSDAQ 상대강도 (성장주 vs 가치주 선호)
    - VKOSPI(한국 변동성지수) 가능 시
    """
    out = {
        "regime": "UNKNOWN",
        "score": None,
        "kospi": {},
        "kosdaq": {},
        "indices": {},
        "kospi_vs_kosdaq": None,
        "kospi_vs_kosdaq_label": None,
        "spy_sma50": None,
        "spy_sma200": None,
        "adx_spy": None,
        "vix": None,
        "spy_vol_ratio": None,
        "vkospi": None,
        "components": {},
        "return_5d": {},
        "return_20d": {},
    }
    if data is None or data.empty:
        return out

    def _get_close_sma(df, n):
        if df is None or "Close" not in df.columns:
            return None, None
        c = df["Close"].dropna()
        if len(c) < n:
            return None, None
        try:
            s = sc.sma(c, n).iloc[-1]
            last = float(c.iloc[-1])
            return last, float(s) if np.isfinite(s) else None
        except Exception:
            return None, None

    def _get_return(df, days):
        if df is None or "Close" not in df.columns or len(df) < days + 1:
            return None
        c = df["Close"].dropna()
        if len(c) < days + 1:
            return None
        try:
            ret = (float(c.iloc[-1]) / float(c.iloc[-(days + 1)]) - 1.0) * 100.0
            return round(ret, 2) if np.isfinite(ret) else None
        except Exception:
            return None

    def _get_vol_ratio(df):
        if df is None or "Volume" not in df.columns or len(df) < 20:
            return None
        try:
            vol = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)
            avg20 = vol.tail(20).mean()
            avg5 = vol.tail(5).mean()
            if avg5 and avg5 > 0 and np.isfinite(avg20):
                return round(float(avg20) / float(avg5), 2)
        except Exception:
            pass
        return None

    indices_score = 0.0
    for sym, label in [("^KS11", "KOSPI"), ("^KQ11", "KOSDAQ")]:
        df = _get_kr_df(data, sym)
        info = {
            "close": None,
            "sma50": None,
            "sma200": None,
            "above_sma50": None,
            "above_sma200": None,
            "adx": None,
            "vol_ratio": None,
            "return_1d": None,
            "return_5d": None,
            "return_20d": None,
        }
        if df is not None and len(df) >= 60:
            last, s50 = _get_close_sma(df, 50)
            _, s200 = _get_close_sma(df, 200)
            info["close"] = round(last, 2) if last is not None else None
            info["sma50"] = round(s50, 2) if s50 is not None else None
            info["sma200"] = round(s200, 2) if s200 is not None else None
            info["above_sma50"] = (last > s50) if (last is not None and s50 is not None) else None
            info["above_sma200"] = (last > s200) if (last is not None and s200 is not None and s200 > 0) else None
            info["above_sma50"] = bool(info["above_sma50"]) if info["above_sma50"] is not None else None
            info["above_sma200"] = bool(info["above_sma200"]) if info["above_sma200"] is not None else None
            if info["above_sma50"]:
                indices_score += 5.0
            if info["above_sma200"]:
                indices_score += 5.0
            try:
                adx_ser = sc.adx(df, 14)
                if adx_ser is not None and len(adx_ser) > 0:
                    info["adx"] = round(float(adx_ser.iloc[-1]), 1)
            except Exception:
                pass
            info["vol_ratio"] = _get_vol_ratio(df)
            # 1일 / 5일 / 20일 수익률
            info["return_1d"] = _get_return(df, 1)
            info["return_5d"] = _get_return(df, 5)
            info["return_20d"] = _get_return(df, 20)
        out["kospi" if label == "KOSPI" else "kosdaq"] = info
        out["indices"][label] = {"above_sma50": info["above_sma50"], "above_sma200": info["above_sma200"]}
    indices_score = min(30.0, indices_score)
    out["components"]["indices"] = round(indices_score, 1)

    # KOSPI 대표값 (regime용)
    kospi_info = out.get("kospi", {})
    out["spy_sma50"] = kospi_info.get("sma50")
    out["spy_sma200"] = kospi_info.get("sma200")
    out["adx_spy"] = kospi_info.get("adx")
    out["spy_vol_ratio"] = kospi_info.get("vol_ratio")

    # ADX 점수 (KOSPI 기준)
    adx_score = 0.0
    df_kospi = _get_kr_df(data, "^KS11")
    if df_kospi is not None and len(df_kospi) >= 50:
        try:
            close = df_kospi["Close"].dropna()
            adx_ser = sc.adx(df_kospi, 14)
            if adx_ser is not None and len(adx_ser) > 0:
                adx_val = float(adx_ser.iloc[-1])
                if adx_val >= 20:
                    if float(close.iloc[-1]) > float(sc.sma(close, 20).iloc[-1]):
                        adx_score = 15.0
                    else:
                        adx_score = -15.0
        except Exception:
            pass
    out["components"]["adx"] = round(adx_score, 1)

    # VKOSPI (한국 변동성지수) - Yahoo에 1326.KS 없음 → KODEX 200 일중 변동성으로 대체
    vix_score = 0.0
    for v_ticker in ["1326.KS", "^VKOSPI", "VKOSPI.KS"]:
        df_v = _get_kr_df(data, v_ticker)
        if df_v is not None and not df_v.empty and "Close" in df_v.columns and len(df_v) >= 5:
            try:
                v_val = float(df_v["Close"].iloc[-1])
                out["vix"] = round(v_val, 1)
                out["vkospi"] = out["vix"]
                if v_val < 15:
                    vix_score = 20.0
                elif v_val < 20:
                    vix_score = 10.0
                elif v_val < 25:
                    vix_score = 0.0
                elif v_val < 30:
                    vix_score = -10.0
                else:
                    vix_score = -20.0
                break
            except Exception:
                pass
    if out.get("vkospi") is None:
        df_kodex = _get_kr_df(data, "069500.KS")
        if df_kodex is not None and len(df_kodex) >= 20 and "High" in df_kodex.columns and "Low" in df_kodex.columns and "Close" in df_kodex.columns:
            try:
                hl = (df_kodex["High"] - df_kodex["Low"]).tail(20)
                cl = df_kodex["Close"].tail(20)
                daily_range_pct = (hl / cl.replace(0, np.nan)).dropna() * 100
                proxy = float(daily_range_pct.mean()) * 8
                out["vkospi"] = round(proxy, 1)
                out["vix"] = out["vkospi"]
                v_val = proxy
                if v_val < 15:
                    vix_score = 20.0
                elif v_val < 20:
                    vix_score = 10.0
                elif v_val < 25:
                    vix_score = 0.0
                elif v_val < 30:
                    vix_score = -10.0
                else:
                    vix_score = -20.0
            except Exception:
                pass
    out["components"]["vix"] = round(vix_score, 1)

    # KOSPI 거래량비 점수 (KOSPI 지수 vol_ratio 없으면 KODEX 200 사용)
    vol_score = 0.0
    vol_r = out.get("spy_vol_ratio")
    if vol_r is None:
        df_kodex = _get_kr_df(data, "069500.KS")
        if df_kodex is not None and len(df_kodex) >= 20:
            vol_r = _get_vol_ratio(df_kodex)
            if vol_r is not None:
                out["spy_vol_ratio"] = vol_r
    if vol_r is not None:
        if vol_r >= 1.0:
            vol_score = 15.0
        elif vol_r >= 0.7:
            vol_score = 0.0
        else:
            vol_score = -15.0
    out["components"]["vol_ratio"] = round(vol_score, 1)

    # 일간 급등/급락 점수 (특히 폭락장 반영)
    day_score = 0.0
    kospi_ret1 = kospi_info.get("return_1d")
    kosdaq_info = out.get("kosdaq", {})
    kosdaq_ret1 = kosdaq_info.get("return_1d") if isinstance(kosdaq_info, dict) else None

    # KOSPI/KOSDAQ 중 더 나쁜(낮은) 1일 수익률 기준으로 평가
    rets = [r for r in [kospi_ret1, kosdaq_ret1] if r is not None]
    if rets:
        worst = min(rets)
        # 큰 폭 하락일수록 강하게 페널티
        if worst <= -7.0:
            day_score = -60.0
        elif worst <= -5.0:
            day_score = -40.0
        elif worst <= -3.0:
            day_score = -25.0
        elif worst <= -2.0:
            day_score = -15.0
        elif worst >= 3.0:
            # 큰 폭 상승장은 약한 가산점만
            day_score = 10.0
    out["components"]["day_return"] = round(day_score, 1)

    # KOSPI vs KOSDAQ 상대강도 (성장주 vs 가치주)
    sector_score = 0.0
    kospi_above = kospi_info.get("above_sma50")
    kosdaq_info = out.get("kosdaq", {})
    kosdaq_above = kosdaq_info.get("above_sma50")
    kospi_ret1 = kospi_info.get("return_1d")
    kosdaq_ret1 = kosdaq_info.get("return_1d") if isinstance(kosdaq_info, dict) else None
    if kospi_above is not None and kosdaq_above is not None:
        if kosdaq_above and not kospi_above:
            sector_score = 20.0
            out["kospi_vs_kosdaq"] = "growth_lead"
            # 단기 등락과 무관하게 "강함" 표현 유지
            out["kospi_vs_kosdaq_label"] = "KOSDAQ 강함 (성장주 선호)"
        elif kosdaq_above and kospi_above:
            sector_score = 10.0
            out["kospi_vs_kosdaq"] = "growth_lead"
            # 양 지수 모두 상승 추세지만, 당일 수익률이 음수면 '상승' 대신 '조정'으로 표현
            if (kospi_ret1 is not None and kospi_ret1 < 0) or (kosdaq_ret1 is not None and kosdaq_ret1 < 0):
                out["kospi_vs_kosdaq_label"] = "KOSDAQ·KOSPI 상승 추세, 단기 조정 (성장주 선호)"
            else:
                out["kospi_vs_kosdaq_label"] = "KOSDAQ·KOSPI 모두 상승 (성장주 선호)"
        elif not kosdaq_above and kospi_above:
            sector_score = -10.0
            out["kospi_vs_kosdaq"] = "value_lead"
            out["kospi_vs_kosdaq_label"] = "KOSPI 강함 (가치주 선호)"
        else:
            sector_score = -20.0
            out["kospi_vs_kosdaq"] = "value_lead"
            out["kospi_vs_kosdaq_label"] = "KOSDAQ·KOSPI 모두 하락 (방어적)"
    out["components"]["sector"] = round(sector_score, 1)

    # return_1d, return_5d, return_20d 저장
    out["return_1d"] = {"KOSPI": kospi_info.get("return_1d"), "KOSDAQ": kosdaq_info.get("return_1d")}
    out["return_5d"] = {"KOSPI": kospi_info.get("return_5d"), "KOSDAQ": kosdaq_info.get("return_5d")}
    out["return_20d"] = {"KOSPI": kospi_info.get("return_20d"), "KOSDAQ": kosdaq_info.get("return_20d")}

    # 최종 점수
    raw = indices_score + adx_score + vix_score + vol_score + sector_score + day_score
    score = max(0.0, min(100.0, 50.0 + raw * 0.5))
    out["score"] = round(score, 1)
    if score <= 33.0:
        out["regime"] = "RISK_OFF"
    elif score <= 66.0:
        out["regime"] = "CAUTION"
    else:
        out["regime"] = "RISK_ON"

    # 최근 5거래일 수익률 높은 섹터 Top3 (유니버스 종목 기준)
    sector_returns = {}
    for ticker in (data.columns.get_level_values(0).unique() if isinstance(data.columns, pd.MultiIndex) else []):
        t = str(ticker).upper().strip()
        if t in ("^KS11", "^KQ11", "069500.KS"):
            continue
        sec = TICKER_TO_SECTOR.get(t)
        # 섹터명이 없거나 'Unknown' 등 의미 없는 값이면 제외
        if not sec:
            continue
        sec_str = str(sec).strip()
        if not sec_str or sec_str.upper() in {"UNKNOWN", "UNCLASSIFIED", "OTHER", "MISC"}:
            continue
        df_t = _get_kr_df(data, ticker)
        if df_t is None or len(df_t) < 6 or "Close" not in df_t.columns:
            continue
        try:
            close = df_t["Close"].dropna()
            if len(close) < 6:
                continue
            ret_5d = (float(close.iloc[-1]) / float(close.iloc[-6]) - 1.0) * 100.0
            if np.isfinite(ret_5d):
                if sec_str not in sector_returns:
                    sector_returns[sec_str] = []
                sector_returns[sec_str].append(ret_5d)
        except Exception:
            continue
    top3 = []
    for sec, rets in sector_returns.items():
        if rets:
            avg_ret = round(float(np.mean(rets)), 2)
            top3.append({"name": sec, "ticker": sec, "return_5d": avg_ret})
    top3.sort(key=lambda x: x["return_5d"], reverse=True)
    out["sector_5d_return_top3"] = top3[:3]

    return out


def _add_spy_alias_from_kospi(data):
    """compute_market_state가 SPY를 사용하므로 ^KS11을 SPY로 별칭"""
    if data is None or data.empty:
        return data
    if not isinstance(data.columns, pd.MultiIndex):
        return data
    if "^KS11" not in data.columns.get_level_values(0):
        return data
    new_data = data.copy()
    for sub in ["Open", "High", "Low", "Close", "Volume"]:
        if ("^KS11", sub) in new_data.columns:
            new_data[("SPY", sub)] = new_data[("^KS11", sub)]
    return new_data


def _should_drop_today_bar_kr() -> bool:
    """한국 장중에는 오늘 진행중 봉 제외"""
    try:
        kst = datetime.now(ZoneInfo("Asia/Seoul"))
        if kst.weekday() >= 5:  # 토일
            return False
        if (kst.hour > 15) or (kst.hour == 15 and kst.minute >= 30):  # 15:30 장마감
            return False
        return True
    except Exception:
        return True


def _drop_today_bar_kr(df):
    """한국 장중일 때 오늘 봉 제거"""
    if df is None or df.empty:
        return df
    try:
        if not _should_drop_today_bar_kr():
            return df
        kst_today = datetime.now(ZoneInfo("Asia/Seoul")).date()
        last_dt = pd.to_datetime(df.index[-1]).date()
        if last_dt >= kst_today:
            return df.iloc[:-1].copy()
    except Exception:
        pass
    return df


def main_kr():
    """한국 증시 스캔 메인"""
    TICKERS = _tickers_universe.TICKERS
    TICKER_BLACKLIST = _tickers_universe.TICKER_BLACKLIST

    run_date = str(datetime.now().date())
    skip_reasons = []
    end = datetime.now().date()
    start = end - timedelta(days=getattr(cfg, "LOOKBACK_DAYS", 2000))

    results = []

    # 보유 포지션
    pos = sc.load_positions(POSITIONS_KR_PATH)
    pos_tickers = []
    if not pos.empty:
        pos_tickers = pos["Ticker"].astype(str).str.upper().str.strip().tolist()

    # 전체 티커 (스캔 + 보유 + 벤치마크)
    scan_universe = [t.upper().strip() for t in TICKERS if t.upper().strip() not in TICKER_BLACKLIST]
    all_tickers = sorted(list(set(scan_universe + pos_tickers + KR_BENCH)))

    if not all_tickers:
        print("[KR] 티커 없음")
        return

    print(f"[KR] 스캔 대상: {len(scan_universe)} 종목 | 보유: {len(pos_tickers)} | 벤치: {KR_BENCH}")

    # 데이터 다운로드
    data = yf.download(
        all_tickers,
        start=str(start),
        end=str(end + timedelta(days=1)),
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=True,
    )

    # 시장 상태 (한국 전용: KOSPI/KOSDAQ, VKOSPI, 시장테마 등)
    sc.MARKET_STATE = compute_market_state_kr(data)
    reg = sc.MARKET_STATE.get("regime", "UNKNOWN")
    score = sc.MARKET_STATE.get("score")
    s50 = sc.MARKET_STATE.get("spy_sma50")  # KOSPI
    s200 = sc.MARKET_STATE.get("spy_sma200")
    q50 = (sc.MARKET_STATE.get("kosdaq") or {}).get("sma50")
    q200 = (sc.MARKET_STATE.get("kosdaq") or {}).get("sma200")
    print(f"[MARKET] regime={reg} | score={score}/100 | KOSPI SMA50={s50} SMA200={s200} | KOSDAQ SMA50={q50} SMA200={q200}")

    # Yahoo 다운로드 결과 확인 (일부 티커는 데이터 미수신)
    if isinstance(data.columns, pd.MultiIndex):
        ticker_to_col = {str(c).upper(): c for c in data.columns.get_level_values(0).unique()}
    else:
        ticker_to_col = {}
    missing_from_yf = [t for t in TICKERS if t.upper().strip() not in TICKER_BLACKLIST and t.upper().strip() not in ticker_to_col]
    if missing_from_yf:
        print(f"[KR] Yahoo 미수신: {len(missing_from_yf)}종 (예: {missing_from_yf[:5]})")

    # 스캔 루프
    for t in TICKERS:
        if t.upper().strip() in TICKER_BLACKLIST:
            continue
        try:
            if isinstance(data.columns, pd.MultiIndex):
                t_key = ticker_to_col.get(t.upper().strip())
                if t_key is None:
                    skip_reasons.append((t, "YF_NO_DATA"))
                    continue
                df = data[t_key].copy()
            else:
                df = data.copy()

            if df is None or df.empty:
                skip_reasons.append((t, "EMPTY_DF"))
                continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # 한국 장중: 오늘 봉 제거
            df = _drop_today_bar_kr(df)

            # 데이터 품질
            end_date = datetime.now(timezone.utc).date()
            ok, q_reason = sc.data_quality_check(df, end_date, max_stale_days=getattr(cfg, "MAX_DATA_STALE_DAYS", 5))
            if not ok:
                skip_reasons.append((t, f"DATA_QUALITY: {q_reason}"))
                continue

            if len(df) < 140:
                skip_reasons.append((t, f"TOO_SHORT: len={len(df)}"))
                continue

            tmp = df.tail(60)
            if tmp[["Open", "High", "Low", "Close", "Volume"]].isna().mean().mean() > 0.05:
                skip_reasons.append((t, "NAN_TOO_MUCH"))
                continue

            # 원화 거래대금 필터 (MIN_DOLLAR_VOL 대신)
            dollar_vol_20 = float((df["Close"].tail(20) * df["Volume"].tail(20)).mean())
            if dollar_vol_20 < MIN_KRW_VOL:
                skip_reasons.append((t, f"LOW_VOL: {dollar_vol_20/1e9:.1f}B KRW"))
                continue

            # 코스닥(.KQ) → KOSDAQ 지수, 코스피(.KS) → KOSPI 지수와 RS 비교
            bench_key = "^KQ11" if str(t).upper().endswith(".KQ") else "^KS11"
            bench_label = "KOSDAQ" if str(t).upper().endswith(".KQ") else "KOSPI"
            r = sc.score_stock(df, t, market_state=sc.MARKET_STATE, data=data,
                              benchmark_key=bench_key, benchmark_label=bench_label)
            if r:
                # RS_vs_SPY → RS_vs_KOSPI or KOSDAQ (종목 시장에 따라 KOSPI 또는 KOSDAQ 비교)
                if "RS_vs_SPY" in r:
                    val = r.pop("RS_vs_SPY")
                    r["RS_vs_KOSPI or KOSDAQ"] = val
                # Ticker → Name (회사명으로 표시, 내부 참조용 Ticker 유지)
                ticker_val = r.get("Ticker", t)
                r["Name"] = TICKER_TO_NAME.get(str(ticker_val).upper(), str(ticker_val))
                r["Ticker"] = ticker_val  # 앱/포트폴리오 연동용 유지
                results.append(r)
        except Exception as e:
            skip_reasons.append((t, str(e)[:80]))
            continue

    df_all = pd.DataFrame(results)
    # RS_vs_KOSPI or KOSDAQ를 ADX와 PctOff52H 사이로 이동
    rs_col = "RS_vs_KOSPI or KOSDAQ"
    if rs_col in df_all.columns and "ADX" in df_all.columns and "PctOff52H" in df_all.columns:
        cols = list(df_all.columns)
        cols.remove(rs_col)
        idx_adx = cols.index("ADX")
        cols.insert(idx_adx + 1, rs_col)
        df_all = df_all[cols].copy()
    # Name을 첫 번째 컬럼으로 (회사명 표시), Ticker는 앱/트래커 연동용 유지
    if "Name" in df_all.columns and "Ticker" in df_all.columns:
        mid = [c for c in df_all.columns if c not in ("Name", "Ticker")]
        df_all = df_all[["Name"] + mid + ["Ticker"]].copy()
    if df_all.empty:
        print("[KR] 조건 만족 후보 없음")
        snapshot_path = os.path.join(SNAPSHOTS_KR_DIR, f"kr_scan_snapshot_{run_date}.json")
        sc.save_scan_snapshot(
            snapshot_path, run_date, sc.MARKET_STATE,
            df_all, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            pd.DataFrame(), pd.DataFrame(),
            f"kr_swing_scanner_{run_date}.csv",
        )
        return

    # -----------------------------
    # 정렬: US 스캐너와 100% 동일
    # -----------------------------
    priority = {
        "BUY_BREAKOUT": 0, "BUY_PULLBACK": 1, "BUY_SMART": 2,
        "WATCH_BREAKOUT": 3, "WATCH_PULLBACK": 4, "WATCH_ADX": 4, "WATCH_RS": 4,
        "WATCH_52WK": 4, "WATCH_MA_STACK": 4, "WATCH_FUNDAMENTAL": 4, "WATCH_EARNINGS": 4,
        "CANDIDATE_BUY": 5, "SKIP": 9,
    }
    df_all["P"] = df_all["Entry"].map(priority).fillna(9).astype(int)
    df_all["RR_sort"] = df_all["RR"].fillna(-1.0).astype(float)
    df_all = df_all.sort_values(["P", "RR_sort", "Score", "Avg$Vol"], ascending=[True, False, False, False]).drop(columns=["RR_sort"])

    # -----------------------------
    # BUY/WATCH 선정: USE_SECTOR_CAP (US와 동일)
    # -----------------------------
    if getattr(cfg, "USE_SECTOR_CAP", True):
        picked = []
        counts = {}
        for _, row in df_all.iterrows():
            sec = row.get("Sector", "Unknown") or "Unknown"
            if counts.get(sec, 0) >= getattr(cfg, "MAX_PER_SECTOR", 3):
                continue
            picked.append(row)
            counts[sec] = counts.get(sec, 0) + 1
            if len(picked) >= 30:
                break
        df_out = pd.DataFrame(picked)
    else:
        df_out = df_all.head(30)

    buy_df = df_out[df_out["Entry"].astype(str).str.startswith("BUY_")].head(getattr(cfg, "MAX_BUY_PER_DAY", 10)).copy()

    watch_df_all = df_all[df_all["Entry"].astype(str).str.startswith("WATCH_")].copy()
    if getattr(cfg, "USE_SECTOR_CAP", True):
        picked_w = []
        counts_w = {}
        for _, row in watch_df_all.iterrows():
            sec = row.get("Sector", "Unknown") or "Unknown"
            if counts_w.get(sec, 0) >= getattr(cfg, "MAX_PER_SECTOR", 3):
                continue
            picked_w.append(row)
            counts_w[sec] = counts_w.get(sec, 0) + 1
            if len(picked_w) >= getattr(cfg, "WATCH_LIMIT", 30):
                break
        watch_df = pd.DataFrame(picked_w)
    else:
        watch_df = watch_df_all.head(getattr(cfg, "WATCH_LIMIT", 30)).copy()

    # -----------------------------
    # 실적 N거래일 이내 BUY → WATCH 강등 (US와 동일)
    # -----------------------------
    downgrade_trading_days = int(getattr(cfg, "EARNINGS_DOWNGRADE_TRADING_DAYS", 5))
    if downgrade_trading_days >= 0 and buy_df is not None and not buy_df.empty and "Ticker" in buy_df.columns:
        try:
            ref_date = pd.Timestamp(run_date).date() if run_date else datetime.now(timezone.utc).date()
        except Exception:
            ref_date = datetime.now(timezone.utc).date()
        keep_buy = []
        downgraded = []
        for _, row in buy_df.iterrows():
            t = str(row.get("Ticker", "")).upper().strip()
            if not t:
                keep_buy.append(row)
                continue
            trading_days_ahead = sc._trading_days_until_next_earnings(t, ref_date)
            if trading_days_ahead is not None and 0 <= trading_days_ahead <= downgrade_trading_days:
                r = row.to_dict()
                r["Entry"] = "WATCH_EARNINGS"
                r["EntryRaw"] = r.get("EntryRaw", row.get("Entry", ""))
                r["Promoted"] = False
                r["PromoTag"] = ""
                orig_note = str(r.get("Note", "")).strip()
                r["Note"] = (orig_note + " [실적 " + str(trading_days_ahead) + "거래일 전 → WATCH 강등]").strip(" |")
                downgraded.append(r)
            else:
                keep_buy.append(row)
        if downgraded:
            buy_df = pd.DataFrame(keep_buy).reset_index(drop=True) if keep_buy else pd.DataFrame()
            downgraded_df = pd.DataFrame(downgraded)
            if not watch_df.empty and len(watch_df.columns) > 0:
                for c in watch_df.columns:
                    if c not in downgraded_df.columns:
                        downgraded_df[c] = np.nan
                downgraded_df = downgraded_df.reindex(columns=watch_df.columns, fill_value=np.nan)
            watch_df = pd.concat([watch_df, downgraded_df], ignore_index=True)
            downgraded_tickers = {str(r.get("Ticker", "")).upper().strip() for r in downgraded if r.get("Ticker")}
            for t in downgraded_tickers:
                if not t:
                    continue
                r = next((x for x in downgraded if str(x.get("Ticker", "")).upper().strip() == t), None)
                if r is None:
                    continue
                for target in [df_out, df_all]:
                    if target is None or target.empty or "Ticker" not in target.columns:
                        continue
                    mask = target["Ticker"].astype(str).str.upper().str.strip() == t
                    if mask.any():
                        target.loc[mask, "Entry"] = r.get("Entry", "WATCH_EARNINGS")
                        if "EntryRaw" in target.columns:
                            target.loc[mask, "EntryRaw"] = r.get("EntryRaw", "")
                        if "Note" in target.columns:
                            target.loc[mask, "Note"] = r.get("Note", "")

    if "Promoted" not in df_out.columns:
        df_out["Promoted"] = False
    if "Promoted" not in buy_df.columns:
        buy_df["Promoted"] = False

    # -----------------------------
    # SMART RELAX: BUY가 적을 때 WATCH에서 승격 (US와 동일)
    # -----------------------------
    def _kr_bench_key(t):
        return "^KQ11" if ".KQ" in str(t).upper() else "^KS11"
    buy_df = sc.apply_smart_relax_promote(
        df_all, buy_df, watch_df, data, run_date,
        rs_col="RS_vs_KOSPI or KOSDAQ",
        benchmark_key_func=_kr_bench_key,
    )

    # BUY 리스트 EV 순 정렬 (US와 동일)
    if buy_df is not None and not buy_df.empty:
        buy_df = sc.ev_rank_top_picks(buy_df, n=getattr(cfg, "MAX_BUY_PER_DAY", 10))
        # ✅ BUY 행 EV/Prob를 ev_rank 결과로 df_out·df_all 동기화 (US와 동일)
        sc._sync_buy_ev_prob_to_dfs(buy_df, df_out, df_all)

    top_picks = buy_df.head(3).copy() if not buy_df.empty else pd.DataFrame()

    # Risk / Recos (간소화)
    risk_df = pd.DataFrame()
    recos_df = pd.DataFrame()

    # CSV 저장 (US와 동일: df_out = 섹터캡 상위 30)
    outname = f"kr_swing_scanner_{run_date}.csv"
    df_out.to_csv(os.path.join(SCAN_CSV_KR_DIR, outname), index=False, encoding="utf-8-sig")

    # 스냅샷 저장
    snapshot_path = os.path.join(SNAPSHOTS_KR_DIR, f"kr_scan_snapshot_{run_date}.json")
    sc.save_scan_snapshot(
        snapshot_path, run_date, sc.MARKET_STATE,
        df_all, buy_df, watch_df, top_picks,
        risk_df, recos_df,
        outname,
    )
    print(f"[SNAPSHOT] saved: {snapshot_path}")
    print(f"[KR] BUY {len(buy_df)} | WATCH {len(watch_df)} | Top3: {list(top_picks['Ticker'].values) if not top_picks.empty else []}")

    if skip_reasons:
        from collections import Counter
        by_reason = Counter(rs for _, rs in skip_reasons)
        print(f"\n[SKIP] {len(skip_reasons)}개 (스캔 통과: {len(df_all)} | 유니버스: {len(scan_universe)})")
        for reason, cnt in by_reason.most_common(10):
            print(f"  - {reason}: {cnt}종")
        for tk, rs in sorted(skip_reasons, key=lambda x: x[1])[:15]:
            print(f"    예: {tk}: {rs}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["scan"], default="scan")
    args = parser.parse_args()
    main_kr()
