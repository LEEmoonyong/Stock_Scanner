import html
import math
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List, Any

import numpy as np
import pandas as pd
import yfinance as yf
import streamlit as st
import os
import json
import glob
from datetime import date
import scanner_config as cfg
import scanner as sc  # 기존 scanner.py 
import plotly.graph_objects as go

try:
    import ohlcv_fetcher
except ImportError:
    ohlcv_fetcher = None

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, JsCode, GridUpdateMode
    _HAS_AGGRID = True
except Exception:
    try:
        from streamlit_aggrid import AgGrid, GridOptionsBuilder, JsCode, GridUpdateMode  # type: ignore[import-not-found]
        _HAS_AGGRID = True
    except Exception:
        _HAS_AGGRID = False
        AgGrid = GridOptionsBuilder = JsCode = None
        GridUpdateMode = None
import sys
import subprocess
import subprocess, sys, time
# (app.py) 파일 상단 근처
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SNAPSHOT_PATTERN = os.path.join(BASE_DIR, "snapshots", "scan_snapshot_*.json")

# ---------- app meta ----------
APP_VERSION = "2026-02-10_v2"  # 너가 UI 수정할 때마다 날짜/버전만 바꿔도 캐시가 자연스럽게 분리됨

# 표 다크 테마 (참고 이미지: 진한 남색 배경, 푸른 광선 테두리, Return% 초록/빨강, Ticker 앞 녹색 사각형)
def _return_pct_cell_style(val):
    if pd.isna(val):
        return "color: #e2e8f0; font-weight: 700;"
    try:
        v = float(val)
        if v > 0:
            return "color: #22c55e; font-weight: 700;"
        if v < 0:
            return "color: #ef4444; font-weight: 700;"
    except (TypeError, ValueError):
        pass
    return "color: #e2e8f0; font-weight: 700;"

def _dark_table_style(df: pd.DataFrame):
    if df is None or df.empty:
        return df
    # Return% / ReturnPct 열만 셀별 색상 적용
    def _style_cells(x):
        out = pd.DataFrame("", index=x.index, columns=x.columns)
        for col in ("Return%", "ReturnPct"):
            if col in x.columns:
                out[col] = x[col].apply(_return_pct_cell_style)
        return out

    fmt = {}
    if "Ticker" in df.columns:
        fmt["Ticker"] = lambda v: ("■ " + str(v).strip()) if pd.notna(v) and str(v).strip() else ""
    # Return% / ReturnPct: 수익 +X.XX / 손실 -X.XX 형태
    def _pct_fmt(v):
        if pd.isna(v): return ""
        try:
            n = float(v)
            if n > 0: return f"+{n:.2f}"
            return f"{n:.2f}"
        except (TypeError, ValueError): return ""
    for c in ("Return%", "ReturnPct"):
        if c in df.columns:
            fmt[c] = _pct_fmt
    # 날짜 열: YYYY-MM-DD만 (00:00:00 제거)
    def _date_fmt(v):
        if pd.isna(v): return ""
        try:
            t = pd.Timestamp(v)
            return t.strftime("%Y-%m-%d")
        except Exception:
            return str(v)[:10] if len(str(v)) >= 10 else str(v)
    for c in df.columns:
        if c in fmt:
            continue
        if "Date" in c or "date" in c.lower():
            fmt[c] = _date_fmt
    # 숫자 열 소수 둘째자리 (가격 등)
    for c in df.columns:
        if c in fmt:
            continue
        if df[c].dtype.kind in "fc" and "Date" not in c and "Pct" not in c and "Return" not in c:
            fmt[c] = "{:,.2f}"

    styler = (
        df.style
        .apply(_style_cells, axis=None)
        .format(fmt, na_rep="")
        .set_properties(
            **{
                "background-color": "rgba(15,23,42,0.92)",
                "color": "#e2e8f0",
                "border": "1px solid rgba(100,116,139,0.35)",
                "padding": "12px 14px",
                "font-size": "0.9rem",
                "font-family": "'Roboto', 'JetBrains Mono', sans-serif",
            }
        )
        .set_table_styles(
            [
                {
                    "selector": "table",
                    "props": [
                        ("border-collapse", "separate"),
                        ("border-spacing", "0"),
                        ("box-shadow", "0 0 24px rgba(59, 130, 246, 0.25), 0 0 48px rgba(99, 102, 241, 0.15)"),
                        ("border-radius", "10px"),
                        ("overflow", "hidden"),
                    ],
                },
                {
                    "selector": "th",
                    "props": [
                        ("background", "linear-gradient(180deg, rgba(30,41,59,0.98) 0%, rgba(30,41,59,0.95) 100%)"),
                        ("color", "#f1f5f9"),
                        ("padding", "12px 14px"),
                        ("font-weight", "700"),
                        ("border", "1px solid rgba(100,116,139,0.4)"),
                        ("border-bottom", "2px solid rgba(148,163,184,0.5)"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [("border", "1px solid rgba(100,116,139,0.3)")],
                },
                {"selector": "tr:hover td", "props": [("background-color", "rgba(51,65,85,0.6)")]},
            ]
        )
    )
    # 행 번호(0,1,2...) 인덱스 열 숨김 → 참고 이미지처럼 깔끔하게
    try:
        styler = styler.hide(axis="index")
    except Exception:
        try:
            styler = styler.hide_index()
        except Exception:
            pass
    # Ticker가 첫 번째 열일 때만 ■ + 녹색 적용
    if len(df.columns) and df.columns[0] == "Ticker":
        styler = styler.set_table_styles(
            [{"selector": "td:first-child", "props": [("color", "#22c55e"), ("font-weight", "600")]}],
            overwrite=False,
        )
    # 숫자 열 오른쪽 정렬 (Ticker 제외)
    num_cols = [c for c in df.columns if c != "Ticker" and df[c].dtype.kind in "iufc" and "Date" not in c]
    if num_cols:
        col_idx = [df.columns.get_loc(c) for c in num_cols]
        for i in col_idx:
            styler = styler.set_table_styles(
                [
                    {
                        "selector": f"td:nth-child({i+1})",
                        "props": [("text-align", "right")],
                    },
                    {
                        "selector": f"th:nth-child({i+1})",
                        "props": [("text-align", "right")],
                    },
                ],
                overwrite=False,
            )
    return styler


# ---------- AgGrid: 참고 이미지와 동일한 다크 테마 표 (theme=dark + custom_css + Return% 색상) ----------
def _prepare_df_for_aggrid(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if "Ticker" in out.columns:
        out["Ticker"] = out["Ticker"].apply(lambda v: ("■ " + str(v).strip()) if pd.notna(v) and str(v).strip() else "")
    for c in ("Return%", "ReturnPct"):
        if c in out.columns:
            out[c] = out[c].apply(lambda v: f"{float(v):.2f}" if pd.notna(v) and str(v).strip() not in ("", "nan") else "")
    for c in out.columns:
        if "Date" in c or "date" in c.lower():
            def _d(v):
                if pd.isna(v): return ""
                try: return pd.Timestamp(v).strftime("%Y-%m-%d")
                except Exception: return str(v)[:10] if len(str(v)) >= 10 else str(v)
            out[c] = out[c].apply(_d)
    return out

def _aggrid_dark_css() -> dict:
    """HTML 표와 동일한 다크 테마: 배경·헤더 그라데이션·테두리·그림자·호버."""
    return {
        ".ag-root.ag-unselectable.ag-layout-normal": {"background-color": "rgba(15,23,42,0.95) !important", "border-radius": "10px"},
        ".ag-header": {"background": "linear-gradient(180deg, rgba(30,41,59,0.98) 0%, rgba(30,41,59,0.95) 100%) !important", "border-bottom": "2px solid rgba(148,163,184,0.5) !important"},
        ".ag-header-cell": {"color": "#f1f5f9 !important", "font-weight": "700 !important", "border": "1px solid rgba(100,116,139,0.4) !important", "padding": "10px 8px !important", "text-align": "center !important"},
        ".ag-header-cell-text": {"color": "#f1f5f9 !important"},
        ".ag-cell": {"background-color": "rgba(15,23,42,0.92) !important", "color": "#e2e8f0 !important", "border": "1px solid rgba(100,116,139,0.35) !important", "padding": "10px 8px !important", "font-size": "0.9rem", "line-height": "1.5 !important"},
        ".ag-row": {"min-height": "42px !important"},
        ".ag-row:hover .ag-cell": {"background-color": "rgba(51,65,85,0.6) !important"},
        ".ag-row .ag-cell:first-child": {"color": "#22c55e !important", "font-weight": "600 !important", "text-align": "left !important", "padding-left": "12px !important"},
        ".ag-body-viewport": {"background-color": "rgba(15,23,42,0.92) !important"},
        ".ag-center-cols-viewport": {"background-color": "rgba(15,23,42,0.92) !important"},
        ".ag-theme-balham-dark": {"border": "1px solid rgba(100,116,139,0.5) !important", "box-shadow": "0 0 24px rgba(59,130,246,0.3) !important", "border-radius": "10px !important"},
        ".ag-cell.return-pct-positive": {"color": "#22c55e !important", "font-weight": "700 !important", "text-align": "right !important"},
        ".ag-cell.return-pct-negative": {"color": "#ef4444 !important", "font-weight": "700 !important", "text-align": "right !important"},
    }

_AGGRID_RETURN_STYLE_JS = JsCode("""
function(params) {
  var base = { fontWeight: '700', textAlign: 'right' };
  if (params.value == null || params.value === '') return Object.assign(base, { color: '#e2e8f0' });
  var v = params.value;
  var n = typeof v === 'number' ? v : parseFloat(String(v).replace(/[^\\d.-]/g, '')) || 0;
  if (n > 0) return Object.assign(base, { color: '#22c55e' });
  if (n < 0) return Object.assign(base, { color: '#ef4444' });
  return Object.assign(base, { color: '#e2e8f0' });
}
""") if _HAS_AGGRID else None

# 수익률 열 색상: cellStyle 대신 cellClass 사용 (streamlit-aggrid에서 더 안정적)
_AGGRID_RETURN_CELL_CLASS_JS = JsCode("""
function(params) {
  if (params.value == null || params.value === '') return '';
  var v = params.value;
  var n = typeof v === 'number' ? v : parseFloat(String(v).replace(/[^\\d.-]/g, '')) || 0;
  if (n > 0) return 'return-pct-positive';
  if (n < 0) return 'return-pct-negative';
  return '';
}
""") if _HAS_AGGRID else None

def _build_aggrid_options(df: pd.DataFrame):
    # 1.x: from_dataframe(dataframe, **default_column_parameters) — resizable 등은 defaultColDef로
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(min_column_width=60, resizable=True, filterable=True, sortable=True)
    for col in ("Return%", "ReturnPct"):
        if col in df.columns and _AGGRID_RETURN_STYLE_JS:
            gb.configure_column(col, cellStyle=_AGGRID_RETURN_STYLE_JS)
    gb.configure_grid_options(domLayout="normal")
    try:
        gb.configure_grid_options(suppressRowNumbers=True)
    except Exception:
        pass
    go = gb.build()
    # 행 번호(0,1,2...) 인덱스 열 제거 — 참고 이미지에는 없음 (표시할 컬럼만 유지)
    col_defs = go.get("columnDefs") or []
    valid_fields = set(df.columns)
    go["columnDefs"] = [c for c in col_defs if c.get("field") in valid_fields]
    return go

def _render_aggrid(df, key: str, height: int = 400):
    """참고 이미지와 동일한 다크 테마 AgGrid. 표시 전용."""
    if not _HAS_AGGRID:
        st.dataframe(_dark_table_style(df), use_container_width=True)
        return
    if df is None or df.empty:
        st.dataframe(_dark_table_style(df), use_container_width=True)
        return
    try:
        display_df = _prepare_df_for_aggrid(df)
        go = _build_aggrid_options(display_df)
        n = max(3, min(25, len(display_df)))
        h = min(500, 80 + n * 36)
        # 1.x API: data, gridOptions, theme, custom_css, update_mode, key (fit_columns_on_grid_load 없음)
        AgGrid(
            display_df,
            gridOptions=go,
            theme="dark",
            height=h,
            custom_css=_aggrid_dark_css(),
            allow_unsafe_jscode=True,
            update_mode=GridUpdateMode.NO_UPDATE if GridUpdateMode else 0,
            key=key,
        )
    except Exception as e:
        # AgGrid 오류 시 기존 스타일 표로 폴백
        st.dataframe(_dark_table_style(df), use_container_width=True)
        if st.session_state.get("aggrid_debug"):
            st.caption(f"AgGrid fallback: {e}")


# 신호 성과 추적 표: HTML 디자인 그대로 AgGrid로 (헤더명·Return% +/- %·색상·Ticker ■)
_AGGRID_RETURN_FORMATTER_JS = JsCode("""
function(params) {
  if (params.value == null || params.value === '') return '';
  var n = Number(params.value);
  if (n > 0) return '+' + n.toFixed(2) + '%';
  return n.toFixed(2) + '%';
}
""") if _HAS_AGGRID else None

def _tracker_table_html(rows: List[dict]) -> str:
    """성과 추적 표를 HTML 문자열로 생성. Return% 셀에 수익=초록/손실=빨강 인라인 스타일 적용."""
    if not rows:
        return ""
    headers = ["Ticker", "Signal Date", "Entry Date", "Entry Price (Prev Close)", "Close (Now)", "Return%", "Days Held"]
    key_map = [
        ("Ticker", "ticker"),
        ("SignalDate", "date"),
        ("EntryDate", "date"),
        ("EntryPrice(PrevClose)", "num"),
        ("Close(Now)", "num"),
        ("Return%", "pct"),
        ("DaysHeld", "num"),
    ]
    wrap = "width:100%;max-height:70vh;overflow:auto;border-radius:10px;box-shadow:0 0 24px rgba(59,130,246,0.3);border:1px solid rgba(100,116,139,0.5);margin:0.5rem 0;"
    table = "width:100%;min-width:800px;table-layout:fixed;border-collapse:collapse;background:rgba(15,23,42,0.95);font-size:0.9rem;"
    th_style = "background:linear-gradient(180deg,rgba(30,41,59,0.98),rgba(30,41,59,0.95));color:#f1f5f9;font-weight:700;padding:10px 8px;text-align:center;border:1px solid rgba(100,116,139,0.4);"
    td_base = "background:rgba(15,23,42,0.92);color:#e2e8f0;padding:10px 8px;border:1px solid rgba(100,116,139,0.35);"
    parts = [f'<div style="{wrap}"><table style="{table}"><thead><tr>']
    for h in headers:
        parts.append(f'<th style="{th_style}">{html.escape(h)}</th>')
    parts.append("</tr></thead><tbody>")
    for r in rows:
        parts.append("<tr>")
        for key, kind in key_map:
            val = r.get(key)
            if kind == "ticker":
                txt = (str(val).strip() if val is not None else "")
                parts.append(f'<td style="{td_base} color:#22c55e;font-weight:600;text-align:left;">■ {html.escape(txt)}</td>')
            elif kind == "date":
                try:
                    txt = pd.Timestamp(val).strftime("%Y-%m-%d") if pd.notna(val) else ""
                except Exception:
                    txt = str(val)[:10] if val else ""
                parts.append(f'<td style="{td_base} text-align:left;">{html.escape(txt)}</td>')
            elif kind == "num":
                if val is None or (isinstance(val, float) and not np.isfinite(val)):
                    txt = ""
                else:
                    txt = f"{float(val):,.2f}" if isinstance(val, (int, float)) else str(val)
                parts.append(f'<td style="{td_base} text-align:right;font-weight:600;">{html.escape(txt)}</td>')
            elif kind == "pct":
                try:
                    n = float(val) if val is not None else 0.0
                except (TypeError, ValueError):
                    n = 0.0
                if n > 0:
                    txt, color = f"+{n:.2f}", "#22c55e"
                elif n < 0:
                    txt, color = f"{n:.2f}", "#ef4444"
                else:
                    txt, color = "0.00", "#e2e8f0"
                pct_style = f"{td_base} color:{color};font-weight:700;text-align:right;"
                parts.append(f'<td style="{pct_style}">{html.escape(txt)}</td>')
        parts.append("</tr>")
    parts.append("</tbody></table></div>")
    return "".join(parts)


def _render_tracker_aggrid(rows: List[dict]) -> None:
    """신호 성과 추적 표 렌더. st.dataframe은 셀별 색상을 지원하지 않으므로 HTML 표로 렌더해 수익=초록/손실=빨강 적용."""
    if not rows:
        st.info("표시할 데이터가 없습니다.")
        return
    html_table = _tracker_table_html(rows)
    if html_table:
        st.markdown(html_table, unsafe_allow_html=True)
    else:
        st.info("표시할 데이터가 없습니다.")


def _dataframe_to_tracker_style_html(df: pd.DataFrame, pct_colors: bool = False, col_widths: Optional[List[str]] = None) -> str:
    """DataFrame을 성과추적 표와 동일한 다크 테마 HTML 표로 변환. Ticker 열 ■+초록.
    pct_colors=True면 수익률 열 초록/빨강(성과추적표처럼), False면 흰색(스캐너용).
    col_widths를 주면 열 비율 고정(예: ["18%%", "27%%", "27%%", "28%%"])."""
    if df is None or df.empty:
        return ""
    wrap = "width:100%;max-height:38vh;overflow-x:auto;overflow-y:auto;border-radius:10px;box-shadow:0 0 24px rgba(59,130,246,0.3);border:1px solid rgba(100,116,139,0.5);margin:0.4rem 0;"
    use_fixed = col_widths is not None and len(col_widths) > 0
    if use_fixed and len(col_widths) == len(df.columns):
        table = "width:100%;table-layout:fixed;border-collapse:collapse;background:rgba(15,23,42,0.95);font-size:0.9rem;"
    else:
        table = "width:max-content;min-width:100%;table-layout:auto;border-collapse:collapse;background:rgba(15,23,42,0.95);font-size:0.9rem;"
    th_style = "background:linear-gradient(180deg,rgba(30,41,59,0.98),rgba(30,41,59,0.95));color:#f1f5f9;font-weight:700;padding:8px 10px;text-align:center;border:1px solid rgba(100,116,139,0.4);white-space:nowrap;"
    td_base = "background:rgba(15,23,42,0.92);color:#e2e8f0;padding:8px 10px;border:1px solid rgba(100,116,139,0.35);"
    cols = list(df.columns)
    parts = [f'<div style="{wrap}"><table style="{table}">']
    if use_fixed and len(col_widths) == len(cols):
        parts.append("<colgroup>")
        for w in col_widths:
            parts.append(f'<col style="width:{w}">')
        parts.append("</colgroup>")
    parts.append("<thead><tr>")
    for c in cols:
        parts.append(f'<th style="{th_style}">{html.escape(str(c))}</th>')
    parts.append("</tr></thead><tbody>")
    for _, row in df.iterrows():
        parts.append("<tr>")
        for i, c in enumerate(cols):
            val = row.get(c)
            is_ticker = (c == "Ticker" or (i == 0 and "ticker" in str(c).lower()))
            is_pct = ("return" in str(c).lower() or "pct" in str(c).lower() or "%" in str(c))
            if is_ticker:
                txt = (str(val).strip() if val is not None and pd.notna(val) else "")
                parts.append(f'<td style="{td_base} color:#22c55e;font-weight:600;text-align:left;">■ {html.escape(txt)}</td>')
            elif is_pct:
                try:
                    n = float(val) if val is not None and pd.notna(val) else 0.0
                except (TypeError, ValueError):
                    n = 0.0
                txt = f"+{n:.2f}" if n > 0 else f"{n:.2f}"
                if pct_colors:
                    color = "#22c55e" if n > 0 else "#ef4444" if n < 0 else "#e2e8f0"
                    parts.append(f'<td style="{td_base} color:{color};font-weight:700;text-align:right;">{html.escape(txt)}</td>')
                else:
                    parts.append(f'<td style="{td_base} text-align:right;font-weight:600;">{html.escape(txt)}</td>')
            elif isinstance(val, (int, float)) and np.isfinite(val):
                txt = f"{val:,.2f}" if isinstance(val, float) else f"{val:,}"
                parts.append(f'<td style="{td_base} text-align:right;font-weight:600;">{html.escape(txt)}</td>')
            else:
                try:
                    if pd.notna(val) and hasattr(val, "strftime"):
                        txt = pd.Timestamp(val).strftime("%Y-%m-%d")
                    else:
                        txt = "" if val is None or (isinstance(val, float) and not np.isfinite(val)) else str(val)
                except Exception:
                    txt = "" if val is None else str(val)
                parts.append(f'<td style="{td_base} text-align:left;">{html.escape(txt)}</td>')
        parts.append("</tr>")
    parts.append("</tbody></table></div>")
    return "".join(parts)


# 목표가/손절가 표 공통 열 비율 (Ticker + 3열)
_TP_STOP_COL_WIDTHS = ["18%", "27%", "27%", "28%"]


def _render_tracker_style_table(df: pd.DataFrame, pct_colors: bool = False, col_widths: Optional[List[str]] = None) -> None:
    """DataFrame을 성과추적 표와 동일한 HTML 스타일로 렌더. pct_colors=True면 수익률 열 초록/빨강. col_widths로 열 비율 고정."""
    if df is None or df.empty:
        st.info("표시할 데이터가 없습니다.")
        return
    html_table = _dataframe_to_tracker_style_html(df, pct_colors=pct_colors, col_widths=col_widths)
    if html_table:
        st.markdown(html_table, unsafe_allow_html=True)


def get_cache_buster():
    """강제 새로고침/스캔 실행 시 data_refresh_ts가 설정되면 캐시 키가 바뀌어 미국 최근 종가 기준으로 재조회됨."""
    ts = st.session_state.get("data_refresh_ts", "")
    return APP_VERSION + ("_" + str(ts) if ts else "")

def hard_refresh():
    # 미국 최근 종가 기준 전 데이터 최신화 (세션 전체 clear 없이 캐시/스냅/트래커만 정리)
    st.session_state["data_refresh_ts"] = datetime.now().isoformat()
    st.cache_data.clear()
    st.session_state.pop("scan_snap", None)
    st.session_state.pop("tp3_tracker", None)
    st.rerun()

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


# ---------- helpers ----------

@st.cache_data(show_spinner=False, ttl=60*60)
def fetch_price(
    ticker: str,
    lookback_days: int,
    cache_buster: str,
    min_rows: int = 0,
    retries: int = 0,
):
    """
    ✅ 캐시(로컬 DB) -> 1차 yfinance -> 2차 Alpha Vantage 폴백.
    - 캐시 hit 시 API 호출 없음. 2차는 ALPHA_VANTAGE_API_KEY 환경변수 필요.
    - min_rows/retries: 데이터 부족 시 lookback 늘려 재시도.
    """

    def _download(days: int, need_rows: int = 0) -> Optional[pd.DataFrame]:
        end = datetime.now(ZoneInfo("America/New_York")).date()
        start = end - timedelta(days=int(days))

        df = None
        if ohlcv_fetcher is not None:
            df = ohlcv_fetcher.fetch_ohlcv_with_fallback(
                ticker, start, end, min_rows=need_rows, base_dir=BASE_DIR
            )
        if df is None or df.empty:
            import yfinance as yf
            df = yf.download(
                tickers=ticker,
                start=str(start),
                end=str(end + timedelta(days=1)),
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
                    df = df[ticker]
                else:
                    df.columns = df.columns.get_level_values(-1)
        need_cols = ["Open", "High", "Low", "Close", "Volume"]
        for c in need_cols:
            if c not in df.columns:
                return None
        df = df.dropna(subset=need_cols).copy()
        df = _drop_today_bar_if_needed(df)
        return df

    # 1차 시도 (min_rows 전달: 캐시만으로 부족하면 yf/AV 재시도하도록)
    df = _download(lookback_days, int(min_rows))

    # 재시도 (데이터 부족/비어있음 대응)
    attempt = 0
    cur_days = int(lookback_days)
    while attempt < int(retries):
        if df is not None and not df.empty and (min_rows <= 0 or len(df) >= int(min_rows)):
            break
        cur_days = int(cur_days * 1.6) + 30
        df = _download(cur_days, int(min_rows))
        attempt += 1

    if df is None or df.empty:
        return None
    if min_rows > 0 and len(df) < int(min_rows):
        return None
    return df





# ---------- UI helpers ----------
# =========================
# Charts (SPY/QQQ/USDKRW) + TopPick BUY Performance Tracker
# =========================

TRACKER_CSV = "top_pick_buy_tracker.csv"

def _load_snapshot(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _records_to_df(records) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    return pd.DataFrame.from_records(records)



def get_latest_snapshot(pattern: Optional[str] = None) -> Optional[str]:
    # ✅ Streamlit CWD와 무관하게 app.py 위치 기준으로 찾기
    pat = pattern or SNAPSHOT_PATTERN
    files = sorted(glob.glob(pat))
    if not files:
        return None
    return files[-1]



def load_scan_snapshot(path: str) -> dict:
    snap = _load_snapshot(path)

    # scanner.py save_scan_snapshot() 스키마 그대로 복원
    out = {
        "run_date": snap.get("run_date"),
        "market_state": snap.get("market_state") or {},
        "out_csv": snap.get("out_csv"),
        "counts": snap.get("counts") or {},

        "df_all": _records_to_df(snap.get("df_all", [])),
        "buy_df": _records_to_df(snap.get("buy_df", [])),
        "watch_df": _records_to_df(snap.get("watch_df", [])),
        "top_picks": _records_to_df(snap.get("top_picks", [])),
        "risk_df": _records_to_df(snap.get("risk_df", [])),
        "recos_df": _records_to_df(snap.get("recos_df", [])),
    }
    return out

def _ensure_dt(x):
    """datetime/날짜 → date. NaT/None이면 None 반환(비교 오류 방지)."""
    try:
        d = pd.to_datetime(x)
        if d is pd.NaT or pd.isna(d):
            return None
        return d.date() if hasattr(d, "date") else d
    except Exception:
        return None

def _get_usdkrw_df(lookback_days: int = 240):
    for fx_ticker in ["KRW=X", "USDKRW=X"]:
        df = fetch_price(fx_ticker, lookback_days, get_cache_buster(), min_rows=60, retries=2)
        if df is not None and not df.empty:
            df = df.copy()
            df["Ticker"] = fx_ticker
            return df
    return None


def add_mas(df: pd.DataFrame, windows=(10, 20, 50)):
    out = df.copy()
    c = out["Close"]
    for w in windows:
        out[f"SMA{w}"] = c.rolling(w).mean()
    return out

def plot_candles(
    df: pd.DataFrame,
    title: str,
    *,
    chart_key: str,          # ✅ Streamlit element key (자리 고정)
    months: int = 3,         # ✅ 기본 3개월
    kind: str = "line",      # "line" | "candle"
    show_ma: bool = False,   # ✅ 기본 MA 숨김
    dark: bool = False,      # ✅ 다크 테마 (시장 차트 등)
):
    if df is None or df.empty:
        st.warning(f"{title}: 데이터 없음")
        return

    # --- 안전장치: 인덱스/컬럼 정리 ---
    d0 = df.copy()

    # 인덱스가 문자열/타임존 섞여도 plotly가 안정적으로 받도록 정리
    try:
        d0.index = pd.to_datetime(d0.index, errors="coerce")
        d0 = d0[~d0.index.isna()]
        d0 = d0.sort_index()
    except Exception:
        pass

    # 거래일 기준 대략 22일/월
    n = max(22 * int(months), 22)
    d = d0.tail(n).copy()

    if d.empty:
        st.warning(f"{title}: 데이터 없음")
        return

    # 필요한 컬럼 체크 (라인은 Close만 있으면 OK)
    has_close = ("Close" in d.columns)
    has_ohlc = all(c in d.columns for c in ("Open", "High", "Low", "Close"))

    if not has_close:
        st.warning(f"{title}: Close 컬럼이 없어 표시할 수 없습니다.")
        return

    # kind가 candle인데 OHLC가 없으면 line으로 fallback
    kind_eff = kind
    if kind_eff == "candle" and not has_ohlc:
        kind_eff = "line"

    fig = go.Figure()

    # --- 본 차트 ---
    if kind_eff == "candle":
        fig.add_trace(go.Candlestick(
            x=d.index,
            open=d["Open"],
            high=d["High"],
            low=d["Low"],
            close=d["Close"],
            name="OHLC"
        ))
    else:
        fig.add_trace(go.Scatter(
            x=d.index,
            y=d["Close"],
            mode="lines",
            name="Close",
        ))

    # --- (선택) 이동평균 ---
    if show_ma:
        try:
            d2 = add_mas(d, windows=(10, 20, 50))
            for w in (10, 20, 50):
                col = f"SMA{w}"
                if col in d2.columns:
                    fig.add_trace(go.Scatter(
                        x=d2.index,
                        y=d2[col],
                        mode="lines",
                        name=col
                    ))
        except Exception:
            # MA 계산 실패해도 차트는 표시되게
            pass

    # --- 레이아웃 ---
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title="Price",
        height=320,
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )

    # candle일 때만 range slider 숨김(라인은 기본 숨김; 필요하면 True로 바꿔도 됨)
    if kind_eff == "candle":
        fig.update_layout(xaxis_rangeslider_visible=False)
    else:
        fig.update_layout(xaxis_rangeslider_visible=False)

    # ✅ key 고정이 핵심 (차트 “밀림/자리바뀜” 방지)
    # 다크 테마 (시장 차트 등 HTML 패널과 통일)
    if dark:
        fig.update_layout(
            paper_bgcolor="rgba(15,23,42,0.95)",
            plot_bgcolor="rgba(15,23,42,0.92)",
            font=dict(color="#e2e8f0", size=12),
            title_font=dict(color="#f1f5f9"),
            xaxis=dict(gridcolor="rgba(100,116,139,0.3)", zerolinecolor="rgba(100,116,139,0.3)"),
            yaxis=dict(gridcolor="rgba(100,116,139,0.3)", zerolinecolor="rgba(100,116,139,0.3)"),
        )
        if kind_eff == "line" and len(fig.data) > 0:
            fig.update_traces(line=dict(color="#6366f1", width=2))

    st.plotly_chart(fig, use_container_width=True, key=chart_key)


def plot_candles_with_signals(
    df: pd.DataFrame,
    title: str,
    buy_dates: list,
    sell_dates: list,
    sell_entry_prices: Optional[list] = None,
    *,
    chart_key: str,
    dark: bool = False,
):
    """최근 1년 캔들 + 매수(초록 삼각형)·매도(빨간 삼각형) 신호 마커. 매도 호버 시 매수 대비 수익률 표시."""
    if df is None or df.empty:
        st.warning(f"{title}: 데이터 없음")
        return
    d0 = df.copy()
    try:
        d0.index = pd.to_datetime(d0.index, errors="coerce")
        d0 = d0[~d0.index.isna()]
        d0 = d0.sort_index()
    except Exception:
        pass
    if not all(c in d0.columns for c in ("Open", "High", "Low", "Close")):
        st.warning(f"{title}: OHLC 컬럼이 없어 캔들을 그릴 수 없습니다.")
        return

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=d0.index,
        open=d0["Open"],
        high=d0["High"],
        low=d0["Low"],
        close=d0["Close"],
        name="OHLC",
        increasing_line_color="#26a69a",
        decreasing_line_color="#ef5350",
        increasing_fillcolor="#26a69a",
        decreasing_fillcolor="#ef5350",
    ))

    idx_min, idx_max = d0.index.min(), d0.index.max()
    def in_range(ts):
        try:
            t = pd.Timestamp(ts)
            return idx_min <= t <= idx_max
        except Exception:
            return False

    buy_in = [ts for ts in buy_dates if in_range(ts)]
    # sell_dates와 sell_entry_prices 순서 동일 유지
    sell_entry = sell_entry_prices if isinstance(sell_entry_prices, list) and len(sell_entry_prices) == len(sell_dates) else []
    sell_in = []
    entry_prices_in = []
    for i, ts in enumerate(sell_dates):
        if in_range(ts):
            sell_in.append(ts)
            entry_prices_in.append(sell_entry[i] if i < len(sell_entry) else None)

    # 다크 테마 시 매수/매도 마커 색상 (가독성)
    buy_marker = dict(symbol="triangle-up", size=26, color="#22c55e", line=dict(width=2, color="#15803d"))
    sell_marker = dict(symbol="triangle-down", size=26, color="#ef4444", line=dict(width=2, color="#b91c1c"))
    if not dark:
        buy_marker = dict(symbol="triangle-up", size=24, color="green", line=dict(width=1.5, color="darkgreen"))
        sell_marker = dict(symbol="triangle-down", size=24, color="red", line=dict(width=1.5, color="darkred"))

    if buy_in:
        buy_ts = pd.to_datetime(buy_in)
        buy_y = []
        for t in buy_ts:
            try:
                if t in d0.index:
                    row = d0.loc[t]
                    lo = float(row["Low"])
                    hi = float(row["High"])
                    buy_y.append(lo - (hi - lo) * 0.05 if hi > lo else lo)
                else:
                    cand = d0[d0.index.normalize() == pd.Timestamp(t).normalize()]
                    if not cand.empty:
                        row = cand.iloc[0]
                        lo = float(row["Low"])
                        hi = float(row["High"])
                        buy_y.append(lo - (hi - lo) * 0.05 if hi > lo else lo)
                    else:
                        buy_y.append(np.nan)
            except Exception:
                buy_y.append(np.nan)
        buy_y = [y if np.isfinite(y) else d0["Low"].min() for y in buy_y]
        fig.add_trace(go.Scatter(
            x=buy_ts,
            y=buy_y,
            mode="markers",
            marker=buy_marker,
            name="매수 신호",
            hovertemplate="%{x|%Y-%m-%d}<br>매수 신호<extra></extra>",
        ))
    if sell_in:
        sell_ts = pd.to_datetime(sell_in)
        sell_y = []
        sell_return_pct = []
        for j, t in enumerate(sell_ts):
            try:
                if t in d0.index:
                    row = d0.loc[t]
                    lo, hi = float(row["Low"]), float(row["High"])
                    close = float(row["Close"])
                    sell_y.append(hi + (hi - lo) * 0.05 if hi > lo else hi)
                else:
                    cand = d0[d0.index.normalize() == pd.Timestamp(t).normalize()]
                    if not cand.empty:
                        row = cand.iloc[0]
                        lo, hi = float(row["Low"]), float(row["High"])
                        close = float(row["Close"])
                        sell_y.append(hi + (hi - lo) * 0.05 if hi > lo else hi)
                    else:
                        sell_y.append(np.nan)
                        close = np.nan
                ep = entry_prices_in[j] if j < len(entry_prices_in) else None
                if ep is not None and float(ep) > 0 and np.isfinite(close):
                    sell_return_pct.append((close / float(ep) - 1) * 100)
                else:
                    sell_return_pct.append(np.nan)
            except Exception:
                sell_y.append(np.nan)
                sell_return_pct.append(np.nan)
        sell_y = [y if np.isfinite(y) else d0["High"].max() for y in sell_y]
        fig.add_trace(go.Scatter(
            x=sell_ts,
            y=sell_y,
            mode="markers",
            marker=sell_marker,
            name="매도 신호",
            customdata=np.array(sell_return_pct),
            hovertemplate="%{x|%Y-%m-%d}<br>매도 신호<br>매수 대비 수익률: %{customdata:.1f}%%<extra></extra>",
        ))

    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title="Price",
        height=800,
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_rangeslider_visible=False,
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(240,240,245,0.8)",
        xaxis=dict(
            rangebreaks=[dict(bounds=["sat", "mon"])],
            showgrid=True,
            gridwidth=1,
            gridcolor="rgba(200,200,210,0.5)",
            tickformat="%Y-%m-%d",
        ),
        yaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor="rgba(200,200,210,0.5)",
            side="right",
        ),
    )
    # 다크 테마 (티커 검색 차트를 HTML 패널 스타일과 통일)
    if dark:
        fig.update_layout(
            paper_bgcolor="rgba(15,23,42,0.95)",
            plot_bgcolor="rgba(15,23,42,0.92)",
            font=dict(color="#e2e8f0", size=12),
            title_font=dict(color="#f1f5f9"),
            xaxis=dict(gridcolor="rgba(100,116,139,0.3)", zerolinecolor="rgba(100,116,139,0.3)"),
            yaxis=dict(gridcolor="rgba(100,116,139,0.3)", zerolinecolor="rgba(100,116,139,0.3)"),
        )
        # 캔들 색상 다크 대비
        fig.update_traces(
            selector=dict(type="candlestick"),
            increasing_line_color="#22c55e",
            decreasing_line_color="#ef4444",
            increasing_fillcolor="#22c55e",
            decreasing_fillcolor="#ef4444",
        )
    st.plotly_chart(fig, use_container_width=True, key=chart_key)


def load_tracker(path=TRACKER_CSV):
    if not os.path.exists(path):
        return pd.DataFrame(columns=[
            "Ticker","SignalDate","EntryDate","EntryPrice",
            "StopPrice",  # ✅ 추가
            "Status","DaysHeld","LastBarDate",
            "ExitDate","ExitPrice","ReturnPct","ExitReason"
        ])

    df = pd.read_csv(path)
    # ✅ (추가) 예전 CSV 호환: StopPrice 없으면 생성
    if "StopPrice" not in df.columns:
        df["StopPrice"] = np.nan
    # 타입 정리
    for c in ["SignalDate","EntryDate","LastBarDate","ExitDate"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    for c in ["EntryPrice","ExitPrice","ReturnPct"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "DaysHeld" in df.columns:
        df["DaysHeld"] = pd.to_numeric(df["DaysHeld"], errors="coerce").fillna(0).astype(int)
    return df

def save_tracker(df: pd.DataFrame, path=TRACKER_CSV):
    df = df.copy()
    df.to_csv(path, index=False, encoding="utf-8-sig")

def _parse_run_date(run_date_like) -> Optional[datetime.date]:
    """snapshot run_date("YYYY-MM-DD") -> date"""
    try:
        if run_date_like is None:
            return None
        return pd.to_datetime(str(run_date_like)).date()
    except Exception:
        return None

def _close_on_date(ticker: str, target_date) -> tuple:
    """target_date( date 또는 str YYYY-MM-DD )에 해당하는 종가 반환. (close_price, target_date) 또는 (None, None)."""
    try:
        d = _parse_run_date(target_date) if not isinstance(target_date, date) else target_date
        if d is None:
            return None, None
        df = fetch_price(ticker, lookback_days=400, cache_buster=get_cache_buster())
        if df is None or df.empty:
            return None, None
        df = df.copy()
        df.index = pd.to_datetime(df.index)
        row = df[df.index.normalize() == pd.Timestamp(d)]
        if row.empty:
            return None, None
        close = float(row["Close"].iloc[0])
        return close, d
    except Exception:
        return None, None


def seed_tracker_from_recent_snapshots(
    *,
    max_files: int = 60,
    max_seed: int = 3,
) -> list[str]:
    """
    ✅ CSV가 없거나/비었을 때:
    - 최근 스냅샷들에서 BUY_BREAKOUT/BUY_PULLBACK 티커를 모아서
      Promoted 제외 후, 최대 max_seed개를 tracker(OPEN)로 '재시드'한다.
    - 반환: 실제로 seed된 티커 리스트
    """
    files = sorted(glob.glob(SNAPSHOT_PATTERN))[-max_files:]
    if not files:
        return []

    # 1) 최근 스냅샷에서 후보 수집 (최신 -> 과거 순)
    candidates: list = []  # list of (Optional[date], ticker_str)
    seen = set()

    for p in reversed(files):
        try:
            snap = load_scan_snapshot(p)
            run_date = snap.get("run_date")
            d = _parse_run_date(run_date)

            top = snap.get("top_picks")
            top = top if isinstance(top, pd.DataFrame) else pd.DataFrame(top)
            if top is None or top.empty or "Ticker" not in top.columns or "Entry" not in top.columns:
                continue

            df = top.copy()
            df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
            df["Entry"] = df["Entry"].astype(str)

            # BUY만
            df = df[df["Entry"].isin(["BUY_BREAKOUT", "BUY_PULLBACK"])].copy()
            if df.empty:
                continue

            # Promoted 제외(가능한 모든 케이스 방어)
            if "Promoted" in df.columns:
                df = df[~df["Promoted"].fillna(False).astype(bool)].copy()

            if "PromoTag" in df.columns:
                df = df[~df["PromoTag"].astype(str).str.contains("PROMOTED|BUY_PROMOTED", case=False, na=False)].copy()

            for col in ["Tag", "Note", "Reasons", "EntryHint"]:
                if col in df.columns:
                    df = df[~df[col].astype(str).str.contains("PROMOTED|BUY_PROMOTED", case=False, na=False)].copy()

            if df.empty:
                continue

            for t in df["Ticker"].tolist():
                if not t or t in seen:
                    continue
                candidates.append((d, t))
                seen.add(t)

                if len(candidates) >= max_seed:
                    break

            if len(candidates) >= max_seed:
                break

        except Exception:
            continue

    if not candidates:
        return []

    # 2) tracker 로드(없으면 빈 DF)
    tr = load_tracker()
    if tr is None or tr.empty:
        tr = pd.DataFrame(columns=[
            "Ticker","SignalDate","EntryDate","EntryPrice",
            "StopPrice",
            "Status","DaysHeld","LastBarDate",
            "ExitDate","ExitPrice","ReturnPct","ExitReason"
        ])
    else:
        # 컬럼 보강
        for c in [
            "Ticker","SignalDate","EntryDate","EntryPrice","StopPrice",
            "Status","DaysHeld","LastBarDate","ExitDate","ExitPrice","ReturnPct","ExitReason"
        ]:
            if c not in tr.columns:
                tr[c] = np.nan

    # 3) 후보를 OPEN으로 seed
    seeded = []
    for d, t in candidates:
        # 이미 OPEN이면 스킵
        already_open = (not tr.empty) and (
            (tr["Ticker"].astype(str).str.upper() == t) &
            (tr["Status"].astype(str) == "OPEN")
        ).any()
        if already_open:
            continue

        # snapshot 날짜 d가 있으면 해당일 종가로 Entry 설정; 없으면 신호날 종가 규칙 (EntryDate=SignalDate)
        if d is not None:
            entry_price, entry_date_px = _close_on_date(t, d)
            signal_date = d
            if entry_price is None or entry_date_px is None:
                continue
        else:
            entry_price, signal_date_px = _entry_price_on_signal_date(t)
            if entry_price is None or signal_date_px is None:
                continue
            signal_date = signal_date_px
            entry_date_px = signal_date_px  # EntryDate = SignalDate

        # StopPrice는 가능하면 계산
        stop_price = np.nan
        try:
            df_for_stop = fetch_price(t, lookback_days=240, cache_buster=get_cache_buster())
            if df_for_stop is not None and not df_for_stop.empty and len(df_for_stop) >= 140:
                df2_for_stop = build_df2(df_for_stop)
                if df2_for_stop is not None and not df2_for_stop.empty and len(df2_for_stop) >= 140:
                    entry_sig, *_ = sc.decide_entry(df2_for_stop)
                    if str(entry_sig).startswith("BUY_"):
                        plan = sc.calc_trade_plan(df2_for_stop, entry_sig)
                        if plan and plan.get("StopPrice") is not None:
                            stop_price = float(plan["StopPrice"])
        except Exception:
            stop_price = np.nan

        tr = pd.concat([tr, pd.DataFrame([{
            "Ticker": t,
            "SignalDate": signal_date if not isinstance(signal_date, date) else pd.Timestamp(signal_date),
            "EntryDate": entry_date_px if not isinstance(entry_date_px, date) else pd.Timestamp(entry_date_px),
            "EntryPrice": float(entry_price),
            "StopPrice": stop_price,
            "Status": "OPEN",
            "DaysHeld": 0,
            "LastBarDate": pd.NaT,
            "ExitDate": pd.NaT,
            "ExitPrice": np.nan,
            "ReturnPct": np.nan,
            "ExitReason": ""
        }])], ignore_index=True)

        seeded.append(t)

    save_tracker(tr)
    return seeded

def _entry_price_on_signal_date(ticker: str):
    """
    EntryDate = SignalDate, EntryPrice = SignalDate의 종가 (신호날 종가)
    """
    df = fetch_price(ticker, lookback_days=240, cache_buster=get_cache_buster())
    if df is None or df.empty or len(df) < 2:
        return None, None

    signal_date = _ensure_dt(df.index[-1])
    entry_price = float(df["Close"].iloc[-1])  # 신호날 종가
    return entry_price, signal_date



def _current_close(ticker: str):
    df = fetch_price(ticker, lookback_days=120, cache_buster=get_cache_buster())  # ✅ 60 -> 120(여유)
    if df is None or df.empty or len(df) < 2:
        return None, None

    return float(df["Close"].iloc[-1]), _ensure_dt(df.index[-1])


def _exit_signal_from_scanner(ticker: str, shares: float = 1.0, avg_price: float = 1.0, days_held=None, max_hold_days=None):
    """
    TOP PICK3 BUY 성과 추적용 exit 시그널:
      - holding_risk_review가 SELL_TRAIL / SELL_TREND / TAKE_PROFIT 이면 exit
      - days_held/max_hold_days 넘기면 2번(만료 근접 시 트레일 강화·컨펌 완화) 적용
    """
    df = fetch_price(ticker, lookback_days=240, cache_buster=get_cache_buster())
    if df is None or df.empty or len(df) < 140:
        return None, None

    df2 = build_df2(df)
    if df2 is None or df2.empty or len(df2) < 140:
        return None, None

    r = sc.holding_risk_review(
        df2, ticker, shares, avg_price,
        days_held=days_held, max_hold_days=max_hold_days,
        apply_near_expiry=True,
    )
    action = r.get("Action")
    reason = r.get("Reason")
    if action in ("SELL_TRAIL", "SELL_TREND", "SELL_STRUCTURE_BREAK", "SELL_LOSS_CUT", "TAKE_PROFIT"):
        return action, reason
    return None, None

from typing import List
from typing import List

def _recent_top3_buy_universe(max_files: int = 30) -> set[str]:
    """
    최근 스냅샷들에서 'TOP3 중 BUY_* & Promoted!=True' 티커를 모아서 유니버스를 만든다.
    - 목적: tracker.csv에 남아있는 엉뚱한 OPEN(예: promoted/오염된 종목)을 자동으로 정리
    """
    try:
        files = sorted(glob.glob(SNAPSHOT_PATTERN))[-max_files:]
        uni: set[str] = set()
        for p in files:
            try:
                snap = load_scan_snapshot(p)
                top = snap.get("top_picks")
                top = top if isinstance(top, pd.DataFrame) else pd.DataFrame(top)
                if top is None or top.empty:
                    continue

                # BUY만
                if "Entry" not in top.columns:
                    continue
                buy = top[top["Entry"].astype(str).isin(["BUY_BREAKOUT", "BUY_PULLBACK"])].copy()

                # Promoted 제외 (컬럼이 있으면)
                if "Promoted" in buy.columns:
                    buy = buy[buy["Promoted"].astype(bool) == False]

                # 티커 수집
                if "Ticker" in buy.columns:
                    for t in buy["Ticker"].astype(str).str.upper().str.strip().tolist():
                        if t:
                            uni.add(t)
            except Exception:
                continue
        return uni
    except Exception:
        return set()


def update_tracker_with_today(top3_buy_tickers: List[str], max_hold_days: int = 15, run_date=None):
    """
    ✅ 안정 버전(요청사항 반영)
    - TOP PICK3 중 BUY(BUY_BREAKOUT/BUY_PULLBACK)만 tracker에 신규 편입
    - run_date가 있으면 해당일 종가로 Entry 가격·날짜 설정(_close_on_date)
    - PROMOTED 종목(SEE/SO 같은) 자동 제거 (OPEN에서만 제거)
    - 기존에 추적 중이던 정상 BUY 종목(과거 OPEN)은 절대 '유니버스 밖' 이유로 삭제하지 않음  ← 핵심
    - 신규 편입 종목은 당일/첫날엔 exit 판정 금지(바로 CLOSED 방지)
    """
    tr = load_tracker()

    # 기본 컬럼 보강(구버전 CSV/빈 DF 방어)
    if tr is None or tr.empty:
        tr = pd.DataFrame(columns=[
            "Ticker","SignalDate","EntryDate","EntryPrice",
            "StopPrice",
            "Status","DaysHeld","LastBarDate",
            "ExitDate","ExitPrice","ReturnPct","ExitReason"
        ])
    else:
        for c in [
            "Ticker","SignalDate","EntryDate","EntryPrice","StopPrice",
            "Status","DaysHeld","LastBarDate","ExitDate","ExitPrice","ReturnPct","ExitReason"
        ]:
            if c not in tr.columns:
                tr[c] = np.nan

    # -------------------------
    # (A) OPEN 포지션 정리(prune) - "PROMOTED만" 제거
    #    (유니버스 밖이라고 지우면 예전 정상 BUY가 다 날아가서 금지)
    # -------------------------
    def _recent_promoted_tickers(max_files: int = 60) -> set[str]:
        promo = set()
        files = sorted(glob.glob(SNAPSHOT_PATTERN))[-max_files:]
        for p in files:
            try:
                snap = load_scan_snapshot(p)
                top = snap.get("top_picks")
                top = top if isinstance(top, pd.DataFrame) else pd.DataFrame(top)
                if top is None or top.empty or "Ticker" not in top.columns:
                    continue

                top2 = top.copy()
                top2["Ticker"] = top2["Ticker"].astype(str).str.upper().str.strip()

                # 1) Promoted 불리언
                if "Promoted" in top2.columns:
                    m = top2["Promoted"].fillna(False).astype(bool)
                    promo |= set(top2.loc[m, "Ticker"].tolist())

                # 2) 문자열 태그류(환경별 컬럼명 섞임 방어)
                for col in ["PromoTag", "Tag", "Note", "Reasons", "EntryHint"]:
                    if col in top2.columns:
                        m = top2[col].astype(str).str.contains("PROMOTED|BUY_PROMOTED", case=False, na=False)
                        promo |= set(top2.loc[m, "Ticker"].tolist())

            except Exception:
                continue
        return promo

    promoted_set = _recent_promoted_tickers(max_files=60)

    if not tr.empty and "Status" in tr.columns and "Ticker" in tr.columns and promoted_set:
        open_mask = (tr["Status"].astype(str) == "OPEN")
        if open_mask.any():
            drop_idx = tr.loc[
                open_mask & tr["Ticker"].astype(str).str.upper().isin(promoted_set)
            ].index
            if len(drop_idx) > 0:
                tr = tr.drop(index=drop_idx).reset_index(drop=True)

    # -------------------------
    # (B) 오늘 TOP3 BUY 티커 신규 등록 (OPEN)
    # -------------------------
    for t in (top3_buy_tickers or []):
        t = str(t).upper().strip()
        if not t:
            continue

        # ✅ 혹시 promoted_set에 걸리면 신규 편입도 차단
        if promoted_set and t in promoted_set:
            continue

        already_open = (not tr.empty) and (
            (tr["Ticker"].astype(str).str.upper() == t) &
            (tr["Status"].astype(str) == "OPEN")
        ).any()
        if already_open:
            continue

        if run_date is not None:
            entry_price, entry_date = _close_on_date(t, run_date)
            signal_date = pd.Timestamp(run_date) if entry_date else None
            if entry_price is None or entry_date is None:
                continue
        else:
            entry_price, signal_date = _entry_price_on_signal_date(t)
            if entry_price is None or signal_date is None:
                continue
            entry_date = signal_date  # EntryDate = SignalDate

        # 같은 signal_date 중복 방지
        dup_mask = (
            (tr["Ticker"].astype(str).str.upper() == t) &
            (tr["SignalDate"] == signal_date)
        ) if (not tr.empty and "SignalDate" in tr.columns) else None
        if dup_mask is not None and dup_mask.any():
            continue

        # StopPrice 계산(가능하면)
        stop_price = np.nan
        try:
            df_for_stop = fetch_price(t, lookback_days=240, cache_buster=get_cache_buster())
            if df_for_stop is not None and (not df_for_stop.empty) and len(df_for_stop) >= 140:
                df2_for_stop = build_df2(df_for_stop)
                if df2_for_stop is not None and (not df2_for_stop.empty) and len(df2_for_stop) >= 140:
                    entry_sig, *_ = sc.decide_entry(df2_for_stop)
                    if str(entry_sig).startswith("BUY_"):
                        plan = sc.calc_trade_plan(df2_for_stop, entry_sig)
                        if plan and plan.get("StopPrice") is not None:
                            stop_price = float(plan["StopPrice"])
        except Exception:
            stop_price = np.nan

        tr = pd.concat([tr, pd.DataFrame([{
            "Ticker": t,
            "SignalDate": signal_date if not isinstance(signal_date, date) else pd.Timestamp(signal_date),
            "EntryDate": entry_date if not isinstance(entry_date, date) else pd.Timestamp(entry_date),
            "EntryPrice": float(entry_price),
            "StopPrice": stop_price,
            "Status": "OPEN",
            "DaysHeld": 0,
            "LastBarDate": pd.NaT,
            "ExitDate": pd.NaT,
            "ExitPrice": np.nan,
            "ReturnPct": np.nan,
            "ExitReason": ""
        }])], ignore_index=True)

    # -------------------------
    # (C) OPEN 업데이트 & 종료 판단
    # -------------------------
    if tr.empty:
        save_tracker(tr)
        return tr, pd.DataFrame()

    # 타입 안정화
    tr["Ticker"] = tr["Ticker"].astype(str).str.upper().str.strip()
    tr["Status"] = tr["Status"].astype(str)

    # OPEN인데 SignalDate/EntryDate 비어 있으면 가격 데이터로 보정
    for idx, row in tr[tr["Status"] == "OPEN"].iterrows():
        t = str(row.get("Ticker", "")).upper().strip()
        if not t:
            continue
        sd = _ensure_dt(row.get("SignalDate"))
        ed = _ensure_dt(row.get("EntryDate"))
        if sd is not None and ed is not None:
            continue
        _, signal_date = _entry_price_on_signal_date(t)
        if signal_date is not None:
            tr.loc[idx, "SignalDate"] = pd.Timestamp(signal_date) if isinstance(signal_date, date) else signal_date
            tr.loc[idx, "EntryDate"] = tr.loc[idx, "SignalDate"]

    open_df = tr[tr["Status"] == "OPEN"].copy()
    closed_today = []

    for idx, row in open_df.iterrows():
        t = str(row.get("Ticker", "")).upper().strip()
        if not t:
            continue

        entry_price = row.get("EntryPrice", None)
        try:
            if entry_price is None or not np.isfinite(float(entry_price)) or float(entry_price) <= 0:
                continue
            entry_price = float(entry_price)
        except Exception:
            continue

        cur_close, cur_date = _current_close(t)
        if cur_close is None or cur_date is None:
            continue
        cur_close = float(cur_close)

        # DaysHeld 업데이트 (봉 날짜가 바뀔 때만 +1)
        prev_last_bar = _ensure_dt(row.get("LastBarDate", None))
        base = int(row.get("DaysHeld", 0) or 0)

        if prev_last_bar is None:
            days_held = 1 if base <= 0 else base
        else:
            if pd.isna(prev_last_bar):
                prev_last_bar = cur_date - timedelta(days=1)
            days_held = base + 1 if cur_date > prev_last_bar else base

        tr.loc[idx, "LastBarDate"] = cur_date
        tr.loc[idx, "DaysHeld"] = days_held

        # 수익률(표시/저장용)
        ret_pct = (cur_close / entry_price - 1) * 100.0

        # ✅ 핵심: "당일/첫날엔 exit 판정 금지"
        signal_date = _ensure_dt(row.get("SignalDate"))
        if signal_date is not None and pd.notna(signal_date) and cur_date <= signal_date:
            # 신호 발생 당일이면 무조건 OPEN 유지
            continue
        if days_held < 2:
            # 최소 2일차부터만 exit 로직 적용
            continue

        exit_reason = None

        # (1) 15거래일 도달
        if days_held >= max_hold_days:
            exit_reason = "TIME_EXIT(15D)"

        # (2) 손절: 저장된 StopPrice 우선
        if exit_reason is None:
            saved_stop = row.get("StopPrice", np.nan)
            try:
                if saved_stop is not None and np.isfinite(float(saved_stop)):
                    if cur_close < float(saved_stop):
                        exit_reason = f"STOP_LOSS(<{float(saved_stop):.2f})"
            except Exception:
                pass

        # (3) 매도/익절 시그널(holding_risk_review 기반). TOP PICK3만 2번(만료근접) 적용
        if exit_reason is None:
            action, _ = _exit_signal_from_scanner(t, shares=1.0, avg_price=entry_price, days_held=days_held, max_hold_days=max_hold_days)
            if action in ("SELL_TRAIL", "SELL_TREND", "SELL_STRUCTURE_BREAK", "SELL_LOSS_CUT", "TAKE_PROFIT"):
                exit_reason = action

        if exit_reason is not None:
            tr.loc[idx, "Status"] = "CLOSED"
            tr.loc[idx, "ExitDate"] = cur_date
            tr.loc[idx, "ExitPrice"] = float(cur_close)
            tr.loc[idx, "ReturnPct"] = float(ret_pct)
            tr.loc[idx, "ExitReason"] = exit_reason
            closed_today.append(tr.loc[idx].to_dict())

    save_tracker(tr)
    closed_df = pd.DataFrame(closed_today) if closed_today else pd.DataFrame()
    return tr, closed_df



def compute_cum_returns(tr: pd.DataFrame, today: datetime.date):
    """
    ✅ CLOSED 확정 수익률을 ReturnPct 컬럼을 그대로 믿지 말고,
       EntryPrice / ExitPrice로 재계산해서 사용.
    ✅ 말도 안되는 레코드(예: EntryPrice<=0, ExitPrice<=0, 1회 트레이드 +400% 등)는 제외.
    """
    if tr is None or tr.empty:
        return {"daily": 0.0, "monthly": 0.0, "yearly": 0.0, "total": 0.0}

    if "Status" not in tr.columns:
        return {"daily": 0.0, "monthly": 0.0, "yearly": 0.0, "total": 0.0}

    c = tr[tr["Status"] == "CLOSED"].copy()
    if c.empty:
        return {"daily": 0.0, "monthly": 0.0, "yearly": 0.0, "total": 0.0}

    # 필수 컬럼
    need = ["ExitDate", "EntryPrice", "ExitPrice"]
    for col in need:
        if col not in c.columns:
            return {"daily": 0.0, "monthly": 0.0, "yearly": 0.0, "total": 0.0}

    c["ExitDate"] = pd.to_datetime(c["ExitDate"], errors="coerce").dt.date
    c["EntryPrice"] = pd.to_numeric(c["EntryPrice"], errors="coerce")
    c["ExitPrice"]  = pd.to_numeric(c["ExitPrice"],  errors="coerce")

    c = c.dropna(subset=["ExitDate", "EntryPrice", "ExitPrice"]).copy()
    c = c[(c["EntryPrice"] > 0) & (c["ExitPrice"] > 0)].copy()

    # ✅ ReturnPct 재계산
    c["ReturnPctCalc"] = (c["ExitPrice"] / c["EntryPrice"] - 1.0) * 100.0

    # ✅ 이상치 제거: 한 번 트레이드가 +445% 이런 건 CSV 오염/가격스케일 꼬임 가능성 매우 큼
    # (너 스캐너가 원래 스윙용이면 1회 트레이드 +200%도 거의 비정상이라 봐도 됨)
    c = c[(c["ReturnPctCalc"] >= -80.0) & (c["ReturnPctCalc"] <= 200.0)].copy()

    def _compound(df: pd.DataFrame) -> float:
        if df is None or df.empty:
            return 0.0
        r = df["ReturnPctCalc"].astype(float).values
        factors = 1.0 + (r / 100.0)
        factors = np.clip(factors, 0.0001, 1000.0)
        return (float(np.prod(factors)) - 1.0) * 100.0

    def _window(days: int) -> float:
        cutoff = today - timedelta(days=days)
        w = c[c["ExitDate"].apply(lambda d: (d is not None) and (d >= cutoff))].copy()
        return _compound(w)

    return {
        "daily": _window(1),
        "monthly": _window(30),
        "yearly": _window(365),
        "total": _compound(c),
    }



def compute_open_avg_return(tr: pd.DataFrame) -> float:
    """
    ✅ OPEN(표에 있는) 종목들의 '현재 Return%' 평균
    - EntryPrice(PrevClose) 대비 현재 종가 기준
    - 데이터 못 가져오는 종목은 제외
    """
    if tr is None or not isinstance(tr, pd.DataFrame) or tr.empty:
        return 0.0

    # 컬럼 방어
    if "Status" not in tr.columns or "Ticker" not in tr.columns or "EntryPrice" not in tr.columns:
        return 0.0

    open_df = tr[tr["Status"] == "OPEN"].copy()
    if open_df.empty:
        return 0.0

    rets = []
    for _, r in open_df.iterrows():
        t = str(r.get("Ticker", "")).upper().strip()
        entry = r.get("EntryPrice", None)

        try:
            if not t or entry is None or not np.isfinite(float(entry)) or float(entry) <= 0:
                continue
        except Exception:
            continue

        cur_close, _ = _current_close(t)
        if cur_close is None:
            continue

        ret = (float(cur_close) / float(entry) - 1) * 100.0
        rets.append(ret)

    if not rets:
        return 0.0

    return float(np.mean(rets))


def _bar10(pct_0_100: float):
    v = 0 if pct_0_100 is None else float(pct_0_100)
    v = max(0.0, min(100.0, v))
    blocks = int(round(v / 10))
    return "█" * blocks + "░" * (10 - blocks)

def risk_meter_text(rsi: Optional[float], atrp: Optional[float]):
    # RSI 상태
    if rsi is None or not np.isfinite(rsi):
        rsi_line = "RSI " + _bar10(0) + " ⚪ N/A"
    else:
        if rsi < 40:
            tag = "🟠 Cold"
        elif rsi < 70:
            tag = "🟢 Healthy"
        elif rsi < 80:
            tag = "🟡 Hot"
        else:
            tag = "🔴 Overheat"
        rsi_line = f"RSI {_bar10(rsi)} {tag} ({rsi:.1f})"

    # ATR% 상태 (너 기존 출력 톤 유지: 낮으면 🟠 Low)
    if atrp is None or not np.isfinite(atrp):
        atr_line = "ATR% " + _bar10(0) + " ⚪ N/A"
    else:
        # atr%는 보통 0~10 안쪽이 많으니 보기 좋게 0~10 => 0~100으로 스케일
        atr_scaled = max(0.0, min(100.0, atrp * 10))
        if atrp < 2:
            tag = "🟠 Low"
        elif atrp < 6:
            tag = "🟢 Normal"
        else:
            tag = "🔴 High"
        atr_line = f"ATR% {_bar10(atr_scaled)} {tag} ({atrp:.2f})"

    return rsi_line, atr_line

def _round_up_to_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.ceil(x / step) * step

def _tp_step_by_price(price: float) -> float:
    if price >= 500:
        return 50
    if price >= 200:
        return 25
    if price >= 100:
        return 10
    if price >= 50:
        return 5
    return 1

def compute_tp_levels_from_df2(df2: pd.DataFrame, boost: bool = False, base_price: Optional[float] = None):
    """
    scanner.py build_partial_tp_plan()과 같은 로직을 숫자(t1/t2/t3)로 반환.
    base_price: None이면 현재가(close) 기준, 주면 해당 가격 기준(예: 매수가)으로 목표가 계산.
    반환: (t1, t2, t3, close) 또는 (None, None, None, close)
    """
    if df2 is None or df2.empty:
        return None, None, None, None

    last = df2.iloc[-1]
    close = float(last["Close"]) if "Close" in last else None
    atr14 = float(last["ATR14"]) if "ATR14" in last else None
    if close is None or atr14 is None or (not np.isfinite(close)) or (not np.isfinite(atr14)) or atr14 <= 0 or close <= 0:
        return None, None, None, close

    base = float(base_price) if (base_price is not None and np.isfinite(base_price) and float(base_price) > 0) else close
    step = _tp_step_by_price(base)

    # cfg 설정값(없으면 기본)
    base_m1 = float(getattr(cfg, "TP_ATR_M1", 1.0))
    base_m2 = float(getattr(cfg, "TP_ATR_M2", 2.0))
    base_m3 = float(getattr(cfg, "TP_ATR_M3", 3.0))

    boost_m1 = float(getattr(cfg, "TP_ATR_BOOST_M1", 1.3))
    boost_m2 = float(getattr(cfg, "TP_ATR_BOOST_M2", 2.6))
    boost_m3 = float(getattr(cfg, "TP_ATR_BOOST_M3", 4.0))

    if boost:
        m1, m2, m3 = boost_m1, boost_m2, boost_m3
    else:
        m1, m2, m3 = base_m1, base_m2, base_m3

    # ATR 목표가(원값) — base(현재가 또는 매수가) 기준
    t1_raw = base + atr14 * m1
    t2_raw = base + atr14 * m2
    t3_raw = base + atr14 * m3

    # High60 캡 + ATR 바닥(최소 보장) — scanner.py와 동일 컨셉
    use_cap = bool(getattr(cfg, "TP_USE_HIGH60_CAP", True))

    high60 = np.nan
    try:
        if "High" in df2.columns and len(df2) >= 60:
            high60 = float(df2["High"].tail(60).max())
    except Exception:
        high60 = np.nan

    floor_m2 = float(getattr(cfg, "TP_FLOOR_ATR_M2", 3.0))
    floor_m3 = float(getattr(cfg, "TP_FLOOR_ATR_M3", 4.5))
    t2_floor = base + atr14 * floor_m2
    t3_floor = base + atr14 * floor_m3

    cap2_mult = float(getattr(cfg, "TP_CAP_H60_MULT_2", 1.02))
    cap3_mult = float(getattr(cfg, "TP_CAP_H60_MULT_3", 1.05))

    if use_cap and np.isfinite(high60) and high60 > 0:
        if bool(getattr(cfg, "TP_CAP_DISABLE_ON_BREAKOUT", True)):
            buf = float(getattr(cfg, "TP_CAP_DISABLE_BUFFER", 0.002))
            if close >= high60 * (1 + buf):
                use_cap = False

        if use_cap:
            cap2 = high60 * cap2_mult
            cap3 = high60 * cap3_mult
            t2_raw = min(t2_raw, cap2)
            t3_raw = min(t3_raw, cap3)

    t2_raw = max(t2_raw, t2_floor)
    t3_raw = max(t3_raw, t3_floor)

    # 라운딩 + 단조 증가 보장
    t1 = _round_up_to_step(t1_raw, step)
    t2 = _round_up_to_step(t2_raw, step)
    t3 = _round_up_to_step(t3_raw, step)

    if t2 <= t1:
        t2 = t1 + step
    if t3 <= t2:
        t3 = t2 + step

    return float(t1), float(t2), float(t3), float(close)


def pick_top3(df_all: pd.DataFrame, buy_df: Optional[pd.DataFrame] = None, watch_df: Optional[pd.DataFrame] = None, n: int = 3):
    """
    scanner.py 정렬/우선순위와 최대한 동일하게 TOP3 선정:
      - Entry 우선순위(P) 먼저
      - 그 다음 RR > Score > 유동성
    """
    def _safe_num(x, default=np.nan):
        try:
            return float(x)
        except Exception:
            return default

    df_all = df_all.copy() if isinstance(df_all, pd.DataFrame) else pd.DataFrame()
    if df_all.empty:
        return pd.DataFrame()

    priority = {
        "BUY_BREAKOUT": 0,
        "BUY_PULLBACK": 1,
        "BUY_SMART": 2,
        "WATCH_BREAKOUT": 3,
        "WATCH_PULLBACK": 4,
        "CANDIDATE_BUY": 5,
        "SKIP": 9,
    }

    # TOP3 풀: df_all 전체에서 "우선순위 정렬" 후 상위 n개
    pool = df_all.copy()
    pool["P"] = pool["Entry"].map(priority).fillna(9).astype(int)

    # 정렬키 준비
    for col in ["RR", "Score", "Avg$Vol"]:
        if col not in pool.columns:
            pool[col] = np.nan

    pool["RR_num"] = pool["RR"].apply(_safe_num).fillna(-1.0)
    pool["Score_num"] = pool["Score"].apply(_safe_num).fillna(-1.0)
    pool["Vol_num"] = pool["Avg$Vol"].apply(_safe_num).fillna(-1.0)

    pool = pool.sort_values(
        ["P", "RR_num", "Score_num", "Vol_num"],
        ascending=[True, False, False, False]
    )

    # 중복 티커 방지 + 상위 n개
    picks = []
    used = set()
    for _, r in pool.iterrows():
        t = str(r.get("Ticker", "")).upper().strip()
        if not t or t in used:
            continue
        picks.append(r)
        used.add(t)
        if len(picks) >= n:
            break

    return pd.DataFrame(picks).reset_index(drop=True) if picks else pd.DataFrame()


def render_ticker_card(row: pd.Series, rank: int, run_date: str):
    t = row.get("Ticker", "-")
    sec = row.get("Sector", "Unknown")
    entry = row.get("Entry", "-")
    close = row.get("Close", None)
    score = row.get("Score", None)
    vol = row.get("VolRatio", None)
    rsi = row.get("RSI", None)
    atrp = row.get("ATR%", None)
    ev = row.get("EV", None)
    prob = row.get("Prob", None)


    entry_p = row.get("EntryPrice", None)
    stop_p  = row.get("StopPrice", None)
    targ_p  = row.get("TargetPrice", None)
    rr      = row.get("RR", None)
    sh      = row.get("Shares", None)
    pv      = row.get("PosValue", None)

    trig = row.get("Trigger", "")
    reasons = row.get("Reasons", "")
    note = row.get("Note", "")

    promo = row.get("PromoTag", "")
    if promo and isinstance(promo, str):
        promo_txt = f" {promo}"
    else:
        promo_txt = " 🟣✅ PROMOTED" if bool(row.get("Promoted", False)) else ""

    with st.expander(f"🧠 TICKER CARD — {t} — #{rank}  {entry}{promo_txt}", expanded=(rank == 1)):
        st.write(f"**{t}** ({sec})  | Close **{close}** | EV **{ev}** | Prob **{prob}** | Score **{score}**")
        st.write(f"Vol **{vol}x** | RSI **{rsi}** | ATR% **{atrp}**")

        if pd.notna(entry_p) and pd.notna(stop_p) and pd.notna(targ_p):
            st.write(f"Entry **{entry_p}** | Stop **{stop_p}** | Target **{targ_p}** | RR **{rr}**")
            if pd.notna(sh) and pd.notna(pv):
                st.write(f"Size **{int(sh)} sh** (~${pv})")
        else:
            st.write("Entry/Stop/Target: - (WATCH 또는 플랜 없음)")

        rsi_line, atr_line = risk_meter_text(
            float(rsi) if pd.notna(rsi) else None,
            float(atrp) if pd.notna(atrp) else None
        )
        st.code(rsi_line + "\n" + atr_line)

        if trig: st.write(f"Trigger: {trig}")
        if reasons: st.write(f"Reasons: {reasons}")
        if note: st.write(f"Note: {note}")


def build_df2(df: pd.DataFrame):
    # scanner.py에서 쓰는 지표 세팅과 동일
    close = df["Close"]
    df["SMA20"] = sc.sma(close, 20)
    df["SMA50"] = sc.sma(close, 50)
    df["SMA200"] = sc.sma(close, 200)
    df["ATR14"] = sc.atr(df, 14)
    df["MACD_H"] = sc.macd_hist(close)
    df["RSI14"] = sc.rsi(close, 14)
    df2 = df.dropna().copy()
    return df2

def update_top3_buy_tracker(top3: pd.DataFrame, run_date: str, cache_buster: str):
    """
    - TOP3 중 BUY 신호(BUY_* 또는 BUY_SMART)만 tracker에 등록
    - tracker는 st.session_state["tp3_tracker"]에 누적 저장
    - 오늘 신호도 즉시 포함되게 run_date를 그대로 StartDate로 기록
    """
    if top3 is None or not isinstance(top3, pd.DataFrame) or top3.empty:
        return

    # 세션에 tracker 없으면 생성
    if "tp3_tracker" not in st.session_state or not isinstance(st.session_state["tp3_tracker"], pd.DataFrame):
        st.session_state["tp3_tracker"] = pd.DataFrame(
            columns=["Ticker", "StartDate", "EntryType", "AvgPrice", "StartClose"]
        )

    tracker = st.session_state["tp3_tracker"].copy()

    # BUY 신호 필터
    def _is_buy(entry: str) -> bool:
        e = str(entry)
        return e in ("BUY_BREAKOUT", "BUY_PULLBACK")

    buy_rows = top3[top3["Entry"].apply(_is_buy)].copy() if "Entry" in top3.columns else pd.DataFrame()
    if buy_rows.empty:
        st.session_state["tp3_tracker"] = tracker
        return

    # 오늘도 즉시 포함되게: start close/avgprice 계산
    for _, r in buy_rows.iterrows():
        t = str(r.get("Ticker", "")).upper().strip()
        if not t:
            continue

        # 이미 추적중이면 skip
        if not tracker.empty and t in tracker["Ticker"].astype(str).str.upper().values:
            continue

        # "매수 신호 다음날"이 아니라, **오늘 신호가 뜬 날** 기준으로
        # AvgPrice는 “전일 종가”가 규칙이라 했으니, 전일 종가를 가져온다.
        df = fetch_price(t, cfg.LOOKBACK_DAYS, cache_buster)
        if df is None or df.empty or len(df) < 2:
            continue

        prev_close = float(df["Close"].iloc[-2])  # 전일 종가
        last_close = float(df["Close"].iloc[-1])  # 오늘 종가

        tracker = pd.concat([tracker, pd.DataFrame([{
            "Ticker": t,
            "StartDate": run_date,
            "EntryType": r.get("Entry", ""),
            "AvgPrice": prev_close,
            "StartClose": last_close,
        }])], ignore_index=True)

    st.session_state["tp3_tracker"] = tracker



def load_scan_snapshot_only(snapshot_path: Optional[str] = None) -> dict:
    """
    ✅ scanner.py 결과와 100% 동일:
    - app.py에서 재계산 금지
    - scan_snapshot_YYYY-MM-DD.json만 로드해서 사용
    """
    if snapshot_path is None:
        snapshot_path = get_latest_snapshot()

    if not snapshot_path or not os.path.exists(snapshot_path):
        return {
            "error": "snapshot_not_found",
            "snapshot_path": snapshot_path,
        }

    snap = load_scan_snapshot(snapshot_path)
    snap["snapshot_path"] = snapshot_path
    # ✅ run_date가 없으면 파일명에서 만든다
    if "run_date" not in snap or not snap.get("run_date"):
        snap["run_date"] = os.path.basename(snapshot_path).replace("scan_snapshot_", "").replace(".json", "")
    return snap



def analyze_ticker_reco(ticker: str, shares: float = 1.0, avg_price: Optional[float] = None, entry_date=None):
    """
    ✅ 티커 분석(첫 클릭부터 안정):
    - yfinance가 첫 호출에 데이터가 짧게 내려오는/빈 값이 오는 케이스 방어
    - SMA200 포함하려면 최소 260봉 이상 필요(네 기준 유지)
    - entry_date 있으면 보유 기간 경고용 days_held 계산 후 holding_risk_review에 전달
    """
    ticker = str(ticker).upper().strip()

    # 1) lookback 넉넉히(900일) 해서 1회 fetch로 260봉 확보 목표 (재시도 최소화)
    lookback = int(max(getattr(cfg, "LOOKBACK_DAYS", 240), 900))

    # 2) 1차 fetch
    df = fetch_price(ticker, lookback, get_cache_buster(), min_rows=260, retries=2)

    # 3) 2차 보강(1차에서 부족할 때만)
    if df is None or df.empty or len(df) < 260:
        df = fetch_price(ticker, 1400, get_cache_buster(), min_rows=260, retries=1)

    # 4) 최종 방어
    if df is None or df.empty:
        return {"error": "OHLCV 데이터가 없습니다. 티커를 확인하세요."}

    if len(df) < 260:
        return {"error": f"데이터 길이 부족(최소 260봉 필요: SMA200 포함) 현재={len(df)}"}

    # 5) 지표 계산
    df2 = build_df2(df)
    if df2 is None or df2.empty or len(df2) < 140:
        return {"error": f"지표 계산 후 유효 데이터 부족(SMA200/ATR 계산 후 남은 봉이 적음) 현재={0 if df2 is None else len(df2)}"}

    last_close = float(df2.iloc[-1]["Close"])
    use_avg = float(avg_price) if (avg_price is not None and avg_price > 0) else last_close

    # 보유 기간(days_held): entry_date 있으면 마지막 봉 날짜 기준으로 계산
    days_held = None
    if entry_date is not None and df2 is not None and not df2.empty:
        try:
            last_idx = df2.index[-1]
            last_date = last_idx.date() if hasattr(last_idx, "date") else pd.Timestamp(last_idx).date()
            ed = pd.Timestamp(entry_date).date() if entry_date is not None else None
            if ed is not None:
                days_held = max(0, (last_date - ed).days)
        except Exception:
            days_held = None
    max_hold = int(getattr(cfg, "MAX_HOLD_DAYS_DEFAULT", 15))

    # 1) 매도/익절/보유(보유관리). 포트폴리오에서는 2번(만료근접) 미적용.
    risk = sc.holding_risk_review(df2, ticker, shares, use_avg, days_held=days_held, max_hold_days=max_hold, apply_near_expiry=False)

    # 2) 추가매수(진입 신호가 다시 뜨는가 + RR/사이징 통과)
    entry, trigger, entry_hint, invalid, note = sc.decide_entry(df2)

    add_ok = False
    plan = None
    if str(entry).startswith("BUY_"):
        plan = sc.calc_trade_plan(df2, entry)
        if plan is not None and plan.get("RR", 0) >= cfg.MIN_RR and plan.get("Shares", 0) > 0:
            add_ok = True
        else:
            entry = "SKIP"
            plan = None

    # 3) 최종 추천
    if risk.get("Action") in ("SELL_TRAIL", "SELL_TREND", "SELL_STRUCTURE_BREAK", "SELL_LOSS_CUT"):
        reco = "SELL"
        why = risk.get("Reason", "")
    elif risk.get("Action") == "TAKE_PROFIT":
        reco = "SELL(부분/익절)"
        why = risk.get("Reason", "")
    elif add_ok:
        reco = "ADD_BUY"
        why = f"{trigger} | RR {plan['RR']} | Inval: {invalid}"
    else:
        reco = "HOLD"
        why = f"{risk.get('Reason','')} | 추가매수 신호 없음"

    # ✅ 목표가(ATR) 계산: use_avg 기준(티커 검색=현재가, 포트폴리오=매수가). ADD_BUY면 목표가 상향(boost=True)
    t1, t2, t3, cur_close = compute_tp_levels_from_df2(df2, boost=(reco == "ADD_BUY"), base_price=use_avg)

    # ✅ 1년 일봉 + 백테스트 매수/매도 신호 날짜 (차트용)
    df_1y = df2.tail(252).copy()
    buy_signal_dates = []
    sell_signal_dates = []
    sell_entry_prices = []
    buy_reasons = []
    sell_reasons = []
    try:
        buy_signal_dates, sell_signal_dates, sell_entry_prices, buy_reasons, sell_reasons = sc.backtest_signal_dates(df2, ticker)
    except Exception:
        pass

    return {
        "ticker": ticker,
        "reco": reco,
        "why": why,
        "close": risk.get("Close"),
        "sell_signal": risk.get("Action"),
        "add_signal": entry,
        "plan": plan,
        "risk": risk,
        "tp": {"t1": t1, "t2": t2, "t3": t3, "close": cur_close},
        "use_avg": use_avg,
        "price_basis": "avg_price" if (avg_price is not None and float(avg_price) > 0) else "current",
        "df_tail": df2.tail(30).copy(),
        "df_1y": df_1y,
        "buy_signal_dates": buy_signal_dates,
        "sell_signal_dates": sell_signal_dates,
        "sell_entry_prices": sell_entry_prices,
        "buy_reasons": buy_reasons,
        "sell_reasons": sell_reasons,
    }



def load_positions(path="positions.csv"):
    if not st.session_state.get("positions_df_loaded"):
        pass
    if not os.path.exists(path):
        return pd.DataFrame(columns=["Ticker", "Shares", "AvgPrice"])
    df = pd.read_csv(path)
    for c in ["Ticker", "Shares", "AvgPrice"]:
        if c not in df.columns:
            return pd.DataFrame(columns=["Ticker", "Shares", "AvgPrice"])
    df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
    df["Shares"] = pd.to_numeric(df["Shares"], errors="coerce")
    df["AvgPrice"] = pd.to_numeric(df["AvgPrice"], errors="coerce")
    df = df.dropna(subset=["Ticker", "Shares", "AvgPrice"])
    return df


def load_portfolio_cash() -> float:
    """포트폴리오 현금 잔고 로드 (portfolio_cash.txt)"""
    p = os.path.join(BASE_DIR, "portfolio_cash.txt")
    if not os.path.exists(p):
        return 0.0
    try:
        with open(p, "r", encoding="utf-8") as f:
            return float(f.read().strip() or 0)
    except Exception:
        return 0.0

def save_portfolio_cash(value: float):
    p = os.path.join(BASE_DIR, "portfolio_cash.txt")
    with open(p, "w", encoding="utf-8") as f:
        f.write(str(value))

def save_positions(df: pd.DataFrame, path="positions.csv"):
    df = df.copy()
    df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
    df["Shares"] = pd.to_numeric(df["Shares"], errors="coerce")
    df["AvgPrice"] = pd.to_numeric(df["AvgPrice"], errors="coerce")
    df = df.dropna(subset=["Ticker", "Shares", "AvgPrice"])
    df = df[df["Ticker"] != ""]
    df = df[["Ticker", "Shares", "AvgPrice"]]
    df.to_csv(path, index=False, encoding="utf-8-sig")


def add_or_merge(df: pd.DataFrame, ticker: str, shares: float, avg_price: float, mode="merge"):
    t = ticker.upper().strip()
    if t == "" or shares <= 0 or avg_price <= 0:
        raise ValueError("Ticker/Shares/AvgPrice 값을 확인하세요.")

    if df.empty:
        return pd.DataFrame([{"Ticker": t, "Shares": shares, "AvgPrice": avg_price}])

    if t in df["Ticker"].values:
        i = df.index[df["Ticker"] == t][0]
        if mode == "replace":
            df.at[i, "Shares"] = shares
            df.at[i, "AvgPrice"] = avg_price
        else:
            old_sh = float(df.at[i, "Shares"])
            old_ap = float(df.at[i, "AvgPrice"])
            tot = old_sh + shares
            wavg = (old_sh * old_ap + shares * avg_price) / tot
            df.at[i, "Shares"] = tot
            df.at[i, "AvgPrice"] = round(wavg, 6)
        return df

    return pd.concat([df, pd.DataFrame([{"Ticker": t, "Shares": shares, "AvgPrice": avg_price}])], ignore_index=True)


def remove_ticker(df: pd.DataFrame, ticker: str):
    t = ticker.upper().strip()
    return df[df["Ticker"] != t].copy()

def convert_recommend_action(rec: dict) -> str:
    if rec is None:
        return ""

    action = rec.get("Reco")
    why = str(rec.get("Why",""))

    # 1차 / 2차 / 3차 목표
    if "1차" in why and "익절" in why:
        return "부분 매도(1차 목표 수익률 달성!)"
    if "2차" in why and "익절" in why:
        return "부분 매도(2차 목표 수익률 달성!)"
    if "3차" in why and "익절" in why:
        return "전량 매도(3차 목표 수익률 달성!)"

    if action == "ADD_BUY":
        return "추가매수 + 목표가 상향"

    if action == "SELL":
        return "매도(하락 추세 전환)"

    return "보유"
def run_scanner_subprocess(timeout_sec: int = 900):
    """
    Windows/Streamlit에서 안 멈추게:
    - Popen으로 실행
    - stdin 차단(=input() 대기 방지)
    - timeout 지나면 terminate -> kill
    - stdout/stderr는 너무 길면 뒤쪽만 반환
    """
    scanner_path = os.path.join(BASE_DIR, "scanner.py")
    if not os.path.exists(scanner_path):
        return False, f"scanner.py not found: {scanner_path}", "", ""

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"  # 출력 버퍼링 최소화

    try:
        try:
            r = subprocess.run(
                [sys.executable, "-u", scanner_path, "--mode", "scan"],
                cwd=BASE_DIR,
                stdin=subprocess.DEVNULL,      # input() 대기 방지
                capture_output=True,
                text=True,
                env=env,
                timeout=timeout_sec,
            )
            ok = (r.returncode == 0)
            return ok, f"returncode={r.returncode}", (r.stdout or ""), (r.stderr or "")


        except subprocess.TimeoutExpired as e:
            out = getattr(e, "stdout", "") or ""
            err = getattr(e, "stderr", "") or ""
            return False, f"TIMEOUT: {timeout_sec}s", out, err


    except Exception as e:
        return False, f"EXCEPTION: {e}", "", ""

def invalidate_snapshot_cache():
    # Streamlit 쪽 snapshot/트래커/캐시 무효화
    st.session_state.pop("scan_snap", None)
    st.session_state.pop("tp3_tracker", None)
    try:
        st.cache_data.clear()
    except Exception:
        pass


# ---------- UI (테마: 사진 분위기. "복구" 요청 시 app_backup.py로 되돌린 뒤 버그수정 3가지 재적용) ----------
st.set_page_config(page_title="US Swing Scanner", layout="wide")

# 다크 테마: 딥 블루/퍼플 배경, 그린/레드 악센트, 모던 폰트. 배경 이미지: assets/background.png
import base64
_bg_data_uri = ""
_bg_path = os.path.join(BASE_DIR, "assets", "background.png")
if os.path.isfile(_bg_path):
    try:
        with open(_bg_path, "rb") as f:
            _bg_data_uri = "data:image/png;base64," + base64.b64encode(f.read()).decode("utf-8")
    except Exception:
        pass
# 배경: 이미지 있으면 .stApp에 직접 적용 (그라데이션 오버레이로 가독성 유지)
_bg_css = ""
if _bg_data_uri:
    _bg_css = """
    .stApp {
        background-image: linear-gradient(145deg, rgba(15,23,42,0.88) 0%%, rgba(30,27,75,0.85) 50%%, rgba(11,15,26,0.9) 100%%),
                          url("%s");
        background-size: cover, cover;
        background-position: center, center;
        background-repeat: no-repeat, no-repeat;
    }
    """ % _bg_data_uri
else:
    _bg_css = """
    .stApp {
        background: linear-gradient(145deg, #0f172a 0%, #1e1b4b 35%, #0b0f1a 100%);
    }
    """
_css_html = (
    '<link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">'
    "<style>:root{--bg-deep:#0b0f1a;--bg-panel:rgba(15,23,42,0.92);--bg-table:rgba(15,23,42,0.95);--text-primary:#e2e8f0;--text-muted:#94a3b8;--accent-green:#34d399;--accent-red:#f87171;--accent-amber:#fbbf24;--border-subtle:rgba(148,163,184,0.25);--glow-green:rgba(52,211,153,0.15);--glow-amber:rgba(251,191,36,0.12)}"
    + _bg_css.replace("\n", " ").strip()
    + " .stApp{font-family:'Roboto',-apple-system,sans-serif} "
+ " .main .block-container{background:var(--bg-panel);color:var(--text-primary);padding:2rem;border-radius:12px;border:1px solid var(--border-subtle);box-shadow:0 4px 24px rgba(0,0,0,0.3)} .stSidebar{background:rgba(15,23,42,0.95)!important;border-right:1px solid var(--border-subtle)} .stSidebar [data-testid=stMarkdown]{color:var(--text-primary)!important} h1,h2,h3,p,span,label,.stMarkdown{color:var(--text-primary)!important} .stMarkdown caption{color:var(--text-muted)!important} "
+ " div[data-testid=stDataFrame]{background:var(--bg-table)!important;border:1px solid rgba(100,116,139,0.4)!important;border-radius:10px!important;overflow:auto;box-shadow:0 0 24px rgba(59,130,246,0.22),0 0 48px rgba(99,102,241,0.12),0 2px 12px rgba(0,0,0,0.25)} div[data-testid=stDataFrame] table{background:transparent!important;border-collapse:separate;border-spacing:0} div[data-testid=stDataFrame] th{background:linear-gradient(180deg,rgba(30,41,59,0.98),rgba(30,41,59,0.95))!important;color:#f1f5f9!important;border:1px solid rgba(100,116,139,0.4)!important;font-family:'JetBrains Mono',monospace!important;padding:12px 14px!important;font-size:0.9rem;font-weight:700} div[data-testid=stDataFrame] td{background:rgba(15,23,42,0.9)!important;color:var(--text-primary)!important;border:1px solid rgba(100,116,139,0.3)!important;padding:12px 14px!important;font-size:0.9rem} div[data-testid=stDataFrame] tr:hover td{background:rgba(51,65,85,0.6)!important} div[data-testid=stDataFrame] tbody td:first-child{color:#22c55e!important;font-weight:600!important} "
+ " div[data-testid=stDataFrame] input{background:rgba(15,23,42,0.95)!important;color:var(--text-primary)!important;border:1px solid var(--accent-amber)!important;border-radius:6px!important;padding:6px 10px!important} div[data-testid=stDataFrame] input:focus{border-color:var(--accent-green)!important;box-shadow:0 0 0 2px var(--glow-green)!important} "
+ " [data-testid=stExpander]{background:rgba(15,23,42,0.7)!important;border:1px solid var(--border-subtle)!important;border-radius:10px!important} [data-testid=stMetricValue]{font-family:'JetBrains Mono',monospace!important;color:var(--text-primary)!important} button[kind=primary]{background:linear-gradient(135deg,#1e3a5f 0%,#1e293b 100%)!important;border:1px solid var(--accent-amber)!important;color:var(--accent-amber)!important} button[kind=primary]:hover{border-color:var(--accent-green)!important;color:var(--accent-green)!important;box-shadow:0 0 12px var(--glow-green)} "
+ " [data-testid=stTabs] [data-baseweb=tab-panel]:nth-of-type(2) button[kind=primary]{background:linear-gradient(135deg,#1e3a5f 0%,#312e81 100%)!important;border:1px solid #6366f1!important;color:#818cf8!important} [data-testid=stTabs] [data-baseweb=tab-panel]:nth-of-type(2) button[kind=primary]:hover{border-color:#818cf8!important;color:#a5b4fc!important;box-shadow:0 0 12px rgba(99,102,241,0.4)!important} [data-testid=stTabs] [data-baseweb=tab-panel]:nth-of-type(2) button[kind=secondary]{background:linear-gradient(135deg,rgba(30,58,95,0.9) 0%,rgba(49,46,129,0.9) 100%)!important;border:1px solid #6366f1!important;color:#818cf8!important} [data-testid=stTabs] [data-baseweb=tab-panel]:nth-of-type(2) button[kind=secondary]:hover{border-color:#818cf8!important;color:#a5b4fc!important;box-shadow:0 0 10px rgba(99,102,241,0.3)!important} "
+ " [data-baseweb=input],[data-baseweb=textarea]{background:rgba(15,23,42,0.9)!important;border-color:var(--border-subtle)!important;color:var(--text-primary)!important} [data-baseweb=input]:focus,[data-baseweb=textarea]:focus{border-color:var(--accent-amber)!important;box-shadow:0 0 0 1px var(--accent-amber)} [data-testid=stTabs] [data-baseweb=tab-list]{background:rgba(15,23,42,0.6)!important;border-bottom:1px solid var(--border-subtle)} [data-testid=stTabs] [aria-selected=true]{color:var(--accent-amber)!important;border-bottom:2px solid var(--accent-amber)} .stSuccess{background:rgba(52,211,153,0.15)!important;border-color:var(--accent-green)!important} .stError{background:rgba(248,113,113,0.15)!important;border-color:var(--accent-red)!important} .stWarning{background:rgba(251,191,36,0.12)!important;border-color:var(--accent-amber)!important} "
+ "</style>"
)
# CSS를 본문에 적용하려면 markdown으로 주입 (st.html은 iframe이라 표 스타일이 안 먹을 수 있음)
st.markdown(_css_html, unsafe_allow_html=True)
# 배경 이미지가 있으면 화면 전체 고정 img로도 넣어서 확실히 표시 (Streamlit CSS 제한 대비)
if _bg_data_uri:
    st.markdown(
        '<div style="position:fixed;top:0;left:0;right:0;bottom:0;z-index:-1;overflow:hidden">'
        '<img src="' + _bg_data_uri + '" style="width:100%;height:100%;object-fit:cover;opacity:0.45" alt="" />'
        '</div>',
        unsafe_allow_html=True,
    )

with st.sidebar:
    st.markdown(f"### ⚙️ App Controls\n- Version: `{APP_VERSION}`")

    colx, coly = st.columns(2)
    with colx:
        if st.button("🧹 캐시 초기화"):
            st.cache_data.clear()
            st.success("cache cleared")

    with coly:
        if st.button("🔄 강제 새로고침"):
            hard_refresh()

    st.divider()

# 로고: 전체 상단, 탭 바로 위 (20% 확대, 왼쪽 아래 밀착)
_logo_path = os.path.join(BASE_DIR, "assets", "us_swing_scanner_logo.png")
_logo_width = int(280 * 1.2)
if os.path.isfile(_logo_path):
    try:
        from PIL import Image
        import io
        import base64
        img = Image.open(_logo_path).convert("RGBA")
        arr = np.array(img)
        thresh = 40
        mask = (arr[:, :, 0] <= thresh) & (arr[:, :, 1] <= thresh) & (arr[:, :, 2] <= thresh)
        arr[mask, 3] = 0
        img_nobg = Image.fromarray(arr)
        buf = io.BytesIO()
        img_nobg.save(buf, format="PNG")
        logo_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        # 고정 높이 영역 안에서 로고만 아래쪽에 배치 (탭 등 다른 요소는 그대로)
        st.markdown(
            '<div style="height:100px;position:relative;">'
            '<img src="data:image/png;base64,' + logo_b64 + '" style="width:' + str(_logo_width) + 'px;position:absolute;bottom:-96px;left:-48px;display:block;" alt="US Swing Scanner" />'
            '</div>',
            unsafe_allow_html=True,
        )
    except Exception:
        _c1, _c2 = st.columns([1, 10])
        with _c1:
            st.image(_logo_path, width=_logo_width)
else:
    st.title("US Swing Scanner")

tab1, tab2, tab3 = st.tabs(["🔎 티커검색", "📁 포트폴리오 관리", "🚀 스캔 실행 (BUY/WATCH/SELL)"])

with tab1:
    # =========================
    # 1) 티커 검색바 (최상단)
    # =========================
    st.subheader("티커 검색")
    ticker = st.text_input("Ticker", value="AAPL", help="티커만 입력해서 조회")
    ticker = (ticker or "").strip()

    # 분석하기(왼쪽) · 닫기(오른쪽, 결과가 있을 때만 표시)
    btn_col1, btn_col2 = st.columns([1, 1])
    with btn_col1:
        if ticker and st.button("분석하기", type="primary"):
            res = analyze_ticker_reco(ticker, shares=1.0, avg_price=None)
            if "error" in res:
                st.error(res["error"])
            else:
                st.session_state["ticker_result"] = res
                st.session_state["show_ticker_result"] = True
    with btn_col2:
        if st.session_state.get("show_ticker_result") and st.session_state.get("ticker_result"):
            if st.button("닫기", key="close_ticker_result", type="secondary"):
                st.session_state["show_ticker_result"] = False
                st.session_state.pop("ticker_result", None)
                st.rerun()

    # 저장된 결과가 있으면 차트/데이터 블록 표시
    if st.session_state.get("show_ticker_result") and st.session_state.get("ticker_result"):
        res = st.session_state["ticker_result"]

        st.success(f"[{res['ticker']}] 추천: {res['reco']}")
        st.write(res["why"])

        tp = res.get("tp", {}) or {}
        risk = res.get("risk", {}) or {}
        t1, t2, t3 = tp.get("t1"), tp.get("t2"), tp.get("t3")
        close = tp.get("close")
        stop_2nd_pct = float(getattr(cfg, "SELL_2ND_CUT_PCT", 5.0))
        loss_cut_pct = float(getattr(cfg, "SELL_LOSS_CUT_PCT", 10.0))
        stop1 = risk.get("Stop1Price")
        stop2 = risk.get("Stop2Price")
        stop3 = risk.get("Stop3Price")
        suggested_pct = risk.get("SuggestedSellPct")
        suggested_reason = risk.get("SuggestedSellReason") or ""

        def _fp(x):
            if x is None or not np.isfinite(float(x)):
                return "—"
            return f"{float(x):,.2f}"

        st.caption("기준가: **현재가** (티커만 입력 시)")
        st.markdown("**1·2·3 목표가(익절)**")
        st.markdown(f"- 현재가: {_fp(close)} → 1차: {_fp(t1)} · 2차: {_fp(t2)} · 3차: {_fp(t3)}")
        st.markdown("**1·2·3 손절가** (1차 ≥ 2차 ≥ 3차)")
        st.markdown(f"- 1차(트레일 이탈): {_fp(stop1)} · 2차(중간 -{stop_2nd_pct:.0f}%): {_fp(stop2)} · 3차(전액 -{loss_cut_pct:.0f}%): {_fp(stop3)}")
        if (suggested_pct is not None and suggested_pct > 0) or (suggested_reason and str(suggested_reason).strip()):
            pct_display = (suggested_pct * 100) if suggested_pct is not None and suggested_pct <= 1.0 else suggested_pct
            st.markdown(f"- **권장 매도 비율(스케일아웃)**: {pct_display:.0f}% — {suggested_reason} (보유 수량 중 이 비율만큼 매도 권장)")

        if res.get("plan"):
            st.markdown("**Plan**")
            st.write(res["plan"])
        # 최근 매수/매도 신호 날짜·근거 (차트 위 한 줄씩)
        buy_dates = res.get("buy_signal_dates") or []
        sell_dates = res.get("sell_signal_dates") or []
        buy_reasons = res.get("buy_reasons") or []
        sell_reasons = res.get("sell_reasons") or []
        last_buy_str = pd.Timestamp(buy_dates[-1]).strftime("%Y-%m-%d") if buy_dates else "—"
        last_sell_str = pd.Timestamp(sell_dates[-1]).strftime("%Y-%m-%d") if sell_dates else "—"
        last_buy_reason = (buy_reasons[-1] if buy_reasons else "") or "—"
        last_sell_reason = (sell_reasons[-1] if sell_reasons else "") or "—"
        st.markdown(f"**최근 매수 신호:** {last_buy_str} — {last_buy_reason}")
        st.markdown(f"**최근 매도 신호:** {last_sell_str} — {last_sell_reason}")
        if res.get("df_1y") is not None and not res["df_1y"].empty:
            st.markdown(
                "<div style='background:rgba(15,23,42,0.95);color:#f1f5f9;padding:12px 16px;border-radius:10px;"
                "box-shadow:0 0 24px rgba(59,130,246,0.3);border:1px solid rgba(100,116,139,0.5);"
                "margin:0.5rem 0;font-size:1.1rem;font-weight:700;'>📊 최근 1년 캔들 · 매수 ▲ / 매도 ▼ 신호</div>",
                unsafe_allow_html=True,
            )
            plot_candles_with_signals(
                res["df_1y"],
                f"{res['ticker']} 최근 1년 (매수/매도 신호)",
                res.get("buy_signal_dates") or [],
                res.get("sell_signal_dates") or [],
                res.get("sell_entry_prices") or [],
                chart_key="ticker_1y_signals",
                dark=True,
            )
        st.markdown("**최근 30봉 데이터(지표 포함)**")
        _render_aggrid(res["df_tail"], key="aggrid_df_tail")

    st.divider()

    # =========================
    # 2) 차트 3개: SPY / QQQ / USDKRW(환율)
    # =========================
    st.markdown(
        "<div style='background:rgba(15,23,42,0.95);color:#f1f5f9;padding:12px 16px;border-radius:10px;"
        "box-shadow:0 0 24px rgba(59,130,246,0.3);border:1px solid rgba(100,116,139,0.5);"
        "margin:0.5rem 0;font-size:1.1rem;font-weight:700;'>📈 시장 차트 (최근 3개월 · 라인)</div>",
        unsafe_allow_html=True,
    )
    c1, c2, c3 = st.columns(3)

    with c1:
        spy_df = fetch_price("SPY", 400, get_cache_buster())
        plot_candles(
            spy_df,
            "SPY (3M Line)",
            chart_key="mkt_spy",
            months=3,
            kind="line",
            show_ma=False,
            dark=True,
        )

    with c2:
        qqq_df = fetch_price("QQQ", 400, get_cache_buster())
        plot_candles(
            qqq_df,
            "QQQ (3M Line)",
            chart_key="mkt_qqq",
            months=3,
            kind="line",
            show_ma=False,
            dark=True,
        )

    with c3:
        fx_df = _get_usdkrw_df(lookback_days=900)
        plot_candles(
            fx_df,
            "USD/KRW (3M Line)",
            chart_key="mkt_usdkrw",
            months=3,
            kind="line",
            show_ma=False,
            dark=True,
        )

    st.divider()

    # =========================
    # 3) TOP PICK3 BUY 성과 트래커
    # =========================
    st.markdown("## 🧾 TOP PICK3 중 BUY 신호 성과 추적 (최대 15거래일)")

    # (선택) 디버그 ON/OFF 스위치
    DEBUG_SNAP = False

    # --- TOP PICK3 tracker용 snapshot 확보 ---
    snap = st.session_state.get("scan_snap")

    if not isinstance(snap, dict):
        snap = load_scan_snapshot_only()
        st.session_state["scan_snap"] = snap

    if DEBUG_SNAP:
        st.write("SNAPSHOT KEYS:", list(snap.keys()))

    if "error" in snap:
        st.error(f"스냅샷 에러: {snap.get('error')} | path={snap.get('snapshot_path')}")
        st.stop()

    # --- snapshot에서 데이터 꺼내기 (항상 DataFrame으로 확정) ---
    def _df(x):
        return x if isinstance(x, pd.DataFrame) else pd.DataFrame(x)

    top3 = _df(snap.get("top_picks"))

    # --- TOP PICK3 중 BUY만 ---
    if top3.empty:
        st.info("TOP PICK3 후보가 없습니다.")
    else:
        if "Entry" not in top3.columns:
            st.warning("top_picks에 'Entry' 컬럼이 없습니다. 스냅샷 포맷을 확인하세요.")
            if DEBUG_SNAP:
                _render_aggrid(top3, key="aggrid_top3_debug")
            st.stop()

        top3_buy = top3[top3["Entry"].astype(str).isin(["BUY_BREAKOUT", "BUY_PULLBACK"])].copy()

        # ✅ promoted 제외(컬럼이 있으면)
        if "Promoted" in top3_buy.columns:
            top3_buy = top3_buy[~top3_buy["Promoted"].fillna(False).astype(bool)].copy()

        # ✅ PromoTag/기타 문자열에도 PROMOTED가 섞인 케이스 방어
        for col in ["PromoTag", "Tag", "Note", "Reasons", "EntryHint"]:
            if col in top3_buy.columns:
                top3_buy = top3_buy[~top3_buy[col].astype(str).str.contains("PROMOTED|BUY_PROMOTED", case=False, na=False)].copy()

        top3_buy_tickers = top3_buy["Ticker"].astype(str).str.upper().tolist() if "Ticker" in top3_buy.columns else []
        

        # ✅ (핵심) tickers가 비면: 최근 스냅샷에서 seed 하고,
        #    seed 된 tickers를 그대로 update_tracker_with_today에 넣는다.
        if not top3_buy_tickers:
            seeded = seed_tracker_from_recent_snapshots(max_files=120, max_seed=3)
            if seeded:
                st.info(f"스냅샷 기반으로 tracker를 복구했습니다: {', '.join(seeded)}")
                top3_buy_tickers = seeded[:]   # ✅ 여기 핵심(빈 리스트로 update 호출 금지)
            else:
                st.warning("스냅샷에서 복구할 BUY 종목을 찾지 못했습니다.")

        run_date = _parse_run_date(snap.get("run_date"))
        tr_all, closed_today = update_tracker_with_today(top3_buy_tickers, max_hold_days=15, run_date=run_date)


        cdbg = tr_all[tr_all["Status"]=="CLOSED"].copy() if ("Status" in tr_all.columns) else pd.DataFrame()
        if not cdbg.empty:
            st.write("DEBUG CLOSED row(s):")
            _render_aggrid(cdbg[["Ticker","SignalDate","EntryDate","EntryPrice","ExitDate","ExitPrice","ReturnPct","ExitReason"]], key="aggrid_cdbg")

        today = datetime.utcnow().date()
        cum = compute_cum_returns(tr_all, today=today)
        st.write("DEBUG closed rows:", int((tr_all["Status"] == "CLOSED").sum()) if "Status" in tr_all.columns else "no Status")
        st.write("DEBUG max ReturnPct:", float(pd.to_numeric(tr_all.get("ReturnPct", pd.Series([])), errors="coerce").max()) if "ReturnPct" in tr_all.columns else "no ReturnPct")

        open_avg = compute_open_avg_return(tr_all)

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("일간 수익률(OPEN 평균)", f"{open_avg:.2f}%")
        k2.metric("월간 수익률(누적)", f"{cum['monthly']:.2f}%")
        k3.metric("연간 수익률(누적)", f"{cum['yearly']:.2f}%")
        k4.metric("총 수익률(누적)", f"{cum['total']:.2f}%")

        st.caption("※ 누적 수익률은 '표에서 나간 종목(CLOSED)의 확정 수익률(ReturnPct)을 단순 합산'합니다.")

        open_df = tr_all[tr_all["Status"] == "OPEN"].copy()
        if open_df.empty:
            st.info("현재 추적 중인 TOP PICK3 BUY 종목이 없습니다.")
        else:
            rows = []
            for _, r in open_df.iterrows():
                t = r["Ticker"]
                entry = float(r["EntryPrice"])
                cur_close, _ = _current_close(t)
                if cur_close is None:
                    continue
                ret = (cur_close / entry - 1) * 100.0
                rows.append({
                    "Ticker": t,
                    "SignalDate": r.get("SignalDate"),
                    "EntryDate": r.get("EntryDate"),
                    "EntryPrice(PrevClose)": round(entry, 2),
                    "Close(Now)": round(float(cur_close), 2),
                    "Return%": round(ret, 2),
                    "DaysHeld": int(r.get("DaysHeld", 0)),
                })
            _render_tracker_aggrid(rows)

        st.markdown("### ✅ 오늘 종료(CLOSED)된 종목(있으면 표시)")
        if closed_today is None or closed_today.empty:
            st.info("오늘 종료된 종목 없음")
        else:
            show_cols = ["Ticker","SignalDate","EntryDate","EntryPrice","ExitDate","ExitPrice","ReturnPct","ExitReason"]
            for c in show_cols:
                if c not in closed_today.columns:
                    closed_today[c] = ""
            _render_aggrid(closed_today[show_cols], key="aggrid_closed_today")



# --- snapshot에서 데이터 꺼내기 (항상 DataFrame으로 확정) ---



with tab2:
    st.subheader("포트폴리오 관리")
    path = "positions.csv"
    dfp = load_positions(path)
    if st.session_state.pop("portfolio_saved", False):
        st.success("저장되었습니다.")

    st.markdown("### 현재 포트폴리오")

    if dfp.empty:
        st.info("포트폴리오가 비어있습니다.")
        cash_loaded = load_portfolio_cash()
        st.metric("현금", f"${cash_loaded:,.0f}")
    else:
        rows = []
        # tracker에서 OPEN인 종목의 EntryDate 조회 → 보유 기간 경고용
        entry_date_map = {}
        try:
            tr = load_tracker()
            if tr is not None and not tr.empty and "Ticker" in tr.columns and "Status" in tr.columns and "EntryDate" in tr.columns:
                open_tr = tr[(tr["Status"].astype(str) == "OPEN") & (tr["Ticker"].notna())]
                for _, row in open_tr.iterrows():
                    ticker_key = str(row["Ticker"]).upper().strip()
                    ed = row.get("EntryDate")
                    if pd.notna(ed):
                        entry_date_map[ticker_key] = ed
        except Exception:
            entry_date_map = {}

        for _, r in dfp.iterrows():
            t = str(r["Ticker"]).upper()
            shares = float(r["Shares"])
            avg_price = float(r["AvgPrice"])
            entry_date = entry_date_map.get(t)

            try:
                rec = analyze_ticker_reco(t, shares=shares, avg_price=avg_price, entry_date=entry_date)

                t1, t2, t3 = None, None, None
                stop1, stop2, stop3 = None, None, None
                risk_action = ""
                if "error" in rec:
                    recommend_text = "데이터 부족"
                else:
                    tp = rec.get("tp", {}) or {}
                    risk = rec.get("risk", {}) or {}
                    t1 = tp.get("t1"); t2 = tp.get("t2"); t3 = tp.get("t3"); close = tp.get("close")
                    stop1 = risk.get("Stop1Price")
                    stop2 = risk.get("Stop2Price")
                    stop3 = risk.get("Stop3Price")

                    risk_action = risk.get("Action", "")
                    base_reco = rec.get("reco", "")

                    def _fmt(x):
                        return "-" if (x is None or (not np.isfinite(float(x)))) else f"{float(x):.0f}"

                    # 1) 추세 이탈/구조 붕괴 매도는 최우선
                    if risk_action in ("SELL_TRAIL", "SELL_TREND"):
                        recommend_text = "매도(하락 추세 전환)"
                    elif risk_action == "SELL_LOSS_CUT":
                        recommend_text = "매도(손절)"
                    elif risk_action == "SELL_STRUCTURE_BREAK":
                        recommend_text = "매도(구조 붕괴)"
                    else:
                        # 2) 목표가 달성(ATR)로 1/2/3차 정확 분리
                        if (close is not None and t3 is not None and np.isfinite(close) and np.isfinite(t3) and close >= t3):
                            recommend_text = f"전량 매도(3차 목표 달성! { _fmt(t3) })"
                        elif (close is not None and t2 is not None and np.isfinite(close) and np.isfinite(t2) and close >= t2):
                            recommend_text = f"부분매도(2차 목표 달성! { _fmt(t2) })"
                        elif (close is not None and t1 is not None and np.isfinite(close) and np.isfinite(t1) and close >= t1):
                            recommend_text = f"부분 매도(1차 목표 달성! { _fmt(t1) })"
                        else:
                            # 3) 추가매수면 목표가 상향 포함
                            if base_reco == "ADD_BUY":
                                recommend_text = f"추가매수 + 목표가 상향(1차 목표가 { _fmt(t1) })"
                            else:
                                # 4) 그 외는 보유 + 다음 목표가 표시
                                recommend_text = f"보유(1차 목표가 { _fmt(t1) })"

            except Exception:
                recommend_text = "분석 실패"
                t1, t2, t3 = None, None, None
                stop1, stop2, stop3 = None, None, None
                risk_action = ""
                rec = {}

            # ✅ 현재가(종가) 가져오기: analyze_ticker_reco 결과(tp.close) 우선 사용
            cur_close = None
            try:
                tp = rec.get("tp", {}) or {}
                cur_close = tp.get("close", None)
                if cur_close is None or (not np.isfinite(float(cur_close))):
                    # fallback: yfinance로 한 번 더
                    cur_close, _ = _current_close(t)
            except Exception:
                cur_close, _ = _current_close(t)

            # ✅ 수익률 계산 (AvgPrice 기준)
            ret_pct = None
            try:
                if cur_close is not None and np.isfinite(float(cur_close)) and avg_price > 0:
                    ret_pct = (float(cur_close) / float(avg_price) - 1) * 100.0
            except Exception:
                ret_pct = None

            rows.append({
                "Ticker": t,
                "Shares": shares,
                "AvgPrice": avg_price,
                "ClosePrice": (round(float(cur_close), 2) if cur_close is not None and np.isfinite(float(cur_close)) else None),
                "Return%": (round(ret_pct, 2) if ret_pct is not None else np.nan),
                "Recommend": recommend_text,
                "risk_action": risk_action,
                "T1": t1, "T2": t2, "T3": t3,
                "Stop1": stop1, "Stop2": stop2, "Stop3": stop3,
            })

        pf_df = pd.DataFrame(rows)
        display_cols = ["Ticker", "Shares", "AvgPrice", "ClosePrice", "Return%", "Recommend"]
        pf_df = pf_df[display_cols]
        _render_tracker_style_table(pf_df, pct_colors=True)

        # 투자금, 투자 수익, 평균 수익률
        inv_total = sum(float(r["Shares"]) * float(r["AvgPrice"]) for r in rows)
        cur_total = sum(float(r["Shares"]) * (float(r["ClosePrice"]) if r.get("ClosePrice") is not None and np.isfinite(r.get("ClosePrice")) else float(r["AvgPrice"])) for r in rows)
        inv_return = cur_total - inv_total
        rets = [r["Return%"] for r in rows if r.get("Return%") is not None and np.isfinite(r.get("Return%"))]
        avg_ret = sum(rets) / len(rets) if rets else 0.0
        # 당일 원화 환산 (USDKRW)
        usdkrw = None
        try:
            df_fx = _get_usdkrw_df(lookback_days=5)
            if df_fx is not None and not df_fx.empty and "Close" in df_fx.columns:
                usdkrw = float(df_fx["Close"].iloc[-1])
        except Exception:
            pass
        inv_str = f"${inv_total:,.0f}"
        ret_str = f"${inv_return:,.0f}"
        if usdkrw is not None and np.isfinite(usdkrw):
            inv_str += f" (₩{inv_total * usdkrw:,.0f})"
            ret_str += f" (₩{inv_return * usdkrw:,.0f})"
        cash_display = load_portfolio_cash()
        balance = inv_total + inv_return + cash_display
        bal_str = f"${balance:,.0f}"
        if usdkrw is not None and np.isfinite(usdkrw):
            bal_str += f" (₩{balance * usdkrw:,.0f})"
        c1, c2, c3, c4, c5 = st.columns([1.25, 0.75, 1, 1, 1])
        with c1:
            st.metric("투자금", inv_str)
        with c2:
            st.metric("평균 수익률", f"{avg_ret:.1f}%")
        with c3:
            st.metric("투자 수익", ret_str)
        with c4:
            st.metric("현금", f"${cash_display:,.0f}")
        with c5:
            st.metric("잔고", bal_str)

        # 파이 차트: 메트릭과 간격 두고 아래로, 현재가치 기준(오른 종목 비중↑, 손실 종목 비중↓), 현금=초록
        st.markdown("<div style='margin-top:1.8rem;'></div>", unsafe_allow_html=True)
        pie_labels = []
        pie_values = []
        for r in rows:
            t = str(r.get("Ticker", "")).strip()
            sh = float(r.get("Shares", 0) or 0)
            close = r.get("ClosePrice")
            if t and sh > 0 and close is not None and np.isfinite(float(close)):
                v = sh * float(close)
                pie_labels.append(t)
                pie_values.append(v)
        if cash_display is not None and float(cash_display) >= 0:
            pie_labels.append("현금")
            pie_values.append(float(cash_display))
        total_pie = sum(pie_values)
        if total_pie > 0:
            ticker_colors = ["#3b82f6", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16", "#f97316", "#6366f1", "#14b8a6"]
            colors = [ticker_colors[i % len(ticker_colors)] for i in range(len(pie_labels) - 1)] + ["#22c55e"]  # 현금 항상 초록
            fig_pie = go.Figure(data=[go.Pie(
                labels=pie_labels,
                values=pie_values,
                hole=0.4,
                marker=dict(colors=colors, line=dict(color="rgba(15,23,42,0.9)", width=1.5)),
                textinfo="none",
                hovertemplate="%{label}: %{value:,.0f} (%{percent})<extra></extra>",
            )])
            fig_pie.update_layout(
                showlegend=False,
                margin=dict(l=20, r=20, t=30, b=20),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=320,
                font=dict(color="#e2e8f0", size=12),
                annotations=[dict(text="현재가치 기준", showarrow=False, font=dict(size=14), x=0.5, y=0.5)],
            )
            pct_list = [(l, (v / total_pie) * 100) for l, v in zip(pie_labels, pie_values)]
            col_pie, col_ratio = st.columns([1.2, 1])
            with col_pie:
                st.plotly_chart(fig_pie, use_container_width=True, key="portfolio_pie")
            with col_ratio:
                st.markdown("**비율 (현재가치 기준)**")
                for label, pct in pct_list:
                    idx = pie_labels.index(label) if label in pie_labels else 0
                    c = colors[idx] if idx < len(colors) else "#e2e8f0"
                    box = f"<span style='display:inline-block;width:12px;height:12px;background-color:{c};margin-right:8px;vertical-align:middle;border-radius:2px;'></span>"
                    st.markdown(f"{box} <span style='color:#e2e8f0'>{html.escape(label)}: **{pct:.1f}%**</span>", unsafe_allow_html=True)

        st.caption("기준가: **매수가(평단가)**")
        if "portfolio_tp_override" not in st.session_state:
            st.session_state["portfolio_tp_override"] = {}
        if "portfolio_stop_override" not in st.session_state:
            st.session_state["portfolio_stop_override"] = {}
        with st.expander("📌 ★1·2·3 목표가"):
            target_rows = []
            for _, r in pd.DataFrame(rows).iterrows():
                t = str(r["Ticker"]).strip()
                def _num(x):
                    if x is None or (isinstance(x, float) and not np.isfinite(x)):
                        return np.nan
                    try:
                        return float(x)
                    except Exception:
                        return np.nan
                ov = st.session_state["portfolio_tp_override"].get(t, {})
                target_rows.append({
                    "Ticker": t,
                    "1차 목표가": ov.get("t1") if ov else _num(r.get("T1")),
                    "2차 목표가": ov.get("t2") if ov else _num(r.get("T2")),
                    "3차 목표가": ov.get("t3") if ov else _num(r.get("T3")),
                })
            if target_rows:
                _tp_df = pd.DataFrame(target_rows)
                _render_tracker_style_table(_tp_df, col_widths=_TP_STOP_COL_WIDTHS)
            else:
                st.caption("데이터 없음")
        with st.expander("📌 ★1·2·3 손절가"):
            stop_rows = []
            for _, r in pd.DataFrame(rows).iterrows():
                t = str(r["Ticker"]).strip()
                def _num(x):
                    if x is None or (isinstance(x, float) and not np.isfinite(x)):
                        return np.nan
                    try:
                        return float(x)
                    except Exception:
                        return np.nan
                ov = st.session_state["portfolio_stop_override"].get(t, {})
                stop_rows.append({
                    "Ticker": t,
                    "1차 손절가": ov.get("s1") if ov else _num(r.get("Stop1")),
                    "2차 손절가": ov.get("s2") if ov else _num(r.get("Stop2")),
                    "3차 손절가(전액)": ov.get("s3") if ov else _num(r.get("Stop3")),
                })
            if stop_rows:
                _stop_df = pd.DataFrame(stop_rows)
                _render_tracker_style_table(_stop_df, col_widths=_TP_STOP_COL_WIDTHS)
            else:
                st.caption("데이터 없음")

        # SELL / TAKE PROFIT 후보 (현재 포트폴리오 중 매도/익절 권장 종목)
        st.subheader("SELL / TAKE PROFIT 후보")
        sell_tp_actions = ("SELL_TRAIL", "SELL_TREND", "SELL_STRUCTURE_BREAK", "SELL_LOSS_CUT", "TAKE_PROFIT")
        sell_tp_rows = [r for r in rows if r.get("risk_action") in sell_tp_actions]
        if sell_tp_rows:
            _render_tracker_style_table(pd.DataFrame(sell_tp_rows)[["Ticker", "Shares", "AvgPrice", "ClosePrice", "Return%", "Recommend"]], pct_colors=True)
        else:
            st.info("현재 포트폴리오 중 매도/익절 권장 종목이 없습니다.")

    st.markdown("### 추가/업데이트")
    c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1.2])
    with c1:
        t_add = st.text_input("Ticker 추가", value="")
    with c2:
        sh_add = st.number_input("Shares", min_value=0.0, value=0.0, step=1.0)
    with c3:
        ap_add = st.number_input("AvgPrice", min_value=0.0, value=0.0, step=1.0)
    with c4:
        mode = st.selectbox("동일 티커 처리", ["merge(가중평단 합산)", "replace(덮어쓰기)"])

    if st.button("포트폴리오에 추가/업데이트", type="primary"):
        try:
            m = "merge" if mode.startswith("merge") else "replace"
            df_new = add_or_merge(dfp.copy(), t_add, float(sh_add), float(ap_add), mode=m)
            save_positions(df_new, path)
            st.success("저장 완료! (positions.csv 업데이트됨)")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(str(e))

    cash_loaded = load_portfolio_cash()
    cash_new = st.number_input("현금 추가/업데이트", min_value=0.0, value=float(cash_loaded), step=100.0, key="portfolio_cash_update")
    if st.button("현금 반영", type="primary"):
        if abs(cash_new - cash_loaded) > 1e-6:
            save_portfolio_cash(cash_new)
            st.session_state["portfolio_saved"] = True
            st.rerun()
        else:
            st.info("변경 없음")

    st.markdown("### 제거")
    if dfp.empty:
        st.info("positions.csv가 비어있습니다.")
    else:
        t_list = dfp["Ticker"].astype(str).str.upper().tolist()
        t_del = st.selectbox("삭제할 티커 선택", t_list)
        if st.button("선택한 티커 삭제", type="primary"):
            df_new = remove_ticker(dfp.copy(), t_del)
            save_positions(df_new, path)
            st.success(f"{t_del} 삭제 완료!")
            st.cache_data.clear()
            st.rerun()



with tab3:
    st.subheader("🚀 스캔 실행 & 결과 (snapshot 표시 전용)")

    # 디버그 토글(평소엔 False)
    DEBUG_TAB3 = False

    # timeout 입력
    timeout_sec = st.number_input("timeout(초)", min_value=60, max_value=3600, value=900, step=30)

    # (선택) 디버그 출력
    if DEBUG_TAB3:
        st.write("CWD:", os.getcwd())
        st.write("__file__:", os.path.abspath(__file__))
        st.write("BASE_DIR:", BASE_DIR)
        st.write("SNAPSHOT FILES (snapshots/):", sorted(glob.glob(os.path.join(BASE_DIR, "snapshots", "scan_snapshot_*.json")))[-10:])
        st.write("SCAN CSV FILES (scan_csv/):", sorted(glob.glob(os.path.join(BASE_DIR, "scan_csv", "us_swing_scanner_*.csv")))[-10:])
        st.write("ALL JSON FILES (RECURSIVE):", sorted(glob.glob(os.path.join(BASE_DIR, "**", "*.json"), recursive=True))[-50:])

    # --- 스캔 실행 버튼 ---
    colA, colB = st.columns([1, 3])
    with colA:
        run_btn = st.button("🚀 스캔 실행", type="primary")
    with colB:
        st.write("")  # spacing

    if run_btn:
        with st.status("scanner.py 실행 중...", expanded=True) as status:
            ok, msg, out, err = run_scanner_subprocess(timeout_sec=int(timeout_sec))

            st.write(msg)

            # ✅ stderr는 무조건 보여주기 (문제 원인 거의 다 여기 뜸)
            with st.expander("stderr (always)", expanded=(not ok)):
                st.code((err or "(empty)")[-12000:])

            with st.expander("stdout", expanded=False):
                st.code((out or "(empty)")[-12000:])

            if ok:
                status.update(label="✅ scanner.py 실행 완료", state="complete")
                # 스캔 성공 시 최신 데이터 반영: data_refresh_ts 설정 후 캐시/스냅/트래커만 정리하고 리런
                st.session_state["data_refresh_ts"] = datetime.now().isoformat()
                st.session_state.pop("scan_snap", None)
                st.session_state.pop("tp3_tracker", None)
                st.cache_data.clear()
                st.rerun()
            else:
                status.update(label="❌ scanner.py 실행 실패/중단", state="error")

    st.divider()

    # --- 최신 스냅샷 로드해서 표시 ---
    snap = load_scan_snapshot_only()

    if "error" in snap:
        st.error(f"스냅샷 없음: {snap.get('snapshot_path')}")
        st.info("먼저 '🚀 스캔 실행' 버튼을 눌러 snapshot을 생성하세요.")
        st.stop()

    run_date = snap.get("run_date") or snap.get("date") or snap.get("asof") or snap.get("runDate")
    if not run_date:
        sp = snap.get("snapshot_path") or ""
        run_date = os.path.basename(sp).replace("scan_snapshot_", "").replace(".json", "") or str(datetime.utcnow().date())

    ms = snap.get("market_state", {}) or {}
    reg = ms.get("regime", "UNKNOWN")
    score = ms.get("score")
    score_str = f"{score}" if score is not None else "—"
    if reg == "RISK_ON":
        st.markdown(f"**:green[🟢 현재 RISK_ON 상태입니다. (시장 점수 {score_str}점 / 100점)]**")
    elif reg == "CAUTION":
        st.markdown(f"**:orange[🟡 현재 CAUTION 상태입니다. (시장 점수 {score_str}점 / 100점)]**")
    elif reg == "RISK_OFF":
        st.markdown(f"**:red[🔴 현재 RISK_OFF 상태입니다. (시장 점수 {score_str}점 / 100점)]**")
    else:
        st.markdown(f"**현재 {reg} 상태입니다. (시장 점수 {score_str}점 / 100점)**")

    with st.expander("시장 상태 상세 (Market State)"):
        def _badge(val, good_cond, bad_cond):
            if val is None:
                return "➖ normal"
            if good_cond(val):
                return "✅ good"
            if bad_cond(val):
                return "❌ bad"
            return "➖ normal"

        _p = lambda s: f"<p style='color:#f1f5f9;font-size:1.05rem;margin:0.22rem 0;'>{s}</p>"
        col_m1, col_m2 = st.columns(2)

        with col_m1:
            reg_badge = "✅ good" if reg == "RISK_ON" else ("❌ bad" if reg == "RISK_OFF" else "➖ normal")
            st.markdown(_p(f"regime: {reg} {reg_badge}"), unsafe_allow_html=True)
            if score is not None:
                sc_badge = _badge(score, lambda x: x >= 67, lambda x: x < 34)
                st.markdown(_p(f"score: {score} / 100 {sc_badge}"), unsafe_allow_html=True)
            s50, s200 = ms.get("spy_sma50"), ms.get("spy_sma200")
            if s50 is not None or s200 is not None:
                st.markdown(_p(f"SPY SMA50: {s50} | SPY SMA200: {s200} ➖ normal"), unsafe_allow_html=True)
            adx_val = ms.get("adx_spy")
            if adx_val is not None:
                try:
                    adx_f = float(adx_val)
                    adx_badge = _badge(adx_f, lambda x: x >= 20, lambda x: x < 15)
                    st.markdown(_p(f"ADX(SPY): {adx_val} {adx_badge}"), unsafe_allow_html=True)
                except (TypeError, ValueError):
                    st.markdown(_p(f"ADX(SPY): {adx_val} ➖ normal"), unsafe_allow_html=True)
            vix_val = ms.get("vix")
            if vix_val is not None:
                try:
                    vix_f = float(vix_val)
                    vix_badge = _badge(vix_f, lambda x: x < 15, lambda x: x > 25)
                    st.markdown(_p(f"VIX: {vix_val} {vix_badge}"), unsafe_allow_html=True)
                except (TypeError, ValueError):
                    st.markdown(_p(f"VIX: {vix_val} ➖ normal"), unsafe_allow_html=True)
            idx = ms.get("indices") or {}
            for sym, d in list(idx.items()):
                if not isinstance(d, dict):
                    continue
                a50, a200 = d.get("above_sma50"), d.get("above_sma200")
                a50_b = "✅ good" if a50 is True else ("❌ bad" if a50 is False else "➖ normal")
                a200_b = "✅ good" if a200 is True else ("❌ bad" if a200 is False else "➖ normal")
                st.markdown(_p(f"{sym} above_sma50: {a50} {a50_b} | above_sma200: {a200} {a200_b}"), unsafe_allow_html=True)

        with col_m2:
            vol_r = ms.get("spy_vol_ratio")
            if vol_r is not None:
                try:
                    vol_f = float(vol_r)
                    vol_badge = _badge(vol_f, lambda x: x >= 1.0, lambda x: x < 0.7)
                    st.markdown(_p(f"SPY 거래량비(20d/5d): {vol_r} {vol_badge}"), unsafe_allow_html=True)
                except (TypeError, ValueError):
                    st.markdown(_p(f"SPY 거래량비(20d/5d): {vol_r} ➖ normal"), unsafe_allow_html=True)
            sector_val = ms.get("sector_qqq_vs_xlp") or ""
            sector_str = str(sector_val).lower()
            if sector_str == "growth_lead":
                sector_badge = "✅ good"
            elif sector_str == "defensive_lead":
                sector_badge = "❌ bad"
            else:
                sector_badge = "➖ normal"
            st.markdown(_p(f"섹터(QQQ vs XLP): {sector_val or '—'} {sector_badge}"), unsafe_allow_html=True)
            comp = ms.get("components") or {}
            comp_label = {"indices": "지수(SPY/QQQ/IWM)", "adx": "ADX(SPY) 점수", "vix": "VIX 점수", "vol_ratio": "거래량비 점수", "sector": "섹터 점수"}
            for k in ["indices", "adx", "vix", "vol_ratio", "sector"]:
                if k not in comp:
                    continue
                v = comp[k]
                label = comp_label.get(k, k)
                if isinstance(v, (int, float)):
                    cb = "✅ good" if v > 0 else ("❌ bad" if v < 0 else "➖ normal")
                    st.markdown(_p(f"{label}: {v} {cb}"), unsafe_allow_html=True)
                else:
                    st.markdown(_p(f"{label}: {v} ➖ normal"), unsafe_allow_html=True)

    # 항상 DataFrame으로 강제
    def _df(x):
        return x if isinstance(x, pd.DataFrame) else pd.DataFrame(x)

    df_all   = _df(snap.get("df_all"))
    buy_df   = _df(snap.get("buy_df"))
    watch_df = _df(snap.get("watch_df"))
    risk_df  = _df(snap.get("risk_df"))
    recos_df = _df(snap.get("recos_df"))
    top3     = _df(snap.get("top_picks"))

    st.subheader("Top Picks (snapshot)")
    if top3.empty:
        st.info("TOP PICK3 후보가 없습니다.")
    else:
        _render_tracker_style_table(top3)

    st.subheader("BUY")
    _render_tracker_style_table(buy_df)

    st.subheader("WATCH")
    _render_tracker_style_table(watch_df)

    with st.expander("ALL (raw)"):
        st.dataframe(df_all, use_container_width=True)
