import html
import math
import re
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
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET

try:
    import requests
except ImportError:
    requests = None
try:
    import feedparser
except ImportError:
    feedparser = None
try:
    from trafilatura import extract, html2txt
    _HAS_TRAFILATURA = True
except ImportError:
    _HAS_TRAFILATURA = False
    html2txt = None
try:
    from googlenewsdecoder import gnewsdecoder
    _HAS_GNEWS_DECODER = True
except ImportError:
    _HAS_GNEWS_DECODER = False
# Fragment: Streamlit 1.33+ (탭별 독립 리런으로 속도 향상)
_st_fragment = getattr(st, "fragment", None) or getattr(st, "experimental_fragment", lambda f: f)

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
def _prepare_df_for_aggrid(df: pd.DataFrame, kr_currency: bool = False) -> pd.DataFrame:
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
    # 원화: 가격·거래량 등만 소수점 제거 (EV·Prob·RR·Score 등은 소수점 유지)
    if kr_currency:
        def _is_price_col(c):
            if c in ("Close", "Open", "High", "Low", "Volume", "EntryPrice", "ExitPrice",
                     "EntryPrice(PrevClose)", "Close(Now)", "MktCap_KRW_T", "PosValue", "Avg$Vol", "Shares"):
                return True
            if isinstance(c, str) and (c.startswith("SMA") or (c.startswith("ATR") and "%" not in c)):
                return True
            return False
        price_cols = [c for c in out.columns if _is_price_col(c)]
        for c in price_cols:
            if c in out.columns and out[c].dtype.kind in "fc":
                def _round_int(v):
                    if pd.isna(v): return v
                    try:
                        f = float(v)
                        return int(round(f)) if np.isfinite(f) else v
                    except (TypeError, ValueError):
                        return v
                out[c] = out[c].apply(_round_int)
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

def _render_aggrid(df, key: str, height: int = 400, kr_currency: bool = False):
    """참고 이미지와 동일한 다크 테마 AgGrid. 표시 전용. kr_currency=True면 원화(소수점 없음)."""
    if not _HAS_AGGRID:
        st.dataframe(_dark_table_style(df), use_container_width=True)
        return
    if df is None or df.empty:
        st.dataframe(_dark_table_style(df), use_container_width=True)
        return
    try:
        display_df = _prepare_df_for_aggrid(df, kr_currency=kr_currency)
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

def _tracker_table_html(rows: List[dict], use_name: bool = False, price_no_decimals: bool = False) -> str:
    """성과 추적 표를 HTML 문자열로 생성. Return% 셀에 수익=초록/손실=빨강 인라인 스타일 적용.
    use_name: 첫 열을 Name(종목명)으로 표시. price_no_decimals: Entry/Close 가격 소수점 제거."""
    if not rows:
        return ""
    first_key = "Name" if (use_name and rows and "Name" in rows[0]) else "Ticker"
    first_header = "Name" if first_key == "Name" else "Ticker"
    headers = [first_header, "Signal Date", "Entry Date", "Entry Price (Prev Close)", "Close (Now)", "Return%", "Days Held"]
    price_kind = "num_int" if price_no_decimals else "num"
    key_map = [
        (first_key, "ticker"),
        ("SignalDate", "date"),
        ("EntryDate", "date"),
        ("EntryPrice(PrevClose)", price_kind),
        ("Close(Now)", price_kind),
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
            elif kind == "num_int":
                if val is None or (isinstance(val, float) and not np.isfinite(val)):
                    txt = ""
                else:
                    txt = f"{int(round(float(val))):,}" if isinstance(val, (int, float)) else str(val)
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


def _render_tracker_aggrid(rows: List[dict], use_name: bool = False, price_no_decimals: bool = False) -> None:
    """신호 성과 추적 표 렌더. st.dataframe은 셀별 색상을 지원하지 않으므로 HTML 표로 렌더해 수익=초록/손실=빨강 적용.
    use_name: Name(종목명) 열 사용. price_no_decimals: Entry/Close 가격 소수점 제거."""
    if not rows:
        st.info("표시할 데이터가 없습니다.")
        return
    html_table = _tracker_table_html(rows, use_name=use_name, price_no_decimals=price_no_decimals)
    if html_table:
        st.markdown(html_table, unsafe_allow_html=True)
    else:
        st.info("표시할 데이터가 없습니다.")


def _dataframe_to_tracker_style_html(df: pd.DataFrame, pct_colors: bool = False, col_widths: Optional[List[str]] = None, kr_currency: bool = False) -> str:
    """DataFrame을 성과추적 표와 동일한 다크 테마 HTML 표로 변환. Ticker 열 ■+초록.
    pct_colors=True면 수익률 열 초록/빨강(성과추적표처럼), False면 흰색(스캐너용).
    col_widths를 주면 열 비율 고정(예: ["18%%", "27%%", "27%%", "28%%"]).
    kr_currency=True면 원화(소수점 없음) 포맷."""
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
            is_ticker = (c == "Ticker" or c == "Name" or (i == 0 and "ticker" in str(c).lower()))
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
                # 원화 관련만 소수점 제거(시가총액·종가·거래량 등), EV·Prob·RR·Score 등은 소수점 유지
                _kr_int_cols = ("Close", "Open", "High", "Low", "Volume", "MktCap_KRW_T",
                                "EntryPrice", "StopPrice", "TargetPrice", "PosValue", "Avg$Vol",
                                "Shares", "EntryPrice(PrevClose)", "Close(Now)")
                _is_kr_int = kr_currency and (
                    c in _kr_int_cols
                    or (isinstance(c, str) and (c.startswith("SMA") or (c.startswith("ATR") and "%" not in str(c))))
                )
                if _is_kr_int:
                    txt = f"{val:,.0f}"
                else:
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


def _render_tracker_style_table(df: pd.DataFrame, pct_colors: bool = False, col_widths: Optional[List[str]] = None, kr_currency: bool = False) -> None:
    """DataFrame을 성과추적 표와 동일한 HTML 스타일로 렌더. pct_colors=True면 수익률 열 초록/빨강. col_widths로 열 비율 고정. kr_currency=True면 원화(소수점 없음)."""
    if df is None or df.empty:
        st.info("표시할 데이터가 없습니다.")
        return
    html_table = _dataframe_to_tracker_style_html(df, pct_colors=pct_colors, col_widths=col_widths, kr_currency=kr_currency)
    if html_table:
        st.markdown(html_table, unsafe_allow_html=True)


def get_cache_buster():
    """강제 새로고침/스캔 실행 시 data_refresh_ts가 설정되면 캐시 키가 바뀌어 미국 최근 종가 기준으로 재조회됨."""
    ts = st.session_state.get("data_refresh_ts", "")
    return APP_VERSION + ("_" + str(ts) if ts else "")


def _strip_html_summary(raw: str, max_len: int = 200) -> str:
    """HTML 태그 제거 후 깔끔한 요약 텍스트 반환 (최대 max_len자)."""
    if not raw or not str(raw).strip():
        return ""
    s = re.sub(r"<[^>]+>", " ", str(raw))
    s = re.sub(r"\s+", " ", s).strip()
    s = html.unescape(s)
    return (s[:max_len] + "…") if len(s) > max_len else s


def _normalize_news_title(raw: str) -> str:
    """
    뉴스 제목에서 쌍따옴표/특수 따옴표로 인한 렌더링 깨짐을 방지.
    - 양쪽을 감싸는 큰따옴표(“중동 쇼크”) 형태면 따옴표 제거.
    - 안쪽의 따옴표는 일반 쌍따옴표로 통일.
    - HTML 태그(<b>중동</b> 같은)와 제어문자는 모두 제거.
    """
    if not raw:
        return ""
    s = str(raw)
    # HTML 엔티티/태그 제거
    try:
        s = html.unescape(s)
    except Exception:
        pass
    s = re.sub(r"<[^>]+>", " ", s)
    # 줄바꿈/공백 정리
    s = s.replace("\n", " ").replace("\r", " ")
    # 다양한 따옴표/백틱 문자 통일/제거 (마크다운 코드/강조 깨짐 방지)
    for ch in ["“", "”", "„", "‟", "＂", "˝"]:
        s = s.replace(ch, "\"")
    # 백틱(`)류는 전부 제거해서 코드 스타일로 렌더링되는 것 방지
    for ch in ["`", "´", "｀"]:
        s = s.replace(ch, "")
    s = s.strip()
    # 양 끝이 쌍따옴표면 제거
    if len(s) >= 2 and s[0] == "\"" and s[-1] == "\"":
        s = s[1:-1].strip()
    # 제어문자 제거 + 공백 재정리
    s = "".join(ch for ch in s if ch.isprintable())
    s = re.sub(r"\s+", " ", s).strip()
    return s


try:
    from article_summary_utils import clean_article_noise, extract_3_sentences, validate_summary
except ImportError:
    def clean_article_noise(t): return (re.sub(r"\s+", " ", t).strip() if t else "")
    def extract_3_sentences(t, title=""): return (re.sub(r"\s+", " ", t).strip()[:300] if t else "")
    def validate_summary(s, t=""): return (s if s else "")


def _resolve_google_news_url(url: str) -> str:
    """구글 뉴스 리다이렉트 URL을 실제 기사 URL로 변환."""
    if not url or "news.google.com" not in url:
        return url
    if _HAS_GNEWS_DECODER:
        try:
            res = gnewsdecoder(url, interval=0)
            if res and res.get("status") and res.get("decoded_url"):
                return str(res["decoded_url"]).strip()
        except Exception:
            pass
    return url


def _fetch_article_html(url: str) -> str:
    """기사 URL HTML 가져오기. 구글 뉴스 URL은 먼저 실제 URL로 변환."""
    url = _resolve_google_news_url(url)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    if requests:
        try:
            r = requests.get(url, headers=headers, timeout=8, allow_redirects=True)
            if r.ok and r.text:
                return r.text
        except Exception:
            pass
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=8) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception:
        return ""


@st.cache_data(show_spinner=False, ttl=60 * 60)
def _summarize_article_from_url(url: str, article_title: str = "", _cache_buster: str = "") -> str:
    """기사 URL에서 본문 추출 후 3줄 요약. article_title로 관련성 검증."""
    if not url or not str(url).strip():
        return ""
    html_content = _fetch_article_html(url)
    if not html_content or len(html_content) < 500:
        return ""
    if _HAS_TRAFILATURA:
        try:
            result = extract(html_content, no_fallback=False)
            if not result or len(result.strip()) < 50:
                result = (html2txt(html_content) or "") if html2txt else ""
            if result and len(result.strip()) >= 50:
                raw = extract_3_sentences(result, article_title)
                return validate_summary(raw, article_title)
        except Exception:
            pass
    return ""


def _is_meaningful_rss_summary(rss: str, title: str) -> bool:
    """RSS 요약이 제목과 다른 실제 내용인지 확인 (제목과 동일하면 False)."""
    if not rss or not rss.strip() or len(rss.strip()) <= 30:
        return False
    rss = rss.strip()
    title = (title or "").strip()
    if not title:
        return True
    if rss == title:
        return False
    if title in rss and len(rss) < len(title) * 1.4:
        return False
    if rss in title:
        return False
    rss_wo_title = rss.replace(title, "").strip()
    if len(rss_wo_title) < 20:
        return False
    return True


def _enrich_news_with_summaries(items: list, cache_buster: str = "") -> list:
    """기사 목록에 실제 본문 기반 3줄 요약 추가 (병렬). 제목과 같은 RSS 요약은 사용 안 함."""
    from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
    out = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(_summarize_article_from_url, n.get("link", ""), n.get("title", ""), cache_buster): i for i, n in enumerate(items)}
        summaries = [""] * len(items)
        try:
            for f in as_completed(futures, timeout=60):
                i = futures[f]
                try:
                    summaries[i] = f.result(timeout=5) or ""
                except Exception:
                    pass
        except FuturesTimeoutError:
            # 타임아웃 시 완료된 것만 반영, 나머지는 RSS 요약/빈 문자열로 대체
            for f in futures:
                if f.done():
                    try:
                        i = futures[f]
                        summaries[i] = f.result(timeout=0) or ""
                    except Exception:
                        pass
    for i, n in enumerate(items):
        s = summaries[i] if i < len(summaries) else ""
        if not s:
            rss = (n.get("rss_summary") or "").strip()
            title = (n.get("title") or "").strip()
            if _is_meaningful_rss_summary(rss, title):
                s = rss
        out.append({**n, "summary": s})
    return out


def _format_pub_kr(pub_raw: str = "", parsed: Optional[tuple] = None) -> str:
    """발행일을 '2026년 2월 24일 12시 34분' 형식으로 변환."""
    if parsed and len(parsed) >= 6:
        return f"{parsed[0]}년 {parsed[1]}월 {parsed[2]}일 {parsed[3]}시 {parsed[4]}분"
    if not pub_raw or not str(pub_raw).strip():
        return ""
    pub_raw = str(pub_raw).strip()
    try:
        if "T" in pub_raw:
            s = pub_raw.replace("Z", "+00:00").replace("+00:00", "")[:19]
            dt = datetime.fromisoformat(s)
            return f"{dt.year}년 {dt.month}월 {dt.day}일 {dt.hour}시 {dt.minute}분"
    except Exception:
        pass
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(pub_raw)
        return f"{dt.year}년 {dt.month}월 {dt.day}일 {dt.hour}시 {dt.minute}분"
    except Exception:
        pass
    return pub_raw[:20]


@st.cache_data(show_spinner=False, ttl=60 * 15)
def _fetch_market_news_raw(region: str) -> tuple:
    """
    RSS에서 시장 주요 기사 최대 10개 수집. (items, error_msg) 반환.
    - 항상 최신성 우선: 현재 시점 기준 24시간 이내 기사만 사용.
    - 조회수/인기 데이터는 없으므로, Google News 정렬 + 발행시각 내림차순으로 정렬해
      "인기·주요" 기사에 최대한 가깝게 근사.
    """
    from datetime import datetime, timedelta, timezone
    from email.utils import parsedate_to_datetime

    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(days=1)
    if region == "us":
        # 미국: 기본 쿼리 그대로 사용 (이미 10개 잘 나옴)
        q = urllib.parse.quote("US stock market Wall Street")
        urls = [f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"]
    else:
        # 한국: 한 쿼리에서 기사가 너무 적게 나오는 경우가 있어,
        # 여러 쿼리를 합쳐서 풀을 넓힌 뒤 상위 10개를 뽑는다.
        q1 = urllib.parse.quote("한국 증시 OR 코스피 OR 코스닥 when:1d")
        q2 = urllib.parse.quote("코스피 지수 when:1d")
        q3 = urllib.parse.quote("코스닥 지수 when:1d")
        urls = [
            f"https://news.google.com/rss/search?q={q1}&hl=ko&gl=KR&ceid=KR:ko",
            f"https://news.google.com/rss/search?q={q2}&hl=ko&gl=KR&ceid=KR:ko",
            f"https://news.google.com/rss/search?q={q3}&hl=ko&gl=KR&ceid=KR:ko",
        ]
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0"}
    last_err = ""
    for url in urls:
        if feedparser is not None:
            try:
                parsed = feedparser.parse(url, request_headers=headers)
                entries = getattr(parsed, "entries", [])  # 전체에서 24시간 필터 후 상위 10개
                fresh_items = []
                older_items = []
                for e in entries:
                    title = (e.get("title") or "").strip()
                    link = (e.get("link") or "").strip()
                    parsed_dt = e.get("published_parsed") or e.get("updated_parsed")
                    pub_raw = e.get("published") or e.get("updated") or ""

                    # 발행 시각 → datetime (UTC 기준)으로 변환
                    pub_dt_utc = None
                    try:
                        if parsed_dt:
                            dt = datetime(*parsed_dt[:6], tzinfo=timezone.utc)
                        else:
                            dt = parsedate_to_datetime(pub_raw)
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            else:
                                dt = dt.astimezone(timezone.utc)
                        pub_dt_utc = dt
                    except Exception:
                        pub_dt_utc = None

                    pub = _format_pub_kr(pub_raw, parsed_dt)
                    src = e.get("source")
                    source = ""
                    if src:
                        try:
                            source = (src.get("title") if hasattr(src, "get") else getattr(src, "title", "") or "").strip()
                        except Exception:
                            pass
                    summary_raw = e.get("summary") or e.get("description") or ""
                    if not summary_raw and e.get("content"):
                        cnt = e.get("content")
                        summary_raw = cnt[0].get("value", "") if isinstance(cnt, list) and cnt else ""
                    rss_summary = _strip_html_summary(summary_raw, max_len=300)
                    if title and link:
                        item = {
                            "title": title,
                            "link": link,
                            "pub": pub,
                            "source": source,
                            "rss_summary": rss_summary,
                            "_dt": pub_dt_utc,
                        }
                        if pub_dt_utc is not None and pub_dt_utc >= cutoff:
                            fresh_items.append(item)
                        else:
                            older_items.append(item)

                # 1순위: 24시간 이내 기사, 2순위: 그 이전 기사로 채워서 최대 10개
                fresh_items = sorted(
                    fresh_items,
                    key=lambda x: (x.get("_dt") is None, x.get("_dt")),
                    reverse=True,
                )
                older_items = sorted(
                    older_items,
                    key=lambda x: (x.get("_dt") is None, x.get("_dt")),
                    reverse=True,
                )
                items = (fresh_items + older_items)[:10]
                # 내부 정렬용 필드 제거
                for it in items:
                    it.pop("_dt", None)
                if items:
                    return (items, "")
            except Exception as e:
                last_err = f"feedparse:{type(e).__name__}:{str(e)[:100]}"
                continue

        # fallback: urllib + ElementTree
        raw = None
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read()
        except Exception as e:
            last_err = f"fetch:{type(e).__name__}:{str(e)[:150]}"
            continue
        if not raw or len(raw) < 100:
            last_err = last_err or "empty_or_short_response"
            continue
        if not (raw.lstrip().startswith(b"<?xml") or raw.lstrip().startswith(b"<rss") or raw.lstrip().startswith(b"<feed")):
            last_err = last_err or "not_xml_response"
            continue
        items = []
        try:
            root = ET.fromstring(raw)
            for elem in root.iter():
                tag = elem.tag if isinstance(elem.tag, str) else ""
                t = tag.lower()
                if not (t.endswith("item") or t.endswith("entry")):
                    continue
                title_el = elem.find("title") or elem.find("{http://www.w3.org/2005/Atom}title")
                link_el = elem.find("link") or elem.find("{http://www.w3.org/2005/Atom}link")
                pub_el = elem.find("pubDate") or elem.find("updated") or elem.find("{http://www.w3.org/2005/Atom}updated")
                source_el = elem.find("source") or elem.find("{http://www.w3.org/2005/Atom}source")
                title = (title_el.text or "").strip() if title_el is not None else ""
                link = (getattr(link_el, "attrib", {}).get("href") or (link_el.text or "")).strip() if link_el is not None else ""
                pub_raw = (pub_el.text or "").strip() if pub_el is not None and pub_el.text else ""

                # 발행 시각 → datetime (UTC) 변환
                pub_dt_utc = None
                try:
                    dt = parsedate_to_datetime(pub_raw)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    else:
                        dt = dt.astimezone(timezone.utc)
                    pub_dt_utc = dt
                except Exception:
                    pub_dt_utc = None

                pub = _format_pub_kr(pub_raw)
                source = ""
                if source_el is not None:
                    st_el = source_el.find("title") or source_el.find("{http://www.w3.org/2005/Atom}title")
                    source = (st_el.text or "").strip() if st_el is not None and st_el.text else (source_el.text or "").strip()
                desc_el = elem.find("description") or elem.find("summary") or elem.find("{http://www.w3.org/2005/Atom}summary") or elem.find("{http://www.w3.org/2005/Atom}content")
                summary_raw = ""
                if desc_el is not None:
                    summary_raw = (desc_el.text or "").strip()
                    if not summary_raw:
                        summary_raw = "".join(desc_el.itertext()).strip()
                rss_summary = _strip_html_summary(summary_raw, max_len=300)
                if title and link:
                    items.append({
                        "title": title,
                        "link": link,
                        "pub": pub,
                        "source": source,
                        "rss_summary": rss_summary,
                        "_dt": pub_dt_utc,
                    })
            # 발행시각 기준 내림차순 정렬:
            # 1순위: 24시간 이내 기사, 2순위: 그 이전 기사로 최대 10개 채우기
            fresh_items = [it for it in items if it.get("_dt") is not None and it["_dt"] >= cutoff]
            older_items = [it for it in items if it not in fresh_items]
            fresh_items = sorted(
                fresh_items,
                key=lambda x: (x.get("_dt") is None, x.get("_dt")),
                reverse=True,
            )
            older_items = sorted(
                older_items,
                key=lambda x: (x.get("_dt") is None, x.get("_dt")),
                reverse=True,
            )
            items = (fresh_items + older_items)[:10]
            for it in items:
                it.pop("_dt", None)
            if items:
                return (items, "")
        except Exception as e:
            last_err = f"parse:{type(e).__name__}:{str(e)[:100]}"
            continue
    return ([], last_err or "no_items")


def _fetch_market_news(region: str, _cache_buster: str = "") -> tuple:
    """RSS 기사 수집. 캐시 없이 직접 호출."""
    return _fetch_market_news_raw(region)


@st.cache_data(show_spinner=False, ttl=60 * 5)
def _fetch_ticker_news(ticker: str, is_kr: bool, max_items: int = 5) -> tuple:
    """구글 뉴스에서 특정 종목 관련 최신 뉴스 최대 5개 수집. (items, error_msg) 반환."""
    if not ticker or not str(ticker).strip():
        return ([], "no_ticker")
    ticker = str(ticker).strip().upper()
    if is_kr:
        try:
            from ticker_universe_kr import TICKER_TO_NAME
            query = (TICKER_TO_NAME.get(ticker, ticker) or ticker) + " 주식"
        except Exception:
            query = ticker.replace(".KS", "").replace(".KQ", "") + " 주식"
        q = urllib.parse.quote(query)
        url = f"https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"
    else:
        q = urllib.parse.quote(f"{ticker} stock")
        url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0"}
    if feedparser is not None:
        try:
            parsed = feedparser.parse(url, request_headers=headers)
            entries = getattr(parsed, "entries", [])[:max_items]
            items = []
            for e in entries:
                title = (e.get("title") or "").strip()
                link = (e.get("link") or "").strip()
                p = e.get("published_parsed") or e.get("updated_parsed")
                pub_raw = e.get("published") or e.get("updated") or ""
                pub = _format_pub_kr(pub_raw, p)
                src = e.get("source")
                source = ""
                if src:
                    try:
                        source = (src.get("title") if hasattr(src, "get") else getattr(src, "title", "") or "").strip()
                    except Exception:
                        pass
                summary_raw = e.get("summary") or e.get("description") or ""
                if not summary_raw and e.get("content"):
                    cnt = e.get("content")
                    summary_raw = cnt[0].get("value", "") if isinstance(cnt, list) and cnt else ""
                rss_summary = _strip_html_summary(summary_raw, max_len=300)
                if title and link:
                    items.append({"title": title, "link": link, "pub": pub, "source": source, "rss_summary": rss_summary})
            return (items, "")
        except Exception as e:
            return ([], f"feedparse:{type(e).__name__}:{str(e)[:80]}")
    return ([], "feedparser_unavailable")


def _render_ticker_news(news: list, ticker: str):
    """홈 뉴스와 동일한 형식으로 종목 관련 뉴스 렌더 (3줄 요약, 출처, 발행일 시분)."""
    if not news:
        st.caption("기사를 불러올 수 없습니다.")
        return
    st.markdown(f"**📰 {ticker} 관련 최신 뉴스 ({len(news)}개)**")
    for i, n in enumerate(news, 1):
        t = _normalize_news_title(n.get("title", "") or "")
        with st.expander(f"{i}. {t}", expanded=False):
            if n.get("summary"):
                st.markdown(n["summary"].replace("\n", "\n\n"))
            st.markdown(f"[**기사 보기**]({n['link']})")
            caps = []
            if n.get("source"):
                caps.append(f"출처: {n['source']}")
            if n.get("pub"):
                caps.append(f"발행: {n['pub']}")
            if caps:
                st.caption(" · ".join(caps))

def hard_refresh():
    # 미국/한국 최근 종가 기준 전 데이터 최신화 (세션 전체 clear 없이 캐시/스냅/트래커만 정리)
    st.session_state["data_refresh_ts"] = datetime.now().isoformat()
    st.cache_data.clear()
    # ohlcv.db(로컬 SQLite) 삭제 → 다음 fetch 시 yfinance/FinanceDataReader에서 새로 받음 (한국 종가 반영)
    ohlcv_db = os.path.join(BASE_DIR, "cache", "ohlcv.db")
    if os.path.exists(ohlcv_db):
        try:
            os.remove(ohlcv_db)
        except OSError:
            pass
    st.session_state.pop("scan_snap", None)
    st.session_state.pop("kr_scan_snap", None)
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


def _should_drop_today_bar_kr() -> bool:
    """한국 장중에는 오늘 진행중 봉 제외 (15:30 KST 장마감 이후엔 미제외)"""
    try:
        kst = datetime.now(ZoneInfo("Asia/Seoul"))
        if kst.weekday() >= 5:
            return False
        if (kst.hour > 15) or (kst.hour == 15 and kst.minute >= 30):
            return False
        return True
    except Exception:
        return True


def _drop_today_bar_if_needed(df: pd.DataFrame, ticker: str = "") -> pd.DataFrame:
    if df is None or df.empty:
        return df

    try:
        t_u = (ticker or "").upper()
        # 한국 종목 + 한국 지수(^KS11, ^KQ11 등)는 모두 한국장 기준으로 today 처리
        is_kr = t_u and (".KS" in t_u or ".KQ" in t_u or t_u.startswith("^K"))
        if is_kr:
            should_drop = _should_drop_today_bar_kr()
            ref_today = datetime.now(ZoneInfo("Asia/Seoul")).date()
        else:
            should_drop = _should_drop_today_bar_us()
            ref_today = datetime.now(ZoneInfo("America/New_York")).date()

        if not should_drop:
            return df

        last_dt = pd.to_datetime(df.index[-1]).date()
        if last_dt >= ref_today:
            return df.iloc[:-1].copy()
    except Exception:
        pass

    return df


# ---------- helpers ----------

def _fetch_price_inner(
    ticker: str,
    lookback_days: int,
    cache_buster: str,
    min_rows: int = 0,
    retries: int = 0,
    debug_out: Optional[List[str]] = None,
) -> Optional[pd.DataFrame]:
    """실제 fetch 로직. debug_out 있으면 로그 추가."""

    def _log(msg: str) -> None:
        if debug_out is not None:
            debug_out.append(msg)

    def _download(days: int, need_rows: int = 0) -> Optional[pd.DataFrame]:
        t_u = (ticker or "").upper()
        is_kr = t_u and (".KS" in t_u or ".KQ" in t_u or t_u.startswith("^K"))
        tz = ZoneInfo("Asia/Seoul") if is_kr else ZoneInfo("America/New_York")
        end = datetime.now(tz).date()
        start = end - timedelta(days=int(days))

        df = None
        if ohlcv_fetcher is not None:
            df = ohlcv_fetcher.fetch_ohlcv_with_fallback(
                ticker, start, end, min_rows=need_rows, base_dir=BASE_DIR
            )
            _log(f"ohlcv_fetcher: df={'None' if df is None else f'len={len(df)}'}")
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
            raw_len = 0 if df is None else len(df)
            raw_cols = list(df.columns) if df is not None and not df.empty else []
            _log(f"yf.download: len={raw_len}, columns={raw_cols}, MultiIndex={isinstance(df.columns, pd.MultiIndex) if df is not None else 'N/A'}")
            if df is not None and not df.empty and len(df) > 0:
                _log(f"tail sample:\n{df.tail(2).to_string()}")
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
                _log(f"missing col '{c}', have={list(df.columns)}")
                return None
        before_dropna = len(df)
        df = df.dropna(subset=need_cols).copy()
        after_dropna = len(df)
        df = _drop_today_bar_if_needed(df, ticker)
        after_drop_today = len(df)
        _log(f"after processing: before_dropna={before_dropna}, after_dropna={after_dropna}, after_drop_today={after_drop_today}")
        return df

    df = _download(lookback_days, int(min_rows))

    attempt = 0
    cur_days = int(lookback_days)
    while attempt < int(retries):
        if df is not None and not df.empty and (min_rows <= 0 or len(df) >= int(min_rows)):
            break
        cur_days = int(cur_days * 1.6) + 30
        df = _download(cur_days, int(min_rows))
        attempt += 1

    if df is None or df.empty:
        _log("최종: df=None 또는 empty → None 반환")
        return None
    if min_rows > 0 and len(df) < int(min_rows):
        _log(f"최종: len={len(df)} < min_rows={min_rows} → None 반환")
        return None
    return df


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
    return _fetch_price_inner(ticker, lookback_days, cache_buster, min_rows, retries, None)





# ---------- UI helpers ----------
# =========================
# Charts (SPY/QQQ/USDKRW) + TopPick BUY Performance Tracker
# =========================

TRACKER_CSV = os.path.join(BASE_DIR, "top_pick_buy_tracker.csv")

# 한국 증시 스캐너용 경로
POSITIONS_KR_PATH = os.path.join(BASE_DIR, "positions_kr.csv")
PORTFOLIO_CASH_KR_PATH = os.path.join(BASE_DIR, "portfolio_cash_kr.txt")
TRACKER_KR_CSV = os.path.join(BASE_DIR, "top_pick_buy_tracker_kr.csv")
SNAPSHOT_KR_PATTERN = os.path.join(BASE_DIR, "snapshots", "kr_scan_snapshot_*.json")

try:
    from ticker_universe_kr import TICKER_TO_NAME, NAME_TO_TICKER
except ImportError:
    TICKER_TO_NAME = {}
    NAME_TO_TICKER = {}


def _get_recent_kr_trading_date() -> str:
    """최근 한국 거래일 YYYYMMDD (주말이면 금요일 등 이전 거래일)."""
    from datetime import date, timedelta
    d = date.today()
    for _ in range(7):
        if d.weekday() < 5:
            return d.isoformat().replace("-", "")
        d -= timedelta(days=1)
    return date.today().isoformat().replace("-", "")


@st.cache_data(show_spinner=False, ttl=60 * 60 * 6)
def _get_kr_name_to_ticker_full() -> dict:
    """pykrx 또는 FinanceDataReader로 전체 시장 종목명→티커 매핑. 유니버스 외 종목 검색용."""
    out = {}
    # 1) pykrx 시도 (거래일 필요)
    try:
        from pykrx import stock
        dt = _get_recent_kr_trading_date()
        for market, suffix in [("KOSPI", ".KS"), ("KOSDAQ", ".KQ")]:
            try:
                tickers = stock.get_market_ticker_list(dt, market=market) or []
                for t in tickers:
                    try:
                        name = stock.get_market_ticker_name(t)
                        if name:
                            out[name] = str(t).zfill(6) + suffix
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception:
        pass
    # 2) pykrx 비었으면 FinanceDataReader fallback
    if not out:
        try:
            import FinanceDataReader as fdr
            for market, suffix in [("KOSPI", ".KS"), ("KOSDAQ", ".KQ")]:
                try:
                    df = fdr.StockListing(market)
                    if df is not None and not df.empty:
                        code_col = "Code" if "Code" in df.columns else "Symbol"
                        name_col = "Name" if "Name" in df.columns else "종목명"
                        if code_col in df.columns and name_col in df.columns:
                            for _, row in df.iterrows():
                                code = str(row.get(code_col, "")).zfill(6)
                                name = row.get(name_col)
                                if code and name and str(name).strip():
                                    out[str(name).strip()] = code + suffix
                except Exception:
                    pass
        except Exception:
            pass
    return out


def _get_kr_display_name(sym: str) -> str:
    """
    한국 종목 티커(.KS/.KQ)를 사람이 보기 좋은 종목명(한국어)으로 변환.
    1순위: ticker_universe_kr.TICKER_TO_NAME
    2순위: _get_kr_name_to_ticker_full()의 역매핑
    실패 시에는 원래 티커 문자열(sym) 그대로 반환.
    """
    s = (sym or "").upper().strip()
    if not s or (".KS" not in s and ".KQ" not in s):
        return s

    # 1) 미리 정의된 유니버스 매핑
    if s in TICKER_TO_NAME and TICKER_TO_NAME[s]:
        return str(TICKER_TO_NAME[s])

    # 2) 전체 시장 매핑 역추적 (이름 → 티커 맵을 뒤집어서 사용)
    try:
        full_map = _get_kr_name_to_ticker_full()
        for name, ticker in full_map.items():
            if str(ticker).upper().strip() == s:
                return str(name)
    except Exception:
        pass

    return s


def _normalize_kr_ticker(sym: str) -> str:
    """
    코스피/코스닥 구분을 위해 한국 종목 티커를 정규화.
    - 입력: '086520.KS' 또는 '086520.KQ'
    - 출력: 실제 상장 시장 기준으로 '.KS' 또는 '.KQ'를 붙인 티커 문자열.
    우선순위:
      1) ticker_universe_kr.TICKER_TO_NAME 키(유니버스 정의에 따른 시장)
      2) pykrx.get_market_ticker_list(KOSPI/KOSDAQ)
      3) FinanceDataReader.StockListing(KOSPI/KOSDAQ)
      4) 실패 시 기존 suffix 유지
    """
    s = (sym or "").upper().strip()
    if not s:
        return s
    has_suffix = (".KS" in s) or (".KQ" in s)
    base = s.replace(".KS", "").replace(".KQ", "").strip()
    if not base.isdigit():
        return s
    base = base.zfill(6)

    # 1) 유니버스 매핑에서 시장 판별
    try:
        for tk in TICKER_TO_NAME.keys():
            tk_u = str(tk).upper().strip()
            if ".KS" not in tk_u and ".KQ" not in tk_u:
                continue
            tk_base = tk_u.replace(".KS", "").replace(".KQ", "").strip()
            if tk_base == base:
                return tk_u
    except Exception:
        pass

    # 2) pykrx로 시장 판별
    try:
        from datetime import date
        from pykrx import stock
        dt = date.today().strftime("%Y%m%d")
        for market, suffix in [("KOSPI", ".KS"), ("KOSDAQ", ".KQ")]:
            try:
                tickers = stock.get_market_ticker_list(dt, market=market) or []
                norm_list = {str(t).zfill(6) for t in tickers}
                if base in norm_list:
                    return base + suffix
            except Exception:
                continue
    except Exception:
        pass

    # 3) FinanceDataReader로 시장 판별
    try:
        import FinanceDataReader as fdr
        for market, suffix in [("KOSPI", ".KS"), ("KOSDAQ", ".KQ")]:
            try:
                df = fdr.StockListing(market)
                if df is None or df.empty:
                    continue
                code_col = "Code" if "Code" in df.columns else "Symbol"
                if code_col not in df.columns:
                    continue
                codes = {str(c).zfill(6) for c in df[code_col].dropna().astype(str)}
                if base in codes:
                    return base + suffix
            except Exception:
                continue
    except Exception:
        pass

    # 4) 실패 시 기존 suffix 유지
    if has_suffix:
        return base + (".KQ" if s.endswith(".KQ") else ".KS")
    return base


def _resolve_kr_name_or_ticker(user_input: str) -> str:
    """종목명 또는 티커 입력 → 티커 반환. 유니버스 외 종목도 pykrx로 조회."""
    s = (user_input or "").strip()
    if not s:
        return ""
    # 이미 티커 형식(.KS/.KQ)이면 그대로
    if ".KS" in s.upper() or ".KQ" in s.upper():
        return s.upper().strip()
    # 1) 유니버스에서 종목명 → 티커
    if s in NAME_TO_TICKER:
        return NAME_TO_TICKER[s]
    for name, ticker in NAME_TO_TICKER.items():
        if name.upper() == s.upper():
            return ticker
    # 2) pykrx 전체 시장에서 종목명 → 티커 (유니버스 외 종목)
    full_map = _get_kr_name_to_ticker_full()
    if s in full_map:
        return full_map[s]
    for name, ticker in full_map.items():
        if name and name.upper() == s.upper():
            return ticker
    # 3) 매칭 없으면 입력값 그대로 (yfinance가 처리 시도)
    return s.upper().strip()


def _fmt_currency(val: float, market: str = "us") -> str:
    """금액 포맷: us=$, kr=₩"""
    if val is None or (isinstance(val, float) and not np.isfinite(val)):
        return "—"
    try:
        v = float(val)
        if market == "kr":
            return f"₩{v:,.0f}"
        return f"${v:,.0f}"
    except (TypeError, ValueError):
        return "—"

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



@st.cache_data(show_spinner=False, ttl=300)
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


def _extract_raw(val):
    """API 응답이 {raw: x, fmt: y} 형태일 때 raw 추출."""
    if val is None:
        return None
    if isinstance(val, dict) and "raw" in val:
        return val["raw"]
    try:
        return float(val) if np.isfinite(float(val)) else None
    except (TypeError, ValueError):
        return None


@st.cache_data(show_spinner=False, ttl=60 * 15)  # 15분 캐시
def _fetch_trending_us_tickers(count: int = 20) -> pd.DataFrame:
    """Yahoo Finance Most Actives(오늘 가장 많이 검색/거래된 미국 증시 티커) 조회."""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    url = (
        "https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved"
        "?formatted=true&lang=en-US&region=US&scrIds=most_actives&count=%d&corsDomain=finance.yahoo.com"
    ) % count

    for fetcher in [_fetch_via_requests, _fetch_via_urllib]:
        try:
            data = fetcher(url, headers)
            if not data:
                continue
            quotes = data.get("finance", {}).get("result") or []
            if not quotes:
                continue
            quotes = (quotes[0] or {}).get("quotes") or []
            if not quotes:
                continue
            rows = []
            for i, q in enumerate(quotes[:count], 1):
                sym = q.get("symbol", "")
                name = q.get("shortName") or q.get("displayName") or q.get("longName") or sym
                price = _extract_raw(q.get("regularMarketPrice"))
                chg = _extract_raw(q.get("regularMarketChangePercent"))
                vol = _extract_raw(q.get("regularMarketVolume"))
                if isinstance(vol, float):
                    vol = int(vol)
                rows.append({
                    "순위": i,
                    "Ticker": sym,
                    "종목명": (str(name)[:20] + "…") if len(str(name)) > 20 else str(name),
                    "현재가": price,
                    "등락률(%)": chg,
                    "거래량": vol,
                })
            return pd.DataFrame(rows)
        except Exception:
            continue
    return _fallback_trending_tickers(count)


def _fetch_via_requests(url: str, headers: dict):
    try:
        import requests
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def _fetch_via_urllib(url: str, headers: dict):
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def _fallback_trending_tickers(count: int) -> pd.DataFrame:
    """API 실패 시 yfinance로 인기 티커 조회."""
    default_tickers = [
        "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "BRK-B", "JPM", "V",
        "WMT", "UNH", "JNJ", "PG", "XOM", "HD", "MA", "CVX", "ABBV", "MRK",
    ][:count]
    rows = []
    for i, sym in enumerate(default_tickers, 1):
        try:
            t = yf.Ticker(sym)
            info = t.info or {}
            price = info.get("regularMarketPrice") or info.get("currentPrice")
            chg = info.get("regularMarketChangePercent")
            name = info.get("shortName") or info.get("longName") or sym
            rows.append({
                "순위": i,
                "Ticker": sym,
                "종목명": (str(name)[:20] + "…") if len(str(name)) > 20 else str(name),
                "현재가": float(price) if price is not None and np.isfinite(float(price)) else None,
                "등락률(%)": float(chg) if chg is not None and np.isfinite(float(chg)) else None,
                "거래량": None,
            })
        except Exception:
            rows.append({"순위": i, "Ticker": sym, "종목명": sym, "현재가": None, "등락률(%)": None, "거래량": None})
    return pd.DataFrame(rows)


# 한국 증시 트렌딩 티커 (yfinance 기반)
KR_DEFAULT_TICKERS = [
    "005930.KS", "000660.KS", "035420.KS", "051910.KS", "207940.KS",
    "005380.KS", "000270.KS", "068270.KS", "006400.KS", "035720.KS",  # 000270=기아
    "105560.KS", "003670.KS", "032830.KS", "017670.KS", "000810.KS",
    "247540.KQ", "086520.KS", "034730.KS", "066570.KS", "009150.KS",
]


@st.cache_data(show_spinner=False, ttl=60 * 15)
def _fetch_trending_kr_tickers(count: int = 20) -> pd.DataFrame:
    """한국 증시 트렌딩 티커 조회."""
    rows = []
    for i, sym in enumerate(KR_DEFAULT_TICKERS[:count], 1):
        try:
            # 코스피/코스닥 시장 기준으로 티커 정규화
            norm_sym = _normalize_kr_ticker(sym)

            t = yf.Ticker(norm_sym)
            info = t.info or {}
            price = info.get("regularMarketPrice") or info.get("currentPrice")
            chg = info.get("regularMarketChangePercent")
            # 종목명은 한국어 기준으로 우선 표시
            name = _get_kr_display_name(norm_sym)
            # 매핑 실패 시에만 yfinance 이름 사용.
            # 0P000... 같은 내부 코드도, 요청에 따라 티커와 함께 그대로 노출.
            if name == norm_sym or name == sym:
                raw_name = info.get("shortName") or info.get("longName")
                if raw_name:
                    rn = str(raw_name).strip()
                    # 내부 코드처럼 보이면 "티커,코드" 형태로 그대로 보여주기
                    if rn.startswith("0P000"):
                        name = f"{norm_sym},{rn}"
                    else:
                        name = rn
            # 최종 표시는 회사명만 사용 (예: 에코프로). .KS/.KQ는 Ticker 컬럼에만 유지.
            display_name = name
            rows.append({
                "순위": i,
                "Ticker": norm_sym,
                "종목명": (str(display_name)[:20] + "…") if len(str(display_name)) > 20 else str(display_name),
                "현재가": float(price) if price is not None and np.isfinite(float(price)) else None,
                "등락률(%)": float(chg) if chg is not None and np.isfinite(float(chg)) else None,
                "거래량": None,
            })
        except Exception:
            rows.append({"순위": i, "Ticker": sym, "종목명": sym, "현재가": None, "등락률(%)": None, "거래량": None})
    return pd.DataFrame(rows)


def add_mas(df: pd.DataFrame, windows=(10, 20, 50)):
    out = df.copy()
    c = out["Close"]
    for w in windows:
        out[f"SMA{w}"] = c.rolling(w).mean()
    return out


def _build_rangebreaks_kr(d: pd.DataFrame, is_kr: bool) -> list:
    """주말 + (한국증시 시) 휴장일 rangebreaks"""
    out = [dict(bounds=["sat", "mon"])]
    if is_kr and d is not None and not d.empty:
        kr_holidays = _get_kr_holidays_for_range(d.index.min(), d.index.max())
        if kr_holidays:
            out.append(dict(values=kr_holidays))
    return out


def _get_kr_holidays_for_range(start, end) -> List[str]:
    """한국거래소 휴장일 → Plotly rangebreaks values용 (YYYY-MM-DD 리스트)"""
    fixed = []
    try:
        t_start = pd.Timestamp(start).normalize()
        t_end = pd.Timestamp(end).normalize()
        for y in range(max(2020, t_start.year), min(2030, t_end.year + 1)):
            fixed.extend([
                f"{y}-01-01", f"{y}-03-01", f"{y}-05-05", f"{y}-06-06",
                f"{y}-08-15", f"{y}-10-03", f"{y}-10-09", f"{y}-12-25",
            ])
            if y == 2024:
                fixed.extend(["2024-02-09", "2024-02-10", "2024-02-11", "2024-02-12",
                             "2024-09-16", "2024-09-17", "2024-09-18"])
            elif y == 2025:
                fixed.extend(["2025-01-28", "2025-01-29", "2025-01-30",
                             "2025-10-05", "2025-10-06", "2025-10-07", "2025-10-08"])
            elif y == 2026:
                fixed.extend(["2026-02-16", "2026-02-17", "2026-02-18",
                             "2026-09-24", "2026-09-25", "2026-09-26"])
        # 데이터 범위 내 휴장일만 반환
        out = []
        for dstr in fixed:
            try:
                t = pd.Timestamp(dstr)
                if t_start <= t <= t_end:
                    out.append(dstr)
            except Exception:
                pass
        return out
    except Exception:
        return []


def plot_candles(
    df: pd.DataFrame,
    title: str,
    *,
    chart_key: str,          # ✅ Streamlit element key (자리 고정)
    months: int = 3,         # ✅ 기본 3개월
    kind: str = "line",      # "line" | "candle"
    show_ma: bool = False,   # ✅ 기본 MA 숨김
    dark: bool = False,      # ✅ 다크 테마 (시장 차트 등)
    is_kr: bool = False,     # ✅ 한국증시: 휴장일 rangebreaks 적용
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

    # OHLC NaN 행 제거 (캔들 갭 원인)
    if all(c in d0.columns for c in ("Open", "High", "Low", "Close")):
        d0 = d0.dropna(subset=["Open", "High", "Low", "Close"])

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
        fig.update_layout(xaxis=dict(rangebreaks=_build_rangebreaks_kr(d, is_kr)))
    else:
        fig.update_layout(xaxis_rangeslider_visible=False)

    # ✅ key 고정이 핵심 (차트 “밀림/자리바뀜” 방지)
    # 다크 테마 (시장 차트 등 HTML 패널과 통일)
    if dark:
        xaxis_upd = dict(gridcolor="rgba(100,116,139,0.3)", zerolinecolor="rgba(100,116,139,0.3)")
        if kind_eff == "candle" and hasattr(fig.layout, "xaxis") and getattr(fig.layout.xaxis, "rangebreaks", None):
            xaxis_upd["rangebreaks"] = fig.layout.xaxis.rangebreaks
        fig.update_layout(
            paper_bgcolor="rgba(15,23,42,0.95)",
            plot_bgcolor="rgba(15,23,42,0.92)",
            font=dict(color="#e2e8f0", size=12),
            title_font=dict(color="#f1f5f9"),
            xaxis=xaxis_upd,
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
    is_kr: bool = False,
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
    # OHLC NaN 행 제거 (캔들 갭 원인)
    d0 = d0.dropna(subset=["Open", "High", "Low", "Close"])

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
            rangebreaks=_build_rangebreaks_kr(d0, is_kr),
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
        xaxis_dark = dict(gridcolor="rgba(100,116,139,0.3)", zerolinecolor="rgba(100,116,139,0.3)")
        xaxis_dark["rangebreaks"] = _build_rangebreaks_kr(d0, is_kr)
        fig.update_layout(
            paper_bgcolor="rgba(15,23,42,0.95)",
            plot_bgcolor="rgba(15,23,42,0.92)",
            font=dict(color="#e2e8f0", size=12),
            title_font=dict(color="#f1f5f9"),
            xaxis=xaxis_dark,
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
    # SignalDate/EntryDate가 저장 시 빠지지 않도록 날짜 컬럼을 YYYY-MM-DD 문자열로 통일
    for col in ["SignalDate", "EntryDate", "LastBarDate", "ExitDate"]:
        if col not in df.columns:
            continue
        def _date_str(v):
            if v is None or (isinstance(v, float) and np.isnan(v)) or (pd.isna(v)):
                return ""
            try:
                d = pd.Timestamp(v)
                if pd.isna(d):
                    return ""
                return d.strftime("%Y-%m-%d")
            except Exception:
                return ""
        df[col] = df[col].apply(_date_str)
    df.to_csv(path, index=False, encoding="utf-8-sig")

def _parse_run_date(run_date_like) -> Optional[datetime.date]:
    """snapshot run_date("YYYY-MM-DD") -> date"""
    try:
        if run_date_like is None:
            return None
        return pd.to_datetime(str(run_date_like)).date()
    except Exception:
        return None

def _prev_trading_day(d: date) -> date:
    """d의 바로 전 거래일 (주말 스킵)."""
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:
        prev = prev - timedelta(days=1)
    return prev


def _close_on_prev_trading_day(ticker: str, signal_date) -> tuple:
    """SignalDate의 전 거래일 종가 반환. Entry Price = 신호일 전일 종가(Prev Close). (close_price, prev_date) 또는 (None, None)."""
    try:
        d = _parse_run_date(signal_date) if not isinstance(signal_date, date) else signal_date
        if d is None:
            return None, None
        prev_d = _prev_trading_day(d)
        return _close_on_date(ticker, prev_d)
    except Exception:
        return None, None


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
    snapshot_pattern: Optional[str] = None,
    tracker_path: Optional[str] = None,
) -> list[str]:
    """
    ✅ CSV가 없거나/비었을 때:
    - 최근 스냅샷들에서 BUY_BREAKOUT/BUY_PULLBACK 티커를 모아서
      Promoted 제외 후, 최대 max_seed개를 tracker(OPEN)로 '재시드'한다.
    - 반환: 실제로 seed된 티커 리스트
    - snapshot_pattern/tracker_path: KR 시장용 (None이면 US 기본값)
    """
    pat = snapshot_pattern or SNAPSHOT_PATTERN
    files = sorted(glob.glob(pat))[-max_files:]
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
    tr = load_tracker(tracker_path) if tracker_path else load_tracker()
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

        # Entry Price: KR=신호일 종가, US=신호일 전 거래일 종가 (Prev Close)
        is_kr_seed = tracker_path is not None and "kr" in str(tracker_path).lower()
        if d is not None:
            if is_kr_seed:
                entry_price, entry_date_px = _close_on_date(t, d)
            else:
                entry_price, entry_date_px = _close_on_prev_trading_day(t, d)
                if entry_price is None:
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

        # StopPrice는 가능하면 계산. lookback 900일로 캐시에 1년치 저장
        stop_price = np.nan
        try:
            df_for_stop = fetch_price(t, lookback_days=900, cache_buster=get_cache_buster())
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

    if tracker_path:
        save_tracker(tr, tracker_path)
    else:
        save_tracker(tr)
    return seeded

def _entry_price_on_signal_date(ticker: str):
    """
    EntryDate = SignalDate, EntryPrice = SignalDate의 종가 (신호날 종가)
    """
    df = fetch_price(ticker, lookback_days=900, cache_buster=get_cache_buster())
    if df is None or df.empty or len(df) < 2:
        return None, None

    signal_date = _ensure_dt(df.index[-1])
    entry_price = float(df["Close"].iloc[-1])  # 신호날 종가
    return entry_price, signal_date



def _fetch_recent_close(ticker: str, min_rows: int = 2) -> Optional[pd.DataFrame]:
    """
    최근 종가용 단일 fetch. Streamlit 캐시 우회 → 티커검색/트래커/포트폴리오 간 종가 일치.
    """
    return _fetch_price_inner(ticker, 30, get_cache_buster(), min_rows=min_rows)


def _get_ref_trading_date(ticker: str):
    """
    시장 마감 기준 '현재 마지막 거래일' 반환.
    - US: 16:20 ET 이후 → 오늘(ET), 그 전 → 어제
    - KR: 15:30 KST 이후 → 오늘(KST), 그 전 → 어제
    주말이면 해당 금요일 등 이전 거래일로 스킵.
    """
    t_u = (ticker or "").upper()
    is_kr = t_u and (".KS" in t_u or ".KQ" in t_u or t_u.startswith("^K"))
    try:
        if is_kr:
            kst = datetime.now(ZoneInfo("Asia/Seoul"))
        else:
            kst = datetime.now(ZoneInfo("America/New_York"))
        d = kst.date()
        # 주말: 이전 거래일로
        while d.weekday() >= 5:
            d = d - timedelta(days=1)
        if is_kr:
            closed = (kst.hour > 15) or (kst.hour == 15 and kst.minute >= 30)
        else:
            closed = (kst.hour > 16) or (kst.hour == 16 and kst.minute >= 20)
        if closed:
            return d
        # 장중: 전 거래일
        d = d - timedelta(days=1)
        while d.weekday() >= 5:
            d = d - timedelta(days=1)
        return d
    except Exception:
        return None


def _current_close(ticker: str):
    """단일 소스: 모든 섹션에서 동일한 종가 사용. 시장 마감 후 DaysHeld 정확 반영을 위해 ref_trading_date 적용."""
    df = _fetch_recent_close(ticker)
    if df is None or df.empty or len(df) < 2:
        return None, None
    close_val = float(df["Close"].iloc[-1])
    last_row_date = _ensure_dt(df.index[-1])
    ref_date = _get_ref_trading_date(ticker)
    # 시장 마감 후: cur_date 설정. US는 시장 마감 후 ref_date+1 (한국 시간 기준 다음날 반영)
    if ref_date is not None:
        t_u = (ticker or "").upper()
        is_kr = t_u and (".KS" in t_u or ".KQ" in t_u or t_u.startswith("^K"))
        try:
            now = datetime.now(ZoneInfo("Asia/Seoul")) if is_kr else datetime.now(ZoneInfo("America/New_York"))
            if now.weekday() < 5:
                closed = (now.hour > 15) or (now.hour == 15 and now.minute >= 30) if is_kr else (now.hour > 16) or (now.hour == 16 and now.minute >= 20)
                if closed:
                    # US: 장 마감 후 cur_date = ref_date+1 (DaysHeld 올바른 증가)
                    cur_d = ref_date + timedelta(days=1) if not is_kr else ref_date
                    return close_val, cur_d
        except Exception:
            pass
    # 시장 개장 중이거나 ref 없음: 데이터 기준
    if ref_date is not None and last_row_date is not None and ref_date > last_row_date:
        return close_val, ref_date
    return close_val, last_row_date


def _exit_signal_from_scanner(ticker: str, shares: float = 1.0, avg_price: float = 1.0, days_held=None, max_hold_days=None):
    """
    TOP PICK3 BUY 성과 추적용 exit 시그널:
      - holding_risk_review가 SELL_TRAIL / SELL_TREND / TAKE_PROFIT 이면 exit
      - days_held/max_hold_days 넘기면 2번(만료 근접 시 트레일 강화·컨펌 완화) 적용
    """
    df = fetch_price(ticker, lookback_days=900, cache_buster=get_cache_buster())
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


def update_tracker_with_today(top3_buy_tickers: List[str], max_hold_days: int = 15, run_date=None, tracker_path: Optional[str] = None, snapshot_pattern: Optional[str] = None):
    """
    ✅ 안정 버전(요청사항 반영)
    - TOP PICK3 중 BUY(BUY_BREAKOUT/BUY_PULLBACK)만 tracker에 신규 편입
    - run_date가 있으면 해당일 종가로 Entry 가격·날짜 설정(_close_on_date)
    - PROMOTED 종목(SEE/SO 같은) 자동 제거 (OPEN에서만 제거)
    - 기존에 추적 중이던 정상 BUY 종목(과거 OPEN)은 절대 '유니버스 밖' 이유로 삭제하지 않음  ← 핵심
    - 신규 편입 종목은 당일/첫날엔 exit 판정 금지(바로 CLOSED 방지)
    - CLOSED는 15거래일 도달 시에만 적용(조기 청산/손절/익절 시그널 무시)
    - tracker_path/snapshot_pattern: KR 시장용 (None이면 US 기본값)
    """
    tr = load_tracker(tracker_path) if tracker_path else load_tracker()

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
    _snap_pat = snapshot_pattern or SNAPSHOT_PATTERN
    def _recent_promoted_tickers(max_files: int = 60) -> set[str]:
        promo = set()
        files = sorted(glob.glob(_snap_pat))[-max_files:]
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
            # KR: 오늘 스캔 선정 → Entry = 오늘 종가. US: Entry = 신호일 전 거래일 종가(Prev Close)
            is_kr_tracker = tracker_path is not None and "kr" in str(tracker_path).lower()
            if is_kr_tracker:
                entry_price, _ = _close_on_date(t, run_date)
            else:
                entry_price, _ = _close_on_prev_trading_day(t, run_date)
            if entry_price is None and not is_kr_tracker:
                entry_price, _ = _close_on_date(t, run_date)
            if entry_price is None:
                entry_price, _ = _entry_price_on_signal_date(t)
            if entry_price is None:
                continue
            # Signal/Entry 날짜는 항상 스캐너를 돌린 날(run_date)로 고정
            signal_date = pd.Timestamp(run_date)
            entry_date = pd.Timestamp(run_date)
        else:
            _, signal_date = _entry_price_on_signal_date(t)
            if signal_date is None:
                continue
            is_kr_tracker = tracker_path is not None and "kr" in str(tracker_path).lower()
            if is_kr_tracker:
                entry_price, _ = _close_on_date(t, signal_date)
            else:
                entry_price, _ = _close_on_prev_trading_day(t, signal_date)
            if entry_price is None:
                entry_price, _ = _entry_price_on_signal_date(t)
            if entry_price is None:
                continue
            entry_date = signal_date  # EntryDate = SignalDate

        # 같은 signal_date 중복 방지
        dup_mask = (
            (tr["Ticker"].astype(str).str.upper() == t) &
            (tr["SignalDate"] == signal_date)
        ) if (not tr.empty and "SignalDate" in tr.columns) else None
        if dup_mask is not None and dup_mask.any():
            continue

        # StopPrice 계산(가능하면). lookback 900일로 캐시에 1년치 저장 → 티커 검색 시 1년 차트 표시
        stop_price = np.nan
        try:
            df_for_stop = fetch_price(t, lookback_days=900, cache_buster=get_cache_buster())
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
        if tracker_path:
            save_tracker(tr, tracker_path)
        else:
            save_tracker(tr)
        return tr, pd.DataFrame()

    # 타입 안정화
    tr["Ticker"] = tr["Ticker"].astype(str).str.upper().str.strip()
    tr["Status"] = tr["Status"].astype(str)

    # OPEN 행 중 SignalDate/EntryDate가 비어 있을 때만 채움. 이미 있으면 절대 덮어쓰지 않음(23일로 바뀌는 현상 방지)
    for idx, row in tr[tr["Status"] == "OPEN"].iterrows():
        sd = row.get("SignalDate")
        ed = row.get("EntryDate")
        if pd.notna(sd) and pd.notna(ed) and str(sd) != "" and str(ed) != "":
            continue
        t = str(row.get("Ticker", "")).upper().strip()
        if not t:
            continue
        _, fill_date = _entry_price_on_signal_date(t)
        if fill_date is not None:
            tr.at[idx, "SignalDate"] = pd.Timestamp(fill_date) if not isinstance(fill_date, date) else fill_date
            tr.at[idx, "EntryDate"] = tr.at[idx, "SignalDate"]

    open_df = tr[tr["Status"] == "OPEN"].copy()
    closed_today = []

    for idx, row in open_df.iterrows():
        t = str(row.get("Ticker", "")).upper().strip()
        if not t:
            continue

        entry_price = row.get("EntryPrice", None)
        signal_date = _ensure_dt(row.get("SignalDate"))

        # EntryPrice 보정: KR=신호일 종가, US=신호일 전 거래일 종가(Prev Close)
        is_kr_tracker = tracker_path is not None and "kr" in str(tracker_path).lower()
        if signal_date is not None:
            try:
                fix_price, _ = _close_on_date(t, signal_date) if is_kr_tracker else _close_on_prev_trading_day(t, signal_date)
                if fix_price is not None and np.isfinite(fix_price):
                    try:
                        cur_ep = float(entry_price) if entry_price is not None and np.isfinite(float(entry_price)) else None
                    except Exception:
                        cur_ep = None
                    if (cur_ep is None) or (abs(float(fix_price) - cur_ep) > 1e-6):
                        entry_price = float(fix_price)
                        tr.loc[idx, "EntryPrice"] = entry_price
            except Exception:
                pass

        try:
            if entry_price is None or not np.isfinite(float(entry_price)) or float(entry_price) <= 0:
                continue
            entry_price = float(entry_price)
        except Exception:
            continue

        cur_close, cur_date = _current_close(t)
        if cur_close is None or cur_date is None:
            # fetch 실패 시에도 DaysHeld만 ref_date로 갱신 (US는 ref_date+1)
            ref_date = _get_ref_trading_date(t)
            if ref_date is not None and signal_date is not None:
                try:
                    t_u = (t or "").upper()
                    is_kr = t_u and (".KS" in t_u or ".KQ" in t_u or t_u.startswith("^K"))
                    cur_d = ref_date + timedelta(days=1) if not is_kr else ref_date
                    days_held = max(1, (cur_d - signal_date).days + 1)
                    tr.loc[idx, "LastBarDate"] = cur_d
                    tr.loc[idx, "DaysHeld"] = days_held
                except Exception:
                    pass
            continue
        cur_close = float(cur_close)

        # DaysHeld 업데이트
        # 기본 규칙: (오늘 날짜 - SignalDate) + 1  (SignalDate 기준 실 보유 일수)
        prev_last_bar = _ensure_dt(row.get("LastBarDate", None))
        base = int(row.get("DaysHeld", 0) or 0)

        if signal_date is not None:
            try:
                delta_days = (cur_date - signal_date).days
                days_held = max(1, delta_days + 1)
            except Exception:
                # 계산 실패 시 기존 로직으로 폴백
                if prev_last_bar is None:
                    days_held = 1 if base <= 0 else base
                else:
                    if pd.isna(prev_last_bar):
                        prev_last_bar = cur_date - timedelta(days=1)
                    days_held = base + 1 if cur_date > prev_last_bar else base
        else:
            # SignalDate가 없으면 과거와 동일한 로직 유지
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
        if signal_date is not None and pd.notna(signal_date) and cur_date <= signal_date:
            # 신호 발생 당일이면 무조건 OPEN 유지
            continue
        if days_held < 2:
            # 최소 2일차부터만 exit 로직 적용
            continue

        # ✅ TOP PICK3 성과 추적: 15거래일 도달 시에만 CLOSED (조기 청산 없음)
        exit_reason = None
        if days_held >= max_hold_days:
            exit_reason = "TIME_EXIT(15D)"

        if exit_reason is not None:
            tr.loc[idx, "Status"] = "CLOSED"
            tr.loc[idx, "ExitDate"] = cur_date
            tr.loc[idx, "ExitPrice"] = float(cur_close)
            tr.loc[idx, "ReturnPct"] = float(ret_pct)
            tr.loc[idx, "ExitReason"] = exit_reason
            closed_today.append(tr.loc[idx].to_dict())

    if tracker_path:
        save_tracker(tr, tracker_path)
    else:
        save_tracker(tr)
    closed_df = pd.DataFrame(closed_today) if closed_today else pd.DataFrame()
    return tr, closed_df



def compute_cum_returns(tr: pd.DataFrame, today: datetime.date):
    """
    ✅ CLOSED 확정 수익률을 EntryPrice/ExitPrice로 재계산.
    ✅ 월간/연간/총 수익률: CLOSED 종목들의 누적(복리) 수익률로 표시.
    """
    if tr is None or tr.empty:
        return {"daily": 0.0, "monthly": 0.0, "yearly": 0.0, "total": 0.0}

    if "Status" not in tr.columns:
        return {"daily": 0.0, "monthly": 0.0, "yearly": 0.0, "total": 0.0}

    c = tr[tr["Status"] == "CLOSED"].copy()
    if c.empty:
        return {"daily": 0.0, "monthly": 0.0, "yearly": 0.0, "total": 0.0}

    need = ["ExitDate", "EntryPrice", "ExitPrice"]
    for col in need:
        if col not in c.columns:
            return {"daily": 0.0, "monthly": 0.0, "yearly": 0.0, "total": 0.0}

    c["ExitDate"] = pd.to_datetime(c["ExitDate"], errors="coerce").dt.date
    c["EntryPrice"] = pd.to_numeric(c["EntryPrice"], errors="coerce")
    c["ExitPrice"]  = pd.to_numeric(c["ExitPrice"],  errors="coerce")

    c = c.dropna(subset=["ExitDate", "EntryPrice", "ExitPrice"]).copy()
    c = c[(c["EntryPrice"] > 0) & (c["ExitPrice"] > 0)].copy()

    c["ReturnPctCalc"] = (c["ExitPrice"] / c["EntryPrice"] - 1.0) * 100.0
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
    ✅ OPEN(표에 있는) 종목들의 '현재 Return%' 평균 (TOP PICK 수익률용)
    - EntryPrice 대비 현재 종가 기준
    - 데이터 못 가져오는 종목은 제외
    """
    if tr is None or not isinstance(tr, pd.DataFrame) or tr.empty:
        return 0.0

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


def compute_open_daily_change_avg(tr: pd.DataFrame) -> float:
    """
    ✅ OPEN 종목들의 '오늘 하루 거래일 동안 변동된 수익률' 평균 (일간 수익률용)
    - 전일 종가 대비 현재가 변동분을 진입가 대비 %로 환산 후 평균
    - 오늘 선정된 종목(EntryDate/SignalDate = 오늘)은 제외 (Return% 0%인데 일간 집계하면 의미 없음)
    """
    if tr is None or not isinstance(tr, pd.DataFrame) or tr.empty:
        return 0.0
    if "Status" not in tr.columns or "Ticker" not in tr.columns or "EntryPrice" not in tr.columns:
        return 0.0

    open_df = tr[tr["Status"] == "OPEN"].copy()
    if open_df.empty:
        return 0.0

    changes = []
    for _, r in open_df.iterrows():
        t = str(r.get("Ticker", "")).upper().strip()
        sig_date = _ensure_dt(r.get("SignalDate")) or _ensure_dt(r.get("EntryDate"))
        ref_date = _get_ref_trading_date(t)
        if ref_date is not None and sig_date is not None and sig_date == ref_date:
            continue  # 오늘 선정된 종목 제외
        entry = r.get("EntryPrice", None)
        try:
            if not t or entry is None or not np.isfinite(float(entry)) or float(entry) <= 0:
                continue
        except Exception:
            continue
        df = _fetch_recent_close(t)
        if df is None or df.empty or len(df) < 2:
            continue
        entry_f = float(entry)
        prev_close = float(df["Close"].iloc[-2])
        cur_close = float(df["Close"].iloc[-1])
        # 오늘 하루 동안 변동된 수익률 = (현재가 - 전일종가) / 진입가 * 100
        daily_change_pct = (cur_close - prev_close) / entry_f * 100.0
        changes.append(daily_change_pct)

    if not changes:
        return 0.0
    return float(np.mean(changes))


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


def build_df2(df: pd.DataFrame, keep_all_rows: bool = False):
    """
    scanner.py에서 쓰는 지표 세팅과 동일.
    keep_all_rows=True: 상장 기간 짧은 종목(340봉 미만)용. dropna 생략 → 상장일~최근 거래일 전부 표시.
    """
    close = df["Close"]
    df["SMA20"] = sc.sma(close, 20)
    df["SMA50"] = sc.sma(close, 50)
    df["SMA150"] = sc.sma(close, 150)
    df["SMA200"] = sc.sma(close, 200)
    df["ATR14"] = sc.atr(df, 14)
    df["ADX14"] = sc.adx(df, 14)
    df["MACD_H"] = sc.macd_hist(close)
    df["RSI14"] = sc.rsi(close, 14)
    df2 = df.copy() if keep_all_rows else df.dropna().copy()
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
    # ✅ run_date가 없으면 파일명에서 만든다 (US: scan_snapshot_, KR: kr_scan_snapshot_)
    if "run_date" not in snap or not snap.get("run_date"):
        base = os.path.basename(snapshot_path).replace(".json", "")
        if base.startswith("kr_scan_snapshot_"):
            snap["run_date"] = base.replace("kr_scan_snapshot_", "")
        else:
            snap["run_date"] = base.replace("scan_snapshot_", "")
    return snap



def analyze_ticker_reco(ticker: str, shares: float = 1.0, avg_price: Optional[float] = None, entry_date=None):
    """
    ✅ 티커 분석(첫 클릭부터 안정):
    - yfinance가 첫 호출에 데이터가 짧게 내려오는/빈 값이 오는 케이스 방어
    - SMA200 포함하려면 최소 260봉 이상 필요(네 기준 유지)
    - entry_date 있으면 보유 기간 경고용 days_held 계산 후 holding_risk_review에 전달
    """
    ticker = str(ticker).upper().strip()

    # 1) lookback 넉넉히(900일) 해서 fetch. 상장 기간 짧은 종목은 오늘 봉 제외한 최대 봉만큼 표시
    lookback = int(max(getattr(cfg, "LOOKBACK_DAYS", 240), 900))

    # 2) 1차 fetch (min_rows=252 → 1년 차트용. 캐시에 240일만 있던 트래커 종목도 yf에서 1년치 받음)
    df = _fetch_price_inner(ticker, lookback, get_cache_buster(), min_rows=252, retries=2, debug_out=None)

    # 3) 2차 보강(1차에서 비었을 때만. 신규상장 SNDK 등 256봉만 있는 종목은 min_rows 낮춰 재시도)
    if df is None or df.empty:
        df = _fetch_price_inner(ticker, 1400, get_cache_buster(), min_rows=0, retries=1, debug_out=None)

    # 4) 최종 방어
    if df is None or df.empty:
        return {"error": "OHLCV 데이터가 없습니다. 티커를 확인하세요.", "ticker": ticker}

    # 5) 지표 계산. 340봉 미만이면 keep_all_rows → 상장일~최근 거래일 전부 표시(SNDK 256봉 등)
    keep_all = len(df) < 340
    df2 = build_df2(df, keep_all_rows=keep_all)
    if df2 is None or df2.empty or len(df2) < 1:
        return {"error": f"지표 계산 후 유효 데이터 없음 현재={0 if df2 is None else len(df2)}", "ticker": ticker}

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
        plan = sc.calc_trade_plan(df2, entry, ticker=ticker)
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

    # ✅ 1년 일봉 또는 상장 이후 전체 (340봉 미만이면 가용한 전부, SNDK 256봉 등)
    df_1y = df2.copy() if len(df2) <= 300 else df2.tail(252).copy()
    buy_signal_dates = []
    sell_signal_dates = []
    sell_entry_prices = []
    buy_reasons = []
    sell_reasons = []
    try:
        buy_signal_dates, sell_signal_dates, sell_entry_prices, buy_reasons, sell_reasons = sc.backtest_signal_dates(df2, ticker)
    except Exception:
        pass

    # ✅ tp.close: _current_close 단일 소스로 통일 (티커검색/트래커/포트폴리오 종가 일치)
    display_close, _ = _current_close(ticker)
    if display_close is not None and np.isfinite(display_close):
        cur_close = display_close

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
        "df_tail": df2.copy() if len(df2) <= 140 else df2.tail(30).copy(),
        "df_1y": df_1y,
        "buy_signal_dates": buy_signal_dates,
        "sell_signal_dates": sell_signal_dates,
        "sell_entry_prices": sell_entry_prices,
        "buy_reasons": buy_reasons,
        "sell_reasons": sell_reasons,
    }



@st.cache_data(show_spinner=False, ttl=60)
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


@st.cache_data(show_spinner=False, ttl=60)
def load_portfolio_cash(path: Optional[str] = None) -> float:
    """포트폴리오 현금 잔고 로드. path 없으면 portfolio_cash.txt"""
    p = path or os.path.join(BASE_DIR, "portfolio_cash.txt")
    if not os.path.exists(p):
        return 0.0
    try:
        with open(p, "r", encoding="utf-8") as f:
            return float(f.read().strip() or 0)
    except Exception:
        return 0.0

def save_portfolio_cash(value: float, path: Optional[str] = None):
    p = path or os.path.join(BASE_DIR, "portfolio_cash.txt")
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


def run_scanner_kr_subprocess(timeout_sec: int = 900):
    """한국 증시 스캐너(scanner_kr.py) 실행"""
    scanner_path = os.path.join(BASE_DIR, "scanner_kr.py")
    if not os.path.exists(scanner_path):
        return False, f"scanner_kr.py not found: {scanner_path}", "", ""

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    try:
        r = subprocess.run(
            [sys.executable, "-u", scanner_path, "--mode", "scan"],
            cwd=BASE_DIR,
            stdin=subprocess.DEVNULL,
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

# URL 쿼리 파라미터로 홈 이동 (뒤로가기 링크용)
if hasattr(st, "query_params"):
    goto = st.query_params.get("goto")
    if goto == "home":
        st.session_state["nav_page_radio"] = "🏠 홈"
        st.query_params.clear()
    elif goto == "us_scanner":
        st.session_state["nav_page_radio"] = "US Stock Scanner"
        st.query_params.clear()
    elif goto == "kr_scanner":
        st.session_state["nav_page_radio"] = "KR Stock Scanner"
        st.query_params.clear()

# 버튼 클릭 시 페이지 전환 (radio key 수정 전에 처리)
if st.session_state.get("_goto_us_scanner"):
    st.session_state["nav_page_radio"] = "US Stock Scanner"
    del st.session_state["_goto_us_scanner"]
if st.session_state.get("_goto_home"):
    st.session_state["nav_page_radio"] = "🏠 홈"
    del st.session_state["_goto_home"]
if st.session_state.get("_goto_kr_scanner"):
    st.session_state["nav_page_radio"] = "KR Stock Scanner"
    del st.session_state["_goto_kr_scanner"]

with st.sidebar:
    st.markdown("### 🧭 페이지")
    _nav_current = st.session_state.get("nav_page_radio", "🏠 홈")
    if _nav_current == "🇺🇸 미국 증시 스캐너":
        _nav_current = "US Stock Scanner"
    elif _nav_current == "🇰🇷 한국 증시 스캐너":
        _nav_current = "KR Stock Scanner"
    _nav_css = """
    <style>
    .nav-link { position:relative; display:flex; align-items:center; gap:8px; padding:8px 12px; margin:4px 0; border-radius:8px; text-decoration:none; color:#e2e8f0; font-size:0.95rem; transition:all 0.2s; overflow:hidden; }
    .nav-link:hover { background:rgba(59,130,246,0.2); }
    .nav-link.active { background:rgba(251,191,36,0.2); border:1px solid rgba(251,191,36,0.5); }
    .nav-link img.nav-flag { width:24px; height:16px; object-fit:contain; }
    .nav-link .flag-bg { position:absolute; inset:0; display:flex; align-items:center; justify-content:center; opacity:0; transition:opacity 0.25s ease; pointer-events:none; }
    .nav-link .flag-bg img { width:56px; height:38px; object-fit:contain; }
    .nav-link:hover .flag-bg { opacity:0.5; }
    .nav-link.us, .nav-link.kr { justify-content:flex-start; padding-left:36px; }
    .nav-link.us:hover { background:linear-gradient(135deg,rgba(60,59,110,0.35),rgba(178,34,52,0.18)); }
    .nav-link.kr:hover { background:linear-gradient(135deg,rgba(205,46,58,0.3),rgba(0,71,160,0.3)); }
    .nav-link.home { justify-content:center; }
    .nav-link.home .home-emoji-bg { position:absolute; inset:0; display:flex; align-items:center; justify-content:center; font-size:2rem; opacity:0; transition:opacity 0.25s ease; pointer-events:none; }
    .nav-link.home:hover .home-emoji-bg { opacity:0.6; }
    </style>
    """
    _home_cls = "nav-link active home" if _nav_current == "🏠 홈" else "nav-link home"
    _us_cls = "nav-link active us" if _nav_current == "US Stock Scanner" else "nav-link us"
    _kr_cls = "nav-link active kr" if _nav_current == "KR Stock Scanner" else "nav-link kr"
    st.markdown(_nav_css + f"""
    <div style="display:flex;flex-direction:column;gap:2px;">
    <a href="?goto=home" target="_self" class="{_home_cls}"><span class="home-emoji-bg">🏠</span>Home</a>
    <a href="?goto=us_scanner" target="_self" class="{_us_cls}"><span class="flag-bg"><img src="https://flagcdn.com/w160/us.png" alt=""></span><img class="nav-flag" src="https://flagcdn.com/24x18/us.png" alt=""> US Stock Scanner</a>
    <a href="?goto=kr_scanner" target="_self" class="{_kr_cls}"><span class="flag-bg"><img src="https://flagcdn.com/w160/kr.png" alt=""></span><img class="nav-flag" src="https://flagcdn.com/24x18/kr.png" alt=""> KR Stock Scanner</a>
    </div>
    """, unsafe_allow_html=True)
    page = _nav_current
    st.divider()
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

# =========================
# 홈 vs 미국 증시 스캐너 분기
# =========================
if page == "🏠 홈":
    st.markdown('<style>.main .block-container { padding-top: 0 !important; }</style>', unsafe_allow_html=True)
    _home_logo_path = os.path.join(BASE_DIR, "assets", "home_logo.png")
    if os.path.isfile(_home_logo_path):
        try:
            from PIL import Image
            import io
            import base64
            img = Image.open(_home_logo_path).convert("RGBA")
            arr = np.array(img)
            thresh = 40
            mask = (arr[:, :, 0] <= thresh) & (arr[:, :, 1] <= thresh) & (arr[:, :, 2] <= thresh)
            arr[mask, 3] = 0
            img_nobg = Image.fromarray(arr)
            buf = io.BytesIO()
            img_nobg.save(buf, format="PNG")
            home_logo_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
            st.markdown(
                '<div style="position:relative;height:140px;margin-top:-16px;"><img src="data:image/png;base64,' + home_logo_b64 + '" style="width:360px;position:absolute;top:-40px;left:-20px;display:block;" alt="Stock Scanner Home" /></div>',
                unsafe_allow_html=True,
            )
        except Exception:
            st.image(_home_logo_path, width=360)
    else:
        st.markdown("## 🏠 홈")
    st.markdown("---")
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        st.markdown("""
        <style>
        .home-nav-btn { position:relative; display:inline-block; width:100%; padding:12px 20px; font-size:1rem; font-weight:600; text-align:center;
            border-radius:12px; border:1px solid rgba(100,116,139,0.5); cursor:pointer; text-decoration:none; color:#e2e8f0;
            background:linear-gradient(135deg,rgba(30,58,95,0.9),rgba(49,46,129,0.9)); transition:all 0.3s ease;
            box-shadow:0 2px 8px rgba(0,0,0,0.3); margin-bottom:8px; overflow:hidden; }
        .home-nav-btn .flag-bg { position:absolute; inset:0; display:flex; align-items:center; justify-content:center;
            opacity:0; transition:opacity 0.3s ease; pointer-events:none; }
        .home-nav-btn .flag-bg img { width:80px; height:54px; object-fit:contain; }
        .home-nav-btn:hover { transform:scale(1.02); box-shadow:0 4px 16px rgba(59,130,246,0.4); }
        .home-nav-btn:hover .flag-bg { opacity:0.45; }
        .home-nav-btn.us:hover { background:linear-gradient(135deg,rgba(60,59,110,0.35),rgba(178,34,52,0.18)); }
        .home-nav-btn.kr:hover { background:linear-gradient(135deg,rgba(205,46,58,0.3),rgba(0,71,160,0.3)); }
        </style>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;justify-items:start;">
        <a href="?goto=us_scanner" target="_self" class="home-nav-btn us"><span class="flag-bg"><img src="https://flagcdn.com/w160/us.png" alt="US"></span>US Stock Scanner로 이동</a>
        <a href="?goto=kr_scanner" target="_self" class="home-nav-btn kr"><span class="flag-bg"><img src="https://flagcdn.com/w160/kr.png" alt="KR"></span>KR Stock Scanner로 이동</a>
        </div>
        """, unsafe_allow_html=True)
    st.markdown("---")

    # 미국 시장 차트: VOO / QQQ / 다우 지수
    st.markdown(
        "<div style='background:rgba(15,23,42,0.95);color:#f1f5f9;padding:12px 16px;border-radius:10px;"
        "box-shadow:0 0 24px rgba(59,130,246,0.3);border:1px solid rgba(100,116,139,0.5);"
        "margin:0.5rem 0;font-size:1.1rem;font-weight:700;'><img src='https://flagcdn.com/w40/us.png' style='height:1.1em;vertical-align:middle;'> 미국 시장 차트 (최근 3개월)</div>",
        unsafe_allow_html=True,
    )
    h1_c1, h1_c2, h1_c3 = st.columns(3)
    with h1_c1:
        voo_df = fetch_price("VOO", 400, get_cache_buster())
        plot_candles(voo_df, "S&P500 (3M Line)", chart_key="home_voo", months=3, kind="line", show_ma=False, dark=True)
    with h1_c2:
        qqq_df = fetch_price("QQQ", 400, get_cache_buster())
        plot_candles(qqq_df, "NASDAQ 100 (3M Line)", chart_key="home_qqq", months=3, kind="line", show_ma=False, dark=True)
    with h1_c3:
        dow_df = fetch_price("^DJI", 400, get_cache_buster())
        plot_candles(dow_df, "DOW JONES (3M Line)", chart_key="home_dow", months=3, kind="line", show_ma=False, dark=True)

    # 한국 시장 차트: 코스피 / 코스닥 / 환율
    st.markdown(
        "<div style='background:rgba(15,23,42,0.95);color:#f1f5f9;padding:12px 16px;border-radius:10px;"
        "box-shadow:0 0 24px rgba(59,130,246,0.3);border:1px solid rgba(100,116,139,0.5);"
        "margin:0.5rem 0;font-size:1.1rem;font-weight:700;'><img src='https://flagcdn.com/w40/kr.png' style='height:1.1em;vertical-align:middle;'> 한국 시장 차트 (최근 3개월)</div>",
        unsafe_allow_html=True,
    )
    h2_c1, h2_c2, h2_c3 = st.columns(3)
    with h2_c1:
        kospi_df = fetch_price("^KS11", 400, get_cache_buster())
        plot_candles(kospi_df, "코스피 (3M Line)", chart_key="home_kospi", months=3, kind="line", show_ma=False, dark=True)
    with h2_c2:
        kosdaq_df = fetch_price("^KQ11", 400, get_cache_buster())
        plot_candles(kosdaq_df, "코스닥 (3M Line)", chart_key="home_kosdaq", months=3, kind="line", show_ma=False, dark=True)
    with h2_c3:
        fx_df = _get_usdkrw_df(lookback_days=900)
        plot_candles(fx_df, "USD/KRW (3M Line)", chart_key="home_usdkrw", months=3, kind="line", show_ma=False, dark=True)

    # 미국/한국 증시 주요 기사 (각 기사별 접었다 펼 수 있음)
    _news_ts = st.session_state.get("news_refresh_ts", "")
    if st.button("📰 기사 새로고침", key="news_refresh_btn"):
        st.session_state["news_refresh_ts"] = datetime.now().isoformat()
        st.rerun()
    us_news, us_err = _fetch_market_news("us", "")
    kr_news, kr_err = _fetch_market_news("kr", "")

    with st.spinner("기사 요약 불러오는 중…"):
        _cb = st.session_state.get("news_refresh_ts", "")
        us_news = _enrich_news_with_summaries(us_news, _cb)
        kr_news = _enrich_news_with_summaries(kr_news, _cb)

    st.markdown("**<img src='https://flagcdn.com/w40/us.png' style='height:1.1em;vertical-align:middle;'> 미국 증시 주요 기사**", unsafe_allow_html=True)
    if not us_news:
        st.caption("기사를 불러올 수 없습니다. 네트워크를 확인하거나 잠시 후 다시 시도해 주세요.")
        if us_err:
            with st.expander("오류 상세 (미국)", expanded=False):
                st.code(us_err)
    else:
        for i, n in enumerate(us_news, 1):
            t = _normalize_news_title(n.get("title", "") or "")
            with st.expander(f"{i}. {t}", expanded=False):
                if n.get("summary"):
                    st.markdown(n["summary"].replace("\n", "\n\n"))
                st.markdown(f"[**기사 보기**]({n['link']})")
                caps = []
                if n.get("source"):
                    caps.append(f"출처: {n['source']}")
                if n.get("pub"):
                    caps.append(f"발행: {n['pub']}")
                if caps:
                    st.caption(" · ".join(caps))

    st.markdown("**<img src='https://flagcdn.com/w40/kr.png' style='height:1.1em;vertical-align:middle;'> 한국 증시 주요 기사**", unsafe_allow_html=True)
    if not kr_news:
        st.caption("기사를 불러올 수 없습니다. 네트워크를 확인하거나 잠시 후 다시 시도해 주세요.")
        if kr_err:
            with st.expander("오류 상세 (한국)", expanded=False):
                st.code(kr_err)
    else:
        for i, n in enumerate(kr_news, 1):
            t = _normalize_news_title(n.get("title", "") or "")
            with st.expander(f"{i}. {t}", expanded=False):
                if n.get("summary"):
                    st.markdown(n["summary"].replace("\n", "\n\n"))
                st.markdown(f"[**기사 보기**]({n['link']})")
                caps = []
                if n.get("source"):
                    caps.append(f"출처: {n['source']}")
                if n.get("pub"):
                    caps.append(f"발행: {n['pub']}")
                if caps:
                    st.caption(" · ".join(caps))

    st.markdown("---")
    st.stop()

if page == "KR Stock Scanner":
    # 한국 증시 스캐너: 뒤로가기 + 탭 구조 (US와 동일, 통화는 KRW)
    st.markdown(
        '<form action="" method="get" target="_self" style="position:fixed;bottom:24px;right:24px;z-index:9999;">'
        '<input type="hidden" name="goto" value="home" />'
        '<button type="submit" style="padding:10px 16px;background:rgba(30,41,59,0.95);color:#e2e8f0;border-radius:8px;'
        'font-size:0.9rem;border:1px solid rgba(100,116,139,0.4);cursor:pointer;'
        'box-shadow:0 2px 8px rgba(0,0,0,0.3);">← 뒤로가기</button></form>',
        unsafe_allow_html=True,
    )
    # 로고: 크기 축소, 탭과 겹치지 않도록 위쪽 배치
    _kr_logo_path = os.path.join(BASE_DIR, "assets", "kr_stock_scanner_logo.png")
    _kr_logo_width = int(240)
    if os.path.isfile(_kr_logo_path):
        try:
            from PIL import Image
            import io
            import base64
            img = Image.open(_kr_logo_path).convert("RGBA")
            arr = np.array(img)
            thresh = 40
            mask = (arr[:, :, 0] <= thresh) & (arr[:, :, 1] <= thresh) & (arr[:, :, 2] <= thresh)
            arr[mask, 3] = 0
            img_nobg = Image.fromarray(arr)
            buf = io.BytesIO()
            img_nobg.save(buf, format="PNG")
            kr_logo_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
            st.markdown(
                '<div style="height:100px;position:relative;">'
                '<img src="data:image/png;base64,' + kr_logo_b64 + '" style="width:' + str(_kr_logo_width) + 'px;position:absolute;top:-60px;left:-35px;display:block;" alt="KR Stock Scanner" />'
                '</div>',
                unsafe_allow_html=True,
            )
        except Exception:
            _c1, _c2 = st.columns([1, 10])
            with _c1:
                st.image(_kr_logo_path, width=_kr_logo_width)
    else:
        st.title("🇰🇷 한국 증시 스캐너")
    kr_tab1, kr_tab2, kr_tab3 = st.tabs(["🔎 종목검색", "📁 포트폴리오 관리", "🚀 스캔 실행"])

    with kr_tab1:
        st.subheader("종목검색")
        if "kr_ticker_input" not in st.session_state or st.session_state.get("kr_ticker_input") == "005930.KS":
            st.session_state["kr_ticker_input"] = "삼성전자"
        kr_ticker = st.text_input("종목명 또는 종목코드", key="kr_ticker_input",
            placeholder="예: 카카오, 삼성전자 또는 035720.KQ",
            help="종목명(카카오, 삼성전자 등) 또는 종목코드(005930.KS, 035720.KQ) 입력")
        kr_ticker = (kr_ticker or "").strip()

        btn_col1, btn_col2 = st.columns([1, 1])
        with btn_col1:
            if kr_ticker and st.button("분석하기", key="kr_analyze_btn", type="primary"):
                kr_resolved = _resolve_kr_name_or_ticker(kr_ticker)
                if not kr_resolved:
                    st.error("종목명 또는 종목코드를 확인하세요.")
                else:
                    res = analyze_ticker_reco(kr_resolved, shares=1.0, avg_price=None)
                    if "error" in res:
                        st.error(res["error"])
                    st.session_state["kr_ticker_result"] = res
                    st.session_state["kr_show_ticker_result"] = True
        with btn_col2:
            if st.session_state.get("kr_show_ticker_result") and st.session_state.get("kr_ticker_result"):
                if st.button("닫기", key="kr_close_ticker_result", type="secondary"):
                    st.session_state["kr_show_ticker_result"] = False
                    st.session_state.pop("kr_ticker_result", None)

        # 검색 결과를 트렌딩 20개 위에 표시
        if st.session_state.get("kr_show_ticker_result") and st.session_state.get("kr_ticker_result"):
            res = st.session_state["kr_ticker_result"]
            if "error" in res:
                st.stop()
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
            st.caption("기준가: **현재가** (티커만 입력 시)")
            st.markdown("**1·2·3 목표가(익절)**")
            st.markdown(f"- 현재가: {_fmt_currency(close, 'kr')} → 1차: {_fmt_currency(t1, 'kr')} · 2차: {_fmt_currency(t2, 'kr')} · 3차: {_fmt_currency(t3, 'kr')}")
            st.markdown("**1·2·3 손절가** (1차 ≥ 2차 ≥ 3차)")
            st.markdown(f"- 1차(트레일 이탈): {_fmt_currency(stop1, 'kr')} · 2차(중간 -{stop_2nd_pct:.0f}%): {_fmt_currency(stop2, 'kr')} · 3차(전액 -{loss_cut_pct:.0f}%): {_fmt_currency(stop3, 'kr')}")
            if (suggested_pct is not None and suggested_pct > 0) or (suggested_reason and str(suggested_reason).strip()):
                pct_display = (suggested_pct * 100) if suggested_pct is not None and suggested_pct <= 1.0 else suggested_pct
                st.markdown(f"- **권장 매도 비율(스케일아웃)**: {pct_display:.0f}% — {suggested_reason} (보유 수량 중 이 비율만큼 매도 권장)")
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
            with st.expander("종목 상태 상세 (Ticker State)"):
                _p = lambda s: f"<p style='color:#f1f5f9;font-size:1.05rem;margin:0.22rem 0;'>{s}</p>"
                df_tail = res.get("df_tail")
                if df_tail is not None and not df_tail.empty and len(df_tail) >= 2:
                    last = df_tail.iloc[-1]
                    prev = df_tail.iloc[-2]
                    close_val = float(last.get("Close", 0)) if "Close" in last else 0
                    open_val = float(last.get("Open", 0)) if "Open" in last else 0
                    high_val = float(last.get("High", 0)) if "High" in last else 0
                    low_val = float(last.get("Low", 0)) if "Low" in last else 0
                    prev_close = float(prev.get("Close", 0)) if "Close" in prev else close_val
                    col_t1, col_t2, col_t3 = st.columns(3)
                    def _add_item(col, label, val_str, badge):
                        col.markdown(_p(f"{label}: {val_str} {badge}"), unsafe_allow_html=True)
                    with col_t1:
                        macd_h = last.get("MACD_H")
                        if macd_h is not None and np.isfinite(float(macd_h)):
                            macd_h_f = float(macd_h)
                            _add_item(col_t1, "MACD", "상승" if macd_h_f > 0 else "하락", "✅ good" if macd_h_f > 0 else "❌ bad")
                        rsi_val = last.get("RSI14")
                        if rsi_val is not None and np.isfinite(float(rsi_val)):
                            rsi_f = float(rsi_val)
                            if rsi_f < 30: _add_item(col_t1, "RSI", f"{rsi_f:.1f} (과매도)", "✅ good")
                            elif rsi_f > 70: _add_item(col_t1, "RSI", f"{rsi_f:.1f} (과매수)", "❌ bad")
                            else: _add_item(col_t1, "RSI", f"{rsi_f:.1f} (적정)", "➖ normal")
                        atr14 = last.get("ATR14")
                        if atr14 is not None and close_val > 0 and np.isfinite(float(atr14)):
                            atr_pct = float(atr14) / close_val * 100
                            if atr_pct < 2: _add_item(col_t1, "변동성(ATR%)", f"{atr_pct:.2f}% (낮음)", "❌ bad")
                            elif atr_pct > 6: _add_item(col_t1, "변동성(ATR%)", f"{atr_pct:.2f}% (높음)", "❌ bad")
                            else: _add_item(col_t1, "변동성(ATR%)", f"{atr_pct:.2f}% (적정)", "➖ normal")
                        adx_val = last.get("ADX14")
                        if adx_val is not None and np.isfinite(float(adx_val)):
                            adx_f = float(adx_val)
                            if adx_f >= 20: _add_item(col_t1, "ADX", f"{adx_f:.1f} (추세 있음)", "✅ good")
                            elif adx_f < 15: _add_item(col_t1, "ADX", f"{adx_f:.1f} (횡보)", "❌ bad")
                            else: _add_item(col_t1, "ADX", f"{adx_f:.1f} (보통)", "➖ normal")
                        sma20 = last.get("SMA20")
                        if sma20 is not None and close_val > 0 and np.isfinite(float(sma20)):
                            above = close_val > float(sma20)
                            _add_item(col_t1, "SMA20", "이평 위" if above else "이평 아래", "✅ good" if above else "❌ bad")
                        sma50 = last.get("SMA50")
                        if sma50 is not None and close_val > 0 and np.isfinite(float(sma50)):
                            above = close_val > float(sma50)
                            _add_item(col_t1, "SMA50", "이평 위" if above else "이평 아래", "✅ good" if above else "❌ bad")
                        sma200 = last.get("SMA200")
                        if sma200 is not None and close_val > 0 and np.isfinite(float(sma200)):
                            above = close_val > float(sma200)
                            _add_item(col_t1, "SMA200", "이평 위" if above else "이평 아래", "✅ good" if above else "❌ bad")
                    with col_t2:
                        s50, s150, s200 = last.get("SMA50"), last.get("SMA150"), last.get("SMA200")
                        if all(x is not None and np.isfinite(float(x)) for x in (s50, s150, s200)):
                            stack = float(s50) > float(s150) > float(s200)
                            _add_item(col_t2, "이평 정렬", "50>150>200" if stack else "정렬 아님", "✅ good" if stack else "❌ bad")
                        if len(df_tail) >= 21 and "High" in df_tail.columns:
                            high20 = float(df_tail["High"].iloc[-21:-1].max())
                            if high20 > 0:
                                near_pct = close_val / high20 * 100
                                if near_pct >= 98: _add_item(col_t2, "20D고점 근접", f"{near_pct:.1f}% (근접)", "✅ good")
                                elif near_pct < 95: _add_item(col_t2, "20D고점 근접", f"{near_pct:.1f}% (멀음)", "➖ normal")
                                else: _add_item(col_t2, "20D고점 근접", f"{near_pct:.1f}%", "➖ normal")
                        if "Volume" in df_tail.columns and len(df_tail) >= 20:
                            vol = float(last.get("Volume", 0))
                            vol20 = float(df_tail["Volume"].tail(20).mean())
                            if vol20 > 0:
                                vol_ratio = vol / vol20
                                if vol_ratio >= 1.5: _add_item(col_t2, "거래량비(Vol/20d)", f"{vol_ratio:.2f}x (강함)", "✅ good")
                                elif vol_ratio < 0.7: _add_item(col_t2, "거래량비(Vol/20d)", f"{vol_ratio:.2f}x (약함)", "❌ bad")
                                else: _add_item(col_t2, "거래량비(Vol/20d)", f"{vol_ratio:.2f}x", "➖ normal")
                        if high_val > low_val and high_val > 0:
                            rng = high_val - low_val
                            upper_wick = (high_val - max(open_val, close_val)) / rng
                            if upper_wick <= 0.3: _add_item(col_t2, "윗꼬리 비율", f"{upper_wick:.2f} (낮음)", "✅ good")
                            elif upper_wick >= 0.6: _add_item(col_t2, "윗꼬리 비율", f"{upper_wick:.2f} (높음)", "❌ bad")
                            else: _add_item(col_t2, "윗꼬리 비율", f"{upper_wick:.2f}", "➖ normal")
                        bullish = close_val > open_val
                        _add_item(col_t2, "캔들", "양봉" if bullish else "음봉", "✅ good" if bullish else "❌ bad")
                    with col_t3:
                        if len(df_tail) >= 21 and "High" in df_tail.columns:
                            high20_prev = float(df_tail["High"].iloc[-21:-1].max())
                            if high20_prev > 0:
                                close_confirm = close_val >= high20_prev * 1.001
                                _add_item(col_t3, "종가확인(돌파)", "고점 위 마감" if close_confirm else "미확인", "✅ good" if close_confirm else "➖ normal")
                        if sma50 is not None and sma200 is not None and np.isfinite(float(sma50)) and np.isfinite(float(sma200)):
                            uptrend = float(sma50) > float(sma200)
                            _add_item(col_t3, "추세(50>200)", "상승" if uptrend else "하락", "✅ good" if uptrend else "❌ bad")
                else:
                    st.markdown(_p("지표 데이터가 부족합니다."), unsafe_allow_html=True)
            if res.get("df_1y") is not None and not res["df_1y"].empty:
                st.markdown(
                    "<div style='background:rgba(15,23,42,0.95);color:#f1f5f9;padding:12px 16px;border-radius:10px;"
                    "box-shadow:0 0 24px rgba(59,130,246,0.3);border:1px solid rgba(100,116,139,0.5);"
                    "margin:0.5rem 0;font-size:1.1rem;font-weight:700;'>📊 최근 1년 캔들 · 매수 ▲ / 매도 ▼ 신호</div>",
                    unsafe_allow_html=True,
                )
                plot_candles_with_signals(
                    res["df_1y"], f"{res['ticker']} 최근 1년 (매수/매도 신호)",
                    res.get("buy_signal_dates") or [], res.get("sell_signal_dates") or [],
                    res.get("sell_entry_prices") or [], chart_key="kr_ticker_1y_signals", dark=True, is_kr=True,
                )
            st.markdown("**최근 30봉 데이터(지표 포함)**")
            df_tail = res.get("df_tail")
            _render_aggrid(pd.DataFrame() if df_tail is None else df_tail, key="kr_aggrid_df_tail", kr_currency=True)

            ticker_news, _ = _fetch_ticker_news(res.get("ticker", ""), is_kr=True, max_items=5)
            with st.spinner("기사 요약 불러오는 중…"):
                ticker_news = _enrich_news_with_summaries(ticker_news, st.session_state.get("news_refresh_ts", ""))
            _render_ticker_news(ticker_news, res.get("ticker", ""))

        st.divider()
        st.markdown(
            "<div style='background:rgba(15,23,42,0.95);color:#f1f5f9;padding:12px 16px;border-radius:10px;"
            "box-shadow:0 0 24px rgba(59,130,246,0.3);border:1px solid rgba(100,116,139,0.5);"
            "margin:0.5rem 0;font-size:1.1rem;font-weight:700;'>📊 오늘 가장 많이 검색한 한국증시 티커 20개</div>",
            unsafe_allow_html=True,
        )
        _kr_trend_df = _fetch_trending_kr_tickers(20)
        if not _kr_trend_df.empty:
            _kr_disp = _kr_trend_df[["순위", "Ticker", "종목명", "현재가", "등락률(%)"]].copy()
            _render_tracker_style_table(_kr_disp, pct_colors=True, col_widths=["5%", "12%", "38%", "22%", "23%"], kr_currency=True)
        else:
            st.info("한국 증시 트렌딩 티커 데이터를 불러올 수 없습니다.")
        st.divider()
        st.markdown("## 🧾 TOP PICK3 중 BUY 신호 성과 추적 (최대 15거래일)")
        _kr_snap_path = get_latest_snapshot(SNAPSHOT_KR_PATTERN)
        if _kr_snap_path is None:
            st.info("한국 증시 스캔 데이터가 없습니다. 스캔 실행 탭에서 스캔을 실행해 주세요.")
        else:
            snap = load_scan_snapshot_only(_kr_snap_path)
            if "error" in snap:
                st.error(f"스냅샷 에러: {snap.get('error')} | path={snap.get('snapshot_path')}")
            else:
                def _df(x):
                    return x if isinstance(x, pd.DataFrame) else pd.DataFrame(x)
                top3 = _df(snap.get("top_picks"))
                if top3.empty:
                    st.info("TOP PICK3 후보가 없습니다.")
                else:
                    if "Entry" not in top3.columns:
                        st.warning("top_picks에 'Entry' 컬럼이 없습니다.")
                    else:
                        top3_buy = top3[top3["Entry"].astype(str).isin(["BUY_BREAKOUT", "BUY_PULLBACK"])].copy()
                        if "Promoted" in top3_buy.columns:
                            top3_buy = top3_buy[~top3_buy["Promoted"].fillna(False).astype(bool)].copy()
                        for col in ["PromoTag", "Tag", "Note", "Reasons", "EntryHint"]:
                            if col in top3_buy.columns:
                                top3_buy = top3_buy[~top3_buy[col].astype(str).str.contains("PROMOTED|BUY_PROMOTED", case=False, na=False)].copy()
                        top3_buy_tickers = top3_buy["Ticker"].astype(str).str.upper().tolist() if "Ticker" in top3_buy.columns else []
                        if not top3_buy_tickers:
                            seeded = seed_tracker_from_recent_snapshots(max_files=120, max_seed=3, snapshot_pattern=SNAPSHOT_KR_PATTERN, tracker_path=TRACKER_KR_CSV)
                            if seeded:
                                st.info(f"스냅샷 기반으로 tracker를 복구했습니다: {', '.join(seeded)}")
                                top3_buy_tickers = seeded[:]
                            else:
                                st.warning("스냅샷에서 복구할 BUY 종목을 찾지 못했습니다.")
                        run_date = _parse_run_date(snap.get("run_date"))
                        tr_all, closed_today = update_tracker_with_today(top3_buy_tickers, max_hold_days=15, run_date=run_date, tracker_path=TRACKER_KR_CSV, snapshot_pattern=SNAPSHOT_KR_PATTERN)
                        today = datetime.utcnow().date()
                        cum = compute_cum_returns(tr_all, today=today)
                        top_pick_ret = compute_open_avg_return(tr_all)
                        daily_change_avg = compute_open_daily_change_avg(tr_all)
                        closed_count = int((tr_all["Status"] == "CLOSED").sum()) if "Status" in tr_all.columns else 0
                        k0, k1, k2, k3, k4, k5 = st.columns(6)
                        k0.metric("TOP PICK 수익률", f"{top_pick_ret:.2f}%")
                        k1.metric("일간 수익률", f"{daily_change_avg:.2f}%")
                        k2.metric("월간 수익률(누적)", f"{cum['monthly']:.2f}%")
                        k3.metric("연간 수익률(누적)", f"{cum['yearly']:.2f}%")
                        k4.metric("총 수익률(누적)", f"{cum['total']:.2f}%")
                        k5.metric("추적 완료 종목", f"{closed_count}개")
                        st.caption("※ TOP PICK 수익률: 현재 보유(OPEN) 종목들의 수익률 평균. 일간 수익률: 오늘 거래일 동안 변동된 수익률의 평균. 월간/연간/총 수익률: 해당 기간 CLOSED 종목들의 누적(복리) 수익률.")
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
                                    "Name": TICKER_TO_NAME.get(str(t).upper(), str(t)),
                                    "SignalDate": r.get("SignalDate"), "EntryDate": r.get("EntryDate"),
                                    "EntryPrice(PrevClose)": entry, "Close(Now)": float(cur_close),
                                    "Return%": round(ret, 2), "DaysHeld": int(r.get("DaysHeld", 0)),
                                })
                            _render_tracker_aggrid(rows, use_name=True, price_no_decimals=True)
                        st.markdown("### ✅ 오늘 종료(CLOSED)된 종목(있으면 표시)")
                        if closed_today is None or closed_today.empty:
                            st.info("오늘 종료된 종목 없음")
                        else:
                            _closed = closed_today.copy()
                            for c in ["Ticker", "SignalDate", "EntryDate", "EntryPrice", "ExitDate", "ExitPrice", "ReturnPct", "ExitReason"]:
                                if c not in _closed.columns:
                                    _closed[c] = ""
                            _closed["Name"] = _closed["Ticker"].map(lambda x: TICKER_TO_NAME.get(str(x).upper(), str(x)))
                            show_cols = ["Name", "SignalDate", "EntryDate", "EntryPrice", "ExitDate", "ExitPrice", "ReturnPct", "ExitReason"]
                            _render_aggrid(_closed[show_cols], key="kr_aggrid_closed_today", kr_currency=True)

    with kr_tab2:
        st.subheader("포트폴리오 관리")
        dfp = load_positions(POSITIONS_KR_PATH)
        if st.session_state.pop("kr_portfolio_saved", False):
            st.success("저장되었습니다.")
        st.markdown("### 현재 포트폴리오")
        if dfp.empty:
            st.info("포트폴리오가 비어있습니다.")
            cash_loaded = load_portfolio_cash(PORTFOLIO_CASH_KR_PATH)
            st.metric("현금", _fmt_currency(cash_loaded, "kr"))
        else:
            rows = []
            entry_date_map = {}
            try:
                tr = load_tracker(TRACKER_KR_CSV)
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
                        if risk_action in ("SELL_TRAIL", "SELL_TREND"):
                            recommend_text = "매도(하락 추세 전환)"
                        elif risk_action == "SELL_LOSS_CUT":
                            recommend_text = "매도(손절)"
                        elif risk_action == "SELL_STRUCTURE_BREAK":
                            recommend_text = "매도(구조 붕괴)"
                        else:
                            if (close is not None and t3 is not None and np.isfinite(close) and np.isfinite(t3) and close >= t3):
                                recommend_text = f"전량 매도(3차 목표 달성! {_fmt(t3)})"
                            elif (close is not None and t2 is not None and np.isfinite(close) and np.isfinite(t2) and close >= t2):
                                recommend_text = f"부분매도(2차 목표 달성! {_fmt(t2)})"
                            elif (close is not None and t1 is not None and np.isfinite(close) and np.isfinite(t1) and close >= t1):
                                recommend_text = f"부분 매도(1차 목표 달성! {_fmt(t1)})"
                            else:
                                if base_reco == "ADD_BUY":
                                    recommend_text = f"추가매수 + 목표가 상향(1차 목표가 {_fmt(t1)})"
                                else:
                                    recommend_text = f"보유(1차 목표가 {_fmt(t1)})"
                except Exception:
                    recommend_text = "분석 실패"
                    t1, t2, t3 = None, None, None
                    stop1, stop2, stop3 = None, None, None
                    risk_action = ""
                    rec = {}
                cur_close, _ = _current_close(t)
                ret_pct = None
                try:
                    if cur_close is not None and np.isfinite(float(cur_close)) and avg_price > 0:
                        ret_pct = (float(cur_close) / float(avg_price) - 1) * 100.0
                except Exception:
                    ret_pct = None
                rows.append({
                    "Ticker": t, "Shares": shares, "AvgPrice": avg_price,
                    "ClosePrice": (round(float(cur_close), 0) if cur_close is not None and np.isfinite(float(cur_close)) else None),
                    "Return%": (round(ret_pct, 2) if ret_pct is not None else np.nan),
                    "Recommend": recommend_text,
                    "risk_action": risk_action,
                    "T1": t1, "T2": t2, "T3": t3,
                    "Stop1": stop1, "Stop2": stop2, "Stop3": stop3,
                })
            pf_df = pd.DataFrame(rows)
            # KR: 종목명 표시 (Ticker → Name)
            pf_df["Name"] = pf_df["Ticker"].map(lambda x: TICKER_TO_NAME.get(str(x).upper(), str(x)))
            display_cols = ["Name", "Shares", "AvgPrice", "ClosePrice", "Return%", "Recommend"]
            _render_tracker_style_table(pf_df[display_cols], pct_colors=True, kr_currency=True)
            inv_total = sum(float(r["Shares"]) * float(r["AvgPrice"]) for r in rows)
            cur_total = sum(float(r["Shares"]) * (float(r["ClosePrice"]) if r.get("ClosePrice") is not None and np.isfinite(r.get("ClosePrice")) else float(r["AvgPrice"])) for r in rows)
            inv_return = cur_total - inv_total
            rets = [r["Return%"] for r in rows if r.get("Return%") is not None and np.isfinite(r.get("Return%"))]
            avg_ret = sum(rets) / len(rets) if rets else 0.0
            cash_display = load_portfolio_cash(PORTFOLIO_CASH_KR_PATH)
            balance = inv_total + inv_return + cash_display
            c1, c2, c3, c4, c5 = st.columns([1.25, 0.75, 1, 1, 1])
            with c1:
                st.metric("투자금", _fmt_currency(inv_total, "kr"))
            with c2:
                st.metric("평균 수익률", f"{avg_ret:.1f}%")
            with c3:
                st.metric("투자 수익", _fmt_currency(inv_return, "kr"))
            with c4:
                st.metric("현금", _fmt_currency(cash_display, "kr"))
            with c5:
                st.metric("잔고", _fmt_currency(balance, "kr"))
            pie_labels = []
            pie_values = []
            for r in rows:
                t = str(r.get("Ticker", "")).strip()
                sh = float(r.get("Shares", 0) or 0)
                close = r.get("ClosePrice")
                if t and sh > 0 and close is not None and np.isfinite(float(close)):
                    v = sh * float(close)
                    pie_labels.append(TICKER_TO_NAME.get(t, t))
                    pie_values.append(v)
            if cash_display is not None and float(cash_display) >= 0:
                pie_labels.append("현금")
                pie_values.append(float(cash_display))
            total_pie = sum(pie_values)
            if total_pie > 0:
                ticker_colors = ["#3b82f6", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16", "#f97316", "#6366f1", "#14b8a6"]
                colors = [ticker_colors[i % len(ticker_colors)] for i in range(len(pie_labels) - 1)] + ["#22c55e"]
                fig_pie = go.Figure(data=[go.Pie(
                    labels=pie_labels, values=pie_values, hole=0.4,
                    marker=dict(colors=colors, line=dict(color="rgba(15,23,42,0.9)", width=1.5)),
                    textinfo="none", hovertemplate="%{label}: %{value:,.0f} (%{percent})<extra></extra>",
                )])
                fig_pie.update_layout(
                    showlegend=False, margin=dict(l=20, r=20, t=30, b=20),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=320,
                    font=dict(color="#e2e8f0", size=12),
                    annotations=[dict(text="현재가치 기준", showarrow=False, font=dict(size=14), x=0.5, y=0.5)],
                )
                pct_list = [(l, (v / total_pie) * 100) for l, v in zip(pie_labels, pie_values)]
                col_pie, col_ratio = st.columns([1.2, 1])
                with col_pie:
                    st.plotly_chart(fig_pie, use_container_width=True, key="kr_portfolio_pie")
                with col_ratio:
                    st.markdown("**비율 (현재가치 기준)**")
                    for label, pct in pct_list:
                        idx = pie_labels.index(label) if label in pie_labels else 0
                        c = colors[idx] if idx < len(colors) else "#e2e8f0"
                        box = f"<span style='display:inline-block;width:12px;height:12px;background-color:{c};margin-right:8px;vertical-align:middle;border-radius:2px;'></span>"
                        st.markdown(f"{box} <span style='color:#e2e8f0'>{html.escape(label)}: **{pct:.1f}%**</span>", unsafe_allow_html=True)
            st.caption("기준가: **매수가(평단가)**")
            if "kr_portfolio_tp_override" not in st.session_state:
                st.session_state["kr_portfolio_tp_override"] = {}
            if "kr_portfolio_stop_override" not in st.session_state:
                st.session_state["kr_portfolio_stop_override"] = {}
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
                    ov = st.session_state["kr_portfolio_tp_override"].get(t, {})
                    target_rows.append({
                        "Name": TICKER_TO_NAME.get(t, t),
                        "1차 목표가": ov.get("t1") if ov else _num(r.get("T1")),
                        "2차 목표가": ov.get("t2") if ov else _num(r.get("T2")),
                        "3차 목표가": ov.get("t3") if ov else _num(r.get("T3")),
                    })
                if target_rows:
                    _tp_df = pd.DataFrame(target_rows)
                    _render_tracker_style_table(_tp_df, col_widths=_TP_STOP_COL_WIDTHS, kr_currency=True)
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
                    ov = st.session_state["kr_portfolio_stop_override"].get(t, {})
                    stop_rows.append({
                        "Name": TICKER_TO_NAME.get(t, t),
                        "1차 손절가": ov.get("s1") if ov else _num(r.get("Stop1")),
                        "2차 손절가": ov.get("s2") if ov else _num(r.get("Stop2")),
                        "3차 손절가(전액)": ov.get("s3") if ov else _num(r.get("Stop3")),
                    })
                if stop_rows:
                    _stop_df = pd.DataFrame(stop_rows)
                    _render_tracker_style_table(_stop_df, col_widths=_TP_STOP_COL_WIDTHS, kr_currency=True)
                else:
                    st.caption("데이터 없음")
            sell_tp_actions = ("SELL_TRAIL", "SELL_TREND", "SELL_STRUCTURE_BREAK", "SELL_LOSS_CUT", "TAKE_PROFIT")
            sell_tp_rows = [r for r in rows if r.get("risk_action") in sell_tp_actions]
            st.subheader("SELL / TAKE PROFIT 후보")
            if sell_tp_rows:
                _st_df = pd.DataFrame(sell_tp_rows)
                _st_df["Name"] = _st_df["Ticker"].map(lambda x: TICKER_TO_NAME.get(str(x).upper(), str(x)))
                _render_tracker_style_table(_st_df[["Name", "Shares", "AvgPrice", "ClosePrice", "Return%", "Recommend"]], pct_colors=True, kr_currency=True)
            else:
                st.info("현재 포트폴리오 중 매도/익절 권장 종목이 없습니다.")
        st.markdown("### 추가/업데이트")
        c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1.2])
        with c1:
            t_add = st.text_input("종목명 또는 티커 추가", value="", key="kr_ticker_add",
                placeholder="예: 카카오, 삼성전자 또는 035720.KQ",
                help="종목명(카카오, 삼성전자 등) 또는 티커(035720.KQ) 입력")
        with c2:
            sh_add = st.number_input("Shares", min_value=0.0, value=0.0, step=1.0, key="kr_shares_add")
        with c3:
            ap_add = st.number_input("AvgPrice", min_value=0.0, value=0.0, step=1.0, key="kr_avgprice_add")
        with c4:
            mode = st.selectbox("동일 티커 처리", ["merge(가중평단 합산)", "replace(덮어쓰기)"], key="kr_mode_select")
        if st.button("포트폴리오에 추가/업데이트", type="primary", key="kr_portfolio_add_btn"):
            try:
                t_resolved = _resolve_kr_name_or_ticker(t_add)
                if not t_resolved:
                    st.error("종목명 또는 티커를 입력하세요.")
                else:
                    m = "merge" if mode.startswith("merge") else "replace"
                    df_new = add_or_merge(dfp.copy(), t_resolved, float(sh_add), float(ap_add), mode=m)
                    save_positions(df_new, POSITIONS_KR_PATH)
                    st.session_state["kr_portfolio_saved"] = True
                    st.cache_data.clear()
                    st.rerun()
            except Exception as e:
                st.error(str(e))
        cash_loaded = load_portfolio_cash(PORTFOLIO_CASH_KR_PATH)
        cash_new = st.number_input("현금 추가/업데이트", min_value=0.0, value=float(cash_loaded), step=100.0, key="kr_portfolio_cash_update")
        if st.button("현금 반영", type="primary", key="kr_cash_btn"):
            if abs(cash_new - cash_loaded) > 1e-6:
                save_portfolio_cash(cash_new, PORTFOLIO_CASH_KR_PATH)
                st.session_state["kr_portfolio_saved"] = True
                st.rerun()
            else:
                st.info("변경 없음")
        st.markdown("### 제거")
        if dfp.empty:
            st.info("positions_kr.csv가 비어있습니다.")
        else:
            t_list = dfp["Ticker"].astype(str).str.upper().tolist()
            # 종목명으로 표시 (매핑 없으면 티커)
            kr_del_options = [TICKER_TO_NAME.get(t, t) for t in t_list]
            kr_display_to_ticker = {TICKER_TO_NAME.get(t, t): t for t in t_list}
            t_del_display = st.selectbox("삭제할 종목 선택", kr_del_options, key="kr_ticker_del")
            t_del = kr_display_to_ticker.get(t_del_display, t_del_display)
            if st.button("선택한 종목 삭제", type="primary", key="kr_ticker_del_btn"):
                df_new = remove_ticker(dfp.copy(), t_del)
                save_positions(df_new, POSITIONS_KR_PATH)
                st.success(f"{t_del_display} 삭제 완료!")
                st.cache_data.clear()
                st.rerun()

    with kr_tab3:
        st.subheader("🚀 스캔 실행 & 결과 (snapshot 표시 전용)")
        kr_timeout = st.number_input("타임아웃(초)", min_value=60, value=900, step=60, key="kr_scan_timeout")
        run_btn = st.button("🚀 스캔 실행", type="primary", key="kr_scan_btn")
        if run_btn:
            with st.status("scanner_kr.py 실행 중...", expanded=True) as status:
                ok, msg, out, err = run_scanner_kr_subprocess(timeout_sec=int(kr_timeout))
                st.write(msg)
                with st.expander("stderr (always)", expanded=(not ok)):
                    st.code((err or "(empty)")[-12000:])
                with st.expander("stdout", expanded=False):
                    st.code((out or "(empty)")[-12000:])
                if ok:
                    status.update(label="✅ scanner_kr.py 실행 완료", state="complete")
                    # 한국 시장 종가 반영: data_refresh_ts + ohlcv.db 삭제로 종목검색/트래커/포트폴리오 최신화
                    st.session_state["data_refresh_ts"] = datetime.now().isoformat()
                    st.session_state.pop("kr_scan_snap", None)
                    st.session_state.pop("tp3_tracker", None)
                    st.cache_data.clear()
                    ohlcv_db = os.path.join(BASE_DIR, "cache", "ohlcv.db")
                    if os.path.exists(ohlcv_db):
                        try:
                            os.remove(ohlcv_db)
                        except OSError:
                            pass
                    st.rerun()
                else:
                    status.update(label="❌ scanner_kr.py 실행 실패/중단", state="error")
        st.divider()
        _kr_snap_path = get_latest_snapshot(SNAPSHOT_KR_PATTERN)
        if _kr_snap_path is None:
            st.info("한국 증시 스캔 데이터가 없습니다.")
        else:
            snap = load_scan_snapshot_only(_kr_snap_path)
            if "error" in snap:
                st.error(f"스냅샷 없음: {snap.get('snapshot_path')}")
            else:
                run_date = snap.get("run_date") or snap.get("date") or snap.get("asof") or snap.get("runDate")
                if not run_date:
                    sp = snap.get("snapshot_path") or ""
                    run_date = os.path.basename(sp).replace("kr_scan_snapshot_", "").replace(".json", "") or str(datetime.utcnow().date())
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

                with st.expander("시장 상태 상세 (한국 증시 전용)"):
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
                        # KOSPI / KOSDAQ SMA
                        ki = ms.get("kospi") or {}
                        qi = ms.get("kosdaq") or {}
                        k50 = ki.get("sma50") or ms.get("spy_sma50")
                        k200 = ki.get("sma200") or ms.get("spy_sma200")
                        if k50 is not None or k200 is not None:
                            st.markdown(_p(f"KOSPI SMA50: {k50} | SMA200: {k200} ➖ normal"), unsafe_allow_html=True)
                        q50, q200 = qi.get("sma50"), qi.get("sma200")
                        if q50 is not None or q200 is not None:
                            st.markdown(_p(f"KOSDAQ SMA50: {q50} | SMA200: {q200} ➖ normal"), unsafe_allow_html=True)
                        adx_val = ms.get("adx_spy")
                        if adx_val is not None:
                            try:
                                adx_f = float(adx_val)
                                adx_badge = _badge(adx_f, lambda x: x >= 20, lambda x: x < 15)
                                st.markdown(_p(f"ADX(KOSPI): {adx_val} {adx_badge}"), unsafe_allow_html=True)
                            except (TypeError, ValueError):
                                st.markdown(_p(f"ADX(KOSPI): {adx_val} ➖ normal"), unsafe_allow_html=True)
                        idx = ms.get("indices") or {}
                        for sym in ["KOSPI", "KOSDAQ"]:
                            d = idx.get(sym)
                            if not isinstance(d, dict):
                                continue
                            a50, a200 = d.get("above_sma50"), d.get("above_sma200")
                            a50_b = "✅ good" if a50 is True else ("❌ bad" if a50 is False else "➖ normal")
                            a200_b = "✅ good" if a200 is True else ("❌ bad" if a200 is False else "➖ normal")
                            st.markdown(_p(f"{sym} above_sma50: {a50} {a50_b} | above_sma200: {a200} {a200_b}"), unsafe_allow_html=True)
                        top3_sector = ms.get("sector_5d_return_top3") or []
                        if top3_sector:
                            st.markdown(_p("**시장테마 TOP3 섹터**"), unsafe_allow_html=True)
                            for i, s in enumerate(top3_sector, 1):
                                name = s.get("name", s.get("ticker", "—"))
                                ret = s.get("return_5d")
                                r = f"{ret:+.2f}%" if ret is not None and np.isfinite(ret) else "—"
                                st.markdown(_p(f"  {i}. {name} {r}"), unsafe_allow_html=True)
                    with col_m2:
                        vol_r = ms.get("spy_vol_ratio")
                        if vol_r is not None:
                            try:
                                vol_f = float(vol_r)
                                vol_badge = _badge(vol_f, lambda x: x >= 1.0, lambda x: x < 0.7)
                                st.markdown(_p(f"KOSPI 거래량비(20d/5d): {vol_r} {vol_badge}"), unsafe_allow_html=True)
                            except (TypeError, ValueError):
                                st.markdown(_p(f"KOSPI 거래량비(20d/5d): {vol_r} ➖ normal"), unsafe_allow_html=True)
                        label_kq = ms.get("kospi_vs_kosdaq_label")
                        if label_kq:
                            sector_val = ms.get("kospi_vs_kosdaq")
                            sector_badge = "✅ good" if sector_val == "growth_lead" else ("❌ bad" if sector_val == "value_lead" else "➖ normal")
                            st.markdown(_p(f"시장 테마: {label_kq} {sector_badge}"), unsafe_allow_html=True)
                        vk = ms.get("vkospi") or ms.get("vix")
                        if vk is not None:
                            try:
                                vk_f = float(vk)
                                vk_b = _badge(vk_f, lambda x: x < 15, lambda x: x > 25)
                                st.markdown(_p(f"VKOSPI: {vk} {vk_b}"), unsafe_allow_html=True)
                            except (TypeError, ValueError):
                                st.markdown(_p(f"VKOSPI: {vk}"), unsafe_allow_html=True)
                        comp = ms.get("components") or {}
                        comp_label = {"indices": "지수(KOSPI/KOSDAQ)", "adx": "ADX", "vix": "VKOSPI", "vol_ratio": "거래량비", "sector": "시장테마"}
                        vk_val = ms.get("vkospi") or ms.get("vix")
                        vol_val = ms.get("spy_vol_ratio")
                        for k in ["indices", "adx", "vix", "vol_ratio", "sector"]:
                            if k not in comp:
                                continue
                            v = comp[k]
                            label = comp_label.get(k, k)
                            if k == "vix" and vk_val is not None:
                                extra = f" (실제값 {vk_val}, 점수 {v})"
                            elif k == "vol_ratio" and vol_val is not None:
                                extra = f" (실제값 {vol_val}, 점수 {v})"
                            else:
                                extra = ""
                            if isinstance(v, (int, float)):
                                cb = "✅ good" if v > 0 else ("❌ bad" if v < 0 else "➖ normal")
                                st.markdown(_p(f"{label}: {v}{extra} {cb}"), unsafe_allow_html=True)
                            else:
                                st.markdown(_p(f"{label}: {v}{extra} ➖ normal"), unsafe_allow_html=True)

                with st.expander("스캐너 도움말 (컬럼 설명)"):
                    _h = lambda s: f"<p style='color:#e2e8f0;font-size:0.95rem;margin:0.15rem 0;'>{s}</p>"
                    st.markdown(_h("**스캔 결과 테이블에 나오는 컬럼들의 간단한 의미입니다.**"), unsafe_allow_html=True)
                    kr_help_items = [
                        ("**Name**", "종목명 (예: 삼성전자, 카카오). 회사 이름입니다."),
                        ("**Ticker**", "종목 코드 (예: 005930.KS, 035720.KQ). 코스피(.KS) / 코스닥(.KQ) 구분."),
                        ("**Sector**", "해당 종목이 속한 업종/섹터 (예: Technology, Healthcare)."),
                        ("**Entry**", "진입 신호 종류. BUY_BREAKOUT(돌파 매수), BUY_PULLBACK(눌림 매수), WATCH(관망) 등."),
                        ("**EntryRaw**", "진입 신호의 원본 값. Entry와 동일하거나 세부 구분용입니다."),
                        ("**Close**", "최근 거래일 종가(원화). 현재 기준 가격입니다."),
                        ("**MktCap_KRW_T**", "시가총액(원화, 억원 단위). 회사 규모를 보는 지표입니다."),
                        ("**EV**", "기대값(Expected Value). 확률×리워드 - (1-확률)×리스크로, 전략 기대 수익을 나타냅니다."),
                        ("**Prob**", "승률 추정치(0~1). Score/Vol/RSI/ATR 등으로 산출한 확률입니다."),
                        ("**RR**", "리워드/리스크 비율(Risk-Reward). 기대 수익 대비 손실 비율로, 1.5 이상이면 유리한 편입니다."),
                        ("**Score**", "종합 점수. 여러 조건을 반영한 순위/점수입니다."),
                        ("**VolRatio**", "거래량 비율. 최근 거래량이 평균(20일) 대비 몇 배인지 보여줍니다."),
                        ("**RSI**", "RSI(14). 과매수(70 근처 이상)/과매도(30 근처 이하)를 보는 지표입니다."),
                        ("**ATR%**", "ATR을 가격으로 나눈 비율(%). 변동성 크기를 보여줍니다."),
                        ("**ADX**", "추세 강도 지표. 숫자가 클수록 추세가 뚜렷합니다 (보통 20 이상)."),
                        ("**RS_vs_KOSPI or KOSDAQ**", "KOSPI(코스피) 또는 KOSDAQ(코스닥) 지수 대비 상대 강도. 시장보다 잘 오른 종목입니다."),
                        ("**PctOff52H**", "52주 고점 대비 현재가가 몇 % 아래인지."),
                        ("**Trigger**", "진입 신호가 나온 이유(트리거). 예: '20일 고점 돌파', 'SMA50 근처 반등' 등."),
                        ("**EntryHint**", "진입 시 참고할 가격/조건."),
                        ("**Invalidation**", "신호가 무효가 되는 조건. 손절가 도달 시 재검토합니다."),
                        ("**Reasons**", "해당 진입/관망 판단의 근거를 요약한 텍스트입니다."),
                        ("**MACDTrigger**", "MACD 지표로 인한 트리거(신호)가 있는지 여부입니다."),
                        ("**Note**", "추가 메모. 이평 정렬, 시장 상태 등 보조 설명이 들어갑니다."),
                        ("**EntryPrice**", "권장 진입가(원화)."),
                        ("**StopPrice**", "손절가(원화). 이 가격 아래로 떨어지면 손절을 고려합니다."),
                        ("**TargetPrice**", "목표가(원화). 익절을 노리는 가격대입니다."),
                        ("**Shares**", "계산된 추천 매수 수량(주)."),
                        ("**PosValue**", "포지션 규모(금액). EntryPrice × Shares."),
                        ("**Avg$Vol**", "평균 거래대금(원화). 유동성 참고용입니다."),
                        ("**P**", "진입 신호 타입 우선순위. 숫자가 작을수록 더 우선입니다."),
                        ("**Promoted**", "BUY 신호 종목이 부족할 때 WATCH 중에서 선별해 승격시킨 종목입니다."),
                    ]
                    for label, desc in kr_help_items:
                        st.markdown(_h(f"{label}: {desc}"), unsafe_allow_html=True)

                def _df(x):
                    return x if isinstance(x, pd.DataFrame) else pd.DataFrame(x)
                df_all = _df(snap.get("df_all"))
                buy_df = _df(snap.get("buy_df"))
                watch_df = _df(snap.get("watch_df"))
                top3 = _df(snap.get("top_picks"))
                # KR: Name 컬럼 있으면 Ticker 숨김 (회사명만 표시)
                def _kr_display(df):
                    if df.empty:
                        return df
                    if "Name" in df.columns and "Ticker" in df.columns:
                        return df.drop(columns=["Ticker"])
                    return df
                st.subheader("TOP PICKS")
                if top3.empty:
                    st.info("TOP PICK3 후보가 없습니다.")
                else:
                    _render_tracker_style_table(_kr_display(top3), kr_currency=True)
                st.subheader("BUY")
                _render_tracker_style_table(_kr_display(buy_df), kr_currency=True)
                st.subheader("WATCH")
                _render_tracker_style_table(_kr_display(watch_df), kr_currency=True)
                with st.expander("ALL (raw)"):
                    st.dataframe(_kr_display(df_all), use_container_width=True)
    st.stop()

# 미국 증시 스캐너
# 우측 하단 고정 뒤로가기 버튼 (같은 탭에서 홈으로 이동)
st.markdown(
    '<form action="" method="get" target="_self" style="position:fixed;bottom:24px;right:24px;z-index:9999;">'
    '<input type="hidden" name="goto" value="home" />'
    '<button type="submit" style="padding:10px 16px;background:rgba(30,41,59,0.95);color:#e2e8f0;border-radius:8px;'
    'font-size:0.9rem;border:1px solid rgba(100,116,139,0.4);cursor:pointer;'
    'box-shadow:0 2px 8px rgba(0,0,0,0.3);">← 뒤로가기</button></form>',
    unsafe_allow_html=True,
)

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
            '<img src="data:image/png;base64,' + logo_b64 + '" style="width:' + str(_logo_width) + 'px;position:absolute;bottom:-96px;left:-80px;display:block;" alt="US Swing Scanner" />'
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

@_st_fragment
def _render_us_tab1():
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
                st.session_state["ticker_result"] = res
                st.session_state["show_ticker_result"] = True
        with btn_col2:
            if st.session_state.get("show_ticker_result") and st.session_state.get("ticker_result"):
                if st.button("닫기", key="close_ticker_result", type="secondary"):
                    st.session_state["show_ticker_result"] = False
                    st.session_state.pop("ticker_result", None)
    
        # 검색 결과를 트렌딩 20개 위에 표시
        if st.session_state.get("show_ticker_result") and st.session_state.get("ticker_result"):
            res = st.session_state["ticker_result"]
    
            if "error" in res:
                st.stop()
    
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
    
            # 종목 상태 상세 (시장상태상세 스타일, 스캐너 판단에 쓰는 지표 전부)
            with st.expander("종목 상태 상세 (Ticker State)"):
                _p = lambda s: f"<p style='color:#f1f5f9;font-size:1.05rem;margin:0.22rem 0;'>{s}</p>"
                df_tail = res.get("df_tail")
                if df_tail is not None and not df_tail.empty and len(df_tail) >= 2:
                    last = df_tail.iloc[-1]
                    prev = df_tail.iloc[-2]
                    close_val = float(last.get("Close", 0)) if "Close" in last else 0
                    open_val = float(last.get("Open", 0)) if "Open" in last else 0
                    high_val = float(last.get("High", 0)) if "High" in last else 0
                    low_val = float(last.get("Low", 0)) if "Low" in last else 0
                    prev_close = float(prev.get("Close", 0)) if "Close" in prev else close_val
    
                    col_t1, col_t2, col_t3 = st.columns(3)
    
                    def _add_item(col, label, val_str, badge):
                        col.markdown(_p(f"{label}: {val_str} {badge}"), unsafe_allow_html=True)
    
                    with col_t1:
                        # MACD
                        macd_h = last.get("MACD_H")
                        if macd_h is not None and np.isfinite(float(macd_h)):
                            macd_h_f = float(macd_h)
                            _add_item(col_t1, "MACD", "상승" if macd_h_f > 0 else "하락", "✅ good" if macd_h_f > 0 else "❌ bad")
    
                        # RSI
                        rsi_val = last.get("RSI14")
                        if rsi_val is not None and np.isfinite(float(rsi_val)):
                            rsi_f = float(rsi_val)
                            if rsi_f < 30: _add_item(col_t1, "RSI", f"{rsi_f:.1f} (과매도)", "✅ good")
                            elif rsi_f > 70: _add_item(col_t1, "RSI", f"{rsi_f:.1f} (과매수)", "❌ bad")
                            else: _add_item(col_t1, "RSI", f"{rsi_f:.1f} (적정)", "➖ normal")
    
                        # 변동성(ATR%)
                        atr14 = last.get("ATR14")
                        if atr14 is not None and close_val > 0 and np.isfinite(float(atr14)):
                            atr_pct = float(atr14) / close_val * 100
                            if atr_pct < 2: _add_item(col_t1, "변동성(ATR%)", f"{atr_pct:.2f}% (낮음)", "❌ bad")
                            elif atr_pct > 6: _add_item(col_t1, "변동성(ATR%)", f"{atr_pct:.2f}% (높음)", "❌ bad")
                            else: _add_item(col_t1, "변동성(ATR%)", f"{atr_pct:.2f}% (적정)", "➖ normal")
    
                        # ADX
                        adx_val = last.get("ADX14")
                        if adx_val is not None and np.isfinite(float(adx_val)):
                            adx_f = float(adx_val)
                            if adx_f >= 20: _add_item(col_t1, "ADX", f"{adx_f:.1f} (추세 있음)", "✅ good")
                            elif adx_f < 15: _add_item(col_t1, "ADX", f"{adx_f:.1f} (횡보)", "❌ bad")
                            else: _add_item(col_t1, "ADX", f"{adx_f:.1f} (보통)", "➖ normal")
    
                        # SMA20
                        sma20 = last.get("SMA20")
                        if sma20 is not None and close_val > 0 and np.isfinite(float(sma20)):
                            above = close_val > float(sma20)
                            _add_item(col_t1, "SMA20", "이평 위" if above else "이평 아래", "✅ good" if above else "❌ bad")
    
                        # SMA50
                        sma50 = last.get("SMA50")
                        if sma50 is not None and close_val > 0 and np.isfinite(float(sma50)):
                            above = close_val > float(sma50)
                            _add_item(col_t1, "SMA50", "이평 위" if above else "이평 아래", "✅ good" if above else "❌ bad")
    
                        # SMA200
                        sma200 = last.get("SMA200")
                        if sma200 is not None and close_val > 0 and np.isfinite(float(sma200)):
                            above = close_val > float(sma200)
                            _add_item(col_t1, "SMA200", "이평 위" if above else "이평 아래", "✅ good" if above else "❌ bad")
    
                    with col_t2:
                        # 이평 정렬 (SMA50 > SMA150 > SMA200)
                        s50 = last.get("SMA50"); s150 = last.get("SMA150"); s200 = last.get("SMA200")
                        if all(x is not None and np.isfinite(float(x)) for x in (s50, s150, s200)):
                            stack = float(s50) > float(s150) > float(s200)
                            _add_item(col_t2, "이평 정렬", "50>150>200" if stack else "정렬 아님", "✅ good" if stack else "❌ bad")
    
                        # 20일 고점 근접도
                        if len(df_tail) >= 21 and "High" in df_tail.columns:
                            high20 = float(df_tail["High"].iloc[-21:-1].max())
                            if high20 > 0:
                                near_pct = close_val / high20 * 100
                                if near_pct >= 98: _add_item(col_t2, "20D고점 근접", f"{near_pct:.1f}% (근접)", "✅ good")
                                elif near_pct < 95: _add_item(col_t2, "20D고점 근접", f"{near_pct:.1f}% (멀음)", "➖ normal")
                                else: _add_item(col_t2, "20D고점 근접", f"{near_pct:.1f}%", "➖ normal")
    
                        # 거래량 비율
                        if "Volume" in df_tail.columns and len(df_tail) >= 20:
                            vol = float(last.get("Volume", 0))
                            vol20 = float(df_tail["Volume"].tail(20).mean())
                            if vol20 > 0:
                                vol_ratio = vol / vol20
                                if vol_ratio >= 1.5: _add_item(col_t2, "거래량비(Vol/20d)", f"{vol_ratio:.2f}x (강함)", "✅ good")
                                elif vol_ratio < 0.7: _add_item(col_t2, "거래량비(Vol/20d)", f"{vol_ratio:.2f}x (약함)", "❌ bad")
                                else: _add_item(col_t2, "거래량비(Vol/20d)", f"{vol_ratio:.2f}x", "➖ normal")
    
                        # 윗꼬리 비율 (돌파 품질)
                        if high_val > low_val and high_val > 0:
                            rng = high_val - low_val
                            upper_wick = (high_val - max(open_val, close_val)) / rng
                            if upper_wick <= 0.3: _add_item(col_t2, "윗꼬리 비율", f"{upper_wick:.2f} (낮음)", "✅ good")
                            elif upper_wick >= 0.6: _add_item(col_t2, "윗꼬리 비율", f"{upper_wick:.2f} (높음)", "❌ bad")
                            else: _add_item(col_t2, "윗꼬리 비율", f"{upper_wick:.2f}", "➖ normal")
    
                        # 갭/ATR (추격 위험)
                        if atr14 is not None and np.isfinite(float(atr14)) and float(atr14) > 0:
                            gap_atr = (open_val - prev_close) / float(atr14)
                            if gap_atr <= 1.0: _add_item(col_t2, "갭/ATR", f"{gap_atr:.2f} (적정)", "✅ good")
                            elif gap_atr > 1.2: _add_item(col_t2, "갭/ATR", f"{gap_atr:.2f} (추격위험)", "❌ bad")
                            else: _add_item(col_t2, "갭/ATR", f"{gap_atr:.2f}", "➖ normal")
    
                        # 캔들 (양봉/음봉)
                        bullish = close_val > open_val
                        _add_item(col_t2, "캔들", "양봉" if bullish else "음봉", "✅ good" if bullish else "❌ bad")
    
                    with col_t3:
                        # 종가 확인 (돌파 시 고점 위 마감) - 20D고점 대비
                        if len(df_tail) >= 21 and "High" in df_tail.columns:
                            high20_prev = float(df_tail["High"].iloc[-21:-1].max())
                            if high20_prev > 0:
                                close_confirm = close_val >= high20_prev * 1.001
                                _add_item(col_t3, "종가확인(돌파)", "고점 위 마감" if close_confirm else "미확인", "✅ good" if close_confirm else "➖ normal")
    
                        # 52주 고점 근접 (있으면)
                        if len(df_tail) >= 252 and "High" in df_tail.columns:
                            high52 = float(df_tail["High"].tail(252).max())
                            if high52 > 0:
                                pct_off = (high52 - close_val) / high52 * 100
                                if pct_off <= 10: _add_item(col_t3, "52주고점", f"{pct_off:.1f}% 아래 (근접)", "✅ good")
                                elif pct_off > 25: _add_item(col_t3, "52주고점", f"{pct_off:.1f}% 아래 (멀음)", "❌ bad")
                                else: _add_item(col_t3, "52주고점", f"{pct_off:.1f}% 아래", "➖ normal")
    
                        # 2일 연속 SMA20 이탈 (매도 컨펌)
                        prev_sma20 = prev.get("SMA20") if "SMA20" in prev else None
                        if sma20 is not None and prev_sma20 is not None and np.isfinite(float(sma20)) and np.isfinite(float(prev_sma20)):
                            two_day_below = (close_val < float(sma20)) and (prev_close < float(prev_sma20))
                            _add_item(col_t3, "2일연속<SMA20", "이탈" if two_day_below else "미이탈", "❌ bad" if two_day_below else "➖ normal")
    
                        # 추세 (SMA50 > SMA200)
                        if sma50 is not None and sma200 is not None and np.isfinite(float(sma50)) and np.isfinite(float(sma200)):
                            uptrend = float(sma50) > float(sma200)
                            _add_item(col_t3, "추세(50>200)", "상승" if uptrend else "하락", "✅ good" if uptrend else "❌ bad")
    
                        # 눌림 구간 (SMA20/SMA50 근접)
                        if sma20 is not None and sma50 is not None and np.isfinite(float(sma20)) and np.isfinite(float(sma50)) and float(sma20) > 0 and float(sma50) > 0:
                            near_20 = abs(close_val / float(sma20) - 1) <= 0.015
                            near_50 = abs(close_val / float(sma50) - 1) <= 0.0225
                            if near_20 or near_50:
                                _add_item(col_t3, "눌림 구간", "SMA 근접" if (near_20 or near_50) else "—", "➖ normal")
                else:
                    st.markdown(_p("지표 데이터가 부족합니다."), unsafe_allow_html=True)
    
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
    
            ticker_news, _ = _fetch_ticker_news(res.get("ticker", ""), is_kr=False, max_items=5)
            with st.spinner("기사 요약 불러오는 중…"):
                ticker_news = _enrich_news_with_summaries(ticker_news, st.session_state.get("news_refresh_ts", ""))
            _render_ticker_news(ticker_news, res.get("ticker", ""))
    
        st.divider()
        st.markdown(
            "<div style='background:rgba(15,23,42,0.95);color:#f1f5f9;padding:12px 16px;border-radius:10px;"
            "box-shadow:0 0 24px rgba(59,130,246,0.3);border:1px solid rgba(100,116,139,0.5);"
            "margin:0.5rem 0;font-size:1.1rem;font-weight:700;'>📊 오늘 가장 많이 검색한 미국 증시 티커 20개</div>",
            unsafe_allow_html=True,
        )
        _trend_df = _fetch_trending_us_tickers(20)
        if not _trend_df.empty:
            _disp = _trend_df[["순위", "Ticker", "종목명", "현재가", "등락률(%)"]].copy()
            _trend_col_widths = ["5%", "12%", "38%", "22%", "23%"]
            _render_tracker_style_table(_disp, pct_colors=True, col_widths=_trend_col_widths)
        else:
            st.info("트렌딩 티커 데이터를 불러올 수 없습니다.")
    
        st.divider()
    
        # =========================
        # 2) TOP PICK3 BUY 성과 트래커
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
    
    
            today = datetime.utcnow().date()
            cum = compute_cum_returns(tr_all, today=today)
            top_pick_ret = compute_open_avg_return(tr_all)
            daily_change_avg = compute_open_daily_change_avg(tr_all)
    
            closed_count = int((tr_all["Status"] == "CLOSED").sum()) if "Status" in tr_all.columns else 0
            k0, k1, k2, k3, k4, k5 = st.columns(6)
            k0.metric("TOP PICK 수익률", f"{top_pick_ret:.2f}%")
            k1.metric("일간 수익률", f"{daily_change_avg:.2f}%")
            k2.metric("월간 수익률(누적)", f"{cum['monthly']:.2f}%")
            k3.metric("연간 수익률(누적)", f"{cum['yearly']:.2f}%")
            k4.metric("총 수익률(누적)", f"{cum['total']:.2f}%")
            k5.metric("추적 완료 종목", f"{closed_count}개")
    
            st.caption("※ TOP PICK 수익률: 현재 보유(OPEN) 종목들의 수익률 평균. 일간 수익률: 오늘 거래일 동안 변동된 수익률의 평균. 월간/연간/총 수익률: 해당 기간 CLOSED 종목들의 누적(복리) 수익률.")
    
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

with tab1:
    _render_us_tab1()




# --- snapshot에서 데이터 꺼내기 (항상 DataFrame으로 확정) ---



with tab2:
    st.subheader("포트폴리오 관리")
    path = os.path.join(BASE_DIR, "positions.csv")
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

            # ✅ 현재가(종가): _current_close 단일 소스 사용 (티커검색/트래커와 동일)
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
                # 미국 시장 종가 반영: data_refresh_ts + ohlcv.db 삭제로 종목검색/트래커 최신화
                st.session_state["data_refresh_ts"] = datetime.now().isoformat()
                st.session_state.pop("scan_snap", None)
                st.session_state.pop("tp3_tracker", None)
                st.cache_data.clear()
                ohlcv_db = os.path.join(BASE_DIR, "cache", "ohlcv.db")
                if os.path.exists(ohlcv_db):
                    try:
                        os.remove(ohlcv_db)
                    except OSError:
                        pass
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
            top3_sector = ms.get("sector_5d_return_top3") or []
            if top3_sector:
                st.markdown(_p("**최근 5거래일 수익률 높은 섹터 Top3**"), unsafe_allow_html=True)
                for i, s in enumerate(top3_sector, 1):
                    name = s.get("name", s.get("ticker", "—"))
                    ticker = s.get("ticker", "")
                    ret = s.get("return_5d")
                    r = f"{ret:+.2f}%" if ret is not None and np.isfinite(ret) else "—"
                    st.markdown(_p(f"  {i}. {name} ({ticker}) {r}"), unsafe_allow_html=True)

    with st.expander("스캐너 도움말 (컬럼 설명)"):
        _h = lambda s: f"<p style='color:#e2e8f0;font-size:0.95rem;margin:0.15rem 0;'>{s}</p>"
        st.markdown(_h("**스캔 결과 테이블에 나오는 컬럼들의 간단한 의미입니다.**"), unsafe_allow_html=True)
        help_items = [
            ("**Ticker**", "종목 코드 (예: AAPL, NVDA). 미국 주식 심볼입니다."),
            ("**Sector**", "해당 종목이 속한 업종/섹터 (예: Technology, Healthcare)."),
            ("**Entry**", "진입 신호 종류. BUY_BREAKOUT(돌파 매수), BUY_PULLBACK(눌림 매수), WATCH(관망) 등으로 표시됩니다."),
            ("**EntryRaw**", "진입 신호의 원본 값. Entry와 동일하거나 세부 구분용입니다."),
            ("**Close**", "최근 거래일 종가. 현재 기준 가격입니다."),
            ("**MktCap_KRW_T**", "시가총액(원화, 억원 단위). 1의 자리=억원. 회사 규모를 보는 지표입니다."),
            ("**EV**", "기업가치(Enterprise Value). 시가총액 + 부채 − 현금으로, 인수 시 필요한 규모를 나타냅니다."),
            ("**Prob**", "전략/모델에서 산출한 확률 관련 수치입니다."),
            ("**RR**", "리워드/리스크 비율(Risk-Reward). 기대 수익 대비 손실 비율로, 1.5 이상이면 유리한 편입니다."),
            ("**Score**", "종합 점수. 여러 조건을 반영한 순위/점수입니다."),
            ("**VolRatio**", "거래량 비율. 최근 거래량이 평균(예: 20일) 대비 몇 배인지 보여줍니다. 돌파 시 거래량이 많을수록 유리합니다."),
            ("**RSI**", "RSI(14). 과매수(70 근처 이상)/과매도(30 근처 이하)를 보는 지표입니다. 30~70 사이가 보통입니다."),
            ("**ATR%**", "ATR을 가격으로 나눈 비율(%). 변동성 크기를 보여줍니다. 클수록 움직임이 큽니다."),
            ("**ADX**", "추세 강도 지표. 숫자가 클수록 추세가 뚜렷하다는 뜻입니다 (보통 20 이상이면 추세 있다고 봅니다)."),
            ("**RS_vs_SPY**", "SPY(미국 대표 지수) 대비 상대 강도. 이 값이 높으면 시장보다 잘 오른 종목입니다."),
            ("**PctOff52H**", "52주 고점 대비 현재가가 몇 % 아래인지. 0에 가까우면 고점 근처, 작을수록 고점에서 멀리 떨어져 있습니다."),
            ("**Trigger**", "진입 신호가 나온 이유(트리거). 예: '20일 고점 돌파', 'SMA50 근처 반등' 등."),
            ("**EntryHint**", "진입 시 참고할 가격/조건. 예: '이 가격 위에서 분할 매수' 같은 안내입니다."),
            ("**Invalidation**", "신호가 무효가 되는 조건. 예: 'SMA50 이탈 또는 손절가 도달' — 이 조건이 되면 매수 관점을 재검토합니다."),
            ("**Reasons**", "해당 진입/관망 판단의 근거를 요약한 텍스트입니다."),
            ("**MACDTrigger**", "MACD 지표로 인한 트리거(신호)가 있는지 여부입니다."),
            ("**Note**", "추가 메모. 이평 정렬, 시장 상태 등 보조 설명이 들어갑니다."),
            ("**EntryPrice**", "권장 진입가. 보통 신호일 종가 또는 그 근처입니다."),
            ("**StopPrice**", "손절가. 이 가격 아래로 떨어지면 손절을 고려하는 구간입니다."),
            ("**TargetPrice**", "목표가. 익절을 노리는 가격대입니다."),
            ("**Shares**", "계산된 추천 매수 수량(주). 포지션 사이징 결과입니다."),
            ("**PosValue**", "포지션 규모(금액). EntryPrice × Shares로, 투입 예상 금액입니다."),
            ("**Avg$Vol**", "평균 거래대금. 일 평균 거래 규모로, 유동성 참고용입니다."),
            ("**P**", "진입 신호 타입 우선순위이고, 숫자가 작을수록 더 우선입니다."),
            ("**Pwin**", "승률(Win rate). 과거/시뮬 기준 이 strategy에서 이길 비율을 나타낼 수 있습니다."),
            ("**RR_s**", "RR(Risk-Reward) 관련 보조 수치입니다."),
            ("**T**", "진입 타입 순위 (0=돌파, 1=눌림, 2=스마트, 9=기타)."),
            ("**Promoted**", "BUY 신호 종목이 부족할 때 WATCH 중에서 선별해 승격시킨 종목입니다."),
        ]
        for label, desc in help_items:
            st.markdown(_h(f"{label}: {desc}"), unsafe_allow_html=True)

    # 항상 DataFrame으로 강제
    def _df(x):
        return x if isinstance(x, pd.DataFrame) else pd.DataFrame(x)

    df_all   = _df(snap.get("df_all"))
    buy_df   = _df(snap.get("buy_df"))
    watch_df = _df(snap.get("watch_df"))
    risk_df  = _df(snap.get("risk_df"))
    recos_df = _df(snap.get("recos_df"))
    top3     = _df(snap.get("top_picks"))

    st.subheader("TOP PICKS")
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
