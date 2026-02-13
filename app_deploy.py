# -*- coding: utf-8 -*-
"""
스파오 스타일별 시즌·입고 여부·출고 여부·온라인 상품 등록 여부 대시보드.
deploy.py와 동일한 Google 시트/BASE 소스를 사용합니다.
실행: streamlit run spao_style_dashboard.py
"""
from __future__ import annotations

import os
import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime

# ========== deploy.py와 동일한 설정/시트 ID (Secrets 사용) ==========
def _secret(key, default=""):
    try:
        v = st.secrets.get(key, default) or default
        return str(v).strip() if v else default
    except Exception:
        return default

def _norm_sheet_id(val):
    s = str(val).strip() if val else ""
    return s if s else ""

_SPREADSHEET_SECRET_KEYS = [
    ("inout", "BASE_SPREADSHEET_ID"),
    ("spao", "SP_SPREADSHEET_ID"),
]
GOOGLE_SPREADSHEET_IDS = {k: _norm_sheet_id(_secret(s)) for k, s in _SPREADSHEET_SECRET_KEYS}
BASE_SPREADSHEET_ID = GOOGLE_SPREADSHEET_IDS.get("inout", "")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

def _get_google_credentials():
    import json
    from google.oauth2.service_account import Credentials
    try:
        raw = None
        if hasattr(st.secrets, "get"):
            raw = st.secrets.get("google_service_account")
        if not raw:
            raw = _secret("google_service_account")
        if raw:
            info = json.loads(raw) if isinstance(raw, str) else dict(raw)
            if "type" in info and "private_key" in info:
                return Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPES)
    except Exception:
        pass
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path or not os.path.isfile(creds_path):
        for name in ("service_account.json", "credentials.json", "google_credentials.json"):
            p = os.path.join(BASE_DIR, name)
            if os.path.isfile(p):
                creds_path = p
                break
    if not creds_path:
        return None
    try:
        return Credentials.from_service_account_file(creds_path, scopes=GOOGLE_SCOPES)
    except Exception:
        return None

def _fetch_google_sheet_via_sheets_api(spreadsheet_id, creds):
    try:
        from googleapiclient.discovery import build
        from openpyxl import Workbook
        sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_names = [s["properties"]["title"] for s in meta.get("sheets", [])]
        if not sheet_names:
            return None
        wb = Workbook()
        wb.remove(wb.active)
        for idx, title in enumerate(sheet_names):
            try:
                range_name = f"'{title.replace(chr(39), chr(39)+chr(39))}'" if title else f"Sheet{idx+1}"
                r = sheets_service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id, range=range_name,
                ).execute()
                rows = r.get("values", [])
            except Exception:
                rows = []
            safe_title = title[:31] if title else f"Sheet{idx+1}"
            ws = wb.create_sheet(title=safe_title, index=idx)
            for row in rows:
                ws.append(row)
        out = BytesIO()
        wb.save(out)
        out.seek(0)
        return out.read()
    except Exception:
        return None

@st.cache_data(ttl=300)
def _fetch_google_sheet_as_xlsx_bytes(spreadsheet_id, _creds_ok=True):
    sid = (str(spreadsheet_id).strip() if spreadsheet_id else "") or ""
    if not sid or not _creds_ok:
        return None
    creds = _get_google_credentials()
    if not creds:
        return None
    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseDownload
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
        request = service.files().export_media(
            fileId=sid,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read()
    except Exception:
        pass
    return _fetch_google_sheet_via_sheets_api(sid, creds)

@st.cache_data(ttl=300)
def get_excel_sources():
    creds_ok = _get_google_credentials() is not None
    sources = {}
    for key in ("inout", "spao"):
        sheet_id = GOOGLE_SPREADSHEET_IDS.get(key)
        sheet_id = str(sheet_id).strip() if sheet_id else ""
        if sheet_id:
            raw = _fetch_google_sheet_as_xlsx_bytes(sheet_id, _creds_ok=creds_ok)
            sources[key] = (raw, f"gs:{sheet_id}") if raw else (None, "none")
        else:
            sources[key] = (None, "none")
    return sources

def find_col(keys, df=None):
    if df is None or df.empty:
        return None
    cols = list(df.columns)
    for k in keys:
        for c in cols:
            if str(c).strip() == k:
                return c
    for k in keys:
        for c in cols:
            if k in str(c):
                return c
    return None

@st.cache_data(ttl=300)
def load_inout_data(io_bytes=None, _cache_key=None):
    if _cache_key is None:
        _cache_key = "default"
    if io_bytes is None or len(io_bytes) == 0:
        return pd.DataFrame()
    excel_file = pd.ExcelFile(BytesIO(io_bytes))
    sheet_candidates = [s for s in excel_file.sheet_names if not str(s).startswith("_")]
    sheet_name = sheet_candidates[0] if sheet_candidates else excel_file.sheet_names[-1]
    preview = pd.read_excel(BytesIO(io_bytes), sheet_name=sheet_name, header=None)
    header_keywords = ["브랜드", "스타일", "칼라", "최초입고일", "입고", "출고"]
    best_row, best_score = None, 0
    for i in range(min(20, len(preview))):
        row = preview.iloc[i].astype(str)
        score = sum(any(k in cell for k in header_keywords) for cell in row)
        if score > best_score:
            best_score, best_row = score, i
    if best_row is not None and best_score > 0:
        df = pd.read_excel(BytesIO(io_bytes), sheet_name=sheet_name, header=best_row)
    else:
        df = pd.read_excel(BytesIO(io_bytes), sheet_name=sheet_name)
    df.columns = [str(c).strip() for c in df.columns]
    style_col = find_col(["스타일코드", "스타일"], df=df)
    if style_col and style_col in df.columns:
        prefix = df[style_col].astype(str).str.strip().str.lower().str.slice(0, 2)
        df["브랜드"] = prefix.map({"sp": "스파오", "rm": "로엠", "mi": "미쏘", "wh": "후아유", "hp": "슈펜", "cv": "클라비스", "eb": "에블린", "nb": "뉴발란스", "nk": "뉴발란스키즈"})
    return df

# ========== 스파오 시트에서 스타일·시즌·등록 정보 로드 ==========
def _normalize(v):
    return "".join(str(v).split()) if v is not None else ""

@st.cache_data(ttl=120)
def load_spao_style_register_df(io_bytes=None, _cache_key=None):
    """SP 시트에서 스타일코드, 시즌, 공홈등록일, 공홈등록여부가 있는 시트를 찾아 DataFrame 반환."""
    if io_bytes is None or len(io_bytes) == 0:
        return pd.DataFrame()
    try:
        excel_file = pd.ExcelFile(BytesIO(io_bytes))
    except Exception:
        return pd.DataFrame()
    for sheet_name in excel_file.sheet_names:
        try:
            df_raw = pd.read_excel(BytesIO(io_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if df_raw is None or df_raw.empty:
            continue
        header_row_idx, header_vals = None, None
        for i in range(min(30, len(df_raw))):
            row = df_raw.iloc[i].tolist()
            norm = [_normalize(v) for v in row]
            if any("스타일코드" in v for v in norm) and (any("공홈등록일" in v for v in norm) or any("시즌" in v for v in norm)):
                header_row_idx, header_vals = i, norm
                break
        if header_row_idx is None:
            continue
        def find_col_idx(key):
            for idx, v in enumerate(header_vals):
                if key in v:
                    return idx
            return None
        style_col = find_col_idx("스타일코드") or find_col_idx("스타일")
        season_col = find_col_idx("시즌") or find_col_idx("Season")
        register_col = find_col_idx("공홈등록일") or find_col_idx("등록일")
        status_col = find_col_idx("공홈등록여부") or find_col_idx("등록여부")
        if style_col is None:
            continue
        data = df_raw.iloc[header_row_idx + 1 :].copy()
        data.columns = range(data.shape[1])
        out = pd.DataFrame()
        out["스타일코드"] = data.iloc[:, style_col].astype(str).str.strip()
        if season_col is not None and season_col < data.shape[1]:
            out["시즌"] = data.iloc[:, season_col].astype(str).str.strip()
        else:
            out["시즌"] = ""
        if register_col is not None and register_col < data.shape[1]:
            reg_series = data.iloc[:, register_col].astype(str).str.strip()
            out["공홈등록일"] = reg_series
        else:
            out["공홈등록일"] = ""
        if status_col is not None and status_col < data.shape[1]:
            out["공홈등록여부"] = data.iloc[:, status_col].astype(str).str.strip()
        else:
            out["공홈등록여부"] = ""
        out = out[out["스타일코드"].str.len() > 0]
        out = out[out["스타일코드"] != "nan"]
        return out
    return pd.DataFrame()

# ========== 입출고(BASE)에서 스파오 스타일별 입고/출고 여부 ==========
def build_inout_status_df(df_inout):
    """BASE 데이터에서 스파오(SP) 스타일별 입고 여부, 출고 여부 집계."""
    if df_inout is None or df_inout.empty:
        return pd.DataFrame()
    style_col = find_col(["스타일코드", "스타일"], df=df_inout)
    brand_col = find_col(["브랜드", "brand"], df=df_inout) or "브랜드"
    first_in_col = find_col(["최초입고일", "첫 입고일", "입고일"], df=df_inout)
    out_amt_col = find_col(["출고액"], df=df_inout)
    out_qty_col = find_col(["출고량", "출고 개수"], df=df_inout)
    if style_col is None:
        return pd.DataFrame()
    df = df_inout.copy()
    if brand_col not in df.columns:
        df[brand_col] = df[style_col].astype(str).str.strip().str.upper().str.slice(0, 2).map({"SP": "스파오"})
    df_sp = df[df[brand_col].astype(str).str.contains("스파오", na=False)].copy()
    if df_sp.empty:
        return pd.DataFrame()
    df_sp["_style"] = df_sp[style_col].astype(str).str.strip()
    df_sp["입고여부"] = False
    if first_in_col and first_in_col in df_sp.columns:
        first_vals = df_sp[first_in_col]
        numeric = pd.to_numeric(first_vals, errors="coerce")
        excel_dates = numeric.between(1, 60000, inclusive="both")
        dt_vals = pd.to_datetime(first_vals, errors="coerce")
        df_sp.loc[excel_dates, "입고여부"] = True
        df_sp.loc[~excel_dates & dt_vals.notna(), "입고여부"] = True
    df_sp["출고여부"] = False
    if out_amt_col and out_amt_col in df_sp.columns:
        amt = pd.to_numeric(df_sp[out_amt_col], errors="coerce").fillna(0)
        df_sp["출고여부"] = amt > 0
    elif out_qty_col and out_qty_col in df_sp.columns:
        qty = pd.to_numeric(df_sp[out_qty_col], errors="coerce").fillna(0)
        df_sp["출고여부"] = qty > 0
    agg = df_sp.groupby("_style").agg(
        입고여부=("입고여부", "any"),
        출고여부=("출고여부", "any"),
    ).reset_index()
    agg = agg.rename(columns={"_style": "스타일코드"})
    return agg

# ========== 메인: 병합 테이블 생성 ==========
@st.cache_data(ttl=120)
def build_spao_style_monitor_table(_spao_key, _inout_key):
    """스파오 시트 + BASE 입출고를 병합한 스타일별 시즌/입고/출고/온라인등록 테이블."""
    sources = get_excel_sources()
    spao_bytes, _ = sources.get("spao", (None, None))
    inout_bytes, _ = sources.get("inout", (None, None))
    df_sp = load_spao_style_register_df(spao_bytes, _cache_key=_spao_key)
    df_inout = load_inout_data(inout_bytes, _cache_key=_inout_key)
    df_io = build_inout_status_df(df_inout)
    if df_sp.empty and df_io.empty:
        return pd.DataFrame()
    df_sp["스타일코드_norm"] = df_sp["스타일코드"].str.strip()
    if not df_io.empty:
        df_io["스타일코드_norm"] = df_io["스타일코드"].str.strip()
    if not df_sp.empty:
        agg_sp = df_sp.groupby("스타일코드_norm").agg(
            시즌=("시즌", lambda s: s.dropna().astype(str).str.strip().iloc[0] if len(s.dropna()) else ""),
            공홈등록일=("공홈등록일", lambda s: s.dropna().astype(str).str.strip().iloc[0] if len(s.dropna()) else ""),
            공홈등록여부=("공홈등록여부", lambda s: "등록" if (s.astype(str).str.strip().str.upper() == "등록").any() else ""),
        ).reset_index()
        agg_sp = agg_sp.rename(columns={"스타일코드_norm": "스타일코드"})
    else:
        agg_sp = pd.DataFrame(columns=["스타일코드", "시즌", "공홈등록일", "공홈등록여부"])
    if not df_io.empty:
        # 스타일 기준 유니크 (입고/출고는 any)
        df_io_u = df_io.groupby("스타일코드_norm").agg(입고여부=("입고여부", "any"), 출고여부=("출고여부", "any")).reset_index()
        if not agg_sp.empty:
            merged = agg_sp.merge(df_io_u, left_on="스타일코드", right_on="스타일코드_norm", how="left").drop(columns=["스타일코드_norm"], errors="ignore")
        else:
            merged = df_io_u.rename(columns={"스타일코드_norm": "스타일코드"}).copy()
            merged["시즌"] = ""
            merged["공홈등록일"] = ""
            merged["공홈등록여부"] = ""
    else:
        merged = agg_sp.copy()
        merged["입고여부"] = False
        merged["출고여부"] = False
    if "공홈등록여부" in merged.columns:
        merged["온라인상품등록여부"] = merged["공홈등록여부"].astype(str).str.strip().apply(lambda x: "등록" if str(x).upper() == "등록" else "미등록")
    else:
        merged["온라인상품등록여부"] = "미등록"
    # 표시용 컬럼 정리
    merged["입고 여부"] = merged["입고여부"].map({True: "Y", False: "N"})
    merged["출고 여부"] = merged["출고여부"].map({True: "Y", False: "N"})
    display_cols = ["스타일코드", "시즌", "입고 여부", "출고 여부", "온라인상품등록여부"]
    if "공홈등록일" in merged.columns:
        display_cols.insert(3, "공홈등록일")
    return merged[[c for c in display_cols if c in merged.columns]]

# ========== Streamlit UI ==========
st.set_page_config(
    page_title="스파오 스타일 현황",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("# 스파오 스타일별 시즌·입고·출고·온라인 등록 현황")
st.caption(f"기준 시각: {datetime.now().strftime('%Y-%m-%d %H:%M')} | deploy.py와 동일한 Google 시트/BASE 소스 사용")

sources = get_excel_sources()
_spao_key = sources.get("spao", (None, None))[1]
_inout_key = sources.get("inout", (None, None))[1]

if not _spao_key or _spao_key == "none":
    st.warning("SP_SPREADSHEET_ID가 설정되지 않았습니다. Streamlit Secrets에 스파오 시트 ID를 넣어주세요.")
    st.stop()

df = build_spao_style_monitor_table(_spao_key, _inout_key)
if df.empty:
    st.info("표시할 스파오 스타일 데이터가 없습니다. 시트에 스타일코드·시즌·공홈등록일(또는 등록여부) 컬럼이 있는지 확인해 주세요.")
    st.stop()

# 필터
st.sidebar.markdown("### 필터")
season_options = ["전체"] + sorted(df["시즌"].dropna().astype(str).str.strip().replace("", "미기입").unique().tolist())
if "미기입" in season_options:
    season_options.remove("미기입")
    season_options.append("미기입")
filter_season = st.sidebar.selectbox("시즌", season_options, index=0)
filter_in = st.sidebar.selectbox("입고 여부", ["전체", "Y", "N"], index=0)
filter_out = st.sidebar.selectbox("출고 여부", ["전체", "Y", "N"], index=0)
filter_reg = st.sidebar.selectbox("온라인 상품 등록 여부", ["전체", "등록", "미등록"], index=0)

df_f = df.copy()
if filter_season != "전체":
    if filter_season == "미기입":
        df_f = df_f[df_f["시즌"].astype(str).str.strip().isin(["", "nan", "None"])]
    else:
        df_f = df_f[df_f["시즌"].astype(str).str.strip() == filter_season]
if filter_in != "전체":
    df_f = df_f[df_f["입고 여부"] == filter_in]
if filter_out != "전체":
    df_f = df_f[df_f["출고 여부"] == filter_out]
if filter_reg != "전체":
    df_f = df_f[df_f["온라인상품등록여부"] == filter_reg]

st.sidebar.metric("표시 행 수", len(df_f))
st.sidebar.metric("전체 스타일 수", len(df))

st.dataframe(
    df_f,
    use_container_width=True,
    hide_index=True,
)

# CSV 다운로드
csv = df_f.to_csv(index=False, encoding="utf-8-sig")
st.download_button(
    label="CSV 다운로드",
    data=csv,
    file_name=f"스파오_스타일현황_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
    mime="text/csv",
    key="spao_style_csv",
)
