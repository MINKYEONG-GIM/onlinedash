# -*- coding: utf-8 -*-
"""
브랜드별·시즌별 스타일 입고/출고/온라인등록 실시간 모니터링.
- 입출고: BASE 시트(전브랜드)
- 온라인등록: 각 브랜드별 스프레드시트
실행: streamlit run spao_style_dashboard.py
"""
from __future__ import annotations

import os
import html as html_lib
import streamlit as st
import pandas as pd
from io import BytesIO
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import JsCode
from datetime import datetime
from google.oauth2.service_account import Credentials

# =====================================================
# 페이지 설정
# =====================================================
st.set_page_config(
    page_title="브랜드별 스타일 모니터링",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =====================================================
# Secrets / 시트 ID (deploy와 동일)
# =====================================================
def _secret(key, default=""):
    try:
        v = st.secrets.get(key, default) or default
        return str(v).strip() if v else default
    except Exception:
        return default

def _norm_sheet_id(val):
    return str(val).strip() if val else ""

_SPREADSHEET_SECRET_KEYS = [
    ("inout", "BASE_SPREADSHEET_ID"),
    ("spao", "SP_SPREADSHEET_ID"),
    ("whoau", "WH_SPREADSHEET_ID"),
    ("clavis", "CV_SPREADSHEET_ID"),
    ("mixxo", "MI_SPREADSHEET_ID"),
    ("roem", "RM_SPREADSHEET_ID"),
    ("shoopen", "HP_SPREADSHEET_ID"),
    ("eblin", "EB_SPREADSHEET_ID"),
]
GOOGLE_SPREADSHEET_IDS = {k: _norm_sheet_id(_secret(s)) for k, s in _SPREADSHEET_SECRET_KEYS}

# 브랜드·BU 그룹 (deploy와 동일)
brands_list = ["스파오", "뉴발란스", "뉴발란스키즈", "후아유", "슈펜", "미쏘", "로엠", "클라비스", "에블린"]
bu_groups = [
    ("캐쥬얼BU", ["스파오"]),
    ("스포츠BU", ["뉴발란스", "뉴발란스키즈", "후아유", "슈펜"]),
    ("여성BU", ["미쏘", "로엠", "클라비스", "에블린"]),
]
BRAND_TO_KEY = {
    "스파오": "spao", "후아유": "whoau", "클라비스": "clavis", "미쏘": "mixxo",
    "로엠": "roem", "슈펜": "shoopen", "에블린": "eblin",
}

# =====================================================
# Google 인증 / 시트 다운로드
# =====================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

def _get_google_credentials():
    import json
    try:
        raw = st.secrets.get("google_service_account") if hasattr(st.secrets, "get") else None
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
        for name in ("service_account.json", "credentials.json"):
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

def _fetch_google_sheet_via_sheets_api(sid, creds):
    try:
        from googleapiclient.discovery import build
        from openpyxl import Workbook
        svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
        meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
        names = [s["properties"]["title"] for s in meta.get("sheets", [])]
        if not names:
            return None
        wb = Workbook()
        wb.remove(wb.active)
        for idx, title in enumerate(names):
            try:
                rng = f"'{title.replace(chr(39), chr(39)+chr(39))}'" if title else f"Sheet{idx+1}"
                rows = svc.spreadsheets().values().get(spreadsheetId=sid, range=rng).execute().get("values", [])
            except Exception:
                rows = []
            ws = wb.create_sheet(title=(title[:31] if title else f"Sheet{idx+1}"), index=idx)
            for row in rows:
                ws.append(row)
        out = BytesIO()
        wb.save(out)
        out.seek(0)
        return out.read()
    except Exception:
        return None

@st.cache_data(ttl=300)
def fetch_sheet_bytes(sheet_id):
    if not sheet_id:
        return None
    creds = _get_google_credentials()
    if not creds:
        return None
    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseDownload
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, service.files().export_media(
            fileId=sheet_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ))
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read()
    except Exception:
        pass
    return _fetch_google_sheet_via_sheets_api(sheet_id, creds)

@st.cache_data(ttl=300)
def get_all_sources():
    return {k: (fetch_sheet_bytes(GOOGLE_SPREADSHEET_IDS.get(k)), k) for k in GOOGLE_SPREADSHEET_IDS}

# =====================================================
# 컬럼 탐지
# =====================================================
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

# =====================================================
# BASE 입출고 로드 (deploy와 유사)
# =====================================================
@st.cache_data(ttl=300)
def load_base_inout(io_bytes=None, _cache_key=None):
    if io_bytes is None or len(io_bytes) == 0:
        return pd.DataFrame()
    excel_file = pd.ExcelFile(BytesIO(io_bytes))
    sheet_candidates = [s for s in excel_file.sheet_names if not str(s).startswith("_")]
    sheet_name = sheet_candidates[0] if sheet_candidates else excel_file.sheet_names[-1]
    preview = pd.read_excel(BytesIO(io_bytes), sheet_name=sheet_name, header=None)
    header_keywords = ["브랜드", "스타일", "최초입고일", "입고", "출고", "판매"]
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
        df["브랜드"] = prefix.map({
            "sp": "스파오", "rm": "로엠", "mi": "미쏘", "wh": "후아유", "hp": "슈펜",
            "cv": "클라비스", "eb": "에블린", "nb": "뉴발란스", "nk": "뉴발란스키즈",
        })
    return df

# =====================================================
# 브랜드별 등록 시트 로드 (스타일코드, 시즌, 공홈등록일 → 온라인상품등록여부)
# =====================================================
def _normalize(v):
    return "".join(str(v).split()) if v is not None else ""

@st.cache_data(ttl=120)
def load_brand_register_df(io_bytes=None, _cache_key=None):
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
            if any("스타일코드" in v for v in norm) and any("공홈등록일" in v for v in norm):
                header_row_idx, header_vals = i, norm
                break

        if header_row_idx is None:
            continue

        def fi(key):
            for idx, v in enumerate(header_vals):
                if key in v:
                    return idx
            return None

        style_col = fi("스타일코드") or fi("스타일")
        season_col = fi("시즌") or fi("Season")
        regdate_col = fi("공홈등록일")

        if style_col is None or regdate_col is None:
            continue

        data = df_raw.iloc[header_row_idx + 1 :].copy()
        data.columns = range(data.shape[1])

        out = pd.DataFrame()
        out["스타일코드"] = data.iloc[:, style_col].astype(str).str.strip()
        out["시즌"] = (
            data.iloc[:, season_col].astype(str).str.strip()
            if season_col is not None and season_col < data.shape[1]
            else ""
        )

        # 핵심: 공홈등록일 기준으로 등록여부 생성
        reg_series = data.iloc[:, regdate_col]
        reg_ok = pd.to_datetime(reg_series, errors="coerce").notna()

        out["온라인상품등록여부"] = reg_ok.map({True: "등록", False: "미등록"})

        out = out[out["스타일코드"].str.len() > 0]
        out = out[out["스타일코드"] != "nan"]

        return out

    return pd.DataFrame()

# =====================================================
# 전체 스타일 테이블 (BASE + 각 브랜드 시트 병합)
# =====================================================
def build_style_table_all(sources):
    base_bytes = sources.get("inout", (None, None))[0]
    df_base = load_base_inout(base_bytes, _cache_key="inout")
    if df_base.empty:
        return pd.DataFrame()
    style_col = find_col(["스타일코드", "스타일"], df=df_base)
    brand_col = "브랜드" if "브랜드" in df_base.columns else None
    season_col = find_col(["시즌", "season"], df=df_base)
    first_in_col = find_col(["최초입고일", "입고일"], df=df_base)
    out_amt_col = find_col(["출고액"], df=df_base)
    if not style_col or not brand_col:
        return pd.DataFrame()
    df_base = df_base[df_base[style_col].astype(str).str.strip().str.len() > 0].copy()
    df_base["_style"] = df_base[style_col].astype(str).str.strip()
    df_base["_brand"] = df_base[brand_col].astype(str).str.strip()
    df_base["_season"] = df_base[season_col].astype(str).str.strip() if season_col and season_col in df_base.columns else ""
    first_vals = df_base[first_in_col] if first_in_col and first_in_col in df_base.columns else pd.Series(dtype=object)
    df_base["_입고"] = pd.to_datetime(first_vals, errors="coerce").notna()
    if first_in_col and first_in_col in df_base.columns:
        num = pd.to_numeric(df_base[first_in_col], errors="coerce")
        df_base.loc[num.between(1, 60000, inclusive="both"), "_입고"] = True
    out_vals = df_base[out_amt_col] if out_amt_col and out_amt_col in df_base.columns else pd.Series(0, index=df_base.index)
    df_base["_출고"] = pd.to_numeric(out_vals, errors="coerce").fillna(0) > 0
    base_agg = df_base.groupby(["_brand", "_style"]).agg(
        _season=("_season", lambda s: s.dropna().astype(str).str.strip().iloc[0] if len(s.dropna()) else ""),
        입고여부=("_입고", "any"),
        출고여부=("_출고", "any"),
    ).reset_index()
    base_agg = base_agg.rename(columns={"_brand": "브랜드", "_style": "스타일코드", "_season": "시즌"})
    rows = []
    all_brands = base_agg["브랜드"].dropna().unique().tolist()
    for brand_name in all_brands:
        brand_key = BRAND_TO_KEY.get(brand_name)
        b_agg = base_agg[base_agg["브랜드"] == brand_name]
        if b_agg.empty:
            continue
        if brand_key is None:
            for _, r in b_agg.iterrows():
                rows.append({
                    "브랜드": brand_name,
                    "스타일코드": r["스타일코드"],
                    "시즌": r["시즌"],
                    "입고 여부": "Y" if r["입고여부"] else "N",
                    "출고 여부": "Y" if r["출고여부"] else "N",
                    "온라인상품등록여부": "미등록",
                })
            continue
        reg_bytes = sources.get(brand_key, (None, None))[0]
        df_reg = load_brand_register_df(reg_bytes, _cache_key=brand_key)
        if df_reg.empty:
            for _, r in b_agg.iterrows():
                rows.append({
                    "브랜드": brand_name,
                    "스타일코드": r["스타일코드"],
                    "시즌": r["시즌"],
                    "입고 여부": "Y" if r["입고여부"] else "N",
                    "출고 여부": "Y" if r["출고여부"] else "N",
                    "온라인상품등록여부": "미등록",
                })
            continue
        df_reg["스타일코드_norm"] = df_reg["스타일코드"].str.strip()
        merged = b_agg.merge(
            df_reg[["스타일코드_norm", "온라인상품등록여부"]],
            left_on="스타일코드",
            right_on="스타일코드_norm",
            how="left",
        )
        for _, r in merged.iterrows():
            reg = r.get("온라인상품등록여부", "미등록")
            if pd.isna(reg) or str(reg).strip() == "":
                reg = "미등록"

            rows.append({
                "브랜드": brand_name,
                "스타일코드": r["스타일코드"],
                "시즌": r["시즌"],
                "입고 여부": "Y" if r["입고여부"] else "N",
                "출고 여부": "Y" if r["출고여부"] else "N",
                "온라인상품등록여부": reg,
            })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)

# =====================================================
# 브랜드별 입출고 집계 (발주/입고/출고/판매 STY·액)
# =====================================================
def build_inout_aggregates(io_bytes):
    df = load_base_inout(io_bytes, _cache_key="base")
    if df.empty:
        return [], {}, pd.DataFrame()
    style_col = find_col(["스타일코드", "스타일"], df=df)
    brand_col = "브랜드" if "브랜드" in df.columns else None
    order_qty_col = find_col(["발주 STY", "발주수", "발주량"], df=df)
    order_amt_col = find_col(["발주액"], df=df)
    in_amt_col = find_col(["누적입고액", "입고액"], df=df)
    out_amt_col = find_col(["출고액"], df=df)
    sale_amt_col = find_col(["누적판매액", "판매액"], df=df)
    first_in_col = find_col(["최초입고일", "입고일"], df=df)
    if not style_col or not brand_col:
        return [], {}, pd.DataFrame()
    season_col = find_col(["시즌", "season"], df=df)
    df["_style"] = df[style_col].astype(str).str.strip()
    df["_brand"] = df[brand_col].astype(str).str.strip()
    df["_season"] = df[season_col].astype(str).str.strip() if season_col and season_col in df.columns else ""
    in_ok = pd.Series(False, index=df.index)
    if first_in_col:
        in_ok = pd.to_datetime(df[first_in_col], errors="coerce").notna()
        num = pd.to_numeric(df[first_in_col], errors="coerce")
        in_ok = in_ok | num.between(1, 60000, inclusive="both")
    df["_in"] = in_ok
    df["_out"] = pd.to_numeric(df[out_amt_col], errors="coerce").fillna(0) > 0 if out_amt_col else False
    df["_sale"] = pd.to_numeric(df[sale_amt_col], errors="coerce").fillna(0) > 0 if sale_amt_col else False
    def cnt_unique(g, col="_style"):
        return g[col].nunique()
    def sum_amt(g, c):
        return pd.to_numeric(g[c], errors="coerce").fillna(0).sum() if c and c in g.columns else 0
    order_g = df.groupby("_brand") if order_qty_col else None
    in_g = df[df["_in"]].groupby("_brand")
    out_g = df[df["_out"]].groupby("_brand")
    sale_g = df[df["_sale"]].groupby("_brand") if sale_amt_col else df.groupby("_brand")
    brands = df["_brand"].dropna().unique().tolist()
    brand_order_qty = order_g["_style"].nunique().to_dict() if order_g is not None else {}
    brand_order_amt = df.groupby("_brand").apply(lambda g: sum_amt(g, order_amt_col)).to_dict() if order_amt_col else {}
    brand_in_qty = in_g["_style"].nunique().to_dict()
    brand_in_amt = df[df["_in"]].groupby("_brand").apply(lambda g: sum_amt(g, in_amt_col)).to_dict() if in_amt_col else {}
    brand_out_qty = out_g["_style"].nunique().to_dict()
    brand_out_amt = df[df["_out"]].groupby("_brand").apply(lambda g: sum_amt(g, out_amt_col)).to_dict() if out_amt_col else {}
    brand_sale_qty = sale_g["_style"].nunique().to_dict()
    brand_sale_amt = df.groupby("_brand").apply(lambda g: sum_amt(g, sale_amt_col)).to_dict() if sale_amt_col else {}
    def fmt_num(v):
        return f"{int(v):,}" if pd.notna(v) and v != "" else "0"
    def fmt_eok(v):
        try:
            return f"{float(v) / 1e8:,.0f} 억 원"
        except Exception:
            return "0 억 원"
    rows = []
    for _, bu_brands in bu_groups:
        for b in bu_brands:
            rows.append({
                "브랜드": b,
                "발주 STY수": fmt_num(brand_order_qty.get(b, 0)),
                "발주액": fmt_eok(brand_order_amt.get(b, 0)),
                "입고 STY수": fmt_num(brand_in_qty.get(b, 0)),
                "입고액": fmt_eok(brand_in_amt.get(b, 0)),
                "출고 STY수": fmt_num(brand_out_qty.get(b, 0)),
                "출고액": fmt_eok(brand_out_amt.get(b, 0)),
                "판매 STY수": fmt_num(brand_sale_qty.get(b, 0)),
                "판매액": fmt_eok(brand_sale_amt.get(b, 0)),
            })
    # 브랜드·시즌 집계 (expander용)
    g = df.groupby(["_brand", "_season"])
    bs_parts = []
    for (b, s), grp in g:
        in_grp = df[(df["_brand"] == b) & (df["_season"] == s) & df["_in"]]
        out_grp = df[(df["_brand"] == b) & (df["_season"] == s) & df["_out"]]
        sale_grp = df[(df["_brand"] == b) & (df["_season"] == s) & df["_sale"]]
        bs_parts.append({
            "브랜드": b, "시즌": s,
            "발주 STY수": grp["_style"].nunique(),
            "발주액": sum_amt(grp, order_amt_col) if order_amt_col else 0,
            "입고 STY수": in_grp["_style"].nunique(),
            "입고액": sum_amt(in_grp, in_amt_col) if in_amt_col else 0,
            "출고 STY수": out_grp["_style"].nunique(),
            "출고액": sum_amt(out_grp, out_amt_col) if out_amt_col else 0,
            "판매 STY수": sale_grp["_style"].nunique(),
            "판매액": sum_amt(grp, sale_amt_col) if sale_amt_col else 0,
        })
    brand_season_df = pd.DataFrame(bs_parts)
    return rows, {"brand_in_qty": brand_in_qty, "brand_out_qty": brand_out_qty, "brand_sale_qty": brand_sale_qty}, brand_season_df

# =====================================================
# 다크 테마 CSS (deploy와 동일)
# =====================================================
DARK_CSS = """
<style>
    .stApp { background: #0f172a; }
    .block-container { background: #0f172a; padding-top: 2.5rem; padding-bottom: 2rem; }
    .fashion-title {
        display: inline-block;
        background: #14b8a6;
        color: #0f172a;
        padding: 0.65rem 1.2rem 0.5rem 1.2rem;
        border-radius: 8px 8px 0 0;
        font-weight: 700;
        font-size: 1.25rem;
        margin-bottom: 0;
        margin-top: 0.5rem;
    }
    .update-time { font-size: 0.85rem; color: #94a3b8; margin-top: 0.25rem; }
    .section-title {
        font-size: 2.2rem;
        font-weight: 700;
        color: #f1f5f9;
        margin: 1rem 0 0.5rem 0;
    }
    .kpi-card-dark {
        background: #1e293b;
        color: #f1f5f9;
        border-radius: 10px;
        padding: 1rem 1.2rem;
        text-align: center;
        font-weight: 600;
        min-height: 100px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        border: 1px solid #334155;
    }
    .kpi-card-dark .label { font-size: 1.1rem; margin-bottom: 0.3rem; color: #cbd5e1; }
    .kpi-card-dark .value { font-size: 1rem; font-weight: 700; color: #f1f5f9; }
    .kpi-card-half {
        background: #1e293b;
        color: #f1f5f9;
        border-radius: 10px;
        padding: 0.5rem 1rem;
        text-align: center;
        font-weight: 600;
        height: 50px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        border: 1px solid #334155;
    }
    .kpi-card-half .label { font-size: 0.9rem; color: #cbd5e1; }
    .kpi-card-half .value { font-size: 0.9rem; font-weight: 700; }
    .monitor-table {
        width: 100%;
        border-collapse: collapse;
        background: #1e293b;
        color: #f1f5f9;
        border: 1px solid #334155;
    }
    .monitor-table th, .monitor-table td {
        border: 1px solid #334155;
        padding: 6px 8px;
        text-align: center;
        font-size: 0.95rem;
    }
    .monitor-table thead th {
        background: #0f172a;
        color: #f1f5f9;
        font-weight: 700;
    }
    .monitor-table .group-head { background: #111827; color: #f1f5f9; font-size: 1rem; }
    .monitor-table tr.bu-row td {
        background-color: #d9f7ee;
        color: #000000;
        font-size: 1.15rem;
        font-weight: 700;
    }
    .monitor-table .rate-help, .monitor-table .avg-help, .monitor-table .sum-help {
        position: relative;
        display: inline-block;
        cursor: help;
    }
    .monitor-table .rate-help::after, .monitor-table .avg-help::after, .monitor-table .sum-help::after {
        content: "";
        position: absolute;
        opacity: 0;
        pointer-events: none;
        transition: opacity 0.15s ease-in-out;
        left: 50%;
        transform: translateX(-50%);
        bottom: calc(100% + 6px);
        white-space: pre;
        word-break: keep-all;
        width: max-content;
        max-width: 280px;
        background: #111827;
        color: #f1f5f9;
        padding: 6px 8px;
        border-radius: 6px;
        font-size: 0.85rem;
        text-align: left;
        box-shadow: 0 4px 12px rgba(0,0,0,0.35);
        z-index: 20;
    }
    .monitor-table .rate-help:hover::after { content: attr(data-tooltip); opacity: 1; }
    .monitor-table .avg-help:hover::after { content: attr(data-tooltip); opacity: 1; }
    .monitor-table .sum-help:hover::after { content: attr(data-tooltip); opacity: 1; }
    .inout-table {
        width: 100%;
        border-collapse: collapse;
        background: #1e293b;
        color: #f1f5f9;
        border: 1px solid #334155;
        border-radius: 8px;
        overflow: hidden;
    }
    .inout-table th, .inout-table td {
        border: 1px solid #334155;
        padding: 6px 8px;
        text-align: center;
        font-size: 0.95rem;
    }
    .inout-table thead th { background: #0f172a; color: #f1f5f9; font-weight: 700; }
    .inout-table tr.bu-row td {
        background-color: #d9f7ee;
        color: #000000;
        font-size: 1.15rem;
        font-weight: 700;
    }
    .inout-table .brand-cell { text-align: left; }
    [data-testid='stSelectbox'] label, [data-testid='stMultiSelect'] label { color: #f1f5f9 !important; }
</style>
"""

# =====================================================
# UI
# =====================================================
update_time = datetime.now()
sources = get_all_sources()
base_bytes = sources.get("inout", (None, None))[0]
df_style_all = build_style_table_all(sources)

st.markdown(DARK_CSS, unsafe_allow_html=True)

# 상단: 타이틀 + 업데이트 시각
col_head_left, col_head_right = st.columns([2, 3])
with col_head_left:
    st.markdown('<div class="fashion-title">온라인 리드타임 대시보드</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="update-time">업데이트시간 {update_time.strftime("%Y-%m-%d %H:%M")}</div>', unsafe_allow_html=True)
with col_head_right:
    col_yr, col_season, col_brand = st.columns([1, 2, 2])
    with col_yr:
        st.markdown('<div style="font-size:0.875rem;color:#f1f5f9;margin-bottom:0.25rem;">연도</div>', unsafe_allow_html=True)
        st.markdown('<div style="font-weight:600;color:#f8fafc;">2026년</div>', unsafe_allow_html=True)
    with col_season:
        seasons = ["1", "2", "A", "S", "F"]
        selected_seasons = st.multiselect("시즌", seasons, default=["2"], key="season_filter")
    with col_brand:
        brand_options = ["브랜드 전체"] + brands_list
        selected_brand = st.selectbox("브랜드", brand_options, key="brand_filter", index=0)

# 필터 적용
df_style = df_style_all.copy()
if selected_seasons and set(selected_seasons) != set(seasons):
    df_style = df_style[df_style["시즌"].astype(str).str.strip().isin(selected_seasons)]
if selected_brand and selected_brand != "브랜드 전체":
    df_style = df_style[df_style["브랜드"] == selected_brand]

# KPI 카드 (BASE 기준 집계, 필터 선택 브랜드 반영)
inout_rows, inout_agg, brand_season_df = build_inout_aggregates(base_bytes)
df_base = load_base_inout(base_bytes, _cache_key="base")

# 브랜드 필터 적용: 특정 브랜드 선택 시 해당 브랜드만 KPI에 반영
brand_col_base = "브랜드" if "브랜드" in df_base.columns else None
if selected_brand and selected_brand != "브랜드 전체" and brand_col_base:
    df_base = df_base[df_base[brand_col_base].astype(str).str.strip() == selected_brand].copy()

if selected_brand and selected_brand != "브랜드 전체":
    total_in_sty = inout_agg.get("brand_in_qty", {}).get(selected_brand, 0)
    total_out_sty = inout_agg.get("brand_out_qty", {}).get(selected_brand, 0)
    total_sale_sty = inout_agg.get("brand_sale_qty", {}).get(selected_brand, 0)
else:
    total_in_sty = sum(inout_agg.get("brand_in_qty", {}).values())
    total_out_sty = sum(inout_agg.get("brand_out_qty", {}).values())
    total_sale_sty = sum(inout_agg.get("brand_sale_qty", {}).values())

in_amt_col = find_col(["누적입고액", "입고액"], df=df_base)
out_amt_col = find_col(["출고액"], df=df_base)
# 판매액 컬럼 (외형매출 기준)
sale_amt_col = find_col(["누적 판매액[외형매출]", "누적판매액", "판매액"], df=df_base)

# 채널 컬럼 탐지
channel_col = find_col(["채널(Now)"], df=df_base)

total_in_amt = pd.to_numeric(df_base[in_amt_col], errors="coerce").sum() if in_amt_col and in_amt_col in df_base.columns else 0
total_out_amt = pd.to_numeric(df_base[out_amt_col], errors="coerce").sum() if out_amt_col and out_amt_col in df_base.columns else 0

total_sale_amt = 0
online_sale_amt = 0
offline_sale_amt = 0

if sale_amt_col and sale_amt_col in df_base.columns:
    sale_series = pd.to_numeric(df_base[sale_amt_col], errors="coerce").fillna(0)
    total_sale_amt = sale_series.sum()

    if channel_col and channel_col in df_base.columns:
        channel_series = df_base[channel_col].astype(str).str.strip()

        online_mask = channel_series == "온라인매장"
        online_sale_amt = sale_series[online_mask].sum()
        offline_sale_amt = sale_series[~online_mask].sum()
    else:
        # 채널 컬럼이 없으면 전체를 오프라인으로 처리
        offline_sale_amt = total_sale_amt

def _eok(x):
    try:
        return f"{float(x) / 1e8:,.2f}"
    except Exception:
        return "0"
st.markdown("<div style='margin-top:1rem;'></div>", unsafe_allow_html=True)

k1, k2, k3, k_right = st.columns([1, 1, 1, 1])

with k1:
    st.markdown(
        f'<div class="kpi-card-dark"><span class="label">입고</span>'
        f'<span class="value">{_eok(total_in_amt)} 억원 / {int(total_in_sty):,}STY</span></div>',
        unsafe_allow_html=True
    )

with k2:
    st.markdown(
        f'<div class="kpi-card-dark"><span class="label">출고</span>'
        f'<span class="value">{_eok(total_out_amt)} 억원 / {int(total_out_sty):,}STY</span></div>',
        unsafe_allow_html=True
    )

with k3:
    st.markdown(
        f'<div class="kpi-card-dark"><span class="label">전체 판매</span>'
        f'<span class="value">{_eok(total_sale_amt)} 억원 / {int(total_sale_sty):,}STY</span></div>',
        unsafe_allow_html=True
    )

with k_right:
    st.markdown(
        f'<div class="kpi-card-half" style="margin-bottom:4px;"><span class="label">온라인 판매</span><span class="value">{_eok(online_sale_amt)} 억원</span></div>'
        f'<div class="kpi-card-half"><span class="label">오프라인 판매</span><span class="value">{_eok(offline_sale_amt)} 억원</span></div>',
        unsafe_allow_html=True
    )

# 브랜드별 상품등록 모니터링
st.markdown("<div style='margin-top:80px;'></div>", unsafe_allow_html=True)
st.markdown("---")
st.markdown('<div class="section-title">브랜드별 상품등록 모니터링</div>', unsafe_allow_html=True)

# 표 전용 (필터 무시, 전체 브랜드)
df_style_unique = df_style_all.drop_duplicates(subset=["브랜드", "시즌", "스타일코드"])
in_count_all = df_style_unique[df_style_unique["입고 여부"] == "Y"].groupby("브랜드")["스타일코드"].nunique()
reg_count_all = df_style_unique[df_style_unique["온라인상품등록여부"] == "등록"].groupby("브랜드")["스타일코드"].nunique()
all_brands = sorted(df_style_unique["브랜드"].unique())
table_df = pd.DataFrame({"브랜드": all_brands})
table_df["입고스타일수"] = table_df["브랜드"].map(in_count_all).fillna(0).astype(int)
table_df["온라인등록스타일수"] = table_df["브랜드"].map(reg_count_all).fillna(0).astype(int)
table_df["온라인등록율"] = (table_df["온라인등록스타일수"] / table_df["입고스타일수"].replace(0, 1)).round(2)
table_df["전체 미등록스타일"] = table_df["입고스타일수"] - table_df["온라인등록스타일수"]
table_df["등록수"] = table_df["온라인등록스타일수"]
table_df["평균 등록 소요일수"] = "-"
table_df["미분배(분배팀)"] = "-"
bu_labels = {label for label, _ in bu_groups}
monitor_df = table_df.copy()
monitor_df["_등록율"] = (monitor_df["온라인등록율"] * 100).astype(int).astype(str) + "%"
monitor_df["_미등록"] = monitor_df["전체 미등록스타일"].astype(int)

def safe_cell(v):
    s = html_lib.escape(str(v)) if v is not None and str(v) != "nan" else ""
    return s
rate_tip = "(초록불) 90% 초과&#10;(노란불) 80% 초과&#10;(빨간불) 80% 이하"
avg_tip = "온라인상품등록일 - 최초입고일"
sum_tip = "입고스타일수 - 온라인등록스타일수"
header_monitor = (
    "<tr>"
    "<th>브랜드</th>"
    "<th>입고스타일수</th>"
    "<th>온라인등록<br>스타일수</th>"
    f"<th><span class='rate-help' data-tooltip='{rate_tip}'>온라인<br>등록율</span></th>"
    f"<th><span class='avg-help' data-tooltip='{avg_tip}'>평균 등록 소요일수<br><span style='font-size:0.8rem;font-weight:500;color:#94a3b8;'>온라인상품등록일 - 최초입고일</span></span></th>"
    "<th>등록수</th>"
    f"<th><span class='rate-help' data-tooltip='{rate_tip}'>온라인등록율</span></th>"
    f"<th><span class='sum-help' data-tooltip='{sum_tip}'>전체 미등록 스타일</span></th>"
    "<th>미분배<br>(분배팀)</th>"
    "</tr>"
)
def _fmt(n):
    return f"{int(n):,}"
body_monitor = "".join(
    ("<tr class='bu-row'>" if r["브랜드"] in bu_labels else "<tr>")
    + f"<td>{safe_cell(r['브랜드'])}</td><td>{safe_cell(_fmt(r['입고스타일수']))}</td><td>{safe_cell(_fmt(r['온라인등록스타일수']))}</td><td>{safe_cell(r['_등록율'])}</td><td>{safe_cell(r['평균 등록 소요일수'])}</td><td>{safe_cell(_fmt(r['등록수']))}</td><td>{safe_cell(r['_등록율'])}</td><td>{safe_cell(_fmt(r['_미등록']))}</td><td>{safe_cell(r['미분배(분배팀)'])}</td></tr>"
    for _, r in monitor_df.iterrows()
)
st.markdown(f"""
<div class="monitor-table">
<table class="monitor-table">
<thead>{header_monitor}</thead>
<tbody>{body_monitor}</tbody>
</table>
</div>
""", unsafe_allow_html=True)

# 브랜드별 입출고 모니터링 (AgGrid 트리)
def build_tree_df(brand_season_df, brand_order):
    rows = []
    for brand in brand_order:
        brand_df = brand_season_df[brand_season_df["브랜드"] == brand]
        if brand_df.empty:
            rows.append({"path": brand, "발주 STY수": 0, "발주액": 0, "입고 STY수": 0, "입고액": 0, "출고 STY수": 0, "출고액": 0, "판매 STY수": 0, "판매액": 0})
            continue
        total = brand_df.sum(numeric_only=True)
        rows.append({
            "path": brand,
            "발주 STY수": int(total["발주 STY수"]), "발주액": int(total["발주액"]),
            "입고 STY수": int(total["입고 STY수"]), "입고액": int(total["입고액"]),
            "출고 STY수": int(total["출고 STY수"]), "출고액": int(total["출고액"]),
            "판매 STY수": int(total["판매 STY수"]), "판매액": int(total["판매액"]),
        })
        for _, row in brand_df.sort_values("시즌").iterrows():
            s = str(row["시즌"]).strip()
            rows.append({
                "path": f"{brand}|{s}",
                "발주 STY수": int(row["발주 STY수"]), "발주액": int(row["발주액"]),
                "입고 STY수": int(row["입고 STY수"]), "입고액": int(row["입고액"]),
                "출고 STY수": int(row["출고 STY수"]), "출고액": int(row["출고액"]),
                "판매 STY수": int(row["판매 STY수"]), "판매액": int(row["판매액"]),
            })
    return pd.DataFrame(rows)

st.markdown('<div style="height:40px;"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">브랜드별 입출고 모니터링</div>', unsafe_allow_html=True)
st.markdown('<div style="font-size:1.1rem;color:#cbd5e1;margin-bottom:0.5rem;">STY 기준 통계</div>', unsafe_allow_html=True)
st.caption("브랜드 옆 화살표를 누르면 시즌별 상세를 볼 수 있습니다")
tree_df = build_tree_df(brand_season_df, [r["브랜드"] for r in inout_rows])
gb = GridOptionsBuilder.from_dataframe(tree_df)
gb.configure_column("path", hide=True, width=0)
# 금액 컬럼: 원 → "N,XXX억 원"
eok_fmt = JsCode("function(params) { if (params.value == null) return ''; var v = Number(params.value)/1e8; return v.toLocaleString('ko-KR') + '억 원'; }")
for col in ["발주액", "입고액", "출고액", "판매액"]:
    gb.configure_column(col, valueFormatter=eok_fmt)
get_data_path_js = JsCode("function(params) { var p = params.data.path; return typeof p === 'string' ? p.split('|') : (p || []); }")
gb.configure_grid_options(
    treeData=True,
    getDataPath=get_data_path_js,
    autoGroupColumnDef={
        "headerName": "브랜드",
        "minWidth": 180,
        "cellRendererParams": {"suppressCount": True, "innerRenderer": None},
    },
    groupDefaultExpanded=0,
)
AgGrid(tree_df, grid_options=gb.build(), allow_unsafe_jscode=True, fit_columns_on_grid_load=True, height=500)
