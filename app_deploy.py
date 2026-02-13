# -*- coding: utf-8 -*-
from __future__ import annotations

import streamlit as st
import pandas as pd
import os
from io import BytesIO
from datetime import datetime
from google.oauth2.service_account import Credentials

# =====================================================
# 기본 설정
# =====================================================
st.set_page_config(
    page_title="브랜드별 스타일 모니터링",
    layout="wide",
)

st.title("브랜드별 스타일 입고 · 출고 · 온라인 등록 현황")
st.caption(f"기준 시각: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# =====================================================
# Secrets 유틸
# =====================================================
def _secret(key, default=""):
    try:
        v = st.secrets.get(key, default) or default
        return str(v).strip()
    except Exception:
        return default

def _norm_sheet_id(val):
    return str(val).strip() if val else ""

# =====================================================
# 스프레드시트 ID 설정
# =====================================================
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

GOOGLE_SPREADSHEET_IDS = {
    k: _norm_sheet_id(_secret(s))
    for k, s in _SPREADSHEET_SECRET_KEYS
}

# 브랜드 매핑
BRAND_MAP = {
    "스파오": "spao",
    "후아유": "whoau",
    "클라비스": "clavis",
    "미쏘": "mixxo",
    "로엠": "roem",
    "슈펜": "shoopen",
    "에블린": "eblin",
}

# =====================================================
# Google 인증
# =====================================================
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

def _get_google_credentials():
    import json
    try:
        raw = st.secrets.get("google_service_account")
        if raw:
            info = json.loads(raw) if isinstance(raw, str) else dict(raw)
            return Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPES)
    except Exception:
        return None
    return None

# =====================================================
# Google 시트 → xlsx 다운로드
# =====================================================
@st.cache_data(ttl=300)
def fetch_sheet_bytes(sheet_id):
    creds = _get_google_credentials()
    if not creds or not sheet_id:
        return None

    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload

    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    request = service.files().export_media(
        fileId=sheet_id,
        mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    return fh.read()

# =====================================================
# BASE 입출고 로드
# =====================================================
@st.cache_data(ttl=300)
def load_base_inout():
    sheet_id = GOOGLE_SPREADSHEET_IDS.get("inout")
    raw = fetch_sheet_bytes(sheet_id)
    if not raw:
        return pd.DataFrame()

    df = pd.read_excel(BytesIO(raw))
    df.columns = [str(c).strip() for c in df.columns]
    return df

# =====================================================
# 브랜드 등록 시트 로드
# =====================================================
@st.cache_data(ttl=300)
def load_brand_register(brand_key):
    sheet_id = GOOGLE_SPREADSHEET_IDS.get(brand_key)
    raw = fetch_sheet_bytes(sheet_id)
    if not raw:
        return pd.DataFrame()

    df = pd.read_excel(BytesIO(raw))
    df.columns = [str(c).strip() for c in df.columns]

    if "스타일코드" not in df.columns:
        return pd.DataFrame()

    df["스타일코드"] = df["스타일코드"].astype(str).str.strip()
    df["공홈등록여부"] = df.get("공홈등록여부", "").astype(str).str.strip()
    df["시즌"] = df.get("시즌", "").astype(str).str.strip()

    return df[["스타일코드", "시즌", "공홈등록여부"]]

# =====================================================
# 입고/출고 집계
# =====================================================
def build_inout_status(df, brand_name):
    if df.empty:
        return pd.DataFrame()

    df = df[df["브랜드"].astype(str).str.contains(brand_name, na=False)].copy()

    df["스타일코드"] = df["스타일코드"].astype(str).str.strip()

    df["입고여부"] = pd.to_datetime(df["최초입고일"], errors="coerce").notna()

    if "출고액" in df.columns:
        df["출고여부"] = pd.to_numeric(df["출고액"], errors="coerce").fillna(0) > 0
    else:
        df["출고여부"] = False

    agg = df.groupby("스타일코드").agg(
        입고여부=("입고여부", "any"),
        출고여부=("출고여부", "any"),
    ).reset_index()

    return agg

# =====================================================
# 병합 테이블 생성
# =====================================================
def build_monitor_table(brand_name, brand_key):
    df_base = load_base_inout()
    df_reg = load_brand_register(brand_key)
    df_io = build_inout_status(df_base, brand_name)

    merged = df_reg.merge(df_io, on="스타일코드", how="left")

    merged["입고 여부"] = merged["입고여부"].map({True: "Y", False: "N"})
    merged["출고 여부"] = merged["출고여부"].map({True: "Y", False: "N"})
    merged["온라인상품등록여부"] = merged["공홈등록여부"].apply(
        lambda x: "등록" if str(x).upper() == "등록" else "미등록"
    )

    return merged[[
        "스타일코드",
        "시즌",
        "입고 여부",
        "출고 여부",
        "온라인상품등록여부"
    ]]

# =====================================================
# UI
# =====================================================
selected_brand = st.sidebar.selectbox("브랜드 선택", list(BRAND_MAP.keys()))
brand_key = BRAND_MAP[selected_brand]

df = build_monitor_table(selected_brand, brand_key)

if df.empty:
    st.warning("데이터가 없습니다.")
    st.stop()

season_filter = st.sidebar.selectbox(
    "시즌 필터",
    ["전체"] + sorted(df["시즌"].unique().tolist())
)

if season_filter != "전체":
    df = df[df["시즌"] == season_filter]

st.sidebar.metric("총 스타일 수", len(df))
st.sidebar.metric("입고 완료", (df["입고 여부"] == "Y").sum())
st.sidebar.metric("출고 완료", (df["출고 여부"] == "Y").sum())
st.sidebar.metric("온라인 등록 완료", (df["온라인상품등록여부"] == "등록").sum())

st.dataframe(df, use_container_width=True, hide_index=True)

csv = df.to_csv(index=False, encoding="utf-8-sig")
st.download_button(
    "CSV 다운로드",
    csv,
    file_name=f"{selected_brand}_스타일현황_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
    mime="text/csv"
)
