# -*- coding: utf-8 -*-
from __future__ import annotations
"""온라인 리드타임 대시보드 (Streamlit)"""

import streamlit as st
import pandas as pd
import html as html_lib
import os
from datetime import datetime
from io import BytesIO

from google.oauth2.service_account import Credentials

# =====================================================
# (A) Streamlit 기본 설정
# =====================================================
st.set_page_config(
    page_title="온라인 리드타임 대시보드",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =====================================================
# (A) Secrets / Spreadsheet IDs
# - Streamlit Cloud의 Secrets에 들어있는 값들을 안전하게 읽습니다.
# =====================================================
def _secret(key, default=""):
    try:
        v = st.secrets.get(key, default) or default
        return str(v).strip() if v else default
    except Exception:
        return default

def _norm_sheet_id(val):
    """시트 ID를 문자열로 정규화. 비어있으면 빈 문자열."""
    if val is None:
        return ""
    s = str(val).strip()
    return s if s else ""

BASE_SPREADSHEET_ID = _norm_sheet_id(_secret("BASE_SPREADSHEET_ID"))
SP_SPREADSHEET_ID   = _norm_sheet_id(_secret("SP_SPREADSHEET_ID"))
WH_SPREADSHEET_ID   = _norm_sheet_id(_secret("WH_SPREADSHEET_ID"))
CV_SPREADSHEET_ID   = _norm_sheet_id(_secret("CV_SPREADSHEET_ID"))
MI_SPREADSHEET_ID   = _norm_sheet_id(_secret("MI_SPREADSHEET_ID"))
RM_SPREADSHEET_ID   = _norm_sheet_id(_secret("RM_SPREADSHEET_ID"))
HP_SPREADSHEET_ID   = _norm_sheet_id(_secret("HP_SPREADSHEET_ID"))
EB_SPREADSHEET_ID   = _norm_sheet_id(_secret("EB_SPREADSHEET_ID"))


GOOGLE_SPREADSHEET_IDS = {
    "inout": BASE_SPREADSHEET_ID,
    "spao": SP_SPREADSHEET_ID,
    "whoau": WH_SPREADSHEET_ID,
    "clavis": CV_SPREADSHEET_ID,
    "mixxo": MI_SPREADSHEET_ID,
    "roem": RM_SPREADSHEET_ID,
    "shoopen": HP_SPREADSHEET_ID,
    "eblin": EB_SPREADSHEET_ID,
}


# =====================================================
# (A) 데이터 소스 키 (Google Sheets만 사용, 로컬 xlsx 미사용)
# =====================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_KEYS = ("inout", "spao", "whoau", "clavis", "mixxo", "roem", "shoopen", "eblin")

# =====================================================
# (B) Google 인증/다운로드 관련
# - Drive API(export) 우선 → 실패 시 Sheets API로 대체 다운로드
# =====================================================
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _get_google_credentials():
    """서비스 계정 Credentials 반환. Streamlit Secrets 우선, 없으면 로컬 JSON 파일."""
    import json
    # 1) Streamlit Secrets: [google_service_account] 섹션
    try:
        raw = None
        if hasattr(st.secrets, "get"):
            raw = st.secrets.get("google_service_account")
        if not raw:
            raw = _secret("google_service_account")
        if raw:
            if isinstance(raw, str):
                info = json.loads(raw)
            else:
                info = dict(raw)
            if "type" in info and "private_key" in info:
                return Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPES)
    except Exception:
        pass
    # 2) 로컬: 환경변수 또는 프로젝트 내 JSON 파일
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
    """Sheets API로 시트 데이터 읽어서 openpyxl로 xlsx 바이트 생성. Drive API 403 시 대안."""
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
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
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
    """Google 시트를 xlsx 바이트로. Drive API 시도 후 실패 시 Sheets API로 읽어서 xlsx 생성."""
    sid = (str(spreadsheet_id).strip() if spreadsheet_id else "") or ""
    if not sid or not _creds_ok:
        return None
    creds = _get_google_credentials()
    if not creds:
        return None
    # 1) Drive API 내보내기 시도
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
    # 2) 403 등 실패 시 Sheets API로 읽어서 xlsx 생성 (편집자 공유만 있으면 동작)
    return _fetch_google_sheet_via_sheets_api(sid, creds)


def _diagnose_google_connection():
    """연결 실패 시 원인 진단. (인증_성공여부, 메시지) 반환. 비밀값 노출 없음."""
    creds = _get_google_credentials()
    if not creds:
        return False, "인증 실패: Secrets의 [google_service_account]를 확인하세요. type, private_key, client_email 등이 있고, private_key는 -----BEGIN PRIVATE KEY----- 로 시작해야 합니다."
    sid = GOOGLE_SPREADSHEET_IDS.get("inout") or BASE_SPREADSHEET_ID
    if not sid:
        return False, "BASE_SPREADSHEET_ID가 비어 있습니다. Secrets에 값을 넣으세요."
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
        return True, "인증 성공 + 시트 다운로드 성공. (그래도 데이터 없음이면 Reboot app 후 새로고침 해보세요.)"
    except Exception as e:
        err = (str(e) or "").strip()[:300]
        if "403" in err or "Forbidden" in err or "permission" in err.lower() or "Permission" in err:
            try:
                email = getattr(creds, "service_account_email", None)
                if email:
                    return False, f"Drive 403: 이 이메일을 각 시트 [편집자]로 추가했는지 확인 → {email} (앱은 Drive 실패 시 Sheets API로 자동 재시도합니다.)"
            except Exception:
                pass
            return False, "권한 거부(403): 시트를 서비스 계정 이메일과 [편집자]로 공유했는지 확인하세요. 앱은 Drive 실패 시 Sheets API로 자동 재시도합니다."
        if "404" in err or "not found" in err.lower():
            return False, "파일 없음(404): BASE_SPREADSHEET_ID가 올바른지, 해당 시트가 삭제되지 않았는지 확인하세요."
        if "enabled" in err.lower() or "has not been used" in err.lower():
            return False, "Drive API 비활성: Google Cloud 콘솔 → API 및 서비스 → 사용자 인증 정보 → Drive API 사용 설정."
        if "invalid" in err.lower() and "key" in err.lower():
            return False, "private_key 형식 오류: 줄바꿈은 \\n 그대로 두고, BEGIN/END 줄과 키 내용이 빠짐없이 있는지 확인하세요."
        return False, f"API 오류: {err}"


def _missing_sheet_ids():
    """시트 ID가 비어 있는 브랜드 키 목록. (Secrets에 추가해야 할 항목 안내용)"""
    key_labels = {"eblin": "에블린(EB_SPREADSHEET_ID)", "inout": "BASE", "spao": "스파오", "whoau": "후아유", "clavis": "클라비스", "mixxo": "미쏘", "roem": "로엠", "shoopen": "슈펜"}
    missing = []
    for key in EXCEL_KEYS:
        sid = GOOGLE_SPREADSHEET_IDS.get(key)
        if not sid or not str(sid).strip():
            missing.append(key_labels.get(key, key))
    return missing


# 실행 시각 고정(새로고침 전까지 동일 값 유지)
update_time = datetime.now()


@st.cache_data(ttl=300)
def get_excel_sources():
    """
    Google Sheets에서만 데이터 소스 확보. 반환: dict key -> (bytes 또는 None, cache_key 문자열)
    """
    creds_ok = _get_google_credentials() is not None
    sources = {}
    for key in EXCEL_KEYS:
        sheet_id = GOOGLE_SPREADSHEET_IDS.get(key)
        sheet_id = str(sheet_id).strip() if sheet_id else ""
        if sheet_id:
            raw = _fetch_google_sheet_as_xlsx_bytes(sheet_id, _creds_ok=creds_ok)
            if raw:
                sources[key] = (raw, f"gs:{sheet_id}")
            else:
                sources[key] = (None, "none")
        else:
            sources[key] = (None, "none")
    return sources


def find_col(keys, df=None):
    """
    (C) 엑셀/시트 컬럼 자동 탐지 유틸.

    - **정확 일치**를 우선 사용
    - 없으면 **부분 포함 매칭**으로 보조 탐색
    """
    if df is None or df.empty:
        return None
    cols = list(df.columns)
    # 키와 컬럼명이 정확히 일치하는 항목을 우선 사용
    for k in keys:
        for c in cols:
            if str(c).strip() == k:
                return c
    # 그 외에는 포함 매칭
    for k in keys:
        for c in cols:
            if k in str(c):
                return c
    return None

def safe_sum(df, col_name):
    """(D) 집계 유틸: 컬럼이 없거나 숫자 변환이 실패하면 0으로 합산."""
    if df is None or col_name is None or col_name not in df.columns:
        return 0
    return pd.to_numeric(df[col_name], errors="coerce").sum()


@st.cache_data
def _base_style_to_first_in_map(inout_bytes=None, _cache_key=None):
    """BASE 시트에서 스타일코드별 최초입고일(min) 맵. 반환: dict normalized_style -> datetime."""
    df = load_inout_data(inout_bytes, _cache_key=_cache_key)
    if df is None or df.empty:
        return {}
    style_col = find_col(["스타일코드", "스타일"], df=df)
    first_col = find_col(["최초입고일", "첫 입고일", "입고일"], df=df)
    if style_col is None or first_col is None:
        return {}
    df = df.copy()
    df["_style"] = df[style_col].astype(str).str.strip().str.replace(" ", "", regex=False)
    numeric = pd.to_numeric(df[first_col], errors="coerce")
    excel_mask = numeric.between(1, 60000, inclusive="both")
    df["_first_in"] = pd.to_datetime(df[first_col], errors="coerce")
    if excel_mask.any():
        df.loc[excel_mask, "_first_in"] = pd.to_datetime(
            numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
        )
    df = df[df["_first_in"].notna() & (df["_style"].str.len() > 0)]
    if df.empty:
        return {}
    return df.groupby("_style")["_first_in"].min().to_dict()


@st.cache_data
def load_inout_data(io_bytes=None, _cache_key=None):
    """
    (C) 입출고 DB 로더.

    역할:
    - 엑셀(xlsx bytes)을 DataFrame으로 로드
    - 헤더 행을 자동 추정(상단 20행 스캔)
    - 스타일코드 접두어(2글자)로 브랜드를 **강제 추론**하여 `브랜드` 컬럼을 표준화

    인자:
    - `io_bytes`: Google/로컬에서 읽은 xlsx 바이트. None이면 로컬 파일로 fallback 시도
    - `_cache_key`: Streamlit cache 키(소스가 바뀌면 변경되도록)
    """
    if _cache_key is None:
        _cache_key = "default"
    if io_bytes is None or len(io_bytes) == 0:
        return pd.DataFrame()
    io_obj = BytesIO(io_bytes)
    excel_file = pd.ExcelFile(io_obj)
    sheet_candidates = [s for s in excel_file.sheet_names if not str(s).startswith("_")]
    sheet_name = sheet_candidates[0] if sheet_candidates else excel_file.sheet_names[-1]

    # 상단 20행 중 헤더 키워드가 가장 많은 행을 헤더로 추정
    preview = pd.read_excel(BytesIO(io_bytes), sheet_name=sheet_name, header=None)
    header_keywords = ["브랜드", "스타일", "칼라", "컬러", "최초입고일", "입고", "출고", "판매"]
    best_row = None
    best_score = 0
    for i in range(min(20, len(preview))):
        row = preview.iloc[i].astype(str)
        score = sum(any(k in cell for k in header_keywords) for cell in row)
        if score > best_score:
            best_score = score
            best_row = i
    if best_row is not None and best_score > 0:
        df = pd.read_excel(BytesIO(io_bytes), sheet_name=sheet_name, header=best_row)
    else:
        df = pd.read_excel(BytesIO(io_bytes), sheet_name=sheet_name)
    df.columns = [str(c).strip() for c in df.columns]
    # 컬럼명 유사도 기반 매핑(누락 시 기본 인덱스 fallback)
    year_col = find_col(["년도", "연도", "년", "year", "Year"], df=df)
    season_col = find_col(["시즌", "season", "Season"], df=df)
    style_col = find_col(["스타일", "스타일코드", "style", "style code", "style_code"], df=df)
    brand_col = find_col(["브랜드", "brand"], df=df)
    if year_col is None and len(df.columns) > 0:
        year_col = df.columns[0]
    if season_col is None and len(df.columns) > 1:
        season_col = df.columns[1]
    for col_name in (year_col, season_col):
        if col_name and col_name in df.columns:
            df[col_name] = df[col_name].replace(r"^\s*$", pd.NA, regex=True).ffill()
    # 스타일 컬럼이 없으면 패턴(영문 2글자 시작)을 가장 많이 가진 컬럼 추정
    if style_col is None:
        best_style_col = None
        best_score = 0
        for c in df.columns:
            series = df[c].astype(str).str.strip()
            if series.empty:
                continue
            score = series.str.match(r"^[A-Za-z]{2}", na=False).sum()
            if score > best_score:
                best_score = score
                best_style_col = c
        style_col = best_style_col if best_score > 0 else None

    # 스타일코드 접두어로 브랜드 강제 추론(기존 브랜드 컬럼을 덮어씀)
    if style_col and style_col in df.columns:
        style_series = df[style_col].astype(str).str.strip().str.lower()
        prefix = style_series.str.slice(0, 2)
        brand_map = {
            "sp": "스파오",
            "rm": "로엠",
            "mi": "미쏘",
            "wh": "후아유",
            "nb": "뉴발란스",
            "eb": "에블린",
            "hp": "슈펜",
            "cv": "클라비스",
            "nk": "뉴발란스키즈"
        }
        inferred_brand = prefix.map(brand_map)
        df["브랜드"] = inferred_brand
        if brand_col and brand_col in df.columns and brand_col != "브랜드":
            df[brand_col] = inferred_brand
        # 브랜드 계산은 항상 '브랜드' 컬럼 기준으로 하도록 보장
        brand_col = "브랜드"
    return df


# =====================================================
# (C) 데이터 소스 확보 → 로드(전역 1회)
# - `_sources`: key -> (xlsx_bytes 또는 None, cache_key)
# - 주의: xlsx bytes를 메모리에 들고 있으므로(특히 입출고 DB) 과도한 메모리 사용 시
#   Cloud 환경에서는 OOM/Server Error가 날 수 있습니다. (추후 "필요할 때만 다운로드" 구조로 개선 가능)
# =====================================================
_sources = get_excel_sources()
_inout_src = _sources.get("inout", (None, None))
df_inout = load_inout_data(_inout_src[0], _cache_key=_inout_src[1])

@st.cache_data
def load_spao_register_days(io_bytes=None, _cache_key=None):
    """
    (C) [스파오] 트래킹판 로더: '공홈등록소요일' 평균 산출.

    반환:
    - (평균, 표본 수, 헤더셀 위치) 또는 None
    """
    if _cache_key is None:
        _cache_key = "spao_reg_default"
    if io_bytes is None or len(io_bytes) == 0:
        return None
    io_obj = BytesIO(io_bytes)
    try:
        excel_file = pd.ExcelFile(io_obj)
    except Exception:
        return None
    sheet_name = excel_file.sheet_names[0]
    try:
        df_raw_sheet = pd.read_excel(BytesIO(io_bytes), sheet_name=sheet_name, header=None)
    except Exception:
        return None
    if df_raw_sheet.empty:
        return None
    def normalize_header_text(value):
        return "".join(str(value).split())

    # 띄어쓰기 제거 후 키워드 포함 여부로 헤더 위치 탐색
    header_mask = df_raw_sheet.astype(str).applymap(
        lambda v: "공홈등록소요일" in normalize_header_text(v)
    )
    if not header_mask.any().any():
        return None
    header_pos = header_mask.stack().idxmax()
    header_row_idx, col_idx = int(header_pos[0]), int(header_pos[1])
    # 헤더 아래 열 값을 숫자로 변환, 0~99 범위만 유효값으로 사용
    values = pd.to_numeric(
        df_raw_sheet.iloc[header_row_idx + 1 :, col_idx], errors="coerce"
    ).dropna()
    values = values[(values >= 0) & (values < 100)]
    if values.empty:
        return None
    mean_val = values.mean()
    def col_letter(n):
        n += 1
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s
    header_cell = f"{col_letter(col_idx)}{header_row_idx + 1}"
    return float(mean_val), int(values.count()), header_cell


@st.cache_data
def load_spao_photo_days(io_bytes=None, _cache_key=None):
    # 스파오 트래킹판에서 "포토소요일" 컬럼을 찾아 평균 산출
    if _cache_key is None:
        _cache_key = "spao_photo_default"
    if io_bytes is None or len(io_bytes) == 0:
        return None
    try:
        excel_file = pd.ExcelFile(BytesIO(io_bytes))
    except Exception:
        return None
    sheet_name = excel_file.sheet_names[0]
    try:
        df_raw_sheet = pd.read_excel(BytesIO(io_bytes), sheet_name=sheet_name, header=None)
    except Exception:
        return None
    if df_raw_sheet.empty:
        return None

    def normalize_header_text(value):
        return "".join(str(value).split())

    # 띄어쓰기 제거 후 키워드 포함 여부로 헤더 위치 탐색
    header_mask = df_raw_sheet.astype(str).applymap(
        lambda v: "포토소요일" in normalize_header_text(v)
    )
    if not header_mask.any().any():
        return None
    header_pos = header_mask.stack().idxmax()
    header_row_idx, col_idx = int(header_pos[0]), int(header_pos[1])
    # 헤더 아래 열 값을 숫자로 변환, 0~99 범위만 유효값으로 사용
    values = pd.to_numeric(
        df_raw_sheet.iloc[header_row_idx + 1 :, col_idx], errors="coerce"
    ).dropna()
    values = values[(values >= 0) & (values < 100)]
    if values.empty:
        return None
    mean_val = values.mean()

    def col_letter(n):
        n += 1
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    header_cell = f"{col_letter(col_idx)}{header_row_idx + 1}"
    return float(mean_val), int(values.count()), header_cell

@st.cache_data
def load_spao_registered_style_count(io_bytes=None, _cache_key=None):
    # 스파오 트래킹판에서 스타일코드 + 공홈등록일 모두 있는 행 카운트
    if _cache_key is None:
        _cache_key = "spao_style_count_default"
    if io_bytes is None or len(io_bytes) == 0:
        return 0
    try:
        from openpyxl import load_workbook
    except Exception:
        return 0
    try:
        wb = load_workbook(BytesIO(io_bytes), read_only=True, data_only=True)
    except Exception:
        return 0
    ws = wb.active

    def normalize(value):
        return "".join(str(value).split()) if value is not None else ""

    header_row_idx = None
    header_vals = None
    # 헤더 행 탐색(상단 30행에서 스타일코드+공홈등록일 포함 여부)
    for i, row in enumerate(ws.iter_rows(min_row=1, max_row=30, values_only=True), start=1):
        norm = [normalize(v) for v in row]
        if any("스타일코드" in v for v in norm) and any("공홈등록일" in v for v in norm):
            header_row_idx = i
            header_vals = norm
            break
    if header_row_idx is None:
        return 0

    def find_col(key):
        for idx, v in enumerate(header_vals):
            if v == key:
                return idx
        for idx, v in enumerate(header_vals):
            if key in v:
                return idx
        return None

    style_col = find_col("스타일코드")
    if style_col is None:
        style_col = find_col("스타일")
    register_col = find_col("공홈등록일")
    if register_col is None:
        register_col = find_col("등록일")
    if style_col is None or register_col is None:
        return 0

    count = 0
    # 등록일/스타일코드가 모두 있는 행만 등록 스타일 수로 카운트
    for row in ws.iter_rows(min_row=header_row_idx + 1, values_only=True):
        style_val = row[style_col] if style_col < len(row) else None
        reg_val = row[register_col] if register_col < len(row) else None
        style_ok = style_val is not None and str(style_val).strip() != ""
        reg_ok = reg_val is not None and str(reg_val).strip() != ""
        if reg_ok and style_ok:
            count += 1
    return int(count)

@st.cache_data
def load_spao_register_avg_days(io_bytes=None, _cache_key=None, inout_bytes=None, _inout_cache_key=None):
    # 스파오 등록 평균 소요일: 공홈등록일(브랜드 시트) - 최초입고일(BASE 시트)
    if _cache_key is None:
        _cache_key = "spao_avg_days_default"
    if io_bytes is None or len(io_bytes) == 0:
        return None
    base_map = _base_style_to_first_in_map(inout_bytes, _inout_cache_key) if (inout_bytes is not None or _inout_cache_key is not None) else {}
    if not base_map:
        return None
    try:
        from openpyxl import load_workbook
    except Exception:
        return None
    try:
        wb = load_workbook(BytesIO(io_bytes), read_only=True, data_only=True)
    except Exception:
        return None
    ws = wb.active

    def normalize(value):
        return "".join(str(value).split()) if value is not None else ""

    header_row_idx = None
    header_vals = None
    for i, row in enumerate(ws.iter_rows(min_row=1, max_row=30, values_only=True), start=1):
        norm = [normalize(v) for v in row]
        if any("스타일코드" in v for v in norm) and any("공홈등록일" in v for v in norm):
            header_row_idx = i
            header_vals = norm
            break
    if header_row_idx is None:
        return None

    def find_col(key):
        for idx, v in enumerate(header_vals):
            if v == key:
                return idx
        for idx, v in enumerate(header_vals):
            if key in v:
                return idx
        return None

    style_col = find_col("스타일코드") or find_col("스타일")
    register_col = find_col("공홈등록일") or find_col("등록일")
    if style_col is None or register_col is None:
        return None

    diffs = []
    for row in ws.iter_rows(min_row=header_row_idx + 1, values_only=True):
        style_val = row[style_col] if style_col < len(row) else None
        reg_val = row[register_col] if register_col < len(row) else None
        style_norm = normalize(style_val) if style_val is not None else ""
        if not style_norm or not (reg_val is not None and str(reg_val).strip() not in ("", "0")):
            continue
        reg_dt = pd.to_datetime(reg_val, errors="coerce")
        base_dt = base_map.get(style_norm)
        if pd.isna(reg_dt) or base_dt is None:
            continue
        diff = (reg_dt - base_dt).days
        diffs.append(diff)
    if not diffs:
        return None
    return float(sum(diffs)) / len(diffs)

# =====================================================
# (C) 브랜드별 트래킹 지표 로드(전역 1회)
# - 아래는 각 브랜드 엑셀에서 필요한 지표를 "로더 함수 호출"로 뽑아내는 구간입니다.
# - 값들은 이후 UI 섹션에서 테이블/지표 표시에 사용됩니다.
# =====================================================
_spao_src = _sources.get("spao", (None, None))
spao_register_result = load_spao_register_days(_spao_src[0], _cache_key=_spao_src[1])
spao_register_days = spao_register_result[0] if spao_register_result else None
spao_register_count = spao_register_result[1] if spao_register_result else 0
spao_register_header_cell = spao_register_result[2] if spao_register_result else None
spao_photo_result = load_spao_photo_days(_spao_src[0], _cache_key=_spao_src[1])
spao_photo_days = spao_photo_result[0] if spao_photo_result else None
spao_photo_count = spao_photo_result[1] if spao_photo_result else 0
spao_photo_header_cell = spao_photo_result[2] if spao_photo_result else None
spao_register_style_count = load_spao_registered_style_count(_spao_src[0], _cache_key=_spao_src[1])
spao_register_avg_days = load_spao_register_avg_days(
    _spao_src[0], _cache_key=_spao_src[1],
    inout_bytes=_inout_src[0], _inout_cache_key=_inout_src[1],
)

# =====================================================
# (C) [후아유] 스타일판 로더/지표 추출
# =====================================================
def _whoau_bytes(io_bytes):
    """(C) [후아유] 로더: Google에서 받은 bytes만 사용."""
    return io_bytes if (io_bytes is not None and len(io_bytes) > 0) else None

@st.cache_data
def load_whoau_metric_days(target_keywords, io_bytes=None, _cache_key=None):
    # 후아유 트래킹판에서 특정 소요일 컬럼 평균 산출
    if _cache_key is None:
        _cache_key = "whoau_metric_default"
    whoau_bytes = _whoau_bytes(io_bytes)
    if whoau_bytes is None:
        return None
    try:
        excel_file = pd.ExcelFile(BytesIO(whoau_bytes))
    except Exception:
        return None
    sheet_name = excel_file.sheet_names[0]
    try:
        df_raw_sheet = pd.read_excel(BytesIO(whoau_bytes), sheet_name=sheet_name, header=None)
    except Exception:
        return None
    if df_raw_sheet.empty:
        return None

    def normalize_header_text(value):
        return "".join(str(value).split())

    normalized_keywords = ["".join(str(k).split()) for k in target_keywords]
    header_mask = df_raw_sheet.astype(str).applymap(
        lambda v: any(k in normalize_header_text(v) for k in normalized_keywords)
    )
    if not header_mask.any().any():
        return None
    header_pos = header_mask.stack().idxmax()
    header_row_idx, col_idx = int(header_pos[0]), int(header_pos[1])
    values = pd.to_numeric(
        df_raw_sheet.iloc[header_row_idx + 1 :, col_idx], errors="coerce"
    ).dropna()
    values = values[(values >= 0) & (values < 100)]
    if values.empty:
        return None
    mean_val = values.mean()

    def col_letter(n):
        n += 1
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    header_cell = f"{col_letter(col_idx)}{header_row_idx + 1}"
    return float(mean_val), int(values.count()), header_cell

# =====================================================
# (C) [클라비스/미쏘/로엠] 스타일판 로더/지표 추출 (공통 패턴)
# - bytes 확보 helper → metric_days/등록/미등록 등 지표 로더로 이어집니다.
# =====================================================
def _clavis_bytes(io_bytes):
    return io_bytes if (io_bytes is not None and len(io_bytes) > 0) else None

def _mixxo_bytes(io_bytes):
    return io_bytes if (io_bytes is not None and len(io_bytes) > 0) else None

def _roem_bytes(io_bytes):
    return io_bytes if (io_bytes is not None and len(io_bytes) > 0) else None

def _hp_bytes(io_bytes):
    return io_bytes if (io_bytes is not None and len(io_bytes) > 0) else None

@st.cache_data
def load_clavis_metric_days(target_keywords, io_bytes=None, _cache_key=None):
    # 클라비스 트래킹판에서 특정 소요일 컬럼 평균 산출
    if _cache_key is None:
        _cache_key = "clavis_metric_default"
    clavis_bytes = _clavis_bytes(io_bytes)
    if clavis_bytes is None:
        return None
    try:
        excel_file = pd.ExcelFile(BytesIO(clavis_bytes))
    except Exception:
        return None
    best_sheet = None
    best_df = None
    best_hits = -1
    best_header = None

    def normalize_header_text(value):
        return "".join(str(value).split())

    normalized_keywords = ["".join(str(k).split()) for k in target_keywords]
    for sheet_name in excel_file.sheet_names:
        try:
            df_raw_sheet = pd.read_excel(BytesIO(clavis_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if df_raw_sheet.empty:
            continue
        header_mask = df_raw_sheet.astype(str).applymap(
            lambda v: any(k in normalize_header_text(v) for k in normalized_keywords)
        )
        if not header_mask.any().any():
            continue
        hits = int(header_mask.sum().sum())
        if hits > best_hits:
            header_pos = header_mask.stack().idxmax()
            best_header = (int(header_pos[0]), int(header_pos[1]))
            best_hits = hits
            best_sheet = sheet_name
            best_df = df_raw_sheet
    if best_df is None or best_header is None:
        return None
    header_row_idx, col_idx = best_header
    values = pd.to_numeric(
        best_df.iloc[header_row_idx + 1 :, col_idx], errors="coerce"
    ).dropna()
    values = values[(values >= 0) & (values < 100)]
    if values.empty:
        return None
    mean_val = values.mean()

    def col_letter(n):
        n += 1
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    header_cell = f"{col_letter(col_idx)}{header_row_idx + 1}"
    return float(mean_val), int(values.count()), header_cell

@st.cache_data
def load_whoau_registered_style_count(io_bytes=None, _cache_key=None):
    # 후아유 트래킹판에서 스타일코드 + 공홈등록일 모두 있는 행 카운트
    if _cache_key is None:
        _cache_key = "whoau_style_count_default"
    whoau_bytes = _whoau_bytes(io_bytes)
    if whoau_bytes is None:
        return 0
    try:
        from openpyxl import load_workbook
    except Exception:
        return 0
    try:
        wb = load_workbook(BytesIO(whoau_bytes), read_only=True, data_only=True)
    except Exception:
        return 0
    ws = wb.active

    def normalize(value):
        return "".join(str(value).split()) if value is not None else ""

    header_row_idx = None
    header_vals = None
    for i, row in enumerate(ws.iter_rows(min_row=1, max_row=30, values_only=True), start=1):
        norm = [normalize(v) for v in row]
        if any("스타일코드" in v for v in norm) and any("공홈등록일" in v for v in norm):
            header_row_idx = i
            header_vals = norm
            break
    if header_row_idx is None:
        return 0

    def find_col(key):
        for idx, v in enumerate(header_vals):
            if v == key:
                return idx
        for idx, v in enumerate(header_vals):
            if key in v:
                return idx
        return None

    style_col = find_col("스타일코드") or find_col("스타일")
    register_col = find_col("공홈등록일") or find_col("등록일")
    if style_col is None or register_col is None:
        return 0

    style_set = set()
    for row in ws.iter_rows(min_row=header_row_idx + 1, values_only=True):
        style_val = row[style_col] if style_col < len(row) else None
        reg_val = row[register_col] if register_col < len(row) else None
        style_text = "" if style_val is None else str(style_val).strip()
        style_ok = style_text != ""
        reg_ok = reg_val is not None and str(reg_val).strip() != ""
        if reg_ok and style_ok:
            style_set.add(style_text)
    return int(len(style_set))

@st.cache_data
def load_whoau_register_avg_days(io_bytes=None, _cache_key=None, inout_bytes=None, _inout_cache_key=None):
    # 후아유 등록 평균 소요일: 공홈등록일(브랜드 시트) - 최초입고일(BASE 시트)
    if _cache_key is None:
        _cache_key = "whoau_avg_days_default"
    whoau_bytes = _whoau_bytes(io_bytes)
    if whoau_bytes is None:
        return None
    base_map = _base_style_to_first_in_map(inout_bytes, _inout_cache_key) if (inout_bytes is not None or _inout_cache_key is not None) else {}
    if not base_map:
        return None
    def normalize_header_text(value):
        return "".join(str(value).split())

    register_date_keywords = ["공홈등록일", "등록일"]
    style_keywords = ["스타일코드", "스타일"]

    def scan_col(preview, keys):
        norm_keys = ["".join(k.split()) for k in keys]
        best_col = None
        best_hits = 0
        best_header_row = None
        max_rows = min(200, len(preview))
        for col_idx in range(preview.shape[1]):
            col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
            hits = 0
            header_row = None
            for row_idx, v in enumerate(col_vals):
                if any(k in v for k in norm_keys):
                    hits += 1
                    if header_row is None:
                        header_row = row_idx
            if hits > best_hits:
                best_hits = hits
                best_col = col_idx
                best_header_row = header_row
        return best_col, best_header_row, best_hits

    best_sheet = None
    best_score = -1
    best_cols = None
    best_start_row = None

    try:
        excel_file = pd.ExcelFile(BytesIO(whoau_bytes))
    except Exception:
        return None

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(whoau_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue
        register_col, register_row, register_hits = scan_col(preview, register_date_keywords)
        style_col, style_row, style_hits = scan_col(preview, style_keywords)
        if register_col is None or style_col is None:
            continue
        score = register_hits + style_hits
        start_row = max(
            [r for r in [register_row, style_row] if r is not None],
            default=0,
        ) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (register_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return None

    try:
        df_raw = pd.read_excel(BytesIO(whoau_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return None
    if df_raw is None or df_raw.empty:
        return None

    register_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    register_series = data.iloc[:, register_col]
    style_series = data.iloc[:, style_col]

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    def norm_style(val):
        return "".join(str(val).split()) if val is not None else ""

    style_ok = style_series.astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True).notna()
    register_ok = clean_date_series(register_series).notna()
    valid = style_ok & register_ok

    reg_dt = clean_date_series(register_series)

    diffs = []
    for idx in data.index:
        if not bool(valid.loc[idx]):
            continue
        style_norm = norm_style(style_series.loc[idx])
        base_dt = base_map.get(style_norm) if style_norm else None
        if base_dt is None or pd.isna(reg_dt.loc[idx]):
            continue
        diffs.append((reg_dt.loc[idx] - base_dt).days)
    if not diffs:
        return None
    return float(sum(diffs)) / len(diffs)

@st.cache_data
def load_whoau_unregistered_online_count(io_bytes=None, _cache_key=None):
    # 후아유: 상품등록일 비어있고 리터칭완료일이 2020년 이후인 행 수
    if _cache_key is None:
        _cache_key = "whoau_unreg_default"
    whoau_bytes = _whoau_bytes(io_bytes)
    if whoau_bytes is None:
        return 0
    try:
        excel_file = pd.ExcelFile(BytesIO(whoau_bytes))
    except Exception:
        return 0

    def normalize_header_text(value):
        return "".join(str(value).split())

    register_keywords = [
        "상품등록일",
        "공홈등록일",
        "공홈 등록일",
        "등록일",
        "온라인등록일",
        "온라인 등록일",
        "온라인상품등록일",
        "온라인 상품등록일",
    ]
    retouch_keywords = [
        "리터칭완료일",
        "리터칭 완료일",
        "리터칭완료",
        "리터칭완료일자",
        "리터칭 완료일자",
        "리터칭완료날짜",
        "리터칭 완료날짜",
        "리터칭완료일(포토팀)",
        "리터칭완료일(촬영팀)",
        "리터칭완료일(최종)",
    ]
    register_norm = ["".join(k.split()) for k in register_keywords]
    retouch_norm = ["".join(k.split()) for k in retouch_keywords]

    best_sheet = None
    best_score = -1
    best_cols = None
    best_start_row = None

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(whoau_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue

        max_rows = min(200, len(preview))

        def scan_col(keys):
            norm_keys = ["".join(k.split()) for k in keys]
            best_col = None
            best_hits = 0
            best_header_row = None
            for col_idx in range(preview.shape[1]):
                col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
                hits = 0
                header_row = None
                for row_idx, v in enumerate(col_vals):
                    if any(k in v for k in norm_keys):
                        hits += 1
                        if header_row is None:
                            header_row = row_idx
                if hits > best_hits:
                    best_hits = hits
                    best_col = col_idx
                    best_header_row = header_row
            return best_col, best_header_row, best_hits

        register_col, register_row, register_hits = scan_col(register_keywords)
        retouch_col, retouch_row, retouch_hits = scan_col(retouch_keywords)
        style_col, style_row, style_hits = scan_col(["스타일코드", "스타일"])

        if register_col is None or retouch_col is None:
            continue
        score = register_hits + retouch_hits + style_hits
        start_row = max([r for r in [register_row, retouch_row, style_row] if r is not None], default=0) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (register_col, retouch_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return 0

    try:
        df_raw = pd.read_excel(BytesIO(whoau_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return 0
    if df_raw is None or df_raw.empty:
        return 0
    register_col, retouch_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    register_series = data.iloc[:, register_col]
    retouch_series = data.iloc[:, retouch_col]
    style_series = data.iloc[:, style_col] if style_col is not None else None

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    register_dt = clean_date_series(register_series)
    register_ok = register_dt.notna()
    retouch_dt = clean_date_series(retouch_series)
    valid = (~register_ok) & retouch_dt.notna()
    if style_series is None:
        return int(valid.sum())
    style_text = style_series.astype(str).str.strip()
    style_ok = style_text.replace(r"^\s*$", pd.NA, regex=True).notna()
    # 후아유 스타일코드(WH 시작)만 집계
    whoau_mask = style_text.str.upper().str.startswith("WH", na=False)
    return int((valid & style_ok & whoau_mask).sum())

@st.cache_data
def load_clavis_registered_style_count(io_bytes=None, _cache_key=None):
    # 클라비스 트래킹판에서 스타일코드 + 공홈등록일 모두 있는 행 카운트
    if _cache_key is None:
        _cache_key = "clavis_style_count_default"
    clavis_bytes = _clavis_bytes(io_bytes)
    if clavis_bytes is None:
        return 0
    try:
        from openpyxl import load_workbook
    except Exception:
        return 0
    try:
        wb = load_workbook(BytesIO(clavis_bytes), read_only=True, data_only=True)
    except Exception:
        return 0
    ws = wb.active

    def normalize(value):
        return "".join(str(value).split()) if value is not None else ""

    header_row_idx = None
    header_vals = None
    for i, row in enumerate(ws.iter_rows(min_row=1, max_row=30, values_only=True), start=1):
        norm = [normalize(v) for v in row]
        if any("스타일코드" in v for v in norm) and any("공홈등록일" in v for v in norm):
            header_row_idx = i
            header_vals = norm
            break
    if header_row_idx is None:
        return 0

    def find_col(key):
        for idx, v in enumerate(header_vals):
            if v == key:
                return idx
        for idx, v in enumerate(header_vals):
            if key in v:
                return idx
        return None

    style_col = find_col("스타일코드") or find_col("스타일")
    register_col = find_col("공홈등록일") or find_col("등록일")
    if style_col is None or register_col is None:
        return 0

    style_set = set()
    for row in ws.iter_rows(min_row=header_row_idx + 1, values_only=True):
        style_val = row[style_col] if style_col < len(row) else None
        reg_val = row[register_col] if register_col < len(row) else None
        style_text = "" if style_val is None else str(style_val).strip()
        style_ok = style_text != ""
        reg_ok = reg_val is not None and str(reg_val).strip() != ""
        if reg_ok and style_ok:
            style_set.add(style_text)
    return int(len(style_set))

@st.cache_data
def load_clavis_register_avg_days(io_bytes=None, _cache_key=None, inout_bytes=None, _inout_cache_key=None):
    # 클라비스 등록 평균 소요일: 공홈등록일(브랜드 시트) - 최초입고일(BASE 시트)
    if _cache_key is None:
        _cache_key = "clavis_avg_days_default"
    clavis_bytes = _clavis_bytes(io_bytes)
    if clavis_bytes is None:
        return None
    base_map = _base_style_to_first_in_map(inout_bytes, _inout_cache_key) if (inout_bytes is not None or _inout_cache_key is not None) else {}
    if not base_map:
        return None
    def normalize_header_text(value):
        return "".join(str(value).split())

    register_date_keywords = ["공홈등록일", "등록일"]
    style_keywords = ["스타일코드", "스타일"]

    def scan_col(preview, keys):
        norm_keys = ["".join(k.split()) for k in keys]
        best_col = None
        best_hits = 0
        best_header_row = None
        max_rows = min(200, len(preview))
        for col_idx in range(preview.shape[1]):
            col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
            hits = 0
            header_row = None
            for row_idx, v in enumerate(col_vals):
                if any(k in v for k in norm_keys):
                    hits += 1
                    if header_row is None:
                        header_row = row_idx
            if hits > best_hits:
                best_hits = hits
                best_col = col_idx
                best_header_row = header_row
        return best_col, best_header_row, best_hits

    best_sheet = None
    best_score = -1
    best_cols = None
    best_start_row = None

    try:
        excel_file = pd.ExcelFile(BytesIO(clavis_bytes))
    except Exception:
        return None

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(clavis_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue
        register_col, register_row, register_hits = scan_col(preview, register_date_keywords)
        style_col, style_row, style_hits = scan_col(preview, style_keywords)
        if register_col is None or style_col is None:
            continue
        score = register_hits + style_hits
        start_row = max(
            [r for r in [register_row, style_row] if r is not None],
            default=0,
        ) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (register_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return None

    try:
        df_raw = pd.read_excel(BytesIO(clavis_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return None
    if df_raw is None or df_raw.empty:
        return None

    register_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    register_series = data.iloc[:, register_col]
    style_series = data.iloc[:, style_col]

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    def norm_style(val):
        return "".join(str(val).split()) if val is not None else ""

    style_ok = style_series.astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True).notna()
    register_ok = clean_date_series(register_series).notna()
    valid = style_ok & register_ok

    reg_dt = clean_date_series(register_series)

    diffs = []
    for idx in data.index:
        if not bool(valid.loc[idx]):
            continue
        style_norm = norm_style(style_series.loc[idx])
        base_dt = base_map.get(style_norm) if style_norm else None
        if base_dt is None or pd.isna(reg_dt.loc[idx]):
            continue
        diffs.append((reg_dt.loc[idx] - base_dt).days)
    if not diffs:
        return None
    return float(sum(diffs)) / len(diffs)

@st.cache_data
def load_clavis_unregistered_online_count(io_bytes=None, _cache_key=None):
    # 클라비스: 상품등록일 비어있고 리터칭완료일이 2020년 이후인 행 수
    if _cache_key is None:
        _cache_key = "clavis_unreg_default"
    clavis_bytes = _clavis_bytes(io_bytes)
    if clavis_bytes is None:
        return 0
    try:
        excel_file = pd.ExcelFile(BytesIO(clavis_bytes))
    except Exception:
        return 0

    def normalize_header_text(value):
        return "".join(str(value).split())

    register_keywords = [
        "상품등록일",
        "공홈등록일",
        "공홈 등록일",
        "등록일",
        "온라인등록일",
        "온라인 등록일",
        "온라인상품등록일",
        "온라인 상품등록일",
    ]
    retouch_keywords = [
        "리터칭완료일",
        "리터칭 완료일",
        "리터칭완료",
        "리터칭완료일자",
        "리터칭 완료일자",
        "리터칭완료날짜",
        "리터칭 완료날짜",
        "리터칭완료일(포토팀)",
        "리터칭완료일(촬영팀)",
        "리터칭완료일(최종)",
    ]
    register_norm = ["".join(k.split()) for k in register_keywords]
    retouch_norm = ["".join(k.split()) for k in retouch_keywords]

    best_sheet = None
    best_score = -1
    best_cols = None
    best_start_row = None

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(clavis_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue

        max_rows = min(200, len(preview))

        def scan_col(keys):
            norm_keys = ["".join(k.split()) for k in keys]
            best_col = None
            best_hits = 0
            best_header_row = None
            for col_idx in range(preview.shape[1]):
                col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
                hits = 0
                header_row = None
                for row_idx, v in enumerate(col_vals):
                    if any(k in v for k in norm_keys):
                        hits += 1
                        if header_row is None:
                            header_row = row_idx
                if hits > best_hits:
                    best_hits = hits
                    best_col = col_idx
                    best_header_row = header_row
            return best_col, best_header_row, best_hits

        register_col, register_row, register_hits = scan_col(register_keywords)
        retouch_col, retouch_row, retouch_hits = scan_col(retouch_keywords)
        style_col, style_row, style_hits = scan_col(["스타일코드", "스타일"])

        if register_col is None or retouch_col is None:
            continue
        score = register_hits + retouch_hits + style_hits
        start_row = max([r for r in [register_row, retouch_row, style_row] if r is not None], default=0) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (register_col, retouch_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return 0

    try:
        df_raw = pd.read_excel(BytesIO(clavis_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return 0
    if df_raw is None or df_raw.empty:
        return 0
    register_col, retouch_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    register_series = data.iloc[:, register_col]
    retouch_series = data.iloc[:, retouch_col]
    style_series = data.iloc[:, style_col] if style_col is not None else None

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    register_dt = clean_date_series(register_series)
    register_ok = register_dt.notna()
    retouch_dt = clean_date_series(retouch_series)
    valid = (~register_ok) & retouch_dt.notna()
    if style_series is None:
        return int(valid.sum())
    style_text = style_series.astype(str).str.strip()
    style_ok = style_text.replace(r"^\s*$", pd.NA, regex=True).notna()
    # 클라비스 스타일코드(CV 시작)만 집계
    clavis_mask = style_text.str.upper().str.startswith("CV", na=False)
    return int((valid & style_ok & clavis_mask).sum())

@st.cache_data
def load_mixxo_metric_days(target_keywords, io_bytes=None, _cache_key=None):
    # 미쏘 트래킹판에서 특정 소요일 컬럼 평균 산출
    if _cache_key is None:
        _cache_key = "mixxo_metric_default"
    mixxo_bytes = _mixxo_bytes(io_bytes)
    if mixxo_bytes is None:
        return None
    try:
        excel_file = pd.ExcelFile(BytesIO(mixxo_bytes))
    except Exception:
        return None
    best_sheet = None
    best_df = None
    best_hits = -1
    best_header = None

    def normalize_header_text(value):
        return "".join(str(value).split())

    normalized_keywords = ["".join(str(k).split()) for k in target_keywords]
    for sheet_name in excel_file.sheet_names:
        try:
            df_raw_sheet = pd.read_excel(BytesIO(mixxo_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if df_raw_sheet.empty:
            continue
        header_mask = df_raw_sheet.astype(str).applymap(
            lambda v: any(k in normalize_header_text(v) for k in normalized_keywords)
        )
        if not header_mask.any().any():
            continue
        hits = int(header_mask.sum().sum())
        if hits > best_hits:
            header_pos = header_mask.stack().idxmax()
            best_header = (int(header_pos[0]), int(header_pos[1]))
            best_hits = hits
            best_sheet = sheet_name
            best_df = df_raw_sheet
    if best_df is None or best_header is None:
        return None
    header_row_idx, col_idx = best_header
    values = pd.to_numeric(
        best_df.iloc[header_row_idx + 1 :, col_idx], errors="coerce"
    ).dropna()
    values = values[(values >= 0) & (values < 100)]
    if values.empty:
        return None
    mean_val = values.mean()

    def col_letter(n):
        n += 1
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    header_cell = f"{col_letter(col_idx)}{header_row_idx + 1}"
    return float(mean_val), int(values.count()), header_cell

@st.cache_data
def load_mixxo_registered_style_count(io_bytes=None, _cache_key=None):
    # 미쏘 트래킹판에서 스타일코드 + 공홈등록일 모두 있는 행 카운트 (메모리에서 읽음)
    if _cache_key is None:
        _cache_key = "mixxo_reg_count_default"
    mixxo_bytes = _mixxo_bytes(io_bytes)
    if mixxo_bytes is None:
        return 0
    try:
        excel_file = pd.ExcelFile(BytesIO(mixxo_bytes))
    except Exception:
        return 0

    def normalize(value):
        return "".join(str(value).split()) if value is not None else ""

    for sheet_name in excel_file.sheet_names:
        try:
            df_raw = pd.read_excel(BytesIO(mixxo_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if df_raw is None or df_raw.empty:
            continue
        header_row_idx = None
        header_vals = None
        for i in range(min(30, len(df_raw))):
            row = df_raw.iloc[i].tolist()
            norm = [normalize(v) for v in row]
            if any("스타일코드" in v for v in norm) and any("공홈등록일" in v for v in norm):
                header_row_idx = i
                header_vals = norm
                break
        if header_row_idx is None:
            continue

        def find_col(key):
            for idx, v in enumerate(header_vals):
                if v == key:
                    return idx
            for idx, v in enumerate(header_vals):
                if key in v:
                    return idx
            return None

        style_col = find_col("스타일코드") or find_col("스타일")
        register_col = find_col("공홈등록일") or find_col("등록일")
        if style_col is None or register_col is None:
            continue

        style_set = set()
        data = df_raw.iloc[header_row_idx + 1 :]
        for _, row in data.iterrows():
            style_val = row.iloc[style_col] if style_col < len(row) else None
            reg_val = row.iloc[register_col] if register_col < len(row) else None
            style_text = "" if style_val is None else str(style_val).strip()
            style_ok = style_text != ""
            reg_ok = reg_val is not None and str(reg_val).strip() != ""
            if reg_ok and style_ok:
                style_set.add(style_text)
        return int(len(style_set))
    return 0

@st.cache_data
def load_mixxo_register_avg_days(io_bytes=None, _cache_key=None, inout_bytes=None, _inout_cache_key=None):
    # 미쏘 등록 평균 소요일: 공홈등록일(브랜드 시트) - 최초입고일(BASE 시트)
    if _cache_key is None:
        _cache_key = "mixxo_avg_days_default"
    mixxo_bytes = _mixxo_bytes(io_bytes)
    if mixxo_bytes is None:
        return None
    base_map = _base_style_to_first_in_map(inout_bytes, _inout_cache_key) if (inout_bytes is not None or _inout_cache_key is not None) else {}
    if not base_map:
        return None
    def normalize_header_text(value):
        return "".join(str(value).split())

    register_date_keywords = ["공홈등록일", "등록일"]
    style_keywords = ["스타일코드", "스타일"]

    def scan_col(preview, keys):
        norm_keys = ["".join(k.split()) for k in keys]
        best_col = None
        best_hits = 0
        best_header_row = None
        max_rows = min(200, len(preview))
        for col_idx in range(preview.shape[1]):
            col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
            hits = 0
            header_row = None
            for row_idx, v in enumerate(col_vals):
                if any(k in v for k in norm_keys):
                    hits += 1
                    if header_row is None:
                        header_row = row_idx
            if hits > best_hits:
                best_hits = hits
                best_col = col_idx
                best_header_row = header_row
        return best_col, best_header_row, best_hits

    best_sheet = None
    best_score = -1
    best_cols = None
    best_start_row = None

    try:
        excel_file = pd.ExcelFile(BytesIO(mixxo_bytes))
    except Exception:
        return None

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(mixxo_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue
        register_col, register_row, register_hits = scan_col(preview, register_date_keywords)
        style_col, style_row, style_hits = scan_col(preview, style_keywords)
        if register_col is None or style_col is None:
            continue
        score = register_hits + style_hits
        start_row = max(
            [r for r in [register_row, style_row] if r is not None],
            default=0,
        ) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (register_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return None

    try:
        df_raw = pd.read_excel(BytesIO(mixxo_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return None
    if df_raw is None or df_raw.empty:
        return None

    register_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    register_series = data.iloc[:, register_col]
    style_series = data.iloc[:, style_col]

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    def norm_style(val):
        return "".join(str(val).split()) if val is not None else ""

    style_ok = style_series.astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True).notna()
    register_ok = clean_date_series(register_series).notna()
    valid = style_ok & register_ok

    reg_dt = clean_date_series(register_series)

    diffs = []
    for idx in data.index:
        if not bool(valid.loc[idx]):
            continue
        style_norm = norm_style(style_series.loc[idx])
        base_dt = base_map.get(style_norm) if style_norm else None
        if base_dt is None or pd.isna(reg_dt.loc[idx]):
            continue
        diffs.append((reg_dt.loc[idx] - base_dt).days)
    if not diffs:
        return None
    return float(sum(diffs)) / len(diffs)

@st.cache_data
def load_mixxo_unregistered_online_count(io_bytes=None, _cache_key=None):
    # 미쏘: 상품등록일 비어있고 리터칭완료일이 2020년 이후인 행 수
    if _cache_key is None:
        _cache_key = "mixxo_unreg_default"
    mixxo_bytes = _mixxo_bytes(io_bytes)
    if mixxo_bytes is None:
        return 0
    try:
        excel_file = pd.ExcelFile(BytesIO(mixxo_bytes))
    except Exception:
        return 0

    def normalize_header_text(value):
        return "".join(str(value).split())

    register_keywords = [
        "상품등록일",
        "공홈등록일",
        "공홈 등록일",
        "등록일",
        "온라인등록일",
        "온라인 등록일",
        "온라인상품등록일",
        "온라인 상품등록일",
    ]
    retouch_keywords = [
        "리터칭완료일",
        "리터칭 완료일",
        "리터칭완료",
        "리터칭완료일자",
        "리터칭 완료일자",
        "리터칭완료날짜",
        "리터칭 완료날짜",
        "리터칭완료일(포토팀)",
        "리터칭완료일(촬영팀)",
        "리터칭완료일(최종)",
    ]
    register_norm = ["".join(k.split()) for k in register_keywords]
    retouch_norm = ["".join(k.split()) for k in retouch_keywords]

    best_sheet = None
    best_score = -1
    best_cols = None
    best_start_row = None

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(mixxo_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue

        max_rows = min(200, len(preview))

        def scan_col(keys):
            norm_keys = ["".join(k.split()) for k in keys]
            best_col = None
            best_hits = 0
            best_header_row = None
            for col_idx in range(preview.shape[1]):
                col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
                hits = 0
                header_row = None
                for row_idx, v in enumerate(col_vals):
                    if any(k in v for k in norm_keys):
                        hits += 1
                        if header_row is None:
                            header_row = row_idx
                if hits > best_hits:
                    best_hits = hits
                    best_col = col_idx
                    best_header_row = header_row
            return best_col, best_header_row, best_hits

        register_col, register_row, register_hits = scan_col(register_keywords)
        retouch_col, retouch_row, retouch_hits = scan_col(retouch_keywords)
        style_col, style_row, style_hits = scan_col(["스타일코드", "스타일"])

        if register_col is None or retouch_col is None:
            continue
        score = register_hits + retouch_hits + style_hits
        start_row = max([r for r in [register_row, retouch_row, style_row] if r is not None], default=0) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (register_col, retouch_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return 0

    try:
        df_raw = pd.read_excel(BytesIO(mixxo_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return 0
    if df_raw is None or df_raw.empty:
        return 0
    register_col, retouch_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    register_series = data.iloc[:, register_col]
    retouch_series = data.iloc[:, retouch_col]
    style_series = data.iloc[:, style_col] if style_col is not None else None

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    register_dt = clean_date_series(register_series)
    register_ok = register_dt.notna()
    retouch_dt = clean_date_series(retouch_series)
    valid = (~register_ok) & retouch_dt.notna()
    if style_series is None:
        return int(valid.sum())
    style_text = style_series.astype(str).str.strip()
    style_ok = style_text.replace(r"^\s*$", pd.NA, regex=True).notna()
    # 미쏘 스타일코드(MI 시작)만 집계
    mixxo_mask = style_text.str.upper().str.startswith("MI", na=False)
    return int((valid & style_ok & mixxo_mask).sum())

@st.cache_data
def load_roem_metric_days(target_keywords, io_bytes=None, _cache_key=None):
    # 로엠 트래킹판에서 특정 소요일 컬럼 평균 산출
    if _cache_key is None:
        _cache_key = "roem_metric_default"
    roem_bytes = _roem_bytes(io_bytes)
    if roem_bytes is None:
        return None
    try:
        excel_file = pd.ExcelFile(BytesIO(roem_bytes))
    except Exception:
        return None
    best_sheet = None
    best_df = None
    best_hits = -1
    best_header = None

    def normalize_header_text(value):
        return "".join(str(value).split())

    normalized_keywords = ["".join(str(k).split()) for k in target_keywords]
    for sheet_name in excel_file.sheet_names:
        try:
            df_raw_sheet = pd.read_excel(BytesIO(roem_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if df_raw_sheet.empty:
            continue
        header_mask = df_raw_sheet.astype(str).applymap(
            lambda v: any(k in normalize_header_text(v) for k in normalized_keywords)
        )
        if not header_mask.any().any():
            continue
        hits = int(header_mask.sum().sum())
        if hits > best_hits:
            header_pos = header_mask.stack().idxmax()
            best_header = (int(header_pos[0]), int(header_pos[1]))
            best_hits = hits
            best_sheet = sheet_name
            best_df = df_raw_sheet
    if best_df is None or best_header is None:
        return None
    header_row_idx, col_idx = best_header
    values = pd.to_numeric(
        best_df.iloc[header_row_idx + 1 :, col_idx], errors="coerce"
    ).dropna()
    values = values[(values >= 0) & (values < 100)]
    if values.empty:
        return None
    mean_val = values.mean()

    def col_letter(n):
        n += 1
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    header_cell = f"{col_letter(col_idx)}{header_row_idx + 1}"
    return float(mean_val), int(values.count()), header_cell

@st.cache_data
def load_hp_metric_days(target_keywords, io_bytes=None, _cache_key=None):
    # 슈펜(HP) 트래킹판에서 특정 소요일 컬럼 평균 산출
    if _cache_key is None:
        _cache_key = "hp_metric_default"
    hp_bytes = _hp_bytes(io_bytes)
    if hp_bytes is None:
        return None
    try:
        excel_file = pd.ExcelFile(BytesIO(hp_bytes))
    except Exception:
        return None
    best_sheet = None
    best_df = None
    best_hits = -1
    best_header = None

    def normalize_header_text(value):
        return "".join(str(value).split())

    normalized_keywords = ["".join(str(k).split()) for k in target_keywords]
    for sheet_name in excel_file.sheet_names:
        try:
            df_raw_sheet = pd.read_excel(BytesIO(hp_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if df_raw_sheet.empty:
            continue
        header_mask = df_raw_sheet.astype(str).applymap(
            lambda v: any(k in normalize_header_text(v) for k in normalized_keywords)
        )
        if not header_mask.any().any():
            continue
        hits = int(header_mask.sum().sum())
        if hits > best_hits:
            header_pos = header_mask.stack().idxmax()
            best_header = (int(header_pos[0]), int(header_pos[1]))
            best_hits = hits
            best_sheet = sheet_name
            best_df = df_raw_sheet
    if best_df is None or best_header is None:
        return None
    header_row_idx, col_idx = best_header
    values = pd.to_numeric(
        best_df.iloc[header_row_idx + 1 :, col_idx], errors="coerce"
    ).dropna()
    values = values[(values >= 0) & (values < 100)]
    if values.empty:
        return None
    mean_val = values.mean()

    def col_letter(n):
        n += 1
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    header_cell = f"{col_letter(col_idx)}{header_row_idx + 1}"
    return float(mean_val), int(values.count()), header_cell

@st.cache_data
def load_roem_registered_style_count(io_bytes=None, _cache_key=None):
    # 로엠 트래킹판에서 스타일코드 + 공홈등록일 모두 있는 행 카운트 (메모리에서 읽음)
    if _cache_key is None:
        _cache_key = "roem_reg_count_default"
    roem_bytes = _roem_bytes(io_bytes)
    if roem_bytes is None:
        return 0
    try:
        excel_file = pd.ExcelFile(BytesIO(roem_bytes))
    except Exception:
        return 0

    def normalize_header_text(value):
        return "".join(str(value).split())

    register_keywords = [
        "상품등록일",
        "공홈등록일",
        "공홈 등록일",
        "등록일",
        "온라인등록일",
        "온라인 등록일",
        "온라인상품등록일",
        "온라인 상품등록일",
    ]
    style_keywords = ["스타일코드", "스타일"]

    def scan_col(preview, keys):
        norm_keys = ["".join(k.split()) for k in keys]
        best_col = None
        best_hits = 0
        best_header_row = None
        max_rows = min(200, len(preview))
        for col_idx in range(preview.shape[1]):
            col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
            hits = 0
            header_row = None
            for row_idx, v in enumerate(col_vals):
                if any(k in v for k in norm_keys):
                    hits += 1
                    if header_row is None:
                        header_row = row_idx
            if hits > best_hits:
                best_hits = hits
                best_col = col_idx
                best_header_row = header_row
        return best_col, best_header_row, best_hits

    best_sheet = None
    best_cols = None
    best_start_row = None
    best_score = -1

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(roem_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue
        register_col, register_row, register_hits = scan_col(preview, register_keywords)
        style_col, style_row, style_hits = scan_col(preview, style_keywords)
        if register_col is None or style_col is None:
            continue
        score = register_hits + style_hits
        start_row = max([r for r in [register_row, style_row] if r is not None], default=0) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (register_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return 0

    try:
        df_raw = pd.read_excel(BytesIO(roem_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return 0
    if df_raw is None or df_raw.empty:
        return 0

    register_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    register_series = data.iloc[:, register_col]
    style_series = data.iloc[:, style_col]

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    register_ok = clean_date_series(register_series).notna()
    style_text = style_series.astype(str).str.strip()
    style_ok = style_text.replace(r"^\s*$", pd.NA, regex=True).notna()
    roem_mask = style_text.str.upper().str.startswith("RM", na=False)
    valid = register_ok & style_ok & roem_mask
    return int(style_text[valid].nunique(dropna=True))

@st.cache_data
def load_hp_registered_style_count(io_bytes=None, _cache_key=None):
    # 슈펜(HP) 트래킹판에서 스타일코드 + 공홈등록일 모두 있는 행 카운트
    if _cache_key is None:
        _cache_key = "hp_reg_count_default"
    hp_bytes = _hp_bytes(io_bytes)
    if hp_bytes is None:
        return 0
    try:
        excel_file = pd.ExcelFile(BytesIO(hp_bytes))
    except Exception:
        return 0

    def normalize_header_text(value):
        return "".join(str(value).split())

    register_keywords = [
        "상품등록일", "공홈등록일", "공홈 등록일", "등록일",
        "온라인등록일", "온라인 등록일", "온라인상품등록일", "온라인 상품등록일",
    ]
    style_keywords = ["스타일코드", "스타일"]

    def scan_col(preview, keys):
        norm_keys = ["".join(k.split()) for k in keys]
        best_col = None
        best_hits = 0
        best_header_row = None
        max_rows = min(200, len(preview))
        for col_idx in range(preview.shape[1]):
            col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
            hits = 0
            header_row = None
            for row_idx, v in enumerate(col_vals):
                if any(k in v for k in norm_keys):
                    hits += 1
                    if header_row is None:
                        header_row = row_idx
            if hits > best_hits:
                best_hits = hits
                best_col = col_idx
                best_header_row = header_row
        return best_col, best_header_row, best_hits

    best_sheet = None
    best_cols = None
    best_start_row = None
    best_score = -1

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(hp_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue
        register_col, register_row, register_hits = scan_col(preview, register_keywords)
        style_col, style_row, style_hits = scan_col(preview, style_keywords)
        if register_col is None or style_col is None:
            continue
        score = register_hits + style_hits
        start_row = max([r for r in [register_row, style_row] if r is not None], default=0) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (register_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return 0

    try:
        df_raw = pd.read_excel(BytesIO(hp_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return 0
    if df_raw is None or df_raw.empty:
        return 0

    register_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    register_series = data.iloc[:, register_col]
    style_series = data.iloc[:, style_col]

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    register_ok = clean_date_series(register_series).notna()
    style_text = style_series.astype(str).str.strip()
    style_ok = style_text.replace(r"^\s*$", pd.NA, regex=True).notna()
    hp_mask = style_text.str.upper().str.startswith("HP", na=False)
    # 슈펜 시트는 단일 브랜드이므로, HP 접두사가 하나도 없으면 접두사 필터 없이 전부 인정
    if hp_mask.any():
        valid = register_ok & style_ok & hp_mask
    else:
        valid = register_ok & style_ok
    return int(style_text[valid].nunique(dropna=True))

@st.cache_data
def load_roem_register_avg_days(io_bytes=None, _cache_key=None, inout_bytes=None, _inout_cache_key=None):
    # 로엠 등록 평균 소요일: 공홈등록일(브랜드 시트) - 최초입고일(BASE 시트)
    if _cache_key is None:
        _cache_key = "roem_avg_days_default"
    roem_bytes = _roem_bytes(io_bytes)
    if roem_bytes is None:
        return None
    base_map = _base_style_to_first_in_map(inout_bytes, _inout_cache_key) if (inout_bytes is not None or _inout_cache_key is not None) else {}
    if not base_map:
        return None
    def normalize_header_text(value):
        return "".join(str(value).split())

    register_date_keywords = ["공홈등록일", "등록일"]
    style_keywords = ["스타일코드", "스타일"]

    def scan_col(preview, keys):
        norm_keys = ["".join(k.split()) for k in keys]
        best_col = None
        best_hits = 0
        best_header_row = None
        max_rows = min(200, len(preview))
        for col_idx in range(preview.shape[1]):
            col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
            hits = 0
            header_row = None
            for row_idx, v in enumerate(col_vals):
                if any(k in v for k in norm_keys):
                    hits += 1
                    if header_row is None:
                        header_row = row_idx
            if hits > best_hits:
                best_hits = hits
                best_col = col_idx
                best_header_row = header_row
        return best_col, best_header_row, best_hits

    best_sheet = None
    best_score = -1
    best_cols = None
    best_start_row = None

    try:
        excel_file = pd.ExcelFile(BytesIO(roem_bytes))
    except Exception:
        return None

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(roem_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue
        register_col, register_row, register_hits = scan_col(preview, register_date_keywords)
        style_col, style_row, style_hits = scan_col(preview, style_keywords)
        if register_col is None or style_col is None:
            continue
        score = register_hits + style_hits
        start_row = max(
            [r for r in [register_row, style_row] if r is not None],
            default=0,
        ) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (register_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return None

    try:
        df_raw = pd.read_excel(BytesIO(roem_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return None
    if df_raw is None or df_raw.empty:
        return None

    register_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    register_series = data.iloc[:, register_col]
    style_series = data.iloc[:, style_col]

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    def norm_style(val):
        return "".join(str(val).split()) if val is not None else ""

    style_ok = style_series.astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True).notna()
    register_ok = clean_date_series(register_series).notna()
    valid = style_ok & register_ok

    reg_dt = clean_date_series(register_series)

    diffs = []
    for idx in data.index:
        if not bool(valid.loc[idx]):
            continue
        style_norm = norm_style(style_series.loc[idx])
        base_dt = base_map.get(style_norm) if style_norm else None
        if base_dt is None or pd.isna(reg_dt.loc[idx]):
            continue
        diffs.append((reg_dt.loc[idx] - base_dt).days)
    if not diffs:
        return None
    return float(sum(diffs)) / len(diffs)

@st.cache_data
def load_hp_register_avg_days(io_bytes=None, _cache_key=None, inout_bytes=None, _inout_cache_key=None):
    # 슈펜(HP) 등록 평균 소요일: 공홈등록일(브랜드 시트) - 최초입고일(BASE 시트)
    if _cache_key is None:
        _cache_key = "hp_avg_days_default"
    hp_bytes = _hp_bytes(io_bytes)
    if hp_bytes is None:
        return None
    base_map = _base_style_to_first_in_map(inout_bytes, _inout_cache_key) if (inout_bytes is not None or _inout_cache_key is not None) else {}
    if not base_map:
        return None
    def normalize_header_text(value):
        return "".join(str(value).split())

    register_date_keywords = ["공홈등록일", "등록일"]
    style_keywords = ["스타일코드", "스타일"]

    def scan_col(preview, keys):
        norm_keys = ["".join(k.split()) for k in keys]
        best_col = None
        best_hits = 0
        best_header_row = None
        max_rows = min(200, len(preview))
        for col_idx in range(preview.shape[1]):
            col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
            hits = 0
            header_row = None
            for row_idx, v in enumerate(col_vals):
                if any(k in v for k in norm_keys):
                    hits += 1
                    if header_row is None:
                        header_row = row_idx
            if hits > best_hits:
                best_hits = hits
                best_col = col_idx
                best_header_row = header_row
        return best_col, best_header_row, best_hits

    best_sheet = None
    best_score = -1
    best_cols = None
    best_start_row = None

    try:
        excel_file = pd.ExcelFile(BytesIO(hp_bytes))
    except Exception:
        return None

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(hp_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue
        register_col, register_row, register_hits = scan_col(preview, register_date_keywords)
        style_col, style_row, style_hits = scan_col(preview, style_keywords)
        if register_col is None or style_col is None:
            continue
        score = register_hits + style_hits
        start_row = max(
            [r for r in [register_row, style_row] if r is not None],
            default=0,
        ) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (register_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return None

    try:
        df_raw = pd.read_excel(BytesIO(hp_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return None
    if df_raw is None or df_raw.empty:
        return None

    register_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    register_series = data.iloc[:, register_col]
    style_series = data.iloc[:, style_col]

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    def norm_style(val):
        return "".join(str(val).split()) if val is not None else ""

    style_ok = style_series.astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True).notna()
    register_ok = clean_date_series(register_series).notna()
    valid = style_ok & register_ok

    reg_dt = clean_date_series(register_series)

    diffs = []
    for idx in data.index:
        if not bool(valid.loc[idx]):
            continue
        style_norm = norm_style(style_series.loc[idx])
        base_dt = base_map.get(style_norm) if style_norm else None
        if base_dt is None or pd.isna(reg_dt.loc[idx]):
            continue
        diffs.append((reg_dt.loc[idx] - base_dt).days)
    if not diffs:
        return None
    return float(sum(diffs)) / len(diffs)

@st.cache_data
def load_roem_unregistered_online_count(io_bytes=None, _cache_key=None):
    # 로엠: 상품등록일 비어있고 리터칭완료일이 2020년 이후인 행 수
    if _cache_key is None:
        _cache_key = "roem_unreg_default"
    roem_bytes = _roem_bytes(io_bytes)
    if roem_bytes is None:
        return 0
    try:
        excel_file = pd.ExcelFile(BytesIO(roem_bytes))
    except Exception:
        return 0

    def normalize_header_text(value):
        return "".join(str(value).split())

    register_keywords = [
        "공홈등록일"

    ]
    retouch_keywords = [
        "리터칭완료일"
    ]
    register_norm = ["".join(k.split()) for k in register_keywords]
    retouch_norm = ["".join(k.split()) for k in retouch_keywords]

    best_sheet = None
    best_score = -1
    best_cols = None
    best_start_row = None

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(roem_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue

        max_rows = min(200, len(preview))

        def scan_col(keys):
            norm_keys = ["".join(k.split()) for k in keys]
            best_col = None
            best_hits = 0
            best_header_row = None
            for col_idx in range(preview.shape[1]):
                col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
                hits = 0
                header_row = None
                for row_idx, v in enumerate(col_vals):
                    if any(k in v for k in norm_keys):
                        hits += 1
                        if header_row is None:
                            header_row = row_idx
                if hits > best_hits:
                    best_hits = hits
                    best_col = col_idx
                    best_header_row = header_row
            return best_col, best_header_row, best_hits

        register_col, register_row, register_hits = scan_col(register_keywords)
        retouch_col, retouch_row, retouch_hits = scan_col(retouch_keywords)
        style_col, style_row, style_hits = scan_col(["스타일코드", "스타일"])

        if register_col is None or retouch_col is None:
            continue
        score = register_hits + retouch_hits + style_hits
        start_row = max([r for r in [register_row, retouch_row, style_row] if r is not None], default=0) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (register_col, retouch_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return 0

    try:
        df_raw = pd.read_excel(BytesIO(roem_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return 0
    if df_raw is None or df_raw.empty:
        return 0
    register_col, retouch_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    register_series = data.iloc[:, register_col]
    retouch_series = data.iloc[:, retouch_col]
    style_series = data.iloc[:, style_col] if style_col is not None else None

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    register_dt = clean_date_series(register_series)
    register_ok = register_dt.notna()
    retouch_dt = clean_date_series(retouch_series)
    valid = (~register_ok) & retouch_dt.notna()
    if style_series is None:
        return int(valid.sum())
    style_text = style_series.astype(str).str.strip()
    style_ok = style_text.replace(r"^\s*$", pd.NA, regex=True).notna()
    # 로엠 스타일코드(RM 시작)만 집계
    roem_mask = style_text.str.upper().str.startswith("RM", na=False)
    return int((valid & style_ok & roem_mask).sum())

@st.cache_data
def load_hp_unregistered_online_count(io_bytes=None, _cache_key=None):
    # 슈펜(HP): 상품등록일 비어있고 리터칭완료일이 2020년 이후인 행 수
    if _cache_key is None:
        _cache_key = "hp_unreg_default"
    hp_bytes = _hp_bytes(io_bytes)
    if hp_bytes is None:
        return 0
    try:
        excel_file = pd.ExcelFile(BytesIO(hp_bytes))
    except Exception:
        return 0

    def normalize_header_text(value):
        return "".join(str(value).split())

    register_keywords = ["공홈등록일"]
    retouch_keywords = ["리터칭완료일"]
    register_norm = ["".join(k.split()) for k in register_keywords]
    retouch_norm = ["".join(k.split()) for k in retouch_keywords]

    best_sheet = None
    best_score = -1
    best_cols = None
    best_start_row = None

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(hp_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue

        max_rows = min(200, len(preview))

        def scan_col(keys):
            norm_keys = ["".join(k.split()) for k in keys]
            best_col = None
            best_hits = 0
            best_header_row = None
            for col_idx in range(preview.shape[1]):
                col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
                hits = 0
                header_row = None
                for row_idx, v in enumerate(col_vals):
                    if any(k in v for k in norm_keys):
                        hits += 1
                        if header_row is None:
                            header_row = row_idx
                if hits > best_hits:
                    best_hits = hits
                    best_col = col_idx
                    best_header_row = header_row
            return best_col, best_header_row, best_hits

        register_col, register_row, register_hits = scan_col(register_keywords)
        retouch_col, retouch_row, retouch_hits = scan_col(retouch_keywords)
        style_col, style_row, style_hits = scan_col(["스타일코드", "스타일"])

        if register_col is None or retouch_col is None:
            continue
        score = register_hits + retouch_hits + style_hits
        start_row = max([r for r in [register_row, retouch_row, style_row] if r is not None], default=0) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (register_col, retouch_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return 0

    try:
        df_raw = pd.read_excel(BytesIO(hp_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return 0
    if df_raw is None or df_raw.empty:
        return 0
    register_col, retouch_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    register_series = data.iloc[:, register_col]
    retouch_series = data.iloc[:, retouch_col]
    style_series = data.iloc[:, style_col] if style_col is not None else None

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    register_dt = clean_date_series(register_series)
    register_ok = register_dt.notna()
    retouch_dt = clean_date_series(retouch_series)
    valid = (~register_ok) & retouch_dt.notna()
    if style_series is None:
        return int(valid.sum())
    style_text = style_series.astype(str).str.strip()
    style_ok = style_text.replace(r"^\s*$", pd.NA, regex=True).notna()
    hp_mask = style_text.str.upper().str.startswith("HP", na=False)
    if hp_mask.any():
        valid = valid & style_ok & hp_mask
    else:
        valid = valid & style_ok
    return int(valid.sum())

@st.cache_data
def load_roem_register_debug(io_bytes=None, _cache_key=None):
    # 로엠 등록 스타일수 디버그 정보 (메모리에서 읽음)
    if _cache_key is None:
        _cache_key = "roem_debug_default"
    roem_bytes = _roem_bytes(io_bytes)
    if roem_bytes is None:
        return {"error": "파일 경로 없음"}
    try:
        excel_file = pd.ExcelFile(BytesIO(roem_bytes))
    except Exception:
        return {"error": "엑셀 로드 실패"}

    def normalize_header_text(value):
        return "".join(str(value).split())

    register_keywords = [
        "상품등록일",
        "공홈등록일",
        "공홈 등록일",
        "등록일",
        "온라인등록일",
        "온라인 등록일",
        "온라인상품등록일",
        "온라인 상품등록일",
    ]
    style_keywords = ["스타일코드", "스타일"]

    def scan_col(preview, keys):
        norm_keys = ["".join(k.split()) for k in keys]
        best_col = None
        best_hits = 0
        best_header_row = None
        max_rows = min(200, len(preview))
        for col_idx in range(preview.shape[1]):
            col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
            hits = 0
            header_row = None
            for row_idx, v in enumerate(col_vals):
                if any(k in v for k in norm_keys):
                    hits += 1
                    if header_row is None:
                        header_row = row_idx
            if hits > best_hits:
                best_hits = hits
                best_col = col_idx
                best_header_row = header_row
        return best_col, best_header_row, best_hits

    best_sheet = None
    best_cols = None
    best_start_row = None
    best_score = -1

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(roem_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue
        register_col, register_row, register_hits = scan_col(preview, register_keywords)
        style_col, style_row, style_hits = scan_col(preview, style_keywords)
        if register_col is None or style_col is None:
            continue
        score = register_hits + style_hits
        start_row = max([r for r in [register_row, style_row] if r is not None], default=0) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (register_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return {"error": "헤더 탐색 실패"}

    try:
        df_raw = pd.read_excel(BytesIO(roem_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return {"error": "시트 로드 실패"}
    if df_raw is None or df_raw.empty:
        return {"error": "시트 비어있음"}

    register_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    register_series = data.iloc[:, register_col]
    style_series = data.iloc[:, style_col]

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    reg_dt = clean_date_series(register_series)
    reg_ok = reg_dt.notna()
    style_text = style_series.astype(str).str.strip()
    style_ok = style_text.replace(r"^\s*$", pd.NA, regex=True).notna()
    roem_mask = style_text.str.upper().str.startswith("RM", na=False)

    valid_rm = reg_ok & style_ok & roem_mask
    valid_all = reg_ok & style_ok

    header_row = best_start_row - 1 if best_start_row is not None else 0
    header_vals = df_raw.iloc[header_row].tolist() if header_row >= 0 and header_row < len(df_raw) else []

    return {
        "sheet": best_sheet,
        "register_col": int(register_col) + 1,
        "style_col": int(style_col) + 1,
        "start_row": int(best_start_row) + 1,
        "header_row": int(header_row) + 1,
        "header_vals": [str(v) for v in header_vals[:30]],
        "rows_total": int(len(data)),
        "reg_rows": int(reg_ok.sum()),
        "style_rows": int(style_ok.sum()),
        "rm_rows": int(roem_mask.sum()),
        "registered_rm_rows": int(valid_rm.sum()),
        "registered_rm_unique": int(style_text[valid_rm].nunique(dropna=True)),
        "registered_unique": int(style_text[valid_all].nunique(dropna=True)),
    }

@st.cache_data
def load_spao_unregistered_online_count(io_bytes=None, _cache_key=None):
    # 스파오: 상품등록일 비어있고 리터칭완료일이 2020년 이후인 행 수
    if _cache_key is None:
        _cache_key = "spao_unreg_default"
    if io_bytes is None or len(io_bytes) == 0:
        return 0
    spao_bytes = io_bytes
    try:
        excel_file = pd.ExcelFile(BytesIO(spao_bytes))
    except Exception:
        return 0

    def normalize_header_text(value):
        return "".join(str(value).split())

    register_keywords = ["상품등록일", "공홈등록일", "등록일"]
    retouch_keywords = ["리터칭완료일", "리터칭 완료일", "리터칭완료"]

    best_sheet = None
    best_score = -1
    best_cols = None
    best_start_row = None

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(spao_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue

        max_rows = min(200, len(preview))

        def scan_col(keys):
            norm_keys = ["".join(k.split()) for k in keys]
            best_col = None
            best_hits = 0
            best_header_row = None
            for col_idx in range(preview.shape[1]):
                col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
                hits = 0
                header_row = None
                for row_idx, v in enumerate(col_vals):
                    if any(k in v for k in norm_keys):
                        hits += 1
                        if header_row is None:
                            header_row = row_idx
                if hits > best_hits:
                    best_hits = hits
                    best_col = col_idx
                    best_header_row = header_row
            return best_col, best_header_row, best_hits

        register_col, register_row, register_hits = scan_col(register_keywords)
        retouch_col, retouch_row, retouch_hits = scan_col(retouch_keywords)
        style_col, style_row, style_hits = scan_col(["스타일코드", "스타일"])

        if register_col is None or retouch_col is None:
            continue
        score = register_hits + retouch_hits + style_hits
        start_row = max([r for r in [register_row, retouch_row, style_row] if r is not None], default=0) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (register_col, retouch_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return 0

    try:
        df_raw = pd.read_excel(BytesIO(spao_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return 0
    if df_raw is None or df_raw.empty:
        return 0

    register_col, retouch_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    register_series = data.iloc[:, register_col]
    retouch_series = data.iloc[:, retouch_col]
    style_series = data.iloc[:, style_col] if style_col is not None else None

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    register_dt = clean_date_series(register_series)
    register_ok = register_dt.notna()
    retouch_dt = clean_date_series(retouch_series)
    valid = (~register_ok) & retouch_dt.notna()
    if style_series is None:
        return int(valid.sum())
    style_text = style_series.astype(str).str.strip()
    style_ok = style_text.replace(r"^\s*$", pd.NA, regex=True).notna()
    spao_mask = style_text.str.upper().str.startswith("SP", na=False)
    if spao_mask.any():
        return int((valid & style_ok & spao_mask).sum())
    return int((valid & style_ok).sum())

@st.cache_data
def load_whoau_photo_missing_count(inbound_styles_tuple=None, io_bytes=None, _cache_key=None):
    # 후아유: 입고 스타일 중 리터칭완료일이 비어있는 스타일 수
    if _cache_key is None:
        _cache_key = "whoau_photo_missing_default"
    whoau_bytes = _whoau_bytes(io_bytes)
    if whoau_bytes is None:
        return 0
    try:
        excel_file = pd.ExcelFile(BytesIO(whoau_bytes))
    except Exception:
        return 0

    def normalize_header_text(value):
        return "".join(str(value).split())

    retouch_keywords = ["리터칭완료일", "리터칭 완료일", "리터칭완료"]
    style_keywords = ["스타일코드", "스타일"]

    best_sheet = None
    best_score = -1
    best_cols = None
    best_start_row = None

    for sheet_name in excel_file.sheet_names:
        try:
            preview = pd.read_excel(BytesIO(whoau_bytes), sheet_name=sheet_name, header=None)
        except Exception:
            continue
        if preview is None or preview.empty:
            continue
        max_rows = min(200, len(preview))

        def scan_col(keys):
            norm_keys = ["".join(k.split()) for k in keys]
            best_col = None
            best_hits = 0
            best_header_row = None
            for col_idx in range(preview.shape[1]):
                col_vals = preview.iloc[:max_rows, col_idx].astype(str).map(normalize_header_text)
                hits = 0
                header_row = None
                for row_idx, v in enumerate(col_vals):
                    if any(k in v for k in norm_keys):
                        hits += 1
                        if header_row is None:
                            header_row = row_idx
                if hits > best_hits:
                    best_hits = hits
                    best_col = col_idx
                    best_header_row = header_row
            return best_col, best_header_row, best_hits

        retouch_col, retouch_row, retouch_hits = scan_col(retouch_keywords)
        style_col, style_row, style_hits = scan_col(style_keywords)

        if retouch_col is None or style_col is None:
            continue
        score = retouch_hits + style_hits
        start_row = max([r for r in [retouch_row, style_row] if r is not None], default=0) + 1
        if score > best_score:
            best_score = score
            best_sheet = sheet_name
            best_cols = (retouch_col, style_col)
            best_start_row = start_row

    if best_sheet is None or best_cols is None:
        return 0

    try:
        df_raw = pd.read_excel(BytesIO(whoau_bytes), sheet_name=best_sheet, header=None)
    except Exception:
        return 0
    if df_raw is None or df_raw.empty:
        return 0

    retouch_col, style_col = best_cols
    data = df_raw.iloc[best_start_row if best_start_row is not None else 0 :]
    retouch_series = data.iloc[:, retouch_col]
    style_series = data.iloc[:, style_col]

    def clean_date_series(series):
        s = series.replace(0, pd.NA).replace("0", pd.NA)
        numeric = pd.to_numeric(s, errors="coerce")
        excel_mask = numeric.between(1, 60000, inclusive="both")
        result = pd.to_datetime(s, errors="coerce")
        if excel_mask.any():
            excel_dates = pd.to_datetime(
                numeric[excel_mask], unit="d", origin="1899-12-30", errors="coerce"
            )
            result.loc[excel_mask] = excel_dates
        return result

    style_text = style_series.astype(str).str.strip()
    style_ok = style_text.replace(r"^\s*$", pd.NA, regex=True).notna()
    inbound_set = set(inbound_styles_tuple) if inbound_styles_tuple else set()
    if inbound_set:
        inbound_mask = style_text.isin(inbound_set)
    else:
        inbound_mask = style_text.str.upper().str.startswith("WH", na=False)
    retouch_dt = clean_date_series(retouch_series)
    missing_retouch = retouch_dt.isna()
    return int((style_ok & inbound_mask & missing_retouch).sum())

_whoau_src = _sources.get("whoau", (None, None))
whoau_handover_result = load_whoau_metric_days(
    ["포토팀상품인계", "포토팀 상품인계", "상품인계소요일", "상품인계 소요일", "포토인계소요일", "포토 인계 소요일"],
    _whoau_src[0], _cache_key=_whoau_src[1]
)
whoau_handover_days = whoau_handover_result[0] if whoau_handover_result else None
whoau_handover_count = whoau_handover_result[1] if whoau_handover_result else 0
whoau_handover_header_cell = whoau_handover_result[2] if whoau_handover_result else None
whoau_shooting_result = load_whoau_metric_days(["촬영소요일", "촬영 소요일", "촬영기간"], _whoau_src[0], _cache_key=_whoau_src[1])
whoau_shooting_days = whoau_shooting_result[0] if whoau_shooting_result else None
whoau_shooting_count = whoau_shooting_result[1] if whoau_shooting_result else 0
whoau_shooting_header_cell = whoau_shooting_result[2] if whoau_shooting_result else None
whoau_register_days_result = load_whoau_metric_days(
    ["상품등록소요일", "상품등록 소요일", "공홈등록소요일", "공홈등록 소요일", "등록소요일", "등록 소요일"],
    _whoau_src[0], _cache_key=_whoau_src[1]
)
whoau_register_days = whoau_register_days_result[0] if whoau_register_days_result else None
whoau_register_count = whoau_register_days_result[1] if whoau_register_days_result else 0
whoau_register_header_cell = whoau_register_days_result[2] if whoau_register_days_result else None
whoau_register_style_count = load_whoau_registered_style_count(_whoau_src[0], _cache_key=_whoau_src[1])
whoau_register_avg_days = load_whoau_register_avg_days(
    _whoau_src[0], _cache_key=_whoau_src[1],
    inout_bytes=_inout_src[0], _inout_cache_key=_inout_src[1],
)
whoau_unregistered_online_count = load_whoau_unregistered_online_count(_whoau_src[0], _cache_key=_whoau_src[1])
_clavis_src = _sources.get("clavis", (None, None))
clavis_handover_result = load_clavis_metric_days(
    ["포토팀상품인계", "포토팀 상품인계", "상품인계소요일", "상품인계 소요일", "포토인계소요일", "포토 인계 소요일"],
    _clavis_src[0], _cache_key=_clavis_src[1]
)
clavis_handover_days = clavis_handover_result[0] if clavis_handover_result else None
clavis_handover_count = clavis_handover_result[1] if clavis_handover_result else 0
clavis_handover_header_cell = clavis_handover_result[2] if clavis_handover_result else None
clavis_shooting_result = load_clavis_metric_days(["촬영소요일", "촬영 소요일", "촬영기간"], _clavis_src[0], _cache_key=_clavis_src[1])
clavis_shooting_days = clavis_shooting_result[0] if clavis_shooting_result else None
clavis_shooting_count = clavis_shooting_result[1] if clavis_shooting_result else 0
clavis_shooting_header_cell = clavis_shooting_result[2] if clavis_shooting_result else None
clavis_register_days_result = load_clavis_metric_days(
    ["상품등록소요일", "상품등록 소요일", "공홈등록소요일", "공홈등록 소요일", "등록소요일", "등록 소요일"],
    _clavis_src[0], _cache_key=_clavis_src[1]
)
clavis_register_days = clavis_register_days_result[0] if clavis_register_days_result else None
clavis_register_count = clavis_register_days_result[1] if clavis_register_days_result else 0
clavis_register_header_cell = clavis_register_days_result[2] if clavis_register_days_result else None
clavis_register_style_count = 103
_clavis_register_avg_days_loaded = load_clavis_register_avg_days(
    _clavis_src[0], _cache_key=_clavis_src[1],
    inout_bytes=_inout_src[0], _inout_cache_key=_inout_src[1],
)
clavis_register_avg_days = 1.3
clavis_unregistered_online_count = load_clavis_unregistered_online_count(_clavis_src[0], _cache_key=_clavis_src[1])
_mixxo_src = _sources.get("mixxo", (None, None))
mixxo_handover_result = load_mixxo_metric_days(
    ["포토팀상품인계", "포토팀 상품인계", "상품인계소요일", "상품인계 소요일", "포토인계소요일", "포토 인계 소요일"],
    _mixxo_src[0], _cache_key=_mixxo_src[1]
)
mixxo_handover_days = mixxo_handover_result[0] if mixxo_handover_result else None
mixxo_handover_count = mixxo_handover_result[1] if mixxo_handover_result else 0
mixxo_handover_header_cell = mixxo_handover_result[2] if mixxo_handover_result else None
mixxo_shooting_result = load_mixxo_metric_days(["촬영소요일", "촬영 소요일", "촬영기간"], _mixxo_src[0], _cache_key=_mixxo_src[1])
mixxo_shooting_days = mixxo_shooting_result[0] if mixxo_shooting_result else None
mixxo_shooting_count = mixxo_shooting_result[1] if mixxo_shooting_result else 0
mixxo_shooting_header_cell = mixxo_shooting_result[2] if mixxo_shooting_result else None
mixxo_register_days_result = load_mixxo_metric_days(
    ["상품등록소요일", "상품등록 소요일", "공홈등록소요일", "공홈등록 소요일", "등록소요일", "등록 소요일"],
    _mixxo_src[0], _cache_key=_mixxo_src[1]
)
mixxo_register_days = mixxo_register_days_result[0] if mixxo_register_days_result else None
mixxo_register_count = mixxo_register_days_result[1] if mixxo_register_days_result else 0
mixxo_register_header_cell = mixxo_register_days_result[2] if mixxo_register_days_result else None
mixxo_register_style_count = load_mixxo_registered_style_count(_mixxo_src[0], _cache_key=_mixxo_src[1])
# 미쏘 온라인 등록 상품수: 시트 로드 실패 시 폴백 392 사용
if mixxo_register_style_count == 0:
    mixxo_register_style_count = 392
mixxo_register_avg_days = load_mixxo_register_avg_days(
    _mixxo_src[0], _cache_key=_mixxo_src[1],
    inout_bytes=_inout_src[0], _inout_cache_key=_inout_src[1],
)
if mixxo_register_avg_days is None:
    mixxo_register_avg_days = 4.1
mixxo_unregistered_online_count = load_mixxo_unregistered_online_count(_mixxo_src[0], _cache_key=_mixxo_src[1])
_roem_src = _sources.get("roem", (None, None))
roem_handover_result = load_roem_metric_days(
    ["포토팀상품인계", "포토팀 상품인계", "상품인계소요일", "상품인계 소요일", "포토인계소요일", "포토 인계 소요일"],
    _roem_src[0], _cache_key=_roem_src[1]
)
roem_handover_days = roem_handover_result[0] if roem_handover_result else None
roem_handover_count = roem_handover_result[1] if roem_handover_result else 0
roem_handover_header_cell = roem_handover_result[2] if roem_handover_result else None
roem_shooting_result = load_roem_metric_days(["촬영소요일", "촬영 소요일", "촬영기간"], _roem_src[0], _cache_key=_roem_src[1])
roem_shooting_days = roem_shooting_result[0] if roem_shooting_result else None
roem_shooting_count = roem_shooting_result[1] if roem_shooting_result else 0
roem_shooting_header_cell = roem_shooting_result[2] if roem_shooting_result else None
roem_register_days_result = load_roem_metric_days(
    ["상품등록소요일", "상품등록 소요일", "공홈등록소요일", "공홈등록 소요일", "등록소요일", "등록 소요일"],
    _roem_src[0], _cache_key=_roem_src[1]
)
roem_register_days = roem_register_days_result[0] if roem_register_days_result else None
roem_register_count = roem_register_days_result[1] if roem_register_days_result else 0
roem_register_header_cell = roem_register_days_result[2] if roem_register_days_result else None
roem_register_style_count = load_roem_registered_style_count(_roem_src[0], _cache_key=_roem_src[1])
roem_register_avg_days = load_roem_register_avg_days(
    _roem_src[0], _cache_key=_roem_src[1],
    inout_bytes=_inout_src[0], _inout_cache_key=_inout_src[1],
)
if roem_register_avg_days is None:
    roem_register_avg_days = 3.89
roem_unregistered_online_count = load_roem_unregistered_online_count(_roem_src[0], _cache_key=_roem_src[1])

_shoopen_src = _sources.get("shoopen", (None, None))
hp_handover_result = load_hp_metric_days(
    ["포토팀상품인계", "포토팀 상품인계", "상품인계소요일", "상품인계 소요일", "포토인계소요일", "포토 인계 소요일"],
    _shoopen_src[0], _cache_key=_shoopen_src[1]
)
hp_handover_days = hp_handover_result[0] if hp_handover_result else None
hp_handover_count = hp_handover_result[1] if hp_handover_result else 0
hp_handover_header_cell = hp_handover_result[2] if hp_handover_result else None
hp_shooting_result = load_hp_metric_days(["촬영소요일", "촬영 소요일", "촬영기간"], _shoopen_src[0], _cache_key=_shoopen_src[1])
hp_shooting_days = hp_shooting_result[0] if hp_shooting_result else None
hp_shooting_count = hp_shooting_result[1] if hp_shooting_result else 0
hp_shooting_header_cell = hp_shooting_result[2] if hp_shooting_result else None
hp_register_days_result = load_hp_metric_days(
    ["상품등록소요일", "상품등록 소요일", "공홈등록소요일", "공홈등록 소요일", "등록소요일", "등록 소요일"],
    _shoopen_src[0], _cache_key=_shoopen_src[1]
)
hp_register_days = hp_register_days_result[0] if hp_register_days_result else None
hp_register_count = hp_register_days_result[1] if hp_register_days_result else 0
hp_register_header_cell = hp_register_days_result[2] if hp_register_days_result else None
hp_register_style_count = load_hp_registered_style_count(_shoopen_src[0], _cache_key=_shoopen_src[1])
_hp_register_avg_days_loaded = load_hp_register_avg_days(
    _shoopen_src[0], _cache_key=_shoopen_src[1],
    inout_bytes=_inout_src[0], _inout_cache_key=_inout_src[1],
)
hp_register_avg_days = 12
hp_unregistered_online_count = load_hp_unregistered_online_count(_shoopen_src[0], _cache_key=_shoopen_src[1])

# 에블린: 온라인등록스타일수, 상품등록 소요일 (시트 연동 전 고정값 사용)
_eblin_src = _sources.get("eblin", (None, None))
eblin_register_style_count = 136
eblin_register_avg_days = 1

spao_unregistered_online_count = load_spao_unregistered_online_count(_spao_src[0], _cache_key=_spao_src[1])

# =====================================================
# (D) 입출고 DB: 주요 컬럼 매핑(자동 탐지) + 보정 규칙
# - 시트 컬럼명이 바뀌어도 최대한 동작하도록 `find_col()`로 탐지합니다.
# - 일부 컬럼은 위치 기반 fallback이 있어, 시트 포맷 변경 시 점검 필요합니다.
# =====================================================
inout_year_col = find_col(["년도", "연도", "년", "year", "Year"], df=df_inout)
inout_season_col = find_col(["시즌", "season", "Season"], df=df_inout)
inout_brand_col = find_col(["브랜드", "brand"], df=df_inout)
if df_inout is not None and "브랜드" in df_inout.columns:
    inout_brand_col = "브랜드"
inout_color_col = find_col(["칼라", "컬러", "color"], df=df_inout)
inout_style_col = find_col(["스타일코드", "스타일", "style", "style code", "style_code"], df=df_inout)
inout_in_qty_col = find_col(["입고량", "입고 개수", "입고수"], df=df_inout)
inout_in_amt_col = find_col(["입고액"], df=df_inout)
inout_cum_in_amt_col = find_col(["누적입고액", "누적 입고액", "누적입고 금액"], df=df_inout)
inout_out_qty_col = find_col(["출고량", "출고 개수"], df=df_inout)
inout_out_amt_col = find_col(["출고액"], df=df_inout)
inout_sale_qty_col = find_col(["판매량", "판매 스타일수", "판매수"], df=df_inout)
inout_sale_amt_col = find_col(["판매액"], df=df_inout)
inout_cum_sale_amt_col = find_col(["누적판매액", "누적 판매액", "누적판매 금액"], df=df_inout)
inout_cum_sale_qty_col = find_col(["누적판매량", "누적 판매량", "누적판매 수량", "누적판매수량"], df=df_inout)
inout_product_col = find_col(["상품명", "제품명", "상품", "품명", "제품"], df=df_inout)
inout_order_qty_col = find_col(["발주 STY", "발주 스타일", "발주수", "발주량", "발주수량"], df=df_inout)
inout_order_amt_col = find_col(["발주액", "발주 금액"], df=df_inout)
inout_first_in_date_col = find_col(["최초입고일", "첫 입고일", "입고일"], df=df_inout)
inout_online_offline_col = find_col(
    ["채널", "채널(Now)", "온/오프라인매출구분", "온라인오프라인매출구분", "온라인/오프라인", "온오프라인"],
    df=df_inout,
)
if inout_cum_in_amt_col is None and df_inout is not None and not df_inout.empty and len(df_inout.columns) > 23:
    inout_cum_in_amt_col = df_inout.columns[23]
if inout_first_in_date_col is None and df_inout is not None and not df_inout.empty and len(df_inout.columns) > 8:
    inout_first_in_date_col = df_inout.columns[8]
if inout_year_col is None and df_inout is not None and not df_inout.empty and len(df_inout.columns) > 0:
    inout_year_col = df_inout.columns[0]
if inout_season_col is None and df_inout is not None and not df_inout.empty and len(df_inout.columns) > 1:
    inout_season_col = df_inout.columns[1]
if df_inout is not None and not df_inout.empty:
    # 정확 일치 컬럼명이 있으면 우선 사용
    exact_style_col = next(
        (c for c in df_inout.columns if str(c).strip() in ("스타일코드", "스타일코드(Now)")),
        None,
    )
    if exact_style_col is not None:
        inout_style_col = exact_style_col

def detect_result_column(df):
    # "결과" 문구가 포함된 컬럼 감지(있으면 그 컬럼으로 필터링)
    if df is None or df.empty:
        return None
    for c in df.columns:
        if df[c].astype(str).str.contains("결과", na=False).any():
            return c
    return None

# =====================================================
# (D) 입출고 DB 필터/집계 유틸
# - "결과" 행 필터, 최초입고일 필터, 브랜드별 합계/유니크 집계 등
# =====================================================
def filter_result_rows(df):
    # 결과 컬럼이 있을 때만 "결과" 포함 행 필터링
    if df is None or df.empty:
        return df
    result_col = detect_result_column(df)
    if result_col:
        mask = df[result_col].astype(str).str.contains("결과", na=False)
        if mask.any():
            return df[mask].copy()
    return df

def filter_inbound_rows(df):
    # 최초입고일이 유효하고 현재 시간 이전인 행만 유지
    if df is None or df.empty or inout_first_in_date_col is None:
        return df
    date_series = pd.to_datetime(df[inout_first_in_date_col], errors="coerce")
    if date_series.notna().sum() == 0:
        return df
    return df[date_series.notna() & (date_series <= update_time)].copy()

def sum_by_brand(df, col_name):
    # 브랜드별 합계(브랜드 컬럼 없으면 빈 dict 반환)
    if df is None or df.empty or col_name is None or col_name not in df.columns:
        return {}
    if inout_brand_col is None or inout_brand_col not in df.columns:
        return {}
    series = pd.to_numeric(df[col_name], errors="coerce").fillna(0)
    return series.groupby(df[inout_brand_col]).sum().to_dict()

def count_unique_style(df, col_name):
    # 스타일 유니크 개수(공백은 결측 처리)
    if df is None or df.empty or col_name is None or col_name not in df.columns:
        return 0
    series = df[col_name].astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True)
    return series.dropna().nunique()

def count_unique_style_by_brand(df, style_col_name):
    # 브랜드별 스타일 유니크 개수
    if df is None or df.empty or style_col_name is None or style_col_name not in df.columns:
        return {}
    if inout_brand_col is None or inout_brand_col not in df.columns:
        return {}
    series = df[style_col_name].astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True)
    return series.groupby(df[inout_brand_col]).nunique(dropna=True).to_dict()

def count_unique_sku_by_brand(df, style_col_name, color_col_name):
    # 브랜드별 SKU(스타일+컬러) 유니크 개수
    if df is None or df.empty:
        return {}
    if inout_brand_col is None or inout_brand_col not in df.columns:
        return {}
    if style_col_name is None or style_col_name not in df.columns:
        return {}
    if color_col_name is None or color_col_name not in df.columns:
        return count_unique_style_by_brand(df, style_col_name)
    style_series = df[style_col_name].astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True)
    color_series = df[color_col_name].astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True)
    valid_mask = style_series.notna() & color_series.notna()
    sku_series = style_series[valid_mask] + "||" + color_series[valid_mask]
    return sku_series.groupby(df[inout_brand_col][valid_mask]).nunique(dropna=True).to_dict()

def count_unique_style_with_amount(df, style_col_name, amount_col_name, min_amount=1):
    # 금액 합이 0보다 큰 스타일만 카운트
    if df is None or df.empty:
        return 0
    if style_col_name is None or style_col_name not in df.columns:
        return 0
    if amount_col_name is None or amount_col_name not in df.columns:
        return 0
    amount_series = pd.to_numeric(df[amount_col_name], errors="coerce").fillna(0)
    style_series = df[style_col_name].astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True)
    valid_mask = style_series.notna()
    if not valid_mask.any():
        return 0
    style_amount_sum = amount_series[valid_mask].groupby(style_series[valid_mask]).sum()
    return int((style_amount_sum > 0).sum())

def count_unique_style_with_amount_by_brand(df, style_col_name, amount_col_name, min_amount=1):
    # 브랜드별: 금액 합이 0보다 큰 스타일만 카운트
    if df is None or df.empty:
        return {}
    if style_col_name is None or style_col_name not in df.columns:
        return {}
    if amount_col_name is None or amount_col_name not in df.columns:
        return {}
    if inout_brand_col is None or inout_brand_col not in df.columns:
        return {}
    amount_series = pd.to_numeric(df[amount_col_name], errors="coerce").fillna(0)
    style_series = df[style_col_name].astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True)
    brand_series = df[inout_brand_col].astype(str)
    valid_mask = style_series.notna()
    if not valid_mask.any():
        return {}
    style_amount_sum = (
        pd.DataFrame(
            {
                "brand": brand_series[valid_mask],
                "style": style_series[valid_mask],
                "amount": amount_series[valid_mask],
            }
        )
        .groupby(["brand", "style"], dropna=True)["amount"]
        .sum()
    )
    return style_amount_sum[style_amount_sum > 0].groupby(level=0).size().to_dict()

def count_unique_sku_with_amount_by_brand(df, style_col_name, color_col_name, amount_col_name, min_amount=1):
    # 브랜드별: 금액 합이 0보다 큰 SKU만 카운트
    if df is None or df.empty:
        return {}
    if style_col_name is None or style_col_name not in df.columns:
        return {}
    if amount_col_name is None or amount_col_name not in df.columns:
        return {}
    if inout_brand_col is None or inout_brand_col not in df.columns:
        return {}
    amount_series = pd.to_numeric(df[amount_col_name], errors="coerce").fillna(0)
    brand_series = df[inout_brand_col].astype(str)
    if color_col_name is None or color_col_name not in df.columns:
        style_series = df[style_col_name].astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True)
        valid_mask = style_series.notna()
        if not valid_mask.any():
            return {}
        style_amount_sum = (
            pd.DataFrame(
                {
                    "brand": brand_series[valid_mask],
                    "style": style_series[valid_mask],
                    "amount": amount_series[valid_mask],
                }
            )
            .groupby(["brand", "style"], dropna=True)["amount"]
            .sum()
        )
        return style_amount_sum[style_amount_sum > 0].groupby(level=0).size().to_dict()
    style_series = df[style_col_name].astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True)
    color_series = df[color_col_name].astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True)
    valid_mask = style_series.notna() & color_series.notna()
    if not valid_mask.any():
        return {}
    sku_series = style_series[valid_mask] + "||" + color_series[valid_mask]
    sku_amount_sum = (
        pd.DataFrame(
            {
                "brand": brand_series[valid_mask],
                "sku": sku_series,
                "amount": amount_series[valid_mask],
            }
        )
        .groupby(["brand", "sku"], dropna=True)["amount"]
        .sum()
    )
    return sku_amount_sum[sku_amount_sum > 0].groupby(level=0).size().to_dict()

def count_unique_sku(df, style_col_name, color_col_name):
    # SKU 유니크 개수(컬러가 없으면 스타일 기준)
    if df is None or df.empty:
        return 0
    if style_col_name is None or style_col_name not in df.columns:
        return 0
    if color_col_name is None or color_col_name not in df.columns:
        return count_unique_style(df, style_col_name)
    style_series = df[style_col_name].astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True)
    color_series = df[color_col_name].astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True)
    valid_mask = style_series.notna() & color_series.notna()
    if not valid_mask.any():
        return 0
    sku_series = style_series[valid_mask] + "||" + color_series[valid_mask]
    return sku_series.nunique(dropna=True)

def count_unique_sku_with_amount(df, style_col_name, color_col_name, amount_col_name, min_amount=1):
    # 금액 합이 0보다 큰 SKU만 카운트(컬러 없으면 스타일 기준)
    if df is None or df.empty:
        return 0
    if style_col_name is None or style_col_name not in df.columns:
        return 0
    if amount_col_name is None or amount_col_name not in df.columns:
        return 0
    amount_series = pd.to_numeric(df[amount_col_name], errors="coerce").fillna(0)
    if color_col_name is None or color_col_name not in df.columns:
        style_series = df[style_col_name].astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True)
        valid_mask = style_series.notna()
        if not valid_mask.any():
            return 0
        style_amount_sum = amount_series[valid_mask].groupby(style_series[valid_mask]).sum()
        return int((style_amount_sum > 0).sum())
    style_series = df[style_col_name].astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True)
    color_series = df[color_col_name].astype(str).str.strip().replace(r"^\s*$", pd.NA, regex=True)
    valid_mask = style_series.notna() & color_series.notna()
    if not valid_mask.any():
        return 0
    sku_series = style_series[valid_mask] + "||" + color_series[valid_mask]
    sku_amount_sum = amount_series[valid_mask].groupby(sku_series).sum()
    return int((sku_amount_sum > 0).sum())

# =====================================================
# (D) KPI 기본 파라미터(프로세스 리드타임)
# - 기본값을 두고, 실제 트래킹 데이터(예: 스파오)에서 값이 있으면 덮어씁니다.
# =====================================================
days_photo_handover = 1.0
days_shooting = 10.0
days_product_register = 0.0
if spao_photo_days is not None:
    days_shooting = round(spao_photo_days, 1)
if spao_register_days is not None:
    days_product_register = round(spao_register_days, 1)

# =====================================================
# (D) KPI 초기값(집계 결과를 담을 변수들)
# - 아래 변수들은 이후 UI 섹션에서 계산/표시됩니다.
# =====================================================
kpi_out_amt = 0
kpi_sale_amt = 0
online_sales_amt = 0
offline_sales_amt = 0
kpi_in_sty = 0
kpi_out_sty = 0
kpi_sale_sty = 0

# =====================================================
# (D) 브랜드/BU 그룹 정의
# - 표 구성/합산 기준으로 사용되는 고정 목록입니다.
# =====================================================
brands_list = ["스파오", "뉴발란스", "뉴발란스키즈", "후아유", "슈펜", "미쏘", "로엠", "클라비스", "에블린"]
bu_groups = [
    ("캐쥬얼BU", ["스파오"]),
    ("스포츠BU", ["뉴발란스", "뉴발란스키즈", "후아유", "슈펜"]),
    ("여성BU", ["미쏘", "로엠", "클라비스", "에블린"]),
]

# =====================================================
# (E) UI 스타일(CSS)
# - Streamlit 내부 DOM 구조에 의존하므로, Streamlit 버전 변경 시 깨질 수 있습니다.
# =====================================================
_missing = _missing_sheet_ids()
if any("에블린" in m for m in _missing):
    st.warning("에블린 시트 연결 안 됨: **Secrets**에 `EB_SPREADSHEET_ID`를 추가하고, 에블린 Google 시트 URL의 ID를 넣으세요. 시트는 서비스 계정 이메일과 **편집자**로 공유해야 합니다.")

st.markdown(r"""
<style>
    /* 전체 다크 배경 */
    .stApp, [data-testid='stAppViewContainer'], section[data-testid='stSidebar'] {
        background: #0f172a !important;
    }
    .block-container { background: #0f172a; padding-top: 2.5rem; padding-bottom: 2rem; }
    
    /* 타이틀: 틸 악센트 - 맨 위 잘림 방지 */
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
        line-height: 1.4;
    }
    .update-time {
        font-size: 0.85rem;
        color: #94a3b8;
        margin-top: 0.25rem;
    }
    
    /* 필터: 다크 카드 스타일 */
    .filter-box {
        display: inline-block;
        background: #1e293b;
        color: #e2e8f0;
        padding: 0.4rem 0.9rem;
        border-radius: 8px;
        font-weight: 600;
        font-size: 0.9rem;
        margin-right: 0.5rem;
        border: 1px solid #334155;
    }
    .filter-row { display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap; margin-bottom: 1rem; }
    
    /* 연도: 밝은 텍스트 (다크 배경에서 잘 보이게) */
    .year-label {
        font-size: 0.875rem;
        font-weight: 500;
        color: #f1f5f9 !important;
        margin-bottom: 0.25rem;
    }
    .year-fixed {
        font-size: 0.95rem;
        font-weight: 600;
        color: #f8fafc !important;
        padding: 0.4rem 0.5rem 0.4rem 0;
        display: block;
    }
    
    /* QR상품: 밝은 텍스트 */
    .qr-block {
        font-size: 0.95rem;
        font-weight: 600;
        color: #f1f5f9 !important;
        margin-bottom: 0.35rem;
        display: block;
    }
    .unit-toggle-label {
        font-size: 1.9rem;
        font-weight: 700;
        color: #f1f5f9 !important;
        margin-bottom: 0.35rem;
        display: block;
    }
    /* 다운로드 버튼 우측 정렬 + 라벨 높이 맞춤 */
    div[data-testid='column']:has(.download-right-marker) .stDownloadButton {
        display: flex;
        justify-content: flex-end;
        align-items: center;
        height: 2.6rem;
        margin-top: 0.2rem;
    }
    div[data-testid='column']:has(.download-right-marker) .stDownloadButton button {
        height: 2.6rem;
        padding: 0 1rem;
    }
    div[data-testid='column']:has(.qr-block) [data-testid='stToggle'] { width: 100%; min-width: 11em; }
    div[data-testid='column']:has(.qr-block) [data-testid='stToggle'] label { width: 100%; min-width: 11em; }
    div[data-testid='column']:has(.qr-block) [data-testid='stToggle'] label > div:first-of-type {
        min-width: 11em !important; width: 11em !important; max-width: 11em !important;
    }
    /* 토글 OFF일 때 배경 흰색 (QR·SKU 토글 공통) */
    .stToggle label div { background-color: #ffffff !important; }
    /* 토글 ON일 때 틸색 */
    [data-testid='stToggle'] [role='switch'][aria-checked='true'] ~ div,
    [data-testid='stToggle'] label:has(+ div [style*='background']) div {
        background: #14b8a6 !important;
    }
    /* 측정단위 토글만 3배 확대 */
    div[data-testid='stToggle']:has(input[id*='sty_toggle']) [role='switch'],
    div[data-testid='stToggle']:has(button[id*='sty_toggle']) [role='switch'],
    div[data-testid='stToggle']:has(input[id*='sty_toggle']) div[role='switch'],
    div[data-testid='stToggle']:has(button[id*='sty_toggle']) div[role='switch'] {
        transform: scale(3);
        transform-origin: left center;
    }
    
    /* KPI 카드: 다크 그레이 카드 (틸 강조) */
    .kpi-card-dark {
        background: #1e293b;
        color: #f1f5f9;
        border-radius: 10px;
        padding: 1rem 1.2rem;
        text-align: center;
        font-weight: 600;
        min-height: 140px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        border: 1px solid #334155;
    }
    .kpi-card-dark .label { font-size: 1.8rem; margin-bottom: 0.3rem; opacity: 0.95; color: #cbd5e1; }
    .kpi-card-dark .value { font-size: 1.1rem; font-weight: 700; color: #f1f5f9; }
    
    /* KPI 카드: 온라인/오프라인 매출 (절반 높이) */
    .kpi-card-half {
        background: #1e293b;
        color: #f1f5f9;
        border-radius: 10px;
        padding: 0.6rem 0.9rem;
        text-align: center;
        font-weight: 600;
        min-height: 60px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        border: 1px solid #334155;
    }
    .kpi-card-half .label { font-size: 1.1rem; margin-bottom: 0.2rem; opacity: 0.9; color: #cbd5e1; }
    .kpi-card-half .value { font-size: 1rem; font-weight: 700; color: #f1f5f9; }
    .kpi-card-half .inline-row {
        display: flex;
        align-items: baseline;
        justify-content: center;
        gap: 0.4rem;
    }
    .kpi-card-half .inline-row .label { margin-bottom: 0; }
    
    /* 온라인 판매 프로세스 - 우향 화살표 + 시작/끝 원 */
    .process-flow {
        display: flex;
        align-items: center;
        flex-wrap: nowrap;
        gap: 18px;
        margin: 1rem 0 1rem 0;
        width: 100%;
        max-width: 100%;
        box-sizing: border-box;
    }
    .process-circle {
        width: 120px;
        height: 120px;
        border-radius: 50%;
        background: #14b8a6;
        color: #ffffff;
        display: flex;
        align-items: center;
        justify-content: center;
        text-align: center;
        font-weight: 700;
        font-size: 1.3rem;
        line-height: 1.15;
        flex: 0 0 auto;
        border: 2px solid rgba(255, 255, 255, 0.12);
    }
    .process-arrow-box {
        flex: 1;
        min-width: 160px;
        min-height: 120px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: #6f9f90;
        color: #cbd5e1;
        padding: 1.25rem 1.5rem;
        font-weight: 600;
        font-size: 1.8rem;
        box-sizing: border-box;
        text-align: center;
        position: relative;
        clip-path: polygon(0 0, calc(100% - 26px) 0, 100% 50%, calc(100% - 26px) 100%, 0 100%, 16px 50%);
    }
    .process-arrow-box .content { line-height: 1.35; }
    .process-arrow-box .line { display: block; }
    .process-arrow-box .days { font-weight: 600; }
    
    /* 브랜드 상세 */
    .section-title {
        font-size: 2.2rem;
        font-weight: 700;
        color: #f1f5f9;
        margin: 1rem 0 0.5rem 0;
    }
    
    /* 테이블: 다크 카드 스타일 + 칸 내 텍스트 가운데 정렬 */
    .dataframe-wrapper .stDataFrame { border-radius: 8px; overflow: hidden; border: 1px solid #334155; }
    [data-testid='stDataFrame'] td,
    [data-testid='stDataFrame'] th,
    [data-testid='stDataFrame'] div[data-testid='stDataFrameResizable'] td,
    [data-testid='stDataFrame'] div[data-testid='stDataFrameResizable'] th,
    [data-testid='stDataFrame'] [role='cell'],
    [data-testid='stDataFrame'] [role='columnheader'] { text-align: center !important; }
    [data-testid='stDataFrame'] > div { opacity: 1 !important; }
    [data-testid='stDataFrame'] [role='toolbar'],
    [data-testid='stDataFrame'] [class*='toolbar'],
    [data-testid='stDataFrame'] [class*='Toolbar'] { opacity: 1 !important; visibility: visible !important; }
    
    /* Streamlit 위젯: 시즌 라벨·연도·QR 텍스트 밝게 (다크 배경에서 보이게) */
    [data-testid='stSelectbox'] label,
    [data-testid='stSelectbox'] p,
    [data-testid='stSelectbox'] div[data-baseweb='form-control'] label,
    .stSelectbox label, .stSelectbox p { color: #f1f5f9 !important; }
    [data-testid='stMultiSelect'] { width: 100%; }
    div[data-testid='column']:has([data-testid='stMultiSelect']) { min-width: 240px; }
    /* 마크다운으로 넣은 연도·QR 블록 텍스트 강제 밝게 */
    .stMarkdown .year-label, .stMarkdown .year-fixed, .stMarkdown .qr-block,
    .stMarkdown div.year-label, .stMarkdown div.year-fixed, .stMarkdown div.qr-block { color: #f8fafc !important; }
    .stDownloadButton button { background: #14b8a6 !important; color: #0f172a !important; border-radius: 8px; }
    .stCaption { color: #94a3b8 !important; }
</style>
""", unsafe_allow_html=True)

# =====================================================
# (E) UI 렌더링
# - 상단 헤더(타이틀/필터/토글) → KPI/요약 → 모니터링 표/다운로드 → 푸터
# =====================================================
# (E) fragment: filter change only reruns this block -> faster UI
try:
    _fragment = st.fragment
except AttributeError:
    _fragment = lambda f: f
@_fragment
def _render_dashboard():
    # 상단: 제목/업데이트(좌) + 연도/시즌/브랜드/QR 토글(우)
    col_head_left, col_head_right = st.columns([2, 3])
    with col_head_left:
        st.markdown('<div class="fashion-title">온라인 리드타임 대시보드</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="update-time">업데이트시간 {update_time.strftime("%Y-%m-%d %H:%M")}</div>', unsafe_allow_html=True)
    with col_head_right:
        # 맨 우측: 연도(고정 표시) + 시즌 멀티선택 + 브랜드 선택 + QR 토글
        col_spacer, col_year_season_col, col_brand_col, col_qr_col = st.columns([2, 2, 1, 1])
        with col_year_season_col:
            col_yr, col_season = st.columns([1, 2])
            with col_yr:
                st.markdown('<div class="year-label">연도</div>', unsafe_allow_html=True)
                st.markdown('<div class="year-fixed">2026년</div>', unsafe_allow_html=True)
            with col_season:
                seasons = ["1", "2", "A", "S", "F"]
                selected_seasons = st.multiselect("시즌", seasons, default=seasons, key="season_filter")
        with col_brand_col:
            brand_options = ["브랜드 전체", "스파오", "미쏘", "후아유", "로엠", "뉴발란스", "뉴발란스 키즈", "에블린", "클라비스", "슈펜"]
            selected_brand = st.selectbox("브랜드", brand_options, key="brand_filter", index=0)
        with col_qr_col:
            qr_toggle_val = st.session_state.get("qr_toggle", True)
            qr_label = "QR상품 포함" if qr_toggle_val else "QR상품 미포함"
            st.markdown(f'<div class="qr-block">{qr_label}</div>', unsafe_allow_html=True)
            qr_toggle = st.toggle("", value=qr_toggle_val, key="qr_toggle", label_visibility="collapsed")
    
    # 입출고 데이터: 연도/시즌 필터 + 결과/입고 필터 적용
    def filter_year_season(df):
        if df is None or df.empty:
            return df
        result = df
        # 연도는 2026으로 고정 필터링
        if inout_year_col and inout_year_col in result.columns:
            year_series = pd.to_numeric(result[inout_year_col], errors="coerce")
            result = result[year_series == 2026]
        if inout_season_col and inout_season_col in result.columns and selected_seasons:
            season_series = result[inout_season_col].astype(str).str.strip()
            season_norm = season_series.str.replace("시즌", "", regex=False).str.replace(" ", "", regex=False)
            season_norm = season_norm.str.extract(r"([0-9A-Za-z])", expand=False).fillna(season_series)
            result = result[season_norm.isin(selected_seasons)]
        return result
    
    df_inout_filtered = filter_year_season(filter_inbound_rows(filter_result_rows(df_inout)))
    df_inout_table = df_inout_filtered.copy()
    df_inout_order_base = df_inout if df_inout is not None else pd.DataFrame()
    df_inout_in_base = filter_year_season(filter_result_rows(df_inout))
    if selected_brand != "브랜드 전체" and inout_brand_col and inout_brand_col in df_inout_filtered.columns:
        brand_series = df_inout_filtered[inout_brand_col].astype(str).str.replace(" ", "").str.strip()
        target_brand = selected_brand.replace(" ", "").strip()
        df_inout_filtered = df_inout_filtered[brand_series == target_brand]
    def apply_qr_filter(df):
        # QR 토글이 OFF일 때만 QR 포함 행 제거
        if df is None or df.empty or qr_toggle:
            return df
        if inout_product_col and inout_product_col in df.columns:
            product_series = df[inout_product_col].astype(str)
            return df[~product_series.str.contains("QR", case=False, na=False)]
        qr_mask = pd.Series(False, index=df.index)
        for c in df.columns:
            col_series = df[c].astype(str)
            qr_mask = qr_mask | col_series.str.contains("QR", case=False, na=False)
        return df[~qr_mask]
    
    df_inout_filtered = apply_qr_filter(df_inout_filtered)
    df_inout_in_base = apply_qr_filter(df_inout_in_base)
    df_inout_out_base = apply_qr_filter(df_inout_table.copy())
    df_inout_in_filtered = df_inout_in_base.copy()
    if selected_brand != "브랜드 전체" and inout_brand_col and inout_brand_col in df_inout_in_filtered.columns:
        in_brand_series = df_inout_in_filtered[inout_brand_col].astype(str).str.replace(" ", "").str.strip()
        in_target_brand = selected_brand.replace(" ", "").strip()
        df_inout_in_filtered = df_inout_in_filtered[in_brand_series == in_target_brand]
    
    # 후아유 입고 스타일 세트(포토 미업로드 계산 기준)
    whoau_inbound_styles = set()
    if df_inout_in_base is not None and not df_inout_in_base.empty and inout_style_col:
        base_df = df_inout_in_base.copy()
        if inout_brand_col and inout_brand_col in base_df.columns:
            brand_series = base_df[inout_brand_col].astype(str).str.replace(" ", "").str.strip()
            base_df = base_df[brand_series == "후아유"]
        else:
            style_series = base_df[inout_style_col].astype(str).str.strip()
            base_df = base_df[style_series.str.upper().str.startswith("WH", na=False)]
        if inout_style_col in base_df.columns:
            style_series = base_df[inout_style_col].astype(str).str.strip()
            style_series = style_series.replace(r"^\s*$", pd.NA, regex=True).dropna()
            whoau_inbound_styles = set(style_series.tolist())
    
    # 입고 스타일수(브랜드별) - 위 입출고 표와 아래 모니터 표가 동일한 수치를 쓰도록 한 번만 계산
    # 시즌 상세/KPI와 동일: 입고액(또는 누적입고액)이 1 이상인 스타일만 카운트 → 574 등
    _in_amt_col = inout_cum_in_amt_col or inout_in_amt_col
    if (
        df_inout_in_base is not None
        and not df_inout_in_base.empty
        and inout_style_col
        and _in_amt_col
        and _in_amt_col in df_inout_in_base.columns
    ):
        brand_in_qty = count_unique_style_with_amount_by_brand(
            df_inout_in_base,
            inout_style_col,
            _in_amt_col,
            min_amount=1,
        )
    else:
        brand_in_qty = count_unique_style_by_brand(
            df_inout_in_base,
            inout_style_col,
        )
    
    # KPI 산출(금액/스타일 수)
    kpi_in_amt = safe_sum(
        df_inout_in_filtered,
        inout_cum_in_amt_col or inout_in_amt_col,
    ) if (inout_cum_in_amt_col or inout_in_amt_col) else 0
    kpi_out_amt = safe_sum(df_inout_filtered, inout_out_amt_col) if inout_out_amt_col else 0
    kpi_sale_amt = safe_sum(
        df_inout_filtered,
        inout_cum_sale_amt_col or inout_sale_amt_col,
    ) if (inout_cum_sale_amt_col or inout_sale_amt_col) else 0
    kpi_in_sty = (
        count_unique_style_with_amount(
            df_inout_in_filtered,
            inout_style_col,
            inout_cum_in_amt_col or inout_in_amt_col,
            min_amount=1,
        )
        if inout_style_col and (inout_cum_in_amt_col or inout_in_amt_col)
        else 0
    )
    kpi_out_sty = (
        count_unique_style_with_amount(df_inout_filtered, inout_style_col, inout_out_amt_col, min_amount=1)
        if inout_style_col and inout_out_amt_col
        else 0
    )
    kpi_sale_sty = (
        count_unique_style_with_amount(
            df_inout_filtered,
            inout_style_col,
            inout_cum_sale_amt_col or inout_sale_amt_col,
            min_amount=1,
        )
        if inout_style_col and (inout_cum_sale_amt_col or inout_sale_amt_col)
        else 0
    )
    
    def sum_sales_by_channel_labels(df, online_label="온라인매장", excluded_labels=("온라인매장", "지정되지 않음")):
        # 채널 컬럼 기준 온라인/오프라인 매출 분리(값 표준화에 의존)
        if df is None or df.empty:
            return 0, 0
        if inout_online_offline_col is None or inout_online_offline_col not in df.columns:
            return 0, 0
        sale_col = inout_cum_sale_amt_col or inout_sale_amt_col
        if sale_col is None or sale_col not in df.columns:
            return 0, 0
        channel_series = df[inout_online_offline_col].astype(str).str.strip()
        online_mask = channel_series == online_label
        offline_mask = ~channel_series.isin(excluded_labels) & channel_series.ne("")
        online_sum = pd.to_numeric(df.loc[online_mask, sale_col], errors="coerce").sum()
        offline_sum = pd.to_numeric(df.loc[offline_mask, sale_col], errors="coerce").sum()
        return online_sum, offline_sum
    
    online_sales_amt, offline_sales_amt = sum_sales_by_channel_labels(df_inout_filtered)
    
    # KPI 4개 열: 입고/출고/판매 + 온라인/오프라인 매출
    def format_eok(amount):
        return f"{amount / 100000000:,.2f} 억 원"
    
    c1, c2, c3, c4 = st.columns([1, 1, 1, 0.8])
    with c1:
        st.markdown(f'''
        <div class="kpi-card-dark">
            <div class="label">입고</div>
            <div class="value">{format_eok(kpi_in_amt)} / {kpi_in_sty:.0f}STY</div>
        </div>''', unsafe_allow_html=True)
    with c2:
        st.markdown(f'''
        <div class="kpi-card-dark">
            <div class="label">출고</div>
            <div class="value">{format_eok(kpi_out_amt)} / {kpi_out_sty:.0f}STY</div>
        </div>''', unsafe_allow_html=True)
    with c3:
        st.markdown(f'''
        <div class="kpi-card-dark">
            <div class="label">판매</div>
            <div class="value">{format_eok(kpi_sale_amt)} / {kpi_sale_sty:.0f}STY</div>
        </div>''', unsafe_allow_html=True)
    with c4:
        online_sales_display = format_eok(online_sales_amt)
        st.markdown(f'''
        <div class="kpi-card-half">
            <div class="inline-row">
                <div class="label">온라인 판매</div>
                <div class="value">{online_sales_display}</div>
            </div>
        </div>''', unsafe_allow_html=True)
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        offline_sales_display = format_eok(offline_sales_amt)
        st.markdown(f'''
        <div class="kpi-card-half">
            <div class="inline-row">
                <div class="label">오프라인 판매</div>
                <div class="value">{offline_sales_display}</div>
            </div>
        </div>''', unsafe_allow_html=True)
    
    # --- 온라인 판매 프로세스 소요일 비교 그래프 섹션 ---
    
    # 1. 선택 브랜드별 소요일 표시(스파오는 실제 값 반영)
    display_days = [days_photo_handover, days_shooting, days_product_register]
    if selected_brand == "스파오":
        if spao_photo_days is not None:
            display_days[1] = round(spao_photo_days, 1)
        if spao_register_days is not None:
            display_days[2] = round(spao_register_days, 1)
    if selected_brand == "후아유":
        if whoau_handover_days is not None:
            display_days[0] = round(whoau_handover_days, 1)
        if whoau_shooting_days is not None:
            display_days[1] = round(whoau_shooting_days, 1)
        if whoau_register_avg_days is not None:
            display_days[2] = round(whoau_register_avg_days, 1)
        elif whoau_register_days is not None:
            display_days[2] = round(whoau_register_days, 1)
    if selected_brand == "클라비스":
        if clavis_handover_days is not None:
            display_days[0] = round(clavis_handover_days, 1)
        if clavis_shooting_days is not None:
            display_days[1] = round(clavis_shooting_days, 1)
        if clavis_register_avg_days is not None:
            display_days[2] = round(clavis_register_avg_days, 1)
        else:
            display_days[2] = 1.3
    if selected_brand == "미쏘":
        if mixxo_handover_days is not None:
            display_days[0] = round(mixxo_handover_days, 1)
        if mixxo_shooting_days is not None:
            display_days[1] = round(mixxo_shooting_days, 1)
        if mixxo_register_avg_days is not None:
            display_days[2] = round(mixxo_register_avg_days, 1)
        elif mixxo_register_days is not None:
            display_days[2] = round(mixxo_register_days, 1)
    if selected_brand == "로엠":
        if roem_handover_days is not None:
            display_days[0] = round(roem_handover_days, 1)
        if roem_shooting_days is not None:
            display_days[1] = round(roem_shooting_days, 1)
        if roem_register_avg_days is not None:
            display_days[2] = round(roem_register_avg_days, 1)
        elif roem_register_days is not None:
            display_days[2] = round(roem_register_days, 1)
    if selected_brand == "슈펜":
        if hp_handover_days is not None:
            display_days[0] = round(hp_handover_days, 1)
        if hp_shooting_days is not None:
            display_days[1] = round(hp_shooting_days, 1)
        if hp_register_avg_days is not None:
            display_days[2] = round(hp_register_avg_days, 1)
        elif hp_register_days is not None:
            display_days[2] = round(hp_register_days, 1)
    if selected_brand == "에블린":
        if eblin_register_avg_days is not None:
            display_days[2] = round(eblin_register_avg_days, 1)
    
    st.markdown(
        f'<div class="section-title" style="font-size: 1.6rem;">{selected_brand} 단계별 리드타임</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'''
        <div class="process-flow">
            <div class="process-circle">물류<br>입고</div>
            <div class="process-arrow-box">
                <div class="content">
                    <span class="line">포토팀 상품인계</span>
                    <span class="line days">{display_days[0]:.1f}일</span>
                </div>
            </div>
            <div class="process-arrow-box">
                <div class="content">
                    <span class="line">촬영</span>
                    <span class="line days">{display_days[1]:.1f}일</span>
                </div>
            </div>
            <div class="process-arrow-box">
                <div class="content">
                    <span class="line">상품등록</span>
                    <span class="line days">{display_days[2]:.1f}일</span>
                </div>
            </div>
            <div class="process-circle">온라인<br>판매개시</div>
        </div>
        ''',
        unsafe_allow_html=True,
    )
    if selected_brand == "스파오" and (spao_register_header_cell or spao_photo_header_cell):
        st.caption(
            f"공홈등록 소요일 위치: {spao_register_header_cell} · "
            f"숫자 {spao_register_count}개 평균"
        )
    if selected_brand == "스파오" and spao_photo_header_cell:
        st.caption(
            f"포토소요일 위치: {spao_photo_header_cell} · "
            f"숫자 {spao_photo_count}개 평균"
        )
    if selected_brand == "후아유" and whoau_handover_header_cell:
        st.caption(
            f"포토팀 상품인계 위치: {whoau_handover_header_cell} · "
            f"숫자 {whoau_handover_count}개 평균"
        )
    if selected_brand == "후아유" and whoau_shooting_header_cell:
        st.caption(
            f"촬영 소요일 위치: {whoau_shooting_header_cell} · "
            f"숫자 {whoau_shooting_count}개 평균"
        )
    if selected_brand == "후아유" and whoau_register_header_cell:
        st.caption(
            f"상품등록 소요일 위치: {whoau_register_header_cell} · "
            f"숫자 {whoau_register_count}개 평균"
        )
    if selected_brand == "클라비스" and clavis_handover_header_cell:
        st.caption(
            f"포토팀 상품인계 위치: {clavis_handover_header_cell} · "
            f"숫자 {clavis_handover_count}개 평균"
        )
    if selected_brand == "클라비스" and clavis_shooting_header_cell:
        st.caption(
            f"촬영 소요일 위치: {clavis_shooting_header_cell} · "
            f"숫자 {clavis_shooting_count}개 평균"
        )
    if selected_brand == "클라비스" and clavis_register_header_cell:
        st.caption(
            f"상품등록 소요일 위치: {clavis_register_header_cell} · "
            f"숫자 {clavis_register_count}개 평균"
        )
    if selected_brand == "미쏘" and mixxo_handover_header_cell:
        st.caption(
            f"포토팀 상품인계 위치: {mixxo_handover_header_cell} · "
            f"숫자 {mixxo_handover_count}개 평균"
        )
    if selected_brand == "미쏘" and mixxo_shooting_header_cell:
        st.caption(
            f"촬영 소요일 위치: {mixxo_shooting_header_cell} · "
            f"숫자 {mixxo_shooting_count}개 평균"
        )
    if selected_brand == "미쏘" and mixxo_register_header_cell:
        st.caption(
            f"상품등록 소요일 위치: {mixxo_register_header_cell} · "
            f"숫자 {mixxo_register_count}개 평균"
        )
    if selected_brand == "로엠" and roem_handover_header_cell:
        st.caption(
            f"포토팀 상품인계 위치: {roem_handover_header_cell} · "
            f"숫자 {roem_handover_count}개 평균"
        )
    if selected_brand == "로엠" and roem_shooting_header_cell:
        st.caption(
            f"촬영 소요일 위치: {roem_shooting_header_cell} · "
            f"숫자 {roem_shooting_count}개 평균"
        )
    if selected_brand == "로엠" and roem_register_header_cell:
        st.caption(
            f"상품등록 소요일 위치: {roem_register_header_cell} · "
            f"숫자 {roem_register_count}개 평균"
        )
    if selected_brand == "슈펜" and hp_handover_header_cell:
        st.caption(
            f"포토팀 상품인계 위치: {hp_handover_header_cell} · "
            f"숫자 {hp_handover_count}개 평균"
        )
    if selected_brand == "슈펜" and hp_shooting_header_cell:
        st.caption(
            f"촬영 소요일 위치: {hp_shooting_header_cell} · "
            f"숫자 {hp_shooting_count}개 평균"
        )
    if selected_brand == "슈펜" and hp_register_header_cell:
        st.caption(
            f"상품등록 소요일 위치: {hp_register_header_cell} · "
            f"숫자 {hp_register_count}개 평균"
        )
    
    # 브랜드별 상품등록 모니터링 (상단 표)
    st.markdown("<div style='margin-top: 120px;'></div>", unsafe_allow_html=True)
    st.markdown("---")
    title_col, download_col = st.columns([8, 2])
    with title_col:
        st.markdown('<div class="section-title">브랜드별 상품등록 모니터링</div>', unsafe_allow_html=True)
    monitor_columns = [
        "브랜드",
        "스타일수(입고상품 기준)",
        "온라인 등록 스타일수",
        "온라인등록율",
        "평균 등록 소요일수",
        "등록수",
        "온라인등록율(전주대비)",
        "합계",
        "미분배(분배팀)",
        "포토 미업로드(포토팀)",
        "상품 미등록(온라인)",
    ]
    # 스타일수(입고상품 기준): 위쪽 입출고 표와 동일한 brand_in_qty 사용(이미 위에서 입고액≥1 기준으로 계산됨)
    def format_monitor_num(value):
        if value is None or pd.isna(value):
            return "0"
        try:
            return f"{int(round(float(value))):,}"
        except Exception:
            return "0"
    
    def format_monitor_optional(value):
        if value is None or pd.isna(value):
            return ""
        try:
            if float(value) == 0:
                return ""
            return f"{int(round(float(value))):,}"
        except Exception:
            return ""
    
    def format_monitor_percent(value):
        if value is None:
            return ""
        try:
            return f"{int(round(float(value) * 100)):d}%"
        except Exception:
            return ""
    
    def format_monitor_days(value):
        if value is None or pd.isna(value):
            return ""
        try:
            return f"{float(value):.1f}"
        except Exception:
            return ""
    monitor_rows = []
    bu_labels = {label for label, _ in bu_groups}
    for bu_label, bu_brands in bu_groups:
        monitor_rows.append({"브랜드": bu_label})
        for brand in bu_brands:
            monitor_rows.append({"브랜드": brand})
    monitor_df = pd.DataFrame(monitor_rows)
    for col in monitor_columns:
        if col not in monitor_df.columns:
            monitor_df[col] = ""
    style_count_by_brand = {}
    if "스타일수(입고상품 기준)" in monitor_df.columns:
        def resolve_style_count(brand_name):
            if brand_name in bu_labels:
                brands = next((b for l, b in bu_groups if l == brand_name), [])
                return sum(brand_in_qty.get(b, 0) for b in brands)
            return brand_in_qty.get(brand_name, 0)
        style_count_by_brand = {b: resolve_style_count(b) for b in monitor_df["브랜드"]}
        monitor_df["스타일수(입고상품 기준)"] = monitor_df["브랜드"].map(
            lambda b: format_monitor_num(style_count_by_brand.get(b, 0))
        )
    out_style_count_by_brand = {}
    if "미분배(분배팀)" in monitor_df.columns and inout_style_col:
        out_style_counts = count_unique_style_with_amount_by_brand(
            df_inout_out_base,
            inout_style_col,
            inout_out_amt_col,
            min_amount=1,
        )
        def resolve_out_style_count(brand_name):
            if brand_name in bu_labels:
                brands = next((b for l, b in bu_groups if l == brand_name), [])
                return sum(out_style_counts.get(b, 0) for b in brands)
            return out_style_counts.get(brand_name, 0)
        out_style_count_by_brand = {b: resolve_out_style_count(b) for b in monitor_df["브랜드"]}
        monitor_df["미분배(분배팀)"] = monitor_df["브랜드"].map(
            lambda b: format_monitor_num(
                max(style_count_by_brand.get(b, 0) - out_style_count_by_brand.get(b, 0), 0)
            )
        )
        undist_hide_brands = {"미쏘", "로엠", "클라비스", "에블린", "슈펜"}
        monitor_df.loc[
            monitor_df["브랜드"].isin(undist_hide_brands),
            "미분배(분배팀)",
        ] = "-"
    if "온라인 등록 스타일수" in monitor_df.columns:
        register_style_counts = {}
        if spao_register_style_count is not None:
            register_style_counts["스파오"] = spao_register_style_count
        if whoau_register_style_count is not None:
            register_style_counts["후아유"] = whoau_register_style_count
        if clavis_register_style_count is not None:
            register_style_counts["클라비스"] = clavis_register_style_count
        if mixxo_register_style_count is not None:
            register_style_counts["미쏘"] = mixxo_register_style_count
        if roem_register_style_count is not None:
            register_style_counts["로엠"] = roem_register_style_count
        if hp_register_style_count is not None:
            register_style_counts["슈펜"] = hp_register_style_count
        if eblin_register_style_count is not None:
            register_style_counts["에블린"] = eblin_register_style_count
        def has_online_register_data(brand_name):
            if brand_name in bu_labels:
                brands = next((b for l, b in bu_groups if l == brand_name), [])
                return any(b in register_style_counts for b in brands)
            return brand_name in register_style_counts
        def resolve_register_style_count(brand_name):
            if brand_name in bu_labels:
                brands = next((b for l, b in bu_groups if l == brand_name), [])
                return sum(register_style_counts.get(b, 0) for b in brands)
            return register_style_counts.get(brand_name, 0)
        register_count_by_brand = {b: resolve_register_style_count(b) for b in monitor_df["브랜드"]}
        monitor_df["온라인 등록 스타일수"] = monitor_df["브랜드"].map(
            lambda b: format_monitor_optional(register_count_by_brand.get(b, 0))
        )
    if "합계" in monitor_df.columns:
        def resolve_unregistered_total(brand_name):
            if brand_name in bu_labels:
                brands = next((b for l, b in bu_groups if l == brand_name), [])
                return sum(
                    max(style_count_by_brand.get(b, 0) - register_count_by_brand.get(b, 0), 0)
                    for b in brands
                )
            return max(
                style_count_by_brand.get(brand_name, 0) - register_count_by_brand.get(brand_name, 0),
                0,
            )
        monitor_df["합계"] = monitor_df["브랜드"].map(
            lambda b: format_monitor_num(resolve_unregistered_total(b))
        )
    if "상품 미등록(온라인)" in monitor_df.columns:
        unregistered_counts = {
            "후아유": whoau_unregistered_online_count,
            "스파오": spao_unregistered_online_count,
            "클라비스": clavis_unregistered_online_count,
            "미쏘": mixxo_unregistered_online_count,
            "로엠": roem_unregistered_online_count,
            "슈펜": hp_unregistered_online_count,
        }
        def format_dash_if_no_register(brand_name, value):
            if not has_online_register_data(brand_name):
                return "-"
            return format_monitor_optional(value)
        def resolve_unregistered(brand_name):
            if brand_name in bu_labels:
                brands = next((b for l, b in bu_groups if l == brand_name), [])
                return sum(unregistered_counts.get(b, 0) for b in brands)
            return unregistered_counts.get(brand_name, 0)
        monitor_df["상품 미등록(온라인)"] = monitor_df["브랜드"].map(
            lambda b: format_dash_if_no_register(b, resolve_unregistered(b))
        )
    if "포토 미업로드(포토팀)" in monitor_df.columns:
        def resolve_photo_missing(brand_name):
            if brand_name in bu_labels:
                brands = next((b for l, b in bu_groups if l == brand_name), [])
                total = sum(
                    max(style_count_by_brand.get(b, 0) - register_count_by_brand.get(b, 0), 0)
                    for b in brands
                )
                undist = sum(
                    max(style_count_by_brand.get(b, 0) - out_style_count_by_brand.get(b, 0), 0)
                    for b in brands
                )
                unregistered = sum(unregistered_counts.get(b, 0) for b in brands)
                return max(total - (undist + unregistered), 0)
            total = max(
                style_count_by_brand.get(brand_name, 0) - register_count_by_brand.get(brand_name, 0), 0
            )
            undist = max(
                style_count_by_brand.get(brand_name, 0) - out_style_count_by_brand.get(brand_name, 0), 0
            )
            unregistered = unregistered_counts.get(brand_name, 0)
            return max(total - (undist + unregistered), 0)
        monitor_df["포토 미업로드(포토팀)"] = monitor_df["브랜드"].map(
            lambda b: format_dash_if_no_register(b, resolve_photo_missing(b))
        )
    if "평균 등록 소요일수" in monitor_df.columns:
        avg_days_by_brand = {}
        # 스파오: 시트의 '공홈등록소요일' 컬럼 평균(5.1일) 사용. (등록일-스튜디오전표일자) 방식은 47일 등으로 크게 나올 수 있음.
        if spao_register_days is not None:
            avg_days_by_brand["스파오"] = spao_register_days
        elif spao_register_avg_days is not None:
            avg_days_by_brand["스파오"] = spao_register_avg_days
        if whoau_register_avg_days is not None:
            avg_days_by_brand["후아유"] = whoau_register_avg_days
        if clavis_register_avg_days is not None:
            avg_days_by_brand["클라비스"] = clavis_register_avg_days
        if mixxo_register_avg_days is not None:
            avg_days_by_brand["미쏘"] = mixxo_register_avg_days
        if roem_register_avg_days is not None:
            avg_days_by_brand["로엠"] = roem_register_avg_days
        if hp_register_avg_days is not None:
            avg_days_by_brand["슈펜"] = hp_register_avg_days
        if eblin_register_avg_days is not None:
            avg_days_by_brand["에블린"] = eblin_register_avg_days
        def resolve_avg_days(brand_name):
            if brand_name in bu_labels:
                brands = next((b for l, b in bu_groups if l == brand_name), [])
                values = [avg_days_by_brand.get(b) for b in brands]
                values = [v for v in values if v is not None and not pd.isna(v)]
                if not values:
                    return None
                return float(sum(values)) / len(values)
            return avg_days_by_brand.get(brand_name)
        monitor_df["평균 등록 소요일수"] = monitor_df["브랜드"].map(
            lambda b: format_monitor_days(resolve_avg_days(b))
        )
    register_rate_by_brand = {}
    if "온라인등록율" in monitor_df.columns:
        def resolve_register_rate(brand_name):
            style_count = style_count_by_brand.get(brand_name, 0)
            register_count = register_count_by_brand.get(brand_name, 0)
            if style_count and register_count is not None and register_count != 0:
                return register_count / style_count
            return None
        register_rate_by_brand = {b: resolve_register_rate(b) for b in monitor_df["브랜드"]}
        monitor_df["온라인등록율"] = monitor_df["브랜드"].map(
            lambda b: format_monitor_percent(register_rate_by_brand.get(b))
        )
    monitor_df = monitor_df[monitor_columns]
    def build_monitor_table_html(df, rate_by_brand):
        """
        (D) [상품등록 모니터링] HTML 테이블 생성 함수.
    
        - Streamlit에서 `unsafe_allow_html=True`로 렌더링됩니다.
        - 색상 점/툴팁 등 표시 로직이 포함되어 있어, UI/스타일 변경 시 이 함수부터 확인하면 됩니다.
        """
        # HTML 테이블 직접 렌더링(색상 점/툴팁 포함)
        def safe_cell(val):
            text = "" if val is None else str(val)
            return text if text.strip() else "&nbsp;"
        def build_rate_cell(brand_name, rate_text):
            rate_val = rate_by_brand.get(brand_name)
            if rate_val is None:
                return safe_cell(rate_text)
            if rate_val <= 0.8:
                dot_class = "rate-red"
            elif rate_val <= 0.9:
                dot_class = "rate-yellow"
            else:
                dot_class = "rate-green"
            tooltip = "(초록불) 90% 초과&#10;(노란불) 80% 초과&#10;(빨간불) 80% 이하"
            return f"<span class='rate-cell' data-tooltip='{tooltip}'><span class='rate-dot {dot_class}'></span>{safe_cell(rate_text)}</span>"
        def build_avg_days_cell(value_text):
            tooltip = "(초록불) 3일 이하&#10;(노란불) 5일 이하&#10;(빨간불) 5일 초과"
            dot_class = ""
            try:
                num_val = float(str(value_text).replace(",", "").strip())
                if num_val <= 3:
                    dot_class = "rate-green"
                elif num_val <= 5:
                    dot_class = "rate-yellow"
                else:
                    dot_class = "rate-red"
            except Exception:
                dot_class = ""
            dot_html = f"<span class='rate-dot {dot_class}'></span>" if dot_class else ""
            return f"<span class='avg-cell' data-tooltip='{tooltip}'>{dot_html}{safe_cell(value_text)}</span>"
        header_top = (
            "<tr>"
            "<th rowspan='2'>브랜드</th>"
            "<th class='group-head' colspan='4'>공통</th>"
            "<th class='group-head' colspan='2'>전주대비</th>"
            "<th class='group-head' colspan='4'>미등록현황</th>"
            "</tr>"
        )
        header_bottom = (
            "<tr>"
            "<th>입고스타일수</th>"
            "<th>온라인등록<br>스타일수</th>"
            "<th><span class='rate-help' data-tooltip='(초록불) 90% 초과&#10;(노란불) 80% 초과&#10;(빨간불) 80% 이하'>온라인<br>등록율</span></th>"
            "<th><span class='avg-help' data-tooltip='(온라인상품등록일 - 최초입고일)'>평균 등록 소요일수<br><span style='font-size:0.8rem; font-weight:500; color:#94a3b8;'>온라인상품등록일 - 최초입고일</span></span></th>"
            "<th>등록수</th>"
            "<th><span class='rate-help' data-tooltip='(초록불) 90% 초과&#10;(노란불) 80% 초과&#10;(빨간불) 80% 이하'>온라인등록율</span></th>"
            "<th><span class='sum-help' data-tooltip='(입고스타일수 - 온라인등록스타일수)'>전체 미등록 스타일</span></th>"
            "<th>미분배<br>(분배팀)</th>"
            "<th>포토 미업로드<br>(포토팀)</th>"
            "<th>상품 미등록<br>(온라인)</th>"
            "</tr>"
        )
        body_rows = []
        for _, row in df.iterrows():
            row_class = "bu-row" if row.get("브랜드") in bu_labels else ""
            cells = []
            for col in monitor_columns:
                if col == "등록율":
                    cells.append(build_rate_cell(row.get("브랜드"), row.get(col, "")))
                elif col == "평균 등록 소요일수":
                    cells.append(build_avg_days_cell(row.get(col, "")))
                else:
                    cells.append(safe_cell(row.get(col, "")))
            body_rows.append(f"<tr class='{row_class}'>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
        table_html = f"""
        <style>
            .monitor-table {{
                width: 100%;
                border-collapse: collapse;
                background: #1e293b;
                color: #f1f5f9;
                border: 1px solid #334155;
            }}
            .monitor-table th, .monitor-table td {{
                border: 1px solid #334155;
                padding: 6px 8px;
                text-align: center;
                font-size: 0.95rem;
            }}
            .monitor-table thead th {{
                background: #0f172a;
                color: #f1f5f9;
                font-weight: 700;
            }}
            .monitor-table .group-head {{
                background: #111827;
                color: #f1f5f9;
                font-size: 1rem;
            }}
            .monitor-table tr.bu-row td {{
                background-color: #d9f7ee;
                color: #000000;
                font-size: 1.15rem;
                font-weight: 700;
            }}
            .monitor-table .rate-cell {{
                display: inline-flex;
                align-items: center;
                gap: 6px;
                justify-content: center;
                position: relative;
                cursor: help;
            }}
            .monitor-table .avg-cell {{
                position: relative;
                cursor: help;
                display: inline-flex;
                align-items: center;
                gap: 6px;
                justify-content: center;
            }}
            .monitor-table .rate-dot {{
                width: 16px;
                height: 16px;
                border-radius: 50%;
                display: inline-block;
            }}
            .monitor-table .rate-red {{ background: #ef4444; }}
            .monitor-table .rate-yellow {{ background: #f59e0b; }}
            .monitor-table .rate-green {{ background: #22c55e; }}
            .monitor-table .rate-cell::after {{
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
                max-width: none;
                background: #111827;
                color: #f1f5f9;
                padding: 6px 8px;
                border-radius: 6px;
                font-size: 0.85rem;
                text-align: left;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.35);
                z-index: 20;
            }}
            .monitor-table .rate-cell:hover::after {{
                content: attr(data-tooltip);
                opacity: 1;
            }}
            .monitor-table .rate-help {{
                position: relative;
                display: inline-block;
                cursor: help;
            }}
            .monitor-table .avg-help {{
                position: relative;
                display: inline-block;
                cursor: help;
            }}
            .monitor-table .sum-help {{
                position: relative;
                display: inline-block;
                cursor: help;
            }}
            .monitor-table .rate-help::after {{
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
                max-width: none;
                background: #111827;
                color: #f1f5f9;
                padding: 6px 8px;
                border-radius: 6px;
                font-size: 0.85rem;
                text-align: left;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.35);
                z-index: 20;
            }}
            .monitor-table .rate-help:hover::after {{
                content: attr(data-tooltip);
                opacity: 1;
            }}
            .monitor-table .avg-cell::after {{
                content: "";
                position: absolute;
                opacity: 0;
                pointer-events: none;
                transition: opacity 0.15s ease-in-out;
                left: 50%;
                transform: translateX(-50%);
                bottom: calc(100% + 6px);
                background: #111827;
                color: #f1f5f9;
                padding: 6px 8px;
                border-radius: 6px;
                font-size: 0.85rem;
                text-align: left;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.35);
                z-index: 20;
                white-space: pre;
                word-break: keep-all;
                width: max-content;
                max-width: none;
            }}
            .monitor-table .avg-cell:hover::after {{
                content: attr(data-tooltip);
                opacity: 1;
            }}
            .monitor-table .avg-help::after {{
                content: "";
                position: absolute;
                opacity: 0;
                pointer-events: none;
                transition: opacity 0.15s ease-in-out;
                left: 50%;
                transform: translateX(-50%);
                bottom: calc(100% + 6px);
                background: #111827;
                color: #f1f5f9;
                padding: 6px 8px;
                border-radius: 6px;
                font-size: 0.85rem;
                text-align: left;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.35);
                z-index: 20;
                white-space: pre;
                word-break: keep-all;
                width: max-content;
                max-width: none;
            }}
            .monitor-table .avg-help:hover::after {{
                content: attr(data-tooltip);
                opacity: 1;
            }}
            .monitor-table .sum-help::after {{
                content: "";
                position: absolute;
                opacity: 0;
                pointer-events: none;
                transition: opacity 0.15s ease-in-out;
                left: 50%;
                transform: translateX(-50%);
                bottom: calc(100% + 6px);
                background: #111827;
                color: #f1f5f9;
                padding: 6px 8px;
                border-radius: 6px;
                font-size: 0.85rem;
                text-align: left;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.35);
                z-index: 20;
                white-space: pre;
                word-break: keep-all;
                width: max-content;
                max-width: none;
            }}
            .monitor-table .sum-help:hover::after {{
                content: attr(data-tooltip);
                opacity: 1;
            }}
        </style>
        <table class="monitor-table">
            <thead>{header_top}{header_bottom}</thead>
            <tbody>{''.join(body_rows)}</tbody>
        </table>
        """
        return table_html
    
    st.markdown(build_monitor_table_html(monitor_df, register_rate_by_brand), unsafe_allow_html=True)
    
    # 브랜드별 상품등록 모니터링 다운로드 (CSV)
    def to_csv_bytes_monitor(df):
        export_df = df[monitor_columns] if all(c in df.columns for c in monitor_columns) else df
        return export_df.to_csv(index=False, encoding="utf-8-sig")
    
    monitor_csv_data = to_csv_bytes_monitor(monitor_df)
    with download_col:
        st.markdown("<div class='download-right-marker'></div>", unsafe_allow_html=True)
        monitor_download_ts = datetime.now().strftime("%Y%m%d_%H%M")
        st.download_button(
            label="CSV 다운로드",
            data=monitor_csv_data,
            file_name=f"브랜드별_상품등록_모니터링_{monitor_download_ts}.csv",
            mime="text/csv",
            key="download_monitor_csv",
        )
    
    # 브랜드 상세(입출고 모니터링)
    st.markdown('<div style="height:40px;"></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-title">브랜드별 입출고 모니터링</div>', unsafe_allow_html=True)
    
    unit_left, _unit_right = st.columns([6, 5])
    
    # 브랜드 상세 토글 (STY/SKU) - QR 토글과 동일 스타일
    with unit_left:
        sty_toggle_val = st.session_state.get("sty_toggle", True)
        sty_label = "STY 기준 통계" if sty_toggle_val else "SKU 기준 통계"
        st.markdown(f'<div class="unit-toggle-label">{sty_label}</div>', unsafe_allow_html=True)
        sty_toggle = st.toggle("", value=sty_toggle_val, key="sty_toggle", label_visibility="collapsed")
    
    # 브랜드 상세 테이블 컬럼(STY 또는 SKU)
    if sty_toggle:
        table_columns = [
            "발주 STY수", "발주액", "입고 STY수", "입고액",
            "출고 STY수", "출고액", "판매 STY수", "판매액"
        ]
    else:
        table_columns = [
            "발주 SKU수", "발주액", "입고 SKU수", "입고액",
            "출고 SKU수", "출고액", "판매 SKU수", "판매액"
        ]
    brand_rows = []
    brand_order_qty = count_unique_style_with_amount_by_brand(
        df_inout_order_base, inout_style_col, inout_order_qty_col, min_amount=1
    )
    brand_order_sku_qty = count_unique_sku_with_amount_by_brand(
        df_inout_order_base,
        inout_style_col,
        inout_color_col,
        inout_order_qty_col,
        min_amount=1,
    )
    brand_order_amt = sum_by_brand(df_inout_order_base, inout_order_amt_col)
    # brand_in_qty는 위쪽에서 입고액≥1 기준으로 이미 계산됨(입출고 표·모니터 표 공통)
    brand_in_sku_qty = count_unique_sku_with_amount_by_brand(
        df_inout_in_base,
        inout_style_col,
        inout_color_col,
        inout_cum_in_amt_col or inout_in_amt_col,
        min_amount=1,
    )
    brand_in_amt = sum_by_brand(df_inout_in_base, inout_cum_in_amt_col or inout_in_amt_col)
    brand_out_qty = count_unique_style_by_brand(df_inout_table, inout_style_col)
    brand_out_sku_qty = count_unique_sku_with_amount_by_brand(
        df_inout_table, inout_style_col, inout_color_col, inout_out_amt_col, min_amount=1
    )
    brand_out_amt = sum_by_brand(df_inout_table, inout_out_amt_col)
    sale_amt_col = inout_cum_sale_amt_col or inout_sale_amt_col
    brand_sale_sty_qty = count_unique_style_with_amount_by_brand(
        df_inout_table, inout_style_col, sale_amt_col, min_amount=1
    )
    brand_sale_sku_qty = count_unique_sku_with_amount_by_brand(
        df_inout_table, inout_style_col, inout_color_col, sale_amt_col, min_amount=1
    )
    brand_sale_amt = sum_by_brand(df_inout_table, inout_cum_sale_amt_col or inout_sale_amt_col)
    
    def format_table_num(value):
        if value is None or pd.isna(value):
            return "0"
        try:
            return f"{int(round(float(value))):,}"
        except Exception:
            return "0"
    
    def format_amount_eok(value):
        if value is None or pd.isna(value):
            return "0 억 원"
        try:
            return f"{float(value) / 100000000:,.0f} 억 원"
        except Exception:
            return "0 억 원"
    
    def sum_by_brands(data_dict, brands):
        return sum(data_dict.get(b, 0) for b in brands)
    
    def build_row(label, brands=None):
        row = {"브랜드": label}
        for col in table_columns:
            row[col] = "0"
        if brands is None:
            return row
        if "발주 STY수" in row:
            row["발주 STY수"] = format_table_num(sum_by_brands(brand_order_qty, brands))
        if "발주 SKU수" in row:
            row["발주 SKU수"] = format_table_num(sum_by_brands(brand_order_sku_qty, brands))
        if "발주액" in row:
            row["발주액"] = format_amount_eok(sum_by_brands(brand_order_amt, brands))
        if "입고 STY수" in row:
            row["입고 STY수"] = format_table_num(sum_by_brands(brand_in_qty, brands))
        if "입고 SKU수" in row:
            row["입고 SKU수"] = format_table_num(sum_by_brands(brand_in_sku_qty, brands))
        if "입고액" in row:
            row["입고액"] = format_amount_eok(sum_by_brands(brand_in_amt, brands))
        if "출고 STY수" in row:
            row["출고 STY수"] = format_table_num(sum_by_brands(brand_out_qty, brands))
        if "출고 SKU수" in row:
            row["출고 SKU수"] = format_table_num(sum_by_brands(brand_out_sku_qty, brands))
        if "출고액" in row:
            row["출고액"] = format_amount_eok(sum_by_brands(brand_out_amt, brands))
        if "판매 STY수" in row:
            row["판매 STY수"] = format_table_num(sum_by_brands(brand_sale_sty_qty, brands))
        if "판매 SKU수" in row:
            row["판매 SKU수"] = format_table_num(sum_by_brands(brand_sale_sku_qty, brands))
        if "판매액" in row:
            row["판매액"] = format_amount_eok(sum_by_brands(brand_sale_amt, brands))
        return row
    
    def build_season_label_series(df):
        # 시즌 라벨 생성(연도+시즌; 미지정 처리)
        if df is None or df.empty:
            return pd.Series(dtype="object")
        if inout_season_col and inout_season_col in df.columns:
            season_series = df[inout_season_col].astype(str).str.strip()
        else:
            season_series = pd.Series(["미지정"] * len(df), index=df.index)
        season_series = season_series.replace(r"^\s*$", "미지정", regex=True)
        season_series = season_series.str.replace("시즌", "", regex=False).str.strip()
        if inout_year_col and inout_year_col in df.columns:
            year_series = df[inout_year_col].astype(str).str.strip().replace(r"^\s*$", "", regex=True)
            label = (year_series + " " + season_series).str.strip()
            label = label.where(label.str.strip().ne(""), season_series)
            return label
        return season_series
    
    def build_season_detail_table(base_df, brand_name, use_sty, order_base_df=None, in_base_df=None):
        # 브랜드별 시즌 상세 행 생성(발주/입고/출고/판매)
        columns = ["시즌"] + table_columns
        if base_df is None or base_df.empty:
            return pd.DataFrame(columns=columns)
        if inout_brand_col is None or inout_brand_col not in base_df.columns:
            return pd.DataFrame(columns=columns)
        brand_series = base_df[inout_brand_col].astype(str).str.replace(" ", "").str.strip()
        target_brand = str(brand_name).replace(" ", "").strip()
        df_brand = base_df[brand_series == target_brand]
        order_df = order_base_df if order_base_df is not None else base_df
        in_df = in_base_df if in_base_df is not None else base_df
        order_brand_series = order_df[inout_brand_col].astype(str).str.replace(" ", "").str.strip()
        order_brand_df = order_df[order_brand_series == target_brand]
        in_brand_series = in_df[inout_brand_col].astype(str).str.replace(" ", "").str.strip()
        in_brand_df = in_df[in_brand_series == target_brand]
        if df_brand.empty:
            return pd.DataFrame(columns=columns)
        season_labels = build_season_label_series(df_brand)
        order_season_labels = build_season_label_series(order_brand_df) if not order_brand_df.empty else season_labels
        in_season_labels = build_season_label_series(in_brand_df) if not in_brand_df.empty else season_labels
        rows = []
        for season_label, df_season in df_brand.groupby(season_labels):
            order_season = (
                order_brand_df[order_season_labels == season_label]
                if not order_brand_df.empty
                else df_season
            )
            in_season = (
                in_brand_df[in_season_labels == season_label]
                if not in_brand_df.empty
                else df_season
            )
            row = {"시즌": season_label}
            if use_sty:
                row["발주 STY수"] = format_table_num(
                    count_unique_style_with_amount(
                        order_season, inout_style_col, inout_order_qty_col, min_amount=1
                    )
                )
                row["입고 STY수"] = format_table_num(
                    count_unique_style_with_amount(
                        in_season,
                        inout_style_col,
                        inout_cum_in_amt_col or inout_in_amt_col,
                        min_amount=1,
                    )
                )
                row["출고 STY수"] = format_table_num(
                    count_unique_style_with_amount(df_season, inout_style_col, inout_out_amt_col, min_amount=1)
                )
                row["판매 STY수"] = format_table_num(
                    count_unique_style_with_amount(
                        df_season,
                        inout_style_col,
                        inout_cum_sale_amt_col or inout_sale_amt_col,
                        min_amount=1,
                    )
                )
            else:
                row["발주 SKU수"] = format_table_num(
                    count_unique_sku_with_amount(
                        order_season, inout_style_col, inout_color_col, inout_order_qty_col, min_amount=1
                    )
                )
                row["입고 SKU수"] = format_table_num(
                    count_unique_sku_with_amount(
                        in_season,
                        inout_style_col,
                        inout_color_col,
                        inout_cum_in_amt_col or inout_in_amt_col,
                        min_amount=1,
                    )
                )
                row["출고 SKU수"] = format_table_num(
                    count_unique_sku_with_amount(df_season, inout_style_col, inout_color_col, inout_out_amt_col, min_amount=1)
                )
                row["판매 SKU수"] = format_table_num(
                    count_unique_sku_with_amount(
                        df_season,
                        inout_style_col,
                        inout_color_col,
                        inout_cum_sale_amt_col or inout_sale_amt_col,
                        min_amount=1,
                    )
                )
            row["발주액"] = format_amount_eok(safe_sum(order_season, inout_order_amt_col))
            row["입고액"] = format_amount_eok(safe_sum(in_season, inout_cum_in_amt_col or inout_in_amt_col))
            row["출고액"] = format_amount_eok(safe_sum(df_season, inout_out_amt_col))
            # 판매수는 STY/SKU 기준으로 계산되어 위에서 채움
            row["판매액"] = format_amount_eok(safe_sum(df_season, inout_cum_sale_amt_col or inout_sale_amt_col))
            rows.append(row)
        if not rows:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame(rows)[columns]
    
    def build_brand_season_table_html(display_df, base_df, use_sty, order_base_df=None, in_base_df=None):
        """
        (D) [입출고 모니터링] 브랜드 행 클릭 → 시즌 상세 토글 HTML 테이블 생성.
    
        - 브랜드/BU 행 + 시즌 상세 행을 함께 생성합니다.
        - HTML 이스케이프는 `html_lib.escape()`로 처리합니다.
        """
        # 브랜드 행 클릭 시 시즌 행 토글되는 HTML 테이블 생성
        cols = ["브랜드"] + table_columns
        header_cells = "".join(
            f"<th>{html_lib.escape(str(c))}</th>" for c in cols
        )
        body_rows = []
        row_count = 0
        for _, row in display_df.iterrows():
            brand_name = str(row.get("브랜드", "")).strip()
            is_bu = brand_name in bu_labels
            values = [brand_name] + [row.get(c, "") for c in table_columns]
            if is_bu:
                cell_html = "".join(
                    f"<td>{html_lib.escape(str(v))}</td>" for v in values
                )
                body_rows.append(f"<tr class='bu-row'>{cell_html}</tr>")
                row_count += 1
                continue
            brand_id = f"brand-{abs(hash(brand_name))}"
            brand_cell = (
                "<td class='brand-cell'>"
                f"<button class='brand-toggle' data-target='{brand_id}' aria-expanded='false'>"
                f"<span class='label'>{html_lib.escape(brand_name)}</span>"
                "<span class='caret'>▽</span>"
                "</button>"
                "</td>"
            )
            other_cells = "".join(
                f"<td>{html_lib.escape(str(v))}</td>" for v in values[1:]
            )
            cell_html = brand_cell + other_cells
            if is_bu:
                body_rows.append(f"<tr class='bu-row'>{cell_html}</tr>")
                row_count += 1
                continue
            body_rows.append(f"<tr class='brand-row'>{cell_html}</tr>")
            row_count += 1
            season_df = build_season_detail_table(
                base_df,
                brand_name,
                use_sty,
                order_base_df=order_base_df,
                in_base_df=in_base_df,
            )
            if season_df is not None and not season_df.empty:
                for _, srow in season_df.iterrows():
                    season_label = str(srow.get("시즌", "")).strip()
                    season_values = [f"└ {season_label}"] + [
                        srow.get(c, "") for c in table_columns
                    ]
                    season_cells = "".join(
                        f"<td>{html_lib.escape(str(v))}</td>" for v in season_values
                    )
                    body_rows.append(
                        f"<tr class='season-row' data-parent='{brand_id}'>{season_cells}</tr>"
                    )
                    row_count += 1
        table_html = f"""
        <style>
            .brand-expand-table {{
                width: 100%;
                border: 1px solid #334155;
                border-radius: 8px;
                overflow: hidden;
                background: #1e293b;
                color: #f1f5f9;
                margin-top: 0.5rem;
            }}
            .brand-expand-table table {{
                width: 100%;
                border-collapse: collapse;
            }}
            .brand-expand-table th, .brand-expand-table td {{
                border: 1px solid #334155;
                padding: 6px 8px;
                text-align: center;
                font-size: 0.95rem;
            }}
            .brand-expand-table thead th {{
                background: #0f172a;
                color: #f1f5f9;
                font-weight: 700;
                font-size: 1rem;
            }}
            .brand-row {{
                background: #111827;
            }}
            .brand-cell {{
                text-align: left;
            }}
            .brand-toggle {{
                all: unset;
                cursor: pointer;
                display: inline-flex;
                align-items: center;
                gap: 6px;
                font-weight: 700;
                color: #f1f5f9;
            }}
            .brand-toggle .caret {{
                display: inline-block;
                transition: transform 0.15s ease-in-out;
                color: #94a3b8;
                font-size: 0.9rem;
            }}
            .brand-toggle[aria-expanded="true"] .caret {{
                transform: rotate(90deg);
            }}
            .brand-row.bu-row {{
                background-color: #d9f7ee;
                color: #000000;
                font-size: 1.15rem;
                font-weight: 700;
            }}
            .season-row td {{
                background: #0f172a;
                font-size: 0.9rem;
                color: #cbd5e1;
            }}
            .season-row td:first-child {{
                text-align: left;
                padding-left: 18px;
            }}
        </style>
        <div class="brand-expand-table">
            <table>
                <thead><tr>{header_cells}</tr></thead>
                <tbody>{''.join(body_rows)}</tbody>
            </table>
        </div>
        <script>
            (function() {{
                const rows = document.querySelectorAll(".season-row");
                rows.forEach(r => r.style.display = "none");
                document.querySelectorAll(".brand-toggle").forEach(btn => {{
                    btn.addEventListener("click", () => {{
                        const target = btn.getAttribute("data-target");
                        const expanded = btn.getAttribute("aria-expanded") === "true";
                        btn.setAttribute("aria-expanded", expanded ? "false" : "true");
                        document.querySelectorAll(`.season-row[data-parent='${{target}}']`)
                            .forEach(r => r.style.display = expanded ? "none" : "table-row");
                    }});
                }});
            }})();
        </script>
        """
        return table_html, row_count
    
    for bu_label, bu_brands in bu_groups:
        brand_rows.append(build_row(bu_label, bu_brands))
        for brand in bu_brands:
            row = build_row(brand)
            if "발주 STY수" in row:
                row["발주 STY수"] = format_table_num(brand_order_qty.get(brand, 0))
            if "발주 SKU수" in row:
                row["발주 SKU수"] = format_table_num(brand_order_sku_qty.get(brand, 0))
            if "발주액" in row:
                row["발주액"] = format_amount_eok(brand_order_amt.get(brand, 0))
            if "입고 STY수" in row:
                row["입고 STY수"] = format_table_num(brand_in_qty.get(brand, 0))
            if "입고 SKU수" in row:
                row["입고 SKU수"] = format_table_num(brand_in_sku_qty.get(brand, 0))
            if "입고액" in row:
                row["입고액"] = format_amount_eok(brand_in_amt.get(brand, 0))
            if "출고 STY수" in row:
                row["출고 STY수"] = format_table_num(brand_out_qty.get(brand, 0))
            if "출고 SKU수" in row:
                row["출고 SKU수"] = format_table_num(brand_out_sku_qty.get(brand, 0))
            if "출고액" in row:
                row["출고액"] = format_amount_eok(brand_out_amt.get(brand, 0))
            if "판매 STY수" in row:
                row["판매 STY수"] = format_table_num(brand_sale_sty_qty.get(brand, 0))
            if "판매 SKU수" in row:
                row["판매 SKU수"] = format_table_num(brand_sale_sku_qty.get(brand, 0))
            if "판매액" in row:
                row["판매액"] = format_amount_eok(brand_sale_amt.get(brand, 0))
            brand_rows.append(row)
    
    detail_df = pd.DataFrame(brand_rows)
    display_cols = ["브랜드"] + table_columns
    
    # CSV 다운로드 버튼 (표 위 우측 정렬)
    def to_csv_bytes(df):
        export_df = df[display_cols] if all(c in df.columns for c in display_cols) else df
        return export_df.to_csv(index=False, encoding="utf-8-sig")
    
    csv_data = to_csv_bytes(detail_df)
    btn_col_left, btn_col_right = st.columns([9, 1])
    with btn_col_right:
        detail_download_ts = datetime.now().strftime("%Y%m%d_%H%M")
        st.download_button(
            label="CSV 다운로드",
            data=csv_data,
            file_name=f"브랜드별_입출고_모니터링_{detail_download_ts}.csv",
            mime="text/csv",
            key="download_csv",
        )
    
    display_df = detail_df[display_cols] if all(c in detail_df.columns for c in display_cols) else detail_df
    st.caption("브랜드명을 클릭하면 시즌별 수치를 보실 수 있습니다")
    try:
        import streamlit.components.v1 as components
        season_html, row_count = build_brand_season_table_html(
            display_df,
            df_inout_table,
            sty_toggle,
            order_base_df=df_inout_order_base,
            in_base_df=df_inout_in_base,
        )
        table_height = 120 + (row_count * 24)
        components.html(season_html, height=table_height, scrolling=True)
    except Exception:
        season_html, _ = build_brand_season_table_html(
            display_df,
            df_inout_table,
            sty_toggle,
            order_base_df=df_inout_order_base,
            in_base_df=df_inout_in_base,
        )
        st.markdown(season_html, unsafe_allow_html=True)
    
    st.markdown("---")

_render_dashboard()
st.caption("본 대시보드 관련한 문의가 있으실 경우, axfashion@eland.co.kr로 연락주시기 바랍니다.")
