import streamlit as st
import pandas as pd
from io import BytesIO
import re
import os

st.set_page_config(page_title="브랜드 상품 흐름 대시보드", layout="wide")

# ----------------------------
# Google Sheets 연동
# ----------------------------
def get_gsheet_client(credentials_dict):
    if credentials_dict is None:
        return None
    import gspread
    from google.oauth2.service_account import Credentials
    scope = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(
        credentials_dict, scopes=scope
    )
    return gspread.authorize(creds)

def spreadsheet_id_from_url(url):
    m = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
    return m.group(1) if m else url.strip()

def load_sheet_as_dataframe(client, spreadsheet_id, sheet_name=None):
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name) if sheet_name else spreadsheet.sheet1
        rows = worksheet.get_all_values()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows[1:], columns=rows[0])
    except Exception as e:
        st.error(f"시트 읽기 오류: {e}")
        return None

# ----------------------------
# 상태 판정 로직
# ----------------------------
def get_verdict(inbound, outbound, is_shot, is_registered, is_on_sale):
    if inbound > 0 and outbound == 0:
        return "입고"
    if outbound > 0 and is_shot == 0:
        return "출고"
    if is_shot == 1 and is_registered == 0:
        return "촬영"
    if is_registered == 1 and is_on_sale == 0:
        return "등록"
    if is_on_sale == 1:
        return "판매개시"
    return "대기"

# ----------------------------
# 스냅샷 증감 계산
# ----------------------------
def compute_flow_deltas(df):
    if len(df) < 2:
        return None
    this_week = df.iloc[0]
    last_week = df.iloc[1]
    return {
        "입고": this_week["inboundDone"] - last_week["inboundDone"],
        "출고": this_week["outboundDone"] - last_week["outboundDone"],
        "촬영": this_week["shotDone"] - last_week["shotDone"],
        "등록": this_week["registeredDone"] - last_week["registeredDone"],
        "판매개시": this_week["onSaleDone"] - last_week["onSaleDone"],
    }

# ----------------------------
# 제목
# ----------------------------
st.title("브랜드 상품 흐름 대시보드")

# ----------------------------
# 사이드바 — Google Sheets 연결
# ----------------------------
st.sidebar.header("Google Sheets 연결")

# 서비스 계정 키: 업로드 또는 환경변수 경로
creds_upload = st.sidebar.file_uploader(
    "서비스 계정 JSON 키",
    type=["json"],
    help="Google Cloud 서비스 계정 키 파일을 업로드하세요. 시트를 해당 계정 이메일과 공유해야 합니다.",
)
creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
if creds_upload:
    import json
    creds_dict = json.load(creds_upload)
    gs_client = get_gsheet_client(creds_dict)
elif creds_path and os.path.isfile(creds_path):
    with open(creds_path, "r", encoding="utf-8") as f:
        import json
        creds_dict = json.load(f)
    gs_client = get_gsheet_client(creds_dict)
else:
    gs_client = None

sheet_url = st.sidebar.text_input(
    "Google 시트 URL",
    placeholder="https://docs.google.com/spreadsheets/d/xxxxx/edit",
)
items_sheet_name = st.sidebar.text_input(
    "상품 시트 이름 (비우면 첫 시트)",
    placeholder="Sheet1 또는 상품데이터",
)
snapshots_sheet_name = st.sidebar.text_input(
    "스냅샷 시트 이름 (선택, 비우면 스냅샷 미사용)",
    placeholder="스냅샷",
)

if not gs_client:
    st.info("왼쪽 사이드바에서 **서비스 계정 JSON 키**를 업로드한 뒤, **Google 시트 URL**을 입력하세요.")
    st.stop()

if not sheet_url:
    st.info("Google 시트 URL을 입력하세요.")
    st.stop()

spreadsheet_id = spreadsheet_id_from_url(sheet_url)
items_df = load_sheet_as_dataframe(
    gs_client,
    spreadsheet_id,
    sheet_name=items_sheet_name if items_sheet_name.strip() else None,
)
if items_df is None:
    st.stop()
if len(items_df) == 0:
    st.warning("시트에 데이터가 없습니다.")
    st.stop()

# 시트에서 읽은 값은 문자열이므로 숫자 컬럼 변환
numeric_cols = [
    "inboundQty", "outboundQty", "stockQty", "salesQty",
    "isShot", "isRegistered", "isOnSale"
]
for col in numeric_cols:
    if col in items_df.columns:
        items_df[col] = pd.to_numeric(items_df[col], errors="coerce").fillna(0).astype(int)

required_columns = [
    "brand","yearSeason","styleCode","productName",
    "colorCode","colorName","sizeCode",
    "inboundQty","outboundQty","stockQty","salesQty",
    "isShot","isRegistered","isOnSale"
]

missing = [col for col in required_columns if col not in items_df.columns]
if missing:
    st.error(f"필수 컬럼 누락: {missing}")
    st.stop()

# ----------------------------
# verdict 생성
# ----------------------------
items_df["verdict"] = items_df.apply(
    lambda r: get_verdict(
        r["inboundQty"],
        r["outboundQty"],
        r["isShot"],
        r["isRegistered"],
        r["isOnSale"],
    ),
    axis=1,
)

# ----------------------------
# 필터 영역
# ----------------------------
col1, col2 = st.columns(2)

with col1:
    brand = st.selectbox("브랜드", sorted(items_df["brand"].unique()))

with col2:
    year_season = st.selectbox("연도·시즌", sorted(items_df["yearSeason"].unique()))

filtered_df = items_df[
    (items_df["brand"] == brand)
    & (items_df["yearSeason"] == year_season)
]

search = st.text_input("스타일코드 / 판정 검색")

if search:
    filtered_df = filtered_df[
        filtered_df["styleCode"].str.contains(search, case=False, na=False)
        | filtered_df["verdict"].str.contains(search, case=False, na=False)
    ]

# ----------------------------
# 흐름 집계 카드
# ----------------------------
st.subheader("흐름 집계")

flow_types = ["입고", "출고", "촬영", "등록", "판매개시"]
flow_counts = filtered_df["verdict"].value_counts()

cols = st.columns(len(flow_types))
for i, flow in enumerate(flow_types):
    count = int(flow_counts.get(flow, 0))
    cols[i].metric(flow, count)

# ----------------------------
# 스냅샷 증감 표시 (Google 시트에 스냅샷 시트가 있는 경우)
# ----------------------------
snapshots_df = None
if snapshots_sheet_name and snapshots_sheet_name.strip():
    snapshots_df = load_sheet_as_dataframe(
        gs_client, spreadsheet_id, sheet_name=snapshots_sheet_name.strip()
    )
if snapshots_df is not None and len(snapshots_df) >= 2:
    snap_cols = ["inboundDone", "outboundDone", "shotDone", "registeredDone", "onSaleDone"]
    for c in snap_cols:
        if c in snapshots_df.columns:
            snapshots_df[c] = pd.to_numeric(snapshots_df[c], errors="coerce").fillna(0).astype(int)
    deltas = compute_flow_deltas(snapshots_df)
    if deltas:
        st.subheader("전주 대비 증감")
        cols = st.columns(len(flow_types))
        for i, flow in enumerate(flow_types):
            cols[i].metric(flow, deltas.get(flow, 0))

# ----------------------------
# 흐름 선택
# ----------------------------
selected_flow = st.radio("상세 보기 흐름 선택", flow_types, horizontal=True)

flow_df = filtered_df[filtered_df["verdict"] == selected_flow]

# ----------------------------
# 상세 테이블
# ----------------------------
st.subheader(f"상세 현황 · {selected_flow}")
st.dataframe(flow_df, use_container_width=True)

# ----------------------------
# 엑셀 다운로드
# ----------------------------
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="상세현황")
    return output.getvalue()

excel_data = to_excel(flow_df)

st.download_button(
    label="엑셀 다운로드",
    data=excel_data,
    file_name=f"상세현황_{selected_flow}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
