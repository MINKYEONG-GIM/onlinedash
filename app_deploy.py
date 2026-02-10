import streamlit as st
import gspread
import traceback
from google.oauth2.service_account import Credentials

st.title("Google Sheet 접근 상세 진단")

st.write("### 1. Secrets 로딩 상태")

try:
    service_account_info = st.secrets["google_service_account"]
    st.success("google_service_account 로딩 성공")
except Exception as e:
    st.error("google_service_account 로딩 실패")
    st.code(traceback.format_exc())
    st.stop()

try:
    spreadsheet_id = st.secrets["BASE_SPREADSHEET_ID"]
    st.success("BASE_SPREADSHEET_ID 로딩 성공")
    st.write("Spreadsheet ID:", spreadsheet_id)
except Exception as e:
    st.error("BASE_SPREADSHEET_ID 로딩 실패")
    st.code(traceback.format_exc())
    st.stop()

st.write("---")
st.write("### 2. Credentials 생성")

try:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    credentials = Credentials.from_service_account_info(
        service_account_info,
        scopes=scopes
    )

    st.success("Credentials 생성 성공")
    st.write("Service Account Email:", service_account_info.get("client_email"))

except Exception:
    st.error("Credentials 생성 실패")
    st.code(traceback.format_exc())
    st.stop()

st.write("---")
st.write("### 3. gspread 클라이언트 생성")

try:
    gc = gspread.authorize(credentials)
    st.success("gspread 클라이언트 생성 성공")
except Exception:
    st.error("gspread 클라이언트 생성 실패")
    st.code(traceback.format_exc())
    st.stop()

st.write("---")
st.write("### 4. 스프레드시트 열기")

try:
    sh = gc.open_by_key(spreadsheet_id)
    st.success("스프레드시트 열기 성공")
    st.write("Spreadsheet title:", sh.title)
except Exception:
    st.error("스프레드시트 열기 실패")
    st.code(traceback.format_exc())
    st.stop()

st.write("---")
st.write("### 5. 시트 데이터 읽기")

try:
    worksheet = sh.sheet1
    value = worksheet.cell(1, 1).value

    st.success("시트 데이터 읽기 성공")
    st.write("A1 셀 값:", value)

except Exception:
    st.error("시트 데이터 읽기 실패")
    st.code(traceback.format_exc())
