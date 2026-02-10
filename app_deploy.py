import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

st.title("Google Sheet 접근 테스트")

try:
    # 1. Secrets에서 서비스 계정 정보 가져오기
    service_account_info = st.secrets["google_service_account"]

    # 2. 인증 범위 설정
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    # 3. Credentials 생성
    credentials = Credentials.from_service_account_info(
        service_account_info,
        scopes=scopes
    )

    # 4. gspread 클라이언트 생성
    gc = gspread.authorize(credentials)

    # 5. 테스트할 스프레드시트 열기
    spreadsheet_id = st.secrets["BASE_SPREADSHEET_ID"]
    sh = gc.open_by_key(spreadsheet_id)

    # 6. 첫 번째 시트 + 첫 번째 셀 읽기
    worksheet = sh.sheet1
    value = worksheet.cell(1, 1).value

    st.success("시트 접근 성공")
    st.write("A1 셀 값:", value)

except Exception as e:
    st.error("시트 접근 실패")
    st.code(str(e))
