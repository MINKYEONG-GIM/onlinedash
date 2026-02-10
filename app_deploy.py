import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

# =========================
# 설정
# =========================
SPREADSHEET_ID = "1CMYhX0SDGfhBs-jMv4OcRC3qrHDRL-7LtCt8McDkrns"

# =========================
# Google 인증
# =========================
def get_creds():
    return Credentials.from_service_account_info(
        dict(st.secrets["google_service_account"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )

st.title("Google Sheet 접근 테스트")

try:
    creds = get_creds()
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.get_worksheet(0)

    st.success("시트 접근 성공")
    st.write("시트 제목:", sh.title)
    st.write("첫 번째 시트 이름:", ws.title)
    st.write("첫 번째 행:", ws.row_values(1))

except Exception as e:
    st.error("시트 접근 실패")
    st.code(str(e))
