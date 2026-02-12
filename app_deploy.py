import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

st.title("EB 시트 연결 테스트")

EB_SPREADSHEET_ID = "1iMp6PyOMRt9xNoSJKDDZHbJ4Y8DZh1-YM8P_GDg-0ew"
st.write("Spreadsheet ID:", EB_SPREADSHEET_ID)

# =========================
# Google 인증
# =========================
try:
    creds_info = st.secrets["google_service_account"]  # secrets key 확인
    creds = Credentials.from_service_account_info(creds_info, scopes=[
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly"
    ])
    client = gspread.authorize(creds)
    st.success("Google 인증 성공!")
except Exception as e:
    st.error(f"Google 인증 실패: {e}")
    client = None

# =========================
# EB 시트 읽기
# =========================
if client:
    try:
        sh = client.open_by_key(EB_SPREADSHEET_ID)
        st.write("워크시트 목록:", [ws.title for ws in sh.worksheets()])
        worksheet = sh.sheet1  # 첫 워크시트
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        st.success("EB 시트 읽기 성공!")
        st.dataframe(df.head(10))
    except Exception as e:
        st.error(f"EB 시트 읽기 실패: {e}")
