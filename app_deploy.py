import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

st.title("EB 시트 데이터 확인")

EB_SPREADSHEET_ID = "1iMp6PyOMRt9xNoSJKDDZHbJ4Y8DZh1-YM8P_GDg-0ew"

# Google 인증
creds_info = st.secrets["google_service_account"]
creds = Credentials.from_service_account_info(
    creds_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly"
    ]
)
client = gspread.authorize(creds)

# 시트 열기
sh = client.open_by_key(EB_SPREADSHEET_ID)
worksheet = sh.sheet1  # 시트1
st.write("워크시트 이름:", worksheet.title)

# 모든 값 읽기
values = worksheet.get_all_values()
if not values:
    st.warning("시트에 데이터가 없습니다.")
else:
    st.success(f"데이터 {len(values)}행 읽기 성공!")
    df = pd.DataFrame(values[1:], columns=values[0])  # 첫 행을 헤더로 사용
    st.dataframe(df)
