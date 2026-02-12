# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread

st.set_page_config(page_title="EB 시트 연결 테스트", layout="wide")

# ------------------------
# Secrets
# ------------------------
def _secret(key, default=""):
    try:
        return st.secrets.get(key, default) or default
    except Exception:
        return default

EB_SPREADSHEET_ID = _secret("EB_SPREADSHEET_ID")

st.write("### EB_SPREADSHEET_ID 테스트")
st.write("Spreadsheet ID:", EB_SPREADSHEET_ID)

# ------------------------
# 구글 인증
# ------------------------
def get_gsheet_client():
    try:
        creds_info = st.secrets["gcp_service_account"]  # 서비스 계정 JSON
        creds = Credentials.from_service_account_info(creds_info, scopes=[
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly"
        ])
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Google 인증 실패: {e}")
        return None

client = get_gsheet_client()

# ------------------------
# EB 시트 읽기
# ------------------------
if client:
    try:
        sh = client.open_by_key(EB_SPREADSHEET_ID)
        worksheet = sh.sheet1  # 첫 번째 워크시트
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        st.success("EB 시트 연결 성공!")
        st.dataframe(df.head(10))  # 상위 10행만 표시
    except Exception as e:
        st.error(f"EB 시트 읽기 실패: {e}")
