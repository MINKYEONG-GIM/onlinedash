import gspread
from google.oauth2.service_account import Credentials
import streamlit as st

creds = Credentials.from_service_account_info(
    dict(st.secrets["google_service_account"]),
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
)

gc = gspread.authorize(creds)
sh = gc.open_by_key(st.secrets["BASE_SPREADSHEET_ID"])
st.write(sh.title)
