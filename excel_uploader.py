import streamlit as st
import pandas as pd

st.title("엑셀 파일 업로드")

uploaded_file = st.file_uploader(
    "엑셀 파일을 선택하세요",
    type=["xlsx", "xls"]
)

if uploaded_file is not None:
    df = pd.read_excel(uploaded_file)
    st.dataframe(df)
