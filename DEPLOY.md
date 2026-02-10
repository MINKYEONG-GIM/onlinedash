# 배포 가이드

## Streamlit Cloud

1. GitHub에 저장소 푸시
2. [share.streamlit.io](https://share.streamlit.io) 접속
3. **New app** → GitHub 저장소 선택
4. 설정:
   - **Main file path**: `app_deploy.py`
   - **Branch**: `main` (또는 사용 중인 브랜치)
5. **Deploy** 클릭

### 참고

- DB 폴더의 엑셀은 Git에 포함되지 않음 → 사용자가 앱 실행 후 사이드바에서 업로드
- `requirements.txt`가 자동으로 인식됨
- Python 3.9+ 환경에서 실행됨

## 기타 플랫폼

- **Heroku**, **Railway**, **Render** 등에서도 `streamlit run app_deploy.py` 명령으로 실행 가능
- 포트는 `8501`이 기본값이며, `--server.port` 옵션으로 변경 가능
