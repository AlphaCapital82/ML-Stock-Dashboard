@echo off
setlocal

cd /d C:\Jon\ML

set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

".\.venv\Scripts\python.exe" -m streamlit run streamlit_dashboard.py

pause
