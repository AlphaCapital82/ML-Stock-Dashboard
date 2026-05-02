@echo off
cd /d C:\Jon\ML
".\.venv\Scripts\python.exe" -m streamlit run streamlit_secondary_stock_research_dashboard.py --server.port 8502 --server.headless true
pause
