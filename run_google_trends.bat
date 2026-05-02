@echo off
setlocal

cd /d C:\Jon\ML

set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

if not exist "5_output\logs" mkdir "5_output\logs"

set "LOG_FILE=5_output\logs\google_trends_%date:~-4%%date:~3,2%%date:~0,2%.log"
echo [%date% %time%] Starting google_trends.py >> "%LOG_FILE%"

".\.venv\Scripts\python.exe" "google_trends.py" >> "%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo [%date% %time%] Finished with exit code %EXIT_CODE% >> "%LOG_FILE%"
exit /b %EXIT_CODE%
