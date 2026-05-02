@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d C:\Jon\ML

set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

if not exist "5_output\logs" mkdir "5_output\logs"

set "TIME_STAMP=%time: =0%"
set "TIME_STAMP=%TIME_STAMP::=%"
set "TIME_STAMP=%TIME_STAMP:,=%"
set "TIME_STAMP=%TIME_STAMP:.=%"
set "LOG_FILE=5_output\logs\ml_pipeline_%date:~-4%%date:~3,2%%date:~0,2%_%TIME_STAMP%_%RANDOM%.log"
echo [%date% %time%] Starting ML pipeline >> "%LOG_FILE%"
echo Starting ML pipeline.
echo Log: %LOG_FILE%
echo.

for %%S in (
    "config popup editor.py"
    "cleaning_script.py"
    "diagnostics.py"
    "XGBoost - feature - tranformation.py"
    "linear_baseline.py"
    "XGBoost - main code.py"
) do (
    set "SCRIPT=%%~S"
    echo [!date! !time!] Running !SCRIPT! >> "!LOG_FILE!"
    echo Running !SCRIPT! ...
    ".\.venv\Scripts\python.exe" "!SCRIPT!" >> "!LOG_FILE!" 2>&1
    set "STEP_EXIT_CODE=!ERRORLEVEL!"
    if not "!STEP_EXIT_CODE!"=="0" (
        echo [!date! !time!] FAILED !SCRIPT! with exit code !STEP_EXIT_CODE! >> "!LOG_FILE!"
        echo FAILED !SCRIPT! with exit code !STEP_EXIT_CODE!.
        echo ML pipeline failed with exit code !STEP_EXIT_CODE!. Log: !LOG_FILE!
        echo.
        echo Last log lines:
        powershell -NoProfile -Command "Get-Content -LiteralPath '!LOG_FILE!' -Tail 40"
        pause
        exit /b !STEP_EXIT_CODE!
    )
    echo [!date! !time!] Completed !SCRIPT! >> "!LOG_FILE!"
    echo Completed !SCRIPT!.
    echo.
)

echo [%date% %time%] ML pipeline finished successfully >> "%LOG_FILE%"
echo ML pipeline finished successfully. Log: %LOG_FILE%
pause
exit /b 0
