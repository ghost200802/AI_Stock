@echo off
chcp 65001 >nul
echo Killing streamlit processes...
taskkill /F /IM streamlit.exe 2>nul
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :8501 ^| findstr LISTENING') do (
    taskkill /F /PID %%a 2>nul
)
timeout /t 2 /nobreak >nul
echo Starting streamlit server...
cd /d "%~dp0"
python scripts\run_visualizer.py
pause
