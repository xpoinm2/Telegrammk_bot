@echo off
setlocal
cd /d "%~dp0"

set PY311="C:\Users\User\AppData\Local\Programs\Python\Python311\python.exe"

REM если есть venv — активируем
if exist venv\Scripts\activate call venv\Scripts\activate

REM проверим нужные пакеты и при необходимости установим
%PY311% -m pip show telethon >nul 2>&1 || %PY311% -m pip install telethon
%PY311% -m pip show PySocks  >nul 2>&1 || %PY311% -m pip install PySocks

REM запуск
%PY311% -X dev tg_manager_bot_dynamic.py

echo.
echo ===== ПРОГРАММА ЗАВЕРШЕНА =====
echo Смотри вывод выше или файл bot.log
pause
