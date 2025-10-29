@echo off
setlocal
cd /d "%~dp0"

REM --- Поиск установленного Python ---
set "PYTHON_CMD="
for %%P in (python.exe python3.exe) do (
    where %%P >nul 2>&1
    if not errorlevel 1 (
        set "PYTHON_CMD=%%P"
        goto :found_python
    )
)

where py.exe >nul 2>&1
if not errorlevel 1 (
    for /f "usebackq delims=" %%P in (`py -3 -c "import sys; print(sys.executable)"`) do (
        set "PYTHON_CMD=%%P"
    )
    if defined PYTHON_CMD goto :found_python
)

echo.
echo [ERROR] Python 3.10+ не найден. Установите Python с https://www.python.org/ и повторите попытку.
pause
exit /b 1

:found_python
echo.
echo Используется Python: %PYTHON_CMD%

"%PYTHON_CMD%" -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" || (
    echo.
    echo [ERROR] Требуется Python версии 3.10 или новее.
    pause
    exit /b 1
)

REM --- Создание и активация виртуального окружения ---
if exist venv (
    if not exist venv\Scripts\python.exe (
        echo.
        echo Обнаружено повреждённое виртуальное окружение, удаляем...
        rmdir /s /q venv || goto :fail
    )
)

if not exist venv (
    echo.
    echo Создаётся виртуальное окружение...
    "%PYTHON_CMD%" -m venv venv >nul 2>&1
    if errorlevel 1 (
        echo.
        echo Обнаружена ошибка ensurepip при создании venv, пробуем fallback через virtualenv...
        "%PYTHON_CMD%" -m pip --version >nul 2>&1 || "%PYTHON_CMD%" -m ensurepip --default-pip || goto :fail
        "%PYTHON_CMD%" -m pip install --user --upgrade virtualenv || goto :fail
        "%PYTHON_CMD%" -m virtualenv venv || goto :fail
    )
)

call venv\Scripts\activate

REM --- Установка зависимостей ---
echo.
echo Обновление pip и установка зависимостей...
python -m pip install --upgrade pip || goto :fail
if exist requirements.txt (
    python -m pip install -r requirements.txt || goto :fail
) else (
    python -m pip install telethon PySocks || goto :fail
)

REM --- Запуск бота ---
echo.
echo Запуск бота...
python -X dev tg_manager_bot_dynamic.py
set "EXIT_CODE=%ERRORLEVEL%"

echo.
echo ===== ПРОГРАММА ЗАВЕРШЕНА ===== (код %EXIT_CODE%)
echo Смотри вывод выше или файл bot.log
pause
exit /b %EXIT_CODE%

:fail
echo.
echo [ERROR] Не удалось подготовить окружение. Проверь подключение к интернету и повтори попытку.
pause
exit /b 1