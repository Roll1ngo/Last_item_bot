@echo off
chcp 65001 >nul
setlocal

:: Определение текущей директории
set "CURRENT_DIR=%~dp0"

:: Поиск python.exe в папке \venv\Scripts
set "PYTHON_EXE=%CURRENT_DIR%.venv\Scripts\python.exe"

:: Проверка существования python.exe
if exist "%PYTHON_EXE%" (
    echo Python interpreter found: %PYTHON_EXE%
) else (
    echo Python interpreter not found in %CURRENT_DIR%venv\Scripts
    echo Please make sure you have a virtual environment set up.
    pause
    exit /b 1
)

:: Запуск main.py в свернутом окне
start /min "YAML Config Editor" "%PYTHON_EXE%" "%CURRENT_DIR%config_editor.py"

exit
