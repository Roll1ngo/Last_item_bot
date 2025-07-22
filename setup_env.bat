@echo off
chcp 65001 >nul
setlocal
:: Определение текущей директории
set "CURRENT_DIR=%~dp0"

:: Установка виртуального окружения в текущей директории
echo Установка виртуального окружения...
python -m venv "%CURRENT_DIR%venv"

:: Проверка существования виртуального окружения
if exist "%CURRENT_DIR%venv\Scripts\activate" (
    echo Виртуальное окружение создано успешно.
) else (
    echo Ошибка при создании виртуального окружения.
    exit /b 1
)

:: Активация виртуального окружения
echo Активация виртуального окружения...
call "%CURRENT_DIR%venv\Scripts\activate"

:: Обновление pip
echo Обновление pip...
python.exe -m pip install --upgrade pip

:: Проверка наличия файла requirements.txt
if not exist "%CURRENT_DIR%requirements.txt" (
    echo Файл requirements.txt не найден. Установка остановлена.
    exit /b 1
)

:: Установка модулей из requirements.txt
echo Установка модулей из requirements.txt...
pip install -r "%CURRENT_DIR%requirements.txt"

:: Проверка наличия playwright в requirements.txt
findstr /i "playwright" "%CURRENT_DIR%requirements.txt" >nul
if %errorlevel% equ 0 (
    echo Установка Chromium для Playwright...
    python -m playwright install chromium
) else (
    echo Playwright не найден в requirements.txt, пропускаем установку Chromium.
)

echo Установка завершена.
