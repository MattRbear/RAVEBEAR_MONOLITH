@echo off
REM ═══════════════════════════════════════════════════════════════════
REM  RAVEBEAR MONOLITH - Live Collection + Processing Bootstrap
REM ═══════════════════════════════════════════════════════════════════
REM  Double-click this file to start the live data pipeline.
REM  Press Ctrl+C to stop gracefully.
REM ═══════════════════════════════════════════════════════════════════

echo.
echo  ██████╗  █████╗ ██╗   ██╗███████╗██████╗ ███████╗ █████╗ ██████╗
echo  ██╔══██╗██╔══██╗██║   ██║██╔════╝██╔══██╗██╔════╝██╔══██╗██╔══██╗
echo  ██████╔╝███████║██║   ██║█████╗  ██████╔╝█████╗  ███████║██████╔╝
echo  ██╔══██╗██╔══██║╚██╗ ██╔╝██╔══╝  ██╔══██╗██╔══╝  ██╔══██║██╔══██╗
echo  ██║  ██║██║  ██║ ╚████╔╝ ███████╗██████╔╝███████╗██║  ██║██║  ██║
echo  ╚═╝  ╚═╝╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝
echo.
echo  Live Collection + Processing Pipeline
echo  ═══════════════════════════════════════════════════════════════════
echo.

REM Change to script directory
cd /d "%~dp0"

REM Check if poetry is available
where poetry >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Poetry not found. Please install Poetry first.
    echo         https://python-poetry.org/docs/#installation
    pause
    exit /b 1
)

REM Check if config exists
if not exist "config\settings.yaml" (
    echo [WARNING] config\settings.yaml not found, using defaults.
)

echo [INFO] Starting RAVEBEAR MONOLITH...
echo [INFO] Mode: live-with-processing
echo [INFO] Press Ctrl+C to stop gracefully.
echo.

REM Run the monolith
poetry run monolith --mode live-with-processing --cursor-name default --poll-interval-s 1.0

REM Capture exit code
set EXIT_CODE=%ERRORLEVEL%

echo.
if %EXIT_CODE%==130 (
    echo [INFO] Shutdown complete (user interrupt).
) else if %EXIT_CODE%==0 (
    echo [INFO] Shutdown complete.
) else (
    echo [WARNING] Exited with code: %EXIT_CODE%
)

pause
