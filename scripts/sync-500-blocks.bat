@echo off
chcp 65001 >nul
echo ========================================
echo    USDT Chain Data Indexer - 500 Blocks Sync
echo ========================================
echo.
echo Starting sync program...
echo Target: Sync latest 500 blocks data
echo Monitor: Real-time performance monitoring
echo.

cd /d "%~dp0.."

echo Checking environment...
if not exist ".env" (
    echo Error: .env config file not found
    echo Please ensure .env file exists and configured correctly
    pause
    exit /b 1
)

echo Checking Go environment...
go version >nul 2>&1
if errorlevel 1 (
    echo Error: Go environment not found
    echo Please ensure Go is installed and added to PATH
    pause
    exit /b 1
)

echo Checking dependencies...
if not exist "go.mod" (
    echo Error: go.mod file not found
    echo Please ensure running in correct project directory
    pause
    exit /b 1
)

echo.
echo ========================================
echo Starting 500 blocks sync...
echo ========================================
echo.

go run monitored_sync.go

echo.
echo ========================================
echo Sync program completed
echo ========================================
echo.
echo Press any key to exit...
pause >nul