@echo off
chcp 65001 >nul
echo ===================================
echo TRON USDT 索引器启动脚本
echo ===================================

echo [1/4] 检查 Go 环境...
go version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到 Go 环境
    pause
    exit /b 1
) else (
    echo Go 环境检查通过
)

echo [2/4] 检查项目文件...
if not exist "main.go" (
    echo 错误: 未找到 main.go 文件
    pause
    exit /b 1
) else (
    echo 项目文件检查通过
)

echo [3/4] 编译项目...
go build -o main.exe .
if errorlevel 1 (
    echo 错误: 项目编译失败
    pause
    exit /b 1
) else (
    echo 项目编译成功
)

echo [4/4] 启动服务...
echo 正在启动 TRON USDT 索引器...
echo 主服务地址: http://localhost:8080
echo 前端服务地址: http://localhost:8000
echo 按 Ctrl+C 停止服务
echo ===================================

start /min python frontend\server.py
main.exe

echo 清理临时文件...
if exist "main.exe" del main.exe
echo 服务已停止
pause