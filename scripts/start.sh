#!/bin/bash
# TRON USDT 索引器启动脚本
# 适用于 Linux/macOS 环境

set -e  # 遇到错误立即退出

echo "=== TRON USDT 索引器启动脚本 ==="
echo "正在检查环境和启动服务..."

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 检查 Go 环境
echo -e "\n${CYAN}1. 检查 Go 环境...${NC}"
if command -v go &> /dev/null; then
    GO_VERSION=$(go version)
    echo -e "${GREEN}✅ Go 环境正常: $GO_VERSION${NC}"
else
    echo -e "${RED}❌ 未找到 Go 环境，请先安装 Go 1.19+${NC}"
    exit 1
fi

# 检查环境配置
echo -e "\n${CYAN}2. 检查环境配置...${NC}"
if [ -f ".env" ]; then
    echo -e "${GREEN}✅ 找到环境配置文件${NC}"
else
    echo -e "${YELLOW}⚠️  未找到 .env 文件，正在创建...${NC}"
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo -e "${YELLOW}已创建 .env 文件，请编辑配置后重新运行${NC}"
        echo -e "${YELLOW}编辑命令: nano .env 或 vim .env${NC}"
        exit 1
    else
        echo -e "${RED}❌ 未找到 .env.example 文件${NC}"
        exit 1
    fi
fi

# 检查 PostgreSQL（可选）
echo -e "\n${CYAN}3. 检查数据库连接...${NC}"
if command -v psql &> /dev/null; then
    echo -e "${GREEN}✅ PostgreSQL 客户端已安装${NC}"
else
    echo -e "${YELLOW}⚠️  未找到 PostgreSQL 客户端，请确保数据库服务正常运行${NC}"
fi

# 安装依赖
echo -e "\n${CYAN}4. 安装 Go 依赖...${NC}"
go mod download
go mod tidy
echo -e "${GREEN}✅ 依赖安装完成${NC}"

# 编译项目
echo -e "\n${CYAN}5. 编译项目...${NC}"
go build -o indexer .
echo -e "${GREEN}✅ 项目编译完成${NC}"

# 启动前端服务器
echo -e "\n${CYAN}6. 启动前端服务器...${NC}"
if command -v python3 &> /dev/null; then
    echo -e "${YELLOW}启动前端服务器 (端口 8000)...${NC}"
    cd frontend
    python3 server.py &
    FRONTEND_PID=$!
    cd ..
    sleep 2
    echo -e "${GREEN}✅ 前端服务器已启动 (PID: $FRONTEND_PID)${NC}"
elif command -v python &> /dev/null; then
    echo -e "${YELLOW}启动前端服务器 (端口 8000)...${NC}"
    cd frontend
    python server.py &
    FRONTEND_PID=$!
    cd ..
    sleep 2
    echo -e "${GREEN}✅ 前端服务器已启动 (PID: $FRONTEND_PID)${NC}"
else
    echo -e "${YELLOW}⚠️  未找到 Python，跳过前端服务器启动${NC}"
    FRONTEND_PID=""
fi

# 清理函数
cleanup() {
    echo -e "\n${YELLOW}正在停止服务...${NC}"
    if [ ! -z "$FRONTEND_PID" ]; then
        kill $FRONTEND_PID 2>/dev/null || true
        echo -e "${YELLOW}前端服务器已停止${NC}"
    fi
    echo -e "${YELLOW}=== 服务已停止 ===${NC}"
    exit 0
}

# 设置信号处理
trap cleanup SIGINT SIGTERM

# 启动主服务
echo -e "\n${CYAN}7. 启动主服务...${NC}"
echo -e "${YELLOW}正在启动 TRON USDT 索引器主服务 (端口 8080)...${NC}"
echo -e "${YELLOW}前端界面: http://localhost:8000${NC}"
echo -e "${YELLOW}API 接口: http://localhost:8080${NC}"
echo -e "${YELLOW}按 Ctrl+C 停止服务${NC}"
echo ""

# 启动主程序
./indexer

# 如果主程序退出，清理资源
cleanup