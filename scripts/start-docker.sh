#!/bin/bash
# TRON USDT 索引器 Docker 启动脚本

set -e

echo "=== TRON USDT 索引器 Docker 启动脚本 ==="

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# 检查 Docker 环境
echo -e "\n${CYAN}1. 检查 Docker 环境...${NC}"
if command -v docker &> /dev/null; then
    DOCKER_VERSION=$(docker --version)
    echo -e "${GREEN}✅ Docker 环境正常: $DOCKER_VERSION${NC}"
else
    echo -e "${RED}❌ 未找到 Docker，请先安装 Docker${NC}"
    exit 1
fi

if command -v docker-compose &> /dev/null; then
    COMPOSE_VERSION=$(docker-compose --version)
    echo -e "${GREEN}✅ Docker Compose 环境正常: $COMPOSE_VERSION${NC}"
else
    echo -e "${RED}❌ 未找到 Docker Compose，请先安装 Docker Compose${NC}"
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
    fi
fi

# 检查 docker-compose.yml
echo -e "\n${CYAN}3. 检查 Docker Compose 配置...${NC}"
if [ -f "docker-compose.yml" ]; then
    echo -e "${GREEN}✅ 找到 Docker Compose 配置文件${NC}"
else
    echo -e "${RED}❌ 未找到 docker-compose.yml 文件${NC}"
    exit 1
fi

# 清理旧容器（可选）
echo -e "\n${CYAN}4. 清理旧容器...${NC}"
read -p "是否清理旧容器和镜像？(y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}正在停止并删除旧容器...${NC}"
    docker-compose down --remove-orphans
    echo -e "${YELLOW}正在删除旧镜像...${NC}"
    docker-compose down --rmi all --volumes --remove-orphans 2>/dev/null || true
    echo -e "${GREEN}✅ 清理完成${NC}"
fi

# 构建镜像
echo -e "\n${CYAN}5. 构建 Docker 镜像...${NC}"
docker-compose build --no-cache
echo -e "${GREEN}✅ 镜像构建完成${NC}"

# 启动服务
echo -e "\n${CYAN}6. 启动服务...${NC}"
echo -e "${YELLOW}正在启动所有服务...${NC}"
echo -e "${YELLOW}前端界面: http://localhost:8000${NC}"
echo -e "${YELLOW}API 接口: http://localhost:8080${NC}"
echo -e "${YELLOW}数据库: localhost:5432${NC}"
echo -e "${YELLOW}Redis: localhost:6379${NC}"
echo -e "${YELLOW}按 Ctrl+C 停止服务${NC}"
echo ""

# 清理函数
cleanup() {
    echo -e "\n${YELLOW}正在停止服务...${NC}"
    docker-compose down
    echo -e "${YELLOW}=== 服务已停止 ===${NC}"
    exit 0
}

# 设置信号处理
trap cleanup SIGINT SIGTERM

# 启动服务
docker-compose up

# 如果服务退出，清理资源
cleanup