#!/bin/bash

# 设置脚本在遇到错误时退出
set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   USDT链上数据索引器 - 500区块同步${NC}"
echo -e "${BLUE}========================================${NC}"
echo
echo -e "${GREEN}正在启动同步程序...${NC}"
echo -e "${GREEN}目标: 同步最近500个区块的数据${NC}"
echo -e "${GREEN}监控: 实时性能监控，检测卡死风险${NC}"
echo

# 切换到项目根目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

echo -e "${YELLOW}检查环境配置...${NC}"
if [ ! -f ".env" ]; then
    echo -e "${RED}错误: 未找到 .env 配置文件${NC}"
    echo -e "${RED}请确保 .env 文件存在并配置正确${NC}"
    exit 1
fi

echo -e "${YELLOW}检查Go环境...${NC}"
if ! command -v go &> /dev/null; then
    echo -e "${RED}错误: 未找到Go环境${NC}"
    echo -e "${RED}请确保已安装Go并添加到PATH环境变量${NC}"
    exit 1
fi

echo -e "${YELLOW}检查依赖...${NC}"
if [ ! -f "go.mod" ]; then
    echo -e "${RED}错误: 未找到 go.mod 文件${NC}"
    echo -e "${RED}请确保在正确的项目目录中运行此脚本${NC}"
    exit 1
fi

echo
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}开始执行500区块同步...${NC}"
echo -e "${BLUE}========================================${NC}"
echo

# 运行同步程序
go run monitored_sync.go

echo
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}同步程序执行完成${NC}"
echo -e "${GREEN}========================================${NC}"
echo