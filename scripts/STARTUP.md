# 项目启动指南

本项目提供了多种启动方式，请根据您的操作系统和需求选择合适的启动脚本。

## 🚀 快速启动

### Windows 用户

#### 方式一：简单批处理脚本（推荐）
```cmd
scripts\start-simple.bat
```

#### 方式二：手动启动
```powershell
# 1. 编译项目
go build -o main.exe .

# 2. 启动前端服务器（可选）
start python frontend\server.py

# 3. 启动主服务
.\main.exe
```

### Linux/macOS 用户

#### 方式一：使用 Bash 脚本（推荐）
```bash
# 添加执行权限（首次运行）
chmod +x scripts/start.sh

# 启动项目
./scripts/start.sh
```

#### 方式二：使用 Docker Compose
```bash
# 添加执行权限（首次运行）
chmod +x scripts/start-docker.sh

# 启动项目
./scripts/start-docker.sh
```

## 📋 启动脚本功能

### 自动检查项目
- ✅ Go 环境检查（要求 Go 1.19+）
- ✅ 环境配置文件检查（.env）
- ✅ 数据库连接检查
- ✅ Python 环境检查（前端服务器）

### 自动安装和编译
- 📦 自动下载 Go 依赖
- 🔨 自动编译项目
- 🚀 自动启动前端和后端服务

### 服务端口
- **前端界面**: http://localhost:8000
- **API 接口**: http://localhost:8080
- **数据库**: localhost:5432 (PostgreSQL)
- **缓存**: localhost:6379 (Redis)

## ⚙️ 环境配置

### 首次运行
1. 脚本会自动复制 `.env.example` 为 `.env`
2. 请编辑 `.env` 文件，配置以下关键参数：
   ```env
   # 数据库配置
   DB_HOST=localhost
   DB_PORT=5432
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_NAME=tron_indexer
   
   # TRON 节点配置
   TRON_NODE_URL=https://api.trongrid.io
   
   # Redis 配置
   REDIS_HOST=localhost
   REDIS_PORT=6379
   ```

### 依赖要求

#### 本地运行
- Go 1.19+
- PostgreSQL 12+
- Redis 6+
- Python 3.7+ (可选，用于前端服务器)

#### Docker 运行
- Docker 20.10+
- Docker Compose 1.29+

## 🛠️ 故障排除

### 常见问题

1. **权限错误**
   - Windows: 以管理员身份运行命令提示符
   - Linux/macOS: 使用 `chmod +x start.sh` 添加执行权限

2. **端口占用**
   ```bash
   # 检查端口占用
   netstat -ano | findstr :8080
   netstat -ano | findstr :8000
   ```

3. **环境变量未设置**
   - 确保 `.env` 文件存在且配置正确
   - 检查数据库连接参数

4. **依赖缺失**
   ```bash
   # 重新安装依赖
   go mod download
   go mod tidy
   ```

### 手动启动

如果自动脚本失败，可以手动启动：

```bash
# 1. 安装依赖
go mod download
go mod tidy

# 2. 编译项目
go build -o indexer .

# 3. 启动服务
./indexer  # Linux/macOS
indexer.exe  # Windows
```

## 🔧 开发模式

### 热重载开发
```bash
# 安装 air（Go 热重载工具）
go install github.com/cosmtrek/air@latest

# 启动热重载
./scripts/start-dev.sh
```

### 调试模式
```bash
# 设置调试级别
export LOG_LEVEL=debug  # Linux/macOS
set LOG_LEVEL=debug     # Windows

# 启动项目
./scripts/start.sh
```

## 📊 监控和日志

### 日志文件位置
- 应用日志: `logs/app.log`
- 错误日志: `logs/error.log`
- 访问日志: `logs/access.log`

### 性能监控
- 访问 http://localhost:8080/metrics 查看性能指标
- 前端界面提供实时 TPS、延迟等监控数据

## 🚪 停止服务

- **交互式停止**: 按 `Ctrl+C`
- **Docker 停止**: `docker-compose down`
- **强制停止**: 关闭终端窗口

---

如有问题，请查看项目 [README.md](./README.md) 或提交 Issue。