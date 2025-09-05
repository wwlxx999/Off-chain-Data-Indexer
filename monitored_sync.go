package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"Off-chainDatainDexer/blockchain"
	"Off-chainDatainDexer/config"
	"Off-chainDatainDexer/database"
	"Off-chainDatainDexer/services"
	"Off-chainDatainDexer/utils"
)

// 系统性能监控器
type PerformanceMonitor struct {
	mu                sync.RWMutex
	lastGoroutines    int
	lastMemory        float64
	lastGCCount       uint32
	stuckCounter      int
	maxStuckCount     int
	memoryThreshold   float64 // MB
	goroutineThreshold int
	running           bool
	cancel            context.CancelFunc
}

// 创建性能监控器
func NewPerformanceMonitor() *PerformanceMonitor {
	return &PerformanceMonitor{
		maxStuckCount:     5,  // 连续5次检测到异常就退出
		memoryThreshold:   100, // 内存超过100MB认为异常
		goroutineThreshold: 50,  // goroutine超过50个认为异常
		running:           false,
	}
}

// 启动监控
func (pm *PerformanceMonitor) Start(ctx context.Context) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	if pm.running {
		return
	}
	
	ctx, cancel := context.WithCancel(ctx)
	pm.cancel = cancel
	pm.running = true
	
	go pm.monitorLoop(ctx)
}

// 停止监控
func (pm *PerformanceMonitor) Stop() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	if !pm.running {
		return
	}
	
	if pm.cancel != nil {
		pm.cancel()
	}
	pm.running = false
}

// 监控循环
func (pm *PerformanceMonitor) monitorLoop(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if pm.checkSystemHealth() {
				log.Printf("⚠️ 检测到系统卡死风险，准备退出进程...")
				os.Exit(1)
			}
		}
	}
}

// 检查系统健康状态
func (pm *PerformanceMonitor) checkSystemHealth() bool {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	currentGoroutines := runtime.NumGoroutine()
	currentMemory := float64(m.Alloc) / 1024 / 1024
	currentGCCount := m.NumGC
	
	// 打印当前状态
	fmt.Printf("[监控] 时间: %s | Goroutines: %d | 内存: %.2f MB | GC次数: %d\n",
		time.Now().Format("15:04:05"),
		currentGoroutines,
		currentMemory,
		currentGCCount)
	
	// 检查是否有异常
	isStuck := false
	
	// 检查内存使用
	if currentMemory > pm.memoryThreshold {
		log.Printf("⚠️ 内存使用过高: %.2f MB (阈值: %.2f MB)", currentMemory, pm.memoryThreshold)
		isStuck = true
	}
	
	// 检查goroutine数量
	if currentGoroutines > pm.goroutineThreshold {
		log.Printf("⚠️ Goroutine数量过多: %d (阈值: %d)", currentGoroutines, pm.goroutineThreshold)
		isStuck = true
	}
	
	// 检查是否卡死（goroutine和内存都没有变化）
	if pm.lastGoroutines > 0 && pm.lastMemory > 0 {
		if currentGoroutines == pm.lastGoroutines && 
		   abs(currentMemory - pm.lastMemory) < 0.1 && 
		   currentGCCount == pm.lastGCCount {
			pm.stuckCounter++
			log.Printf("⚠️ 系统可能卡死 (连续 %d 次无变化)", pm.stuckCounter)
			
			if pm.stuckCounter >= pm.maxStuckCount {
				isStuck = true
			}
		} else {
			pm.stuckCounter = 0
		}
	}
	
	// 更新历史数据
	pm.lastGoroutines = currentGoroutines
	pm.lastMemory = currentMemory
	pm.lastGCCount = currentGCCount
	
	return isStuck
}

// 辅助函数：计算绝对值
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func main() {
	fmt.Println("=== 多节点系统监控同步开始 ===")
	fmt.Println("目标: 同步300个区块")
	fmt.Println("监控: 实时性能监控，检测卡死风险")
	fmt.Println("========================================")
	
	// 初始化日志系统
	logLevel := utils.INFO
	if err := utils.InitGlobalLogger(logLevel, "logs/app.log"); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer func() {
		if utils.GlobalLogger != nil {
			utils.GlobalLogger.Close()
		}
	}()
	
	// 初始化错误处理器
	utils.InitGlobalErrorHandler(utils.GlobalLogger)
	
	// 加载配置
	cfg := config.LoadConfig()
	utils.Info("Configuration loaded successfully")
	
	// 初始化数据库
	database.InitDB(cfg)
	utils.Info("Database initialized successfully")
	defer func() {
		if db := database.GetDB(); db != nil {
			if sqlDB, err := db.DB(); err == nil {
				sqlDB.Close()
			}
		}
	}()
	
	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// 创建性能监控器
	monitor := NewPerformanceMonitor()
	monitor.Start(ctx)
	defer monitor.Stop()
	
	// 设置信号处理
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	go func() {
		<-sigChan
		fmt.Println("\n收到退出信号，正在安全关闭...")
		cancel()
	}()
	
	// 初始化TronScan客户端
	tronScanClient := blockchain.NewTronScanClient(cfg.TronScanConfig, utils.GlobalLogger)
	if tronScanClient != nil && tronScanClient.IsEnabled() {
		fmt.Printf("\n=== TronScan API测试 ===\n")
		fmt.Printf("API端点: %s\n", cfg.TronScanConfig.APIURL)
		fmt.Printf("可用API密钥数量: %d\n", tronScanClient.GetAPIKeyCount())
		
		// 测试获取最新区块
		latestBlockInfo, err := tronScanClient.GetLatestBlock()
		if err != nil {
			fmt.Printf("❌ TronScan API测试失败: %v\n", err)
		} else {
			fmt.Printf("✅ TronScan API测试成功!\n")
			fmt.Printf("最新区块号: %d\n", latestBlockInfo.BlockHeader.RawData.Number)
			fmt.Printf("区块时间: %s\n", time.Unix(latestBlockInfo.BlockHeader.RawData.Timestamp/1000, 0).Format("2006-01-02 15:04:05"))
			fmt.Printf("交易数量: %d\n", len(latestBlockInfo.Transactions))
		}
		fmt.Println("========================")
	} else {
		fmt.Println("\n⚠️ TronScan API未启用或配置不完整")
	}

	// 创建同步服务
	syncService, err := services.NewSyncService(database.GetDB())
	if err != nil {
		log.Fatalf("Failed to create sync service: %v", err)
	}
	
	// 获取当前最新区块号
	latestBlock, err := syncService.GetLatestBlockNumber()
	if err != nil {
		log.Fatalf("获取最新区块号失败: %v", err)
	}
	
	// 计算同步范围
	startBlock := latestBlock - 300 + 1
	endBlock := latestBlock
	
	fmt.Printf("同步范围: %d - %d (共300个区块)\n", startBlock, endBlock)
	fmt.Printf("开始时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println("========================================")
	
	// 记录开始时间
	startTime := time.Now()
	
	// 启动同步
	err = syncService.SyncBlockRange(ctx, startBlock, endBlock)
	if err != nil {
		if ctx.Err() != nil {
			fmt.Println("\n同步被用户中断")
		} else {
			log.Printf("同步失败: %v", err)
		}
	} else {
		fmt.Println("\n✅ 同步完成！")
	}
	
	// 计算总耗时
	duration := time.Since(startTime)
	
	// 最终统计
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	fmt.Println("\n========================================")
	fmt.Println("=== 同步完成统计 ===")
	fmt.Printf("总耗时: %v\n", duration)
	fmt.Printf("平均每区块: %v\n", duration/300)
	fmt.Printf("最终Goroutines: %d\n", runtime.NumGoroutine())
	fmt.Printf("最终内存使用: %.2f MB\n", float64(m.Alloc)/1024/1024)
	fmt.Printf("总GC次数: %d\n", m.NumGC)
	fmt.Println("========================================")
	
	// 检查是否有异常
	if runtime.NumGoroutine() > 10 {
		fmt.Printf("⚠️ 检测到可能的goroutine泄漏: %d\n", runtime.NumGoroutine())
	}
	
	if float64(m.Alloc)/1024/1024 > 50 {
		fmt.Printf("⚠️ 内存使用较高: %.2f MB\n", float64(m.Alloc)/1024/1024)
	}
	
	fmt.Println("\n程序正常退出")
}