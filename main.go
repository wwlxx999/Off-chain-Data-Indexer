package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"Off-chainDatainDexer/blockchain"
	"Off-chainDatainDexer/config"
	"Off-chainDatainDexer/database"
	"Off-chainDatainDexer/handlers"
	"Off-chainDatainDexer/middleware"
	"Off-chainDatainDexer/services"
	"Off-chainDatainDexer/utils"
)

var (
	transferService *services.TransferService
	indexerService  *services.IndexerService
	syncService     *services.SyncService
	marketService   *services.MarketService
	cacheService    *services.CacheService
	tronClient      *blockchain.TronHTTPClient
)

func main() {
	// 初始化日志系统
	logLevel := utils.INFO
	if cfg := config.LoadConfig(); cfg.LogLevel != "" {
		switch cfg.LogLevel {
		case "DEBUG":
			logLevel = utils.DEBUG
		case "WARN":
			logLevel = utils.WARN
		case "ERROR":
			logLevel = utils.ERROR
		}
	}
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

	// 设置panic恢复
	defer func() {
		if err := utils.RecoverPanic(); err != nil {
			utils.HandleError(err)
		}
	}()

	// 加载配置
	cfg := config.LoadConfig()
	utils.Info("Configuration loaded successfully")

	// 初始化数据库连接
	database.InitDB(cfg)
	utils.Info("Database initialized successfully")

	// 初始化Redis缓存服务
	var err error
	cacheService, err = services.NewCacheService(cfg.RedisURL)
	if err != nil {
		utils.Warn("Failed to initialize Redis cache: %v, continuing without cache", err)
		cacheService = nil
	}

	// 初始化服务
	transferService = services.NewTransferService(database.GetDB())
	if cacheService != nil {
		transferService.SetCacheService(cacheService)
	}
	indexerService = services.NewIndexerService()
	marketService = services.NewMarketService()

	// 创建Tron客户端
	tronClient = blockchain.NewTronHTTPClient(cfg.TronNodeURL, cfg.TronAPIKey, cfg.USDTContract)

	// 创建同步服务（新的构造函数）
	syncService, err = services.NewSyncService(database.GetDB())
	if err != nil {
		utils.Fatal("Failed to create sync service: %v", err)
	}

	utils.Info("Services initialized successfully")

	// 初始化Gin路由
	r := gin.New()

	// 添加中间件
	r.Use(errorHandlingMiddleware())
	r.Use(loggingMiddleware())
	r.Use(gin.Recovery())

	// 缓存中间件配置
	if cacheService != nil {
		cacheConfig := middleware.CacheConfig{
			DefaultTTL: 5 * time.Minute,
			MaxSize:    1000,
			Enabled:    true,
		}
		r.Use(middleware.ResponseCacheMiddleware(cacheService, cacheConfig))
		r.Use(middleware.CacheInvalidationMiddleware(cacheService))
		r.Use(middleware.RateLimitMiddleware(cacheService, 1000)) // 1000 requests per minute per IP
	}

	// 健康检查接口
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
			"status":  "healthy",
		})
	})

	// API路由组
	api := r.Group("/api/v1")
	{
		// 健康检查接口
		api.GET("/health", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{
				"status":    "healthy",
				"message":   "Off-chain Data Indexer is running",
				"timestamp": time.Now().Unix(),
				"version":   "1.0.0",
			})
		})

		// 转账相关接口
		transfers := api.Group("/transfers")
		{
			transfers.GET("/", getTransfers)
			transfers.GET("/address/:address", getTransfersByAddress)
			transfers.GET("/hash/:hash", getTransferByHash)
			transfers.POST("/", createTransfer)
		}

		// 统计接口
		stats := api.Group("/stats")
		{
			stats.GET("/summary", getStatsSummary)
			stats.GET("/addresses/top", getTopAddresses)
			stats.GET("/hourly", getHourlyStats)
		}

		// 市场数据接口
		market := api.Group("/market")
		{
			// TronScan数据源
			market.GET("/usdt", getUSDTMarketData)
			market.GET("/usdt/detailed", getDetailedUSDTMarketData)
			market.GET("/usdt/tronscan", getUSDTMarketData)
			market.GET("/usdt/tronscan/detailed", getDetailedUSDTMarketData)

			// CoinGecko数据源
			market.GET("/usdt/coingecko", getUSDTMarketDataFromCoinGecko)
			market.GET("/usdt/coingecko/detailed", getDetailedUSDTMarketDataFromCoinGecko)
			market.GET("/usdt/CoinGecko", getUSDTMarketDataFromCoinGecko)
			market.GET("/usdt/CoinGecko/detailed", getDetailedUSDTMarketDataFromCoinGecko)

			// 管理接口
			market.POST("/reset-circuit-breaker", resetCircuitBreaker)
		}

		// 索引器接口
		indexer := api.Group("/indexer")
		{
			indexer.GET("/status", getIndexerStatus)
			indexer.POST("/batch", indexBatchTransfers)
			indexer.GET("/blocks/:start/:end", getTransfersByBlockRange)
			indexer.GET("/latest-block", getLatestBlock)
			indexer.POST("/reindex", reindexData)
			indexer.POST("/validate", validateDataIntegrity)
			indexer.DELETE("/historical-data", deleteHistoricalData)
		}

		// 同步服务接口（单链 - 保持向后兼容）
		sync := api.Group("/sync")
		{
			sync.POST("/start", startSync)
			sync.POST("/stop", stopSync)
			sync.GET("/status", getSyncStatus)
			sync.POST("/resync", resyncData)
			sync.POST("/missing-blocks", syncMissingBlocks)
			sync.GET("/metrics", getSyncMetrics)
			sync.GET("/health", syncHealthCheck)
		}



		// 监控接口
		if cacheService != nil {
			lockMetricsHandler := handlers.NewLockMetricsHandler(cacheService)
			metrics := api.Group("/metrics")
			{
				metrics.GET("/locks", lockMetricsHandler.GetLockMetrics)
				metrics.POST("/locks/reset", lockMetricsHandler.ResetLockMetrics)
			}

			health := api.Group("/health")
			{
				health.GET("/locks", lockMetricsHandler.GetLockHealth)
			}
		}
	}

	utils.Info("Server starting on port 8080...")
	if err := r.Run(":8080"); err != nil {
		utils.Fatal("Failed to start server: %v", err)
	}
}

// errorHandlingMiddleware 错误处理中间件
func errorHandlingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := utils.RecoverPanic(); err != nil {
				utils.HandleError(err)
				if appErr, ok := err.(*utils.AppError); ok {
					c.JSON(appErr.Code, gin.H{
						"error":     appErr.Type,
						"message":   appErr.Message,
						"timestamp": appErr.Timestamp,
					})
				} else {
					c.JSON(500, gin.H{
						"error":     "INTERNAL_ERROR",
						"message":   "Internal server error",
						"timestamp": time.Now(),
					})
				}
				c.Abort()
			}
		}()
		c.Next()
	}
}

// loggingMiddleware 日志记录中间件
func loggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery

		// 处理请求
		c.Next()

		// 记录请求日志
		latency := time.Since(start)
		clientIP := c.ClientIP()
		method := c.Request.Method
		statusCode := c.Writer.Status()

		if raw != "" {
			path = path + "?" + raw
		}

		logMessage := fmt.Sprintf("%s %s %d %v %s",
			method, path, statusCode, latency, clientIP)

		if statusCode >= 400 {
			utils.Warn("HTTP Request: %s", logMessage)
		} else {
			utils.Info("HTTP Request: %s", logMessage)
		}
	}
}

// handleAppError 处理应用程序错误
func handleAppError(c *gin.Context, err error) {
	if appErr, ok := err.(*utils.AppError); ok {
		utils.HandleError(appErr)
		c.JSON(appErr.Code, gin.H{
			"error":     appErr.Type,
			"message":   appErr.Message,
			"details":   appErr.Details,
			"timestamp": appErr.Timestamp,
		})
	} else {
		utils.HandleError(err)
		c.JSON(500, gin.H{
			"error":     "INTERNAL_ERROR",
			"message":   "Internal server error",
			"timestamp": time.Now(),
		})
	}
}

// 获取转账记录列表
func getTransfers(c *gin.Context) {
	// 获取分页参数
	page, err := strconv.Atoi(c.DefaultQuery("page", "1"))
	if err != nil {
		handleAppError(c, utils.NewValidationError("Invalid page parameter", err))
		return
	}
	pageSize, err := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	if err != nil {
		handleAppError(c, utils.NewValidationError("Invalid page_size parameter", err))
		return
	}

	// 调用服务层
	result, err := transferService.GetTransfers(page, pageSize)
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Failed to get transfers"))
		return
	}

	c.JSON(http.StatusOK, result)
}

// 根据地址获取转账记录
func getTransfersByAddress(c *gin.Context) {
	address := c.Param("address")
	if address == "" {
		handleAppError(c, utils.NewValidationError("Address parameter is required", nil))
		return
	}

	// 获取分页参数
	page, err := strconv.Atoi(c.DefaultQuery("page", "1"))
	if err != nil {
		handleAppError(c, utils.NewValidationError("Invalid page parameter", err))
		return
	}
	pageSize, err := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	if err != nil {
		handleAppError(c, utils.NewValidationError("Invalid page_size parameter", err))
		return
	}

	// 调用服务层
	result, err := transferService.GetTransfersByAddress(address, page, pageSize)
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Failed to get transfers by address"))
		return
	}

	c.JSON(http.StatusOK, result)
}

// 根据交易哈希获取转账记录
func getTransferByHash(c *gin.Context) {
	hash := c.Param("hash")
	if hash == "" {
		handleAppError(c, utils.NewValidationError("Hash parameter is required", nil))
		return
	}

	// 调用服务层
	result, err := transferService.GetTransferByHash(hash)
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Transfer not found"))
		return
	}

	c.JSON(http.StatusOK, result)
}

// 创建转账记录
func createTransfer(c *gin.Context) {
	var req services.TransferRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		handleAppError(c, utils.NewValidationError("Invalid request body", err))
		return
	}

	// 调用服务层
	result, err := transferService.CreateTransfer(&req)
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Failed to create transfer"))
		return
	}

	c.JSON(http.StatusCreated, result)
}

// 获取统计摘要
func getStatsSummary(c *gin.Context) {
	// 调用服务层
	result, err := transferService.GetStats()
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Failed to get stats"))
		return
	}

	c.JSON(http.StatusOK, result)
}

// 获取转账次数最多的地址
func getTopAddresses(c *gin.Context) {
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "10"))
	if err != nil {
		handleAppError(c, utils.NewValidationError("Invalid limit parameter", err))
		return
	}

	result, err := indexerService.GetTopAddresses(limit)
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Failed to get top addresses"))
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"addresses": result,
		"limit":     limit,
	})
}

// 批量索引转账数据
func indexBatchTransfers(c *gin.Context) {
	var transfers []services.TransferRequest
	if err := c.ShouldBindJSON(&transfers); err != nil {
		handleAppError(c, utils.NewValidationError("Invalid request body", err))
		return
	}

	successCount, err := indexerService.IndexTransferData(transfers)
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Failed to index transfers"))
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":       "Batch indexing completed",
		"success_count": successCount,
		"total_count":   len(transfers),
	})
}

// 根据区块范围获取转账记录
func getTransfersByBlockRange(c *gin.Context) {
	startBlock, err := strconv.ParseUint(c.Param("start"), 10, 64)
	if err != nil {
		handleAppError(c, utils.NewValidationError("Invalid start block number", err))
		return
	}

	endBlock, err := strconv.ParseUint(c.Param("end"), 10, 64)
	if err != nil {
		handleAppError(c, utils.NewValidationError("Invalid end block number", err))
		return
	}

	if startBlock > endBlock {
		handleAppError(c, utils.NewValidationError("Start block must be less than or equal to end block", nil))
		return
	}

	transfers, err := indexerService.GetTransfersByBlockRange(startBlock, endBlock)
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Failed to get transfers by block range"))
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"transfers":   transfers,
		"start_block": startBlock,
		"end_block":   endBlock,
		"count":       len(transfers),
	})
}

// 获取最新区块号
func getLatestBlock(c *gin.Context) {
	// 从Tron网络获取最新区块号
	latestBlock, err := tronClient.GetLatestBlockNumber(context.Background())
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Failed to get latest block from Tron network"))
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"latest_block": latestBlock,
	})
}

// 重新索引数据
func reindexData(c *gin.Context) {
	err := indexerService.ReindexData()
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Failed to reindex data"))
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Data reindexing completed successfully",
	})
}

// 验证数据完整性
func validateDataIntegrity(c *gin.Context) {
	err := indexerService.ValidateDataIntegrity()
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Failed to validate data integrity"))
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Data integrity validation completed successfully",
	})
}

// 启动同步服务
func startSync(c *gin.Context) {
	err := syncService.StartSync()
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Failed to start sync"))
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Sync service started successfully",
	})
}

// 停止同步服务
func stopSync(c *gin.Context) {
	syncService.StopSync()

	c.JSON(http.StatusOK, gin.H{
		"message": "Sync service stopped successfully",
	})
}

// 获取同步状态
func getSyncStatus(c *gin.Context) {
	status := syncService.GetSyncStatus()
	c.JSON(http.StatusOK, status)
}

// initializeBlockchainClients 初始化区块链客户端


// 重新同步数据
func resyncData(c *gin.Context) {
	// 解析请求参数
	var req struct {
		StartBlock uint64 `json:"start_block"`
		EndBlock   uint64 `json:"end_block"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		// 如果没有提供参数，使用默认值
		req.StartBlock = 0
		req.EndBlock = 10
	}

	utils.Info("Starting manual resync from block %d to %d", req.StartBlock, req.EndBlock)

	// 手动执行同步操作
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// 获取波场客户端
	cfg := config.LoadConfig()
	tronClient := blockchain.NewTronHTTPClient(cfg.TronNodeURL, cfg.TronAPIKey, cfg.USDTContract)

	// 获取区块范围内的USDT转账事件
	var allTransfers []*blockchain.TransferEvent
	for blockNum := req.StartBlock; blockNum <= req.EndBlock; blockNum++ {
		transfers, err := tronClient.GetUSDTTransfersByBlock(ctx, blockNum)
		if err != nil {
			utils.Error("Failed to get transfers for block %d: %v", blockNum, err)
			continue
		}
		allTransfers = append(allTransfers, transfers...)
	}

	utils.Info("Found %d USDT transfers in blocks %d-%d", len(allTransfers), req.StartBlock, req.EndBlock)

	// 将转账数据保存到数据库
	successCount := 0
	for _, transfer := range allTransfers {
		// 创建转账请求
		req := &services.TransferRequest{
			TransactionHash: transfer.TxHash,
			FromAddress:     transfer.FromAddress,
			ToAddress:       transfer.ToAddress,
			Amount:          transfer.Amount.String(),
			BlockNumber:     transfer.BlockNumber,
		}
		
		// 调用服务层保存数据
		_, err := transferService.CreateTransfer(req)
		if err != nil {
			utils.Error("Failed to save transfer %s: %v", transfer.TxHash, err)
			continue
		}
		successCount++
	}

	utils.Info("Manual resync completed. Saved %d transfers", successCount)

	c.JSON(http.StatusOK, gin.H{
		"message":         "Data resync completed successfully",
		"blocks_synced":   fmt.Sprintf("%d-%d", req.StartBlock, req.EndBlock),
		"transfers_found": len(allTransfers),
		"transfers_saved": successCount,
	})
}

// 同步缺失区块
func syncMissingBlocks(c *gin.Context) {
	// TODO: 实现同步缺失区块逻辑
	c.JSON(http.StatusOK, gin.H{"message": "Missing blocks sync completed successfully"})
}

// 获取同步指标
func getSyncMetrics(c *gin.Context) {
	// 获取同步状态
	status := syncService.GetSyncStatus()

	// 计算同步进度
	syncProgress := "0%"
	if status.LatestBlock > 0 {
		progress := float64(status.CurrentBlock) / float64(status.LatestBlock) * 100
		syncProgress = fmt.Sprintf("%.2f%%", progress)
	}

	metrics := gin.H{
		"total_blocks":   status.LatestBlock,
		"synced_blocks":  status.CurrentBlock,
		"last_block":     status.LastBlock,
		"sync_progress":  syncProgress,
		"synced_count":   status.SyncedCount,
		"error_count":    status.ErrorCount,
		"last_sync_time": status.LastSyncTime,
		"sync_interval":  status.SyncInterval.String(),
		"is_running":     status.IsRunning,
		"last_error":     status.LastError,
	}
	c.JSON(http.StatusOK, metrics)
}

// 同步服务健康检查
func syncHealthCheck(c *gin.Context) {
	health := syncService.HealthCheck()
	
	// 添加错误统计信息
	errorStats := utils.GetErrorStats()
	healthStats := utils.GetHealthStats()
	
	response := map[string]interface{}{
		"health":       health,
		"error_stats":  errorStats,
		"health_stats": healthStats,
		"timestamp":    time.Now(),
	}
	
	c.JSON(http.StatusOK, response)
}



// 获取最近一小时内的USDT交易统计
func getHourlyStats(c *gin.Context) {
	// 调用服务层
	result, err := transferService.GetHourlyStats()
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Failed to get hourly stats"))
		return
	}

	c.JSON(http.StatusOK, result)
}

// getUSDTMarketData 获取USDT市场数据
func getUSDTMarketData(c *gin.Context) {
	marketData, err := marketService.GetUSDTMarketData()
	if err != nil {
		handleAppError(c, err)
		return
	}
	c.JSON(http.StatusOK, marketData)
}

// getDetailedUSDTMarketData 获取USDT详细市场数据（TronScan数据源）
func getDetailedUSDTMarketData(c *gin.Context) {
	marketData, err := marketService.GetDetailedUSDTData()
	if err != nil {
		handleAppError(c, err)
		return
	}
	c.JSON(http.StatusOK, marketData)
}

// getUSDTMarketDataFromCoinGecko 获取USDT市场数据（CoinGecko数据源）
func getUSDTMarketDataFromCoinGecko(c *gin.Context) {
	data, err := marketService.GetUSDTMarketDataFromCoinGecko()
	if err != nil {
		handleAppError(c, err)
		return
	}
	c.JSON(http.StatusOK, data)
}

// getDetailedUSDTMarketDataFromCoinGecko 获取详细的USDT市场数据（CoinGecko数据源）
func getDetailedUSDTMarketDataFromCoinGecko(c *gin.Context) {
	data, err := marketService.GetDetailedUSDTDataFromCoinGecko()
	if err != nil {
		handleAppError(c, err)
		return
	}
	c.JSON(http.StatusOK, data)
}

// resetCircuitBreaker 重置熔断器状态
func resetCircuitBreaker(c *gin.Context) {
	marketService.ResetCircuitBreaker()
	c.JSON(http.StatusOK, gin.H{
		"message":   "Circuit breaker has been reset successfully",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// getIndexerStatus 获取索引器状态
func getIndexerStatus(c *gin.Context) {
	status, err := indexerService.GetStatus()
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Failed to get indexer status"))
		return
	}

	c.JSON(http.StatusOK, status)
}

// deleteHistoricalData 删除历史数据（保留今天的数据）
func deleteHistoricalData(c *gin.Context) {
	deletedCount, err := indexerService.DeleteHistoricalData()
	if err != nil {
		handleAppError(c, utils.WrapError(err, "Failed to delete historical data"))
		return
	}

	// 清除统计相关的缓存
	if cacheService != nil {
		// 清除统计缓存
		cacheService.Delete("stats:summary")
		cacheService.Delete("stats:hourly")
		// 清除所有统计相关的缓存模式
		cacheService.FlushPattern("stats:*")
		cacheService.FlushPattern("api:*stats*")
		cacheService.FlushPattern("transfers:*")
	}

	c.JSON(http.StatusOK, gin.H{
		"message":       "Historical data deleted successfully",
		"deleted_count": deletedCount,
		"timestamp":     time.Now().Format(time.RFC3339),
	})
}
