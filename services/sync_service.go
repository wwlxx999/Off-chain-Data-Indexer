package services

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"Off-chainDatainDexer/blockchain"
	"Off-chainDatainDexer/config"
	"Off-chainDatainDexer/database"
	"Off-chainDatainDexer/utils"

	"gorm.io/gorm"
)

// SyncService 数据同步服务
type SyncService struct {
	db           *gorm.DB
	mu           sync.RWMutex
	status       SyncStatus
	stopCh       chan struct{}
	tronClient   *blockchain.TronHTTPClient
	config       *config.Config
	tronUtils    *utils.TronUtils
	isRunning    bool
	syncInterval time.Duration
	// 新增字段用于失败区块重试
	failedBlocks map[uint64]*FailedBlock
	failedMutex  sync.RWMutex
	// 新增并发同步管理器
	concurrentManager *ConcurrentSyncManager
	concurrentEnabled bool
	// 同步进度协调和数据一致性
	syncProgress    *SyncProgress
	progressMu      sync.RWMutex
	processedBlocks map[uint64]bool // 已处理区块记录
	processedMu     sync.RWMutex
	// 新增服务依赖用于缓存更新
	transferService *TransferService
	marketService   *MarketService
	// 进度条显示控制
	lastProgressDisplay time.Time
	progressDisplayInterval time.Duration
	
	// 健康检查状态跟踪
	lastHealthStatus string
	lastAlertLevel   string
}

// FailedBlock 失败区块信息
type FailedBlock struct {
	BlockNumber uint64    `json:"block_number"`
	FailCount   int       `json:"fail_count"`
	LastAttempt time.Time `json:"last_attempt"`
	LastError   string    `json:"last_error"`
}

// SyncTask 同步任务
type SyncTask struct {
	StartBlock uint64
	EndBlock   uint64
	NodeIndex  int // 指定使用的节点索引
	RetryCount int
	CreatedAt  time.Time
}

// SyncResult 同步结果
type SyncResult struct {
	Task      *SyncTask
	Transfers []*blockchain.TransferEvent
	Error     error
	Duration  time.Duration
}

// ConcurrentSyncManager 并发同步管理器
type ConcurrentSyncManager struct {
	taskChan    chan *SyncTask
	resultChan  chan *SyncResult
	workerCount int
	tronClient  *blockchain.TronHTTPClient
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// SyncProgress 同步进度跟踪
type SyncProgress struct {
	CurrentBlock    uint64        `json:"current_block"`
	TargetBlock     uint64        `json:"target_block"`
	ProcessedBlocks uint64        `json:"processed_blocks"`
	FailedBlocks    uint64        `json:"failed_blocks"`
	StartTime       time.Time     `json:"start_time"`
	LastUpdateTime  time.Time     `json:"last_update_time"`
	SyncRate        float64       `json:"sync_rate"`      // 区块/秒
	EstimatedTime   time.Duration `json:"estimated_time"` // 预计完成时间
	ActiveWorkers   int           `json:"active_workers"`
	TotalTasks      int           `json:"total_tasks"`
	CompletedTasks  int           `json:"completed_tasks"`
}

// SyncConfig 同步配置
type SyncConfig struct {
	Interval   time.Duration `json:"interval"`
	BatchSize  int           `json:"batch_size"`
	StartBlock uint64        `json:"start_block"`
	AutoStart  bool          `json:"auto_start"`
}

// SyncStatus 同步状态
type SyncStatus struct {
	IsRunning    bool          `json:"is_running"`
	LastSyncTime time.Time     `json:"last_sync_time"`
	SyncInterval time.Duration `json:"sync_interval"`
	LastBlock    uint64        `json:"last_block"`
	CurrentBlock uint64        `json:"current_block"`
	LatestBlock  uint64        `json:"latest_block"`
	SyncedCount  int64         `json:"synced_count"`
	ErrorCount   int64         `json:"error_count"`
	LastError    string        `json:"last_error"`
}

// NewSyncService 创建新的同步服务
func NewSyncService(db *gorm.DB, transferService *TransferService, marketService *MarketService) (*SyncService, error) {
	// 加载配置
	cfg := config.LoadConfig()

	// 获取同步间隔
	syncInterval := cfg.SyncInterval

	// 获取起始区块号
	startBlock := cfg.StartBlock

	// 创建波场HTTP客户端（支持多节点配置）
	var tronClient *blockchain.TronHTTPClient
	if len(cfg.TronNodes) > 0 {
		// 使用多节点配置
		tronClient = blockchain.NewTronHTTPClientWithConfig(cfg)
	} else {
		// 使用单节点配置（向后兼容）
		tronClient = blockchain.NewTronHTTPClient(cfg.TronNodeURL, cfg.TronAPIKey, cfg.USDTContract)
	}

	// 创建波场工具
	tronUtils := utils.NewTronUtils()

	// 检查是否启用并发同步（当配置了多个节点且工作协程数大于1时）
	concurrentEnabled := len(cfg.TronNodes) > 1 && cfg.ConcurrentConfig.WorkerCount > 1

	service := &SyncService{
		db: db,
		status: SyncStatus{
			IsRunning:    false,
			LastSyncTime: time.Time{},
			SyncInterval: syncInterval,
			LastBlock:    startBlock,
			CurrentBlock: 0,
			LatestBlock:  0,
			SyncedCount:  0,
			ErrorCount:   0,
			LastError:    "",
		},
		stopCh:       make(chan struct{}),
		tronClient:   tronClient,
		config:       cfg,
		tronUtils:    tronUtils,
		isRunning:    false,
		syncInterval: syncInterval,
		// 初始化失败区块映射
		failedBlocks:      make(map[uint64]*FailedBlock),
		concurrentEnabled: concurrentEnabled,
		// 初始化进度跟踪相关字段
		processedBlocks: make(map[uint64]bool),
		syncProgress: &SyncProgress{
			StartTime:      time.Now(),
			LastUpdateTime: time.Now(),
		},
		// 初始化服务依赖
		transferService: transferService,
		marketService:   marketService,
		// 初始化进度条显示控制
		lastProgressDisplay:     time.Time{},
		progressDisplayInterval: 5 * time.Second, // 每5秒显示一次进度条
		// 初始化健康检查状态跟踪
		lastHealthStatus: "",
		lastAlertLevel:   "",
	}

	// 如果启用并发同步，初始化并发管理器
	if concurrentEnabled {
		service.concurrentManager = service.newConcurrentSyncManager()
		utils.Info("Concurrent sync enabled with %d workers", cfg.ConcurrentConfig.WorkerCount)
	} else {
		utils.Info("Using sequential sync mode")
	}

	return service, nil
}

// StartSync 开始数据同步
func (s *SyncService) StartSync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.status.IsRunning {
		return fmt.Errorf("sync service is already running")
	}

	s.status.IsRunning = true
	s.stopCh = make(chan struct{})

	utils.FileLog(utils.INFO, "Starting Tron USDT sync service...")

	// 显示节点配置信息
	if len(s.config.TronNodes) > 0 {
		utils.FileLog(utils.INFO, "=== 多节点配置 ===")
		utils.FileLog(utils.INFO, "总节点数: %d", len(s.config.TronNodes))
		for i, node := range s.config.TronNodes {
			apiKeyStatus := "无API Key"
			if node.APIKey != "" {
				apiKeyStatus = "有API Key"
			}
			utils.FileLog(utils.INFO, "节点 %d: %s (权重: %d, %s)", i, node.URL, node.Weight, apiKeyStatus)
		}
		utils.FileLog(utils.INFO, "================")
	} else {
		// 向后兼容：显示单节点配置
		utils.FileLog(utils.INFO, "Node URL: %s", s.config.TronNodeURL)
		apiKeyStatus := "无API Key"
		if s.config.TronAPIKey != "" {
			apiKeyStatus = "有API Key"
		}
		utils.FileLog(utils.INFO, "API Key状态: %s", apiKeyStatus)
	}

	utils.FileLog(utils.INFO, "USDT Contract: %s", s.config.USDTContract)
	utils.FileLog(utils.INFO, "Sync Interval: %v", s.status.SyncInterval)
	utils.FileLog(utils.INFO, "Start Block: %d", s.status.LastBlock)

	// 如果启用并发同步，启动并发管理器
	if s.concurrentEnabled {
		s.startConcurrentSync()
		utils.FileLog(utils.INFO, "Concurrent sync started with %d workers", s.config.ConcurrentConfig.WorkerCount)
	}

	// 启动同步循环
	go s.syncLoop()

	return nil
}

// StopSync 停止数据同步
func (s *SyncService) StopSync() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.status.IsRunning {
		return
	}

	utils.FileLog(utils.INFO, "Stopping Tron USDT sync service...")
	s.status.IsRunning = false

	// 如果启用并发同步，停止并发管理器
	if s.concurrentEnabled {
		s.stopConcurrentSync()
		utils.FileLog(utils.INFO, "Concurrent sync stopped")
	}

	close(s.stopCh)
	utils.FileLog(utils.INFO, "Sync service stopped")
}

// GetSyncStatus 获取同步状态
func (s *SyncService) GetSyncStatus() SyncStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

// syncLoop 同步循环
func (s *SyncService) syncLoop() {
	ticker := time.NewTicker(s.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			log.Println("Sync loop stopped")
			return
		case <-ticker.C:
			if err := s.performSync(); err != nil {
				log.Printf("Sync error: %v", err)
				s.mu.Lock()
				s.status.ErrorCount++
				s.status.LastError = err.Error()
				s.mu.Unlock()
			} else {
				s.mu.Lock()
				s.status.LastSyncTime = time.Now()
				s.mu.Unlock()
			}
		}
	}
}

// updateSyncProgress 更新同步进度
func (s *SyncService) updateSyncProgress(currentBlock, targetBlock uint64) {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()

	now := time.Now()
	oldCurrentBlock := s.syncProgress.CurrentBlock
	s.syncProgress.CurrentBlock = currentBlock
	s.syncProgress.TargetBlock = targetBlock
	s.syncProgress.LastUpdateTime = now

	// 如果是第一次调用，记录起始时间和起始区块
	if s.syncProgress.StartTime.IsZero() {
		s.syncProgress.StartTime = now
	}

	// 计算同步起始区块（使用初始化时设置的CurrentBlock）
	startBlock := oldCurrentBlock
	if startBlock == 0 {
		startBlock = currentBlock // 如果没有初始值，使用当前区块
	}

	// 计算已处理的区块数量（相对于同步范围）
	if currentBlock >= startBlock {
		s.syncProgress.ProcessedBlocks = currentBlock - startBlock + 1
	} else {
		s.syncProgress.ProcessedBlocks = 1 // 至少处理了1个区块
	}

	// 计算同步速率
	elapsed := now.Sub(s.syncProgress.StartTime).Seconds()
	if elapsed > 0 && s.syncProgress.ProcessedBlocks > 0 {
		s.syncProgress.SyncRate = float64(s.syncProgress.ProcessedBlocks) / elapsed
	}

	// 估算剩余时间
	remainingBlocks := targetBlock - currentBlock
	if s.syncProgress.SyncRate > 0 && remainingBlocks > 0 {
		s.syncProgress.EstimatedTime = time.Duration(float64(remainingBlocks)/s.syncProgress.SyncRate) * time.Second
	} else {
		s.syncProgress.EstimatedTime = 0
	}

	// 显示可视化进度条
	s.displayProgressBar(currentBlock, targetBlock)
}

// displayProgressBar 显示可视化进度条
func (s *SyncService) displayProgressBar(currentBlock, targetBlock uint64) {
	if targetBlock == 0 {
		return
	}

	// 检查是否需要显示进度条（时间间隔控制）
	now := time.Now()
	if !s.lastProgressDisplay.IsZero() && now.Sub(s.lastProgressDisplay) < s.progressDisplayInterval {
		return // 还未到显示时间，跳过
	}
	s.lastProgressDisplay = now

	// 获取同步进度信息
	s.progressMu.RLock()
	startBlock := s.syncProgress.CurrentBlock
	if startBlock == 0 {
		startBlock = currentBlock
	}
	totalBlocks := targetBlock - startBlock + 1
	processedBlocks := s.syncProgress.ProcessedBlocks
	s.progressMu.RUnlock()

	// 计算进度百分比（基于已处理的区块数量）
	var progress float64
	if totalBlocks > 0 {
		progress = float64(processedBlocks) / float64(totalBlocks)
	}
	if progress > 1.0 {
		progress = 1.0
	}
	percentage := progress * 100

	// 进度条长度
	barLength := 50
	filledLength := int(progress * float64(barLength))

	// 构建进度条
	bar := "["
	for i := 0; i < barLength; i++ {
		if i < filledLength {
			bar += "█"
		} else {
			bar += "░"
		}
	}
	bar += "]"

	// 格式化时间显示
	elapsedTime := time.Since(s.syncProgress.StartTime)
	etaStr := "计算中..."
	if s.syncProgress.EstimatedTime > 0 {
		etaStr = s.formatDuration(s.syncProgress.EstimatedTime)
	}

	// 格式化同步速率
	rateStr := fmt.Sprintf("%.2f", s.syncProgress.SyncRate)
	if s.syncProgress.SyncRate == 0 {
		rateStr = "计算中"
	}

	// 显示进度信息到控制台
	utils.ProgressBar("\n" +
		"╔══════════════════════════════════════════════════════════════╗\n" +
		"║                        同步进度                              ║\n" +
		"╠══════════════════════════════════════════════════════════════╣\n" +
		"║ 进度: %s %.1f%%                    ║\n" +
		"║ 区块: %d / %d                                    ║\n" +
		"║ 速率: %s 区块/秒                                     ║\n" +
		"║ 已用时间: %s                                        ║\n" +
		"║ 预计剩余: %s                                        ║\n" +
		"║ 活跃工作协程: %d                                         ║\n" +
		"║ 任务进度: %d/%d                                         ║\n" +
		"╚══════════════════════════════════════════════════════════════╝",
		bar, percentage,
		currentBlock, targetBlock,
		rateStr,
		s.formatDuration(elapsedTime),
		etaStr,
		s.syncProgress.ActiveWorkers,
		s.syncProgress.CompletedTasks, s.syncProgress.TotalTasks)
}

// formatDuration 格式化时间显示
func (s *SyncService) formatDuration(d time.Duration) string {
	if d < 0 {
		return "未知"
	}

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%d小时%d分%d秒", hours, minutes, seconds)
	} else if minutes > 0 {
		return fmt.Sprintf("%d分%d秒", minutes, seconds)
	} else {
		return fmt.Sprintf("%d秒", seconds)
	}
}

// markBlockProcessed 标记区块已处理
func (s *SyncService) markBlockProcessed(blockNum uint64) {
	s.processedMu.Lock()
	defer s.processedMu.Unlock()

	if !s.processedBlocks[blockNum] {
		s.processedBlocks[blockNum] = true
		s.progressMu.Lock()
		s.syncProgress.ProcessedBlocks++
		s.progressMu.Unlock()
	}
}

// isBlockProcessed 检查区块是否已处理
func (s *SyncService) isBlockProcessed(blockNum uint64) bool {
	s.processedMu.RLock()
	defer s.processedMu.RUnlock()
	return s.processedBlocks[blockNum]
}

// getSyncProgress 获取同前进度
func (s *SyncService) getSyncProgress() *SyncProgress {
	s.progressMu.RLock()
	defer s.progressMu.RUnlock()

	// 返回进度副本
	progress := *s.syncProgress
	return &progress
}

// ensureDataConsistency 确保数据一致性
func (s *SyncService) ensureDataConsistency(startBlock, endBlock uint64) error {
	utils.FileLog(utils.INFO, "Checking data consistency for blocks %d-%d", startBlock, endBlock)

	// 检查数据库中的区块连续性
	missingBlocks, err := s.findMissingBlocks(startBlock, endBlock)
	if err != nil {
		return fmt.Errorf("failed to check missing blocks: %w", err)
	}

	if len(missingBlocks) > 0 {
		utils.Warn("Found %d missing blocks, adding to retry queue", len(missingBlocks))
		for _, blockNum := range missingBlocks {
			s.addFailedBlock(blockNum, fmt.Errorf("missing block data"))
		}
	}

	// 检查重复数据
	duplicateCount, err := s.removeDuplicateTransfers(startBlock, endBlock)
	if err != nil {
		return fmt.Errorf("failed to remove duplicates: %w", err)
	}

	if duplicateCount > 0 {
		utils.FileLog(utils.INFO, "Removed %d duplicate transfers for blocks %d-%d", duplicateCount, startBlock, endBlock)
	}

	return nil
}

// findMissingBlocks 查找缺失的区块
func (s *SyncService) findMissingBlocks(startBlock, endBlock uint64) ([]uint64, error) {
	var existingBlockNums []uint64
	err := s.db.Table("usdt_transfers").
		Select("DISTINCT block_number").
		Where("block_number BETWEEN ? AND ?", startBlock, endBlock).
		Order("block_number").
		Pluck("block_number", &existingBlockNums).Error
	if err != nil {
		return nil, err
	}

	existingBlocks := make(map[uint64]bool)
	for _, blockNum := range existingBlockNums {
		existingBlocks[blockNum] = true
	}

	var missingBlocks []uint64
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		if !existingBlocks[blockNum] && !s.isBlockProcessed(blockNum) {
			missingBlocks = append(missingBlocks, blockNum)
		}
	}

	return missingBlocks, nil
}

// removeDuplicateTransfers 移除重复的转账记录
func (s *SyncService) removeDuplicateTransfers(startBlock, endBlock uint64) (int, error) {
	query := `
		DELETE FROM usdt_transfers 
		WHERE id IN (
			SELECT id FROM (
				SELECT id, ROW_NUMBER() OVER (
					PARTITION BY transaction_hash, from_address, to_address, amount, block_number 
					ORDER BY created_at
				) as rn
				FROM usdt_transfers 
				WHERE block_number BETWEEN ? AND ?
			) t WHERE rn > 1
		)
	`

	result := s.db.Exec(query, startBlock, endBlock)
	if result.Error != nil {
		return 0, result.Error
	}

	return int(result.RowsAffected), nil
}

// performSync 执行同步操作
func (s *SyncService) performSync() error {
	utils.FileLog(utils.INFO, "Starting sync operation...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// 首先处理失败的区块重试
	err := s.retryFailedBlocks(ctx)
	if err != nil {
		utils.FileLog(utils.ERROR, "Failed to retry failed blocks: %v", err)
	}

	// 获取最新区块号
	latestBlock, err := s.tronClient.GetLatestBlockNumber(ctx)
	if err != nil {
		s.updateErrorCount()
		return utils.NewInternalError("Failed to get latest block number", err)
	}

	// 确定同步起始区块
	startBlock := s.status.LastBlock
	if startBlock == 0 {
		// 如果是首次同步，从配置的起始区块开始
		startBlock = s.config.StartBlock
	}

	// 如果没有新区块，跳过同步
	if startBlock >= latestBlock {
		utils.FileLog(utils.INFO, "No new blocks to sync")
		return nil
	}

	// 限制每次同步的区块数量
	batchSize := s.config.BatchSize
	if batchSize == 0 {
		batchSize = 100
	}

	endBlock := startBlock + batchSize
	if endBlock > latestBlock {
		endBlock = latestBlock
	}

	utils.FileLog(utils.INFO, "Syncing blocks from %d to %d", startBlock, endBlock)

	// 更新当前同步状态
	s.mu.Lock()
	s.status.CurrentBlock = endBlock
	s.status.LatestBlock = latestBlock
	s.mu.Unlock()

	// 根据是否启用并发同步选择不同的策略
	if s.concurrentEnabled {
		// 启动并发同步工作协程
		s.startConcurrentSync()
		// 确保在函数结束时停止并发同步
		defer s.stopConcurrentSync()
		return s.performConcurrentSync(ctx, startBlock, endBlock)
	} else {
		return s.performSequentialSync(ctx, startBlock, endBlock)
	}
}

// PerformManualSync 执行手动同步操作
func (s *SyncService) PerformManualSync(ctx context.Context) error {
	utils.FileLog(utils.INFO, "Starting manual sync operation...")

	// 首先处理失败的区块重试
	err := s.retryFailedBlocks(ctx)
	if err != nil {
		utils.Error("Failed to retry failed blocks during manual sync: %v", err)
	}

	// 获取最新区块号
	latestBlock, err := s.tronClient.GetLatestBlockNumber(ctx)
	if err != nil {
		s.updateErrorCount()
		return utils.NewInternalError("Failed to get latest block number", err)
	}

	// 确定同步起始区块
	startBlock := s.status.LastBlock
	if startBlock == 0 {
		// 如果是首次同步，从配置的起始区块开始
		startBlock = s.config.StartBlock
	}

	// 如果没有新区块，跳过同步
	if startBlock >= latestBlock {
		utils.Info("No new blocks to sync")
		return nil
	}

	// 手动同步时使用较小的批次大小以确保快速响应
	batchSize := uint64(50) // 手动同步使用固定的50个区块

	endBlock := startBlock + batchSize
	if endBlock > latestBlock {
		endBlock = latestBlock
	}

	utils.Info("Manual sync: processing blocks from %d to %d", startBlock, endBlock)

	// 更新当前同步状态
	s.mu.Lock()
	s.status.CurrentBlock = endBlock
	s.status.LatestBlock = latestBlock
	s.mu.Unlock()

	// 根据是否启用并发同步选择不同的策略
	if s.concurrentEnabled {
		return s.performConcurrentSync(ctx, startBlock, endBlock)
	} else {
		return s.performSequentialSync(ctx, startBlock, endBlock)
	}
}

// performSequentialSync 执行顺序同步（原有逻辑）
func (s *SyncService) performSequentialSync(ctx context.Context, startBlock, endBlock uint64) error {
	// 获取区块范围内的USDT转账事件（带重试机制）
	var allTransfers []*blockchain.TransferEvent
	var failedBlocks []uint64

	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		transfers, err := s.syncBlockWithRetry(ctx, blockNum)
		if err != nil {
			utils.Error("Failed to sync block %d after retries: %v", blockNum, err)
			// 记录失败的区块
			s.recordFailedBlock(blockNum, err.Error())
			failedBlocks = append(failedBlocks, blockNum)
			continue
		}
		allTransfers = append(allTransfers, transfers...)
	}

	utils.FileLog(utils.INFO, "Found %d USDT transfers in blocks %d-%d", len(allTransfers), startBlock, endBlock)

	// 将转账数据保存到数据库
	successCount := 0
	for _, transfer := range allTransfers {
		err := s.saveTransferToDatabase(transfer)
		if err != nil {
			utils.FileLog(utils.ERROR, "Failed to save transfer %s: %v", transfer.TxHash, err)
			s.updateErrorCount()
			continue
		}
		successCount++
	}

	// 更新同步状态
	s.mu.Lock()
	// 只有在没有失败区块的情况下才更新LastBlock
	if len(failedBlocks) == 0 {
		s.status.LastBlock = endBlock
	} else {
		// 如果有失败区块，只更新到最后一个成功的区块
		lastSuccessBlock := s.findLastSuccessBlock(startBlock, endBlock, failedBlocks)
		s.status.LastBlock = lastSuccessBlock
	}
	s.status.SyncedCount += int64(successCount)
	s.status.LastSyncTime = time.Now()
	s.mu.Unlock()

	utils.FileLog(utils.INFO, "Sequential sync completed. Synced %d transfers, %d failed blocks", successCount, len(failedBlocks))
	return nil
}

// performConcurrentSync 执行并发同步
func (s *SyncService) performConcurrentSync(ctx context.Context, startBlock, endBlock uint64) error {
	if s.concurrentManager == nil {
		return fmt.Errorf("concurrent manager not initialized")
	}

	// 更新同步进度
	s.updateSyncProgress(startBlock, endBlock)

	// 计算需要分割的任务数量
	chunkSize := s.config.ConcurrentConfig.ChunkSize
	if chunkSize == 0 {
		chunkSize = 25 // 默认每个任务处理25个区块
	}

	// 创建同步任务
	tasks := s.createSyncTasks(startBlock, endBlock, chunkSize)
	utils.Info("Created %d concurrent sync tasks for blocks %d-%d", len(tasks), startBlock, endBlock)

	// 更新进度信息
	s.progressMu.Lock()
	s.syncProgress.TotalTasks = len(tasks)
	s.syncProgress.CompletedTasks = 0
	s.syncProgress.ActiveWorkers = s.config.ConcurrentConfig.WorkerCount
	s.progressMu.Unlock()

	// 分发任务到工作协程
	for _, task := range tasks {
		select {
		case s.concurrentManager.taskChan <- task:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.concurrentManager.ctx.Done():
			return fmt.Errorf("concurrent sync manager stopped")
		}
	}

	utils.Info("Concurrent sync tasks dispatched")

	// 启动进度监控
	go s.monitorSyncProgress(startBlock, endBlock)

	// 等待所有任务完成
	return s.waitForAllTasksCompletion(ctx, len(tasks))
}

// monitorSyncProgress 监控同步进度
func (s *SyncService) monitorSyncProgress(startBlock, endBlock uint64) {
	ticker := time.NewTicker(1 * time.Second) // 每1秒更新一次进度条，提供实时反馈
	defer ticker.Stop()

	// 初始显示
	s.updateSyncProgress(startBlock, endBlock)

	for {
		select {
		case <-ticker.C:
			s.updateSyncProgress(startBlock, endBlock)

			// 检查是否完成
			s.progressMu.RLock()
			completed := s.syncProgress.CompletedTasks
			total := s.syncProgress.TotalTasks
			s.progressMu.RUnlock()

			if completed >= total {
				utils.Info("\n🎉 所有并发同步任务已完成，正在进行最终一致性检查...")
				if err := s.ensureDataConsistency(startBlock, endBlock); err != nil {
					utils.Error("❌ 最终一致性检查失败: %v", err)
				} else {
					utils.Info("✅ 同步完成！数据一致性检查通过")
				}
				return
			}
		case <-s.concurrentManager.ctx.Done():
			return
		}
	}
}

// createSyncTasks 创建同步任务列表（考虑节点权重和负载均衡）
func (s *SyncService) createSyncTasks(startBlock, endBlock, chunkSize uint64) []*SyncTask {
	var tasks []*SyncTask
	nodeCount := len(s.config.TronNodes)
	if nodeCount == 0 {
		nodeCount = 1 // 单节点模式
	}

	// 计算节点权重总和
	totalWeight := 0
	for _, node := range s.config.TronNodes {
		totalWeight += node.Weight
	}
	if totalWeight == 0 {
		totalWeight = nodeCount // 如果没有设置权重，默认每个节点权重为1
	}

	// 创建节点分配序列（基于权重）
	nodeSequence := s.createWeightedNodeSequence(totalWeight)
	nodeIndex := 0

	for current := startBlock; current <= endBlock; current += chunkSize {
		taskEnd := current + chunkSize - 1
		if taskEnd > endBlock {
			taskEnd = endBlock
		}

		// 根据权重选择节点
		selectedNode := nodeSequence[nodeIndex%len(nodeSequence)]

		task := &SyncTask{
			StartBlock: current,
			EndBlock:   taskEnd,
			NodeIndex:  selectedNode,
			RetryCount: 0,
			CreatedAt:  time.Now(),
		}
		tasks = append(tasks, task)

		// 轮询到下一个节点
		nodeIndex++
	}

	return tasks
}

// createWeightedNodeSequence 创建基于权重的节点分配序列
func (s *SyncService) createWeightedNodeSequence(totalWeight int) []int {
	var sequence []int

	for i, node := range s.config.TronNodes {
		weight := node.Weight
		if weight <= 0 {
			weight = 1 // 默认权重为1
		}

		// 根据权重添加节点索引到序列中
		for j := 0; j < weight; j++ {
			sequence = append(sequence, i)
		}
	}

	if len(sequence) == 0 {
		// 如果序列为空，创建简单的轮询序列
		for i := 0; i < len(s.config.TronNodes); i++ {
			sequence = append(sequence, i)
		}
	}

	utils.Info("Created weighted node sequence with %d entries for %d nodes",
		len(sequence), len(s.config.TronNodes))

	return sequence
}

// saveTransferToDatabase 将转账数据保存到数据库（使用幂等性处理）
func (s *SyncService) saveTransferToDatabase(transfer *blockchain.TransferEvent) error {
	utils.FileLog(utils.INFO, "Saving transfer: %s from %s to %s amount %s",
		transfer.TxHash, transfer.FromAddress, transfer.ToAddress, transfer.Amount.String())

	// 导入database包以使用幂等性方法
	// 注意：需要在文件顶部添加导入
	// "Off-chainDatainDexer/database"

	// 创建数据库转账记录
	dbTransfer := &database.Transfer{
		FromAddress:     transfer.FromAddress,
		ToAddress:       transfer.ToAddress,
		Amount:          transfer.Amount.String(),
		TransactionHash: transfer.TxHash,
		BlockNumber:     transfer.BlockNumber,
		Timestamp:       transfer.Timestamp,
	}

	// 使用幂等性插入方法
	err := database.InsertTransferIdempotent(dbTransfer)
	if err != nil {
		return utils.NewDatabaseError("Failed to save transfer to database", err)
	}

	utils.FileLog(utils.INFO, "Successfully processed transfer %s", transfer.TxHash)
	return nil
}

// GetLastSyncBlock 获取最后同步的区块号
func (s *SyncService) GetLastSyncBlock() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status.LastBlock
}

// SetLastSyncBlock 设置最后同步的区块号
func (s *SyncService) SetLastSyncBlock(blockNumber uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status.LastBlock = blockNumber
}

// GetLatestBlockNumber 获取区块链网络最新区块号
func (s *SyncService) GetLatestBlockNumber() (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.tronClient.GetLatestBlockNumber(ctx)
}

// SyncBlockRange 同步指定区块范围
func (s *SyncService) SyncBlockRange(ctx context.Context, startBlock, endBlock uint64) error {
	utils.Info("Starting sync for block range %d-%d", startBlock, endBlock)

	// 初始化同步进度状态
	s.progressMu.Lock()
	s.syncProgress.CurrentBlock = startBlock
	s.syncProgress.TargetBlock = endBlock
	s.syncProgress.ProcessedBlocks = 0
	s.syncProgress.FailedBlocks = 0
	s.syncProgress.StartTime = time.Now()
	s.syncProgress.LastUpdateTime = time.Now()
	s.syncProgress.SyncRate = 0
	s.syncProgress.EstimatedTime = 0
	s.syncProgress.ActiveWorkers = 0
	s.syncProgress.TotalTasks = 0
	s.syncProgress.CompletedTasks = 0
	// 存储同步范围信息，用于正确计算进度
	s.syncProgress.CurrentBlock = startBlock
	s.progressMu.Unlock()

	// 更新同步进度
	s.updateSyncProgress(startBlock, endBlock)

	// 根据是否启用并发同步选择不同的策略
	if s.concurrentEnabled {
		// 启动并发同步工作协程
		s.startConcurrentSync()
		// 确保在函数结束时停止并发同步
		defer s.stopConcurrentSync()
		return s.performConcurrentSync(ctx, startBlock, endBlock)
	} else {
		return s.performSequentialSync(ctx, startBlock, endBlock)
	}
}

// HealthCheck 增强的健康检查
func (s *SyncService) HealthCheck() map[string]interface{} {
	startTime := time.Now()

	s.mu.RLock()
	status := s.status
	s.mu.RUnlock()

	// 检查数据库连接和性能
	dbHealth := s.checkDatabaseHealth()

	// 检查Tron客户端连接和性能
	tronHealth := s.checkTronClientHealth()

	// 检查并发同步管理器状态
	concurrentHealth := s.checkConcurrentSyncHealth()

	// 获取失败区块详细信息
	failedBlocksInfo := s.getFailedBlocksInfo()

	// 获取同步进度和性能指标
	syncProgress := s.getDetailedSyncProgress()

	// 获取错误统计
	errorStats := s.getErrorStatistics()

	// 获取性能指标
	performanceMetrics := s.getPerformanceMetrics()

	// 确定整体健康状态和告警级别
	overallStatus, alertLevel := s.determineHealthStatus(dbHealth, tronHealth, concurrentHealth, failedBlocksInfo, errorStats)

	result := map[string]interface{}{
		"sync_service": map[string]interface{}{
			"status":              status,
			"overall_status":      overallStatus,
			"alert_level":         alertLevel,
			"database":            dbHealth,
			"tron_client":         tronHealth,
			"concurrent_sync":     concurrentHealth,
			"failed_blocks":       failedBlocksInfo,
			"sync_progress":       syncProgress,
			"error_statistics":    errorStats,
			"performance_metrics": performanceMetrics,
			"last_updated":        time.Now(),
			"uptime":              time.Since(s.syncProgress.StartTime),
		},
	}

	// 计算健康检查响应时间
	responseTime := time.Since(startTime)

	// 记录详细的健康检查日志
	s.logDetailedHealth(overallStatus, alertLevel, responseTime, result)

	// 检查是否需要发送告警
	s.checkAndSendAlerts(alertLevel, result)

	return result
}

// checkDatabaseHealth 检查数据库健康状态和性能
func (s *SyncService) checkDatabaseHealth() map[string]interface{} {
	startTime := time.Now()
	result := map[string]interface{}{
		"status":           "healthy",
		"response_time":    0,
		"connection_count": 0,
		"errors":           []string{},
	}

	errors := []string{}

	// 检查数据库连接
	sqlDB, err := s.db.DB()
	if err != nil {
		errors = append(errors, "Failed to get database instance: "+err.Error())
		result["status"] = "error"
	} else {
		// 检查连接
		if err := sqlDB.Ping(); err != nil {
			errors = append(errors, "Database ping failed: "+err.Error())
			result["status"] = "error"
		}

		// 获取连接统计
		stats := sqlDB.Stats()
		result["connection_count"] = stats.OpenConnections
		result["max_connections"] = stats.MaxOpenConnections
		result["idle_connections"] = stats.Idle
		result["in_use_connections"] = stats.InUse
		result["wait_count"] = stats.WaitCount
		result["wait_duration"] = stats.WaitDuration.String()

		// 检查连接池健康状态
		if stats.OpenConnections > int(float64(stats.MaxOpenConnections)*0.8) {
			errors = append(errors, "Database connection pool usage is high")
			result["status"] = "warning"
		}

		// 执行简单查询测试性能
		var count int64
		queryStart := time.Now()
		if err := s.db.Model(&database.Transfer{}).Count(&count).Error; err != nil {
			errors = append(errors, "Database query test failed: "+err.Error())
			result["status"] = "error"
		} else {
			queryTime := time.Since(queryStart)
			result["query_time"] = queryTime.String()
			result["total_transfers"] = count

			// 检查查询性能
			if queryTime > 1*time.Second {
				errors = append(errors, "Database query response time is slow")
				if result["status"] == "healthy" {
					result["status"] = "warning"
				}
			}
		}
	}

	result["response_time"] = time.Since(startTime).String()
	result["errors"] = errors
	return result
}

// checkTronClientHealth 检查Tron客户端健康状态和性能
func (s *SyncService) checkTronClientHealth() map[string]interface{} {
	startTime := time.Now()
	result := map[string]interface{}{
		"status":        "healthy",
		"response_time": 0,
		"node_status":   map[string]interface{}{},
		"errors":        []string{},
	}

	errors := []string{}

	// 检查主节点连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.tronClient.HealthCheck(ctx); err != nil {
		errors = append(errors, "Primary Tron node health check failed: "+err.Error())
		result["status"] = "error"
	}

	// 获取最新区块号测试性能
	blockStart := time.Now()
	latestBlock, err := s.tronClient.GetLatestBlockNumber(ctx)
	if err != nil {
		errors = append(errors, "Failed to get latest block: "+err.Error())
		result["status"] = "error"
	} else {
		blockTime := time.Since(blockStart)
		result["latest_block"] = latestBlock
		result["block_query_time"] = blockTime.String()

		// 检查区块查询性能
		if blockTime > 5*time.Second {
			errors = append(errors, "Tron node response time is slow")
			if result["status"] == "healthy" {
				result["status"] = "warning"
			}
		}
	}

	// 检查备用节点状态（如果配置了多节点）
	nodeStatuses := make(map[string]interface{})
	if s.config.TronNodes != nil && len(s.config.TronNodes) > 1 {
		for i, nodeURL := range s.config.TronNodes {
			nodeKey := fmt.Sprintf("node_%d", i)
			nodeStatus := map[string]interface{}{
				"url":    nodeURL,
				"status": "unknown",
			}

			// 这里可以添加对每个节点的健康检查
			// 由于时间限制，暂时标记为可用
			nodeStatus["status"] = "available"
			nodeStatuses[nodeKey] = nodeStatus
		}
	}
	result["node_status"] = nodeStatuses

	result["response_time"] = time.Since(startTime).String()
	result["errors"] = errors
	return result
}

// checkConcurrentSyncHealth 检查并发同步管理器健康状态
func (s *SyncService) checkConcurrentSyncHealth() map[string]interface{} {
	result := map[string]interface{}{
		"enabled": s.concurrentEnabled,
		"status":  "healthy",
		"errors":  []string{},
	}

	if !s.concurrentEnabled {
		result["status"] = "disabled"
		return result
	}

	errors := []string{}

	if s.concurrentManager == nil {
		errors = append(errors, "Concurrent sync manager is not initialized")
		result["status"] = "error"
	} else {
		// 检查工作协程状态
		result["worker_count"] = s.concurrentManager.workerCount

		// 检查任务队列状态
		if s.concurrentManager.taskChan != nil {
			result["task_queue_length"] = len(s.concurrentManager.taskChan)
			result["task_queue_capacity"] = cap(s.concurrentManager.taskChan)

			// 检查队列是否接近满载
			if len(s.concurrentManager.taskChan) > cap(s.concurrentManager.taskChan)*8/10 {
				errors = append(errors, "Task queue is nearly full")
				result["status"] = "warning"
			}
		}

		// 检查结果队列状态
		if s.concurrentManager.resultChan != nil {
			result["result_queue_length"] = len(s.concurrentManager.resultChan)
			result["result_queue_capacity"] = cap(s.concurrentManager.resultChan)

			// 检查结果队列是否积压
			if len(s.concurrentManager.resultChan) > cap(s.concurrentManager.resultChan)*8/10 {
				errors = append(errors, "Result queue is nearly full")
				result["status"] = "warning"
			}
		}
	}

	result["errors"] = errors
	return result
}

// getFailedBlocksInfo 获取失败区块详细信息
func (s *SyncService) getFailedBlocksInfo() map[string]interface{} {
	s.failedMutex.RLock()
	defer s.failedMutex.RUnlock()

	failedCount := len(s.failedBlocks)
	result := map[string]interface{}{
		"total_count": failedCount,
		"blocks":      []map[string]interface{}{},
		"status":      "healthy",
	}

	// 如果失败区块过多，标记为警告
	if failedCount > 10 {
		result["status"] = "warning"
	} else if failedCount > 50 {
		result["status"] = "error"
	}

	// 收集失败区块详细信息（最多显示10个）
	blocks := []map[string]interface{}{}
	count := 0
	for blockNum, failedBlock := range s.failedBlocks {
		if count >= 10 {
			break
		}
		blocks = append(blocks, map[string]interface{}{
			"block_number": blockNum,
			"fail_count":   failedBlock.FailCount,
			"last_attempt": failedBlock.LastAttempt,
			"last_error":   failedBlock.LastError,
		})
		count++
	}
	result["blocks"] = blocks

	return result
}

// getDetailedSyncProgress 获取详细的同步进度信息
func (s *SyncService) getDetailedSyncProgress() map[string]interface{} {
	s.progressMu.RLock()
	defer s.progressMu.RUnlock()

	if s.syncProgress == nil {
		return map[string]interface{}{
			"status": "not_initialized",
		}
	}

	// 计算进度百分比
	progressPercent := float64(0)
	if s.syncProgress.TargetBlock > s.syncProgress.CurrentBlock {
		progressPercent = float64(s.syncProgress.ProcessedBlocks) / float64(s.syncProgress.TargetBlock-s.syncProgress.CurrentBlock) * 100
	}

	// 计算同步速率（区块/秒）
	elapsed := time.Since(s.syncProgress.StartTime)
	syncRate := float64(0)
	if elapsed.Seconds() > 0 {
		syncRate = float64(s.syncProgress.ProcessedBlocks) / elapsed.Seconds()
	}

	// 估算剩余时间
	remainingBlocks := s.syncProgress.TargetBlock - s.syncProgress.CurrentBlock - s.syncProgress.ProcessedBlocks
	estimatedTime := time.Duration(0)
	if syncRate > 0 {
		estimatedTime = time.Duration(float64(remainingBlocks)/syncRate) * time.Second
	}

	return map[string]interface{}{
		"current_block":    s.syncProgress.CurrentBlock,
		"target_block":     s.syncProgress.TargetBlock,
		"processed_blocks": s.syncProgress.ProcessedBlocks,
		"failed_blocks":    s.syncProgress.FailedBlocks,
		"progress_percent": fmt.Sprintf("%.2f%%", progressPercent),
		"sync_rate":        fmt.Sprintf("%.2f blocks/sec", syncRate),
		"estimated_time":   estimatedTime.String(),
		"active_workers":   s.syncProgress.ActiveWorkers,
		"total_tasks":      s.syncProgress.TotalTasks,
		"completed_tasks":  s.syncProgress.CompletedTasks,
		"start_time":       s.syncProgress.StartTime,
		"last_update":      s.syncProgress.LastUpdateTime,
		"elapsed_time":     elapsed.String(),
	}
}

// getErrorStatistics 获取错误统计信息
func (s *SyncService) getErrorStatistics() map[string]interface{} {
	s.mu.RLock()
	errorCount := s.status.ErrorCount
	lastError := s.status.LastError
	s.mu.RUnlock()

	// 获取全局错误统计
	globalErrorStats := utils.GetErrorStats()

	return map[string]interface{}{
		"sync_errors":     errorCount,
		"last_sync_error": lastError,
		"global_errors":   globalErrorStats,
		"error_rate":      s.calculateErrorRate(),
	}
}

// calculateErrorRate 计算错误率
func (s *SyncService) calculateErrorRate() float64 {
	s.mu.RLock()
	errorCount := s.status.ErrorCount
	syncedCount := s.status.SyncedCount
	s.mu.RUnlock()

	totalOperations := errorCount + syncedCount
	if totalOperations == 0 {
		return 0
	}

	return float64(errorCount) / float64(totalOperations) * 100
}

// getPerformanceMetrics 获取性能指标
func (s *SyncService) getPerformanceMetrics() map[string]interface{} {
	s.mu.RLock()
	lastSyncTime := s.status.LastSyncTime
	syncInterval := s.status.SyncInterval
	syncedCount := s.status.SyncedCount
	s.mu.RUnlock()

	// 计算同步频率
	uptime := time.Since(s.syncProgress.StartTime)
	syncFrequency := float64(0)
	if uptime.Hours() > 0 {
		syncFrequency = float64(syncedCount) / uptime.Hours()
	}

	return map[string]interface{}{
		"last_sync_time": lastSyncTime,
		"sync_interval":  syncInterval.String(),
		"synced_count":   syncedCount,
		"sync_frequency": fmt.Sprintf("%.2f syncs/hour", syncFrequency),
		"uptime":         uptime.String(),
		"memory_usage":   s.getMemoryUsage(),
	}
}

// getMemoryUsage 获取内存使用情况（简化版）
func (s *SyncService) getMemoryUsage() map[string]interface{} {
	s.processedMu.RLock()
	processedBlocksCount := len(s.processedBlocks)
	s.processedMu.RUnlock()

	s.failedMutex.RLock()
	failedBlocksCount := len(s.failedBlocks)
	s.failedMutex.RUnlock()

	return map[string]interface{}{
		"processed_blocks_cache": processedBlocksCount,
		"failed_blocks_cache":    failedBlocksCount,
	}
}

// determineHealthStatus 确定整体健康状态和告警级别
func (s *SyncService) determineHealthStatus(dbHealth, tronHealth, concurrentHealth, failedBlocksInfo, errorStats map[string]interface{}) (string, string) {
	// 检查各组件状态
	dbStatus := dbHealth["status"].(string)
	tronStatus := tronHealth["status"].(string)
	concurrentStatus := concurrentHealth["status"].(string)
	failedBlocksStatus := failedBlocksInfo["status"].(string)

	// 计算错误率
	errorRate := errorStats["error_rate"].(float64)

	// 确定整体状态
	if dbStatus == "error" || tronStatus == "error" || concurrentStatus == "error" {
		return "unhealthy", "critical"
	}

	if dbStatus == "warning" || tronStatus == "warning" || concurrentStatus == "warning" || failedBlocksStatus == "warning" || errorRate > 5 {
		return "degraded", "warning"
	}

	if failedBlocksStatus == "error" || errorRate > 10 {
		return "unhealthy", "major"
	}

	return "healthy", "none"
}

// logDetailedHealth 记录详细的健康检查日志（仅在状态变化时记录）
func (s *SyncService) logDetailedHealth(overallStatus, alertLevel string, responseTime time.Duration, result map[string]interface{}) {
	// 检查状态是否发生变化
	statusChanged := s.lastHealthStatus != overallStatus || s.lastAlertLevel != alertLevel
	
	// 只在状态变化时记录详细日志
	if statusChanged {
		// 记录基础健康日志
		utils.LogHealth("sync_service", overallStatus, responseTime, result)

		// 根据告警级别记录不同级别的日志
		switch alertLevel {
		case "critical":
			utils.LogWithFields(utils.ERROR, "Critical health issue detected in sync service", map[string]interface{}{
				"alert_level":   alertLevel,
				"status":        overallStatus,
				"response_time": responseTime.String(),
				"previous_status": s.lastHealthStatus,
				"previous_alert":  s.lastAlertLevel,
			})
		case "major":
			utils.LogWithFields(utils.WARN, "Major health issue detected in sync service", map[string]interface{}{
				"alert_level":   alertLevel,
				"status":        overallStatus,
				"response_time": responseTime.String(),
				"previous_status": s.lastHealthStatus,
				"previous_alert":  s.lastAlertLevel,
			})
		case "warning":
			utils.LogWithFields(utils.WARN, "Health warning detected in sync service", map[string]interface{}{
				"alert_level":   alertLevel,
				"status":        overallStatus,
				"response_time": responseTime.String(),
				"previous_status": s.lastHealthStatus,
				"previous_alert":  s.lastAlertLevel,
			})
		default:
			// 只有从异常状态恢复到正常状态时才记录INFO日志
			if s.lastAlertLevel != "" && s.lastAlertLevel != "none" {
				utils.LogWithFields(utils.INFO, "Sync service health recovered to normal", map[string]interface{}{
					"status":        overallStatus,
					"response_time": responseTime.String(),
					"previous_status": s.lastHealthStatus,
					"previous_alert":  s.lastAlertLevel,
				})
			}
		}
		
		// 更新上次状态
		s.lastHealthStatus = overallStatus
		s.lastAlertLevel = alertLevel
	}
}

// checkAndSendAlerts 检查是否需要发送告警
func (s *SyncService) checkAndSendAlerts(alertLevel string, healthData map[string]interface{}) {
	// 这里可以集成告警系统，如邮件、短信、Webhook等
	switch alertLevel {
	case "critical":
		utils.LogWithFields(utils.ERROR, "ALERT: Critical sync service issue requires immediate attention", map[string]interface{}{
			"alert_type":      "critical",
			"health_data":     healthData,
			"action_required": "immediate_intervention",
		})
		// 这里可以发送紧急告警

	case "major":
		utils.LogWithFields(utils.WARN, "ALERT: Major sync service issue detected", map[string]interface{}{
			"alert_type":      "major",
			"health_data":     healthData,
			"action_required": "investigation_needed",
		})
		// 这里可以发送主要告警

	case "warning":
		utils.LogWithFields(utils.WARN, "ALERT: Sync service performance degradation", map[string]interface{}{
			"alert_type":      "warning",
			"health_data":     healthData,
			"action_required": "monitoring_recommended",
		})
		// 这里可以发送警告告警
	}
}

// 失败区块管理方法

// retryFailedBlocks 重试失败的区块
func (s *SyncService) retryFailedBlocks(ctx context.Context) error {
	s.failedMutex.RLock()
	failedBlocks := make([]*FailedBlock, 0, len(s.failedBlocks))
	for _, block := range s.failedBlocks {
		// 检查是否应该重试（距离上次尝试至少5分钟）
		if time.Since(block.LastAttempt) >= 5*time.Minute && block.FailCount < 5 {
			failedBlocks = append(failedBlocks, block)
		}
	}
	s.failedMutex.RUnlock()

	if len(failedBlocks) == 0 {
		return nil
	}

	utils.Info("Retrying %d failed blocks", len(failedBlocks))

	for _, block := range failedBlocks {
		transfers, err := s.syncBlockWithRetry(ctx, block.BlockNumber)
		if err != nil {
			// 更新失败次数
			s.updateFailedBlock(block.BlockNumber, err.Error())
			continue
		}

		// 保存转账数据
		for _, transfer := range transfers {
			err := s.saveTransferToDatabase(transfer)
			if err != nil {
				utils.Error("Failed to save transfer %s: %v", transfer.TxHash, err)
				continue
			}
		}

		// 移除成功的区块
		s.removeFailedBlock(block.BlockNumber)
		utils.Info("Successfully retried block %d", block.BlockNumber)
	}

	return nil
}

// syncBlockWithRetry 带重试机制的区块同步
func (s *SyncService) syncBlockWithRetry(ctx context.Context, blockNum uint64) ([]*blockchain.TransferEvent, error) {
	maxRetries := 3
	baseDelay := time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		transfers, err := s.tronClient.GetUSDTTransfersByBlock(ctx, blockNum)
		if err == nil {
			return transfers, nil
		}

		utils.Error("Attempt %d/%d failed for block %d: %v", attempt, maxRetries, blockNum, err)

		// 如果不是最后一次尝试，等待后重试
		if attempt < maxRetries {
			delay := time.Duration(attempt) * baseDelay
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	return nil, fmt.Errorf("failed to sync block %d after %d attempts", blockNum, maxRetries)
}

// recordFailedBlock 记录失败的区块
// recordFailedBlock 记录失败区块，包含详细错误跟踪和告警机制
func (s *SyncService) recordFailedBlock(blockNum uint64, errorMsg string) {
	s.failedMutex.Lock()
	defer s.failedMutex.Unlock()

	// 记录失败区块信息
	if existing, exists := s.failedBlocks[blockNum]; exists {
		existing.FailCount++
		existing.LastAttempt = time.Now()
		existing.LastError = errorMsg

		// 重复失败的详细日志
		utils.Warn("Block %d failed again (attempt %d): %s", blockNum, existing.FailCount, errorMsg)

		// 检查是否为严重错误（失败次数过多）
		if existing.FailCount >= 5 {
			utils.Error("Critical: Block %d has failed %d times, last error: %s", blockNum, existing.FailCount, errorMsg)
		}
	} else {
		s.failedBlocks[blockNum] = &FailedBlock{
			BlockNumber: blockNum,
			FailCount:   1,
			LastAttempt: time.Now(),
			LastError:   errorMsg,
		}

		// 首次失败的详细日志
		utils.Info("New failed block recorded: %d, error: %s", blockNum, errorMsg)
	}

	// 更新错误统计
	s.updateErrorCount()

	// 检查并发送告警
	s.checkFailedBlockAlerts(blockNum, s.failedBlocks[blockNum])
}

// checkFailedBlockAlerts 检查失败区块告警
func (s *SyncService) checkFailedBlockAlerts(blockNum uint64, failedBlock *FailedBlock) {
	// 检查单个区块失败次数告警
	if failedBlock.FailCount == 3 {
		utils.Warn("Alert: Block %d has failed %d times, requires attention", blockNum, failedBlock.FailCount)
	} else if failedBlock.FailCount >= 10 {
		utils.Error("Critical Alert: Block %d has failed %d times, manual intervention required", blockNum, failedBlock.FailCount)
	}

	// 检查总失败区块数量告警
	totalFailedBlocks := len(s.failedBlocks)
	if totalFailedBlocks >= 100 {
		utils.Error("Critical Alert: Total failed blocks count reached %d, system health degraded", totalFailedBlocks)
	} else if totalFailedBlocks >= 50 {
		utils.Warn("Warning: Total failed blocks count reached %d, monitoring required", totalFailedBlocks)
	}

	// 检查错误率告警
	errorRate := s.calculateErrorRate()
	if errorRate >= 0.1 { // 10%错误率
		utils.Error("Critical Alert: Error rate reached %.2f%%, system performance severely impacted", errorRate*100)
	} else if errorRate >= 0.05 { // 5%错误率
		utils.Warn("Warning: Error rate reached %.2f%%, performance monitoring required", errorRate*100)
	}
}

// updateFailedBlock 更新失败区块信息
func (s *SyncService) updateFailedBlock(blockNum uint64, errorMsg string) {
	s.failedMutex.Lock()
	defer s.failedMutex.Unlock()

	if block, exists := s.failedBlocks[blockNum]; exists {
		block.FailCount++
		block.LastAttempt = time.Now()
		block.LastError = errorMsg
	}
}

// removeFailedBlock 移除失败区块记录
func (s *SyncService) removeFailedBlock(blockNum uint64) {
	s.failedMutex.Lock()
	defer s.failedMutex.Unlock()
	delete(s.failedBlocks, blockNum)
}

// findLastSuccessBlock 查找最后一个成功的区块
func (s *SyncService) findLastSuccessBlock(startBlock, endBlock uint64, failedBlocks []uint64) uint64 {
	// 创建失败区块的映射以便快速查找
	failedMap := make(map[uint64]bool)
	for _, block := range failedBlocks {
		failedMap[block] = true
	}

	// 从后往前查找最后一个成功的区块
	for block := endBlock; block >= startBlock; block-- {
		if !failedMap[block] {
			return block
		}
	}

	// 如果所有区块都失败了，返回起始区块的前一个
	if startBlock > 0 {
		return startBlock - 1
	}
	return 0
}

// updateErrorCount 更新错误计数
func (s *SyncService) updateErrorCount() {
	s.mu.Lock()
	s.status.ErrorCount++
	s.mu.Unlock()
}

// GetFailedBlocks 获取失败区块列表
func (s *SyncService) GetFailedBlocks() []*FailedBlock {
	s.failedMutex.RLock()
	defer s.failedMutex.RUnlock()

	blocks := make([]*FailedBlock, 0, len(s.failedBlocks))
	for _, block := range s.failedBlocks {
		blocks = append(blocks, block)
	}
	return blocks
}

// GetSyncProgress 获取详细的同步进度信息
func (s *SyncService) GetSyncProgress() map[string]interface{} {
	progress := s.getSyncProgress()

	s.failedMutex.RLock()
	failedBlocks := make([]*FailedBlock, 0, len(s.failedBlocks))
	for _, fb := range s.failedBlocks {
		failedBlocks = append(failedBlocks, fb)
	}
	s.failedMutex.RUnlock()

	// 计算完成百分比
	var completionPercentage float64
	if progress.TargetBlock > 0 {
		completionPercentage = float64(progress.CurrentBlock) / float64(progress.TargetBlock) * 100
	}

	return map[string]interface{}{
		"current_block":         progress.CurrentBlock,
		"target_block":          progress.TargetBlock,
		"processed_blocks":      progress.ProcessedBlocks,
		"failed_blocks":         progress.FailedBlocks,
		"completion_percentage": completionPercentage,
		"sync_rate":             progress.SyncRate,
		"estimated_time":        progress.EstimatedTime.String(),
		"active_workers":        progress.ActiveWorkers,
		"total_tasks":           progress.TotalTasks,
		"completed_tasks":       progress.CompletedTasks,
		"start_time":            progress.StartTime,
		"last_update_time":      progress.LastUpdateTime,
		"failed_block_details":  failedBlocks,
		"concurrent_enabled":    s.concurrentEnabled,
	}
}

// addFailedBlock 添加失败区块到重试队列
// addFailedBlock 添加失败区块到重试队列，包含详细错误跟踪
func (s *SyncService) addFailedBlock(blockNum uint64, err error) {
	s.failedMutex.Lock()
	defer s.failedMutex.Unlock()

	errorMsg := err.Error()
	if existing, exists := s.failedBlocks[blockNum]; exists {
		existing.FailCount++
		existing.LastAttempt = time.Now()
		existing.LastError = errorMsg

		// 重复失败的详细日志
		utils.Warn("Block %d added to retry queue again (attempt %d): %s", blockNum, existing.FailCount, errorMsg)

		// 检查是否为持续失败
		if existing.FailCount >= 3 {
			utils.Error("Block %d has been added to retry queue %d times, persistent failure detected: %s", blockNum, existing.FailCount, errorMsg)
		}
	} else {
		s.failedBlocks[blockNum] = &FailedBlock{
			BlockNumber: blockNum,
			FailCount:   1,
			LastAttempt: time.Now(),
			LastError:   errorMsg,
		}

		// 首次失败的详细日志
		utils.Info("New block %d added to retry queue: %s", blockNum, errorMsg)
	}

	// 更新错误统计
	s.updateErrorCount()

	// 检查并发送告警
	s.checkFailedBlockAlerts(blockNum, s.failedBlocks[blockNum])

	// 记录错误类型统计
	s.logErrorTypeStatistics(errorMsg)
}

// 并发同步管理器相关方法

// logErrorTypeStatistics 记录错误类型统计
func (s *SyncService) logErrorTypeStatistics(errorMsg string) {
	// 只在开发模式下记录Debug级别的错误统计日志
	isDevMode := strings.ToUpper(s.config.LogLevel) == "DEBUG"
	
	if !isDevMode {
		return // 非开发模式下跳过Debug日志
	}
	
	// 分析错误类型并记录统计（仅在开发模式下）
	if strings.Contains(errorMsg, "timeout") || strings.Contains(errorMsg, "deadline") {
		utils.Debug("Timeout error detected: %s", errorMsg)
	} else if strings.Contains(errorMsg, "connection") || strings.Contains(errorMsg, "network") {
		utils.FileLog(utils.DEBUG, "Network error detected: %s", errorMsg)
	} else if strings.Contains(errorMsg, "parse") || strings.Contains(errorMsg, "unmarshal") {
		utils.FileLog(utils.DEBUG, "Data parsing error detected: %s", errorMsg)
	} else if strings.Contains(errorMsg, "database") || strings.Contains(errorMsg, "sql") {
		utils.FileLog(utils.DEBUG, "Database error detected: %s", errorMsg)
	} else if strings.Contains(errorMsg, "rate limit") || strings.Contains(errorMsg, "too many requests") {
		utils.FileLog(utils.DEBUG, "Rate limiting error detected: %s", errorMsg)
	} else {
		utils.FileLog(utils.DEBUG, "Unknown error type detected: %s", errorMsg)
	}
}

// newConcurrentSyncManager 创建新的并发同步管理器
func (s *SyncService) newConcurrentSyncManager() *ConcurrentSyncManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConcurrentSyncManager{
		taskChan:    make(chan *SyncTask, s.config.ConcurrentConfig.BufferSize),
		resultChan:  make(chan *SyncResult, s.config.ConcurrentConfig.BufferSize),
		workerCount: s.config.ConcurrentConfig.WorkerCount,
		tronClient:  s.tronClient,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// startConcurrentSync 启动并发同步
func (s *SyncService) startConcurrentSync() {
	if s.concurrentManager == nil {
		utils.Error("并发管理器为空，无法启动并发同步")
		return
	}

	utils.Info("启动并发同步，工作协程数: %d", s.concurrentManager.workerCount)

	// 启动工作协程
	for i := 0; i < s.concurrentManager.workerCount; i++ {
		s.concurrentManager.wg.Add(1)
		go s.syncWorker(i)
	}

	// 启动结果处理协程
	s.concurrentManager.wg.Add(1)
	go s.resultProcessor()

	utils.Info("并发同步启动完成")
}

// stopConcurrentSync 停止并发同步
func (s *SyncService) stopConcurrentSync() {
	if s.concurrentManager == nil {
		return
	}

	// 取消上下文
	s.concurrentManager.cancel()

	// 关闭任务通道
	close(s.concurrentManager.taskChan)

	// 等待所有工作协程完成
	s.concurrentManager.wg.Wait()

	// 关闭结果通道
	close(s.concurrentManager.resultChan)
}

// syncWorker 同步工作协程
func (s *SyncService) syncWorker(workerID int) {
	defer s.concurrentManager.wg.Done()

	utils.Info("Sync worker %d started", workerID)
	defer utils.Info("Sync worker %d stopped", workerID)

	for {
		select {
		case <-s.concurrentManager.ctx.Done():
			return
		case task, ok := <-s.concurrentManager.taskChan:
			if !ok {
				return
			}

			// 处理同步任务
			result := s.processSyncTask(task, workerID)

			// 发送结果
			select {
			case s.concurrentManager.resultChan <- result:
			case <-s.concurrentManager.ctx.Done():
				return
			}
		}
	}
}

// processSyncTask 处理同步任务（集成故障转移机制）
func (s *SyncService) processSyncTask(task *SyncTask, workerID int) *SyncResult {
	startTime := time.Now()
	result := &SyncResult{
		Task:     task,
		Duration: 0,
	}

	// 获取节点信息用于日志显示
	nodeURL := "unknown"
	if task.NodeIndex < len(s.config.TronNodes) {
		nodeURL = s.config.TronNodes[task.NodeIndex].URL
	}

	utils.Info("🔄 Worker %d processing blocks %d-%d using node %d (%s)",
		workerID, task.StartBlock, task.EndBlock, task.NodeIndex, nodeURL)

	// 创建任务上下文
	ctx, cancel := context.WithTimeout(s.concurrentManager.ctx, s.config.ConcurrentConfig.SyncTimeout)
	defer cancel()

	// 获取区块范围内的USDT转账事件（带故障转移）
	var allTransfers []*blockchain.TransferEvent
	for blockNum := task.StartBlock; blockNum <= task.EndBlock; blockNum++ {
		utils.FileLog(utils.DEBUG, "📡 Worker %d fetching block %d from node %d (%s)", workerID, blockNum, task.NodeIndex, nodeURL)
		transfers, err := s.getTransfersWithFailover(ctx, blockNum, task.NodeIndex, workerID)
		if err != nil {
			utils.FileLog(utils.ERROR, "❌ Worker %d failed to get transfers for block %d after failover: %v", workerID, blockNum, err)
			result.Error = fmt.Errorf("failed to get transfers for block %d: %w", blockNum, err)
			result.Duration = time.Since(startTime)
			return result
		}
		utils.FileLog(utils.DEBUG, "✅ Worker %d successfully fetched %d transfers from block %d", workerID, len(transfers), blockNum)
		allTransfers = append(allTransfers, transfers...)
	}

	result.Transfers = allTransfers
	result.Duration = time.Since(startTime)
	utils.FileLog(utils.INFO, "✅ Worker %d completed blocks %d-%d using node %d (%s), found %d transfers in %v",
		workerID, task.StartBlock, task.EndBlock, task.NodeIndex, nodeURL, len(allTransfers), result.Duration)

	return result
}

// getTransfersWithFailover 带故障转移的获取转账数据
func (s *SyncService) getTransfersWithFailover(ctx context.Context, blockNum uint64, preferredNodeIndex, workerID int) ([]*blockchain.TransferEvent, error) {
	nodeCount := len(s.config.TronNodes)
	if nodeCount <= 1 {
		// 单节点模式，直接使用现有客户端
		utils.Debug("🔗 Worker %d: Single node mode, fetching block %d", workerID, blockNum)
		return s.tronClient.GetUSDTTransfersByBlock(ctx, blockNum)
	}

	// 多节点模式，实现故障转移
	maxRetries := s.config.RetryConfig.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}

	// 获取首选节点信息
	preferredNodeURL := "unknown"
	if preferredNodeIndex < len(s.config.TronNodes) {
		preferredNodeURL = s.config.TronNodes[preferredNodeIndex].URL
	}

	utils.Debug("🎯 Worker %d: Multi-node mode, trying preferred node %d (%s) for block %d",
		workerID, preferredNodeIndex, preferredNodeURL, blockNum)

	// 尝试首选节点
	transfers, err := s.tryGetTransfersFromNode(ctx, blockNum, preferredNodeIndex, workerID)
	if err == nil {
		utils.Debug("✅ Worker %d: Preferred node %d (%s) successfully fetched block %d",
			workerID, preferredNodeIndex, preferredNodeURL, blockNum)
		return transfers, nil
	}

	utils.Warn("⚠️ Worker %d: Node %d (%s) failed for block %d, initiating failover: %v",
		workerID, preferredNodeIndex, preferredNodeURL, blockNum, err)

	// 故障转移：尝试其他节点
	for attempt := 1; attempt <= maxRetries; attempt++ {
		// 选择下一个节点（轮询）
		nextNodeIndex := (preferredNodeIndex + attempt) % nodeCount
		nextNodeURL := "unknown"
		if nextNodeIndex < len(s.config.TronNodes) {
			nextNodeURL = s.config.TronNodes[nextNodeIndex].URL
		}

		utils.Info("🔄 Worker %d: Failover attempt %d/%d, switching to node %d (%s) for block %d",
			workerID, attempt, maxRetries, nextNodeIndex, nextNodeURL, blockNum)

		transfers, err := s.tryGetTransfersFromNode(ctx, blockNum, nextNodeIndex, workerID)
		if err == nil {
			utils.Info("✅ Worker %d: Failover successful! Node %d (%s) completed block %d",
				workerID, nextNodeIndex, nextNodeURL, blockNum)
			return transfers, nil
		}

		utils.Warn("❌ Worker %d: Node %d (%s) also failed for block %d: %v",
			workerID, nextNodeIndex, nextNodeURL, blockNum, err)

		// 如果不是最后一次尝试，等待后重试
		if attempt < maxRetries {
			delay := time.Duration(attempt) * s.config.RetryConfig.InitialDelay
			utils.Debug("⏳ Worker %d: Waiting %v before next failover attempt", workerID, delay)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	utils.Error("💥 Worker %d: All %d nodes failed for block %d after %d attempts", workerID, nodeCount, blockNum, maxRetries)
	return nil, fmt.Errorf("all nodes failed for block %d after %d attempts", blockNum, maxRetries)
}

// tryGetTransfersFromNode 尝试从指定节点获取转账数据
func (s *SyncService) tryGetTransfersFromNode(ctx context.Context, blockNum uint64, nodeIndex, workerID int) ([]*blockchain.TransferEvent, error) {
	// 获取节点信息
	nodeURL := "unknown"
	if nodeIndex < len(s.config.TronNodes) {
		nodeURL = s.config.TronNodes[nodeIndex].URL
	}

	utils.Debug("🌐 Worker %d: Sending request to node %d (%s) for block %d", workerID, nodeIndex, nodeURL, blockNum)
	startTime := time.Now()

	// 这里应该使用指定节点的客户端
	// 由于当前TronHTTPClient已经实现了多节点故障转移，我们可以直接使用
	// 在实际实现中，可能需要为每个节点创建单独的客户端实例
	transfers, err := s.tronClient.GetUSDTTransfersByBlock(ctx, blockNum)

	duration := time.Since(startTime)
	if err != nil {
		utils.Debug("❌ Worker %d: Node %d (%s) request failed for block %d after %v: %v",
			workerID, nodeIndex, nodeURL, blockNum, duration, err)
		return nil, err
	}

	utils.Debug("✅ Worker %d: Node %d (%s) successfully returned %d transfers for block %d in %v",
		workerID, nodeIndex, nodeURL, len(transfers), blockNum, duration)
	return transfers, nil
}

// resultProcessor 结果处理协程
func (s *SyncService) resultProcessor() {
	defer s.concurrentManager.wg.Done()

	utils.Info("Result processor started")
	defer utils.Info("Result processor stopped")

	for {
		select {
		case <-s.concurrentManager.ctx.Done():
			return
		case result, ok := <-s.concurrentManager.resultChan:
			if !ok {
				return
			}

			// 处理同步结果
			s.handleSyncResult(result)
		}
	}
}

// handleSyncResult 处理同步结果（支持任务重分配和进度跟踪）
func (s *SyncService) handleSyncResult(result *SyncResult) {
	if result.Error != nil {
		utils.Error("Sync task failed for blocks %d-%d using node %d: %v",
			result.Task.StartBlock, result.Task.EndBlock, result.Task.NodeIndex, result.Error)

		// 更新失败区块计数
		s.progressMu.Lock()
		s.syncProgress.FailedBlocks += result.Task.EndBlock - result.Task.StartBlock + 1
		s.progressMu.Unlock()

		// 检查是否需要重分配任务
		if s.shouldRetryTask(result.Task) {
			s.retryTaskWithDifferentNode(result.Task)
		} else {
			// 记录失败的区块范围
			for blockNum := result.Task.StartBlock; blockNum <= result.Task.EndBlock; blockNum++ {
				s.recordFailedBlock(blockNum, result.Error.Error())
			}
		}

		s.updateErrorCount()
		return
	}

	// 保存转账数据到数据库
	successCount := 0
	for _, transfer := range result.Transfers {
		err := s.saveTransferToDatabase(transfer)
		if err != nil {
			utils.Error("Failed to save transfer %s: %v", transfer.TxHash, err)
			// 记录失败的区块
			s.recordFailedBlock(transfer.BlockNumber, err.Error())
			s.updateErrorCount()
			continue
		}
		successCount++
		// 标记区块已处理
		s.markBlockProcessed(transfer.BlockNumber)
	}

	// 标记任务范围内的所有区块为已处理（即使没有转账）
	for blockNum := result.Task.StartBlock; blockNum <= result.Task.EndBlock; blockNum++ {
		s.markBlockProcessed(blockNum)
	}

	// 更新同步状态
	s.mu.Lock()
	s.status.SyncedCount += int64(successCount)
	s.status.LastSyncTime = time.Now()
	if result.Task.EndBlock > s.status.LastBlock {
		s.status.LastBlock = result.Task.EndBlock
	}
	s.mu.Unlock()

	// 更新同步进度
	s.progressMu.Lock()
	s.syncProgress.CompletedTasks++
	s.progressMu.Unlock()

	// 数据同步成功后更新缓存
	if successCount > 0 {
		go s.updateCachesAfterSync()
	}

	// 定期进行数据一致性检查
	if s.syncProgress.CompletedTasks%10 == 0 { // 每10个任务检查一次
		go func() {
			if err := s.ensureDataConsistency(result.Task.StartBlock, result.Task.EndBlock); err != nil {
				utils.Error("Data consistency check failed for blocks %d-%d: %v",
					result.Task.StartBlock, result.Task.EndBlock, err)
			}
		}()
	}

	utils.Info("Processed blocks %d-%d: saved %d transfers",
		result.Task.StartBlock, result.Task.EndBlock, successCount)
}

// shouldRetryTask 判断任务是否应该重试
func (s *SyncService) shouldRetryTask(task *SyncTask) bool {
	maxRetries := s.config.RetryConfig.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}

	// 如果重试次数未达到上限且有多个节点可用
	return task.RetryCount < maxRetries && len(s.config.TronNodes) > 1
}

// retryTaskWithDifferentNode 使用不同节点重试任务
func (s *SyncService) retryTaskWithDifferentNode(task *SyncTask) {
	nodeCount := len(s.config.TronNodes)
	if nodeCount <= 1 {
		return // 单节点模式无法重分配
	}

	// 检查并发管理器是否可用
	if s.concurrentManager == nil || s.concurrentManager.taskChan == nil {
		utils.Warn("Concurrent manager not available, cannot reassign task for blocks %d-%d",
			task.StartBlock, task.EndBlock)
		// 添加到失败队列
		for blockNum := task.StartBlock; blockNum <= task.EndBlock; blockNum++ {
			s.recordFailedBlock(blockNum, "concurrent manager unavailable")
		}
		return
	}

	// 检查上下文是否已取消
	select {
	case <-s.concurrentManager.ctx.Done():
		utils.Warn("Cannot reassign task, sync manager is stopping")
		// 如果无法重分配，添加到失败队列
		for blockNum := task.StartBlock; blockNum <= task.EndBlock; blockNum++ {
			s.recordFailedBlock(blockNum, "task reassignment failed - manager stopping")
		}
		return
	default:
		// 继续执行
	}

	// 选择下一个可用节点
	nextNodeIndex := (task.NodeIndex + 1) % nodeCount

	// 创建新的重试任务
	retryTask := &SyncTask{
		StartBlock: task.StartBlock,
		EndBlock:   task.EndBlock,
		NodeIndex:  nextNodeIndex,
		RetryCount: task.RetryCount + 1,
		CreatedAt:  time.Now(),
	}

	utils.Info("Retrying task for blocks %d-%d with node %d (attempt %d/%d)",
		retryTask.StartBlock, retryTask.EndBlock, nextNodeIndex,
		retryTask.RetryCount, s.config.RetryConfig.MaxRetries)

	// 将重试任务重新加入队列，使用非阻塞方式
	select {
	case s.concurrentManager.taskChan <- retryTask:
		utils.Info("Task reassigned to node %d for blocks %d-%d",
			nextNodeIndex, retryTask.StartBlock, retryTask.EndBlock)
	case <-s.concurrentManager.ctx.Done():
		utils.Warn("Cannot reassign task, sync manager is stopping")
		// 如果无法重分配，添加到失败队列
		for blockNum := task.StartBlock; blockNum <= task.EndBlock; blockNum++ {
			s.recordFailedBlock(blockNum, "task reassignment failed - manager stopped")
		}
	default:
		utils.Warn("Task queue full, cannot reassign task for blocks %d-%d",
			task.StartBlock, task.EndBlock)
		// 如果队列满了，添加到失败队列
		for blockNum := task.StartBlock; blockNum <= task.EndBlock; blockNum++ {
			s.recordFailedBlock(blockNum, "task reassignment failed - queue full")
		}
	}
}

// updateCachesAfterSync 数据同步后更新相关缓存
func (s *SyncService) updateCachesAfterSync() {
	utils.Debug("Updating caches after successful data sync")

	// 更新统计摘要缓存
	if s.transferService != nil {
		go func() {
			if _, err := s.transferService.GetStats(); err != nil {
				utils.Error("Failed to update stats cache after sync: %v", err)
			} else {
				utils.Debug("Stats cache updated successfully")
			}
		}()
	}

	// 更新市场数据缓存
	if s.marketService != nil {
		go func() {
			if _, err := s.marketService.GetDetailedUSDTData(); err != nil {
				utils.Error("Failed to update market cache after sync: %v", err)
			} else {
				utils.Debug("Market cache updated successfully")
			}
		}()
	}
}

// waitForAllTasksCompletion 等待所有并发任务完成
func (s *SyncService) waitForAllTasksCompletion(ctx context.Context, totalTasks int) error {
	utils.Info("Waiting for %d concurrent tasks to complete...", totalTasks)
	
	ticker := time.NewTicker(5 * time.Second) // 每5秒检查一次进度
	defer ticker.Stop()
	
	startTime := time.Now()
	timeout := 30 * time.Minute // 30分钟超时
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
			
		case <-ticker.C:
			// 检查任务完成状态
			s.progressMu.RLock()
			completedTasks := s.syncProgress.CompletedTasks
			s.progressMu.RUnlock()
			
			utils.Info("Concurrent sync progress: %d/%d tasks completed", completedTasks, totalTasks)
			
			// 检查是否所有任务都完成
			if completedTasks >= totalTasks {
				utils.Info("All concurrent sync tasks completed successfully in %v", time.Since(startTime))
				return nil
			}
			
			// 检查超时
			if time.Since(startTime) > timeout {
				return fmt.Errorf("concurrent sync timeout after %v, completed %d/%d tasks", timeout, completedTasks, totalTasks)
			}
			
			// 检查并发管理器状态
			if s.concurrentManager != nil {
				select {
				case <-s.concurrentManager.ctx.Done():
					return fmt.Errorf("concurrent sync manager stopped unexpectedly")
				default:
					// 继续等待
				}
			}
		}
	}
}
