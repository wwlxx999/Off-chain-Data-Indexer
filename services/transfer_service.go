package services

import (
	"errors"
	"time"

	"Off-chainDatainDexer/database"
	"Off-chainDatainDexer/utils"

	"gorm.io/gorm"
)

// TransferRequest 创建转账记录的请求结构
type TransferRequest struct {
	FromAddress     string `json:"from_address" binding:"required"`
	ToAddress       string `json:"to_address" binding:"required"`
	Amount          string `json:"amount" binding:"required"`
	TransactionHash string `json:"transaction_hash" binding:"required"`
	BlockNumber     uint64 `json:"block_number" binding:"required"`
}

// TransferResponse 转账记录的响应结构
type TransferResponse struct {
	ID              uint      `json:"id"`
	FromAddress     string    `json:"from_address"`
	ToAddress       string    `json:"to_address"`
	Amount          string    `json:"amount"`
	TransactionHash string    `json:"transaction_hash"`
	BlockNumber     uint64    `json:"block_number"`
	Timestamp       time.Time `json:"timestamp"`
	CreatedAt       time.Time `json:"created_at"`
}

// TransferListResponse 转账记录列表响应
type TransferListResponse struct {
	Transfers []TransferResponse `json:"transfers"`
	Total     int64              `json:"total"`
	Page      int                `json:"page"`
	PageSize  int                `json:"page_size"`
}

// StatsResponse 统计信息响应
type StatsResponse struct {
	TotalTransfers    int64  `json:"total_transfers"`
	TotalAmount       string `json:"total_amount"`
	UniqueAddresses   int64  `json:"unique_addresses"`
	LatestBlockNumber uint64 `json:"latest_block_number"`
}

// HourlyStatsResponse 小时统计信息响应
type HourlyStatsResponse struct {
	TransferCount   int64     `json:"transfer_count"`
	TotalAmount     string    `json:"total_amount"`
	UniqueAddresses int64     `json:"unique_addresses"`
	TimeRange       string    `json:"time_range"`
	StartTime       time.Time `json:"start_time"`
	EndTime         time.Time `json:"end_time"`
}

// TransferService 转账服务
type TransferService struct {
	db           *gorm.DB
	cacheService *CacheService
	bytePool     *utils.BytePool
	perfMonitor  *utils.PerformanceMonitor
}

// NewTransferService 创建新的转账服务
func NewTransferService(db *gorm.DB) *TransferService {
	return &TransferService{
		db:          db,
		bytePool:    utils.NewBytePool(4096), // 4KB缓冲池
		perfMonitor: utils.NewPerformanceMonitor(),
	}
}

// SetCacheService 设置缓存服务
func (s *TransferService) SetCacheService(cache *CacheService) {
	s.cacheService = cache
}

// GetTransfers 获取转账记录列表（使用分布式锁防止缓存击穿）
func (s *TransferService) GetTransfers(page, pageSize int) (*TransferListResponse, error) {
	start := time.Now()
	defer func() {
		s.perfMonitor.Record("GetTransfers", time.Since(start))
	}()

	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}

	// 使用分布式锁防止缓存击穿
	if s.cacheService != nil {
		cacheKey := CacheKeys.TransferListKey(page, pageSize)
		var response TransferListResponse

		err := s.cacheService.GetWithLock(cacheKey, &response, 30*time.Second, func() (interface{}, error) {
			return s.fetchTransfersFromDB(page, pageSize)
		})

		if err != nil {
			utils.Warn("Failed to get transfers with lock, falling back to direct DB query: %v", err)
			return s.fetchTransfersFromDB(page, pageSize)
		}

		s.perfMonitor.Record("GetTransfers_Cache_Hit", time.Since(start))
		return &response, nil
	}

	// 如果没有缓存服务，直接查询数据库
	return s.fetchTransfersFromDB(page, pageSize)
}

// fetchTransfersFromDB 从数据库获取转账记录列表
func (s *TransferService) fetchTransfersFromDB(page, pageSize int) (*TransferListResponse, error) {
	var transfers []database.Transfer
	var total int64

	// 获取总数
	if err := database.DB.Model(&database.Transfer{}).Count(&total).Error; err != nil {
		return nil, utils.NewDatabaseError("Failed to count transfers", err)
	}

	// 分页查询
	offset := (page - 1) * pageSize
	if err := database.DB.Order("created_at DESC").Offset(offset).Limit(pageSize).Find(&transfers).Error; err != nil {
		return nil, utils.NewDatabaseError("Failed to query transfers", err)
	}

	// 转换为响应格式
	responseTransfers := make([]TransferResponse, len(transfers))
	for i, transfer := range transfers {
		responseTransfers[i] = TransferResponse{
			ID:              transfer.ID,
			FromAddress:     transfer.FromAddress,
			ToAddress:       transfer.ToAddress,
			Amount:          transfer.Amount,
			TransactionHash: transfer.TransactionHash,
			BlockNumber:     transfer.BlockNumber,
			Timestamp:       transfer.Timestamp,
			CreatedAt:       transfer.CreatedAt,
		}
	}

	response := &TransferListResponse{
		Transfers: responseTransfers,
		Total:     total,
		Page:      page,
		PageSize:  pageSize,
	}

	// 缓存结果（缓存5分钟）
	if s.cacheService != nil {
		cacheKey := CacheKeys.TransferListKey(page, pageSize)
		if err := s.cacheService.Set(cacheKey, response, 5*time.Minute); err != nil {
			utils.Warn("Failed to cache transfer list: %v", err)
		}
	}

	return response, nil
}

// GetTransfersByAddress 根据地址获取转账记录（使用分布式锁防止缓存击穿）
func (s *TransferService) GetTransfersByAddress(address string, page, pageSize int) (*TransferListResponse, error) {
	if address == "" {
		return nil, errors.New("address cannot be empty")
	}

	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}

	// 使用分布式锁防止缓存击穿
	if s.cacheService != nil {
		cacheKey := CacheKeys.TransferByAddressKey(address, page, pageSize)
		var response TransferListResponse

		err := s.cacheService.GetWithLock(cacheKey, &response, 30*time.Second, func() (interface{}, error) {
			return s.fetchTransfersByAddressFromDB(address, page, pageSize)
		})

		if err != nil {
			utils.Warn("Failed to get transfers by address with lock, falling back to direct DB query: %v", err)
			return s.fetchTransfersByAddressFromDB(address, page, pageSize)
		}

		return &response, nil
	}

	// 如果没有缓存服务，直接查询数据库
	return s.fetchTransfersByAddressFromDB(address, page, pageSize)
}

// fetchTransfersByAddressFromDB 从数据库根据地址获取转账记录
func (s *TransferService) fetchTransfersByAddressFromDB(address string, page, pageSize int) (*TransferListResponse, error) {
	var transfers []database.Transfer
	var total int64

	// 查询条件：发送方或接收方地址匹配
	query := database.DB.Where("from_address = ? OR to_address = ?", address, address)

	// 获取总数
	if err := query.Model(&database.Transfer{}).Count(&total).Error; err != nil {
		return nil, utils.NewDatabaseError("Failed to count transfers by address", err)
	}

	// 分页查询
	offset := (page - 1) * pageSize
	if err := query.Order("created_at DESC").Offset(offset).Limit(pageSize).Find(&transfers).Error; err != nil {
		return nil, utils.NewDatabaseError("Failed to query transfers by address", err)
	}

	// 转换为响应格式
	responseTransfers := make([]TransferResponse, len(transfers))
	for i, transfer := range transfers {
		responseTransfers[i] = TransferResponse{
			ID:              transfer.ID,
			FromAddress:     transfer.FromAddress,
			ToAddress:       transfer.ToAddress,
			Amount:          transfer.Amount,
			TransactionHash: transfer.TransactionHash,
			BlockNumber:     transfer.BlockNumber,
			Timestamp:       transfer.Timestamp,
			CreatedAt:       transfer.CreatedAt,
		}
	}

	return &TransferListResponse{
		Transfers: responseTransfers,
		Total:     total,
		Page:      page,
		PageSize:  pageSize,
	}, nil
}

// GetTransferByHash 根据交易哈希获取转账记录
func (s *TransferService) GetTransferByHash(hash string) (*TransferResponse, error) {
	if hash == "" {
		return nil, errors.New("transaction hash cannot be empty")
	}

	var transfer database.Transfer
	if err := database.DB.Where("transaction_hash = ?", hash).First(&transfer).Error; err != nil {
		return nil, utils.NewDatabaseError("Failed to find transfer by hash", err)
	}

	return &TransferResponse{
		ID:              transfer.ID,
		FromAddress:     transfer.FromAddress,
		ToAddress:       transfer.ToAddress,
		Amount:          transfer.Amount,
		TransactionHash: transfer.TransactionHash,
		BlockNumber:     transfer.BlockNumber,
		Timestamp:       transfer.Timestamp,
		CreatedAt:       transfer.CreatedAt,
	}, nil
}

// CreateTransfer 创建转账记录
func (s *TransferService) CreateTransfer(req *TransferRequest) (*TransferResponse, error) {
	// 检查交易哈希是否已存在
	var existingTransfer database.Transfer
	if err := database.DB.Where("transaction_hash = ?", req.TransactionHash).First(&existingTransfer).Error; err == nil {
		return nil, utils.NewConflictError("Transfer with this transaction hash already exists", nil)
	}

	// 创建新的转账记录
	transfer := database.Transfer{
		FromAddress:     req.FromAddress,
		ToAddress:       req.ToAddress,
		Amount:          req.Amount,
		TransactionHash: req.TransactionHash,
		BlockNumber:     req.BlockNumber,
		Timestamp:       time.Now(),
		CreatedAt:       time.Now(),
	}

	if err := database.DB.Create(&transfer).Error; err != nil {
		return nil, utils.NewDatabaseError("Failed to create transfer record", err)
	}

	return &TransferResponse{
		ID:              transfer.ID,
		FromAddress:     transfer.FromAddress,
		ToAddress:       transfer.ToAddress,
		Amount:          transfer.Amount,
		TransactionHash: transfer.TransactionHash,
		BlockNumber:     transfer.BlockNumber,
		Timestamp:       transfer.Timestamp,
		CreatedAt:       transfer.CreatedAt,
	}, nil
}

// GetStats 获取统计信息（使用分布式锁防止缓存击穿）
func (s *TransferService) GetStats() (*StatsResponse, error) {
	if s.cacheService != nil {
		cacheKey := CacheKeys.StatsKey()
		var stats StatsResponse

		// 使用分布式锁防止缓存击穿
		err := s.cacheService.GetWithLock(cacheKey, &stats, 30*time.Second, func() (interface{}, error) {
			return s.fetchStatsFromDB()
		})

		if err != nil {
			utils.Warn("Failed to get stats with lock, falling back to direct DB query: %v", err)
			return s.fetchStatsFromDB()
		}

		return &stats, nil
	}

	// 如果没有缓存服务，直接查询数据库
	return s.fetchStatsFromDB()
}

// fetchStatsFromDB 从数据库获取统计信息
func (s *TransferService) fetchStatsFromDB() (*StatsResponse, error) {
	var totalTransfers int64
	var uniqueAddresses int64
	var latestBlockNumber uint64

	// 获取总转账数
	if err := database.DB.Model(&database.Transfer{}).Count(&totalTransfers).Error; err != nil {
		return nil, utils.NewDatabaseError("Failed to count total transfers", err)
	}

	// 获取唯一地址数（发送方和接收方）
	var result struct {
		Count int64
	}
	if err := database.DB.Raw(`
		SELECT COUNT(DISTINCT address) as count FROM (
			SELECT from_address as address FROM transfers
			UNION
			SELECT to_address as address FROM transfers
		) as unique_addresses
	`).Scan(&result).Error; err != nil {
		return nil, utils.NewDatabaseError("Failed to count unique addresses", err)
	}
	uniqueAddresses = result.Count

	// 获取最新区块号，使用COALESCE处理空表情况
	if err := database.DB.Model(&database.Transfer{}).Select("COALESCE(MAX(block_number), 0)").Scan(&latestBlockNumber).Error; err != nil {
		return nil, utils.NewDatabaseError("Failed to get latest block number", err)
	}

	stats := &StatsResponse{
		TotalTransfers:    totalTransfers,
		TotalAmount:       "0", // 这里可以根据需要计算总金额
		UniqueAddresses:   uniqueAddresses,
		LatestBlockNumber: latestBlockNumber,
	}

	// 缓存结果（缓存10分钟）
	if s.cacheService != nil {
		cacheKey := CacheKeys.StatsKey()
		if err := s.cacheService.Set(cacheKey, stats, 10*time.Minute); err != nil {
			utils.Warn("Failed to cache stats: %v", err)
		}
	}

	return stats, nil
}

// GetHourlyStats 获取最近一小时内的USDT交易统计
func (s *TransferService) GetHourlyStats() (*HourlyStatsResponse, error) {
	// 计算一小时前的时间
	endTime := time.Now()
	startTime := endTime.Add(-1 * time.Hour)

	var transferCount int64
	var uniqueAddresses int64

	// 获取最近一小时内的转账数量
	if err := database.DB.Model(&database.Transfer{}).
		Where("created_at >= ? AND created_at <= ?", startTime, endTime).
		Count(&transferCount).Error; err != nil {
		return nil, utils.NewDatabaseError("Failed to count hourly transfers", err)
	}

	// 获取最近一小时内的唯一地址数
	var result struct {
		Count int64
	}
	if err := database.DB.Raw(`
		SELECT COUNT(DISTINCT address) as count FROM (
			SELECT from_address as address FROM transfers WHERE created_at >= ? AND created_at <= ?
			UNION
			SELECT to_address as address FROM transfers WHERE created_at >= ? AND created_at <= ?
		) as unique_addresses
	`, startTime, endTime, startTime, endTime).Scan(&result).Error; err != nil {
		return nil, utils.NewDatabaseError("Failed to count unique addresses in last hour", err)
	}
	uniqueAddresses = result.Count

	// 计算总金额（这里简化处理，实际应该根据USDT精度计算）
	var totalAmountResult struct {
		Sum string
	}
	if err := database.DB.Raw(`
		SELECT COALESCE(SUM(CAST(amount AS DECIMAL)), 0) as sum 
		FROM transfers 
		WHERE created_at >= ? AND created_at <= ?
	`, startTime, endTime).Scan(&totalAmountResult).Error; err != nil {
		return nil, utils.NewDatabaseError("Failed to calculate total amount in last hour", err)
	}

	return &HourlyStatsResponse{
		TransferCount:   transferCount,
		TotalAmount:     totalAmountResult.Sum,
		UniqueAddresses: uniqueAddresses,
		TimeRange:       "Last 1 hour",
		StartTime:       startTime,
		EndTime:         endTime,
	}, nil
}
