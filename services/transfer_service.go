package services

import (
	"errors"
	"fmt"
	"math/big"
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

// HourlyTrendPoint 小时趋势数据点
type HourlyTrendPoint struct {
	Time        time.Time `json:"time"`
	Hour        string    `json:"hour"`
	Count       int64     `json:"count"`
	TotalAmount string    `json:"total_amount"`
}

// HourlyTrendResponse 小时趋势响应
type HourlyTrendResponse struct {
	Data      []HourlyTrendPoint `json:"data"`
	TimeRange string             `json:"time_range"`
	StartTime time.Time          `json:"start_time"`
	EndTime   time.Time          `json:"end_time"`
}

// TransferService 转账服务
type TransferService struct {
	db            *gorm.DB
	cacheService  *CacheService
	marketService *MarketService
	bytePool      *utils.BytePool
	perfMonitor   *utils.PerformanceMonitor
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

// SetMarketService 设置市场服务
func (s *TransferService) SetMarketService(market *MarketService) {
	s.marketService = market
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
		// 格式化金额
		amount, ok := new(big.Int).SetString(transfer.Amount, 10)
		if !ok {
			amount = big.NewInt(0)
		}
		formattedAmount := utils.FormatUSDTAmount(amount)

		responseTransfers[i] = TransferResponse{
			ID:              transfer.ID,
			FromAddress:     transfer.FromAddress,
			ToAddress:       transfer.ToAddress,
			Amount:          formattedAmount,
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
		// 格式化金额
		amount, ok := new(big.Int).SetString(transfer.Amount, 10)
		if !ok {
			amount = big.NewInt(0)
		}
		formattedAmount := utils.FormatUSDTAmount(amount)

		responseTransfers[i] = TransferResponse{
			ID:              transfer.ID,
			FromAddress:     transfer.FromAddress,
			ToAddress:       transfer.ToAddress,
			Amount:          formattedAmount,
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

	// 格式化金额
	amount, ok := new(big.Int).SetString(transfer.Amount, 10)
	if !ok {
		amount = big.NewInt(0)
	}
	formattedAmount := utils.FormatUSDTAmount(amount)

	return &TransferResponse{
		ID:              transfer.ID,
		FromAddress:     transfer.FromAddress,
		ToAddress:       transfer.ToAddress,
		Amount:          formattedAmount,
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

	// 格式化金额
	amount, ok := new(big.Int).SetString(transfer.Amount, 10)
	if !ok {
		amount = big.NewInt(0)
	}
	formattedAmount := utils.FormatUSDTAmount(amount)

	return &TransferResponse{
		ID:              transfer.ID,
		FromAddress:     transfer.FromAddress,
		ToAddress:       transfer.ToAddress,
		Amount:          formattedAmount,
		TransactionHash: transfer.TransactionHash,
		BlockNumber:     transfer.BlockNumber,
		Timestamp:       transfer.Timestamp,
		CreatedAt:       transfer.CreatedAt,
	}, nil
}

// GetStats 获取统计信息（临时禁用缓存进行调试）
func (s *TransferService) GetStats() (*StatsResponse, error) {
	// 临时直接查询数据库，绕过缓存
	return s.fetchStatsFromDB()
}

// fetchStatsFromDB 从数据库获取统计信息
func (s *TransferService) fetchStatsFromDB() (*StatsResponse, error) {
	var totalTransfers int64
	var uniqueAddresses int64
	var latestBlockNumber uint64

	// 获取真实的TRON链上USDT交易总数
	if s.marketService != nil {
		marketData, err := s.marketService.GetUSDTMarketData()
		if err == nil && marketData.Transfers > 0 {
			totalTransfers = marketData.Transfers
		} else {
			utils.Warn("Failed to get real TRON chain data, falling back to database count: %v", err)
			// 回退到数据库统计
			if err := database.DB.Model(&database.Transfer{}).Count(&totalTransfers).Error; err != nil {
				return nil, utils.NewDatabaseError("Failed to count total transfers", err)
			}
		}
	} else {
		// 如果没有市场服务，使用数据库统计
		if err := database.DB.Model(&database.Transfer{}).Count(&totalTransfers).Error; err != nil {
			return nil, utils.NewDatabaseError("Failed to count total transfers", err)
		}
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

	// 计算总交易量（USDT金额）
	var totalAmountResult struct {
		Sum float64
	}
	if err := database.DB.Raw(`
		SELECT COALESCE(SUM(amount::NUMERIC), 0) as sum 
		FROM transfers
	`).Scan(&totalAmountResult).Error; err != nil {
		return nil, utils.NewDatabaseError("Failed to calculate total amount", err)
	}

	// 将最小单位转换为USDT（除以10^6）
	totalAmountUSDT := totalAmountResult.Sum / 1000000

	stats := &StatsResponse{
		TotalTransfers:    totalTransfers,
		TotalAmount:       fmt.Sprintf("%.6f", totalAmountUSDT),
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

// GetYesterdayTransferCount 获取昨天的转账数量
func (s *TransferService) GetYesterdayTransferCount() (int64, error) {
	// 计算昨天的时间范围
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	startOfYesterday := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, yesterday.Location())
	endOfYesterday := startOfYesterday.Add(24 * time.Hour)

	var transferCount int64

	// 获取昨天的转账数量（使用timestamp字段，即实际转账时间）
	if err := database.DB.Model(&database.Transfer{}).
		Where("timestamp >= ? AND timestamp < ?", startOfYesterday, endOfYesterday).
		Count(&transferCount).Error; err != nil {
		return 0, utils.NewDatabaseError("Failed to count yesterday transfers", err)
	}

	return transferCount, nil
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

// GetHourlyTrend 获取指定时段按5分钟间隔的转账趋势数据
func (s *TransferService) GetHourlyTrend(date *time.Time, timeSlot string) (*HourlyTrendResponse, error) {
	// 使用北京时间时区
	loc, _ := time.LoadLocation("Asia/Shanghai")
	
	// 如果没有指定日期，使用当前时间
	var baseTime time.Time
	if date != nil {
		baseTime = date.In(loc)
	} else {
		baseTime = time.Now().In(loc)
	}

	// 根据时段确定时间范围
	var startTime, endTime time.Time
	var timeRange string

	switch timeSlot {
	case "morning": // 上午 6:00-12:00
		startTime = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day(), 6, 0, 0, 0, loc)
		endTime = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day(), 12, 0, 0, 0, loc)
		timeRange = "上午 (6:00-12:00)"
	case "afternoon": // 下午 12:00-18:00
		startTime = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day(), 12, 0, 0, 0, loc)
		endTime = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day(), 18, 0, 0, 0, loc)
		timeRange = "下午 (12:00-18:00)"
	case "evening": // 晚上 18:00-24:00
		startTime = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day(), 18, 0, 0, 0, loc)
		endTime = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day()+1, 0, 0, 0, 0, loc)
		timeRange = "晚上 (18:00-24:00)"
	case "night": // 深夜 0:00-6:00
		startTime = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day(), 0, 0, 0, 0, loc)
		endTime = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day(), 6, 0, 0, 0, loc)
		timeRange = "深夜 (0:00-6:00)"
	default: // 全天 0:00-24:00
		startTime = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day(), 0, 0, 0, 0, loc)
		endTime = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day()+1, 0, 0, 0, 0, loc)
		timeRange = "全天 (0:00-24:00)"
	}

	// 根据时段选择不同的时间间隔
	var intervalMinutes int
	if timeSlot == "all" {
		intervalMinutes = 60 // 全天时段使用1小时间隔
	} else {
		intervalMinutes = 15 // 特定时段使用15分钟间隔
	}

	// 计算时间间隔数量
	duration := endTime.Sub(startTime)
	intervalCount := int(duration.Minutes()) / intervalMinutes

	// 创建指定数量的时间间隔数据点
	var trendData []HourlyTrendPoint
	for i := 0; i < intervalCount; i++ {
		intervalStart := startTime.Add(time.Duration(i) * time.Duration(intervalMinutes) * time.Minute)
		intervalEnd := intervalStart.Add(time.Duration(intervalMinutes) * time.Minute)

		// 将时间转换为UTC进行数据库查询
		intervalStartUTC := intervalStart.UTC()
		intervalEndUTC := intervalEnd.UTC()

		// 查询该时间间隔内的转账数量
		var transferCount int64
		if err := database.DB.Model(&database.Transfer{}).
			Where("timestamp >= ? AND timestamp < ?", intervalStartUTC, intervalEndUTC).
			Count(&transferCount).Error; err != nil {
			return nil, utils.NewDatabaseError("Failed to count transfers in interval", err)
		}

		// 查询该时间间隔内的总交易金额（amount字段是最小单位，需要转换为USDT）
		var totalAmountResult struct {
			Sum string
		}
		if err := database.DB.Raw(`
			SELECT COALESCE(SUM(CAST(amount AS DECIMAL)), 0) as sum 
			FROM transfers 
			WHERE timestamp >= ? AND timestamp < ?
		`, intervalStartUTC, intervalEndUTC).Scan(&totalAmountResult).Error; err != nil {
			return nil, utils.NewDatabaseError("Failed to calculate total amount in interval", err)
		}

		// 将最小单位转换为USDT（除以10^6）
		formattedAmount := "0"
		if totalAmountResult.Sum != "" && totalAmountResult.Sum != "0" {
			// 将字符串转换为big.Int进行精确计算
			totalAmountBig := new(big.Int)
			if _, ok := totalAmountBig.SetString(totalAmountResult.Sum, 10); ok {
				formattedAmount = utils.FormatUSDTAmount(totalAmountBig)
			}
		}

		trendData = append(trendData, HourlyTrendPoint{
			Time:        intervalStart,
			Hour:        intervalStart.Format("15:04"),
			Count:       transferCount,
			TotalAmount: formattedAmount,
		})
	}

	return &HourlyTrendResponse{
		Data:      trendData,
		TimeRange: timeRange,
		StartTime: startTime,
		EndTime:   endTime,
	}, nil
}
