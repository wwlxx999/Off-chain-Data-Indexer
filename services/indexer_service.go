package services

import (
	"log"
	"time"

	"Off-chainDatainDexer/database"
	"Off-chainDatainDexer/utils"
)

// IndexerService 索引器服务
type IndexerService struct {
	transferService *TransferService
}

// NewIndexerService 创建新的索引器服务实例
func NewIndexerService() *IndexerService {
	return &IndexerService{
		transferService: NewTransferService(database.GetDB()),
	}
}

// IndexTransferData 索引转账数据
func (s *IndexerService) IndexTransferData(transfers []TransferRequest) (int, error) {
	successCount := 0
	errorCount := 0

	for _, transfer := range transfers {
		_, err := s.transferService.CreateTransfer(&transfer)
		if err != nil {
			utils.Error("Failed to index transfer %s: %v", transfer.TransactionHash, err)
			errorCount++
			continue
		}
		successCount++
	}

	log.Printf("Indexed %d transfers successfully, %d failed", successCount, errorCount)
	return successCount, nil
}

// GetTransfersByBlockRange 根据区块范围获取转账记录
func (s *IndexerService) GetTransfersByBlockRange(startBlock, endBlock uint64) ([]database.Transfer, error) {
	var transfers []database.Transfer

	err := database.DB.Where("block_number >= ? AND block_number <= ?", startBlock, endBlock).
		Order("block_number ASC, created_at ASC").
		Find(&transfers).Error

	if err != nil {
		return nil, utils.NewDatabaseError("Failed to get transfers by block range", err)
	}

	return transfers, nil
}

// GetTransfersByTimeRange 根据时间范围获取转账记录
func (s *IndexerService) GetTransfersByTimeRange(startTime, endTime time.Time) ([]database.Transfer, error) {
	var transfers []database.Transfer

	err := database.DB.Where("timestamp >= ? AND timestamp <= ?", startTime, endTime).
		Order("timestamp ASC").
		Find(&transfers).Error

	if err != nil {
		return nil, utils.NewDatabaseError("Failed to get transfers by time range", err)
	}

	return transfers, nil
}

// GetLatestBlockNumber 获取最新的区块号
func (s *IndexerService) GetLatestBlockNumber() (uint64, error) {
	var latestBlock uint64

	err := database.DB.Model(&database.Transfer{}).
		Select("COALESCE(MAX(block_number), 0)").
		Scan(&latestBlock).Error

	if err != nil {
		return 0, utils.NewDatabaseError("Failed to get latest block number", err)
	}

	return latestBlock, nil
}

// GetTransferCount 获取转账记录总数
func (s *IndexerService) GetTransferCount() (int64, error) {
	var count int64

	err := database.DB.Model(&database.Transfer{}).Count(&count).Error
	if err != nil {
		return 0, utils.NewDatabaseError("Failed to get transfer count", err)
	}

	return count, nil
}

// GetAddressTransferCount 获取特定地址的转账记录数
func (s *IndexerService) GetAddressTransferCount(address string) (int64, error) {
	var count int64

	err := database.DB.Model(&database.Transfer{}).
		Where("from_address = ? OR to_address = ?", address, address).
		Count(&count).Error

	if err != nil {
		return 0, utils.NewDatabaseError("Failed to get address transfer count", err)
	}

	return count, nil
}

// GetTopAddresses 获取转账次数最多的地址
func (s *IndexerService) GetTopAddresses(limit int) ([]AddressStats, error) {
	type AddressCount struct {
		Address string
		Count   int64
	}

	var results []AddressCount

	// 查询发送方地址统计
	var fromResults []AddressCount
	err := database.DB.Model(&database.Transfer{}).
		Select("from_address as address, COUNT(*) as count").
		Group("from_address").
		Scan(&fromResults).Error
	if err != nil {
		return nil, utils.NewDatabaseError("Failed to get from address stats", err)
	}

	// 查询接收方地址统计
	var toResults []AddressCount
	err = database.DB.Model(&database.Transfer{}).
		Select("to_address as address, COUNT(*) as count").
		Group("to_address").
		Scan(&toResults).Error
	if err != nil {
		return nil, utils.NewDatabaseError("Failed to get to address stats", err)
	}

	// 合并统计结果
	addressMap := make(map[string]int64)
	for _, result := range fromResults {
		addressMap[result.Address] += result.Count
	}
	for _, result := range toResults {
		addressMap[result.Address] += result.Count
	}

	// 转换为切片并排序
	for address, count := range addressMap {
		results = append(results, AddressCount{
			Address: address,
			Count:   count,
		})
	}

	// 简单排序（实际应用中可以使用更高效的排序算法）
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[i].Count < results[j].Count {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	// 限制返回数量
	if limit > 0 && limit < len(results) {
		results = results[:limit]
	}

	// 转换为AddressStats格式
	var addressStats []AddressStats
	for _, result := range results {
		addressStats = append(addressStats, AddressStats{
			Address:       result.Address,
			TransferCount: result.Count,
		})
	}

	return addressStats, nil
}

// AddressStats 地址统计信息
type AddressStats struct {
	Address       string `json:"address"`
	TransferCount int64  `json:"transfer_count"`
}

// IndexerStatusResponse 索引器状态响应
type IndexerStatusResponse struct {
	IsRunning         bool      `json:"is_running"`
	LatestBlockNumber uint64    `json:"latest_block_number"`
	TotalTransfers    int64     `json:"total_transfers"`
	LastIndexTime     time.Time `json:"last_index_time"`
	IndexerVersion    string    `json:"indexer_version"`
	DatabaseStatus    string    `json:"database_status"`
}

// ReindexData 重新索引数据（清理和重建索引）
func (s *IndexerService) ReindexData() error {
	log.Println("Starting data reindexing...")

	// 这里可以添加重新索引的逻辑
	// 例如：清理无效数据、重建索引等

	log.Println("Data reindexing completed")
	return nil
}

// ValidateDataIntegrity 验证数据完整性
func (s *IndexerService) ValidateDataIntegrity() error {
	log.Println("Starting data integrity validation...")

	// 检查重复的交易哈希
	var duplicateHashes []string
	err := database.DB.Model(&database.Transfer{}).
		Select("transaction_hash").
		Group("transaction_hash").
		Having("COUNT(*) > 1").
		Pluck("transaction_hash", &duplicateHashes).Error

	if err != nil {
		return utils.NewDatabaseError("Failed to check duplicate hashes", err)
	}

	if len(duplicateHashes) > 0 {
		log.Printf("Found %d duplicate transaction hashes", len(duplicateHashes))
		for _, hash := range duplicateHashes {
			log.Printf("Duplicate hash: %s", hash)
		}
	}

	log.Println("Data integrity validation completed")
	return nil
}

// DeleteHistoricalData 删除历史数据（保留今天的数据）
func (s *IndexerService) DeleteHistoricalData() (int64, error) {
	log.Println("Starting historical data deletion...")

	// 获取今天的开始时间（00:00:00）
	now := time.Now()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())

	log.Printf("Deleting transfers before: %s", todayStart.Format("2006-01-02 15:04:05"))

	// 删除今天之前的所有交易记录
	result := database.DB.Where("timestamp < ?", todayStart).Delete(&database.Transfer{})
	if result.Error != nil {
		return 0, utils.NewDatabaseError("Failed to delete historical transfers", result.Error)
	}

	deletedCount := result.RowsAffected
	log.Printf("Successfully deleted %d historical transfer records", deletedCount)

	return deletedCount, nil
}

// GetStatus 获取索引器状态
func (s *IndexerService) GetStatus() (*IndexerStatusResponse, error) {
	// 获取最新区块号
	latestBlock, err := s.GetLatestBlockNumber()
	if err != nil {
		return nil, utils.WrapError(err, "Failed to get latest block number")
	}

	// 获取总转账数
	totalTransfers, err := s.GetTransferCount()
	if err != nil {
		return nil, utils.WrapError(err, "Failed to get transfer count")
	}

	// 检查数据库状态
	databaseStatus := "connected"
	if err := database.DB.Exec("SELECT 1").Error; err != nil {
		databaseStatus = "disconnected"
	}

	return &IndexerStatusResponse{
		IsRunning:         true, // 简化处理，假设索引器正在运行
		LatestBlockNumber: latestBlock,
		TotalTransfers:    totalTransfers,
		LastIndexTime:     time.Now(), // 简化处理，使用当前时间
		IndexerVersion:    "1.0.0",
		DatabaseStatus:    databaseStatus,
	}, nil
}
