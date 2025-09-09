package database

import (
	"fmt"
	"log"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"Off-chainDatainDexer/config"
	"Off-chainDatainDexer/utils"
)

var DB *gorm.DB

// Transfer represents a USDT transfer event on Tron network.
type Transfer struct {
	ID              uint      `gorm:"primaryKey"`
	FromAddress     string    `gorm:"index:idx_from_address;not null"`
	ToAddress       string    `gorm:"index:idx_to_address;not null"`
	Amount          string    `gorm:"type:varchar(50);not null"`
	TransactionHash string    `gorm:"uniqueIndex:idx_tx_hash;type:varchar(66);not null"`
	BlockNumber     uint64    `gorm:"index:idx_block_number;not null"`
	Timestamp       time.Time `gorm:"index:idx_timestamp;not null"`
	CreatedAt       time.Time `gorm:"index:idx_created_at"`
	UpdatedAt       time.Time
}

// InitDB initializes the database connection and performs migrations.
func InitDB(cfg *config.Config) {
	var err error
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		cfg.DBHost, cfg.DBUser, cfg.DBPassword, cfg.DBName, cfg.DBPort)

	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent), // 禁用GORM日志输出
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// 配置连接池
	sqlDB, err := DB.DB()
	if err != nil {
		log.Fatalf("Failed to get underlying sql.DB: %v", err)
	}

	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetConnMaxLifetime(time.Hour)

	// 自动迁移数据库模式
	err = DB.AutoMigrate(&Transfer{})
	if err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}

	// 创建额外的索引以提升查询性能
	createOptimizedIndexes()

	log.Println("Database connection successful and schema migrated.")
}

// GetDB returns the database instance
func GetDB() *gorm.DB {
	return DB
}

// createOptimizedIndexes 创建优化的数据库索引
func createOptimizedIndexes() {
	// 地址+时间复合索引
	DB.Exec(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_address_time 
			 ON transfers (from_address, created_at DESC)`)
	DB.Exec(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_to_address_time 
			 ON transfers (to_address, created_at DESC)`)

	// 区块+时间复合索引
	DB.Exec(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_block_time 
			 ON transfers (block_number, created_at DESC)`)

	// 时间+金额复合索引（用于统计查询）
	DB.Exec(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_time_amount 
			 ON transfers (timestamp DESC, amount)`)

	// 部分索引：只索引最近的数据（最近30天）
	DB.Exec(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_recent 
			 ON transfers (created_at DESC) 
			 WHERE created_at > NOW() - INTERVAL '30 days'`)

	log.Println("Optimized database indexes created successfully")
}

// 幂等性处理方法

// InsertTransferIdempotent 幂等性插入转账记录
func InsertTransferIdempotent(transfer *Transfer) error {
	// 使用事务确保原子性
	tx := DB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	if tx.Error != nil {
		return fmt.Errorf("failed to begin transaction: %w", tx.Error)
	}

	// 检查记录是否已存在
	var existingTransfer Transfer
	result := tx.Where("transaction_hash = ?", transfer.TransactionHash).First(&existingTransfer)
	
	if result.Error == nil {
		// 记录已存在，检查是否需要更新
		if shouldUpdateTransfer(&existingTransfer, transfer) {
			// 更新现有记录
			updateFields := map[string]interface{}{
				"updated_at": time.Now(),
			}
			
			// 只更新可能变化的字段
			if existingTransfer.Amount != transfer.Amount {
				updateFields["amount"] = transfer.Amount
			}
			if existingTransfer.Timestamp != transfer.Timestamp {
				updateFields["timestamp"] = transfer.Timestamp
			}
			
			if err := tx.Model(&existingTransfer).Updates(updateFields).Error; err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to update existing transfer: %w", err)
			}
			
			utils.LogToFile("Updated existing transfer: %s", transfer.TransactionHash)
		} else {
			utils.LogToFile("Transfer already exists and no update needed: %s", transfer.TransactionHash)
		}
		
		return tx.Commit().Error
	} else if result.Error != gorm.ErrRecordNotFound {
		// 数据库查询错误
		tx.Rollback()
		return fmt.Errorf("failed to check existing transfer: %w", result.Error)
	}

	// 记录不存在，插入新记录
	transfer.CreatedAt = time.Now()
	transfer.UpdatedAt = time.Now()
	
	if err := tx.Create(transfer).Error; err != nil {
		tx.Rollback()
		// 检查是否是唯一约束冲突
		if isUniqueConstraintError(err) {
			// 可能是并发插入导致的冲突，尝试重新查询
			var conflictTransfer Transfer
			if findErr := DB.Where("transaction_hash = ?", transfer.TransactionHash).First(&conflictTransfer).Error; findErr == nil {
				utils.LogToFile("Concurrent insert detected for transfer: %s", transfer.TransactionHash)
				return nil // 记录已存在，视为成功
			}
		}
		return fmt.Errorf("failed to insert transfer: %w", err)
	}

	utils.LogToFile("Inserted new transfer: %s", transfer.TransactionHash)
	return tx.Commit().Error
}

// BatchInsertTransfersIdempotent 批量幂等性插入转账记录
func BatchInsertTransfersIdempotent(transfers []*Transfer) error {
	if len(transfers) == 0 {
		return nil
	}

	// 使用事务确保原子性
	tx := DB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	if tx.Error != nil {
		return fmt.Errorf("failed to begin transaction: %w", tx.Error)
	}

	// 收集所有交易哈希
	txHashes := make([]string, len(transfers))
	for i, transfer := range transfers {
		txHashes[i] = transfer.TransactionHash
	}

	// 批量查询已存在的记录
	var existingTransfers []Transfer
	if err := tx.Where("transaction_hash IN ?", txHashes).Find(&existingTransfers).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to query existing transfers: %w", err)
	}

	// 创建已存在记录的映射
	existingMap := make(map[string]*Transfer)
	for i := range existingTransfers {
		existingMap[existingTransfers[i].TransactionHash] = &existingTransfers[i]
	}

	// 分离新记录和需要更新的记录
	var newTransfers []*Transfer
	var updateTransfers []*Transfer
	
	for _, transfer := range transfers {
		if existing, exists := existingMap[transfer.TransactionHash]; exists {
			if shouldUpdateTransfer(existing, transfer) {
				// 保留原有ID和创建时间
				transfer.ID = existing.ID
				transfer.CreatedAt = existing.CreatedAt
				transfer.UpdatedAt = time.Now()
				updateTransfers = append(updateTransfers, transfer)
			}
		} else {
			transfer.CreatedAt = time.Now()
			transfer.UpdatedAt = time.Now()
			newTransfers = append(newTransfers, transfer)
		}
	}

	// 批量插入新记录
	if len(newTransfers) > 0 {
		if err := tx.CreateInBatches(newTransfers, 100).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to batch insert transfers: %w", err)
		}
		utils.LogToFile("Batch inserted %d new transfers", len(newTransfers))
	}

	// 批量更新记录
	for _, transfer := range updateTransfers {
		if err := tx.Save(transfer).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to update transfer %s: %w", transfer.TransactionHash, err)
		}
	}
	
	if len(updateTransfers) > 0 {
		utils.LogToFile("Updated %d existing transfers", len(updateTransfers))
	}

	return tx.Commit().Error
}

// shouldUpdateTransfer 判断是否需要更新转账记录
func shouldUpdateTransfer(existing, new *Transfer) bool {
	// 检查关键字段是否有变化
	return existing.Amount != new.Amount ||
		existing.Timestamp != new.Timestamp ||
		existing.BlockNumber != new.BlockNumber
}

// isUniqueConstraintError 检查是否是唯一约束错误
func isUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	errorStr := err.Error()
	// PostgreSQL唯一约束错误关键词
	return fmt.Sprintf("%v", errorStr) == "duplicate key value violates unique constraint" ||
		fmt.Sprintf("%v", errorStr) == "UNIQUE constraint failed"
}

// GetTransferByHash 根据交易哈希获取转账记录
func GetTransferByHash(txHash string) (*Transfer, error) {
	var transfer Transfer
	err := DB.Where("transaction_hash = ?", txHash).First(&transfer).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get transfer by hash: %w", err)
	}
	return &transfer, nil
}

// TransferExists 检查转账记录是否存在
func TransferExists(txHash string) (bool, error) {
	var count int64
	err := DB.Model(&Transfer{}).Where("transaction_hash = ?", txHash).Count(&count).Error
	if err != nil {
		return false, fmt.Errorf("failed to check transfer existence: %w", err)
	}
	return count > 0, nil
}

// CleanupOldTransfers 清理旧的转账记录（可选的维护操作）
func CleanupOldTransfers(olderThan time.Duration) error {
	cutoffTime := time.Now().Add(-olderThan)
	result := DB.Where("created_at < ?", cutoffTime).Delete(&Transfer{})
	if result.Error != nil {
		return fmt.Errorf("failed to cleanup old transfers: %w", result.Error)
	}
	utils.LogToFile("Cleaned up %d old transfer records", result.RowsAffected)
	return nil
}
