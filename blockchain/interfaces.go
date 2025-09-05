package blockchain

import (
	"context"
	"math/big"
	"time"
)

// ChainType 区块链类型枚举
type ChainType string

const (
	ChainTypeTron ChainType = "TRON"
)

// ChainInfo TRON区块链基本信息
type ChainInfo struct {
	ChainType    ChainType `json:"chain_type"`
	ChainID      string    `json:"chain_id"`
	Name         string    `json:"name"`
	Symbol       string    `json:"symbol"`
	RPCURL       string    `json:"rpc_url"`
	ExplorerURL  string    `json:"explorer_url"`
	USDTContract string    `json:"usdt_contract"`
}

// BlockInfo 区块信息
type BlockInfo struct {
	ChainType  ChainType `json:"chain_type"`
	Number     uint64    `json:"number"`
	Hash       string    `json:"hash"`
	ParentHash string    `json:"parent_hash"`
	Timestamp  time.Time `json:"timestamp"`
	TxCount    int       `json:"tx_count"`
	GasUsed    uint64    `json:"gas_used"`
	GasLimit   uint64    `json:"gas_limit"`
}

// SyncProgress 同步进度信息
type SyncProgress struct {
	ChainType    ChainType `json:"chain_type"`
	CurrentBlock uint64    `json:"current_block"`
	LatestBlock  uint64    `json:"latest_block"`
	SyncedBlocks uint64    `json:"synced_blocks"`
	SyncProgress float64   `json:"sync_progress"`
	LastSyncTime time.Time `json:"last_sync_time"`
	SyncSpeed    float64   `json:"sync_speed"` // blocks per second
}

// BlockchainClient 通用区块链客户端接口
type BlockchainClient interface {
	// 基础信息
	GetChainInfo() *ChainInfo
	GetChainType() ChainType
	IsConnected() bool

	// 区块相关
	GetLatestBlockNumber(ctx context.Context) (uint64, error)
	GetBlockByNumber(ctx context.Context, blockNumber uint64) (*BlockInfo, error)
	GetBlockByHash(ctx context.Context, blockHash string) (*BlockInfo, error)

	// 转账事件相关
	GetUSDTTransfersByBlock(ctx context.Context, blockNumber uint64) ([]*TransferEvent, error)
	GetUSDTTransfersByBlockRange(ctx context.Context, startBlock, endBlock uint64) ([]*TransferEvent, error)
	GetUSDTTransferByHash(ctx context.Context, txHash string) (*TransferEvent, error)

	// 地址相关
	GetUSDTBalance(ctx context.Context, address string) (*big.Int, error)
	IsValidAddress(address string) bool

	// 同步相关
	GetSyncProgress(ctx context.Context) (*SyncProgress, error)

	// 连接管理
	Connect(ctx context.Context) error
	Disconnect() error
	HealthCheck(ctx context.Context) error
}

// TRON链信息
var (
	TronMainnet = &ChainInfo{
		ChainType:    ChainTypeTron,
		ChainID:      "0x2b6653dc",
		Name:         "TRON Mainnet",
		Symbol:       "TRX",
		RPCURL:       "https://api.trongrid.io",
		ExplorerURL:  "https://tronscan.org",
		USDTContract: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
	}
)

// GetTronChainInfo 获取TRON链信息
func GetTronChainInfo() *ChainInfo {
	return TronMainnet
}

// IsValidChainType 验证链类型是否有效
func IsValidChainType(chainType ChainType) bool {
	return chainType == ChainTypeTron
}

// GetChainTypeFromString 从字符串获取链类型
func GetChainTypeFromString(s string) (ChainType, bool) {
	chainType := ChainType(s)
	return chainType, IsValidChainType(chainType)
}
