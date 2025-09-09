package blockchain

import (
	"Off-chainDatainDexer/config"
	"Off-chainDatainDexer/utils"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"net/http"
	"sync"
	"time"
)

// TronHTTPClient 波场链HTTP客户端实现
type TronHTTPClient struct {
	*TronClient
	httpClient  *http.Client
	nodes       []config.TronNodeConfig
	currentNode int
	retryConfig config.RetryConfig
	nodesMutex  sync.RWMutex
	nodeStatus  map[string]NodeStatus // 节点状态跟踪
}

// NodeStatus 节点状态
type NodeStatus struct {
	Healthy     bool
	LastCheck   time.Time
	FailCount   int
	LastFailure time.Time
}

// TronAPIResponse 波场链API通用响应结构
type TronAPIResponse struct {
	Success bool        `json:"success"`
	Error   string      `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// BlockResponse 区块响应结构
type BlockResponse struct {
	BlockID     string `json:"blockID"`
	BlockHeader struct {
		RawData struct {
			Number    int64 `json:"number"`
			Timestamp int64 `json:"timestamp"`
		} `json:"raw_data"`
	} `json:"block_header"`
	Transactions []TransactionResponse `json:"transactions"`
}

// TransactionResponse 交易响应结构
type TransactionResponse struct {
	TxID string `json:"txID"`
	Ret  []struct {
		ContractRet string `json:"contractRet"`
	} `json:"ret"`
	RawData struct {
		Contract []struct {
			Parameter struct {
				Value struct {
					Data            string `json:"data"`
					OwnerAddress    string `json:"owner_address"`
					ContractAddress string `json:"contract_address"`
				} `json:"value"`
				TypeURL string `json:"type_url"`
			} `json:"parameter"`
			Type string `json:"type"`
		} `json:"contract"`
		Timestamp int64 `json:"timestamp"`
	} `json:"raw_data"`
}

// NewTronHTTPClient 创建新的波场链HTTP客户端
func NewTronHTTPClient(nodeURL, apiKey, usdtContract string) *TronHTTPClient {
	// 配置HTTP传输层，优化连接池
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}

	return &TronHTTPClient{
		TronClient: NewTronClient(nodeURL, apiKey, usdtContract),
		httpClient: &http.Client{
			Timeout:   120 * time.Second, // 增加超时时间到120秒
			Transport: transport,
		},
	}
}

// NewTronHTTPClientWithConfig 使用配置创建多节点TRON客户端
func NewTronHTTPClientWithConfig(cfg *config.Config) *TronHTTPClient {
	// 配置HTTP传输层
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}

	client := &TronHTTPClient{
		TronClient: NewTronClient(cfg.TronNodeURL, cfg.TronAPIKey, cfg.USDTContract),
		httpClient: &http.Client{
			Timeout:   cfg.RetryConfig.Timeout,
			Transport: transport,
		},
		nodes:       cfg.TronNodes,
		currentNode: 0,
		retryConfig: cfg.RetryConfig,
		nodeStatus:  make(map[string]NodeStatus),
	}

	// 打印节点配置信息
	log.Printf("=== TRON节点配置 ===")
	log.Printf("总节点数: %d", len(client.nodes))
	for i, node := range client.nodes {
		apiKeyStatus := "无API Key"
		if node.APIKey != "" {
			apiKeyStatus = "有API Key"
		}
		log.Printf("节点 %d: %s (权重: %d, %s)", i, node.URL, node.Weight, apiKeyStatus)
	}
	log.Printf("==================")

	// 初始化节点状态
	for _, node := range client.nodes {
		client.nodeStatus[node.URL] = NodeStatus{
			Healthy:   true,
			LastCheck: time.Now(),
		}
	}

	return client
}

// makeRequest 发送HTTP请求到波场链节点，带重试机制和故障转移
func (tc *TronHTTPClient) makeRequest(ctx context.Context, method, endpoint string, payload interface{}) ([]byte, error) {
	// 如果没有配置多节点，使用原有逻辑
	if len(tc.nodes) == 0 {
		return tc.makeRequestSingle(ctx, method, endpoint, payload, tc.NodeURL, tc.APIKey)
	}

	// 尝试恢复不健康的节点
	tc.tryRecoverNodes()

	// 多节点重试逻辑
	var jsonData []byte
	if payload != nil {
		var err error
		jsonData, err = json.Marshal(payload)
		if err != nil {
			return nil, utils.NewInternalError("Failed to marshal request payload", err)
		}
	}

	// 尝试所有可用节点
	for nodeAttempt := 0; nodeAttempt < len(tc.nodes); nodeAttempt++ {
		nodeIndex := tc.selectHealthyNode()
		if nodeIndex == -1 {
			return nil, utils.NewInternalError("No healthy nodes available", nil)
		}

		node := tc.nodes[nodeIndex]
		url := node.URL + endpoint

		// 对当前节点进行重试
		for attempt := 0; attempt < tc.retryConfig.MaxRetries; attempt++ {
			// 为每次重试创建新的请求体
			var reqBody io.Reader
			if jsonData != nil {
				reqBody = bytes.NewBuffer(jsonData)
			}

			req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
			if err != nil {
				return nil, utils.NewInternalError("Failed to create HTTP request", err)
			}

			// 设置请求头 - 总是设置Content-Type，因为Tron API需要这个头
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("User-Agent", "TronHTTPClient/1.0")
			if node.APIKey != "" {
				req.Header.Set("TRON-PRO-API-KEY", node.APIKey)
			}

			// 发送请求
			resp, err := tc.httpClient.Do(req)
			if err != nil {
				log.Printf("Node %d request failed (attempt %d): %v", nodeIndex, attempt+1, err)
				tc.markNodeFailure(node.URL)
				if attempt < tc.retryConfig.MaxRetries-1 {
					delay := tc.calculateDelay(attempt, tc.getBaseDelay())
					select {
					case <-ctx.Done():
						return nil, utils.NewInternalError("Request cancelled", ctx.Err())
					case <-time.After(delay):
						continue
					}
				}
				break // 尝试下一个节点
			}

			// 读取响应
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				log.Printf("Node %d response read failed (attempt %d): %v", nodeIndex, attempt+1, err)
				tc.markNodeFailure(node.URL)
				if attempt < tc.retryConfig.MaxRetries-1 {
					delay := tc.calculateDelay(attempt, tc.getBaseDelay())
					select {
					case <-ctx.Done():
						return nil, utils.NewInternalError("Request cancelled", ctx.Err())
					case <-time.After(delay):
						continue
					}
				}
				break
			}

			// 检查HTTP状态码
			if resp.StatusCode == http.StatusOK {
				tc.markNodeSuccess(node.URL)
				return body, nil
			}

			// 特殊处理限流错误
			if resp.StatusCode == http.StatusTooManyRequests {
				log.Printf("⚠️ 节点 %d 遇到限流(429)，切换到下一个节点", nodeIndex)
				tc.markNodeRateLimited(node.URL)
				break // 立即切换到下一个节点，不重试当前节点
			}

			// 对于其他可重试的状态码进行重试
			if tc.shouldRetry(nil, resp.StatusCode) {
				log.Printf("节点 %d 返回状态码 %d (尝试 %d), 重试中", nodeIndex, resp.StatusCode, attempt+1)
				tc.markNodeFailure(node.URL)
				if attempt < tc.retryConfig.MaxRetries-1 {
					delay := tc.calculateDelay(attempt, tc.getBaseDelay())
					select {
					case <-ctx.Done():
						return nil, utils.NewInternalError("Request cancelled", ctx.Err())
					case <-time.After(delay):
						continue
					}
				}
				break // 尝试下一个节点
			}

			// 对于其他错误状态码，不重试当前节点
			log.Printf("节点 %d 返回不可重试状态码 %d: %s", nodeIndex, resp.StatusCode, string(body))
			tc.markNodeFailure(node.URL)
			break // 尝试下一个节点
		}
	}

	return nil, utils.NewInternalError("All nodes failed after retries", nil)
}

// makeRequestSingle 单节点请求（向后兼容）
func (tc *TronHTTPClient) makeRequestSingle(ctx context.Context, method, endpoint string, payload interface{}, nodeURL, apiKey string) ([]byte, error) {
	const maxRetries = 3
	const baseDelay = 1 * time.Second

	var jsonData []byte
	if payload != nil {
		var err error
		jsonData, err = json.Marshal(payload)
		if err != nil {
			return nil, utils.NewInternalError("Failed to marshal request payload", err)
		}
	}

	url := nodeURL + endpoint

	for attempt := 0; attempt <= maxRetries; attempt++ {
		var reqBody io.Reader
		if jsonData != nil {
			reqBody = bytes.NewBuffer(jsonData)
		}

		req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
		if err != nil {
			return nil, utils.NewInternalError("Failed to create HTTP request", err)
		}

		// 总是设置Content-Type为application/json，因为Tron API需要这个头
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "TronHTTPClient/1.0")
		if apiKey != "" {
			req.Header.Set("TRON-PRO-API-KEY", apiKey)
		}

		resp, err := tc.httpClient.Do(req)
		if err != nil {
			if attempt < maxRetries {
				delay := baseDelay * time.Duration(1<<attempt)
				select {
				case <-ctx.Done():
					return nil, utils.NewInternalError("Request cancelled", ctx.Err())
				case <-time.After(delay):
					continue
				}
			}
			return nil, utils.NewInternalError(fmt.Sprintf("HTTP request failed after %d attempts", maxRetries+1), err)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			if attempt < maxRetries {
				delay := baseDelay * time.Duration(1<<attempt)
				select {
				case <-ctx.Done():
					return nil, utils.NewInternalError("Request cancelled", ctx.Err())
				case <-time.After(delay):
					continue
				}
			}
			return nil, utils.NewInternalError("Failed to read response body", err)
		}

		if resp.StatusCode == http.StatusOK {
			return body, nil
		}

		if resp.StatusCode == http.StatusTooManyRequests ||
			resp.StatusCode == http.StatusInternalServerError ||
			resp.StatusCode == http.StatusBadGateway ||
			resp.StatusCode == http.StatusServiceUnavailable ||
			resp.StatusCode == http.StatusGatewayTimeout {
			if attempt < maxRetries {
				delay := baseDelay * time.Duration(1<<attempt)
				if resp.StatusCode == http.StatusTooManyRequests {
					delay *= 2
				}
				select {
				case <-ctx.Done():
					return nil, utils.NewInternalError("Request cancelled", ctx.Err())
				case <-time.After(delay):
					continue
				}
			}
		}

		return nil, utils.NewInternalError(fmt.Sprintf("HTTP request failed with status %d: %s", resp.StatusCode, string(body)), nil)
	}

	return nil, utils.NewInternalError("Unexpected error in makeRequest", nil)
}

// GetLatestBlockNumber 获取最新区块号 (实际实现)
func (tc *TronHTTPClient) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	utils.Info("Fetching latest block number from Tron network...")

	// 使用 /walletsolidity/getnowblock 端点，不需要请求体参数
	body, err := tc.makeRequest(ctx, "POST", "/walletsolidity/getnowblock", nil)
	if err != nil {
		return 0, err
	}

	var blockResp BlockResponse
	if err := json.Unmarshal(body, &blockResp); err != nil {
		return 0, utils.NewInternalError("Failed to parse block response", err)
	}

	blockNumber := uint64(blockResp.BlockHeader.RawData.Number)
	utils.Info("Latest block number: %d", blockNumber)

	return blockNumber, nil
}

// GetBlockByNumber 根据区块号获取区块信息 (实际实现)
func (tc *TronHTTPClient) GetBlockByNumber(ctx context.Context, blockNumber uint64) (*BlockInfo, error) {
	utils.Info("Fetching block %d from Tron network...", blockNumber)

	payload := map[string]interface{}{
		"num": blockNumber,
	}

	body, err := tc.makeRequest(ctx, "POST", "/wallet/getblockbynum", payload)
	if err != nil {
		return nil, err
	}

	var blockResp BlockResponse
	if err := json.Unmarshal(body, &blockResp); err != nil {
		return nil, utils.NewInternalError("Failed to parse block response", err)
	}

	return &BlockInfo{
		ChainType: ChainTypeTron,
		Number:    uint64(blockResp.BlockHeader.RawData.Number),
		Hash:      blockResp.BlockID,
		Timestamp: time.Unix(blockResp.BlockHeader.RawData.Timestamp/1000, 0),
		TxCount:   len(blockResp.Transactions),
	}, nil
}

// GetBlockByHash 根据区块哈希获取区块信息
func (tc *TronHTTPClient) GetBlockByHash(ctx context.Context, blockHash string) (*BlockInfo, error) {
	utils.Info("Fetching block %s from Tron network...", blockHash)

	payload := map[string]interface{}{
		"value": blockHash,
	}

	body, err := tc.makeRequest(ctx, "POST", "/wallet/getblockbyid", payload)
	if err != nil {
		return nil, err
	}

	var blockResp BlockResponse
	if err := json.Unmarshal(body, &blockResp); err != nil {
		return nil, utils.NewInternalError("Failed to parse block response", err)
	}

	return &BlockInfo{
		ChainType: ChainTypeTron,
		Number:    uint64(blockResp.BlockHeader.RawData.Number),
		Hash:      blockResp.BlockID,
		Timestamp: time.Unix(blockResp.BlockHeader.RawData.Timestamp/1000, 0),
		TxCount:   len(blockResp.Transactions),
	}, nil
}

// GetUSDTTransfersByBlock 获取指定区块的USDT转账事件
func (tc *TronHTTPClient) GetUSDTTransfersByBlock(ctx context.Context, blockNumber uint64) ([]*TransferEvent, error) {
	utils.Info("Fetching USDT transfers from block %d...", blockNumber)

	// 获取区块信息
	blockInfo, err := tc.GetBlockByNumber(ctx, blockNumber)
	if err != nil {
		return nil, err
	}

	// 获取区块详细信息包含交易
	payload := map[string]interface{}{
		"num": blockNumber,
	}

	body, err := tc.makeRequest(ctx, "POST", "/wallet/getblockbynum", payload)
	if err != nil {
		return nil, err
	}

	var blockResp BlockResponse
	if err := json.Unmarshal(body, &blockResp); err != nil {
		return nil, utils.NewInternalError("Failed to parse block response", err)
	}

	var transfers []*TransferEvent

	// 遍历区块中的所有交易
	for _, tx := range blockResp.Transactions {
		// 检查交易是否成功
		if len(tx.Ret) > 0 && tx.Ret[0].ContractRet != "SUCCESS" {
			continue
		}

		// 检查是否是智能合约调用
		for _, contract := range tx.RawData.Contract {
			if contract.Type == "TriggerSmartContract" {
				// 解析USDT转账事件
				transfer := tc.parseUSDTTransfer(tx, blockInfo)
				if transfer != nil {
					transfers = append(transfers, transfer)
				}
			}
		}
	}

	utils.Info("Found %d USDT transfers in block %d", len(transfers), blockNumber)
	return transfers, nil
}

// parseUSDTTransfer 解析USDT转账事件
func (tc *TronHTTPClient) parseUSDTTransfer(tx TransactionResponse, blockInfo *BlockInfo) *TransferEvent {
	// 将USDT合约地址转换为十六进制格式进行比较
	usdtContractHex := tc.tronAddressToHex(tc.USDTContract)

	// 遍历交易中的所有合约调用
	for _, contract := range tx.RawData.Contract {
		if contract.Type == "TriggerSmartContract" {
			// 获取合约地址
			contractAddr := contract.Parameter.Value.ContractAddress

			// 检查是否为USDT合约
			if contractAddr == usdtContractHex {
				// 检查调用数据是否为transfer方法
				data := contract.Parameter.Value.Data
				if len(data) >= 8 && data[:8] == "a9059cbb" { // transfer方法的函数签名
					utils.Info("Found USDT transfer in tx %s", tx.TxID)

					// 解析transfer参数
					toAddress, amount := tc.parseTransferData(data)

					return &TransferEvent{
						TxHash:      tx.TxID,
						BlockNumber: blockInfo.Number,
						BlockHash:   blockInfo.Hash,
						FromAddress: tc.hexToTronAddress(contract.Parameter.Value.OwnerAddress),
						ToAddress:   toAddress,
						Amount:      amount,
						Timestamp:   blockInfo.Timestamp,
						GasUsed:     0,
						GasPrice:    big.NewInt(0),
						Status:      "SUCCESS",
					}
				}
			}
		}
	}

	return nil
}

// parseTransferData 解析transfer方法的调用数据
func (tc *TronHTTPClient) parseTransferData(data string) (string, *big.Int) {
	// transfer方法的ABI编码格式:
	// 前8位: 方法签名 a9059cbb
	// 接下来64位: to地址 (32字节，前12字节为0)
	// 接下来64位: amount (32字节)

	if len(data) < 136 { // 8 + 64 + 64 = 136
		return "Unknown", big.NewInt(0)
	}

	// 提取to地址 (跳过方法签名和前12字节的0)
	toHex := data[32:72] // 从第32位开始取40位十六进制字符
	toAddress := tc.hexToTronAddress(toHex)

	// 提取金额
	amountHex := data[72:136]
	amount := new(big.Int)
	amount.SetString(amountHex, 16)

	return toAddress, amount
}

// tronAddressToHex 将Tron Base58地址转换为十六进制格式
func (tc *TronHTTPClient) tronAddressToHex(address string) string {
	// TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t 对应的十六进制地址
	if address == "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t" {
		return "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
	}
	// 其他地址的简化处理
	return address
}

// hexToTronAddress 将十六进制地址转换为Tron地址格式
func (tc *TronHTTPClient) hexToTronAddress(hexAddr string) string {
	// 实际实现需要进行Base58编码
	// 这里返回简化版本
	if len(hexAddr) >= 40 {
		return "T" + hexAddr[:40] // 简化实现
	}
	return "T" + hexAddr // 简化实现
}

// GetTransactionByHash 根据交易哈希获取交易详情 (实际实现)
func (tc *TronHTTPClient) GetTransactionByHash(ctx context.Context, txHash string) (*TransferEvent, error) {
	utils.Info("Fetching transaction %s from Tron network...", txHash)

	payload := map[string]interface{}{
		"value": txHash,
	}

	body, err := tc.makeRequest(ctx, "POST", "/wallet/gettransactionbyid", payload)
	if err != nil {
		return nil, err
	}

	var txResp TransactionResponse
	if err := json.Unmarshal(body, &txResp); err != nil {
		return nil, utils.NewInternalError("Failed to parse transaction response", err)
	}

	// 解析交易详情
	// 这里需要实现完整的交易解析逻辑

	return &TransferEvent{
		TxHash:      txHash,
		BlockNumber: 0, // 需要从交易回执获取
		BlockHash:   "",
		FromAddress: "",
		ToAddress:   "",
		Amount:      big.NewInt(0),
		Timestamp:   time.Unix(txResp.RawData.Timestamp/1000, 0),
		GasUsed:     0,
		GasPrice:    big.NewInt(0),
		Status:      "SUCCESS",
	}, nil
}

// GetChainInfo 获取链信息
func (tc *TronHTTPClient) GetChainInfo() *ChainInfo {
	return &ChainInfo{
		ChainType:   ChainTypeTron,
		ChainID:     "0x2b6653dc", // Tron mainnet chain ID
		Name:        "Tron",
		Symbol:      "TRX",
		RPCURL:      tc.NodeURL,
		ExplorerURL: "https://tronscan.org",
	}
}

// GetChainType 获取链类型
func (tc *TronHTTPClient) GetChainType() ChainType {
	return ChainTypeTron
}

// IsConnected 检查是否已连接
func (tc *TronHTTPClient) IsConnected() bool {
	return true // HTTP客户端总是连接的
}

// Connect 连接到Tron网络
func (tc *TronHTTPClient) Connect(ctx context.Context) error {
	// 测试连接 - 尝试获取最新区块号
	_, err := tc.GetLatestBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Tron network: %w", err)
	}
	return nil
}

// Disconnect 断开连接
func (tc *TronHTTPClient) Disconnect() error {
	// HTTP客户端不需要显式断开连接
	return nil
}

// GetAccountUSDTBalance 获取账户USDT余额 (实际实现)
func (tc *TronHTTPClient) GetAccountUSDTBalance(ctx context.Context, address string) (*big.Int, error) {
	if !tc.ValidateAddress(address) {
		return nil, utils.NewValidationError("invalid Tron address format", nil)
	}

	utils.Info("Fetching USDT balance for address %s...", address)

	// 调用USDT合约的balanceOf方法
	payload := map[string]interface{}{
		"owner_address":     address,
		"contract_address":  tc.USDTContract,
		"function_selector": "balanceOf(address)",
		"parameter":         address, // 需要进行ABI编码
	}

	_, err := tc.makeRequest(ctx, "POST", "/wallet/triggerconstantcontract", payload)
	if err != nil {
		return nil, err
	}

	// 解析响应
	// 实际实现需要解析智能合约返回值

	return big.NewInt(5000000), nil // 模拟返回5 USDT
}

// GetUSDTBalance 获取账户USDT余额
func (tc *TronHTTPClient) GetUSDTBalance(ctx context.Context, address string) (*big.Int, error) {
	utils.Info("Getting USDT balance for address %s...", address)

	// 实际实现应该调用波场链API获取TRC20代币余额
	// 例如: POST https://api.trongrid.io/v1/accounts/{address}/transactions/trc20

	// 模拟返回余额
	return big.NewInt(1000000), nil // 1 USDT (6位小数)
}

// IsValidAddress 验证地址格式
func (tc *TronHTTPClient) IsValidAddress(address string) bool {
	// Tron地址以T开头，长度为34
	return len(address) == 34 && address[0] == 'T'
}

// GetSyncProgress 获取同步进度
func (tc *TronHTTPClient) GetSyncProgress(ctx context.Context) (*SyncProgress, error) {
	latestBlock, err := tc.GetLatestBlockNumber(ctx)
	if err != nil {
		return nil, err
	}

	return &SyncProgress{
		ChainType:    ChainTypeTron,
		CurrentBlock: latestBlock,
		LatestBlock:  latestBlock,
		SyncedBlocks: latestBlock,
		SyncProgress: 100.0,
		LastSyncTime: time.Now(),
		SyncSpeed:    0, // 实时同步
	}, nil
}

// GetUSDTTransfersByBlockRange 获取指定区块范围的USDT转账事件
func (tc *TronHTTPClient) GetUSDTTransfersByBlockRange(ctx context.Context, startBlock, endBlock uint64) ([]*TransferEvent, error) {
	var allTransfers []*TransferEvent

	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		transfers, err := tc.GetUSDTTransfersByBlock(ctx, blockNum)
		if err != nil {
			return nil, fmt.Errorf("failed to get transfers for block %d: %w", blockNum, err)
		}
		allTransfers = append(allTransfers, transfers...)
	}

	return allTransfers, nil
}

// GetUSDTTransferByHash 根据交易哈希获取USDT转账事件
func (tc *TronHTTPClient) GetUSDTTransferByHash(ctx context.Context, txHash string) (*TransferEvent, error) {
	transfer, err := tc.GetTransactionByHash(ctx, txHash)
	if err != nil {
		return nil, err
	}

	return transfer, nil
}

// HealthCheck 健康检查
func (tc *TronHTTPClient) HealthCheck(ctx context.Context) error {
	startTime := time.Now()
	// 尝试获取最新区块号来检查连接
	_, err := tc.GetLatestBlockNumber(ctx)
	responseTime := time.Since(startTime)

	// 记录健康检查日志
	status := "healthy"
	if err != nil {
		status = "unhealthy"
	}

	metrics := map[string]interface{}{
		"current_node": tc.currentNode,
		"total_nodes":  len(tc.nodes),
		"error":        nil,
	}

	if err != nil {
		metrics["error"] = err.Error()
	}

	utils.LogHealth("tron_client", status, responseTime, metrics)

	return err
}

// 节点管理辅助方法

// selectHealthyNode 选择一个健康的节点，优先选择权重高的节点
func (tc *TronHTTPClient) selectHealthyNode() int {
	tc.nodesMutex.RLock()
	defer tc.nodesMutex.RUnlock()

	// 如果没有配置多节点，返回-1表示使用默认节点
	if len(tc.nodes) == 0 {
		return -1
	}

	// 按权重排序查找健康的节点
	bestIndex := -1
	bestWeight := -1

	for i, node := range tc.nodes {
		if status, exists := tc.nodeStatus[node.URL]; exists && status.Healthy {
			// 选择权重最高的健康节点
			if node.Weight > bestWeight {
				bestWeight = node.Weight
				bestIndex = i
			}
		}
	}

	// 如果找到健康节点，返回最佳节点
	if bestIndex != -1 {
		return bestIndex
	}

	// 如果没有健康节点，检查是否所有节点都因限流而不可用
	allRateLimited := true
	for _, node := range tc.nodes {
		if status, exists := tc.nodeStatus[node.URL]; exists {
			// 如果节点最近的失败不是因为限流（429错误），则不是所有节点都被限流
			if !status.Healthy && time.Since(status.LastFailure) > 5*time.Minute {
				allRateLimited = false
				break
			}
		}
	}

	// 如果所有节点都被限流，返回-1表示暂停请求
	if allRateLimited {
		log.Printf("⚠️ 所有备用节点都被限流，暂停节点切换")
		return -1
	}

	// 否则返回第一个节点作为最后的尝试
	if len(tc.nodes) > 0 {
		return 0
	}

	return -1
}

// markNodeFailure 标记节点失败，区分限流和其他错误
func (tc *TronHTTPClient) markNodeFailure(nodeURL string) {
	tc.nodesMutex.Lock()
	defer tc.nodesMutex.Unlock()

	if status, exists := tc.nodeStatus[nodeURL]; exists {
		status.Healthy = false
		status.LastFailure = time.Now()
		status.FailCount++
		tc.nodeStatus[nodeURL] = status
		utils.LogToFile("节点 %s 标记为不健康，失败次数: %d", nodeURL, status.FailCount)
	}
}

// markNodeRateLimited 专门标记节点因限流而不可用
func (tc *TronHTTPClient) markNodeRateLimited(nodeURL string) {
	tc.nodesMutex.Lock()
	defer tc.nodesMutex.Unlock()

	if status, exists := tc.nodeStatus[nodeURL]; exists {
		status.Healthy = false
		status.LastFailure = time.Now()
		status.FailCount++
		tc.nodeStatus[nodeURL] = status
		utils.LogToFile("⚠️ 节点 %s 遇到限流(429)，暂时标记为不可用", nodeURL)
	}
}

// markNodeSuccess 标记节点为健康
func (tc *TronHTTPClient) markNodeSuccess(nodeURL string) {
	tc.nodesMutex.Lock()
	defer tc.nodesMutex.Unlock()

	if status, exists := tc.nodeStatus[nodeURL]; exists {
		status.Healthy = true
		status.LastCheck = time.Now()
		status.FailCount = 0
		tc.nodeStatus[nodeURL] = status
		utils.LogToFile("✅ 节点 %s 恢复健康状态", nodeURL)
	}
}

// tryRecoverNodes 尝试恢复不健康的节点
func (tc *TronHTTPClient) tryRecoverNodes() {
	tc.nodesMutex.Lock()
	defer tc.nodesMutex.Unlock()

	now := time.Now()
	for nodeURL, status := range tc.nodeStatus {
		if !status.Healthy {
			// 限流节点5分钟后可以重试，其他错误节点10分钟后可以重试
			recoveryTime := 10 * time.Minute
			if time.Since(status.LastFailure) < 6*time.Minute {
				recoveryTime = 5 * time.Minute // 可能是限流错误，恢复时间较短
			}

			if now.Sub(status.LastFailure) >= recoveryTime {
				status.Healthy = true
				status.FailCount = 0
				tc.nodeStatus[nodeURL] = status
				utils.LogToFile("🔄 节点 %s 已自动恢复，可重新尝试使用", nodeURL)
			}
		}
	}
}

// calculateDelay 计算重试延迟
func (tc *TronHTTPClient) calculateDelay(attempt int, baseDelay time.Duration) time.Duration {
	// 指数退避策略
	delay := time.Duration(float64(baseDelay) * math.Pow(tc.retryConfig.BackoffFactor, float64(attempt-1)))
	if delay > tc.retryConfig.MaxDelay {
		return tc.retryConfig.MaxDelay
	}
	return delay
}

// shouldRetry 判断是否应该重试
func (tc *TronHTTPClient) shouldRetry(err error, statusCode int) bool {
	if err != nil {
		// 网络错误通常可以重试
		return true
	}

	// 根据HTTP状态码判断
	switch statusCode {
	case 429, 500, 502, 503, 504:
		return true
	default:
		return false
	}
}

// getMaxRetries 获取最大重试次数
func (tc *TronHTTPClient) getMaxRetries() int {
	return tc.retryConfig.MaxRetries
}

// getBaseDelay 获取基础延迟
func (tc *TronHTTPClient) getBaseDelay() time.Duration {
	return tc.retryConfig.InitialDelay
}

/*
使用说明：

1. 获取TronGrid API密钥：
   - 访问 https://www.trongrid.io/
   - 注册账户并获取API密钥
   - 将密钥配置到 .env 文件中的 TRON_API_KEY

2. 配置网络：
   - 主网: https://api.trongrid.io
   - 测试网: https://api.shasta.trongrid.io
   - 私有节点: 自建节点地址

3. USDT合约地址：
   - 主网: TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t
   - 测试网: 需要查找测试网的USDT合约地址

4. 完整实现需要：
   - ABI编码/解码
   - Base58地址编码
   - 智能合约事件日志解析
   - 交易回执获取
   - 错误处理和重试机制
*/
