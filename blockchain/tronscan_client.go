package blockchain

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"Off-chainDatainDexer/config"
	"Off-chainDatainDexer/utils"
)

// TronScanClient TronScan API客户端
type TronScanClient struct {
	config     config.TronScanConfig
	httpClient *http.Client
	currentKey int // 当前使用的API密钥索引
	logger     *utils.Logger
}



// TronScanBlockInfo TronScan区块信息
type TronScanBlockInfo struct {
	BlockID     string `json:"blockID"`
	BlockHeader struct {
		RawData struct {
			Number    int64  `json:"number"`
			Timestamp int64  `json:"timestamp"`
			ParentHash string `json:"parentHash"`
		} `json:"raw_data"`
	} `json:"block_header"`
	Transactions []interface{} `json:"transactions"`
}

// TransactionInfo TronScan交易信息
type TransactionInfo struct {
	TxID      string `json:"txID"`
	BlockNum  int64  `json:"blockNumber"`
	Timestamp int64  `json:"timestamp"`
	From      string `json:"ownerAddress"`
	To        string `json:"toAddress"`
	Amount    string `json:"amount"`
	TokenInfo struct {
		TokenId   string `json:"tokenId"`
		TokenName string `json:"tokenName"`
		Decimals  int    `json:"tokenDecimal"`
	} `json:"tokenInfo"`
}

// TransferInfo 转账信息结构
type TransferInfo struct {
	TxID         string `json:"transaction_id"`
	BlockNum     int64  `json:"block_timestamp"`
	FromAddress  string `json:"from_address"`
	ToAddress    string `json:"to_address"`
	Amount       string `json:"quant"`
	TokenAddress string `json:"token_info"`
	Confirmed    bool   `json:"confirmed"`
}

// NewTronScanClient 创建TronScan客户端
func NewTronScanClient(cfg config.TronScanConfig, logger *utils.Logger) *TronScanClient {
	if !cfg.Enabled {
		return nil
	}

	client := &TronScanClient{
		config: cfg,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		currentKey: 0,
		logger:     logger,
	}

	logger.Info(fmt.Sprintf("TronScan客户端初始化完成 - API端点: %s, API密钥数量: %d",
		cfg.APIURL, len(cfg.APIKeys)))

	return client
}

// makeRequest 发送HTTP请求到TronScan API
func (c *TronScanClient) makeRequest(endpoint string, params map[string]string) ([]byte, error) {
	if !c.config.Enabled {
		return nil, fmt.Errorf("TronScan API未启用")
	}

	// 构建请求URL
	url := strings.TrimSuffix(c.config.APIURL, "/") + "/api/" + strings.TrimPrefix(endpoint, "/")

	// 添加查询参数
	if len(params) > 0 {
		queryParams := make([]string, 0, len(params))
		for key, value := range params {
			queryParams = append(queryParams, fmt.Sprintf("%s=%s", key, value))
		}
		url += "?" + strings.Join(queryParams, "&")
	}

	// 创建请求
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	// 添加API密钥到请求头
	if len(c.config.APIKeys) > 0 {
		apiKey := c.config.APIKeys[c.currentKey]
		req.Header.Set("TRON-PRO-API-KEY", apiKey)
		c.logger.Debug(fmt.Sprintf("使用API密钥索引: %d", c.currentKey))
	}

	// 设置其他请求头
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "TronIndexer/1.0")

	// 发送请求
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// 如果请求失败，尝试切换API密钥
		if len(c.config.APIKeys) > 1 {
			c.switchAPIKey()
			c.logger.Warn(fmt.Sprintf("请求失败，切换到API密钥索引: %d", c.currentKey))
		}
		return nil, fmt.Errorf("请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %v", err)
	}

	// 检查HTTP状态码
	if resp.StatusCode != http.StatusOK {
		// 如果是429限流错误，尝试切换API密钥
		if resp.StatusCode == 429 && len(c.config.APIKeys) > 1 {
			c.switchAPIKey()
			c.logger.Warn(fmt.Sprintf("遇到限流，切换到API密钥索引: %d", c.currentKey))
			return c.makeRequest(endpoint, params) // 重试
		}
		return nil, fmt.Errorf("HTTP错误: %d, 响应: %s", resp.StatusCode, string(body))
	}

	c.logger.Debug(fmt.Sprintf("TronScan API请求成功: %s", endpoint))
	return body, nil
}

// switchAPIKey 切换到下一个API密钥
func (c *TronScanClient) switchAPIKey() {
	if len(c.config.APIKeys) > 1 {
		c.currentKey = (c.currentKey + 1) % len(c.config.APIKeys)
	}
}

// GetLatestBlock 获取最新区块信息
func (c *TronScanClient) GetLatestBlock() (*TronScanBlockInfo, error) {
	body, err := c.makeRequest("block/latest", nil)
	if err != nil {
		return nil, err
	}

	// TronScan API直接返回区块数据，不需要包装结构
	var blockInfo TronScanBlockInfo
	if err := json.Unmarshal(body, &blockInfo); err != nil {
		return nil, fmt.Errorf("解析区块信息失败: %v", err)
	}

	return &blockInfo, nil
}

// GetBlockByNumber 根据区块号获取区块信息
func (c *TronScanClient) GetBlockByNumber(blockNum int64) (*TronScanBlockInfo, error) {
	params := map[string]string{
		"num": fmt.Sprintf("%d", blockNum),
	}

	body, err := c.makeRequest("block", params)
	if err != nil {
		return nil, err
	}

	// TronScan API直接返回区块数据，不需要包装结构
	var blockInfo TronScanBlockInfo
	if err := json.Unmarshal(body, &blockInfo); err != nil {
		return nil, fmt.Errorf("解析区块信息失败: %v", err)
	}

	return &blockInfo, nil
}

// GetTransactionsByBlock 获取指定区块的交易列表
func (c *TronScanClient) GetTransactionsByBlock(blockNum int64) ([]TransactionInfo, error) {
	params := map[string]string{
		"block": fmt.Sprintf("%d", blockNum),
		"limit": "200", // 每页最多200条
	}

	body, err := c.makeRequest("transaction", params)
	if err != nil {
		return nil, err
	}

	// TronScan API直接返回交易数据
	var apiResponse struct {
		Data []TransactionInfo `json:"data"`
	}
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		return nil, fmt.Errorf("解析交易信息失败: %v", err)
	}

	return apiResponse.Data, nil
}

// GetUSDTTransfers 获取USDT转账记录
func (c *TronScanClient) GetUSDTTransfers(address string, limit int) ([]TransferInfo, error) {
	params := map[string]string{
		"address": address,
		"limit":   fmt.Sprintf("%d", limit),
		"token":   "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", // USDT合约地址
	}

	body, err := c.makeRequest("transfer", params)
	if err != nil {
		return nil, err
	}

	// TronScan API直接返回转账数据
	var apiResponse struct {
		Data []TransferInfo `json:"data"`
	}
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		return nil, fmt.Errorf("解析转账信息失败: %v", err)
	}

	return apiResponse.Data, nil
}

// IsEnabled 检查TronScan API是否启用
func (c *TronScanClient) IsEnabled() bool {
	return c != nil && c.config.Enabled
}

// GetAPIKeyCount 获取可用API密钥数量
func (c *TronScanClient) GetAPIKeyCount() int {
	if c == nil {
		return 0
	}
	return len(c.config.APIKeys)
}
