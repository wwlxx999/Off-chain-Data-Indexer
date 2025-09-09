package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"Off-chainDatainDexer/utils"
)

// MarketDataResponse 市场数据响应结构
type MarketDataResponse struct {
	ID                       string   `json:"id"`
	Symbol                   string   `json:"symbol"`
	Name                     string   `json:"name"`
	CurrentPrice             float64  `json:"current_price"`
	MarketCap                float64  `json:"market_cap"`
	MarketCapRank            int      `json:"market_cap_rank"`
	TotalVolume              float64  `json:"total_volume"`
	CirculatingSupply        float64  `json:"circulating_supply"`
	TotalSupply              float64  `json:"total_supply"`
	MaxSupply                *float64 `json:"max_supply"`
	PriceChange24h           float64  `json:"price_change_24h"`
	PriceChangePercentage24h float64  `json:"price_change_percentage_24h"`
	LastUpdated              string   `json:"last_updated"`
}

// USDTMarketData USDT市场数据响应结构
type USDTMarketData struct {
	Symbol                   string   `json:"symbol"`
	Name                     string   `json:"name"`
	CurrentPrice             float64  `json:"current_price"`
	TotalSupply              float64  `json:"total_supply"`
	CirculatingSupply        float64  `json:"circulating_supply"`
	MaxSupply                *float64 `json:"max_supply,omitempty"`
	MarketCap                float64  `json:"market_cap"`
	CirculatingMarketCap     float64  `json:"circulating_market_cap"`
	TotalVolume              float64  `json:"total_volume"`
	PriceChange24h           float64  `json:"price_change_24h"`
	PriceChangePercentage24h float64  `json:"price_change_percentage_24h"`
	Holders                  int64    `json:"holders,omitempty"`
	Transfers                int64    `json:"transfers,omitempty"`
	YesterdayTransfers       int64    `json:"yesterday_transfers"`
	LastUpdated              string   `json:"last_updated"`
	DataSource               string   `json:"data_source"`
}

// CircuitBreaker 熔断器状态
type CircuitBreakerState int

const (
	Closed CircuitBreakerState = iota
	Open
	HalfOpen
)

// CircuitBreaker 熔断器
type CircuitBreaker struct {
	state        CircuitBreakerState
	failureCount int
	lastFailTime time.Time
	successCount int
	maxFailures  int
	timeout      time.Duration
	mutex        sync.RWMutex
}

// CacheEntry 缓存条目
type CacheEntry struct {
	data      interface{}
	timestamp time.Time
	ttl       time.Duration
}

// MarketService 市场数据服务
type MarketService struct {
	client          *http.Client
	circuitBreaker  *CircuitBreaker
	cache           map[string]*CacheEntry
	cacheMutex      sync.RWMutex
	maxRetries      int
	retryDelay      time.Duration
	transferService *TransferService
}

// NewMarketService 创建新的市场数据服务
func NewMarketService() *MarketService {
	return &MarketService{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		circuitBreaker: &CircuitBreaker{
			state:       Closed,
			maxFailures: 3,
			timeout:     30 * time.Second, // 降低超时时间
		},
		cache:      make(map[string]*CacheEntry),
		maxRetries: 3,
		retryDelay: 2 * time.Second,
	}
}

// SetTransferService 设置转账服务
func (ms *MarketService) SetTransferService(transferService *TransferService) {
	ms.transferService = transferService
}

// ResetCircuitBreaker 重置熔断器状态
func (ms *MarketService) ResetCircuitBreaker() {
	ms.circuitBreaker.mutex.Lock()
	defer ms.circuitBreaker.mutex.Unlock()
	ms.circuitBreaker.state = Closed
	ms.circuitBreaker.failureCount = 0
	ms.circuitBreaker.successCount = 0
}

// GetUSDTMarketData 获取TRON网络USDT市场数据
func (ms *MarketService) GetUSDTMarketData() (*USDTMarketData, error) {
	cacheKey := "tronscan_usdt_data"

	// 尝试从缓存获取
	if cached, found := ms.getFromCache(cacheKey); found {
		if data, ok := cached.(*USDTMarketData); ok {
			utils.Info("Returning cached TronScan USDT data")
			return data, nil
		}
	}

	// 使用TronScan API获取TRON网络上的USDT数据
	url := "https://apilist.tronscanapi.com/api/token_trc20?contract=TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t&showAll=1"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body, err := ms.makeRequestWithRetry(ctx, url)
	if err != nil {
		return nil, utils.WrapError(err, "failed to fetch TRON USDT data")
	}

	// 解析TronScan API响应
	var tronResponse map[string]interface{}
	if err := json.Unmarshal(body, &tronResponse); err != nil {
		return nil, utils.NewExternalError("failed to parse TronScan JSON response", err)
	}

	// 获取trc20_tokens数组中的第一个元素
	var tronData map[string]interface{}
	if tokensArray, ok := tronResponse["trc20_tokens"].([]interface{}); ok && len(tokensArray) > 0 {
		if tokenData, ok := tokensArray[0].(map[string]interface{}); ok {
			tronData = tokenData
		} else {
			return nil, utils.NewExternalError("invalid TronScan data format", nil)
		}
	} else {
		return nil, utils.NewExternalError("no tokens found in TronScan response", nil)
	}

	// 从TronScan获取TRON网络USDT数据
	totalSupply := 0.0
	// 尝试从total_supply_with_decimals获取总供应量
	if ts, ok := tronData["total_supply_with_decimals"].(string); ok {
		if parsed, err := parseStringToFloat(ts); err == nil {
			// 转换为正常单位（除以10^6，因为USDT有6位小数）
			totalSupply = parsed / 1000000
		}
	}

	// 对于USDT，流通供应量通常略小于总供应量（考虑到一些代币可能被锁定或销毁）
	circulatingSupply := totalSupply * 0.995 // 假设99.5%的代币在流通
	
	// 获取持有者数量和转账数量
	holders := int64(0)
	if h, ok := tronData["holders_count"].(float64); ok {
		holders = int64(h)
	}
	
	transfers := int64(0)
	if t, ok := tronData["transfer_num"].(float64); ok {
		transfers = int64(t)
	}

	// 获取CoinGecko的价格数据作为补充
	priceData, err := ms.getCoinGeckoPriceData()
	if err != nil {
		// 如果CoinGecko失败，使用默认价格
		priceData = &MarketDataResponse{
			CurrentPrice:             1.0,
			MarketCap:                totalSupply,
			PriceChange24h:           0,
			PriceChangePercentage24h: 0,
			LastUpdated:              time.Now().Format(time.RFC3339),
		}
	}

	// 计算市值
	marketCap := totalSupply * priceData.CurrentPrice
	if priceData.MarketCap > 0 {
		// 如果CoinGecko提供了市值数据，优先使用
		marketCap = priceData.MarketCap
	}
	
	// 计算流通市值（通常略小于总市值）
	circulatingMarketCap := circulatingSupply * priceData.CurrentPrice
	if circulatingSupply < totalSupply {
		// 如果流通供应量小于总供应量，流通市值应该相应减少
		circulatingMarketCap = marketCap * (circulatingSupply / totalSupply)
	}

	result := &USDTMarketData{
		Symbol:                   "USDT",
		Name:                     "Tether USD",
		CurrentPrice:             priceData.CurrentPrice,
		TotalSupply:              totalSupply,
		CirculatingSupply:        circulatingSupply,
		MaxSupply:                nil, // USDT没有最大供应量
		MarketCap:                marketCap,
		CirculatingMarketCap:     circulatingMarketCap,
		TotalVolume:              priceData.TotalVolume,
		PriceChange24h:           priceData.PriceChange24h,
		PriceChangePercentage24h: priceData.PriceChangePercentage24h,
		Holders:                  holders,
		Transfers:                transfers,
		LastUpdated:              priceData.LastUpdated,
		DataSource:               "TronScan",
	}

	// 缓存结果（5分钟TTL）
	ms.setCache(cacheKey, result, 3*time.Minute)
	utils.Info("Successfully fetched and cached TronScan USDT data")

	return result, nil
}

// parseStringToFloat 解析字符串为浮点数，处理科学计数法
func parseStringToFloat(s string) (float64, error) {
	// 移除可能的逗号分隔符
	s = strings.ReplaceAll(s, ",", "")
	return strconv.ParseFloat(s, 64)
}

// 熔断器方法

// CanExecute 检查是否可以执行请求
func (cb *CircuitBreaker) CanExecute() bool {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()

	switch cb.state {
	case Closed:
		return true
	case Open:
		return time.Since(cb.lastFailTime) >= cb.timeout
	case HalfOpen:
		return true
	default:
		return false
	}
}

// OnSuccess 记录成功调用
func (cb *CircuitBreaker) OnSuccess() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	if cb.state == HalfOpen {
		cb.successCount++
		if cb.successCount >= 3 { // 连续3次成功后关闭熔断器
			cb.state = Closed
			cb.failureCount = 0
			cb.successCount = 0
		}
	} else {
		cb.failureCount = 0
	}
}

// OnFailure 记录失败调用
func (cb *CircuitBreaker) OnFailure() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.failureCount++
	cb.lastFailTime = time.Now()
	cb.successCount = 0

	if cb.failureCount >= cb.maxFailures {
		cb.state = Open
	} else if cb.state == Open && time.Since(cb.lastFailTime) >= cb.timeout {
		cb.state = HalfOpen
	}
}

// 缓存方法

// getFromCache 从缓存获取数据
func (ms *MarketService) getFromCache(key string) (interface{}, bool) {
	ms.cacheMutex.RLock()
	defer ms.cacheMutex.RUnlock()

	entry, exists := ms.cache[key]
	if !exists {
		return nil, false
	}

	// 检查是否过期
	if time.Since(entry.timestamp) > entry.ttl {
		delete(ms.cache, key)
		return nil, false
	}

	return entry.data, true
}

// setCache 设置缓存
func (ms *MarketService) setCache(key string, data interface{}, ttl time.Duration) {
	ms.cacheMutex.Lock()
	defer ms.cacheMutex.Unlock()

	ms.cache[key] = &CacheEntry{
		data:      data,
		timestamp: time.Now(),
		ttl:       ttl,
	}
}

// clearExpiredCache 清理过期缓存
func (ms *MarketService) clearExpiredCache() {
	ms.cacheMutex.Lock()
	defer ms.cacheMutex.Unlock()

	now := time.Now()
	for key, entry := range ms.cache {
		if now.Sub(entry.timestamp) > entry.ttl {
			delete(ms.cache, key)
		}
	}
}

// makeRequestWithRetry 带重试的HTTP请求
func (ms *MarketService) makeRequestWithRetry(ctx context.Context, url string) ([]byte, error) {
	// 检查熔断器状态
	if !ms.circuitBreaker.CanExecute() {
		return nil, utils.NewExternalError("Circuit breaker is open, request blocked", nil)
	}

	var lastErr error
	for attempt := 0; attempt <= ms.maxRetries; attempt++ {
		if attempt > 0 {
			// 等待重试延迟
			select {
			case <-ctx.Done():
				return nil, utils.NewTimeoutError("Request cancelled", ctx.Err())
			case <-time.After(ms.retryDelay * time.Duration(attempt)):
			}
		}

		resp, err := ms.client.Get(url)
		if err != nil {
			lastErr = utils.NewNetworkError(fmt.Sprintf("HTTP request failed (attempt %d/%d)", attempt+1, ms.maxRetries+1), err)
			continue
		}
		defer resp.Body.Close()

		// 检查HTTP状态码
		if resp.StatusCode == http.StatusTooManyRequests {
			lastErr = utils.NewExternalError(fmt.Sprintf("Rate limited (attempt %d/%d)", attempt+1, ms.maxRetries+1), nil)
			continue
		}

		if resp.StatusCode >= 500 {
			lastErr = utils.NewExternalError(fmt.Sprintf("Server error %d (attempt %d/%d)", resp.StatusCode, attempt+1, ms.maxRetries+1), nil)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			lastErr = utils.NewExternalError(fmt.Sprintf("API request failed with status %d: %s", resp.StatusCode, string(body)), nil)
			ms.circuitBreaker.OnFailure()
			return nil, lastErr
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			lastErr = utils.NewInternalError("Failed to read response body", err)
			continue
		}

		// 请求成功
		ms.circuitBreaker.OnSuccess()
		return body, nil
	}

	// 所有重试都失败了
	ms.circuitBreaker.OnFailure()
	return nil, lastErr
}

// getCoinGeckoPriceData 获取CoinGecko价格数据作为补充
func (ms *MarketService) getCoinGeckoPriceData() (*MarketDataResponse, error) {
	cacheKey := "coingecko_price_data"

	// 尝试从缓存获取
	if cached, found := ms.getFromCache(cacheKey); found {
		if data, ok := cached.(*MarketDataResponse); ok {
			utils.Info("Returning cached CoinGecko price data")
			return data, nil
		}
	}

	url := "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=tether&order=market_cap_desc&per_page=1&page=1&sparkline=false"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body, err := ms.makeRequestWithRetry(ctx, url)
	if err != nil {
		return nil, utils.WrapError(err, "failed to fetch CoinGecko price data")
	}

	var marketData []MarketDataResponse
	if err := json.Unmarshal(body, &marketData); err != nil {
		return nil, utils.NewExternalError("failed to parse CoinGecko JSON response", err)
	}

	if len(marketData) == 0 {
		return nil, utils.NewExternalError("no price data found for USDT", nil)
	}

	// 缓存结果（5分钟TTL）
	ms.setCache(cacheKey, &marketData[0], 3*time.Minute)
	utils.Info("Successfully fetched and cached CoinGecko price data")

	return &marketData[0], nil
}

// getDetailedCoinGeckoPriceData 获取CoinGecko详细价格数据
func (ms *MarketService) getDetailedCoinGeckoPriceData() (*MarketDataResponse, error) {
	cacheKey := "coingecko_detailed_price_data"

	// 尝试从缓存获取
	if cached, found := ms.getFromCache(cacheKey); found {
		if data, ok := cached.(*MarketDataResponse); ok {
			utils.Info("Returning cached CoinGecko detailed price data")
			return data, nil
		}
	}

	url := "https://api.coingecko.com/api/v3/coins/tether?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false&sparkline=false"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body, err := ms.makeRequestWithRetry(ctx, url)
	if err != nil {
		return nil, utils.WrapError(err, "failed to fetch detailed CoinGecko data")
	}

	// 解析详细的API响应
	var detailedData map[string]interface{}
	if err := json.Unmarshal(body, &detailedData); err != nil {
		return nil, utils.NewExternalError("failed to parse detailed CoinGecko JSON response", err)
	}

	marketData, ok := detailedData["market_data"].(map[string]interface{})
	if !ok {
		return nil, utils.NewExternalError("market_data not found in detailed response", nil)
	}

	// 提取市场数据
	currentPrice := 0.9998 // 默认价格（更接近实际USDT价格）
	if priceData, ok := marketData["current_price"].(map[string]interface{}); ok {
		if usdPrice, ok := priceData["usd"].(float64); ok {
			currentPrice = usdPrice
		}
	}

	totalVolume := 0.0
	if volData, ok := marketData["total_volume"].(map[string]interface{}); ok {
		if usdVol, ok := volData["usd"].(float64); ok {
			totalVolume = usdVol
		}
	}

	priceChange24h := 0.0
	if pc, ok := marketData["price_change_24h"].(float64); ok {
		priceChange24h = pc
	}

	priceChangePercentage24h := 0.0
	if pcp, ok := marketData["price_change_percentage_24h"].(float64); ok {
		priceChangePercentage24h = pcp
	}

	lastUpdated := time.Now().Format(time.RFC3339)
	if lu, ok := marketData["last_updated"].(string); ok {
		lastUpdated = lu
	}

	result := &MarketDataResponse{
		CurrentPrice:             currentPrice,
		TotalVolume:              totalVolume,
		PriceChange24h:           priceChange24h,
		PriceChangePercentage24h: priceChangePercentage24h,
		LastUpdated:              lastUpdated,
	}

	// 缓存结果（5分钟TTL）
	ms.setCache(cacheKey, result, 3*time.Minute)
	utils.Info("Successfully fetched and cached CoinGecko detailed price data")

	return result, nil
}

// GetUSDTDataFromCoinGecko 从CoinGecko获取USDT数据
func (ms *MarketService) GetUSDTDataFromCoinGecko() (*USDTMarketData, error) {
	cacheKey := "coingecko_usdt_data"

	// 尝试从缓存获取
	if cached, found := ms.getFromCache(cacheKey); found {
		if data, ok := cached.(*USDTMarketData); ok {
			utils.Info("Returning cached CoinGecko USDT data")
			return data, nil
		}
	}

	priceData, err := ms.getCoinGeckoPriceData()
	if err != nil {
		return nil, utils.WrapError(err, "failed to get CoinGecko price data")
	}

	result := &USDTMarketData{
		Symbol:                   "USDT",
		Name:                     "Tether USD",
		CurrentPrice:             priceData.CurrentPrice,
		MarketCap:                priceData.MarketCap,
		TotalVolume:              priceData.TotalVolume,
		PriceChange24h:           priceData.PriceChange24h,
		PriceChangePercentage24h: priceData.PriceChangePercentage24h,
		LastUpdated:              priceData.LastUpdated,
		DataSource:               "CoinGecko",
	}

	// 缓存结果（3分钟TTL）
	ms.setCache(cacheKey, result, 3*time.Minute)
	utils.Info("Successfully fetched and cached CoinGecko USDT data")

	return result, nil
}

// GetUSDTMarketDataFromCoinGecko 从CoinGecko获取USDT市场数据
func (ms *MarketService) GetUSDTMarketDataFromCoinGecko() (*USDTMarketData, error) {
	cacheKey := "coingecko_market_usdt_data"

	// 尝试从缓存获取
	if cached, found := ms.getFromCache(cacheKey); found {
		if data, ok := cached.(*USDTMarketData); ok {
			utils.Info("Returning cached CoinGecko market USDT data")
			return data, nil
		}
	}

	url := "https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body, err := ms.makeRequestWithRetry(ctx, url)
	if err != nil {
		return nil, utils.WrapError(err, "failed to fetch CoinGecko data")
	}

	var marketData map[string]map[string]interface{}
	if err := json.Unmarshal(body, &marketData); err != nil {
		return nil, utils.NewExternalError("failed to parse CoinGecko JSON response", err)
	}

	tetherData, ok := marketData["tether"]
	if !ok {
		return nil, utils.NewExternalError("tether data not found in CoinGecko response", nil)
	}

	currentPrice := 0.9998 // 默认USDT价格
	if price, ok := tetherData["usd"].(float64); ok {
		currentPrice = price
	}

	marketCap := 0.0
	if mc, ok := tetherData["usd_market_cap"].(float64); ok {
		marketCap = mc
	}

	totalVolume := 0.0
	if vol, ok := tetherData["usd_24h_vol"].(float64); ok {
		totalVolume = vol
	}

	priceChangePercentage24h := 0.0
	if change, ok := tetherData["usd_24h_change"].(float64); ok {
		priceChangePercentage24h = change
	}

	// 计算总供应量和流通供应量（基于市值和价格）
	circulatingSupply := marketCap / currentPrice
	totalSupply := circulatingSupply // CoinGecko中USDT的总供应量通常等于流通供应量

	lastUpdated := time.Now().Format(time.RFC3339)
	if timestamp, ok := tetherData["last_updated_at"].(float64); ok {
		lastUpdated = time.Unix(int64(timestamp), 0).Format(time.RFC3339)
	}

	result := &USDTMarketData{
		Symbol:                   "USDT",
		Name:                     "Tether USD",
		CurrentPrice:             currentPrice,
		TotalSupply:              totalSupply,
		CirculatingSupply:        circulatingSupply,
		MarketCap:                marketCap,
		CirculatingMarketCap:     marketCap,
		TotalVolume:              totalVolume,
		PriceChangePercentage24h: priceChangePercentage24h,
		LastUpdated:              lastUpdated,
		DataSource:               "CoinGecko",
	}

	// 缓存结果（3分钟TTL）
	ms.setCache(cacheKey, result, 3*time.Minute)
	utils.Info("Successfully fetched and cached CoinGecko market USDT data")

	return result, nil
}

// GetDetailedUSDTDataFromCoinGecko 从CoinGecko获取USDT详细数据
func (ms *MarketService) GetDetailedUSDTDataFromCoinGecko() (*USDTMarketData, error) {
	cacheKey := "coingecko_detailed_usdt_data"

	// 尝试从缓存获取
	if cached, found := ms.getFromCache(cacheKey); found {
		if data, ok := cached.(*USDTMarketData); ok {
			utils.Info("Returning cached CoinGecko detailed USDT data")
			return data, nil
		}
	}

	priceData, err := ms.getDetailedCoinGeckoPriceData()
	if err != nil {
		return nil, utils.WrapError(err, "failed to get detailed CoinGecko price data")
	}

	// 从详细API获取更多信息
	url := "https://api.coingecko.com/api/v3/coins/tether?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false&sparkline=false"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body, err := ms.makeRequestWithRetry(ctx, url)
	if err != nil {
		return nil, utils.WrapError(err, "failed to fetch detailed CoinGecko data")
	}

	var detailedData map[string]interface{}
	if err := json.Unmarshal(body, &detailedData); err != nil {
		return nil, utils.NewExternalError("failed to parse detailed CoinGecko JSON response", err)
	}

	marketData, ok := detailedData["market_data"].(map[string]interface{})
	if !ok {
		return nil, utils.NewExternalError("market_data not found in detailed response", nil)
	}

	// 提取详细市场数据
	currentPrice := priceData.CurrentPrice

	marketCap := 0.0
	if mcData, ok := marketData["market_cap"].(map[string]interface{}); ok {
		if usdMc, ok := mcData["usd"].(float64); ok {
			marketCap = usdMc
		}
	}

	circulatingSupply := 0.0
	if cs, ok := marketData["circulating_supply"].(float64); ok {
		circulatingSupply = cs
	}

	totalSupply := 0.0
	if ts, ok := marketData["total_supply"].(float64); ok {
		totalSupply = ts
	}

	var maxSupply *float64
	if ms, ok := marketData["max_supply"].(float64); ok {
		maxSupply = &ms
	}

	result := &USDTMarketData{
		Symbol:                   "USDT",
		Name:                     "Tether USD",
		CurrentPrice:             currentPrice,
		TotalSupply:              totalSupply,
		CirculatingSupply:        circulatingSupply,
		MaxSupply:                maxSupply,
		MarketCap:                marketCap,
		CirculatingMarketCap:     circulatingSupply * currentPrice,
		TotalVolume:              priceData.TotalVolume,
		PriceChange24h:           priceData.PriceChange24h,
		PriceChangePercentage24h: priceData.PriceChangePercentage24h,
		LastUpdated:              priceData.LastUpdated,
		DataSource:               "CoinGecko (Detailed)",
	}

	// 缓存结果（3分钟TTL）
	ms.setCache(cacheKey, result, 3*time.Minute)
	utils.Info("Successfully fetched and cached CoinGecko detailed USDT data")

	return result, nil
}

// GetDetailedUSDTData 获取TRON网络USDT详细数据（包含更多信息）
func (ms *MarketService) GetDetailedUSDTData() (*USDTMarketData, error) {
	cacheKey := "tronscan_detailed_usdt_data"

	// 尝试从缓存获取
	if cached, found := ms.getFromCache(cacheKey); found {
		if data, ok := cached.(*USDTMarketData); ok {
			utils.Info("Returning cached TronScan detailed USDT data")
			return data, nil
		}
	}

	// 使用TronScan API获取详细的TRON网络USDT数据
	tronUrl := "https://apilist.tronscanapi.com/api/token_trc20?contract=TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t&showAll=1"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body, err := ms.makeRequestWithRetry(ctx, tronUrl)
	if err != nil {
		return nil, utils.WrapError(err, "failed to fetch detailed TRON USDT data")
	}

	// 解析TronScan API响应
	var tronResponse map[string]interface{}
	if err := json.Unmarshal(body, &tronResponse); err != nil {
		return nil, utils.NewExternalError("failed to parse TronScan JSON response", err)
	}

	// 获取trc20_tokens数组中的第一个元素
	var tronData map[string]interface{}
	if tokensArray, ok := tronResponse["trc20_tokens"].([]interface{}); ok && len(tokensArray) > 0 {
		if tokenData, ok := tokensArray[0].(map[string]interface{}); ok {
			tronData = tokenData
		} else {
			return nil, utils.NewExternalError("invalid TronScan data format", nil)
		}
	} else {
		return nil, utils.NewExternalError("no tokens found in TronScan response", nil)
	}

	// 从TronScan获取TRON网络USDT数据
	totalSupply := 0.0
	if ts, ok := tronData["total_supply_with_decimals"].(string); ok {
		utils.Info("Found total_supply_with_decimals: %s", ts)
		if parsed, err := parseStringToFloat(ts); err == nil {
			utils.Info("Parsed total_supply_with_decimals: %f", parsed)
			// 转换为正常单位（除以10^6，因为USDT有6位小数）
			totalSupply = parsed / 1000000
			utils.Info("Final totalSupply: %f", totalSupply)
		} else {
			utils.Error("Failed to parse total_supply_with_decimals: %v", err)
		}
	} else {
		utils.Error("total_supply_with_decimals not found or not string type")
	}

	// 对于USDT，流通供应量通常略小于总供应量（考虑锁定或销毁的代币）
	circulatingSupply := totalSupply * 0.995 // 假设99.5%的代币在流通

	// 提取持有者数量
	holderCount := int64(0)
	if holders, ok := tronData["holders_count"].(float64); ok {
		holderCount = int64(holders)
	}

	// 提取转账数量
	transferCount := int64(0)
	if transfers, ok := tronData["transfer_num"].(float64); ok {
		transferCount = int64(transfers)
	}

	// 获取昨天的转账数量
	yesterdayTransferCount := int64(0)
	if ms.transferService != nil {
		if count, err := ms.transferService.GetYesterdayTransferCount(); err == nil {
			yesterdayTransferCount = count
		} else {
			utils.Warn("Failed to get yesterday transfer count: %v", err)
		}
	}

	// 获取CoinGecko的详细价格数据
	priceData, err := ms.getDetailedCoinGeckoPriceData()
	if err != nil {
		utils.Warn("CoinGecko API failed, using fallback price data: %v", err)
		// 如果CoinGecko失败，使用更合理的默认价格（USDT通常接近1美元，但不完全等于1）
		priceData = &MarketDataResponse{
			CurrentPrice:             0.9998, // 更接近实际USDT价格
			MarketCap:                0,       // 将在下面重新计算
			TotalVolume:              0,       // 无法获取准确数据时设为0
			PriceChange24h:           0,
			PriceChangePercentage24h: 0,
			LastUpdated:              time.Now().Format(time.RFC3339),
		}
	}

	// 计算市值
	marketCap := totalSupply * priceData.CurrentPrice
	if priceData.MarketCap > 0 {
		// 如果CoinGecko提供了市值数据，优先使用
		marketCap = priceData.MarketCap
	}
	
	// 计算流通市值（通常略小于总市值）
	circulatingMarketCap := circulatingSupply * priceData.CurrentPrice
	if circulatingSupply < totalSupply {
		// 如果流通供应量小于总供应量，流通市值应该相应减少
		circulatingMarketCap = marketCap * (circulatingSupply / totalSupply)
	}

	result := &USDTMarketData{
		Symbol:                   "USDT",
		Name:                     "Tether USD",
		CurrentPrice:             priceData.CurrentPrice,
		TotalSupply:              totalSupply,
		CirculatingSupply:        circulatingSupply,
		MaxSupply:                nil, // USDT没有最大供应量
		MarketCap:                marketCap,
		CirculatingMarketCap:     circulatingMarketCap,
		TotalVolume:              priceData.TotalVolume,
		PriceChange24h:           priceData.PriceChange24h,
		PriceChangePercentage24h: priceData.PriceChangePercentage24h,
		Holders:                  holderCount,
		Transfers:                transferCount,
		YesterdayTransfers:       yesterdayTransferCount,
		LastUpdated:              priceData.LastUpdated,
		DataSource:               "TronScan (Detailed)",
	}

	// 缓存结果（3分钟TTL）
	ms.setCache(cacheKey, result, 3*time.Minute)
	utils.Info("Successfully fetched and cached TronScan detailed USDT data")

	return result, nil
}
