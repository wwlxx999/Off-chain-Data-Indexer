package config

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config 配置结构（保持向后兼容）
type Config struct {
	DBHost       string
	DBPort       string
	DBUser       string
	DBPassword   string
	DBName       string
	RedisURL     string
	TronNodeURL  string
	TronAPIKey   string
	USDTContract string
	SyncInterval time.Duration
	StartBlock   uint64
	BatchSize    uint64
	LogLevel     string // 日志级别配置
	// 新增故障处理配置
	TronNodes    []TronNodeConfig
	RetryConfig  RetryConfig
	// 新增并发同步配置
	ConcurrentConfig ConcurrentConfig
	// TronScan API配置
	TronScanConfig TronScanConfig
}

// TronNodeConfig TRON节点配置
type TronNodeConfig struct {
	URL    string
	APIKey string
	Weight int // 节点权重，用于负载均衡
}

// RetryConfig 重试配置
type RetryConfig struct {
	MaxRetries    int           // 最大重试次数
	InitialDelay  time.Duration // 初始延迟
	MaxDelay      time.Duration // 最大延迟
	BackoffFactor float64       // 退避因子
	Timeout       time.Duration // 请求超时时间
}

// ConcurrentConfig 并发同步配置
type ConcurrentConfig struct {
	WorkerCount     int           // 工作协程数量
	ChunkSize       uint64        // 每个工作块的大小
	MaxConcurrent   int           // 最大并发请求数
	BufferSize      int           // 任务缓冲区大小
	SyncTimeout     time.Duration // 单次同步超时时间
	CoordinateDelay time.Duration // 协调延迟时间
}

// TronScanConfig TronScan API配置
type TronScanConfig struct {
	APIURL   string   // TronScan API端点URL
	APIKeys  []string // TronScan API密钥列表
	Timeout  time.Duration // 请求超时时间
	Enabled  bool     // 是否启用TronScan API
}

// LoadConfig 加载配置（保持向后兼容）
func LoadConfig() *Config {
	// 加载 .env 文件
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: .env file not found, using environment variables")
	}

	config := &Config{
		DBHost:       getEnv("DB_HOST", "localhost"),
		DBPort:       getEnv("DB_PORT", "5432"),
		DBUser:       getEnv("DB_USER", "postgres"),
		DBPassword:   getEnv("DB_PASSWORD", ""),
		DBName:       getEnv("DB_NAME", "tron_usdt"),
		RedisURL:     getEnv("REDIS_URL", "redis://localhost:6379"),
		TronNodeURL:  getEnv("TRON_NODE_URL", "https://api.trongrid.io"),
		TronAPIKey:   getEnv("TRON_API_KEY", ""),
		USDTContract: getEnv("USDT_CONTRACT", "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"),
		SyncInterval: parseDuration(getEnv("SYNC_INTERVAL", "10s")),
		StartBlock:   parseUint64(getEnv("START_BLOCK", "0")),
		BatchSize:    parseUint64(getEnv("BATCH_SIZE", "100")),
		LogLevel:     getEnv("LOG_LEVEL", "INFO"),
	}

	// 加载多节点配置
	config.TronNodes = loadTronNodes()
	// 加载重试配置
	config.RetryConfig = loadRetryConfig()
	// 加载并发配置
	config.ConcurrentConfig = loadConcurrentConfig()
	// 加载TronScan配置
	config.TronScanConfig = loadTronScanConfig()

	return config
}

// LoadEnvFile 加载环境变量文件（供多链配置使用）
func LoadEnvFile() error {
	return godotenv.Load()
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// parseDuration 解析时间间隔
func parseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		log.Printf("Warning: invalid duration %s, using default 10s", s)
		return 10 * time.Second
	}
	return d
}

// parseUint64 解析无符号整数
func parseUint64(s string) uint64 {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		log.Printf("Warning: invalid uint64 %s, using default 0", s)
		return 0
	}
	return v
}

// parseInt 解析整数
func parseInt(s string, defaultValue int) int {
	v, err := strconv.Atoi(s)
	if err != nil {
		log.Printf("Warning: invalid int %s, using default %d", s, defaultValue)
		return defaultValue
	}
	return v
}

// parseFloat64 解析浮点数
func parseFloat64(s string, defaultValue float64) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Printf("Warning: invalid float64 %s, using default %f", s, defaultValue)
		return defaultValue
	}
	return v
}

// loadTronNodes 加载TRON节点配置
func loadTronNodes() []TronNodeConfig {
	nodes := []TronNodeConfig{}
	
	// 主节点（向后兼容）
	mainNode := TronNodeConfig{
		URL:    getEnv("TRON_NODE_URL", "https://api.trongrid.io"),
		APIKey: getEnv("TRON_API_KEY", ""),
		Weight: 100,
	}
	nodes = append(nodes, mainNode)
	
	// 扩展的备用节点配置 - 支持更多节点
	backupNodeConfigs := []struct{
		envKey string
		apiKeyEnv string
		weight int
	}{
		{"TRON_NODE_URL_2", "TRON_API_KEY_2", 80},
		{"TRON_NODE_URL_3", "TRON_API_KEY_3", 70},
		{"TRON_NODE_URL_4", "TRON_API_KEY_4", 60},
		{"TRON_NODE_URL_5", "TRON_API_KEY_5", 50},
		{"TRON_NODE_URL_6", "TRON_API_KEY_6", 40},
		{"TRON_NODE_URL_7", "TRON_API_KEY_7", 30},
	}
	
	for _, config := range backupNodeConfigs {
		url := getEnv(config.envKey, "")
		if url != "" {
			node := TronNodeConfig{
				URL:    url,
				APIKey: getEnv(config.apiKeyEnv, ""),
				Weight: config.weight,
			}
			nodes = append(nodes, node)
		}
	}
	
	return nodes
}

// loadRetryConfig 加载重试配置
func loadRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:    parseInt(getEnv("MAX_RETRIES", "3"), 3),
		InitialDelay:  parseDuration(getEnv("INITIAL_DELAY", "1s")),
		MaxDelay:      parseDuration(getEnv("MAX_DELAY", "30s")),
		BackoffFactor: parseFloat64(getEnv("BACKOFF_FACTOR", "2.0"), 2.0),
		Timeout:       parseDuration(getEnv("REQUEST_TIMEOUT", "30s")),
	}
}

// loadTronScanConfig 加载TronScan API配置
func loadTronScanConfig() TronScanConfig {
	apiURL := getEnv("TRONSCAN_API_URL", "https://apilist.tronscanapi.com")
	
	// 收集所有配置的API密钥
	var apiKeys []string
	if key1 := getEnv("TRONSCAN_API_KEY_1", ""); key1 != "" {
		apiKeys = append(apiKeys, key1)
	}
	if key2 := getEnv("TRONSCAN_API_KEY_2", ""); key2 != "" {
		apiKeys = append(apiKeys, key2)
	}
	
	// 如果有API密钥则启用TronScan API
	enabled := len(apiKeys) > 0
	
	return TronScanConfig{
		APIURL:  apiURL,
		APIKeys: apiKeys,
		Timeout: parseDuration(getEnv("TRONSCAN_TIMEOUT", "30s")),
		Enabled: enabled,
	}
}

// loadConcurrentConfig 加载并发配置
func loadConcurrentConfig() ConcurrentConfig {
	return ConcurrentConfig{
		WorkerCount:     parseInt(getEnv("CONCURRENT_WORKER_COUNT", "4"), 4),
		ChunkSize:       parseUint64(getEnv("CONCURRENT_CHUNK_SIZE", "25")),
		MaxConcurrent:   parseInt(getEnv("CONCURRENT_MAX_CONCURRENT", "8"), 8),
		BufferSize:      parseInt(getEnv("CONCURRENT_BUFFER_SIZE", "100"), 100),
		SyncTimeout:     parseDuration(getEnv("CONCURRENT_SYNC_TIMEOUT", "5m")),
		CoordinateDelay: parseDuration(getEnv("CONCURRENT_COORDINATE_DELAY", "100ms")),
	}
}
