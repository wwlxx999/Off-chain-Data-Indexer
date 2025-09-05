package services

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"Off-chainDatainDexer/utils"
)

// CacheService Redis缓存服务
type CacheService struct {
	client          *redis.Client
	ctx             context.Context
	distributedLock *utils.RedisDistributedLock
}

// NewCacheService 创建新的缓存服务实例
func NewCacheService(redisURL string) (*CacheService, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	client := redis.NewClient(opt)
	ctx := context.Background()

	// 测试连接
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// 创建分布式锁
	distributedLock := utils.NewRedisDistributedLock(client)

	utils.Info("Redis cache service initialized successfully")
	return &CacheService{
		client:          client,
		ctx:             ctx,
		distributedLock: distributedLock,
	}, nil
}

// Set 设置缓存
func (c *CacheService) Set(key string, value interface{}, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal cache value: %w", err)
	}

	return c.client.Set(c.ctx, key, data, expiration).Err()
}

// Get 获取缓存
func (c *CacheService) Get(key string, dest interface{}) error {
	val, err := c.client.Get(c.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("cache miss for key: %s", key)
		}
		return fmt.Errorf("failed to get cache: %w", err)
	}

	return json.Unmarshal([]byte(val), dest)
}

// Delete 删除缓存
func (c *CacheService) Delete(key string) error {
	return c.client.Del(c.ctx, key).Err()
}

// Exists 检查缓存是否存在
func (c *CacheService) Exists(key string) (bool, error) {
	result, err := c.client.Exists(c.ctx, key).Result()
	return result > 0, err
}

// SetNX 设置缓存（仅当key不存在时）
func (c *CacheService) SetNX(key string, value interface{}, expiration time.Duration) (bool, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return false, fmt.Errorf("failed to marshal cache value: %w", err)
	}

	return c.client.SetNX(c.ctx, key, data, expiration).Result()
}

// Increment 递增计数器
func (c *CacheService) Increment(key string) (int64, error) {
	return c.client.Incr(c.ctx, key).Result()
}

// IncrementWithExpire 递增计数器并设置过期时间
func (c *CacheService) IncrementWithExpire(key string, expiration time.Duration) (int64, error) {
	pipe := c.client.Pipeline()
	incrCmd := pipe.Incr(c.ctx, key)
	pipe.Expire(c.ctx, key, expiration)
	_, err := pipe.Exec(c.ctx)
	if err != nil {
		return 0, err
	}
	return incrCmd.Val(), nil
}

// GetMultiple 批量获取缓存
func (c *CacheService) GetMultiple(keys []string) (map[string]string, error) {
	if len(keys) == 0 {
		return make(map[string]string), nil
	}

	results, err := c.client.MGet(c.ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	response := make(map[string]string)
	for i, result := range results {
		if result != nil {
			response[keys[i]] = result.(string)
		}
	}

	return response, nil
}

// SetMultiple 批量设置缓存
func (c *CacheService) SetMultiple(data map[string]interface{}, expiration time.Duration) error {
	pipe := c.client.Pipeline()
	for key, value := range data {
		jsonData, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
		}
		pipe.Set(c.ctx, key, jsonData, expiration)
	}
	_, err := pipe.Exec(c.ctx)
	return err
}

// FlushPattern 删除匹配模式的所有key
func (c *CacheService) FlushPattern(pattern string) error {
	keys, err := c.client.Keys(c.ctx, pattern).Result()
	if err != nil {
		return err
	}

	if len(keys) > 0 {
		return c.client.Del(c.ctx, keys...).Err()
	}
	return nil
}

// Close 关闭Redis连接
func (c *CacheService) Close() error {
	return c.client.Close()
}

// GetStats 获取缓存统计信息
func (c *CacheService) GetStats() (map[string]string, error) {
	info, err := c.client.Info(c.ctx, "stats").Result()
	if err != nil {
		return nil, err
	}

	stats := make(map[string]string)
	stats["info"] = info
	return stats, nil
}

// 缓存键生成器
type CacheKeyGenerator struct{}

// TransferListKey 生成转账列表缓存键
func (g *CacheKeyGenerator) TransferListKey(page, pageSize int) string {
	return fmt.Sprintf("transfers:list:%d:%d", page, pageSize)
}

// TransferByAddressKey 生成地址转账缓存键
func (g *CacheKeyGenerator) TransferByAddressKey(address string, page, pageSize int) string {
	return fmt.Sprintf("transfers:address:%s:%d:%d", address, page, pageSize)
}

// TransferByHashKey 生成哈希转账缓存键
func (g *CacheKeyGenerator) TransferByHashKey(hash string) string {
	return fmt.Sprintf("transfers:hash:%s", hash)
}

// StatsKey 生成统计信息缓存键
func (g *CacheKeyGenerator) StatsKey() string {
	return "stats:summary"
}

// HourlyStatsKey 生成小时统计缓存键
func (g *CacheKeyGenerator) HourlyStatsKey() string {
	return "stats:hourly"
}

// TopAddressesKey 生成热门地址缓存键
func (g *CacheKeyGenerator) TopAddressesKey(limit int) string {
	return fmt.Sprintf("stats:top_addresses:%d", limit)
}

// MarketDataKey 生成市场数据缓存键
func (g *CacheKeyGenerator) MarketDataKey(source string) string {
	return fmt.Sprintf("market:usdt:%s", source)
}

// BlockRangeKey 生成区块范围缓存键
func (g *CacheKeyGenerator) BlockRangeKey(start, end uint64) string {
	return fmt.Sprintf("transfers:blocks:%d:%d", start, end)
}

// GetWithLock 使用分布式锁获取缓存，防止缓存击穿
func (c *CacheService) GetWithLock(key string, dest interface{}, lockTTL time.Duration, fetchFunc func() (interface{}, error)) error {
	// 先尝试直接从缓存获取
	if err := c.Get(key, dest); err == nil {
		return nil
	}

	// 缓存未命中，使用分布式锁防止缓存击穿
	lockKey := fmt.Sprintf("cache_lock:%s", key)
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	// 使用优化的重试配置
	retryConfig := &utils.RetryConfig{
		MaxRetries:    3,
		BaseDelay:     50 * time.Millisecond,
		MaxDelay:      2 * time.Second,
		JitterEnabled: true,
		BackoffFactor: 1.5,
	}

	lock, err := c.distributedLock.LockWithRetryConfig(ctx, lockKey, lockTTL, retryConfig)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer lock.Release()

	// 再次检查缓存（可能在等待锁的过程中已被其他进程填充）
	if err := c.Get(key, dest); err == nil {
		return nil
	}

	// 执行数据获取函数
	data, err := fetchFunc()
	if err != nil {
		return fmt.Errorf("failed to fetch data: %w", err)
	}

	// 设置缓存
	if err := c.Set(key, data, 10*time.Minute); err != nil {
		return fmt.Errorf("failed to set cache: %w", err)
	}

	// 将数据复制到目标变量
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	return json.Unmarshal(dataBytes, dest)
}

// GetWithTryLock 尝试使用分布式锁获取缓存（不阻塞）
func (c *CacheService) GetWithTryLock(key string, dest interface{}, lockTTL time.Duration, fetchFunc func() (interface{}, error)) error {
	// 先尝试直接从缓存获取
	if err := c.Get(key, dest); err == nil {
		return nil
	}

	// 缓存未命中，尝试获取分布式锁
	lockKey := fmt.Sprintf("cache_lock:%s", key)
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()

	return utils.WithTryLock(c.distributedLock, ctx, lockKey, lockTTL, func() error {
		// 再次检查缓存
		if err := c.Get(key, dest); err == nil {
			return nil
		}

		// 执行数据获取函数
		data, err := fetchFunc()
		if err != nil {
			return fmt.Errorf("failed to fetch data: %w", err)
		}

		// 设置缓存
		if err := c.Set(key, data, 10*time.Minute); err != nil {
			return fmt.Errorf("failed to set cache: %w", err)
		}

		// 将数据复制到目标变量
		dataBytes, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to marshal data: %w", err)
		}

		return json.Unmarshal(dataBytes, dest)
	})
}

// SetWithLock 使用分布式锁设置缓存
func (c *CacheService) SetWithLock(key string, value interface{}, expiration time.Duration) error {
	lockKey := fmt.Sprintf("cache_lock:%s", key)
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()

	return utils.WithTryLock(c.distributedLock, ctx, lockKey, 5*time.Second, func() error {
		return c.Set(key, value, expiration)
	})
}

// DeleteWithLock 使用分布式锁删除缓存
func (c *CacheService) DeleteWithLock(key string) error {
	lockKey := fmt.Sprintf("cache_lock:%s", key)
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()

	return utils.WithTryLock(c.distributedLock, ctx, lockKey, 5*time.Second, func() error {
		return c.Delete(key)
	})
}

// FlushPatternWithLock 使用分布式锁批量删除缓存
func (c *CacheService) FlushPatternWithLock(pattern string) error {
	lockKey := fmt.Sprintf("cache_lock:flush:%s", pattern)
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	return utils.WithTryLock(c.distributedLock, ctx, lockKey, 10*time.Second, func() error {
		return c.FlushPattern(pattern)
	})
}

// GetDistributedLockMetrics 获取分布式锁监控指标
func (c *CacheService) GetDistributedLockMetrics() *utils.LockMetrics {
	if c.distributedLock == nil {
		return nil
	}
	return c.distributedLock.GetMetrics()
}

// ResetDistributedLockMetrics 重置分布式锁监控指标
func (c *CacheService) ResetDistributedLockMetrics() {
	if c.distributedLock != nil {
		c.distributedLock.ResetMetrics()
	}
}

// 全局缓存键生成器实例
var CacheKeys = &CacheKeyGenerator{}