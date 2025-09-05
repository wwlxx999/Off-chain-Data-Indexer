package utils

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// DistributedLock 分布式锁接口
type DistributedLock interface {
	// Lock 获取锁，返回锁实例和错误
	Lock(ctx context.Context, key string, ttl time.Duration) (*LockInstance, error)
	
	// TryLock 尝试获取锁，不阻塞
	TryLock(ctx context.Context, key string, ttl time.Duration) (*LockInstance, error)
	
	// LockWithRetry 带重试的锁获取
	LockWithRetry(ctx context.Context, key string, ttl time.Duration, maxRetries int, retryDelay time.Duration) (*LockInstance, error)
}

// LockInstance 锁实例
type LockInstance struct {
	key        string
	value      string
	ttl        time.Duration
	client     *redis.Client
	ctx        context.Context
	released   bool
	mu         sync.RWMutex
	lock       *RedisDistributedLock
	acquiredAt time.Time
}

// LockMetrics 锁的监控指标
type LockMetrics struct {
	TotalLocks       int64         `json:"total_locks"`       // 总锁获取次数
	SuccessfulLocks  int64         `json:"successful_locks"`  // 成功获取锁次数
	FailedLocks      int64         `json:"failed_locks"`      // 获取锁失败次数
	TotalRetries     int64         `json:"total_retries"`     // 总重试次数
	AverageWaitTime  time.Duration `json:"average_wait_time"` // 平均等待时间
	AverageLockTime  time.Duration `json:"average_lock_time"` // 平均持锁时间
	ActiveLocksCount int64         `json:"active_locks_count"` // 当前活跃锁数量
	mutex            sync.RWMutex
}

// RedisDistributedLock Redis分布式锁实现
type RedisDistributedLock struct {
	client  *redis.Client
	ctx     context.Context
	metrics *LockMetrics
	logger  *log.Logger
}

// LockConfig 锁配置
type LockConfig struct {
	DefaultTTL    time.Duration // 默认锁超时时间
	RetryDelay    time.Duration // 重试延迟
	MaxRetries    int           // 最大重试次数
	RenewalRatio  float64       // 续期比例 (0.5 表示在50%时间点续期)
}

// DefaultLockConfig 默认锁配置
var DefaultLockConfig = LockConfig{
	DefaultTTL:   30 * time.Second,
	RetryDelay:   100 * time.Millisecond,
	MaxRetries:   10,
	RenewalRatio: 0.5,
}

// NewRedisDistributedLock 创建Redis分布式锁
func NewRedisDistributedLock(client *redis.Client) *RedisDistributedLock {
	return &RedisDistributedLock{
		client:  client,
		ctx:     context.Background(),
		metrics: &LockMetrics{},
		logger:  log.New(log.Writer(), "[DistributedLock] ", log.LstdFlags|log.Lshortfile),
	}
}

// GetMetrics 获取锁的监控指标
func (r *RedisDistributedLock) GetMetrics() *LockMetrics {
	r.metrics.mutex.RLock()
	defer r.metrics.mutex.RUnlock()
	
	// 返回指标的副本
	return &LockMetrics{
		TotalLocks:       r.metrics.TotalLocks,
		SuccessfulLocks:  r.metrics.SuccessfulLocks,
		FailedLocks:      r.metrics.FailedLocks,
		TotalRetries:     r.metrics.TotalRetries,
		AverageWaitTime:  r.metrics.AverageWaitTime,
		AverageLockTime:  r.metrics.AverageLockTime,
		ActiveLocksCount: r.metrics.ActiveLocksCount,
	}
}

// ResetMetrics 重置监控指标
func (r *RedisDistributedLock) ResetMetrics() {
	r.metrics.mutex.Lock()
	defer r.metrics.mutex.Unlock()
	
	r.metrics.TotalLocks = 0
	r.metrics.SuccessfulLocks = 0
	r.metrics.FailedLocks = 0
	r.metrics.TotalRetries = 0
	r.metrics.AverageWaitTime = 0
	r.metrics.AverageLockTime = 0
	r.metrics.ActiveLocksCount = 0
}

// Lock 获取锁（阻塞直到获取成功或超时）
func (r *RedisDistributedLock) Lock(ctx context.Context, key string, ttl time.Duration) (*LockInstance, error) {
	return r.LockWithRetry(ctx, key, ttl, DefaultLockConfig.MaxRetries, DefaultLockConfig.RetryDelay)
}

// TryLock 尝试获取锁（不阻塞）
func (r *RedisDistributedLock) TryLock(ctx context.Context, key string, ttl time.Duration) (*LockInstance, error) {
	start := time.Now()
	value, err := generateLockValue()
	if err != nil {
		return nil, fmt.Errorf("failed to generate lock value: %w", err)
	}

	lockKey := fmt.Sprintf("lock:%s", key)
	
	// 更新监控指标
	r.updateMetrics(func(m *LockMetrics) {
		m.TotalLocks++
	})
	
	r.logger.Printf("Attempting to acquire lock: %s (TTL: %v)", lockKey, ttl)
	
	// 使用 SET key value NX EX ttl 命令原子性地设置锁
	result, err := r.client.SetNX(ctx, lockKey, value, ttl).Result()
	if err != nil {
		r.updateMetrics(func(m *LockMetrics) {
			m.FailedLocks++
		})
		r.logger.Printf("Failed to acquire lock %s: %v", lockKey, err)
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	if !result {
		r.updateMetrics(func(m *LockMetrics) {
			m.FailedLocks++
		})
		r.logger.Printf("Lock %s already held by another process", lockKey)
		return nil, errors.New("lock already held by another process")
	}

	// 成功获取锁
	waitTime := time.Since(start)
	r.updateMetrics(func(m *LockMetrics) {
		m.SuccessfulLocks++
		m.ActiveLocksCount++
		// 更新平均等待时间
		if m.SuccessfulLocks == 1 {
			m.AverageWaitTime = waitTime
		} else {
			m.AverageWaitTime = (m.AverageWaitTime*time.Duration(m.SuccessfulLocks-1) + waitTime) / time.Duration(m.SuccessfulLocks)
		}
	})
	
	r.logger.Printf("Successfully acquired lock: %s (wait time: %v)", lockKey, waitTime)
	
	return &LockInstance{
		key:        lockKey,
		value:      value,
		ttl:        ttl,
		client:     r.client,
		ctx:        ctx,
		released:   false,
		lock:       r,
		acquiredAt: time.Now(),
	}, nil
}

// RetryConfig 重试配置
type RetryConfig struct {
	MaxRetries    int           `json:"max_retries"`    // 最大重试次数
	BaseDelay     time.Duration `json:"base_delay"`     // 基础延迟时间
	MaxDelay      time.Duration `json:"max_delay"`      // 最大延迟时间
	JitterEnabled bool          `json:"jitter_enabled"` // 是否启用抖动
	BackoffFactor float64       `json:"backoff_factor"` // 退避因子
}

// DefaultRetryConfig 默认重试配置
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:    5,
		BaseDelay:     100 * time.Millisecond,
		MaxDelay:      5 * time.Second,
		JitterEnabled: true,
		BackoffFactor: 2.0,
	}
}

// LockWithRetry 带重试的锁获取
func (r *RedisDistributedLock) LockWithRetry(ctx context.Context, key string, ttl time.Duration, maxRetries int, retryDelay time.Duration) (*LockInstance, error) {
	config := &RetryConfig{
		MaxRetries:    maxRetries,
		BaseDelay:     retryDelay,
		MaxDelay:      5 * time.Second,
		JitterEnabled: true,
		BackoffFactor: 1.5,
	}
	return r.LockWithRetryConfig(ctx, key, ttl, config)
}

// LockWithRetryConfig 使用配置进行重试获取锁
func (r *RedisDistributedLock) LockWithRetryConfig(ctx context.Context, key string, ttl time.Duration, config *RetryConfig) (*LockInstance, error) {
	r.logger.Printf("Starting lock acquisition with retry for key: %s (max retries: %d)", key, config.MaxRetries)
	
	for i := 0; i <= config.MaxRetries; i++ {
		lock, err := r.TryLock(ctx, key, ttl)
		if err == nil {
			return lock, nil
		}

		// 更新重试计数
		r.updateMetrics(func(m *LockMetrics) {
			m.TotalRetries++
		})

		// 如果是最后一次重试，返回错误
		if i == config.MaxRetries {
			r.logger.Printf("Failed to acquire lock %s after %d retries", key, config.MaxRetries)
			return nil, fmt.Errorf("failed to acquire lock after %d retries: %w", config.MaxRetries, err)
		}

		// 计算延迟时间（指数退避 + 可选抖动）
		delay := r.calculateDelay(i, config)
		r.logger.Printf("Retrying lock acquisition for %s in %v (attempt %d/%d)", key, delay, i+1, config.MaxRetries)

		// 检查上下文是否已取消
		select {
		case <-ctx.Done():
			r.logger.Printf("Lock acquisition cancelled for %s: %v", key, ctx.Err())
			return nil, ctx.Err()
		case <-time.After(delay):
			// 继续下一次重试
		}
	}

	return nil, errors.New("unexpected end of retry loop")
}

// calculateDelay 计算延迟时间
func (r *RedisDistributedLock) calculateDelay(attempt int, config *RetryConfig) time.Duration {
	// 指数退避
	delay := time.Duration(float64(config.BaseDelay) * math.Pow(config.BackoffFactor, float64(attempt)))
	
	// 限制最大延迟
	if delay > config.MaxDelay {
		delay = config.MaxDelay
	}
	
	// 添加抖动以避免惊群效应
	if config.JitterEnabled {
		jitter := r.generateJitter(delay)
		delay = delay + jitter
	}
	
	return delay
}

// generateJitter 生成抖动时间
func (r *RedisDistributedLock) generateJitter(baseDelay time.Duration) time.Duration {
	// 生成 0 到 baseDelay/4 的随机抖动
	maxJitter := int64(baseDelay / 4)
	if maxJitter <= 0 {
		return 0
	}
	
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return 0
	}
	
	// 将字节转换为int64
	var n int64
	for i := 0; i < 8; i++ {
		n = (n << 8) | int64(bytes[i])
	}
	if n < 0 {
		n = -n
	}
	
	return time.Duration(n % maxJitter)
}

// updateMetrics 线程安全地更新监控指标
func (r *RedisDistributedLock) updateMetrics(updateFunc func(*LockMetrics)) {
	r.metrics.mutex.Lock()
	defer r.metrics.mutex.Unlock()
	updateFunc(r.metrics)
}

// Release 释放锁
func (l *LockInstance) Release() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.released {
		return errors.New("lock already released")
	}

	// 计算持锁时间
	lockDuration := time.Since(l.acquiredAt)

	// 使用Lua脚本确保只有锁的持有者才能释放锁
	luaScript := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`

	result, err := l.client.Eval(l.ctx, luaScript, []string{l.key}, l.value).Result()
	if err != nil {
		if l.lock != nil {
			l.lock.logger.Printf("Failed to release lock %s: %v", l.key, err)
		}
		return fmt.Errorf("failed to release lock: %w", err)
	}

	if result.(int64) == 0 {
		if l.lock != nil {
			l.lock.logger.Printf("Lock %s not held or expired when trying to release", l.key)
		}
		return errors.New("lock not held by this instance")
	}

	// 更新监控指标
	if l.lock != nil {
		l.lock.updateMetrics(func(m *LockMetrics) {
			m.ActiveLocksCount--
			// 更新平均持锁时间
			if m.SuccessfulLocks == 1 {
				m.AverageLockTime = lockDuration
			} else {
				m.AverageLockTime = (m.AverageLockTime*time.Duration(m.SuccessfulLocks-1) + lockDuration) / time.Duration(m.SuccessfulLocks)
			}
		})
		
		l.lock.logger.Printf("Successfully released lock: %s (held for: %v)", l.key, lockDuration)
	}

	l.released = true
	return nil
}

// Renew 续期锁
func (l *LockInstance) Renew(newTTL time.Duration) error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.released {
		return errors.New("cannot renew released lock")
	}

	// 使用Lua脚本确保只有锁的持有者才能续期
	luaScript := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("EXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`

	result, err := l.client.Eval(l.ctx, luaScript, []string{l.key}, l.value, int(newTTL.Seconds())).Result()
	if err != nil {
		return fmt.Errorf("failed to renew lock: %w", err)
	}

	if result.(int64) == 0 {
		return errors.New("lock not held by this instance")
	}

	l.ttl = newTTL
	Debug("Distributed lock renewed: %s, new TTL: %v", l.key, newTTL)
	return nil
}

// IsHeld 检查锁是否仍被持有
func (l *LockInstance) IsHeld() (bool, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.released {
		return false, nil
	}

	value, err := l.client.Get(l.ctx, l.key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}

	return value == l.value, nil
}

// StartAutoRenewal 启动自动续期
func (l *LockInstance) StartAutoRenewal(ctx context.Context) {
	go func() {
		renewalInterval := time.Duration(float64(l.ttl) * DefaultLockConfig.RenewalRatio)
		ticker := time.NewTicker(renewalInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				l.mu.RLock()
				if l.released {
					l.mu.RUnlock()
					return
				}
				l.mu.RUnlock()

				if err := l.Renew(l.ttl); err != nil {
					Warn("Failed to auto-renew lock %s: %v", l.key, err)
					return
				}
			}
		}
	}()
}

// generateLockValue 生成唯一的锁值
func generateLockValue() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// WithLock 使用分布式锁执行函数
func WithLock(lock DistributedLock, ctx context.Context, key string, ttl time.Duration, fn func() error) error {
	lockInstance, err := lock.Lock(ctx, key, ttl)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	defer func() {
		if releaseErr := lockInstance.Release(); releaseErr != nil {
			Warn("Failed to release lock %s: %v", key, releaseErr)
		}
	}()

	// 启动自动续期
	lockInstance.StartAutoRenewal(ctx)

	return fn()
}

// WithTryLock 尝试使用分布式锁执行函数（不阻塞）
func WithTryLock(lock DistributedLock, ctx context.Context, key string, ttl time.Duration, fn func() error) error {
	lockInstance, err := lock.TryLock(ctx, key, ttl)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	defer func() {
		if releaseErr := lockInstance.Release(); releaseErr != nil {
			Warn("Failed to release lock %s: %v", key, releaseErr)
		}
	}()

	return fn()
}