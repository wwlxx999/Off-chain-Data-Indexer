package middleware

import (
	"crypto/md5"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"Off-chainDatainDexer/services"
	"Off-chainDatainDexer/utils"
)

// CacheConfig 缓存配置
type CacheConfig struct {
	DefaultTTL time.Duration
	MaxSize    int64
	Enabled    bool
}

// ResponseCacheMiddleware API响应缓存中间件
func ResponseCacheMiddleware(cacheService *services.CacheService, config CacheConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 如果缓存服务不可用或缓存被禁用，直接跳过
		if cacheService == nil || !config.Enabled {
			c.Next()
			return
		}

		// 只缓存GET请求
		if c.Request.Method != http.MethodGet {
			c.Next()
			return
		}

		// 生成缓存键
		cacheKey := generateCacheKey(c)
		
		// 尝试从缓存获取响应
		var cachedResponse CachedResponse
		if err := cacheService.Get(cacheKey, &cachedResponse); err == nil {
			// 设置响应头
			for key, value := range cachedResponse.Headers {
				c.Header(key, value)
			}
			c.Header("X-Cache", "HIT")
			c.Header("X-Cache-Key", cacheKey)
			
			// 返回缓存的响应
			c.Data(cachedResponse.StatusCode, cachedResponse.ContentType, cachedResponse.Body)
			c.Abort()
			return
		}

		// 缓存未命中，继续处理请求
		c.Header("X-Cache", "MISS")
		c.Header("X-Cache-Key", cacheKey)

		// 创建响应写入器来捕获响应
		writer := &responseWriter{
			ResponseWriter: c.Writer,
			body:           make([]byte, 0),
			headers:        make(map[string]string),
		}
		c.Writer = writer

		// 继续处理请求
		c.Next()

		// 如果响应成功，缓存响应
		if writer.statusCode == http.StatusOK && len(writer.body) > 0 {
			// 确定缓存TTL
			ttl := determineCacheTTL(c.Request.URL.Path, config.DefaultTTL)
			
			// 创建缓存响应
			cachedResp := CachedResponse{
				StatusCode:  writer.statusCode,
				ContentType: writer.Header().Get("Content-Type"),
				Headers:     writer.headers,
				Body:        writer.body,
				CachedAt:    time.Now(),
			}

			// 存储到缓存
			if err := cacheService.Set(cacheKey, cachedResp, ttl); err != nil {
				utils.Warn("Failed to cache response: %v", err)
			}
		}
	}
}

// CachedResponse 缓存的响应结构
type CachedResponse struct {
	StatusCode  int               `json:"status_code"`
	ContentType string            `json:"content_type"`
	Headers     map[string]string `json:"headers"`
	Body        []byte            `json:"body"`
	CachedAt    time.Time         `json:"cached_at"`
}

// responseWriter 自定义响应写入器
type responseWriter struct {
	gin.ResponseWriter
	body       []byte
	headers    map[string]string
	statusCode int
}

func (w *responseWriter) Write(data []byte) (int, error) {
	w.body = append(w.body, data...)
	return w.ResponseWriter.Write(data)
}

func (w *responseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	// 复制重要的响应头
	for key, values := range w.ResponseWriter.Header() {
		if len(values) > 0 {
			w.headers[key] = values[0]
		}
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

// generateCacheKey 生成缓存键
func generateCacheKey(c *gin.Context) string {
	// 基于URL路径、查询参数和相关头部生成缓存键
	path := c.Request.URL.Path
	query := c.Request.URL.RawQuery
	
	// 对于某些端点，包含用户相关信息
	var keyParts []string
	keyParts = append(keyParts, "api_cache")
	keyParts = append(keyParts, path)
	
	if query != "" {
		keyParts = append(keyParts, query)
	}

	// 生成MD5哈希以确保键的唯一性和长度限制
	keyString := strings.Join(keyParts, ":")
	hash := md5.Sum([]byte(keyString))
	return fmt.Sprintf("api:%x", hash)
}

// determineCacheTTL 根据API端点确定缓存TTL
func determineCacheTTL(path string, defaultTTL time.Duration) time.Duration {
	switch {
	case strings.Contains(path, "/health"):
		return 30 * time.Second // 健康检查缓存30秒
	case strings.Contains(path, "/stats"):
		return 5 * time.Minute // 统计信息缓存5分钟
	case strings.Contains(path, "/transfers"):
		return 2 * time.Minute // 转账记录缓存2分钟
	case strings.Contains(path, "/market"):
		return 1 * time.Minute // 市场数据缓存1分钟
	case strings.Contains(path, "/indexer"):
		return 10 * time.Minute // 索引器状态缓存10分钟
	default:
		return defaultTTL
	}
}

// CacheInvalidationMiddleware 缓存失效中间件
func CacheInvalidationMiddleware(cacheService *services.CacheService) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 处理请求
		c.Next()

		// 如果是修改操作且成功，清除相关缓存
		if cacheService != nil && isModifyingRequest(c.Request.Method) && c.Writer.Status() < 400 {
			go func() {
				// 异步清除缓存，避免影响响应时间
				if err := invalidateRelatedCache(cacheService, c.Request.URL.Path); err != nil {
					utils.Warn("Failed to invalidate cache: %v", err)
				}
			}()
		}
	}
}

// isModifyingRequest 检查是否为修改请求
func isModifyingRequest(method string) bool {
	return method == http.MethodPost || method == http.MethodPut || 
		   method == http.MethodPatch || method == http.MethodDelete
}

// invalidateRelatedCache 清除相关缓存
func invalidateRelatedCache(cacheService *services.CacheService, path string) error {
	var patterns []string
	
	switch {
	case strings.Contains(path, "/transfers"):
		// 清除转账相关的所有缓存
		patterns = append(patterns, "api:*transfers*")
		patterns = append(patterns, "transfers:*")
		patterns = append(patterns, "stats:*")
	case strings.Contains(path, "/indexer"):
		// 清除索引器相关缓存
		patterns = append(patterns, "api:*indexer*")
		patterns = append(patterns, "api:*stats*")
	case strings.Contains(path, "/sync"):
		// 清除同步相关缓存
		patterns = append(patterns, "api:*sync*")
		patterns = append(patterns, "api:*stats*")
	}

	// 执行缓存清除
	for _, pattern := range patterns {
		if err := cacheService.FlushPattern(pattern); err != nil {
			return fmt.Errorf("failed to flush pattern %s: %w", pattern, err)
		}
	}

	return nil
}

// RateLimitMiddleware 基于Redis的限流中间件
func RateLimitMiddleware(cacheService *services.CacheService, requestsPerMinute int) gin.HandlerFunc {
	return func(c *gin.Context) {
		if cacheService == nil {
			c.Next()
			return
		}

		// 获取客户端IP
		clientIP := c.ClientIP()
		key := fmt.Sprintf("rate_limit:%s", clientIP)

		// 检查当前请求数
		count, err := cacheService.IncrementWithExpire(key, time.Minute)
		if err != nil {
			utils.Warn("Rate limit check failed: %v", err)
			c.Next()
			return
		}

		// 设置响应头
		c.Header("X-RateLimit-Limit", strconv.Itoa(requestsPerMinute))
		c.Header("X-RateLimit-Remaining", strconv.Itoa(max(0, requestsPerMinute-int(count))))
		c.Header("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10))

		// 检查是否超过限制
		if count > int64(requestsPerMinute) {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":   "Rate limit exceeded",
				"message": fmt.Sprintf("Maximum %d requests per minute allowed", requestsPerMinute),
			})
			c.Abort()
			return
		}

		c.Next()
	}
}

// max 返回两个整数中的较大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}