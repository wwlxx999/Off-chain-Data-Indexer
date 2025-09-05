package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"Off-chainDatainDexer/services"
	"Off-chainDatainDexer/utils"
)

// LockMetricsHandler 分布式锁监控处理器
type LockMetricsHandler struct {
	cacheService *services.CacheService
}

// NewLockMetricsHandler 创建分布式锁监控处理器
func NewLockMetricsHandler(cacheService *services.CacheService) *LockMetricsHandler {
	return &LockMetricsHandler{
		cacheService: cacheService,
	}
}

// ErrorResponse 错误响应结构
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// LockMetricsResponse 锁监控响应
type LockMetricsResponse struct {
	Metrics   *utils.LockMetrics `json:"metrics"`
	Timestamp time.Time          `json:"timestamp"`
	Status    string             `json:"status"`
}

// GetLockMetrics 获取分布式锁监控指标
// @Summary 获取分布式锁监控指标
// @Description 返回分布式锁的使用情况和性能指标
// @Tags monitoring
// @Accept json
// @Produce json
// @Success 200 {object} LockMetricsResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/metrics/locks [get]
func (h *LockMetricsHandler) GetLockMetrics(c *gin.Context) {
	if h.cacheService == nil {
		c.JSON(http.StatusServiceUnavailable, ErrorResponse{
			Error:   "Cache service not available",
			Message: "Distributed lock metrics are not available when cache service is disabled",
		})
		return
	}

	// 获取分布式锁的监控指标
	metrics := h.cacheService.GetDistributedLockMetrics()
	if metrics == nil {
		c.JSON(http.StatusServiceUnavailable, ErrorResponse{
			Error:   "Metrics not available",
			Message: "Distributed lock metrics are not available",
		})
		return
	}

	response := LockMetricsResponse{
		Metrics:   metrics,
		Timestamp: time.Now(),
		Status:    "success",
	}

	c.JSON(http.StatusOK, response)
}

// ResetLockMetrics 重置分布式锁监控指标
// @Summary 重置分布式锁监控指标
// @Description 重置所有分布式锁的监控指标计数器
// @Tags monitoring
// @Accept json
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/metrics/locks/reset [post]
func (h *LockMetricsHandler) ResetLockMetrics(c *gin.Context) {
	if h.cacheService == nil {
		c.JSON(http.StatusServiceUnavailable, ErrorResponse{
			Error:   "Cache service not available",
			Message: "Cannot reset distributed lock metrics when cache service is disabled",
		})
		return
	}

	// 重置分布式锁的监控指标
	h.cacheService.ResetDistributedLockMetrics()

	c.JSON(http.StatusOK, gin.H{
		"message":   "Distributed lock metrics reset successfully",
		"timestamp": time.Now(),
		"status":    "success",
	})
}

// GetLockHealth 获取分布式锁健康状态
// @Summary 获取分布式锁健康状态
// @Description 检查分布式锁系统的健康状态
// @Tags monitoring
// @Accept json
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Failure 503 {object} ErrorResponse
// @Router /api/v1/health/locks [get]
func (h *LockMetricsHandler) GetLockHealth(c *gin.Context) {
	if h.cacheService == nil {
		c.JSON(http.StatusServiceUnavailable, ErrorResponse{
			Error:   "Cache service not available",
			Message: "Distributed lock system is not available",
		})
		return
	}

	// 获取监控指标来评估健康状态
	metrics := h.cacheService.GetDistributedLockMetrics()
	if metrics == nil {
		c.JSON(http.StatusServiceUnavailable, ErrorResponse{
			Error:   "Metrics not available",
			Message: "Cannot determine lock system health",
		})
		return
	}

	// 计算成功率
	successRate := float64(0)
	if metrics.TotalLocks > 0 {
		successRate = float64(metrics.SuccessfulLocks) / float64(metrics.TotalLocks) * 100
	}

	// 判断健康状态
	status := "healthy"
	if successRate < 95 {
		status = "degraded"
	}
	if successRate < 80 {
		status = "unhealthy"
	}

	health := gin.H{
		"status":              status,
		"success_rate":        successRate,
		"total_locks":         metrics.TotalLocks,
		"successful_locks":    metrics.SuccessfulLocks,
		"failed_locks":        metrics.FailedLocks,
		"active_locks_count":  metrics.ActiveLocksCount,
		"average_wait_time":   metrics.AverageWaitTime.String(),
		"average_lock_time":   metrics.AverageLockTime.String(),
		"total_retries":       metrics.TotalRetries,
		"timestamp":           time.Now(),
	}

	if status == "healthy" {
		c.JSON(http.StatusOK, health)
	} else {
		c.JSON(http.StatusServiceUnavailable, health)
	}
}