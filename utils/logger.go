package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// LogLevel 日志级别
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	FATAL
)

// String 返回日志级别的字符串表示
func (l LogLevel) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// Logger 自定义日志器
type Logger struct {
	level         LogLevel
	fileLogger    *log.Logger  // 文件日志器
	consoleLogger *log.Logger  // 控制台日志器
	filePath      string
	errorCount    map[string]int64 // 错误统计
	mutex         sync.RWMutex     // 保护错误统计的互斥锁
	lastHealth    time.Time        // 最后健康检查时间
	healthStats   map[string]interface{} // 健康状态统计
	fileOutput    *os.File     // 文件输出
}

// StructuredLog 结构化日志条目
type StructuredLog struct {
	Timestamp string                 `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	File      string                 `json:"file"`
	Line      int                    `json:"line"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// HealthMetrics 健康检查指标
type HealthMetrics struct {
	Timestamp    time.Time              `json:"timestamp"`
	Component    string                 `json:"component"`
	Status       string                 `json:"status"`
	ResponseTime time.Duration          `json:"response_time"`
	ErrorCount   int64                  `json:"error_count"`
	Metrics      map[string]interface{} `json:"metrics"`
}

// NewLogger 创建新的日志器
func NewLogger(level LogLevel, filePath string) (*Logger, error) {
	var fileOutput *os.File
	var err error

	// 创建控制台日志器
	consoleLogger := log.New(os.Stdout, "", 0)

	// 创建文件日志器
	var fileLogger *log.Logger
	if filePath != "" {
		// 确保日志目录存在
		logDir := filepath.Dir(filePath)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %v", err)
		}

		// 打开或创建日志文件
		fileOutput, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %v", err)
		}
		fileLogger = log.New(fileOutput, "", 0)
	} else {
		// 如果没有文件路径，文件日志器也使用标准输出
		fileLogger = consoleLogger
	}

	return &Logger{
		level:         level,
		fileLogger:    fileLogger,
		consoleLogger: consoleLogger,
		filePath:      filePath,
		errorCount:    make(map[string]int64),
		lastHealth:    time.Now(),
		healthStats:   make(map[string]interface{}),
		fileOutput:    fileOutput,
	}, nil
}

// log 内部日志方法
// logToFile 输出日志到文件
func (l *Logger) logToFile(level LogLevel, format string, args ...interface{}) {
	if level < l.level {
		return
	}

	// 获取调用者信息
	_, file, line, ok := runtime.Caller(3)
	if !ok {
		file = "unknown"
		line = 0
	} else {
		file = filepath.Base(file)
	}

	// 格式化时间戳
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	// 构建日志消息
	message := fmt.Sprintf(format, args...)
	logMessage := fmt.Sprintf("[%s] %s %s:%d - %s", level.String(), timestamp, file, line, message)

	// 输出到文件
	l.fileLogger.Println(logMessage)

	// 如果是FATAL级别，退出程序
	if level == FATAL {
		os.Exit(1)
	}
}

// logToConsole 输出日志到控制台
func (l *Logger) logToConsole(format string, args ...interface{}) {
	// 直接输出到控制台，不添加时间戳和文件信息
	message := fmt.Sprintf(format, args...)
	l.consoleLogger.Println(message)
}

// log 内部日志方法（保持向后兼容）
func (l *Logger) log(level LogLevel, format string, args ...interface{}) {
	l.logToFile(level, format, args...)
}

// Debug 输出DEBUG级别日志
func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(DEBUG, format, args...)
}

// Info 输出INFO级别日志
func (l *Logger) Info(format string, args ...interface{}) {
	l.log(INFO, format, args...)
}

// Warn 输出WARN级别日志
func (l *Logger) Warn(format string, args ...interface{}) {
	l.log(WARN, format, args...)
}

// Error 输出ERROR级别日志并统计错误
func (l *Logger) Error(format string, args ...interface{}) {
	l.log(ERROR, format, args...)
	
	// 统计错误
	l.mutex.Lock()
	errorKey := fmt.Sprintf(format, args...)
	l.errorCount[errorKey]++
	l.mutex.Unlock()
}

// Fatal 输出FATAL级别日志并退出程序
func (l *Logger) Fatal(format string, args ...interface{}) {
	l.log(FATAL, format, args...)
}

// SetLevel 设置日志级别
func (l *Logger) SetLevel(level LogLevel) {
	l.level = level
}

// Close 关闭日志文件
func (l *Logger) Close() error {
	if l.fileOutput != nil && l.fileOutput != os.Stdout && l.fileOutput != os.Stderr {
		return l.fileOutput.Close()
	}
	return nil
}

// 全局日志器实例
var GlobalLogger *Logger

// InitGlobalLogger 初始化全局日志器
func InitGlobalLogger(level LogLevel, filePath string) error {
	var err error
	GlobalLogger, err = NewLogger(level, filePath)
	return err
}

// 全局日志方法
func Debug(format string, args ...interface{}) {
	if GlobalLogger != nil {
		GlobalLogger.Debug(format, args...)
	} else {
		log.Printf("[DEBUG] "+format, args...)
	}
}

func Info(format string, args ...interface{}) {
	if GlobalLogger != nil {
		GlobalLogger.Info(format, args...)
	} else {
		log.Printf("[INFO] "+format, args...)
	}
}

func Warn(format string, args ...interface{}) {
	if GlobalLogger != nil {
		GlobalLogger.Warn(format, args...)
	} else {
		log.Printf("[WARN] "+format, args...)
	}
}

func Error(format string, args ...interface{}) {
	if GlobalLogger != nil {
		GlobalLogger.Error(format, args...)
	} else {
		log.Printf("[ERROR] "+format, args...)
	}
}

func Fatal(format string, args ...interface{}) {
	if GlobalLogger != nil {
		GlobalLogger.Fatal(format, args...)
	} else {
		log.Fatalf(format, args...)
	}
}

// Console 输出到控制台（用于进度条等重要信息）
func Console(format string, args ...interface{}) {
	if GlobalLogger != nil {
		GlobalLogger.logToConsole(format, args...)
	} else {
		fmt.Printf(format+"\n", args...)
	}
}

// ProgressBar 专门用于进度条输出
func ProgressBar(format string, args ...interface{}) {
	Console(format, args...)
}

// FileLog 输出到文件（用于详细日志）
func FileLog(level LogLevel, format string, args ...interface{}) {
	if GlobalLogger != nil {
		GlobalLogger.logToFile(level, format, args...)
	}
}

// LogWithFields 输出带字段的结构化日志
func (l *Logger) LogWithFields(level LogLevel, message string, fields map[string]interface{}) {
	if level < l.level {
		return
	}

	// 获取调用者信息
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "unknown"
		line = 0
	} else {
		file = filepath.Base(file)
	}

	// 创建结构化日志
	structLog := StructuredLog{
		Timestamp: time.Now().Format(time.RFC3339),
		Level:     level.String(),
		Message:   message,
		File:      file,
		Line:      line,
		Fields:    fields,
	}

	// 序列化为JSON
	if jsonData, err := json.Marshal(structLog); err == nil {
		l.fileLogger.Println(string(jsonData))
	} else {
		// 如果JSON序列化失败，回退到普通日志
		l.log(level, "%s %v", message, fields)
	}

	// 如果是FATAL级别，退出程序
	if level == FATAL {
		os.Exit(1)
	}
}

// LogHealth 记录健康检查日志
func (l *Logger) LogHealth(component string, status string, responseTime time.Duration, metrics map[string]interface{}) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// 更新健康状态
	l.lastHealth = time.Now()
	l.healthStats[component] = map[string]interface{}{
		"status":        status,
		"response_time": responseTime,
		"last_check":    l.lastHealth,
		"metrics":       metrics,
	}

	// 获取错误计数
	errorCount := int64(0)
	for _, count := range l.errorCount {
		errorCount += count
	}

	// 创建健康检查指标（用于未来扩展）
	_ = HealthMetrics{
		Timestamp:    l.lastHealth,
		Component:    component,
		Status:       status,
		ResponseTime: responseTime,
		ErrorCount:   errorCount,
		Metrics:      metrics,
	}

	// 记录结构化健康日志
	fields := map[string]interface{}{
		"component":     component,
		"status":        status,
		"response_time": responseTime.String(),
		"error_count":   errorCount,
		"metrics":       metrics,
	}

	l.LogWithFields(INFO, fmt.Sprintf("Health check for %s: %s", component, status), fields)
}

// GetErrorStats 获取错误统计信息
func (l *Logger) GetErrorStats() map[string]int64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	// 复制错误统计
	stats := make(map[string]int64)
	for k, v := range l.errorCount {
		stats[k] = v
	}
	return stats
}

// GetHealthStats 获取健康状态统计
func (l *Logger) GetHealthStats() map[string]interface{} {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	// 复制健康状态统计
	stats := make(map[string]interface{})
	for k, v := range l.healthStats {
		stats[k] = v
	}
	return stats
}

// ClearErrorStats 清空错误统计
func (l *Logger) ClearErrorStats() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.errorCount = make(map[string]int64)
}

// GetLastHealthCheck 获取最后健康检查时间
func (l *Logger) GetLastHealthCheck() time.Time {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.lastHealth
}

// 全局健康检查和结构化日志方法
func LogWithFields(level LogLevel, message string, fields map[string]interface{}) {
	if GlobalLogger != nil {
		GlobalLogger.LogWithFields(level, message, fields)
	} else {
		// 回退到普通日志
		log.Printf("[%s] %s %v", level.String(), message, fields)
	}
}

func LogHealth(component string, status string, responseTime time.Duration, metrics map[string]interface{}) {
	if GlobalLogger != nil {
		GlobalLogger.LogHealth(component, status, responseTime, metrics)
	} else {
		log.Printf("[HEALTH] %s: %s (response_time: %s, metrics: %v)", component, status, responseTime, metrics)
	}
}

func GetErrorStats() map[string]int64 {
	if GlobalLogger != nil {
		return GlobalLogger.GetErrorStats()
	}
	return make(map[string]int64)
}

func GetHealthStats() map[string]interface{} {
	if GlobalLogger != nil {
		return GlobalLogger.GetHealthStats()
	}
	return make(map[string]interface{})
}

func ClearErrorStats() {
	if GlobalLogger != nil {
		GlobalLogger.ClearErrorStats()
	}
}

// LogToFile 专门用于将日志输出到文件，不输出到控制台
func LogToFile(format string, args ...interface{}) {
	if GlobalLogger != nil {
		GlobalLogger.fileLogger.Printf(format, args...)
	}
}