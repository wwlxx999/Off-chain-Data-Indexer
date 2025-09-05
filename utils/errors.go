package utils

import (
	"fmt"
	"runtime"
	"time"
)

// ErrorType 错误类型
type ErrorType string

const (
	DatabaseError   ErrorType = "DATABASE_ERROR"
	ValidationError ErrorType = "VALIDATION_ERROR"
	NotFoundError   ErrorType = "NOT_FOUND_ERROR"
	ConflictError   ErrorType = "CONFLICT_ERROR"
	InternalError   ErrorType = "INTERNAL_ERROR"
	ExternalError   ErrorType = "EXTERNAL_ERROR"
	NetworkError    ErrorType = "NETWORK_ERROR"
	TimeoutError    ErrorType = "TIMEOUT_ERROR"
	AuthError       ErrorType = "AUTH_ERROR"
	PermissionError ErrorType = "PERMISSION_ERROR"
)

// AppError 应用程序错误结构
type AppError struct {
	Type        ErrorType `json:"type"`
	Message     string    `json:"message"`
	Code        int       `json:"code"`
	Details     string    `json:"details,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	File        string    `json:"file,omitempty"`
	Line        int       `json:"line,omitempty"`
	Function    string    `json:"function,omitempty"`
	OriginalErr error     `json:"-"`
}

// Error 实现error接口
func (e *AppError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Type, e.Message)
}

// Unwrap 返回原始错误
func (e *AppError) Unwrap() error {
	return e.OriginalErr
}

// NewAppError 创建新的应用程序错误
func NewAppError(errType ErrorType, message string, code int, originalErr error) *AppError {
	// 获取调用者信息
	pc, file, line, ok := runtime.Caller(1)
	var function string
	if ok {
		if fn := runtime.FuncForPC(pc); fn != nil {
			function = fn.Name()
		}
	}

	return &AppError{
		Type:        errType,
		Message:     message,
		Code:        code,
		Timestamp:   time.Now(),
		File:        file,
		Line:        line,
		Function:    function,
		OriginalErr: originalErr,
	}
}

// WithDetails 添加错误详情
func (e *AppError) WithDetails(details string) *AppError {
	e.Details = details
	return e
}

// 预定义的错误创建函数

// NewDatabaseError 创建数据库错误
func NewDatabaseError(message string, originalErr error) *AppError {
	return NewAppError(DatabaseError, message, 500, originalErr)
}

// NewValidationError 创建验证错误
func NewValidationError(message string, originalErr error) *AppError {
	return NewAppError(ValidationError, message, 400, originalErr)
}

// NewNotFoundError 创建未找到错误
func NewNotFoundError(message string, originalErr error) *AppError {
	return NewAppError(NotFoundError, message, 404, originalErr)
}

// NewConflictError 创建冲突错误
func NewConflictError(message string, originalErr error) *AppError {
	return NewAppError(ConflictError, message, 409, originalErr)
}

// NewInternalError 创建内部错误
func NewInternalError(message string, originalErr error) *AppError {
	return NewAppError(InternalError, message, 500, originalErr)
}

// NewExternalError 创建外部错误
func NewExternalError(message string, originalErr error) *AppError {
	return NewAppError(ExternalError, message, 502, originalErr)
}

// NewNetworkError 创建网络错误
func NewNetworkError(message string, originalErr error) *AppError {
	return NewAppError(NetworkError, message, 502, originalErr)
}

// NewTimeoutError 创建超时错误
func NewTimeoutError(message string, originalErr error) *AppError {
	return NewAppError(TimeoutError, message, 408, originalErr)
}

// NewAuthError 创建认证错误
func NewAuthError(message string, originalErr error) *AppError {
	return NewAppError(AuthError, message, 401, originalErr)
}

// NewPermissionError 创建权限错误
func NewPermissionError(message string, originalErr error) *AppError {
	return NewAppError(PermissionError, message, 403, originalErr)
}

// ErrorHandler 错误处理器
type ErrorHandler struct {
	logger *Logger
}

// NewErrorHandler 创建新的错误处理器
func NewErrorHandler(logger *Logger) *ErrorHandler {
	return &ErrorHandler{
		logger: logger,
	}
}

// Handle 处理错误
func (h *ErrorHandler) Handle(err error) {
	if err == nil {
		return
	}

	if appErr, ok := err.(*AppError); ok {
		// 记录应用程序错误
		h.logAppError(appErr)
	} else {
		// 记录普通错误
		if h.logger != nil {
			h.logger.Error("Unhandled error: %v", err)
		} else {
			Error("Unhandled error: %v", err)
		}
	}
}

// logAppError 记录应用程序错误
func (h *ErrorHandler) logAppError(appErr *AppError) {
	logMessage := fmt.Sprintf("[%s] %s", appErr.Type, appErr.Message)
	if appErr.Details != "" {
		logMessage += fmt.Sprintf(" - Details: %s", appErr.Details)
	}
	if appErr.OriginalErr != nil {
		logMessage += fmt.Sprintf(" - Original: %v", appErr.OriginalErr)
	}
	if appErr.File != "" && appErr.Line > 0 {
		logMessage += fmt.Sprintf(" - Location: %s:%d", appErr.File, appErr.Line)
	}

	if h.logger != nil {
		switch appErr.Type {
		case ValidationError, NotFoundError:
			h.logger.Warn(logMessage)
		case DatabaseError, InternalError, NetworkError, TimeoutError:
			h.logger.Error(logMessage)
		default:
			h.logger.Error(logMessage)
		}
	} else {
		switch appErr.Type {
		case ValidationError, NotFoundError:
			Warn(logMessage)
		case DatabaseError, InternalError, NetworkError, TimeoutError:
			Error(logMessage)
		default:
			Error(logMessage)
		}
	}
}

// RecoverPanic 恢复panic并转换为错误
func RecoverPanic() error {
	if r := recover(); r != nil {
		// 获取堆栈信息
		buf := make([]byte, 1024)
		n := runtime.Stack(buf, false)
		stackTrace := string(buf[:n])

		// 创建panic错误
		panicErr := NewInternalError(
			fmt.Sprintf("Panic recovered: %v", r),
			nil,
		).WithDetails(stackTrace)

		// 记录panic
		Error("Panic recovered: %v\nStack trace:\n%s", r, stackTrace)

		return panicErr
	}
	return nil
}

// WrapError 包装错误
func WrapError(err error, message string) error {
	if err == nil {
		return nil
	}

	if appErr, ok := err.(*AppError); ok {
		// 如果已经是AppError，添加更多上下文
		appErr.Message = message + ": " + appErr.Message
		return appErr
	}

	// 创建新的AppError
	return NewInternalError(message, err)
}

// IsErrorType 检查错误类型
func IsErrorType(err error, errType ErrorType) bool {
	if appErr, ok := err.(*AppError); ok {
		return appErr.Type == errType
	}
	return false
}

// GetErrorCode 获取错误代码
func GetErrorCode(err error) int {
	if appErr, ok := err.(*AppError); ok {
		return appErr.Code
	}
	return 500 // 默认内部服务器错误
}

// 全局错误处理器
var GlobalErrorHandler *ErrorHandler

// InitGlobalErrorHandler 初始化全局错误处理器
func InitGlobalErrorHandler(logger *Logger) {
	GlobalErrorHandler = NewErrorHandler(logger)
}

// HandleError 全局错误处理函数
func HandleError(err error) {
	if GlobalErrorHandler != nil {
		GlobalErrorHandler.Handle(err)
	} else {
		// 如果没有全局处理器，直接记录错误
		Error("Error: %v", err)
	}
}
