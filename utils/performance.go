package utils

import (
	"runtime"
	"strings"
	"sync"
	"time"
)

// BytePool 字节切片池，减少内存分配
type BytePool struct {
	pool sync.Pool
	size int
}

// NewBytePool 创建新的字节池
func NewBytePool(size int) *BytePool {
	return &BytePool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, size)
			},
		},
		size: size,
	}
}

// Get 获取字节切片
func (p *BytePool) Get() []byte {
	return p.pool.Get().([]byte)[:0]
}

// Put 归还字节切片
func (p *BytePool) Put(b []byte) {
	if cap(b) >= p.size {
		p.pool.Put(b)
	}
}

// StringBuilderPool 字符串构建器池
type StringBuilderPool struct {
	pool sync.Pool
}

// NewStringBuilderPool 创建字符串构建器池
func NewStringBuilderPool() *StringBuilderPool {
	return &StringBuilderPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &strings.Builder{}
			},
		},
	}
}

// Get 获取字符串构建器
func (p *StringBuilderPool) Get() *strings.Builder {
	return p.pool.Get().(*strings.Builder)
}

// Put 归还字符串构建器
func (p *StringBuilderPool) Put(sb *strings.Builder) {
	sb.Reset()
	p.pool.Put(sb)
}

// WorkerPool 工作池，控制并发数量
type WorkerPool struct {
	workerCount int
	jobQueue    chan func()
	wg          sync.WaitGroup
	quit        chan bool
}

// NewWorkerPool 创建工作池
func NewWorkerPool(workerCount, queueSize int) *WorkerPool {
	return &WorkerPool{
		workerCount: workerCount,
		jobQueue:    make(chan func(), queueSize),
		quit:        make(chan bool),
	}
}

// Start 启动工作池
func (p *WorkerPool) Start() {
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// Stop 停止工作池
func (p *WorkerPool) Stop() {
	close(p.quit)
	p.wg.Wait()
	close(p.jobQueue)
}

// Submit 提交任务
func (p *WorkerPool) Submit(job func()) bool {
	select {
	case p.jobQueue <- job:
		return true
	default:
		return false // 队列已满
	}
}

// worker 工作协程
func (p *WorkerPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case job := <-p.jobQueue:
			if job != nil {
				job()
			}
		case <-p.quit:
			return
		}
	}
}

// MemoryStats 内存统计
type MemoryStats struct {
	Alloc        uint64 // 当前分配的内存
	TotalAlloc   uint64 // 总分配的内存
	Sys          uint64 // 系统内存
	NumGC        uint32 // GC次数
	PauseTotalNs uint64 // GC暂停总时间
}

// GetMemoryStats 获取内存统计信息
func GetMemoryStats() MemoryStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return MemoryStats{
		Alloc:        m.Alloc,
		TotalAlloc:   m.TotalAlloc,
		Sys:          m.Sys,
		NumGC:        m.NumGC,
		PauseTotalNs: m.PauseTotalNs,
	}
}

// ForceGC 强制执行垃圾回收
func ForceGC() {
	runtime.GC()
}

// SetGCPercent 设置GC触发百分比
func SetGCPercent(percent int) int {
	return runtime.GOMAXPROCS(percent)
}

// BatchProcessor 批处理器，减少频繁的小操作
type BatchProcessor struct {
	batchSize    int
	flushTimeout time.Duration
	processFunc  func([]interface{})
	batch        []interface{}
	mutex        sync.Mutex
	timer        *time.Timer
	stopped      bool
}

// NewBatchProcessor 创建批处理器
func NewBatchProcessor(batchSize int, flushTimeout time.Duration, processFunc func([]interface{})) *BatchProcessor {
	bp := &BatchProcessor{
		batchSize:    batchSize,
		flushTimeout: flushTimeout,
		processFunc:  processFunc,
		batch:        make([]interface{}, 0, batchSize),
	}
	bp.resetTimer()
	return bp
}

// Add 添加项目到批处理器
func (bp *BatchProcessor) Add(item interface{}) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	if bp.stopped {
		return
	}

	bp.batch = append(bp.batch, item)
	if len(bp.batch) >= bp.batchSize {
		bp.flush()
	}
}

// Flush 立即处理当前批次
func (bp *BatchProcessor) Flush() {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	bp.flush()
}

// Stop 停止批处理器
func (bp *BatchProcessor) Stop() {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	bp.stopped = true
	bp.flush()
	if bp.timer != nil {
		bp.timer.Stop()
	}
}

// flush 内部刷新方法（需要持有锁）
func (bp *BatchProcessor) flush() {
	if len(bp.batch) > 0 {
		batchCopy := make([]interface{}, len(bp.batch))
		copy(batchCopy, bp.batch)
		bp.batch = bp.batch[:0]
		
		// 异步处理，避免阻塞
		go bp.processFunc(batchCopy)
	}
	bp.resetTimer()
}

// resetTimer 重置定时器
func (bp *BatchProcessor) resetTimer() {
	if bp.timer != nil {
		bp.timer.Stop()
	}
	bp.timer = time.AfterFunc(bp.flushTimeout, func() {
		bp.Flush()
	})
}

// ConcurrentMap 并发安全的映射
type ConcurrentMap struct {
	shards []*mapShard
	count  int
}

type mapShard struct {
	mutex sync.RWMutex
	data  map[string]interface{}
}

// NewConcurrentMap 创建并发映射
func NewConcurrentMap(shardCount int) *ConcurrentMap {
	cm := &ConcurrentMap{
		shards: make([]*mapShard, shardCount),
		count:  shardCount,
	}
	for i := 0; i < shardCount; i++ {
		cm.shards[i] = &mapShard{
			data: make(map[string]interface{}),
		}
	}
	return cm
}

// getShard 获取分片
func (cm *ConcurrentMap) getShard(key string) *mapShard {
	hash := fnv32(key)
	return cm.shards[hash%uint32(cm.count)]
}

// Set 设置值
func (cm *ConcurrentMap) Set(key string, value interface{}) {
	shard := cm.getShard(key)
	shard.mutex.Lock()
	shard.data[key] = value
	shard.mutex.Unlock()
}

// Get 获取值
func (cm *ConcurrentMap) Get(key string) (interface{}, bool) {
	shard := cm.getShard(key)
	shard.mutex.RLock()
	value, ok := shard.data[key]
	shard.mutex.RUnlock()
	return value, ok
}

// Delete 删除值
func (cm *ConcurrentMap) Delete(key string) {
	shard := cm.getShard(key)
	shard.mutex.Lock()
	delete(shard.data, key)
	shard.mutex.Unlock()
}

// fnv32 简单的哈希函数
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return hash
}

// PerformanceMonitor 性能监控器
type PerformanceMonitor struct {
	metrics map[string]*Metric
	mutex   sync.RWMutex
}

// Metric 性能指标
type Metric struct {
	Count    int64
	Total    time.Duration
	Min      time.Duration
	Max      time.Duration
	LastTime time.Time
}

// NewPerformanceMonitor 创建性能监控器
func NewPerformanceMonitor() *PerformanceMonitor {
	return &PerformanceMonitor{
		metrics: make(map[string]*Metric),
	}
}

// Record 记录性能指标
func (pm *PerformanceMonitor) Record(name string, duration time.Duration) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	metric, exists := pm.metrics[name]
	if !exists {
		metric = &Metric{
			Min: duration,
			Max: duration,
		}
		pm.metrics[name] = metric
	}

	metric.Count++
	metric.Total += duration
	metric.LastTime = time.Now()

	if duration < metric.Min {
		metric.Min = duration
	}
	if duration > metric.Max {
		metric.Max = duration
	}
}

// GetMetrics 获取所有指标
func (pm *PerformanceMonitor) GetMetrics() map[string]*Metric {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	result := make(map[string]*Metric)
	for name, metric := range pm.metrics {
		result[name] = &Metric{
			Count:    metric.Count,
			Total:    metric.Total,
			Min:      metric.Min,
			Max:      metric.Max,
			LastTime: metric.LastTime,
		}
	}
	return result
}

// Average 计算平均值
func (m *Metric) Average() time.Duration {
	if m.Count == 0 {
		return 0
	}
	return m.Total / time.Duration(m.Count)
}