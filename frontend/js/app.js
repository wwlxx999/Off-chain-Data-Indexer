// 全局配置
const CONFIG = {
    API_BASE_URL: '/api/v1',
    ITEMS_PER_PAGE: 10,
    // 差异化刷新频率策略
    REFRESH_INTERVALS: {
        HIGH_FREQUENCY: 10000,   // 10秒 - 系统状态、最新区块
        MEDIUM_FREQUENCY: 30000, // 30秒 - 统计数据、转账概览
        LOW_FREQUENCY: 60000     // 60秒 - 市场数据、历史图表
    },
    CHART_COLORS: {
        primary: '#3b82f6',
        secondary: '#10b981',
        accent: '#f59e0b',
        danger: '#ef4444'
    }
};

// 全局状态
let currentPage = 1;
let totalPages = 1;
// 多频率刷新定时器管理
let refreshIntervals = {
    highFrequency: null,    // 高频数据定时器
    mediumFrequency: null,  // 中频数据定时器
    lowFrequency: null      // 低频数据定时器
};
let charts = {};
let isInitialized = false;
let isLoading = false;

// DOM 元素
const elements = {
    totalTransfers: document.getElementById('totalTransfers'),
    totalVolume: document.getElementById('totalVolume'),
    latestBlock: document.getElementById('latestBlock'),
    syncStatus: document.getElementById('syncStatus'),
    transfersTableBody: document.getElementById('transfersTableBody'),
    searchInput: document.getElementById('searchInput'),
    refreshBtn: document.getElementById('refreshBtn'),
    prevPage: document.getElementById('prevPage'),
    nextPage: document.getElementById('nextPage'),
    pageInfo: document.getElementById('pageInfo'),
    nodeStatus: document.getElementById('nodeStatus'),
    syncProgress: document.getElementById('syncProgress'),
    syncPercentage: document.getElementById('syncPercentage'),
    tpsValue: document.getElementById('tpsValue'),
    latencyValue: document.getElementById('latencyValue'),
    memoryValue: document.getElementById('memoryValue'),
    loadingOverlay: document.getElementById('loadingOverlay'),
    // 市场数据元素
    totalSupply: document.getElementById('totalSupply'),
    circulatingSupply: document.getElementById('circulatingSupply'),
    marketCap: document.getElementById('marketCap'),
    circulatingMarketCap: document.getElementById('circulatingMarketCap'),
    holders: document.getElementById('holders'),
    transfersYesterday: document.getElementById('transfersYesterday'),
    tradingVolumeYesterday: document.getElementById('tradingVolumeYesterday'),
    liquidity: document.getElementById('liquidity')
};

// 工具函数
class Utils {
    static formatNumber(num) {
        return Math.floor(num).toString();
    }

    static formatUSDT(amount) {
        const num = parseFloat(amount);
        
        // 如果是整数，直接返回整数
        if (num % 1 === 0) {
            return num.toString();
        }
        
        // 保留最多6位小数，去掉末尾的0
        const formatted = num.toFixed(6);
        return parseFloat(formatted).toString();
    }

    // 专门用于概览数据的格式化函数，只返回整数
    static formatOverviewNumber(num) {
        return Math.floor(parseFloat(num)).toString();
    }

    static formatOverviewUSDT(amount) {
        return Math.floor(parseFloat(amount)).toString();
    }

    // 格式化大数值，添加数量级单位
    static formatLargeNumber(num) {
        if (num >= 100000000) { // 1亿及以上
            return (num / 100000000).toFixed(1) + '亿';
        } else if (num >= 10000000) { // 1千万及以上
            return (num / 10000000).toFixed(1) + '千万';
        } else if (num >= 10000) { // 1万及以上
            return (num / 10000).toFixed(1) + '万';
        } else {
            return num.toLocaleString();
        }
    }

    static formatAddress(address, length = 8) {
        if (!address) return '-';
        return `${address.slice(0, length)}...${address.slice(-length)}`;
    }

    static formatTime(timestamp) {
        // 处理不同的时间戳格式
        if (!timestamp) return '-';
        
        let date;
        if (typeof timestamp === 'string') {
            // ISO 8601格式字符串
            date = new Date(timestamp);
        } else if (typeof timestamp === 'number') {
            // Unix时间戳（秒或毫秒）
            date = timestamp > 1000000000000 ? new Date(timestamp) : new Date(timestamp * 1000);
        } else {
            return 'Invalid Date';
        }
        
        // 检查日期是否有效
        if (isNaN(date.getTime())) {
            return 'Invalid Date';
        }
        
        return date.toLocaleString('zh-CN', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
    }

    static showLoading() {
        elements.loadingOverlay.classList.add('show');
    }

    static hideLoading() {
        elements.loadingOverlay.classList.remove('show');
    }

    static showError(message) {
        console.error('Error:', message);
        // 这里可以添加更多的错误显示逻辑
    }

    static async retryWithDelay(asyncFunction, maxRetries = 3, delay = 2000, context = '') {
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return await asyncFunction();
            } catch (error) {
                console.warn(`${context} 第${attempt}次尝试失败:`, error.message);
                
                if (attempt === maxRetries) {
                    throw error;
                }
                
                // 显示等待提示
                if (attempt === 1) {
                    this.showRetryMessage(context, attempt, maxRetries);
                }
                
                // 等待指定时间后重试
                await new Promise(resolve => setTimeout(resolve, delay * attempt));
            }
        }
    }

    static showRetryMessage(context, attempt, maxRetries) {
        console.info(`${context} 请稍等片刻，正在重新获取数据... (${attempt}/${maxRetries})`);
        // 可以在这里添加用户界面提示
    }
}

// API 服务
class ApiService {
    // 失败计数器
    static failureCount = {
        '/stats/summary': 0,
        '/market/usdt/detailed': 0,
        '/dashboard/summary': 0
    };
    
    // 增强的本地缓存系统
    static localCache = {
        '/stats/summary': null,
        '/market/usdt/detailed': null,
        '/dashboard/summary': null
    };
    
    // 缓存配置：不同数据类型的缓存有效期（毫秒）
    static cacheConfig = {
        '/stats/summary': {
            ttl: 30000,        // 30秒 - 统计数据变化较频繁
            maxAge: 300000,    // 5分钟 - 最大缓存时间
            staleWhileRevalidate: 60000  // 1分钟 - 过期后仍可使用的时间
        },
        '/market/usdt/detailed': {
            ttl: 60000,        // 1分钟 - 市场数据变化相对较慢
            maxAge: 600000,    // 10分钟 - 最大缓存时间
            staleWhileRevalidate: 120000 // 2分钟 - 过期后仍可使用的时间
        },
        '/dashboard/summary': {
            ttl: 20000,        // 20秒 - 综合数据，需要较新
            maxAge: 180000,    // 3分钟 - 最大缓存时间
            staleWhileRevalidate: 45000  // 45秒 - 过期后仍可使用的时间
        }
    };
    
    static async request(endpoint, options = {}) {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000); // 10秒超时
        
        try {
            const response = await fetch(`${CONFIG.API_BASE_URL}${endpoint}`, {
                mode: 'cors',
                credentials: 'omit',
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers
                },
                signal: controller.signal,
                ...options
            });

            clearTimeout(timeoutId);

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            return await response.json();
        } catch (error) {
            clearTimeout(timeoutId);
            if (error.name === 'AbortError') {
                console.warn(`API请求超时: ${endpoint}`);
                throw new Error('请求超时');
            }
            console.warn(`API请求失败: ${endpoint} - ${error.message}`);
            throw error;
        }
    }
    
    // 检查缓存是否有效
    static isCacheValid(endpoint, forceRefresh = false) {
        if (forceRefresh) return false;
        
        const cache = this.localCache[endpoint];
        const config = this.cacheConfig[endpoint];
        
        if (!cache || !config) return false;
        
        const now = Date.now();
        const age = now - cache.timestamp;
        
        // 检查是否在TTL内（新鲜数据）
        if (age <= config.ttl) {
            return 'fresh';
        }
        
        // 检查是否在stale-while-revalidate期间（可用但需要后台更新）
        if (age <= config.ttl + config.staleWhileRevalidate) {
            return 'stale';
        }
        
        // 检查是否超过最大缓存时间
        if (age > config.maxAge) {
            return false;
        }
        
        return 'expired';
    }
    
    // 带智能缓存的请求方法
    static async requestWithSmartCache(endpoint, options = {}) {
        const { forceRefresh = false, backgroundUpdate = true } = options;
        const cacheStatus = this.isCacheValid(endpoint, forceRefresh);
        
        // 如果缓存新鲜，直接返回
        if (cacheStatus === 'fresh') {
            console.log(`使用新鲜缓存: ${endpoint}`);
            return this.localCache[endpoint].data;
        }
        
        // 如果缓存过期但仍可用，返回缓存数据并在后台更新
        if (cacheStatus === 'stale' && backgroundUpdate) {
            console.log(`使用过期缓存并后台更新: ${endpoint}`);
            // 后台更新，不等待结果
            this.updateCacheInBackground(endpoint);
            return this.localCache[endpoint].data;
        }
        
        // 缓存无效或不存在，发起新请求
        return await this.requestWithRetryAndCache(endpoint);
    }
    
    // 后台更新缓存
    static async updateCacheInBackground(endpoint) {
        try {
            const result = await this.request(endpoint);
            this.updateCache(endpoint, result);
            console.log(`后台缓存更新成功: ${endpoint}`);
        } catch (error) {
            console.warn(`后台缓存更新失败: ${endpoint} - ${error.message}`);
        }
    }
    
    // 更新缓存
    static updateCache(endpoint, data) {
        this.localCache[endpoint] = {
            data: data,
            timestamp: Date.now(),
            etag: this.generateETag(data)
        };
    }
    
    // 生成简单的ETag
    static generateETag(data) {
        return btoa(JSON.stringify(data)).slice(0, 16);
    }
    
    // 带重试和缓存回退的请求方法（增强版）
    static async requestWithRetryAndCache(endpoint, maxRetries = 2) {
        let lastError = null;
        
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const result = await this.request(endpoint);
                // 请求成功，重置失败计数并更新缓存
                this.failureCount[endpoint] = 0;
                this.updateCache(endpoint, result);
                console.log(`API请求成功: ${endpoint}`);
                return result;
            } catch (error) {
                lastError = error;
                this.failureCount[endpoint] = (this.failureCount[endpoint] || 0) + 1;
                console.warn(`API请求失败 (${attempt}/${maxRetries}): ${endpoint} - ${error.message}`);
                
                if (attempt < maxRetries) {
                    // 等待后重试
                    await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
                }
            }
        }
        
        // 所有重试都失败了，检查是否可以使用过期缓存
        const cache = this.localCache[endpoint];
        const config = this.cacheConfig[endpoint];
        
        if (cache && config) {
            const age = Date.now() - cache.timestamp;
            // 如果在最大缓存时间内，使用过期缓存
            if (age <= config.maxAge) {
                console.warn(`API请求失败，使用过期缓存: ${endpoint} (缓存年龄: ${Math.round(age/1000)}秒)`);
                return cache.data;
            }
        }
        
        // 没有可用缓存，抛出错误
        throw lastError || new Error(`API请求失败: ${endpoint}`);
    }
    
    // 清理过期缓存
    static cleanExpiredCache() {
        const now = Date.now();
        Object.keys(this.localCache).forEach(endpoint => {
            const cache = this.localCache[endpoint];
            const config = this.cacheConfig[endpoint];
            
            if (cache && config) {
                const age = now - cache.timestamp;
                if (age > config.maxAge) {
                    console.log(`清理过期缓存: ${endpoint}`);
                    this.localCache[endpoint] = null;
                }
            }
        });
    }

    static async getTransfers(page = 1, limit = CONFIG.ITEMS_PER_PAGE, search = '') {
        const params = new URLSearchParams({
            page: page.toString(),
            page_size: limit.toString()
        });
        
        if (search) {
            params.append('search', search);
        }

        return this.request(`/transfers?${params}`);
    }

    static async getStats() {
        return this.requestWithSmartCache('/stats/summary');
    }

    static async getDashboardSummary() {
        return this.requestWithSmartCache('/dashboard/summary');
    }

    static async getSystemStatus() {
        return this.request('/sync/status');
    }

    static async getHealthCheck() {
        return this.request('/health');
    }

    static async getTransferTrend(hours = 24) {
        return this.request('/stats/hourly');
    }

    static async getHourlyTrend(date = null, timeSlot = 'all') {
        let params = '';
        if (date || timeSlot !== 'all') {
            const urlParams = new URLSearchParams();
            if (date) urlParams.append('date', date);
            if (timeSlot !== 'all') urlParams.append('timeSlot', timeSlot);
            params = `?${urlParams.toString()}`;
        }
        return this.request(`/stats/hourly-trend${params}`);
    }

    static async getVolumeDistribution() {
        return this.request('/stats/volume-distribution');
    }

    static async getSyncMetrics() {
        return this.request('/sync/metrics');
    }

    static async getIndexerStatus() {
        return this.request('/indexer/status');
    }

    static async getLatestBlock() {
        return this.request('/indexer/latest-block');
    }



    static async getDetailedUSDTMarketData() {
        return this.requestWithSmartCache('/market/usdt/detailed');
    }

    static async getTronChainStatus() {
        return this.request('/sync/health');
    }
}

// 数据管理器
class DataManager {
    // 使用合并API加载仪表板概览数据
    static async loadDashboardSummary() {
        try {
            const summaryData = await ApiService.getDashboardSummary();
            if (summaryData && summaryData.success) {
                const { stats, market, system, sync } = summaryData.data;
                
                // 更新统计数据
                if (stats) {
                    this.updateStatsDisplay(stats);
                }
                
                // 更新市场数据
                if (market) {
                    this.updateMarketDisplay(market);
                }
                
                // 更新系统状态
                if (system) {
                    this.updateSystemDisplay(system);
                }
                
                // 更新同步状态
                if (sync) {
                    this.updateSyncDisplay(sync);
                }
                
                return true;
            }
        } catch (error) {
            console.error('Load dashboard summary error:', error);
            // 降级到分别加载
            return this.loadOverviewDataFallback();
        }
        return false;
    }
    
    // 降级方案：分别加载数据
    static async loadOverviewDataFallback() {
        try {
            // 只使用统计API加载数据，避免数据冲突
            // 统计API已经包含了从链上获取的最新区块号
            this.loadAdditionalData();
            
            // 加载市场数据
            this.loadMarketData();
            
            return true;
        } catch (error) {
            console.error('Load overview fallback error:', error);
            // 设置默认值但不显示错误消息，避免用户体验差
            this.setDefaultValues();
            return false;
        }
    }
    
    // 兼容旧方法
    static async loadOverviewData() {
        return this.loadDashboardSummary();
    }
    
    // 更新统计数据显示
    static updateStatsDisplay(stats) {
        if (stats.total_transfers !== undefined) {
            elements.totalTransfers.textContent = Utils.formatOverviewNumber(stats.total_transfers);
        }
        if (stats.total_volume !== undefined) {
            elements.totalVolume.textContent = Utils.formatOverviewUSDT(stats.total_volume);
        }
        if (stats.latest_block !== undefined) {
            elements.latestBlock.textContent = Utils.formatOverviewNumber(stats.latest_block);
        }
    }
    
    // 更新市场数据显示
    static updateMarketDisplay(market) {
        if (market.total_supply !== undefined) {
            elements.totalSupply.textContent = Utils.formatLargeNumber(market.total_supply);
        }
        if (market.circulating_supply !== undefined) {
            elements.circulatingSupply.textContent = Utils.formatLargeNumber(market.circulating_supply);
        }
        if (market.market_cap !== undefined) {
            elements.marketCap.textContent = Utils.formatLargeNumber(market.market_cap);
        }
        if (market.circulating_market_cap !== undefined) {
            elements.circulatingMarketCap.textContent = Utils.formatLargeNumber(market.circulating_market_cap);
        }
        if (market.holders !== undefined) {
            elements.holders.textContent = Utils.formatLargeNumber(market.holders);
        }
        if (market.transfers_yesterday !== undefined) {
            elements.transfersYesterday.textContent = Utils.formatLargeNumber(market.transfers_yesterday);
        }
        if (market.trading_volume_yesterday !== undefined) {
            elements.tradingVolumeYesterday.textContent = Utils.formatLargeNumber(market.trading_volume_yesterday);
        }
        if (market.liquidity !== undefined) {
            elements.liquidity.textContent = Utils.formatLargeNumber(market.liquidity);
        }
    }
    
    // 更新系统状态显示
    static updateSystemDisplay(system) {
        if (system.status !== undefined) {
            elements.syncStatus.textContent = this.getSyncStatusText(system.status);
            elements.syncStatus.className = `status ${system.status}`;
        }
    }
    
    // 更新同步状态显示
    static updateSyncDisplay(sync) {
        if (sync.progress !== undefined) {
            this.updateSyncProgress(sync.progress);
        }
        if (sync.metrics !== undefined) {
            this.updatePerformanceMetrics(sync.metrics);
        }
    }
    
    static async loadLatestBlockData() {
        try {
            await Utils.retryWithDelay(async () => {
                const blockData = await ApiService.getLatestBlock();
                if (blockData && blockData.latest_block) {
                    elements.latestBlock.textContent = Utils.formatOverviewNumber(blockData.latest_block);
                } else {
                    throw new Error('最新区块数据无效');
                }
            }, 3, 2000, '获取最新区块');
        } catch (error) {
            console.error('获取最新区块最终失败:', error.message);
            elements.latestBlock.textContent = '获取失败';
        }
    }
    
    static async loadAdditionalData() {
        // 获取统计数据（内置重试和缓存回退）
        try {
            const statsResponse = await ApiService.getStats();
            if (statsResponse) {
                // 更新总转账数
                if (statsResponse.total_transfers !== undefined) {
                    elements.totalTransfers.textContent = Utils.formatOverviewNumber(statsResponse.total_transfers);
                }
                // 更新总交易量
                if (statsResponse.total_amount !== undefined) {
                    const amount = parseFloat(statsResponse.total_amount) || 0;
                    elements.totalVolume.textContent = Utils.formatOverviewUSDT(amount.toString());
                }
                // 使用统计API中的最新区块号（已从链上获取）
                if (statsResponse.latest_block_number !== undefined) {
                    elements.latestBlock.textContent = Utils.formatOverviewNumber(statsResponse.latest_block_number);
                }
            } else {
                throw new Error('统计数据响应无效');
            }
        } catch (error) {
            console.error('获取统计数据失败:', error.message);
            elements.totalTransfers.textContent = '获取失败';
            elements.totalVolume.textContent = '获取失败';
        }
        
        // 设置同步状态默认值
        elements.syncStatus.textContent = '同步中';
        elements.syncStatus.className = 'stat-value status-syncing';
        
        // 使用重试机制获取健康状态
        try {
            await Utils.retryWithDelay(async () => {
                const healthData = await ApiService.getHealthCheck();
                if (healthData && healthData.status) {
                    elements.syncStatus.textContent = this.getSyncStatusText(healthData.status);
                    elements.syncStatus.className = `stat-value status-${healthData.status}`;
                } else {
                    throw new Error('健康状态数据无效');
                }
            }, 3, 2000, '获取健康状态');
        } catch (error) {
            console.error('获取健康状态最终失败:', error.message);
            elements.syncStatus.textContent = '获取失败';
            elements.syncStatus.className = 'stat-value status-error';
        }
    }
    
    static setDefaultValues() {
        elements.totalTransfers.textContent = '获取失败';
        elements.totalVolume.textContent = '获取失败';
        elements.latestBlock.textContent = '获取失败';
        elements.syncStatus.textContent = '获取失败';
        elements.syncStatus.className = 'stat-value status-error';
    }
    
    static async loadMarketData() {
        // 获取市场数据（内置重试和缓存回退）
        try {
            const marketData = await ApiService.getDetailedUSDTMarketData();
            if (marketData) {
                // 更新总供应量
                if (marketData.total_supply !== undefined) {
                    elements.totalSupply.textContent = Utils.formatOverviewUSDT(marketData.total_supply.toString());
                }
                
                // 更新流通供应量
                if (marketData.circulating_supply !== undefined) {
                    elements.circulatingSupply.textContent = Utils.formatOverviewUSDT(marketData.circulating_supply.toString());
                }
                
                // 更新市值
                if (marketData.market_cap !== undefined) {
                    elements.marketCap.textContent = '$' + Utils.formatOverviewUSDT(marketData.market_cap.toString());
                }
                
                // 更新流通市值
                if (marketData.circulating_market_cap !== undefined) {
                    elements.circulatingMarketCap.textContent = '$' + Utils.formatOverviewUSDT(marketData.circulating_market_cap.toString());
                }
                
                // 更新持有者数量
                if (marketData.holders !== undefined) {
                    elements.holders.textContent = Utils.formatOverviewNumber(marketData.holders);
                }
                
                // 更新昨天转账数量
                if (marketData.yesterday_transfers !== undefined) {
                    elements.transfersYesterday.textContent = Utils.formatOverviewNumber(marketData.yesterday_transfers);
                }
                
                // 更新昨日交易量（使用yesterday_transfers字段）
                if (marketData.yesterday_transfers !== undefined) {
                    elements.tradingVolumeYesterday.textContent = Utils.formatOverviewNumber(marketData.yesterday_transfers);
                }
                
                // 流动性暂时显示为市值的一部分
                if (marketData.market_cap !== undefined) {
                    const liquidity = marketData.market_cap * 0.1; // 假设流动性为市值的10%
                    elements.liquidity.textContent = '$' + Utils.formatOverviewUSDT(liquidity.toString());
                }
            } else {
                throw new Error('市场数据响应无效');
            }
        } catch (error) {
            console.error('获取市场数据失败:', error.message);
            // 设置失败提示
            elements.totalSupply.textContent = '获取失败';
            elements.circulatingSupply.textContent = '获取失败';
            elements.marketCap.textContent = '获取失败';
            elements.circulatingMarketCap.textContent = '获取失败';
            elements.holders.textContent = '获取失败';
            elements.transfersYesterday.textContent = '获取失败';
            elements.tradingVolumeYesterday.textContent = '获取失败';
            elements.liquidity.textContent = '获取失败';
        }
    }
    
    // loadStatsData函数已移除，避免与loadAdditionalData冲突
    // 所有统计数据现在统一通过loadAdditionalData函数获取

    static async loadTransfersData(page = 1, search = '') {
        try {
            Utils.showLoading();
            const response = await ApiService.getTransfers(page, CONFIG.ITEMS_PER_PAGE, search);
            
            // 适配后端API响应格式
            if (response && (response.transfers || response.data)) {
                this.renderTransfersTable(response.transfers || response.data.transfers || response.data || []);
                const pagination = {
                    current_page: page,
                    total_pages: response.total_pages || response.data?.total_pages || Math.ceil((response.total || response.data?.total || 0) / CONFIG.ITEMS_PER_PAGE),
                    total_count: response.total || response.data?.total || 0
                };
                this.updatePagination(pagination);
            } else {
                Utils.showError('加载转账数据失败');
                this.renderTransfersTable([]);
            }
        } catch (error) {
            console.error('Load transfers error:', error);
            Utils.showError('加载转账数据失败');
            this.renderTransfersTable([]);
        } finally {
            Utils.hideLoading();
        }
    }

    static async loadSystemStatus() {
        try {
            const response = await ApiService.getSystemStatus();
            
            // 适配后端API响应格式
            if (response) {
                const data = response.success ? response.data : response;
                this.renderNodeStatus(data.nodes || []);
                this.updateSyncProgress(data.sync_progress || {});
                this.updatePerformanceMetrics(data.metrics || {});
            }
        } catch (error) {
            Utils.showError('加载系统状态失败');
        }
    }

    static renderTransfersTable(transfers) {
        const tbody = elements.transfersTableBody;
        tbody.innerHTML = '';

        if (transfers.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="7" style="text-align: center; padding: 40px; color: #666;">
                        <i class="fas fa-inbox" style="font-size: 2rem; margin-bottom: 10px; display: block;"></i>
                        暂无数据
                    </td>
                </tr>
            `;
            return;
        }

        transfers.forEach(transfer => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>
                    <a href="https://tronscan.org/#/transaction/${transfer.tx_hash}" target="_blank" class="tx-link">
                        ${Utils.formatAddress(transfer.tx_hash, 10)}
                    </a>
                </td>
                <td>${Utils.formatAddress(transfer.from_address)}</td>
                <td>${Utils.formatAddress(transfer.to_address)}</td>
                <td class="amount">${Utils.formatUSDT(transfer.amount)}</td>
                <td>${Utils.formatNumber(transfer.block_number)}</td>
                <td>${Utils.formatTime(transfer.timestamp)}</td>
                <td>
                    <span class="status-badge status-${transfer.status || 'confirmed'}">
                        ${this.getStatusText(transfer.status || 'confirmed')}
                    </span>
                </td>
            `;
            tbody.appendChild(row);
        });
    }

    static updatePagination(pagination) {
        currentPage = pagination.current_page || 1;
        totalPages = pagination.total_pages || 1;
        
        elements.pageInfo.textContent = `第 ${currentPage} 页，共 ${totalPages} 页`;
        elements.prevPage.disabled = currentPage <= 1;
        elements.nextPage.disabled = currentPage >= totalPages;
    }

    static renderNodeStatus(nodes) {
        const container = elements.nodeStatus;
        container.innerHTML = '';

        if (nodes.length === 0) {
            container.innerHTML = '<p style="color: #666;">暂无节点信息</p>';
            return;
        }

        nodes.forEach(node => {
            const nodeItem = document.createElement('div');
            nodeItem.className = 'node-item';
            nodeItem.innerHTML = `
                <span class="node-name">${node.name || node.url}</span>
                <div class="node-indicator ${node.status || 'offline'}"></div>
            `;
            container.appendChild(nodeItem);
        });
    }

    static updateSyncProgress(progress) {
        const percentage = progress.percentage || 0;
        elements.syncProgress.style.width = `${percentage}%`;
        elements.syncPercentage.textContent = `${percentage.toFixed(1)}%`;
    }

    static updatePerformanceMetrics(metrics) {
        elements.tpsValue.textContent = metrics.tps || '-';
        elements.latencyValue.textContent = metrics.latency ? `${metrics.latency}ms` : '-';
        elements.memoryValue.textContent = metrics.memory_usage ? `${metrics.memory_usage}%` : '-';
    }

    static getSyncStatusText(status) {
        const statusMap = {
            'syncing': '同步中',
            'synced': '已同步',
            'healthy': '链接正常',
            'error': '连接错误',
            'stopped': '已停止',
            'connecting': '连接中',
            'disconnected': '连接断开'
        };
        return statusMap[status] || '未知';
    }

    static getStatusText(status) {
        const statusMap = {
            'confirmed': '已确认',
            'pending': '待确认',
            'failed': '失败'
        };
        return statusMap[status] || status;
    }
}

// 图表管理器
class ChartManager {
    static async initCharts() {
        await this.initTransferTrendChart();
        await this.initVolumeDistributionChart();
    }

    static async initTransferTrendChart(selectedDate = '2025-09-06', timeSlot = 'all') {
        try {
            const response = await ApiService.getHourlyTrend(selectedDate, timeSlot);
            const ctx = document.getElementById('transferTrendChart').getContext('2d');
            
            // 适配后端API响应格式
            const data = response && response.data ? response.data : [];
            
            // 销毁现有图表
            if (charts.transferTrend) {
                charts.transferTrend.destroy();
            }
            
            charts.transferTrend = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: data.map(item => item.hour),
                    datasets: [{
                        label: '总交易金额 (USDT)',
                        data: data.map(item => Math.floor(parseFloat(item.total_amount) || 0)),
                        borderColor: '#667eea',
                        backgroundColor: 'rgba(102, 126, 234, 0.1)',
                        tension: 0.4,
                        fill: true,
                        pointBackgroundColor: '#667eea',
                        pointBorderColor: '#fff',
                        pointBorderWidth: 2,
                        pointRadius: 4
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            display: false
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    return `总交易金额: ${Math.floor(context.parsed.y).toLocaleString()} USDT`;
                                }
                            }
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            grid: {
                                color: 'rgba(0,0,0,0.1)'
                            },
                            ticks: {
                                callback: function(value) {
                                    return Utils.formatLargeNumber(Math.floor(value)) + ' USDT';
                                }
                            }
                        },
                        x: {
                            grid: {
                                color: 'rgba(0,0,0,0.1)'
                            }
                        }
                    }
                }
            });
        } catch (error) {
            Utils.showError('加载转账趋势图表失败');
        }
    }

    static async initVolumeDistributionChart() {
        try {
            const response = await ApiService.getVolumeDistribution();
            const ctx = document.getElementById('volumeDistributionChart').getContext('2d');
            
            // 适配后端API响应格式
            const data = response && response.data ? response.data : [];
            const totalTransactions = response && response.total_transactions ? response.total_transactions : 0;
            
            // 销毁现有图表
            if (charts.volumeDistribution) {
                charts.volumeDistribution.destroy();
            }
            
            charts.volumeDistribution = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: data.map(item => item.range),
                    datasets: [{
                        data: data.map(item => item.count),
                        backgroundColor: [
                            '#667eea',
                            '#764ba2', 
                            '#f093fb',
                            '#f5576c',
                            '#4facfe',
                            '#00f2fe'
                        ],
                        borderWidth: 2,
                        borderColor: '#ffffff'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom',
                            labels: {
                                padding: 20,
                                usePointStyle: true,
                                font: {
                                    size: 12
                                }
                            }
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    const item = data[context.dataIndex];
                                    const count = item.count.toLocaleString();
                                    const percentage = item.percentage.toFixed(1);
                                    const totalAmount = parseFloat(item.total_amount).toLocaleString();
                                    return [
                                        `${context.label}`,
                                        `交易数量: ${count} 笔 (${percentage}%)`,
                                        `总金额: ${totalAmount} USDT`
                                    ];
                                }
                            },
                            backgroundColor: 'rgba(0, 0, 0, 0.8)',
                            titleColor: '#ffffff',
                            bodyColor: '#ffffff',
                            borderColor: '#667eea',
                            borderWidth: 1
                        },
                        title: {
                            display: true,
                            text: `交易量分布 (总计: ${totalTransactions.toLocaleString()} 笔)`,
                            font: {
                                size: 14,
                                weight: 'bold'
                            },
                            color: '#333333'
                        }
                    },
                    cutout: '60%',
                    animation: {
                        animateRotate: true,
                        duration: 1000
                    },
                    onClick: (event, elements) => {
                        if (elements.length > 0) {
                            const index = elements[0].index;
                            const item = data[index];
                            ChartManager.showVolumeDistributionDetail(item);
                        }
                    },
                    onHover: (event, elements) => {
                        event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
                    }
                }
            });
            
            // 更新洞察面板
            ChartManager.updateVolumeInsights(data, totalTransactions);
        } catch (error) {
            console.error('Volume distribution chart error:', error);
            Utils.showError('加载交易量分布图表失败');
        }
    }
    
    static updateVolumeInsights(data, totalTransactions) {
        try {
            // 总交易数
            document.getElementById('totalTransactions').textContent = totalTransactions.toLocaleString();
            
            // 主要交易范围（占比最高的）
            const dominantItem = data.reduce((max, item) => item.percentage > max.percentage ? item : max, data[0]);
            document.getElementById('dominantRange').textContent = dominantItem.range;
            
            // 更新主要交易范围tooltip内容
            const mediumTransactions = data.find(item => item.range.includes('100-1K'));
            const dominantRangeTooltipText = document.getElementById('dominantRangeTooltipText');
            if (dominantRangeTooltipText && mediumTransactions) {
                const mediumRatio = mediumTransactions.percentage.toFixed(1);
                dominantRangeTooltipText.textContent = `100-1K USDT 交易占总交易数的 ${mediumRatio}%（包含 ${mediumTransactions.count.toLocaleString()} 笔交易）`;
            }
            
            // 平均交易金额
            const totalAmount = data.reduce((sum, item) => sum + parseFloat(item.total_amount), 0);
            const avgAmount = totalAmount / totalTransactions;
            document.getElementById('avgTransactionSize').textContent = avgAmount.toFixed(0) + ' USDT';
            
            // 大额交易占比（10K以上）
            const largeTransactions = data.filter(item => 
                item.range.includes('10K') || item.range.includes('100K+')
            ).reduce((sum, item) => sum + item.count, 0);
            const largeRatio = (largeTransactions / totalTransactions * 100).toFixed(1);
            document.getElementById('largeTransactionRatio').textContent = largeRatio + '%';
            
            // 更新tooltip内容
            const tooltipText = document.getElementById('largeTransactionTooltipText');
            if (tooltipText) {
                tooltipText.textContent = `统计金额超过 10,000 USDT 以上的交易占总交易数的占比（包含 ${largeTransactions.toLocaleString()} 笔大额交易）`;
            }
        } catch (error) {
            console.error('Update volume insights error:', error);
        }
    }

    static showVolumeDistributionDetail(item) {
        const modal = document.createElement('div');
        modal.className = 'modal fade';
        modal.innerHTML = `
            <div class="modal-dialog modal-lg">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">交易量分布详情 - ${item.range}</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="row">
                            <div class="col-md-6">
                                <div class="card">
                                    <div class="card-body">
                                        <h6 class="card-title">交易统计</h6>
                                        <p class="card-text">
                                            <strong>交易数量:</strong> ${item.count.toLocaleString()} 笔<br>
                                            <strong>占比:</strong> ${item.percentage.toFixed(2)}%<br>
                                            <strong>总金额:</strong> ${parseFloat(item.total_amount).toLocaleString()} USDT
                                        </p>
                                    </div>
                                </div>
                            </div>
                            <div class="col-md-6">
                                <div class="card">
                                    <div class="card-body">
                                        <h6 class="card-title">平均值分析</h6>
                                        <p class="card-text">
                                            <strong>平均交易金额:</strong> ${(parseFloat(item.total_amount) / item.count).toFixed(2)} USDT<br>
                                            <strong>金额范围:</strong> ${item.range}<br>
                                            <strong>活跃度:</strong> ${item.percentage > 20 ? '高' : item.percentage > 10 ? '中' : '低'}
                                        </p>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div class="mt-3">
                            <div class="alert alert-info">
                                <strong>分析建议:</strong> 
                                ${ChartManager.getVolumeAnalysis(item)}
                            </div>
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">关闭</button>
                    </div>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        const bsModal = new bootstrap.Modal(modal);
        bsModal.show();
        
        modal.addEventListener('hidden.bs.modal', () => {
            document.body.removeChild(modal);
        });
    }

    static getVolumeAnalysis(item) {
        const percentage = item.percentage;
        const avgAmount = parseFloat(item.total_amount) / item.count;
        
        if (item.range.includes('0-100')) {
            return '小额交易占主导，主要为日常转账和小额支付，用户活跃度高。';
        } else if (item.range.includes('100-1K')) {
            return '中小额交易，可能包括商业支付和个人转账，是平台的主要交易类型。';
        } else if (item.range.includes('1K-10K')) {
            return '中等金额交易，通常涉及商业往来或投资活动，需要关注合规性。';
        } else if (item.range.includes('10K-100K')) {
            return '大额交易，可能涉及企业间转账或大宗交易，建议加强监控。';
        } else {
            return '超大额交易，需要特别关注反洗钱和合规要求，建议人工审核。';
        }
    }

    static updateCharts() {
        this.initTransferTrendChart();
        this.initVolumeDistributionChart();
    }
}

// 事件处理器
class EventHandler {
    static init() {
        // 导航链接点击
        document.querySelectorAll('.nav-link').forEach(link => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                const target = link.getAttribute('href');
                this.switchSection(target);
                this.updateActiveNav(link);
            });
        });

        // 板块选择器事件
        const selectorTabs = document.querySelectorAll('.selector-tab');
        selectorTabs.forEach(tab => {
            tab.addEventListener('click', () => {
                const targetPanel = tab.getAttribute('data-tab');
                App.switchAnalyticsPanel(targetPanel);
            });
        });

        // 刷新按钮
        elements.refreshBtn.addEventListener('click', () => {
            this.refreshData();
        });

        // 搜索输入
        elements.searchInput.addEventListener('input', this.debounce(() => {
            currentPage = 1;
            DataManager.loadTransfersData(currentPage, elements.searchInput.value);
        }, 500));

        // 分页按钮
        elements.prevPage.addEventListener('click', () => {
            if (currentPage > 1) {
                currentPage--;
                DataManager.loadTransfersData(currentPage, elements.searchInput.value);
            }
        });

        elements.nextPage.addEventListener('click', () => {
            if (currentPage < totalPages) {
                currentPage++;
                DataManager.loadTransfersData(currentPage, elements.searchInput.value);
            }
        });

        // 趋势图表日期选择器
        const trendDatePicker = document.getElementById('trendDatePicker');
        if (trendDatePicker) {
            // 设置默认值为有数据的日期 (2025-09-06)
            trendDatePicker.value = '2025-09-06';
            
            trendDatePicker.addEventListener('change', () => {
                const selectedDate = trendDatePicker.value;
                const timeSlotPicker = document.getElementById('timeSlotPicker');
                const selectedTimeSlot = timeSlotPicker ? timeSlotPicker.value : 'all';
                ChartManager.initTransferTrendChart(selectedDate, selectedTimeSlot);
            });
        }

        // 时段选择器
        const timeSlotPicker = document.getElementById('timeSlotPicker');
        if (timeSlotPicker) {
            timeSlotPicker.addEventListener('change', () => {
                const selectedDate = trendDatePicker ? trendDatePicker.value : null;
                const selectedTimeSlot = timeSlotPicker.value;
                ChartManager.initTransferTrendChart(selectedDate, selectedTimeSlot);
            });
        }

        // 趋势图表刷新按钮
        const refreshTrendBtn = document.getElementById('refreshTrendBtn');
        if (refreshTrendBtn) {
            refreshTrendBtn.addEventListener('click', () => {
                const selectedDate = trendDatePicker ? trendDatePicker.value : null;
                const selectedTimeSlot = timeSlotPicker ? timeSlotPicker.value : 'all';
                ChartManager.initTransferTrendChart(selectedDate, selectedTimeSlot);
            });
        }

        // 交易量分布图表刷新按钮
        const refreshVolumeBtn = document.getElementById('refreshVolumeBtn');
        if (refreshVolumeBtn) {
            refreshVolumeBtn.addEventListener('click', () => {
                ChartManager.initVolumeDistributionChart();
            });
        }

        // 手动同步按钮
        const manualSyncBtn = document.getElementById('manualSyncBtn');
        if (manualSyncBtn) {
            manualSyncBtn.addEventListener('click', async () => {
                await this.performManualSync();
            });
        }
    }

    static switchSection(target) {
        document.querySelectorAll('.section').forEach(section => {
            section.style.display = 'none';
        });
        
        const targetSection = document.querySelector(target);
        if (targetSection) {
            targetSection.style.display = 'block';
        }
    }

    static updateActiveNav(activeLink) {
        document.querySelectorAll('.nav-link').forEach(link => {
            link.classList.remove('active');
        });
        activeLink.classList.add('active');
    }

    static refreshData() {
        DataManager.loadOverviewData();
        DataManager.loadTransfersData(currentPage, elements.searchInput.value);
        DataManager.loadSystemStatus();
        ChartManager.updateCharts();
    }

    static debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }

    static async performManualSync() {
        const manualSyncBtn = document.getElementById('manualSyncBtn');
        if (!manualSyncBtn) return;

        // 禁用按钮并显示加载状态
        manualSyncBtn.disabled = true;
        const originalText = manualSyncBtn.innerHTML;
        manualSyncBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 同步中...';

        try {
            const response = await fetch(`${CONFIG.API_BASE_URL}/sync/manual`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            if (response.ok) {
                const result = await response.json();
                console.log('手动同步成功:', result);
                
                // 显示成功消息
                manualSyncBtn.innerHTML = '<i class="fas fa-check"></i> 同步完成';
                
                // 刷新数据
                setTimeout(() => {
                    UIManager.refreshData();
                }, 1000);
                
                // 2秒后恢复按钮状态
                setTimeout(() => {
                    manualSyncBtn.innerHTML = originalText;
                    manualSyncBtn.disabled = false;
                }, 2000);
            } else {
                throw new Error(`同步失败: ${response.status}`);
            }
        } catch (error) {
            console.error('手动同步失败:', error);
            
            // 显示错误消息
            manualSyncBtn.innerHTML = '<i class="fas fa-exclamation-triangle"></i> 同步失败';
            
            // 2秒后恢复按钮状态
            setTimeout(() => {
                manualSyncBtn.innerHTML = originalText;
                manualSyncBtn.disabled = false;
            }, 2000);
        }
    }
}

// 应用初始化
class App {
    static async init() {
        // 防止重复初始化
        if (isInitialized || isLoading) {
            console.log('应用已初始化或正在初始化中');
            return;
        }
        
        isLoading = true;
        Utils.showLoading();
        
        try {
            // 初始化事件处理器
            EventHandler.init();
            
            // 初始化分析选择器
            this.initAnalyticsSelector();
            
            // 加载初始数据（使用更安全的方式）
            await this.loadInitialDataSafely();
            
            // 初始化图表（可选，失败不影响主要功能）
            this.initChartsAsync();
            
            // 设置快速初始刷新
            this.startFastInitialRefresh();
            
            isInitialized = true;
            console.log('应用初始化完成');
        } catch (error) {
            console.error('App initialization error:', error);
            // 即使初始化失败，也要设置基本功能
            DataManager.setDefaultValues();
        } finally {
            isLoading = false;
            Utils.hideLoading();
        }
    }


    
    static async loadInitialDataSafely() {
        // 优先加载最重要的数据
        try {
            await DataManager.loadOverviewData();
        } catch (error) {
            console.warn('加载概览数据失败:', error.message);
            DataManager.setDefaultValues();
        }
        
        // 异步加载其他数据，不阻塞主流程
        setTimeout(() => {
            DataManager.loadTransfersData().catch(err => 
                console.warn('加载转账数据失败:', err.message)
            );
            DataManager.loadSystemStatus().catch(err => 
                console.warn('加载系统状态失败:', err.message)
            );
        }, 100);
    }
    
    static startFastInitialRefresh() {
        // 首次加载时使用更短的刷新间隔
        const fastIntervals = {
            highFrequency: 3000,   // 3秒 - 系统状态、最新区块
            mediumFrequency: 5000, // 5秒 - 统计数据、转账概览
            lowFrequency: 8000     // 8秒 - 市场数据、历史图表
        };
        
        // 记录各频率数据的首次成功状态
        const successFlags = {
            highFrequency: false,
            mediumFrequency: false,
            lowFrequency: false
        };
        
        // 检查是否所有频率都已成功，如果是则切换到正常间隔
        const checkAndSwitchToNormal = () => {
            if (successFlags.highFrequency && successFlags.mediumFrequency && successFlags.lowFrequency) {
                console.log('所有频率数据首次请求成功，切换到正常刷新间隔');
                this.stopAutoRefresh();
                this.startAutoRefresh();
                return true;
            }
            return false;
        };
        
        console.log('启动快速初始刷新定时器');
        
        // 高频数据刷新
        refreshIntervals.highFrequency = setInterval(async () => {
            if (!document.hidden && isInitialized && !isLoading) {
                try {
                    await this.refreshHighFrequencyData();
                    if (!successFlags.highFrequency) {
                        successFlags.highFrequency = true;
                        console.log('高频数据首次刷新成功');
                        checkAndSwitchToNormal();
                    }
                } catch (error) {
                    console.warn('快速刷新高频数据失败:', error.message);
                }
            }
        }, fastIntervals.highFrequency);
        
        // 中频数据刷新
        refreshIntervals.mediumFrequency = setInterval(async () => {
            if (!document.hidden && isInitialized && !isLoading) {
                try {
                    await this.refreshMediumFrequencyData();
                    if (!successFlags.mediumFrequency) {
                        successFlags.mediumFrequency = true;
                        console.log('中频数据首次刷新成功');
                        checkAndSwitchToNormal();
                    }
                } catch (error) {
                    console.warn('快速刷新中频数据失败:', error.message);
                }
            }
        }, fastIntervals.mediumFrequency);
        
        // 低频数据刷新
        refreshIntervals.lowFrequency = setInterval(async () => {
            if (!document.hidden && isInitialized && !isLoading) {
                try {
                    await DataManager.loadMarketData();
                    await DataManager.loadAdditionalData();
                    await ChartManager.updateCharts();
                    if (!successFlags.lowFrequency) {
                        successFlags.lowFrequency = true;
                        console.log('低频数据首次刷新成功');
                        checkAndSwitchToNormal();
                    }
                } catch (error) {
                    console.warn('快速刷新低频数据失败:', error.message);
                }
            }
        }, fastIntervals.lowFrequency);
        
        // 备用机制：最多30秒后强制切换到正常刷新间隔
        setTimeout(() => {
            if (!successFlags.highFrequency || !successFlags.mediumFrequency || !successFlags.lowFrequency) {
                console.log('30秒超时，强制切换到正常刷新间隔');
                this.stopAutoRefresh();
                this.startAutoRefresh();
            }
        }, 30000);
    }
    
    static initChartsAsync() {
        // 异步初始化图表，不阻塞主流程
        setTimeout(async () => {
            try {
                await ChartManager.initCharts();
                console.log('图表初始化完成');
            } catch (error) {
                console.warn('图表初始化失败:', error.message);
            }
        }, 500);
    }

    static startAutoRefresh() {
        // 清除现有的刷新定时器
        this.stopAutoRefresh();
        
        // 高频数据刷新 (10秒) - 系统状态、最新区块
        refreshIntervals.highFrequency = setInterval(() => {
            if (!document.hidden && isInitialized && !isLoading) {
                try {
                    DataManager.loadSystemStatus().catch(err => 
                        console.warn('高频刷新系统状态失败:', err.message)
                    );
                    DataManager.loadLatestBlockData().catch(err => 
                        console.warn('高频刷新区块数据失败:', err.message)
                    );
                } catch (error) {
                    console.warn('高频刷新出错:', error.message);
                }
            }
        }, CONFIG.REFRESH_INTERVALS.HIGH_FREQUENCY);
        
        // 中频数据刷新 (30秒) - 统计数据、转账概览
        refreshIntervals.mediumFrequency = setInterval(() => {
            if (!document.hidden && isInitialized && !isLoading) {
                try {
                    DataManager.loadAdditionalData().catch(err => 
                        console.warn('中频刷新统计数据失败:', err.message)
                    );
                } catch (error) {
                    console.warn('中频刷新出错:', error.message);
                }
            }
        }, CONFIG.REFRESH_INTERVALS.MEDIUM_FREQUENCY);
        
        // 低频数据刷新 (60秒) - 市场数据、历史图表
        refreshIntervals.lowFrequency = setInterval(() => {
            if (!document.hidden && isInitialized && !isLoading) {
                try {
                    DataManager.loadMarketData().catch(err => 
                        console.warn('低频刷新市场数据失败:', err.message)
                    );
                    ChartManager.updateCharts().catch(err => 
                        console.warn('低频刷新图表失败:', err.message)
                    );
                } catch (error) {
                    console.warn('低频刷新出错:', error.message);
                }
            }
        }, CONFIG.REFRESH_INTERVALS.LOW_FREQUENCY);
        

        
        console.log('差异化自动刷新已启动:');
        console.log('- 高频数据(系统状态):', CONFIG.REFRESH_INTERVALS.HIGH_FREQUENCY / 1000, '秒');
        console.log('- 中频数据(统计数据):', CONFIG.REFRESH_INTERVALS.MEDIUM_FREQUENCY / 1000, '秒');
        console.log('- 低频数据(市场数据):', CONFIG.REFRESH_INTERVALS.LOW_FREQUENCY / 1000, '秒');
    }

    static stopAutoRefresh() {
        // 清除所有频率的定时器
        Object.keys(refreshIntervals).forEach(key => {
            if (refreshIntervals[key]) {
                clearInterval(refreshIntervals[key]);
                refreshIntervals[key] = null;
            }
        });
        

        
        console.log('所有自动刷新定时器已停止');
    }

    // 暂停自动刷新（页面隐藏时）
    static pauseAutoRefresh() {
        this.stopAutoRefresh();
        this.isPaused = true;
    }

    // 恢复自动刷新（页面可见时）
    static resumeAutoRefresh() {
        if (this.isPaused && isInitialized) {
            this.startAutoRefresh();
            this.isPaused = false;
        }
    }

    // 处理页面可见性变化
    static handleVisibilityChange() {
        const now = Date.now();
        const timeSinceLastUpdate = now - (this.lastUpdateTime || 0);
        
        // 如果超过30秒未更新，立即刷新所有数据
        if (timeSinceLastUpdate > 30000) {
            console.log('长时间未更新，立即刷新所有数据');
            this.forceRefreshAll();
        } else {
            // 否则只刷新高频数据
            console.log('刷新高频数据');
            this.refreshHighFrequencyData();
        }
        
        // 恢复自动刷新
        this.resumeAutoRefresh();
    }

    // 处理用户交互
    static handleUserInteraction() {
        const now = Date.now();
        
        // 防抖：避免频繁的用户交互触发过多刷新
        if (this.lastInteractionTime && (now - this.lastInteractionTime) < 5000) {
            return;
        }
        
        this.lastInteractionTime = now;
        
        // 用户交互时，刷新中频数据
        console.log('用户交互，刷新中频数据');
        this.refreshMediumFrequencyData();
    }

    // 处理网络重连
    static handleNetworkReconnect() {
        console.log('网络重连，强制刷新所有数据');
        this.forceRefreshAll();
    }

    // 强制刷新所有数据
    static async forceRefreshAll() {
        try {
            // 清理过期缓存
            ApiService.cleanExpiredCache();
            
            // 优先使用合并API，提高效率
            const dashboardSuccess = await DataManager.loadDashboardSummary();
            
            if (dashboardSuccess) {
                // 合并API成功，只需额外加载系统状态
                await DataManager.loadSystemStatus();
            } else {
                // 降级到分别加载所有数据
                await Promise.all([
                    DataManager.loadSystemStatus(),
                    DataManager.loadLatestBlockData(),
                    DataManager.loadAdditionalData(),
                    DataManager.loadMarketData()
                ]);
            }
            
            this.lastUpdateTime = Date.now();
            console.log('所有数据刷新完成');
        } catch (error) {
            console.warn('强制刷新数据失败:', error.message);
        }
    }

    // 刷新高频数据
    static async refreshHighFrequencyData() {
        try {
            await Promise.all([
                DataManager.loadSystemStatus(),
                DataManager.loadLatestBlockData()
            ]);
            console.log('高频数据刷新完成');
        } catch (error) {
            console.warn('高频数据刷新失败:', error.message);
        }
    }

    // 刷新中频数据 - 使用合并API优化
    static async refreshMediumFrequencyData() {
        try {
            // 优先使用合并API，降级到分别加载
            const success = await DataManager.loadDashboardSummary();
            if (!success) {
                // 降级方案
                await DataManager.loadAdditionalData();
            }
            console.log('中频数据刷新完成');
        } catch (error) {
            console.warn('中频数据刷新失败:', error.message);
        }
    }

    static switchAnalyticsPanel(panelName) {
        // 移除所有选择器的active状态
        const allTabs = document.querySelectorAll('.selector-tab');
        allTabs.forEach(tab => tab.classList.remove('active'));
        
        // 添加当前选择器的active状态
        const activeTab = document.querySelector(`[data-tab="${panelName}"]`);
        if (activeTab) {
            activeTab.classList.add('active');
        }
        
        // 隐藏所有面板
        const allPanels = document.querySelectorAll('.analytics-panel');
        allPanels.forEach(panel => panel.classList.remove('active'));
        
        // 显示目标面板
        const targetPanel = document.getElementById(`${panelName}-panel`);
        if (targetPanel) {
            targetPanel.classList.add('active');
            
            // 根据面板类型初始化相应的图表
            setTimeout(() => {
                if (panelName === 'volume-distribution') {
                    ChartManager.initVolumeDistributionChart();
                } else if (panelName === 'transfer-trend') {
                    const selectedDate = document.getElementById('trendDatePicker')?.value || '2025-09-06';
                    const timeSlot = document.getElementById('timeSlotPicker')?.value || 'all';
                    ChartManager.initTransferTrendChart(selectedDate, timeSlot);
                }
            }, 100);
        }
    }

    static initAnalyticsSelector() {
        // 设置默认选中交易量分布选择器
        const defaultTab = document.querySelector('[data-tab="volume-distribution"]');
        if (defaultTab) {
            defaultTab.classList.add('active');
        }
    }
}

// 页面加载完成后初始化应用
document.addEventListener('DOMContentLoaded', () => {
    App.init();
});

// 页面可见性变化时的处理
document.addEventListener('visibilitychange', () => {
    if (!document.hidden && isInitialized) {
        // 页面重新可见时，智能刷新数据
        console.log('页面重新可见，执行智能刷新');
        App.handleVisibilityChange();
    } else if (document.hidden) {
        // 页面隐藏时，暂停自动刷新以节省资源
        console.log('页面隐藏，暂停自动刷新');
        App.pauseAutoRefresh();
    }
});

// 用户交互事件监听
document.addEventListener('click', () => {
    App.handleUserInteraction();
});

document.addEventListener('keydown', () => {
    App.handleUserInteraction();
});

// 网络状态变化监听
window.addEventListener('online', () => {
    console.log('网络连接恢复，恢复自动刷新');
    if (isInitialized && !document.hidden) {
        App.resumeAutoRefresh();
        App.handleNetworkReconnect();
    }
});

window.addEventListener('offline', () => {
    console.log('网络连接断开，暂停自动刷新');
    App.pauseAutoRefresh();
});

// 页面卸载时清理资源
window.addEventListener('beforeunload', () => {
    App.stopAutoRefresh();
});

// 页面错误处理
window.addEventListener('error', (event) => {
    console.error('页面错误:', event.error);
    // 不显示错误消息给用户，避免影响体验
});

// 未处理的Promise错误
window.addEventListener('unhandledrejection', (event) => {
    console.warn('未处理的Promise错误:', event.reason);
    // 阻止错误显示在控制台
    event.preventDefault();
});