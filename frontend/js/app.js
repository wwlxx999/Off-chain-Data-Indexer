// 全局配置
const CONFIG = {
    API_BASE_URL: '/api/v1',
    ITEMS_PER_PAGE: 10,
    REFRESH_INTERVAL: 10000, // 10秒
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
let refreshInterval;
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
        return this.request('/stats/summary');
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
        return this.request('/stats/summary');
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

    static async getUSDTMarketData() {
        return this.request('/market/usdt');
    }

    static async getDetailedUSDTMarketData() {
        return this.request('/market/usdt/detailed');
    }

    static async getTronChainStatus() {
        return this.request('/sync/health');
    }
}

// 数据管理器
class DataManager {
    static async loadOverviewData() {
        try {
            // 只使用统计API加载数据，避免数据冲突
            // 统计API已经包含了从链上获取的最新区块号
            this.loadAdditionalData();
            
            // 加载市场数据
            this.loadMarketData();
            
        } catch (error) {
            console.error('Load overview error:', error);
            // 设置默认值但不显示错误消息，避免用户体验差
            this.setDefaultValues();
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
        // 使用重试机制获取统计数据
        try {
            await Utils.retryWithDelay(async () => {
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
            }, 3, 2000, '获取统计数据');
        } catch (error) {
            console.error('获取统计数据最终失败:', error.message);
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
        try {
            await Utils.retryWithDelay(async () => {
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
            }, 3, 2000, '获取市场数据');
        } catch (error) {
            console.error('获取市场数据最终失败:', error.message);
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
            const data = response ? (response.success ? response.data : response) : [];
            
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
                        ]
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom'
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    return `${context.label}: ${context.parsed.toLocaleString()} 笔`;
                                }
                            }
                        }
                    }
                }
            });
        } catch (error) {
            Utils.showError('加载交易量分布图表失败');
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
            
            // 加载初始数据（使用更安全的方式）
            await this.loadInitialDataSafely();
            
            // 初始化图表（可选，失败不影响主要功能）
            this.initChartsAsync();
            
            // 设置自动刷新
            this.startAutoRefresh();
            
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

    static async loadInitialData() {
        await Promise.all([
            DataManager.loadOverviewData(),
            DataManager.loadTransfersData(),
            DataManager.loadSystemStatus()
        ]);
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
        
        refreshInterval = setInterval(() => {
            // 只在页面可见时刷新数据
            if (!document.hidden && isInitialized && !isLoading) {
                try {
                    DataManager.loadOverviewData().catch(err => 
                        console.warn('自动刷新概览数据失败:', err.message)
                    );
                    DataManager.loadSystemStatus().catch(err => 
                        console.warn('自动刷新系统状态失败:', err.message)
                    );
                } catch (error) {
                    console.warn('自动刷新出错:', error.message);
                }
            }
        }, CONFIG.REFRESH_INTERVAL);
        
        console.log('自动刷新已启动，间隔:', CONFIG.REFRESH_INTERVAL / 1000, '秒');
    }

    static stopAutoRefresh() {
        if (refreshInterval) {
            clearInterval(refreshInterval);
            refreshInterval = null;
            console.log('自动刷新已停止');
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
        // 页面重新可见时，刷新数据
        console.log('页面重新可见，刷新数据');
        DataManager.loadOverviewData().catch(err => 
            console.warn('页面可见时刷新数据失败:', err.message)
        );
    }
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