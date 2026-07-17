/**
 * Dashboard Module - 类实时效果
 *
 * 原理：
 * 1. 全量加载 Doris 数据（不过滤时间）
 * 2. 记录数据的最小/最大时间（秒数）
 * 3. 记录页面加载时刻
 * 4. 模拟当前时间 = 数据最小时间 + 页面加载后经过的秒数
 * 5. 只展示 <= 模拟当前时间 的数据
 * 6. 随着真实时间推移，模拟时间推进，数据逐步展现
 * 7. 告警表格显示模拟时间前2分钟的数据
 * 8. 只在模拟分钟变化时刷新视图（避免动画跳动）
 */

const Dashboard = {
    timer: null,
    _timeUpdateInterval: null,
    _prevKPI: { total: 0, alerts: 0, amount: 0 },
    _prevAlertIds: new Set(),
    _lastAlertCount: 0,
    _dataLoaded: false,
    _lastDataLoadTime: 0,
    _DATA_REFRESH_INTERVAL: 3000,
    data: {
        kpi: { total: 0, alerts: 0, amount: 0, latency: 0 },
        alerts: []
    },

    // 缓存全部数据（不过滤时间）
    _allData: {
        alerts: [],
        metrics: null,
        traffic: null,
        modelStatus: null,
    },

    // 流量图滑动窗口数据（本地维护，避免跳动）
    _trafficBuffer: {
        timestamps: [],
        normal: [],
        fraud: [],
    },
    _trafficBufferMax: 120, // 最多保留120秒

    // 数据时间范围（秒数，从0点开始）
    _dataMinSecond: -1,
    _dataMaxSecond: -1,
    _dataMinMinute: -1,
    _dataMaxMinute: -1,

    // 页面加载时刻（毫秒时间戳）
    _pageLoadTime: 0,

    // 上次渲染时的模拟分钟（用于判断是否需要刷新）
    _lastSimulatedMinute: -1,

    async init() {
        api.startSimulation();
        this._pageLoadTime = Date.now();

        // 一次性加载全部数据
        await this.loadAllData();

        // 数据加载完成后统一渲染
        this.refreshView();
        this.startTimeUpdate();

        // 定期从后端拉取数据（保持与Doris同步）
        this.timer = setInterval(() => this.loadAllData(), this._DATA_REFRESH_INTERVAL);
    },

    /**
     * 从Doris加载全部数据（不过滤时间）
     */
    async loadAllData() {
        try {
            const [metrics, traffic, alerts, totalCount, modelStatus] = await Promise.all([
                api.getMetrics(),
                api.getTrafficHistory(120),
                api.getRecentAlerts(10000),
                api.getTotalCount(),
                api.getModelStatus(),
            ]);

            this._allData.metrics = metrics;
            this._allData.alerts = alerts || [];
            this._allData.totalCount = totalCount;
            this._allData.modelStatus = modelStatus;
            this._dataLoaded = true;
            this._lastDataLoadTime = Date.now();

            // 流量数据：滑动窗口模式，追加新数据，移除旧数据
            if (traffic && traffic.timestamps && traffic.timestamps.length > 0) {
                this._mergeTrafficData(traffic);
            }

            // 计算数据的时间范围
            this._computeDataTimeRange();

            // 数据加载后刷新视图
            this.refreshView();
        } catch (err) {
            console.warn('加载数据失败:', err);
        }
    },

    /**
     * 合并流量数据（滑动窗口）
     */
    _mergeTrafficData(newTraffic) {
        const buffer = this._trafficBuffer;
        const maxLen = this._trafficBufferMax;

        // 如果是首次加载，直接赋值
        if (buffer.timestamps.length === 0) {
            buffer.timestamps = newTraffic.timestamps.slice();
            buffer.normal = newTraffic.normal.slice();
            buffer.fraud = newTraffic.fraud.slice();
            return;
        }

        // 追加新数据点（去重）
        for (let i = 0; i < newTraffic.timestamps.length; i++) {
            const ts = newTraffic.timestamps[i];
            // 检查是否已存在
            if (!buffer.timestamps.includes(ts)) {
                buffer.timestamps.push(ts);
                buffer.normal.push(newTraffic.normal[i] || 0);
                buffer.fraud.push(newTraffic.fraud[i] || 0);
            }
        }

        // 保持最大长度，移除最旧的数据
        while (buffer.timestamps.length > maxLen) {
            buffer.timestamps.shift();
            buffer.normal.shift();
            buffer.fraud.shift();
        }
    },

    /**
     * 计算数据的最小/最大时间（秒数/分钟数）
     */
    _computeDataTimeRange() {
        const alerts = this._allData.alerts || [];
        let minSecond = Infinity, maxSecond = -1;
        let minMinute = Infinity, maxMinute = -1;

        // 从告警中找时间范围
        for (const a of alerts) {
            const alertTime = a.alert_time || a.alert_time_formatted || '';
            const sec = this._parseTimeToSecond(alertTime);
            if (sec >= 0) {
                if (sec < minSecond) minSecond = sec;
                if (sec > maxSecond) maxSecond = sec;
            }
            const min = this._parseTimeToMinute(alertTime);
            if (min >= 0) {
                if (min < minMinute) minMinute = min;
                if (min > maxMinute) maxMinute = min;
            }
        }

        // 从流量数据中找时间范围
        const traffic = this._trafficBuffer;
        if (traffic && traffic.timestamps) {
            for (const ts of traffic.timestamps) {
                const match = ts.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
                if (match) {
                    const h = parseInt(match[1], 10);
                    const m = parseInt(match[2], 10);
                    const s = match[3] ? parseInt(match[3], 10) : 0;
                    const sec = h * 3600 + m * 60 + s;
                    const minute = h * 60 + m;
                    if (minute < minMinute) minMinute = minute;
                    if (minute > maxMinute) maxMinute = minute;
                    if (sec < minSecond) minSecond = sec;
                    if (sec > maxSecond) maxSecond = sec;
                }
            }
        }

        this._dataMinSecond = minSecond === Infinity ? -1 : minSecond;
        this._dataMaxSecond = maxSecond;
        this._dataMinMinute = minMinute === Infinity ? -1 : minMinute;
        this._dataMaxMinute = maxMinute;
    },

    /**
     * 获取当前模拟时间（秒数，从0点开始）
     * 直接使用系统当前时间，不超过数据最大时间
     */
    getSimulatedSecond() {
        if (this._dataMinSecond < 0) return this._dataMaxSecond;
        
        // 系统当前时间对应的秒数（从0点开始）
        const now = new Date();
        const currentSecondOfDay = now.getHours() * 3600 + now.getMinutes() * 60 + now.getSeconds();
        
        // 不超过数据最大时间
        return Math.min(currentSecondOfDay, this._dataMaxSecond);
    },

    /**
     * 获取当前模拟分钟（从0点开始）
     */
    getSimulatedMinute() {
        return Math.floor(this.getSimulatedSecond() / 60);
    },

    /**
     * 刷新整个视图（KPI + 图表 + 告警表）
     * 只在模拟分钟变化时才真正刷新（避免动画跳动）
     */
    refreshView() {
        if (!this._dataLoaded) return;
        this.updateKPIs();
        this.updateTrafficChart();
        this.updateDistributionCharts();
        this.updateAlertTable();
    },

    /**
     * 从告警时间字符串解析分钟数
     */
    _parseTimeToMinute(timeStr) {
        if (!timeStr) return -1;
        const match = timeStr.match(/^(\d{1,2}):(\d{2})/);
        if (match) {
            return parseInt(match[1], 10) * 60 + parseInt(match[2], 10);
        }
        return -1;
    },

    /**
     * 从告警时间字符串解析秒数
     */
    _parseTimeToSecond(timeStr) {
        if (!timeStr) return -1;
        const match = timeStr.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?/);
        if (match) {
            const h = parseInt(match[1], 10);
            const m = parseInt(match[2], 10);
            const s = match[3] ? parseInt(match[3], 10) : 0;
            return h * 3600 + m * 60 + s;
        }
        return -1;
    },

    /**
     * 按模拟当前时间过滤告警
     * 只显示 <= 模拟当前时间 的数据
     */
    filterAlertsBySimulatedTime(alerts, simulatedSecond) {
        if (!alerts || alerts.length === 0) return [];
        return alerts.filter(a => {
            const alertTime = a.alert_time || a.alert_time_formatted || '';
            const alertSecond = this._parseTimeToSecond(alertTime);
            if (alertSecond < 0) return true;
            return alertSecond <= simulatedSecond;
        });
    },

    /**
     * 过滤流量数据，按模拟当前时间
     */
    filterTrafficBySimulatedTime(traffic, simulatedMinute) {
        if (!traffic || !traffic.timestamps) return { timestamps: [], normal: [], fraud: [] };
        const filtered = { timestamps: [], normal: [], fraud: [] };

        for (let i = 0; i < traffic.timestamps.length; i++) {
            const ts = traffic.timestamps[i];
            const match = ts.match(/^(\d{1,2}):(\d{2})$/);
            if (match) {
                const minute = parseInt(match[1], 10) * 60 + parseInt(match[2], 10);
                if (minute <= simulatedMinute) {
                    filtered.timestamps.push(ts);
                    filtered.normal.push(traffic.normal[i] || 0);
                    filtered.fraud.push(traffic.fraud[i] || 0);
                }
            }
        }
        return filtered;
    },

    /**
     * 数字滚动动画
     */
    animateValue(element, start, end, duration = 500) {
        if (start === end) {
            element.textContent = this.formatNumber(end);
            return;
        }
        const startTime = performance.now();
        const diff = end - start;
        const step = (timestamp) => {
            const elapsed = timestamp - startTime;
            const progress = Math.min(elapsed / duration, 1);
            const eased = 1 - Math.pow(1 - progress, 3);
            const current = Math.round(start + diff * eased);
            element.textContent = this.formatNumber(current);
            if (progress < 1) requestAnimationFrame(step);
        };
        requestAnimationFrame(step);
    },

    /**
     * 基于模拟时间计算KPI
     */
    updateKPIs() {
        if (!this._dataLoaded) return;

        const simulatedSecond = this.getSimulatedSecond();
        const allAlerts = this._allData.alerts || [];
        const metrics = this._allData.metrics || {};

        // 按模拟时间过滤告警
        const filteredAlerts = this.filterAlertsBySimulatedTime(allAlerts, simulatedSecond);

        // 告警总数和金额使用过滤后的数据
        const totalFraud = filteredAlerts.length;
        const fraudAmount = filteredAlerts.reduce((sum, a) => {
            const amt = parseFloat(a.amount);
            return sum + (isNaN(amt) ? 0 : amt);
        }, 0);

        // 正常交易总数：按比例估算（基于模拟进度）
        const totalCount = this._allData.totalCount || {};
        const totalNormalFull = totalCount.normal_count || 0;
        const totalFraudFull = totalCount.fraud_count || 0;

        // 按模拟进度比例计算正常交易数
        let progressRatio = 1.0;
        if (this._dataMinSecond >= 0 && this._dataMaxSecond > this._dataMinSecond) {
            progressRatio = (simulatedSecond - this._dataMinSecond) / (this._dataMaxSecond - this._dataMinSecond);
            progressRatio = Math.max(0, Math.min(1, progressRatio));
        }
        const totalNormal = Math.round(totalNormalFull * progressRatio);

        const total = totalNormal + totalFraud;
        const latency = metrics.avg_latency_ms || 67;

        const prevTotal = this.data.kpi.total;
        const prevAlerts = this.data.kpi.alerts;
        const prevAmount = this.data.kpi.amount;

        this.data.kpi.total = total;
        this.data.kpi.alerts = totalFraud;
        this.data.kpi.amount = fraudAmount;
        this.data.kpi.latency = latency;

        const fraudAmountRounded = Math.round(fraudAmount);
        const prevAmountRounded = Math.round(prevAmount);

        // 数字滚动动画
        this.animateValue(document.getElementById('kpiTotal'), prevTotal, total);
        this.animateValue(document.getElementById('kpiAlerts'), prevAlerts, totalFraud);

        // 金额动画
        const amountEl = document.getElementById('kpiAmount');
        if (prevAmountRounded !== fraudAmountRounded) {
            const amountStart = performance.now();
            const amountDiff = fraudAmountRounded - prevAmountRounded;
            const self = this;
            const amountStep = (ts) => {
                const progress = Math.min((ts - amountStart) / 500, 1);
                const eased = 1 - Math.pow(1 - progress, 3);
                const current = Math.round(prevAmountRounded + amountDiff * eased);
                amountEl.textContent = '¥' + self.formatNumber(current);
                if (progress < 1) requestAnimationFrame(amountStep);
            };
            requestAnimationFrame(amountStep);
        } else {
            amountEl.textContent = '¥' + this.formatNumber(fraudAmountRounded);
        }

        document.getElementById('kpiLatency').textContent = latency;

        // 增量趋势
        const deltaTotal = total - this._prevKPI.total;
        const deltaFraud = totalFraud - this._prevKPI.alerts;
        const deltaAmount = fraudAmountRounded - this._prevKPI.amount;

        document.getElementById('kpiTotalTrend').textContent = deltaTotal > 0 ? `↑ +${this.formatNumber(deltaTotal)}` : '';
        document.getElementById('kpiAlertsTrend').textContent = deltaFraud > 0 ? `↑ +${deltaFraud}` : '';
        document.getElementById('kpiAmountTrend').textContent = deltaAmount > 0 ? `↑ +¥${this.formatNumber(deltaAmount)}` : '';

        this._prevKPI = { total, alerts: totalFraud, amount: fraudAmountRounded };

        const latencyBadge = document.getElementById('kpiLatencyBadge');
        if (latency < 100) {
            latencyBadge.textContent = '正常';
            latencyBadge.style.background = 'rgba(16,185,129,0.15)';
            latencyBadge.style.color = '#10b981';
        } else if (latency < 200) {
            latencyBadge.textContent = '偏高';
            latencyBadge.style.background = 'rgba(245,158,11,0.15)';
            latencyBadge.style.color = '#f59e0b';
        } else {
            latencyBadge.textContent = '警告';
            latencyBadge.style.background = 'rgba(239,68,68,0.15)';
            latencyBadge.style.color = '#ef4444';
        }
    },

    /**
     * 流量图：显示最近120秒的数据（按秒级）
     * 首次渲染用 renderTrafficChart，后续用 updateTrafficChart 平滑更新
     */
    _trafficChartRendered: false,

    updateTrafficChart() {
        if (!this._dataLoaded) return;

        const buffer = this._trafficBuffer;
        if (!buffer.timestamps || buffer.timestamps.length === 0) return;

        if (!this._trafficChartRendered) {
            Charts.renderTrafficChart({
                timestamps: buffer.timestamps,
                normal: buffer.normal,
                fraud: buffer.fraud
            });
            this._trafficChartRendered = true;
        } else {
            Charts.updateTrafficChart({
                timestamps: buffer.timestamps,
                normal: buffer.normal,
                fraud: buffer.fraud
            });
        }
    },

    /**
     * 分布图：按模拟时间过滤
     */
    updateDistributionCharts() {
        if (!this._dataLoaded) return;

        const simulatedSecond = this.getSimulatedSecond();
        const filteredAlerts = this.filterAlertsBySimulatedTime(this._allData.alerts, simulatedSecond);

        // 告警类型分布
        const typeCount = {};
        filteredAlerts.forEach(a => {
            const t = a.fraud_type || '未知';
            typeCount[t] = (typeCount[t] || 0) + 1;
        });
        const typeDist = Object.entries(typeCount)
            .map(([name, value]) => ({ name, value }))
            .sort((a, b) => b.value - a.value);
        if (typeDist.length > 0) Charts.renderAlertTypesChart({ types: typeDist });

        // 检测来源分布
        const sourceCount = {};
        filteredAlerts.forEach(a => {
            const s = a.detection_source || a.source || '未知';
            let name = s;
            if (s.startsWith('CEP')) name = 'CEP规则';
            else if (s.startsWith('SQL')) name = 'SQL引擎';
            else if (s.startsWith('GRAPH')) name = '图分析';
            else if (s.startsWith('GNN')) name = 'GNN检测';
            else if (s.startsWith('ML')) name = 'ML集成';
            sourceCount[name] = (sourceCount[name] || 0) + 1;
        });
        const sourceDist = Object.entries(sourceCount)
            .map(([name, value]) => ({ name, value }))
            .sort((a, b) => b.value - a.value);
        if (sourceDist.length > 0) Charts.renderDetectionLayersChart({ layers: sourceDist });

        // 置信度分布
        const confBins = [
            { range: '高危(>90%)', count: 0 },
            { range: '中危(70-90%)', count: 0 },
            { range: '低危(<70%)', count: 0 }
        ];
        filteredAlerts.forEach(a => {
            const c = parseFloat(a.confidence) || 0;
            if (c >= 0.9) confBins[0].count++;
            else if (c >= 0.7) confBins[1].count++;
            else confBins[2].count++;
        });
        Charts.renderConfidenceChart({ bins: confBins });

        // 地域分布
        const geoCount = {};
        filteredAlerts.forEach(a => {
            const g = a.city || a.location || '未知';
            geoCount[g] = (geoCount[g] || 0) + 1;
        });
        const geoDist = Object.entries(geoCount)
            .map(([name, value]) => ({ name, value }))
            .sort((a, b) => b.value - a.value);
        if (geoDist.length > 0) Charts.renderGeoHeatChart({ geo: geoDist });

        // 模型训练历史
        const modelStatus = this._allData.modelStatus;
        if (modelStatus && modelStatus.training_history && modelStatus.training_history.length > 0) {
            Charts.renderModelTrainingChart({
                training_history: modelStatus.training_history
            });
        }
    },

    /**
     * 告警表格：显示模拟时间前2分钟内的告警，最新的在前
     */
    updateAlertTable() {
        if (!this._dataLoaded) return;

        const allAlerts = this._allData.alerts || [];
        const simulatedSecond = this.getSimulatedSecond();

        // 模拟基准时间
        const baseH = Math.floor(simulatedSecond / 3600);
        const baseM = Math.floor((simulatedSecond % 3600) / 60);
        const baseS = simulatedSecond % 60;
        const baseTime = new Date();
        baseTime.setHours(baseH, baseM, baseS, 0);

        const twoMinutesAgo = new Date(baseTime.getTime() - 2 * 60 * 1000);

        // 筛选最近2分钟的数据，最新的在前
        const recentAlerts = allAlerts.filter(a => {
            const alertTime = a.alert_time || a.alert_time_formatted || '';
            if (!alertTime) return false;
            const match = alertTime.match(/^(\d{1,2}):(\d{2}):(\d{2})/);
            if (!match) return false;
            const alertDate = new Date();
            alertDate.setHours(parseInt(match[1], 10), parseInt(match[2], 10), parseInt(match[3], 10));
            return alertDate >= twoMinutesAgo && alertDate <= baseTime;
        }).sort((a, b) => {
            // 按时间降序排列（最新的在前）
            const timeA = this._parseTimeToSecond(a.alert_time || a.alert_time_formatted || '');
            const timeB = this._parseTimeToSecond(b.alert_time || b.alert_time_formatted || '');
            return timeB - timeA;
        }).slice(0, 50);

        const tbody = document.getElementById('alertTableBody');
        if (!tbody) return;

        const currentIds = new Set(recentAlerts.map(a => a.alert_id || a.tx_id));
        const newIds = [...currentIds].filter(id => !this._prevAlertIds.has(id));
        this._prevAlertIds = currentIds;

        if (recentAlerts.length === this._lastAlertCount && newIds.length === 0) return;
        this._lastAlertCount = recentAlerts.length;

        tbody.innerHTML = '';
        recentAlerts.forEach(alert => {
            const conf = parseFloat(alert.confidence);
            const confClass = conf > 0.9 ? 'high' : conf > 0.7 ? 'medium' : 'low';
            const time = alert.timestamp ? new Date(alert.timestamp).toLocaleTimeString('zh-CN') : (alert.alert_time_formatted || '');
            const amountStr = typeof alert.amount === 'number' || /^\d/.test(String(alert.amount))
                ? '¥' + parseFloat(alert.amount).toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
                : alert.amount;

            const tr = document.createElement('tr');
            if (newIds.includes(alert.alert_id || alert.tx_id)) {
                tr.classList.add('alert-row-new');
            }
            tr.innerHTML = `
                <td>${alert.account_id}</td>
                <td><span class="alert-type-badge">${alert.fraud_type}</span></td>
                <td><span class="source-badge">${alert.detection_source || alert.source}</span></td>
                <td><span class="confidence-value ${confClass}">${conf.toFixed(2)}</span></td>
                <td>${amountStr}</td>
                <td>${time}</td>
            `;
            tbody.appendChild(tr);
        });
    },

    startTimeUpdate() {
        if (this._timeUpdateInterval) clearInterval(this._timeUpdateInterval);
        this.updateTime();
        this._lastSimulatedMinute = this.getSimulatedMinute();

        // 每秒更新时钟和流量图，模拟分钟变化时才刷新其他视图
        this._timeUpdateInterval = setInterval(() => {
            this.updateTime();
            // 流量图每秒刷新（基于系统当前时间）
            this.updateTrafficChart();
            const currentSimMinute = this.getSimulatedMinute();
            // 只在模拟分钟变化时刷新其他视图（避免动画跳动）
            if (currentSimMinute !== this._lastSimulatedMinute) {
                this._lastSimulatedMinute = currentSimMinute;
                this.updateKPIs();
                this.updateDistributionCharts();
                this.updateAlertTable();
            }
        }, 1000);
    },

    updateTime() {
        const now = new Date();
        const timeStr = now.toLocaleTimeString('zh-CN', { hour12: false });
        const el = document.getElementById('currentTime');
        if (el) el.textContent = timeStr;
    },

    formatNumber(num) {
        return Number(num).toLocaleString();
    }
};
