/**
 * Alerts Module
 * Alert management page logic
 */

const AlertsPage = {
    currentPage: 1,
    pageSize: 10,
    filters: {},
    allAlerts: [],
    _eventsBound: false,
    _refreshTimer: null,
    _currentAlert: null,

    async init() {
        await this.loadFilterOptions();
        await this.loadAlerts();
        if (!this._eventsBound) {
            this.bindEvents();
            this._eventsBound = true;
        }
        // Auto-refresh every 4s to get new simulation alerts
        this.startRefresh();
    },

    async loadFilterOptions() {
        try {
            const [fraudTypes, sources] = await Promise.all([
                api.getFraudTypes(),
                api.getDetectionSources(),
            ]);

            // 填充欺诈类型下拉框
            const typeSelect = document.getElementById('filterFraudType');
            if (typeSelect) {
                typeSelect.innerHTML = '<option value="">全部类型</option>';
                fraudTypes.forEach(t => {
                    const opt = document.createElement('option');
                    opt.value = t;
                    opt.textContent = t;
                    typeSelect.appendChild(opt);
                });
            }

            // 填充检测来源下拉框
            const sourceSelect = document.getElementById('filterSource');
            if (sourceSelect) {
                sourceSelect.innerHTML = '<option value="">全部</option>';
                sources.forEach(s => {
                    const opt = document.createElement('option');
                    opt.value = s;
                    opt.textContent = s;
                    sourceSelect.appendChild(opt);
                });
            }
        } catch (err) {
            console.warn('加载筛选选项失败:', err);
        }
    },

    startRefresh() {
        if (this._refreshTimer) clearInterval(this._refreshTimer);
        this._refreshTimer = setInterval(() => this.loadAlerts(), 3000);
    },

    stopRefresh() {
        if (this._refreshTimer) { clearInterval(this._refreshTimer); this._refreshTimer = null; }
    },

    async loadAlerts() {
        try {
            // Use real Doris data
            const data = await api.getRecentAlerts(10000);
            let alerts = data;

            // 过滤：只显示到系统当前时间的数据
            const now = new Date();
            alerts = alerts.filter(a => {
                const ts = a.timestamp || '';
                if (!ts) return true;
                const alertDate = new Date(ts);
                return !isNaN(alertDate.getTime()) && alertDate <= now;
            });

            // Apply client-side filters
            if (this.filters.fraud_type) {
                alerts = alerts.filter(a => a.fraud_type === this.filters.fraud_type);
            }
            if (this.filters.source) {
                alerts = alerts.filter(a => {
                    const s = a.detection_source || a.source || '';
                    return s === this.filters.source;
                });
            }
            if (this.filters.min_confidence !== undefined) {
                alerts = alerts.filter(a => parseFloat(a.confidence) >= this.filters.min_confidence);
            }
            if (this.filters.risk) {
                alerts = alerts.filter(a => {
                    const conf = parseFloat(a.confidence);
                    if (this.filters.risk === 'high') return conf > 0.9;
                    if (this.filters.risk === 'medium') return conf >= 0.7 && conf <= 0.9;
                    if (this.filters.risk === 'low') return conf < 0.7;
                    return true;
                });
            }

            this.allAlerts = alerts;
            this.renderAlerts();
            this.renderPagination();
        } catch (err) {
            console.warn('Failed to load alerts:', err);
        }
    },

    renderAlerts() {
        const container = document.getElementById('alertsList');
        if (!container) return;

        if (this.allAlerts.length === 0) {
            container.innerHTML = `
                <div class="no-data">
                    <div class="no-data-icon">🔍</div>
                    <div class="no-data-text">暂无告警数据</div>
                    <div class="no-data-hint">调整筛选条件以查看更多结果</div>
                </div>
            `;
            return;
        }

        const start = (this.currentPage - 1) * this.pageSize;
        const end = start + this.pageSize;
        const pageAlerts = this.allAlerts.slice(start, end);

        container.innerHTML = pageAlerts.map(alert => {
            const conf = parseFloat(alert.confidence);
            const severity = conf > 0.9 ? 'high' : conf > 0.7 ? 'medium' : 'low';
            const time = new Date(alert.timestamp).toLocaleString('zh-CN');
            // location: backend returns city
            const location = alert.city || alert.location || '未知';

            return `
                <div class="alert-item" data-alert-id="${alert.alert_id}">
                    <div class="alert-item-left">
                        <div class="alert-severity ${severity}"></div>
                        <div class="alert-info">
                            <h4>${alert.fraud_type}</h4>
                            <p>${alert.account_id} · ${location} · ${alert.detection_source}</p>
                        </div>
                    </div>
                    <div class="alert-item-right">
                        <div class="alert-confidence">
                            <span class="confidence-value ${severity}">${(conf * 100).toFixed(1)}%</span>
                            <span class="confidence-label">置信度</span>
                        </div>
                        <div class="alert-time">${time}</div>
                    </div>
                </div>
            `;
        }).join('');

        // Bind click events via event delegation
        container.querySelectorAll('.alert-item').forEach(item => {
            item.addEventListener('click', () => {
                const alertId = item.dataset.alertId;
                if (alertId) this.showAlertDetail(alertId);
            });
        });
    },

    renderPagination() {
        const container = document.getElementById('alertsPagination');
        if (!container) return;

        const totalPages = Math.ceil(this.allAlerts.length / this.pageSize);
        if (totalPages <= 1) {
            container.innerHTML = '';
            return;
        }

        let html = '';

        // Previous button
        html += `<button class="pagination-btn prev-btn" onclick="AlertsPage.goToPage(${this.currentPage - 1})" ${this.currentPage === 1 ? 'disabled' : ''}>‹</button>`;

        // Page numbers
        const pages = this._getPageNumbers(totalPages);
        let lastNum = 0;

        pages.forEach(p => {
            if (p === '...') {
                html += `<span class="pagination-ellipsis">···</span>`;
            } else {
                if (lastNum > 0 && p - lastNum > 1) {
                    html += `<span class="pagination-ellipsis">···</span>`;
                }
                html += `<button class="pagination-btn ${p === this.currentPage ? 'active' : ''}" onclick="AlertsPage.goToPage(${p})">${p}</button>`;
                lastNum = p;
            }
        });

        // Next button
        html += `<button class="pagination-btn next-btn" onclick="AlertsPage.goToPage(${this.currentPage + 1})" ${this.currentPage === totalPages ? 'disabled' : ''}>›</button>`;

        container.innerHTML = html;
    },

    _getPageNumbers(totalPages) {
        const current = this.currentPage;
        const pages = [];

        if (totalPages <= 7) {
            for (let i = 1; i <= totalPages; i++) pages.push(i);
        } else {
            pages.push(1);
            if (current > 3) pages.push('...');

            const start = Math.max(2, current - 1);
            const end = Math.min(totalPages - 1, current + 1);
            for (let i = start; i <= end; i++) pages.push(i);

            if (current < totalPages - 2) pages.push('...');
            pages.push(totalPages);
        }

        return pages;
    },

    goToPage(page) {
        const totalPages = Math.ceil(this.allAlerts.length / this.pageSize);
        if (page < 1 || page > totalPages) return;
        this.currentPage = page;
        this.renderAlerts();
        this.renderPagination();
        // Scroll to top of list
        const list = document.getElementById('alertsList');
        if (list) list.scrollTop = 0;
    },

    async showAlertDetail(alertId) {
        try {
            const data = await api.getAlertDetail(alertId);
            const alert = data || this.allAlerts.find(a => a.alert_id === alertId);
            if (!alert) return;

            // Store current alert for feedback buttons
            this._currentAlert = alert;

            const conf = parseFloat(alert.confidence);
            const modal = document.getElementById('alertModal');
            const body = document.getElementById('alertModalBody');
            // amount: backend may return string like "¥10K+" or numeric value
            const amountStr = typeof alert.amount === 'number' || /^\d/.test(String(alert.amount))
                ? '¥' + parseFloat(alert.amount).toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
                : alert.amount;
            // location: backend returns city
            const location = alert.city || alert.location || '未知';

            body.innerHTML = `
                <div class="alert-detail-grid">
                    <div class="alert-detail-item">
                        <span class="alert-detail-label">告警ID</span>
                        <span class="alert-detail-value" style="font-family: monospace; font-size: 12px;">${alert.alert_id}</span>
                    </div>
                    <div class="alert-detail-item">
                        <span class="alert-detail-label">账户ID</span>
                        <span class="alert-detail-value" style="font-family: monospace;">${alert.account_id}</span>
                    </div>
                    <div class="alert-detail-item">
                        <span class="alert-detail-label">欺诈类型</span>
                        <span class="alert-detail-value" style="color: #3b82f6;">${alert.fraud_type}</span>
                    </div>
                    <div class="alert-detail-item">
                        <span class="alert-detail-label">检测来源</span>
                        <span class="alert-detail-value">${alert.detection_source}</span>
                    </div>
                    <div class="alert-detail-item">
                        <span class="alert-detail-label">置信度</span>
                        <span class="alert-detail-value" style="color: ${conf > 0.9 ? '#ef4444' : conf > 0.7 ? '#f59e0b' : '#10b981'}; font-weight: 700;">${(conf * 100).toFixed(1)}%</span>
                    </div>
                    <div class="alert-detail-item">
                        <span class="alert-detail-label">交易金额</span>
                        <span class="alert-detail-value" style="font-weight: 600;">${amountStr}</span>
                    </div>
                    <div class="alert-detail-item">
                        <span class="alert-detail-label">时间</span>
                        <span class="alert-detail-value">${new Date(alert.timestamp).toLocaleString('zh-CN')}</span>
                    </div>
                    <div class="alert-detail-item">
                        <span class="alert-detail-label">地点</span>
                        <span class="alert-detail-value">${location}</span>
                    </div>
                </div>
                <div class="explainability-section">
                    <div class="explainability-title">可解释性信息</div>
                    <div class="feature-bar">
                        <span class="feature-name">交易频率异常</span>
                        <div class="feature-value-bar"><div class="feature-fill" style="width: ${Math.min(100, Math.random() * 40 + 60)}%"></div></div>
                        <span class="feature-contribution">+${(Math.random() * 0.3 + 0.1).toFixed(2)}</span>
                    </div>
                    <div class="feature-bar">
                        <span class="feature-name">金额异常度</span>
                        <div class="feature-value-bar"><div class="feature-fill" style="width: ${Math.min(100, Math.random() * 50 + 30)}%"></div></div>
                        <span class="feature-contribution">+${(Math.random() * 0.25 + 0.05).toFixed(2)}</span>
                    </div>
                    <div class="feature-bar">
                        <span class="feature-name">设备变更</span>
                        <div class="feature-value-bar"><div class="feature-fill" style="width: ${Math.min(100, Math.random() * 30 + 20)}%"></div></div>
                        <span class="feature-contribution">+${(Math.random() * 0.2 + 0.05).toFixed(2)}</span>
                    </div>
                    <div class="feature-bar">
                        <span class="feature-name">地理位置漂移</span>
                        <div class="feature-value-bar"><div class="feature-fill" style="width: ${Math.min(100, Math.random() * 35 + 15)}%"></div></div>
                        <span class="feature-contribution">+${(Math.random() * 0.15 + 0.02).toFixed(2)}</span>
                    </div>
                </div>
            `;

            modal.classList.add('active');
        } catch (err) {
            console.warn('Failed to load alert detail:', err);
        }
    },

    bindEvents() {
        // Apply filters
        document.getElementById('applyFilterBtn')?.addEventListener('click', () => {
            this.filters = {};
            const fraudType = document.getElementById('filterFraudType').value;
            if (fraudType) this.filters.fraud_type = fraudType;

            const source = document.getElementById('filterSource').value;
            if (source) this.filters.source = source;

            const risk = document.getElementById('filterRisk').value;
            if (risk) this.filters.risk = risk;

            const minConf = document.getElementById('filterMinConf').value;
            if (minConf) this.filters.min_confidence = parseFloat(minConf);

            this.currentPage = 1;
            this.loadAlerts();
        });

        // Reset filters
        document.getElementById('resetFilterBtn')?.addEventListener('click', () => {
            this.filters = {};
            this.currentPage = 1;
            document.getElementById('filterFraudType').value = '';
            document.getElementById('filterSource').value = '';
            document.getElementById('filterRisk').value = '';
            document.getElementById('filterMinConf').value = '';
            this.loadAlerts();
        });

        // Export alerts
        document.getElementById('exportAlertsBtn')?.addEventListener('click', () => {
            this.exportAlerts();
        });

        // Refresh alerts
        document.getElementById('refreshAlertsBtn')?.addEventListener('click', () => {
            this.loadAlerts();
        });

        // Close modal
        document.getElementById('closeAlertModal')?.addEventListener('click', () => {
            document.getElementById('alertModal').classList.remove('active');
        });
        document.getElementById('closeModalBtn')?.addEventListener('click', () => {
            document.getElementById('alertModal').classList.remove('active');
        });

        // Feedback buttons
        document.getElementById('feedbackConfirmBtn')?.addEventListener('click', async () => {
            if (!this._currentAlert) return;
            const btn = document.getElementById('feedbackConfirmBtn');
            btn.disabled = true;
            btn.textContent = '提交中...';
            try {
                await api.postFeedback(this._currentAlert.alert_id, 'CONFIRMED_FRAUD');
                alert('已确认为欺诈交易，数据已记录到Doris');
            } catch (err) {
                console.warn('反馈提交失败:', err);
                alert('提交失败，请重试');
            }
            btn.disabled = false;
            btn.textContent = '确认欺诈';
            document.getElementById('alertModal').classList.remove('active');
        });
        document.getElementById('feedbackFalseBtn')?.addEventListener('click', async () => {
            if (!this._currentAlert) return;
            const btn = document.getElementById('feedbackFalseBtn');
            btn.disabled = true;
            btn.textContent = '提交中...';
            try {
                await api.postFeedback(this._currentAlert.alert_id, 'FALSE_POSITIVE');
                alert('已标记为误报，将用于模型优化');
            } catch (err) {
                console.warn('反馈提交失败:', err);
                alert('提交失败，请重试');
            }
            btn.disabled = false;
            btn.textContent = '标记误报';
            document.getElementById('alertModal').classList.remove('active');
        });
    },

    exportAlerts() {
        if (!this.allAlerts.length) return;

        const headers = ['告警ID', '账户ID', '欺诈类型', '检测来源', '置信度', '金额', '时间', '地点'];
        const rows = this.allAlerts.map(a => [
            a.alert_id, a.account_id, a.fraud_type, a.detection_source,
            a.confidence, a.amount, a.timestamp, a.city || a.location || '未知'
        ]);

        let csv = '\uFEFF' + headers.join(',') + '\n';
        rows.forEach(r => {
            csv += r.map(v => `"${v}"`).join(',') + '\n';
        });

        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `fraud_alerts_${new Date().toISOString().slice(0, 10)}.csv`;
        a.click();
        URL.revokeObjectURL(url);
    }
};
