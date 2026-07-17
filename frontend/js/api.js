/**
 * API Client Module
 * Reads real data from Doris via the FastAPI backend.
 * Falls back to demo mode if backend is unavailable.
 */

const API_CONFIG = {
    baseUrl: 'http://localhost:8002',
    timeout: 5000,
    retryCount: 3
};

class ApiClient {
    constructor() {
        this.base = API_CONFIG.baseUrl;
        this.demoMode = false;          // try real Doris first
        this._backendHealthy = null;    // null = not checked yet
        this._token = localStorage.getItem('auth_token') || null;
        this._user = null;

        // Rules state (still managed in frontend for toggling)
        this._rules = [
            { id: 1, name: '小额试探大额转出', description: '3分钟内3笔≤100元试探后跟随≥10000元转出', enabled: true },
            { id: 2, name: '多层链式洗钱', description: '5分钟内3层转账路径，中间节点不重叠', enabled: true },
            { id: 3, name: '异地跨设备突发大额', description: '30分钟内异地+新设备+单笔≥50000元', enabled: true },
            { id: 4, name: '分散转入集中提现', description: '24小时内5个不同账户转入同一账户，集中取现', enabled: true },
            { id: 5, name: '多渠道轮番转账', description: '1小时内使用网银、手机银行、ATM、柜台交替转账', enabled: true },
            { id: 6, name: '凌晨分批掏空', description: '0:00-5:00分批转出账户余额，间隔5-10分钟', enabled: true },
            { id: 7, name: '小额掩护大额跑路', description: '2笔小额正常交易后立即全额转出', enabled: true },
            { id: 8, name: '团伙同IP批量作案', description: '10分钟内同一IP段3+账户转账', enabled: true },
            { id: 9, name: '账户被盗急速转账', description: '10分钟内设备变更+密码重置+大额转账', enabled: true },
            { id: 10, name: '虚假交易退款套利', description: '5笔交易+3笔退款，商户与用户同设备', enabled: true },
            { id: 11, name: '养卡提额异常消费', description: '新卡30天内大额+整额消费≥5笔', enabled: true }
        ];

        // Fallback simulation (only used when Doris is unreachable)
        this._fallbackState = null;
        this._simTimer = null;
        this._simRunning = false;

        // Cached Doris results to avoid over-querying
        this._cache = {
            metrics: null,
            alerts: null,
            fraudTypes: null,
            sources: null,
            confidence: null,
            geo: null,
            traffic: null,
            drift: null,
            driftEvents: null,
            feedbackStats: null,
            models: null,
        };
    }

    setBaseUrl(url) { this.base = url; }
    setDemoMode(val) { this.demoMode = val; }  // no-op, kept for compatibility

    // ============================================================
    // HTTP Request wrapper with Doris fallback
    // ============================================================
    async request(dorisEndpoint, demoEndpoint) {
        // Try Doris first
        if (this._backendHealthy === null) {
            await this._checkBackend();
        }
        if (this._backendHealthy === true) {
            try {
                const url = `${this.base}${dorisEndpoint}`;
                const headers = { 'Content-Type': 'application/json' };
                if (this._token) headers['Authorization'] = `Bearer ${this._token}`;
                const res = await fetch(url, { headers, signal: AbortSignal.timeout(API_CONFIG.timeout) });
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                const data = await res.json();
                // Only fallback to demo for critical endpoints (metrics) when empty;
                // other endpoints (traffic, alerts, etc.) returning [] is normal
                if (Array.isArray(data) && data.length === 0 && demoEndpoint === 'metrics') {
                    return this._demoGet(demoEndpoint);
                }
                return data;
            } catch (e) {
                console.warn(`[API] Doris fetch failed (${dorisEndpoint}), falling back to demo:`, e.message);
                this._backendHealthy = false;
                this._startFallbackSimulation();
                return this._demoGet(demoEndpoint);
            }
        }
        // Backend unhealthy, use demo
        return this._demoGet(demoEndpoint);
    }

    async _checkBackend() {
        try {
            const res = await fetch(`${this.base}/api/v1/health`, { signal: AbortSignal.timeout(3000) });
            this._backendHealthy = res.ok;
            if (this._backendHealthy) {
                console.log('[API] Doris backend is healthy');
            } else {
                console.warn('[API] Doris backend is unhealthy, using demo mode');
                this._startFallbackSimulation();
            }
        } catch (e) {
            console.warn('[API] Cannot reach Doris backend, using demo mode:', e.message);
            this._backendHealthy = false;
            this._startFallbackSimulation();
        }
    }

    // ============================================================
    // Fallback Simulation (only when Doris is unreachable)
    // ============================================================
    _startFallbackSimulation() {
        console.log('[API] Backend unavailable, no fallback simulation');
    }

    _fallbackTick() {
        // No longer used
    }

    _persistState() {
        // No longer used
    }

    // ============================================================
    // Demo data generator (used as fallback)
    // ============================================================
    _demoGet(endpoint) {
        if (endpoint === 'metrics') {
            const s = this._fallbackState;
            if (!s) return { total_processed: 0, total_fraud: 0, fraud_rate: 0, avg_latency_ms: 0, precision: 0, recall: 0, f1_score: 0, accuracy: 0 };
            return {
                total_processed: s.totalNormal + s.totalFraud,
                total_fraud: s.totalFraud,
                fraud_rate: s.totalFraud / (s.totalNormal + s.totalFraud),
                avg_latency_ms: 55 + Math.floor(Math.random() * 25),
                precision: 0.992, recall: 0.986, f1_score: 0.989, accuracy: 0.991
            };
        }
        if (endpoint === 'traffic') {
            if (!this._fallbackState) return { timestamps: [], normal: [], fraud: [] };
            const s = this._fallbackState;
            const total = s.elapsedSeconds;
            const lastKey = total - 1;
            const count = Math.min(60, total);
            const ts = [], n = [], f = [];
            for (let i = count - 1; i >= 0; i--) {
                const bk = lastKey - i;
                const b = s.trafficBuckets[bk] || { normal: 0, fraud: 0 };
                const ago = total - 1 - bk;
                ts.push(ago === 0 ? '现在' : ago < 60 ? `${ago}s前` : `${Math.floor(ago/60)}分钟前`);
                n.push(b.normal); f.push(b.fraud);
            }
            return { timestamps: ts, normal: n, fraud: f };
        }
        if (endpoint === 'alerts') {
            if (!this._fallbackState) return [];
            return this._fallbackState.alertStream.slice(-200).reverse();
        }
        if (endpoint === 'fraud-types') {
            if (!this._fallbackState) return [];
            const c = {};
            for (const a of this._fallbackState.alertStream) c[a.fraud_type] = (c[a.fraud_type]||0)+1;
            return Object.entries(c).map(([name, value]) => ({ name, value })).sort((a,b)=>b.value-a.value);
        }
        if (endpoint === 'sources') {
            if (!this._fallbackState) return [];
            const c = {};
            for (const a of this._fallbackState.alertStream) c[a.detection_source] = (c[a.detection_source]||0)+1;
            return Object.entries(c).map(([name, value]) => ({ name, value }));
        }
        if (endpoint === 'confidence') {
            if (!this._fallbackState) return [];
            const c = {};
            for (const a of this._fallbackState.alertStream) {
                const conf = parseFloat(a.confidence);
                c[conf >= 0.9 ? '高危(>90%)' : conf >= 0.7 ? '中危(70-90%)' : '低危(<70%)'] = (c[conf >= 0.9 ? '高危(>90%)' : conf >= 0.7 ? '中危(70-90%)' : '低危(<70%)']||0)+1;
            }
            return Object.entries(c).map(([name, value]) => ({ name, value }));
        }
        if (endpoint === 'geo') {
            if (!this._fallbackState) return [];
            const c = {};
            for (const a of this._fallbackState.alertStream) c[a.location] = (c[a.location]||0)+1;
            return Object.entries(c).map(([name, value]) => ({ name, value })).sort((a,b)=>b.value-a.value);
        }
        if (endpoint === 'drift') {
            const data = [];
            const now = Date.now();
            for (let i = 29; i >= 0; i--) {
                data.push({
                    sample_id: `S-${now - i * 4000}`,
                    f1_score: 0.982 + Math.random() * 0.01,
                    precision: 0.985 + Math.random() * 0.01,
                    recall: 0.978 + Math.random() * 0.01,
                    accuracy: 0.988 + Math.random() * 0.008,
                    total_samples: 500 + Math.floor(Math.random() * 200),
                    fraud_samples: 30 + Math.floor(Math.random() * 20)
                });
            }
            return data;
        }
        if (endpoint === 'drift-events') {
            return [];
        }
        if (endpoint === 'feedback-stats') {
            return [];
        }
        if (endpoint === 'models') {
            return [{ f1_score: 0.989, precision: 0.992, recall: 0.986, accuracy: 0.991, total_samples: 10000, fraud_samples: 150, normal_samples: 9850 }];
        }
        if (endpoint.startsWith('alert-detail')) {
            const id = endpoint.split('/')[1];
            if (this._fallbackState) {
                const found = this._fallbackState.alertStream.find(a => a.alert_id === id);
                if (found) return found;
            }
            const types = this._rules.filter(r => r.enabled).map(r => r.name);
            return {
                alert_id: id,
                account_id: `ACCT${String(Math.floor(Math.random() * 999999)).padStart(6, '0')}`,
                fraud_type: types[0] || '未知',
                detection_source: 'CEP_RULE',
                confidence: (0.7 + Math.random() * 0.3).toFixed(3),
                amount: (Math.random() * 200000 + 5000).toFixed(2),
                timestamp: new Date().toISOString(),
                location: ['北京', '上海', '广州', '深圳', '杭州', '成都'][Math.floor(Math.random() * 6)]
            };
        }
        return null;
    }

    // ============================================================
    // Public API methods — call Doris endpoints, fallback to demo
    // ============================================================

    async getMetrics(beforeHour = null) {
        const params = beforeHour !== null ? `?before_hour=${beforeHour}` : '';
        const data = await this.request(`/api/v1/doris/metrics${params}`, 'metrics');
        this._cache.metrics = data;
        // Warm up all other caches in parallel (sync methods need these)
        Promise.all([
            this.getTrafficHistory(120, beforeHour),
            this.getRecentAlerts(200, beforeHour),
            this.getFraudTypeDistribution(beforeHour),
            this.getSourceDistribution(beforeHour),
            this.getConfidenceDistribution(beforeHour),
            this.getGeoDistribution(beforeHour),
        ]).catch(() => {});
        return data;
    }

    async getTrafficHistory(seconds = 60, beforeHour = null) {
        let params = `seconds=${seconds}`;
        if (beforeHour !== null) params += `&before_hour=${beforeHour}`;
        const data = await this.request(`/api/v1/doris/traffic?${params}`, 'traffic');
        this._cache.traffic = data;
        return data;
    }

    async getTotalCount() {
        const data = await this.request('/api/v1/doris/total-count', 'total-count');
        return data;
    }

    async getRecentAlerts(limit = 200, beforeHour = null) {
        let params = `limit=${limit}`;
        if (beforeHour !== null) params += `&before_hour=${beforeHour}`;
        const data = await this.request(`/api/v1/doris/alerts?${params}`, 'alerts');
        this._cache.alerts = data;
        return data;
    }

    async getAlertDetail(id) {
        const data = await this.request(`/api/v1/doris/alerts/${id}`, `alert-detail/${id}`);
        return data;
    }

    async getFraudTypeDistribution(beforeHour = null) {
        const params = beforeHour !== null ? `?before_hour=${beforeHour}` : '';
        const data = await this.request(`/api/v1/doris/fraud-types${params}`, 'fraud-types');
        this._cache.fraudTypes = data;
        return data;
    }

    async getSourceDistribution(beforeHour = null) {
        const params = beforeHour !== null ? `?before_hour=${beforeHour}` : '';
        const data = await this.request(`/api/v1/doris/sources${params}`, 'sources');
        this._cache.sources = data;
        return data;
    }

    async getConfidenceDistribution(beforeHour = null) {
        const params = beforeHour !== null ? `?before_hour=${beforeHour}` : '';
        const data = await this.request(`/api/v1/doris/confidence${params}`, 'confidence');
        this._cache.confidence = data;
        return data;
    }

    async getGeoDistribution(beforeHour = null) {
        const params = beforeHour !== null ? `?before_hour=${beforeHour}` : '';
        const data = await this.request(`/api/v1/doris/geo${params}`, 'geo');
        this._cache.geo = data;
        return data;
    }

    async getDriftHistory() {
        const data = await this.request('/api/v1/doris/drift?limit=30', 'drift');
        this._cache.drift = data;
        return data;
    }

    async getDriftEvents() {
        const data = await this.request('/api/v1/doris/drift-events?limit=30', 'drift-events');
        this._cache.driftEvents = data;
        return data;
    }

    async getFeedbackStats() {
        const data = await this.request('/api/v1/doris/feedback-stats?limit=30', 'feedback-stats');
        this._cache.feedbackStats = data;
        return data;
    }

    async getModelStatus() {
        const data = await this.request('/api/v1/doris/models', 'models');
        this._cache.models = data;
        return data;
    }

    async getFraudTypes() {
        const data = await this.request('/api/v1/doris/fraud-types', 'fraud-types');
        return data || [];
    }

    async getDetectionSources() {
        const data = await this.request('/api/v1/doris/detection-sources', 'detection-sources');
        return data || [];
    }

    async getMlAnomalyCount() {
        const data = await this.request('/api/v1/ml-anomaly-count', 'ml-anomaly-count');
        return data?.count || 0;
    }

    // ============================================================
    // Sync wrappers — return cached data instantly (dashboard uses these)
    // ============================================================
    getTimeSeriesState() {
        if (this._fallbackState) {
            const s = this._fallbackState;
            return {
                totalNormal: s.totalNormal,
                totalFraud: s.totalFraud,
                totalAmount: s.totalAmount,
                fraudAmount: s.fraudAmount,
                alertCount: s.alertStream.length,
                elapsedSeconds: s.elapsedSeconds
            };
        }
        const m = this._cache.metrics;
        if (m) {
            return {
                totalNormal: (m.total_processed || 0) - (m.total_fraud || 0),
                totalFraud: m.total_fraud || 0,
                totalAmount: m.total_amount || m.grand_total || ((m.total_processed || 0) * 5000),
                fraudAmount: m.total_fraud_amount || m.fraud_amount || ((m.total_fraud || 0) * 50000),
                alertCount: m.total_fraud || 0,
                elapsedSeconds: 0
            };
        }
        return { totalNormal: 0, totalFraud: 0, totalAmount: 0, fraudAmount: 0, alertCount: 0, elapsedSeconds: 0 };
    }

    getRules() {
        // Map fraud type distribution from Doris to rule hit counts
        const hits = {};
        if (this._cache.fraudTypes) {
            for (const t of this._cache.fraudTypes) {
                hits[t.name] = t.value;
            }
        } else if (this._fallbackState) {
            // In demo mode, count alerts by fraud type from the alert stream
            const stream = this._fallbackState.alertStream || [];
            for (const alert of stream) {
                hits[alert.fraud_type] = (hits[alert.fraud_type] || 0) + 1;
            }
        }
        return this._rules.map(r => ({ ...r, hits: hits[r.name] || 0 }));
    }
    updateRule(id, data) {
        const rule = this._rules.find(r => r.id === id);
        if (rule && data.enabled !== undefined) rule.enabled = data.enabled;
        return { success: true };
    }

    startSimulation() { /* no-op — replaced by Doris polling */ }
    stopSimulation() { /* no-op */ }

    // Legacy aliases
    getHealth() { return { status: this._backendHealthy === false ? 'degraded' : 'healthy' }; }
    getAlerts() { return this._cache.alerts || []; }
    async postFeedback(alertId, feedback) {
        // Use fetch directly since request() only supports GET
        const url = `${this.base}/api/v1/feedback`;
        const headers = { 'Content-Type': 'application/json' };
        if (this._token) headers['Authorization'] = `Bearer ${this._token}`;
        try {
            const res = await fetch(url, {
                method: 'POST',
                headers,
                body: JSON.stringify({
                    alert_id: alertId,
                    feedback: feedback,
                    corrected_fraud_type: null,
                    confidence_adjustment: null,
                    comment: '',
                }),
                signal: AbortSignal.timeout(API_CONFIG.timeout),
            });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            return await res.json();
        } catch (e) {
            console.warn('[API] Feedback POST failed:', e.message);
            return { success: false, error: e.message };
        }
    }
    getDriftStatus() { return { status: 'available', alerts: 0 }; }

    // ============================================================
    // Auth
    // ============================================================
    async login(username, password) {
        const url = `${this.base}/api/v1/auth/login`;
        const res = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password }),
            signal: AbortSignal.timeout(API_CONFIG.timeout),
        });
        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            throw new Error(err.detail || '登录失败');
        }
        const data = await res.json();
        this._token = data.access_token;
        this._user = data.user;
        localStorage.setItem('auth_token', data.access_token);
        return data;
    }

    logout() {
        this._token = null;
        this._user = null;
        localStorage.removeItem('auth_token');
    }

    isLoggedIn() {
        return !!this._token;
    }

    getCurrentUser() {
        return this._user;
    }

    // ============================================================
    // Retrain
    // ============================================================
    async triggerRetrain(reason = 'manual') {
        const url = `${this.base}/api/v1/models/retrain`;
        const headers = { 'Content-Type': 'application/json' };
        if (this._token) headers['Authorization'] = `Bearer ${this._token}`;
        const res = await fetch(url, {
            method: 'POST',
            headers,
            body: JSON.stringify({ reason }),
            signal: AbortSignal.timeout(API_CONFIG.timeout),
        });
        if (!res.ok) throw new Error(`重训练请求失败: ${res.status}`);
        return await res.json();
    }

    async getRetrainProgress() {
        const url = `${this.base}/api/v1/models/retrain/progress`;
        const headers = {};
        if (this._token) headers['Authorization'] = `Bearer ${this._token}`;
        try {
            const res = await fetch(url, {
                headers,
                signal: AbortSignal.timeout(5000),
            });
            if (!res.ok) return { status: 'idle', progress: null };
            return await res.json();
        } catch {
            return { status: 'idle', progress: null };
        }
    }

    async getRetrainHistory() {
        const url = `${this.base}/api/v1/models/history`;
        const headers = {};
        if (this._token) headers['Authorization'] = `Bearer ${this._token}`;
        const res = await fetch(url, {
            headers,
            signal: AbortSignal.timeout(API_CONFIG.timeout),
        });
        if (!res.ok) return { history: [] };
        return await res.json();
    }

    async getModelInfo() {
        const url = `${this.base}/api/v1/models/info`;
        const headers = {};
        if (this._token) headers['Authorization'] = `Bearer ${this._token}`;
        const res = await fetch(url, {
            headers,
            signal: AbortSignal.timeout(API_CONFIG.timeout),
        });
        if (!res.ok) return null;
        return await res.json();
    }

    // ============================================================
    // 检测分类开关
    // ============================================================
    async getCategories() {
        const url = `${this.base}/api/v1/categories`;
        const headers = {};
        if (this._token) headers['Authorization'] = `Bearer ${this._token}`;
        try {
            const res = await fetch(url, { headers, signal: AbortSignal.timeout(API_CONFIG.timeout) });
            if (!res.ok) return [];
            return await res.json();
        } catch (e) {
            console.warn('[API] 获取分类失败:', e.message);
            return [];
        }
    }

    async toggleCategory(name, enabled) {
        const url = `${this.base}/api/v1/categories/${encodeURIComponent(name)}`;
        const headers = { 'Content-Type': 'application/json' };
        if (this._token) headers['Authorization'] = `Bearer ${this._token}`;
        try {
            const res = await fetch(url, {
                method: 'PUT',
                headers,
                body: JSON.stringify({ enabled }),
                signal: AbortSignal.timeout(API_CONFIG.timeout),
            });
            if (!res.ok) return { success: false };
            return await res.json();
        } catch (e) {
            console.warn('[API] 切换分类失败:', e.message);
            return { success: false };
        }
    }
}

const api = new ApiClient();
