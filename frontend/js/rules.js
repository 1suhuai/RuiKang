/**
 * Rules Page Module
 * Detection categories configuration and management
 * Shows 5 detection layers with toggle switches
 */

const RulesPage = {
    categories: [],
    _refreshTimer: null,

    async init() {
        await this.loadCategories();
        this.startRefresh();
    },

    startRefresh() {
        if (this._refreshTimer) clearInterval(this._refreshTimer);
        this._refreshTimer = setInterval(() => this.loadCategories(), 5000);
    },

    stopRefresh() {
        if (this._refreshTimer) { clearInterval(this._refreshTimer); this._refreshTimer = null; }
    },

    async loadCategories() {
        try {
            this.categories = await api.getCategories();
            this.renderCategories();
        } catch (err) {
            console.warn('Failed to load categories:', err);
        }
    },

    renderCategories() {
        const container = document.getElementById('rulesGrid');
        if (!container) return;

        container.innerHTML = this.categories.map(cat => `
            <div class="rule-card" data-name="${cat.name}">
                <div class="rule-card-header">
                    <span class="rule-name">${cat.name}</span>
                    <label class="rule-switch">
                        <input type="checkbox" ${cat.enabled ? 'checked' : ''} onchange="RulesPage.toggleCategory('${cat.name}', this.checked)">
                        <span class="rule-slider"></span>
                    </label>
                </div>
                <p class="rule-desc">${cat.description}</p>
                <div class="rule-stats">
                    <div>状态: <span class="rule-stat-value" style="color: ${cat.enabled ? '#10b981' : '#6b7294'}">${cat.enabled ? '已启用' : '已禁用'}</span></div>
                </div>
            </div>
        `).join('');
    },

    async loadCategories() {
        try {
            this.categories = await api.getCategories();
            this.renderCategories();
        } catch (err) {
            console.warn('Failed to load categories:', err);
        }
    },

    async toggleCategory(name, enabled) {
        try {
            const result = await api.toggleCategory(name, enabled);
            if (result.success) {
                const cat = this.categories.find(c => c.name === name);
                if (cat) cat.enabled = enabled;
                this.renderCategories();
                // Reload alerts to apply filter
                if (typeof AlertsPage !== 'undefined') {
                    AlertsPage.loadAlerts();
                }
                if (typeof DashboardPage !== 'undefined') {
                    DashboardPage.loadAlerts();
                }
            }
        } catch (err) {
            console.warn('Failed to toggle category:', err);
            // Revert UI
            const cat = this.categories.find(c => c.name === name);
            if (cat) {
                cat.enabled = !enabled;
                this.renderCategories();
            }
        }
    }
};
