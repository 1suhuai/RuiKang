/**
 * Main Application Module
 * Page navigation, settings, and initialization
 */

const App = {
    currentPage: 'dashboard',
    refreshInterval: 2000,
    demoMode: true,
    _pageInitialized: {},

    init() {
        this.loadSettings();
        this.bindNavigation();
        this.bindSettings();
        this.bindLogout();
        this.bindWindowEvents();
        this.currentPage = 'dashboard';
        this.initCurrentPage();
        this.updateLoginUI();
        console.log('RuiKang Dashboard v2.0 initialized');
    },

    loadSettings() {
        const savedInterval = localStorage.getItem('refreshInterval');
        if (savedInterval) this.refreshInterval = parseInt(savedInterval) * 1000;

        this.demoMode = false;
        api.setDemoMode(false);

        const savedUrl = localStorage.getItem('apiBaseUrl');
        const correctUrl = 'http://localhost:8002';
        if (savedUrl && savedUrl !== correctUrl) {
            localStorage.removeItem('apiBaseUrl');
        }
        if (savedUrl === correctUrl) api.setBaseUrl(savedUrl);

        const intervalInput = document.getElementById('settingsRefreshInterval');
        if (intervalInput) intervalInput.value = this.refreshInterval / 1000;

        const demoToggle = document.getElementById('settingsDemoMode');
        if (demoToggle) demoToggle.checked = this.demoMode;

        const urlInput = document.getElementById('settingsApiUrl');
        if (urlInput) urlInput.value = api.base;
    },

    bindNavigation() {
        document.querySelectorAll('.nav-item').forEach(item => {
            item.addEventListener('click', () => {
                const page = item.dataset.page;
                if (page !== this.currentPage) {
                    this.navigateTo(page);
                }
            });
        });
    },

    navigateTo(page) {
        document.querySelectorAll('.nav-item').forEach(item => item.classList.remove('active'));
        document.querySelector(`.nav-item[data-page="${page}"]`)?.classList.add('active');

        document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
        document.getElementById(`${page}-page`)?.classList.add('active');

        const pageNames = {
            dashboard: '实时监控',
            alerts: '告警管理',
            models: '模型监控',
            rules: '规则配置',
            experiments: '消融实验'
        };
        document.getElementById('currentPage').textContent = pageNames[page] || page;

        // Manage simulation lifecycle: keep running always so data keeps accumulating
        // regardless of which page the user is viewing
        this.currentPage = page;
        this.initCurrentPage();
    },

    initCurrentPage() {
        if (this._pageInitialized[this.currentPage]) {
            // Already initialized - just trigger refresh if the page has auto-refresh
            switch (this.currentPage) {
                case 'dashboard':
                    if (Dashboard.updateKPIs) Dashboard.updateKPIs();
                    if (Dashboard.updateAlertTable) Dashboard.updateAlertTable();
                    break;
                case 'models':
                    if (ModelsPage.loadMetrics) ModelsPage.loadMetrics();
                    if (ModelsPage.loadDriftHistory) ModelsPage.loadDriftHistory();
                    break;
                case 'alerts':
                    if (AlertsPage.loadAlerts) AlertsPage.loadAlerts();
                    break;
            }
            requestAnimationFrame(() => Charts.resize());
            return;
        }
        this._pageInitialized[this.currentPage] = true;

        switch (this.currentPage) {
            case 'dashboard':
                Dashboard.init();
                break;
            case 'alerts':
                AlertsPage.init();
                break;
            case 'models':
                ModelsPage.init();
                break;
            case 'rules':
                RulesPage.init();
                break;
            case 'experiments':
                ExperimentsPage.init();
                break;
        }

        requestAnimationFrame(() => Charts.resize());
    },

    bindSettings() {
        document.getElementById('settingsBtn')?.addEventListener('click', () => {
            document.getElementById('settingsModal').classList.add('active');
        });

        document.getElementById('closeSettingsModal')?.addEventListener('click', () => {
            document.getElementById('settingsModal').classList.remove('active');
        });

        document.getElementById('saveSettingsBtn')?.addEventListener('click', () => {
            this.saveSettings();
            document.getElementById('settingsModal').classList.remove('active');
        });

        document.getElementById('refreshBtn')?.addEventListener('click', () => {
            Dashboard.updateKPIs();
            Dashboard.updateAlertTable();
        });
    },

    saveSettings() {
        const url = document.getElementById('settingsApiUrl').value;
        const interval = parseInt(document.getElementById('settingsRefreshInterval').value) || 2;
        const demo = document.getElementById('settingsDemoMode').checked;

        localStorage.setItem('apiBaseUrl', url);
        localStorage.setItem('refreshInterval', interval);
        localStorage.setItem('demoMode', demo);

        api.setBaseUrl(url);
        api.setDemoMode(demo);
        this.demoMode = demo;

        Dashboard.refreshInterval = interval * 1000;
        Dashboard.startAutoRefresh();
    },

    bindWindowEvents() {
        window.addEventListener('resize', () => Charts.resize());

        document.querySelectorAll('.modal-overlay').forEach(overlay => {
            overlay.addEventListener('click', (e) => {
                if (e.target === overlay) overlay.classList.remove('active');
            });
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                document.querySelectorAll('.modal-overlay.active').forEach(m => m.classList.remove('active'));
            }
        });
    },

    bindLogout() {
        const loginBtn = document.getElementById('loginBtn');
        loginBtn?.addEventListener('click', () => {
            if (api.isLoggedIn()) {
                api.logout();
                window.location.href = 'login.html';
            }
        });
    },

    updateLoginUI() {
        const loginBtn = document.getElementById('loginBtn');
        const loginBtnText = document.getElementById('loginBtnText');
        if (!loginBtn) return;

        if (api.isLoggedIn()) {
            const user = api.getCurrentUser();
            loginBtn.classList.add('logged-in');
            loginBtnText.textContent = (user && user.id) || '已登录';
            loginBtn.title = '点击退出登录';
        } else {
            loginBtn.classList.remove('logged-in');
            loginBtnText.textContent = '登录';
            loginBtn.title = '登录';
        }
    }
};

document.addEventListener('DOMContentLoaded', () => App.init());
