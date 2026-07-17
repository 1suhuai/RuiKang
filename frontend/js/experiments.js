/**
 * Experiments Page Module
 * Ablation experiment visualization and analysis
 */

const ExperimentsPage = {
    experiments: [],

    async init() {
        await this.loadExperiments();
        this.renderSummary();
        this.renderCharts();
        this.renderTable();
        this.renderContributions();
    },

    async loadExperiments() {
        // Demo ablation experiment data from project results
        this.experiments = [
            { name: '四层全开', cep: true, sql: true, graph: true, ml: true, precision: 1.0, recall: 0.978, f1: 0.989, accuracy: 0.991, latency: 70, throughput: 8500 },
            { name: 'CEP+SQL+Graph+ML', cep: true, sql: true, graph: true, ml: true, precision: 0.996, recall: 0.974, f1: 0.985, accuracy: 0.988, latency: 65, throughput: 9200 },
            { name: 'CEP+SQL+ML', cep: true, sql: true, graph: false, ml: true, precision: 0.992, recall: 0.965, f1: 0.978, accuracy: 0.982, latency: 55, throughput: 10500 },
            { name: 'CEP+ML', cep: true, sql: false, graph: false, ml: true, precision: 0.985, recall: 0.946, f1: 0.965, accuracy: 0.970, latency: 45, throughput: 12000 },
            { name: '仅ML', cep: false, sql: false, graph: false, ml: true, precision: 0.962, recall: 0.910, f1: 0.935, accuracy: 0.942, latency: 35, throughput: 15000 },
            { name: '仅CEP', cep: true, sql: false, graph: false, ml: false, precision: 0.920, recall: 0.850, f1: 0.884, accuracy: 0.896, latency: 25, throughput: 18000 }
        ];
    },

    renderSummary() {
        const container = document.getElementById('experimentSummary');
        if (!container) return;

        const best = this.experiments[0];
        const summary = [
            { label: '最佳F1 Score', value: best.f1.toFixed(3), desc: '四层全开配置' },
            { label: '最佳Precision', value: best.precision.toFixed(3), desc: '零误报' },
            { label: '最佳Recall', value: best.recall.toFixed(3), desc: '高召回率' },
            { label: '系统延迟', value: best.latency + 'ms', desc: '实时检测' }
        ];

        container.innerHTML = summary.map(s => `
            <div class="summary-card">
                <div class="summary-label">${s.label}</div>
                <div class="summary-value">${s.value}</div>
                <div class="summary-desc">${s.desc}</div>
            </div>
        `).join('');
    },

    renderCharts() {
        Charts.renderExperimentF1Chart({ configs: this.experiments.map(e => ({ name: e.name, f1: e.f1 })) });
        Charts.renderExperimentRadarChart();
        Charts.renderExperimentWaterfallChart();
    },

    renderTable() {
        const container = document.getElementById('experimentTableBody');
        if (!container) return;

        container.innerHTML = this.experiments.map(e => `
            <tr>
                <td>${e.name}</td>
                <td>${e.precision.toFixed(3)}</td>
                <td>${e.recall.toFixed(3)}</td>
                <td><strong>${e.f1.toFixed(3)}</strong></td>
                <td>${e.accuracy.toFixed(3)}</td>
                <td>${e.latency}</td>
                <td>${e.throughput.toLocaleString()}</td>
            </tr>
        `).join('');
    },

    renderContributions() {
        const container = document.getElementById('contributionGrid');
        if (!container) return;

        // Calculate marginal contributions
        const fullF1 = 0.989;
        const noGraphF1 = 0.978;
        const noSqlF1 = 0.965;
        const noCepF1 = 0.935;
        const onlyCepF1 = 0.884;

        const contributions = [
            { layer: 'CEP规则', class: 'layer-cep', delta: (onlyCepF1 - 0.85).toFixed(3), f1: onlyCepF1.toFixed(3), desc: '规则引擎基础检测' },
            { layer: 'SQL跨键', class: 'layer-sql', delta: (noSqlF1 - noCepF1).toFixed(3), f1: noSqlF1.toFixed(3), desc: '跨账户关联分析' },
            { layer: '图分析', class: 'layer-graph', delta: (noGraphF1 - noSqlF1).toFixed(3), f1: noGraphF1.toFixed(3), desc: '复杂关系网络挖掘' },
            { layer: 'ML集成', class: 'layer-ml', delta: (fullF1 - noGraphF1).toFixed(3), f1: fullF1.toFixed(3), desc: '5模型动态加权融合' }
        ];

        container.innerHTML = contributions.map(c => `
            <div class="contribution-card ${c.class}">
                <div class="contribution-label">${c.layer}</div>
                <div class="contribution-value">${c.f1}</div>
                <div class="contribution-desc">边际贡献: +${c.delta} F1</div>
                <div class="contribution-desc">${c.desc}</div>
            </div>
        `).join('');
    }
};
