/**
 * Charts Module
 * Handles all ECharts rendering and updates
 * Dark Terminal Theme
 */

const Charts = {
    instances: {},

    init(chartId, options) {
        const dom = document.getElementById(chartId);
        if (!dom) return null;

        if (this.instances[chartId]) {
            this.instances[chartId].dispose();
        }

        const chart = echarts.init(dom, null, { renderer: 'canvas' });
        chart.setOption(options, true);
        this.instances[chartId] = chart;
        return chart;
    },

    update(chartId, options) {
        const chart = this.instances[chartId];
        if (chart) {
            chart.setOption(options);
        }
    },

    resize() {
        Object.values(this.instances).forEach(c => c.resize());
    },

    getTheme() {
        return {
            textStyle: { fontFamily: "'SF Mono', 'Cascadia Code', 'Consolas', monospace" }
        };
    },

    // Color palette - Dark terminal theme
    colors: {
        blue: '#58a6ff',
        cyan: '#3fb950',
        green: '#3fb950',
        orange: '#d29922',
        red: '#f85149',
        purple: '#bc8cff',
        pink: '#f778ba',
        blueArea: 'rgba(88,166,255,0.12)',
        redArea: 'rgba(248,81,73,0.10)',
        textPrimary: '#e6edf3',
        textSecondary: '#8b949e',
        textMuted: '#6e7681',
        borderLight: '#30363d',
        bgTertiary: '#1c2128',
        bgCard: '#161b22',
    },

    // Initial render traffic chart
    renderTrafficChart(data = {}) {
        const timestamps = data.timestamps || [];
        const normal = data.normal || [];
        const fraud = data.fraud || [];
        const c = this.colors;

        // 120个点，每20秒显示一个标签
        const interval = Math.max(1, Math.floor(timestamps.length / 6));

        const option = {
            tooltip: { trigger: 'axis', axisPointer: { type: 'cross' }, backgroundColor: '#1c2128', borderColor: '#30363d', textStyle: { color: '#e6edf3', fontSize: 11 } },
            legend: { data: ['正常交易', '欺诈检测'], top: 0, textStyle: { color: c.textSecondary, fontSize: 11 } },
            grid: { top: 36, right: 16, bottom: 28, left: 50 },
            xAxis: {
                type: 'category',
                data: timestamps,
                axisLine: { lineStyle: { color: c.borderLight } },
                axisTick: { show: false },
                axisLabel: {
                    color: c.textMuted,
                    fontSize: 10,
                    interval: interval - 1,
                    rotate: 30
                }
            },
            yAxis: [
                {
                    type: 'value',
                    name: '正常交易',
                    nameTextStyle: { color: c.textMuted, fontSize: 10 },
                    axisLine: { show: false },
                    axisTick: { show: false },
                    splitLine: { lineStyle: { color: c.borderLight, type: 'dashed' } },
                    axisLabel: { color: c.textMuted, fontSize: 10 }
                },
                {
                    type: 'value',
                    name: '欺诈',
                    nameTextStyle: { color: c.textMuted, fontSize: 10 },
                    axisLine: { show: false },
                    axisTick: { show: false },
                    splitLine: { show: false },
                    axisLabel: { color: c.textMuted, fontSize: 10 }
                }
            ],
            series: [
                {
                    name: '正常交易',
                    type: 'line',
                    smooth: true,
                    data: normal,
                    areaStyle: { color: 'rgba(88,166,255,0.08)' },
                    lineStyle: { color: c.blue, width: 1.5 },
                    itemStyle: { color: c.blue },
                    showSymbol: false
                },
                {
                    name: '欺诈检测',
                    type: 'bar',
                    yAxisIndex: 1,
                    data: fraud,
                    barWidth: '60%',
                    itemStyle: {
                        color: 'rgba(248,81,73,0.7)',
                        borderRadius: [1, 1, 0, 0]
                    }
                }
            ]
        };

        this.init('chartTraffic', option);
    },

    updateTrafficChart(data = {}) {
        const chart = this.instances['chartTraffic'];
        if (!chart) return;
        if (!data.timestamps || !data.normal || !data.fraud) return;

        chart.setOption({
            xAxis: { data: data.timestamps },
            series: [
                { data: data.normal },
                { data: data.fraud }
            ]
        }, false);
    },

    // Chart: Alert Types Pie
    renderAlertTypesChart(data = {}) {
        const types = data.types;
        if (!types || types.length === 0) return;
        const c = this.colors;

        const existing = this.instances['chartAlertTypes'];
        if (existing) {
            existing.setOption({
                legend: { data: types.map(t => t.name) },
                series: [{ data: types }]
            }, false);
            return;
        }

        const option = {
            tooltip: { trigger: 'item', formatter: '{b}: {c}条 ({d}%)', backgroundColor: '#1c2128', borderColor: '#30363d', textStyle: { color: '#e6edf3', fontSize: 11 } },
            legend: {
                orient: 'vertical',
                right: 0,
                top: 10,
                bottom: 10,
                textStyle: { color: c.textSecondary, fontSize: 10 },
                itemWidth: 8,
                itemHeight: 8,
                itemGap: 4,
                type: 'scroll',
                pageButtonItemGap: 2,
                pageIconColor: c.blue,
                pageIconInactiveColor: '#30363d',
                pageTextStyle: { color: c.textMuted, fontSize: 9 }
            },
            series: [{
                name: '告警类型',
                type: 'pie',
                radius: ['42%', '68%'],
                center: ['30%', '50%'],
                avoidLabelOverlap: false,
                itemStyle: { borderRadius: 1, borderColor: c.bgCard, borderWidth: 1 },
                label: { show: false },
                emphasis: {
                    label: { show: true, fontSize: 11, fontWeight: 'bold', color: c.textPrimary }
                },
                data: types,
                color: [c.blue, c.cyan, c.purple, c.orange, c.green, c.pink, c.red, '#39d353', '#e3b341', '#8b949e', '#f778ba']
            }]
        };

        this.init('chartAlertTypes', option);
    },

    // Chart: Detection Layers
    renderDetectionLayersChart(data = {}) {
        const layers = data.layers;
        if (!layers || layers.length === 0) return;
        const c = this.colors;

        const existing = this.instances['chartDetectionLayers'];
        if (existing) {
            existing.setOption({
                yAxis: { data: layers.map(l => l.name).reverse() },
                series: [{ data: layers.map(l => l.value).reverse() }]
            }, false);
            return;
        }

        const option = {
            tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, backgroundColor: '#1c2128', borderColor: '#30363d', textStyle: { color: '#e6edf3', fontSize: 11 } },
            grid: { top: 8, right: 36, bottom: 8, left: 100 },
            xAxis: {
                type: 'value',
                axisLine: { show: false },
                axisTick: { show: false },
                splitLine: { lineStyle: { color: c.borderLight, type: 'dashed' } },
                axisLabel: { color: c.textMuted, fontSize: 10 }
            },
            yAxis: {
                type: 'category',
                data: layers.map(l => l.name).reverse(),
                axisLine: { show: false },
                axisTick: { show: false },
                axisLabel: { color: c.textSecondary, fontSize: 11, width: 90, overflow: 'truncate' }
            },
            series: [{
                type: 'bar',
                data: layers.map(l => l.value).reverse(),
                barWidth: '60%',
                barMaxWidth: 24,
                itemStyle: {
                    color: c.blue,
                    borderRadius: [0, 1, 1, 0]
                },
                label: { show: true, position: 'right', color: c.textSecondary, fontSize: 10 }
            }]
        };

        this.init('chartDetectionLayers', option);
    },

    // Chart: Network Graph
    renderNetworkChart(data = {}) {
        const nodes = data.nodes || this.generateNetworkNodes(15);
        const links = data.links || this.generateNetworkLinks(nodes);
        const c = this.colors;

        const existing = this.instances['chartNetwork'];
        if (existing) {
            existing.setOption({
                series: [{ data: nodes, links: links }]
            }, false);
            return;
        }

        const option = {
            tooltip: { backgroundColor: '#1c2128', borderColor: '#30363d', textStyle: { color: '#e6edf3', fontSize: 11 } },
            series: [{
                type: 'graph',
                layout: 'force',
                force: { repulsion: 300, edgeLength: [80, 150] },
                data: nodes,
                links: links,
                roam: true,
                label: { show: true, position: 'right', fontSize: 9, color: c.textSecondary },
                lineStyle: { color: 'rgba(88,166,255,0.3)', curveness: 0.2, width: 1, opacity: 0.5 },
                emphasis: {
                    focus: 'adjacency',
                    lineStyle: { width: 3 }
                }
            }]
        };

        this.init('chartNetwork', option);
    },

    // Chart: Model Performance Radar
    renderModelPerfChart(data = {}) {
        const value = [data.precision || 0.99, data.recall || 0.98, data.f1 || 0.99,
                        data.accuracy || 0.99, data.auc || 0.99, data.ks || 0.95];
        const c = this.colors;

        const existing = this.instances['chartModelPerf'];
        if (existing) {
            existing.setOption({
                series: [{ data: [{ value, name: '当前模型' }] }]
            }, false);
            return;
        }

        const option = {
            radar: {
                indicator: [
                    { name: 'Precision', max: 1 },
                    { name: 'Recall', max: 1 },
                    { name: 'F1-Score', max: 1 },
                    { name: '准确率', max: 1 },
                    { name: 'AUC', max: 1 },
                    { name: 'KS', max: 1 }
                ],
                axisName: { color: c.textSecondary, fontSize: 10 },
                splitLine: { lineStyle: { color: c.borderLight } },
                splitArea: { areaStyle: { color: ['rgba(255,255,255,0.02)', 'rgba(255,255,255,0.04)'] } }
            },
            series: [{
                type: 'radar',
                data: [{
                    value: value,
                    name: '当前模型',
                    areaStyle: { color: 'rgba(88,166,255,0.08)' },
                    lineStyle: { color: c.blue, width: 1.5 },
                    itemStyle: { color: c.blue }
                }]
            }]
        };

        this.init('chartModelPerf', option);
    },

    renderModelTrainingChart(data = {}) {
        const history = data.training_history || [];
        const c = this.colors;

        if (history.length === 0) return;

        const samples = history.map(h => h.samples);
        const f1 = history.map(h => h.f1 || 0);
        const precision = history.map(h => h.precision || 0);
        const recall = history.map(h => h.recall || 0);

        const option = {
            tooltip: {
                trigger: 'axis',
                backgroundColor: '#1c2128',
                borderColor: '#30363d',
                textStyle: { color: '#e6edf3', fontSize: 11 }
            },
            legend: {
                data: ['F1-Score', 'Precision', 'Recall'],
                top: 0,
                textStyle: { color: c.textSecondary, fontSize: 11 }
            },
            grid: { top: 36, right: 16, bottom: 28, left: 50 },
            xAxis: {
                type: 'category',
                name: '训练样本数',
                nameTextStyle: { color: c.textMuted, fontSize: 10 },
                data: samples,
                axisLine: { lineStyle: { color: c.borderLight } },
                axisTick: { show: false },
                axisLabel: {
                    color: c.textMuted,
                    fontSize: 10,
                    interval: Math.max(0, Math.floor(samples.length / 6) - 1)
                }
            },
            yAxis: {
                type: 'value',
                name: '分数',
                nameTextStyle: { color: c.textMuted, fontSize: 10 },
                min: 0,
                max: 1,
                axisLine: { show: false },
                axisTick: { show: false },
                splitLine: { lineStyle: { color: c.borderLight, type: 'dashed' } },
                axisLabel: { color: c.textMuted, fontSize: 10 }
            },
            series: [
                {
                    name: 'F1-Score',
                    type: 'line',
                    smooth: true,
                    data: f1,
                    lineStyle: { color: '#58a6ff', width: 2 },
                    itemStyle: { color: '#58a6ff' },
                    showSymbol: false
                },
                {
                    name: 'Precision',
                    type: 'line',
                    smooth: true,
                    data: precision,
                    lineStyle: { color: '#3fb950', width: 1.5 },
                    itemStyle: { color: '#3fb950' },
                    showSymbol: false
                },
                {
                    name: 'Recall',
                    type: 'line',
                    smooth: true,
                    data: recall,
                    lineStyle: { color: '#f0883e', width: 1.5 },
                    itemStyle: { color: '#f0883e' },
                    showSymbol: false
                }
            ]
        };

        this.init('chartModelTraining', option);
    },

    // Chart: Confidence Distribution
    renderConfidenceChart(data = {}) {
        const bins = data.bins;
        if (!bins) return;
        const c = this.colors;

        const existing = this.instances['chartConfidence'];
        if (existing) {
            existing.setOption({
                xAxis: { data: bins.map(b => b.range) },
                series: [{
                    data: bins.map((b, i) => ({
                        value: b.count,
                        itemStyle: {
                            color: i >= 7 ? 'rgba(248,81,73,0.7)' : 'rgba(88,166,255,0.7)',
                            borderRadius: [1, 1, 0, 0]
                        }
                    }))
                }]
            }, false);
            return;
        }

        const option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'shadow' },
                backgroundColor: '#1c2128', borderColor: '#30363d', textStyle: { color: '#e6edf3', fontSize: 11 }
            },
            grid: { top: 16, right: 16, bottom: 28, left: 50 },
            xAxis: {
                type: 'category',
                data: bins.map(b => b.range),
                axisLine: { lineStyle: { color: c.borderLight } },
                axisTick: { show: false },
                axisLabel: { color: c.textMuted, fontSize: 9, rotate: 30 }
            },
            yAxis: {
                type: 'value',
                axisLine: { show: false },
                axisTick: { show: false },
                splitLine: { lineStyle: { color: c.borderLight, type: 'dashed' } },
                axisLabel: { color: c.textMuted, fontSize: 10 }
            },
            series: [{
                type: 'bar',
                data: bins.map((b, i) => ({
                    value: b.count,
                    itemStyle: {
                        color: i >= 7 ? 'rgba(248,81,73,0.7)' : 'rgba(88,166,255,0.7)',
                        borderRadius: [1, 1, 0, 0]
                    }
                })),
                barWidth: '60%'
            }]
        };

        this.init('chartConfidence', option);
    },

    // Chart: Geo Heatmap
    renderGeoHeatChart(data = {}) {
        const geoData = (data.geo || []).slice(0, 10);
        if (!geoData || geoData.length === 0) return;
        const c = this.colors;

        const existing = this.instances['chartGeoHeat'];
        if (existing) {
            existing.setOption({
                xAxis: { data: geoData.map(g => g.name) },
                series: [{
                    data: geoData.map(g => ({
                        value: g.value,
                        itemStyle: {
                            color: 'rgba(210,153,34,0.7)',
                            borderRadius: [1, 1, 0, 0]
                        }
                    }))
                }]
            }, false);
            return;
        }

        const option = {
            tooltip: { trigger: 'item', formatter: '{b}: {c}条告警', backgroundColor: '#1c2128', borderColor: '#30363d', textStyle: { color: '#e6edf3', fontSize: 11 } },
            xAxis: {
                type: 'category',
                data: geoData.map(g => g.name),
                axisLine: { lineStyle: { color: c.borderLight } },
                axisTick: { show: false },
                axisLabel: { color: c.textSecondary, fontSize: 10 }
            },
            yAxis: {
                type: 'value',
                axisLine: { show: false },
                axisTick: { show: false },
                splitLine: { lineStyle: { color: c.borderLight, type: 'dashed' } },
                axisLabel: { color: c.textMuted, fontSize: 10 }
            },
            series: [{
                type: 'bar',
                data: geoData.map(g => ({
                    value: g.value,
                    itemStyle: {
                        color: 'rgba(210,153,34,0.7)',
                        borderRadius: [1, 1, 0, 0]
                    }
                })),
                barWidth: '50%',
                label: { show: true, position: 'top', color: c.textSecondary, fontSize: 10 }
            }]
        };

        this.init('chartGeoHeat', option);
    },

    // Chart: Drift History
    renderDriftHistoryChart(data = {}) {
        const timestamps = data.timestamps || this.generateTimestamps(30);
        const amountPValue = data.amountPValue || Array(30).fill(0).map(() => 0.1 + Math.random() * 0.8);
        const f1Scores = data.f1Scores || Array(30).fill(0).map(() => 0.96 + Math.random() * 0.04);
        const c = this.colors;

        const existing = this.instances['chartDriftHistory'];
        if (existing) {
            existing.setOption({
                xAxis: { data: timestamps },
                series: [
                    { data: amountPValue },
                    { data: f1Scores }
                ]
            }, false);
            return;
        }

        const option = {
            tooltip: {
                trigger: 'axis',
                backgroundColor: '#1c2128', borderColor: '#30363d', textStyle: { color: '#e6edf3', fontSize: 11 },
                formatter: function(params) {
                    let result = params[0].axisValue + '<br/>';
                    params.forEach(p => {
                        result += p.marker + p.seriesName + ': ' + (p.value != null ? Number(p.value).toFixed(2) : '-') + '<br/>';
                    });
                    return result;
                }
            },
            legend: { data: ['金额p-value', 'F1 Score'], top: 0, textStyle: { color: c.textSecondary, fontSize: 11 } },
            grid: { top: 36, right: 50, bottom: 28, left: 50 },
            xAxis: {
                type: 'category',
                data: timestamps,
                axisLine: { lineStyle: { color: c.borderLight } },
                axisTick: { show: false },
                axisLabel: { color: c.textMuted, fontSize: 10 }
            },
            yAxis: [
                {
                    type: 'value',
                    name: 'p-value',
                    min: 0,
                    max: 1,
                    nameTextStyle: { color: c.textMuted, fontSize: 10 },
                    axisLine: { show: false },
                    axisTick: { show: false },
                    splitLine: { lineStyle: { color: c.borderLight, type: 'dashed' } },
                    axisLabel: { color: c.textMuted, fontSize: 10 }
                },
                {
                    type: 'value',
                    name: 'F1',
                    min: 0.9,
                    max: 1,
                    nameTextStyle: { color: c.textMuted, fontSize: 10 },
                    axisLine: { show: false },
                    axisTick: { show: false },
                    splitLine: { show: false },
                    axisLabel: { color: c.textMuted, fontSize: 10 }
                }
            ],
            series: [
                {
                    name: '金额p-value',
                    type: 'line',
                    smooth: true,
                    data: amountPValue,
                    lineStyle: { color: c.orange, width: 1.5 },
                    itemStyle: { color: c.orange },
                    areaStyle: { color: 'rgba(210,153,34,0.06)' },
                    showSymbol: false,
                    markLine: {
                        data: [{ yAxis: 0.05, name: '漂移阈值' }],
                        lineStyle: { color: c.red, type: 'dashed' },
                        label: { formatter: '阈值 0.05', color: c.red, fontSize: 10, position: 'end' }
                    }
                },
                {
                    name: 'F1 Score',
                    type: 'line',
                    yAxisIndex: 1,
                    smooth: true,
                    data: f1Scores,
                    lineStyle: { color: c.green, width: 1.5 },
                    itemStyle: { color: c.green },
                    showSymbol: false
                }
            ]
        };

        this.init('chartDriftHistory', option);
    },

    // Chart: Experiment F1 Comparison
    renderExperimentF1Chart(data = {}) {
        const configs = data.configs || [
            { name: '四层全开', f1: 0.989 },
            { name: 'CEP+SQL+Graph+ML', f1: 0.985 },
            { name: 'CEP+SQL+ML', f1: 0.978 },
            { name: 'CEP+ML', f1: 0.965 },
            { name: '仅ML', f1: 0.942 },
            { name: '仅CEP', f1: 0.856 }
        ];
        const c = this.colors;

        const existing = this.instances['chartExperimentF1'];
        if (existing) {
            existing.setOption({
                yAxis: { data: configs.map(c => c.name).reverse() },
                series: [{
                    data: configs.map(cfg => ({
                        value: cfg.f1,
                        itemStyle: {
                            color: cfg.name === '四层全开' ? c.green : c.blue,
                            borderRadius: [0, 1, 1, 0]
                        }
                    })).reverse()
                }]
            }, false);
            return;
        }

        const option = {
            tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, backgroundColor: '#1c2128', borderColor: '#30363d', textStyle: { color: '#e6edf3', fontSize: 11 } },
            grid: { top: 16, right: 16, bottom: 36, left: 110 },
            xAxis: {
                type: 'value',
                min: 0.8,
                max: 1,
                axisLine: { show: false },
                axisTick: { show: false },
                splitLine: { lineStyle: { color: c.borderLight, type: 'dashed' } },
                axisLabel: { color: c.textMuted, formatter: v => v.toFixed(2), fontSize: 10 }
            },
            yAxis: {
                type: 'category',
                data: configs.map(cfg => cfg.name).reverse(),
                axisLine: { show: false },
                axisTick: { show: false },
                axisLabel: { color: c.textSecondary, fontSize: 10 }
            },
            series: [{
                type: 'bar',
                data: configs.map(cfg => ({
                    value: cfg.f1,
                    itemStyle: {
                        color: cfg.name === '四层全开' ? c.green : c.blue,
                        borderRadius: [0, 1, 1, 0]
                    }
                })).reverse(),
                barWidth: '50%',
                label: { show: true, position: 'right', color: c.textSecondary, fontSize: 10, formatter: p => p.value.toFixed(3) }
            }]
        };

        this.init('chartExperimentF1', option);
    },

    // Chart: Experiment Radar
    renderExperimentRadarChart(data = {}) {
        const existing = this.instances['chartExperimentRadar'];
        if (existing) return;
        const c = this.colors;

        const option = {
            legend: {
                data: ['四层全开', '仅CEP', '仅ML'],
                orient: 'vertical',
                right: 8,
                top: 'center',
                textStyle: { color: c.textSecondary, fontSize: 10 }
            },
            radar: {
                indicator: [
                    { name: 'Precision', max: 1 },
                    { name: 'Recall', max: 1 },
                    { name: 'F1-Score', max: 1 },
                    { name: '低延迟', max: 1 },
                    { name: '模式覆盖', max: 1 }
                ],
                radius: '65%',
                center: ['42%', '50%'],
                axisName: { color: c.textSecondary, fontSize: 10 },
                splitLine: { lineStyle: { color: c.borderLight } },
                splitArea: { areaStyle: { color: ['rgba(255,255,255,0.02)', 'rgba(255,255,255,0.04)'] } }
            },
            series: [{
                type: 'radar',
                data: [
                    {
                        value: [1.0, 0.978, 0.989, 0.95, 1.0],
                        name: '四层全开',
                        areaStyle: { color: 'rgba(63,185,80,0.08)' },
                        lineStyle: { color: c.green },
                        itemStyle: { color: c.green }
                    },
                    {
                        value: [0.92, 0.85, 0.884, 0.98, 0.73],
                        name: '仅CEP',
                        areaStyle: { color: 'rgba(88,166,255,0.06)' },
                        lineStyle: { color: c.blue },
                        itemStyle: { color: c.blue }
                    },
                    {
                        value: [0.95, 0.92, 0.935, 0.90, 0.80],
                        name: '仅ML',
                        areaStyle: { color: 'rgba(188,140,255,0.06)' },
                        lineStyle: { color: c.purple },
                        itemStyle: { color: c.purple }
                    }
                ]
            }]
        };

        this.init('chartExperimentRadar', option);
    },

    // Chart: Experiment Waterfall
    renderExperimentWaterfallChart(data = {}) {
        const layers = [
            { name: '基线', value: 0.85 },
            { name: '+CEP', value: 0.06 },
            { name: '+SQL', value: 0.04 },
            { name: '+Graph', value: 0.02 },
            { name: '+ML', value: 0.019 }
        ];
        const c = this.colors;

        const existing = this.instances['chartExperimentWaterfall'];
        if (existing) return;

        const option = {
            tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, backgroundColor: '#1c2128', borderColor: '#30363d', textStyle: { color: '#e6edf3', fontSize: 11 } },
            grid: { top: 16, right: 24, bottom: 36, left: 50 },
            xAxis: {
                type: 'category',
                data: layers.map(l => l.name),
                axisLine: { lineStyle: { color: c.borderLight } },
                axisTick: { show: false },
                axisLabel: { color: c.textSecondary, fontSize: 10 }
            },
            yAxis: {
                type: 'value',
                name: 'F1 Score',
                min: 0.8,
                max: 1,
                nameTextStyle: { color: c.textMuted, fontSize: 10 },
                axisLine: { show: false },
                axisTick: { show: false },
                splitLine: { lineStyle: { color: c.borderLight, type: 'dashed' } },
                axisLabel: { color: c.textMuted, formatter: v => v.toFixed(2), fontSize: 10 }
            },
            series: [{
                type: 'bar',
                stack: 'total',
                data: layers.map((l, i) => ({
                    value: l.value,
                    itemStyle: {
                        color: i === 0 ? c.blue :
                            i === 1 ? c.cyan :
                            i === 2 ? c.purple :
                            i === 3 ? c.orange : c.green,
                        borderRadius: [1, 1, 0, 0]
                    }
                })),
                barWidth: '50%',
                label: { show: true, position: 'top', color: c.textSecondary, fontSize: 10, formatter: (p) => p.value.toFixed(3) }
            }]
        };

        this.init('chartExperimentWaterfall', option);
    },

    // Utility: Generate timestamps
    generateTimestamps(count) {
        const ts = [];
        for (let i = count - 1; i >= 0; i--) {
            if (i === 0) {
                ts.push('now');
            } else if (i < 60) {
                ts.push(`${i}s`);
            } else {
                const totalMin = i / 60;
                const m = Math.floor(totalMin);
                const s = i % 60;
                if (m < 60) {
                    if (s === 0) {
                        ts.push(`${m}m`);
                    } else {
                        ts.push(`${m}m${s}s`);
                    }
                } else {
                    const h = Math.floor(m / 60);
                    const rm = m % 60;
                    ts.push(rm > 0 ? `${h}h${rm}m` : `${h}h`);
                }
            }
        }
        return ts;
    },

    // Utility: Generate network nodes
    generateNetworkNodes(count) {
        const types = ['normal', 'fraud', 'suspect'];
        const c = this.colors;
        const nodes = [];
        for (let i = 0; i < count; i++) {
            const type = types[Math.floor(Math.random() * (i < 12 ? 1 : 3))];
            nodes.push({
                name: `ACCT${String(i).padStart(4, '0')}`,
                value: Math.floor(Math.random() * 100),
                symbolSize: type === 'fraud' ? 24 : type === 'suspect' ? 16 : 10,
                itemStyle: {
                    color: type === 'fraud' ? c.red : type === 'suspect' ? c.orange : c.blue
                }
            });
        }
        return nodes;
    },

    generateNetworkLinks(nodes) {
        const links = [];
        for (let i = 0; i < nodes.length; i++) {
            const connCount = 1 + Math.floor(Math.random() * 2);
            for (let j = 0; j < connCount; j++) {
                const target = Math.floor(Math.random() * nodes.length);
                if (target !== i) {
                    links.push({ source: i, target });
                }
            }
        }
        return links;
    }
};
