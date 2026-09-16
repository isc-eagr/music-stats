const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

class Element {
    constructor(value = '') { this.value = value; this.children = []; this.listeners = {}; this.style = {}; this.textContent = ''; }
    get selectedOptions() { return [{ text: this.value }]; }
    append(...children) {
        this.children.push(...children);
        this.textContent = this.children.map(child => typeof child === 'string' ? child : child.textContent).join('');
    }
    replaceChildren(...children) {
        this.children = children;
        this.textContent = this.children.map(child => typeof child === 'string' ? child : child.textContent).join('');
    }
    setAttribute() {}
    addEventListener(name, callback) { this.listeners[name] = callback; }
    insertRow() { const row = new Element(); this.append(row); return row; }
    insertCell() { return this.insertRow(); }
    createTHead() { this.head = new Element(); this.append(this.head); return this.head; }
    createTBody() { this.body = new Element(); this.append(this.body); return this.body; }
}

const flush = () => new Promise(resolve => setImmediate(resolve));
function setup(data, options = {}) {
    const defaults = { breakdown: 'gender', measure: 'plays', mode: 'cumulative', interval: 'month', year: '',
        chart: 'area', scale: 'percent', categories: '6', ...options };
    const elements = new Map();
    const get = id => {
        if (!elements.has(id)) elements.set(id, new Element(defaults[id.replace('history-', '')]));
        return elements.get(id);
    };
    const charts = [];
    const requests = [];
    const document = { getElementById: get, createElement: () => new Element(), createTextNode: text => text, body: new Element() };
    const context = { document, URLSearchParams, AbortController,
        getComputedStyle: () => ({ getPropertyValue: () => '#fff' }),
        requestAnimationFrame: callback => { callback(); return 1; },
        Chart: class { constructor(canvas, config) { charts.push(config); } destroy() {} },
        fetch: (url, options) => {
            requests.push({ url, options });
            return typeof data === 'function' ? data(url, options) : Promise.resolve({ ok: true, json: async () => data });
        }
    };
    context.Chart.Interaction = { modes: {} };
    context.Chart.helpers = { getRelativePosition: event => event };
    vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../../main/resources/static/js/breakdown-history.js'), 'utf8'), context);
    return { get: name => get(`history-${name}`), charts, requests, interaction: context.Chart.Interaction.modes,
        async change(name, value) { const element = get(`history-${name}`); element.value = value; element.listeners.change(); await flush(); } };
}

const periods = [
    { start: '2024-01-01', end: '2024-01-31' },
    { start: '2024-02-01', end: '2024-02-29' },
    { start: '2024-03-01', end: '2024-03-31' }
];

test('shares sum to 100, empty periods stay gaps, and tooltips retain raw values', async () => {
    const ui = setup({ periods, totals: [4, 0, 5], series: [
        { name: 'Male', values: [3, 0, 1] }, { name: 'Female', values: [1, 0, 4] }
    ] });
    await flush();
    const chart = ui.charts.at(-1);
    assert.equal(chart.data.datasets[0].data[0], 75);
    assert.equal(chart.data.datasets[0].data[1], null);
    assert.equal(chart.data.datasets[1].data[1], null);
    assert.equal(chart.data.datasets.reduce((sum, series) => sum + series.data[2], 0), 100);
    assert.equal(chart.data.datasets[0].borderColor, '#4a9eff');
    assert.equal(chart.data.datasets[1].borderColor, '#ff6b9d');
    assert.equal(chart.options.plugins.tooltip.callbacks.label({ datasetIndex: 0, dataIndex: 0, dataset: { label: 'Male' } }), 'Male: 3 (75.00%)');
    assert.equal(ui.get('data').hidden, false);
    assert.equal(ui.get('table').body.children[1].children[2].textContent, '0 (No data)');
});

test('weekly is available only for a selected history range', async () => {
    const ui = setup({ periods, totals: [4, 5, 10], series: [
        { name: 'Male', values: [3, 4, 8] }, { name: 'Female', values: [1, 1, 2] }
    ] }, { mode: 'period' });
    await flush();
    assert.equal(ui.get('interval-week').hidden, true);
    assert.equal(ui.get('interval-week').disabled, true);
    await ui.change('year', '2024');
    assert.equal(ui.get('interval-week').hidden, false);
    assert.equal(ui.get('interval-week').disabled, false);
    await ui.change('interval', 'week');
    let params = new URL(ui.requests.at(-1).url, 'http://localhost').searchParams;
    assert.equal(params.get('interval'), 'week');
    assert.equal(params.get('year'), '2024');
    await ui.change('year', '');
    assert.equal(ui.get('interval').value, 'month');
    assert.equal(ui.get('interval-week').hidden, true);
    assert.equal(ui.get('interval-week').disabled, true);
    params = new URL(ui.requests.at(-1).url, 'http://localhost').searchParams;
    assert.equal(params.get('interval'), 'month');
    assert.equal(params.has('year'), false);
});

test('top categories are fixed across periods and grouping preserves every total', async () => {
    const series = Array.from({ length: 8 }, (_, i) => ({ name: `Category ${i}`, values: [9 - i, i + 1, i + 2] }));
    const totals = periods.map((_, i) => series.reduce((sum, series) => sum + series.values[i], 0));
    const ui = setup({ periods, series, totals }, { breakdown: 'genre' });
    await flush();
    assert.equal(ui.charts.at(-1).data.datasets.length, 7);
    assert.equal(ui.charts.at(-1).data.datasets.at(-1).label, 'Remaining categories');
    for (let i = 0; i < periods.length; i++) {
        assert.ok(Math.abs(ui.charts.at(-1).data.datasets.reduce((sum, series) => sum + series.data[i], 0) - 100) < 0.00001);
    }
    await ui.change('categories', 'all');
    assert.equal(ui.charts.at(-1).data.datasets.length, 8);
    assert.equal(ui.requests.length, 1);
});

test('line and amount controls display unstacked hours without refetching', async () => {
    const ui = setup({ periods, totals: [3600, 7200, 10800], series: [{ name: 'Male', values: [3600, 7200, 10800] }] }, { measure: 'time' });
    await flush();
    await ui.change('chart', 'line');
    await ui.change('scale', 'absolute');
    const chart = ui.charts.at(-1);
    assert.equal(chart.options.scales.y.stacked, false);
    assert.equal(chart.data.datasets[0].fill, false);
    assert.deepEqual(Array.from(chart.data.datasets[0].data), [1, 2, 3]);
    assert.equal(ui.requests.length, 1);
});

test('stale responses cannot replace the newest selection and a lone period renders visibly', async () => {
    const pending = [];
    const ui = setup(() => new Promise(resolve => pending.push(resolve)));
    await ui.change('measure', 'songs');
    assert.equal(ui.requests[0].options.signal.aborted, true);
    pending[1]({ ok: true, json: async () => ({ periods: periods.slice(0, 1), totals: [2], series: [{ name: 'Female', values: [2] }] }) });
    await flush();
    assert.equal(ui.charts.at(-1).type, 'bar');
    pending[0]({ ok: true, json: async () => ({ periods, totals: [1, 1, 1], series: [{ name: 'Male', values: [1, 1, 1] }] }) });
    await flush();
    assert.equal(ui.charts.length, 1);
    assert.equal(ui.charts.at(-1).data.datasets[0].label, 'Female');
});

test('failed loads show retry and empty selections clear the graph', async () => {
    const failed = setup(() => Promise.resolve({ ok: false, status: 500 }));
    await flush();
    assert.equal(failed.get('retry').hidden, false);
    assert.match(failed.get('status').textContent, /Could not load/);
    const empty = setup({ periods: [], totals: [], series: [] });
    await flush();
    assert.equal(empty.charts.length, 0);
    assert.equal(empty.get('chart-wrap').hidden, true);
    assert.match(empty.get('status').textContent, /No data/);
});

test('non-gender categories use distinct contrasting colors that survive view changes', async () => {
    const names = ['Black', 'East Asian', 'Latin', 'Mixed', 'Other', 'White'];
    for (const breakdown of ['ethnicity', 'genre', 'language']) {
        const ui = setup({ periods, totals: [6, 6, 6], series: names.map(name => ({ name, values: [1, 1, 1] })) }, { breakdown });
        await flush();
        const datasets = ui.charts.at(-1).data.datasets;
        assert.equal(new Set(datasets.map(series => series.backgroundColor)).size, 6);
        const rgb = hex => [1, 3, 5].map(i => parseInt(hex.slice(i, i + 2), 16));
        const black = rgb(datasets.find(series => series.label === 'Black').backgroundColor);
        const mixed = rgb(datasets.find(series => series.label === 'Mixed').backgroundColor);
        assert.ok(Math.hypot(...black.map((value, i) => value - mixed[i])) > 100);
        const original = datasets.map(series => series.backgroundColor).join();
        await ui.change('categories', 'all');
        assert.equal(ui.charts.at(-1).data.datasets.map(series => series.backgroundColor).join(), original);
    }
});

test('hover selects only the band containing the pointer, including between samples', async () => {
    const ui = setup({ periods: periods.slice(0, 2), totals: [4, 4], series: [
        { name: 'Male', values: [3, 1] }, { name: 'Female', values: [1, 3] }
    ] });
    await flush();
    const config = ui.charts.at(-1);
    const mockChart = { options: config.options, config, data: config.data,
        chartArea: { left: 0, right: 100, top: 0, bottom: 100 },
        scales: { x: { getPixelForValue: i => i * 100 }, y: { getPixelForValue: value => 100 - value } },
        getDatasetMeta: () => ({ data: [{}, {}] }) };
    const hover = event => ui.interaction.historySection(mockChart, event, {}, false);
    assert.equal(hover({ x: 25, y: 36 }).length, 1);
    assert.equal(hover({ x: 25, y: 36 })[0].datasetIndex, 1);
    assert.equal(hover({ x: 25, y: 80 })[0].datasetIndex, 0);
    assert.equal(hover({ x: 25, y: -1 }).length, 0);
    assert.equal(config.options.plugins.tooltip.callbacks.footer([{ dataIndex: 0 }]), '');
    config.data.datasets.forEach(series => { series.data[1] = null; });
    assert.equal(hover({ x: 50, y: 80 }).length, 0);
});

test('hover tooltips color count and share movement independently while clicks omit movement', async () => {
    const ui = setup({ periods, totals: [10, 20, 20], series: [
        { name: 'Male', values: [4, 6, 5] }, { name: 'Female', values: [6, 14, 15] }
    ] }, { mode: 'period' });
    await flush();
    const config = ui.charts.at(-1);
    const external = config.options.plugins.tooltip.external;
    const male = { datasetIndex: 0, dataIndex: 1, dataset: config.data.datasets[0] };
    const female = { datasetIndex: 1, dataIndex: 1, dataset: config.data.datasets[1] };
    external({ tooltip: { opacity: 1, caretX: 50, caretY: 40, dataPoints: [male] } });
    const hoverTooltip = ui.get('tooltip');
    assert.equal(hoverTooltip.hidden, false);
    assert.match(hoverTooltip.textContent, /Change: \+2.*-10.00 pp/);
    const movement = hoverTooltip.children[1].children[2];
    assert.equal(movement.children[1].className, 'history-tooltip-change increase');
    assert.equal(movement.children[3].className, 'history-tooltip-change decrease');

    config.options.onClick({ x: 50, y: 40 }, [], {
        data: config.data,
        getElementsAtEventForMode: () => [{ datasetIndex: 0, index: 1 }],
        setActiveElements() {}, tooltip: { setActiveElements() {} }, update() {}
    });
    external({ tooltip: { opacity: 1, caretX: 50, caretY: 40, dataPoints: [male, female] } });
    assert.doesNotMatch(hoverTooltip.textContent, /Change:/);
    assert.match(hoverTooltip.children[1].className, /is-selected/);
});

test('cumulative peak and period leader markers use distinct point treatments', async () => {
    const ui = setup({ periods, totals: [4, 5, 10], series: [
        { name: 'Male', values: [3, 4, 8] }, { name: 'Female', values: [1, 1, 2] }
    ] });
    await flush();
    const config = ui.charts.at(-1);
    assert.match(ui.get('peak').textContent, /Male: 80.00%.*29\/02\/2024/);
    assert.match(ui.get('peak').textContent, /Female: 25.00%.*31\/01\/2024/);
    assert.equal(config.data.datasets[0].pointRadius[0], 3);
    assert.equal(config.data.datasets[0].pointRadius[1], 7);
    assert.equal(config.data.datasets[0].pointRadius[2], 3);
    assert.equal(config.data.datasets[0].pointBackgroundColor[0], 'rgb(137, 192, 255)');
    assert.equal(config.data.datasets[0].pointStyle[1], 'rectRot');
    assert.equal(config.data.datasets[1].pointRadius[0], 7);
    assert.equal(config.options.scales.y.ticks.stepSize, 5);
    assert.equal(config.options.scales.y.ticks.maxTicksLimit, 21);
    assert.equal(config.options.scales.y.ticks.autoSkip, false);
    assert.equal(config.plugins[0].id, 'history-peaks-and-overflow');
    const limited = setup({ periods: [{ start: '2023-12-01', end: '2023-12-31' }, ...periods],
        totals: [1, 4, 5, 10], series: [
            { name: 'Male', values: [1, 3, 4, 8] }, { name: 'Female', values: [0, 1, 1, 2] }
        ] }, { year: '2024' });
    await flush();
    assert.equal(new URL(limited.requests[0].url, 'http://localhost').searchParams.has('year'), false);
    assert.equal(limited.charts.at(-1).data.labels.length, 3);
    assert.match(limited.get('peak').textContent, /Male: 100.00%.*31\/12\/2023.*outside range/);
});

test('each non-gender cumulative breakdown marks every visible category peak', async () => {
    const namesByBreakdown = {
        genre: ['Rap', 'R&B', 'Reggaeton'],
        ethnicity: ['White', 'Latino', 'Black'],
        language: ['English', 'Spanish', 'French']
    };
    for (const [breakdown, names] of Object.entries(namesByBreakdown)) {
        const ui = setup({ periods, totals: [100, 100, 100], series: [
            { name: names[0], values: [10, 18, 17] },
            { name: names[1], values: [20, 12, 15] },
            { name: names[2], values: [7, 8, 14] }
        ] }, { breakdown });
        await flush();
        const config = ui.charts.at(-1);
        const peakText = ui.get('peak').textContent;
        assert.ok(peakText.includes(`${names[0]}: 18.00% \u00b7 29/02/2024`));
        assert.ok(peakText.includes(`${names[1]}: 20.00% \u00b7 31/01/2024`));
        assert.ok(peakText.includes(`${names[2]}: 14.00% \u00b7 31/03/2024`));
        assert.deepEqual(Array.from(config.data.datasets[0].pointRadius), [0, 7, 3]);
        assert.deepEqual(Array.from(config.data.datasets[1].pointRadius), [7, 0, 0]);
        assert.deepEqual(Array.from(config.data.datasets[2].pointRadius), [0, 0, 7]);
        assert.notEqual(config.data.datasets[0].pointBackgroundColor[2], '#fff');
        assert.notEqual(config.data.datasets[0].pointBackgroundColor[2], config.data.datasets[0].backgroundColor);
    }
    const periodUi = setup({ periods, totals: [100, 100, 100], series: [
        { name: 'Rap', values: [10, 18, 17] }, { name: 'R&B', values: [20, 12, 15] }
    ] }, { breakdown: 'genre', mode: 'period' });
    await flush();
    assert.equal(periodUi.get('peak').hidden, true);
    assert.deepEqual(Array.from(periodUi.charts.at(-1).data.datasets[0].pointRadius), [0, 0, 0]);
});

test('share line charts use a rounded five-percent ceiling based on the highest series value', async () => {
    const ui = setup({ periods, totals: [100, 100, 100], series: [
        { name: 'Adult Contemporary', values: [27, 24, 23] },
        { name: 'Alternative', values: [19, 20, 18] }
    ] }, { breakdown: 'genre', chart: 'line' });
    await flush();
    const config = ui.charts.at(-1);
    assert.equal(config.options.scales.y.max, 30);
    assert.equal(config.options.scales.y.ticks.stepSize, 5);
    assert.equal(config.options.scales.y.ticks.maxTicksLimit, 7);
    assert.equal(config.options.scales.y.ticks.autoSkip, false);
});

test('initial import stays in the data and receives a capped scale with overflow markers', async () => {
    const importPeriods = [
        { start: '2005-02-01', end: '2005-02-28' },
        { start: '2005-03-01', end: '2005-03-31' },
        { start: '2005-04-01', end: '2005-04-30' }
    ];
    const ui = setup({ periods: importPeriods, totals: [10000, 2400, 2700], series: [
        { name: 'Male', values: [7000, 1200, 1350] },
        { name: 'Female', values: [3000, 1200, 1350] }
    ] }, { mode: 'period', scale: 'absolute' });
    await flush();
    let config = ui.charts.at(-1);
    assert.equal(new URL(ui.requests[0].url, 'http://localhost').searchParams.has('excludeInitialImport'), false);
    assert.equal(config.options.scales.y.max, 3000);
    assert.deepEqual(Array.from(config.data.datasets[0].data), [2100, 1200, 1350]);
    assert.deepEqual(Array.from(config.data.datasets[1].data), [900, 1200, 1350]);
    assert.match(ui.get('definition').textContent, /Initial import: 10,000; scale capped at 3,000/);
    const markerCalls = [];
    const ctx = { save() {}, restore() {}, setLineDash() {}, beginPath() {}, moveTo() {}, lineTo() {}, closePath() {},
        stroke() {}, fill() { markerCalls.push('fill'); } };
    config.plugins[0].afterDatasetsDraw({ ctx, chartArea: { left: 0, right: 500, top: 0, bottom: 500 },
        scales: { x: { getPixelForValue: () => 10 } }, getDatasetMeta: () => ({ data: [] }) });
    assert.equal(markerCalls.length, 1);
    assert.equal(config.options.plugins.tooltip.callbacks.label({ datasetIndex: 0, dataIndex: 0, dataset: { label: 'Male' } }), 'Male: 7,000 (70.00%)');

    await ui.change('chart', 'line');
    config = ui.charts.at(-1);
    assert.equal(config.options.scales.y.max, 1500);
    assert.deepEqual(Array.from(config.data.datasets[0].data), [1500, 1200, 1350]);
    assert.deepEqual(Array.from(config.data.datasets[1].data), [1500, 1200, 1350]);
});

test('clicking a section expands its tooltip to the full period breakdown', async () => {
    const ui = setup({ periods, totals: [4, 5, 10], series: [
        { name: 'Male', values: [3, 4, 8] }, { name: 'Female', values: [1, 1, 2] }
    ] });
    await flush();
    const config = ui.charts.at(-1);
    let activeElements;
    let chartActiveElements;
    let position;
    let updates = 0;
    config.options.onClick({ x: 25, y: 40 }, [], {
        data: config.data,
        getElementsAtEventForMode: () => [{ datasetIndex: 1, index: 1 }],
        setActiveElements: elements => { chartActiveElements = elements; },
        tooltip: { setActiveElements: (elements, point) => { activeElements = elements; position = point; } },
        update: () => { updates++; }
    });
    assert.deepEqual(JSON.parse(JSON.stringify(activeElements)), [{ datasetIndex: 0, index: 1 }, { datasetIndex: 1, index: 1 }]);
    assert.deepEqual(JSON.parse(JSON.stringify(chartActiveElements)), [{ datasetIndex: 0, index: 1 }, { datasetIndex: 1, index: 1 }]);
    assert.deepEqual(JSON.parse(JSON.stringify(position)), { x: 25, y: 40 });
    assert.equal(updates, 2);
    const tooltip = config.options.plugins.tooltip;
    const male = { datasetIndex: 0, dataIndex: 1, dataset: config.data.datasets[0] };
    const female = { datasetIndex: 1, dataIndex: 1, dataset: config.data.datasets[1] };
    assert.ok(tooltip.itemSort(male, female) < 0);
    assert.equal(tooltip.callbacks.label(male), 'Male: 4 (80.00%)');
    assert.equal(tooltip.callbacks.label(female), 'Female: 1 (20.00%)');
    assert.equal(tooltip.callbacks.labelTextColor(female), '#fde047');
    assert.equal(tooltip.callbacks.labelTextColor(male), '#fff');
    assert.deepEqual(JSON.parse(JSON.stringify(tooltip.callbacks.labelColor(female))), {
        backgroundColor: '#ff6b9d', borderColor: '#fde047', borderWidth: 2, borderRadius: 2
    });
    config.options.onHover({ type: 'mousemove' });
    assert.equal(tooltip.callbacks.label(female), 'Female: 1 (20.00%)');
    assert.equal(tooltip.callbacks.footer([{ dataIndex: 1 }, { dataIndex: 1 }]), 'Total: 5');
    const source = fs.readFileSync(path.join(__dirname, '../../main/resources/templates/playground.html'), 'utf8');
    assert.doesNotMatch(source, /history-import/);
});
