/*
 * Share over time panel above the catalog lists (fragments/share-history.html).
 * Cumulative monthly share of the list's top items against the rest of what its filters match.
 * Clicking a band or a legend chip focuses that item and highlights its card or row; hovering a card
 * or row lights up its band.
 */
(() => {
    'use strict';
    const root = document.getElementById('shareHistory');
    if (!root) return;

    const sh = name => root.querySelector(`[data-sh="${name}"]`);
    const catalog = root.dataset.catalog;
    const cardAttr = root.dataset.cardAttr;
    const rowAttr = root.dataset.rowAttr;
    const body = root.querySelector('.sh-body');
    const STORE_KEY = `shareHistory:${catalog}`;
    const REST = '__rest';
    const ITEMS = 10;
    const PRIMARY = ['artists', 'albums', 'songs'].includes(catalog);
    // Lookup catalogs (genres, languages...) are small: fetch every listed item and chart the top ones by value.
    // Artists, albums and songs chart the first items of the list, in its own sort.
    const RANKED = !PRIMARY;
    const MAX_KEYS = 100;
    // A handful of albums or songs is a sliver of everything, so they are compared with each other instead.
    const SHARE_OF_SHOWN = catalog === 'albums' || catalog === 'songs';
    const MEASURES = { plays: 'Plays', time: 'Play time', artists: 'Artists', albums: 'Albums', songs: 'Songs' };

    // Muted colors that sit with the dark cards. Artists, albums and songs are tinted by gender:
    // blues for male, rose for female, sand for everything else.
    const PALETTE = ['#5b8fb9', '#c9a04a', '#4f9a7f', '#b0739a', '#a39a5c', '#c07a52',
        '#7f78c4', '#4f9fa1', '#c46f86', '#7f9a4a', '#9a826a', '#5a72b0'];
    const OTHER_GENDER_PALETTE = ['#8b7fc0', '#b9925a', '#5b9c94', '#8f9a5a'];
    const GENDER_COLORS = { male: '#5a8dcc', female: '#c96a98' };
    const RAMPS = { male: [214, 48], female: [332, 44], other: [36, 42] };
    const RAMP_LIGHTNESS = [52, 38, 63, 45, 57, 33, 67, 41, 55, 60];
    const REST_COLOR = [63, 63, 70];
    const TEXT = 'rgba(212, 212, 216, 0.5)';
    const reducedMotion = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;

    let data = null;
    let chart = null;
    let request = null;
    let focusKey = null;
    let colors = new Map();

    /* ---------- Remembered per catalog in this browser ---------- */

    function readStore() {
        try { return JSON.parse(localStorage.getItem(STORE_KEY) || '{}') || {}; } catch { return {}; }
    }

    function writeStore(patch) {
        try { localStorage.setItem(STORE_KEY, JSON.stringify({ ...readStore(), ...patch })); } catch { /* storage off */ }
    }

    function setCollapsed(collapsed) {
        root.classList.toggle('is-collapsed', collapsed);
        sh('toggle').setAttribute('aria-expanded', String(!collapsed));
        sh('toggle').setAttribute('aria-label', collapsed ? 'Show chart' : 'Hide chart');
        body.hidden = collapsed;
    }

    /* ---------- Formatting ---------- */

    const measure = () => sh('measure').value;
    const number = value => Math.round(value).toLocaleString();
    const displayDate = date => date.split('-').reverse().join('/');
    const restName = () => data?.filtered ? 'Rest of these filters' : 'Everything else';

    function amount(value) {
        if (measure() === 'time') return `${Math.floor(value / 3600).toLocaleString()}h ${Math.floor(value % 3600 / 60)}m`;
        return `${number(value)} ${measure() === 'plays' ? 'plays' : measure()}`;
    }

    function periodLabel(period) {
        const [year, month] = period.start.split('-');
        return `${month}/${year}`;
    }

    /* ---------- Colors ---------- */

    const hexRgb = hex => [1, 3, 5].map(i => Number.parseInt(hex.slice(i, i + 2), 16));

    function hslRgb(h, s, l) {
        s /= 100; l /= 100;
        const k = n => (n + h / 30) % 12;
        const a = s * Math.min(l, 1 - l);
        const f = n => l - a * Math.max(-1, Math.min(k(n) - 3, 9 - k(n), 1));
        return [f(0), f(8), f(4)].map(v => Math.round(v * 255));
    }

    function assignColors() {
        colors = new Map([[REST, REST_COLOR]]);
        const used = { male: 0, female: 0, other: 0 };
        data.series.forEach((series, index) => {
            const kind = series.genderId === 2 ? 'male' : series.genderId === 1 ? 'female' : 'other';
            if (catalog === 'genders') {
                colors.set(series.key, kind === 'other'
                    ? hexRgb(OTHER_GENDER_PALETTE[used.other++ % OTHER_GENDER_PALETTE.length]) : hexRgb(GENDER_COLORS[kind]));
            } else if (PRIMARY) {
                const [hue, saturation] = RAMPS[kind];
                const n = used[kind]++;
                colors.set(series.key, hslRgb(hue + (n % 2 ? -6 : 4), saturation, RAMP_LIGHTNESS[n % RAMP_LIGHTNESS.length]));
            } else {
                colors.set(series.key, index < PALETTE.length ? hexRgb(PALETTE[index])
                    : hslRgb((index * 137.508) % 360, 38, 52));
            }
        });
    }

    function rgba(key, alpha = 1) {
        const [r, g, b] = colors.get(key) || REST_COLOR;
        return `rgba(${r}, ${g}, ${b}, ${alpha})`;
    }

    function lighter(key) {
        const [r, g, b] = (colors.get(key) || REST_COLOR).map(v => Math.round(v + (255 - v) * 0.25));
        return `rgb(${r}, ${g}, ${b})`;
    }

    /* ---------- Data ---------- */

    function listKeys() {
        const limit = RANKED ? MAX_KEYS : ITEMS;
        const keys = [];
        const collect = (scope, attr) => document.querySelectorAll(`${scope} [${attr}]`).forEach(element => {
            const key = element.getAttribute(attr);
            if (key && !keys.includes(key) && keys.length < limit) keys.push(key);
        });
        collect('#cardView', cardAttr);
        if (!keys.length) collect('#tableView', rowAttr);
        return keys;
    }

    function status(text) {
        sh('status').textContent = text;
        sh('status').hidden = !text;
    }

    function destroyChart() {
        if (chart) { chart.destroy(); chart = null; }
        hideTooltip();
    }

    async function load() {
        if (request) request.abort();
        const current = new AbortController();
        request = current;
        destroyChart();
        data = null;
        focusKey = null;
        sh('retry').hidden = true;
        sh('wrap').hidden = true;
        sh('legend').replaceChildren();
        sh('note').textContent = '';
        const keys = listKeys();
        // An empty list (e.g. filters that match nothing) has nothing to chart.
        root.hidden = !keys.length;
        if (!keys.length) return;
        status('Loading history...');
        // The page's own query string carries its filters, so "everything" means everything they match.
        const params = new URLSearchParams({ catalog, measure: measure(), filters: window.location.search.replace(/^\?/, '') });
        keys.forEach(key => params.append('key', key));
        try {
            const response = await fetch(`/api/catalog/share-history?${params}`, { signal: current.signal });
            if (!response.ok) throw new Error('Could not load the share history. Please try again.');
            const result = await response.json();
            if (current !== request) return;
            data = result;
            assignColors();
            render();
        } catch (error) {
            if (current !== request || error.name === 'AbortError') return;
            status(error.message);
            sh('retry').hidden = false;
        }
    }

    // Items with data; ranked catalogs keep the top ones and fold the others into the rest.
    function pickItems() {
        const items = data.series.filter(series => series.values.some(value => value > 0));
        if (!RANKED) return { items, rest: data.rest };
        const ranked = [...items].sort((a, b) => b.values.at(-1) - a.values.at(-1) || a.name.localeCompare(b.name));
        const others = ranked.slice(ITEMS);
        return {
            items: ranked.slice(0, ITEMS),
            rest: data.rest.map((value, i) => value + others.reduce((sum, series) => sum + series.values[i], 0))
        };
    }

    // Highest share of everything each item ever reached.
    function findPeaks() {
        const peaks = new Map();
        if (SHARE_OF_SHOWN) return peaks;
        data.series.forEach(series => data.periods.forEach((period, i) => {
            const total = data.totals[i];
            const value = series.values[i];
            const previous = peaks.get(series.key);
            if (total > 0 && value > 0 && (!previous || value / total > previous.share)) peaks.set(series.key, { index: i, share: value / total });
        }));
        return peaks;
    }

    /* ---------- Chart ---------- */

    function render() {
        destroyChart();
        sh('legend').replaceChildren();
        sh('wrap').hidden = true;
        if (!data) return;
        const { items, rest } = pickItems();
        const versus = SHARE_OF_SHOWN ? ', against each other'
            : ` vs ${data.filtered ? 'the rest of these filters' : 'everything else'}`;
        sh('subtitle').textContent = (RANKED ? `Top ${items.length} by ${MEASURES[measure()].toLowerCase()}` : `First ${items.length} of this list`) + versus;
        if (!data.periods.length || !data.totals.some(total => total > 0) || !items.length) {
            status('No data for these items yet.');
            sh('note').textContent = '';
            return;
        }
        const showRest = !SHARE_OF_SHOWN && rest.some(value => value > 0);
        const series = showRest ? [...items, { key: REST, name: restName(), values: rest }] : items;
        if (!items.some(item => item.key === focusKey)) focusKey = null;
        const shownTotals = data.periods.map((_, i) => items.reduce((sum, item) => sum + item.values[i], 0));
        const denominator = i => SHARE_OF_SHOWN ? shownTotals[i] : data.totals[i];
        const shares = series.map(item => item.values.map((value, i) => denominator(i) > 0 ? 100 * value / denominator(i) : null));
        renderNote();
        renderLegend(series, shares);
        status('');
        if (typeof Chart === 'undefined') {
            status('The chart library could not load. Reload the page to retry.');
            return;
        }
        ShareChartCore.registerSectionMode();
        const peaks = findPeaks();
        const leaders = data.periods.map((_, i) => {
            const highest = Math.max(0, ...items.map(item => item.values[i]));
            return new Set(highest > 0 ? items.filter(item => item.values[i] === highest).map(item => item.key) : []);
        });
        sh('wrap').hidden = false;
        const font = { family: 'Inter, system-ui, sans-serif', size: 11 };
        chart = new Chart(sh('canvas'), {
            type: data.periods.length === 1 ? 'bar' : 'line',
            data: {
                labels: data.periods.map(periodLabel),
                datasets: series.map((item, datasetIndex) => {
                    const peakIndex = peaks.get(item.key)?.index ?? -1;
                    const isLeader = i => item.key !== REST && leaders[i].has(item.key);
                    return {
                        label: item.name,
                        key: item.key,
                        data: shares[datasetIndex],
                        backgroundColor: rgba(item.key),
                        borderColor: 'rgba(12, 12, 14, 0.55)',
                        fill: 'stack',
                        borderWidth: 1,
                        pointRadius: item.values.map((_, i) => i === peakIndex ? 5 : isLeader(i) ? 2 : data.periods.length < 3 ? 4 : 0),
                        pointStyle: item.values.map((_, i) => i === peakIndex ? 'rectRot' : 'circle'),
                        pointBackgroundColor: item.values.map((_, i) => isLeader(i) && i !== peakIndex ? lighter(item.key) : rgba(item.key)),
                        pointBorderColor: item.values.map((_, i) => i === peakIndex ? 'rgba(244, 244, 245, 0.85)' : rgba(item.key)),
                        pointBorderWidth: item.values.map((_, i) => i === peakIndex ? 1.5 : 1),
                        pointHoverRadius: item.values.map((_, i) => i === peakIndex ? 6 : 0),
                        pointHitRadius: 6,
                        tension: 0
                    };
                })
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                interaction: { mode: 'stackedSection', intersect: false },
                onHover: (event, elements) => {
                    const key = elements[0] ? chart.data.datasets[elements[0].datasetIndex].key : null;
                    event.native.target.style.cursor = key && key !== REST ? 'pointer' : 'default';
                },
                onClick: (event, elements) => {
                    const key = elements[0] ? chart.data.datasets[elements[0].datasetIndex].key : null;
                    if (!key || key === REST) setFocus(null);
                    else select(key);
                },
                scales: {
                    x: { stacked: true, border: { display: false }, grid: { display: false },
                        ticks: { color: TEXT, font, maxTicksLimit: sh('wrap').clientWidth < 500 ? 4 : 8, maxRotation: 0 } },
                    y: { stacked: true, min: 0, max: 100, border: { display: false },
                        grid: { color: 'rgba(255, 255, 255, 0.05)' },
                        ticks: { color: TEXT, font, stepSize: 25, callback: value => `${value}%` } }
                },
                plugins: {
                    legend: { display: false },
                    tooltip: { enabled: false, external: context => renderTooltip(context, series, denominator) }
                }
            }
        });
        applyFocus();
    }

    function renderNote() {
        const notes = ['Cumulative, month by month.'];
        notes.push(RANKED ? 'Items come from the list\'s current filters.' : 'Items follow the list\'s current sort and filters.');
        if (!SHARE_OF_SHOWN) notes.push(`Shares are out of ${data.filtered ? 'everything matching those filters' : 'your whole library'}; diamonds mark each item's peak.`);
        if (catalog === 'artists') notes.push('Artist plays count songs where they are the main artist, so shares never overlap.');
        sh('note').textContent = notes.join(' ');
    }

    function renderLegend(series, shares) {
        const legend = sh('legend');
        const last = data.periods.length - 1;
        series.forEach((item, datasetIndex) => {
            const isRest = item.key === REST;
            const chip = document.createElement(isRest ? 'span' : 'button');
            chip.className = `sh-chip${isRest ? ' sh-chip-rest' : ''}`;
            chip.dataset.key = item.key;
            if (!isRest) {
                chip.type = 'button';
                chip.addEventListener('click', () => select(item.key));
                chip.addEventListener('mouseenter', () => previewFocus(item.key));
                chip.addEventListener('mouseleave', () => previewFocus(null));
                chip.addEventListener('focus', () => previewFocus(item.key));
                chip.addEventListener('blur', () => previewFocus(null));
            }
            const swatch = document.createElement('i');
            swatch.className = 'sh-swatch';
            swatch.style.backgroundColor = rgba(item.key);
            swatch.setAttribute('aria-hidden', 'true');
            const name = document.createElement('span');
            name.className = 'sh-chip-name';
            name.textContent = item.name;
            const value = document.createElement('span');
            value.className = 'sh-chip-value';
            const current = shares[datasetIndex][last];
            value.textContent = current == null ? '' : `${current.toFixed(1)}%`;
            chip.append(swatch, name, value);
            chip.title = [item.name + (item.subtitle ? ` - ${item.subtitle}` : ''),
                isRest ? (data.filtered ? 'Everything else matching the filters' : 'Everything outside these items')
                    : 'Click to highlight its card or row'].join('\n');
            legend.append(chip);
        });
        const clear = document.createElement('button');
        clear.type = 'button';
        clear.className = 'sh-clear';
        clear.dataset.sh = 'clear';
        clear.textContent = 'Show all';
        clear.hidden = true;
        clear.addEventListener('click', () => setFocus(null));
        legend.append(clear);
    }

    /* ---------- Tooltip ---------- */

    function hideTooltip() {
        const element = sh('tooltip');
        element.hidden = true;
        element.replaceChildren();
    }

    function renderTooltip({ tooltip }, series, denominator) {
        const element = sh('tooltip');
        const point = tooltip.dataPoints?.[0];
        if (!tooltip.opacity || !point) { hideTooltip(); return; }
        const item = series[point.datasetIndex];
        const i = point.dataIndex;
        const value = item.values[i];
        const base = denominator(i);
        element.replaceChildren();
        const line = (className, text) => {
            const row = document.createElement('div');
            row.className = className;
            row.textContent = text;
            element.append(row);
            return row;
        };
        line('sh-tooltip-title', `Through ${displayDate(data.periods[i].end)}`);
        const name = line('sh-tooltip-name', item.name);
        const swatch = document.createElement('i');
        swatch.className = 'sh-swatch';
        swatch.style.backgroundColor = rgba(item.key);
        name.prepend(swatch);
        if (item.subtitle) line('sh-tooltip-sub', item.subtitle);
        const share = base > 0 ? `${(100 * value / base).toFixed(2)}%` : '';
        line('sh-tooltip-value', `${amount(value)}${share ? ` · ${share} ${SHARE_OF_SHOWN ? 'of the shown items' : data.filtered ? 'of the filtered list' : 'of everything'}` : ''}`);
        if (i > 0) {
            const previousBase = denominator(i - 1);
            const change = value - item.values[i - 1];
            const pp = base > 0 && previousBase > 0 ? 100 * value / base - 100 * item.values[i - 1] / previousBase : null;
            const row = line('sh-tooltip-change', 'Change: ');
            const part = (sign, text) => {
                const span = document.createElement('span');
                span.className = sign > 0 ? 'up' : sign < 0 ? 'down' : '';
                span.textContent = text;
                row.append(span);
            };
            part(change, `${change > 0 ? '+' : change < 0 ? '-' : ''}${amount(Math.abs(change))}`);
            if (pp != null) {
                row.append(' · ');
                part(pp, `${pp > 0 ? '+' : pp < 0 ? '-' : ''}${Math.abs(pp).toFixed(2)} pp`);
            }
        }
        if (item.key !== REST) line('sh-tooltip-hint', `Click to highlight its ${tableVisible() ? 'row' : 'card'}`);
        element.hidden = false;
        const wrap = sh('wrap');
        const left = Math.min(Math.max(tooltip.caretX, element.offsetWidth / 2 + 4), wrap.clientWidth - element.offsetWidth / 2 - 4);
        element.style.left = `${left}px`;
        element.style.top = `${tooltip.caretY}px`;
    }

    /* ---------- Focus and highlight ---------- */

    // Temporary emphasis while hovering a chip, card or row; falls back to the clicked item.
    function previewFocus(key) {
        applyFocus(key || focusKey);
    }

    function setFocus(key) {
        focusKey = key;
        applyFocus();
    }

    function applyFocus(key = focusKey) {
        root.querySelectorAll('.sh-chip').forEach(chip => {
            chip.classList.toggle('is-active', Boolean(key) && chip.dataset.key === key);
            chip.classList.toggle('is-dimmed', Boolean(key) && chip.dataset.key !== key);
        });
        const clear = sh('clear');
        if (clear) clear.hidden = !focusKey;
        if (!chart) return;
        chart.data.datasets.forEach(dataset => {
            dataset.backgroundColor = rgba(dataset.key, !key || dataset.key === key ? 1 : 0.22);
        });
        chart.update('none');
    }

    function tableVisible() {
        const table = document.getElementById('tableView');
        return Boolean(table) && table.style.display !== 'none';
    }

    function select(key) {
        setFocus(key);
        const scope = document.getElementById(tableVisible() ? 'tableView' : 'cardView');
        const attr = tableVisible() ? rowAttr : cardAttr;
        const target = scope?.querySelector(`[${attr}="${CSS.escape(key)}"]`);
        if (!target) {
            status('That item is not on screen yet. Scroll the list to load it.');
            return;
        }
        status('');
        target.style.setProperty('--share-glow', (colors.get(key) || REST_COLOR).join(', '));
        target.classList.remove('share-glow');
        void target.offsetWidth;
        target.classList.add('share-glow');
        clearTimeout(target.shareGlowTimer);
        target.shareGlowTimer = setTimeout(() => target.classList.remove('share-glow'), 3200);
        target.scrollIntoView({ behavior: reducedMotion ? 'auto' : 'smooth', block: 'center' });
    }

    // Hovering a card or table row lights up its band, when that item is in the chart.
    function bindListHover(scopeId, attr) {
        const scope = document.getElementById(scopeId);
        if (!scope) return;
        let hovered = null;
        scope.addEventListener('mouseover', event => {
            const item = event.target.closest(`[${attr}]`);
            const key = item && scope.contains(item) ? item.getAttribute(attr) : null;
            if (key === hovered) return;
            hovered = key;
            previewFocus(chart?.data.datasets.some(dataset => dataset.key === key) ? key : null);
        });
        scope.addEventListener('mouseleave', () => {
            hovered = null;
            previewFocus(null);
        });
    }

    /* ---------- Wiring ---------- */

    (root.dataset.measures || 'plays,time').split(',').forEach(value =>
        sh('measure').append(new Option(MEASURES[value] || value, value)));
    const saved = readStore();
    if ([...sh('measure').options].some(option => option.value === saved.measure)) sh('measure').value = saved.measure;
    // Phones start collapsed so the list stays in reach; the choice is remembered once toggled.
    setCollapsed(typeof saved.collapsed === 'boolean' ? saved.collapsed
        : window.matchMedia?.('(max-width: 768px)').matches === true);

    sh('measure').addEventListener('change', () => {
        writeStore({ measure: measure() });
        load();
    });
    sh('retry').addEventListener('click', load);
    sh('toggle').addEventListener('click', () => {
        const collapsed = !root.classList.contains('is-collapsed');
        setCollapsed(collapsed);
        writeStore({ collapsed });
        if (!collapsed && !data) load();
    });
    root.addEventListener('keydown', event => {
        if (event.key === 'Escape' && focusKey) setFocus(null);
    });
    sh('canvas').addEventListener('mouseleave', hideTooltip);
    bindListHover('cardView', cardAttr);
    bindListHover('tableView', rowAttr);
    if (!root.classList.contains('is-collapsed')) load();
})();
