/** Accessible library breakdowns, independent of catalog table layouts. */
(function () {
    'use strict';
    const metrics = { artists: 'Artists', albums: 'Albums', songs: 'Songs', plays: 'Plays', listeningTime: 'Listening time' };
    const breakdowns = { general: 'Overview', country: 'Country', ethnicity: 'Ethnicity', genre: 'Genre', language: 'Language', listenYear: 'Listen year', releaseYear: 'Release year', subgenre: 'Subgenre' };
    const uiParams = ['view', 'tab', 'source', 'graphMetric', 'graphScale', 'graphOrder', 'graphDirection', 'tableSort', 'tableDirection', 'categoryQ', 'categoryPage', 'categorySize', 'compareMeasures', 'filtersOnly'];
    const listParams = ['page', 'perpage', 'sortby', 'sortdir', 'sortby2', 'sortdir2', 'sortby3', 'sortdir3', 'randomSeed', 'lazyCards'];
    const genders = ['male', 'female', 'other'];
    const tableStats = [
        { key: 'total', label: 'Total' }, { key: 'male', label: 'Male', gender: 'male' },
        { key: 'maleShare', label: 'Male %', gender: 'male', percent: true }
    ];

    function values(item) {
        const result = {};
        genders.forEach(gender => { result[gender] = Math.max(0, Number(item?.[gender]) || 0); });
        result.total = result.male + result.female + result.other;
        return result;
    }

    function normalizeData(data, tab) {
        const rows = new Map();
        const available = new Set();
        Object.keys(metrics).forEach(metric => {
            const suffix = tab === 'general' ? 'Gender' : tab[0].toUpperCase() + tab.slice(1);
            const source = data[metric + 'By' + suffix];
            if (source == null) return;
            available.add(metric);
            const items = tab === 'general' ? [{ ...source, name: 'Overall' }] : source;
            items.forEach(item => {
                const name = String(item.name ?? 'Unknown');
                if (!rows.has(name)) rows.set(name, { name, measures: {} });
                rows.get(name).measures[metric] = values(item);
            });
        });
        return { rows: Array.from(rows.values()), available };
    }

    function measure(row, metric) { return row.measures[metric] || values(null); }
    function share(value, total) { return total > 0 ? value / total * 100 : 0; }
    function visibleRows(rows, metric, search, order, direction = order === 'name' ? 'asc' : 'desc') {
        const needle = search.trim().toLocaleLowerCase();
        return rows.filter(row => row.name.toLocaleLowerCase().includes(needle)).sort((a, b) => {
            const left = measure(a, metric), right = measure(b, metric);
            let result = 0;
            if (order === 'name') result = a.name.localeCompare(b.name, undefined, { numeric: true });
            else if (order === 'year') {
                const leftYear = Number.parseInt(a.name, 10), rightYear = Number.parseInt(b.name, 10);
                if (!Number.isFinite(leftYear) || !Number.isFinite(rightYear)) {
                    return Number.isFinite(leftYear) ? -1 : Number.isFinite(rightYear) ? 1 : a.name.localeCompare(b.name);
                }
                result = leftYear - rightYear;
            }
            else if (order === 'male') result = share(left.male, left.total) - share(right.male, right.total);
            else result = left.total - right.total;
            return result * (direction === 'asc' ? 1 : -1) || a.name.localeCompare(b.name, undefined, { numeric: true });
        });
    }

    function tableColumns(metric, available = new Set(Object.keys(metrics))) {
        if (!Object.hasOwn(metrics, metric)) return [];
        const selected = tableStats.map(stat => ({ ...stat, key: metric + '.' + stat.key, stat: stat.key, metric }));
        const totals = Object.keys(metrics).filter(other => other !== metric && available.has(other)).map(other => ({
            key: other + '.total', stat: 'total', metric: other, label: metrics[other], comparison: true
        }));
        return [...selected, ...totals];
    }

    function columnValue(row, column) {
        const item = measure(row, column.metric);
        return column.percent ? share(item.male, item.total) : item[column.stat];
    }

    function sortTableRows(rows, key, direction) {
        const column = tableColumns(key.split('.')[0]).find(column => column.key === key);
        return [...rows].sort((a, b) => {
            const result = column ? columnValue(a, column) - columnValue(b, column)
                : a.name.localeCompare(b.name, undefined, { numeric: true });
            return result * (direction === 'asc' ? 1 : -1) || a.name.localeCompare(b.name, undefined, { numeric: true });
        });
    }

    function formatValue(value, metric) {
        if (metric !== 'listeningTime') return value.toLocaleString();
        const seconds = Math.floor(value);
        if (seconds >= 86400) return `${Math.floor(seconds / 86400)}d ${Math.floor(seconds % 86400 / 3600)}h`;
        if (seconds >= 3600) return `${Math.floor(seconds / 3600)}h ${Math.floor(seconds % 3600 / 60)}m`;
        if (seconds >= 60) return `${Math.floor(seconds / 60)}m`;
        return `${seconds}s`;
    }

    function readState(params) {
        const tab = Object.hasOwn(breakdowns, params.get('tab')) ? params.get('tab') : 'general';
        const order = ['total', 'male', 'name', ...(tab.endsWith('Year') ? ['year'] : [])].includes(params.get('graphOrder')) ? params.get('graphOrder') : (tab.endsWith('Year') ? 'year' : 'total');
        const metric = Object.hasOwn(metrics, params.get('graphMetric')) ? params.get('graphMetric') : 'plays';
        const tableSort = params.get('tableSort');
        const validTableSort = tableSort === 'name' || Object.keys(metrics).flatMap(metric => tableColumns(metric)).some(column => column.key === tableSort);
        return {
            params: new URLSearchParams(params), tab,
            source: ['artists', 'albums', 'songs'].includes(params.get('source')) ? params.get('source') : 'songs',
            metric,
            scale: params.get('graphScale') === 'relative' ? 'relative' : 'share', order,
            direction: ['asc', 'desc'].includes(params.get('graphDirection')) ? params.get('graphDirection') : order === 'name' ? 'asc' : 'desc',
            tableSort: validTableSort ? tableSort : metric + '.total',
            tableDirection: ['asc', 'desc'].includes(params.get('tableDirection')) ? params.get('tableDirection') : tableSort === 'name' ? 'asc' : 'desc',
            search: params.get('categoryQ') || '', page: Math.max(0, Number.parseInt(params.get('categoryPage'), 10) || 0),
            size: [12, 24, 48].includes(Number(params.get('categorySize'))) ? Number(params.get('categorySize')) : 12
        };
    }

    function apiParams(state) {
        const params = new URLSearchParams(state.params);
        [...uiParams, ...listParams].forEach(key => params.delete(key));
        params.set('source', state.source);
        return params;
    }

    function createExplorer(document, window, fetchData) {
        const root = document.getElementById('graphs-explorer');
        if (!root) return;
        const get = id => document.getElementById('graphs-' + id);
        let state = readState(new URLSearchParams(window.location.search));
        let dataset = null, request = null, sequence = 0;
        const cache = new Map();
        const el = (tag, text, className) => {
            const node = document.createElement(tag);
            if (text != null) node.textContent = text;
            if (className) node.className = className;
            return node;
        };

        function writeState(push = false) {
            const params = state.params;
            params.set('view', 'graphs'); params.set('source', state.source); params.set('tab', state.tab);
            params.delete('compareMeasures');
            const settings = { graphMetric: state.metric, graphScale: state.scale, graphOrder: state.order, graphDirection: state.direction, tableSort: state.tableSort, tableDirection: state.tableDirection, categoryQ: state.search, categoryPage: state.page, categorySize: state.size };
            Object.entries(settings).forEach(([key, value]) => { if (value === '' || value === false || value === 0) params.delete(key); else params.set(key, String(value)); });
            const url = new URL(window.location.href); url.search = params.toString();
            if (url.href !== window.location.href) window.history[push ? 'pushState' : 'replaceState']({}, '', url.toString());
        }

        function updateControls() {
            ['metric', 'scale', 'order', 'direction'].forEach(key => { get(key).value = state[key]; });
            get('search').value = state.search; get('page-size').value = String(state.size);
            get('filter-source').value = state.source;
            get('include-groups').checked = state.params.get('includeGroups') === 'true';
            get('include-featured').checked = state.params.get('includeFeatured') === 'true';
            get('groups-control').hidden = state.source !== 'artists';
            get('featured-control').hidden = state.source !== 'artists';
            get('item-limit').value = state.params.get('limit') || '';
            get('limit-entity').value = state.params.get('limitEntity') || 'song';
            get('order').querySelector('[value="year"]').hidden = !state.tab.endsWith('Year');
            root.querySelectorAll('[data-breakdown]').forEach(button => {
                const active = button.dataset.breakdown === state.tab;
                button.classList.toggle('active', active); button.setAttribute('aria-pressed', String(active));
            });
        }

        function render() {
            if (!dataset) return;
            if (!dataset.available.has(state.metric)) state.metric = dataset.available.has('plays') ? 'plays' : Array.from(dataset.available)[0] || 'plays';
            updateControls();
            Array.from(get('metric').options).forEach(option => { option.disabled = !dataset.available.has(option.value); });
            const totals = {};
            Object.keys(metrics).forEach(metric => { totals[metric] = dataset.rows.reduce((sum, row) => sum + measure(row, metric).total, 0); });
            get('summary').replaceChildren();
            Object.entries(metrics).forEach(([metric, label]) => {
                const card = el('button', null, 'graphs-summary-card'); card.type = 'button';
                card.setAttribute('aria-pressed', String(state.metric === metric)); card.disabled = !dataset.available.has(metric);
                card.append(el('span', label), el('strong', dataset.available.has(metric) ? formatValue(totals[metric], metric) : 'Unavailable'));
                card.addEventListener('click', () => { state.metric = metric; state.page = 0; render(); });
                get('summary').append(card);
            });
            const rows = visibleRows(dataset.rows, state.metric, state.search, state.order, state.direction);
            const pages = Math.ceil(rows.length / state.size);
            state.page = Math.min(state.page, Math.max(0, pages - 1));
            const start = state.page * state.size, page = rows.slice(start, start + state.size);
            const max = Math.max(0, ...dataset.rows.map(row => measure(row, state.metric).total));
            const caption = state.scale === 'relative'
                ? 'Bar length compares each category with the largest category in this breakdown. Colors show gender; values keep their original units.'
                : 'Each bar shows the gender share within its category. Equal bar lengths do not mean equal totals.';
            get('chart-title').textContent = state.tab === 'general' ? `${metrics[state.metric]}: gender breakdown` : `${metrics[state.metric]} by ${breakdowns[state.tab].toLowerCase()}`;
            get('scale-help').textContent = caption + (state.tab === 'listenYear' ? ' Items heard in multiple years count in each year.' : '');
            get('summary').setAttribute('aria-label', state.tab === 'general' ? 'Library totals' : 'Totals across all categories');
            get('bars').replaceChildren();
            page.forEach(row => {
                const item = measure(row, state.metric), bar = el('div', null, 'graphs-bar-row');
                const track = el('div', null, 'graphs-bar-track'); track.setAttribute('aria-hidden', 'true');
                const description = genders.map(gender => `${gender === 'other' ? 'Other / unknown' : gender[0].toUpperCase() + gender.slice(1)}: ${formatValue(item[gender], state.metric)} (${share(item[gender], item.total).toFixed(1)}%)`).join('; ');
                genders.forEach(gender => {
                    const segment = el('span', null, 'graphs-bar-segment ' + gender);
                    segment.style.width = share(item[gender], state.scale === 'relative' ? max : item.total) + '%'; track.append(segment);
                });
                const name = el('span', row.name, 'graphs-bar-name');
                const total = el('div', formatValue(item.total, state.metric), 'graphs-bar-value');
                total.append(el('small', share(item.male, item.total).toFixed(1) + '% male'));
                bar.title = description;
                const accessible = el('span', description, 'sr-only');
                bar.append(name, track, total, accessible); get('bars').append(bar);
            });
            if (!rows.length) get('bars').append(el('p', dataset.rows.length ? 'No categories match your search. Try another name.' : 'No data matches these filters. Edit or clear the filters to explore again.', 'graphs-empty'));
            else if (!totals[state.metric]) get('bars').prepend(el('p', 'No recorded values for this measure in the selected scope.', 'graphs-empty'));
            get('page-label').textContent = rows.length ? `${start + 1}-${start + page.length} of ${rows.length.toLocaleString()} categories` : '0 categories';
            get('previous').disabled = state.page === 0; get('next').disabled = state.page >= pages - 1;
            renderTable(rows);
            writeState();
        }

        function renderTable(rows) {
            const table = get('data-table'); table.replaceChildren();
            const columns = tableColumns(state.metric, dataset.available);
            if (state.tableSort !== 'name' && !columns.some(column => column.key === state.tableSort)) {
                const stat = state.tableSort.split('.')[1];
                state.tableSort = state.metric + '.' + (tableStats.some(column => column.key === stat) ? stat : 'total');
            }
            table.append(el('caption', `${metrics[state.metric]} by ${breakdowns[state.tab].toLowerCase()}: ${rows.length.toLocaleString()} matching categories. Total, Male and Male % use ${metrics[state.metric].toLowerCase()}; other measure columns show totals. Table sorting is independent of the bars.`));
            const head = el('thead'), header = el('tr'), body = el('tbody');
            const buttons = new Map();
            function sortHeading(cell, key, label, accessibleLabel) {
                cell.scope = 'col'; cell.dataset.sortKey = key;
                const active = key === state.tableSort;
                cell.setAttribute('aria-sort', active ? state.tableDirection === 'asc' ? 'ascending' : 'descending' : 'none');
                const nextDirection = active ? state.tableDirection === 'desc' ? 'asc' : 'desc' : key === 'name' ? 'asc' : 'desc';
                const button = el('button', label, 'graphs-table-sort'); button.type = 'button';
                button.setAttribute('aria-label', `${accessibleLabel}: sort ${nextDirection === 'asc' ? 'ascending' : 'descending'}`);
                const indicator = el('span', active ? state.tableDirection === 'asc' ? '\u2191' : '\u2193' : '\u2195', 'graphs-table-sort-indicator');
                indicator.setAttribute('aria-hidden', 'true'); button.append(indicator);
                button.addEventListener('click', () => {
                    state.tableSort = key; state.tableDirection = nextDirection;
                    const updatedButtons = renderTable(rows); writeState(); updatedButtons.get(key).focus();
                });
                buttons.set(key, button); cell.append(button);
            }
            const category = el('th', null, 'graphs-category-column');
            sortHeading(category, 'name', 'Category', 'Category'); header.append(category);
            columns.forEach(column => {
                const cell = el('th', null, column.gender);
                sortHeading(cell, column.key, column.label, column.comparison ? column.label + ' total' : metrics[column.metric] + ' ' + column.label); header.append(cell);
            });
            head.append(header);
            sortTableRows(rows, state.tableSort, state.tableDirection).forEach(row => {
                const tr = el('tr'), name = el('th', row.name, 'graphs-category-column'); name.scope = 'row'; tr.append(name);
                columns.forEach(column => {
                    const value = columnValue(row, column);
                    const cell = el('td', column.percent ? value.toFixed(1) + '%' : formatValue(value, column.metric), column.gender);
                    tr.append(cell);
                });
                body.append(tr);
            });
            if (!rows.length) {
                const row = el('tr'), cell = el('td', 'No matching categories.', 'graphs-empty');
                cell.colSpan = columns.length + 1; row.append(cell); body.append(row);
            }
            table.append(head, body);
            return buttons;
        }

        async function load() {
            const token = ++sequence; request?.abort(); request = new AbortController();
            dataset = null; get('results').hidden = true; get('results').setAttribute('aria-busy', 'true');
            get('retry').hidden = true; get('status').hidden = false; get('status').textContent = `Loading ${breakdowns[state.tab].toLowerCase()} breakdown...`;
            const url = `/playground/api/charts/${state.tab}?${apiParams(state)}`;
            try {
                let data = cache.get(url);
                if (!data) {
                    const response = await fetchData(url, { signal: request.signal });
                    if (!response.ok) throw new Error('Unable to load breakdown');
                    data = await response.json();
                    if (token !== sequence) return;
                    cache.set(url, data); if (cache.size > 16) cache.delete(cache.keys().next().value);
                }
                if (token !== sequence) return;
                dataset = normalizeData(data, state.tab); render();
                get('status').hidden = true; get('results').hidden = false;
            } catch (error) {
                if (token !== sequence || error.name === 'AbortError') return;
                get('status').textContent = 'This breakdown could not be loaded. Your filters are still here; try again.'; get('retry').hidden = false;
            } finally { if (token === sequence) get('results').setAttribute('aria-busy', 'false'); }
        }

        root.querySelectorAll('[data-breakdown]').forEach(button => button.addEventListener('click', () => {
            state.tab = button.dataset.breakdown; state.page = 0; state.search = ''; state.order = state.tab.endsWith('Year') ? 'year' : 'total';
            state.direction = 'desc';
            updateControls(); writeState(true); load();
        }));
        ['metric', 'scale', 'order', 'direction'].forEach(key => get(key).addEventListener('change', () => {
            state[key] = get(key).value;
            if (key === 'order') state.direction = state.order === 'name' ? 'asc' : 'desc';
            state.page = 0; render();
        }));
        get('search').addEventListener('input', () => { state.search = get('search').value; state.page = 0; render(); });
        get('page-size').addEventListener('change', () => { state.size = Number(get('page-size').value); state.page = 0; render(); });
        get('previous').addEventListener('click', () => { state.page--; render(); });
        get('next').addEventListener('click', () => { state.page++; render(); });
        get('retry').addEventListener('click', load);
        ['include-groups', 'include-featured', 'item-limit', 'limit-entity'].forEach(id => get(id).addEventListener('change', () => {
            state.params.set('includeGroups', String(get('include-groups').checked)); state.params.set('includeFeatured', String(get('include-featured').checked));
            const limit = Number.parseInt(get('item-limit').value, 10);
            if (limit > 0) { state.params.set('limit', String(Math.min(999, limit))); state.params.set('limitEntity', get('limit-entity').value); }
            else { state.params.delete('limit'); state.params.delete('limitEntity'); }
            state.page = 0; updateControls(); writeState(true); load();
        }));
        document.addEventListener('playground:filters-applied', event => {
            const params = new URLSearchParams(event.detail.params);
            uiParams.forEach(key => params.delete(key)); listParams.forEach(key => params.delete(key));
            state.params = params; state.source = event.detail.source; state.page = 0;
            updateControls(); writeState(true); load();
        });
        window.addEventListener('popstate', () => { state = readState(new URLSearchParams(window.location.search)); updateControls(); load(); });
        updateControls(); load();
        return { load };
    }

    if (typeof module !== 'undefined' && module.exports) module.exports = { normalizeData, visibleRows, share, formatValue, readState, apiParams, tableColumns, sortTableRows, createExplorer };
    if (typeof document !== 'undefined') document.addEventListener('DOMContentLoaded', () => createExplorer(document, window, window.fetch.bind(window)));
})();
