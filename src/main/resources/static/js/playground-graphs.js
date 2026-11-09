/** Accessible library breakdowns, independent of catalog table layouts. */
(function () {
    'use strict';
    const metrics = { artists: 'Artists', albums: 'Albums', songs: 'Songs', plays: 'Plays', listeningTime: 'Listening time' };
    const maleLabels = { artists: 'Male artists', albums: 'Male albums', songs: 'Male songs', plays: 'Male plays', listeningTime: 'Male time' };
    const breakdowns = { general: 'Overview', country: 'Country', ethnicity: 'Ethnicity', genre: 'Genre', language: 'Language', listenYear: 'Listen year', releaseYear: 'Release year', subgenre: 'Subgenre' };
    // Legacy keys (graphOrder ... compareMeasures) stay listed so old bookmarks never leak into catalog filters.
    const uiParams = ['view', 'tab', 'source', 'graphMetric', 'graphScale', 'graphExpand', 'tableSort', 'tableDirection', 'graphOrder', 'graphDirection', 'categoryQ', 'categoryPage', 'categorySize', 'compareMeasures', 'filtersOnly'];
    const legacyParams = ['graphOrder', 'graphDirection', 'categoryQ', 'categoryPage', 'categorySize', 'compareMeasures'];
    const listParams = ['page', 'perpage', 'sortby', 'sortdir', 'sortby2', 'sortdir2', 'sortby3', 'sortdir3', 'randomSeed', 'lazyCards'];
    const genders = ['male', 'female', 'other'];
    const tableStats = [
        { key: 'total' }, { key: 'share', label: 'Share %', percent: true, ofTotal: true },
        { key: 'male', gender: 'male' }, { key: 'maleShare', label: 'Male %', gender: 'male', percent: true }
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
    function metricTotals(rows) {
        const totals = {};
        Object.keys(metrics).forEach(metric => { totals[metric] = rows.reduce((sum, row) => sum + measure(row, metric).total, 0); });
        return totals;
    }

    function column(metric, stat, selected) {
        const label = stat.key === 'total' ? metrics[metric] : stat.key === 'male' ? maleLabels[metric] : stat.label;
        return { ...stat, key: metric + '.' + stat.key, stat: stat.key, metric, label, selected, comparison: !selected };
    }

    /** Selected measure gets the full breakdown; every other measure is one total column unless expanded. */
    function tableColumns(metric, available = new Set(Object.keys(metrics)), expanded = new Set()) {
        if (!Object.hasOwn(metrics, metric)) return [];
        const others = Object.keys(metrics).filter(other => other !== metric && available.has(other));
        return [metric, ...others].flatMap(group => {
            const selected = group === metric;
            return (selected || expanded.has(group) ? tableStats : tableStats.slice(0, 1)).map(stat => column(group, stat, selected));
        });
    }

    function columnFor(key) {
        const [metric, statKey] = String(key).split('.');
        const stat = tableStats.find(item => item.key === statKey);
        return Object.hasOwn(metrics, metric) && stat ? column(metric, stat, true) : null;
    }

    function columnValue(row, column, totals) {
        const item = measure(row, column.metric);
        if (column.ofTotal) return share(item.total, totals[column.metric]);
        return column.percent ? share(item.male, item.total) : item[column.stat];
    }

    /** Numeric names (years) compare as numbers and non-numeric names such as Unknown stay last either way. */
    function compareNames(a, b, direction) {
        const left = Number(a), right = Number(b);
        if (Number.isFinite(left) !== Number.isFinite(right)) return Number.isFinite(left) ? -1 : 1;
        const result = Number.isFinite(left) ? left - right : a.localeCompare(b, undefined, { numeric: true });
        return direction === 'asc' ? result : -result;
    }

    function sortTableRows(rows, key, direction, totals = metricTotals(rows)) {
        const sortColumn = columnFor(key);
        return [...rows].sort((a, b) => {
            if (!sortColumn) return compareNames(a.name, b.name, direction);
            const result = (columnValue(a, sortColumn, totals) - columnValue(b, sortColumn, totals)) * (direction === 'asc' ? 1 : -1);
            return result || compareNames(a.name, b.name, 'asc');
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

    function defaultSort(tab, metric) { return tab.endsWith('Year') ? 'name' : metric + '.total'; }

    function readState(params) {
        const tab = Object.hasOwn(breakdowns, params.get('tab')) ? params.get('tab') : 'general';
        const metric = Object.hasOwn(metrics, params.get('graphMetric')) ? params.get('graphMetric') : 'plays';
        const expanded = new Set((params.get('graphExpand') || '').split(',').filter(other => Object.hasOwn(metrics, other) && other !== metric));
        const requested = params.get('tableSort');
        const tableSort = requested === 'name' || columnFor(requested) ? requested : defaultSort(tab, metric);
        const sortColumn = columnFor(tableSort);
        if (sortColumn && sortColumn.metric !== metric && sortColumn.stat !== 'total') expanded.add(sortColumn.metric);
        return {
            params: new URLSearchParams(params), tab, metric, expanded,
            source: ['artists', 'albums', 'songs'].includes(params.get('source')) ? params.get('source') : 'songs',
            scale: params.get('graphScale') === 'relative' ? 'relative' : 'share',
            tableSort,
            tableDirection: ['asc', 'desc'].includes(params.get('tableDirection')) ? params.get('tableDirection') : tableSort === 'name' && !tab.endsWith('Year') ? 'asc' : 'desc'
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
        const button = (text, className, onClick) => {
            const node = el('button', text, className); node.type = 'button';
            node.addEventListener('click', onClick);
            return node;
        };

        function writeState(push = false) {
            const params = state.params;
            params.set('view', 'graphs'); params.set('source', state.source); params.set('tab', state.tab);
            legacyParams.forEach(key => params.delete(key));
            const settings = { graphMetric: state.metric, graphScale: state.scale, graphExpand: Array.from(state.expanded).join(','), tableSort: state.tableSort, tableDirection: state.tableDirection };
            Object.entries(settings).forEach(([key, value]) => { if (value === '') params.delete(key); else params.set(key, String(value)); });
            const url = new URL(window.location.href); url.search = params.toString();
            if (url.href !== window.location.href) window.history[push ? 'pushState' : 'replaceState']({}, '', url.toString());
        }

        function updateControls() {
            get('filter-source').value = state.source;
            get('include-groups').checked = state.params.get('includeGroups') === 'true';
            get('include-featured').checked = state.params.get('includeFeatured') === 'true';
            get('groups-control').hidden = state.source !== 'artists';
            get('featured-control').hidden = state.source !== 'artists';
            get('item-limit').value = state.params.get('limit') || '';
            get('limit-entity').value = state.params.get('limitEntity') || 'song';
            root.querySelectorAll('[data-breakdown]').forEach(item => {
                const active = item.dataset.breakdown === state.tab;
                item.classList.toggle('active', active); item.setAttribute('aria-pressed', String(active));
            });
        }

        function selectMetric(metric) {
            const sorted = columnFor(state.tableSort);
            if (sorted && sorted.metric === state.metric) state.tableSort = metric + '.' + sorted.stat;
            state.metric = metric; state.expanded.delete(metric);
            render();
        }

        function render() {
            if (!dataset) return;
            if (!dataset.available.has(state.metric)) state.metric = dataset.available.has('plays') ? 'plays' : Array.from(dataset.available)[0] || 'plays';
            state.expanded.delete(state.metric);
            updateControls();
            const totals = metricTotals(dataset.rows);
            get('summary').replaceChildren();
            Object.entries(metrics).forEach(([metric, label]) => {
                const available = dataset.available.has(metric);
                const male = dataset.rows.reduce((sum, row) => sum + measure(row, metric).male, 0);
                const card = button(null, 'graphs-summary-card', () => selectMetric(metric));
                card.setAttribute('aria-pressed', String(state.metric === metric)); card.disabled = !available;
                const track = el('span', null, 'graphs-mini-track'); track.setAttribute('aria-hidden', 'true');
                genders.forEach(gender => {
                    const segment = el('span', null, 'graphs-bar-segment ' + gender);
                    segment.style.width = share(dataset.rows.reduce((sum, row) => sum + measure(row, metric)[gender], 0), totals[metric]) + '%';
                    track.append(segment);
                });
                card.append(el('span', label), el('strong', available ? formatValue(totals[metric], metric) : 'Unavailable'), track,
                    el('small', available ? share(male, totals[metric]).toFixed(1) + '% male' : '', 'graphs-summary-male'));
                get('summary').append(card);
            });
            get('table-title').textContent = state.tab === 'general' ? `${metrics[state.metric]}: gender breakdown` : `${metrics[state.metric]} by ${breakdowns[state.tab].toLowerCase()}`;
            const notes = [];
            if (dataset.rows.length && !totals[state.metric]) notes.push('No recorded values for this measure in the selected scope.');
            if (state.tab === 'listenYear') notes.push('Items heard in multiple years count in each year.');
            get('table-note').textContent = notes.join(' ');
            renderTable(totals);
            writeState();
        }

        function renderTable(totals) {
            const table = get('data-table'); table.replaceChildren();
            const rows = dataset.rows;
            const columns = tableColumns(state.metric, dataset.available, state.expanded);
            if (state.tableSort !== 'name' && !columns.some(item => item.key === state.tableSort)) {
                const stat = columnFor(state.tableSort)?.stat;
                state.tableSort = state.metric + '.' + (stat || 'total');
            }
            const metricLabel = metrics[state.metric].toLowerCase();
            table.append(el('caption', `${metrics[state.metric]} by ${breakdowns[state.tab].toLowerCase()}: ${rows.length.toLocaleString()} categories. The first columns use ${metricLabel}; Share % is each category's part of the total across all categories. Other measures show totals and can be expanded to the same breakdown.`, 'sr-only'));
            const head = el('thead'), groups = el('tr', null, 'graphs-group-row'), header = el('tr'), body = el('tbody');
            const focusable = new Map();
            const selectedGroup = el('th', `Selected: ${metrics[state.metric]}`, 'graphs-group-selected');
            selectedGroup.scope = 'colgroup'; selectedGroup.colSpan = 2 + tableStats.length; groups.append(selectedGroup);
            Object.keys(metrics).filter(metric => metric !== state.metric && dataset.available.has(metric)).forEach((metric, index) => {
                const expanded = state.expanded.has(metric);
                const cell = el('th', null, 'graphs-group' + (index === 0 ? ' graphs-separator' : '') + (expanded ? ' graphs-expanded' : ''));
                cell.scope = 'colgroup'; cell.colSpan = expanded ? tableStats.length : 1;
                if (expanded) cell.append(el('span', metrics[metric], 'graphs-group-label'));
                const toggle = button(expanded ? 'Collapse' : 'Expand', 'graphs-expand-toggle', () => {
                    if (expanded) {
                        state.expanded.delete(metric);
                        if (columnFor(state.tableSort)?.metric === metric) state.tableSort = metric + '.total';
                    } else state.expanded.add(metric);
                    renderTable(totals).get('toggle:' + metric).focus(); writeState();
                });
                toggle.setAttribute('aria-expanded', String(expanded));
                toggle.setAttribute('aria-label', `${expanded ? 'Collapse' : 'Expand'} ${metrics[metric]}`);
                focusable.set('toggle:' + metric, toggle); cell.append(toggle); groups.append(cell);
            });
            function sortHeading(cell, key, label, accessibleLabel) {
                cell.scope = 'col'; cell.dataset.sortKey = key;
                const active = key === state.tableSort;
                cell.setAttribute('aria-sort', active ? state.tableDirection === 'asc' ? 'ascending' : 'descending' : 'none');
                const nextDirection = active ? state.tableDirection === 'desc' ? 'asc' : 'desc' : key === 'name' && !state.tab.endsWith('Year') ? 'asc' : 'desc';
                const sort = button(label, 'graphs-table-sort', () => {
                    state.tableSort = key; state.tableDirection = nextDirection;
                    renderTable(totals).get(key).focus(); writeState();
                });
                sort.setAttribute('aria-label', `${accessibleLabel}: sort ${nextDirection === 'asc' ? 'ascending' : 'descending'}`);
                const indicator = el('span', active ? state.tableDirection === 'asc' ? '↑' : '↓' : '↕', 'graphs-table-sort-indicator');
                indicator.setAttribute('aria-hidden', 'true'); sort.append(indicator);
                focusable.set(key, sort); cell.append(sort);
            }
            const category = el('th', null, 'graphs-category-column');
            sortHeading(category, 'name', 'Category', 'Category'); header.append(category);
            const split = el('th', null, 'graphs-split-column'); split.scope = 'col';
            split.append(el('span', 'Split', 'graphs-split-label'));
            [['share', 'share'], ['relative', 'volume']].forEach(([scale, label]) => {
                const option = button(label, 'graphs-scale-option', () => { state.scale = scale; render(); });
                option.setAttribute('aria-pressed', String(state.scale === scale));
                option.setAttribute('aria-label', `Bar length: ${scale === 'share' ? 'gender share within each category' : 'volume compared with the largest category'}`);
                split.append(option);
            });
            header.append(split);
            const firstComparison = columns.find(item => item.comparison);
            const cellClass = item => [item.gender, item === firstComparison ? 'graphs-separator' : '', item.comparison && state.expanded.has(item.metric) ? 'graphs-expanded' : ''].filter(Boolean).join(' ');
            columns.forEach(item => {
                const cell = el('th', null, cellClass(item));
                sortHeading(cell, item.key, item.label, item.comparison && item.stat === 'total' ? item.label + ' total' : metrics[item.metric] + ' ' + item.label); header.append(cell);
            });
            head.append(groups, header);
            const max = Math.max(0, ...rows.map(row => measure(row, state.metric).total));
            sortTableRows(rows, state.tableSort, state.tableDirection, totals).forEach(row => {
                const tr = el('tr'), name = el('th', row.name, 'graphs-category-column'); name.scope = 'row'; tr.append(name);
                const selected = measure(row, state.metric), bar = el('td', null, 'graphs-split-column');
                const track = el('div', null, 'graphs-bar-track'); track.setAttribute('aria-hidden', 'true');
                genders.forEach(gender => {
                    const segment = el('span', null, 'graphs-bar-segment ' + gender);
                    segment.style.width = share(selected[gender], state.scale === 'relative' ? max : selected.total) + '%'; track.append(segment);
                });
                const description = `${share(selected.total, totals[state.metric]).toFixed(1)}% of total; ` + genders.map(gender => `${gender === 'other' ? 'Other / unknown' : gender[0].toUpperCase() + gender.slice(1)}: ${formatValue(selected[gender], state.metric)} (${share(selected[gender], selected.total).toFixed(1)}%)`).join('; ');
                bar.title = description; bar.append(track, el('span', description, 'sr-only')); tr.append(bar);
                columns.forEach(item => {
                    const value = columnValue(row, item, totals);
                    tr.append(el('td', item.percent ? value.toFixed(1) + '%' : formatValue(value, item.metric), [cellClass(item), item.ofTotal ? 'graphs-share' : ''].filter(Boolean).join(' ')));
                });
                body.append(tr);
            });
            if (!rows.length) {
                const row = el('tr'), cell = el('td', 'No data matches these filters. Edit or clear the filters to explore again.', 'graphs-empty');
                cell.colSpan = columns.length + 2; row.append(cell); body.append(row);
            }
            table.append(head, body);
            return focusable;
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

        root.querySelectorAll('[data-breakdown]').forEach(item => item.addEventListener('click', () => {
            const leavingYears = state.tab.endsWith('Year');
            state.tab = item.dataset.breakdown;
            if (state.tab.endsWith('Year')) { state.tableSort = 'name'; state.tableDirection = 'desc'; }
            else if (leavingYears && state.tableSort === 'name') { state.tableSort = state.metric + '.total'; state.tableDirection = 'desc'; }
            updateControls(); writeState(true); load();
        }));
        get('retry').addEventListener('click', load);
        ['include-groups', 'include-featured', 'item-limit', 'limit-entity'].forEach(id => get(id).addEventListener('change', () => {
            state.params.set('includeGroups', String(get('include-groups').checked)); state.params.set('includeFeatured', String(get('include-featured').checked));
            const limit = Number.parseInt(get('item-limit').value, 10);
            if (limit > 0) { state.params.set('limit', String(Math.min(999, limit))); state.params.set('limitEntity', get('limit-entity').value); }
            else { state.params.delete('limit'); state.params.delete('limitEntity'); }
            updateControls(); writeState(true); load();
        }));
        document.addEventListener('playground:filters-applied', event => {
            const params = new URLSearchParams(event.detail.params);
            uiParams.forEach(key => params.delete(key)); listParams.forEach(key => params.delete(key));
            state.params = params; state.source = event.detail.source;
            updateControls(); writeState(true); load();
        });
        window.addEventListener('popstate', () => { state = readState(new URLSearchParams(window.location.search)); updateControls(); load(); });
        updateControls(); load();
        return { load };
    }

    if (typeof module !== 'undefined' && module.exports) module.exports = { normalizeData, share, formatValue, readState, apiParams, tableColumns, sortTableRows, createExplorer };
    if (typeof document !== 'undefined') document.addEventListener('DOMContentLoaded', () => createExplorer(document, window, window.fetch.bind(window)));
})();
