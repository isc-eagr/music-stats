/*
 * Chart Overview (Weekly, Hot 100, TRL, Vato's Cuntdown).
 * Renders the summary, the sort pills + levels popover, the Table / Cards / Leaderboard views,
 * the chart-run row and infinite scroll from window.chartOverviewState (first page) and
 * /charts/overview/data. All state lives in the URL; sorting and view changes navigate.
 * Admin tools (library links, TRL chart links, merges, imports) live in chart-overview-admin.js.
 */
(function () {
    'use strict';

    const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const TIER_LABELS = { 1: '#1', 5: 'Top 5', 10: 'Top 10', 20: 'Top 20', 50: 'Top 50', 100: 'Top 100' };
    const TIER_KEYS = { 1: 'top1', 5: 'top5', 10: 'top10', 20: 'top20', 50: 'top50', 100: 'top100' };
    const DATE_BASE = {
        weekly: key => '/charts/weekly/' + encodeURIComponent(key),
        hot100: key => '/misc/billboard-hot-100/recaps?date=' + encodeURIComponent(key),
        trl: key => '/misc/trl/recaps?date=' + encodeURIComponent(key),
        cuntdown: key => '/misc/vatos-cuntdown/recaps?date=' + encodeURIComponent(key)
    };

    const root = document.getElementById('chartOverview');
    if (!root) return;

    const state = Object.assign({ rows: [], sorts: [], tiers: [1, 5, 10] }, window.chartOverviewState || {});
    state.view = root.dataset.view;
    state.loading = false;
    const unit = state.unit === 'day' ? 'Day' : 'Week';
    const units = unit + 's';
    const isExternal = state.chart !== 'weekly';
    const runCache = new Map();

    // ------------------------------------------------------------------ helpers

    function esc(value) {
        return String(value == null ? '' : value).replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
    }

    function fmtNum(value) {
        return value == null ? '' : Number(value).toLocaleString('en-US');
    }

    function fmtDate(iso) {
        if (!iso) return '';
        const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso);
        return m ? m[3] + '-' + MONTHS[Number(m[2]) - 1] + '-' + m[1] : iso;
    }

    /** Weekly rows carry the week's start date; the app labels a week by its last day. */
    function chartDate(iso) {
        if (state.chart !== 'weekly' || !/^\d{4}-\d{2}-\d{2}/.test(iso || '')) return iso;
        const date = new Date(iso.slice(0, 10) + 'T00:00:00Z');
        date.setUTCDate(date.getUTCDate() + 6);
        return date.toISOString().slice(0, 10);
    }

    function ago(iso) {
        const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso || '');
        if (!m) return '';
        const years = Math.floor((Date.now() - Date.UTC(+m[1], +m[2] - 1, +m[3])) / (365.25 * 86400000));
        return years >= 1 ? years + (years === 1 ? ' year ago' : ' years ago') : 'this year';
    }

    function band(peak) {
        if (peak == null) return 4;
        return peak === 1 ? 1 : peak <= 5 ? 2 : peak <= 10 ? 3 : 4;
    }

    function genderClass(row) {
        return row.gender === 'male' ? 'ct-male' : row.gender === 'female' ? 'ct-female' : '';
    }

    function readStore(key, fallback) {
        try { const value = localStorage.getItem(key); return value == null ? fallback : value; } catch (e) { return fallback; }
    }

    function writeStore(key, value) {
        try { localStorage.setItem(key, value); } catch (e) { /* private mode */ }
    }

    function dateLink(iso, key) {
        if (!iso) return '<span class="ct-dim">-</span>';
        const href = key && DATE_BASE[state.chart] ? DATE_BASE[state.chart](key) : null;
        const text = fmtDate(chartDate(iso));
        return href ? '<a class="co-date-link" href="' + esc(href) + '">' + esc(text) + '</a>' : esc(text);
    }

    function peakBadge(peak) {
        if (peak == null) return '<span class="ct-dim">-</span>';
        return '<span class="co-pk co-band-' + band(peak) + '">#' + peak + '</span>';
    }

    function coverHtml(row, kind) {
        const full = row.coverUrl ? row.coverUrl.replace('?thumbnail=true', '') : '';
        const hover = row.coverHoverUrl ? '<img class="co-cover-hover" src="' + esc(row.coverHoverUrl) + '" alt="" loading="lazy">' : '';
        const img = row.coverUrl ? '<img src="' + esc(row.coverUrl) + '" alt="" loading="lazy">' : '';
        const zoom = row.coverHoverUrl ? row.coverHoverUrl.replace('?thumbnail=true', '') : full;
        return '<span class="co-cover' + (kind === 'artist' ? ' co-cover-artist' : '') + '"' + (zoom ? ' data-zoom="' + esc(zoom) + '"' : '') + '>' + img + hover + '</span>';
    }

    function nameLink(text, href, cls) {
        if (!text) return '';
        return href ? '<a class="' + cls + '" href="' + esc(href) + '">' + esc(text) + '</a>' : '<span class="' + cls + '">' + esc(text) + '</span>';
    }

    function weeklyMetricLink(value, artistId, list, peak) {
        if (state.chart !== 'weekly' || !artistId || !value) return fmtNum(value);
        const href = '/' + list + '?sortby=plays&sortdir=desc&artist=' + artistId + '&weeklyChartPeak=' + peak;
        return '<a href="' + href + '">' + fmtNum(value) + '</a>';
    }

    function pips(count, span) {
        if (!count) return '';
        const shown = Math.min(count, 14);
        let html = '<span class="co-pips" aria-hidden="true">';
        for (let i = 0; i < shown; i++) {
            const height = Math.max(3, Math.min(16, Math.round(3 + (span / count) * 2)));
            html += '<i style="height:' + height + 'px"></i>';
        }
        return html + '</span>';
    }

    // ------------------------------------------------------------------ columns

    function songColumns() {
        const cols = [
            { key: 'span', label: units, num: true, dir: 'desc',
                cell: r => '<span class="co-num">' + fmtNum(r.span) + '</span>' + (r.linkedSongTitles && r.linkedSongTitles.length
                    ? ' <span class="co-flag" title="' + esc('Combined song versions: ' + r.linkedSongTitles.join(', ')) + '">+' + r.linkedSongTitles.length + '</span>' : '')
                    + '<div class="ct-bar" style="width:' + barWidth(r.span) + '%"></div>' },
            { key: 'actualSpan', label: 'Actual days', num: true, dir: 'desc', only: ['trl'],
                cell: r => r.actualSpan == null ? '<span class="ct-dim">-</span>' : '<span class="co-num">' + fmtNum(r.actualSpan) + '</span>' + gapTag(r) },
            { key: 'peak', label: 'Peak', dir: 'asc', center: true, cell: r => peakBadge(r.peak) },
            { key: 'spanAtPeak', label: units + ' at peak', num: true, dir: 'desc', cell: r => fmtNum(r.spanAtPeak) },
            { key: 'debutDate', label: 'Debut date', dir: 'desc', cell: r => dateLink(r.debutDate, r.debutKey) + '<span class="ct-muted">' + esc(ago(r.debutDate)) + '</span>' },
            { key: 'debutPosition', label: 'Debut pos.', num: true, dir: 'asc', cell: r => fmtNum(r.debutPosition) },
            { key: 'peakDate', label: 'Peak date', dir: 'desc', cell: r => dateLink(r.peakDate, r.peakKey) },
            { key: 'lastDate', label: 'Last date', dir: 'desc', cell: r => dateLink(r.lastDate, r.lastKey) }
        ];
        state.tiers.forEach((tier, index) => cols.push({
            key: TIER_KEYS[tier], label: TIER_LABELS[tier] + ' ' + units.toLowerCase(), num: true, dir: 'desc', hidden: true,
            cell: r => fmtNum(r.tiers ? r.tiers[index] : null)
        }));
        return cols;
    }

    function albumColumns() {
        if (!isExternal) {
            return [
                { key: 'span', label: units, num: true, dir: 'desc', cell: r => '<span class="co-num">' + fmtNum(r.span) + '</span><div class="ct-bar" style="width:' + barWidth(r.span) + '%"></div>' },
                { key: 'peak', label: 'Peak', dir: 'asc', center: true, cell: r => peakBadge(r.peak) },
                { key: 'spanAtPeak', label: units + ' at peak', num: true, dir: 'desc', cell: r => fmtNum(r.spanAtPeak) },
                { key: 'debutDate', label: 'Debut date', dir: 'desc', cell: r => dateLink(r.debutDate, r.debutKey) },
                { key: 'debutPosition', label: 'Debut pos.', num: true, dir: 'asc', cell: r => fmtNum(r.debutPosition) },
                { key: 'peakDate', label: 'Peak date', dir: 'desc', cell: r => dateLink(r.peakDate, r.peakKey) },
                { key: 'lastDate', label: 'Last date', dir: 'desc', cell: r => dateLink(r.lastDate, r.lastKey) }
            ];
        }
        return [
            { key: 'songs', label: 'Songs charted', num: true, dir: 'desc', cell: r => fmtNum(r.songs) },
            { key: 'span', label: 'Total ' + units.toLowerCase(), num: true, dir: 'desc', cell: r => '<span class="co-num">' + fmtNum(r.span) + '</span><div class="ct-bar" style="width:' + barWidth(r.span) + '%"></div>' },
            { key: 'peak', label: 'Highest peak', dir: 'asc', center: true, cell: r => peakBadge(r.peak) },
            { key: 'numberOnes', label: '#1 songs', num: true, dir: 'desc', cell: r => titled(fmtNum(r.numberOnes), r.numberOneTitles) },
            { key: 'spanAtNumberOne', label: units + ' at #1', num: true, dir: 'desc', cell: r => fmtNum(r.spanAtNumberOne) },
            { key: 'debutDate', label: 'First debut', dir: 'desc', cell: r => dateLink(r.debutDate, r.debutKey) },
            { key: 'lastDate', label: 'Last appearance', dir: 'desc', cell: r => dateLink(r.lastDate, r.lastKey) }
        ];
    }

    function artistColumns() {
        if (!isExternal) {
            return [
                { key: 'songs', label: 'Charted', group: 'Songs', num: true, dir: 'desc', cell: r => weeklyMetricLink(r.songs, r.artistId, 'songs', topParam('topSong', 20)) },
                { key: 'span', label: units, group: 'Songs', num: true, dir: 'desc', cell: r => '<span class="co-num">' + fmtNum(r.span) + '</span><div class="ct-bar" style="width:' + barWidth(r.span) + '%"></div>' },
                { key: 'numberOnes', label: '#1 songs', group: 'Songs', num: true, dir: 'desc',
                    cell: r => titled(weeklyMetricLink(r.numberOnes, r.artistId, 'songs', 1), r.numberOneSongTitles) + pips(r.numberOnes, r.spanAtNumberOne) },
                { key: 'spanAtNumberOne', label: units + ' at #1', group: 'Songs', num: true, dir: 'desc', cell: r => fmtNum(r.spanAtNumberOne) },
                { key: 'albums', label: 'Charted', group: 'Albums', sep: true, num: true, dir: 'desc', cell: r => weeklyMetricLink(r.albums, r.artistId, 'albums', topParam('topAlbum', 10)) },
                { key: 'albumSpan', label: units, group: 'Albums', num: true, dir: 'desc', cell: r => fmtNum(r.albumSpan) },
                { key: 'albumNumberOnes', label: '#1 albums', group: 'Albums', num: true, dir: 'desc', cell: r => titled(weeklyMetricLink(r.albumNumberOnes, r.artistId, 'albums', 1), r.numberOneAlbumTitles) },
                { key: 'albumSpanAtNumberOne', label: units + ' at #1', group: 'Albums', num: true, dir: 'desc', cell: r => fmtNum(r.albumSpanAtNumberOne) }
            ];
        }
        return [
            { key: 'songs', label: 'Songs charted', num: true, dir: 'desc', cell: r => fmtNum(r.songs) },
            { key: 'span', label: 'Total ' + units.toLowerCase(), num: true, dir: 'desc', cell: r => '<span class="co-num">' + fmtNum(r.span) + '</span><div class="ct-bar" style="width:' + barWidth(r.span) + '%"></div>' },
            { key: 'peak', label: 'Highest peak', dir: 'asc', center: true, cell: r => peakBadge(r.peak) },
            { key: 'numberOnes', label: '#1 songs', num: true, dir: 'desc', cell: r => titled(fmtNum(r.numberOnes), r.numberOneSongTitles) + pips(r.numberOnes, r.spanAtNumberOne) },
            { key: 'spanAtNumberOne', label: units + ' at #1', num: true, dir: 'desc', cell: r => fmtNum(r.spanAtNumberOne) },
            { key: 'debutDate', label: 'First debut', dir: 'desc', cell: r => dateLink(r.debutDate, r.debutKey) },
            { key: 'lastDate', label: 'Last appearance', dir: 'desc', cell: r => dateLink(r.lastDate, r.lastKey) }
        ];
    }

    function topParam(name, fallback) {
        const value = new URLSearchParams(window.location.search).get(name);
        return value ? value : fallback;
    }

    function titled(html, titles) {
        if (!titles || !titles.length) return html;
        return '<span title="' + esc(titles.join('\n')) + '">' + html + '</span>';
    }

    function gapTag(row) {
        if (row.actualSpan == null || row.actualSpan === row.span) return '';
        const diff = row.actualSpan - row.span;
        return '<span class="co-gap' + (diff > 0 ? ' co-gap-up' : '') + '" title="' + (diff < 0 ? 'Skipped episodes during the run' : 'More chart days than counted') + '">' + (diff > 0 ? '+' : '') + diff + '</span>';
    }

    let maxSpan = 1;
    function barWidth(value) {
        return Math.max(2, Math.round((value || 0) / maxSpan * 100));
    }

    function nameColumn() {
        if (state.tab === 'album') {
            return { key: 'title', label: 'Album', dir: 'asc', cell: r => nameLink(r.albumName, r.albumId ? '/albums/' + r.albumId : null, 'co-name') + nameLink(r.artistName, r.artistId ? '/artists/' + r.artistId : null, 'co-sub') };
        }
        if (state.tab === 'artist') {
            return { key: 'artist', label: 'Artist', dir: 'asc', cell: r => nameLink(r.artistName, r.artistId ? '/artists/' + r.artistId : null, 'co-name') };
        }
        return { key: 'title', label: 'Song', dir: 'asc', cell: r => nameLink(r.title, r.songId ? '/songs/' + r.songId : null, 'co-name')
            + '<span class="co-sub-line">' + nameLink(r.artistName, r.artistId ? '/artists/' + r.artistId : null, 'co-sub') + flags(r) + '</span>' };
    }

    function flags(row) {
        let html = '';
        if (row.retired) html += '<span class="co-flag" title="TRL Hall of Fame">Hall of fame</span>';
        if (isExternal && !row.matched) html += '<button type="button" class="co-flag co-unmatched" data-co-match>Not in library</button>';
        if (state.chart === 'trl' && row.actualSpan == null) html += '<button type="button" class="co-flag co-unmatched" data-co-chart-link>No chart entries</button>';
        return html;
    }

    function allColumns() {
        const metrics = state.tab === 'album' ? albumColumns() : state.tab === 'artist' ? artistColumns() : songColumns();
        return metrics.filter(col => !col.only || col.only.includes(state.chart));
    }

    /** Every key the server can sort by, with a label, for the popover select. */
    function sortOptions() {
        const options = [nameColumn()];
        if (state.tab !== 'artist') options.push({ key: 'artist', label: 'Artist', dir: 'asc' });
        return options.concat(allColumns()).map(col => ({ key: col.key, label: (col.group ? col.group + ' · ' : '') + col.label, dir: col.dir }));
    }

    function sortLabel(key) {
        const option = sortOptions().find(item => item.key === key);
        return option ? option.label : key;
    }

    function hiddenKey() {
        return 'co-hidden:' + state.chart + ':' + state.tab;
    }

    function hiddenColumns() {
        const stored = readStore(hiddenKey(), null);
        if (stored != null) return new Set(stored ? stored.split(',') : []);
        return new Set(allColumns().filter(col => col.hidden).map(col => col.key));
    }

    // ------------------------------------------------------------------ navigation

    function navigateWith(mutate) {
        const params = new URLSearchParams(window.location.search);
        params.delete('page');
        mutate(params);
        window.location.assign(window.location.pathname + (params.toString() ? '?' + params.toString() : ''));
    }

    function applySorts(sorts) {
        navigateWith(params => {
            params.delete('sort');
            sorts.forEach(spec => params.append('sort', spec.key + ':' + spec.dir));
        });
    }

    function dataUrl(extra) {
        const url = new URL('/charts/overview/data', window.location.origin);
        new URLSearchParams(window.location.search).forEach((value, key) => {
            if (key !== 'view' && key !== 'page') url.searchParams.append(key, value);
        });
        url.searchParams.set('chart', state.chart);
        url.searchParams.set('tab', state.tab);
        Object.entries(extra || {}).forEach(([key, value]) => url.searchParams.set(key, value));
        return url;
    }

    // ------------------------------------------------------------------ sort pills + popover

    function renderSortPills() {
        const list = document.getElementById('coSortPills');
        if (!list) return;
        list.innerHTML = state.sorts.map((spec, index) =>
            '<button type="button" class="toolbar-sort-row" data-co-pop>'
            + '<span class="toolbar-sort-label">' + (index + 1) + '</span>' + esc(sortLabel(spec.key))
            + '<span class="co-dir" aria-label="' + (spec.dir === 'asc' ? 'Ascending' : 'Descending') + '">' + (spec.dir === 'asc' ? '&#8593;' : '&#8595;') + '</span></button>'
        ).join('') + '<button type="button" class="toolbar-sort-row co-sort-add" data-co-pop><span class="toolbar-sort-label">+</span>Then by...</button>';
    }

    let draft = null;
    function openSortPopover() {
        const host = document.getElementById('coSort');
        closeSortPopover();
        draft = state.sorts.map(spec => Object.assign({}, spec));
        const pop = document.createElement('div');
        pop.className = 'co-pop';
        pop.id = 'coSortPop';
        host.classList.add('co-sort-open');
        host.append(pop);
        renderDraft();
    }

    function closeSortPopover() {
        const pop = document.getElementById('coSortPop');
        if (pop) pop.remove();
        document.getElementById('coSort')?.classList.remove('co-sort-open');
        draft = null;
    }

    function renderDraft() {
        const pop = document.getElementById('coSortPop');
        if (!pop) return;
        const options = sortOptions();
        pop.innerHTML = '<div class="co-pop-head"><span class="co-eyebrow">Sort levels</span><span class="co-hint">Drag to reorder</span></div>'
            + draft.map((spec, index) => '<div class="co-level" draggable="true" data-index="' + index + '">'
                + '<span class="co-grip" aria-hidden="true">&#8942;&#8942;</span>'
                + '<span class="co-level-num">' + (index + 1) + '</span>'
                + '<select aria-label="Sort level ' + (index + 1) + '">' + options.map(option =>
                    '<option value="' + esc(option.key) + '"' + (option.key === spec.key ? ' selected' : '') + '>' + esc(option.label) + '</option>').join('') + '</select>'
                + '<span class="co-ad"><button type="button" data-dir="asc" class="' + (spec.dir === 'asc' ? 'active' : '') + '">Asc</button>'
                + '<button type="button" data-dir="desc" class="' + (spec.dir === 'desc' ? 'active' : '') + '">Desc</button></span>'
                + '<button type="button" class="co-icon-btn" data-remove aria-label="Remove level ' + (index + 1) + '">&times;</button></div>').join('')
            + '<div class="co-row" style="justify-content:space-between;padding:4px 2px 0">'
            + '<span class="co-row"><button type="button" class="co-small-btn" data-add>+ Add level</button><button type="button" class="co-small-btn" data-reset>Reset to default</button></span>'
            + '<span class="co-row"><button type="button" class="co-small-btn" data-cancel>Cancel</button><button type="button" class="co-small-btn toolbar-btn-primary" data-apply>Apply</button></span></div>';
    }

    function bindSortPopover() {
        const host = document.getElementById('coSort');
        if (!host) return;
        let dragFrom = null;
        host.addEventListener('click', event => {
            const pill = event.target.closest('[data-co-pop]');
            if (pill) {
                if (document.getElementById('coSortPop')) closeSortPopover(); else openSortPopover();
                return;
            }
            const level = event.target.closest('.co-level');
            if (event.target.closest('[data-dir]') && level) {
                draft[+level.dataset.index].dir = event.target.closest('[data-dir]').dataset.dir;
                renderDraft();
            } else if (event.target.closest('[data-remove]') && level) {
                draft.splice(+level.dataset.index, 1);
                renderDraft();
            } else if (event.target.closest('[data-add]')) {
                const used = new Set(draft.map(spec => spec.key));
                const next = sortOptions().find(option => !used.has(option.key));
                if (next) draft.push({ key: next.key, dir: next.dir });
                renderDraft();
            } else if (event.target.closest('[data-reset]')) {
                navigateWith(params => params.delete('sort'));
            } else if (event.target.closest('[data-cancel]')) {
                closeSortPopover();
            } else if (event.target.closest('[data-apply]')) {
                const seen = new Set();
                applySorts(draft.filter(spec => !seen.has(spec.key) && seen.add(spec.key)));
            }
        });
        host.addEventListener('change', event => {
            const level = event.target.closest('.co-level');
            if (!level || event.target.tagName !== 'SELECT') return;
            const option = sortOptions().find(item => item.key === event.target.value);
            draft[+level.dataset.index] = { key: event.target.value, dir: option ? option.dir : 'desc' };
            renderDraft();
        });
        host.addEventListener('dragstart', event => {
            const level = event.target.closest('.co-level');
            if (!level) return;
            dragFrom = +level.dataset.index;
            level.classList.add('co-dragging');
            event.dataTransfer.effectAllowed = 'move';
        });
        host.addEventListener('dragover', event => {
            const level = event.target.closest('.co-level');
            if (dragFrom == null || !level) return;
            event.preventDefault();
            host.querySelectorAll('.co-drop-target').forEach(item => item.classList.remove('co-drop-target'));
            level.classList.add('co-drop-target');
        });
        host.addEventListener('drop', event => {
            const level = event.target.closest('.co-level');
            if (dragFrom == null || !level) return;
            event.preventDefault();
            const [moved] = draft.splice(dragFrom, 1);
            draft.splice(+level.dataset.index, 0, moved);
            dragFrom = null;
            renderDraft();
        });
        host.addEventListener('dragend', () => { dragFrom = null; renderDraft(); });
        document.addEventListener('click', event => {
            // Popover buttons re-render the popover, so a detached target still counts as inside.
            if (draft && event.target.isConnected && !event.target.closest('#coSort')) closeSortPopover();
        });
        document.addEventListener('keydown', event => {
            if (event.key === 'Escape') { closeSortPopover(); closeHeaderMenu(); }
        });
    }

    // ------------------------------------------------------------------ summary

    function renderGenders() {
        const box = document.getElementById('coGenders');
        const summary = state.summary;
        if (!box || !summary || !summary.genders) return;
        const total = Math.max(1, summary.total);
        const g = summary.genders;
        const pct = value => (value / total * 100).toFixed(1) + '%';
        box.innerHTML = '<span class="co-chip male"><span class="co-dot"></span>Male ' + fmtNum(g.male) + ' · ' + pct(g.male) + '</span>'
            + '<span class="co-chip female"><span class="co-dot"></span>Female ' + fmtNum(g.female) + ' · ' + pct(g.female) + '</span>'
            + (g.other ? '<span class="co-chip other"><span class="co-dot"></span>Mixed / other ' + fmtNum(g.other) + ' · ' + pct(g.other) + '</span>' : '');
    }

    /** Tiny male / female / other bar under a tile's number (male first). */
    function genderBar(counts) {
        const total = counts.male + counts.female + counts.other;
        if (!total) return '<span class="co-kpi-bar"></span>';
        const part = key => counts[key] ? '<i class="' + key + '" style="width:' + (counts[key] / total * 100) + '%"></i>' : '';
        return '<span class="co-kpi-bar" aria-hidden="true">' + part('male') + part('female') + part('other') + '</span>';
    }

    function kpi(label, counts, gold) {
        const total = counts.male + counts.female + counts.other;
        return '<div class="co-kpi"><span class="co-eyebrow">' + esc(label) + '</span><span class="co-kpi-value"' + (gold ? ' style="color:#fbbf24"' : '') + '>' + fmtNum(total) + '</span>'
            + '<span class="co-kpi-sub">' + fmtNum(counts.male) + ' male · ' + fmtNum(counts.female) + ' female' + (counts.other ? ' · ' + fmtNum(counts.other) + ' other' : '') + '</span>'
            + genderBar(counts) + '</div>';
    }

    /** Rows that peaked inside the first {@code bands} peak bands (#1, #2-5, #6-10), per gender. */
    function peakedWithin(spread, bands) {
        const sum = key => (spread && spread[key] ? spread[key].slice(0, bands).reduce((a, b) => a + b, 0) : 0);
        return { male: sum('male'), female: sum('female'), other: sum('other') };
    }

    function renderSummary() {
        const box = document.getElementById('coSummary');
        const s = state.summary;
        if (!box || !s) return;
        const artist = state.tab === 'artist';
        const noun = state.tab === 'album' ? 'albums' : artist ? 'artists' : 'songs';
        const title = noun.charAt(0).toUpperCase() + noun.slice(1);
        const tiles = kpi(title + ' charted', s.genders)
            + kpi(artist ? 'Artists with a #1' : '#1 ' + noun, peakedWithin(s.peakSpread, 1), true)
            + kpi(artist ? 'Artists in the top 5' : title + ' in the top 5', peakedWithin(s.peakSpread, 2))
            + kpi(artist ? 'Artists in the top 10' : title + ' in the top 10', peakedWithin(s.peakSpread, 3));
        box.innerHTML = '<div class="co-summary">' + tiles + spreadHtml(s.peakSpread) + '</div>'
            + (artist && state.view === 'leaderboard' ? leaderboardHtml(s) : '');
    }

    function spreadHtml(spread) {
        if (!spread) return '';
        const row = (label, color, values) => {
            const sum = values.reduce((a, b) => a + b, 0);
            if (!sum) return '';
            return '<div class="co-spread-row"><span style="color:' + color + '">' + label + '</span><div class="co-bar-stack">'
                + values.map((value, index) => value ? '<span class="co-band-' + (index + 1) + '" style="width:' + (value / sum * 100) + '%" title="' + value + '">' + value + '</span>' : '').join('')
                + '</div></div>';
        };
        return '<div class="co-panel co-spread"><div class="co-row" style="justify-content:space-between"><span class="co-eyebrow">Peak spread</span><span class="co-row" style="gap:12px">'
            + '<span class="co-legend"><i class="co-band-1"></i>#1</span><span class="co-legend"><i class="co-band-2"></i>#2-5</span>'
            + '<span class="co-legend"><i class="co-band-3"></i>#6-10</span><span class="co-legend"><i class="co-band-4"></i>#11+</span></span></div>'
            + row('Male', 'var(--co-male-text)', spread.male) + row('Female', 'var(--co-female-text)', spread.female) + row('Other', 'var(--co-muted)', spread.other) + '</div>';
    }

    function splitBar(values) {
        const sum = values.male + values.female + values.other;
        if (!sum) return '<div class="co-split"></div>';
        const part = (key, label) => values[key] ? '<span class="' + key + '" style="width:' + (values[key] / sum * 100) + '%" title="' + label + ' ' + fmtNum(values[key]) + '">'
            + (label ? label + ' ' : '') + fmtNum(values[key]) + ' · ' + Math.round(values[key] / sum * 100) + '%</span>' : '';
        return '<div class="co-split">' + part('male', 'Male') + part('female', 'Female') + part('other', '') + '</div>';
    }

    function leaderboardHtml(s) {
        const board = (title, color, rows) => {
            const max = Math.max(1, ...rows.map(row => row.span));
            const items = rows.map((row, index) => '<div class="co-lb"' + (index >= 8 ? ' data-more hidden' : '') + '>'
                + '<span class="co-num" style="color:#8b8b94">' + (index + 1) + '</span>'
                + '<span class="co-avatar">' + (row.coverUrl ? '<img src="' + esc(row.coverUrl) + '" alt="" loading="lazy">' : '') + '</span>'
                + '<div style="min-width:0">' + nameLink(row.artistName, row.artistId ? '/artists/' + row.artistId : null, 'co-name')
                + '<div class="co-lb-track"><i style="width:' + (row.span / max * 100) + '%;background:' + color + '"></i></div></div>'
                + '<span class="co-num" style="text-align:right">' + fmtNum(row.span) + '</span>'
                + '<span class="co-gold-text">' + (row.numberOnes ? '#1 &#215;' + row.numberOnes : '') + '</span></div>').join('');
            const more = rows.length > 8 ? '<button type="button" class="co-small-btn" data-co-more style="margin-top:10px">Show more</button>' : '';
            return '<div class="co-panel"><div class="co-row" style="justify-content:space-between;margin-bottom:6px"><span class="co-eyebrow" style="color:' + color + '">' + title + '</span>'
                + '<span class="co-hint">' + units + ' · #1 songs</span></div>' + (items || '<div class="co-hint">No artists</div>') + more + '</div>';
        };
        return '<div class="co-panel" style="display:flex;flex-direction:column;gap:12px;margin-bottom:16px">'
            + '<div class="co-row" style="justify-content:space-between"><span class="co-eyebrow">Who owns the chart</span><span class="co-hint">' + fmtNum(s.total) + ' artists · '
            + fmtNum(s.spanTotal) + ' song ' + units.toLowerCase() + (state.chart === 'weekly' ? ' · ' + fmtNum(s.albumSpanTotal) + ' album ' + units.toLowerCase() : '') + '</span></div>'
            + '<div class="co-split-row"><span class="co-hint">Song ' + units.toLowerCase() + '</span>' + splitBar(s.spanByGender) + '</div>'
            + '<div class="co-split-row"><span class="co-hint">' + units + ' at #1</span>' + splitBar(s.numberOneSpanByGender) + '</div></div>'
            + '<div class="co-boards" style="margin-bottom:16px">' + board('Male artists · song ' + units.toLowerCase(), '#60a5fa', s.topMale || [])
            + board('Female artists · song ' + units.toLowerCase(), '#f472b6', s.topFemale || []) + '</div>';
    }

    // ------------------------------------------------------------------ table

    function sortBadge(key) {
        const index = state.sorts.findIndex(spec => spec.key === key);
        if (index < 0) return '';
        const spec = state.sorts[index];
        const level = index === 0 ? '' : index < 3 ? ' ct-level-' + (index + 1) : ' ct-level-n';
        return '<span class="ct-sort-badge' + level + '">' + (index + 1) + ' ' + (spec.dir === 'asc' ? '&#8593;' : '&#8595;') + '</span>';
    }

    function headerCell(col, extraClass) {
        const sorted = state.sorts.some(spec => spec.key === col.key);
        const canAdd = !sorted;
        return '<th data-sort="' + esc(col.key) + '" class="' + (sorted ? 'ct-sorted ' : '') + (col.num ? 'co-r ' : '') + (col.center ? 'co-c ' : '') + (col.sep ? 'co-sep ' : '') + (extraClass || '') + '"'
            + ' style="text-align:' + (col.num ? 'right' : col.center ? 'center' : 'left') + '">'
            + '<span class="ct-h">' + esc(col.label) + sortBadge(col.key)
            + (canAdd ? '<button type="button" class="ct-add ct-available" data-co-add title="Then by ' + esc(col.label) + '">+ ' + (state.sorts.length + 1) + '</button>' : '')
            + '<button type="button" class="ct-menu-btn" data-co-menu aria-haspopup="menu" aria-label="' + esc(col.label) + ' options">&#9662;</button></span></th>';
    }

    function renderTable() {
        const content = document.getElementById('coContent');
        const hidden = hiddenColumns();
        const cols = allColumns().filter(col => !hidden.has(col.key));
        const expandable = state.tab === 'song';
        const name = nameColumn();
        maxSpan = Math.max(1, ...state.rows.map(row => row.span || 0));
        const groups = state.tab === 'artist' && !isExternal;
        let head = '';
        if (groups) {
            const songs = cols.filter(col => col.group === 'Songs').length;
            const albums = cols.filter(col => col.group === 'Albums').length;
            head += '<tr><th colspan="3"></th>' + (songs ? '<th class="co-col-group co-sep" colspan="' + songs + '">Songs</th>' : '')
                + (albums ? '<th class="co-col-group co-sep" colspan="' + albums + '">Albums</th>' : '') + '</tr>';
        }
        head += '<tr>' + (expandable ? '<th class="co-expand-col" aria-label="Chart run"></th>' : '') + '<th style="text-align:right">#</th><th aria-label="Cover"></th>'
            + headerCell(name) + cols.map(col => headerCell(col)).join('') + '</tr>';

        content.innerHTML = '<div class="co-table-meta"><span class="co-hint">Click a header to sort by it. Hover a header and click its + badge to add the next sort level, or open its arrow menu for more.</span>'
            + '<span class="co-row"><span class="co-hint" id="coCount"></span>'
            + '<button type="button" class="co-small-btn" id="coColumnsBtn">Columns</button></span></div>'
            + '<div id="tableView"><div class="table-scroll top-table-container co-table-wrap">'
            + '<table class="sticky-head catalog-table co-table"><thead>' + head + '</thead><tbody id="coBody"></tbody></table></div></div>'
            + '<div class="co-load-row" id="coLoadRow"></div>';
        document.getElementById('coBody').innerHTML = state.rows.map((row, index) => rowHtml(row, index, cols, name, expandable)).join('')
            || '<tr><td colspan="' + (cols.length + 4) + '" class="co-empty">Nothing matches these filters.</td></tr>';
        updateCount();
    }

    function rowHtml(row, index, cols, name, expandable) {
        const rank = index + 1;
        const canRun = expandable && row.runKey && !(state.chart === 'trl' && row.actualSpan == null);
        return '<tr class="' + genderClass(row) + '" data-index="' + index + '">'
            + (expandable ? '<td class="co-expand-col">' + (canRun ? '<button type="button" class="co-twisty" data-co-run aria-expanded="false" aria-label="Show chart run">&#9654;</button>' : '') + '</td>' : '')
            + '<td class="rank-col' + (rank <= 3 ? ' ct-medal-' + rank : '') + '" style="text-align:right">' + rank + '</td>'
            + '<td style="width:44px">' + coverHtml(row, state.tab) + '</td>'
            + '<td>' + name.cell(row) + '</td>'
            + cols.map(col => '<td class="' + (col.num ? 'ct-num ' : '') + (col.sep ? 'co-sep' : '') + '" style="text-align:' + (col.num ? 'right' : col.center ? 'center' : 'left') + '">' + col.cell(row) + '</td>').join('')
            + '</tr>';
    }

    function columnValue(col, row) {
        const values = { span: row.span, actualSpan: row.actualSpan, peak: row.peak, spanAtPeak: row.spanAtPeak, debutPosition: row.debutPosition,
            songs: row.songs, numberOnes: row.numberOnes, spanAtNumberOne: row.spanAtNumberOne, albums: row.albums, albumSpan: row.albumSpan,
            albumNumberOnes: row.albumNumberOnes, albumSpanAtNumberOne: row.albumSpanAtNumberOne };
        if (col.key in values) return values[col.key];
        const tierIndex = state.tiers.findIndex(tier => TIER_KEYS[tier] === col.key);
        return tierIndex >= 0 && row.tiers ? row.tiers[tierIndex] : null;
    }

    function updateCount() {
        const count = document.getElementById('coCount');
        if (count) count.textContent = fmtNum(Math.min(state.rows.length, state.totalCount)) + ' of ' + fmtNum(state.totalCount);
        const loadAll = document.getElementById('coLoadAll');
        if (loadAll) loadAll.hidden = !state.hasMore;
        const loadRow = document.getElementById('coLoadRow');
        if (loadRow) loadRow.innerHTML = state.hasMore ? '<button type="button" class="toolbar-btn" data-co-more-rows>Load ' + fmtNum(state.pageSize) + ' more</button>' : '';
    }

    // ------------------------------------------------------------------ header menu + columns popover

    function closeHeaderMenu() {
        document.querySelectorAll('.ct-menu.co-menu').forEach(menu => menu.remove());
        document.querySelectorAll('[data-co-menu][aria-expanded="true"]').forEach(btn => btn.setAttribute('aria-expanded', 'false'));
    }

    function openMenu(anchor, items) {
        closeHeaderMenu();
        const menu = document.createElement('div');
        menu.className = 'ct-menu co-menu';
        menu.setAttribute('role', 'menu');
        items.forEach(item => {
            if (item === 'sep') { menu.insertAdjacentHTML('beforeend', '<div class="ct-menu-sep"></div>'); return; }
            const button = document.createElement('button');
            button.type = 'button';
            button.className = 'ct-menu-item';
            button.setAttribute('role', 'menuitem');
            button.innerHTML = esc(item.text) + (item.hint ? '<span class="ct-menu-k">' + esc(item.hint) + '</span>' : '');
            button.addEventListener('click', () => { closeHeaderMenu(); item.run(); });
            menu.append(button);
        });
        document.body.append(menu);
        const rect = anchor.getBoundingClientRect();
        menu.style.top = (window.scrollY + rect.bottom + 6) + 'px';
        menu.style.left = Math.max(8, Math.min(window.scrollX + rect.left - 120, window.scrollX + document.documentElement.clientWidth - 270)) + 'px';
        anchor.setAttribute('aria-expanded', 'true');
        menu.querySelector('button')?.focus();
    }

    function columnByKey(key) {
        return [nameColumn()].concat(allColumns()).find(col => col.key === key) || { key, dir: 'desc', label: key };
    }

    function headerMenu(th, button) {
        const key = th.dataset.sort;
        const col = columnByKey(key);
        const index = state.sorts.findIndex(spec => spec.key === key);
        const items = [
            { text: 'Sort ascending', hint: 'replaces sort', run: () => applySorts([{ key, dir: 'asc' }]) },
            { text: 'Sort descending', run: () => applySorts([{ key, dir: 'desc' }]) }
        ];
        if (index < 0) items.push({ text: 'Add as level ' + (state.sorts.length + 1), run: () => applySorts(state.sorts.concat([{ key, dir: col.dir }])) });
        if (index > 0) items.push({ text: 'Move to level 1', run: () => applySorts([state.sorts[index]].concat(state.sorts.filter((_, i) => i !== index))) });
        if (index >= 0 && state.sorts.length > 1) items.push({ text: 'Remove from sort', run: () => applySorts(state.sorts.filter((_, i) => i !== index)) });
        if (index >= 0) items.push({ text: 'Flip direction', run: () => applySorts(state.sorts.map((spec, i) => i === index ? { key, dir: spec.dir === 'asc' ? 'desc' : 'asc' } : spec)) });
        if (allColumns().some(item => item.key === key)) {
            items.push('sep', { text: 'Hide column', run: () => { const hidden = hiddenColumns(); hidden.add(key); writeStore(hiddenKey(), Array.from(hidden).join(',')); renderView(); } });
        }
        openMenu(button, items);
    }

    function columnsMenu(button) {
        const hidden = hiddenColumns();
        openMenu(button, allColumns().map(col => ({
            text: (hidden.has(col.key) ? '□ ' : '☑ ') + (col.group ? col.group + ' · ' : '') + col.label,
            run: () => {
                if (hidden.has(col.key)) hidden.delete(col.key); else hidden.add(col.key);
                writeStore(hiddenKey(), Array.from(hidden).join(','));
                renderView();
                columnsMenu(document.getElementById('coColumnsBtn'));
            }
        })).concat(['sep', { text: 'Show all columns', run: () => { writeStore(hiddenKey(), ''); renderView(); } }]));
    }

    // ------------------------------------------------------------------ chart runs

    function fetchRuns(keys) {
        const missing = keys.filter(key => key && !runCache.has(key));
        if (!missing.length) return Promise.resolve();
        const url = new URL('/charts/overview/runs', window.location.origin);
        url.searchParams.set('chart', state.chart);
        missing.forEach(key => url.searchParams.append('key', key));
        const pending = fetch(url).then(response => {
            if (!response.ok) throw new Error('HTTP ' + response.status);
            return response.json();
        }).then(data => {
            missing.forEach(key => runCache.set(key, data[key] || []));
        });
        missing.forEach(key => runCache.set(key, pending));
        return pending;
    }

    function getRun(key) {
        const cached = runCache.get(key);
        if (Array.isArray(cached)) return Promise.resolve(cached);
        if (cached) return cached.then(() => runCache.get(key));
        return fetchRuns([key]).then(() => runCache.get(key));
    }

    // Runs in the shared chart-run.js shape: {onChart, position, periodKey, dateRange}
    const runUnit = state.unit === 'day' ? 'days' : 'weeks';
    const periodHref = DATE_BASE[state.chart]('KEY').replace('KEY', '{key}');
    const albumRuns = new Map();

    function runWeeks(points) {
        return (points || []).map(point => ({
            onChart: point.position != null,
            position: point.position,
            periodKey: point.key,
            dateRange: state.chart === 'weekly' ? (point.label || point.key) : fmtDate(point.label || point.key)
        }));
    }

    /** Chart size the run bars are drawn against. */
    function runScale() {
        if (state.chart === 'hot100') return 100;
        if (state.chart === 'weekly') return state.tab === 'album' ? 10 : 20;
        return 10;
    }

    /** Song runs come from the batched overview endpoint; weekly album runs from the album's own run. */
    function runWeeksFor(row) {
        if (state.tab === 'song') return getRun(row.runKey).then(runWeeks);
        if (!albumRuns.has(row.albumId)) {
            const pending = fetch('/charts/album/' + row.albumId + '/run').then(response => {
                if (!response.ok) throw new Error('HTTP ' + response.status);
                return response.json();
            }).then(data => (data && data.weeks) || []);
            albumRuns.set(row.albumId, pending);
            pending.catch(() => albumRuns.delete(row.albumId));
        }
        return albumRuns.get(row.albumId);
    }

    function hasCardRun(row) {
        return state.tab === 'song' ? !!row.runKey : !isExternal && !!row.albumId;
    }

    /** Gender tokens for the Chart History card / box / modal styles. */
    function scopeClass(row) {
        return 'chh-scope' + (row.gender === 'male' ? ' chh-male' : row.gender === 'female' ? ' chh-female' : '');
    }

    function runRowHtml(row, loading) {
        // TRL and Vato's Cuntdown were top 10 countdowns, so their Top 10 tile would always read 100% of the run.
        const topTenOnly = state.chart === 'trl' || state.chart === 'cuntdown';
        const tiles = state.tiers.map((tier, index) => {
            if (topTenOnly && tier === 10) return '';
            const value = row.tiers ? row.tiers[index] || 0 : 0;
            const pct = row.span ? Math.round(value / row.span * 100) : 0;
            return '<div class="co-tile' + (tier === 1 ? ' co-tile-one' : '') + '"><span class="co-eyebrow"' + (tier === 1 ? ' style="color:#fcd34d"' : '') + '>' + units + ' at ' + (tier === 1 ? '#1' : TIER_LABELS[tier].toLowerCase()) + '</span>'
                + '<span class="co-tile-value">' + fmtNum(value) + '</span><span class="co-tile-bar"><i style="width:' + pct + '%"></i></span>'
                + '<span class="co-hint">' + pct + '% of the run</span></div>';
        }).join('');
        const boxes = loading ? '<span class="co-hint">Loading chart run...</span>' : '';
        const runLinks = (row.songId ? '<a class="co-small-btn" href="/songs/' + row.songId + '">Open song</a>' : '')
            + (row.debutKey ? '<a class="co-small-btn" href="' + esc(DATE_BASE[state.chart](row.debutKey)) + '">Debut ' + unit.toLowerCase() + ' chart</a>' : '')
            + (row.peakKey ? '<a class="co-small-btn" href="' + esc(DATE_BASE[state.chart](row.peakKey)) + '">Peak ' + unit.toLowerCase() + ' chart</a>' : '')
            + (state.chart === 'trl' && row.actualSpan != null ? '<button type="button" class="co-small-btn" data-co-unlink>Unlink chart entries</button>' : '');
        return '<div class="co-run ' + scopeClass(row) + '"><div class="co-tiles">' + tiles + '</div>'
            + '<span class="chh-eyebrow">Chart run</span>'
            + '<div class="chh-run-boxes">' + boxes + '</div>'
            + '<div class="co-row"><span class="co-hint">Each box is a ' + unit.toLowerCase() + ' on the chart. Click one to open that chart; hover for the date.</span><span class="co-spacer"></span>' + runLinks + '</div></div>';
    }

    function toggleRun(button) {
        const tr = button.closest('tr');
        const row = state.rows[+tr.dataset.index];
        const next = tr.nextElementSibling;
        if (next && next.classList.contains('co-run-row')) {
            next.remove();
            button.setAttribute('aria-expanded', 'false');
            button.innerHTML = '&#9654;';
            return;
        }
        const colspan = tr.children.length;
        const detail = document.createElement('tr');
        detail.className = 'co-run-row ' + genderClass(row) + (row.gender ? '' : ' co-run-other');
        detail.dataset.index = tr.dataset.index;
        detail.innerHTML = '<td colspan="' + colspan + '">' + runRowHtml(row, true) + '</td>';
        tr.after(detail);
        button.setAttribute('aria-expanded', 'true');
        button.innerHTML = '&#9660;';
        runWeeksFor(row).then(weeks => {
            detail.firstElementChild.innerHTML = runRowHtml(row, false);
            window.ChartRun.renderBoxes(detail.querySelector('.chh-run-boxes'), window.ChartRun.trim(weeks), periodHref, runUnit);
        }).catch(() => {
            detail.firstElementChild.innerHTML = '<div class="co-run"><span class="co-hint" style="color:#f87171">Failed to load chart run.</span></div>';
        });
    }

    // ------------------------------------------------------------------ cards
    // Same hit cards and run modal as the detail Chart History tab (detail-chart-history.css, chart-run.js).

    // Like the catalog list cards, the default sort (Debut date on songs, Weeks elsewhere; see ChartOverviewService.parseSorts)
    // gets neither a highlight nor a chip, whether it comes from the URL or is the fallback.
    const DEFAULT_SORT_KEY = state.tab === 'song' ? 'debutDate' : 'span';
    const sortChosen = !!state.sorts[0] && state.sorts[0].key !== DEFAULT_SORT_KEY;

    /** The primary sort key, when the card shows that metric itself (highlighted like the catalog list cards). */
    function shownSortKey(row) {
        const key = sortChosen && state.sorts[0] ? state.sorts[0].key : null;
        if (key === 'span' || key === 'peak' || key === 'debutDate' || key === 'peakDate') return key;
        if (key === 'spanAtPeak' && row.spanAtPeak > 0) return key;
        // Hot 100 / TRL / Cuntdown album cards show songs charted and #1 songs under the artist
        if (state.tab === 'album' && isExternal && (key === 'songs' || (key === 'numberOnes' && row.numberOnes))) return key;
        return null;
    }

    /** A chip with the sort value, only for metrics the card does not show. */
    function cardSortTag(row) {
        const primary = state.sorts[0];
        if (!sortChosen || !primary || shownSortKey(row)) return '';
        const col = columnByKey(primary.key);
        const value = primary.key === 'title' || primary.key === 'artist' ? null : columnValue(col, row);
        const dates = { debutDate: row.debutDate, peakDate: row.peakDate, lastDate: row.lastDate };
        let text = null;
        if (primary.key in dates) text = fmtDate(chartDate(dates[primary.key]));
        else if (value != null) text = (primary.key === 'peak' || primary.key === 'debutPosition' ? '#' : '') + fmtNum(value);
        return text ? '<span class="co-card-tag">' + esc(col.label) + ': ' + esc(text) + '</span>' : '';
    }

    function unitWord(count) {
        return state.unit === 'day' ? (count === 1 ? 'day' : 'days') : (count === 1 ? 'week' : 'weeks');
    }

    function initials(text) {
        return String(text || '').split(/\s+/).filter(Boolean).slice(0, 2).map(word => word.charAt(0)).join('').toUpperCase();
    }

    function cardCover(row, title, href) {
        const img = row.coverUrl ? '<img loading="lazy" decoding="async" alt="" src="' + esc(row.coverUrl) + '" onerror="this.hidden=true;this.nextElementSibling.hidden=false;">' : '';
        const inner = img + '<span class="chh-cover-ph"' + (row.coverUrl ? ' hidden' : '') + '>' + esc(initials(title)) + '</span>';
        return href ? '<a class="chh-cover" href="' + esc(href) + '" tabindex="-1" aria-hidden="true">' + inner + '</a>' : '<span class="chh-cover" aria-hidden="true">' + inner + '</span>';
    }

    function cardDate(label, iso, key, cls) {
        if (!iso) return '';
        const text = label + ' <b>' + esc(fmtDate(chartDate(iso))) + '</b>';
        return key ? '<a class="' + cls + '" href="' + esc(DATE_BASE[state.chart](key)) + '">' + text + '</a>' : '<span class="' + cls + '">' + text + '</span>';
    }

    function cardHtml(row, index) {
        const song = state.tab === 'song';
        const title = song ? row.title : row.albumName;
        const href = song ? (row.songId ? '/songs/' + row.songId : null) : (row.albumId ? '/albums/' + row.albumId : null);
        const one = row.peak === 1;
        const sorted = shownSortKey(row);
        const hl = key => sorted === key ? ' co-hl' : '';
        const peakHref = row.peakKey ? DATE_BASE[state.chart](row.peakKey) : null;
        const atPeak = row.spanAtPeak || 0;
        const peakTag = peakHref ? 'a' : 'span';
        const peak = '<' + peakTag + ' class="chh-hit-peak"' + (peakHref ? ' href="' + esc(peakHref) + '"' : '')
            + (row.peakDate ? ' title="' + esc('First reached #' + row.peak + ' on ' + fmtDate(chartDate(row.peakDate))) + '"' : '') + '>'
            + '<span class="chh-peak-line"><span class="chh-num' + (one ? ' is-one' : '') + hl('peak') + '">' + (row.peak != null ? '#' + row.peak : '-') + '</span>'
            + (atPeak > 0 ? '<span class="chh-times' + (one ? ' is-one' : '') + hl('spanAtPeak') + '" title="' + esc(atPeak + ' ' + unitWord(atPeak) + ' at #' + row.peak) + '">&#215;' + fmtNum(atPeak) + '</span>' : '')
            + '</span><span class="chh-mini-label">' + (song || !isExternal ? 'peak' : 'highest peak') + '</span></' + peakTag + '>';
        let extra = '';
        if (song) extra = '<span class="chh-hit-sub co-sub-line">' + flags(row) + '</span>';
        else if (isExternal) extra = '<span class="chh-hit-sub"><span class="co-hl-inline' + hl('songs') + '">' + fmtNum(row.songs) + (row.songs === 1 ? ' song' : ' songs') + ' charted</span>'
            + (row.numberOnes ? ' · <span class="co-hl-inline' + hl('numberOnes') + '">' + fmtNum(row.numberOnes) + ' #1</span>' : '') + '</span>';
        const head = '<div class="chh-hit-head">' + cardCover(row, title, href)
            + '<div class="chh-hit-title">' + nameLink(title, href, 'chh-hit-name')
            + nameLink(row.artistName, row.artistId ? '/artists/' + row.artistId : null, 'chh-hit-sub') + extra + '</div>' + peak + '</div>';
        const run = hasCardRun(row)
            ? '<button type="button" class="chh-run" data-co-card-run aria-haspopup="dialog" aria-label="' + esc('Open the chart run of ' + (title || '')) + '" title="Open the full chart run">'
              + '<span class="chh-run-bars is-loading" aria-hidden="true"></span></button>'
            : '';
        const linked = row.linkedSongTitles && row.linkedSongTitles.length
            ? ' <span class="co-flag" title="' + esc('Combined song versions: ' + row.linkedSongTitles.join(', ')) + '">+' + row.linkedSongTitles.length + '</span>' : '';
        const actual = song && row.actualSpan != null ? ' · actual ' + fmtNum(row.actualSpan) + gapTag(row) : '';
        const foot = '<div class="chh-hit-foot"><span class="chh-hit-total' + hl('span') + '"><span><b class="chh-num">' + fmtNum(row.span) + '</b>' + linked + '</span>'
            + '<small>' + unitWord(row.span) + ' on chart' + actual + '</small></span>'
            + '<span class="chh-hit-dates">' + cardDate('Debut', row.debutDate, row.debutKey, 'chh-hit-debut' + hl('debutDate'))
            + cardDate('Peak', row.peakDate, row.peakKey, 'chh-hit-peakdate' + (one ? ' is-one' : '') + hl('peakDate')) + '</span></div>';
        return '<article class="chh-hit co-hit ' + scopeClass(row) + (one ? ' is-one' : '') + '" data-index="' + index + '"' + (hasCardRun(row) ? ' data-has-run' : '') + '>'
            + head + run + cardSortTag(row) + foot + '</article>';
    }

    let cardObserver = null;
    function renderCards() {
        const content = document.getElementById('coContent');
        const hint = state.rows.some(hasCardRun) ? 'Bars show the run, one per ' + unit.toLowerCase() + ', gold at #1. Click the bars for the full run.' : '';
        content.innerHTML = '<div class="co-table-meta"><span class="co-hint">' + hint + '</span><span class="co-hint" id="coCount"></span></div>'
            + '<div class="chh-grid" id="coCards">' + state.rows.map(cardHtml).join('') + '</div><div class="co-load-row" id="coLoadRow"></div>';
        if (!state.rows.length) content.querySelector('#coCards').innerHTML = '<div class="co-empty">Nothing matches these filters.</div>';
        updateCount();
        observeCardRuns();
    }

    /** Loads the run bars as cards scroll into view; song runs are fetched in batches. */
    function observeCardRuns() {
        if (cardObserver) cardObserver.disconnect();
        const cards = Array.from(document.querySelectorAll('.co-hit[data-has-run]:not([data-loaded])'));
        if (!cards.length) return;
        let queue = [];
        let timer = null;
        const flush = () => {
            const batch = queue.splice(0, 30);
            timer = null;
            const keys = state.tab === 'song' ? batch.map(card => state.rows[+card.dataset.index].runKey) : [];
            (keys.length ? fetchRuns(keys) : Promise.resolve()).then(() => batch.forEach(paintCardRun)).catch(() => batch.forEach(failCardRun));
            if (queue.length) timer = setTimeout(flush, 50);
        };
        cardObserver = new IntersectionObserver(entries => {
            entries.forEach(entry => {
                if (!entry.isIntersecting) return;
                cardObserver.unobserve(entry.target);
                entry.target.dataset.loaded = 'true';
                queue.push(entry.target);
            });
            if (queue.length && !timer) timer = setTimeout(flush, 30);
        }, { rootMargin: '300px' });
        cards.forEach(card => cardObserver.observe(card));
    }

    function paintCardRun(card) {
        const row = state.rows[+card.dataset.index];
        const bars = card.querySelector('.chh-run-bars');
        runWeeksFor(row).then(weeks => {
            bars.classList.remove('is-loading');
            window.ChartRun.renderBars(bars, window.ChartRun.trim(weeks), window.ChartRun.scaleOf(runScale(), weeks), runUnit, false);
        }).catch(() => failCardRun(card));
    }

    function failCardRun(card) {
        const bars = card.querySelector('.chh-run-bars');
        if (bars) bars.classList.remove('is-loading');
    }

    function openCardRun(card) {
        const row = state.rows[+card.dataset.index];
        const head = card.querySelector('.chh-hit-head').cloneNode(true);
        // Admin flag buttons only work on the card itself
        head.querySelectorAll('.co-sub-line').forEach(line => line.remove());
        const dates = card.querySelector('.chh-hit-dates');
        window.ChartRun.openModal({
            host: root,
            scopeClass: scopeClass(row),
            head,
            id: state.tab + ':' + (row.runKey || row.albumId),
            load: () => runWeeksFor(row).then(weeks => ({ weeks, scale: runScale() })),
            unit: runUnit,
            periodHref,
            foot: dates ? Array.from(dates.children).map(node => node.cloneNode(true)) : []
        });
    }

    // ------------------------------------------------------------------ views + paging

    function renderView() {
        closeHeaderMenu();
        renderGenders();
        renderSummary();
        if (state.view === 'cards') renderCards();
        else renderTable();
    }

    function appendPage(data) {
        state.rows = state.rows.concat(data.rows || []);
        state.hasMore = !!data.hasMore;
        state.page = data.page;
        state.totalCount = data.totalCount;
        if (state.view === 'cards') {
            const cards = document.getElementById('coCards');
            const start = state.rows.length - (data.rows || []).length;
            cards.insertAdjacentHTML('beforeend', (data.rows || []).map((row, offset) => cardHtml(row, start + offset)).join(''));
            updateCount();
            observeCardRuns();
        } else {
            // Append instead of re-rendering so open chart-run rows stay open while scrolling.
            const hidden = hiddenColumns();
            const cols = allColumns().filter(col => !hidden.has(col.key));
            const start = state.rows.length - (data.rows || []).length;
            const body = document.getElementById('coBody');
            body.insertAdjacentHTML('beforeend', (data.rows || []).map((row, offset) => rowHtml(row, start + offset, cols, nameColumn(), state.tab === 'song')).join(''));
            updateCount();
        }
    }

    function loadMore() {
        if (state.loading || !state.hasMore) return Promise.resolve();
        state.loading = true;
        return fetch(dataUrl({ page: (state.page || 0) + 1, size: state.pageSize }))
            .then(response => response.json())
            .then(appendPage)
            .finally(() => { state.loading = false; });
    }

    function loadAll() {
        if (!state.hasMore) return Promise.resolve();
        const button = document.getElementById('coLoadAll');
        if (button) { button.disabled = true; button.textContent = 'Loading all...'; }
        state.loading = true;
        return fetch(dataUrl({ all: 'true' })).then(response => response.json()).then(data => {
            state.rows = data.rows || [];
            state.hasMore = false;
            state.totalCount = data.totalCount;
            renderView();
        }).finally(() => {
            state.loading = false;
            if (button) { button.disabled = false; button.textContent = 'Load all'; }
        });
    }

    /** Reloads the rows already on screen (after a library link, a merge or an auto-link) without leaving the page. */
    function refresh() {
        const size = Math.max(state.pageSize || 50, state.rows.length);
        runCache.clear();
        albumRuns.clear();
        return fetch(dataUrl({ page: 0, size, fresh: 'true' })).then(response => response.json()).then(data => {
            state.rows = data.rows || [];
            state.hasMore = !!data.hasMore;
            state.totalCount = data.totalCount;
            state.page = Math.ceil(state.rows.length / Math.max(1, state.pageSize)) - 1;
            if (data.summary) state.summary = data.summary;
            renderView();
        });
    }

    function bindPage() {
        root.addEventListener('click', event => {
            const target = event.target;
            if (target.closest('[data-co-run]')) { toggleRun(target.closest('[data-co-run]')); return; }
            if (target.closest('[data-co-card-run]')) { openCardRun(target.closest('.co-hit')); return; }
            const menuBtn = target.closest('[data-co-menu]');
            if (menuBtn) { event.stopPropagation(); headerMenu(menuBtn.closest('th'), menuBtn); return; }
            const addBtn = target.closest('[data-co-add]');
            if (addBtn) {
                event.stopPropagation();
                const col = columnByKey(addBtn.closest('th').dataset.sort);
                applySorts(state.sorts.concat([{ key: col.key, dir: col.dir }]));
                return;
            }
            const th = target.closest('th[data-sort]');
            if (th && !target.closest('button')) {
                const col = columnByKey(th.dataset.sort);
                const primary = state.sorts[0];
                const dir = primary && primary.key === col.key && state.sorts.length === 1 ? (primary.dir === 'asc' ? 'desc' : 'asc') : col.dir;
                applySorts([{ key: col.key, dir }]);
                return;
            }
            if (target.closest('#coColumnsBtn')) { event.stopPropagation(); columnsMenu(target.closest('#coColumnsBtn')); return; }
            if (target.closest('[data-co-more-rows]')) { loadMore(); return; }
            if (target.closest('[data-co-more]')) {
                const panel = target.closest('.co-panel');
                panel.querySelectorAll('[data-more]').forEach(item => { item.hidden = false; });
                target.closest('[data-co-more]').remove();
                return;
            }
            const cover = target.closest('.co-cover[data-zoom]');
            if (cover && typeof window.openImageModal === 'function') { window.openImageModal(cover.dataset.zoom); }
        });
        document.addEventListener('click', event => {
            if (event.target.isConnected && !event.target.closest('.co-menu') && !event.target.closest('[data-co-menu]') && !event.target.closest('#coColumnsBtn')) closeHeaderMenu();
        });
        document.getElementById('coViewToggle')?.addEventListener('click', event => {
            const button = event.target.closest('[data-view]');
            if (!button || button.dataset.view === state.view) return;
            navigateWith(params => params.set('view', button.dataset.view));
        });
        document.getElementById('coIncludeFeatured')?.addEventListener('change', event => {
            navigateWith(params => { if (event.target.checked) params.set('includeFeatured', 'true'); else params.delete('includeFeatured'); });
        });
        document.getElementById('coLoadAll')?.addEventListener('click', loadAll);
        document.getElementById('coPlaylist')?.addEventListener('click', openPlaylist);
        const sentinel = document.getElementById('coSentinel');
        if (sentinel && 'IntersectionObserver' in window) {
            new IntersectionObserver(entries => {
                if (entries.some(entry => entry.isIntersecting)) loadMore();
            }, { rootMargin: '400px' }).observe(sentinel);
        }
    }

    function openPlaylist() {
        if (typeof window.openPlaylistGeneratorModal !== 'function') return;
        const button = document.getElementById('coPlaylist');
        const source = state.hasMore ? fetch(dataUrl({ all: 'true' })).then(response => response.json()).then(data => data.rows || []) : Promise.resolve(state.rows);
        if (button) button.disabled = true;
        source.then(rows => {
            const songs = rows.filter(row => row.songId).map(row => ({ id: row.songId, name: row.title, artist: row.artistName, album: row.albumName || '' }));
            window.openPlaylistGeneratorModal(songs, state.chartLabel + ' Song Overview (' + songs.length + ' tracks)');
        }).catch(() => window.showErrorToast && window.showErrorToast('Could not load the songs for the playlist.'))
            .finally(() => { if (button) button.disabled = false; });
    }

    window.chartOverview = { state, refresh, renderView, rowFor: el => state.rows[+el.closest('[data-index]').dataset.index], fmtDate, esc };

    renderSortPills();
    bindSortPopover();
    bindPage();
    renderView();
})();
