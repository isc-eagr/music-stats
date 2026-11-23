/*
 * Chart recaps (Billboard Hot 100, TRL, Vato's Cuntdown) in the Weekly Chart design: summary strip (#1 + male share),
 * Genre / Ethnicity / Language / Country tiles, the chart table with a chart-run panel per row, close calls
 * (Vato's Cuntdown) and fall-offs. One date at a time from <base>/recaps/data; the date stays in the URL.
 * Config comes from window.chartRecapConfig (ChartRecapPage): key, label, unit (week | day), tiers, base, dates.
 */
(function () {
    'use strict';

    const config = window.chartRecapConfig || {};
    const root = document.getElementById('chartRecap');
    if (!root || !config.base) return;

    const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const BREAKDOWNS = [
        ['genre', 'Genre', 'genreName'],
        ['ethnicity', 'Ethnicity', 'ethnicityName'],
        ['language', 'Language', 'languageName'],
        ['country', 'Country', 'countryName']
    ];
    const isDay = config.unit === 'day';
    const units = isDay ? 'Days' : 'Weeks';
    const spanKey = isDay ? 'days' : 'weeks';
    const atPeakKey = isDay ? 'daysAtPeak' : 'weeksAtPeak';
    const tiers = config.tiers && config.tiers.length ? config.tiers : [1, 5, 10];
    const runCache = new Map();
    const content = document.getElementById('recapContent');
    let current = null;

    // ------------------------------------------------------------------ helpers

    function esc(value) {
        return String(value == null ? '' : value).replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
    }

    /** "week" / "weeks" or "day" / "days" for a count. */
    function unitWord(count) {
        return (isDay ? 'day' : 'week') + (Number(count) === 1 ? '' : 's');
    }

    /** yyyy-MM-dd to dd/MM/yyyy. */
    function fmtDate(iso) {
        const parts = String(iso || '').split('-');
        return parts.length === 3 ? parts[2] + '/' + parts[1] + '/' + parts[0] : (iso || '-');
    }

    function weekday(iso) {
        const parts = String(iso || '').split('-').map(Number);
        if (parts.length !== 3 || parts.some(isNaN)) return '';
        return new Date(parts[0], parts[1] - 1, parts[2]).toLocaleDateString('en-US', { weekday: 'long' });
    }

    function genderTone(entry) {
        return entry.genderClass === 'gender-male' ? 'ct-male' : entry.genderClass === 'gender-female' ? 'ct-female' : '';
    }

    function recapUrl(date) {
        return config.base + '/recaps?date=' + encodeURIComponent(date);
    }

    function band(position) {
        return position === 1 ? 1 : position <= 5 ? 2 : position <= 10 ? 3 : 4;
    }

    function coverHtml(entry, cls, full) {
        const img = entry.songId
            ? '<img src="/songs/' + entry.songId + '/image' + (full ? '' : '?thumbnail=true') + '" alt="" loading="lazy" onerror="this.remove()">'
            : '';
        return entry.songId
            ? '<a class="co-cover ' + cls + '" href="/songs/' + entry.songId + '" tabindex="-1" aria-hidden="true">' + img + '</a>'
            : '<span class="co-cover ' + cls + ' rc-cover-empty" aria-hidden="true"></span>';
    }

    function nameHtml(entry, nameCls, artistCls) {
        const title = entry.songId
            ? '<a class="' + nameCls + '" href="/songs/' + entry.songId + '">' + esc(entry.songTitle) + '</a>'
            : '<span class="' + nameCls + ' rc-unlinked" title="Not linked to a library song">' + esc(entry.songTitle) + '</span>';
        const retired = entry.retired ? '<span class="co-flag rc-retired-flag" title="Retired to the TRL Hall of Fame">Retired</span>' : '';
        const artist = entry.artistId
            ? '<a class="' + artistCls + '" href="/artists/' + entry.artistId + '">' + esc(entry.artistName) + '</a>'
            : '<span class="' + artistCls + ' rc-unlinked">' + esc(entry.artistName) + '</span>';
        return '<span class="rc-name-line">' + title + retired + '</span>' + artist;
    }

    function movementHtml(entry, mode) {
        if (mode === 'falloff') return '<span class="mvmt mvmt-down">OFF</span>';
        if (entry.movement == null || entry.movement === '-') return '<span class="ct-dim">-</span>';
        return '<span class="mvmt ' + esc(entry.movementClass || '') + '">' + esc(entry.movement) + '</span>';
    }

    function peakHtml(entry, mode) {
        if (entry.peak == null) return '<span class="ct-dim">-</span>';
        const now = mode === 'chart' && Number(entry.peak) === Number(entry.position)
            ? '<span class="wk-peak-now wk-desk" title="At its peak on this chart">Peak</span>' : '';
        return '<span class="co-pk co-band-' + band(Number(entry.peak)) + '">#' + esc(entry.peak) + '</span>' + now;
    }

    // ------------------------------------------------------------------ summary + breakdown tiles

    function summaryHtml(entries) {
        const top = entries[0];
        const counts = { male: 0, female: 0, other: 0 };
        entries.forEach(e => { counts[e.genderClass === 'gender-male' ? 'male' : e.genderClass === 'gender-female' ? 'female' : 'other']++; });
        const total = entries.length;
        const pct = value => value * 100 / total;

        const meta = [];
        if (top[spanKey] != null) meta.push('<span><b class="co-num">' + top[spanKey] + '</b> ' + unitWord(top[spanKey]) + ' on chart</span>');
        if (Number(top.peak) === 1 && top[atPeakKey] != null) {
            meta.push('<span><b class="co-num">' + top[atPeakKey] + '</b> ' + unitWord(top[atPeakKey]) + ' at #1</span>');
        }
        const hero = '<div class="wk-hero ' + genderTone(top) + '">'
            + coverHtml(top, 'wk-hero-cover', true)
            + '<div class="wk-hero-body"><span class="co-eyebrow wk-gold">Number one</span>'
            + nameHtml(top, 'wk-hero-title', 'wk-hero-artist')
            + '<div class="wk-hero-meta">' + meta.join('') + '</div></div></div>';

        const share = Math.round(pct(counts.male));
        const kpi = '<div class="co-kpi wk-kpi"><span class="co-eyebrow">Male share</span>'
            + '<span class="co-kpi-value wk-male-value' + (counts.male === total ? ' wk-full' : '') + '">' + share + '%</span>'
            + '<span class="co-kpi-sub">' + counts.male + ' male · ' + counts.female + ' female' + (counts.other ? ' · ' + counts.other + ' other' : '') + '</span>'
            + '<span class="co-kpi-bar" aria-hidden="true"><i class="male" style="width:' + pct(counts.male) + '%"></i>'
            + '<i class="female" style="width:' + pct(counts.female) + '%"></i><i class="other" style="width:' + pct(counts.other) + '%"></i></span></div>';

        return '<div class="co-summary wk-summary">' + hero + kpi + '</div>';
    }

    /** Every value with its count, biggest first, entries without the value last as "Unknown" (same rules as the weekly chart). */
    function slicesFor(entries, field) {
        const counts = new Map();
        entries.forEach(e => {
            const name = e[field] && String(e[field]).trim() ? String(e[field]).trim() : 'Unknown';
            counts.set(name, (counts.get(name) || 0) + 1);
        });
        return Array.from(counts, ([name, count]) => ({ name, count, percent: Math.round(count * 100 / entries.length) }))
            .sort((a, b) => (a.name === 'Unknown') - (b.name === 'Unknown') || b.count - a.count || a.name.localeCompare(b.name));
    }

    function breakdownsHtml(entries) {
        return '<div class="wk-breakdowns">' + BREAKDOWNS.map(([key, label, field]) => {
            const slices = slicesFor(entries, field);
            const top = slices[0];
            const unknown = s => s.name === 'Unknown' ? ' wk-unknown' : '';
            const title = label + ': ' + slices.map(s => s.name + ' ' + s.count + ' (' + s.percent + '%)').join(', ');
            return '<div class="wk-bd wk-bd-' + key + '" title="' + esc(title) + '">'
                + '<div class="wk-bd-head"><span class="co-eyebrow">' + label + '</span>'
                + '<span class="wk-bd-top"><span>' + esc(top.name) + '</span> <b class="co-num">' + top.percent + '%</b></span></div>'
                + '<span class="wk-bd-bar" aria-hidden="true">' + slices.map((s, i) => '<i class="' + unknown(s).trim() + '" style="width:' + (s.count * 100 / entries.length) + '%;--i:' + i + '"></i>').join('') + '</span>'
                + '<div class="wk-bd-list">' + slices.map((s, i) => '<span class="wk-bd-item' + unknown(s) + '"><i style="--i:' + i + '"></i>' + esc(s.name) + ' <b class="co-num">' + s.count + '</b></span>').join('') + '</div>'
                + '</div>';
        }).join('') + '</div>';
    }

    // ------------------------------------------------------------------ tables

    function headHtml(sortable) {
        const sort = type => sortable ? ' data-sort="' + type + '"' : '';
        return '<thead><tr><th class="wk-c-run" aria-label="Chart run"></th>'
            + '<th class="wk-c-pos"' + sort('number') + '>#</th>'
            + '<th class="wk-c-move wk-desk">Move</th>'
            + '<th class="wk-c-cover" aria-label="Cover"></th>'
            + '<th class="wk-c-name"' + sort('text') + '>Song</th>'
            + '<th class="wk-c-num"' + sort('number') + '><span class="wk-desk">' + units + '</span><span class="wk-mob">' + (isDay ? 'Days' : 'Wks') + '</span></th>'
            + '<th class="wk-c-peak"' + sort('number') + '>Peak</th>'
            + '<th class="wk-c-num wk-desk"' + sort('number') + '>' + (isDay ? 'Days' : 'Wks') + ' at peak</th></tr></thead>';
    }

    function rowHtml(entry, index, mode) {
        const cls = ['wk-row', genderTone(entry), entry.retired ? 'rc-retired' : ''].filter(Boolean).join(' ');
        const position = Number(entry.position);
        const medal = mode === 'chart' && position <= 3 ? ' ct-medal-' + position : '';
        return '<tr class="' + cls + '" data-mode="' + mode + '" data-index="' + index + '">'
            + '<td class="wk-c-run"><button type="button" class="co-twisty wk-twisty" data-rc-run aria-expanded="false" aria-label="Show chart run">'
            + '<svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m9 6 6 6-6 6"/></svg></button></td>'
            + '<td class="wk-c-pos rank-col' + medal + '" data-sort-val="' + position + '"><span class="wk-pos">' + esc(entry.position) + '</span>'
            + '<span class="wk-mob wk-mv-mini">' + movementHtml(entry, mode) + '</span></td>'
            + '<td class="wk-c-move wk-desk">' + movementHtml(entry, mode) + '</td>'
            + '<td class="wk-c-cover">' + coverHtml(entry, 'wk-cover', false) + '</td>'
            + '<td class="wk-c-name" data-sort-val="' + esc(entry.songTitle) + '">' + nameHtml(entry, 'wk-name', 'wk-artist') + '</td>'
            + '<td class="wk-c-num" data-sort-val="' + (entry[spanKey] != null ? entry[spanKey] : -1) + '">' + (entry[spanKey] != null ? entry[spanKey] : '-') + '</td>'
            + '<td class="wk-c-peak" data-sort-val="' + (entry.peak != null ? entry.peak : 9999) + '">' + peakHtml(entry, mode) + '</td>'
            + '<td class="wk-c-num wk-desk" data-sort-val="' + (entry[atPeakKey] != null ? entry[atPeakKey] : -1) + '">' + (entry[atPeakKey] != null ? 'x' + entry[atPeakKey] : '-') + '</td>'
            + '</tr>';
    }

    function tableHtml(rows, mode, id) {
        return '<div class="table-scroll wk-table-shell"><table class="sticky-head catalog-table wk-table"' + (id ? ' id="' + id + '"' : '') + '>'
            + headHtml(mode === 'chart') + '<tbody>' + rows.map((row, i) => rowHtml(row, i, mode)).join('') + '</tbody></table></div>';
    }

    /** Vato's Cuntdown close calls: cover and name only (they have no position). */
    function closeCallsHtml(rows) {
        return '<div class="table-scroll wk-table-shell"><table class="sticky-head catalog-table wk-table rc-closecalls"><thead><tr>'
            + '<th class="wk-c-cover" aria-label="Cover"></th><th class="wk-c-name">Song</th></tr></thead><tbody>'
            + rows.map(e => '<tr class="wk-row ' + genderTone(e) + '">'
                + '<td class="wk-c-cover">' + coverHtml(e, 'wk-cover', false) + '</td>'
                + '<td class="wk-c-name">' + nameHtml(e, 'wk-name', 'wk-artist') + '</td></tr>').join('')
            + '</tbody></table></div>';
    }

    function sectionHead(title, hint) {
        return '<div class="wk-section-head"><h2 class="wk-section-title">' + title + '</h2>'
            + (hint ? '<span class="co-hint">' + hint + '</span>' : '') + '</div>';
    }

    function render(data) {
        const entries = (data.entries || []).filter(e => !e.isClosecall);
        const closeCalls = (data.entries || []).filter(e => e.isClosecall);
        const fallOffs = data.fallOffs || [];
        current = { date: data.date, entries, fallOffs };

        if (!entries.length) {
            content.innerHTML = '<div class="co-panel wk-empty"><h2>No chart for this date</h2><p>Pick another date with the arrows or Jump to.</p></div>';
            return;
        }
        const label = config.key === 'hot100' ? 'Hot 100' : isDay ? 'Countdown' : 'Chart';
        let html = '<section class="wk-section">' + summaryHtml(entries) + breakdownsHtml(entries)
            + sectionHead(label, entries.length + (entries.length === 1 ? ' entry' : ' entries')) + tableHtml(entries, 'chart', 'recapChartTable');
        if (closeCalls.length) {
            html += '<div class="wk-sub">' + sectionHead('Close calls', 'Just missed the countdown.') + closeCallsHtml(closeCalls) + '</div>';
        }
        if (fallOffs.length) {
            html += '<div class="wk-sub">' + sectionHead('Fall-offs', 'Dropped out since the previous chart. # is the last position held.') + tableHtml(fallOffs, 'falloff') + '</div>';
        }
        content.innerHTML = html + '</section>';
    }

    // ------------------------------------------------------------------ chart-run panel

    function runFacts(entry, mode) {
        const facts = [['Movement', movementHtml(entry, mode)], [units + ' at peak', entry[atPeakKey] != null ? 'x' + esc(entry[atPeakKey]) : '-']];
        return '<div class="wk-run-facts">' + facts.map(([label, value]) =>
            '<div class="wk-run-fact"><span class="co-eyebrow">' + label + '</span><b>' + value + '</b></div>').join('') + '</div>';
    }

    /** Recap run points ({chart_date, on_chart, position}) in the shape js/chart-run.js draws. */
    function toRunWeeks(points) {
        return points.map(p => ({ onChart: !!p.on_chart, position: p.position, periodKey: p.chart_date, dateRange: fmtDate(p.chart_date) }));
    }

    function runPanel(entry, mode, points) {
        const facts = runFacts(entry, mode);
        if (points === null) return '<div class="co-run">' + facts + '<span class="co-hint">Loading chart run...</span></div>';
        if (points === false) return '<div class="co-run">' + facts + '<span class="co-hint" style="color:#f87171">Failed to load chart run.</span></div>';

        const onChart = points.filter(p => p.on_chart && p.position != null);
        const total = onChart.length;
        const tiles = tiers.map((tier, index) => {
            const count = onChart.filter(p => p.position <= tier).length;
            const pct = total ? Math.round(count / total * 100) : 0;
            const one = index === 0;
            return '<div class="co-tile' + (one ? ' co-tile-one' : '') + '"><span class="co-eyebrow"' + (one ? ' style="color:#fcd34d"' : '') + '>'
                + units + ' at ' + (tier === 1 ? '#1' : 'top ' + tier) + '</span>'
                + '<span class="co-tile-value">' + count + '</span><span class="co-tile-bar"><i style="width:' + pct + '%"></i></span>'
                + '<span class="co-hint">' + pct + '% of the run</span></div>';
        }).join('');

        const open = entry.songId ? '<a class="co-small-btn" href="/songs/' + entry.songId + '">Open song</a>' : '';
        // The boxes are drawn by chart-run.js once the panel is in the page (paintBoxes)
        return '<div class="co-run">' + facts + '<div class="co-tiles">' + tiles + '</div>'
            + '<span class="chh-eyebrow">Chart run</span><div class="chh-run-boxes"></div>'
            + '<div class="co-row"><span class="co-hint">Each box is a ' + (isDay ? 'countdown' : 'week') + ' on the chart up to this date; this one is outlined. Click one to open it.</span>'
            + '<span class="co-spacer"></span>' + open + '</div></div>';
    }

    function paintBoxes(cell, points) {
        const boxes = cell.querySelector('.chh-run-boxes');
        if (!boxes || !window.ChartRun) return;
        const weeks = window.ChartRun.trim(toRunWeeks(points));
        window.ChartRun.renderBoxes(boxes, weeks, config.base + '/recaps?date={key}', config.unit, current.date);
    }

    function runUrl(entry) {
        return config.base + '/chart-run?' + (entry.songId
            ? 'songId=' + encodeURIComponent(entry.songId)
            : 'artist=' + encodeURIComponent(entry.artistName || '') + '&song=' + encodeURIComponent(entry.songTitle || ''));
    }

    async function toggleRun(button) {
        const row = button.closest('tr');
        const next = row.nextElementSibling;
        if (next && next.classList.contains('co-run-row')) {
            next.remove();
            button.setAttribute('aria-expanded', 'false');
            return;
        }
        const mode = row.dataset.mode;
        const entry = (mode === 'falloff' ? current.fallOffs : current.entries)[Number(row.dataset.index)];
        const detail = document.createElement('tr');
        const tone = genderTone(entry);
        detail.className = 'co-run-row chh-scope ' + (tone ? tone + (tone === 'ct-male' ? ' chh-male' : ' chh-female') : 'co-run-other');
        const cell = document.createElement('td');
        // Span only the visible columns: phones hide some, and extra spanned columns squeeze the name column
        cell.colSpan = Array.from(row.cells).filter(c => getComputedStyle(c).display !== 'none').length;
        cell.innerHTML = runPanel(entry, mode, null);
        detail.appendChild(cell);
        row.after(detail);
        button.setAttribute('aria-expanded', 'true');

        const url = runUrl(entry);
        try {
            if (!runCache.has(url)) {
                const response = await fetch(url);
                if (!response.ok) throw new Error('Failed to load chart run');
                runCache.set(url, await response.json());
            }
            const points = (runCache.get(url) || []).filter(p => !p.chart_date || p.chart_date <= current.date);
            cell.innerHTML = runPanel(entry, mode, points);
            paintBoxes(cell, points);
        } catch (error) {
            cell.innerHTML = runPanel(entry, mode, false);
        }
    }

    // ------------------------------------------------------------------ sorting

    function sortTable(th) {
        const table = th.closest('table');
        const tbody = table.tBodies[0];
        const type = th.dataset.sort;
        const sorted = th.classList.contains('ct-sorted');
        const dir = sorted && !th.classList.contains('wk-asc') ? 'asc' : sorted ? 'desc' : 'asc';
        table.querySelectorAll('thead th').forEach(h => h.classList.remove('ct-sorted', 'wk-asc'));
        th.classList.add('ct-sorted');
        if (dir === 'asc') th.classList.add('wk-asc');

        const groups = [];
        Array.from(tbody.rows).forEach(row => {
            if (row.classList.contains('wk-row')) groups.push({ row, extra: [] });
            else if (groups.length) groups[groups.length - 1].extra.push(row);
        });
        const value = row => row.cells[th.cellIndex]?.dataset.sortVal ?? '';
        groups.sort((a, b) => {
            const cmp = type === 'number' ? (parseFloat(value(a.row)) || 0) - (parseFloat(value(b.row)) || 0) : value(a.row).localeCompare(value(b.row));
            return dir === 'asc' ? cmp : -cmp;
        });
        groups.forEach(g => { tbody.appendChild(g.row); g.extra.forEach(r => tbody.appendChild(r)); });
    }

    // ------------------------------------------------------------------ date navigation

    const prevBtn = document.getElementById('recapPrev');
    const nextBtn = document.getElementById('recapNext');
    const daySelect = document.getElementById('recapDay');
    const monthSelect = document.getElementById('recapMonth');
    const yearSelect = document.getElementById('recapYear');

    function fillSelects() {
        const add = (select, label, options) => {
            select.add(new Option(label, ''));
            options.forEach(([value, text]) => select.add(new Option(text, value)));
        };
        add(daySelect, 'Day', Array.from({ length: 31 }, (_, i) => { const d = String(i + 1).padStart(2, '0'); return [d, d]; }));
        add(monthSelect, 'Month', MONTHS.map((m, i) => [String(i + 1).padStart(2, '0'), m]));
        const years = Array.from(new Set((config.availableDates || []).map(d => String(d).split('-')[0]))).sort();
        add(yearSelect, 'Year', years.map(y => [y, y]));
    }

    function syncSelects(date) {
        const parts = String(date || '').split('-');
        if (parts.length !== 3) return;
        yearSelect.value = parts[0];
        monthSelect.value = parts[1];
        daySelect.value = parts[2];
    }

    function showDate(date) {
        document.getElementById('recapDate').textContent = fmtDate(date);
        const day = weekday(date);
        document.getElementById('recapEyebrow').textContent = (isDay ? 'Countdown' : 'Chart week') + (day ? ' · ' + day : '');
    }

    async function load(date) {
        if (!date) return;
        root.classList.add('rc-loading');
        try {
            const response = await fetch(config.base + '/recaps/data?date=' + encodeURIComponent(date));
            const data = await response.json();
            const effective = data.date || date;
            showDate(effective);
            syncSelects(effective);
            prevBtn.disabled = !data.prevDate;
            prevBtn.dataset.date = data.prevDate || '';
            nextBtn.disabled = !data.nextDate;
            nextBtn.dataset.date = data.nextDate || '';
            window.history.replaceState({}, '', recapUrl(effective));
            render(data);
        } catch (error) {
            content.innerHTML = '<div class="co-panel wk-empty"><h2>Failed to load this chart</h2><p>Try another date.</p></div>';
        } finally {
            root.classList.remove('rc-loading');
        }
    }

    function openPlaylist() {
        const songs = (current ? current.entries : []).filter(e => e.songId).map(e => ({
            id: Number(e.songId), name: e.songTitle || '', artist: e.artistName || '', album: ''
        }));
        if (!songs.length) {
            showWarningToast('No linked library songs on this chart');
            return;
        }
        openPlaylistGeneratorModal(songs, (config.key === 'hot100' ? 'Billboard Hot 100' : config.label) + ' Recap - ' + fmtDate(current.date));
    }

    // ------------------------------------------------------------------ wiring

    fillSelects();
    [daySelect, monthSelect, yearSelect].forEach(select => select.addEventListener('change', () => {
        if (daySelect.value && monthSelect.value && yearSelect.value) {
            load(yearSelect.value + '-' + monthSelect.value + '-' + daySelect.value);
        }
    }));
    prevBtn.addEventListener('click', () => load(prevBtn.dataset.date));
    nextBtn.addEventListener('click', () => load(nextBtn.dataset.date));
    document.getElementById('recapPlaylistBtn')?.addEventListener('click', openPlaylist);
    content.addEventListener('click', event => {
        const run = event.target.closest('[data-rc-run]');
        if (run) { toggleRun(run); return; }
        const header = event.target.closest('th[data-sort]');
        if (header) sortTable(header);
    });

    if (config.selectedDate) {
        load(config.selectedDate);
    } else {
        content.innerHTML = '<div class="co-panel wk-empty"><h2>No charts yet</h2><p>Nothing has been imported for this chart.</p></div>';
    }
})();
