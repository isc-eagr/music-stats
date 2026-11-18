/* Plays tab (artist, album and song detail pages): the over-time chart with its three modes,
   the When you listen panels with their all time / year scope, rank moves and the Full history toggle.
   Data comes from window.detailPlaysData (DetailPlaysService.build). */
(function () {
    'use strict';

    const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
    const WEEKDAY_NAMES = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
    /* Day of week bands: name, first day index, day count */
    const WEEK_BANDS = [['Weekdays', 0, 5], ['Weekend', 5, 2]];
    /* Month of year season bands (meteorological, northern hemisphere): name, icon paths, month indexes */
    const SNOW = '<path d="M12 3v18M4.2 7.5l15.6 9M4.2 16.5l15.6-9M9.5 4.5L12 6.5l2.5-2M9.5 19.5l2.5-2 2.5 2"/>';
    const SEASONS = [
        ['Winter', SNOW, [0, 1]],
        ['Spring', '<path d="M12 21v-9M12 12c0-4 3-7 7-7 0 4-3 7-7 7zM12 15c0-3-2.5-5.5-6-5.5 0 3 2.5 5.5 6 5.5z"/>', [2, 3, 4]],
        ['Summer', '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/>', [5, 6, 7]],
        ['Fall', '<path d="M5 19c0-8 6-14 14-14 0 8-6 14-14 14zM5 19l8-8"/>', [8, 9, 10]],
        ['Winter', SNOW, [11]]
    ];
    const SEASON_SPAN = { Winter: [11, 1], Spring: [2, 4], Summer: [5, 7], Fall: [8, 10] };
    const GENDER_DOT = { male: '#60a5fa', female: '#f472b6', other: '#a1a1aa' };
    const MOVES_SHOWN = 12;
    const SVG = 'http://www.w3.org/2000/svg';

    const fmt = (n) => Number(n).toLocaleString('en-US');
    const pad = (n) => String(n).padStart(2, '0');
    const display = (iso) => iso.substring(8, 10) + '/' + iso.substring(5, 7) + '/' + iso.substring(0, 4);
    const weekday = (iso) => (new Date(iso + 'T12:00:00').getDay() + 6) % 7;

    function el(tag, cls, text) {
        const node = document.createElement(tag);
        if (cls) node.className = cls;
        if (text != null) node.textContent = text;
        return node;
    }

    function svg(tag, attrs) {
        const node = document.createElementNS(SVG, tag);
        Object.entries(attrs || {}).forEach(([k, v]) => node.setAttribute(k, v));
        return node;
    }

    /* Compact year picker: older / newer arrows around a dropdown, so it stays one line however many
       years there are. values run newest first; 'all' shows as All time. */
    function yearPicker(box, values, current, label, onPick) {
        const at = values.indexOf(current);
        const arrow = (dir, older) => {
            const b = el('button', 'dp-yp-step');
            b.type = 'button';
            b.setAttribute('aria-label', older ? 'Older' : 'Newer');
            b.innerHTML = '<svg class="ad-ico" viewBox="0 0 24 24" aria-hidden="true"><path d="' + dir + '"/></svg>';
            const target = at + (older ? 1 : -1);
            b.disabled = target < 0 || target >= values.length;
            b.addEventListener('click', () => onPick(values[target]));
            return b;
        };
        const select = el('select', 'dp-yp-select');
        select.setAttribute('aria-label', label);
        values.forEach((v) => {
            const o = el('option', null, v === 'all' ? 'All time' : String(v));
            o.value = String(v);
            o.selected = v === current;
            select.appendChild(o);
        });
        select.addEventListener('change', () => {
            const v = select.value === 'all' ? 'all' : Number(select.value);
            onPick(v);
        });
        const wrap = el('span', 'dp-yp');
        wrap.append(arrow('M15 6l-6 6 6 6', true), select, arrow('M9 6l6 6-6 6', false));
        box.replaceChildren(wrap);
    }

    function init() {
        const root = document.querySelector('.dp');
        const data = window.detailPlaysData;
        if (!root || !data) return;

        const days = data.days || [];
        const dayMap = new Map(days.map(([d, n]) => [d, n]));
        const releases = data.releases || [];
        const releaseDays = new Map(releases.map((r) => [r.day, r.name]));
        const milestones = data.milestones || [];

        // Months from the first play to the last, empty months included
        const months = [];
        if (days.length) {
            const first = days[0][0], last = days[days.length - 1][0];
            let y = +first.substring(0, 4), m = +first.substring(5, 7);
            const ly = +last.substring(0, 4), lm = +last.substring(5, 7);
            const totals = new Map();
            days.forEach(([d, n]) => {
                const key = d.substring(0, 7);
                totals.set(key, (totals.get(key) || 0) + n);
            });
            while (y < ly || (y === ly && m <= lm)) {
                const key = y + '-' + pad(m);
                months.push({ key, y, m, v: totals.get(key) || 0 });
                m++;
                if (m > 12) { m = 1; y++; }
            }
        }
        let running = 0;
        months.forEach((x) => { running += x.v; x.cum = running; });
        const monthIndex = new Map(months.map((x, i) => [x.key, i]));
        const years = [...new Set(months.map((x) => x.y))].sort((a, b) => b - a);

        const state = { mode: 'monthly', calYear: years[0], scope: 'all' };

        /* ---------- Plays over time ---------- */
        const chartCard = root.querySelector('[data-dp-chart]');
        const plot = root.querySelector('[data-dp-plot]');
        const axis = root.querySelector('[data-dp-axis]');
        const lineBox = root.querySelector('[data-dp-line]');
        const calBox = root.querySelector('[data-dp-cal]');
        const headCount = root.querySelector('[data-dp-head-count]');

        chartCard.querySelectorAll('[data-dp-mode]').forEach((btn) => {
            btn.addEventListener('click', () => {
                state.mode = btn.dataset.dpMode;
                chartCard.querySelectorAll('[data-dp-mode]').forEach((b) => {
                    b.classList.toggle('is-active', b === btn);
                    b.setAttribute('aria-pressed', String(b === btn));
                });
                renderChart();
            });
        });

        function renderChart() {
            if (!plot) return;
            const isCal = state.mode === 'calendar';
            lineBox.hidden = isCal;
            calBox.hidden = !isCal;
            const total = days.reduce((a, [, n]) => a + n, 0);
            if (isCal) {
                const yearTotal = days.filter(([d]) => d.startsWith(String(state.calYear))).reduce((a, [, n]) => a + n, 0);
                headCount.textContent = fmt(yearTotal) + ' in ' + state.calYear;
                renderCalendar();
            } else {
                headCount.textContent = fmt(total);
                renderLine();
            }
        }

        function renderLine() {
            plot.replaceChildren();
            axis.replaceChildren();
            const W = plot.clientWidth;
            if (!W || !months.length) return;
            const H = plot.clientHeight || 220;
            const isM = state.mode === 'monthly';
            root.querySelector('[data-dp-line-label]').textContent = isM ? 'Plays per month' : 'Running total';
            root.querySelector('[data-dp-legend-peak]').hidden = !isM;
            root.querySelector('[data-dp-legend-release]').hidden = !releases.some((r) => monthIndex.has(r.day.substring(0, 7)));
            root.querySelector('[data-dp-legend-milestone]').hidden = !milestones.length;

            const vals = months.map((x) => (isM ? x.v : x.cum));
            const top = Math.max(1, ...vals);
            const n = months.length;
            const xOf = (i) => (n === 1 ? W / 2 : (i / (n - 1)) * W);
            const yOf = (v) => H - (v / top) * (H - 12);

            [top, Math.round(top / 2)].forEach((v) => {
                const g = el('div', 'dp-gl');
                g.style.top = yOf(v) + 'px';
                g.appendChild(el('span', null, fmt(v)));
                plot.appendChild(g);
            });

            const s = svg('svg', { width: W, height: H, viewBox: '0 0 ' + W + ' ' + H, 'aria-hidden': 'true' });
            const pts = vals.map((v, i) => xOf(i).toFixed(1) + ',' + yOf(v).toFixed(1)).join(' ');
            s.appendChild(svg('polygon', { points: xOf(0).toFixed(1) + ',' + H + ' ' + pts + ' ' + xOf(n - 1).toFixed(1) + ',' + H, class: 'dp-area' }));
            s.appendChild(svg('polyline', { points: pts, class: 'dp-line' }));
            const guide = svg('line', { x1: 0, x2: 0, y1: 0, y2: H, class: 'dp-guide' });
            guide.style.display = 'none';
            s.appendChild(guide);
            plot.appendChild(s);

            // Album / song releases: dashed line, label in one of two lanes above the plot
            const lanes = [-Infinity, -Infinity];
            releases.forEach((r) => {
                const i = monthIndex.get(r.day.substring(0, 7));
                if (i === undefined) return;
                const x = xOf(i);
                const lane = x - lanes[0] > 110 ? 0 : (x - lanes[1] > 110 ? 1 : -1);
                const pin = el('div', 'dp-pin');
                pin.style.left = x + 'px';
                pin.title = r.name + ' · ' + display(r.day);
                if (lane >= 0) {
                    lanes[lane] = x;
                    pin.style.top = (lane ? -26 : -4) + 'px';
                    pin.appendChild(el('span', null, r.name));
                } else {
                    pin.style.top = '-4px';
                }
                plot.appendChild(pin);
            });

            // Milestones sit on the line at the month they were reached; labels skip when crowded
            let lastLabel = -Infinity;
            milestones.forEach((ms) => {
                const i = monthIndex.get((ms.day || '').substring(0, 7));
                if (i === undefined) return;
                const x = xOf(i), y = yOf(vals[i]);
                const dot = el('span', 'dp-dot');
                dot.style.left = x + 'px';
                dot.style.top = y + 'px';
                dot.title = fmt(ms.n) + ' plays · ' + display(ms.day) + (ms.song ? ' · ' + ms.song : '');
                if (x - lastLabel > 34) {
                    dot.appendChild(el('span', null, ms.n >= 1000 ? (ms.n / 1000) + 'K' : String(ms.n)));
                    lastLabel = x;
                }
                plot.appendChild(dot);
            });

            if (isM) {
                const pi = vals.indexOf(top);
                const peak = el('span', 'dp-peak');
                peak.style.left = xOf(pi) + 'px';
                peak.style.top = yOf(top) + 'px';
                const lbl = el('span', null, MONTHS[months[pi].m - 1] + ' ' + months[pi].y + ' · ' + fmt(top));
                if (xOf(pi) > W - 160) lbl.classList.add('is-left');
                peak.appendChild(lbl);
                plot.appendChild(peak);
            }

            // Hover: guide line plus the month's plays, running total and anything that happened that month
            const tip = el('div', 'dp-tip');
            tip.hidden = true;
            plot.appendChild(tip);
            plot.onmousemove = (e) => {
                const rect = plot.getBoundingClientRect();
                const i = Math.max(0, Math.min(n - 1, Math.round(((e.clientX - rect.left) / W) * (n - 1))));
                const x = xOf(i);
                const mo = months[i];
                guide.setAttribute('x1', x);
                guide.setAttribute('x2', x);
                guide.style.display = '';
                const extra = [];
                releases.filter((r) => r.day.startsWith(mo.key)).forEach((r) => extra.push('Release: ' + r.name));
                milestones.filter((m) => (m.day || '').startsWith(mo.key)).forEach((m) => extra.push(fmt(m.n) + 'th play on ' + display(m.day)));
                tip.replaceChildren(el('b', null, MONTHS[mo.m - 1] + ' ' + mo.y),
                    el('span', null, fmt(mo.v) + ' plays · ' + fmt(mo.cum) + ' total'));
                extra.forEach((t) => tip.appendChild(el('small', null, t)));
                tip.hidden = false;
                tip.style.left = Math.min(Math.max(x, 80), W - 80) + 'px';
            };
            plot.onmouseleave = () => { tip.hidden = true; guide.style.display = 'none'; };

            // Year labels at each January (or the first month)
            let lastX = -Infinity;
            months.forEach((mo, i) => {
                if (i !== 0 && mo.m !== 1) return;
                const x = xOf(i);
                if (x - lastX < 34) return;
                lastX = x;
                const lab = el('span', null, String(mo.y));
                lab.style.left = x + 'px';
                axis.appendChild(lab);
            });
        }

        function renderCalendar() {
            yearPicker(root.querySelector('[data-dp-cal-years]'), years, state.calYear, 'Calendar year',
                (y) => { state.calYear = y; renderChart(); });
            const grid = root.querySelector('[data-dp-cal-grid]');
            grid.replaceChildren();
            const year = state.calYear;
            const firstDay = days.length ? days[0][0] : null;
            const lastDay = days.length ? days[days.length - 1][0] : null;
            const jan1 = new Date(year, 0, 1);
            const offset = (jan1.getDay() + 6) % 7;
            let max = 0;
            for (const [d, n] of days) if (d.startsWith(year + '-')) max = Math.max(max, n);
            const labels = el('div', 'dp-cal-months');
            const cells = el('div', 'dp-cal-cells');
            WEEKDAYS.forEach((w, i) => {
                if (i % 2 === 0) {
                    const lab = el('span', 'dp-cal-wd', w);
                    lab.style.gridRow = String(i + 1);
                    lab.style.gridColumn = '1';
                    cells.appendChild(lab);
                }
            });
            let best = 0, bestDay = null, active = 0, span = 0, run = 0, streak = 0, gapRun = 0, gap = 0;
            for (let d = new Date(year, 0, 1), i = 0; d.getFullYear() === year; d.setDate(d.getDate() + 1), i++) {
                const iso = d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate());
                const col = Math.floor((offset + i) / 7) + 2;
                if (d.getDate() === 1) {
                    const ml = el('span', null, MONTHS[d.getMonth()]);
                    ml.style.gridColumn = String(col);
                    labels.appendChild(ml);
                }
                const v = dayMap.get(iso) || 0;
                const inSpan = firstDay && iso >= firstDay && iso <= lastDay;
                const cell = el('i', inSpan ? 'lv' + (v === 0 ? 0 : Math.ceil((4 * v) / max)) : 'lv-out');
                if (releaseDays.has(iso)) cell.classList.add('is-rel');
                cell.style.gridColumn = String(col);
                cell.style.gridRow = String(((offset + i) % 7) + 1);
                if (inSpan) {
                    cell.title = display(iso) + ': ' + fmt(v) + (v === 1 ? ' play' : ' plays') + (releaseDays.has(iso) ? ' · ' + releaseDays.get(iso) + ' release' : '');
                    span++;
                    if (v) { active++; run++; gapRun = 0; streak = Math.max(streak, run); } else { run = 0; gapRun++; gap = Math.max(gap, gapRun); }
                    if (v > best) { best = v; bestDay = iso; }
                }
                cells.appendChild(cell);
            }
            grid.append(labels, cells);
            const facts = root.querySelector('[data-dp-cal-facts]');
            const fact = (label, value, gold) => {
                const f = el('span', 'ad-fact' + (gold ? ' is-gold' : ''), label + ' ');
                f.appendChild(el('b', 'ad-num', value));
                return f;
            };
            facts.replaceChildren(
                fact('Best day', bestDay ? display(bestDay) + ' · ' + fmt(best) : '-', true),
                fact('Active days', fmt(active) + ' of ' + fmt(span)),
                fact('Longest streak', streak + (streak === 1 ? ' day' : ' days')),
                fact('Longest gap', gap + (gap === 1 ? ' day' : ' days')));
        }

        /* ---------- When you listen ---------- */
        const rhythm = root.querySelector('[data-dp-rhythm]');
        const excluded = data.excludedDay;

        function renderRhythm() {
            if (!rhythm) return;
            yearPicker(rhythm.querySelector('[data-dp-rh-scopes]'), ['all', ...years], state.scope, 'Listening period',
                (v) => { state.scope = v; renderRhythm(); });
            const inScope = (d) => state.scope === 'all' || d.startsWith(state.scope + '-');
            const scoped = days.filter(([d]) => inScope(d) && d !== excluded);
            const total = scoped.reduce((a, [, n]) => a + n, 0);
            const excludedPlays = days.filter(([d]) => d === excluded && inScope(d)).reduce((a, [, n]) => a + n, 0);
            rhythm.querySelector('[data-dp-rh-summary]').textContent =
                (state.scope === 'all' ? 'All time' : state.scope) + ' · ' + fmt(total) + ' plays · ' + fmt(scoped.filter(([, n]) => n > 0).length) + ' active days';
            const note = rhythm.querySelector('[data-dp-excluded]');
            note.hidden = excludedPlays === 0;
            note.textContent = 'Leaves out ' + fmt(excludedPlays) + ' plays from ' + display(excluded) + ', the initial Last.fm load without real listen dates.';

            // Hour of day
            const hours = new Array(24).fill(0);
            Object.entries(data.hours || {}).forEach(([y, arr]) => {
                if (state.scope === 'all' || String(state.scope) === y) arr.forEach((v, h) => { hours[h] += v; });
            });
            const hSum = hours.reduce((a, b) => a + b, 0);
            const hMax = Math.max(...hours);
            const hoursBox = rhythm.querySelector('[data-dp-hours]');
            hoursBox.replaceChildren();
            const bands = [['Night', 0, 6], ['Morning', 6, 12], ['Afternoon', 12, 18], ['Evening', 18, 24]];
            bands.forEach(([name, a, b], i) => {
                const share = hSum ? Math.round((hours.slice(a, b).reduce((x, y) => x + y, 0) / hSum) * 100) : 0;
                const band = el('span', 'dp-band');
                band.style.left = (i * 25) + '%';
                const lab = el('span', null, name + ' ');
                lab.appendChild(el('b', 'ad-num', share + '%'));
                band.appendChild(lab);
                hoursBox.appendChild(band);
            });
            const peakHour = hours.indexOf(hMax);
            hours.forEach((v, h) => {
                const bar = el('i', h === peakHour && hMax > 0 ? 'is-best' : '');
                bar.style.height = (hMax ? Math.max(v ? 2 : 0, Math.round((v / hMax) * 110)) : 0) + 'px';
                bar.title = pad(h) + ':00 · ' + fmt(v) + ' plays';
                hoursBox.appendChild(bar);
            });
            rhythm.querySelector('[data-dp-hours-note]').textContent = hSum
                ? 'Peak hour ' + pad(peakHour) + ':00 · ' + ((hMax / hSum) * 100).toFixed(1) + '% of plays'
                : 'No listen times recorded';

            // Day of week: Weekdays and Weekend bands over one bar per day (same layout as the catalog detail page)
            const week = new Array(7).fill(0);
            scoped.forEach(([d, n]) => { week[weekday(d)] += n; });
            const wMax = Math.max(...week);
            const wSum = week.reduce((a, b) => a + b, 0);
            const peakDay = week.indexOf(wMax);
            rhythm.querySelector('[data-dp-week]').replaceChildren(...WEEK_BANDS.map(([name, first, count]) => {
                const share = wSum ? Math.round((week.slice(first, first + count).reduce((a, b) => a + b, 0) / wSum) * 100) : 0;
                const band = el('span', 'dp-band dp-sband');
                band.style.left = 'calc((100% - 36px) * ' + first + ' / 7 + ' + (first * 6) + 'px)';
                band.style.width = 'calc((100% - 36px) * ' + count + ' / 7 + ' + ((count - 1) * 6) + 'px)';
                band.title = name + ' · ' + share + '% of plays';
                const lab = el('span');
                lab.append(el('em', null, name), el('b', 'ad-num', share + '%'));
                band.appendChild(lab);
                return band;
            }), ...week.map((v, i) => {
                const col = el('div');
                col.appendChild(el('b', 'ad-num', v ? fmt(v) : ''));
                const bar = el('i', v > 0 && i === peakDay ? 'is-best' : '');
                bar.style.height = (wMax && v ? Math.max(2, Math.round((v / wMax) * 92)) : 0) + 'px';
                bar.title = WEEKDAY_NAMES[i] + ': ' + fmt(v) + ' plays';
                col.append(bar, el('span', null, WEEKDAYS[i]));
                return col;
            }));
            rhythm.querySelector('[data-dp-week-note]').textContent = wSum
                ? 'Peak day ' + WEEKDAY_NAMES[peakDay] + ' · ' + ((wMax / wSum) * 100).toFixed(1) + '% of plays'
                : 'No plays yet';

            // Months: added over all years, or that year's months
            const mo = new Array(12).fill(0);
            scoped.forEach(([d, n]) => { mo[+d.substring(5, 7) - 1] += n; });
            const mMax = Math.max(...mo);
            rhythm.querySelector('[data-dp-months-title]').textContent = state.scope === 'all' ? 'Month of year · all years added' : 'Months of ' + state.scope;
            const moBox = rhythm.querySelector('[data-dp-months]');
            const moSum = mo.reduce((a, b) => a + b, 0);
            moBox.replaceChildren(...SEASONS.map(([name, icon, months], i) => {
                // Winter wraps the year: its share sits on Jan-Feb, December gets the icon only
                const share = moSum ? Math.round((SEASONS.filter((s) => s[0] === name)
                    .reduce((a, s) => a + s[2].reduce((x, m) => x + mo[m], 0), 0) / moSum) * 100) : 0;
                const first = months[0];
                const band = el('span', 'dp-band dp-sband');
                band.style.left = 'calc((100% - 66px) * ' + first + ' / 12 + ' + (first * 6) + 'px)';
                band.style.width = 'calc((100% - 66px) * ' + months.length + ' / 12 + ' + ((months.length - 1) * 6) + 'px)';
                band.title = name + ' (' + MONTHS[SEASON_SPAN[name][0]] + ' to ' + MONTHS[SEASON_SPAN[name][1]] + ') · ' + share + '% of plays';
                const lab = el('span');
                lab.innerHTML = '<svg class="ad-ico" viewBox="0 0 24 24" aria-hidden="true">' + icon + '</svg>';
                if (i < SEASONS.length - 1) {
                    lab.appendChild(el('em', null, name));
                    lab.appendChild(el('b', 'ad-num', share + '%'));
                }
                band.appendChild(lab);
                return band;
            }), ...mo.map((v, i) => {
                const col = el('div');
                col.appendChild(el('b', 'ad-num', v ? fmt(v) : ''));
                const bar = el('i', v > 0 && v === mMax ? 'is-best' : '');
                bar.style.height = (mMax && v ? Math.max(2, Math.round((v / mMax) * 92)) : 0) + 'px';
                bar.title = MONTHS[i] + ': ' + fmt(v) + ' plays';
                col.append(bar, el('span', null, MONTHS[i].charAt(0)));
                return col;
            }));

            // Biggest days
            const top = scoped.slice().sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1)).slice(0, 8);
            const topMax = top.length ? top[0][1] : 1;
            rhythm.querySelector('[data-dp-top-days]').replaceChildren(...top.map(([d, n], i) => {
                const row = el('div', 'dp-td');
                row.append(el('span', 'dp-ix ad-num', String(i + 1)), el('span', 'ad-num', display(d)), el('span', 'dp-dim', WEEKDAYS[weekday(d)]));
                const what = el('span', 'dp-td-x');
                if (releaseDays.has(d)) what.appendChild(el('span', 'dp-rel', 'Release · ' + releaseDays.get(d)));
                const ms = milestones.filter((m) => m.day === d);
                if (ms.length) what.appendChild(el('span', 'dp-msc', fmt(ms[ms.length - 1].n) + 'th play'));
                const bx = el('span', 'dp-bx');
                const track = el('span');
                const fill = el('i');
                fill.style.width = Math.round((n / topMax) * 100) + '%';
                track.appendChild(fill);
                bx.append(track, el('b', 'ad-num', fmt(n)));
                row.append(what, bx);
                return row;
            }));
        }

        /* ---------- Rank moves (loaded once the tab is visible) ---------- */
        const movesCard = root.querySelector('[data-dp-moves]');
        let movesLoaded = false;
        function loadMoves() {
            if (movesLoaded || !movesCard) return;
            movesLoaded = true;
            fetch(root.dataset.rankMovesUrl, { headers: { Accept: 'application/json' } })
                .then((r) => (r.ok ? r.json() : Promise.reject(r.status)))
                .then(renderMoves)
                .catch(() => { movesCard.querySelector('[data-dp-moves-summary]').textContent = 'Rank moves could not be loaded'; });
        }

        function renderMoves(res) {
            const rank = movesCard.querySelector('[data-dp-moves-rank]');
            if (res.rank != null) {
                rank.textContent = '#' + fmt(res.rank) + ' overall';
                rank.hidden = false;
                rank.classList.toggle('is-gold', res.rank === 1);
            }
            const summary = movesCard.querySelector('[data-dp-moves-summary]');
            const moves = res.moves || [];
            if (!moves.length) {
                summary.textContent = 'No rank changes yet';
                return;
            }
            summary.replaceChildren(el('b', 'is-up', res.ups + ' up'), document.createTextNode(' · '),
                el('b', 'is-down', res.downs + ' down'),
                document.createTextNode(' in the last ' + moves.length + ' moves · dot = the other ' + (root.dataset.kind === 'artist' ? "artist's" : root.dataset.kind + "'s") + ' gender'));
            const list = movesCard.querySelector('[data-dp-moves-list]');
            const base = '/' + root.dataset.kind + 's/';
            const draw = (count) => list.replaceChildren(...moves.slice(0, count).map((m) => {
                const row = el('div', 'dp-mv');
                const ic = el('span', 'dp-mv-ic ' + (m.up ? 'is-up' : 'is-down'));
                ic.innerHTML = '<svg class="ad-ico" viewBox="0 0 24 24" aria-hidden="true"><path d="' + (m.up ? 'M12 19V5M6 11l6-6 6 6' : 'M12 5v14M6 13l6 6 6-6') + '"/></svg>';
                const name = el('span', 'dp-mv-n');
                const dot = el('i');
                dot.style.background = GENDER_DOT[m.otherGender] || GENDER_DOT.other;
                const link = el('a', null, m.otherName);
                link.href = base + m.otherId;
                name.append(dot, document.createTextNode(m.up ? 'Passed ' : ''), link, document.createTextNode(m.up ? '' : ' moved ahead'));
                row.append(el('span', 'ad-num dp-mv-d', m.date), ic, name,
                    el('span', 'ad-num dp-mv-r ' + (m.up ? 'is-up' : 'is-down'), '#' + fmt(m.from) + ' → #' + fmt(m.to)));
                return row;
            }));
            draw(MOVES_SHOWN);
            const more = movesCard.querySelector('[data-dp-moves-more]');
            if (moves.length > MOVES_SHOWN) {
                more.hidden = false;
                const btn = more.querySelector('button');
                let open = false;
                btn.textContent = 'Show ' + (moves.length - MOVES_SHOWN) + ' older moves';
                btn.addEventListener('click', () => {
                    open = !open;
                    draw(open ? moves.length : MOVES_SHOWN);
                    btn.textContent = open ? 'Show fewer' : 'Show ' + (moves.length - MOVES_SHOWN) + ' older moves';
                });
            }
        }

        /* ---------- Plays by year: open / close a year ---------- */
        root.querySelectorAll('[data-dp-year-toggle]').forEach((btn) => {
            btn.addEventListener('click', () => {
                const row = btn.closest('[data-dp-year]');
                const panel = root.querySelector('[data-dp-panel="' + row.dataset.dpYear + '"]');
                const open = panel.hidden;
                panel.hidden = !open;
                row.classList.toggle('is-open', open);
                btn.setAttribute('aria-expanded', String(open));
            });
        });

        /* ---------- Full history ---------- */
        const historyBtn = root.querySelector('[data-dp-history-toggle]');
        const history = document.getElementById('dpFullHistory');
        if (historyBtn && history) {
            const sync = () => {
                historyBtn.setAttribute('aria-expanded', String(!history.hidden));
                historyBtn.firstChild.textContent = history.hidden ? 'Full history ' : 'Hide full history ';
            };
            sync();
            historyBtn.addEventListener('click', () => {
                history.hidden = !history.hidden;
                sync();
                if (!history.hidden) history.scrollIntoView({ behavior: 'smooth', block: 'start' });
            });
        }

        /* Draw once the tab is visible (it starts hidden on the other tabs) and again on resize */
        let lastWidth = 0;
        const ro = new ResizeObserver(() => {
            const w = plot ? plot.clientWidth : root.clientWidth;
            if (w && w !== lastWidth) {
                lastWidth = w;
                if (state.mode !== 'calendar') renderLine();
            }
            if (root.clientWidth) loadMoves();
        });
        ro.observe(root);
        renderChart();
        renderRhythm();
    }

    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
    else init();
})();
