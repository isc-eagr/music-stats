/* Detail pages Chart History tab (fragments/detail-chart-history.html):
   chart tiles switch sections, Songs / Albums switches, run bars loaded lazily from each
   card's run endpoint (expand a card for the numbered run), and the Seasonal + Yearly calendar. */
(function () {
    'use strict';

    var MAX_PARALLEL = 4;
    var runCache = new Map();
    var queue = [];
    var running = 0;
    var observer = null;

    function init() {
        document.querySelectorAll('.chh').forEach(function (root) {
            bindTiles(root);
            bindSwitches(root);
            bindCalendar(root);
            restoreState(root);
            bindRuns(root);
        });
    }

    // State kept in the URL (detail-state.js) so a reload or an Include toggle comes back here:
    // chart = tile key, chartView = songs / albums, chartPeriod = calendar cell id
    function record(name, value, defaultValue) {
        if (window.DetailState) {
            window.DetailState.setParam(name, value, defaultValue);
        }
    }

    function restoreState(root) {
        if (!window.DetailState) {
            return;
        }
        var chart = window.DetailState.getParam('chart');
        var tile = chart ? root.querySelector('.chh-tile[data-chh-key="' + cssEscape(chart) + '"]') : null;
        if (tile) {
            activateTile(root, tile);
        }
        var period = window.DetailState.getParam('chartPeriod');
        var cell = period ? root.querySelector('[data-chh-cell="' + cssEscape(period) + '"]') : null;
        var view = window.DetailState.getParam('chartView');
        var section = visibleSection(root);
        if (cell && section && section.contains(cell)) {
            view = cell.closest('[data-chh-view]').dataset.chhView;
        }
        if (view && section) {
            var btn = section.querySelector('[data-chh-view-btn="' + cssEscape(view) + '"]');
            if (btn) {
                activateView(section, btn);
            }
        }
        if (cell) {
            selectCell(cell.closest('.chh-cal'), cell);
        }
    }

    function visibleSection(root) {
        var sections = root.querySelectorAll('[data-chh-section]');
        for (var i = 0; i < sections.length; i++) {
            if (!sections[i].hidden) return sections[i];
        }
        return null;
    }

    function cssEscape(value) {
        return window.CSS && CSS.escape ? CSS.escape(value) : String(value).replace(/["\\]/g, '\\$&');
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // ── Tiles: show the section the tile targets ──
    function bindTiles(root) {
        var tiles = root.querySelectorAll('.chh-tile');
        var firstKey = tiles.length ? tiles[0].dataset.chhKey : null;
        tiles.forEach(function (tile) {
            tile.addEventListener('click', function () {
                activateTile(root, tile);
                record('chart', tile.dataset.chhKey, firstKey);
                record('chartView', null);
            });
        });
    }

    function activateTile(root, tile) {
        root.querySelectorAll('.chh-tile').forEach(function (t) {
            var on = t === tile;
            t.classList.toggle('is-active', on);
            t.setAttribute('aria-pressed', on ? 'true' : 'false');
        });
        var target = tile.dataset.chhTarget;
        root.querySelectorAll('[data-chh-section]').forEach(function (section) {
            section.hidden = section.dataset.chhSection !== target;
        });
    }

    // ── Songs / Albums switch inside a section ──
    function bindSwitches(root) {
        root.querySelectorAll('.chh-switch').forEach(function (group) {
            var section = group.closest('[data-chh-section]');
            group.querySelectorAll('[data-chh-view-btn]').forEach(function (btn) {
                btn.addEventListener('click', function () {
                    activateView(section, btn);
                    record('chartView', btn.dataset.chhViewBtn);
                });
            });
        });
    }

    function activateView(section, btn) {
        var view = btn.dataset.chhViewBtn;
        section.querySelectorAll('[data-chh-view-btn]').forEach(function (b) {
            var on = b === btn;
            b.classList.toggle('is-active', on);
            b.setAttribute('aria-pressed', on ? 'true' : 'false');
        });
        section.querySelectorAll('[data-chh-view]').forEach(function (panel) {
            panel.hidden = panel.dataset.chhView !== view;
        });
        var sub = section.querySelector('.chh-title-sub');
        var text = sub && sub.getAttribute('data-chh-sub-' + view);
        if (text) {
            sub.textContent = text;
        }
    }

    // ── Calendar: a cell shows its period's entries ──
    function bindCalendar(root) {
        root.querySelectorAll('.chh-cal').forEach(function (grid) {
            grid.querySelectorAll('[data-chh-cell]').forEach(function (cell) {
                cell.addEventListener('click', function () {
                    selectCell(grid, cell);
                    record('chartPeriod', cell.dataset.chhCell);
                });
            });
        });
    }

    function selectCell(grid, cell) {
        var id = cell.dataset.chhCell;
        grid.querySelectorAll('[data-chh-cell]').forEach(function (c) {
            var on = c === cell;
            c.classList.toggle('is-selected', on);
            c.setAttribute('aria-pressed', on ? 'true' : 'false');
        });
        grid.querySelectorAll('[data-chh-period]').forEach(function (panel) {
            panel.hidden = panel.dataset.chhPeriod !== id;
        });
    }

    // ── Run bars ──
    function bindRuns(root) {
        var cards = root.querySelectorAll('.chh-hit[data-run-url]');
        if (!cards.length) {
            return;
        }
        if ('IntersectionObserver' in window) {
            if (!observer) {
                observer = new IntersectionObserver(function (entries) {
                    entries.forEach(function (entry) {
                        if (entry.isIntersecting) {
                            observer.unobserve(entry.target);
                            loadBars(entry.target);
                        }
                    });
                }, { rootMargin: '200px 0px' });
            }
            cards.forEach(function (card) { observer.observe(card); });
        } else {
            cards.forEach(loadBars);
        }
        cards.forEach(function (card) {
            var button = card.querySelector('.chh-run');
            if (button) {
                button.addEventListener('click', function () { openRunModal(root, card); });
            }
        });
    }

    function loadBars(card) {
        if (card.dataset.runState) {
            return;
        }
        card.dataset.runState = 'loading';
        var bars = card.querySelector('.chh-run-bars');
        if (bars) {
            bars.classList.add('is-loading');
        }
        fetchRun(card.dataset.runUrl).then(function (data) {
            card.dataset.runState = 'loaded';
            if (bars) {
                bars.classList.remove('is-loading');
                renderBars(bars, segments(trimRun(data)), scaleOf(card, data), card.dataset.unit, false);
            }
        }).catch(function () {
            card.dataset.runState = 'error';
            if (bars) {
                bars.classList.remove('is-loading');
            }
        });
    }

    function fetchRun(url) {
        if (runCache.has(url)) {
            return runCache.get(url);
        }
        var promise = new Promise(function (resolve, reject) {
            queue.push({ url: url, resolve: resolve, reject: reject });
            pump();
        });
        runCache.set(url, promise);
        promise.catch(function () { runCache.delete(url); });
        return promise;
    }

    function pump() {
        while (running < MAX_PARALLEL && queue.length) {
            var job = queue.shift();
            running++;
            fetch(job.url)
                .then(function (r) {
                    if (!r.ok) {
                        throw new Error('HTTP ' + r.status);
                    }
                    return r.json();
                })
                .then(job.resolve, job.reject)
                .finally(function () {
                    running--;
                    pump();
                });
        }
    }

    /* Weeks from the first to the last charted period (gaps kept, so a re-entry shows as a dip). */
    function trimRun(data) {
        var weeks = (data && data.weeks) || [];
        var start = 0;
        var end = weeks.length - 1;
        while (start <= end && !weeks[start].onChart) start++;
        while (end >= start && !weeks[end].onChart) end--;
        return start > end ? [] : weeks.slice(start, end + 1);
    }

    function scaleOf(card, data) {
        var scale = parseInt(card.dataset.scale, 10) || 20;
        ((data && data.weeks) || []).forEach(function (w) {
            if (w.onChart && w.position > scale) {
                scale = w.position;
            }
        });
        return scale;
    }

    function band(position) {
        if (position <= 1) return 'one';
        if (position <= 5) return 'top5';
        if (position <= 10) return 'top10';
        return 'rest';
    }

    function weekLabel(w) {
        return w.dateRange || w.periodKey || '';
    }

    // ── Gaps: an absence of GAP_MIN or more periods collapses into one break marker ──
    var GAP_MIN = 3;

    function charted(w) {
        return w.onChart && w.position != null;
    }

    /* [{week}] for a charted period, [{off}] for a short absence, [{gap: [periods]}] for a long one. */
    function segments(weeks) {
        var out = [];
        var i = 0;
        while (i < weeks.length) {
            if (charted(weeks[i])) {
                out.push({ week: weeks[i] });
                i++;
                continue;
            }
            var start = i;
            while (i < weeks.length && !charted(weeks[i])) i++;
            var off = weeks.slice(start, i);
            if (off.length >= GAP_MIN) {
                out.push({ gap: off });
            } else {
                off.forEach(function (w) { out.push({ off: w }); });
            }
        }
        return out;
    }

    function plural(n, one, many) {
        return n + ' ' + (n === 1 ? one : many);
    }

    function gapLength(n, unit) {
        return unit === 'days' ? plural(n, 'day', 'days') : plural(n, 'week', 'weeks');
    }

    /* Compact gap label in the chart's own unit, never converted: 80w on weekly charts, 45d on daily ones. */
    function gapShort(n, unit) {
        return n + (unit === 'days' ? 'd' : 'w');
    }

    function gapTitle(off, unit) {
        return 'Off the chart for ' + gapLength(off.length, unit) + ' (' + weekLabel(off[0]) + ' to ' + weekLabel(off[off.length - 1]) + ')';
    }

    function barHeight(position, scale) {
        return Math.max(8, Math.round((scale + 1 - position) / scale * 100)) + '%';
    }

    function renderBars(container, segs, scale, unit, large) {
        container.textContent = '';
        var chartedCount = segs.filter(function (s) { return s.week; }).length;
        container.classList.toggle('is-dense', !large && chartedCount > 70);
        segs.forEach(function (s) {
            var el = document.createElement('span');
            if (s.week) {
                el.className = 'chh-bar chh-band-' + band(s.week.position);
                el.style.height = barHeight(s.week.position, scale);
                el.title = weekLabel(s.week) + ': #' + s.week.position;
            } else if (s.off) {
                el.className = 'chh-bar is-off';
                el.title = weekLabel(s.off) + ': off the chart';
            } else {
                el.className = 'chh-gap';
                el.title = gapTitle(s.gap, unit);
                if (large) {
                    var label = document.createElement('span');
                    label.className = 'chh-gap-label';
                    label.textContent = gapShort(s.gap.length, unit);
                    el.appendChild(label);
                }
            }
            container.appendChild(el);
        });
    }

    /* Numbered boxes linking to each chart; absences become one box (Nx, or 34w / 1.5y when long). */
    function renderBoxes(container, segs, periodHref, unit) {
        container.textContent = '';
        var i = 0;
        while (i < segs.length) {
            var s = segs[i];
            if (s.week) {
                var box;
                if (periodHref && s.week.periodKey) {
                    box = document.createElement('a');
                    box.href = periodHref.replace('{key}', encodeURIComponent(s.week.periodKey));
                } else {
                    box = document.createElement('span');
                }
                box.className = 'chh-box chh-band-' + band(s.week.position);
                box.textContent = s.week.position;
                box.title = weekLabel(s.week) + ': #' + s.week.position;
                container.appendChild(box);
                i++;
                continue;
            }
            var off = [];
            while (i < segs.length && !segs[i].week) {
                off = off.concat(segs[i].gap || [segs[i].off]);
                i++;
            }
            var gap = document.createElement('span');
            gap.className = 'chh-box is-off';
            gap.textContent = off.length >= GAP_MIN ? gapShort(off.length, unit) : off.length + 'x';
            gap.title = gapTitle(off, unit);
            container.appendChild(gap);
        }
    }

    // ── Run modal ──

    function el(tag, className, text) {
        var node = document.createElement(tag);
        if (className) node.className = className;
        if (text !== undefined && text !== null) node.textContent = text;
        return node;
    }

    function ensureDialog(root) {
        if (root.chhDialog) {
            return root.chhDialog;
        }
        var dialog = el('dialog', 'chh-modal');
        dialog.setAttribute('aria-labelledby', 'chhModalTitle');
        var inner = el('div', 'chh-modal-inner');
        var close = el('button', 'chh-modal-close');
        close.type = 'button';
        close.setAttribute('aria-label', 'Close');
        close.innerHTML = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"/></svg>';
        close.addEventListener('click', function () { dialog.close(); });
        inner.appendChild(close);
        inner.appendChild(el('div', 'chh-modal-head'));
        inner.appendChild(el('div', 'chh-modal-body'));
        dialog.appendChild(inner);
        // A click on the backdrop lands on the dialog itself
        dialog.addEventListener('click', function (event) {
            if (event.target === dialog) dialog.close();
        });
        root.appendChild(dialog);
        root.chhDialog = dialog;
        return dialog;
    }

    function openRunModal(root, card) {
        var dialog = ensureDialog(root);
        var head = dialog.querySelector('.chh-modal-head');
        var body = dialog.querySelector('.chh-modal-body');
        head.textContent = '';
        var cardHead = card.querySelector('.chh-hit-head').cloneNode(true);
        var name = cardHead.querySelector('.chh-hit-name');
        if (name) name.id = 'chhModalTitle';
        head.appendChild(cardHead);
        body.textContent = '';
        body.appendChild(el('p', 'chh-run-note', 'Loading run...'));
        if (!dialog.open) dialog.showModal();

        var url = card.dataset.runUrl;
        dialog.dataset.runUrl = url;
        fetchRun(url).then(function (data) {
            if (dialog.dataset.runUrl === url) {
                renderModalBody(body, card, trimRun(data), scaleOf(card, data));
            }
        }).catch(function () {
            if (dialog.dataset.runUrl === url) {
                body.textContent = '';
                body.appendChild(el('p', 'chh-run-note', 'Could not load the run.'));
            }
        });
    }

    function renderModalBody(body, card, weeks, scale) {
        var unit = card.dataset.unit === 'days' ? 'days' : 'weeks';
        var unitOne = unit === 'days' ? 'day' : 'week';
        body.textContent = '';
        var chartedWeeks = weeks.filter(charted);
        if (!chartedWeeks.length) {
            body.appendChild(el('p', 'chh-run-note', 'No chart entries.'));
            return;
        }

        // Stats from the run itself
        var peak = Math.min.apply(null, chartedWeeks.map(function (w) { return w.position; }));
        var count = function (max) { return chartedWeeks.filter(function (w) { return w.position <= max; }).length; };
        var runs = 0, longest = 0, current = 0;
        weeks.forEach(function (w) {
            if (charted(w)) {
                if (current === 0) runs++;
                current++;
                longest = Math.max(longest, current);
            } else {
                current = 0;
            }
        });
        var stats = el('div', 'chh-modal-stats');
        [
            ['Peak', '#' + peak, peak === 1],
            [unit + ' on chart', chartedWeeks.length],
            [unit + ' at peak', chartedWeeks.filter(function (w) { return w.position === peak; }).length],
            [unit + ' at #1', count(1), count(1) > 0],
            ['Top 5', count(5)],
            ['Top 10', count(10)],
            ['Chart runs', runs],
            ['Longest run', plural(longest, unitOne, unit)]
        ].forEach(function (s) {
            var stat = el('div', 'chh-stat');
            stat.appendChild(el('span', 'chh-stat-label', s[0]));
            stat.appendChild(el('span', 'chh-stat-value chh-num' + (s[2] ? ' is-one' : ''), String(s[1])));
            stats.appendChild(stat);
        });
        body.appendChild(stats);

        // Large run chart
        var chartHead = el('div', 'chh-modal-chart-head');
        chartHead.appendChild(el('span', 'chh-eyebrow', 'Chart run'));
        chartHead.appendChild(el('span', 'chh-modal-range', weekLabel(weeks[0]) + '  to  ' + weekLabel(weeks[weeks.length - 1])));
        body.appendChild(chartHead);

        var chart = el('div', 'chh-modal-chart');
        var mid = Math.max(2, Math.round(scale / 2));
        [[1, 'top'], [mid, 'mid'], [scale, 'bottom']].forEach(function (a) {
            chart.appendChild(el('span', 'chh-axis chh-axis-' + a[1], '#' + a[0]));
            chart.appendChild(el('span', 'chh-guide chh-guide-' + a[1]));
        });
        var bars = el('div', 'chh-run-bars is-large');
        renderBars(bars, segments(weeks), scale, card.dataset.unit, true);
        chart.appendChild(bars);
        body.appendChild(chart);

        var legend = el('div', 'chh-legend');
        [['one', '#1'], ['top5', '#2 to #5'], ['top10', '#6 to #10'], ['rest', '#11 and below']].forEach(function (l) {
            var item = el('span');
            item.appendChild(el('i', 'chh-band-' + l[0]));
            item.appendChild(document.createTextNode(l[1]));
            legend.appendChild(item);
        });
        var gapItem = el('span');
        gapItem.appendChild(el('i', 'chh-legend-gap'));
        gapItem.appendChild(document.createTextNode('Off the chart ' + GAP_MIN + '+ ' + unit));
        legend.appendChild(gapItem);
        body.appendChild(legend);

        // Numbered run, each box links to its chart
        var boxesHead = el('div', 'chh-modal-chart-head');
        boxesHead.appendChild(el('span', 'chh-eyebrow', unit === 'days' ? 'Day by day' : 'Week by week'));
        body.appendChild(boxesHead);
        var boxes = el('div', 'chh-run-boxes');
        renderBoxes(boxes, segments(weeks), card.dataset.periodHref, card.dataset.unit);
        body.appendChild(boxes);

        // Debut / first peak links from the card
        var dates = card.querySelector('.chh-hit-dates');
        if (dates && dates.children.length) {
            var foot = el('div', 'chh-modal-foot');
            Array.prototype.forEach.call(dates.children, function (d) { foot.appendChild(d.cloneNode(true)); });
            body.appendChild(foot);
        }
    }
})();
