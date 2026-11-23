/* Shared chart-run drawing (styles in detail-chart-history.css): run bars, the numbered "Chart run"
   boxes and the run modal. Used by the detail Chart History tab, Chart Overview and the Weekly chart.
   A run is a list of periods {onChart, position, periodKey, dateRange}. */
(function () {
    'use strict';

    // An absence of GAP_MIN or more periods collapses into one break marker
    var GAP_MIN = 3;

    function charted(w) {
        return w.onChart && w.position != null;
    }

    /* Periods from the first to the last charted one (gaps kept, so a re-entry shows as a dip). */
    function trim(weeks) {
        weeks = weeks || [];
        var start = 0;
        var end = weeks.length - 1;
        while (start <= end && !charted(weeks[start])) start++;
        while (end >= start && !charted(weeks[end])) end--;
        return start > end ? [] : weeks.slice(start, end + 1);
    }

    /* The chart size the bars are drawn against, grown when the run went lower than it. */
    function scaleOf(base, weeks) {
        var scale = base || 20;
        (weeks || []).forEach(function (w) {
            if (charted(w) && w.position > scale) scale = w.position;
        });
        return scale;
    }

    function band(position) {
        if (position <= 1) return 'one';
        if (position <= 5) return 'top5';
        if (position <= 10) return 'top10';
        return 'rest';
    }

    function isDays(unit) {
        return unit === 'days' || unit === 'day';
    }

    function weekLabel(w) {
        return w.dateRange || w.periodKey || '';
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

    /* Compact gap label in the chart's own unit, never converted: 80w on weekly charts, 45d on daily ones. */
    function gapShort(n, unit) {
        return n + (isDays(unit) ? 'd' : 'w');
    }

    function gapTitle(off, unit) {
        var length = isDays(unit) ? plural(off.length, 'day', 'days') : plural(off.length, 'week', 'weeks');
        return 'Off the chart for ' + length + ' (' + weekLabel(off[0]) + ' to ' + weekLabel(off[off.length - 1]) + ')';
    }

    function barHeight(position, scale) {
        return Math.max(8, Math.round((scale + 1 - position) / scale * 100)) + '%';
    }

    function el(tag, className, text) {
        var node = document.createElement(tag);
        if (className) node.className = className;
        if (text !== undefined && text !== null) node.textContent = text;
        return node;
    }

    /* Best position of the run, or null when it never charted. */
    function peakOf(weeks) {
        var peak = null;
        (weeks || []).forEach(function (w) {
            if (charted(w) && (peak === null || w.position < peak)) peak = w.position;
        });
        return peak;
    }

    /* One bar per period, taller is better; the peak periods get their own color and long gaps show their length. */
    function renderBars(container, weeks, scale, unit, large) {
        var segs = segments(weeks);
        var peak = peakOf(weeks);
        container.textContent = '';
        var chartedCount = segs.filter(function (s) { return s.week; }).length;
        container.classList.toggle('is-dense', !large && chartedCount > 70);
        segs.forEach(function (s) {
            var node = document.createElement('span');
            if (s.week) {
                node.className = 'chh-bar chh-band-' + band(s.week.position) + (s.week.position === peak ? ' is-peak' : '');
                node.style.height = barHeight(s.week.position, scale);
                node.title = weekLabel(s.week) + ': #' + s.week.position;
            } else if (s.off) {
                node.className = 'chh-bar is-off';
                node.title = weekLabel(s.off) + ': off the chart';
            } else {
                node.className = 'chh-gap';
                node.title = gapTitle(s.gap, unit);
                node.appendChild(el('span', 'chh-gap-label', gapShort(s.gap.length, unit)));
            }
            container.appendChild(node);
        });
    }

    /* Numbered boxes linking to each chart; absences become one box (Nx, or 34w when long).
       periodHref holds {key}; currentKey marks the chart being viewed. */
    function renderBoxes(container, weeks, periodHref, unit, currentKey) {
        var segs = segments(weeks);
        container.textContent = '';
        if (!segs.length) {
            container.appendChild(el('span', 'chh-run-note', 'No chart entries found.'));
            return;
        }
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
                box.className = 'chh-box chh-band-' + band(s.week.position) + (currentKey && s.week.periodKey === currentKey ? ' is-now' : '');
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
            container.appendChild(el('span', 'chh-box is-off', off.length >= GAP_MIN ? gapShort(off.length, unit) : off.length + 'x'));
            container.lastChild.title = gapTitle(off, unit);
        }
    }

    // ── Run modal ──

    function ensureDialog(host) {
        if (host.chartRunDialog) {
            return host.chartRunDialog;
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
        host.appendChild(dialog);
        host.chartRunDialog = dialog;
        return dialog;
    }

    /* opts: host (element the dialog lives in), scopeClass (extra classes for the dialog, e.g. gender tokens),
       head (a .chh-hit-head node), id (identifies the run), load () => Promise<{weeks, scale}>,
       unit, periodHref, foot (nodes for the debut / peak line). */
    function openModal(opts) {
        var dialog = ensureDialog(opts.host);
        dialog.className = 'chh-modal' + (opts.scopeClass ? ' ' + opts.scopeClass : '');
        var head = dialog.querySelector('.chh-modal-head');
        var body = dialog.querySelector('.chh-modal-body');
        head.textContent = '';
        var name = opts.head.querySelector('.chh-hit-name');
        if (name) name.id = 'chhModalTitle';
        head.appendChild(opts.head);
        body.textContent = '';
        body.appendChild(el('p', 'chh-run-note', 'Loading run...'));
        if (!dialog.open) dialog.showModal();

        var id = String(opts.id);
        dialog.dataset.runId = id;
        opts.load().then(function (run) {
            if (dialog.dataset.runId === id) {
                renderModalBody(body, trim(run.weeks), scaleOf(run.scale, run.weeks), opts);
            }
        }).catch(function () {
            if (dialog.dataset.runId === id) {
                body.textContent = '';
                body.appendChild(el('p', 'chh-run-note', 'Could not load the run.'));
            }
        });
    }

    function renderModalBody(body, weeks, scale, opts) {
        var unit = isDays(opts.unit) ? 'days' : 'weeks';
        body.textContent = '';
        var chartedWeeks = weeks.filter(charted);
        if (!chartedWeeks.length) {
            body.appendChild(el('p', 'chh-run-note', 'No chart entries.'));
            return;
        }

        // Time on the chart, then time in the top tiers as a share of the run (peak and time at peak sit in the header).
        // TRL and Vato's Cuntdown are top 10 countdowns, so their top 10 tile would always read 100%.
        var total = chartedWeeks.length;
        var count = function (max) { return chartedWeeks.filter(function (w) { return w.position <= max; }).length; };
        var stats = el('div', 'chh-modal-stats');
        var onChart = el('div', 'chh-stat');
        onChart.appendChild(el('span', 'chh-stat-label', unit + ' on chart'));
        onChart.appendChild(el('span', 'chh-stat-value chh-num', String(total)));
        stats.appendChild(onChart);
        [5, 10].forEach(function (tier) {
            if (tier >= scale) return;
            var value = count(tier);
            var pct = Math.round(value / total * 100);
            var stat = el('div', 'chh-stat');
            stat.appendChild(el('span', 'chh-stat-label', unit + ' at top ' + tier));
            stat.appendChild(el('span', 'chh-stat-value chh-num', String(value)));
            var bar = el('span', 'chh-stat-bar');
            var fill = el('i');
            fill.style.width = pct + '%';
            bar.appendChild(fill);
            stat.appendChild(bar);
            stat.appendChild(el('span', 'chh-stat-note', pct + '% of the run'));
            stats.appendChild(stat);
        });
        body.appendChild(stats);

        // Large run chart (the numbered boxes below carry the "Chart run" title)
        var chartHead = el('div', 'chh-modal-chart-head');
        chartHead.appendChild(el('span', 'chh-modal-range', weekLabel(weeks[0]) + '  to  ' + weekLabel(weeks[weeks.length - 1])));
        body.appendChild(chartHead);

        var chart = el('div', 'chh-modal-chart');
        var mid = Math.max(2, Math.round(scale / 2));
        [[1, 'top'], [mid, 'mid'], [scale, 'bottom']].forEach(function (a) {
            chart.appendChild(el('span', 'chh-axis chh-axis-' + a[1], '#' + a[0]));
            chart.appendChild(el('span', 'chh-guide chh-guide-' + a[1]));
        });
        var bars = el('div', 'chh-run-bars is-large');
        renderBars(bars, weeks, scale, unit, true);
        chart.appendChild(bars);
        body.appendChild(chart);

        var legend = el('div', 'chh-legend');
        [['one', '#1'], ['top5', '#2 to #5'], ['top10', '#6 to #10'], ['rest', '#11 and below']].forEach(function (l) {
            var item = el('span');
            item.appendChild(el('i', 'chh-band-' + l[0]));
            item.appendChild(document.createTextNode(l[1]));
            legend.appendChild(item);
        });
        var peak = peakOf(chartedWeeks);
        if (peak > 1) {
            var peakItem = el('span');
            peakItem.appendChild(el('i', 'chh-legend-peak'));
            peakItem.appendChild(document.createTextNode('Peak (#' + peak + ')'));
            legend.appendChild(peakItem);
        }
        var gapItem = el('span');
        gapItem.appendChild(el('i', 'chh-legend-gap'));
        gapItem.appendChild(document.createTextNode('Off the chart ' + GAP_MIN + '+ ' + unit));
        legend.appendChild(gapItem);
        body.appendChild(legend);

        // Numbered run, each box links to its chart
        var boxesHead = el('div', 'chh-modal-chart-head');
        boxesHead.appendChild(el('span', 'chh-eyebrow', 'Chart run'));
        body.appendChild(boxesHead);
        var boxes = el('div', 'chh-run-boxes');
        renderBoxes(boxes, weeks, opts.periodHref, unit);
        body.appendChild(boxes);

        if (opts.foot && opts.foot.length) {
            var foot = el('div', 'chh-modal-foot');
            opts.foot.forEach(function (node) { foot.appendChild(node); });
            body.appendChild(foot);
        }
    }

    window.ChartRun = {
        trim: trim,
        scaleOf: scaleOf,
        renderBars: renderBars,
        renderBoxes: renderBoxes,
        openModal: openModal
    };
})();
