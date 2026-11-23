/*
 * Gender bars per year above Listen Years and Release Years (fragments/share-history.html :: year-panel).
 * One bar per year, oldest first, split into male, female and other, read from the year cards' data-bars
 * attribute so the bars always match the cards. Clicking a bar highlights that year's card or row;
 * hovering a card or row lights up its bar.
 */
(() => {
    'use strict';
    const root = document.getElementById('yearBars');
    if (!root) return;

    const sh = name => root.querySelector(`[data-sh="${name}"]`);
    const yearType = root.dataset.yearType;
    const settings = ShareChartCore.store(`yearBars:${yearType}`);
    // Order of the measures inside each card's data-bars ("m f o|m f o|...").
    const MEASURES = ['plays', 'time', 'artists', 'albums', 'songs'];
    const LABELS = { plays: 'plays', time: 'play time', artists: 'artists', albums: 'albums', songs: 'songs' };
    // The app's male blue and female pink, muted to sit with the dark cards.
    const GENDERS = [
        { name: 'Male', rgb: [90, 141, 204] },
        { name: 'Female', rgb: [201, 106, 152] },
        { name: 'Other / unknown', rgb: [107, 107, 117] }
    ];
    const TEXT = 'rgba(212, 212, 216, 0.5)';

    let chart = null;
    let focusIndex = null;

    const measure = () => sh('measure').value;
    const color = (gender, alpha = 1) => `rgba(${gender.rgb.join(', ')}, ${alpha})`;
    const compact = new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 });

    function amount(value) {
        if (measure() === 'time') return `${Math.floor(value / 3600).toLocaleString()}h ${Math.floor(value % 3600 / 60)}m`;
        return `${Math.round(value).toLocaleString()} ${LABELS[measure()]}`;
    }

    /** Years from the cards, oldest first: { year, values: { plays: [male, female, other], ... } }. */
    function readYears() {
        return [...document.querySelectorAll('#cardView [data-bars]')].map(card => {
            const parts = card.dataset.bars.split('|');
            const values = {};
            MEASURES.forEach((name, i) => {
                values[name] = (parts[i] || '').split(' ').map(value => Number(value) || 0);
                while (values[name].length < 3) values[name].push(0);
            });
            return { year: card.dataset.id, values };
        }).sort((a, b) => Number(a.year) - Number(b.year));
    }

    const years = readYears();
    if (!years.length) {
        root.hidden = true;
        return;
    }
    const index = new Map(years.map((entry, i) => [entry.year, i]));

    /* ---------- Chart ---------- */

    function render() {
        if (chart) { chart.destroy(); chart = null; }
        hideTooltip();
        const name = measure();
        const totals = GENDERS.map((_, g) => years.reduce((sum, entry) => sum + entry.values[name][g], 0));
        const grand = totals.reduce((sum, value) => sum + value, 0);
        renderLegend(totals, grand);
        if (typeof Chart === 'undefined') {
            sh('status').textContent = 'The chart library could not load. Reload the page to retry.';
            sh('status').hidden = false;
            return;
        }
        const font = { family: 'Inter, system-ui, sans-serif', size: 11 };
        const narrow = sh('wrap').clientWidth < 500;
        chart = new Chart(sh('canvas'), {
            type: 'bar',
            data: {
                labels: years.map(entry => entry.year),
                datasets: GENDERS.map((gender, g) => ({
                    label: gender.name,
                    data: years.map(entry => name === 'time' ? entry.values[name][g] / 3600 : entry.values[name][g]),
                    backgroundColor: years.map(() => color(gender)),
                    borderWidth: 0,
                    borderRadius: 2,
                    barPercentage: 0.82,
                    categoryPercentage: 0.9
                }))
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                interaction: { mode: 'index', intersect: false },
                onHover: (event, elements) => {
                    event.native.target.style.cursor = elements.length ? 'pointer' : 'default';
                },
                onClick: (event, elements) => {
                    if (elements.length) select(elements[0].index);
                    else setFocus(null);
                },
                scales: {
                    x: { stacked: true, border: { display: false }, grid: { display: false },
                        ticks: { color: TEXT, font, autoSkip: true, maxTicksLimit: narrow ? 6 : 16, maxRotation: 0 } },
                    y: { stacked: true, min: 0, border: { display: false },
                        grid: { color: 'rgba(255, 255, 255, 0.05)' },
                        ticks: { color: TEXT, font, maxTicksLimit: 5,
                            callback: value => name === 'time' ? `${compact.format(value)}h` : compact.format(value) } }
                },
                plugins: {
                    legend: { display: false },
                    tooltip: { enabled: false, external: renderTooltip }
                }
            }
        });
        applyFocus();
    }

    function renderLegend(totals, grand) {
        sh('legend').replaceChildren(...GENDERS.map((gender, g) => {
            const chip = document.createElement('span');
            chip.className = 'sh-chip';
            const swatch = document.createElement('i');
            swatch.className = 'sh-swatch';
            swatch.style.backgroundColor = color(gender);
            swatch.setAttribute('aria-hidden', 'true');
            const label = document.createElement('span');
            label.className = 'sh-chip-name';
            label.textContent = gender.name;
            const value = document.createElement('span');
            value.className = 'sh-chip-value';
            value.textContent = grand > 0 ? `${(100 * totals[g] / grand).toFixed(1)}%` : '';
            chip.title = `${gender.name}: ${amount(totals[g])} across these years`;
            chip.append(swatch, label, value);
            return chip;
        }));
    }

    /* ---------- Tooltip ---------- */

    function hideTooltip() {
        const element = sh('tooltip');
        element.hidden = true;
        element.replaceChildren();
    }

    function renderTooltip({ tooltip }) {
        const element = sh('tooltip');
        const point = tooltip.dataPoints?.[0];
        if (!tooltip.opacity || !point) { hideTooltip(); return; }
        const entry = years[point.dataIndex];
        const values = entry.values[measure()];
        const total = values.reduce((sum, value) => sum + value, 0);
        element.replaceChildren();
        const line = (className, text) => {
            const row = document.createElement('div');
            row.className = className;
            row.textContent = text;
            element.append(row);
            return row;
        };
        line('sh-tooltip-title', `${yearType === 'listen' ? 'Listen' : 'Release'} year`);
        line('sh-tooltip-name', entry.year);
        GENDERS.forEach((gender, g) => {
            if (!values[g]) return;
            const row = line('sh-tooltip-value sh-tooltip-gender',
                `${gender.name}: ${amount(values[g])} · ${(100 * values[g] / total).toFixed(1)}%`);
            const swatch = document.createElement('i');
            swatch.className = 'sh-swatch';
            swatch.style.backgroundColor = color(gender);
            row.prepend(swatch);
        });
        line('sh-tooltip-change', `Total: ${amount(total)}`);
        line('sh-tooltip-hint', `Click to highlight its ${ShareChartCore.tableVisible() ? 'row' : 'card'}`);
        element.hidden = false;
        const wrap = sh('wrap');
        const left = Math.min(Math.max(tooltip.caretX, element.offsetWidth / 2 + 4), wrap.clientWidth - element.offsetWidth / 2 - 4);
        element.style.left = `${left}px`;
        element.style.top = `${Math.max(tooltip.caretY, element.offsetHeight + 16)}px`;
    }

    /* ---------- Focus and highlight ---------- */

    function setFocus(i) {
        focusIndex = i;
        applyFocus();
    }

    // Dims every other year; a hovered card previews its bar, falling back to the clicked one.
    function applyFocus(i = focusIndex) {
        if (!chart) return;
        chart.data.datasets.forEach((dataset, g) => {
            dataset.backgroundColor = years.map((_, bar) => color(GENDERS[g], i == null || bar === i ? 1 : 0.25));
        });
        chart.update('none');
    }

    function select(i) {
        setFocus(i);
        const table = ShareChartCore.tableVisible();
        const scope = document.getElementById(table ? 'tableView' : 'cardView');
        const target = scope?.querySelector(`[${table ? 'data-year' : 'data-id'}="${CSS.escape(years[i].year)}"]`);
        if (!target) return;
        // Glow in the color of the gender that leads that year.
        const values = years[i].values[measure()];
        const leader = values.indexOf(Math.max(...values));
        ShareChartCore.glow(target, GENDERS[leader].rgb.join(', '));
    }

    /* ---------- Wiring ---------- */

    const savedMeasure = settings.read().measure;
    if (MEASURES.includes(savedMeasure)) sh('measure').value = savedMeasure;
    const collapsed = ShareChartCore.collapsible(root, settings, () => { if (!chart) render(); });
    sh('measure').addEventListener('change', () => {
        settings.write({ measure: measure() });
        render();
    });
    root.addEventListener('keydown', event => {
        if (event.key === 'Escape' && focusIndex != null) setFocus(null);
    });
    sh('canvas').addEventListener('mouseleave', hideTooltip);
    const hoverBar = key => applyFocus(key != null && index.has(key) ? index.get(key) : focusIndex);
    ShareChartCore.bindListHover('cardView', 'data-id', hoverBar);
    ShareChartCore.bindListHover('tableView', 'data-year', hoverBar);
    if (!collapsed) render();
})();
