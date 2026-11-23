/*
 * Helpers shared by the charts built on Chart.js: Playground > Breakdown over time (breakdown-history.js),
 * the Share over time panel (catalog-share-history.js) and the gender bars per year (catalog-year-bars.js).
 */
window.ShareChartCore = (() => {
    'use strict';
    const reducedMotion = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;

    // Area hit testing uses the actual filled band at the pointer's x coordinate.
    // Nearest-point mode alone can select the adjacent band instead of the hovered one.
    function sectionInteraction(chart, event, options, useFinalPosition) {
        if (!chart.options.scales.y.stacked || chart.config.type === 'bar') {
            return chart.getElementsAtEventForMode(event, 'nearest',
                { axis: 'xy', intersect: chart.config.type === 'bar' }, useFinalPosition).slice(0, 1);
        }
        const { x, y } = Chart.helpers.getRelativePosition(event, chart);
        const area = chart.chartArea;
        if (x < area.left || x > area.right || y < area.top || y > area.bottom) return [];
        const count = chart.data.labels.length;
        const firstX = chart.scales.x.getPixelForValue(0);
        const spacing = count > 1 ? chart.scales.x.getPixelForValue(1) - firstX : 1;
        const position = Math.max(0, Math.min(count - 1, (x - firstX) / spacing));
        const left = Math.floor(position), right = Math.ceil(position), index = Math.round(position);
        let bottom = 0;
        for (let datasetIndex = 0; datasetIndex < chart.data.datasets.length; datasetIndex++) {
            const values = chart.data.datasets[datasetIndex].data;
            if (values[left] === null || values[right] === null) return [];
            const value = values[left] + (values[right] - values[left]) * (position - left);
            const top = bottom + value;
            if (value > 0 && y <= chart.scales.y.getPixelForValue(bottom) && y >= chart.scales.y.getPixelForValue(top)) {
                return [{ element: chart.getDatasetMeta(datasetIndex).data[index], datasetIndex, index }];
            }
            bottom = top;
        }
        return [];
    }

    /** Registers the band hit-testing mode once, as interaction mode 'stackedSection'. */
    function registerSectionMode() {
        if (typeof Chart !== 'undefined') Chart.Interaction.modes.stackedSection = sectionInteraction;
    }

    function niceCeiling(value) {
        if (value <= 0) return 0;
        const step = Math.max(1, 10 ** Math.floor(Math.log10(value)) / 2);
        return Math.ceil(value / step) * step;
    }

    /** Per-viewer settings in localStorage; reads fall back to {} when storage is off. */
    function store(key) {
        const read = () => {
            try { return JSON.parse(localStorage.getItem(key) || '{}') || {}; } catch { return {}; }
        };
        return {
            read,
            write(patch) {
                try { localStorage.setItem(key, JSON.stringify({ ...read(), ...patch })); } catch { /* storage off */ }
            }
        };
    }

    /**
     * Collapse button of a list chart panel (.is-collapsed on root, [data-sh="toggle"] button, .sh-body).
     * Phones start collapsed so the list stays in reach; the choice is remembered once toggled.
     * onExpand runs whenever the panel opens. Returns whether it starts collapsed.
     */
    function collapsible(root, settings, onExpand) {
        const toggle = root.querySelector('[data-sh="toggle"]');
        const body = root.querySelector('.sh-body');
        const set = collapsed => {
            root.classList.toggle('is-collapsed', collapsed);
            toggle.setAttribute('aria-expanded', String(!collapsed));
            toggle.setAttribute('aria-label', collapsed ? 'Show chart' : 'Hide chart');
            body.hidden = collapsed;
        };
        const saved = settings.read().collapsed;
        set(typeof saved === 'boolean' ? saved : window.matchMedia?.('(max-width: 768px)').matches === true);
        toggle.addEventListener('click', () => {
            const collapsed = !root.classList.contains('is-collapsed');
            set(collapsed);
            settings.write({ collapsed });
            if (!collapsed) onExpand();
        });
        return root.classList.contains('is-collapsed');
    }

    /** Scrolls to a card or table row and makes it glow twice in the given "r, g, b" color. */
    function glow(target, rgb) {
        target.style.setProperty('--share-glow', rgb);
        target.classList.remove('share-glow');
        void target.offsetWidth;
        target.classList.add('share-glow');
        clearTimeout(target.shareGlowTimer);
        target.shareGlowTimer = setTimeout(() => target.classList.remove('share-glow'), 3200);
        target.scrollIntoView({ behavior: reducedMotion ? 'auto' : 'smooth', block: 'center' });
    }

    /** Calls onKey with the hovered card's or row's key (the value of attr), and null when the pointer leaves. */
    function bindListHover(scopeId, attr, onKey) {
        const scope = document.getElementById(scopeId);
        if (!scope) return;
        let hovered = null;
        scope.addEventListener('mouseover', event => {
            const item = event.target.closest(`[${attr}]`);
            const key = item && scope.contains(item) ? item.getAttribute(attr) : null;
            if (key === hovered) return;
            hovered = key;
            onKey(key);
        });
        scope.addEventListener('mouseleave', () => {
            hovered = null;
            onKey(null);
        });
    }

    /** True while the list shows its Table view instead of the cards. */
    function tableVisible() {
        const table = document.getElementById('tableView');
        return Boolean(table) && table.style.display !== 'none';
    }

    return { registerSectionMode, niceCeiling, store, collapsible, glow, bindListHover, tableVisible };
})();
