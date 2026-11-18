/**
 * Single-key sorting for detail page sections (artist albums shelf/table and songs table).
 *
 * Markup contract inside the section root:
 *   [data-sort-key]           chips or header buttons; clicking sorts by that key, clicking the active one flips it
 *   [data-sort-type]          on the same buttons: number | date | text (decides the default direction)
 *   [data-sort-container]     elements whose [data-sort-item] children get reordered
 *   [data-sort-item]          items carrying data-<key> values
 *   [data-sort-pin]           items that always stay last (summary rows/cards)
 *   [data-sort-index]         elements inside items that show the position, renumbered after each sort
 *   [data-bar-value]          bars inside items, scaled against the largest value in their container
 * The root gets data-active-sort="<key>" so items can reveal the metric they are sorted by.
 */
(function () {
    const MONTHS = { jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5, jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11 };

    function parseDate(value) {
        if (!value || value === '-') return null;
        const v = value.trim();
        let m = v.match(/^(\d{1,2})[-\s]([A-Za-z]{3})[A-Za-z]*[-\s](\d{4})$/);
        if (m) {
            const month = MONTHS[m[2].toLowerCase()];
            return month === undefined ? null : new Date(+m[3], month, +m[1]).getTime();
        }
        m = v.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
        if (m) return new Date(+m[3], +m[2] - 1, +m[1]).getTime();
        m = v.match(/^(\d{4})-(\d{2})-(\d{2})/);
        if (m) return new Date(+m[1], +m[2] - 1, +m[3]).getTime();
        return null;
    }

    function readValue(item, key, type) {
        const raw = item.dataset[key];
        if (type === 'number') {
            const n = parseFloat(raw);
            return isNaN(n) ? null : n;
        }
        if (type === 'date') return parseDate(raw);
        return raw ? raw.toLowerCase() : null;
    }

    function compare(a, b, dir) {
        // Missing values always sink to the bottom, whichever the direction
        if (a === null && b === null) return 0;
        if (a === null) return 1;
        if (b === null) return -1;
        const result = a < b ? -1 : a > b ? 1 : 0;
        return dir === 'asc' ? result : -result;
    }

    function scaleBars(container) {
        const bars = Array.from(container.querySelectorAll('[data-bar-value]'));
        const max = bars.reduce((m, bar) => Math.max(m, parseFloat(bar.dataset.barValue) || 0), 0);
        bars.forEach(bar => {
            const value = parseFloat(bar.dataset.barValue) || 0;
            bar.style.width = max > 0 ? Math.max(value > 0 ? 3 : 0, value / max * 100) + '%' : '0%';
        });
    }

    function init(root, options) {
        if (!root) return null;
        const state = { key: options.defaultKey, dir: options.defaultDir || 'desc' };
        const triggers = Array.from(root.querySelectorAll('[data-sort-key]'));
        const containers = Array.from(root.querySelectorAll('[data-sort-container]'));

        function typeOf(key) {
            const trigger = triggers.find(t => t.dataset.sortKey === key);
            return trigger ? (trigger.dataset.sortType || 'text') : 'text';
        }

        function apply() {
            const type = typeOf(state.key);
            containers.forEach(container => {
                // Expanded mobile detail rows belong to the row above them; close them before moving rows
                container.querySelectorAll('.mobile-table-detail-row').forEach(row => row.remove());
                container.querySelectorAll('.mobile-row-expand-btn.expanded').forEach(btn => {
                    btn.classList.remove('expanded');
                    btn.textContent = '+';
                    btn.setAttribute('aria-expanded', 'false');
                });

                const items = Array.from(container.children).filter(el => el.hasAttribute('data-sort-item'));
                const pinned = items.filter(el => el.hasAttribute('data-sort-pin'));
                const sortable = items.filter(el => !el.hasAttribute('data-sort-pin'));
                sortable.sort((a, b) => {
                    const primary = compare(readValue(a, state.key, type), readValue(b, state.key, type), state.dir);
                    if (primary !== 0) return primary;
                    return compare(readValue(a, 'name', 'text'), readValue(b, 'name', 'text'), 'asc');
                });
                sortable.concat(pinned).forEach(el => container.appendChild(el));
                sortable.forEach((el, index) => {
                    el.querySelectorAll('[data-sort-index]').forEach(n => {
                        n.textContent = n.dataset.sortIndex === 'hash' ? '#' + (index + 1) : String(index + 1);
                    });
                });
            });

            // Lets cards show the metric they are sorted by (e.g. [data-active-sort="releasedate"])
            root.dataset.activeSort = state.key;

            triggers.forEach(trigger => {
                const active = trigger.dataset.sortKey === state.key;
                trigger.classList.toggle('is-active', active);
                trigger.setAttribute('aria-pressed', String(active));
                if (active) {
                    trigger.dataset.sortDir = state.dir;
                } else {
                    delete trigger.dataset.sortDir;
                }
            });
        }

        triggers.forEach(trigger => {
            trigger.addEventListener('click', () => {
                const key = trigger.dataset.sortKey;
                if (state.key === key) {
                    state.dir = state.dir === 'asc' ? 'desc' : 'asc';
                } else {
                    state.key = key;
                    state.dir = (trigger.dataset.sortType || 'text') === 'text' ? 'asc' : 'desc';
                }
                apply();
            });
        });

        containers.forEach(scaleBars);
        apply();
        return { apply, state };
    }

    window.initDetailSort = function (rootId, options) {
        return init(document.getElementById(rootId), options || {});
    };
    window.parseDetailSortDate = parseDate;
})();
