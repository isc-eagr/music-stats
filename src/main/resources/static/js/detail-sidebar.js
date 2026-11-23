/**
 * Detail pages (artist, album, song): sidebar behavior. Rank neighbors tooltips on the profile ranks (below), and
 * an overlay scroll thumb for the sticky sidebar card.
 * The sidebar hides its native scrollbar (no reserved strip); this thumb floats over the right edge,
 * shows on hover (detail-general.css), tracks the scroll position and can be dragged.
 */
(function () {
    function init() {
        const sidebar = document.querySelector('.container-wide.dt-page > .detail-grid > .detail-sidebar.dt-card');
        if (!sidebar) return;
        const thumb = document.createElement('div');
        thumb.className = 'dt-sb-thumb';
        thumb.setAttribute('aria-hidden', 'true');
        sidebar.appendChild(thumb);

        function update() {
            const view = sidebar.clientHeight;
            const full = sidebar.scrollHeight;
            const range = full - view;
            // Only when the sidebar actually scrolls (it is taller than the window and not the phone layout)
            if (range <= 1 || getComputedStyle(sidebar).overflowY === 'visible') {
                thumb.hidden = true;
                return;
            }
            thumb.hidden = false;
            const height = Math.max(32, view * view / full);
            const top = sidebar.scrollTop + (sidebar.scrollTop / range) * (view - height);
            thumb.style.height = height + 'px';
            thumb.style.top = top + 'px';
        }

        let dragStart = null;
        thumb.addEventListener('pointerdown', event => {
            event.preventDefault();
            thumb.setPointerCapture(event.pointerId);
            thumb.classList.add('is-dragging');
            dragStart = { y: event.clientY, scroll: sidebar.scrollTop };
        });
        thumb.addEventListener('pointermove', event => {
            if (!dragStart) return;
            const view = sidebar.clientHeight;
            const range = sidebar.scrollHeight - view;
            const track = view - thumb.offsetHeight;
            if (track > 0) sidebar.scrollTop = dragStart.scroll + (event.clientY - dragStart.y) * range / track;
        });
        const endDrag = () => {
            dragStart = null;
            thumb.classList.remove('is-dragging');
        };
        thumb.addEventListener('pointerup', endDrag);
        thumb.addEventListener('pointercancel', endDrag);

        sidebar.addEventListener('scroll', update, { passive: true });
        window.addEventListener('resize', update);
        if (window.ResizeObserver) {
            const observer = new ResizeObserver(update);
            observer.observe(sidebar);
            Array.from(sidebar.children).forEach(child => { if (child !== thumb) observer.observe(child); });
        }
        update();
    }

    /**
     * Rank neighbors: hovering a ranked profile row (genre, country, release year, ...) or the header Overall
     * badge shows the items ranked
     * 5 above and 5 below this one in that attribute, with their plays. Loaded on first hover and cached.
     * The tooltip stays open while the pointer is over it so its rows can be clicked.
     */
    function initRankNeighbors() {
        const page = document.querySelector('.container-wide.dt-page');
        const match = location.pathname.match(/^\/(artists|albums|songs)\/(\d+)/);
        if (!page || !match || !window.matchMedia('(hover: hover)').matches) return;
        const rows = page.querySelectorAll('.detail-sidebar a.dt-row[data-rank-attr], a.dt-standing[data-rank-attr]');
        if (!rows.length) return;
        const [, catalog, itemId] = match;
        const cache = new Map();
        const tip = document.createElement('div');
        tip.className = 'dt-rn';
        tip.hidden = true;
        page.appendChild(tip);
        let activeRow = null;
        let showTimer = null;
        let hideTimer = null;

        const esc = value => String(value == null ? '' : value)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
        const fmt = n => Number(n || 0).toLocaleString('en-US');

        function load(attr) {
            if (!cache.has(attr)) {
                cache.set(attr, fetch('/' + catalog + '/' + itemId + '/rank-neighbors?attr=' + encodeURIComponent(attr))
                    .then(r => (r.ok ? r.json() : Promise.reject(r.status)))
                    .catch(err => { cache.delete(attr); throw err; }));
            }
            return cache.get(attr);
        }

        function render(row, data) {
            const current = data.items.find(item => item.current);
            const head = '<div class="dt-rn-head"><b>' + esc(row.dataset.rankValue) + '</b><span>'
                + (current ? '#' + fmt(current.rank) + ' of ' : '') + fmt(data.total) + ' ' + esc(row.dataset.rankNoun) + '</span></div>';
            const body = data.items.map(item => '<a class="dt-rn-row' + (item.current ? ' is-current' : '') + '" href="/'
                + catalog + '/' + item.id + '">'
                + '<span class="dt-rn-rank' + (item.rank === 1 ? ' is-gold' : '') + '">#' + fmt(item.rank) + '</span>'
                + '<span class="dt-rn-dot is-' + esc(item.gender) + '" aria-hidden="true"></span>'
                + '<span class="dt-rn-name"><span>' + esc(item.name) + '</span>'
                + (item.artistName ? '<small>' + esc(item.artistName) + '</small>' : '') + '</span>'
                + '<span class="dt-rn-plays">' + fmt(item.plays) + '</span></a>').join('');
            tip.innerHTML = head + '<div class="dt-rn-list">' + body + '</div>';
        }

        function position(row) {
            const rect = row.getBoundingClientRect();
            const sidebar = row.closest('.detail-sidebar');
            const sidebarRect = sidebar ? sidebar.getBoundingClientRect() : null;
            const width = tip.offsetWidth;
            const height = tip.offsetHeight;
            let left;
            let top;
            if (sidebarRect && sidebarRect.right + 12 + width <= window.innerWidth - 8) {
                // Desktop: beside the sidebar, centered on the hovered row
                left = sidebarRect.right + 12;
                top = rect.top + rect.height / 2 - height / 2;
            } else {
                // Header Overall badge or a narrow window: under the element, or above it when there is no room below
                left = rect.left + rect.width / 2 - width / 2;
                top = rect.bottom + 8 + height <= window.innerHeight - 8 ? rect.bottom + 8 : rect.top - height - 8;
            }
            tip.style.left = Math.round(Math.max(8, Math.min(left, window.innerWidth - width - 8))) + 'px';
            tip.style.top = Math.round(Math.max(8, Math.min(top, window.innerHeight - height - 8))) + 'px';
        }

        function show(row) {
            activeRow = row;
            const attr = row.dataset.rankAttr;
            tip.innerHTML = '<div class="dt-rn-head"><b>' + esc(row.dataset.rankValue) + '</b><span>Loading...</span></div>';
            tip.hidden = false;
            position(row);
            load(attr).then(data => {
                if (activeRow !== row) return;
                render(row, data);
                position(row);
            }).catch(() => {
                if (activeRow !== row) return;
                tip.innerHTML = '<div class="dt-rn-head"><b>' + esc(row.dataset.rankValue) + '</b><span>Could not load ranks</span></div>';
            });
        }

        function hide() {
            activeRow = null;
            tip.hidden = true;
        }

        function scheduleHide() {
            clearTimeout(showTimer);
            clearTimeout(hideTimer);
            hideTimer = setTimeout(hide, 180);
        }

        rows.forEach(row => {
            // The native title would pop up over the tooltip; the tooltip header says the same thing
            row.removeAttribute('title');
            row.addEventListener('mouseenter', () => {
                clearTimeout(hideTimer);
                clearTimeout(showTimer);
                if (activeRow === row && !tip.hidden) return;
                showTimer = setTimeout(() => show(row), activeRow ? 0 : 150);
            });
            row.addEventListener('mouseleave', scheduleHide);
        });
        tip.addEventListener('mouseenter', () => clearTimeout(hideTimer));
        tip.addEventListener('mouseleave', scheduleHide);
        window.addEventListener('scroll', () => activeRow && position(activeRow), true);
        window.addEventListener('resize', () => activeRow && position(activeRow));
    }

    function start() {
        init();
        initRankNeighbors();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', start);
    } else {
        start();
    }
})();
