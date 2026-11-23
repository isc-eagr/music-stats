/*
 * Floating table headers (see "Tables" in AGENTS.md, CSS in global.css).
 * A .table-scroll wrapper starts as a scroll box capped to the window height, so a
 * .sticky-head header stays visible even when the table scrolls sideways. When the
 * table fits the width and no ancestor clips it, the box is released
 * (.is-page-scroll): the page keeps a single scrollbar and the header sticks to the
 * top of the window instead. Re-checked on resize and when tables are added.
 */
(function () {
    'use strict';

    const resizeObserver = window.ResizeObserver ? new ResizeObserver(entries => {
        entries.forEach(entry => {
            const box = entry.target.closest('.table-scroll');
            if (box) update(box);
        });
    }) : null;
    const tracked = new WeakSet();

    function clippedByAncestor(box) {
        for (let el = box.parentElement; el && el !== document.body && el !== document.documentElement; el = el.parentElement) {
            const style = getComputedStyle(el);
            if (/(auto|scroll|hidden)/.test(style.overflowX + ' ' + style.overflowY)) return true;
        }
        return false;
    }

    function update(box) {
        const table = box.querySelector('table');
        if (!table) return;
        const fits = table.offsetWidth <= box.clientWidth + 1;
        box.classList.toggle('is-page-scroll', fits && !clippedByAncestor(box));
    }

    function scan() {
        document.querySelectorAll('.table-scroll').forEach(box => {
            const table = box.querySelector('table');
            if (!tracked.has(box)) {
                tracked.add(box);
                if (resizeObserver) resizeObserver.observe(box);
            }
            if (table && !tracked.has(table)) {
                tracked.add(table);
                if (resizeObserver) resizeObserver.observe(table);
            }
            update(box);
        });
    }

    let scanQueued = false;
    function queueScan() {
        if (scanQueued) return;
        scanQueued = true;
        requestAnimationFrame(() => {
            scanQueued = false;
            scan();
        });
    }

    function init() {
        scan();
        new MutationObserver(queueScan).observe(document.body, { childList: true, subtree: true });
        window.addEventListener('resize', queueScan);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
}());
