(function () {
    'use strict';

    const tableSelector = '.overview-table, .pc-table, .trl-table, .bb-table';

    function initializeStickyHeaders() {
        const tables = Array.from(document.querySelectorAll(tableSelector)).filter(table => table.tHead);
        if (!tables.length) return;

        const overlays = tables.map(table => createOverlay(table));
        let frameRequested = false;

        function scheduleUpdate() {
            if (frameRequested) return;
            frameRequested = true;
            window.requestAnimationFrame(() => {
                frameRequested = false;
                overlays.forEach(updateOverlay);
            });
        }

        document.addEventListener('scroll', scheduleUpdate, true);
        window.addEventListener('resize', scheduleUpdate);
        if (window.ResizeObserver) {
            const observer = new window.ResizeObserver(scheduleUpdate);
            tables.forEach(table => observer.observe(table));
        }
        scheduleUpdate();
    }

    function createOverlay(table) {
        const overlay = document.createElement('div');
        overlay.className = 'overview-sticky-header-layer';
        overlay.setAttribute('aria-hidden', 'true');

        const clone = table.cloneNode(false);
        clone.removeAttribute('id');
        clone.classList.add('overview-sticky-header-table');
        clone.appendChild(table.tHead.cloneNode(true));
        overlay.appendChild(clone);
        document.body.appendChild(overlay);

        overlay.addEventListener('click', event => {
            const cloneLink = event.target.closest('a[href]');
            if (cloneLink) {
                event.preventDefault();
                window.location.assign(cloneLink.href);
                return;
            }
            const cloneCell = event.target.closest('th');
            if (!cloneCell) return;
            event.preventDefault();
            const cloneCells = Array.from(clone.querySelectorAll('thead th'));
            const originalCell = table.querySelectorAll('thead th')[cloneCells.indexOf(cloneCell)];
            originalCell?.click();
        });

        return { table, overlay, clone };
    }

    function updateOverlay({ table, overlay, clone }) {
        const header = table.tHead;
        const tableRect = table.getBoundingClientRect();
        const headerRect = header.getBoundingClientRect();
        const shouldShow = headerRect.top < 0 && tableRect.bottom > headerRect.height && tableRect.top < 0;

        if (!shouldShow) {
            overlay.hidden = true;
            return;
        }

        const sourceCells = Array.from(header.querySelectorAll('th'));
        const cloneCells = Array.from(clone.querySelectorAll('thead th'));
        sourceCells.forEach((cell, index) => {
            const cloneCell = cloneCells[index];
            if (cloneCell) cloneCell.style.width = cell.getBoundingClientRect().width + 'px';
        });
        clone.style.width = tableRect.width + 'px';
        clone.style.transform = 'translateX(' + tableRect.left + 'px)';
        overlay.hidden = false;
    }

    document.addEventListener('DOMContentLoaded', initializeStickyHeaders);
}());
