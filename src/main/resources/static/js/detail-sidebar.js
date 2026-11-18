/**
 * Detail pages (artist, album, song): overlay scroll thumb for the sticky sidebar card.
 * The sidebar hides its native scrollbar (no reserved strip); this thumb floats over the right edge,
 * shows on hover (detail-general.css), tracks the scroll position and can be dragged.
 */
(function () {
    function init() {
        const sidebar = document.querySelector('.container-wide.ad-page > .detail-grid > .detail-sidebar.ad-card');
        if (!sidebar) return;
        const thumb = document.createElement('div');
        thumb.className = 'ad-sb-thumb';
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

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
