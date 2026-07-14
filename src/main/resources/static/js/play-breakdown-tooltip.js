(function () {
    let tooltip = null;
    let activeTrigger = null;

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#039;');
    }

    function ensureTooltip() {
        if (!tooltip) {
            tooltip = document.createElement('div');
            tooltip.className = 'linked-song-tooltip';
            document.body.appendChild(tooltip);
        }
        return tooltip;
    }

    function position(trigger) {
        const node = ensureTooltip();
        const rect = trigger.getBoundingClientRect();
        const left = Math.max(8, Math.min(rect.left + rect.width / 2 - node.offsetWidth / 2, window.innerWidth - node.offsetWidth - 8));
        const top = rect.top >= node.offsetHeight + 12 ? rect.top - node.offsetHeight - 8 : rect.bottom + 8;
        node.style.left = Math.round(left) + 'px';
        node.style.top = Math.round(top) + 'px';
    }

    function show(trigger) {
        const items = Array.from(trigger.querySelectorAll('.play-breakdown-tooltip-item-source'))
            .map(item => item.textContent.trim()).filter(Boolean);
        if (!items.length) return;
        const node = ensureTooltip();
        node.innerHTML = '<div class="tooltip-title">' + escapeHtml(trigger.dataset.tooltipTitle || 'Play breakdown:') + '</div>'
            + items.map(item => '<div class="tooltip-song">' + escapeHtml(item) + '</div>').join('');
        node.style.display = 'block';
        activeTrigger = trigger;
        position(trigger);
    }

    function hide(trigger) {
        if (!trigger || activeTrigger === trigger) {
            ensureTooltip().style.display = 'none';
            activeTrigger = null;
        }
    }

    function initialize(root) {
        (root || document).querySelectorAll('.play-breakdown-tooltip-trigger').forEach(trigger => {
            if (trigger.dataset.playBreakdownBound === 'true') return;
            trigger.dataset.playBreakdownBound = 'true';
            trigger.addEventListener('mouseenter', () => show(trigger));
            trigger.addEventListener('mouseleave', () => hide(trigger));
            trigger.addEventListener('focusin', () => show(trigger));
            trigger.addEventListener('focusout', () => hide(trigger));
        });
    }

    window.buildPlayBreakdownTooltip = function (contentHtml, items, title) {
        const safeItems = (items || []).filter(Boolean);
        // A breakdown is useful only when multiple artist sources are selected.
        if (safeItems.length <= 1) return contentHtml;
        return '<span class="play-breakdown-tooltip-trigger linked-song-tooltip-trigger tooltip-enabled" tabindex="0" data-tooltip-title="'
            + escapeHtml(title || 'Play breakdown:') + '">' + contentHtml
            + safeItems.map(item => '<span class="play-breakdown-tooltip-item-source linked-song-tooltip-item-source" hidden>' + escapeHtml(item) + '</span>').join('')
            + '</span>';
    };
    window.initializePlayBreakdownTooltips = initialize;
    window.addEventListener('scroll', () => activeTrigger && position(activeTrigger), true);
    window.addEventListener('resize', () => activeTrigger && position(activeTrigger));
    document.addEventListener('DOMContentLoaded', () => initialize(document));
})();
