(function () {
    let activeTooltip = null;
    let activeTrigger = null;

    function escapeHtml(value) {
        return String(value ?? '').replace(/[&<>"']/g, function (character) {
            return {
                '&': '&amp;',
                '<': '&lt;',
                '>': '&gt;',
                '"': '&quot;',
                "'": '&#39;'
            }[character];
        });
    }

    function ensureTooltip() {
        if (!activeTooltip) {
            activeTooltip = document.createElement('div');
            activeTooltip.className = 'linked-song-tooltip';
            document.body.appendChild(activeTooltip);
        }
        return activeTooltip;
    }

    function positionTooltip(trigger) {
        const tooltip = ensureTooltip();
        const rect = trigger.getBoundingClientRect();
        tooltip.style.left = rect.left + 'px';
        tooltip.style.top = Math.max(8, rect.top - tooltip.offsetHeight - 8) + 'px';
    }

    function showTooltip(trigger) {
        const items = Array.from(trigger.querySelectorAll('.linked-song-tooltip-item-source'))
            .map((item) => item.textContent.trim())
            .filter(Boolean);
        if (!items.length) {
            return;
        }

        const tooltip = ensureTooltip();
        tooltip.innerHTML = `<div class="tooltip-title">${escapeHtml(trigger.dataset.tooltipTitle || 'Combined song versions:')}</div>`
            + items.map((item) => `<div class="tooltip-song">${escapeHtml(item)}</div>`).join('');
        tooltip.style.display = 'block';
        activeTrigger = trigger;
        positionTooltip(trigger);
    }

    function hideTooltip(trigger) {
        ensureTooltip().style.display = 'none';
        if (!trigger || activeTrigger === trigger) {
            activeTrigger = null;
        }
    }

    function initialize(root = document) {
        root.querySelectorAll('.linked-song-tooltip-trigger.tooltip-enabled').forEach((trigger) => {
            if (trigger.dataset.tooltipBound === 'true') {
                return;
            }
            trigger.dataset.tooltipBound = 'true';
            trigger.addEventListener('mouseenter', () => showTooltip(trigger));
            trigger.addEventListener('mouseleave', () => hideTooltip(trigger));
            trigger.addEventListener('focusin', () => showTooltip(trigger));
            trigger.addEventListener('focusout', () => hideTooltip(trigger));
        });
    }

    window.initializeLinkedSongTooltips = initialize;
    window.buildLinkedSongTooltipTriggerHtml = function (contentHtml, items, title) {
        const safeItems = Array.isArray(items) ? items.filter(Boolean) : [];
        if (!safeItems.length) {
            return contentHtml;
        }
        return `<span class="linked-song-tooltip-trigger tooltip-enabled" data-tooltip-title="${escapeHtml(title || 'Combined song versions:')}">${contentHtml}${safeItems.map((item) => `<span class="linked-song-tooltip-item-source" hidden>${escapeHtml(item)}</span>`).join('')}</span>`;
    };

    window.addEventListener('scroll', () => {
        if (activeTrigger) {
            positionTooltip(activeTrigger);
        }
    }, true);
    window.addEventListener('resize', () => {
        if (activeTrigger) {
            positionTooltip(activeTrigger);
        }
    });
    document.addEventListener('DOMContentLoaded', () => initialize());
})();
