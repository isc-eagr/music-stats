/**
 * Common List Page Utilities
 * Shared JavaScript functions for list pages (pagination, filters, etc.)
 */

if (window.matchMedia && window.matchMedia('(max-width: 768px)').matches) {
    document.documentElement.classList.add('mobile-sort-init-pending');
}

function setMobileSortReadyState() {
    document.documentElement.classList.remove('mobile-sort-init-pending');
    document.documentElement.classList.add('mobile-sort-init-ready');
}

/**
 * Navigate to a specific page number.
 * Updates the 'page' URL parameter and reloads.
 * @param {number} pageNum - The zero-based page number to navigate to
 */
function goToPage(pageNum) {
    const url = new URL(window.location);
    url.searchParams.set('page', pageNum);
    window.location.href = url.toString();
}

/**
 * Remove a filter group from the URL and reload.
 * Handles both single parameters and mode suffixes.
 * @param {string} paramName - The base parameter name to remove (e.g., 'genre', 'ethnicity')
 */
function removeFilterGroup(paramName) {
    const url = new URL(window.location);
    url.searchParams.delete(paramName);
    url.searchParams.delete(paramName + 'Mode');
    // Reset to page 0 when filters change
    url.searchParams.set('page', '0');
    window.location.href = url.toString();
}

/**
 * Remove a range filter group (min/max parameters).
 * @param {string} baseName - The base parameter name (e.g., 'artistCount' for artistCountMin/artistCountMax)
 */
function removeRangeFilter(baseName) {
    const url = new URL(window.location);
    url.searchParams.delete(baseName + 'Min');
    url.searchParams.delete(baseName + 'Max');
    url.searchParams.set('page', '0');
    window.location.href = url.toString();
}

/**
 * Remove a date filter group (single, from, to, mode parameters).
 * @param {string} baseName - The base parameter name (e.g., 'releaseDate')
 */
function removeDateFilter(baseName) {
    const url = new URL(window.location);
    url.searchParams.delete(baseName);
    url.searchParams.delete(baseName + 'From');
    url.searchParams.delete(baseName + 'To');
    url.searchParams.delete(baseName + 'Mode');
    url.searchParams.set('page', '0');
    window.location.href = url.toString();
}

/**
 * Clear all filters from the URL and reload.
 * Preserves sortby, sortdir, and perpage parameters.
 */
function clearAllFilters() {
    const url = new URL(window.location);
    const sortBy = url.searchParams.get('sortby');
    const sortDir = url.searchParams.get('sortdir');
    const sortBy2 = url.searchParams.get('sortby2');
    const sortDir2 = url.searchParams.get('sortdir2');
    const sortBy3 = url.searchParams.get('sortby3');
    const sortDir3 = url.searchParams.get('sortdir3');
    const perPage = url.searchParams.get('perpage');
    
    // Clear all search params
    url.search = '';
    
    // Restore sort/pagination preferences
    if (sortBy) url.searchParams.set('sortby', sortBy);
    if (sortDir) url.searchParams.set('sortdir', sortDir);
    if (sortBy2) {
        url.searchParams.set('sortby2', sortBy2);
        if (sortDir2) url.searchParams.set('sortdir2', sortDir2);
    }
    if (sortBy3) {
        url.searchParams.set('sortby3', sortBy3);
        if (sortDir3) url.searchParams.set('sortdir3', sortDir3);
    }
    if (perPage) url.searchParams.set('perpage', perPage);
    url.searchParams.set('page', '0');
    
    window.location.href = url.toString();
}

/**
 * Update the per-page setting and reload.
 * @param {number} newPerPage - The new number of items per page
 */
function changePerPage(newPerPage) {
    const url = new URL(window.location);
    url.searchParams.set('perpage', newPerPage);
    url.searchParams.set('page', '0'); // Reset to first page
    window.location.href = url.toString();
}

function ensureHiddenInput(form, name) {
    let input = form.querySelector(`input[type="hidden"][name="${name}"]`);
    if (!input) {
        input = document.createElement('input');
        input.type = 'hidden';
        input.name = name;
        form.appendChild(input);
    }
    return input;
}

function getSortParamName(level) {
    return level === 1 ? 'sortby' : 'sortby' + level;
}

function getSortDirParamName(level) {
    return level === 1 ? 'sortdir' : 'sortdir' + level;
}

function getSortSelectId(level) {
    return level === 1 ? 'sortBySelect' : 'sortBySelect' + level;
}

function getSortButtonId(level) {
    return level === 1 ? 'sortDirBtn' : 'sortDirBtn' + level;
}

function updateSortButton(level, direction) {
    const button = document.getElementById(getSortButtonId(level));
    if (!button) {
        return;
    }

    const normalizedDirection = direction === 'desc' ? 'desc' : 'asc';
    button.title = normalizedDirection === 'desc'
        ? 'Descending (click to change)'
        : 'Ascending (click to change)';
    button.innerHTML = normalizedDirection === 'desc' ? '<span>↓</span>' : '<span>↑</span>';
}

function initializeAdditionalSortSelects() {
    const primarySelect = document.getElementById('sortBySelect');
    if (!primarySelect) {
        return;
    }

    [2, 3].forEach(function(level) {
        const select = document.getElementById(getSortSelectId(level));
        if (!select || select.dataset.initialized === 'true') {
            return;
        }

        const selectedValue = select.dataset.selectedSort || '';
        const placeholder = document.createElement('option');
        placeholder.value = '';
        placeholder.textContent = 'None';
        select.appendChild(placeholder);

        Array.from(primarySelect.options).forEach(function(option) {
            const clone = option.cloneNode(true);
            select.appendChild(clone);
        });

        select.value = selectedValue;
        select.dataset.initialized = 'true';
    });

    [1, 2, 3].forEach(function(level) {
        const dirParam = new URL(window.location.href).searchParams.get(getSortDirParamName(level)) || 'asc';
        updateSortButton(level, dirParam);
    });
}

function isCompactMobileSortViewport() {
    return window.matchMedia('(max-width: 768px)').matches;
}

function getSortRows(panel) {
    return Array.from(panel.querySelectorAll('.toolbar-sort-row'));
}

function getSortLevelFromRow(row, fallbackLevel) {
    const select = row.querySelector('select[id^="sortBySelect"]');
    if (select) {
        const match = select.id.match(/sortBySelect(\d+)?$/);
        if (match) {
            return match[1] ? parseInt(match[1], 10) : 1;
        }
    }
    return fallbackLevel;
}

function getSortDirectionForLevel(level) {
    const currentUrl = new URL(window.location.href);
    return currentUrl.searchParams.get(getSortDirParamName(level)) === 'desc' ? 'desc' : 'asc';
}

function getSortOptionText(select) {
    if (!select) {
        return '';
    }

    const selectedOption = select.options[select.selectedIndex];
    return selectedOption ? selectedOption.textContent.trim() : '';
}

function buildMobileSortSummary(panel) {
    const fragments = getSortRows(panel).map(function(row, index) {
        const select = row.querySelector('select');
        if (!select) {
            return '';
        }

        if (index > 0 && !select.value) {
            return '';
        }

        const optionText = getSortOptionText(select);
        if (!optionText) {
            return '';
        }

        const level = getSortLevelFromRow(row, index + 1);
        const direction = getSortDirectionForLevel(level) === 'desc' ? '↓' : '↑';
        return optionText + ' ' + direction;
    }).filter(Boolean);

    return fragments.length ? fragments.join(', ') : 'No sort';
}

function createMobileSortActionButton(text, onClick) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'toolbar-sort-mobile-add-btn';
    button.textContent = text;
    button.addEventListener('click', onClick);
    return button;
}

function updateMobileSortPanel(panel) {
    const summaryButton = panel.querySelector('.toolbar-sort-mobile-summary');
    const summaryValue = panel.querySelector('.toolbar-sort-mobile-summary-value');
    const additions = panel.querySelector('.toolbar-sort-mobile-additions');
    const isExpanded = panel.dataset.mobileSortExpanded === 'true';
    const rows = getSortRows(panel);

    if (summaryButton) {
        summaryButton.setAttribute('aria-expanded', isExpanded ? 'true' : 'false');
    }
    if (summaryValue) {
        summaryValue.textContent = buildMobileSortSummary(panel);
    }

    rows.forEach(function(row, index) {
        if (index === 0) {
            row.classList.remove('mobile-sort-row-hidden');
            return;
        }

        const select = row.querySelector('select');
        const hasValue = Boolean(select && select.value);
        const isRevealed = row.dataset.mobileSortRevealed === 'true';
        row.classList.toggle('mobile-sort-row-hidden', !hasValue && !isRevealed);
    });

    if (!additions) {
        return;
    }

    additions.innerHTML = '';

    if (rows.length > 1) {
        const secondaryRow = rows[1];
        const secondarySelect = secondaryRow.querySelector('select');
        const secondaryVisible = !secondaryRow.classList.contains('mobile-sort-row-hidden');
        const secondaryHasValue = Boolean(secondarySelect && secondarySelect.value);

        if (!secondaryVisible) {
            additions.appendChild(createMobileSortActionButton('Add secondary sort', function() {
                secondaryRow.dataset.mobileSortRevealed = 'true';
                updateMobileSortPanel(panel);
            }));
        }

        if (rows.length > 2) {
            const tertiaryRow = rows[2];
            const tertiaryVisible = !tertiaryRow.classList.contains('mobile-sort-row-hidden');

            if (secondaryHasValue && !tertiaryVisible) {
                additions.appendChild(createMobileSortActionButton('Add tertiary sort', function() {
                    tertiaryRow.dataset.mobileSortRevealed = 'true';
                    updateMobileSortPanel(panel);
                }));
            }
        }
    }

    additions.hidden = additions.childElementCount === 0;
}

function initializeMobileSortPanels() {
    document.querySelectorAll('.toolbar-sort-panel').forEach(function(panel) {
        if (panel.dataset.mobileSortEnhanced === 'true') {
            updateMobileSortPanel(panel);
            return;
        }

        const sortList = panel.querySelector('.toolbar-sort-list');
        if (!sortList) {
            return;
        }

        const summaryButton = document.createElement('button');
        summaryButton.type = 'button';
        summaryButton.className = 'toolbar-sort-mobile-summary';
        summaryButton.setAttribute('aria-expanded', 'false');
        summaryButton.innerHTML = '<span class="toolbar-sort-mobile-summary-label">Sort</span>' +
            '<span class="toolbar-sort-mobile-summary-value"></span>' +
            '<span class="toolbar-sort-mobile-summary-chevron" aria-hidden="true"></span>';
        summaryButton.addEventListener('click', function() {
            if (!isCompactMobileSortViewport()) {
                return;
            }

            panel.dataset.mobileSortExpanded = panel.dataset.mobileSortExpanded === 'true' ? 'false' : 'true';
            panel.classList.toggle('mobile-sort-expanded', panel.dataset.mobileSortExpanded === 'true');
            updateMobileSortPanel(panel);
        });

        panel.insertBefore(summaryButton, sortList);

        const additions = document.createElement('div');
        additions.className = 'toolbar-sort-mobile-additions';
        panel.appendChild(additions);

        getSortRows(panel).forEach(function(row, index) {
            if (index === 0) {
                row.dataset.mobileSortRevealed = 'true';
                return;
            }

            const select = row.querySelector('select');
            row.dataset.mobileSortRevealed = select && select.value ? 'true' : 'false';
        });

        panel.dataset.mobileSortEnhanced = 'true';
        panel.classList.add('mobile-sort-enhanced');
        panel.dataset.mobileSortExpanded = 'false';
        panel.classList.remove('mobile-sort-expanded');
        updateMobileSortPanel(panel);
    });

    setMobileSortReadyState();
}

function initializeMobileToolbarActions() {
    const isMobile = window.matchMedia('(max-width: 768px)').matches;
    const navContainer = document.querySelector('.app-nav-container');
    let navActions = document.getElementById('mobilePageActions');

    if (!navActions && navContainer) {
        navActions = document.createElement('div');
        navActions.id = 'mobilePageActions';
        navActions.hidden = true;
        navContainer.appendChild(navActions);
    }

    document.querySelectorAll('.catalog-toolbar').forEach(function(toolbar) {
        const actions = toolbar.querySelector('.catalog-toolbar-actions');
        const searchRow = toolbar.querySelector('.catalog-toolbar-search-row');
        const filterButton = toolbar.querySelector('.filter-toggle');
        const primaryAction = actions ? actions.querySelector('.toolbar-btn-primary') : null;

        if (!actions) {
            return;
        }

        let anchor = toolbar.querySelector('.mobile-filter-toggle-anchor');
        if (filterButton && searchRow && !anchor) {
            anchor = document.createElement('span');
            anchor.className = 'mobile-filter-toggle-anchor';
            anchor.hidden = true;
            searchRow.insertBefore(anchor, filterButton);
        }

        let actionAnchor = toolbar.querySelector('.mobile-primary-action-anchor');
        if (primaryAction && !actionAnchor) {
            actionAnchor = document.createElement('span');
            actionAnchor.className = 'mobile-primary-action-anchor';
            actionAnchor.hidden = true;
            actions.insertBefore(actionAnchor, primaryAction);
        }

        if (isMobile && navActions) {
            if (filterButton && filterButton.parentElement !== navActions) {
                navActions.appendChild(filterButton);
            }

            if (primaryAction && primaryAction.parentElement !== navActions) {
                navActions.appendChild(primaryAction);
            }

            navActions.hidden = navActions.childElementCount === 0;
            actions.classList.add('mobile-actions-stashed');
            return;
        }

        actions.classList.remove('mobile-actions-stashed');

        if (primaryAction && actionAnchor && (primaryAction.parentElement !== actions || primaryAction.previousElementSibling !== actionAnchor)) {
            actionAnchor.insertAdjacentElement('afterend', primaryAction);
        }

        if (filterButton && searchRow && anchor && (filterButton.parentElement !== searchRow || filterButton.previousElementSibling !== anchor)) {
            anchor.insertAdjacentElement('afterend', filterButton);
        }
    });

    if (navActions && !isMobile) {
        navActions.hidden = true;
    }
}

function changeListSort(level, sortValue) {
    const url = new URL(window.location.href);
    const sortParam = getSortParamName(level);
    const dirParam = getSortDirParamName(level);

    if (sortValue) {
        url.searchParams.set(sortParam, sortValue);
        if (!url.searchParams.get(dirParam)) {
            url.searchParams.set(dirParam, 'asc');
        }
    } else {
        url.searchParams.delete(sortParam);
        url.searchParams.delete(dirParam);
    }

    url.searchParams.set('page', '0');
    window.location.href = url.toString();
}

function toggleListSortDirection(level) {
    const url = new URL(window.location.href);
    const sortParam = getSortParamName(level);
    const dirParam = getSortDirParamName(level);

    if (!url.searchParams.get(sortParam)) {
        const select = document.getElementById(getSortSelectId(level));
        if (!select || !select.value) {
            return;
        }
        url.searchParams.set(sortParam, select.value);
    }

    const currentDir = url.searchParams.get(dirParam) || 'asc';
    url.searchParams.set(dirParam, currentDir === 'asc' ? 'desc' : 'asc');
    url.searchParams.set('page', '0');
    window.location.href = url.toString();
}

function buildSortOnlyUrl(basePath) {
    const url = new URL(basePath, window.location.origin);
    const currentUrl = new URL(window.location.href);

    [1, 2, 3].forEach(function(level) {
        const sortParam = getSortParamName(level);
        const dirParam = getSortDirParamName(level);
        const sortValue = currentUrl.searchParams.get(sortParam);
        const dirValue = currentUrl.searchParams.get(dirParam);
        if (sortValue) {
            url.searchParams.set(sortParam, sortValue);
            if (dirValue) {
                url.searchParams.set(dirParam, dirValue);
            }
        }
    });

    return url.toString();
}

function hasMeaningfulFilterValue(nameToValues, baseName) {
    if (nameToValues.has(baseName)) {
        return true;
    }

    const relatedSuffixes = ['Min', 'Max', 'From', 'To', 'Years', 'Weeks', 'Seasons'];
    return relatedSuffixes.some(function(suffix) {
        return nameToValues.has(baseName + suffix);
    });
}

function buildCleanFilterParams(form) {
    const rawEntries = Array.from(new FormData(form).entries()).map(function(entry) {
        const value = typeof entry[1] === 'string' ? entry[1].trim() : entry[1];
        return [entry[0], value];
    }).filter(function(entry) {
        return !(typeof entry[1] === 'string' && entry[1] === '');
    });

    const nameToValues = new Map();
    rawEntries.forEach(function(entry) {
        const name = entry[0];
        if (!nameToValues.has(name)) {
            nameToValues.set(name, []);
        }
        nameToValues.get(name).push(entry[1]);
    });

    const params = new URLSearchParams();
    rawEntries.forEach(function(entry) {
        const name = entry[0];
        const value = entry[1];

        if (name.endsWith('Mode')) {
            const baseName = name.slice(0, -4);
            const keepsItself = value === 'isnull' || value === 'isnotnull';
            if (!keepsItself && !hasMeaningfulFilterValue(nameToValues, baseName)) {
                return;
            }
        }

        if (name === 'page' && value === '0') {
            return;
        }

        if ((name === 'sortdir2' && !nameToValues.has('sortby2')) ||
                (name === 'sortdir3' && !nameToValues.has('sortby3'))) {
            return;
        }

        params.append(name, value);
    });

    return params;
}

function preserveListStateOnFilterSubmit(form) {
    if (!form || form.dataset.listStateBound === 'true') {
        return;
    }

    form.dataset.listStateBound = 'true';
    form.addEventListener('submit', function(event) {
        const url = new URL(window.location.href);
        const perPageInput = ensureHiddenInput(form, 'perpage');
        const pageInput = ensureHiddenInput(form, 'page');
        const pageSizeInput = document.getElementById('pageSizeInput');

        [1, 2, 3].forEach(function(level) {
            const sortParam = getSortParamName(level);
            const dirParam = getSortDirParamName(level);
            const sortInput = ensureHiddenInput(form, sortParam);
            const dirInput = ensureHiddenInput(form, dirParam);
            const sortSelect = document.getElementById(getSortSelectId(level));

            sortInput.value = sortSelect ? sortSelect.value : (url.searchParams.get(sortParam) || sortInput.value || '');
            dirInput.value = sortInput.value ? (url.searchParams.get(dirParam) || dirInput.value || 'asc') : '';
        });
        perPageInput.value = pageSizeInput ? pageSizeInput.value : (url.searchParams.get('perpage') || perPageInput.value || '');
        pageInput.value = '0';

        if ((form.getAttribute('method') || 'get').toLowerCase() !== 'get') {
            return;
        }

        event.preventDefault();
        const destination = new URL(form.getAttribute('action') || window.location.pathname, window.location.origin);
        destination.search = buildCleanFilterParams(form).toString();
        window.location.href = destination.toString();
    });
}

function submitFilterForm(formOrId) {
    const form = typeof formOrId === 'string'
        ? document.getElementById(formOrId)
        : formOrId;
    if (!form) {
        return;
    }

    if (typeof form.requestSubmit === 'function') {
        form.requestSubmit();
        return;
    }

    const destination = new URL(form.getAttribute('action') || window.location.pathname, window.location.origin);
    destination.search = buildCleanFilterParams(form).toString();
    window.location.href = destination.toString();
}

/**
 * Toggle sort direction for a column.
 * @param {string} column - The column name to sort by
 */
function toggleSort(column) {
    const url = new URL(window.location);
    const currentSort = url.searchParams.get('sortby');
    const currentDir = url.searchParams.get('sortdir') || 'asc';
    
    if (currentSort === column) {
        // Toggle direction
        url.searchParams.set('sortdir', currentDir === 'asc' ? 'desc' : 'asc');
    } else {
        // New column, default to asc (or desc for numeric columns)
        url.searchParams.set('sortby', column);
        url.searchParams.set('sortdir', 'asc');
    }
    
    url.searchParams.set('page', '0');
    window.location.href = url.toString();
}

/**
 * Format a duration in seconds to a human-readable string.
 * @param {number} totalSeconds - Duration in seconds
 * @returns {string} Formatted duration (e.g., "2d 5h 30m", "3h 15m", "45m")
 */
function formatDuration(totalSeconds) {
    if (!totalSeconds || totalSeconds <= 0) return '0m';
    
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    
    if (days > 0) {
        return days + 'd ' + hours + 'h ' + minutes + 'm';
    } else if (hours > 0) {
        return hours + 'h ' + minutes + 'm';
    } else {
        return minutes + 'm';
    }
}

/**
 * Format a song length in seconds to mm:ss format.
 * @param {number} seconds - Duration in seconds
 * @returns {string} Formatted duration (e.g., "3:45")
 */
function formatSongLength(seconds) {
    if (!seconds || seconds <= 0) return '-';
    
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return mins + ':' + (secs < 10 ? '0' : '') + secs;
}

/**
 * Format seconds to a human-readable time string (hh:mm:ss with optional leading parts).
 * Examples: 4 -> "4", 107 -> "1:47", 3661 -> "1:01:01"
 * @param {number} totalSeconds - Total duration in seconds
 * @returns {string} Formatted time string
 */
function formatLengthFilter(totalSeconds) {
    if (totalSeconds == null || totalSeconds < 0) return '';
    totalSeconds = Math.floor(totalSeconds);
    if (totalSeconds === 0) return '0';
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    if (hours > 0) {
        return hours + ':' + (minutes < 10 ? '0' : '') + minutes + ':' + (seconds < 10 ? '0' : '') + seconds;
    } else if (minutes > 0) {
        return minutes + ':' + (seconds < 10 ? '0' : '') + seconds;
    } else {
        return '' + seconds;
    }
}

/**
 * Parse a time string (hh:mm:ss, mm:ss, or ss) to total seconds.
 * Accepts formats: "4" (4 sec), "1:47" (1 min 47 sec), "1:01:01" (1 hr 1 min 1 sec).
 * Non-significant zeros are optional (e.g., "1:47" instead of "01:47").
 * @param {string} str - The time string to parse
 * @returns {number|null} Total seconds, or null if invalid
 */
function parseLengthFilter(str) {
    if (!str || str.trim() === '') return null;
    str = str.trim();
    const parts = str.split(':');
    if (parts.length > 3) return null;
    for (let i = 0; i < parts.length; i++) {
        if (!/^\d+$/.test(parts[i])) return null;
    }
    if (parts.length === 1) {
        return parseInt(parts[0], 10);
    } else if (parts.length === 2) {
        return parseInt(parts[0], 10) * 60 + parseInt(parts[1], 10);
    } else {
        return parseInt(parts[0], 10) * 3600 + parseInt(parts[1], 10) * 60 + parseInt(parts[2], 10);
    }
}

/**
 * Initialize length filter inputs with display formatting.
 * Converts hidden second values to hh:mm:ss display in text inputs,
 * and converts back to seconds on form submit.
 * @param {string} formId - The form element ID
 */
function initLengthFilter(formId) {
    const form = document.getElementById(formId);
    if (!form) return;
    const minHidden = form.querySelector('input[name="lengthMin"]');
    const maxHidden = form.querySelector('input[name="lengthMax"]');
    const minDisplay = document.getElementById('lengthMinDisplay');
    const maxDisplay = document.getElementById('lengthMaxDisplay');
    if (!minDisplay || !maxDisplay) return;

    // On page load, format seconds to display
    if (minHidden && minHidden.value) {
        minDisplay.value = formatLengthFilter(parseInt(minHidden.value, 10));
    }
    if (maxHidden && maxHidden.value) {
        maxDisplay.value = formatLengthFilter(parseInt(maxHidden.value, 10));
    }

    // On form submit, convert display to seconds
    form.addEventListener('submit', function() {
        if (minDisplay.value.trim()) {
            const parsed = parseLengthFilter(minDisplay.value);
            if (parsed !== null && minHidden) minHidden.value = parsed;
        } else {
            if (minHidden) minHidden.value = '';
        }
        if (maxDisplay.value.trim()) {
            const parsed = parseLengthFilter(maxDisplay.value);
            if (parsed !== null && maxHidden) maxHidden.value = parsed;
        } else {
            if (maxHidden) maxHidden.value = '';
        }
    });
}

/**
 * Debounce a function to limit how often it can be called.
 * @param {Function} func - The function to debounce
 * @param {number} wait - The wait time in milliseconds
 * @returns {Function} The debounced function
 */
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

/**
 * Navigate to the songs list page with a filter applied.
 * Used by catalog cards (genres, ethnicities, languages, subgenres).
 * @param {HTMLElement} element - The element containing data-id and data-name attributes
 * @param {string} filterType - The type of filter (genre, ethnicity, language, subgenre)
 */
function navigateToGraph(element, filterType) {
    const id = element.dataset.id;
    window.location.href = '/songs?' + filterType + '=' + encodeURIComponent(id) + '&' + filterType + 'Mode=includes';
}

function switchCatalogView(viewName) {
    const normalizedView = viewName === 'table' ? 'table' : 'card';
    const cardView = document.getElementById('cardView');
    const tableView = document.getElementById('tableView');

    if (cardView) cardView.style.display = normalizedView === 'card' ? '' : 'none';
    if (tableView) tableView.style.display = normalizedView === 'table' ? '' : 'none';

    ['card', 'table'].forEach(function(view) {
        const button = document.getElementById('viewBtn-' + view);
        if (button) button.classList.toggle('active', view === normalizedView);
    });

    const url = new URL(window.location.href);
    if (normalizedView === 'table') {
        url.searchParams.set('view', 'table');
    } else {
        url.searchParams.delete('view');
    }
    window.history.replaceState({}, '', url.toString());
}

function sortCatalogList(sortKey, defaultDir) {
    if (!sortKey) return;

    const url = new URL(window.location.href);
    const currentSort = url.searchParams.get('sortby') || '';
    const currentDir = url.searchParams.get('sortdir') || '';
    const firstDir = defaultDir || (sortKey === 'name' || sortKey === 'genre' ? 'asc' : 'desc');

    url.searchParams.set('sortby', sortKey);
    url.searchParams.set('sortdir', currentSort === sortKey ? (currentDir === 'asc' ? 'desc' : 'asc') : firstDir);
    url.searchParams.set('page', '0');
    url.searchParams.set('view', 'table');
    window.location.href = url.toString();
}

function navigateCatalogListRow(row) {
    const directUrl = row.dataset.directUrl;
    if (directUrl) {
        window.location.href = directUrl;
        return;
    }

    const filterType = row.dataset.filterType;
    const filterValue = row.dataset.filterValue;
    if (!filterType || !filterValue) return;

    window.location.href = '/songs?'
        + filterType + '=' + encodeURIComponent(filterValue)
        + '&' + filterType + 'Mode=includes';
}

function initializeCatalogListView() {
    if (!document.getElementById('catalogViewToggle')) {
        return;
    }

    const requestedView = new URLSearchParams(window.location.search).get('view');
    switchCatalogView(requestedView === 'table' ? 'table' : 'card');

    document.querySelectorAll('#catalogListTable th[data-sort-key]').forEach(function(header) {
        header.addEventListener('click', function() {
            sortCatalogList(this.getAttribute('data-sort-key'), this.getAttribute('data-default-dir'));
        });
    });

    const table = document.getElementById('catalogListTable');
    if (table) {
        table.addEventListener('click', function(event) {
            if (event.target.closest('a, button, input, select, textarea')) {
                return;
            }
            const row = event.target.closest('.catalog-list-row');
            if (row) navigateCatalogListRow(row);
        });
    }
}

document.addEventListener('DOMContentLoaded', function() {
    initializeAdditionalSortSelects();
    initializeMobileToolbarActions();
    initializeMobileSortPanels();
    initializeCatalogListView();
    preserveListStateOnFilterSubmit(document.getElementById('filterForm'));

    window.addEventListener('resize', debounce(function() {
        initializeMobileToolbarActions();
        initializeMobileSortPanels();
    }, 80));
});

/**
 * Toggle the expanded stats section of a card (used on detail pages).
 * @param {HTMLElement} btn - The toggle button element
 */
function toggleCardExpand(btn) {
    btn.classList.toggle('expanded');
    const stats = btn.nextElementSibling;
    if (stats) {
        stats.classList.toggle('visible');
    }
}

/**
 * Open a modal showing the full card with all stats.
 * Used on list pages as a replacement for inline expand.
 * @param {HTMLElement} btn - The "More" toggle button element inside the card
 */
function openCardModal(btn) {
    const card = btn.closest('.artist-card, .album-card, .song-card');
    if (!card) return;

    if (window.innerWidth <= 768) {
        card.classList.toggle('mobile-expanded');
        return;
    }

    let modal = document.getElementById('cardDetailModal');
    if (!modal) return;

    const clone = card.cloneNode(true);
    // Remove IDs from clone to prevent conflicts
    clone.querySelectorAll('[id]').forEach(function(el) { el.removeAttribute('id'); });

    const body = modal.querySelector('.card-detail-modal-body');
    body.innerHTML = '';
    body.appendChild(clone);

    // Lazy-load Last Full Listen Date for album cards
    const albumId = card.getAttribute('data-album-id');
    if (albumId) {
        const lflSpan = clone.querySelector('.last-full-listen-value');
        if (lflSpan) {
            fetch('/albums/api/' + albumId + '/last-full-listen')
                .then(function(r) { return r.json(); })
                .then(function(data) {
                    lflSpan.textContent = data.lastFullListenDate || 'N/A';
                })
                .catch(function() { lflSpan.textContent = 'N/A'; });
        }
    }

    modal.classList.add('show');
    document.body.style.overflow = 'hidden';
}

// Add click listener for mobile cards to expand inline
document.addEventListener('click', function(e) {
    if (window.innerWidth <= 768) {
        const card = e.target.closest('.artist-card, .album-card, .song-card');
        // Only expand if clicking the card itself, not a link or the expand toggle
        if (card && !e.target.closest('a') && !e.target.closest('.card-expand-toggle') && !e.target.closest('.clickable-image')) {
            card.classList.toggle('mobile-expanded');
        }
    }
});

/**
 * Close the card detail modal.
 */
function closeCardModal() {
    var modal = document.getElementById('cardDetailModal');
    if (modal) {
        modal.classList.remove('show');
        document.body.style.overflow = '';
    }
}

// Close card modal on Escape key
document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
        closeCardModal();
    }
});

/**
 * Initialize search form to preserve filters when searching.
 * Call this on page load to set up the search form interception.
 * @param {string} formId - The ID of the search form (e.g., 'artistSearchForm')
 */
function initializeSearchFormWithFilters(formId) {
    const form = document.getElementById(formId);
    if (!form) {
        console.warn('Search form not found:', formId);
        return;
    }
    
    const searchInput = form.querySelector('input[name="q"]');
    if (!searchInput) {
        console.warn('Search input not found in form:', formId);
        return;
    }
    
    // Handle form submit
    form.addEventListener('submit', function(e) {
        e.preventDefault();
        performSearchWithFilters(searchInput.value);
    });
    
    // Also handle Enter key press on the input
    searchInput.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            performSearchWithFilters(searchInput.value);
        }
    });
}

function cancelPendingSearchNavigation() {
    if (!window.__listSearchNavigationPending) {
        return;
    }

    if (typeof window.stop === 'function') {
        window.stop();
    }

    window.__listSearchNavigationPending = false;
}

/**
 * Perform search while preserving current URL filters.
 * @param {string} searchQuery - The search query string
 */
function performSearchWithFilters(searchQuery) {
    cancelPendingSearchNavigation();

    // Get current URL parameters
    const currentUrl = new URL(window.location);
    
    // Update or set the search query
    if (searchQuery && searchQuery.trim() !== '') {
        currentUrl.searchParams.set('q', searchQuery.trim());
    } else {
        currentUrl.searchParams.delete('q');
    }
    
    // Reset to page 0 when searching
    currentUrl.searchParams.set('page', '0');
    
    // Navigate with all preserved parameters
    window.__listSearchNavigationPending = true;
    window.location.href = currentUrl.toString();
}

/**
 * Handle sticky sidebar logic for detail pages.
 * Makes the sidebar stick to the bottom if it's taller than the viewport.
 */
document.addEventListener('DOMContentLoaded', () => {
    const sidebar = document.querySelector('.detail-sidebar');
    if (!sidebar) return;

    function updateSidebarSticky() {
        const viewportHeight = window.innerHeight;
        const sidebarHeight = sidebar.offsetHeight;
        
        if (sidebarHeight + 40 > viewportHeight) {
            sidebar.style.top = (viewportHeight - sidebarHeight - 20) + 'px';
        } else {
            sidebar.style.top = '20px';
        }
    }

    window.addEventListener('resize', updateSidebarSticky);
    
    // Use ResizeObserver if available
    if (window.ResizeObserver) {
        const observer = new ResizeObserver(updateSidebarSticky);
        observer.observe(sidebar);
    }
    
    // Also update if images load
    const images = sidebar.querySelectorAll('img');
    images.forEach(img => {
        if (!img.complete) {
            img.addEventListener('load', updateSidebarSticky);
        }
    });

    updateSidebarSticky();
});

