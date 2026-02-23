/**
 * Common List Page Utilities
 * Shared JavaScript functions for list pages (pagination, filters, etc.)
 */

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
    const perPage = url.searchParams.get('perpage');
    
    // Clear all search params
    url.search = '';
    
    // Restore sort/pagination preferences
    if (sortBy) url.searchParams.set('sortby', sortBy);
    if (sortDir) url.searchParams.set('sortdir', sortDir);
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
 * Navigate to the graphs page with a filter applied.
 * Used by catalog cards (genres, ethnicities, languages, subgenres).
 * @param {HTMLElement} element - The element containing data-id and data-name attributes
 * @param {string} filterType - The type of filter (genre, ethnicity, language, subgenre)
 */
function navigateToGraph(element, filterType) {
    const id = element.dataset.id;
    const name = encodeURIComponent(element.dataset.name);
    window.location.href = '/graphs?filterType=' + filterType + '&filterId=' + id + '&filterName=' + name;
}

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

/**
 * Perform search while preserving current URL filters.
 * @param {string} searchQuery - The search query string
 */
function performSearchWithFilters(searchQuery) {
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
    window.location.href = currentUrl.toString();
}
