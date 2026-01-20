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
