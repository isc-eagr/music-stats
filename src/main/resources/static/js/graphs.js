/**
 * Global Charts Module
 * Shared chart functions for gender breakdown charts across all list pages
 */

// Chart.js instances storage
let chartInstances = {};

// Relative scaling mode - when true, bars are scaled relative to the max across all categories
let useRelativeScaling = false;

// Store data for re-rendering when scaling mode changes
let lastChartData = {};

// Track which tabs have been loaded
let loadedTabs = {
    general: false,
    top: false,
    genre: false,
    subgenre: false,
    ethnicity: false,
    language: false,
    country: false,
    releaseYear: false,
    listenYear: false
};

// Store top data for client-side sorting
let topTabData = {
    artists: [],
    albums: [],
    songs: []
};

// Current sort state for each table
let topSortState = {
    artists: { column: 'plays', direction: 'desc' },
    albums: { column: 'plays', direction: 'desc' },
    songs: { column: 'plays', direction: 'desc' }
};

// Current sort state for chart groups (genre, subgenre, etc.)
let chartSortMetric = 'plays';  // Options: 'artists', 'albums', 'songs', 'plays', 'listeningTime'

// Store stats tables data for re-rendering when sort changes
let lastStatsTablesData = {};

// Store current sort state for each stats table (per tab, per table)
let statsTableSortState = {};  // e.g., { 'genre': { 'Artists': { column: 'count', direction: 'desc' } } }

// Whether comparison mode is enabled (aligns all tables by the chart sort metric)
let statsComparisonMode = false;

// Global sort column for all tables (null = individual sorting, 'count' or 'malePercent' = global)
let statsGlobalSortColumn = 'count';

// Helper functions to read artist include toggles from the form checkboxes
function getArtistIncludeGroups() {
    const toggle = document.getElementById('includeGroupsToggle');
    return toggle ? toggle.checked : false;
}

function getArtistIncludeFeatured() {
    const toggle = document.getElementById('includeFeaturedToggle');
    return toggle ? toggle.checked : false;
}

// Register datalabels plugin if Chart.js is available
if (typeof Chart !== 'undefined' && typeof ChartDataLabels !== 'undefined') {
    Chart.register(ChartDataLabels);
}

/**
 * Format listening time in seconds to human-readable format
 * Matches the Java formatTime() method in SongService.java
 */
function formatListeningTime(totalSeconds) {
    if (!totalSeconds || totalSeconds === 0) {
        return '0m';
    }
    
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    
    if (days > 0) {
        return days + 'd ' + hours + 'h';
    } else if (hours > 0) {
        return hours + 'h ' + minutes + 'm';
    } else {
        return minutes + 'm';
    }
}

/**
 * Toggle stats tables visibility
 */
function toggleStatsTables(tabName) {
    const content = document.getElementById('statsTablesContent-' + tabName);
    const icon = document.getElementById('statsToggleIcon-' + tabName);
    
    if (content && icon) {
        const isExpanded = content.classList.contains('expanded');
        if (isExpanded) {
            content.classList.remove('expanded');
            icon.textContent = '+';
        } else {
            content.classList.add('expanded');
            icon.textContent = '−';
        }
    }
}

/**
 * Render stats breakdown tables for a category tab
 * @param {string} tabName - The tab name (genre, subgenre, etc.)
 * @param {object} data - The chart data containing arrays for each metric
 * @param {boolean} includeArtists - Whether to include artists table (false for releaseYear)
 */
function renderStatsTables(tabName, data, includeArtists = true) {
    const container = document.getElementById('statsTablesRow-' + tabName);
    if (!container) return;
    
    // Store data for re-rendering when sort changes
    lastStatsTablesData[tabName] = { data, includeArtists };
    
    // Initialize sort state for this tab if not exists
    if (!statsTableSortState[tabName]) {
        statsTableSortState[tabName] = {};
    }
    
    // Get the data arrays based on tab name
    const categoryKey = tabName.charAt(0).toUpperCase() + tabName.slice(1);
    const artistsData = includeArtists ? data['artistsBy' + categoryKey] : null;
    const albumsData = data['albumsBy' + categoryKey];
    const songsData = data['songsBy' + categoryKey];
    const playsData = data['playsBy' + categoryKey];
    const listeningTimeData = data['listeningTimeBy' + categoryKey];
    
    // Determine comparison sort order if comparison mode is enabled
    let comparisonOrder = null;
    if (statsComparisonMode) {
        const sortDataSource = getSortDataSource(chartSortMetric, artistsData, albumsData, songsData, playsData, listeningTimeData, includeArtists);
        if (sortDataSource && sortDataSource.length > 0) {
            comparisonOrder = sortDataSource
                .map(item => ({ name: item.name, total: (item.male || 0) + (item.female || 0) + (item.other || 0) }))
                .sort((a, b) => b.total - a.total)
                .map(item => item.name);
        }
    }
    
    // Build tables HTML
    let html = '';
    
    if (includeArtists && artistsData) {
        const isHighlighted = statsComparisonMode && chartSortMetric === 'artists';
        html += buildStatsTable(tabName, 'Artists', artistsData, false, comparisonOrder, isHighlighted);
    }
    if (albumsData) {
        const isHighlighted = statsComparisonMode && chartSortMetric === 'albums';
        html += buildStatsTable(tabName, 'Albums', albumsData, false, comparisonOrder, isHighlighted);
    }
    if (songsData) {
        const isHighlighted = statsComparisonMode && chartSortMetric === 'songs';
        html += buildStatsTable(tabName, 'Songs', songsData, false, comparisonOrder, isHighlighted);
    }
    if (playsData) {
        const isHighlighted = statsComparisonMode && chartSortMetric === 'plays';
        html += buildStatsTable(tabName, 'Plays', playsData, false, comparisonOrder, isHighlighted);
    }
    if (listeningTimeData) {
        const isHighlighted = statsComparisonMode && chartSortMetric === 'listeningTime';
        html += buildStatsTable(tabName, 'Listen Time', listeningTimeData, true, comparisonOrder, isHighlighted);
    }
    
    container.innerHTML = html;
}

/**
 * Get the data source to use for sorting based on the current metric
 */
function getSortDataSource(metric, artistsData, albumsData, songsData, playsData, listeningTimeData, includeArtists) {
    switch (metric) {
        case 'artists':
            return includeArtists && artistsData ? artistsData : playsData;
        case 'albums':
            return albumsData || playsData;
        case 'songs':
            return songsData || playsData;
        case 'plays':
            return playsData;
        case 'listeningTime':
            return listeningTimeData || playsData;
        default:
            return playsData;
    }
}

/**
 * Build a single stats table HTML
 * @param {string} tabName - The tab name for tracking sort state
 * @param {string} title - Table title (Artists, Albums, Songs, Plays, Listen Time)
 * @param {Array} data - Array of objects with name, male, female, other properties
 * @param {boolean} isTime - Whether values represent time (for formatting)
 * @param {Array} comparisonOrder - Array of names in the order they should appear (comparison mode only)
 * @param {boolean} isHighlighted - Whether this table should be highlighted as the comparison source
 */
function buildStatsTable(tabName, title, data, isTime, comparisonOrder = null, isHighlighted = false) {
    if (!data || data.length === 0) {
        return `<div class="stats-table-wrapper${isHighlighted ? ' stats-table-highlighted' : ''}"><h4>${title}</h4><div style="color:#666;font-size:11px;text-align:center;padding:20px;">No data</div></div>`;
    }
    
    // Get or initialize sort state for this table
    if (!statsTableSortState[tabName]) statsTableSortState[tabName] = {};
    if (!statsTableSortState[tabName][title]) {
        statsTableSortState[tabName][title] = { column: 'count', direction: 'desc' };
    }
    const sortState = statsTableSortState[tabName][title];
    
    // Determine effective sort column and direction
    // Global sort takes precedence over individual sort (when not in comparison mode)
    const effectiveColumn = (!statsComparisonMode && statsGlobalSortColumn) ? statsGlobalSortColumn : sortState.column;
    const effectiveDirection = (!statsComparisonMode && statsGlobalSortColumn) ? 'desc' : sortState.direction;
    
    // Prepare data with calculated totals and male percentage
    let processedData = data.map(row => {
        const total = (row.male || 0) + (row.female || 0) + (row.other || 0);
        return {
            ...row,
            total: total,
            malePercent: total > 0 ? ((row.male || 0) / total * 100) : 0
        };
    });
    
    // Calculate positions based on count (descending) - this is fixed regardless of current sort
    const countSortedData = [...processedData].sort((a, b) => b.total - a.total);
    const positionMap = new Map();
    countSortedData.forEach((row, idx) => {
        positionMap.set(row.name, idx + 1);
    });
    
    // Sort data
    let sortedData;
    if (statsComparisonMode && comparisonOrder) {
        // In comparison mode, use the comparison order
        const orderMap = new Map(comparisonOrder.map((name, idx) => [name, idx]));
        sortedData = [...processedData].sort((a, b) => {
            const idxA = orderMap.has(a.name) ? orderMap.get(a.name) : 9999;
            const idxB = orderMap.has(b.name) ? orderMap.get(b.name) : 9999;
            return idxA - idxB;
        });
    } else {
        // Normal mode - sort by the effective column
        sortedData = [...processedData].sort((a, b) => {
            let valA, valB;
            if (effectiveColumn === 'count') {
                valA = a.total;
                valB = b.total;
            } else if (effectiveColumn === 'male') {
                valA = a.male || 0;
                valB = b.male || 0;
            } else if (effectiveColumn === 'malePercent') {
                valA = a.malePercent;
                valB = b.malePercent;
            }
            return effectiveDirection === 'desc' ? valB - valA : valA - valB;
        });
    }
    
    // Calculate totals
    let totalCount = 0;
    let totalMale = 0;
    
    sortedData.forEach(row => {
        totalCount += row.total;
        totalMale += (row.male || 0);
    });
    
    // Determine sort indicator classes (show for both individual and global sort)
    const showSortIndicators = !statsComparisonMode;
    const countSortClass = showSortIndicators && effectiveColumn === 'count' ? ` sorted-${effectiveDirection}` : '';
    const maleSortClass = showSortIndicators && effectiveColumn === 'male' ? ` sorted-${effectiveDirection}` : '';
    const malePercentSortClass = showSortIndicators && effectiveColumn === 'malePercent' ? ` sorted-${effectiveDirection}` : '';
    
    // Build table rows
    let rowsHtml = '';
    sortedData.forEach(row => {
        const position = positionMap.get(row.name) || 0;
        const percentage = totalCount > 0 ? ((row.total / totalCount) * 100).toFixed(2) : '0.00';
        const malePercentage = row.malePercent.toFixed(2);
        
        const displayTotal = isTime ? formatListeningTime(row.total) : row.total.toLocaleString();
        const displayMale = isTime ? formatListeningTime(row.male || 0) : (row.male || 0).toLocaleString();
        
        // Add data attribute and hover handlers for cross-table highlighting
        const dataAttr = ` data-value-name="${escapeHtml(row.name)}"`;
        const hoverHandlers = ` onmouseenter="highlightCrossTableRows('${escapeHtml(row.name)}')" onmouseleave="clearCrossTableHighlight()"`;
        
        rowsHtml += `<tr${dataAttr}${hoverHandlers}>
            <td class="position-col">${position}</td>
            <td title="${escapeHtml(row.name)}">${escapeHtml(row.name)}</td>
            <td>${displayTotal}</td>
            <td>${percentage}%</td>
            <td class="male-col">${displayMale}</td>
            <td class="male-col">${malePercentage}%</td>
        </tr>`;
    });
    
    // Total row
    const displayTotalCount = isTime ? formatListeningTime(totalCount) : totalCount.toLocaleString();
    const displayTotalMale = isTime ? formatListeningTime(totalMale) : totalMale.toLocaleString();
    const totalMalePercent = totalCount > 0 ? ((totalMale / totalCount) * 100).toFixed(2) : '0.00';
    
    rowsHtml += `<tr>
        <td class="position-col"></td>
        <td>Total</td>
        <td>${displayTotalCount}</td>
        <td>100%</td>
        <td class="male-col">${displayTotalMale}</td>
        <td class="male-col">${totalMalePercent}%</td>
    </tr>`;
    
    const highlightClass = isHighlighted ? ' stats-table-highlighted' : '';
    const tableId = `statsTable-${tabName}-${title.replace(/\s+/g, '')}`;
    
    // Make headers clickable only when not in comparison mode and no global sort is active
    const canClickHeaders = !statsComparisonMode && !statsGlobalSortColumn;
    const countHeaderClick = canClickHeaders ? ` onclick="sortStatsTable('${tabName}', '${title}', 'count')" style="cursor:pointer;"` : '';
    const maleHeaderClick = canClickHeaders ? ` onclick="sortStatsTable('${tabName}', '${title}', 'male')" style="cursor:pointer;"` : '';
    const malePercentHeaderClick = canClickHeaders ? ` onclick="sortStatsTable('${tabName}', '${title}', 'malePercent')" style="cursor:pointer;"` : '';
    
    return `<div class="stats-table-wrapper${highlightClass}">
        <h4>${title}</h4>
        <table class="stats-table" id="${tableId}">
            <thead>
                <tr>
                    <th class="position-col">#</th>
                    <th>Value</th>
                    <th class="sortable-header${countSortClass}"${countHeaderClick}>Count</th>
                    <th>%</th>
                    <th class="male-col sortable-header${maleSortClass}"${maleHeaderClick}>♂</th>
                    <th class="male-col sortable-header${malePercentSortClass}"${malePercentHeaderClick}>♂ %</th>
                </tr>
            </thead>
            <tbody>${rowsHtml}</tbody>
        </table>
    </div>`;
}

/**
 * Sort a stats table by the specified column
 */
function sortStatsTable(tabName, tableTitle, column) {
    if (statsComparisonMode) return; // Don't allow individual sorting in comparison mode
    if (statsGlobalSortColumn) return; // Don't allow individual sorting when global sort is active
    
    if (!statsTableSortState[tabName]) statsTableSortState[tabName] = {};
    if (!statsTableSortState[tabName][tableTitle]) {
        statsTableSortState[tabName][tableTitle] = { column: 'count', direction: 'desc' };
    }
    
    const state = statsTableSortState[tabName][tableTitle];
    
    // Toggle direction if same column, otherwise set to desc
    if (state.column === column) {
        state.direction = state.direction === 'desc' ? 'asc' : 'desc';
    } else {
        state.column = column;
        state.direction = 'desc';
    }
    
    // Re-render the stats tables for this tab
    const stored = lastStatsTablesData[tabName];
    if (stored) {
        renderStatsTables(tabName, stored.data, stored.includeArtists);
    }
}

/**
 * Toggle comparison mode for stats tables
 */
function toggleStatsComparisonMode(tabName) {
    const checkbox = document.getElementById('comparisonMode-' + tabName);
    statsComparisonMode = checkbox ? checkbox.checked : false;
    
    // Clear global sort when entering comparison mode
    if (statsComparisonMode) {
        statsGlobalSortColumn = null;
    }
    
    // Always update global sort checkboxes (enable/disable based on comparison mode)
    updateGlobalSortCheckboxes(tabName);
    
    // Re-render the stats tables for this tab
    const stored = lastStatsTablesData[tabName];
    if (stored) {
        renderStatsTables(tabName, stored.data, stored.includeArtists);
    }
}

/**
 * Set global sort column for all tables
 * @param {string} tabName - The tab name
 * @param {string} column - 'count' or 'malePercent', or null to disable
 */
function setStatsGlobalSort(tabName, column) {
    if (statsComparisonMode) return; // Not allowed in comparison mode
    
    // Toggle off if clicking the same column
    if (statsGlobalSortColumn === column) {
        statsGlobalSortColumn = null;
    } else {
        statsGlobalSortColumn = column;
    }
    
    // Update checkbox states
    updateGlobalSortCheckboxes(tabName);
    
    // Re-render the stats tables for this tab
    const stored = lastStatsTablesData[tabName];
    if (stored) {
        renderStatsTables(tabName, stored.data, stored.includeArtists);
    }
}

/**
 * Update the global sort checkbox states
 */
function updateGlobalSortCheckboxes(tabName) {
    const globalSortDiv = document.getElementById('globalSort-' + tabName);
    const countCheckbox = document.getElementById('globalSortCount-' + tabName);
    const malePercentCheckbox = document.getElementById('globalSortMalePercent-' + tabName);
    
    // Update disabled class on wrapper for visual styling
    if (globalSortDiv) {
        if (statsComparisonMode) {
            globalSortDiv.classList.add('disabled');
        } else {
            globalSortDiv.classList.remove('disabled');
        }
    }
    
    if (countCheckbox) {
        countCheckbox.checked = statsGlobalSortColumn === 'count';
        countCheckbox.disabled = statsComparisonMode;
    }
    if (malePercentCheckbox) {
        malePercentCheckbox.checked = statsGlobalSortColumn === 'malePercent';
        malePercentCheckbox.disabled = statsComparisonMode;
    }
}

/**
 * Highlight rows with matching value name across all visible stats tables
 */
function highlightCrossTableRows(valueName) {
    // Find all rows with matching data-value-name
    const matchingRows = document.querySelectorAll(`tr[data-value-name="${valueName}"]`);
    matchingRows.forEach(row => {
        row.classList.add('cross-hover-highlight');
    });
}

/**
 * Clear all cross-table row highlights
 */
function clearCrossTableHighlight() {
    const highlightedRows = document.querySelectorAll('.cross-hover-highlight');
    highlightedRows.forEach(row => {
        row.classList.remove('cross-hover-highlight');
    });
}

/**
 * Get inline row style for gender-based coloring
 * Female (genderId 1) = pink, Male (genderId 2) = blue
 */
function getGenderRowStyle(genderId) {
    if (genderId === 1) {
        // Female = pink
        return 'background: rgba(236, 72, 153, 0.15); border-left: 3px solid rgba(236, 72, 153, 0.6);';
    } else if (genderId === 2) {
        // Male = blue
        return 'background: rgba(59, 130, 246, 0.15); border-left: 3px solid rgba(59, 130, 246, 0.6);';
    }
    return '';
}

/**
 * Opens the charts modal and loads chart data
 */
function openChartsModal() {
    // Clear any previous date range
    chartsDateRange.from = null;
    chartsDateRange.to = null;
    
    // Clear any previous entity filter
    chartsEntityFilter.type = null;
    chartsEntityFilter.id = null;
    chartsEntityFilter.name = null;
    
    // Reset tabs so they reload with cleared date filter
    resetChartsLoaded();
    
    document.getElementById('chartsModal').classList.add('show');
    document.body.style.overflow = 'hidden';
    updateChartsFiltersDisplay();
    
    // Always default to Top tab
    switchTab('top');
}

// Store date range for charts modal when opened from timeframe cards
let chartsDateRange = { from: null, to: null };

// Store entity filter for charts modal when opened from genre/subgenre/ethnicity/language cards
let chartsEntityFilter = { type: null, id: null, name: null };

/**
 * Opens the charts modal with a specific entity filter
 * Called from genre/subgenre/ethnicity/language cards
 * @param {string} filterType - The filter type: 'genre', 'subgenre', 'ethnicity', or 'language'
 * @param {number} filterId - The ID of the entity to filter by
 * @param {string} filterName - The display name of the entity (for showing in filter display)
 */
function openChartsModalWithFilter(filterType, filterId, filterName) {
    // Clear any previous filters
    chartsDateRange.from = null;
    chartsDateRange.to = null;
    
    // Store the entity filter
    chartsEntityFilter.type = filterType;
    chartsEntityFilter.id = filterId;
    chartsEntityFilter.name = filterName;
    
    // Reset all tabs so they reload with new filter
    resetChartsLoaded();
    
    // Open the modal
    document.getElementById('chartsModal').classList.add('show');
    document.body.style.overflow = 'hidden';
    
    // Update filter display to show the entity filter
    updateChartsFiltersDisplayWithEntity(filterType, filterName);
    
    // Always default to Top tab
    switchTab('top');
}

/**
 * Updates the filter display in charts modal to show entity filter
 */
function updateChartsFiltersDisplayWithEntity(filterType, filterName) {
    const container = document.getElementById('chartsFiltersDisplay');
    const noFiltersMsg = document.getElementById('noFiltersMsg');
    
    if (!container) return;
    
    // Remove existing filter chips from charts modal
    container.querySelectorAll('.filter-chip').forEach(el => el.remove());
    
    // Hide no filters message
    if (noFiltersMsg) noFiltersMsg.style.display = 'none';
    
    // Create a filter chip for the entity
    const chip = document.createElement('span');
    chip.className = 'filter-chip';
    
    const labelSpan = document.createElement('span');
    labelSpan.className = 'filter-chip-label';
    // Capitalize the filter type for display
    const displayType = filterType.charAt(0).toUpperCase() + filterType.slice(1) + ':';
    labelSpan.textContent = displayType;
    chip.appendChild(labelSpan);
    
    const valueSpan = document.createElement('span');
    valueSpan.className = 'filter-chip-value filter-value-highlight';
    valueSpan.textContent = filterName;
    chip.appendChild(valueSpan);
    
    container.appendChild(chip);
}

/**
 * Opens the charts modal with specific date range filter
 * Called from timeframe cards to filter charts by listened date
 * @param {string} listenedDateFrom - Start date in yyyy-MM-dd format
 * @param {string} listenedDateTo - End date in yyyy-MM-dd format  
 * @param {string} periodLabel - Display label for the time period (optional)
 */
function openChartsModalWithDates(listenedDateFrom, listenedDateTo, periodLabel) {
    // Clear any previous entity filter
    chartsEntityFilter.type = null;
    chartsEntityFilter.id = null;
    chartsEntityFilter.name = null;
    // Store the date range for use in API calls
    chartsDateRange.from = listenedDateFrom;
    chartsDateRange.to = listenedDateTo;
    
    // Reset all tabs so they reload with new date filter
    resetChartsLoaded();
    
    // Open the modal
    document.getElementById('chartsModal').classList.add('show');
    document.body.style.overflow = 'hidden';
    
    // Update filter display to show the date range
    updateChartsFiltersDisplayWithDates(listenedDateFrom, listenedDateTo, periodLabel);
    
    // Always default to Top tab
    switchTab('top');
}

/**
 * Updates the filter display in charts modal to show date range
 */
function updateChartsFiltersDisplayWithDates(listenedDateFrom, listenedDateTo, periodLabel) {
    const container = document.getElementById('chartsFiltersDisplay');
    const noFiltersMsg = document.getElementById('noFiltersMsg');
    
    if (!container) return;
    
    // Remove existing filter chips from charts modal
    container.querySelectorAll('.filter-chip').forEach(el => el.remove());
    
    // Hide no filters message
    if (noFiltersMsg) noFiltersMsg.style.display = 'none';
    
    // Create a filter chip for the date range
    const chip = document.createElement('span');
    chip.className = 'filter-chip';
    
    const labelSpan = document.createElement('span');
    labelSpan.className = 'filter-chip-label';
    labelSpan.textContent = 'Period:';
    chip.appendChild(labelSpan);
    
    const valueSpan = document.createElement('span');
    valueSpan.className = 'filter-chip-value filter-value-highlight';
    valueSpan.textContent = periodLabel || (listenedDateFrom + ' to ' + listenedDateTo);
    chip.appendChild(valueSpan);
    
    container.appendChild(chip);
}

/**
 * Closes the charts modal
 */
function closeChartsModal() {
    document.getElementById('chartsModal').classList.remove('show');
    document.body.style.overflow = '';
    
    // Clear date range when closing modal
    chartsDateRange.from = null;
    chartsDateRange.to = null;
    
    // Clear entity filter when closing modal
    chartsEntityFilter.type = null;
    chartsEntityFilter.id = null;
    chartsEntityFilter.name = null;
}

/**
 * Switches to a specific tab and loads its data if not already loaded
 */
function switchTab(tabName) {
    // Update tab buttons
    document.querySelectorAll('.charts-tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tabName);
    });
    
    // Update tab content visibility
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    document.getElementById('tab-' + tabName).classList.add('active');
    
    // Show/hide the Generate Playlist button based on tab and sub-tab
    const headerPlaylistBtn = document.getElementById('headerPlaylistBtn');
    if (headerPlaylistBtn) {
        // Show only if on Top tab AND Songs sub-tab is active
        const isTopTab = tabName === 'top';
        const songsSubTabActive = document.querySelector('.top-subtabs .charts-tab-btn[data-subtab="songs"].active') !== null;
        headerPlaylistBtn.style.display = (isTopTab && songsSubTabActive) ? '' : 'none';
    }
    
    // Load data if not already loaded
    if (!loadedTabs[tabName]) {
        loadTabData(tabName);
    }
}

/**
 * Loads data for a specific tab
 */
function loadTabData(tabName, forceReload = false) {
    if (loadedTabs[tabName] && !forceReload) {
        return;
    }
    
    const loadingEl = document.getElementById('loading-' + tabName);
    const errorEl = document.getElementById('error-' + tabName);
    const contentEl = document.getElementById('content-' + tabName);
    
    // Show loading, hide error and content
    if (loadingEl) loadingEl.style.display = 'flex';
    if (errorEl) errorEl.style.display = 'none';
    if (contentEl) contentEl.style.display = 'none';
    
    // Build query string from current URL parameters
    const params = getFilterParams();
    
    // Add artist include toggles for all tabs (read from form checkboxes)
    if (getArtistIncludeGroups()) {
        params.set('includeGroups', 'true');
    }
    if (getArtistIncludeFeatured()) {
        params.set('includeFeatured', 'true');
    }
    
    // Add top limit only for top tab (always) or other tabs if their "Apply Limit" checkbox is checked
    if (tabName === 'top') {
        params.set('limit', getTopLimit());
    } else {
        // For non-top tabs, only apply limit if checkbox is checked
        const checkboxId = 'applyLimit' + tabName.charAt(0).toUpperCase() + tabName.slice(1);
        const checkbox = document.getElementById(checkboxId);
        if (checkbox && checkbox.checked) {
            params.set('limit', getTopLimit());
        }
    }
    
    const apiUrl = '/songs/api/charts/' + tabName + '?' + params.toString();
    
    console.log('Fetching chart data from:', apiUrl);
    
    fetch(apiUrl)
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok: ' + response.status);
            }
            return response.json();
        })
        .then(data => {
            console.log('Chart data received for', tabName + ':', data);
            
            // Hide loading, show content
            if (loadingEl) loadingEl.style.display = 'none';
            if (contentEl) contentEl.style.display = 'block';
            
            // Render charts based on tab
            renderTabCharts(tabName, data);
            loadedTabs[tabName] = true;
        })
        .catch(error => {
            console.error('Error loading chart data for', tabName + ':', error);
            if (loadingEl) loadingEl.style.display = 'none';
            if (errorEl) errorEl.style.display = 'flex';
        });
}

/**
 * Gets filter parameters from current URL and includes any date range or entity filter set
 */
function getFilterParams() {
    const currentUrl = new URL(window.location.href);
    const params = new URLSearchParams(currentUrl.search);
    
    // Remove pagination and sorting params, keep only filters
    params.delete('page');
    params.delete('perpage');
    params.delete('sortby');
    params.delete('sortdir');
    
    // Add date range if set (from timeframe cards)
    if (chartsDateRange.from && chartsDateRange.to) {
        params.set('listenedDateFrom', chartsDateRange.from);
        params.set('listenedDateTo', chartsDateRange.to);
    }
    
    // Add entity filter if set (from genre/subgenre/ethnicity/language cards)
    if (chartsEntityFilter.type && chartsEntityFilter.id) {
        params.set(chartsEntityFilter.type, chartsEntityFilter.id);
        params.set(chartsEntityFilter.type + 'Mode', 'includes');
    }
    
    return params;
}

/**
 * Renders charts for a specific tab based on the data received
 */
function renderTabCharts(tabName, data) {
    switch (tabName) {
        case 'general':
            createCombinedBarChart('generalCombinedChartContainer', 'generalCombinedChart', {
                artists: [{ name: 'Overall', male: data.artistsByGender?.male || 0, female: data.artistsByGender?.female || 0, other: data.artistsByGender?.other || 0 }],
                albums: [{ name: 'Overall', male: data.albumsByGender?.male || 0, female: data.albumsByGender?.female || 0, other: data.albumsByGender?.other || 0 }],
                songs: [{ name: 'Overall', male: data.songsByGender?.male || 0, female: data.songsByGender?.female || 0, other: data.songsByGender?.other || 0 }],
                plays: [{ name: 'Overall', male: data.playsByGender?.male || 0, female: data.playsByGender?.female || 0, other: data.playsByGender?.other || 0 }],
                listeningTime: [{ name: 'Overall', male: data.listeningTimeByGender?.male || 0, female: data.listeningTimeByGender?.female || 0, other: data.listeningTimeByGender?.other || 0 }]
            });
            break;
        
        case 'top':
            renderTopTables(data);
            break;
            
        case 'genre':
            renderStatsTables('genre', data, true);
            createCombinedBarChart('genreCombinedChartContainer', 'genreCombinedChart', {
                artists: data.artistsByGenre,
                albums: data.albumsByGenre,
                songs: data.songsByGenre,
                plays: data.playsByGenre,
                listeningTime: data.listeningTimeByGenre
            });
            break;
            
        case 'subgenre':
            renderStatsTables('subgenre', data, true);
            createCombinedBarChart('subgenreCombinedChartContainer', 'subgenreCombinedChart', {
                artists: data.artistsBySubgenre,
                albums: data.albumsBySubgenre,
                songs: data.songsBySubgenre,
                plays: data.playsBySubgenre,
                listeningTime: data.listeningTimeBySubgenre
            });
            break;
            
        case 'ethnicity':
            renderStatsTables('ethnicity', data, true);
            createCombinedBarChart('ethnicityCombinedChartContainer', 'ethnicityCombinedChart', {
                artists: data.artistsByEthnicity,
                albums: data.albumsByEthnicity,
                songs: data.songsByEthnicity,
                plays: data.playsByEthnicity,
                listeningTime: data.listeningTimeByEthnicity
            });
            break;
            
        case 'language':
            renderStatsTables('language', data, true);
            createCombinedBarChart('languageCombinedChartContainer', 'languageCombinedChart', {
                artists: data.artistsByLanguage,
                albums: data.albumsByLanguage,
                songs: data.songsByLanguage,
                plays: data.playsByLanguage,
                listeningTime: data.listeningTimeByLanguage
            });
            break;
            
        case 'country':
            renderStatsTables('country', data, true);
            createCombinedBarChart('countryCombinedChartContainer', 'countryCombinedChart', {
                artists: data.artistsByCountry,
                albums: data.albumsByCountry,
                songs: data.songsByCountry,
                plays: data.playsByCountry,
                listeningTime: data.listeningTimeByCountry
            });
            break;
            
        case 'releaseYear':
            renderStatsTables('releaseYear', data, false); // No artists for release year
            createCombinedBarChart('releaseYearCombinedChartContainer', 'releaseYearCombinedChart', {
                artists: null, // No artists for release year
                albums: data.albumsByReleaseYear,
                songs: data.songsByReleaseYear,
                plays: data.playsByReleaseYear,
                listeningTime: data.listeningTimeByReleaseYear
            });
            break;
            
        case 'listenYear':
            renderStatsTables('listenYear', data, true);
            createCombinedBarChart('listenYearCombinedChartContainer', 'listenYearCombinedChart', {
                artists: data.artistsByListenYear,
                albums: data.albumsByListenYear,
                songs: data.songsByListenYear,
                plays: data.playsByListenYear,
                listeningTime: data.listeningTimeByListenYear
            });
            break;
    }
}

/**
 * Updates the filters display in the charts modal
 * Copies filter chips from the page's active filters section
 */
function updateChartsFiltersDisplay() {
    const container = document.getElementById('chartsFiltersDisplay');
    const noFiltersMsg = document.getElementById('noFiltersMsg');
    
    if (!container) return;
    
    // Copy filter chips from the page's active filters section (if it exists)
    // This is used when charts modal is opened from song/album/artist list pages
    const pageFilters = document.getElementById('activeFilters');
    
    // If there's no activeFilters element on the page, the filter chips were already
    // rendered by Thymeleaf (e.g., on the standalone /charts page), so don't touch them
    if (!pageFilters) {
        // Just check if we have any filter chips already rendered
        const existingChips = container.querySelectorAll('.filter-chip');
        if (existingChips.length > 0) {
            if (noFiltersMsg) noFiltersMsg.style.display = 'none';
        }
        return;
    }
    
    // Remove existing filter chips from charts modal (we'll copy fresh ones from the page)
    container.querySelectorAll('.filter-chip').forEach(el => el.remove());
    
    const pageChips = pageFilters.querySelectorAll('.filter-chip');
    
    if (pageChips.length === 0) {
        if (noFiltersMsg) noFiltersMsg.style.display = 'inline';
    } else {
        if (noFiltersMsg) noFiltersMsg.style.display = 'none';
        pageChips.forEach(chip => {
            const label = chip.querySelector('.filter-chip-label');
            const values = chip.querySelectorAll('.filter-chip-value');
            if (label) {
                const newChip = document.createElement('span');
                newChip.className = 'filter-chip';
                
                // Create label span
                const labelSpan = document.createElement('span');
                labelSpan.className = 'filter-chip-label';
                labelSpan.textContent = label.textContent;
                newChip.appendChild(labelSpan);
                
                // Add values with bold styling
                if (values.length > 0) {
                    values.forEach((value, idx) => {
                        const valueSpan = document.createElement('span');
                        valueSpan.className = 'filter-chip-value filter-value-highlight';
                        valueSpan.textContent = value.textContent.trim();
                        newChip.appendChild(valueSpan);
                    });
                }
                
                container.appendChild(newChip);
            }
        });
    }
}

/**
 * Creates a gender pie/doughnut chart
 * @param {string} canvasId - The canvas element ID
 * @param {object} genderData - Object with male and female counts
 * @param {string} label - Label for the chart
 * @param {boolean} isListeningTime - Whether this is a listening time chart (format values differently)
 */
function createGenderPieChart(canvasId, genderData, label, isListeningTime = false) {
    if (chartInstances[canvasId]) {
        chartInstances[canvasId].destroy();
    }
    
    // Handle null/undefined data
    if (!genderData) {
        console.warn('No gender data for chart:', canvasId);
        genderData = { male: 0, female: 0 };
    }
    
    const canvas = document.getElementById(canvasId);
    if (!canvas) {
        console.warn('Canvas not found:', canvasId);
        return;
    }
    
    const ctx = canvas.getContext('2d');
    chartInstances[canvasId] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Male', 'Female'],
            datasets: [{
                data: [genderData.male || 0, genderData.female || 0],
                backgroundColor: ['rgba(59, 130, 246, 0.8)', 'rgba(236, 72, 153, 0.8)'],
                borderColor: ['rgba(59, 130, 246, 1)', 'rgba(236, 72, 153, 1)'],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        color: '#e5e7eb',
                        padding: 16,
                        font: { size: 12 }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const value = context.raw;
                            const percentage = total > 0 ? ((value / total) * 100).toFixed(2) : '0.00';
                            const displayValue = isListeningTime ? formatListeningTime(value) : value.toLocaleString();
                            return context.label + ': ' + displayValue + ' (' + percentage + '%)';
                        }
                    }
                },
                datalabels: {
                    color: '#fff',
                    font: { weight: 'bold', size: 14 },
                    formatter: (value, ctx) => {
                        const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
                        if (total === 0 || value === 0) return '';
                        const pct = ((value / total) * 100).toFixed(2);
                        return pct + '%';
                    }
                }
            }
        }
    });
}

/**
 * Creates a dynamically-sized stacked bar chart
 * @param {string} containerId - The container div ID for the canvas
 * @param {string} canvasId - The canvas element ID to create
 * @param {array} categoryData - Array of objects with name, male, female properties
 * @param {boolean} isListeningTime - Whether this is a listening time chart
 */
function createDynamicStackedBarChart(containerId, canvasId, categoryData, isListeningTime = false) {
    if (chartInstances[canvasId]) {
        chartInstances[canvasId].destroy();
    }
    
    if (!categoryData || categoryData.length === 0) {
        categoryData = [{ name: 'No data', male: 0, female: 0 }];
    }
    
    const container = document.getElementById(containerId);
    if (!container) {
        console.warn('Container not found:', containerId);
        return;
    }
    
    // Calculate dynamic height based on number of items
    const itemHeight = 35;
    const baseHeight = 100;
    const calculatedHeight = categoryData.length * itemHeight + baseHeight;
    
    // Create canvas element
    container.innerHTML = '';
    const canvas = document.createElement('canvas');
    canvas.id = canvasId;
    canvas.style.height = calculatedHeight + 'px';
    container.appendChild(canvas);
    
    const ctx = canvas.getContext('2d');
    const labels = categoryData.map(item => item.name);
    const maleData = categoryData.map(item => item.male || 0);
    const femaleData = categoryData.map(item => item.female || 0);
    
    chartInstances[canvasId] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Male',
                    data: maleData,
                    backgroundColor: 'rgba(59, 130, 246, 0.8)',
                    borderColor: 'rgba(59, 130, 246, 1)',
                    borderWidth: 1
                },
                {
                    label: 'Female',
                    data: femaleData,
                    backgroundColor: 'rgba(236, 72, 153, 0.8)',
                    borderColor: 'rgba(236, 72, 153, 1)',
                    borderWidth: 1
                }
            ]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    stacked: true,
                    ticks: { 
                        color: '#e5e7eb',
                        callback: function(value) {
                            if (isListeningTime) {
                                return formatListeningTime(value);
                            }
                            return value.toLocaleString();
                        }
                    },
                    grid: { color: 'rgba(255, 255, 255, 0.05)' }
                },
                y: {
                    stacked: true,
                    ticks: { color: '#e5e7eb' },
                    grid: { color: 'rgba(255, 255, 255, 0.05)' }
                }
            },
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        color: '#e5e7eb',
                        padding: 16,
                        font: { size: 12 }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const dataIndex = context.dataIndex;
                            const male = maleData[dataIndex] || 0;
                            const female = femaleData[dataIndex] || 0;
                            const total = male + female;
                            const pct = total > 0 ? ((context.raw / total) * 100).toFixed(2) : '0.00';
                            const displayValue = isListeningTime ? formatListeningTime(context.raw) : context.raw.toLocaleString();
                            return context.dataset.label + ': ' + displayValue + ' (' + pct + '%)';
                        }
                    }
                },
                datalabels: {
                    color: '#fff',
                    font: { weight: 'bold', size: 11 },
                    anchor: 'center',
                    align: 'center',
                    formatter: (value, ctx) => {
                        const dataIndex = ctx.dataIndex;
                        const male = maleData[dataIndex] || 0;
                        const female = femaleData[dataIndex] || 0;
                        const total = male + female;
                        if (total === 0 || value === 0) return '';
                        const pct = ((value / total) * 100).toFixed(2);
                        if (parseFloat(pct) < 5) return ''; // Hide very small percentages
                        return pct + '%';
                    }
                }
            }
        }
    });
}

/**
 * Creates a combined bar chart showing all 5 metrics (Artists, Albums, Songs, Plays, Listening Time) for each category
 * Each category (e.g., "Pop") gets 5 grouped bars, one for each metric
 * @param {string} containerId - The container div ID
 * @param {string} canvasId - The canvas element ID to create
 * @param {object} allData - Object with artists, albums, songs, plays, listeningTime arrays
 */
function createCombinedBarChart(containerId, canvasId, allData) {
    // Cache chart data for re-rendering when scaling mode changes
    lastChartData[canvasId] = { containerId, allData };
    
    if (chartInstances[canvasId]) {
        chartInstances[canvasId].destroy();
    }
    
    const container = document.getElementById(containerId);
    if (!container) {
        console.warn('Container not found:', containerId);
        return;
    }
    
    // Get all unique category names from all datasets
    const categorySet = new Set();
    ['artists', 'albums', 'songs', 'plays', 'listeningTime'].forEach(metric => {
        if (allData[metric]) {
            allData[metric].forEach(item => categorySet.add(item.name));
        }
    });
    let categories = Array.from(categorySet);
    
    // Determine if this is a year-based chart (Release Year or Listen Year)
    const isYearChart = canvasId.includes('releaseYear') || canvasId.includes('listenYear');
    
    // Sort categories
    if (isYearChart) {
        // For year charts, sort by year descending (newest first)
        categories.sort((a, b) => {
            // Handle 'Unknown' - put it last
            if (a === 'Unknown') return 1;
            if (b === 'Unknown') return -1;
            // Sort years descending
            const yearA = parseInt(a);
            const yearB = parseInt(b);
            return yearB - yearA;
        });
    } else {
        // For non-year charts, sort by the selected metric descending
        const sortMetricKey = chartSortMetric;
        const sortData = allData[sortMetricKey];
        if (sortData) {
            const sortMap = {};
            sortData.forEach(item => {
                sortMap[item.name] = (item.male || 0) + (item.female || 0) + (item.other || 0);
            });
            categories.sort((a, b) => {
                const valA = sortMap[a] || 0;
                const valB = sortMap[b] || 0;
                return valB - valA; // Descending order
            });
        }
    }
    
    if (categories.length === 0) {
        container.innerHTML = '<div style="text-align: center; color: #666; padding: 40px;">No data available</div>';
        return;
    }
    
    // Create lookup maps for each metric
    const createLookup = (data) => {
        if (!data) return {};
        const map = {};
        data.forEach(item => {
            map[item.name] = { male: item.male || 0, female: item.female || 0, other: item.other || 0 };
        });
        return map;
    };
    
    const artistsLookup = createLookup(allData.artists);
    const albumsLookup = createLookup(allData.albums);
    const songsLookup = createLookup(allData.songs);
    const playsLookup = createLookup(allData.plays);
    const listeningTimeLookup = createLookup(allData.listeningTime);
    
    // Calculate dynamic height - more space per category since we have 5 bars per category
    const itemHeight = 120; // Height per category group
    const baseHeight = 150;
    const calculatedHeight = categories.length * itemHeight + baseHeight;
    
    // Create canvas element
    container.innerHTML = '';
    const canvas = document.createElement('canvas');
    canvas.id = canvasId;
    canvas.style.height = calculatedHeight + 'px';
    container.appendChild(canvas);
    
    const ctx = canvas.getContext('2d');
    
    // Define colors for each metric (blue shades for male, pink shades for female, gray shades for other)
    const metricColors = {
        artists: { male: 'rgba(30, 64, 175, 0.9)', female: 'rgba(190, 24, 93, 0.9)', other: 'rgba(75, 85, 99, 0.9)' },       // Dark blue / Dark pink / Dark gray
        albums: { male: 'rgba(59, 130, 246, 0.9)', female: 'rgba(236, 72, 153, 0.9)', other: 'rgba(107, 114, 128, 0.9)' },      // Medium blue / Medium pink / Medium gray
        songs: { male: 'rgba(96, 165, 250, 0.9)', female: 'rgba(244, 114, 182, 0.9)', other: 'rgba(156, 163, 175, 0.9)' },      // Light blue / Light pink / Light gray
        plays: { male: 'rgba(147, 197, 253, 0.9)', female: 'rgba(249, 168, 212, 0.9)', other: 'rgba(209, 213, 219, 0.9)' },     // Lighter blue / Lighter pink / Lighter gray
        listeningTime: { male: 'rgba(191, 219, 254, 0.9)', female: 'rgba(252, 231, 243, 0.9)', other: 'rgba(229, 231, 235, 0.9)' } // Lightest blue / Lightest pink / Lightest gray
    };
    
    // Build datasets - each metric gets male and female bars, normalized to percentages
    const datasets = [];
    const metrics = [
        { key: 'artists', label: 'Artists', lookup: artistsLookup },
        { key: 'albums', label: 'Albums', lookup: albumsLookup },
        { key: 'songs', label: 'Songs', lookup: songsLookup },
        { key: 'plays', label: 'Plays', lookup: playsLookup },
        { key: 'listeningTime', label: 'Listen Time', lookup: listeningTimeLookup, isTime: true }
    ];
    
    // Store raw values for tooltips and totals
    const rawValues = {};
    const totalsByMetric = {}; // Store totals for each metric per category

    // Calculate global max totals for each metric (used in relative scaling mode)
    const globalMaxByMetric = {};
    // Calculate sum of all totals across all categories for each metric (for global percentage)
    const globalSumByMetric = {};
    metrics.forEach(metric => {
        if (!allData[metric.key]) return;
        const lookup = metric.lookup;
        let maxTotal = 0;
        let sumTotal = 0;
        categories.forEach(cat => {
            const total = (lookup[cat]?.male || 0) + (lookup[cat]?.female || 0) + (lookup[cat]?.other || 0);
            if (total > maxTotal) maxTotal = total;
            sumTotal += total;
        });
        globalMaxByMetric[metric.key] = maxTotal;
        globalSumByMetric[metric.key] = sumTotal;
    });

    metrics.forEach((metric, idx) => {
        if (!allData[metric.key]) return; // Skip if no data (e.g., artists for release year)
        
        const maleData = categories.map(cat => metric.lookup[cat]?.male || 0);
        const femaleData = categories.map(cat => metric.lookup[cat]?.female || 0);
        const otherData = categories.map(cat => metric.lookup[cat]?.other || 0);
        
        // Calculate total for each category
        const totals = categories.map((cat, i) => maleData[i] + femaleData[i] + otherData[i]);
        totalsByMetric[metric.key] = totals;
        
        // Get global sum for this metric
        const globalTotal = globalSumByMetric[metric.key] || 0;
        
        // Calculate percentages based on scaling mode
        const globalMax = globalMaxByMetric[metric.key] || 1;
        
        const malePercentages = categories.map((cat, i) => {
            const total = totals[i];
            if (useRelativeScaling) {
                // Relative mode: scale bar based on global max of this metric
                return globalMax > 0 ? (maleData[i] / globalMax) * 100 : 0;
            } else {
                // Percentage mode: scale bar to 100% within category
                return total > 0 ? (maleData[i] / total) * 100 : 0;
            }
        });
        const femalePercentages = categories.map((cat, i) => {
            const total = totals[i];
            if (useRelativeScaling) {
                return globalMax > 0 ? (femaleData[i] / globalMax) * 100 : 0;
            } else {
                return total > 0 ? (femaleData[i] / total) * 100 : 0;
            }
        });
        const otherPercentages = categories.map((cat, i) => {
            const total = totals[i];
            if (useRelativeScaling) {
                return globalMax > 0 ? (otherData[i] / globalMax) * 100 : 0;
            } else {
                return total > 0 ? (otherData[i] / total) * 100 : 0;
            }
        });
        
        // Store raw values for tooltips
        rawValues[metric.key] = { male: maleData, female: femaleData, other: otherData, totals: totals, isTime: metric.isTime };
        
        // Male bar for this metric
        datasets.push({
            label: metric.label + ' (M)',
            data: malePercentages,
            backgroundColor: metricColors[metric.key].male,
            borderColor: metricColors[metric.key].male.replace('0.9', '1'),
            borderWidth: 1,
            stack: 'stack' + idx,
            metricKey: metric.key,
            isTime: metric.isTime || false,
            genderType: 'male',
            rawData: maleData,
            totalData: totals,
            globalTotal: globalTotal
        });
        
        // Female bar for this metric
        datasets.push({
            label: metric.label + ' (F)',
            data: femalePercentages,
            backgroundColor: metricColors[metric.key].female,
            borderColor: metricColors[metric.key].female.replace('0.9', '1'),
            borderWidth: 1,
            stack: 'stack' + idx,
            metricKey: metric.key,
            isTime: metric.isTime || false,
            genderType: 'female',
            rawData: femaleData,
            totalData: totals,
            globalTotal: globalTotal
        });
        
        // Other bar for this metric (only add if there's any "other" data)
        const hasOtherData = otherData.some(v => v > 0);
        if (hasOtherData) {
            datasets.push({
                label: metric.label + ' (O)',
                data: otherPercentages,
                backgroundColor: metricColors[metric.key].other,
                borderColor: metricColors[metric.key].other.replace('0.9', '1'),
                borderWidth: 1,
                stack: 'stack' + idx,
                metricKey: metric.key,
                isTime: metric.isTime || false,
                genderType: 'other',
                rawData: otherData,
                totalData: totals,
                globalTotal: globalTotal
            });
        }
    });
    
    chartInstances[canvasId] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: categories,
            datasets: datasets
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            layout: {
                padding: {
                    left: 0 // Totals are drawn inside the bar area now
                }
            },
            scales: {
                x: {
                    stacked: true,
                    min: 0,
                    max: 100,
                    ticks: { 
                        color: '#e5e7eb',
                        callback: function(value) {
                            return value + '%';
                        }
                    },
                    grid: { color: 'rgba(255, 255, 255, 0.05)' }
                },
                y: {
                    stacked: true,
                    ticks: { 
                        color: '#e5e7eb',
                        font: { size: 12, weight: 'bold' }
                    },
                    grid: { color: 'rgba(255, 255, 255, 0.05)' }
                }
            },
            plugins: {
                legend: {
                    display: false  // Remove the legend labels at the top
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const isTime = context.dataset.isTime;
                            const rawValue = context.dataset.rawData[context.dataIndex];
                            const totalValue = context.dataset.totalData[context.dataIndex];
                            const globalTotal = context.dataset.globalTotal || 0;
                            const displayValue = isTime ? formatListeningTime(rawValue) : rawValue.toLocaleString();
                            const displayTotal = isTime ? formatListeningTime(totalValue) : totalValue.toLocaleString();
                            const displayGlobalTotal = isTime ? formatListeningTime(globalTotal) : globalTotal.toLocaleString();
                            
                            // Percentage within this category
                            const catPct = totalValue > 0 ? ((rawValue / totalValue) * 100).toFixed(2) : '0.00';
                            // Percentage of category vs global
                            const globalPct = globalTotal > 0 ? ((totalValue / globalTotal) * 100).toFixed(2) : '0.00';
                            
                            return [
                                context.dataset.label + ': ' + displayValue + ' (' + catPct + '%)',
                                'Category Total: ' + displayTotal,
                                'Global Total: ' + displayGlobalTotal + ' (' + globalPct + '% of global)'
                            ];
                        }
                    }
                },
                datalabels: {
                    color: function(context) {
                        // Use darker text for lighter backgrounds and for "other" bars
                        const metricKey = context.dataset.metricKey;
                        const genderType = context.dataset.genderType;
                        if (metricKey === 'listeningTime' || metricKey === 'plays' || genderType === 'other') {
                            return '#333';
                        }
                        return '#fff';
                    },
                    font: { weight: 'bold', size: 9 },
                    anchor: 'center',
                    align: 'center',
                    formatter: (value, ctx) => {
                        if (value < 5) return ''; // Hide small percentages
                        const rawValue = ctx.dataset.rawData[ctx.dataIndex];
                        const isTime = ctx.dataset.isTime;
                        const displayValue = isTime ? formatListeningTime(rawValue) : rawValue.toLocaleString();
                        return displayValue + ' (' + value.toFixed(2) + '%)';
                    }
                }
            }
        },
        plugins: [{
            // Custom plugin to draw totals at the beginning of each bar
            id: 'barTotals',
            afterDraw: function(chart) {
                const ctx = chart.ctx;
                const yAxis = chart.scales.y;
                const xAxis = chart.scales.x;
                
                // Group datasets by their stack (each stack represents one metric)
                const stackGroups = {};
                chart.data.datasets.forEach((dataset, datasetIndex) => {
                    const stack = dataset.stack;
                    if (!stackGroups[stack]) {
                        stackGroups[stack] = {
                            metricKey: dataset.metricKey,
                            isTime: dataset.isTime,
                            totalData: dataset.totalData
                        };
                    }
                });
                
                // For each category (y-axis label), draw totals for each metric
                chart.data.labels.forEach((label, categoryIndex) => {
                    const yPos = yAxis.getPixelForValue(categoryIndex);
                    
                    // Get the number of stacks (metrics)
                    const stackKeys = Object.keys(stackGroups);
                    const numStacks = stackKeys.length;
                    
                    // Calculate the bar height per metric within each category
                    const categoryHeight = yAxis.height / chart.data.labels.length;
                    const barGroupHeight = categoryHeight * 0.8; // 80% of category height for bars
                    const singleBarHeight = barGroupHeight / numStacks;
                    
                    // Draw total for each metric in this category
                    stackKeys.forEach((stackKey, stackIndex) => {
                        const stackData = stackGroups[stackKey];
                        const total = stackData.totalData ? stackData.totalData[categoryIndex] : 0;
                        
                        if (total > 0) {
                            const displayTotal = stackData.isTime ? formatListeningTime(total) : total.toLocaleString();
                            
                            // Calculate y position for this specific bar within the category
                            const barYOffset = (stackIndex - (numStacks - 1) / 2) * singleBarHeight;
                            const barY = yPos + barYOffset;
                            
                            // Draw at the start of the bars (inside the chart area)
                            ctx.save();
                            ctx.font = 'bold 10px sans-serif';
                            // Draw text with dark outline for visibility on light backgrounds
                            ctx.strokeStyle = 'rgba(0, 0, 0, 0.7)';
                            ctx.lineWidth = 3;
                            ctx.textAlign = 'left';
                            ctx.textBaseline = 'middle';
                            ctx.strokeText(displayTotal, xAxis.left + 3, barY);
                            ctx.fillStyle = '#ffffff';
                            ctx.fillText(displayTotal, xAxis.left + 3, barY);
                            ctx.restore();
                        }
                    });
                });
            }
        }]
    });
}

/**
 * Toggles between percentage-based and relative scaling modes
 * Percentage mode: Each category bar reaches 100% (shows M/F/O distribution within category)
 * Relative mode: Bars are scaled relative to the largest category (shows cross-category comparison)
 */
function toggleRelativeScaling() {
    useRelativeScaling = !useRelativeScaling;
    
    // Update button state
    const btn = document.getElementById('scalingToggleBtn');
    if (btn) {
        btn.classList.toggle('active', useRelativeScaling);
        btn.textContent = useRelativeScaling ? '📊 Relative Scaling (ON)' : '📊 Relative Scaling (OFF)';
    }
    
    // Re-render all charts that have been loaded with cached data
    Object.keys(lastChartData).forEach(canvasId => {
        const data = lastChartData[canvasId];
        if (data) {
            createCombinedBarChart(data.containerId, canvasId, data.allData);
        }
    });
}

/**
 * Changes the sort metric for chart groups and re-renders all charts
 * @param {string} metric - The metric to sort by: 'artists', 'albums', 'songs', 'plays', 'listeningTime'
 */
function changeChartSortMetric(metric) {
    chartSortMetric = metric;
    
    // Update button states
    document.querySelectorAll('.chart-sort-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.metric === metric);
    });
    
    // Re-render all charts that have been loaded with cached data
    Object.keys(lastChartData).forEach(canvasId => {
        const data = lastChartData[canvasId];
        if (data) {
            createCombinedBarChart(data.containerId, canvasId, data.allData);
        }
    });
    
    // Re-render stats tables only if comparison mode is enabled
    if (statsComparisonMode) {
        Object.keys(lastStatsTablesData).forEach(tabName => {
            const stored = lastStatsTablesData[tabName];
            if (stored) {
                renderStatsTables(tabName, stored.data, stored.includeArtists);
            }
        });
    }
}

/**
 * Resets the charts loaded flags so charts will reload on next open
 * Call this when filters change
 */
function resetChartsLoaded() {
    loadedTabs = {
        general: false,
        top: false,
        genre: false,
        subgenre: false,
        ethnicity: false,
        language: false,
        releaseYear: false,
        listenYear: false
    };
    // Also clear cached stats tables data and sort state
    lastStatsTablesData = {};
    statsTableSortState = {};
    statsComparisonMode = false;
    statsGlobalSortColumn = 'count';
}

/**
 * Renders the Top tables with artist, album, and song data
 */
function renderTopTables(data) {
    // Store data for client-side sorting
    topTabData.artists = data.topArtists || [];
    topTabData.albums = data.topAlbums || [];
    topTabData.songs = data.topSongs || [];
    
    // Render each table
    renderTopArtistsTable();
    renderTopAlbumsTable();
    renderTopSongsTable();
    
    // Setup sorting handlers
    setupTopTableSorting();
}

/**
 * Renders the podium section for artists
 */
function renderArtistsPodium(data) {
    const podiumContainer = document.getElementById('artistsPodium');
    if (!podiumContainer || !data || data.length === 0) {
        if (podiumContainer) podiumContainer.innerHTML = '';
        return;
    }

    const top3 = data.slice(0, 3);
    const podiumClasses = ['podium-gold', 'podium-silver', 'podium-bronze'];
    const playsClasses = ['podium-plays-gold', 'podium-plays-silver', 'podium-plays-bronze'];
    const ranks = ['1', '2', '3'];

    podiumContainer.innerHTML = top3.map((artist, index) => `
        <a href="/artists/${artist.id}" class="podium-card ${podiumClasses[index]}">
            <div class="podium-rank">${ranks[index]}</div>
            <img src="/artists/${artist.id}/image" alt="" class="podium-image artist" 
                 onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
            <div class="podium-no-image artist" style="display:none;">🎤</div>
            <div class="podium-name" title="${escapeHtml(artist.name || '')}">${escapeHtml(artist.name || '-')}</div>
            <div class="podium-plays ${playsClasses[index]}">${(artist.plays || 0).toLocaleString()} plays</div>
        </a>
    `).join('');
}

/**
 * Renders the podium section for albums
 */
function renderAlbumsPodium(data) {
    const podiumContainer = document.getElementById('albumsPodium');
    if (!podiumContainer || !data || data.length === 0) {
        if (podiumContainer) podiumContainer.innerHTML = '';
        return;
    }

    const top3 = data.slice(0, 3);
    const podiumClasses = ['podium-gold', 'podium-silver', 'podium-bronze'];
    const playsClasses = ['podium-plays-gold', 'podium-plays-silver', 'podium-plays-bronze'];
    const ranks = ['1', '2', '3'];

    podiumContainer.innerHTML = top3.map((album, index) => `
        <a href="/albums/${album.id}" class="podium-card ${podiumClasses[index]}">
            <div class="podium-rank">${ranks[index]}</div>
            <img src="/albums/${album.id}/image" alt="" class="podium-image" 
                 onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
            <div class="podium-no-image" style="display:none;">💿</div>
            <div class="podium-name" title="${escapeHtml(album.name || '')}">${escapeHtml(album.name || '-')}</div>
            <div class="podium-artist" title="${escapeHtml(album.artistName || '')}">${escapeHtml(album.artistName || '-')}</div>
            <div class="podium-plays ${playsClasses[index]}">${(album.plays || 0).toLocaleString()} plays</div>
        </a>
    `).join('');
}

/**
 * Renders the podium section for songs
 */
function renderSongsPodium(data) {
    const podiumContainer = document.getElementById('songsPodium');
    if (!podiumContainer || !data || data.length === 0) {
        if (podiumContainer) podiumContainer.innerHTML = '';
        return;
    }

    const top3 = data.slice(0, 3);
    const podiumClasses = ['podium-gold', 'podium-silver', 'podium-bronze'];
    const playsClasses = ['podium-plays-gold', 'podium-plays-silver', 'podium-plays-bronze'];
    const ranks = ['1', '2', '3'];

    podiumContainer.innerHTML = top3.map((song, index) => {
        const imageUrl = song.hasImage ? `/songs/${song.id}/image` : (song.albumId ? `/albums/${song.albumId}/image` : null);
        return `
        <a href="/songs/${song.id}" class="podium-card ${podiumClasses[index]}">
            <div class="podium-rank">${ranks[index]}</div>
            ${imageUrl ? 
                `<img src="${imageUrl}" alt="" class="podium-image" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
                 <div class="podium-no-image" style="display:none;">🎵</div>` : 
                `<div class="podium-no-image">🎵</div>`}
            <div class="podium-name" title="${escapeHtml(song.name || '')}">${escapeHtml(song.name || '-')}</div>
            <div class="podium-artist" title="${escapeHtml(song.artistName || '')}">${escapeHtml(song.artistName || '-')}</div>
            <div class="podium-plays ${playsClasses[index]}">${(song.plays || 0).toLocaleString()} plays</div>
        </a>
    `}).join('');
}

/**
 * Renders the Top Artists table
 */
function renderTopArtistsTable() {
    const tbody = document.querySelector('#topArtistsTable tbody');
    if (!tbody) return;
    
    const data = sortTopData(topTabData.artists, topSortState.artists);
    
    // Render podium with sorted data
    renderArtistsPodium(data);

    if (data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; color: #666; padding: 20px;">No artist data available</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.map((artist, index) => {
        const rank = index + 1;
        return `
        <tr style="${getGenderRowStyle(artist.genderId)}">
            <td class="rank-col">${rank}</td>
            <td class="cover-col artist-cover">
                <div style="cursor:pointer;">
                    <img src="/artists/${artist.id}/image" alt="" class="clickable-image" onclick="openArtistImageModalFromGraphs(${artist.id})" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" style="width:40px;height:60px;object-fit:cover;border-radius:4px;">
                    <div style="display:none;width:40px;height:60px;background:#2a2a2a;border-radius:4px;align-items:center;justify-content:center;color:#666;font-size:14px;">🎤</div>
                </div>
            </td>
            <td><a href="/artists/${artist.id}">${escapeHtml(artist.name || '-')}</a></td>
            <td style="text-align:right;"${(artist.plays || 0) >= 1000 ? ' class="high-plays"' : ''}>${(artist.plays || 0).toLocaleString()}</td>
            <td style="text-align:right;">${(artist.primaryPlays || 0).toLocaleString()}</td>
            <td style="text-align:right;">${(artist.legacyPlays || 0).toLocaleString()}</td>
            <td style="text-align:right;">${artist.timeListenedFormatted || '-'}</td>
            <td>${artist.firstListened || '-'}</td>
            <td>${artist.lastListened || '-'}</td>
        </tr>
    `}).join('');

    updateSortIndicators('topArtistsTable', topSortState.artists);
}

/**
 * Renders the Top Albums table
 */
function renderTopAlbumsTable() {
    const tbody = document.querySelector('#topAlbumsTable tbody');
    if (!tbody) return;
    
    const data = sortTopData(topTabData.albums, topSortState.albums);
    
    // Render podium with sorted data
    renderAlbumsPodium(data);

    if (data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="12" style="text-align: center; color: #666; padding: 20px;">No album data available</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.map((album, index) => {
        const rank = index + 1;
        return `
        <tr style="${getGenderRowStyle(album.genderId)}">
            <td class="rank-col">${rank}</td>
            <td class="cover-col">
                <div style="cursor:pointer;">
                    <img src="/albums/${album.id}/image" alt="" class="clickable-image" onclick="openAlbumImageModalFromGraphs(${album.id})" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" style="width:50px;height:50px;object-fit:cover;border-radius:4px;">
                    <div style="display:none;width:50px;height:50px;background:#2a2a2a;border-radius:4px;align-items:center;justify-content:center;color:#666;font-size:14px;">💿</div>
                </div>
            </td>
            <td><a href="/artists/${album.artistId}">${escapeHtml(album.artistName || '-')}</a></td>
            <td><a href="/albums/${album.id}">${escapeHtml(album.name || '-')}</a></td>
            <td style="text-align:right;"${(album.plays || 0) >= 1000 ? ' class="high-plays"' : ''}>${(album.plays || 0).toLocaleString()}</td>
            <td style="text-align:right;">${(album.primaryPlays || 0).toLocaleString()}</td>
            <td style="text-align:right;">${(album.legacyPlays || 0).toLocaleString()}</td>
            <td style="text-align:right;">${album.lengthFormatted || '-'}</td>
            <td style="text-align:right;">${album.timeListenedFormatted || '-'}</td>
            <td>${album.releaseDate || '-'}</td>
            <td>${album.firstListened || '-'}</td>
            <td>${album.lastListened || '-'}</td>
        </tr>
    `}).join('');

    updateSortIndicators('topAlbumsTable', topSortState.albums);
}

/**
 * Renders the Top Songs table
 */
function renderTopSongsTable() {
    const tbody = document.querySelector('#topSongsTable tbody');
    if (!tbody) return;
    
    const data = sortTopData(topTabData.songs, topSortState.songs);
    
    // Render podium with sorted data
    renderSongsPodium(data);

    if (data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="13" style="text-align: center; color: #666; padding: 20px;">No song data available</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.map((song, index) => {
        const rank = index + 1;
        let coverHtml;
        if (song.hasImage && song.albumHasImage && song.albumId) {
            // Case 1: Song has image AND album has image - show album by default, song on hover
            coverHtml = `<div class="hover-image-container" style="display:block;position:relative;width:50px;height:50px;cursor:pointer;">
                <img src="/albums/${song.albumId}/image" alt="" class="album-image-default clickable-image" onclick="openSongImageModalFromGraphs(${song.id})" style="position:absolute;top:0;left:0;width:50px;height:50px;object-fit:cover;border-radius:4px;opacity:1;transition:opacity 0.2s;z-index:2;">
                <img src="/songs/${song.id}/image" alt="" class="song-image-hover clickable-image" onclick="openSongImageModalFromGraphs(${song.id})" style="position:absolute;top:0;left:0;width:50px;height:50px;object-fit:cover;border-radius:4px;opacity:0;transition:opacity 0.2s;z-index:1;">
            </div>`;
        } else if (song.hasImage) {
            // Case 2: Song has image but album doesn't - just show song image
            coverHtml = `<div style="cursor:pointer;">
                <img src="/songs/${song.id}/image" alt="" class="clickable-image" onclick="openSongImageModalFromGraphs(${song.id})" style="width:50px;height:50px;object-fit:cover;border-radius:4px;">
            </div>`;
        } else if (song.albumId) {
            // Case 3: Song doesn't have image but album does - show album image
            coverHtml = `<div style="cursor:pointer;">
                   <img src="/albums/${song.albumId}/image" alt="" class="inherited-cover clickable-image" onclick="openAlbumImageModalFromGraphs(${song.albumId})" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" style="width:50px;height:50px;object-fit:cover;border-radius:4px;">
                   <div style="display:none;width:50px;height:50px;background:#2a2a2a;border-radius:4px;align-items:center;justify-content:center;color:#666;font-size:14px;">💿</div>
               </div>`;
        } else {
            // Case 4: No image available
            coverHtml = `<div style="width:50px;height:50px;background:#2a2a2a;border-radius:4px;display:flex;align-items:center;justify-content:center;color:#666;font-size:14px;">🎵</div>`;
        }
        // Build row classes for featured/group styling
        const rowClasses = [];
        if (song.featuredOn) rowClasses.push('featured-song-row');
        if (song.fromGroup) rowClasses.push('group-item-row');
        const rowClassAttr = rowClasses.length > 0 ? ` class="${rowClasses.join(' ')}"` : '';
        
        // Build featured/group labels
        let songLabel = '';
        if (song.featuredOn && song.primaryArtistName) {
            songLabel = ` <span class="featured-artist-label">(feat. on <a href="/artists/${song.primaryArtistId}">${escapeHtml(song.primaryArtistName)}</a>)</span>`;
        } else if (song.fromGroup && song.sourceArtistName) {
            songLabel = ` <span class="group-artist-label">(by <a href="/artists/${song.sourceArtistId}">${escapeHtml(song.sourceArtistName)}</a>)</span>`;
        }
        
        return `
        <tr${rowClassAttr} style="${getGenderRowStyle(song.genderId)}">
            <td class="rank-col">${rank}</td>
            <td class="cover-col">${coverHtml}</td>
            <td><a href="/artists/${song.artistId}">${escapeHtml(song.artistName || '-')}</a></td>
            <td>${song.albumId ? `<a href="/albums/${song.albumId}">${escapeHtml(song.albumName)}</a>` : '-'}</td>
            <td><a href="/songs/${song.id}">${escapeHtml(song.name || '-')}</a>${song.isSingle ? ' <span class="single-indicator" title="Single">🔹</span>' : ''}${songLabel}</td>
            <td style="text-align:right;"${(song.plays || 0) >= 100 ? ' class="high-plays"' : ''}>${(song.plays || 0).toLocaleString()}</td>
            <td style="text-align:right;">${(song.primaryPlays || 0).toLocaleString()}</td>
            <td style="text-align:right;">${(song.legacyPlays || 0).toLocaleString()}</td>
            <td style="text-align:right;">${song.lengthFormatted || '-'}</td>
            <td style="text-align:right;">${song.timeListenedFormatted || '-'}</td>
            <td>${song.releaseDate || '-'}</td>
            <td>${song.firstListened || '-'}</td>
            <td>${song.lastListened || '-'}</td>
        </tr>
    `}).join('');
    
    // Add hover event listeners for image swap
    tbody.querySelectorAll('.hover-image-container').forEach(container => {
        container.addEventListener('mouseenter', function() {
            const albumImg = this.querySelector('.album-image-default');
            const songImg = this.querySelector('.song-image-hover');
            if (albumImg) albumImg.style.opacity = '0';
            if (songImg) songImg.style.opacity = '1';
        });
        container.addEventListener('mouseleave', function() {
            const albumImg = this.querySelector('.album-image-default');
            const songImg = this.querySelector('.song-image-hover');
            if (albumImg) albumImg.style.opacity = '1';
            if (songImg) songImg.style.opacity = '0';
        });
    });
    
    updateSortIndicators('topSongsTable', topSortState.songs);
}

/**
 * Sorts top data array by the specified column and direction
 */
function sortTopData(data, sortState) {
    if (!data || data.length === 0) return [];
    
    const sorted = [...data];
    const { column, direction } = sortState;
    
    sorted.sort((a, b) => {
        let aVal = a[column];
        let bVal = b[column];
        
        // Handle null/undefined values
        if (aVal == null) aVal = '';
        if (bVal == null) bVal = '';
        
        // Numeric columns
        if (column === 'plays' || column === 'primaryPlays' || column === 'legacyPlays' || column === 'timeListened') {
            aVal = Number(aVal) || 0;
            bVal = Number(bVal) || 0;
        } else if (column === 'releaseDate' || column === 'firstListened' || column === 'lastListened') {
            // Parse date strings like "1 Feb 2012" or "15 Sep 2006"
            aVal = parseDisplayDate(aVal);
            bVal = parseDisplayDate(bVal);
        } else if (typeof aVal === 'string') {
            aVal = aVal.toLowerCase();
            bVal = bVal.toLowerCase();
        }
        
        let result = 0;
        if (aVal < bVal) result = -1;
        else if (aVal > bVal) result = 1;
        
        return direction === 'desc' ? -result : result;
    });
    
    return sorted;
}

/**
 * Parses a display date string like "1 Feb 2012" into a sortable timestamp
 */
function parseDisplayDate(dateStr) {
    if (!dateStr || dateStr === '') return 0;
    
    const months = {
        'jan': 0, 'feb': 1, 'mar': 2, 'apr': 3, 'may': 4, 'jun': 5,
        'jul': 6, 'aug': 7, 'sep': 8, 'oct': 9, 'nov': 10, 'dec': 11
    };
    
    const parts = dateStr.trim().split(' ');
    if (parts.length === 3) {
        const day = parseInt(parts[0], 10);
        const month = months[parts[1].toLowerCase().substring(0, 3)];
        const year = parseInt(parts[2], 10);
        if (!isNaN(day) && month !== undefined && !isNaN(year)) {
            return new Date(year, month, day).getTime();
        }
    }
    
    return 0;
}

/**
 * Updates sort indicators on table headers
 */
function updateSortIndicators(tableId, sortState) {
    const table = document.getElementById(tableId);
    if (!table) return;
    
    // Remove existing sort classes
    table.querySelectorAll('th').forEach(th => {
        th.classList.remove('sorted-asc', 'sorted-desc');
    });
    
    // Add sort class to current column
    const sortedTh = table.querySelector(`th[data-sort="${sortState.column}"]`);
    if (sortedTh) {
        sortedTh.classList.add(sortState.direction === 'asc' ? 'sorted-asc' : 'sorted-desc');
    }
}

/**
 * Sets up click handlers for table header sorting
 */
function setupTopTableSorting() {
    // Artists table
    document.querySelectorAll('#topArtistsTable th[data-sort]').forEach(th => {
        th.onclick = () => {
            const column = th.dataset.sort;
            if (topSortState.artists.column === column) {
                topSortState.artists.direction = topSortState.artists.direction === 'asc' ? 'desc' : 'asc';
            } else {
                topSortState.artists.column = column;
                topSortState.artists.direction = column === 'plays' || column === 'timeListened' ? 'desc' : 'asc';
            }
            renderTopArtistsTable();
        };
    });
    
    // Albums table
    document.querySelectorAll('#topAlbumsTable th[data-sort]').forEach(th => {
        th.onclick = () => {
            const column = th.dataset.sort;
            if (topSortState.albums.column === column) {
                topSortState.albums.direction = topSortState.albums.direction === 'asc' ? 'desc' : 'asc';
            } else {
                topSortState.albums.column = column;
                topSortState.albums.direction = column === 'plays' || column === 'timeListened' ? 'desc' : 'asc';
            }
            renderTopAlbumsTable();
        };
    });
    
    // Songs table
    document.querySelectorAll('#topSongsTable th[data-sort]').forEach(th => {
        th.onclick = () => {
            const column = th.dataset.sort;
            if (topSortState.songs.column === column) {
                topSortState.songs.direction = topSortState.songs.direction === 'asc' ? 'desc' : 'asc';
            } else {
                topSortState.songs.column = column;
                topSortState.songs.direction = column === 'plays' || column === 'timeListened' ? 'desc' : 'asc';
            }
            renderTopSongsTable();
        };
    });
}

/**
 * Reloads top data when the limit changes
 */
function reloadTopData() {
    loadedTabs.top = false;
    loadTabData('top', true);
}

/**
 * Gets the current top limit value
 */
function getTopLimit() {
    const input = document.getElementById('topLimitInput');
    return input ? parseInt(input.value) || 50 : 50;
}

/**
 * Handles when the apply limit checkbox changes on any tab - reloads that tab with new limit setting
 */
function onApplyLimitChanged(tabName) {
    // Reload the tab with the new apply limit setting
    loadTabData(tabName, true);
}

/**
 * Switches between Artists, Albums, and Songs sub-tabs in the Top tab
 */
function switchTopSubTab(subtab) {
    // Update button states
    const buttons = document.querySelectorAll('.top-subtabs .charts-tab-btn');
    buttons.forEach(btn => {
        if (btn.getAttribute('data-subtab') === subtab) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });
    
    // Show/hide content sections
    const sections = document.querySelectorAll('.top-subtab-content');
    sections.forEach(section => {
        section.style.display = 'none';
    });
    
    const activeSection = document.getElementById('top-subtab-' + subtab);
    if (activeSection) {
        activeSection.style.display = 'block';
    }
    
    // Show/hide the Generate Playlist button in the header based on sub-tab
    const headerPlaylistBtn = document.getElementById('headerPlaylistBtn');
    if (headerPlaylistBtn) {
        headerPlaylistBtn.style.display = (subtab === 'songs') ? '' : 'none';
    }
}

/**
 * Escapes HTML special characters
 */
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Gallery-aware image modal functions for graphs page
function openArtistImageModalFromGraphs(artistId) {
    fetch(`/artists/${artistId}/images`)
        .then(response => response.json())
        .then(data => {
            const imageIds = data.map(img => img.id);
            const hasDefaultImage = true;
            
            let artistGalleryImages = hasDefaultImage ? [0, ...imageIds] : (imageIds.length > 0 ? imageIds : [0]);
            
            if (artistGalleryImages.length > 1) {
                const galleryContext = {
                    images: artistGalleryImages,
                    entityType: 'artists',
                    entityId: artistId,
                    currentIndex: 0
                };
                openImageModal(`/artists/${artistId}/image`, galleryContext);
            } else {
                openImageModal(`/artists/${artistId}/image`);
            }
        })
        .catch(error => {
            console.error('Error loading artist gallery:', error);
            openImageModal(`/artists/${artistId}/image`);
        });
}

function openAlbumImageModalFromGraphs(albumId) {
    fetch(`/albums/${albumId}/images`)
        .then(response => response.json())
        .then(data => {
            const imageIds = data.map(img => img.id);
            const hasDefaultImage = true;
            
            let albumGalleryImages = hasDefaultImage ? [0, ...imageIds] : (imageIds.length > 0 ? imageIds : [0]);
            
            if (albumGalleryImages.length > 1) {
                const galleryContext = {
                    images: albumGalleryImages,
                    entityType: 'albums',
                    entityId: albumId,
                    currentIndex: 0
                };
                openImageModal(`/albums/${albumId}/image`, galleryContext);
            } else {
                openImageModal(`/albums/${albumId}/image`);
            }
        })
        .catch(error => {
            console.error('Error loading album gallery:', error);
            openImageModal(`/albums/${albumId}/image`);
        });
}

function openSongImageModalFromGraphs(songId) {
    fetch(`/songs/${songId}/images`)
        .then(response => response.json())
        .then(data => {
            const imageIds = data.map(img => img.id);
            const hasDefaultImage = true;
            
            let songGalleryImages = hasDefaultImage ? [0, ...imageIds] : (imageIds.length > 0 ? imageIds : [0]);
            
            if (songGalleryImages.length > 1) {
                const galleryContext = {
                    images: songGalleryImages,
                    entityType: 'songs',
                    entityId: songId,
                    currentIndex: 0
                };
                openImageModal(`/songs/${songId}/image`, galleryContext);
            } else {
                openImageModal(`/songs/${songId}/image`);
            }
        })
        .catch(error => {
            console.error('Error loading song gallery:', error);
            openImageModal(`/songs/${songId}/image`);
        });
}

// Initialize modal close on click outside and read URL params for toggles
document.addEventListener('DOMContentLoaded', function() {
    const modal = document.getElementById('chartsModal');
    if (modal) {
        modal.addEventListener('click', function(e) {
            if (e.target === this) {
                closeChartsModal();
            }
        });
    }
    // Note: includeGroups and includeFeatured toggles are now handled as regular form inputs
    // They get their checked state from th:checked in the HTML and are submitted with the form
});
