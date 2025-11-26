/**
 * Shared Top Table Sorting Module
 * Used for sortable tables in Charts modal and Detail pages
 */

/**
 * Parses a display date string like "1 Feb 2012" into a sortable timestamp
 */
function parseDisplayDate(dateStr) {
    if (!dateStr || dateStr === '' || dateStr === '-') return 0;
    
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
function updateTopTableSortIndicators(tableId, column, direction) {
    const table = document.getElementById(tableId);
    if (!table) return;
    
    // Remove existing sort classes
    table.querySelectorAll('th').forEach(th => {
        th.classList.remove('sorted-asc', 'sorted-desc', 'sort-asc', 'sort-desc');
    });
    
    // Add sort class to current column
    const sortedTh = table.querySelector(`th[data-sort="${column}"]`);
    if (sortedTh) {
        sortedTh.classList.add('sorted-' + direction);
    }
}

/**
 * Initializes sorting on a top-table
 * @param {string} tableId - The ID of the table element
 * @param {Object} options - Configuration options
 *   @param {string} options.defaultColumn - Default column to sort by (e.g., 'totalplays')
 *   @param {string} options.defaultDirection - Default sort direction ('asc' or 'desc')
 *   @param {string[]} options.numericColumns - Array of column names that should be sorted numerically
 *   @param {string[]} options.dateColumns - Array of column names that contain dates
 *   @param {string|null} options.summaryRowId - ID of summary row to keep at bottom (or null)
 */
function initTopTableSort(tableId, options = {}) {
    const table = document.getElementById(tableId);
    if (!table) return;
    
    const defaults = {
        defaultColumn: 'totalplays',
        defaultDirection: 'desc',
        numericColumns: ['primaryplays', 'legacyplays', 'totalplays', 'timelistened'],
        dateColumns: ['releasedate', 'firstlistened', 'lastlistened'],
        summaryRowId: null
    };
    
    const config = { ...defaults, ...options };
    
    // Current sort state
    let currentSort = {
        column: config.defaultColumn,
        direction: config.defaultDirection
    };
    
    // Sorting function
    function sortTable() {
        const tbody = table.querySelector('tbody');
        if (!tbody) return;
        
        let rows = Array.from(tbody.querySelectorAll('tr'));
        let summaryRow = null;
        
        // Remove summary row from sorting
        if (config.summaryRowId) {
            summaryRow = rows.find(r => r.id === config.summaryRowId);
            rows = rows.filter(r => r.id !== config.summaryRowId);
        }
        
        // Filter out no-data rows
        rows = rows.filter(r => !r.classList.contains('no-data'));
        
        const column = currentSort.column;
        const direction = currentSort.direction;
        
        rows.sort((a, b) => {
            let aVal = a.dataset[column] || '';
            let bVal = b.dataset[column] || '';
            
            // Determine sort type
            if (config.numericColumns.includes(column)) {
                aVal = parseFloat(aVal) || 0;
                bVal = parseFloat(bVal) || 0;
            } else if (config.dateColumns.includes(column)) {
                aVal = parseDisplayDate(aVal);
                bVal = parseDisplayDate(bVal);
            } else {
                aVal = aVal.toString().toLowerCase();
                bVal = bVal.toString().toLowerCase();
            }
            
            let result = 0;
            if (aVal < bVal) result = -1;
            else if (aVal > bVal) result = 1;
            
            return direction === 'desc' ? -result : result;
        });
        
        // Re-append rows in sorted order
        rows.forEach(row => tbody.appendChild(row));
        
        // Append summary row at end
        if (summaryRow) {
            tbody.appendChild(summaryRow);
        }
        
        updateTopTableSortIndicators(tableId, currentSort.column, currentSort.direction);
    }
    
    // Setup click handlers for sortable headers
    table.querySelectorAll('th[data-sort]').forEach(th => {
        th.addEventListener('click', function() {
            const column = this.dataset.sort;
            
            if (currentSort.column === column) {
                // Toggle direction
                currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
            } else {
                currentSort.column = column;
                // Default to desc for numeric/time columns, asc for text
                if (config.numericColumns.includes(column) || column === 'timelistened') {
                    currentSort.direction = 'desc';
                } else {
                    currentSort.direction = 'asc';
                }
            }
            
            sortTable();
        });
    });
    
    // Apply initial sort
    sortTable();
    
    // Return function to allow external re-sorting
    return {
        sort: sortTable,
        setSort: function(column, direction) {
            currentSort.column = column;
            currentSort.direction = direction;
            sortTable();
        }
    };
}
