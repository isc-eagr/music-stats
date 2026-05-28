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

// Current view (card / table / graphs) - initialized here so it's always available
let currentView = 'card';
let listViewState = { page: 0, totalCount: 0, loading: false, allLoaded: false };
let listViewObserver = null;
let listViewScrollContainer = null;
let listViewScrollHandler = null;
let listViewRequestToken = 0;
let listViewLoadAllInProgress = false;
let listViewUserHasScrolled = false;

const validGraphsTabs = new Set(['general', 'genre', 'subgenre', 'ethnicity', 'language', 'country', 'releaseYear', 'listenYear']);

// Track which tabs have been loaded
let loadedTabs = {
    general: false,
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

// Infinite scroll state for top tables - tracks how many rows are currently displayed
let topInfiniteScrollState = {
    artists: { displayedRows: 100, batchSize: 50 },
    albums: { displayedRows: 100, batchSize: 50 },
    songs: { displayedRows: 100, batchSize: 50 }
};

// Cached sorted data - updated every time a render function runs so that
// infinite scroll append pulls rows in the correct sort order
let topSortedData = {
    artists: [],
    albums: [],
    songs: []
};

// Cache sorted arrays by source data reference + sort key to avoid expensive re-sorts
let topSortedCache = {
    artists: { dataRef: null, sortKey: '', sorted: [] },
    albums: { dataRef: null, sortKey: '', sorted: [] },
    songs: { dataRef: null, sortKey: '', sorted: [] }
};

// Current sort state for each table
let topSortState = {
    artists: [{ column: 'plays', direction: 'desc' }],
    albums: [{ column: 'plays', direction: 'desc' }],
    songs: [{ column: 'plays', direction: 'desc' }]
};

const listViewSortParamMap = {
    artists: {
        name: 'name',
        plays: 'plays',
        primaryPlays: 'primary_plays',
        legacyPlays: 'legacy_plays',
        timeListened: 'time',
        firstListened: 'first_listened',
        lastListened: 'last_listened',
        daysListened: 'days_listened',
        weeksListened: 'weeks_listened',
        monthsListened: 'months_listened',
        yearsListened: 'years_listened',
        age: 'age',
        albumCount: 'albums',
        avgLength: 'avg_length',
        avgPlays: 'avg_plays',
        avgPlaysAlbum: 'avg_plays_album',
        birthDate: 'birth_date',
        deathDate: 'death_date',
        songCount: 'songs',
        featuredOnCount: 'featured',
        featuredArtistCount: 'featured_artist_count',
        soloSongCount: 'solo_songs',
        songsWithFeatCount: 'songs_with_features',
        genre: 'genre',
        subgenre: 'subgenre',
        ethnicity: 'ethnicity',
        country: 'country',
        language: 'language'
    },
    albums: {
        artistName: 'artist',
        name: 'name',
        plays: 'plays',
        primaryPlays: 'primary_plays',
        legacyPlays: 'legacy_plays',
        length: 'album_length',
        timeListened: 'time',
        releaseDate: 'release_date',
        firstListened: 'first_listened',
        lastListened: 'last_listened',
        daysListened: 'days_listened',
        weeksListened: 'weeks_listened',
        monthsListened: 'months_listened',
        yearsListened: 'years_listened',
        lastFullListen: 'last_full_listen',
        ageAtRelease: 'age_at_release',
        avgLength: 'avg_length',
        avgPlays: 'avg_plays',
        seasonalChartPeak: 'seasonal_chart_peak',
        songCount: 'song_count',
        featuredArtistCount: 'featured_artist_count',
        soloSongCount: 'solo_songs',
        songsWithFeatCount: 'songs_with_features',
        weeklyChartPeak: 'weekly_chart_peak',
        weeklyChartWeeks: 'weekly_chart_weeks',
        yearlyChartPeak: 'yearly_chart_peak',
        genre: 'genre',
        subgenre: 'subgenre',
        ethnicity: 'ethnicity',
        country: 'country',
        language: 'language',
        birthDate: 'birth_date',
        deathDate: 'death_date'
    },
    songs: {
        artistName: 'artist',
        albumName: 'album',
        name: 'name',
        plays: 'plays',
        primaryPlays: 'primary_plays',
        legacyPlays: 'legacy_plays',
        length: 'length',
        timeListened: 'time',
        releaseDate: 'release_date',
        firstListened: 'first_listened',
        lastListened: 'last_listened',
        daysListened: 'days_listened',
        weeksListened: 'weeks_listened',
        monthsListened: 'months_listened',
        yearsListened: 'years_listened',
        trackNumber: 'track_number',
        ageAtRelease: 'age_at_release',
        featuredArtistCount: 'featured_artist_count',
        seasonalChartPeak: 'seasonal_chart_peak',
        weeklyChartPeak: 'weekly_chart_peak',
        weeklyChartWeeks: 'weekly_chart_weeks',
        trlPeak: 'trl_peak',
        trlDays: 'trl_days',
        vatosCuntdownPeak: 'vatos_cuntdown_peak',
        vatosCuntdownDays: 'vatos_cuntdown_days',
        billboardPeak: 'billboard_peak',
        billboardWeeks: 'billboard_weeks',
        yearlyChartPeak: 'yearly_chart_peak',
        genre: 'genre',
        subgenre: 'subgenre',
        ethnicity: 'ethnicity',
        country: 'country',
        language: 'language',
        birthDate: 'birth_date',
        deathDate: 'death_date'
    }
};

const listViewSortColumnMap = Object.fromEntries(
    Object.entries(listViewSortParamMap).map(([entityType, mapping]) => [
        entityType,
        Object.fromEntries(Object.entries(mapping).map(([column, sortParam]) => [sortParam, column]))
    ])
);

// Column visibility configuration for top tables
// Each column: { key: data field name, label: display name, defaultVisible: boolean, align: 'left'|'right' }
const topColumnConfig = {
    artists: [
        { key: 'plays', label: 'Total Plays', defaultVisible: true, align: 'right' },
        { key: 'primaryPlays', label: 'Primary Plays', defaultVisible: true, align: 'right' },
        { key: 'legacyPlays', label: 'Legacy Plays', defaultVisible: true, align: 'right' },
        { key: 'timeListened', label: 'Time Listened', defaultVisible: true, align: 'right' },
        { key: 'firstListened', label: 'First Listened', defaultVisible: true, align: 'left' },
        { key: 'lastListened', label: 'Last Listened', defaultVisible: true, align: 'left' },
        { key: 'daysListened', label: 'Days Listened', defaultVisible: false, align: 'right' },
        { key: 'weeksListened', label: 'Weeks Listened', defaultVisible: false, align: 'right' },
        { key: 'monthsListened', label: 'Months Listened', defaultVisible: false, align: 'right' },
        { key: 'yearsListened', label: 'Years Listened', defaultVisible: false, align: 'right' },
        { key: 'age', label: 'Age', defaultVisible: false, align: 'right' },
        { key: 'albumCount', label: 'Album Count', defaultVisible: false, align: 'right' },
        { key: 'avgLength', label: 'Avg Length', defaultVisible: false, align: 'right' },
        { key: 'avgPlays', label: 'Avg Plays / Song', defaultVisible: false, align: 'right' },
        { key: 'avgPlaysAlbum', label: 'Avg Plays / Album', defaultVisible: false, align: 'right' },
        { key: 'birthDate', label: 'Birth Date', defaultVisible: false, align: 'left' },
        { key: 'deathDate', label: 'Death Date', defaultVisible: false, align: 'left' },
        { key: 'songCount', label: 'Song Count', defaultVisible: false, align: 'right' },
        { key: 'featuredOnCount', label: 'Featured Song Count', defaultVisible: false, align: 'right' },
        { key: 'featuredArtistCount', label: 'Featured Artist Count', defaultVisible: false, align: 'right' },
        { key: 'soloSongCount', label: 'Solo Songs', defaultVisible: false, align: 'right' },
        { key: 'songsWithFeatCount', label: 'Songs w/ Features', defaultVisible: false, align: 'right' },
        { key: 'genre', label: 'Genre', defaultVisible: false, align: 'left' },
        { key: 'subgenre', label: 'Subgenre', defaultVisible: false, align: 'left' },
        { key: 'ethnicity', label: 'Ethnicity', defaultVisible: false, align: 'left' },
        { key: 'country', label: 'Country', defaultVisible: false, align: 'left' },
        { key: 'language', label: 'Language', defaultVisible: false, align: 'left' }
    ],
    albums: [
        { key: 'plays', label: 'Total Plays', defaultVisible: true, align: 'right' },
        { key: 'primaryPlays', label: 'Primary Plays', defaultVisible: true, align: 'right' },
        { key: 'legacyPlays', label: 'Legacy Plays', defaultVisible: true, align: 'right' },
        { key: 'length', label: 'Length', defaultVisible: true, align: 'right' },
        { key: 'timeListened', label: 'Time Listened', defaultVisible: true, align: 'right' },
        { key: 'releaseDate', label: 'Release Date', defaultVisible: true, align: 'left' },
        { key: 'firstListened', label: 'First Listened', defaultVisible: true, align: 'left' },
        { key: 'lastListened', label: 'Last Listened', defaultVisible: true, align: 'left' },
        { key: 'daysListened', label: 'Days Listened', defaultVisible: false, align: 'right' },
        { key: 'weeksListened', label: 'Weeks Listened', defaultVisible: false, align: 'right' },
        { key: 'monthsListened', label: 'Months Listened', defaultVisible: false, align: 'right' },
        { key: 'yearsListened', label: 'Years Listened', defaultVisible: false, align: 'right' },
        { key: 'lastFullListen', label: 'Last Full Listen', defaultVisible: false, align: 'left' },
        { key: 'ageAtRelease', label: 'Age at Release', defaultVisible: false, align: 'right' },
        { key: 'avgLength', label: 'Avg Length', defaultVisible: false, align: 'right' },
        { key: 'avgPlays', label: 'Avg Plays', defaultVisible: false, align: 'right' },
        { key: 'seasonalChartPeak', label: 'Seasonal Peak', defaultVisible: false, align: 'right' },
        { key: 'songCount', label: 'Song Count', defaultVisible: false, align: 'right' },
        { key: 'featuredArtistCount', label: 'Featured Artist Count', defaultVisible: false, align: 'right' },
        { key: 'soloSongCount', label: 'Solo Songs', defaultVisible: false, align: 'right' },
        { key: 'songsWithFeatCount', label: 'Songs w/ Features', defaultVisible: false, align: 'right' },
        { key: 'weeklyChartPeak', label: 'Weekly Peak', defaultVisible: false, align: 'right' },
        { key: 'weeklyChartWeeks', label: 'Weekly Weeks', defaultVisible: false, align: 'right' },
        { key: 'yearlyChartPeak', label: 'Yearly Peak', defaultVisible: false, align: 'right' },
        { key: 'genre', label: 'Genre', defaultVisible: false, align: 'left' },
        { key: 'subgenre', label: 'Subgenre', defaultVisible: false, align: 'left' },
        { key: 'ethnicity', label: 'Ethnicity', defaultVisible: false, align: 'left' },
        { key: 'country', label: 'Country', defaultVisible: false, align: 'left' },
        { key: 'language', label: 'Language', defaultVisible: false, align: 'left' }
    ],
    songs: [
        { key: 'billboardPeak', label: 'Billboard Peak', defaultVisible: false, align: 'right' },
        { key: 'billboardWeeks', label: 'Billboard Weeks', defaultVisible: false, align: 'right' },
        { key: 'plays', label: 'Total Plays', defaultVisible: true, align: 'right' },
        { key: 'primaryPlays', label: 'Primary Plays', defaultVisible: true, align: 'right' },
        { key: 'legacyPlays', label: 'Legacy Plays', defaultVisible: true, align: 'right' },
        { key: 'length', label: 'Length', defaultVisible: true, align: 'right' },
        { key: 'timeListened', label: 'Time Listened', defaultVisible: true, align: 'right' },
        { key: 'releaseDate', label: 'Release Date', defaultVisible: true, align: 'left' },
        { key: 'firstListened', label: 'First Listened', defaultVisible: true, align: 'left' },
        { key: 'lastListened', label: 'Last Listened', defaultVisible: true, align: 'left' },
        { key: 'daysListened', label: 'Days Listened', defaultVisible: false, align: 'right' },
        { key: 'weeksListened', label: 'Weeks Listened', defaultVisible: false, align: 'right' },
        { key: 'monthsListened', label: 'Months Listened', defaultVisible: false, align: 'right' },
        { key: 'yearsListened', label: 'Years Listened', defaultVisible: false, align: 'right' },
        { key: 'trackNumber', label: 'Track #', defaultVisible: false, align: 'right' },
        { key: 'ageAtRelease', label: 'Age at Release', defaultVisible: false, align: 'right' },
        { key: 'featuredArtistCount', label: 'Featured Artist Count', defaultVisible: false, align: 'right' },
        { key: 'seasonalChartPeak', label: 'Seasonal Peak', defaultVisible: false, align: 'right' },
        { key: 'trlDays', label: 'TRL Days', defaultVisible: false, align: 'right' },
        { key: 'trlPeak', label: 'TRL Peak', defaultVisible: false, align: 'right' },
        { key: 'vatosCuntdownDays', label: 'Vato\'s Days', defaultVisible: false, align: 'right' },
        { key: 'vatosCuntdownPeak', label: 'Vato\'s Peak', defaultVisible: false, align: 'right' },
        { key: 'weeklyChartPeak', label: 'Weekly Peak', defaultVisible: false, align: 'right' },
        { key: 'weeklyChartWeeks', label: 'Weekly Weeks', defaultVisible: false, align: 'right' },
        { key: 'yearlyChartPeak', label: 'Yearly Peak', defaultVisible: false, align: 'right' },
        { key: 'genre', label: 'Genre', defaultVisible: false, align: 'left' },
        { key: 'subgenre', label: 'Subgenre', defaultVisible: false, align: 'left' },
        { key: 'ethnicity', label: 'Ethnicity', defaultVisible: false, align: 'left' },
        { key: 'country', label: 'Country', defaultVisible: false, align: 'left' },
        { key: 'language', label: 'Language', defaultVisible: false, align: 'left' }
    ]
};

// Track current column visibility state
let topColumnVisibility = {
    artists: {},
    albums: {},
    songs: {}
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

// Register datalabels plugin if Chart.js v3+ is available
if (typeof Chart !== 'undefined' && typeof Chart.register === 'function' && typeof ChartDataLabels !== 'undefined') {
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
    syncGraphsViewUrl(currentView, tabName);
    
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
    
    const checkboxId = 'applyLimit' + tabName.charAt(0).toUpperCase() + tabName.slice(1);
    const checkbox = document.getElementById(checkboxId);
    if (checkbox && checkbox.checked) {
        params.set('limit', getTopLimit());
    }
    
    const apiUrl = getChartsApiBase() + '/' + tabName + '?' + params.toString();
    
    fetch(apiUrl)
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok: ' + response.status);
            }
            return response.json();
        })
        .then(data => {
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
 * Gets filter parameters from current URL.
 */
function getFilterParams() {
    const currentUrl = new URL(window.location.href);
    const params = new URLSearchParams(currentUrl.search);
    
    // Remove pagination and sorting params, keep only filters
    params.delete('page');
    params.delete('perpage');
    params.delete('sortby');
    params.delete('sortdir');
    
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
    container.style.height = calculatedHeight + 'px';
    const canvas = document.createElement('canvas');
    canvas.id = canvasId;
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
    const itemHeight = 160; // Height per category group
    const baseHeight = 150;
    const calculatedHeight = categories.length * itemHeight + baseHeight;
    
    // Create canvas element
    container.innerHTML = '';
    container.style.height = calculatedHeight + 'px';
    const canvas = document.createElement('canvas');
    canvas.id = canvasId;
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
    
// Gray colors for inactive metrics in relative scaling mode
        const grayColors = {
            male:   'rgba(70,  70,  70,  0.9)',  // darkest gray
            female: 'rgba(120, 120, 120, 0.9)',  // medium gray
            other:  'rgba(170, 170, 170, 0.9)'   // lightest gray
        };

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
        
        const totals = categories.map((cat, i) => maleData[i] + femaleData[i] + otherData[i]);
        
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
        
        // When relative scaling is active, only the sort metric keeps its colors; others go gray
        const isActiveMetric = !useRelativeScaling || metric.key === chartSortMetric;
        const maleColor   = isActiveMetric ? metricColors[metric.key].male   : grayColors.male;
        const femaleColor = isActiveMetric ? metricColors[metric.key].female : grayColors.female;
        const otherColor  = isActiveMetric ? metricColors[metric.key].other  : grayColors.other;

        // Male bar for this metric
        datasets.push({
            label: metric.label + ' (M)',
            data: malePercentages,
            backgroundColor: maleColor,
            borderColor: maleColor.replace('0.9', '1'),
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
            backgroundColor: femaleColor,
            borderColor: femaleColor.replace('0.9', '1'),
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
                backgroundColor: otherColor,
                borderColor: otherColor.replace('0.9', '1'),
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
        btn.textContent = useRelativeScaling ? 'Relative Scaling (ON)' : 'Relative Scaling (OFF)';
    }

    const checkbox = document.getElementById('relativeScalingToggle');
    if (checkbox) {
        checkbox.checked = useRelativeScaling;
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
 * Initialize column toggle checkboxes for all top sub-tabs
 */
function initColumnToggles() {
    ['artists', 'albums', 'songs'].forEach(type => {
        const singularType = type === 'artists' ? 'artist' : type === 'albums' ? 'album' : 'song';
        const container = document.getElementById(singularType + 'ColumnToggles');
        if (!container) return;
        
        const config = topColumnConfig[type];
        // Preserve any user-selected visibility state across table reloads/sorts.
        const existingVisibility = topColumnVisibility[type] || {};
        topColumnVisibility[type] = {};
        config.forEach(col => {
            topColumnVisibility[type][col.key] = Object.prototype.hasOwnProperty.call(existingVisibility, col.key)
                ? existingVisibility[col.key]
                : col.defaultVisible;
        });
        
        // Sort checkboxes alphabetically by label
        const sortedConfig = [...config].sort((a, b) => a.label.localeCompare(b.label));

        // Build checkboxes preceded by a select-all / deselect-all row
        container.innerHTML = `<div class="column-toggle-all-row">
                <a href="#" onclick="selectAllColumns('${type}', true); return false;">Select All</a>
                &nbsp;/&nbsp;
                <a href="#" onclick="selectAllColumns('${type}', false); return false;">Deselect All</a>
            </div>` +
            sortedConfig.map(col => `
            <label>
                <input type="checkbox" data-type="${type}" data-col="${col.key}" 
                       ${topColumnVisibility[type][col.key] ? 'checked' : ''} 
                       onchange="onColumnToggle('${type}', '${col.key}', this.checked)">
                <span>${col.label}</span>
            </label>
        `).join('');
    });
}

/**
 * Select or deselect all column toggles for a given type
 */
function selectAllColumns(type, visible) {
    const singularType = type === 'artists' ? 'artist' : type === 'albums' ? 'album' : 'song';
    const container = document.getElementById(singularType + 'ColumnToggles');
    if (container) {
        container.querySelectorAll('input[type="checkbox"]').forEach(cb => {
            cb.checked = visible;
        });
    }
    topColumnConfig[type].forEach(col => {
        topColumnVisibility[type][col.key] = visible;
        onColumnToggle(type, col.key, visible);
    });
}

/**
 * Toggle the column panel expand/collapse
 */
function toggleColumnPanel(type) {
    const arrow = document.getElementById(type + 'ToggleArrow');
    const grid = document.getElementById(type + 'ColumnToggles');
    if (!arrow || !grid) return;
    
    const isExpanded = grid.classList.contains('expanded');
    if (isExpanded) {
        grid.classList.remove('expanded');
        arrow.classList.remove('expanded');
    } else {
        grid.classList.add('expanded');
        arrow.classList.add('expanded');
    }
}

/**
 * Handle column visibility toggle
 */
function onColumnToggle(type, colKey, visible) {
    topColumnVisibility[type][colKey] = visible;
    
    // Update header visibility
    const tableId = type === 'artists' ? 'topArtistsTable' : type === 'albums' ? 'topAlbumsTable' : 'topSongsTable';
    const table = document.getElementById(tableId);
    if (!table) return;
    
    // Find the column index by data-col attribute
    const th = table.querySelector(`th[data-col="${colKey}"]`);
    if (!th) return;
    
    const colIndex = Array.from(th.parentElement.children).indexOf(th);
    
    // Show/hide the th
    th.style.display = visible ? '' : 'none';
    
    // Show/hide all td cells in that column
    const rows = table.querySelectorAll('tbody tr');
    rows.forEach(row => {
        const td = row.children[colIndex];
        if (td) td.style.display = visible ? '' : 'none';
    });
}

/**
 * Get the display style for a column based on visibility
 */
function getColDisplay(type, colKey) {
    return topColumnVisibility[type] && topColumnVisibility[type][colKey] ? '' : 'none';
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
 * Generates HTML for a cell value, handling nulls
 */
function cellVal(val, fallback) {
    if (val == null || val === '') return fallback || '-';
    if (typeof val === 'number') return val.toLocaleString();
    return val;
}

/**
 * Builds a single artist row for the top table
 */
function buildArtistRow(artist, rank) {
    const vis = (col) => getColDisplay('artists', col);
    return `
        <tr style="${getGenderRowStyle(artist.genderId)}">
            <td class="rank-col">${rank}</td>
            <td class="cover-col artist-cover">
                <div style="cursor:pointer;">
                    <img src="/artists/${artist.id}/image" alt="" class="clickable-image" onclick="openArtistImageModalFromGraphs(${artist.id})" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" style="width:40px;height:60px;object-fit:cover;border-radius:4px;">
                    <div style="display:none;width:40px;height:60px;background:#2a2a2a;border-radius:4px;align-items:center;justify-content:center;color:#666;font-size:14px;">&#127908;</div>
                </div>
            </td>
            <td><a href="/artists/${artist.id}">${escapeHtml(artist.name || '-')}</a></td>
            <td style="text-align:right;display:${vis('plays')};"${(artist.plays || 0) >= 1000 ? ' class="high-plays"' : ''}>${(artist.plays || 0).toLocaleString()}</td>
            <td style="text-align:right;display:${vis('primaryPlays')};">${(artist.primaryPlays || 0).toLocaleString()}</td>
            <td style="text-align:right;display:${vis('legacyPlays')};">${(artist.legacyPlays || 0).toLocaleString()}</td>
            <td style="text-align:right;display:${vis('timeListened')};">${artist.timeListenedFormatted || '-'}</td>
            <td style="display:${vis('firstListened')};">${artist.firstListened || '-'}</td>
            <td style="display:${vis('lastListened')};">${artist.lastListened || '-'}</td>
            <td style="text-align:right;display:${vis('daysListened')};">${cellVal(artist.daysListened)}</td>
            <td style="text-align:right;display:${vis('weeksListened')};">${cellVal(artist.weeksListened)}</td>
            <td style="text-align:right;display:${vis('monthsListened')};">${cellVal(artist.monthsListened)}</td>
            <td style="text-align:right;display:${vis('yearsListened')};">${cellVal(artist.yearsListened)}</td>
            <td style="text-align:right;display:${vis('age')};">${cellVal(artist.age)}</td>
            <td style="text-align:right;display:${vis('albumCount')};">${cellVal(artist.albumCount)}</td>
            <td style="text-align:right;display:${vis('avgLength')};">${artist.avgLengthFormatted || '-'}</td>
            <td style="text-align:right;display:${vis('avgPlays')};">${cellVal(artist.avgPlays)}</td>
            <td style="text-align:right;display:${vis('avgPlaysAlbum')};">${cellVal(artist.avgPlaysAlbum)}</td>
            <td style="display:${vis('birthDate')};">${artist.birthDate || '-'}</td>
            <td style="display:${vis('deathDate')};">${artist.deathDate || '-'}</td>
            <td style="text-align:right;display:${vis('songCount')};">${cellVal(artist.songCount)}</td>
            <td style="text-align:right;display:${vis('featuredOnCount')};">${cellVal(artist.featuredOnCount)}</td>
            <td style="text-align:right;display:${vis('featuredArtistCount')};">${cellVal(artist.featuredArtistCount)}</td>
            <td style="text-align:right;display:${vis('soloSongCount')};">${cellVal(artist.soloSongCount)}</td>
            <td style="text-align:right;display:${vis('songsWithFeatCount')};">${cellVal(artist.songsWithFeatCount)}</td>
            <td style="display:${vis('genre')};">${artist.genre || '-'}</td>
            <td style="display:${vis('subgenre')};">${artist.subgenre || '-'}</td>
            <td style="display:${vis('ethnicity')};">${artist.ethnicity || '-'}</td>
            <td style="display:${vis('country')};">${artist.country || '-'}</td>
            <td style="display:${vis('language')};">${artist.language || '-'}</td>
        </tr>`;
}

/**
 * Renders the Top Artists table
 */
function renderTopArtistsTable() {
    const tbody = document.querySelector('#topArtistsTable tbody');
    if (!tbody) return;
    
    const data = getRenderedTopData('artists', topTabData.artists, topSortState.artists);
    topSortedData.artists = data;
    
    // Render podium with sorted data
    renderArtistsPodium(data);

    if (data.length === 0) {
        const colCount = document.querySelectorAll('#topArtistsTable thead th').length;
        tbody.innerHTML = `<tr><td colspan="${colCount}" style="text-align: center; color: #666; padding: 20px;">No artist data available</td></tr>`;
        return;
    }
    
    const rowsToRender = data.slice(0, topInfiniteScrollState.artists.displayedRows);
    
    tbody.innerHTML = rowsToRender.map((artist, index) => buildArtistRow(artist, index + 1)).join('');

    updateSortIndicators('topArtistsTable', topSortState.artists);
}

/**
 * Renders the Top Albums table
 */
/**
 * Builds a single album row for the top table
 */
function buildAlbumRow(album, rank) {
    const vis = (col) => getColDisplay('albums', col);
    return `
        <tr style="${getGenderRowStyle(album.genderId)}">
            <td class="rank-col">${rank}</td>
            <td class="cover-col">
                <div style="cursor:pointer;">
                    <img src="/albums/${album.id}/image" alt="" class="clickable-image" onclick="openAlbumImageModalFromGraphs(${album.id})" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" style="width:50px;height:50px;object-fit:cover;border-radius:4px;">
                    <div style="display:none;width:50px;height:50px;background:#2a2a2a;border-radius:4px;align-items:center;justify-content:center;color:#666;font-size:14px;">&#128191;</div>
                </div>
            </td>
            <td><a href="/artists/${album.artistId}">${escapeHtml(album.artistName || '-')}</a></td>
            <td><a href="/albums/${album.id}">${escapeHtml(album.name || '-')}</a></td>
            <td style="text-align:right;display:${vis('plays')};"${(album.plays || 0) >= 500 ? ' class="high-plays"' : ''}>${(album.plays || 0).toLocaleString()}</td>
            <td style="text-align:right;display:${vis('primaryPlays')};">${(album.primaryPlays || 0).toLocaleString()}</td>
            <td style="text-align:right;display:${vis('legacyPlays')};">${(album.legacyPlays || 0).toLocaleString()}</td>
            <td style="text-align:right;display:${vis('length')};">${album.lengthFormatted || '-'}</td>
            <td style="text-align:right;display:${vis('timeListened')};">${album.timeListenedFormatted || '-'}</td>
            <td style="display:${vis('releaseDate')};">${album.releaseDate || '-'}</td>
            <td style="display:${vis('firstListened')};">${album.firstListened || '-'}</td>
            <td style="display:${vis('lastListened')};">${album.lastListened || '-'}</td>
            <td style="text-align:right;display:${vis('daysListened')};">${cellVal(album.daysListened)}</td>
            <td style="text-align:right;display:${vis('weeksListened')};">${cellVal(album.weeksListened)}</td>
            <td style="text-align:right;display:${vis('monthsListened')};">${cellVal(album.monthsListened)}</td>
            <td style="text-align:right;display:${vis('yearsListened')};">${cellVal(album.yearsListened)}</td>
            <td style="display:${vis('lastFullListen')};">${album.lastFullListen || '-'}</td>
            <td style="text-align:right;display:${vis('ageAtRelease')};">${cellVal(album.ageAtRelease)}</td>
            <td style="text-align:right;display:${vis('avgLength')};">${album.avgLengthFormatted || '-'}</td>
            <td style="text-align:right;display:${vis('avgPlays')};">${cellVal(album.avgPlays)}</td>
            <td style="text-align:right;display:${vis('seasonalChartPeak')};">${cellVal(album.seasonalChartPeak)}</td>
            <td style="text-align:right;display:${vis('songCount')};">${cellVal(album.songCount)}</td>
            <td style="text-align:right;display:${vis('featuredArtistCount')};">${cellVal(album.featuredArtistCount)}</td>
            <td style="text-align:right;display:${vis('soloSongCount')};">${cellVal(album.soloSongCount)}</td>
            <td style="text-align:right;display:${vis('songsWithFeatCount')};">${cellVal(album.songsWithFeatCount)}</td>
            <td style="text-align:right;display:${vis('weeklyChartPeak')};">${cellVal(album.weeklyChartPeak)}</td>
            <td style="text-align:right;display:${vis('weeklyChartWeeks')};">${cellVal(album.weeklyChartWeeks)}</td>
            <td style="text-align:right;display:${vis('yearlyChartPeak')};">${cellVal(album.yearlyChartPeak)}</td>
            <td style="display:${vis('genre')};">${album.genre || '-'}</td>
            <td style="display:${vis('subgenre')};">${album.subgenre || '-'}</td>
            <td style="display:${vis('ethnicity')};">${album.ethnicity || '-'}</td>
            <td style="display:${vis('country')};">${album.country || '-'}</td>
            <td style="display:${vis('language')};">${album.language || '-'}</td>
        </tr>`;
}

function renderTopAlbumsTable() {
    const tbody = document.querySelector('#topAlbumsTable tbody');
    if (!tbody) return;
    
    const data = getRenderedTopData('albums', topTabData.albums, topSortState.albums);
    topSortedData.albums = data;
    
    // Render podium with sorted data
    renderAlbumsPodium(data);

    if (data.length === 0) {
        const colCount = document.querySelectorAll('#topAlbumsTable thead th').length;
        tbody.innerHTML = `<tr><td colspan="${colCount}" style="text-align: center; color: #666; padding: 20px;">No album data available</td></tr>`;
        return;
    }
    
    const rowsToRender = data.slice(0, topInfiniteScrollState.albums.displayedRows);
    
    tbody.innerHTML = rowsToRender.map((album, index) => buildAlbumRow(album, index + 1)).join('');

    updateSortIndicators('topAlbumsTable', topSortState.albums);
}

/**
 * Renders the Top Songs table
 */
/**
 * Builds a single song row for the top table
 */
function buildSongRow(song, rank) {
    const vis = (col) => getColDisplay('songs', col);
    const buildLinkedSongTooltip = (value, items, title, extraClass = '') => {
        const contentHtml = `<span${extraClass ? ` class="${extraClass}"` : ''}>${value}</span>`;
        if (typeof window.buildLinkedSongTooltipTriggerHtml === 'function') {
            return window.buildLinkedSongTooltipTriggerHtml(contentHtml, items, title);
        }
        return contentHtml;
    };
    let coverHtml;
    if (song.hasImage && song.albumHasImage && song.albumId) {
        coverHtml = `<div class="hover-image-container" style="display:block;position:relative;width:50px;height:50px;cursor:pointer;">
            <img src="/albums/${song.albumId}/image" alt="" class="album-image-default clickable-image" onclick="openSongImageModalFromGraphs(${song.id})" style="position:absolute;top:0;left:0;width:50px;height:50px;object-fit:cover;border-radius:4px;opacity:1;transition:opacity 0.2s;z-index:2;">
            <img src="/songs/${song.id}/image" alt="" class="song-image-hover clickable-image" onclick="openSongImageModalFromGraphs(${song.id})" style="position:absolute;top:0;left:0;width:50px;height:50px;object-fit:cover;border-radius:4px;opacity:0;transition:opacity 0.2s;z-index:1;">
        </div>`;
    } else if (song.hasImage) {
        coverHtml = `<div style="cursor:pointer;">
            <img src="/songs/${song.id}/image" alt="" class="clickable-image" onclick="openSongImageModalFromGraphs(${song.id})" style="width:50px;height:50px;object-fit:cover;border-radius:4px;">
        </div>`;
    } else if (song.albumId) {
        coverHtml = `<div style="cursor:pointer;">
               <img src="/albums/${song.albumId}/image" alt="" class="inherited-cover clickable-image" onclick="openAlbumImageModalFromGraphs(${song.albumId})" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" style="width:50px;height:50px;object-fit:cover;border-radius:4px;">
               <div style="display:none;width:50px;height:50px;background:#2a2a2a;border-radius:4px;align-items:center;justify-content:center;color:#666;font-size:14px;">&#128191;</div>
           </div>`;
    } else {
        coverHtml = `<div style="width:50px;height:50px;background:#2a2a2a;border-radius:4px;display:flex;align-items:center;justify-content:center;color:#666;font-size:14px;">&#127925;</div>`;
    }
    const rowClasses = [];
    if (song.featuredOn) rowClasses.push('featured-song-row');
    if (song.fromGroup) rowClasses.push('group-item-row');
    const rowClassAttr = rowClasses.length > 0 ? ` class="${rowClasses.join(' ')}"` : '';
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
            <td><a href="/songs/${song.id}">${escapeHtml(song.name || '-')}</a>${song.isSingle ? ' <span class="single-indicator" title="Single">&#128313;</span>' : ''}${songLabel}</td>
            <td style="text-align:right;display:${vis('plays')};"${(song.plays || 0) >= 100 ? ' class="high-plays"' : ''}>${buildLinkedSongTooltip((song.plays || 0).toLocaleString(), song.totalPlayBreakdownItems, 'Combined song versions (Total Plays):')}</td>
            <td style="text-align:right;display:${vis('primaryPlays')};">${buildLinkedSongTooltip((song.primaryPlays || 0).toLocaleString(), song.primaryPlayBreakdownItems, 'Combined song versions (Primary Plays):')}</td>
            <td style="text-align:right;display:${vis('legacyPlays')};">${buildLinkedSongTooltip((song.legacyPlays || 0).toLocaleString(), song.legacyPlayBreakdownItems, 'Combined song versions (Legacy Plays):')}</td>
            <td style="text-align:right;display:${vis('length')};">${song.lengthFormatted || '-'}</td>
            <td style="text-align:right;display:${vis('timeListened')};">${song.timeListenedFormatted || '-'}</td>
            <td style="display:${vis('releaseDate')};">${song.releaseDate || '-'}</td>
            <td style="display:${vis('firstListened')};">${song.firstListened || '-'}</td>
            <td style="display:${vis('lastListened')};">${song.lastListened || '-'}</td>
            <td style="text-align:right;display:${vis('daysListened')};">${cellVal(song.daysListened)}</td>
            <td style="text-align:right;display:${vis('weeksListened')};">${cellVal(song.weeksListened)}</td>
            <td style="text-align:right;display:${vis('monthsListened')};">${cellVal(song.monthsListened)}</td>
            <td style="text-align:right;display:${vis('yearsListened')};">${cellVal(song.yearsListened)}</td>
            <td style="text-align:right;display:${vis('trackNumber')};">${cellVal(song.trackNumber)}</td>
            <td style="text-align:right;display:${vis('ageAtRelease')};">${cellVal(song.ageAtRelease)}</td>
            <td style="text-align:right;display:${vis('featuredArtistCount')};">${cellVal(song.featuredArtistCount)}</td>
            <td style="text-align:right;display:${vis('seasonalChartPeak')};">${cellVal(song.seasonalChartPeak)}</td>
            <td style="text-align:right;display:${vis('weeklyChartPeak')};">${cellVal(song.weeklyChartPeak)}</td>
            <td style="text-align:right;display:${vis('weeklyChartWeeks')};">${cellVal(song.weeklyChartWeeks)}</td>
            <td style="text-align:right;display:${vis('trlPeak')};">${cellVal(song.trlPeak)}</td>
            <td style="text-align:right;display:${vis('trlDays')};">${cellVal(song.trlDays)}</td>
            <td style="text-align:right;display:${vis('vatosCuntdownPeak')};">${cellVal(song.vatosCuntdownPeak)}</td>
            <td style="text-align:right;display:${vis('vatosCuntdownDays')};">${cellVal(song.vatosCuntdownDays)}</td>
            <td style="text-align:right;display:${vis('billboardPeak')};">${cellVal(song.billboardPeak)}</td>
            <td style="text-align:right;display:${vis('billboardWeeks')};">${cellVal(song.billboardWeeks)}</td>
            <td style="text-align:right;display:${vis('yearlyChartPeak')};">${cellVal(song.yearlyChartPeak)}</td>
            <td style="display:${vis('genre')};">${song.genre || '-'}</td>
            <td style="display:${vis('subgenre')};">${song.subgenre || '-'}</td>
            <td style="display:${vis('ethnicity')};">${song.ethnicity || '-'}</td>
            <td style="display:${vis('country')};">${song.country || '-'}</td>
            <td style="display:${vis('language')};">${song.language || '-'}</td>
        </tr>`;
}

function renderTopSongsTable() {
    const tbody = document.querySelector('#topSongsTable tbody');
    if (!tbody) return;
    
    const data = getRenderedTopData('songs', topTabData.songs, topSortState.songs);
    topSortedData.songs = data;
    
    // Render podium with sorted data
    renderSongsPodium(data);

    if (data.length === 0) {
        const colCount = document.querySelectorAll('#topSongsTable thead th').length;
        tbody.innerHTML = `<tr><td colspan="${colCount}" style="text-align: center; color: #666; padding: 20px;">No song data available</td></tr>`;
        return;
    }
    
    const rowsToRender = data.slice(0, topInfiniteScrollState.songs.displayedRows);
    
    tbody.innerHTML = rowsToRender.map((song, index) => buildSongRow(song, index + 1)).join('');
    if (typeof window.initializeLinkedSongTooltips === 'function') {
        window.initializeLinkedSongTooltips(tbody);
    }
    
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
const numericSortColumns = new Set([
    'plays', 'primaryPlays', 'legacyPlays', 'timeListened', 'length',
    'trackNumber', 'daysListened', 'weeksListened', 'monthsListened', 'yearsListened',
    'age', 'albumCount', 'songCount', 'avgPlays', 'avgPlaysAlbum', 'avgLength',
    'featuredOnCount', 'featuredArtistCount', 'soloSongCount', 'songsWithFeatCount',
    'ageAtRelease', 'billboardPeak', 'billboardWeeks', 'seasonalChartPeak',
    'trlDays', 'trlPeak', 'vatosCuntdownDays', 'vatosCuntdownPeak',
    'weeklyChartPeak', 'weeklyChartWeeks', 'yearlyChartPeak'
]);

const dateSortColumns = new Set(['releaseDate', 'firstListened', 'lastListened', 'lastFullListen', 'birthDate', 'deathDate']);

function parseDisplayDate(dateStr) {
    if (!dateStr || dateStr === '' || dateStr === '-') return 0;

    const months = {
        jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
        jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11
    };

    const separator = dateStr.includes('-') ? '-' : ' ';
    const parts = dateStr.trim().split(separator);

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

// Columns where lower numeric value is "better" (chart peaks)
const descDefaultColumns = new Set([
    'plays', 'primaryPlays', 'legacyPlays', 'timeListened', 'length',
    'albumCount', 'songCount', 'avgPlays', 'avgPlaysAlbum', 'avgLength', 'age',
    'daysListened', 'weeksListened', 'monthsListened', 'yearsListened',
    'featuredOnCount', 'featuredArtistCount', 'soloSongCount', 'songsWithFeatCount',
    'ageAtRelease', 'billboardWeeks', 'trlDays', 'vatosCuntdownDays', 'weeklyChartWeeks'
]);

function sortTopData(data, sortState) {
    if (!data || data.length === 0) return [];

    const sorted = [...data];
    const sorts = Array.isArray(sortState) ? sortState : [sortState];
    const hasExplicitPlaysSort = sorts.some(sort => sort.column === 'plays');

    function compareByColumn(a, b, column) {
        let aVal = a[column];
        let bVal = b[column];

        if (aVal == null && bVal == null) return 0;
        if (aVal == null) return 1;
        if (bVal == null) return -1;

        if (numericSortColumns.has(column)) {
            aVal = Number(aVal) || 0;
            bVal = Number(bVal) || 0;
        } else if (dateSortColumns.has(column)) {
            aVal = parseDisplayDate(aVal);
            bVal = parseDisplayDate(bVal);
        } else if (typeof aVal === 'string') {
            aVal = aVal.toLowerCase();
            bVal = (bVal || '').toString().toLowerCase();
        }

        if (aVal < bVal) return -1;
        if (aVal > bVal) return 1;
        return 0;
    }

    function compareDisplayIdentity(a, b) {
        const aName = (a.name || '').toString().toLowerCase();
        const bName = (b.name || '').toString().toLowerCase();
        if (aName < bName) return -1;
        if (aName > bName) return 1;

        const aArtist = (a.artistName || '').toString().toLowerCase();
        const bArtist = (b.artistName || '').toString().toLowerCase();
        if (aArtist < bArtist) return -1;
        if (aArtist > bArtist) return 1;

        const aAlbum = (a.albumName || '').toString().toLowerCase();
        const bAlbum = (b.albumName || '').toString().toLowerCase();
        if (aAlbum < bAlbum) return -1;
        if (aAlbum > bAlbum) return 1;

        return (a.id || 0) - (b.id || 0);
    }

    sorted.sort((a, b) => {
        for (const sort of sorts) {
            const result = compareByColumn(a, b, sort.column);
            if (result !== 0) {
                return sort.direction === 'desc' ? -result : result;
            }
        }

        if (!hasExplicitPlaysSort) {
            const playResult = compareByColumn(a, b, 'plays');
            if (playResult !== 0) {
                return -playResult;
            }
        }

        return compareDisplayIdentity(a, b);
    });

    return sorted;
}

function normalizeTopSortState(sortState) {
    const normalized = (Array.isArray(sortState) ? sortState : [sortState])
        .filter(sort => sort && sort.column)
        .map(sort => ({
            column: sort.column,
            direction: sort.direction === 'asc' ? 'asc' : 'desc'
        }))
        .slice(0, 3);

    if (normalized.length === 0) {
        return [{ column: 'plays', direction: 'desc' }];
    }

    return normalized;
}

function getTopSortKey(sortState) {
    return normalizeTopSortState(sortState)
        .map(sort => `${sort.column}:${sort.direction}`)
        .join('|');
}

function getSortedTopData(type, data, sortState) {
    const sourceData = Array.isArray(data) ? data : [];
    const normalizedSorts = normalizeTopSortState(sortState);
    const sortKey = getTopSortKey(normalizedSorts);
    const cache = topSortedCache[type];

    if (cache && cache.dataRef === sourceData && cache.sortKey === sortKey) {
        return cache.sorted;
    }

    const sorted = sortTopData(sourceData, normalizedSorts);

    if (cache) {
        cache.dataRef = sourceData;
        cache.sortKey = sortKey;
        cache.sorted = sorted;
    }

    return sorted;
}

function getRenderedTopData(type, data, sortState) {
    if (currentView === 'table' && type === getCurrentEntityType()) {
        return Array.isArray(data) ? data : [];
    }
    return getSortedTopData(type, data, sortState);
}

/**
 * Updates sort indicators on table headers
 */
function updateSortIndicators(tableId, sortState) {
    const table = document.getElementById(tableId);
    if (!table) return;
    const sorts = Array.isArray(sortState) ? sortState : [sortState];
    
    table.querySelectorAll('th').forEach(th => {
        th.classList.remove('sorted-asc', 'sorted-desc');
        const badge = th.querySelector('.sort-priority-badge');
        if (badge) {
            badge.remove();
        }
    });
    
    sorts.forEach((sort, index) => {
        const sortedTh = table.querySelector(`th[data-sort="${sort.column}"]`);
        if (!sortedTh) return;

        sortedTh.classList.add(sort.direction === 'asc' ? 'sorted-asc' : 'sorted-desc');

        const badge = document.createElement('span');
        badge.className = 'sort-priority-badge';
        badge.textContent = String(index + 1);
        badge.style.cssText = 'display:inline-flex;align-items:center;justify-content:center;min-width:16px;height:16px;margin-left:4px;border-radius:999px;background:#222;color:#fff;font-size:10px;line-height:1;padding:0 4px;vertical-align:middle;';
        sortedTh.appendChild(badge);
        sortedTh.title = `${index === 0 ? 'Primary' : index === 1 ? 'Secondary' : 'Tertiary'} sort: ${sort.direction.toUpperCase()}. Shift+click to add another sort level.`;
    });
}

/**
 * Sets up click handlers for table header sorting
 */
function setupTopTableSorting() {
    function rerenderOrReload(stateKey, renderFn) {
        if (currentView === 'table' && stateKey === getCurrentEntityType()) {
            reloadListViewData(stateKey);
            return;
        }
        renderFn();
    }

    function bindTableSort(tableId, stateKey, renderFn) {
        document.querySelectorAll(`#${tableId} th[data-sort]`).forEach(th => {
            th.title = 'Click to sort by this column. Shift+click to add a secondary or tertiary sort.';
            th.onclick = (event) => {
                const column = th.dataset.sort;
                const currentSorts = Array.isArray(topSortState[stateKey]) ? [...topSortState[stateKey]] : [topSortState[stateKey]];
                const existingIndex = currentSorts.findIndex(sort => sort.column === column);

                if (event.shiftKey) {
                    if (existingIndex >= 0) {
                        currentSorts[existingIndex] = {
                            column,
                            direction: currentSorts[existingIndex].direction === 'asc' ? 'desc' : 'asc'
                        };
                    } else if (currentSorts.length < 3) {
                        currentSorts.push({
                            column,
                            direction: descDefaultColumns.has(column) ? 'desc' : 'asc'
                        });
                    } else {
                        currentSorts[2] = {
                            column,
                            direction: descDefaultColumns.has(column) ? 'desc' : 'asc'
                        };
                    }
                } else {
                    const currentPrimary = currentSorts[0];
                    if (currentPrimary && currentPrimary.column === column) {
                        topSortState[stateKey] = [{
                            column,
                            direction: currentPrimary.direction === 'asc' ? 'desc' : 'asc'
                        }];
                    } else {
                        topSortState[stateKey] = [{
                            column,
                            direction: descDefaultColumns.has(column) ? 'desc' : 'asc'
                        }];
                    }
                    rerenderOrReload(stateKey, renderFn);
                    return;
                }

                topSortState[stateKey] = currentSorts;
                rerenderOrReload(stateKey, renderFn);
            };
        });
    }
    bindTableSort('topArtistsTable', 'artists', renderTopArtistsTable);
    bindTableSort('topAlbumsTable', 'albums', renderTopAlbumsTable);
    bindTableSort('topSongsTable', 'songs', renderTopSongsTable);
}

/**
 * Gets the current top limit value
 */
function getTopLimit() {
    const activeInput = document.querySelector('.tab-content.active .chart-limit-input');
    const input = activeInput || document.querySelector('.chart-limit-input') || document.getElementById('topLimitInput');
    return input ? parseInt(input.value) || 50 : 50;
}

function syncChartLimitInputs(sourceInput) {
    if (!sourceInput) {
        return;
    }

    const normalizedValue = Math.max(1, Math.min(999, parseInt(sourceInput.value, 10) || 50));
    sourceInput.value = normalizedValue;

    document.querySelectorAll('.chart-limit-input').forEach(input => {
        if (input !== sourceInput) {
            input.value = normalizedValue;
        }
    });
}

function onChartLimitChanged(tabName, input) {
    syncChartLimitInputs(input);
    reloadAllCharts();
}

/**
 * Handles when the apply limit checkbox changes on any tab - reloads that tab with new limit setting
 */
function onApplyLimitChanged(tabName) {
    // Reload the tab with the new apply limit setting
    loadTabData(tabName, true);
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
function openEntityImageModalFromGraphs(entityType, entityId) {
    const imagePath = `/${entityType}/${entityId}/image`;

    fetch(`/${entityType}/${entityId}/images`)
        .then(response => response.json())
        .then(data => {
            const imageIds = Array.isArray(data) ? data.map(img => img.id) : [];
            const galleryImages = [0, ...imageIds];

            if (galleryImages.length > 1) {
                openImageModal(imagePath, {
                    images: galleryImages,
                    entityType,
                    entityId,
                    currentIndex: 0
                });
                return;
            }

            openImageModal(imagePath);
        })
        .catch(error => {
            console.error(`Error loading ${entityType.slice(0, -1)} gallery:`, error);
            openImageModal(imagePath);
        });
}

function openArtistImageModalFromGraphs(artistId) {
    openEntityImageModalFromGraphs('artists', artistId);
}

function openAlbumImageModalFromGraphs(albumId) {
    openEntityImageModalFromGraphs('albums', albumId);
}

function openSongImageModalFromGraphs(songId) {
    openEntityImageModalFromGraphs('songs', songId);
}

/**
 * Append artists rows to table
 */
function appendTopArtistsRows(data, startRank) {
    const tbody = document.querySelector('#topArtistsTable tbody');
    if (!tbody) return;
    data.forEach((artist, index) => {
        tbody.insertAdjacentHTML('beforeend', buildArtistRow(artist, startRank + index + 1));
    });
}

/**
 * Append albums rows to table
 */
function appendTopAlbumsRows(data, startRank) {
    const tbody = document.querySelector('#topAlbumsTable tbody');
    if (!tbody) return;
    data.forEach((album, index) => {
        tbody.insertAdjacentHTML('beforeend', buildAlbumRow(album, startRank + index + 1));
    });
}

/**
 * Append songs rows to table
 */
function appendTopSongsRows(data, startRank) {
    const tbody = document.querySelector('#topSongsTable tbody');
    if (!tbody) return;
    data.forEach((song, index) => {
        tbody.insertAdjacentHTML('beforeend', buildSongRow(song, startRank + index + 1));
    });
    if (typeof window.initializeLinkedSongTooltips === 'function') {
        window.initializeLinkedSongTooltips(tbody);
    }
    // Re-bind hover listeners for image swap on newly appended rows
    tbody.querySelectorAll('.hover-image-container').forEach(container => {
        if (!container.dataset.hoverBound) {
            container.dataset.hoverBound = 'true';
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
        }
    });
}

// Read URL params for initial embedded graphs state.
document.addEventListener('DOMContentLoaded', function() {
    // Note: includeGroups and includeFeatured toggles are now handled as regular form inputs
    // They get their checked state from th:checked in the HTML and are submitted with the form

    const requestedView = getRequestedViewFromUrl();
    if (requestedView && document.getElementById('viewToggle')) {
        currentView = '__uninitialized__';
        switchView(requestedView);

        const requestedTab = getRequestedGraphsTabFromUrl();
        if (requestedView === 'graphs' && requestedTab && requestedTab !== 'general') {
            switchTab(requestedTab);
        }
    }
});

// ============================================================
// VIEW SWITCHING (Card / Table / Graphs)
// ============================================================

// (currentView, listViewState, listViewObserver declared at top of file)

/**
 * Switch between card, table, and graphs views on a list page.
 * @param {string} viewName - 'card', 'table', or 'graphs'
 */
function switchView(viewName) {
    const normalizedView = viewName === 'list' ? 'table' : viewName;
    if (currentView === normalizedView) return;
    currentView = normalizedView;

    const cardView = document.getElementById('cardView');
    const tableView = document.getElementById('tableView');
    const graphsView = document.getElementById('graphsView');

    // Hide all
    if (cardView) cardView.style.display = 'none';
    if (tableView) tableView.style.display = 'none';
    if (graphsView) graphsView.style.display = 'none';

    // Update button active states
    ['card', 'table', 'graphs'].forEach(v => {
        const btn = document.getElementById('viewBtn-' + v);
        if (btn) btn.classList.toggle('active', v === normalizedView);
    });

    if (normalizedView === 'card') {
        if (cardView) cardView.style.display = '';
    } else if (normalizedView === 'table') {
        if (tableView) tableView.style.display = '';
        listViewUserHasScrolled = true;
        // Load first page if not yet loaded
        if (listViewState.page === 0 && !listViewState.loading && !listViewState.allLoaded) {
            syncListViewSortStateFromUrl(getCurrentEntityType());
            fetchListPage(0);
        } else {
            updateListViewPaginationInfo();
        }
        setupListViewInfiniteScroll();
    } else if (normalizedView === 'graphs') {
        if (graphsView) graphsView.style.display = '';
        // Auto-load general tab
        if (!loadedTabs.general) {
            loadTabData('general');
        }
    }

    syncGraphsViewUrl(normalizedView, getActiveGraphsTab());
    window.refreshPageLoadAllButtonState?.();
}

function getChartsApiBase() {
    return '/' + getCurrentEntityType() + '/api/charts';
}

function getRequestedViewFromUrl() {
    const params = new URLSearchParams(window.location.search);
    const requestedView = params.get('view');
    return ['card', 'table', 'graphs'].includes(requestedView) ? requestedView : null;
}

function getRequestedGraphsTabFromUrl() {
    const params = new URLSearchParams(window.location.search);
    const requestedTab = params.get('tab');
    return validGraphsTabs.has(requestedTab) ? requestedTab : null;
}

function getActiveGraphsTab() {
    const activeTab = document.querySelector('.charts-tab-btn.active[data-tab]');
    return activeTab ? activeTab.dataset.tab : 'general';
}

function syncGraphsViewUrl(viewName, tabName) {
    if (!document.getElementById('viewToggle')) {
        return;
    }

    const url = new URL(window.location.href);

    if (viewName && viewName !== 'card') {
        url.searchParams.set('view', viewName);
    } else {
        url.searchParams.delete('view');
    }

    if (viewName === 'graphs' && tabName && validGraphsTabs.has(tabName)) {
        url.searchParams.set('tab', tabName);
    } else {
        url.searchParams.delete('tab');
    }

    window.history.replaceState({}, '', url.toString());
}

/**
 * Determine the entity type for the current page based on URL.
 */
function getCurrentEntityType() {
    const path = window.location.pathname;
    if (path.startsWith('/artists')) return 'artists';
    if (path.startsWith('/albums')) return 'albums';
    if (path.startsWith('/songs')) return 'songs';
    return 'songs';
}

function getListViewTableContainer(entityType = getCurrentEntityType()) {
    const suffix = entityType.charAt(0).toUpperCase() + entityType.slice(1);
    return document.getElementById('top' + suffix + 'TableContainer');
}

function getListViewTableBody(entityType = getCurrentEntityType()) {
    const suffix = entityType.charAt(0).toUpperCase() + entityType.slice(1);
    return document.querySelector('#top' + suffix + 'Table tbody');
}

function camelToSnakeCase(value) {
    return value.replace(/([a-z0-9])([A-Z])/g, '$1_$2').toLowerCase();
}

function snakeToCamelCase(value) {
    return value.replace(/_([a-z])/g, (_, ch) => ch.toUpperCase());
}

function getListViewSortColumn(sortParam, entityType) {
    return listViewSortColumnMap[entityType]?.[sortParam] || snakeToCamelCase(sortParam);
}

function syncListViewSortStateFromUrl(entityType = getCurrentEntityType()) {
    const url = new URL(window.location.href);
    const sorts = [];

    [1, 2, 3].forEach(level => {
        const suffix = level === 1 ? '' : String(level);
        const sortParam = url.searchParams.get('sortby' + suffix);
        if (!sortParam) {
            return;
        }

        sorts.push({
            column: getListViewSortColumn(sortParam, entityType),
            direction: url.searchParams.get('sortdir' + suffix) === 'asc' ? 'asc' : 'desc'
        });
    });

    if (sorts.length > 0) {
        topSortState[entityType] = normalizeTopSortState(sorts);
    }
}

function getListViewSortParam(column, entityType) {
    return listViewSortParamMap[entityType]?.[column] || camelToSnakeCase(column);
}

function applyListViewSortParams(params, entityType) {
    ['sortby', 'sortdir', 'sortby2', 'sortdir2', 'sortby3', 'sortdir3'].forEach(key => params.delete(key));

    const sorts = normalizeTopSortState(topSortState[entityType] || [{ column: 'plays', direction: 'desc' }]);
    sorts.slice(0, 3).forEach((sort, index) => {
        const suffix = index === 0 ? '' : String(index + 1);
        params.set('sortby' + suffix, getListViewSortParam(sort.column, entityType));
        params.set('sortdir' + suffix, sort.direction);
    });
}

function reloadListViewData(entityType = getCurrentEntityType()) {
    listViewRequestToken += 1;
    listViewState.page = 0;
    listViewState.totalCount = 0;
    listViewState.loading = false;
    listViewState.allLoaded = false;
    listViewUserHasScrolled = currentView === 'table';

    topTabData[entityType] = [];
    topSortedData[entityType] = [];
    topSortedCache[entityType] = { dataRef: null, sortKey: '', sorted: [] };
    topInfiniteScrollState[entityType] = { displayedRows: 999999, batchSize: 50 };

    const tbody = getListViewTableBody(entityType);
    if (tbody) tbody.innerHTML = '';

    const podium = document.getElementById(entityType + 'Podium');
    if (podium) podium.innerHTML = '';

    const endEl = document.getElementById('tableViewEnd');
    if (endEl) endEl.hidden = true;

    setListViewLoadingIndicator(false);

    const container = getListViewTableContainer(entityType);
    if (container) container.scrollTop = 0;

    fetchListPage(0);
}

function maybeLoadMoreListRows() {
    if (currentView !== 'table' || listViewState.loading || listViewState.allLoaded) return;
    if (!listViewUserHasScrolled && !listViewLoadAllInProgress) return;

    const container = getListViewTableContainer();
    if (!container) return;

    const nearBottom = container.scrollTop + container.clientHeight >= container.scrollHeight - 120;
    if (nearBottom) {
        fetchListPage(listViewState.page);
    }
}

function getListViewLoadedCount() {
    const entityType = getCurrentEntityType();
    const items = topTabData[entityType];
    return Array.isArray(items) ? items.length : 0;
}

function formatLoadAllProgressText(baseText, loadedCount, totalCount) {
    if (!Number.isFinite(totalCount) || totalCount <= 0) {
        return baseText;
    }

    const safeLoaded = Math.min(Math.max(loadedCount || 0, 0), totalCount);
    return baseText + ' (' + safeLoaded + '/' + totalCount + ')';
}

function updateListViewLoaderText() {
    const loader = getOrCreateListViewLoader();
    if (!loader) return;

    loader.textContent = listViewLoadAllInProgress
        ? formatLoadAllProgressText('Loading all...', getListViewLoadedCount(), listViewState.totalCount)
        : 'Loading records...';
}

function updateListViewPaginationInfo() {
    const infoEl = document.querySelector('.pagination-info span');
    if (!infoEl) return;

    const loadedCount = getListViewLoadedCount();
    const totalCount = listViewState.totalCount || loadedCount;
    const startCount = loadedCount > 0 ? 1 : 0;
    infoEl.textContent = startCount + ' - ' + loadedCount + ' of ' + totalCount;
}

function getOrCreateListViewLoader() {
    const tableView = document.getElementById('tableView');
    if (!tableView) return null;

    let loader = document.getElementById('tableViewLoader');
    if (loader) return loader;

    loader = document.createElement('div');
    loader.id = 'tableViewLoader';
    loader.hidden = true;
    loader.style.cssText = 'text-align:center;padding:18px 0;color:#888;font-size:0.85rem;letter-spacing:0.4px;';
    loader.textContent = 'Loading records...';

    const endEl = document.getElementById('tableViewEnd');
    if (endEl && endEl.parentElement === tableView) {
        tableView.insertBefore(loader, endEl);
    } else {
        tableView.appendChild(loader);
    }

    return loader;
}

function setListViewLoadingIndicator(isLoading) {
    const loader = getOrCreateListViewLoader();
    if (!loader) return;

    updateListViewLoaderText();
    loader.hidden = !isLoading;
}

/**
 * Fetch a page of list data from the entity's /api endpoint.
 * Uses the same top-table infrastructure as the Graphs Top tab.
 * @param {number} pageNum - 0-based page number
 */
function fetchListPage(pageNum) {
    if (listViewState.loading) return;
    listViewState.loading = true;
    setListViewLoadingIndicator(true);
    window.refreshPageLoadAllButtonState?.();

    const entityType = getCurrentEntityType();
    const params = getListFilterParams();
    params.set('page', pageNum);
    params.set('perpage', 100);
    applyListViewSortParams(params, entityType);

    const requestToken = ++listViewRequestToken;

    return fetch('/' + entityType + '/api?' + params.toString())
        .then(r => r.json())
        .then(data => {
            if (requestToken !== listViewRequestToken) return;
            listViewState.loading = false;
            setListViewLoadingIndicator(false);
            listViewState.totalCount = data.totalCount;

            const items = (data.items || []).map(item => normalizeListItem(item, entityType));

            if (pageNum === 0) {
                // Full init: replace data, init column toggles, render, setup sorting
                topTabData[entityType] = items;
                topSortedCache[entityType] = { dataRef: null, sortKey: '', sorted: [] };
                topInfiniteScrollState[entityType] = { displayedRows: 999999, batchSize: 50 };

                initColumnToggles();
                if (entityType === 'artists') renderTopArtistsTable();
                else if (entityType === 'albums') renderTopAlbumsTable();
                else renderTopSongsTable();
                setupTopTableSorting();

                listViewState.page = 1;
                listViewState.allLoaded = items.length >= data.totalCount;
            } else {
                // Append: add items to existing data and append rows to table
                const startRank = topTabData[entityType].length;
                topTabData[entityType].push(...items);
                topSortedCache[entityType] = { dataRef: null, sortKey: '', sorted: [] };
                topInfiniteScrollState[entityType].displayedRows = 999999;

                if (entityType === 'artists') appendTopArtistsRows(items, startRank);
                else if (entityType === 'albums') appendTopAlbumsRows(items, startRank);
                else appendTopSongsRows(items, startRank);

                listViewState.page = pageNum + 1;
                listViewState.allLoaded = topTabData[entityType].length >= data.totalCount;
            }

            const endEl = document.getElementById('tableViewEnd');
            if (endEl) endEl.hidden = !listViewState.allLoaded;
            updateListViewPaginationInfo();
            window.refreshPageLoadAllButtonState?.();
            if (currentView === 'table') {
                requestAnimationFrame(maybeLoadMoreListRows);
            }
        })
        .catch(err => {
            if (requestToken !== listViewRequestToken) return;
            listViewState.loading = false;
            setListViewLoadingIndicator(false);
            window.refreshPageLoadAllButtonState?.();
            console.error('Error loading list view data:', err);
        });
}

window.isListViewLoadAllInProgress = function() {
    return listViewLoadAllInProgress;
};

window.loadAllTableViewResults = async function() {
    if (listViewLoadAllInProgress || listViewState.loading || listViewState.allLoaded) {
        return;
    }

    listViewLoadAllInProgress = true;
    window.refreshPageLoadAllButtonState?.();

    try {
        if (listViewState.page === 0) {
            await fetchListPage(0);
        }

        while (!listViewState.allLoaded) {
            await fetchListPage(listViewState.page);
        }
    } finally {
        listViewLoadAllInProgress = false;
        window.refreshPageLoadAllButtonState?.();
    }
};

window.getTableViewProgressState = function() {
    const loadedCount = getListViewLoadedCount();
    return {
        loadedCount,
        totalCount: listViewState.totalCount || loadedCount,
        loading: listViewState.loading,
        loadAllInProgress: listViewLoadAllInProgress,
        allLoaded: listViewState.allLoaded
    };
};

window.formatLoadAllProgressText = formatLoadAllProgressText;


/**
 * Get filter params from the current URL (preserving all filter params, removing pagination/sort).
 */
function getListFilterParams() {
    const currentUrl = new URL(window.location.href);
    const params = new URLSearchParams(currentUrl.search);
    params.delete('page');
    params.delete('perpage');
    params.delete('sortby');
    params.delete('sortdir');
    params.delete('sortby2');
    params.delete('sortdir2');
    params.delete('sortby3');
    params.delete('sortdir3');
    return params;
}

/**
 * Normalize field names from CardDTO to match topColumnConfig/buildRow field names.
 */
function normalizeListItem(item, entityType) {
    // Common play count mappings
    item.plays = item.playCount;
    item.primaryPlays = item.vatitoPlayCount;
    item.legacyPlays = item.robertloverPlayCount;

    if (entityType === 'artists') {
        item.firstListened = item.firstListenedDate;
        item.lastListened = item.lastListenedDate;
        item.genre = item.genreName;
        item.subgenre = item.subgenreName;
        item.language = item.languageName;
        item.ethnicity = item.ethnicityName;
        item.avgPlaysAlbum = item.avgPlaysPerAlbum;
        item.featuredOnCount = item.featuredSongCount;
        // Age from birthDate
        if (item.birthDate) {
            const birth = new Date(item.birthDate);
            const end = item.deathDate ? new Date(item.deathDate) : new Date();
            item.age = Math.floor((end - birth) / (365.25 * 24 * 3600 * 1000));
        }
    } else if (entityType === 'albums') {
        item.firstListened = item.firstListenedDate;
        item.lastListened = item.lastListenedDate;
        item.lastFullListen = item.lastFullListenDate;
        item.genre = item.genreName;
        item.subgenre = item.subgenreName;
        item.language = item.languageName;
        item.ethnicity = item.ethnicityName;
        item.lengthFormatted = item.albumLengthFormatted || formatAlbumLength(item.albumLength);
    } else if (entityType === 'songs') {
        item.firstListened = item.firstListenedDate;
        item.lastListened = item.lastListenedDate;
        item.genre = item.genreName;
        item.subgenre = item.subgenreName;
        item.language = item.languageName;
        item.ethnicity = item.ethnicityName;
        item.lengthFormatted = item.lengthFormatted || formatSongLength(item.lengthSeconds);
    }

    return item;
}

/**
 * Format album length in seconds to mm:ss or hh:mm:ss
 */
function formatAlbumLength(totalSeconds) {
    if (!totalSeconds) return '';
    const h = Math.floor(totalSeconds / 3600);
    const m = Math.floor((totalSeconds % 3600) / 60);
    const s = totalSeconds % 60;
    if (h > 0) return h + ':' + String(m).padStart(2, '0') + ':' + String(s).padStart(2, '0');
    return m + ':' + String(s).padStart(2, '0');
}

/**
 * Format song length in seconds to mm:ss
 */
function formatSongLength(totalSeconds) {
    if (!totalSeconds) return '';
    const m = Math.floor(totalSeconds / 60);
    const s = totalSeconds % 60;
    return m + ':' + String(s).padStart(2, '0');
}

/**
 * Setup intersection observer for table view infinite scroll.
 * Watches tableViewEnd marker and fetches more pages from server when visible.
 */
function setupListViewInfiniteScroll() {
    const container = getListViewTableContainer();
    if (!container) return;

    if (listViewObserver) {
        listViewObserver.disconnect();
        listViewObserver = null;
    }

    if (listViewScrollContainer && listViewScrollHandler && listViewScrollContainer !== container) {
        listViewScrollContainer.removeEventListener('scroll', listViewScrollHandler);
    }

    if (listViewScrollContainer !== container || !listViewScrollHandler) {
        listViewScrollContainer = container;
        listViewScrollHandler = () => {
            listViewUserHasScrolled = true;
            maybeLoadMoreListRows();
        };
        container.addEventListener('scroll', listViewScrollHandler, { passive: true });
    }

    if (currentView === 'table') {
        requestAnimationFrame(maybeLoadMoreListRows);
    }
}

/**
 * Reload all chart tabs (force-reload).
 */
function reloadAllCharts() {
    // Reset loaded state for all tabs
    Object.keys(loadedTabs).forEach(tab => { loadedTabs[tab] = false; });
    // Destroy existing chart instances
    Object.keys(chartInstances).forEach(key => {
        if (chartInstances[key]) {
            chartInstances[key].destroy();
            delete chartInstances[key];
        }
    });
    // Reload currently active tab
    const activeBtn = document.querySelector('.charts-tab-btn.active');
    if (activeBtn && activeBtn.dataset.tab) {
        loadTabData(activeBtn.dataset.tab);
    }
}

