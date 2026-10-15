/**
 * Catalog table module.
 * Shared columns, sorting, image galleries, and pagination for catalog lists.
 */

// Current catalog view, initialized here so list templates can read it.
let currentView = 'card';
let listViewState = { page: 0, totalCount: 0, loading: false, allLoaded: false };
let listViewObserver = null;
let listViewScrollContainer = null;
let listViewScrollHandler = null;
let listViewWindowScrollHandler = null;
let listViewRequestToken = 0;
let listViewLoadAllInProgress = false;
let listViewUserHasScrolled = false;

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
        random: 'random',
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
        random: 'random',
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
        firstFullListen: 'first_full_listen',
        lastFullListen: 'last_full_listen',
        fullAlbumPlays: 'full_album_plays',
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
        weeklyChartPeakWeeks: 'weekly_chart_peak_weeks',
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
        random: 'random',
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
        weeklyChartPeakWeeks: 'weekly_chart_peak_weeks',
        trlPeak: 'trl_peak',
        trlDays: 'trl_days',
        trlDaysAtPeak: 'trl_days_at_peak',
        vatosCuntdownPeak: 'vatos_cuntdown_peak',
        vatosCuntdownDays: 'vatos_cuntdown_days',
        vatosCuntdownDaysAtPeak: 'vatos_cuntdown_days_at_peak',
        billboardPeak: 'billboard_peak',
        billboardWeeks: 'billboard_weeks',
        billboardWeeksAtPeak: 'billboard_weeks_at_peak',
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
        { key: 'random', label: 'Random', defaultVisible: false, align: 'right' },
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
        { key: 'random', label: 'Random', defaultVisible: false, align: 'right' },
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
        { key: 'firstFullListen', label: 'First Full Listen', defaultVisible: false, align: 'left' },
        { key: 'lastFullListen', label: 'Last Full Listen', defaultVisible: false, align: 'left' },
        { key: 'fullAlbumPlays', label: 'Full Album Plays', defaultVisible: false, align: 'right' },
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
        { key: 'weeklyChartPeakWeeks', label: 'Weekly Weeks @ Peak', defaultVisible: false, align: 'right' },
        { key: 'yearlyChartPeak', label: 'Yearly Peak', defaultVisible: false, align: 'right' },
        { key: 'genre', label: 'Genre', defaultVisible: false, align: 'left' },
        { key: 'subgenre', label: 'Subgenre', defaultVisible: false, align: 'left' },
        { key: 'ethnicity', label: 'Ethnicity', defaultVisible: false, align: 'left' },
        { key: 'country', label: 'Country', defaultVisible: false, align: 'left' },
        { key: 'language', label: 'Language', defaultVisible: false, align: 'left' }
    ],
    songs: [
        { key: 'random', label: 'Random', defaultVisible: false, align: 'right' },
        { key: 'billboardPeak', label: 'Billboard Peak', defaultVisible: false, align: 'right' },
        { key: 'billboardWeeks', label: 'Billboard Weeks', defaultVisible: false, align: 'right' },
        { key: 'billboardWeeksAtPeak', label: 'Billboard Weeks @ Peak', defaultVisible: false, align: 'right' },
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
        { key: 'trlDaysAtPeak', label: 'TRL Days @ Peak', defaultVisible: false, align: 'right' },
        { key: 'trlPeak', label: 'TRL Peak', defaultVisible: false, align: 'right' },
        { key: 'vatosCuntdownDays', label: 'Vato\'s Days', defaultVisible: false, align: 'right' },
        { key: 'vatosCuntdownDaysAtPeak', label: 'Vato\'s Days @ Peak', defaultVisible: false, align: 'right' },
        { key: 'vatosCuntdownPeak', label: 'Vato\'s Peak', defaultVisible: false, align: 'right' },
        { key: 'weeklyChartPeak', label: 'Weekly Peak', defaultVisible: false, align: 'right' },
        { key: 'weeklyChartWeeks', label: 'Weekly Weeks', defaultVisible: false, align: 'right' },
        { key: 'weeklyChartPeakWeeks', label: 'Weekly Weeks @ Peak', defaultVisible: false, align: 'right' },
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
const lazyTablePageRequests = new Map();
const albumFullListenColumns = new Set(['firstFullListen', 'lastFullListen', 'fullAlbumPlays']);

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
async function selectAllColumns(type, visible) {
    const singularType = type === 'artists' ? 'artist' : type === 'albums' ? 'album' : 'song';
    const container = document.getElementById(singularType + 'ColumnToggles');
    if (container) {
        container.querySelectorAll('input[type="checkbox"]').forEach(cb => {
            cb.checked = visible;
        });
    }
    if (visible && currentView === 'table' && type === getCurrentEntityType()) {
        window.showCatalogLoading?.('Loading column data...');
        try {
            await ensureLazyTableDetails(type, type === 'albums');
        } finally {
            window.hideCatalogLoading?.();
        }
    }
    topColumnConfig[type].forEach(col => setColumnVisibility(type, col.key, visible));
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
async function onColumnToggle(type, colKey, visible) {
    const column = topColumnConfig[type].find(col => col.key === colKey);
    if (visible && column && !column.defaultVisible && currentView === 'table' && type === getCurrentEntityType()) {
        window.showCatalogLoading?.('Loading column data...');
        try {
            await ensureLazyTableDetails(type, type === 'albums' && albumFullListenColumns.has(colKey));
        } catch (error) {
            console.error('Error loading table column data:', error);
            const singularType = type === 'artists' ? 'artist' : type === 'albums' ? 'album' : 'song';
            const checkbox = document.querySelector(`#${singularType}ColumnToggles input[data-col="${colKey}"]`);
            if (checkbox) checkbox.checked = false;
            return;
        } finally {
            window.hideCatalogLoading?.();
        }
    }
    setColumnVisibility(type, colKey, visible);
}

function setColumnVisibility(type, colKey, visible) {
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

function hasVisibleLazyColumns(type) {
    return topColumnConfig[type].some(col => !col.defaultVisible && topColumnVisibility[type][col.key]);
}

function hasVisibleAlbumFullListenColumns() {
    return [...albumFullListenColumns].some(key => topColumnVisibility.albums[key]);
}

function fetchLazyTablePage(type, page, includeFullListenStats = false) {
    const params = getListFilterParams();
    params.set('page', page);
    params.set('perpage', 100);
    params.set('includeExtendedStats', 'true');
    if (type === 'albums' && includeFullListenStats) {
        params.set('includeFullListenStats', 'true');
    }
    applyListViewSortParams(params, type);
    const cacheKey = type + ':' + page + ':' + params.toString();
    if (!lazyTablePageRequests.has(cacheKey)) {
        const request = fetch('/' + type + '/api?' + params.toString())
            .then(response => {
                if (!response.ok) throw new Error('Unable to load extended table statistics');
                return response.json();
            })
            .then(data => (data.items || []).map(item => normalizeListItem(item, type)))
            .catch(error => {
                lazyTablePageRequests.delete(cacheKey);
                throw error;
            });
        lazyTablePageRequests.set(cacheKey, request);
    }
    return lazyTablePageRequests.get(cacheKey);
}

function mergeLazyTableDetails(type, details, includeFullListenStats = false) {
    const detailsById = new Map(details.map(item => [String(item.id), item]));
    topTabData[type].forEach(item => {
        const detail = detailsById.get(String(item.id));
        if (detail) {
            Object.assign(item, detail, { __extendedStatsLoaded: true });
            if (includeFullListenStats) item.__fullListenStatsLoaded = true;
        }
    });
    topSortedCache[type] = { dataRef: null, sortKey: '', sorted: [] };
}

async function ensureLazyTableDetails(type, includeFullListenStats = false) {
    const pages = [...new Set(topTabData[type]
        .filter(item => includeFullListenStats ? !item.__fullListenStatsLoaded : !item.__extendedStatsLoaded)
        .map(item => item.__sourcePage ?? 0))]
        .sort((a, b) => a - b);
    for (const page of pages) {
        mergeLazyTableDetails(type, await fetchLazyTablePage(type, page, includeFullListenStats), includeFullListenStats);
    }
    if (type === 'artists') renderTopArtistsTable();
    else if (type === 'albums') renderTopAlbumsTable();
    else renderTopSongsTable();
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
    const breakdownItems = (main, groups, featured) => [
        window.artistScopeIncludeMain ? `Main: ${(main || 0).toLocaleString()}` : null,
        window.artistScopeIncludeGroups ? `Groups: ${(groups || 0).toLocaleString()}` : null,
        window.artistScopeIncludeFeatured ? `Featured: ${(featured || 0).toLocaleString()}` : null
    ].filter(Boolean);
    const withBreakdown = (value, items, title) => typeof window.buildPlayBreakdownTooltip === 'function'
        ? window.buildPlayBreakdownTooltip((value || 0).toLocaleString(), items, title)
        : (value || 0).toLocaleString();
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
            <td style="text-align:right;display:${vis('random')};">${cellVal(artist.__randomSortValue)}</td>
            <td style="text-align:right;display:${vis('plays')};"${(artist.plays || 0) >= 1000 ? ' class="high-plays"' : ''}>${withBreakdown(artist.plays, breakdownItems(artist.mainPlayCount, artist.groupPlayCount, artist.featuredPlayCount), 'Total plays by source:')}</td>
            <td style="text-align:right;display:${vis('primaryPlays')};">${withBreakdown(artist.primaryPlays, breakdownItems(artist.mainVatitoPlayCount, artist.groupVatitoPlayCount, artist.featuredVatitoPlayCount), 'Primary plays by source:')}</td>
            <td style="text-align:right;display:${vis('legacyPlays')};">${withBreakdown(artist.legacyPlays, breakdownItems(artist.mainRobertloverPlayCount, artist.groupRobertloverPlayCount, artist.featuredRobertloverPlayCount), 'Legacy plays by source:')}</td>
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
    if (typeof window.initializePlayBreakdownTooltips === 'function') {
        window.initializePlayBreakdownTooltips(tbody);
    }

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
            <td style="text-align:right;display:${vis('random')};">${cellVal(album.__randomSortValue)}</td>
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
            <td style="display:${vis('firstFullListen')};">${album.fullListenEligible ? (album.firstFullListen || '-') : 'Not an album'}</td>
            <td style="display:${vis('lastFullListen')};">${album.fullListenEligible ? (album.lastFullListen || '-') : 'Not an album'}</td>
            <td style="text-align:right;display:${vis('fullAlbumPlays')};">${album.fullListenEligible ? cellVal(album.fullAlbumPlays) : 'Not an album'}</td>
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
            <td style="text-align:right;display:${vis('weeklyChartPeakWeeks')};">${cellVal(album.weeklyChartPeakWeeks)}</td>
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
    let songLabel = '';
    if (song.featuredOn && song.primaryArtistName) {
        songLabel = ` <span class="featured-artist-label">(feat. on <a href="/artists/${song.primaryArtistId}">${escapeHtml(song.primaryArtistName)}</a>)</span>`;
    } else if (song.fromGroup && song.sourceArtistName) {
        songLabel = ` <span class="group-artist-label">(by <a href="/artists/${song.sourceArtistId}">${escapeHtml(song.sourceArtistName)}</a>)</span>`;
    }
    const selectionEnabled = typeof window.isSongBulkSelectionEnabled === 'function' && window.isSongBulkSelectionEnabled();
    const isSelected = selectionEnabled
        && typeof window.isSongSelectedForBulkTagging === 'function'
        && window.isSongSelectedForBulkTagging(song.id);
    const selectionCellHtml = selectionEnabled
        ? `<label class="song-select-cell">
                <input type="checkbox" autocomplete="off" class="song-bulk-select" value="${song.id}" ${isSelected ? 'checked' : ''} onchange="window.onSongBulkSelectionChanged && window.onSongBulkSelectionChanged(this)">
                <span>${rank}</span>
           </label>`
        : rank;
    if (isSelected) rowClasses.push('song-row-selected');
    const finalRowClassAttr = rowClasses.length > 0 ? ` class="${rowClasses.join(' ')}"` : '';
    return `
        <tr${finalRowClassAttr} style="${getGenderRowStyle(song.genderId)}">
            <td class="rank-col">${selectionCellHtml}</td>
            <td class="cover-col">${coverHtml}</td>
            <td><a href="/artists/${song.artistId}">${escapeHtml(song.artistName || '-')}</a></td>
            <td>${song.albumId ? `<a href="/albums/${song.albumId}">${escapeHtml(song.albumName)}</a>` : '-'}</td>
            <td><a href="/songs/${song.id}">${escapeHtml(song.name || '-')}</a>${song.isSingle ? ' <span class="single-indicator" title="Single">&#128313;</span>' : ''}${songLabel}</td>
            <td style="text-align:right;display:${vis('random')};">${cellVal(song.__randomSortValue)}</td>
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
            <td style="text-align:right;display:${vis('weeklyChartPeakWeeks')};">${cellVal(song.weeklyChartPeakWeeks)}</td>
            <td style="text-align:right;display:${vis('trlPeak')};">${cellVal(song.trlPeak)}</td>
            <td style="text-align:right;display:${vis('trlDays')};">${cellVal(song.trlDays)}</td>
            <td style="text-align:right;display:${vis('trlDaysAtPeak')};">${cellVal(song.trlDaysAtPeak)}</td>
            <td style="text-align:right;display:${vis('vatosCuntdownPeak')};">${cellVal(song.vatosCuntdownPeak)}</td>
            <td style="text-align:right;display:${vis('vatosCuntdownDays')};">${cellVal(song.vatosCuntdownDays)}</td>
            <td style="text-align:right;display:${vis('vatosCuntdownDaysAtPeak')};">${cellVal(song.vatosCuntdownDaysAtPeak)}</td>
            <td style="text-align:right;display:${vis('billboardPeak')};">${cellVal(song.billboardPeak)}</td>
            <td style="text-align:right;display:${vis('billboardWeeks')};">${cellVal(song.billboardWeeks)}</td>
            <td style="text-align:right;display:${vis('billboardWeeksAtPeak')};">${cellVal(song.billboardWeeksAtPeak)}</td>
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
        if (typeof window.syncSongTableSelectionControls === 'function') {
            window.syncSongTableSelectionControls();
        }
        return;
    }
    
    const rowsToRender = data.slice(0, topInfiniteScrollState.songs.displayedRows);
    
    tbody.innerHTML = rowsToRender.map((song, index) => buildSongRow(song, index + 1)).join('');
    if (typeof window.initializeLinkedSongTooltips === 'function') {
        window.initializeLinkedSongTooltips(tbody);
    }
    if (typeof window.syncSongTableSelectionControls === 'function') {
        window.syncSongTableSelectionControls();
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
    'ageAtRelease', 'billboardPeak', 'billboardWeeks', 'billboardWeeksAtPeak', 'seasonalChartPeak',
    'trlDays', 'trlDaysAtPeak', 'trlPeak', 'vatosCuntdownDays', 'vatosCuntdownDaysAtPeak', 'vatosCuntdownPeak',
    'weeklyChartPeak', 'weeklyChartWeeks', 'weeklyChartPeakWeeks', 'yearlyChartPeak', 'fullAlbumPlays'
]);

const dateSortColumns = new Set(['releaseDate', 'firstListened', 'lastListened', 'firstFullListen', 'lastFullListen', 'birthDate', 'deathDate']);

function getRandomSortValue(item) {
    const seed = new URLSearchParams(window.location.search).get('randomSeed');
    if (seed) {
        const key = [
            item.id,
            item.name,
            item.artistId,
            item.artistName,
            item.albumId,
            item.albumName
        ].filter(value => value != null && value !== '').join('|');
        return hashSeededRandomValue(seed + '|' + key);
    }

    if (item.__randomSortValue == null) {
        item.__randomSortValue = Math.random();
    }
    return item.__randomSortValue;
}

function hashSeededRandomValue(value) {
    let hash = 2166136261;
    const text = String(value || '');
    for (let i = 0; i < text.length; i++) {
        hash ^= text.charCodeAt(i);
        hash = Math.imul(hash, 16777619);
    }
    return (hash >>> 0) / 4294967296;
}

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

        if (column === 'random') {
            aVal = getRandomSortValue(a);
            bVal = getRandomSortValue(b);
        }

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
 * Escapes HTML special characters
 */
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Gallery-aware image modals used by the catalog tables.
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
    if (typeof window.initializePlayBreakdownTooltips === 'function') {
        window.initializePlayBreakdownTooltips(tbody);
    }
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
    if (typeof window.syncSongTableSelectionControls === 'function') {
        window.syncSongTableSelectionControls();
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

// Restore the catalog layout from the URL.
document.addEventListener('DOMContentLoaded', function() {
    const requestedView = getRequestedViewFromUrl();
    if (requestedView && document.getElementById('viewToggle')) {
        currentView = '__uninitialized__';
        switchView(requestedView);
    }
});

// ============================================================
// VIEW SWITCHING (Card / Table)
// ============================================================

// (currentView, listViewState, listViewObserver declared at top of file)

/**
 * Switch between card and table views on a catalog list page.
 * @param {string} viewName - 'card' or 'table'
 */
function switchView(viewName) {
    const normalizedView = viewName === 'list' ? 'table' : viewName;
    if (!['card', 'table'].includes(normalizedView)) return;
    if (currentView === normalizedView) return;
    currentView = normalizedView;

    const cardView = document.getElementById('cardView');
    const tableView = document.getElementById('tableView');

    // Hide all
    if (cardView) cardView.style.display = 'none';
    if (tableView) tableView.style.display = 'none';

    // Update button active states
    ['card', 'table'].forEach(v => {
        const btn = document.getElementById('viewBtn-' + v);
        if (btn) btn.classList.toggle('active', v === normalizedView);
    });

    if (normalizedView === 'card') {
        if (cardView) cardView.style.display = '';
    } else if (normalizedView === 'table') {
        if (tableView) tableView.style.display = '';
        listViewUserHasScrolled = false;
        // Load first page if not yet loaded
        if (listViewState.page === 0 && !listViewState.loading && !listViewState.allLoaded) {
            syncListViewSortStateFromUrl(getCurrentEntityType());
            fetchListPage(0);
        } else {
            updateListViewPaginationInfo();
        }
        setupListViewInfiniteScroll();
    }

    syncCatalogViewUrl(normalizedView);
    window.refreshPageLoadAllButtonState?.();
}

function getRequestedViewFromUrl() {
    const params = new URLSearchParams(window.location.search);
    const requestedView = params.get('view');
    return ['card', 'table'].includes(requestedView) ? requestedView : null;
}

function syncCatalogViewUrl(viewName) {
    if (!document.getElementById('viewToggle')) {
        return;
    }

    const url = new URL(window.location.href);

    if (viewName && viewName !== 'card') {
        url.searchParams.set('view', viewName);
    } else {
        url.searchParams.delete('view');
    }

    url.searchParams.delete('tab');

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
    listViewUserHasScrolled = false;

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

    if (isListViewNearBottom(container)) {
        fetchListPage(listViewState.page);
    }
}

function isListViewNearBottom(container) {
    if (!container) return false;

    const threshold = 120;
    if (container.scrollHeight > container.clientHeight + 4) {
        return container.scrollTop + container.clientHeight >= container.scrollHeight - threshold;
    }

    const rect = container.getBoundingClientRect();
    return rect.bottom <= window.innerHeight + threshold;
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
    const requestedPageSize = 100;
    params.set('page', pageNum);
    params.set('perpage', requestedPageSize);
    applyListViewSortParams(params, entityType);

    const requestToken = ++listViewRequestToken;

    return fetch('/' + entityType + '/api?' + params.toString())
        .then(r => r.json())
        .then(async data => {
            if (requestToken !== listViewRequestToken) return;
            listViewState.loading = false;
            setListViewLoadingIndicator(false);

            const items = (data.items || []).map(item => {
                const normalized = normalizeListItem(item, entityType);
                normalized.__sourcePage = pageNum;
                normalized.__extendedStatsLoaded = false;
                normalized.__fullListenStatsLoaded = false;
                return normalized;
            });
            if (hasVisibleLazyColumns(entityType) && items.length > 0) {
                const includeFullListenStats = entityType === 'albums' && hasVisibleAlbumFullListenColumns();
                const extendedItems = await fetchLazyTablePage(entityType, pageNum, includeFullListenStats);
                const extendedById = new Map(extendedItems.map(item => [String(item.id), item]));
                items.forEach(item => {
                    const extended = extendedById.get(String(item.id));
                    if (extended) {
                        Object.assign(item, extended, { __extendedStatsLoaded: true });
                        if (includeFullListenStats) item.__fullListenStatsLoaded = true;
                    }
                });
            }
            listViewState.totalCount = data.totalCount;

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
            }

            const loadedCount = getListViewLoadedCount();
            const reachedShortPage = items.length < requestedPageSize;
            if (reachedShortPage && loadedCount < listViewState.totalCount) {
                listViewState.totalCount = loadedCount;
            }
            listViewState.allLoaded = loadedCount >= listViewState.totalCount || reachedShortPage;

            const endEl = document.getElementById('tableViewEnd');
            if (endEl) endEl.hidden = !listViewState.allLoaded;
            updateListViewPaginationInfo();
            window.refreshPageLoadAllButtonState?.();
            if (currentView === 'table' && (listViewUserHasScrolled || listViewLoadAllInProgress)) {
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
    params.delete('view');
    params.delete('tab');
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
        item.firstFullListen = item.firstFullListenDate;
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
 * Setup table view infinite scroll.
 * Watches the table wrapper and page scroll, then fetches more pages near the table bottom.
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

    if (!listViewWindowScrollHandler) {
        listViewWindowScrollHandler = () => {
            if (currentView !== 'table') return;
            listViewUserHasScrolled = true;
            maybeLoadMoreListRows();
        };
        window.addEventListener('scroll', listViewWindowScrollHandler, { passive: true });
    }

    if (currentView === 'table') {
        requestAnimationFrame(maybeLoadMoreListRows);
    }
}

