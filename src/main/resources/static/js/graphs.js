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
        consistency: 'consistency',
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
        consistency: 'consistency',
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
        consistency: 'consistency',
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
        { key: 'consistency', label: 'Consistency', defaultVisible: false, align: 'right' },
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
        { key: 'consistency', label: 'Consistency', defaultVisible: false, align: 'right' },
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
        { key: 'consistency', label: 'Consistency', defaultVisible: false, align: 'right' },
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

// ============================================================
// CELL DETAILS (styles in css/catalog-table.css)
// ============================================================

// Largest play count among the loaded rows, so the thin bar under Total Plays compares rows.
const ctPlayScale = { artists: 0, albums: 0, songs: 0 };

/** Row class for the gender cues (cover ring, tinted artist / album links, bar color). Male = 2, female = 1. */
function ctGenderClass(genderId) {
    return genderId === 2 ? 'ct-male' : genderId === 1 ? 'ct-female' : '';
}

function ctRankClass(rank) {
    return rank >= 1 && rank <= 3 ? ' ct-medal-' + rank : '';
}

/** Parses dd/MM/yyyy, yyyy-MM-dd and the "dd MMM yyyy" display format. */
function ctParseDate(value) {
    if (value == null || value === '' || value === '-') return null;
    const text = String(value).trim();
    let match = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (match) return new Date(Number(match[3]), Number(match[2]) - 1, Number(match[1]));
    match = text.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (match) return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
    const time = parseDisplayDate(text);
    return time ? new Date(time) : null;
}

function ctDaysAgo(date) {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const day = new Date(date.getTime());
    day.setHours(0, 0, 0, 0);
    return Math.round((today - day) / 86400000);
}

function ctAgoText(days) {
    if (days < 0) return 'Upcoming';
    if (days === 0) return 'Today';
    if (days === 1) return 'Yesterday';
    if (days < 7) return days + ' days ago';
    if (days < 30) {
        const weeks = Math.floor(days / 7);
        return weeks + (weeks === 1 ? ' week ago' : ' weeks ago');
    }
    if (days < 365) {
        const months = Math.max(1, Math.floor(days / 30.44));
        return months + (months === 1 ? ' month ago' : ' months ago');
    }
    const years = Math.floor(days / 365.25);
    return years + (years === 1 ? ' year ago' : ' years ago');
}

/** Date with how long ago it was underneath (first listened, release date, ...). */
function ctDateAgoCell(value) {
    const date = ctParseDate(value);
    if (!date) return value ? escapeHtml(String(value)) : '-';
    return `${escapeHtml(String(value))}<span class="ct-muted">${ctAgoText(ctDaysAgo(date))}</span>`;
}

/** Recency dot: green within 1 year, amber 1-3 years, red 3-5 years, grey after that. */
function ctRecencyCell(value) {
    const date = ctParseDate(value);
    if (!date) return value ? escapeHtml(String(value)) : '-';
    const days = ctDaysAgo(date);
    const years = days / 365.25;
    const dotClass = years <= 1 ? ' fresh' : years <= 3 ? ' aging' : years <= 5 ? ' stale' : '';
    return `<span class="ct-rec"><span class="ct-dot${dotClass}" aria-hidden="true"></span>${ctAgoText(days)}</span><span class="ct-muted">${escapeHtml(String(value))}</span>`;
}

function ctItunesBadge(inItunes) {
    if (inItunes == null) return '';
    return inItunes
        ? '<span class="ct-it yes" title="In iTunes Library"><svg width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M20 6L9 17l-5-5"/></svg></span>'
        : '<span class="ct-it no" title="Not in iTunes Library"><svg width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="4" stroke-linecap="round" aria-hidden="true"><path d="M18 6L6 18M6 6l12 12"/></svg></span>';
}

function ctRefreshPlayScale(type) {
    const max = (topTabData[type] || []).reduce((best, item) => Math.max(best, Number(item.plays) || 0), 0);
    const changed = max !== ctPlayScale[type];
    ctPlayScale[type] = max;
    return changed;
}

/** Total plays with a thin bar scaled to the most played loaded row. */
function ctPlaysCell(type, plays, innerHtml) {
    const max = ctPlayScale[type] || 0;
    const width = max > 0 ? Math.max(2, Math.round((Number(plays) || 0) * 100 / max)) : 0;
    return `<span class="ct-plays">${innerHtml}</span><div class="ct-bar" style="width:${width}%;" aria-hidden="true"></div>`;
}

/** Share of the total under Primary / Legacy plays. */
function ctShareLine(part, total) {
    const value = Number(part) || 0;
    const whole = Number(total) || 0;
    if (whole <= 0) return '';
    return `<span class="ct-muted">${value > 0 ? Math.round(value * 100 / whole) + '%' : 'none'}</span>`;
}

function ctZeroClass(value) {
    return (Number(value) || 0) === 0 ? ' class="ct-dim"' : '';
}

/** Time listened with an "about N days" hint once it passes a day. */
function ctTimeCell(formatted, seconds) {
    const total = Number(seconds) || 0;
    const hint = total >= 86400 ? `<span class="ct-muted">&#8776; ${(total / 86400).toFixed(1)} days</span>` : '';
    return (formatted || '-') + hint;
}

const ctStarSvg = '<svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M12 2l3 6.9 7.5.7-5.7 5 1.7 7.4L12 18l-6.5 4 1.7-7.4-5.7-5 7.5-.7z"/></svg>';

/**
 * Chart peak chip: #1 gold with a star, #2 silver, #3 bronze, lower peaks in the chart's own color.
 * Billboard keeps its cyan and only gets a gold edge at #1.
 */
function ctPeakChip(value, family, atPeak, unit) {
    if (value == null || value === '' || value === '-') return '<span class="ct-dim">-</span>';
    const peak = parseInt(String(value).replace('#', ''), 10);
    if (!Number.isFinite(peak)) return escapeHtml(String(value));
    const medal = family === 'billboard'
        ? (peak === 1 ? ' p1' : '')
        : (peak <= 3 ? ' p' + peak : '');
    const count = Number(atPeak) || 0;
    const tail = count > 0 ? ` <em>&#215;${count} ${count === 1 ? unit : unit + 's'}</em>` : '';
    return `<span class="ct-pk ${family}${medal}">${peak === 1 ? ctStarSvg : ''}#${peak}${tail}</span>`;
}

function ctChip(value, kind) {
    if (!value) return '<span class="ct-dim">-</span>';
    return `<span class="ct-chip ${kind}" title="${escapeHtml(String(value))}">${escapeHtml(String(value))}</span>`;
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
        // Lists opened from a timeframe after the legacy account stopped start without Primary / Legacy
        // (Total says it all); they stay available in Show/Hide Columns.
        const legacyHidden = document.getElementById('tableView')?.dataset.legacyHidden === 'true';
        // Preserve any user-selected visibility state across table reloads/sorts.
        const existingVisibility = topColumnVisibility[type] || {};
        topColumnVisibility[type] = {};
        config.forEach(col => {
            topColumnVisibility[type][col.key] = Object.prototype.hasOwnProperty.call(existingVisibility, col.key)
                ? existingVisibility[col.key]
                : col.defaultVisible && !(legacyHidden && (col.key === 'primaryPlays' || col.key === 'legacyPlays'));
        });
        if (legacyHidden) {
            const tableId = type === 'artists' ? 'topArtistsTable' : type === 'albums' ? 'topAlbumsTable' : 'topSongsTable';
            ['primaryPlays', 'legacyPlays'].forEach(key => {
                const th = document.querySelector(`#${tableId} thead th[data-col="${key}"]`);
                if (th) th.style.display = topColumnVisibility[type][key] ? '' : 'none';
            });
        }
        
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
    finishCatalogTableRender(type);
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
        <tr class="${ctGenderClass(artist.genderId)}" data-id="${artist.id}">
            <td class="rank-col ct-pin ct-pin-rank${ctRankClass(rank)}">${rank}</td>
            <td class="cover-col artist-cover ct-pin ct-pin-cover">
                <div style="cursor:pointer;">
                    <img src="/artists/${artist.id}/image" alt="" class="clickable-image" onclick="openArtistImageModalFromGraphs(${artist.id})" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" style="width:40px;height:60px;object-fit:cover;border-radius:4px;">
                    <div style="display:none;width:40px;height:60px;background:#2a2a2a;border-radius:4px;align-items:center;justify-content:center;color:#666;font-size:14px;">&#127908;</div>
                </div>
            </td>
            <td class="ct-name ct-pin ct-pin-name"><a href="/artists/${artist.id}">${escapeHtml(artist.name || '-')}</a>${ctItunesBadge(artist.inItunes)}</td>
            <td style="text-align:right;display:${vis('random')};">${cellVal(artist.__randomSortValue)}</td>
            <td style="text-align:right;display:${vis('plays')};">${ctPlaysCell('artists', artist.plays, withBreakdown(artist.plays, breakdownItems(artist.mainPlayCount, artist.groupPlayCount, artist.featuredPlayCount), 'Total plays by source:'))}</td>
            <td style="text-align:right;display:${vis('primaryPlays')};">${withBreakdown(artist.primaryPlays, breakdownItems(artist.mainVatitoPlayCount, artist.groupVatitoPlayCount, artist.featuredVatitoPlayCount), 'Primary plays by source:')}${ctShareLine(artist.primaryPlays, artist.plays)}</td>
            <td style="text-align:right;display:${vis('legacyPlays')};"${ctZeroClass(artist.legacyPlays)}>${withBreakdown(artist.legacyPlays, breakdownItems(artist.mainRobertloverPlayCount, artist.groupRobertloverPlayCount, artist.featuredRobertloverPlayCount), 'Legacy plays by source:')}${ctShareLine(artist.legacyPlays, artist.plays)}</td>
            <td style="text-align:right;display:${vis('timeListened')};">${ctTimeCell(artist.timeListenedFormatted, artist.timeListened)}</td>
            <td style="display:${vis('firstListened')};">${ctDateAgoCell(artist.firstListened)}</td>
            <td style="display:${vis('lastListened')};">${ctRecencyCell(artist.lastListened)}</td>
            <td style="text-align:right;display:${vis('daysListened')};">${cellVal(artist.daysListened)}</td>
            <td style="text-align:right;display:${vis('weeksListened')};">${cellVal(artist.weeksListened)}</td>
            <td style="text-align:right;display:${vis('monthsListened')};">${cellVal(artist.monthsListened)}</td>
            <td style="text-align:right;display:${vis('yearsListened')};">${cellVal(artist.yearsListened)}</td>
            <td style="text-align:right;display:${vis('consistency')};">${cellVal(artist.consistencyLabel)}</td>
            <td style="text-align:right;display:${vis('age')};">${cellVal(artist.age)}</td>
            <td style="text-align:right;display:${vis('albumCount')};">${cellVal(artist.albumCount)}</td>
            <td style="text-align:right;display:${vis('avgLength')};">${artist.avgLengthFormatted || '-'}</td>
            <td style="text-align:right;display:${vis('avgPlays')};">${cellVal(artist.avgPlays)}</td>
            <td style="text-align:right;display:${vis('avgPlaysAlbum')};">${cellVal(artist.avgPlaysAlbum)}</td>
            <td style="display:${vis('birthDate')};">${artist.birthDate || '-'}</td>
            <td style="display:${vis('deathDate')};">${ctDateAgoCell(artist.deathDate)}</td>
            <td style="text-align:right;display:${vis('songCount')};">${cellVal(artist.songCount)}</td>
            <td style="text-align:right;display:${vis('featuredOnCount')};">${cellVal(artist.featuredOnCount)}</td>
            <td style="text-align:right;display:${vis('featuredArtistCount')};">${cellVal(artist.featuredArtistCount)}</td>
            <td style="text-align:right;display:${vis('soloSongCount')};">${cellVal(artist.soloSongCount)}</td>
            <td style="text-align:right;display:${vis('songsWithFeatCount')};">${cellVal(artist.songsWithFeatCount)}</td>
            <td style="display:${vis('genre')};">${ctChip(artist.genre, 'genre')}</td>
            <td style="display:${vis('subgenre')};">${ctChip(artist.subgenre, 'subgenre')}</td>
            <td style="display:${vis('ethnicity')};">${ctChip(artist.ethnicity, 'ethnicity')}</td>
            <td style="display:${vis('country')};">${ctChip(artist.country, 'country')}</td>
            <td style="display:${vis('language')};">${ctChip(artist.language, 'language')}</td>
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
    ctRefreshPlayScale('artists');
    
    tbody.innerHTML = rowsToRender.map((artist, index) => buildArtistRow(artist, index + 1)).join('');
    if (typeof window.initializePlayBreakdownTooltips === 'function') {
        window.initializePlayBreakdownTooltips(tbody);
    }

    updateSortIndicators('topArtistsTable', topSortState.artists);
    finishCatalogTableRender('artists');
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
        <tr class="${ctGenderClass(album.genderId)}" data-id="${album.id}">
            <td class="rank-col ct-pin ct-pin-rank${ctRankClass(rank)}">${rank}</td>
            <td class="cover-col ct-pin ct-pin-cover">
                <div style="cursor:pointer;">
                    <img src="/albums/${album.id}/image" alt="" class="clickable-image" onclick="openAlbumImageModalFromGraphs(${album.id})" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" style="width:50px;height:50px;object-fit:cover;border-radius:4px;">
                    <div style="display:none;width:50px;height:50px;background:#2a2a2a;border-radius:4px;align-items:center;justify-content:center;color:#666;font-size:14px;">&#128191;</div>
                </div>
            </td>
            <td class="ct-name ct-pin ct-pin-name"><a href="/albums/${album.id}">${escapeHtml(album.name || '-')}</a>${ctItunesBadge(album.inItunes)}</td>
            <td class="ct-sub"><a href="/artists/${album.artistId}">${escapeHtml(album.artistName || '-')}</a></td>
            <td style="text-align:right;display:${vis('random')};">${cellVal(album.__randomSortValue)}</td>
            <td style="text-align:right;display:${vis('plays')};">${ctPlaysCell('albums', album.plays, (album.plays || 0).toLocaleString())}</td>
            <td style="text-align:right;display:${vis('primaryPlays')};">${(album.primaryPlays || 0).toLocaleString()}${ctShareLine(album.primaryPlays, album.plays)}</td>
            <td style="text-align:right;display:${vis('legacyPlays')};"${ctZeroClass(album.legacyPlays)}>${(album.legacyPlays || 0).toLocaleString()}${ctShareLine(album.legacyPlays, album.plays)}</td>
            <td style="text-align:right;display:${vis('length')};">${album.lengthFormatted || '-'}</td>
            <td style="text-align:right;display:${vis('timeListened')};">${ctTimeCell(album.timeListenedFormatted, album.timeListened)}</td>
            <td style="display:${vis('releaseDate')};">${ctDateAgoCell(album.releaseDate)}</td>
            <td style="display:${vis('firstListened')};">${ctDateAgoCell(album.firstListened)}</td>
            <td style="display:${vis('lastListened')};">${ctRecencyCell(album.lastListened)}</td>
            <td style="text-align:right;display:${vis('daysListened')};">${cellVal(album.daysListened)}</td>
            <td style="text-align:right;display:${vis('weeksListened')};">${cellVal(album.weeksListened)}</td>
            <td style="text-align:right;display:${vis('monthsListened')};">${cellVal(album.monthsListened)}</td>
            <td style="text-align:right;display:${vis('yearsListened')};">${cellVal(album.yearsListened)}</td>
            <td style="text-align:right;display:${vis('consistency')};">${cellVal(album.consistencyLabel)}</td>
            <td style="display:${vis('firstFullListen')};">${album.fullListenEligible ? (album.firstFullListen || '-') : 'Not an album'}</td>
            <td style="display:${vis('lastFullListen')};">${album.fullListenEligible ? ctRecencyCell(album.lastFullListen) : 'Not an album'}</td>
            <td style="text-align:right;display:${vis('fullAlbumPlays')};">${album.fullListenEligible ? cellVal(album.fullAlbumPlays) : 'Not an album'}</td>
            <td style="text-align:right;display:${vis('ageAtRelease')};">${cellVal(album.ageAtRelease)}</td>
            <td style="text-align:right;display:${vis('avgLength')};">${album.avgLengthFormatted || '-'}</td>
            <td style="text-align:right;display:${vis('avgPlays')};">${cellVal(album.avgPlays)}</td>
            <td style="text-align:right;display:${vis('seasonalChartPeak')};">${ctPeakChip(album.seasonalChartPeak, 'seasonal')}</td>
            <td style="text-align:right;display:${vis('songCount')};">${cellVal(album.songCount)}</td>
            <td style="text-align:right;display:${vis('featuredArtistCount')};">${cellVal(album.featuredArtistCount)}</td>
            <td style="text-align:right;display:${vis('soloSongCount')};">${cellVal(album.soloSongCount)}</td>
            <td style="text-align:right;display:${vis('songsWithFeatCount')};">${cellVal(album.songsWithFeatCount)}</td>
            <td style="text-align:right;display:${vis('weeklyChartPeak')};">${ctPeakChip(album.weeklyChartPeak, 'weekly', album.weeklyChartPeakWeeks, 'wk')}</td>
            <td style="text-align:right;display:${vis('weeklyChartWeeks')};">${cellVal(album.weeklyChartWeeks)}</td>
            <td style="text-align:right;display:${vis('weeklyChartPeakWeeks')};">${cellVal(album.weeklyChartPeakWeeks)}</td>
            <td style="text-align:right;display:${vis('yearlyChartPeak')};">${ctPeakChip(album.yearlyChartPeak, 'yearly')}</td>
            <td style="display:${vis('genre')};">${ctChip(album.genre, 'genre')}</td>
            <td style="display:${vis('subgenre')};">${ctChip(album.subgenre, 'subgenre')}</td>
            <td style="display:${vis('ethnicity')};">${ctChip(album.ethnicity, 'ethnicity')}</td>
            <td style="display:${vis('country')};">${ctChip(album.country, 'country')}</td>
            <td style="display:${vis('language')};">${ctChip(album.language, 'language')}</td>
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
    ctRefreshPlayScale('albums');
    
    tbody.innerHTML = rowsToRender.map((album, index) => buildAlbumRow(album, index + 1)).join('');

    updateSortIndicators('topAlbumsTable', topSortState.albums);
    finishCatalogTableRender('albums');
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
    const genderClass = ctGenderClass(song.genderId);
    if (genderClass) rowClasses.push(genderClass);
    const finalRowClassAttr = rowClasses.length > 0 ? ` class="${rowClasses.join(' ')}"` : '';
    return `
        <tr${finalRowClassAttr} data-id="${song.id}">
            <td class="rank-col ct-pin ct-pin-rank${ctRankClass(rank)}">${selectionCellHtml}</td>
            <td class="cover-col ct-pin ct-pin-cover">${coverHtml}</td>
            <td class="ct-name ct-pin ct-pin-name"><a href="/songs/${song.id}">${escapeHtml(song.name || '-')}</a>${song.isSingle ? '<span class="ct-single" title="Single">Single</span>' : ''}${ctItunesBadge(song.inItunes)}${songLabel}</td>
            <td class="ct-sub"><a href="/artists/${song.artistId}">${escapeHtml(song.artistName || '-')}</a></td>
            <td class="ct-sub">${song.albumId ? `<a href="/albums/${song.albumId}">${escapeHtml(song.albumName)}</a>` : '-'}</td>
            <td style="text-align:right;display:${vis('random')};">${cellVal(song.__randomSortValue)}</td>
            <td style="text-align:right;display:${vis('plays')};">${ctPlaysCell('songs', song.plays, buildLinkedSongTooltip((song.plays || 0).toLocaleString(), song.totalPlayBreakdownItems, 'Combined song versions (Total Plays):'))}</td>
            <td style="text-align:right;display:${vis('primaryPlays')};">${buildLinkedSongTooltip((song.primaryPlays || 0).toLocaleString(), song.primaryPlayBreakdownItems, 'Combined song versions (Primary Plays):')}${ctShareLine(song.primaryPlays, song.plays)}</td>
            <td style="text-align:right;display:${vis('legacyPlays')};"${ctZeroClass(song.legacyPlays)}>${buildLinkedSongTooltip((song.legacyPlays || 0).toLocaleString(), song.legacyPlayBreakdownItems, 'Combined song versions (Legacy Plays):')}${ctShareLine(song.legacyPlays, song.plays)}</td>
            <td style="text-align:right;display:${vis('length')};">${song.lengthFormatted || '-'}</td>
            <td style="text-align:right;display:${vis('timeListened')};">${ctTimeCell(song.timeListenedFormatted, song.timeListened)}</td>
            <td style="display:${vis('releaseDate')};">${ctDateAgoCell(song.releaseDate)}</td>
            <td style="display:${vis('firstListened')};">${ctDateAgoCell(song.firstListened)}</td>
            <td style="display:${vis('lastListened')};">${ctRecencyCell(song.lastListened)}</td>
            <td style="text-align:right;display:${vis('daysListened')};">${cellVal(song.daysListened)}</td>
            <td style="text-align:right;display:${vis('weeksListened')};">${cellVal(song.weeksListened)}</td>
            <td style="text-align:right;display:${vis('monthsListened')};">${cellVal(song.monthsListened)}</td>
            <td style="text-align:right;display:${vis('yearsListened')};">${cellVal(song.yearsListened)}</td>
            <td style="text-align:right;display:${vis('consistency')};">${cellVal(song.consistencyLabel)}</td>
            <td style="text-align:right;display:${vis('trackNumber')};">${cellVal(song.trackNumber)}</td>
            <td style="text-align:right;display:${vis('ageAtRelease')};">${cellVal(song.ageAtRelease)}</td>
            <td style="text-align:right;display:${vis('featuredArtistCount')};">${cellVal(song.featuredArtistCount)}</td>
            <td style="text-align:right;display:${vis('seasonalChartPeak')};">${ctPeakChip(song.seasonalChartPeak, 'seasonal')}</td>
            <td style="text-align:right;display:${vis('weeklyChartPeak')};">${ctPeakChip(song.weeklyChartPeak, 'weekly', song.weeklyChartPeakWeeks, 'wk')}</td>
            <td style="text-align:right;display:${vis('weeklyChartWeeks')};">${cellVal(song.weeklyChartWeeks)}</td>
            <td style="text-align:right;display:${vis('weeklyChartPeakWeeks')};">${cellVal(song.weeklyChartPeakWeeks)}</td>
            <td style="text-align:right;display:${vis('trlPeak')};">${ctPeakChip(song.trlPeak, 'trl', song.trlDaysAtPeak, 'day')}</td>
            <td style="text-align:right;display:${vis('trlDays')};">${cellVal(song.trlDays)}</td>
            <td style="text-align:right;display:${vis('trlDaysAtPeak')};">${cellVal(song.trlDaysAtPeak)}</td>
            <td style="text-align:right;display:${vis('vatosCuntdownPeak')};">${ctPeakChip(song.vatosCuntdownPeak, 'vatos', song.vatosCuntdownDaysAtPeak, 'day')}</td>
            <td style="text-align:right;display:${vis('vatosCuntdownDays')};">${cellVal(song.vatosCuntdownDays)}</td>
            <td style="text-align:right;display:${vis('vatosCuntdownDaysAtPeak')};">${cellVal(song.vatosCuntdownDaysAtPeak)}</td>
            <td style="text-align:right;display:${vis('billboardPeak')};">${ctPeakChip(song.billboardPeak, 'billboard', song.billboardWeeksAtPeak, 'wk')}</td>
            <td style="text-align:right;display:${vis('billboardWeeks')};">${cellVal(song.billboardWeeks)}</td>
            <td style="text-align:right;display:${vis('billboardWeeksAtPeak')};">${cellVal(song.billboardWeeksAtPeak)}</td>
            <td style="text-align:right;display:${vis('yearlyChartPeak')};">${ctPeakChip(song.yearlyChartPeak, 'yearly')}</td>
            <td style="display:${vis('genre')};">${ctChip(song.genre, 'genre')}</td>
            <td style="display:${vis('subgenre')};">${ctChip(song.subgenre, 'subgenre')}</td>
            <td style="display:${vis('ethnicity')};">${ctChip(song.ethnicity, 'ethnicity')}</td>
            <td style="display:${vis('country')};">${ctChip(song.country, 'country')}</td>
            <td style="display:${vis('language')};">${ctChip(song.language, 'language')}</td>
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
    ctRefreshPlayScale('songs');
    
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
    finishCatalogTableRender('songs');
}

/**
 * Sorts top data array by the specified column and direction
 */
const numericSortColumns = new Set([
    'plays', 'primaryPlays', 'legacyPlays', 'timeListened', 'length',
    'trackNumber', 'daysListened', 'weeksListened', 'monthsListened', 'yearsListened', 'consistency',
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
    'daysListened', 'weeksListened', 'monthsListened', 'yearsListened', 'consistency',
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

// ============================================================
// HEADERS: sort badges, "+N" next sort level, column menu
// ============================================================

// Filters panel section that matches a column, used by "Filter this column".
const ctFilterTitles = {
    plays: 'Play Count', primaryPlays: 'Play Count', legacyPlays: 'Play Count',
    length: 'Length', trackNumber: 'Track Number',
    releaseDate: 'Release Date', firstListened: 'First Listened Date', lastListened: 'Last Listened Date',
    firstFullListen: 'First Full Listen Date', lastFullListen: 'Last Full Listen Date', fullAlbumPlays: 'Full Album Plays',
    age: 'Age', ageAtRelease: 'Age at Release', birthDate: 'Birth Date', deathDate: 'Death Date',
    albumCount: 'Album Count', songCount: 'Song Count',
    artistName: 'Artist', albumName: 'Album',
    weeklyChartPeak: 'Weekly Chart', weeklyChartWeeks: 'Weekly Chart', weeklyChartPeakWeeks: 'Weekly Chart',
    seasonalChartPeak: 'Seasonal Chart', yearlyChartPeak: 'Yearly Chart',
    trlPeak: 'TRL', trlDays: 'TRL', trlDaysAtPeak: 'TRL',
    vatosCuntdownPeak: "Vato's Cuntdown", vatosCuntdownDays: "Vato's Cuntdown", vatosCuntdownDaysAtPeak: "Vato's Cuntdown",
    billboardPeak: 'Billboard', billboardWeeks: 'Billboard', billboardWeeksAtPeak: 'Billboard',
    genre: 'Genre', subgenre: 'Subgenre', ethnicity: 'Ethnicity', country: 'Country', language: 'Language'
};

const ctChevronSvg = '<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" aria-hidden="true"><path d="m6 9 6 6 6-6"/></svg>';
const ctMenuIcons = {
    desc: '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M12 5v14M6 13l6 6 6-6"/></svg>',
    asc: '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M12 19V5M6 11l6-6 6 6"/></svg>',
    then: '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M4 6h10M4 12h7M4 18h4M17 8v12M14 17l3 3 3-3"/></svg>',
    remove: '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M18 6L6 18M6 6l12 12"/></svg>',
    filter: '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M3 5h18l-7 8v6l-4-2v-4z"/></svg>',
    hide: '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M3 3l18 18M10.6 10.6a2 2 0 0 0 2.8 2.8M9.9 5.1A10 10 0 0 1 12 5c5 0 9 5 10 7a13 13 0 0 1-3 3.6M6.1 6.1C3.9 7.6 2.5 10 2 12c1 2 5 7 10 7a9.6 9.6 0 0 0 4-.9"/></svg>'
};

let ctOpenMenu = null;

function ctTableId(type) {
    return type === 'artists' ? 'topArtistsTable' : type === 'albums' ? 'topAlbumsTable' : 'topSongsTable';
}

function ctRenderTable(type) {
    if (type === 'artists') renderTopArtistsTable();
    else if (type === 'albums') renderTopAlbumsTable();
    else renderTopSongsTable();
}

function ctDefaultDirection(column) {
    return descDefaultColumns.has(column) ? 'desc' : 'asc';
}

function ctSortWords(column) {
    if (dateSortColumns.has(column)) return { desc: 'newest first', asc: 'oldest first' };
    if (numericSortColumns.has(column) || column === 'random') return { desc: 'high to low', asc: 'low to high' };
    return { desc: 'Z to A', asc: 'A to Z' };
}

/** Wraps each sortable header label with its sort badge, "+N" button and menu button (once). */
function decorateCatalogTableHeaders(table) {
    table.querySelectorAll('thead th[data-sort]').forEach(th => {
        if (th.dataset.ctDecorated === 'true') return;
        const label = th.textContent.trim();
        th.dataset.ctLabel = label;
        th.innerHTML = `<span class="ct-h"><span class="ct-label">${escapeHtml(label)}</span>`
            + '<span class="ct-sort-badge" hidden></span>'
            + '<button type="button" class="ct-add"></button>'
            + `<button type="button" class="ct-menu-btn" aria-label="${escapeHtml(label)} column menu" aria-haspopup="menu" aria-expanded="false">${ctChevronSvg}</button></span>`;
        th.dataset.ctDecorated = 'true';
    });
}

/**
 * Updates sort indicators on table headers: lime "1 ↓" badge on sorted columns and the "+N" badge
 * that adds an unsorted column as the next level.
 */
function updateSortIndicators(tableId, sortState) {
    const table = document.getElementById(tableId);
    if (!table) return;
    const sorts = normalizeTopSortState(sortState);

    table.querySelectorAll('thead th[data-sort]').forEach(th => {
        const label = th.dataset.ctLabel || th.textContent.trim();
        const index = sorts.findIndex(sort => sort.column === th.dataset.sort);
        th.classList.remove('sorted-asc', 'sorted-desc', 'ct-sorted');

        const badge = th.querySelector('.ct-sort-badge');
        if (badge) {
            badge.hidden = index < 0;
            if (index >= 0) {
                badge.className = 'ct-sort-badge ct-level-' + (index + 1);
                badge.textContent = (index + 1) + ' ' + (sorts[index].direction === 'asc' ? '↑' : '↓');
            }
        }

        const add = th.querySelector('.ct-add');
        if (add) {
            const available = index < 0 && sorts.length < 3;
            add.classList.toggle('ct-available', available);
            add.textContent = '+ ' + (sorts.length + 1);
            add.title = 'Then by ' + label + ' (sort level ' + (sorts.length + 1) + ')';
            add.setAttribute('aria-label', add.title);
        }

        if (index >= 0) {
            th.classList.add('ct-sorted', sorts[index].direction === 'asc' ? 'sorted-asc' : 'sorted-desc');
            th.title = `Sort level ${index + 1}: ${label}, ${sorts[index].direction === 'asc' ? 'ascending' : 'descending'}. Click to sort only by this column.`;
        } else {
            th.title = 'Click to sort only by ' + label;
        }
    });
}

/** Header click: sort only by this column (a second click on the first sort level flips it). */
function sortCatalogTableOnly(type, column, direction) {
    const current = normalizeTopSortState(topSortState[type]);
    let dir = direction;
    if (!dir) {
        dir = current[0] && current[0].column === column
            ? (current[0].direction === 'asc' ? 'desc' : 'asc')
            : ctDefaultDirection(column);
    }
    applyCatalogTableSorts(type, [{ column, direction: dir }]);
}

function addCatalogSortLevel(type, column) {
    const current = normalizeTopSortState(topSortState[type]);
    if (current.length >= 3 || current.some(sort => sort.column === column)) return;
    applyCatalogTableSorts(type, [...current, { column, direction: ctDefaultDirection(column) }]);
}

function removeCatalogSortLevel(type, column) {
    const remaining = normalizeTopSortState(topSortState[type]).filter(sort => sort.column !== column);
    if (remaining.length === 0) return;
    applyCatalogTableSorts(type, remaining);
}

function flipCatalogSortLevel(type, column) {
    applyCatalogTableSorts(type, normalizeTopSortState(topSortState[type]).map(sort => sort.column === column
        ? { column, direction: sort.direction === 'asc' ? 'desc' : 'asc' }
        : sort));
}

function applyCatalogTableSorts(type, sorts) {
    topSortState[type] = normalizeTopSortState(sorts);
    syncToolbarSortControls(type);
    if (currentView === 'table' && type === getCurrentEntityType()) {
        reloadListViewData(type);
        return;
    }
    ctRenderTable(type);
}

/** Keeps the toolbar Sort pills and the URL in step with sorts picked from the table headers. */
function syncToolbarSortControls(type) {
    if (type !== getCurrentEntityType()) return;
    const sorts = normalizeTopSortState(topSortState[type]);
    const url = new URL(window.location.href);
    applyListViewSortParams(url.searchParams, type);
    if (typeof syncRandomSeedParam === 'function') syncRandomSeedParam(url);
    window.history.replaceState({}, '', url.toString());

    [1, 2, 3].forEach(level => {
        const sort = sorts[level - 1];
        const select = document.getElementById(level === 1 ? 'sortBySelect' : 'sortBySelect' + level);
        if (select) {
            const value = sort ? getListViewSortParam(sort.column, type) : '';
            select.value = value;
            select.dataset.selectedSort = value;
        }
        if (typeof updateSortButton === 'function') updateSortButton(level, sort ? sort.direction : 'asc');
    });
    if (typeof initializeMobileSortPanels === 'function') initializeMobileSortPanels();
}

function ctFindFilterItem(column) {
    const title = ctFilterTitles[column];
    if (!title) return null;
    const wanted = title.toLowerCase();
    return Array.from(document.querySelectorAll('#filterPanel .filter-item')).find(item => {
        const titleEl = item.querySelector('.filter-item-title');
        if (!titleEl) return false;
        const copy = titleEl.cloneNode(true);
        copy.querySelectorAll('.filter-item-chevron').forEach(el => el.remove());
        return copy.textContent.replace(/\s+/g, ' ').trim().toLowerCase() === wanted;
    }) || null;
}

/** Opens the Filters panel at the section for this column. */
function openCatalogColumnFilter(filterItem) {
    const panel = document.getElementById('filterPanel');
    if (panel && panel.style.display !== 'block' && typeof window.toggleFilters === 'function') {
        window.toggleFilters();
    }
    window.setTimeout(() => {
        if (typeof expandFilterItem === 'function') expandFilterItem(filterItem);
        filterItem.scrollIntoView({ block: 'center', behavior: 'smooth' });
        const field = filterItem.querySelector('.filter-item-content input:not([type=hidden]), .filter-item-content select, .filter-item-content button');
        if (field) field.focus({ preventScroll: true });
    }, 0);
}

function hideCatalogColumn(type, column) {
    const singular = type === 'artists' ? 'artist' : type === 'albums' ? 'album' : 'song';
    const checkbox = document.querySelector(`#${singular}ColumnToggles input[data-col="${column}"]`);
    if (checkbox) checkbox.checked = false;
    setColumnVisibility(type, column, false);
}

function closeCatalogColumnMenu() {
    if (!ctOpenMenu) return;
    ctOpenMenu.menu.remove();
    ctOpenMenu.button?.setAttribute('aria-expanded', 'false');
    ctOpenMenu = null;
}

function openCatalogColumnMenu(type, th) {
    if (ctOpenMenu && ctOpenMenu.th === th) {
        closeCatalogColumnMenu();
        return;
    }
    closeCatalogColumnMenu();

    const column = th.dataset.sort;
    const label = th.dataset.ctLabel || column;
    const sorts = normalizeTopSortState(topSortState[type]);
    const index = sorts.findIndex(sort => sort.column === column);
    const words = ctSortWords(column);
    const firstDir = ctDefaultDirection(column);
    const secondDir = firstDir === 'desc' ? 'asc' : 'desc';
    const items = [
        { icon: ctMenuIcons[firstDir], text: 'Sort only by this, ' + words[firstDir], run: () => sortCatalogTableOnly(type, column, firstDir) },
        { icon: ctMenuIcons[secondDir], text: 'Sort only by this, ' + words[secondDir], run: () => sortCatalogTableOnly(type, column, secondDir) }
    ];
    if (index < 0 && sorts.length < 3) {
        items.push({ icon: ctMenuIcons.then, text: 'Then by this', key: 'level ' + (sorts.length + 1), run: () => addCatalogSortLevel(type, column) });
    }
    if (index > 0) {
        items.push({ icon: ctMenuIcons[sorts[index].direction === 'asc' ? 'desc' : 'asc'], text: 'Flip this sort level', key: 'level ' + (index + 1), run: () => flipCatalogSortLevel(type, column) });
    }
    if (index >= 0 && sorts.length > 1) {
        items.push({ icon: ctMenuIcons.remove, text: 'Remove from sort', key: 'level ' + (index + 1), run: () => removeCatalogSortLevel(type, column) });
    }
    const filterItem = ctFindFilterItem(column);
    const hideable = topColumnConfig[type].some(col => col.key === column);
    if (filterItem || hideable) items.push('sep');
    if (filterItem) items.push({ icon: ctMenuIcons.filter, text: 'Filter this column', run: () => openCatalogColumnFilter(filterItem) });
    if (hideable) items.push({ icon: ctMenuIcons.hide, text: 'Hide column', run: () => hideCatalogColumn(type, column) });

    const menu = document.createElement('div');
    menu.className = 'ct-menu';
    menu.setAttribute('role', 'menu');
    menu.setAttribute('aria-label', label + ' column');
    items.forEach(item => {
        if (item === 'sep') {
            const sep = document.createElement('div');
            sep.className = 'ct-menu-sep';
            menu.appendChild(sep);
            return;
        }
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'ct-menu-item';
        button.setAttribute('role', 'menuitem');
        button.innerHTML = item.icon + `<span>${escapeHtml(item.text)}</span>` + (item.key ? `<span class="ct-menu-k">${escapeHtml(item.key)}</span>` : '');
        button.addEventListener('click', event => {
            event.stopPropagation();
            closeCatalogColumnMenu();
            item.run();
        });
        menu.appendChild(button);
    });

    document.body.appendChild(menu);
    const rect = th.getBoundingClientRect();
    const left = Math.min(Math.max(8, rect.left), window.innerWidth - menu.offsetWidth - 8);
    menu.style.left = (left + window.scrollX) + 'px';
    menu.style.top = (rect.bottom + window.scrollY + 4) + 'px';

    const button = th.querySelector('.ct-menu-btn');
    button?.setAttribute('aria-expanded', 'true');
    ctOpenMenu = { menu, th, button };
    menu.querySelector('.ct-menu-item')?.focus();
}

document.addEventListener('click', event => {
    if (ctOpenMenu && !ctOpenMenu.menu.contains(event.target) && !ctOpenMenu.th.contains(event.target)) {
        closeCatalogColumnMenu();
    }
});
document.addEventListener('keydown', event => {
    if (event.key === 'Escape') closeCatalogColumnMenu();
});
window.addEventListener('resize', () => {
    closeCatalogColumnMenu();
    ['artists', 'albums', 'songs'].forEach(type => updatePinnedOffsets(type));
});

/**
 * Sets up header sorting. Click = sort only by this column, "+N" = add it as the next sort level,
 * arrow = column menu.
 */
function setupTopTableSorting() {
    ['artists', 'albums', 'songs'].forEach(type => {
        const table = document.getElementById(ctTableId(type));
        if (!table) return;
        decorateCatalogTableHeaders(table);
        table.querySelectorAll('thead th[data-sort]').forEach(th => {
            th.onclick = event => {
                const column = th.dataset.sort;
                if (event.target.closest('.ct-menu-btn')) {
                    event.stopPropagation();
                    openCatalogColumnMenu(type, th);
                    return;
                }
                if (event.target.closest('.ct-add')) {
                    event.stopPropagation();
                    addCatalogSortLevel(type, column);
                    return;
                }
                sortCatalogTableOnly(type, column);
            };
        });
        const container = table.closest('.top-table-container');
        if (container && !container.dataset.ctMenuScrollBound) {
            container.dataset.ctMenuScrollBound = 'true';
            container.addEventListener('scroll', closeCatalogColumnMenu, { passive: true });
        }
        updateSortIndicators(ctTableId(type), topSortState[type]);
    });
}

// ============================================================
// PINNED COLUMNS AND TOTALS ROW
// ============================================================

/** Measures the rank and cover columns so the pinned name column sits right after them. */
function updatePinnedOffsets(type) {
    const table = document.getElementById(ctTableId(type));
    if (!table) return;
    const rank = table.querySelector('thead th.ct-pin-rank');
    const cover = table.querySelector('thead th.ct-pin-cover');
    if (!rank || !cover || rank.offsetWidth === 0) return;
    table.style.setProperty('--ct-cover-left', rank.offsetWidth + 'px');
    table.style.setProperty('--ct-name-left', (rank.offsetWidth + cover.offsetWidth) + 'px');
}

let ctTotalsMode = (() => {
    try {
        return localStorage.getItem('catalogTableTotalsMode') || 'sum';
    } catch (error) {
        return 'sum';
    }
})();

function setCatalogTotalsMode(mode) {
    ctTotalsMode = ['sum', 'avg', 'median', 'off'].includes(mode) ? mode : 'sum';
    try {
        localStorage.setItem('catalogTableTotalsMode', ctTotalsMode);
    } catch (error) {
        // Storage can be blocked; the choice then lasts until reload.
    }
    renderCatalogTotals(getCurrentEntityType());
}

function syncTotalsModeButtons() {
    document.querySelectorAll('#ctTotalsMode button').forEach(button => {
        const active = button.dataset.mode === ctTotalsMode;
        button.classList.toggle('active', active);
        button.setAttribute('aria-pressed', active ? 'true' : 'false');
    });
}

// Columns added up in Sum mode (the play and time columns also show the male / female split).
const ctSumColumns = new Set([
    'plays', 'primaryPlays', 'legacyPlays', 'timeListened', 'albumCount', 'songCount', 'featuredOnCount',
    'featuredArtistCount', 'soloSongCount', 'songsWithFeatCount', 'fullAlbumPlays', 'daysListened',
    'weeksListened', 'monthsListened', 'yearsListened', 'weeklyChartWeeks', 'weeklyChartPeakWeeks',
    'trlDays', 'trlDaysAtPeak', 'vatosCuntdownDays', 'vatosCuntdownDaysAtPeak', 'billboardWeeks', 'billboardWeeksAtPeak'
]);
const ctSplitColumns = new Set(['plays', 'primaryPlays', 'legacyPlays', 'timeListened']);
// Columns where a sum means nothing, so Sum mode shows their average instead.
const ctAverageColumns = new Set(['length', 'avgLength', 'avgPlays', 'avgPlaysAlbum', 'age', 'ageAtRelease']);
const ctPeakColumns = new Set(['weeklyChartPeak', 'seasonalChartPeak', 'yearlyChartPeak', 'trlPeak', 'vatosCuntdownPeak', 'billboardPeak']);
const ctSecondsColumns = new Set(['timeListened', 'length', 'avgLength']);
const ctLatestDateColumns = new Set(['lastListened', 'lastFullListen']);

function ctColumnValue(item, column) {
    if (column === 'length') return item.lengthSeconds ?? item.albumLength;
    if (column === 'consistency') return item.consistencyShare;
    return item[column];
}

/** Same shape as TimeFormatUtils.formatTime ("2d 5h", "3h 45m", "20m"), or m:ss for lengths. */
function ctFormatSeconds(seconds, asClock) {
    const total = Math.round(Number(seconds) || 0);
    if (asClock) return formatAlbumLength(total) || '0:00';
    if (total <= 0) return '0m';
    const days = Math.floor(total / 86400);
    const hours = Math.floor((total % 86400) / 3600);
    const minutes = Math.floor((total % 3600) / 60);
    if (days > 0) return days + 'd ' + hours + 'h';
    if (hours > 0) return hours + 'h ' + minutes + 'm';
    return minutes + 'm';
}

function ctMedian(values) {
    const sorted = [...values].sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

function ctFormatNumber(value, decimals) {
    return Number(value).toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: decimals });
}

function ctFormatDate(date) {
    const day = String(date.getDate()).padStart(2, '0');
    const month = String(date.getMonth() + 1).padStart(2, '0');
    return `${day}/${month}/${date.getFullYear()}`;
}

function ctAggregateCell(column, items) {
    if (dateSortColumns.has(column)) {
        const dates = items.map(item => ctParseDate(item[column])).filter(Boolean).map(date => date.getTime());
        if (dates.length === 0) return '';
        const latest = ctLatestDateColumns.has(column);
        return `${ctFormatDate(new Date(latest ? Math.max(...dates) : Math.min(...dates)))}<span class="ct-muted">${latest ? 'latest' : 'earliest'}</span>`;
    }

    const values = items.map(item => ctColumnValue(item, column))
        .filter(value => value != null && value !== '' && Number.isFinite(Number(value)))
        .map(Number);
    if (values.length === 0) return '';

    if (ctPeakColumns.has(column)) {
        if (ctTotalsMode === 'sum') {
            const ones = values.filter(value => value === 1).length;
            return `${ones.toLocaleString()} at #1<span class="ct-muted">${values.length.toLocaleString()} charted</span>`;
        }
        const peak = ctTotalsMode === 'avg' ? values.reduce((a, b) => a + b, 0) / values.length : ctMedian(values);
        return '#' + ctFormatNumber(peak, 1);
    }

    let mode = ctTotalsMode;
    if (mode === 'sum' && !ctSumColumns.has(column)) {
        if (!ctAverageColumns.has(column)) return '';
        mode = 'avg';
    }
    const total = values.reduce((a, b) => a + b, 0);
    const result = mode === 'sum' ? total : mode === 'avg' ? total / values.length : ctMedian(values);
    let text = ctSecondsColumns.has(column)
        ? ctFormatSeconds(result, column === 'length' || column === 'avgLength')
        : ctFormatNumber(result, mode === 'sum' ? 0 : 1);
    if (column === 'consistency') text += '% of years';
    if (mode !== ctTotalsMode) text += '<span class="ct-muted">average</span>';

    if (mode === 'sum' && ctSplitColumns.has(column) && total > 0) {
        let male = 0;
        let female = 0;
        items.forEach(item => {
            const value = Number(ctColumnValue(item, column)) || 0;
            if (item.genderId === 2) male += value;
            else if (item.genderId === 1) female += value;
        });
        text += `<span class="ct-split"><span class="m">${(male * 100 / total).toFixed(1)}%</span> &#183; <span class="f">${(female * 100 / total).toFixed(1)}%</span></span>`;
    }
    return text;
}

/** Totals row under the table, computed from the rows loaded so far. */
function renderCatalogTotals(type) {
    syncTotalsModeButtons();
    const table = document.getElementById(ctTableId(type));
    if (!table) return;
    const items = topTabData[type] || [];
    let tfoot = table.querySelector('tfoot');
    if (ctTotalsMode === 'off' || items.length === 0) {
        if (tfoot) tfoot.remove();
        return;
    }
    if (!tfoot) {
        tfoot = document.createElement('tfoot');
        table.appendChild(tfoot);
    }

    const totalCount = listViewState.totalCount || items.length;
    const modeWord = ctTotalsMode === 'sum' ? 'Sum' : ctTotalsMode === 'avg' ? 'Average' : 'Median';
    const scope = items.length >= totalCount
        ? `of all ${totalCount.toLocaleString()}`
        : `of ${items.length.toLocaleString()} loaded<span class="ct-muted">${totalCount.toLocaleString()} match. Load all for full totals.</span>`;

    const cells = Array.from(table.querySelectorAll('thead tr:last-child th')).map(th => {
        const pinClasses = Array.from(th.classList).filter(cls => cls.startsWith('ct-pin')).join(' ');
        const style = (th.style.textAlign ? `text-align:${th.style.textAlign};` : '') + (th.style.display === 'none' ? 'display:none;' : '');
        let content = '';
        if (th.classList.contains('ct-pin-name')) {
            content = `<span class="ct-total-label">${modeWord} ${scope}</span>`;
        } else if (th.dataset.col && th.dataset.col !== 'random') {
            content = ctAggregateCell(th.dataset.col, items);
        }
        return `<td class="${pinClasses}" style="${style}">${content}</td>`;
    });
    tfoot.innerHTML = `<tr>${cells.join('')}</tr>`;
}

/** Shared tail of every render / append: totals row and pinned column offsets. */
function finishCatalogTableRender(type) {
    renderCatalogTotals(type);
    updatePinnedOffsets(type);
    revealHighlightedRow(type);
}

/*
 * Detail pages link their rank rows here with ?highlight=<id> (e.g. a song's Genre rank opens the
 * Songs table filtered to that genre). Once the item's row is rendered it scrolls into view and
 * glows; until then more pages load, up to a cap, since the item can sit past the first page.
 */
const HIGHLIGHT_MAX_ROWS = 3000;
let pendingHighlightId = new URLSearchParams(window.location.search).get('highlight');

function revealHighlightedRow(type) {
    if (!pendingHighlightId || type !== getCurrentEntityType()) return;
    const row = document.querySelector(`tr[data-id="${CSS.escape(pendingHighlightId)}"]`);
    if (row) {
        pendingHighlightId = null;
        const url = new URL(window.location.href);
        url.searchParams.delete('highlight');
        history.replaceState(history.state, '', url);
        requestAnimationFrame(() => {
            row.scrollIntoView({ behavior: 'smooth', block: 'center' });
            row.classList.remove('ct-row-glow');
            void row.offsetWidth;
            row.classList.add('ct-row-glow');
        });
        return;
    }
    // Not rendered yet: load the next page once the current one has settled
    setTimeout(() => {
        if (!pendingHighlightId || listViewState.loading || listViewState.allLoaded) return;
        if (getListViewLoadedCount() >= HIGHLIGHT_MAX_ROWS) {
            pendingHighlightId = null;
            return;
        }
        fetchListPage(listViewState.page);
    }, 0);
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
    if (ctRefreshPlayScale('artists')) {
        renderTopArtistsTable();
        return;
    }
    data.forEach((artist, index) => {
        tbody.insertAdjacentHTML('beforeend', buildArtistRow(artist, startRank + index + 1));
    });
    if (typeof window.initializePlayBreakdownTooltips === 'function') {
        window.initializePlayBreakdownTooltips(tbody);
    }
    finishCatalogTableRender('artists');
}

/**
 * Append albums rows to table
 */
function appendTopAlbumsRows(data, startRank) {
    const tbody = document.querySelector('#topAlbumsTable tbody');
    if (!tbody) return;
    if (ctRefreshPlayScale('albums')) {
        renderTopAlbumsTable();
        return;
    }
    data.forEach((album, index) => {
        tbody.insertAdjacentHTML('beforeend', buildAlbumRow(album, startRank + index + 1));
    });
    finishCatalogTableRender('albums');
}

/**
 * Append songs rows to table
 */
function appendTopSongsRows(data, startRank) {
    const tbody = document.querySelector('#topSongsTable tbody');
    if (!tbody) return;
    if (ctRefreshPlayScale('songs')) {
        renderTopSongsTable();
        return;
    }
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
    finishCatalogTableRender('songs');
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
    if (tbody) {
        tbody.innerHTML = '';
        tbody.parentElement.querySelector('tfoot')?.remove();
    }

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
    params.delete('highlight');
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
    applyConsistency(item);

    return item;
}

/**
 * Consistency column: years with plays out of the years since the first play. The sort value is years first,
 * then the later first year (higher share), matching the server's Consistency sort; plays break the rest.
 */
function applyConsistency(item) {
    const years = Number(item.yearsListened) || 0;
    const firstYear = parseInt(String(item.firstListenedDate || '').trim().split(/[-\/ ]/).pop(), 10);
    if (!firstYear || years <= 0) {
        item.consistency = years * 10000;
        item.consistencyShare = null;
        item.consistencyLabel = null;
        return;
    }
    const total = Math.max(1, new Date().getFullYear() - firstYear + 1);
    item.consistency = years * 10000 + firstYear;
    item.consistencyShare = Math.min(100, years * 100 / total);
    item.consistencyLabel = `${years} / ${total} (${Math.round(item.consistencyShare)}%)`;
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

