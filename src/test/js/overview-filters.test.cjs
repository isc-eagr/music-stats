const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

function loadFilters(search) {
    let destination;
    const window = { location: { search, pathname: '/charts/weekly/overview', assign: value => { destination = value; } } };
    const document = {
        addEventListener() {},
        querySelectorAll: () => ['artist', 'totalChartSpan'].map(filterName => ({ dataset: { filterName } }))
    };
    vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../../main/resources/static/js/overview-filters.js'), 'utf8'), { window, document, URLSearchParams, Set });
    return { filters: window.overviewFilters, destination: () => destination };
}

test('paging and export retain repeated values and modes without changing requested page or sort', () => {
    const { filters } = loadFilters('?artist=1&artist=2&artistMode=excludes&includeFeaturedSongs=true&totalChartSpan=5&totalChartSpanTo=10&totalChartSpanMode=between&topSong=3&q=music');
    for (const endpoint of ['data', 'song-export']) {
        const url = new URL('http://localhost/charts/weekly/overview/' + endpoint + '?page=2&sort=song&dir=asc');
        filters.appendTo(url);
        assert.deepEqual(url.searchParams.getAll('artist'), ['1', '2']);
        assert.equal(url.searchParams.get('artistMode'), 'excludes');
        assert.equal(url.searchParams.get('includeFeaturedSongs'), 'true');
        assert.equal(url.searchParams.get('totalChartSpanTo'), '10');
        assert.equal(url.searchParams.get('totalChartSpanMode'), 'between');
        assert.equal(url.searchParams.get('topSong'), '3');
        assert.equal(url.searchParams.get('q'), 'music');
        assert.equal(url.searchParams.get('page'), '2');
        assert.equal(url.searchParams.get('sort'), 'song');
    }
});

test('reset clears values, modes, search, thresholds and paging while preserving the overview settings', () => {
    const state = loadFilters('?overviewTab=artist&artist=1&artistMode=excludes&includeFeaturedSongs=true&topSong=3&q=music&page=2&sort=songs&dir=desc&includeFeatured=true');
    state.filters.reset();
    const result = new URL(state.destination(), 'http://localhost');
    assert.equal(result.searchParams.get('overviewTab'), 'artist');
    assert.equal(result.searchParams.get('includeFeatured'), 'true');
    assert.equal(result.searchParams.get('sort'), 'songs');
    for (const name of ['artist', 'artistMode', 'includeFeaturedSongs', 'topSong', 'q', 'page']) assert.equal(result.searchParams.has(name), false);
});

test('reusing a URL cannot duplicate filters or retain cleared values', () => {
    const { filters } = loadFilters('?artist=1');
    const url = new URL('http://localhost/data?artist=2&totalChartSpan=5&q=old');
    filters.appendTo(url);
    filters.appendTo(url);
    assert.deepEqual(url.searchParams.getAll('artist'), ['1']);
    assert.equal(url.searchParams.has('totalChartSpan'), false);
    assert.equal(url.searchParams.has('q'), false);
});

test('sticky header support targets every chart overview table family', () => {
    const source = fs.readFileSync(path.join(__dirname, '../../main/resources/static/js/overview-sticky-headers.js'), 'utf8');
    for (const selector of ['.overview-table', '.pc-table', '.trl-table', '.bb-table']) {
        assert.match(source, new RegExp(selector.replace('.', '\\.')));
    }
    assert.match(source, /position: fixed|overview-sticky-header-layer/);
});
