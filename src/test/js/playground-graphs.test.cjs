const assert = require('node:assert/strict');
const test = require('node:test');
const { normalizeData, visibleRows, formatValue, readState, apiParams, tableColumns, sortTableRows, createExplorer } = require('../../main/resources/static/js/playground-graphs.js');

test('breakdowns align measures by category name and retain other/unknown gender', () => {
    const data = normalizeData({ playsByGenre: [{ name: 'Rock', male: 30, female: 60, other: 10 }], songsByGenre: [{ name: 'Pop', male: 2 }, { name: 'Rock', female: 4 }] }, 'genre');
    assert.deepEqual(data.rows.map(row => row.name).sort(), ['Pop', 'Rock']);
    const rock = data.rows.find(row => row.name === 'Rock');
    assert.equal(rock.measures.plays.total, 100);
    assert.equal(rock.measures.plays.other, 10);
    assert.equal(rock.measures.songs.total, 4);
    assert.equal(data.available.has('artists'), false);
});

test('share ordering differs from volume ordering, search is case insensitive, years stay chronological', () => {
    const rows = normalizeData({ playsByGenre: [{ name: 'Heavy', male: 900, female: 100 }, { name: 'Quiet', male: 1, female: 9 }] }, 'genre').rows;
    assert.equal(visibleRows(rows, 'plays', '', 'total')[0].name, 'Heavy');
    assert.equal(visibleRows(rows, 'plays', '', 'male')[0].name, 'Heavy');
    assert.equal(visibleRows(rows, 'plays', '', 'male', 'asc')[0].name, 'Quiet');
    assert.equal(visibleRows(rows, 'plays', '', 'total', 'asc')[0].name, 'Quiet');
    assert.equal(visibleRows(rows, 'plays', '', 'name', 'desc')[0].name, 'Quiet');
    assert.equal(visibleRows(rows, 'plays', 'QUI', 'total').length, 1);
    assert.equal(rows[0].name, 'Heavy');
    const years = normalizeData({ songsByListenYear: [{ name: 'Unknown' }, { name: '1999' }, { name: '2026' }] }, 'listenYear');
    assert.deepEqual(visibleRows(years.rows, 'songs', '', 'year').map(row => row.name), ['2026', '1999', 'Unknown']);
    assert.deepEqual(visibleRows(years.rows, 'songs', '', 'year', 'asc').map(row => row.name), ['1999', '2026', 'Unknown']);
});

test('API query retains repeated catalog filters and modes while removing display settings', () => {
    const state = readState(new URLSearchParams('view=graphs&tab=genre&source=albums&gender=1&gender=2&genreMode=isnull&lengthMin=600&graphMetric=songs&graphDirection=asc&tableSort=plays.maleShare&tableDirection=asc&categoryQ=pop&categorySize=48&sortby=name'));
    const params = apiParams(state);
    assert.deepEqual(params.getAll('gender'), ['1', '2']);
    assert.equal(params.get('genreMode'), 'isnull');
    assert.equal(params.get('lengthMin'), '600');
    assert.equal(params.get('source'), 'albums');
    assert.equal(params.has('sortby'), false);
    assert.equal(params.has('categoryQ'), false);
    for (const key of ['graphDirection', 'tableSort', 'tableDirection']) assert.equal(params.has(key), false);
    assert.equal(state.direction, 'asc');
    assert.equal(state.tableSort, 'plays.maleShare');
    assert.equal(state.tableDirection, 'asc');
    assert.equal(state.size, 48);
});

test('invalid bookmarks fall back safely and unavailable artist release-year measure remains explicit', () => {
    const state = readState(new URLSearchParams('source=bad&tab=bad&graphMetric=bad&categoryPage=-9&categorySize=9999'));
    assert.equal(state.source, 'songs'); assert.equal(state.tab, 'general'); assert.equal(state.page, 0); assert.equal(state.size, 12);
    const data = normalizeData({ albumsByReleaseYear: [], songsByReleaseYear: [] }, 'releaseYear');
    assert.equal(data.available.has('artists'), false);
    assert.equal(formatValue(5, 'listeningTime'), '5s');
    assert.equal(formatValue(90000, 'listeningTime'), '1d 1h');
    const oldFemaleSort = readState(new URLSearchParams('graphOrder=female&tableSort=plays.female&tableDirection=invalid'));
    assert.equal(oldFemaleSort.order, 'total');
    assert.equal(oldFemaleSort.tableSort, 'plays.total');
    assert.equal(oldFemaleSort.tableDirection, 'desc');
});

class Element {
    constructor() { this.children = []; this.listeners = {}; this.style = {}; this.attributes = {}; this.value = ''; this.options = []; this.dataset = {}; this.classList = { toggle() {} }; }
    append(...children) { this.children.push(...children); }
    prepend(child) { this.children.unshift(child); }
    replaceChildren(...children) { this.children = children; }
    setAttribute(key, value) { this.attributes[key] = value; }
    addEventListener(key, callback) { this.listeners[key] = callback; }
    focus() { this.focused = true; }
    querySelector() { return new Element(); }
    querySelectorAll() { return []; }
}
const flush = () => new Promise(resolve => setImmediate(resolve));
function setup(fetchData, search = '?view=graphs&tab=genre&graphScale=relative') {
    const elements = new Map(), listeners = {};
    const get = key => { if (!elements.has(key)) elements.set(key, new Element()); return elements.get(key); };
    get('graphs-metric').options = ['artists', 'albums', 'songs', 'plays', 'listeningTime'].map(value => ({ value }));
    const document = { getElementById: get, createElement: () => new Element(), addEventListener: (name, callback) => { listeners[name] = callback; } };
    const window = { location: { search, href: 'http://localhost/playground' + search }, addEventListener() {}, history: {
        replaceState(_a, _b, url) { window.location.href = url; }, pushState(_a, _b, url) { window.location.href = url; }
    } };
    createExplorer(document, window, fetchData);
    return { get: key => get('graphs-' + key), dispatch: (name, detail) => listeners[name]({ detail }) };
}

test('category search and pagination do not change totals or relative scale denominator', async () => {
    const data = { playsByGenre: Array.from({ length: 30 }, (_, i) => ({ name: `Category ${i + 1}`, male: (i + 1) * 10, female: 0, other: 0 })) };
    const ui = setup(async () => ({ ok: true, json: async () => data }));
    await flush();
    assert.equal(ui.get('bars').children.length, 12);
    assert.equal(ui.get('page-label').textContent, '1-12 of 30 categories');
    ui.get('search').value = 'Category 1'; ui.get('search').listeners.input();
    assert.equal(ui.get('bars').children.length, 11);
    const last = ui.get('bars').children.at(-1);
    assert.equal(last.children[0].textContent, 'Category 1');
    assert.ok(Math.abs(parseFloat(last.children[1].children[0].style.width) - 100 / 30) < .001);
    assert.equal(ui.get('summary').children[3].children[1].textContent, (4650).toLocaleString());
});

test('bar totals highlight male share, including other gender values and empty totals', async () => {
    const ui = setup(async () => ({ ok: true, json: async () => ({ playsByGenre: [
        { name: 'Mixed', male: 30, female: 60, other: 10 },
        { name: 'Empty', male: 0, female: 0, other: 0 }
    ] }) }));
    await flush();
    const rows = ui.get('bars').children;
    assert.equal(rows[0].children[2].children[0].textContent, '30.0% male');
    assert.equal(rows[1].children[2].children[0].textContent, '0.0% male');
});

test('the selected measure has gender columns and other measures have one sortable total column each', () => {
    const available = new Set(['artists', 'albums', 'songs', 'plays', 'listeningTime']);
    const columns = [...available].flatMap(metric => {
        const selected = tableColumns(metric, available);
        assert.equal(selected.length, 7);
        assert.deepEqual(selected.filter(column => !column.comparison).map(column => column.label), ['Total', 'Male', 'Male %']);
        assert.equal(selected.filter(column => column.comparison && column.stat === 'total').length, 4);
        assert.equal(new Set(selected.map(column => column.key)).size, 7);
        return selected;
    });
    assert.equal(columns.some(column => /female|other/i.test(column.key + column.label)), false);
    for (const removed of ['plays.other', 'plays.otherShare']) {
        assert.equal(readState(new URLSearchParams('tableSort=' + removed)).tableSort, 'plays.total');
    }
    const low = { name: 'Low', measures: {} }, high = { name: 'High', measures: {} };
    for (const metric of available) {
        low.measures[metric] = { male: 1, female: 98, other: 1, total: 100 };
        high.measures[metric] = { male: 20, female: 160, other: 20, total: 200 };
    }
    for (const column of columns) {
        assert.equal(sortTableRows([low, high], column.key, 'asc')[0].name, 'Low', column.key);
        assert.equal(sortTableRows([low, high], column.key, 'desc')[0].name, 'High', column.key);
    }
    assert.equal(sortTableRows([low, high], 'name', 'asc')[0].name, 'High');
    assert.equal(sortTableRows([low, high], 'name', 'desc')[0].name, 'Low');
});

test('one table follows the selected measure, keeps other totals visible, and sorts all matching categories', async () => {
    const requests = [];
    const ui = setup(async url => {
        requests.push(url);
        return { ok: true, json: async () => ({
            playsByGenre: Array.from({ length: 30 }, (_, i) => ({ name: `Category ${i + 1}`, male: (i + 1) * 10, female: 0, other: 0 })),
            listeningTimeByGenre: [{ name: 'Category 1', male: 7200 }, { name: 'Category 30', male: 1800 }]
        }) };
    });
    await flush();
    const table = () => ui.get('data-table');
    const body = () => table().children[2];
    const heading = key => table().children[1].children.flatMap(row => row.children).find(cell => cell.dataset.sortKey === key);
    assert.equal(ui.get('bars').children.length, 12);
    assert.equal(body().children.length, 30);
    assert.equal(body().children[0].children.length, 5);
    assert.equal(heading('listeningTime.male'), undefined);
    assert.equal(body().children[0].children[0].textContent, 'Category 30');
    heading('plays.total').children[0].listeners.click();
    assert.equal(body().children[0].children[0].textContent, 'Category 1');
    assert.equal(heading('plays.total').attributes['aria-sort'], 'ascending');
    assert.equal(heading('plays.total').children[0].focused, true);
    assert.equal(ui.get('bars').children[0].children[0].textContent, 'Category 30');
    heading('plays.total').children[0].listeners.click();
    assert.equal(body().children[0].children[0].textContent, 'Category 30');
    heading('listeningTime.total').children[0].listeners.click();
    assert.equal(body().children[0].children[0].textContent, 'Category 1');
    heading('name').children[0].listeners.click();
    assert.equal(body().children[1].children[0].textContent, 'Category 2');
    ui.get('direction').value = 'asc'; ui.get('direction').listeners.change();
    assert.equal(ui.get('bars').children[0].children[0].textContent, 'Category 1');
    ui.get('metric').value = 'listeningTime'; ui.get('metric').listeners.change();
    assert.deepEqual(table().children[1].children[0].children.map(cell => cell.dataset.sortKey),
        ['name', 'listeningTime.total', 'listeningTime.male', 'listeningTime.maleShare', 'plays.total']);
    assert.equal(body().children[0].children[1].textContent, '2h 0m');
    assert.equal(body().children[0].children[4].textContent, '10');
    assert.ok(table().children[0].textContent.startsWith('Listening time by genre:'));
    heading('listeningTime.maleShare').children[0].listeners.click();
    ui.get('summary').children[3].listeners.click();
    assert.equal(heading('plays.maleShare').attributes['aria-sort'], 'descending');
    assert.equal(heading('listeningTime.male'), undefined);
    ui.get('search').value = 'Category 30'; ui.get('search').listeners.input();
    assert.equal(body().children.length, 1);
    assert.equal(requests.length, 1);
});

test('a late response cannot replace newer filters, and failure keeps retry available', async () => {
    const requests = [];
    const ui = setup((url, options) => new Promise(resolve => requests.push({ url, options, resolve })));
    ui.dispatch('playground:filters-applied', { source: 'songs', params: new URLSearchParams('genre=2') });
    assert.equal(requests[0].options.signal.aborted, true);
    requests[1].resolve({ ok: true, json: async () => ({ playsByGenre: [{ name: 'New', female: 9 }] }) });
    await flush();
    requests[0].resolve({ ok: true, json: async () => ({ playsByGenre: [{ name: 'Old', male: 99 }] }) });
    await flush();
    assert.equal(ui.get('bars').children[0].children[0].textContent, 'New');
    ui.dispatch('playground:filters-applied', { source: 'songs', params: new URLSearchParams('genre=3') });
    requests[2].resolve({ ok: false }); await flush();
    assert.equal(ui.get('retry').hidden, false);
    assert.equal(ui.get('results').hidden, true);
});
