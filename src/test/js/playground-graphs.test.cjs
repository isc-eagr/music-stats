const assert = require('node:assert/strict');
const test = require('node:test');
const { normalizeData, formatValue, readState, apiParams, tableColumns, sortTableRows, createExplorer } = require('../../main/resources/static/js/playground-graphs.js');

test('breakdowns align measures by category name and retain other/unknown gender', () => {
    const data = normalizeData({ playsByGenre: [{ name: 'Rock', male: 30, female: 60, other: 10 }], songsByGenre: [{ name: 'Pop', male: 2 }, { name: 'Rock', female: 4 }] }, 'genre');
    assert.deepEqual(data.rows.map(row => row.name).sort(), ['Pop', 'Rock']);
    const rock = data.rows.find(row => row.name === 'Rock');
    assert.equal(rock.measures.plays.total, 100);
    assert.equal(rock.measures.plays.other, 10);
    assert.equal(rock.measures.songs.total, 4);
    assert.equal(data.available.has('artists'), false);
});

test('API query retains repeated catalog filters and modes while removing display and legacy settings', () => {
    const state = readState(new URLSearchParams('view=graphs&tab=genre&source=albums&gender=1&gender=2&genreMode=isnull&lengthMin=600&graphMetric=songs&graphExpand=plays,artists&graphDirection=asc&tableSort=plays.maleShare&tableDirection=asc&categoryQ=pop&categorySize=48&sortby=name'));
    const params = apiParams(state);
    assert.deepEqual(params.getAll('gender'), ['1', '2']);
    assert.equal(params.get('genreMode'), 'isnull');
    assert.equal(params.get('lengthMin'), '600');
    assert.equal(params.get('source'), 'albums');
    for (const key of ['sortby', 'categoryQ', 'categorySize', 'graphDirection', 'graphExpand', 'tableSort', 'tableDirection']) assert.equal(params.has(key), false, key);
    assert.deepEqual([...state.expanded].sort(), ['artists', 'plays']);
    assert.equal(state.tableSort, 'plays.maleShare');
    assert.equal(state.tableDirection, 'asc');
});

test('bookmarks fall back safely, a sort on a collapsed measure expands it, and years default to newest first', () => {
    const state = readState(new URLSearchParams('source=bad&tab=bad&graphMetric=bad&graphExpand=bad,plays'));
    assert.equal(state.source, 'songs'); assert.equal(state.tab, 'general'); assert.equal(state.metric, 'plays');
    assert.equal(state.expanded.size, 0);
    assert.equal(state.tableSort, 'plays.total'); assert.equal(state.tableDirection, 'desc');
    const oldFemaleSort = readState(new URLSearchParams('tableSort=plays.female&tableDirection=invalid'));
    assert.equal(oldFemaleSort.tableSort, 'plays.total');
    const collapsedSort = readState(new URLSearchParams('graphMetric=plays&tableSort=songs.male'));
    assert.deepEqual([...collapsedSort.expanded], ['songs']);
    const years = readState(new URLSearchParams('tab=listenYear'));
    assert.equal(years.tableSort, 'name'); assert.equal(years.tableDirection, 'desc');
    assert.equal(formatValue(5, 'listeningTime'), '5s');
    assert.equal(formatValue(90000, 'listeningTime'), '1d 1h');
});

test('the selected measure has male columns, other measures collapse to one total and expand to the same breakdown', () => {
    const available = new Set(['artists', 'albums', 'songs', 'plays', 'listeningTime']);
    const collapsed = tableColumns('plays', available);
    assert.deepEqual(collapsed.map(column => column.label), ['Plays', 'Share %', 'Male plays', 'Male %', 'Artists', 'Albums', 'Songs', 'Listening time']);
    assert.equal(collapsed.filter(column => column.comparison).every(column => column.stat === 'total'), true);
    const expanded = tableColumns('plays', available, new Set(['artists']));
    assert.deepEqual(expanded.filter(column => column.metric === 'artists').map(column => column.label), ['Artists', 'Share %', 'Male artists', 'Male %']);
    assert.equal(expanded.length, 11);
    const all = tableColumns('artists', available, available);
    assert.equal(all.length, 20);
    assert.equal(all.some(column => /female|other/i.test(column.key + column.label)), false);
    assert.equal(tableColumns('plays', new Set(['plays', 'songs'])).length, 5);
});

test('every column sorts both ways and numeric names keep Unknown last', () => {
    const available = new Set(['artists', 'albums', 'songs', 'plays', 'listeningTime']);
    const low = { name: 'Low', measures: {} }, high = { name: 'High', measures: {} };
    for (const metric of available) {
        low.measures[metric] = { male: 1, female: 98, other: 1, total: 100 };
        high.measures[metric] = { male: 20, female: 160, other: 20, total: 200 };
    }
    for (const column of tableColumns('plays', available, available)) {
        assert.equal(sortTableRows([low, high], column.key, 'asc')[0].name, 'Low', column.key);
        assert.equal(sortTableRows([low, high], column.key, 'desc')[0].name, 'High', column.key);
    }
    assert.equal(sortTableRows([low, high], 'name', 'asc')[0].name, 'High');
    const years = ['Unknown', '1999', '2026', '2005'].map(name => ({ name, measures: {} }));
    assert.deepEqual(sortTableRows(years, 'name', 'desc').map(row => row.name), ['2026', '2005', '1999', 'Unknown']);
    assert.deepEqual(sortTableRows(years, 'name', 'asc').map(row => row.name), ['1999', '2005', '2026', 'Unknown']);
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
function setup(fetchData, search = '?view=graphs&tab=genre') {
    const elements = new Map(), listeners = {};
    const get = key => { if (!elements.has(key)) elements.set(key, new Element()); return elements.get(key); };
    const document = { getElementById: get, createElement: () => new Element(), addEventListener: (name, callback) => { listeners[name] = callback; } };
    const window = { location: { search, href: 'http://localhost/playground' + search }, addEventListener() {}, history: {
        replaceState(_a, _b, url) { window.location.href = url; }, pushState(_a, _b, url) { window.location.href = url; }
    } };
    createExplorer(document, window, fetchData);
    const table = () => get('graphs-data-table');
    return {
        window,
        get: key => get('graphs-' + key),
        dispatch: (name, detail) => listeners[name]({ detail }),
        body: () => table().children[2].children,
        groups: () => table().children[1].children[0].children,
        headings: () => table().children[1].children[1].children,
        heading: key => table().children[1].children[1].children.find(cell => cell.dataset.sortKey === key)
    };
}
const genreData = () => ({
    playsByGenre: Array.from({ length: 30 }, (_, i) => ({ name: `Category ${i + 1}`, male: (i + 1) * 10, female: 0, other: 0 })),
    artistsByGenre: [{ name: 'Category 1', male: 3, female: 1 }, { name: 'Category 30', male: 1 }],
    listeningTimeByGenre: [{ name: 'Category 1', male: 7200 }, { name: 'Category 30', male: 1800 }]
});

test('every category is shown, one sort drives the table, and summary tiles switch the painted measure', async () => {
    const requests = [];
    const ui = setup(async url => { requests.push(url); return { ok: true, json: async () => genreData() }; });
    await flush();
    assert.equal(ui.body().length, 30);
    assert.deepEqual(ui.headings().map(cell => cell.dataset.sortKey), ['name', undefined, 'plays.total', 'plays.share', 'plays.male', 'plays.maleShare', 'artists.total', 'listeningTime.total']);
    assert.equal(ui.body()[0].children[0].textContent, 'Category 30');
    ui.heading('plays.total').children[0].listeners.click();
    assert.equal(ui.body()[0].children[0].textContent, 'Category 1');
    assert.equal(ui.heading('plays.total').attributes['aria-sort'], 'ascending');
    assert.equal(ui.heading('plays.total').children[0].focused, true);
    ui.heading('listeningTime.total').children[0].listeners.click();
    assert.equal(ui.body()[0].children[0].textContent, 'Category 1');
    ui.heading('plays.maleShare').children[0].listeners.click();
    ui.get('summary').children[4].listeners.click();
    assert.equal(ui.heading('listeningTime.maleShare').attributes['aria-sort'], 'descending');
    assert.deepEqual(ui.headings().slice(2, 6).map(cell => cell.children[0].textContent), ['Listening time', 'Share %', 'Male time', 'Male %']);
    assert.equal(ui.body()[0].children[2].textContent, '2h 0m');
    assert.equal(ui.body()[0].children[3].textContent, '80.0%');
    assert.equal(ui.get('summary').children[3].children[3].textContent, '100.0% male');
    assert.equal(ui.get('table-title').textContent, 'Listening time by genre');
    assert.equal(requests.length, 1);
});

test('expanding a measure right of the separator shows its full breakdown and collapsing folds it back', async () => {
    const ui = setup(async () => ({ ok: true, json: async () => genreData() }));
    await flush();
    const toggle = () => ui.groups().find(cell => cell.children.at(-1)?.attributes['aria-label']?.endsWith('Artists')).children.at(-1);
    assert.equal(toggle().attributes['aria-expanded'], 'false');
    toggle().listeners.click();
    assert.equal(toggle().attributes['aria-expanded'], 'true');
    assert.equal(toggle().focused, true);
    assert.ok(ui.window.location.href.includes('graphExpand=artists'));
    assert.deepEqual(ui.headings().slice(6, 10).map(cell => cell.dataset.sortKey), ['artists.total', 'artists.share', 'artists.male', 'artists.maleShare']);
    const first = ui.body().find(row => row.children[0].textContent === 'Category 1');
    assert.deepEqual(first.children.slice(6, 10).map(cell => cell.textContent), ['4', '80.0%', '3', '75.0%']);
    ui.heading('artists.male').children[0].listeners.click();
    toggle().listeners.click();
    assert.equal(ui.heading('artists.male'), undefined);
    assert.equal(ui.heading('artists.total').attributes['aria-sort'], 'descending');
    assert.ok(!ui.window.location.href.includes('graphExpand'));
});

test('bars show the gender split by share or by volume, including other gender values', async () => {
    const ui = setup(async () => ({ ok: true, json: async () => ({ playsByGenre: [
        { name: 'Mixed', male: 30, female: 60, other: 10 },
        { name: 'Small', male: 25 }
    ] }) }));
    await flush();
    const widths = row => ui.body()[row].children[1].children[0].children.map(segment => parseFloat(segment.style.width));
    assert.deepEqual(widths(0), [30, 60, 10]);
    assert.ok(ui.body()[0].children[1].title.startsWith('80.0% of total; '));
    const volume = ui.headings()[1].children.find(child => child.textContent === 'volume');
    volume.listeners.click();
    assert.deepEqual(widths(1), [25, 0, 0]);
    assert.ok(ui.window.location.href.includes('graphScale=relative'));
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
    assert.equal(ui.body()[0].children[0].textContent, 'New');
    ui.dispatch('playground:filters-applied', { source: 'songs', params: new URLSearchParams('genre=3') });
    requests[2].resolve({ ok: false }); await flush();
    assert.equal(ui.get('retry').hidden, false);
    assert.equal(ui.get('results').hidden, true);
});
