const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

function loadFilters() {
    const window = {};
    const document = { addEventListener() {} };
    vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../../main/resources/static/js/playground-graph-filters.js'), 'utf8'), {
        window, document, URLSearchParams, Set, Map
    });
    return window.playgroundGraphFilters;
}

const filters = loadFilters();
const field = (name, value, extra = {}) => ({ name, value, type: 'text', ...extra });

// The project has no browser DOM dependency. Adapt the catalog form fixtures to
// the small read-only DOM surface used by schema extraction.
function catalogFormFixture(catalog) {
    const html = fs.readFileSync(path.join(__dirname, '../../main/resources/templates', catalog, 'list.html'), 'utf8');
    const form = html.match(/<form\b[^>]*id="filterForm"[\s\S]*?<\/form>/)[0];
    const attributes = tag => Object.fromEntries(Array.from(tag.matchAll(/\s([\w:-]+)=(?:"([^"]*)"|'([^']*)')/g), match => [match[1], match[2] ?? match[3]]));
    const text = value => value.replace(/<[^>]*>/g, '').replace(/\s+/g, ' ').trim();
    const items = form.split('<div class="filter-item">').slice(1).map(part => {
        const controls = Array.from(part.matchAll(/<input\b[^>]*>|<(select|textarea)\b[^>]*>[\s\S]*?<\/\1>/g), match => {
            const attrs = attributes(match[0].match(/^<[^>]*>/)[0]);
            const tagName = match[0].startsWith('<input') ? 'INPUT' : match[1].toUpperCase();
            const options = Array.from(match[0].matchAll(/<option\b([^>]*)>([\s\S]*?)<\/option>/g), option => ({ value: attributes('<option ' + option[1] + '>').value || '', textContent: text(option[2]) }));
            return { name: attrs.name, tagName, type: attrs.type || 'text', value: attrs.value || '', checked: false,
                placeholder: attrs.placeholder || '', options, closest: () => null, previousElementSibling: null,
                getAttribute: name => attrs[name] ?? null };
        }).filter(control => control.name);
        const modifiers = Array.from(part.matchAll(/<div class="filter-modifiers">([\s\S]*?)<\/div>/g), match => ({
            querySelectorAll: () => Array.from(match[1].matchAll(/<button\b([^>]*)>([\s\S]*?)<\/button>/g), button => {
                const attrs = attributes('<button ' + button[1] + '>');
                return { dataset: { filter: attrs['data-filter'], mode: attrs['data-mode'] }, textContent: text(button[2]), classList: { contains: () => false } };
            }).filter(button => button.dataset.filter && button.dataset.mode)
        }));
        const chipSelects = Array.from(part.matchAll(/\bid="([^"]+ChipSelect)"/g), match => ({ id: match[1] }));
        const title = text(part.match(/filter-item-title[^>]*>([\s\S]*?)<\/div>/)[1]);
        return {
            controls,
            querySelector: selector => selector === '.filter-item-title' ? { textContent: title } : null,
            querySelectorAll: selector => {
                if (selector === '.filter-modifiers') return modifiers;
                if (selector === '[id$="ChipSelect"]') return chipSelects;
                return selector === 'input[name]' ? controls.filter(control => control.tagName === 'INPUT') : controls;
            }
        };
    });
    return { items, querySelectorAll: () => items };
}

test('applying catalog filters keeps repeated selected values and explorer settings', () => {
    const current = new URLSearchParams('view=graphs&tab=genre&graphMetric=plays&graphScale=share&graphDirection=asc&tableSort=songs.male&tableDirection=desc&categoryQ=pop&categorySize=24&compareMeasures=true&page=9&sortby=name&country=old&limit=8&limitEntity=artist');
    const result = filters.serializeFields([
        field('genre', '1', { type: 'checkbox', checked: true }),
        field('genre', '2', { type: 'checkbox', checked: true }),
        field('genre', '3', { type: 'checkbox', checked: false }),
        field('genreMode', 'excludes'), field('q', 'live'), field('playCountMin', '0')
    ], current, 'songs');
    assert.deepEqual(result.getAll('genre'), ['1', '2']);
    assert.equal(result.get('genreMode'), 'excludes');
    assert.equal(result.get('q'), 'live');
    assert.equal(result.get('playCountMin'), '0');
    for (const key of ['view', 'tab', 'graphMetric', 'graphScale', 'graphDirection', 'tableSort', 'tableDirection', 'categoryQ', 'categorySize', 'limit', 'limitEntity']) {
        assert.equal(result.get(key), current.get(key));
    }
    for (const key of ['page', 'sortby', 'country', 'compareMeasures']) assert.equal(result.has(key), false);
});

test('null filters retain their modes and discard irrelevant stale values', () => {
    const result = filters.serializeFields([
        field('genderMode', 'isnull'), field('gender', '2', { type: 'checkbox', checked: true }),
        field('deathDateMode', 'isnotnull'), field('deathDate', '14/10/2026'),
        field('lengthMode', 'zero'), field('lengthMin', '3:45', { duration: true }),
        field('genreMode', 'includes')
    ], new URLSearchParams(), 'songs');
    for (const [key, value] of [['genderMode', 'isnull'], ['deathDateMode', 'isnotnull'], ['lengthMode', 'zero']]) assert.equal(result.get(key), value);
    for (const key of ['gender', 'deathDate', 'lengthMin', 'genreMode']) assert.equal(result.has(key), false);
});

test('date range and exact modes submit only their matching date fields', () => {
    const fields = [field('releaseDateMode', 'between'), field('releaseDate', '01/01/2000'), field('releaseDateFrom', '14/10/2025'), field('releaseDateTo', '14/10/2026')];
    const range = filters.serializeFields(fields, new URLSearchParams(), 'albums');
    assert.equal(range.has('releaseDate'), false);
    assert.equal(range.get('releaseDateFrom'), '14/10/2025');
    assert.equal(range.get('releaseDateTo'), '14/10/2026');
    fields[0].value = 'exact';
    const exact = filters.serializeFields(fields, new URLSearchParams(), 'albums');
    assert.equal(exact.get('releaseDate'), '01/01/2000');
    assert.equal(exact.has('releaseDateFrom'), false);
    assert.equal(exact.has('releaseDateTo'), false);
});

test('length format converts into seconds with correct less/greater range boundaries', () => {
    assert.equal(filters.parseDuration('1:03:45'), '3825');
    assert.equal(filters.parseDuration('3:45'), '225');
    assert.equal(filters.parseDuration('45'), '45');
    assert.equal(filters.parseDuration(''), '');
    for (const value of ['-1', 'abc', '1::3', '1:2:3:4']) assert.equal(filters.parseDuration(value), null);
    assert.equal(filters.formatDuration('3825'), '1:03:45');
    assert.equal(filters.formatDuration('225'), '3:45');
    const fields = [field('lengthMode', 'lt'), field('lengthMin', '3:00', { duration: true }), field('lengthMax', '4:30', { duration: true })];
    const result = filters.serializeFields(fields, new URLSearchParams(), 'songs');
    assert.equal(result.has('lengthMin'), false);
    assert.equal(result.get('lengthMax'), '270');
    fields[0].value = 'gt';
    const greater = filters.serializeFields(fields, new URLSearchParams(), 'songs');
    assert.equal(greater.get('lengthMin'), '180');
    assert.equal(greater.has('lengthMax'), false);
});

test('source changes clear incompatible catalog filters and scopes while preserving presentation', () => {
    const current = new URLSearchParams('view=graphs&tab=general&source=artists&graphOrder=alpha&includeGroups=true&includeFeatured=true&limit=10&limitEntity=song&artist=7&ageMin=20&genderMode=isnull&q=test');
    const switched = filters.clearParams(current, 'songs', true);
    assert.equal(switched.get('source'), 'songs');
    assert.equal(switched.get('graphOrder'), 'alpha');
    assert.equal(switched.get('view'), 'graphs');
    for (const key of ['includeGroups', 'includeFeatured', 'limit', 'limitEntity', 'artist', 'ageMin', 'genderMode', 'q']) assert.equal(switched.has(key), false);
    const cleared = filters.clearParams(current, 'artists');
    assert.equal(cleared.get('limit'), '10');
    assert.equal(cleared.get('includeGroups'), 'true');
});

test('artist scopes do not accumulate duplicate URL values when edited', () => {
    const result = filters.serializeFields([field('includeMain', 'false'), field('includeGroups', 'false'), field('includeFeatured', 'true')], new URLSearchParams('includeGroups=true&includeFeatured=false'), 'artists');
    assert.deepEqual(result.getAll('includeGroups'), ['false']);
    assert.deepEqual(result.getAll('includeFeatured'), ['true']);
    assert.equal(result.get('includeMain'), 'false');
});

test('ISO date deep links become day/month/year for the date picker', () => {
    assert.equal(filters.displayDate('2026-10-14'), '14/10/2026');
    assert.equal(filters.displayDate('14/10/2026'), '14/10/2026');
    assert.equal(filters.displayDate(''), '');
});

test('active filter groups combine ranges and mode values into removable groups', () => {
    const params = new URLSearchParams('source=songs&graphMetric=plays&ageMin=20&ageMax=30&gender=1&gender=2&genderMode=includes&deathDateMode=isnull&includeGroups=false&includeFeatured=true&limit=10&limitEntity=song');
    const groups = filters.activeFilterGroups(params);
    const byTitle = new Map(groups.map(group => [group.title, group]));
    assert.equal(groups.length, 5);
    assert.deepEqual(Array.from(byTitle.get('Age').names), ['ageMin', 'ageMax']);
    assert.deepEqual(Array.from(byTitle.get('Gender').names), ['gender', 'genderMode']);
    assert.equal(byTitle.has('Death Date'), true);
    assert.equal(byTitle.has('Include Groups'), false);
    assert.deepEqual(Array.from(byTitle.get('Limit').names), ['limit', 'limitEntity']);
});

test('schema extraction preserves every filter and mode from the three real catalog forms', () => {
    for (const catalog of ['artists', 'albums', 'songs']) {
        const fixture = catalogFormFixture(catalog);
        const schema = filters.extractSchema(fixture);
        const actual = new Set(schema.flatMap(group => [...group.fields, ...group.modes].map(control => control.name)));
        const expected = new Set(fixture.items.flatMap(item => item.controls.map(control => control.name)));
        assert.deepEqual(Array.from(actual).sort(), Array.from(expected).sort(), catalog + ' must retain every field');
        const titles = Array.from(schema, group => group.title);
        assert.deepEqual(titles, [...titles].sort((a, b) => a.localeCompare(b)));
        for (const group of schema) {
            for (const field of group.fields.filter(field => /Date(?:From|To)?$/.test(field.name))) {
                assert.equal(field.date, true, catalog + ':' + field.name + ' must use the date picker');
            }
        }
    }
});

test('image theme native radio modes and hidden age modes survive extraction', () => {
    const fixture = catalogFormFixture('artists');
    const theme = fixture.items.find(item => item.controls.some(control => control.name === 'imageTheme'));
    theme.controls.find(control => control.name === 'imageThemeMode' && control.value === 'doesntHave').checked = true;
    const age = fixture.items.find(item => item.controls.some(control => control.name === 'ageMode'));
    age.controls.find(control => control.name === 'ageMode').value = 'range';
    const schema = filters.extractSchema(fixture);
    const themeMode = schema.flatMap(group => group.modes).find(mode => mode.name === 'imageThemeMode');
    assert.equal(themeMode.value, 'doesntHave');
    assert.deepEqual(Array.from(themeMode.options, option => option.value), ['has', 'doesntHave']);
    const ageMode = schema.flatMap(group => group.modes).find(mode => mode.name === 'ageMode');
    assert.equal(ageMode.value, 'range');
    const submitted = filters.serializeFields([field('imageTheme', '4'), field(themeMode.name, themeMode.value), field('ageMin', '20'), field(ageMode.name, ageMode.value)], new URLSearchParams(), 'artists');
    assert.equal(submitted.get('imageThemeMode'), 'doesntHave');
    assert.equal(submitted.get('ageMode'), 'range');
});

test('applying unfiltered imported forms never enables a default match filter', () => {
    for (const catalog of ['artists', 'albums', 'songs']) {
        const schema = filters.extractSchema(catalogFormFixture(catalog));
        const fields = schema.flatMap(group => [
            ...group.fields.map(control => ({ ...control, value: control.duration ? filters.formatDuration(control.value) : control.value })),
            ...group.modes.map(mode => field(mode.name, mode.value))
        ]);
        if (catalog === 'artists') fields.push(field('includeMain', 'true'), field('includeGroups', 'false'), field('includeFeatured', 'false'));
        const result = filters.serializeFields(fields, new URLSearchParams('view=graphs&tab=genre'), catalog);
        for (const name of Array.from(result.keys())) {
            assert.ok(['view', 'tab', 'source', 'includeMain', 'includeGroups', 'includeFeatured'].includes(name), catalog + ' must not silently activate ' + name);
        }
        if (catalog === 'artists') {
            assert.equal(result.get('includeMain'), 'true');
            assert.equal(result.get('includeGroups'), 'false');
            assert.equal(result.get('includeFeatured'), 'false');
        }
    }
});
