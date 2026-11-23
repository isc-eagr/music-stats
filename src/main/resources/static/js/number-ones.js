/*
 * Number Ones page: year and search filters, column sorting and year group rows.
 * Rows are rendered by the server; filters and sort live in the URL.
 */
(function () {
    const page = document.getElementById('numberOnes');
    const body = document.getElementById('n1Rows');
    if (!page || !body) return;

    const MAX_TICKS = 12;
    const unit = page.dataset.unit || 'week';
    const rows = Array.from(body.querySelectorAll('tr.n1-row')).map(el => ({
        el,
        gender: el.dataset.gender || '',
        year: el.dataset.year || '',
        start: el.dataset.start || '',
        end: el.dataset.end || '',
        length: Number(el.dataset.length) || 0,
        run: Number(el.dataset.run) || 0,
        cumulative: Number(el.dataset.cumulative) || 0,
        total: Number(el.dataset.total) || 0,
        title: el.dataset.title || '',
        artist: el.dataset.artist || '',
        search: ((el.dataset.title || '') + ' ' + (el.dataset.artist || '')).toLowerCase()
    }));
    rows.forEach(row => drawTicks(row));

    const params = new URLSearchParams(location.search);
    const state = {
        year: params.get('year') || '',
        q: params.get('q') || '',
        sort: params.get('sort') || 'start',
        dir: params.get('dir') === 'asc' ? 'asc' : 'desc'
    };

    const yearSelect = document.getElementById('n1Year');
    const search = document.getElementById('n1Search');
    const years = Array.from(new Set(rows.map(row => row.year).filter(Boolean))).sort().reverse();
    years.forEach(year => yearSelect.add(new Option(year, year)));
    if (!years.includes(state.year)) state.year = '';
    yearSelect.value = state.year;
    search.value = state.q;

    yearSelect.addEventListener('change', () => {
        state.year = yearSelect.value;
        render();
    });
    search.addEventListener('input', () => {
        state.q = search.value.trim();
        render();
    });
    page.querySelectorAll('th [data-sort]').forEach(button => button.addEventListener('click', () => {
        const key = button.dataset.sort;
        if (state.sort === key) {
            state.dir = state.dir === 'desc' ? 'asc' : 'desc';
        } else {
            state.sort = key;
            state.dir = key === 'title' || key === 'artist' ? 'asc' : 'desc';
        }
        render();
    }));

    render();

    function drawTicks(row) {
        const holder = row.el.querySelector('.n1-ticks');
        if (!holder) return;
        const count = Math.min(row.length, MAX_TICKS);
        for (let i = 0; i < count; i++) {
            const tick = document.createElement('span');
            tick.className = 'n1-tick';
            holder.appendChild(tick);
        }
        if (row.length > MAX_TICKS) holder.appendChild(document.createTextNode('+' + (row.length - MAX_TICKS)));
    }

    function matches(row) {
        if (state.year && row.year !== state.year) return false;
        return !state.q || row.search.includes(state.q.toLowerCase());
    }

    function compare(a, b) {
        const key = state.sort;
        const dir = state.dir === 'asc' ? 1 : -1;
        let result;
        if (key === 'title' || key === 'artist') {
            result = a[key].localeCompare(b[key], undefined, { sensitivity: 'base' });
        } else if (key === 'start' || key === 'end') {
            result = a[key] < b[key] ? -1 : a[key] > b[key] ? 1 : 0;
        } else {
            result = a[key] - b[key];
        }
        if (result !== 0) return result * dir;
        return a.start < b.start ? 1 : a.start > b.start ? -1 : 0;
    }

    function render() {
        const visible = rows.filter(matches).sort(compare);
        const grouped = state.sort === 'start' || state.sort === 'end';
        const fragment = document.createDocumentFragment();
        let currentYear = null;
        visible.forEach(row => {
            const year = grouped ? (state.sort === 'end' ? row.end : row.start).slice(0, 4) : null;
            if (grouped && year !== currentYear) {
                currentYear = year;
                fragment.appendChild(yearRow(year, visible.filter(other =>
                    (state.sort === 'end' ? other.end : other.start).slice(0, 4) === year)));
            }
            fragment.appendChild(row.el);
        });
        body.replaceChildren(fragment);

        document.getElementById('n1NoMatch').hidden = visible.length > 0;
        document.getElementById('n1Count').textContent = 'Showing ' + visible.length.toLocaleString() + ' of '
            + rows.length.toLocaleString() + ' runs';
        updateSortButtons();
        writeUrl();
    }

    function yearRow(year, yearRows) {
        const male = sum(yearRows, 'male');
        const female = sum(yearRows, 'female');
        const all = yearRows.reduce((total, row) => total + row.length, 0);
        const tr = document.createElement('tr');
        tr.className = 'n1-year';
        const td = document.createElement('td');
        td.colSpan = 9;
        td.innerHTML = '<div class="n1-year-inner">'
            + '<span class="n1-year-label"></span>'
            + '<span class="n1-year-stats"><b class="n1-male-text"></b> male ' + plural(unit, male)
            + ' &middot; <b class="n1-female-text"></b> female ' + plural(unit, female)
            + ' &middot; ' + yearRows.length + ' ' + plural('run', yearRows.length) + '</span>'
            + '<span class="n1-year-bar"><span class="n1-share-male"></span><span class="n1-share-female"></span></span>'
            + '</div>';
        td.querySelector('.n1-year-label').textContent = year;
        const numbers = td.querySelectorAll('.n1-year-stats b');
        numbers[0].textContent = male;
        numbers[1].textContent = female;
        td.querySelector('.n1-year-bar .n1-share-male').style.width = percent(male, all) + '%';
        td.querySelector('.n1-year-bar .n1-share-female').style.width = percent(female, all) + '%';
        tr.appendChild(td);
        return tr;
    }

    function updateSortButtons() {
        page.querySelectorAll('th [data-sort]').forEach(button => {
            const active = button.dataset.sort === state.sort;
            button.classList.toggle('sorted', active);
            button.classList.toggle('asc', active && state.dir === 'asc');
            button.closest('th').setAttribute('aria-sort', active ? (state.dir === 'asc' ? 'ascending' : 'descending') : 'none');
        });
    }

    function writeUrl() {
        const next = new URLSearchParams(location.search);
        const set = (name, value) => value ? next.set(name, value) : next.delete(name);
        set('year', state.year);
        set('q', state.q);
        set('sort', state.sort === 'start' ? '' : state.sort);
        set('dir', state.sort === 'start' && state.dir === 'desc' ? '' : state.dir);
        const query = next.toString();
        history.replaceState(null, '', location.pathname + (query ? '?' + query : ''));
    }

    function sum(list, gender) {
        return list.reduce((total, row) => total + (row.gender === gender ? row.length : 0), 0);
    }

    function percent(part, whole) {
        return whole ? 100 * part / whole : 0;
    }

    function plural(word, count) {
        return count === 1 ? word : word + 's';
    }
})();
