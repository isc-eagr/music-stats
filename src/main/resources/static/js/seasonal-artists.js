// Seasonal / Yearly overview > Artists: click-to-sort columns, a name filter and the featured-songs toggle.
(function () {
    'use strict';

    const table = document.getElementById('swArtistBoard');
    if (!table) return;
    const body = table.tBodies[0];
    const rows = Array.from(body.rows);
    const headers = Array.from(table.querySelectorAll('th[data-sort]'));
    const search = document.getElementById('swArtistSearch');
    const count = document.getElementById('swArtistCount');
    let sortKey = 'sp';
    let descending = true;

    function value(row, key) {
        return key === 'name' ? row.dataset.name : Number(row.dataset[key]);
    }

    function compare(a, b) {
        const x = value(a, sortKey);
        const y = value(b, sortKey);
        const order = sortKey === 'name' ? x.localeCompare(y) : x - y;
        if (order !== 0) return descending ? -order : order;
        // Ties keep the board's default order: seasonal points, then yearly points, then name.
        return (Number(b.dataset.sp) - Number(a.dataset.sp))
            || (Number(b.dataset.yp) - Number(a.dataset.yp))
            || a.dataset.name.localeCompare(b.dataset.name);
    }

    function render() {
        const query = (search ? search.value : '').trim().toLowerCase();
        rows.sort(compare);
        let shown = 0;
        rows.forEach(row => {
            const match = !query || row.dataset.name.includes(query);
            row.hidden = !match;
            if (match) row.cells[0].textContent = ++shown;
            body.appendChild(row);
        });
        if (count) count.textContent = shown + (shown === 1 ? ' artist' : ' artists');
        headers.forEach(th => {
            th.classList.toggle('sw-sorted-desc', th.dataset.sort === sortKey && descending);
            th.classList.toggle('sw-sorted-asc', th.dataset.sort === sortKey && !descending);
        });
    }

    headers.forEach(th => th.addEventListener('click', () => {
        const key = th.dataset.sort;
        if (key === sortKey) {
            descending = !descending;
        } else {
            sortKey = key;
            // Names and first season read best A-Z / oldest first; every count starts with the biggest.
            descending = !(th.dataset.type === 'text' || th.dataset.first === 'asc');
        }
        render();
    }));

    if (search) search.addEventListener('input', render);

    // Featured credits change every total, so the server rebuilds the board.
    document.getElementById('swIncludeFeatured')?.addEventListener('change', event => {
        const url = new URL(window.location.href);
        if (event.target.checked) url.searchParams.set('includeFeatured', 'true');
        else url.searchParams.delete('includeFeatured');
        window.location.assign(url.toString());
    });
})();
