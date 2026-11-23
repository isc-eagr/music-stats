// Seasonal / Yearly chart page (charts/period-chart.html). One page shows a finalized chart and edits a draft:
// Songs / Albums switch, sortable finalized tables, period jump menus, and the draft editor (search and add,
// drag or arrow reordering, remove, open spots, progress panel, save, finalize and unlock).
(function () {
    'use strict';

    const S = window.periodChartState || {};
    const page = document.getElementById('periodChart');
    if (!page) return;

    const seasonal = S.periodType === 'seasonal';
    const base = '/charts/' + S.periodType + '/' + encodeURIComponent(S.periodKey);
    const lists = {
        song: document.getElementById('songList'),
        album: document.getElementById('albumList'),
        extra: document.getElementById('extraList')
    };
    const editing = { song: !S.songFinalized, album: !S.albumFinalized, extra: !S.songFinalized };
    const maxOf = { song: S.maxSongs, album: S.maxAlbums, extra: S.maxExtraSongs };
    const kindOf = list => (list === 'album' ? 'album' : 'song');
    let dirty = false;

    const esc = value => String(value == null ? '' : value)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

    // ── Songs / Albums switch (remembered in the URL like the Weekly chart) ──
    function setView(view) {
        const albums = view === 'albums';
        document.getElementById('songsSection').hidden = albums;
        document.getElementById('albumsSection').hidden = !albums;
        page.querySelectorAll('[data-pd-view]').forEach(btn => btn.classList.toggle('active', btn.dataset.pdView === view));
        const url = new URL(window.location.href);
        if (albums) url.searchParams.set('view', 'albums'); else url.searchParams.delete('view');
        history.replaceState(null, '', url);
    }
    page.querySelectorAll('[data-pd-view]').forEach(btn => btn.addEventListener('click', () => setView(btn.dataset.pdView)));
    if (new URLSearchParams(window.location.search).get('view') === 'albums') setView('albums');

    // ── Sorting on finalized tables ──
    page.querySelectorAll('.pd-table:not(.pd-editing) th[data-sort]').forEach(th => th.addEventListener('click', () => {
        const table = th.closest('table');
        const body = table.tBodies[0];
        const index = th.cellIndex;
        const asc = th.classList.contains('ct-sorted') ? !th.classList.contains('wk-asc') : th.dataset.sort === 'text';
        table.querySelectorAll('thead th').forEach(h => h.classList.remove('ct-sorted', 'wk-asc'));
        th.classList.add('ct-sorted');
        th.classList.toggle('wk-asc', asc);
        const value = row => {
            const cell = row.cells[index];
            return cell ? (cell.dataset.sortVal ?? cell.textContent.trim()) : '';
        };
        Array.from(body.querySelectorAll('tr.pd-row'))
            .sort((a, b) => {
                const x = value(a);
                const y = value(b);
                const order = th.dataset.sort === 'number' ? (parseFloat(x) || 0) - (parseFloat(y) || 0) : x.localeCompare(y);
                return asc ? order : -order;
            })
            .forEach(row => body.appendChild(row));
    }));

    // ── Jump menus ──
    const jumpYear = document.getElementById('pdJumpYear');
    const jumpSeason = document.getElementById('pdJumpSeason');
    const jumpPeriod = document.getElementById('pdJumpPeriod');
    const go = key => { if (key && key !== S.periodKey) window.location.href = '/charts/' + S.periodType + '/' + encodeURIComponent(key); };
    if (jumpYear && jumpSeason) {
        const keys = S.seasonKeys || [];
        const years = Array.from(new Set(keys.map(k => k.split('-')[0]).concat([S.periodYear]))).sort().reverse();
        jumpYear.innerHTML = years.map(y => '<option' + (y === S.periodYear ? ' selected' : '') + '>' + esc(y) + '</option>').join('');
        const jump = () => {
            const wanted = jumpYear.value + '-' + jumpSeason.value;
            if (keys.includes(wanted)) return go(wanted);
            const order = Array.from(jumpSeason.options, o => o.value);
            const fallback = order.map(season => jumpYear.value + '-' + season).find(key => keys.includes(key));
            go(fallback || wanted);
        };
        jumpYear.addEventListener('change', jump);
        jumpSeason.addEventListener('change', jump);
    }
    if (jumpPeriod) jumpPeriod.addEventListener('change', () => go(jumpPeriod.value));

    // ── Open the Songs list filtered to this period's plays ──
    page.querySelector('[data-pd-list="songs"]')?.addEventListener('click', () => {
        if (!S.periodStart || !S.periodEnd) return;
        const params = new URLSearchParams({
            listenedDateFrom: S.periodStart.substring(0, 10),
            listenedDateTo: S.periodEnd.substring(0, 10),
            periodLabel: S.displayName,
            view: 'table'
        });
        window.open('/songs?' + params.toString(), '_blank');
    });

    // ── Playlist (top 30 then bubbling under) ──
    document.getElementById('pdPlaylistBtn')?.addEventListener('click', () => {
        const songs = ['song', 'extra'].flatMap(list => rows(list)).map(row => ({
            id: parseInt(row.dataset.id, 10),
            name: row.querySelector('.wk-name')?.textContent.trim() || '',
            artist: row.querySelector('.wk-artist')?.textContent.trim() || '',
            album: row.dataset.album || ''
        }));
        if (!songs.length) {
            toast('error', 'No songs on this chart');
            return;
        }
        if (typeof window.openPlaylistGeneratorModal === 'function') window.openPlaylistGeneratorModal(songs, S.displayName);
    });

    // ── Helpers shared by the editor ──
    function rows(list) {
        return lists[list] ? Array.from(lists[list].querySelectorAll('tr.pd-row')) : [];
    }

    function ids(list) {
        return rows(list).map(row => parseInt(row.dataset.id, 10));
    }

    function toast(type, message) {
        const box = document.getElementById('saveIndicator');
        if (!box) return;
        box.className = 'pd-toast pd-toast-show pd-toast-' + type;
        box.textContent = message;
        clearTimeout(toast.timer);
        if (type !== 'saving') toast.timer = setTimeout(() => { box.className = 'pd-toast'; }, 3000);
    }

    function setSaveState(text, warn) {
        const el = document.getElementById('pdSaveState');
        if (!el) return;
        el.textContent = text;
        el.classList.toggle('pd-save-warn', !!warn);
    }

    function markDirty() {
        dirty = true;
        setSaveState('Unsaved changes', true);
    }

    window.addEventListener('beforeunload', event => {
        if (dirty) {
            event.preventDefault();
            event.returnValue = '';
        }
    });

    // ── Editor: positions, open spots, counters, progress, finalize state ──
    function slotRow(position, text) {
        const columns = lists.song ? lists.song.closest('table').tHead.rows[0].cells.length : 9;
        return '<tr class="pd-slot"><td class="pd-c-grip"></td><td class="wk-c-pos"><span class="wk-pos">' + position + '</span></td>'
            + '<td colspan="' + (columns - 2) + '">' + esc(text) + '</td></tr>';
    }

    function renderSlots(list) {
        const body = lists[list];
        body.querySelectorAll('tr.pd-slot, tr.pd-empty').forEach(row => row.remove());
        const count = rows(list).length;
        const start = parseInt(body.dataset.pdStart || '1', 10);
        const open = maxOf[list] - count;
        if (open <= 0) return;
        const first = start + count;
        const noun = kindOf(list) === 'album' ? 'album' : 'song';
        let html = slotRow(first, 'Open spot. Add ' + (noun === 'album' ? 'an album' : 'a song') + ' from the search above.');
        if (open > 1) html += slotRow(first + 1, 'Open spot');
        if (open > 2) {
            html += '<tr class="pd-slot pd-slot-more"><td class="pd-c-grip"></td><td colspan="20">+ ' + (open - 2) + ' more open spots ('
                + (first + 2) + ' - ' + (start + maxOf[list] - 1) + ')</td></tr>';
        }
        body.insertAdjacentHTML('beforeend', html);
    }

    function update(list) {
        if (!lists[list] || !editing[list]) return;
        const all = rows(list);
        const start = parseInt(lists[list].dataset.pdStart || '1', 10);
        all.forEach((row, index) => {
            const pos = row.querySelector('.pd-pos');
            if (pos) pos.textContent = start + index;
            const up = row.querySelector('[data-pd-move="up"]');
            const down = row.querySelector('[data-pd-move="down"]');
            if (up) up.disabled = index === 0;
            if (down) down.disabled = index === all.length - 1;
        });
        renderSlots(list);

        const count = all.length;
        const over = count > maxOf[list];
        const badge = page.querySelector('[data-pd-count="' + list + '"]');
        if (badge) {
            badge.textContent = list === 'extra' ? count + ' / ' + maxOf[list] : count + '/' + maxOf[list];
            badge.classList.toggle('pd-over', over);
        }
        const progress = page.querySelector('[data-pd-progress="' + kindOf(list) + '"]');
        if (progress && list !== 'extra') {
            const placed = progress.querySelector('[data-pd-placed]');
            placed.textContent = count;
            placed.classList.toggle('pd-over', over);
            progress.querySelectorAll('[data-pd-blocks] i').forEach((block, index) => {
                const gender = all[index] ? all[index].dataset.gender : null;
                block.className = all[index] ? (gender === '2' ? 'male' : gender === '1' ? 'female' : 'other') : '';
            });
        }
        if (progress && list === 'extra') {
            const extra = progress.querySelector('[data-pd-extra-count]');
            if (extra) {
                extra.textContent = count;
                extra.classList.toggle('pd-over', over);
            }
        }
        updateFinalize();
    }

    function updateFinalize() {
        const btn = document.getElementById('finalizeBtn');
        if (!btn) return;
        const songs = ids('song').length;
        const albums = ids('album').length;
        const songsFull = !S.songFinalized && songs === S.maxSongs;
        const albumsFull = !S.albumFinalized && albums === S.maxAlbums;
        const songsPartial = !S.songFinalized && songs > 0 && songs !== S.maxSongs;
        const albumsPartial = !S.albumFinalized && albums > 0 && albums !== S.maxAlbums;
        if (songsPartial || albumsPartial) {
            btn.disabled = true;
        } else if (songsFull && albumsFull) {
            btn.disabled = false;
            btn.textContent = 'Finalize';
        } else if (songsFull || albumsFull) {
            btn.disabled = false;
            btn.textContent = 'Finalize ' + (songsFull ? 'songs' : 'albums');
        } else {
            btn.disabled = true;
        }
    }

    // ── Editor: rows the search adds ──
    function chipsHtml(appearances, periodType) {
        const chips = (appearances || []).filter(a => a.periodType === periodType).map(a =>
            '<a class="pd-chip' + (periodType === 'yearly' ? ' pd-chip-year' : (a.position === 1 ? ' pd-chip-one' : '')) + '" href="'
            + esc(a.link) + '">' + esc(a.label + ' · #' + a.position) + '</a>');
        return chips.join('');
    }

    function fillStats(row, stats) {
        row.querySelector('.wk-plays').textContent = (stats.plays || 0).toLocaleString('en-US');
        const peak = row.querySelector('td.wk-c-peak');
        peak.innerHTML = stats.weeklyPeak ? '<span class="co-pk ' + esc(stats.weeklyBand) + '">#' + stats.weeklyPeak + '</span>' : '<span class="ct-dim">-</span>';
        const weeks = row.querySelector('[data-pd-weeks]');
        if (weeks) weeks.textContent = stats.weeklyWeeks > 0 ? stats.weeklyWeeks : '-';
        const chipCells = row.querySelectorAll('td.pd-c-chips .pd-chips');
        const dash = '<span class="ct-dim">-</span>';
        if (seasonal) {
            chipCells[0].innerHTML = chipsHtml(stats.appearances, 'yearly') || dash;
            if (chipCells[1]) chipCells[1].innerHTML = chipsHtml(stats.appearances, 'seasonal') || dash;
        } else {
            chipCells[0].innerHTML = (chipsHtml(stats.appearances, 'seasonal') + chipsHtml(stats.appearances, 'yearly')) || dash;
        }
        if (stats.genderId != null) {
            row.dataset.gender = stats.genderId;
            row.classList.toggle('ct-male', String(stats.genderId) === '2');
            row.classList.toggle('ct-female', String(stats.genderId) === '1');
        }
    }

    function newRowHtml(list, item) {
        const album = kindOf(list) === 'album';
        const link = (album ? '/albums/' : '/songs/') + item.id;
        const cover = item.hasImage ? '<img src="' + link + '/image?thumbnail=true" alt="" loading="lazy">' : '';
        const artist = item.artistId
            ? '<a class="wk-artist" href="/artists/' + esc(item.artistId) + '">' + esc(item.artist) + '</a>'
            : '<span class="wk-artist">' + esc(item.artist) + '</span>';
        const chipCells = seasonal
            ? '<td class="pd-c-chips"><span class="pd-chips"><span class="ct-dim">-</span></span></td><td class="pd-c-chips wk-desk"><span class="pd-chips"><span class="ct-dim">-</span></span></td>'
            : '<td class="pd-c-chips"><span class="pd-chips"><span class="ct-dim">-</span></span></td>';
        return '<tr class="wk-row pd-row" data-id="' + esc(item.id) + '" data-album="' + esc(item.albumName || '') + '">'
            + '<td class="pd-c-grip"><span class="pd-grip" draggable="true" title="Drag to reorder" aria-hidden="true"><i></i><i></i><i></i></span></td>'
            + '<td class="wk-c-pos rank-col"><span class="wk-pos pd-pos"></span></td>'
            + '<td class="wk-c-cover"><a class="co-cover wk-cover" href="' + link + '" tabindex="-1" aria-hidden="true">' + cover + '</a></td>'
            + '<td class="wk-c-name"><a class="wk-name" href="' + link + '">' + esc(item.name) + '</a>' + artist + '</td>'
            + '<td class="wk-c-num"><span class="wk-plays">-</span></td>'
            + '<td class="wk-c-peak"><span class="ct-dim">-</span></td>'
            + '<td class="wk-c-num" data-pd-weeks>-</td>'
            + chipCells
            + '<td class="pd-c-edit"><span class="pd-edit">'
            + '<button type="button" class="pd-ib" data-pd-move="up" aria-label="Move up" title="Move up">&#8593;</button>'
            + '<button type="button" class="pd-ib" data-pd-move="down" aria-label="Move down" title="Move down">&#8595;</button>'
            + '<button type="button" class="pd-ib pd-ib-x" data-pd-remove aria-label="Remove from chart" title="Remove">&times;</button>'
            + '</span></td></tr>';
    }

    function add(list, item) {
        const body = lists[list];
        const firstSlot = body.querySelector('tr.pd-slot, tr.pd-empty');
        const html = newRowHtml(list, item);
        if (firstSlot) firstSlot.insertAdjacentHTML('beforebegin', html); else body.insertAdjacentHTML('beforeend', html);
        const row = rows(list).find(r => r.dataset.id === String(item.id));
        if (count(list) > maxOf[list]) toast('error', 'Over the limit: ' + count(list) + '/' + maxOf[list]);
        update(list);
        markDirty();
        fetch(base + '/entry-stats?type=' + kindOf(list) + '&id=' + encodeURIComponent(item.id))
            .then(r => (r.ok ? r.json() : null))
            .then(stats => {
                if (stats && row) {
                    fillStats(row, stats);
                    update(list);
                }
            })
            .catch(() => {});
    }

    function count(list) {
        return rows(list).length;
    }

    // ── Editor: search ──
    page.querySelectorAll('[data-pd-search]').forEach(input => {
        const list = input.dataset.pdSearch;
        const results = page.querySelector('[data-pd-results="' + list + '"]');
        let timer = null;
        let found = [];

        const taken = () => (list === 'album' ? ids('album') : ids('song').concat(ids('extra')));
        const close = () => { results.hidden = true; };
        const render = () => {
            const takenIds = taken();
            const next = parseInt(lists[list].dataset.pdStart || '1', 10) + count(list);
            if (!found.length) {
                results.innerHTML = '<div class="pd-result-none">No matches</div>';
                results.hidden = false;
                return;
            }
            results.innerHTML = found.map((item, index) => {
                const on = takenIds.includes(item.id);
                const meta = esc(item.artist) + (item.albumName && list !== 'album' ? ' · ' + esc(item.albumName) : '');
                return '<div class="pd-result' + (on ? ' pd-result-on' : ' pd-result-pick') + '" data-index="' + index + '">'
                    + '<span class="pd-result-text"><b>' + esc(item.name) + '</b><span>' + meta + '</span></span>'
                    + (on ? '<span class="pd-result-tag">On the chart</span>'
                          : '<button type="button" class="toolbar-btn pd-result-add">Add as #' + next + '</button>')
                    + '</div>';
            }).join('');
            results.hidden = false;
        };
        const pick = index => {
            const item = found[index];
            if (!item || taken().includes(item.id)) return;
            add(list, item);
            input.value = '';
            close();
        };

        input.addEventListener('input', () => {
            clearTimeout(timer);
            const query = input.value.trim();
            if (query.length < 2) {
                close();
                return;
            }
            timer = setTimeout(() => {
                const url = list === 'album'
                    ? '/albums/api/search?query=' + encodeURIComponent(query) + '&limit=0'
                    : '/songs/api/search?song=' + encodeURIComponent(query) + '&artist=' + encodeURIComponent(query) + '&limit=0';
                fetch(url).then(r => r.json()).then(data => {
                    found = (data || []).map(x => ({
                        id: x.id, name: x.name, artist: x.artistName || '', artistId: x.artistId || '',
                        albumName: x.albumName || '', hasImage: !!x.hasImage
                    }));
                    render();
                }).catch(() => {
                    results.innerHTML = '<div class="pd-result-none">Search failed</div>';
                    results.hidden = false;
                });
            }, 300);
        });
        input.addEventListener('keydown', event => {
            if (event.key === 'Escape') close();
            if (event.key === 'Enter') {
                event.preventDefault();
                const takenIds = taken();
                const first = found.findIndex(item => !takenIds.includes(item.id));
                if (!results.hidden && first >= 0) pick(first);
            }
        });
        results.addEventListener('click', event => {
            const row = event.target.closest('.pd-result-pick');
            if (row) pick(parseInt(row.dataset.index, 10));
        });
        document.addEventListener('click', event => {
            if (!event.target.closest('.pd-search')) close();
        });
    });

    // ── Editor: arrows, remove, drag ──
    Object.entries(lists).forEach(([list, body]) => {
        if (!body || !editing[list]) return;
        body.addEventListener('click', event => {
            const row = event.target.closest('tr.pd-row');
            if (!row) return;
            const move = event.target.closest('[data-pd-move]');
            if (move) {
                const sibling = move.dataset.pdMove === 'up' ? row.previousElementSibling : row.nextElementSibling;
                if (sibling && sibling.classList.contains('pd-row')) {
                    if (move.dataset.pdMove === 'up') body.insertBefore(row, sibling); else body.insertBefore(sibling, row);
                    update(list);
                    markDirty();
                }
            }
            if (event.target.closest('[data-pd-remove]')) {
                row.remove();
                update(list);
                markDirty();
            }
        });

        let dragged = null;
        body.addEventListener('dragstart', event => {
            const grip = event.target.closest('.pd-grip');
            if (!grip) return;
            dragged = grip.closest('tr.pd-row');
            dragged.classList.add('pd-dragging');
            event.dataTransfer.effectAllowed = 'move';
            event.dataTransfer.setData('text/plain', dragged.dataset.id);
            event.dataTransfer.setDragImage(dragged, 24, 24);
        });
        body.addEventListener('dragover', event => {
            if (!dragged) return;
            event.preventDefault();
            const target = event.target.closest('tr.pd-row');
            if (!target || target === dragged) return;
            const box = target.getBoundingClientRect();
            body.insertBefore(dragged, event.clientY > box.top + box.height / 2 ? target.nextElementSibling : target);
        });
        const finish = () => {
            if (!dragged) return;
            dragged.classList.remove('pd-dragging');
            dragged = null;
            update(list);
            markDirty();
        };
        body.addEventListener('drop', event => { event.preventDefault(); finish(); });
        body.addEventListener('dragend', finish);

        update(list);
    });

    // ── Saving, finalizing, unlocking (same endpoints as before) ──
    function entries(list) {
        return rows(list).map((row, index) => ({ position: index + 1, itemId: parseInt(row.dataset.id, 10) }));
    }

    async function post(path, payload) {
        const response = await fetch(base + path, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const result = await response.json();
        if (!result.success) throw new Error(result.error || 'Request failed');
        return result;
    }

    async function saveChart(type) {
        const payload = { chartType: type, entries: entries(type) };
        if (type === 'song' && seasonal) payload.extraEntries = rows('extra').map(row => ({ itemId: parseInt(row.dataset.id, 10) }));
        await post('/save', payload);
    }

    async function saveAll() {
        toast('saving', 'Saving...');
        try {
            if (editing.song) await saveChart('song');
            if (editing.album) await saveChart('album');
            dirty = false;
            setSaveState('All changes saved');
            toast('saved', 'Draft saved');
        } catch (err) {
            toast('error', 'Could not save: ' + err.message);
        }
    }

    async function finalize() {
        const songsFull = !S.songFinalized && ids('song').length === S.maxSongs;
        const albumsFull = !S.albumFinalized && ids('album').length === S.maxAlbums;
        if (!songsFull && !albumsFull) return;
        const label = songsFull && albumsFull ? 'both charts' : (songsFull ? 'the Songs chart' : 'the Albums chart');
        if (!confirm('Finalize ' + label + ' for ' + S.displayName + '? You can unlock it again later with Edit chart.')) return;
        toast('saving', 'Finalizing...');
        try {
            if (songsFull && albumsFull) {
                await saveChart('song');
                await saveChart('album');
                await post('/finalize', { songChartId: S.songChartId, albumChartId: S.albumChartId });
            } else {
                const type = songsFull ? 'song' : 'album';
                await saveChart(type);
                await post('/finalize-single', { chartId: type === 'song' ? S.songChartId : S.albumChartId });
            }
            dirty = false;
            toast('saved', 'Finalized');
            setTimeout(() => window.location.reload(), 800);
        } catch (err) {
            toast('error', 'Could not finalize: ' + err.message);
        }
    }

    async function unlock() {
        if (!confirm('Unlock both charts of ' + S.displayName + ' for editing? They go back to draft until you finalize them again.')) return;
        toast('saving', 'Unlocking...');
        try {
            // One after the other: SQLite allows one writer at a time
            await post('/unfinalize', { chartId: S.songChartId });
            await post('/unfinalize', { chartId: S.albumChartId });
            toast('saved', 'Charts unlocked');
            setTimeout(() => window.location.reload(), 600);
        } catch (err) {
            toast('error', 'Could not unlock: ' + err.message);
        }
    }

    document.getElementById('pdSaveBtn')?.addEventListener('click', saveAll);
    document.getElementById('finalizeBtn')?.addEventListener('click', finalize);
    document.getElementById('pdEditBtn')?.addEventListener('click', unlock);
})();
