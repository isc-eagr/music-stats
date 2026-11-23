/*
 * Chart Overview admin tools: link chart entries to library songs (Hot 100, TRL, Vato's Cuntdown),
 * link / unlink TRL chart entries, merge Cuntdown spellings, auto-link, normalize, Hot 100 import.
 * Every change refreshes the rows in place through window.chartOverview.refresh().
 */
(function () {
    'use strict';

    const root = document.getElementById('chartOverview');
    if (!root) return;
    const chart = root.dataset.chart;
    const IMPORT_TOAST_KEY = 'musicStats.billboardImportToast';
    let modalMode = null;
    let modalRow = null;
    let searchTimer = null;
    let searchSeq = 0;

    function api() {
        return window.chartOverview;
    }

    function toast(kind, message) {
        const fn = kind === 'error' ? window.showErrorToast : kind === 'warning' ? window.showWarningToast : window.showSuccessToast;
        if (typeof fn === 'function') fn(message);
    }

    function post(url, params) {
        const options = { method: 'POST' };
        if (params) {
            options.headers = { 'Content-Type': 'application/x-www-form-urlencoded' };
            options.body = new URLSearchParams(params).toString();
        }
        return fetch(url, options).then(response => {
            if (!response.ok) throw new Error('HTTP ' + response.status);
            return response.json();
        });
    }

    function refreshAfter(promise, success) {
        return promise.then(data => {
            if (data && data.ok === false) throw new Error(data.error || 'Request failed');
            toast('success', typeof success === 'function' ? success(data) : success);
            return api().refresh();
        }).catch(error => toast('error', 'Could not finish: ' + error.message));
    }

    // ------------------------------------------------------------------ modal

    const modal = document.getElementById('coMatchModal');
    const input = document.getElementById('coMatchInput');
    const results = document.getElementById('coMatchResults');

    function openModal(mode, row) {
        modalMode = mode;
        modalRow = row;
        document.getElementById('coMatchTitle').textContent = mode === 'chart' ? 'Link to TRL chart entries' : 'Link to song in Library';
        document.getElementById('coMatchSubtitle').textContent = (row.artistName || '') + ' - ' + (row.title || '');
        document.getElementById('coMatchUnlink').hidden = true;
        input.value = row.title || '';
        modal.hidden = false;
        input.focus();
        input.select();
        search(input.value);
    }

    function closeModal() {
        modal.hidden = true;
        modalMode = null;
        modalRow = null;
        results.innerHTML = '';
    }

    function resultItem(attrs, title, sub) {
        const esc = api().esc;
        return '<button type="button" class="co-modal-result"' + attrs + '><span class="co-name">' + esc(title) + '</span><span class="co-sub">' + esc(sub) + '</span></button>';
    }

    function search(query) {
        const q = (query || '').trim();
        // Only the latest search may fill the list: a slower earlier one must not overwrite newer results
        const seq = ++searchSeq;
        if (!q) {
            results.innerHTML = '<div class="co-hint">Start typing to search</div>';
            return;
        }
        const enc = encodeURIComponent(q);
        const esc = api().esc;
        const request = modalMode === 'chart'
            ? fetch('/misc/trl/chart-entries/search?q=' + enc)
            : fetch('/songs/api/search?song=' + enc + '&artist=' + enc + '&limit=0');
        request.then(response => response.json()).then(data => {
            if (seq !== searchSeq) return;
            if (!data.length) {
                results.innerHTML = '<div class="co-hint">No matches</div>';
                return;
            }
            results.innerHTML = modalMode === 'chart'
                ? data.map(item => resultItem(' data-artist="' + esc(item.artistName) + '" data-song="' + esc(item.songTitle) + '"',
                    item.songTitle, item.artistName + ' · ' + item.appearances + ' appearances')).join('')
                : data.map(item => resultItem(' data-song-id="' + esc(item.id) + '"', item.name, item.artistName)).join('');
        }).catch(() => {
            if (seq !== searchSeq) return;
            results.innerHTML = '<div class="co-hint">Error searching. Try again.</div>';
        });
    }

    function linkLibrary(songId) {
        const row = modalRow;
        closeModal();
        let request;
        if (chart === 'trl') request = post('/misc/trl/' + encodeURIComponent(row.runKey) + '/match?songId=' + encodeURIComponent(songId));
        else if (chart === 'cuntdown') request = post('/misc/vatos-cuntdown/match-group', { rawArtist: row.artistName || '', rawSong: row.title || '', songId });
        else request = post('/misc/billboard-hot-100/match', { rawArtist: row.artistName || '', rawSong: row.title || '', songId });
        refreshAfter(request, 'Linked ' + (row.title || 'the song') + ' to your library');
    }

    function linkChartEntries(artist, song) {
        const row = modalRow;
        closeModal();
        refreshAfter(post('/misc/trl/' + encodeURIComponent(row.runKey) + '/link-chart', { chartArtist: artist, chartSong: song }),
            'Linked ' + (row.title || 'the song') + ' to TRL chart entries');
    }

    if (modal) {
        input.addEventListener('input', () => {
            clearTimeout(searchTimer);
            searchTimer = setTimeout(() => search(input.value), 220);
        });
        results.addEventListener('click', event => {
            const item = event.target.closest('.co-modal-result');
            if (!item) return;
            if (modalMode === 'chart') linkChartEntries(item.dataset.artist, item.dataset.song);
            else linkLibrary(item.dataset.songId);
        });
        modal.querySelector('[data-co-close]').addEventListener('click', closeModal);
        // Close on the backdrop only when the press also started there (text selection drags stay open).
        let downOnBackdrop = false;
        modal.addEventListener('mousedown', event => { downOnBackdrop = event.target === modal; });
        modal.addEventListener('click', event => {
            if (downOnBackdrop && event.target === modal) closeModal();
            downOnBackdrop = false;
        });
        document.addEventListener('keydown', event => {
            if (event.key === 'Escape' && !modal.hidden) closeModal();
        });
    }

    // ------------------------------------------------------------------ merge (Vato's Cuntdown)

    const merge = { on: false, source: null, targets: [] };
    const bar = document.getElementById('coMergeBar');

    function setMergeMode(on) {
        merge.on = on;
        merge.source = null;
        merge.targets = [];
        root.classList.toggle('co-merging', on);
        if (bar) bar.hidden = !on;
        paintMerge();
    }

    function rowLabel(row) {
        return row ? (row.artistName || '') + ' - ' + (row.title || '') : '';
    }

    function paintMerge() {
        document.querySelectorAll('#coBody tr[data-index]').forEach(tr => {
            const row = api().state.rows[+tr.dataset.index];
            tr.classList.toggle('co-merge-source', merge.on && row === merge.source);
            tr.classList.toggle('co-merge-target', merge.on && merge.targets.includes(row));
        });
        if (!bar) return;
        document.getElementById('coMergeSource').textContent = merge.source ? rowLabel(merge.source) : 'pick a row';
        document.getElementById('coMergeTargets').textContent = merge.targets.length ? merge.targets.map(rowLabel).join(', ') : 'none';
        document.getElementById('coMergeGo').disabled = !merge.source || !merge.targets.length;
    }

    function runMerge() {
        const source = merge.source;
        const targets = merge.targets.slice();
        if (!source || !targets.length) return;
        const message = 'Source: ' + rowLabel(source) + '\n\nTargets:\n' + targets.map(rowLabel).join('\n')
            + '\n\nAll target Vato\'s Cuntdown rows will be renamed to the Source values. Continue?';
        if (!window.confirm(message)) return;
        let chain = Promise.resolve();
        targets.forEach(target => {
            chain = chain.then(() => post('/misc/vatos-cuntdown/merge', {
                sourceArtist: source.artistName || '', sourceSong: source.title || '',
                targetArtist: target.artistName || '', targetSong: target.title || ''
            })).then(data => {
                if (!data.ok) throw new Error(data.error || 'Merge failed');
            });
        });
        setMergeMode(false);
        refreshAfter(chain.then(() => ({ ok: true })), 'Merged ' + targets.length + ' entr' + (targets.length === 1 ? 'y' : 'ies'));
    }

    if (bar) {
        document.getElementById('coMergeGo').addEventListener('click', runMerge);
        document.getElementById('coMergeCancel').addEventListener('click', () => setMergeMode(false));
    }

    // ------------------------------------------------------------------ page actions

    function actionButton(button, busyText, request, success) {
        const label = button.textContent;
        button.disabled = true;
        button.textContent = busyText;
        refreshAfter(request(), success).finally(() => {
            button.disabled = false;
            button.textContent = label;
        });
    }

    function runImport(button) {
        button.disabled = true;
        button.textContent = 'Refreshing...';
        post('/misc/billboard-hot-100/import').then(data => {
            const message = data.ok
                ? 'Imported ' + (data.chartsImported || 0) + ' charts (' + (data.rowsImported || 0) + ' rows), kept ' + (data.preservedLinks || 0) + ' links'
                : 'Import failed: ' + (data.error || 'unknown error');
            sessionStorage.setItem(IMPORT_TOAST_KEY, JSON.stringify({ kind: data.ok ? 'success' : 'error', message }));
        }).catch(error => {
            sessionStorage.setItem(IMPORT_TOAST_KEY, JSON.stringify({ kind: 'error', message: 'Import failed: ' + error.message }));
        }).finally(() => window.location.reload());
    }

    function showQueuedImportToast() {
        let queued = null;
        try {
            queued = JSON.parse(sessionStorage.getItem(IMPORT_TOAST_KEY) || 'null');
            sessionStorage.removeItem(IMPORT_TOAST_KEY);
        } catch (e) { /* storage blocked */ }
        if (queued) toast(queued.kind, queued.message);
    }

    document.addEventListener('click', event => {
        const action = event.target.closest('[data-co-action]');
        if (action) {
            switch (action.dataset.coAction) {
                case 'import': runImport(action); break;
                case 'auto-link':
                    actionButton(action, 'Linking...', () => post(chart === 'trl' ? '/misc/trl/auto-link' : '/misc/vatos-cuntdown/auto-link'),
                        data => 'Auto-linked ' + (data.linked || 0) + ' entries');
                    break;
                case 'normalize-linked':
                    actionButton(action, 'Normalizing...', () => post('/misc/trl/normalize-linked'), data => 'Normalized ' + (data.updated || 0) + ' linked entries');
                    break;
                case 'normalize-case':
                    actionButton(action, 'Normalizing...', () => post('/misc/vatos-cuntdown/normalize-case'), data => 'Normalized ' + (data.normalized || 0) + ' case differences');
                    break;
                case 'merge-mode': setMergeMode(!merge.on); break;
                default: break;
            }
            return;
        }
        if (!root.contains(event.target)) return;
        const match = event.target.closest('[data-co-match]');
        if (match) { openModal('library', api().rowFor(match)); return; }
        const chartLink = event.target.closest('[data-co-chart-link]');
        if (chartLink) { openModal('chart', api().rowFor(chartLink)); return; }
        const unlink = event.target.closest('[data-co-unlink]');
        if (unlink) {
            const row = api().rowFor(unlink);
            if (window.confirm('Unlink ' + rowLabel(row) + ' from its TRL chart entries?')) {
                refreshAfter(post('/misc/trl/' + encodeURIComponent(row.runKey) + '/unlink-chart'), 'Unlinked ' + (row.title || 'the song'));
            }
            return;
        }
        if (merge.on) {
            const tr = event.target.closest('#coBody tr[data-index]');
            if (!tr || event.target.closest('a, button')) return;
            const row = api().state.rows[+tr.dataset.index];
            if (!merge.source) merge.source = row;
            else if (row === merge.source) { merge.source = null; merge.targets = []; }
            else if (merge.targets.includes(row)) merge.targets = merge.targets.filter(item => item !== row);
            else merge.targets.push(row);
            paintMerge();
        }
    });

    if (chart === 'hot100') showQueuedImportToast();
})();
