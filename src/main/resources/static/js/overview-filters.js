(function () {
    'use strict';

    const params = new URLSearchParams(window.location.search);
    const filterNames = () => Array.from(document.querySelectorAll('#overviewFilterForm [data-filter-name]'), item => item.dataset.filterName);
    const filterKeys = () => new Set(['q', 'topSong', 'topAlbum', 'includeFeaturedSongs', ...filterNames().flatMap(name => [name, name + 'Mode', name + 'To'])]);
    const overviewTab = params.get('overviewTab') || 'song';
    let facetOptionsPromise;

    function getFacetOptions() {
        if (!facetOptionsPromise) {
            facetOptionsPromise = fetch('/api/overview-filters/options')
                .then(response => response.ok ? response.json() : {})
                .catch(() => ({}));
        }
        return facetOptionsPromise;
    }

    // Every page, data request and export uses the same applied URL state.
    function appendTo(url) {
        filterKeys().forEach(key => {
            url.searchParams.delete(key);
            params.getAll(key).forEach(value => url.searchParams.append(key, value));
        });
        return url;
    }

    function navigate(next) {
        next.delete('page');
        next.delete('filter');
        window.location.assign(window.location.pathname + (next.size ? '?' + next.toString() : ''));
    }

    function reset() {
        const next = new URLSearchParams(params);
        filterKeys().forEach(key => next.delete(key));
        navigate(next);
    }

    window.overviewFilters = { appendTo, reset };

    document.addEventListener('DOMContentLoaded', () => {
        const root = document.getElementById('overviewFilters');
        if (!root) return;
        const form = document.getElementById('overviewFilterForm');
        const panel = document.getElementById('overviewFilterPanel');
        const backdrop = document.getElementById('overviewFilterBackdrop');
        const toggle = document.getElementById('overviewFilterToggle');
        const search = document.getElementById('overviewSearchInput');
        const active = document.getElementById('overviewActiveFilters');
        const fields = Array.from(form.querySelectorAll('[data-filter-name]'));
        let previousOverflow = '';

        function openPanel(open) {
            panel.hidden = !open;
            backdrop.hidden = !open;
            toggle.setAttribute('aria-expanded', String(open));
            if (open) {
                previousOverflow = document.body.style.overflow;
                document.body.style.overflow = 'hidden';
                document.getElementById('overviewFilterClose').focus();
            } else {
                document.body.style.overflow = previousOverflow;
                toggle.focus();
            }
        }

        toggle.addEventListener('click', () => openPanel(true));
        document.getElementById('overviewFilterClose').addEventListener('click', () => openPanel(false));
        backdrop.addEventListener('click', () => openPanel(false));
        panel.addEventListener('keydown', event => {
            if (event.key === 'Escape') openPanel(false);
            if (event.key !== 'Tab') return;
            const focusable = Array.from(panel.querySelectorAll('button, input, select, a[href]')).filter(element => !element.disabled && element.getClientRects().length);
            const first = focusable[0];
            const last = focusable[focusable.length - 1];
            if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
            if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
        });
        root.querySelectorAll('[data-overview-reset]').forEach(button => button.addEventListener('click', reset));

        function chip(label, value, keys) {
            const element = document.createElement('span');
            element.className = 'filter-chip';
            const text = document.createElement('span');
            text.textContent = label + ': ' + value;
            const remove = document.createElement('button');
            remove.type = 'button';
            remove.className = 'filter-chip-remove';
            remove.textContent = '\u00d7';
            remove.setAttribute('aria-label', 'Remove ' + label + ' filter');
            remove.addEventListener('click', () => {
                const next = new URLSearchParams(params);
                keys.forEach(key => next.delete(key));
                navigate(next);
            });
            element.append(text, remove);
            active.append(element);
            return { element, text };
        }

        search.value = params.get('q') || '';
        if (search.value) chip('Search', search.value, ['q']);
        document.getElementById('overviewSearchForm').addEventListener('submit', event => {
            event.preventDefault();
            const next = new URLSearchParams(params);
            if (search.value.trim()) next.set('q', search.value.trim());
            else next.delete('q');
            navigate(next);
        });

        const labels = { includes: 'includes', excludes: 'excludes', exact: '=', gte: '>=', lte: '<=', between: 'between', isnull: 'is empty', isnotnull: 'is not empty' };
        fields.forEach(item => {
            const { filterName: name, filterLabel: label, filterType: type } = item.dataset;
            const content = item.querySelector('.filter-item-content');
            const header = item.querySelector('.filter-item-header');
            const numeric = type === 'number' || type === 'date';
            const modes = numeric ? ['gte', 'exact', 'lte', 'between', 'isnull', 'isnotnull'] : ['includes', 'excludes', 'isnull', 'isnotnull'];
            let mode = params.get(name + 'Mode') || (numeric ? 'exact' : 'includes');
            if (!modes.includes(mode)) mode = modes[0];
            const modeInput = document.createElement('input');
            modeInput.type = 'hidden';
            modeInput.name = name + 'Mode';
            const modifiers = document.createElement('div');
            modifiers.className = 'filter-modifiers';
            const values = document.createElement('div');
            values.className = 'overview-filter-values';
            const buttons = modes.map(value => {
                const button = document.createElement('button');
                button.type = 'button';
                button.className = 'filter-modifier';
                button.textContent = labels[value];
                button.addEventListener('click', () => setMode(value));
                modifiers.append(button);
                return button;
            });
            content.append(modifiers, modeInput, values);

            function inputValue(value, upper) {
                const input = document.createElement('input');
                input.type = type === 'number' ? 'number' : 'text';
                input.name = name + (upper ? 'To' : '');
                input.value = value || '';
                input.setAttribute('aria-label', label + (upper ? ' maximum' : ' value'));
                if (type === 'number') { input.min = '0'; input.step = '1'; }
                input.placeholder = type === 'date' ? 'dd/MM/yyyy' : upper ? 'Maximum' : label;
                if (type === 'date') input.pattern = '\\d{2}/\\d{2}/\\d{4}';
                if (type === 'date' && window.flatpickr) window.flatpickr(input, { dateFormat: 'd/m/Y', allowInput: true });
                return input;
            }

            if (type === 'artist') {
                const artistContainer = document.createElement('div');
                artistContainer.id = 'overviewArtistChipSelect';
                values.append(artistContainer);
                const artistIds = params.getAll(name);
                const artistSelect = window.initChipSelect({
                    containerId: artistContainer.id,
                    inputName: name,
                    placeholder: 'Type to search artists...',
                    searchUrl: '/artists/api/search',
                    searchParam: 'q',
                    valueField: 'id',
                    labelField: 'name',
                    genderField: 'genderId',
                    minChars: 2,
                    initialValues: artistIds.map(id => ({ value: id, label: 'Artist #' + id }))
                });
                if (artistIds.length) {
                    const lookup = new URL('/artists/api/filter-values', window.location.origin);
                    artistIds.forEach(id => lookup.searchParams.append('id', id));
                    fetch(lookup)
                        .then(response => response.ok ? response.json() : [])
                        .then(items => items.forEach(artist => artistSelect.updateChip(artist.id, artist.name, artist.genderId)))
                        .catch(() => {});
                }
                if (overviewTab === 'song') {
                    const featuredToggle = document.createElement('label');
                    featuredToggle.className = 'overview-featured-toggle';
                    const featuredInput = document.createElement('input');
                    featuredInput.type = 'checkbox';
                    featuredInput.name = 'includeFeaturedSongs';
                    featuredInput.value = 'true';
                    featuredInput.checked = params.get('includeFeaturedSongs') === 'true';
                    featuredToggle.append(featuredInput, document.createTextNode('Include songs where the artist is featured'));
                    values.append(featuredToggle);
                }
            } else if (type === 'facet') {
                const facetContainer = document.createElement('div');
                facetContainer.id = 'overviewFacetChipSelect-' + name;
                values.append(facetContainer);
                const selectedValues = params.getAll(name);
                const facetSelect = window.initChipSelect({
                    containerId: facetContainer.id,
                    inputName: name,
                    placeholder: 'Type to search ' + label.toLowerCase() + '...',
                    valueField: 'value',
                    labelField: 'label',
                    genderField: name === 'gender' ? 'genderId' : null,
                    minChars: 0,
                    showAllOnFocus: true,
                    staticOptions: [],
                    initialValues: selectedValues.map(value => ({ value, label: value }))
                });
                getFacetOptions().then(allOptions => {
                    const options = allOptions[name] || [];
                    facetSelect.staticOptions = options;
                    const labelsByValue = new Map(options.map(option => [String(option.value), option.label]));
                    const optionsByValue = new Map(options.map(option => [String(option.value), option]));
                    selectedValues.forEach(value => {
                        const option = optionsByValue.get(String(value));
                        facetSelect.updateChip(value, option?.label || value, option?.genderId);
                    });
                });
            } else if (type === 'gender' || type === 'boolean') {
                values.classList.add('overview-filter-options');
                const options = type === 'gender' ? [['gender-female', 'Female'], ['gender-male', 'Male'], ['gender-other', 'Other']] : [['true', 'Yes'], ['false', 'No']];
                options.forEach(([value, title]) => {
                    const option = document.createElement('label');
                    if (type === 'gender') option.className = value;
                    const input = document.createElement('input');
                    input.type = 'checkbox'; input.name = name; input.value = value;
                    input.checked = params.getAll(name).includes(value);
                    option.append(input, document.createTextNode(title));
                    values.append(option);
                });
            } else if (numeric) {
                values.append(inputValue(params.get(name), false), inputValue(params.get(name + 'To'), true));
            } else {
                const add = document.createElement('button');
                add.type = 'button'; add.className = 'filter-modifier'; add.textContent = 'Add value';
                function addValue(value) {
                    const row = document.createElement('div'); row.className = 'overview-filter-value-row';
                    const remove = document.createElement('button');
                    remove.type = 'button'; remove.className = 'filter-modifier'; remove.textContent = '\u00d7';
                    remove.setAttribute('aria-label', 'Remove ' + label + ' value');
                    remove.addEventListener('click', () => row.remove());
                    row.append(inputValue(value, false), remove);
                    values.insertBefore(row, add);
                }
                values.append(add);
                (params.has(name) ? params.getAll(name) : ['']).forEach(addValue);
                add.addEventListener('click', () => addValue(''));
            }

            function setMode(value) {
                modeInput.value = value;
                buttons.forEach((button, index) => {
                    button.classList.toggle('active', modes[index] === value);
                    button.setAttribute('aria-pressed', String(modes[index] === value));
                });
                const empty = value === 'isnull' || value === 'isnotnull';
                values.hidden = empty;
                values.querySelectorAll('input').forEach(input => {
                    const upper = input.name === name + 'To';
                    input.hidden = upper && value !== 'between';
                    input.disabled = empty || (upper && value !== 'between');
                    input.required = value === 'between';
                });
            }
            setMode(mode);
            const applied = params.has(name) || ['isnull', 'isnotnull'].includes(params.get(name + 'Mode'));
            function expand(expanded) {
                header.classList.toggle('active', expanded);
                header.setAttribute('aria-expanded', String(expanded));
                content.classList.toggle('expanded', expanded);
            }
            header.addEventListener('click', () => expand(header.getAttribute('aria-expanded') !== 'true'));
            expand(applied);
            if (applied) {
                const appliedValue = type === 'artist'
                    ? params.getAll(name).length + (params.getAll(name).length === 1 ? ' artist' : ' artists')
                    : params.getAll(name).join(', ');
                const featuredSuffix = type === 'artist' && overviewTab === 'song' && params.get('includeFeaturedSongs') === 'true' ? ' including featured' : '';
                const activeChip = chip(label, [labels[mode], appliedValue + featuredSuffix, params.has(name + 'To') ? 'to ' + params.get(name + 'To') : ''].filter(Boolean).join(' '), [name, name + 'Mode', name + 'To', ...(type === 'artist' ? ['includeFeaturedSongs'] : [])]);
                if (type === 'facet' && params.has(name)) {
                    getFacetOptions().then(allOptions => {
                        const labelsByValue = new Map((allOptions[name] || []).map(option => [String(option.value), option.label]));
                        const selectedLabels = params.getAll(name).map(value => labelsByValue.get(String(value)) || value);
                        activeChip.text.textContent = label + ': ' + [labels[mode], selectedLabels.join(', ')].filter(Boolean).join(' ');
                    });
                }
            }
        });

        const thresholdControls = [['artistTopAlbumFilter', 'topAlbum', 'Top X Albums'], ['artistTopSongFilter', 'topSong', 'Top X Songs']];
        thresholdControls.forEach(([id, name, label]) => {
            const select = document.getElementById(id);
            if (!select) return;
            const item = document.createElement('div');
            item.className = 'filter-item';
            item.dataset.filterLabel = label;
            const header = document.createElement('button');
            header.type = 'button'; header.className = 'filter-item-header';
            const title = document.createElement('span'); title.className = 'filter-item-title'; title.textContent = label;
            header.append(title);
            const content = document.createElement('div'); content.className = 'filter-item-content';
            content.append(select.closest('label'));
            const expanded = params.has(name);
            header.classList.toggle('active', expanded); content.classList.toggle('expanded', expanded);
            header.setAttribute('aria-expanded', String(expanded));
            header.addEventListener('click', () => {
                const open = header.getAttribute('aria-expanded') !== 'true';
                header.setAttribute('aria-expanded', String(open));
                header.classList.toggle('active', open); content.classList.toggle('expanded', open);
            });
            item.append(header, content);
            form.querySelector('.filter-content').append(item);
            if (expanded) chip(label, params.get(name), [name]);
        });
        document.querySelector('.overview-artist-thresholds')?.remove();
        const fieldContainer = form.querySelector('.filter-content');
        Array.from(fieldContainer.children).sort((a, b) => a.dataset.filterLabel.localeCompare(b.dataset.filterLabel)).forEach(item => fieldContainer.append(item));
        toggle.hidden = !fieldContainer.children.length;
        if (active.children.length) toggle.textContent = 'Filters (' + active.children.length + ')';
        form.addEventListener('submit', event => {
            event.preventDefault();
            const next = new URLSearchParams(params);
            const data = new FormData(form);
            if (data.get('includeFeaturedSongs') === 'true') next.set('includeFeaturedSongs', 'true');
            else next.delete('includeFeaturedSongs');
            thresholdControls.forEach(([id, name]) => {
                const select = document.getElementById(id);
                if (!select) return;
                if (select.value) next.set(name, select.value);
                else next.delete(name);
            });
            fields.forEach(item => {
                const name = item.dataset.filterName;
                [name, name + 'Mode', name + 'To'].forEach(key => next.delete(key));
                const values = data.getAll(name).map(value => String(value).trim()).filter(Boolean);
                const mode = data.get(name + 'Mode');
                if (values.length || mode === 'isnull' || mode === 'isnotnull') {
                    values.forEach(value => next.append(name, value));
                    next.set(name + 'Mode', mode);
                    if (mode === 'between' && data.get(name + 'To')) next.set(name + 'To', data.get(name + 'To'));
                }
            });
            navigate(next);
        });

        // Only overview navigation inherits filters; catalog/detail links keep their own queries.
        document.querySelectorAll('.overview-tab-link, a.sort-link, a.pc-sort-link, a.trl-sort-link, a.bb-sort-link, .overview-toolbar a, .pc-toolbar a, .trl-toolbar a, .bb-toolbar a').forEach(link => {
            const url = new URL(link.href, window.location.origin);
            if (url.pathname !== window.location.pathname) return;
            appendTo(url);
            link.href = url.toString();
        });
    });
})();
