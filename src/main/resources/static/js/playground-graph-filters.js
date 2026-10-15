(function () {
    'use strict';

    const EXPLORER_PARAMS = new Set(['view', 'tab', 'source', 'graphMetric', 'graphScale', 'graphOrder', 'graphDirection', 'tableSort', 'tableDirection', 'categoryQ', 'categoryPage', 'categorySize']);
    const SAME_SOURCE_PARAMS = new Set(['includeGroups', 'includeFeatured', 'limit', 'limitEntity']);
    const CATALOG_PARAMS = new Set(['view', 'page', 'perpage', 'sortby', 'sortdir', 'sortby2', 'sortdir2', 'sortby3', 'sortdir3', 'filtersOnly', 'compareMeasures']);
    const NULL_MODES = new Set(['isnull', 'isnotnull', 'zero', 'nonzero']);

    function cleanText(value) {
        return String(value || '').replace(/[\u25b6\u25bc\u2705\u274c\u{1f4e6}\u{1f4ec}\ufe0f]/gu, '').replace(/\s+/g, ' ').trim();
    }

    function humanize(name) {
        return name.replace(/([A-Z])/g, ' $1').replace(/^./, char => char.toUpperCase());
    }

    function displayDate(value) {
        const text = String(value || '');
        return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text.slice(8, 10) + '/' + text.slice(5, 7) + '/' + text.slice(0, 4) : text;
    }

    function activeFilterGroups(params, schema) {
        const groups = new Map();
        const definitions = new Map();
        (schema || []).forEach(group => {
            [...group.fields, ...group.modes, ...group.searchFields].forEach(field => definitions.set(field.name, group.title));
        });
        for (const [name, value] of params) {
            if (!value || EXPLORER_PARAMS.has(name) || CATALOG_PARAMS.has(name)) continue;
            if (['includeGroups', 'includeFeatured'].includes(name) && value !== 'true') continue;
            if (name === 'includeMain' && value !== 'false') continue;
            if (/Mode$/.test(name) && !NULL_MODES.has(value)) continue;
            const base = name === 'limitEntity' ? 'limit' : name.replace(/(?:Mode|Min|Max|From|To)$/, '');
            const title = definitions.get(name) || (base === 'q' ? 'Catalog search' : humanize(base));
            if (!groups.has(title)) groups.set(title, { title, names: new Set() });
            groups.get(title).names.add(name);
            const mode = base + 'Mode';
            if (params.has(mode)) groups.get(title).names.add(mode);
        }
        return Array.from(groups.values()).sort((a, b) => a.title.localeCompare(b.title));
    }

    function parseDuration(value) {
        const text = String(value || '').trim();
        if (!text) return '';
        const parts = text.split(':');
        if (parts.length > 3 || parts.some(part => !/^\d+$/.test(part))) return null;
        const result = parts.reduce((seconds, part) => seconds * 60 + Number(part), 0);
        return Number.isSafeInteger(result) ? String(result) : null;
    }

    function formatDuration(value) {
        if (value === '' || value == null) return '';
        const seconds = Number(value);
        if (!Number.isFinite(seconds) || seconds < 0) return '';
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor(seconds / 60) % 60;
        const remainder = String(Math.floor(seconds) % 60).padStart(2, '0');
        return hours ? hours + ':' + String(minutes).padStart(2, '0') + ':' + remainder : Math.floor(seconds / 60) + ':' + remainder;
    }

    function fieldEnabled(name, modes) {
        for (const [modeName, mode] of Object.entries(modes)) {
            const base = modeName.replace(/Mode$/, '');
            if (!name.startsWith(base) || name === modeName) continue;
            if (NULL_MODES.has(mode)) return false;
            if (base === 'length') {
                if (mode === 'lt' && name === 'lengthMin') return false;
                if (mode === 'gt' && name === 'lengthMax') return false;
            }
            if (/Date$/.test(base)) {
                const range = name === base + 'From' || name === base + 'To';
                if (mode === 'between') return range;
                if (range) return false;
            }
        }
        return true;
    }

    function serializeFields(fields, currentParams, source) {
        const result = new URLSearchParams();
        for (const [name, value] of currentParams) {
            if ((EXPLORER_PARAMS.has(name) || (SAME_SOURCE_PARAMS.has(name) && !fields.some(field => field.name === name))) && name !== 'source') result.append(name, value);
        }
        result.set('source', source);
        const modes = Object.fromEntries(fields.filter(field => /Mode$/.test(field.name)).map(field => [field.name, field.value]));
        const activeBases = new Set();
        for (const field of fields) {
            if (!field.name || /Mode$/.test(field.name) || CATALOG_PARAMS.has(field.name) || field.disabled || !fieldEnabled(field.name, modes)) continue;
            if ((field.type === 'checkbox' || field.type === 'radio') && !field.checked) continue;
            const value = field.duration ? parseDuration(field.value) : String(field.value || '').trim();
            if (value == null || value === '') continue;
            result.append(field.name, value);
            for (const modeName of Object.keys(modes)) {
                const base = modeName.replace(/Mode$/, '');
                if (field.name.startsWith(base)) activeBases.add(base);
            }
        }
        for (const [name, value] of Object.entries(modes)) {
            if (value && (activeBases.has(name.replace(/Mode$/, '')) || NULL_MODES.has(value))) result.set(name, value);
        }
        return result;
    }

    function clearParams(params, source, switchingSource) {
        const result = new URLSearchParams();
        for (const [name, value] of params) if ((EXPLORER_PARAMS.has(name) || (!switchingSource && SAME_SOURCE_PARAMS.has(name))) && name !== 'source') result.append(name, value);
        result.set('source', source);
        return result;
    }

    function create(tag, className, text) {
        const element = document.createElement(tag);
        if (className) element.className = className;
        if (text !== undefined) element.textContent = text;
        return element;
    }

    function inputLabel(input, fallback) {
        const label = input.closest('label');
        if (label) {
            const copy = label.cloneNode(true);
            copy.querySelectorAll('input,select,button,.filter-modifiers').forEach(element => element.remove());
            if (cleanText(copy.textContent)) return cleanText(copy.textContent);
        }
        const previous = input.previousElementSibling;
        if (previous && previous.tagName === 'LABEL') return cleanText(previous.textContent);
        return input.placeholder && !/dd\/mm\/yyyy/.test(input.placeholder) ? cleanText(input.placeholder) : humanize(input.name || fallback);
    }

    function extractSchema(sourceForm) {
        return Array.from(sourceForm.querySelectorAll('.filter-item')).map(item => {
            const title = cleanText(item.querySelector('.filter-item-title')?.textContent);
            const controls = Array.from(item.querySelectorAll('input[name],select[name],textarea[name]'));
            const modes = Array.from(item.querySelectorAll('.filter-modifiers')).map(modifiers => {
                const buttons = Array.from(modifiers.querySelectorAll('[data-mode][data-filter]'));
                if (!buttons.length) return null;
                const name = buttons[0].dataset.filter + 'Mode';
                const hidden = Array.from(item.querySelectorAll('input[name]')).find(input => input.name === name);
                return { name, label: buttons.length > 1 && title === 'Billboard' ? humanize(buttons[0].dataset.filter) + ' rule' : 'Match',
                    value: hidden?.value || buttons.find(button => button.classList.contains('active'))?.dataset.mode || buttons.find(button => !NULL_MODES.has(button.dataset.mode))?.dataset.mode || '',
                    options: buttons.map(button => ({ value: button.dataset.mode, label: cleanText(button.textContent) })) };
            }).filter(Boolean);
            controls.filter(input => /Mode$/.test(input.name)).forEach(input => {
                if (modes.some(mode => mode.name === input.name)) return;
                const peers = controls.filter(control => control.name === input.name);
                let options;
                let value = input.value;
                if (input.tagName === 'SELECT') {
                    options = Array.from(input.options).map(option => ({ value: option.value, label: cleanText(option.textContent) }));
                } else if (input.type === 'radio') {
                    options = peers.map(peer => ({ value: peer.value, label: inputLabel(peer, title) }));
                    value = peers.find(peer => peer.checked)?.value || '';
                } else {
                    options = [{ value: value || '', label: value ? humanize(value) : 'Range' }];
                }
                modes.push({ name: input.name, label: 'Match', value, options });
            });
            if (modes.length > 1) modes.forEach(mode => { mode.label = humanize(mode.name.replace(/Mode$/, '')) + ' rule'; });
            const fields = controls.filter(input => !/Mode$/.test(input.name)).map(input => ({
                name: input.name, type: input.tagName === 'SELECT' ? 'select' : input.type || 'text', value: input.value,
                checked: input.checked, label: inputLabel(input, title), placeholder: input.placeholder || '',
                min: input.getAttribute('min'), max: input.getAttribute('max'), step: input.getAttribute('step'),
                date: /dd\/mm\/yyyy/i.test(input.placeholder || ''), duration: /^length(Min|Max)$/.test(input.name),
                options: input.tagName === 'SELECT' ? Array.from(input.options).map(option => ({ value: option.value, label: cleanText(option.textContent) })) : []
            }));
            const searchFields = Array.from(item.querySelectorAll('[id$="ChipSelect"]')).map(container => ({
                name: container.id.replace(/ChipSelect$/, ''), label: title
            }));
            return { title, modes, fields, searchFields, note: cleanText(item.querySelector('.filter-note')?.textContent) };
        }).sort((a, b) => a.title.localeCompare(b.title));
    }

    window.playgroundGraphFilters = { parseDuration, formatDuration, displayDate, fieldEnabled, serializeFields, clearParams, activeFilterGroups, extractSchema };

    function initialize() {
        const editor = document.getElementById('graphs-filter-editor');
        const toggle = document.getElementById('graphs-filter-toggle');
        const sourceSelect = document.getElementById('graphs-filter-source');
        if (!editor || !toggle || !sourceSelect) return;
        let source = ['artists', 'albums', 'songs'].includes(new URLSearchParams(window.location.search).get('source'))
            ? new URLSearchParams(window.location.search).get('source') : 'songs';
        sourceSelect.value = source;
        let requestId = 0;
        let currentForm;
        let groups = [];
        let loadedSchema = [];
        let appliedParams = new URLSearchParams(window.location.search);
        const schemaCache = new Map();
        const pickers = [];
        editor.classList.add('playground-filter-editor');
        editor.hidden = true;
        toggle.setAttribute('aria-controls', editor.id);
        toggle.setAttribute('aria-expanded', 'false');

        function status(message, retry) {
            pickers.splice(0).forEach(picker => picker.destroy());
            editor.replaceChildren(create('p', 'pg-filter-status', message));
            if (retry) {
                const button = create('button', 'pg-filter-secondary', 'Try again');
                button.type = 'button';
                button.addEventListener('click', load);
                editor.append(button);
            }
        }

        function applied(params) {
            appliedParams = new URLSearchParams(params);
            document.dispatchEvent(new CustomEvent('playground:filters-applied', { detail: { params, source } }));
            updateContext();
        }

        function updateContext() {
            const active = activeFilterGroups(appliedParams, loadedSchema);
            const context = document.getElementById('graphs-filter-context');
            if (context) context.textContent = humanize(source) + ' · ' + (active.length ? active.length + ' active filter' + (active.length === 1 ? '' : 's') : 'All items');
            toggle.textContent = active.length ? 'Filters (' + active.length + ')' : 'Filters';
            const chips = document.getElementById('graphs-active-filters');
            if (!chips) return;
            chips.replaceChildren();
            active.forEach(group => {
                const chip = create('button', 'pg-filter-chip', group.title);
                chip.type = 'button';
                chip.setAttribute('aria-label', 'Remove ' + group.title.toLowerCase() + ' filter');
                chip.append(create('span', '', ' ×'));
                chip.addEventListener('click', () => {
                    const params = new URLSearchParams(window.location.search);
                    group.names.forEach(name => params.delete(name));
                    if (group.names.has('limit')) params.delete('limitEntity');
                    applied(params);
                    if (!editor.hidden) load(params);
                });
                chips.append(chip);
            });
            chips.hidden = !active.length;
        }

        function readFields() {
            return Array.from(currentForm.querySelectorAll('[name]')).map(input => ({
                name: input.name, value: input.value, type: input.type, checked: input.checked,
                disabled: input.disabled, duration: input.dataset.duration === 'true'
            }));
        }

        function updateControls() {
            const fields = readFields();
            const modes = Object.fromEntries(fields.filter(field => /Mode$/.test(field.name)).map(field => [field.name, field.value]));
            currentForm.querySelectorAll('[name]').forEach(input => {
                input.disabled = !fieldEnabled(input.name, modes);
                if (input.dataset.fieldType !== 'checklist') input.closest('.pg-filter-field')?.classList.toggle('pg-filter-field-muted', input.disabled);
            });
            const params = serializeFields(readFields(), new URLSearchParams(), source);
            groups.forEach(group => {
                const activeNames = new Set(Array.from(group.element.querySelectorAll('[name]')).map(input => input.name).filter(name => params.has(name)));
                const count = Array.from(activeNames).filter(name => !/Mode$/.test(name)).length || (activeNames.size ? 1 : 0);
                group.badge.textContent = count ? String(count) : '';
                group.badge.hidden = !count;
                group.element.classList.toggle('pg-filter-group-active', Boolean(count));
            });
            const active = activeFilterGroups(params, loadedSchema).length;
            const counter = currentForm.querySelector('.pg-filter-count');
            counter.textContent = active ? active + ' active filter' + (active === 1 ? '' : 's') : 'All ' + source;
        }

        function labeledControl(labelText, control) {
            const label = create('label', 'pg-filter-field');
            label.append(create('span', 'pg-filter-field-label', labelText), control);
            return label;
        }

        function createInput(field, params) {
            let input;
            if (field.type === 'select') {
                input = create('select');
                field.options.forEach(option => {
                    const element = create('option', '', option.label);
                    element.value = option.value;
                    input.append(element);
                });
            } else {
                input = create('input');
                input.type = field.duration || field.date || field.type === 'hidden' ? 'text' : field.type;
                for (const attribute of ['min', 'max', 'step']) if (field[attribute] != null) input.setAttribute(attribute, field[attribute]);
                input.placeholder = field.duration ? 'm:ss or h:mm:ss' : field.date ? 'dd/mm/yyyy' : field.placeholder;
            }
            input.name = field.name;
            input.autocomplete = 'off';
            if (field.date) { input.pattern = '\\d{2}/\\d{2}/\\d{4}'; input.title = 'Use dd/mm/yyyy'; }
            const value = params.has(field.name) ? params.get(field.name) : field.value;
            input.value = field.duration ? formatDuration(value) : field.date ? displayDate(value) : value || '';
            if (field.duration) {
                input.dataset.duration = 'true';
                input.addEventListener('input', () => {
                    input.setCustomValidity(parseDuration(input.value) === null ? 'Enter seconds, m:ss, or h:mm:ss.' : '');
                });
            }
            if (field.date && typeof window.flatpickr === 'function') {
                pickers.push(window.flatpickr(input, { dateFormat: 'd/m/Y', allowInput: true, disableMobile: true }));
            }
            return input;
        }

        function createChecklist(fields, params) {
            const wrapper = create('div', 'pg-filter-checklist');
            if (fields.length > 7) {
                const search = create('input', 'pg-filter-option-search');
                search.type = 'search';
                search.placeholder = 'Search options';
                search.setAttribute('aria-label', 'Search ' + fields[0].name + ' options');
                search.addEventListener('input', () => {
                    wrapper.querySelectorAll('.pg-filter-option').forEach(option => {
                        option.hidden = !option.textContent.toLocaleLowerCase().includes(search.value.toLocaleLowerCase());
                    });
                });
                wrapper.append(search);
            }
            const options = create('div', 'pg-filter-options');
            fields.forEach(field => {
                const label = create('label', 'pg-filter-option');
                const input = create('input');
                input.type = field.type;
                input.name = field.name;
                input.value = field.value;
                input.dataset.fieldType = 'checklist';
                input.checked = params.has(field.name) ? params.getAll(field.name).includes(field.value) : field.checked;
                label.append(input, create('span', '', field.label));
                if (field.name === 'gender') {
                    if (/^female$/i.test(field.label)) label.classList.add('pg-filter-female');
                    if (/^male$/i.test(field.label)) label.classList.add('pg-filter-male');
                }
                options.append(label);
            });
            wrapper.append(options);
            return wrapper;
        }

        function createArtistSearch(field, params) {
            const wrapper = create('div', 'pg-filter-artist-search');
            const selected = create('div', 'pg-filter-selected-artists');
            const search = create('input');
            search.type = 'search';
            search.placeholder = 'Search artists (2 or more characters)';
            search.setAttribute('aria-label', 'Search ' + field.label.toLowerCase());
            search.autocomplete = 'off';
            const results = create('div', 'pg-filter-artist-results');
            results.setAttribute('aria-live', 'polite');
            let timer;
            let searchRequest = 0;

            function addArtist(artist) {
                const value = String(artist.id);
                if (Array.from(selected.querySelectorAll('input')).some(input => input.value === value)) return;
                const chip = create('div', 'pg-filter-artist-chip');
                chip.dataset.artistId = value;
                const input = create('input');
                input.type = 'checkbox';
                input.name = field.name;
                input.value = value;
                input.checked = true;
                const name = create('span', '', artist.name || 'Artist #' + value);
                const remove = create('button', '', 'Remove');
                remove.type = 'button';
                remove.setAttribute('aria-label', 'Remove ' + name.textContent);
                remove.addEventListener('click', () => { chip.remove(); updateControls(); });
                chip.append(input, name, remove);
                if (Number(artist.genderId) === 1) chip.classList.add('pg-filter-female');
                if (Number(artist.genderId) === 2) chip.classList.add('pg-filter-male');
                selected.append(chip);
            }

            const ids = params.getAll(field.name);
            ids.forEach(id => addArtist({ id }));
            if (ids.length) {
                const query = new URLSearchParams();
                ids.forEach(id => query.append('id', id));
                fetch('/artists/api/filter-values?' + query).then(response => {
                    if (!response.ok) throw new Error('Could not resolve selected artists');
                    return response.json();
                }).then(artists => {
                    artists.forEach(artist => {
                        const chip = Array.from(selected.children).find(element => element.dataset.artistId === String(artist.id));
                        if (chip) {
                            chip.querySelector('span').textContent = artist.name;
                            chip.querySelector('button').setAttribute('aria-label', 'Remove ' + artist.name);
                            if (Number(artist.genderId) === 1) chip.classList.add('pg-filter-female');
                            if (Number(artist.genderId) === 2) chip.classList.add('pg-filter-male');
                        }
                    });
                }).catch(() => {});
            }
            search.addEventListener('input', () => {
                clearTimeout(timer);
                const token = ++searchRequest;
                results.replaceChildren();
                const query = search.value.trim();
                if (query.length < 2) return;
                timer = setTimeout(async () => {
                    results.textContent = 'Searching...';
                    try {
                        const response = await fetch('/artists/api/search?q=' + encodeURIComponent(query) + '&limit=20');
                        if (!response.ok) throw new Error('Search unavailable');
                        const artists = await response.json();
                        if (token !== searchRequest) return;
                        results.replaceChildren();
                        artists.forEach(artist => {
                            const button = create('button', 'pg-filter-artist-result', artist.name);
                            button.type = 'button';
                            if (Number(artist.genderId) === 1) button.classList.add('pg-filter-female');
                            if (Number(artist.genderId) === 2) button.classList.add('pg-filter-male');
                            button.addEventListener('click', () => {
                                addArtist(artist);
                                search.value = '';
                                results.replaceChildren();
                                updateControls();
                                search.focus();
                            });
                            results.append(button);
                        });
                        if (!artists.length) results.textContent = 'No matching artists.';
                    } catch (error) {
                        if (token === searchRequest) results.textContent = 'Artist search is unavailable. Try again.';
                    }
                }, 250);
            });
            wrapper.append(selected, search, results);
            return wrapper;
        }

        function savedFiltersToolbar(params) {
            const toolbar = create('div', 'pg-filter-saved');
            const select = create('select');
            select.setAttribute('aria-label', 'Saved ' + source + ' filters');
            select.append(create('option', '', 'Saved filters'));
            select.options[0].value = '';
            const name = create('input');
            name.type = 'text';
            name.placeholder = 'Name this filter';
            name.setAttribute('aria-label', 'Saved filter name');
            name.maxLength = 80;
            const save = create('button', 'pg-filter-secondary', 'Save');
            save.type = 'button';
            const message = create('span', 'pg-filter-saved-status');
            message.setAttribute('role', 'status');
            const toolbarSource = source;
            let saved = [];
            async function refresh() {
                const response = await fetch('/api/saved-filters?pageKey=' + encodeURIComponent('/' + toolbarSource));
                if (!response.ok) throw new Error('Saved filters unavailable');
                saved = await response.json();
                select.replaceChildren(create('option', '', 'Saved filters'));
                select.options[0].value = '';
                saved.forEach((filter, index) => {
                    const option = create('option', '', filter.name);
                    option.value = String(index);
                    select.append(option);
                });
            }
            select.addEventListener('change', () => {
                const filter = saved[Number(select.value)];
                if (select.value === '' || !filter) return;
                const merged = clearParams(new URLSearchParams(window.location.search), source);
                const loadedNames = new Set();
                new URLSearchParams(filter.query).forEach((value, key) => {
                    if (!EXPLORER_PARAMS.has(key) && !CATALOG_PARAMS.has(key)) {
                        if (!loadedNames.has(key)) merged.delete(key);
                        loadedNames.add(key);
                        merged.append(key, value);
                    }
                });
                applied(merged);
                currentForm = null;
                load(merged);
            });
            save.addEventListener('click', async () => {
                if (!name.value.trim()) { name.focus(); message.textContent = 'Enter a name first.'; return; }
                if (!currentForm.reportValidity()) return;
                const filters = serializeFields(readFields(), params, source);
                for (const key of EXPLORER_PARAMS) filters.delete(key);
                save.disabled = true;
                try {
                    const response = await fetch('/api/saved-filters', {
                        method: 'POST', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ pageKey: '/' + toolbarSource, name: name.value.trim(), query: filters.toString() })
                    });
                    if (!response.ok) throw new Error('Could not save');
                    await refresh();
                    message.textContent = 'Filter saved.';
                } catch (error) { message.textContent = 'Could not save this filter. Try again.'; }
                finally { save.disabled = false; }
            });
            toolbar.append(select, name, save, message);
            refresh().catch(() => { message.textContent = 'Saved filters unavailable.'; });
            return toolbar;
        }

        function render(schema, sourceForm, params) {
            loadedSchema = schema;
            pickers.splice(0).forEach(picker => picker.destroy());
            currentForm = create('form', 'pg-filter-form');
            currentForm.autocomplete = 'off';
            groups = [];
            const intro = create('div', 'pg-filter-intro');
            intro.append(create('p', '', 'Choose a catalog to define your selection. Its filters apply to every breakdown below.'));
            const search = create('input', 'pg-filter-find');
            search.type = 'search';
            search.placeholder = 'Find a filter (genre, date, play count...)';
            search.setAttribute('aria-label', 'Find a filter');
            intro.append(search);
            currentForm.append(intro, savedFiltersToolbar(params));

            const q = create('input');
            q.type = 'search';
            q.name = 'q';
            q.value = params.get('q') || sourceForm.querySelector('[name="q"]')?.value || '';
            q.placeholder = 'Search ' + source + ' by name';
            currentForm.append(labeledControl('Catalog search', q));

            if (source === 'artists') {
                const scopes = create('fieldset', 'pg-filter-scopes');
                scopes.append(create('legend', '', 'Artist play scope'));
                for (const [name, title, defaultValue] of [['includeMain', 'Main artist', 'true'], ['includeGroups', 'Group memberships', 'false'], ['includeFeatured', 'Featured appearances', 'false']]) {
                    const select = create('select');
                    select.name = name;
                    for (const [value, label] of [['true', 'Include'], ['false', 'Exclude']]) {
                        const option = create('option', '', label);
                        option.value = value;
                        select.append(option);
                    }
                    select.value = params.get(name) || sourceForm.querySelector('[name="' + name + '"]')?.value || defaultValue;
                    scopes.append(labeledControl(title, select));
                }
                currentForm.append(scopes);
            }

            const grid = create('div', 'pg-filter-groups');
            schema.forEach(group => {
                const details = create('details', 'pg-filter-group');
                details.dataset.filterTitle = group.title.toLocaleLowerCase();
                const summary = create('summary');
                const badge = create('span', 'pg-filter-group-count');
                badge.hidden = true;
                summary.append(create('span', '', group.title), badge);
                const body = create('div', 'pg-filter-group-body');
                group.modes.forEach(mode => {
                    const select = create('select');
                    select.name = mode.name;
                    mode.options.forEach(option => {
                        const element = create('option', '', option.label);
                        element.value = option.value;
                        select.append(element);
                    });
                    select.value = params.get(mode.name) || mode.value;
                    body.append(labeledControl(mode.label, select));
                });
                const checkboxes = group.fields.filter(field => field.type === 'checkbox' || field.type === 'radio');
                if (checkboxes.length) body.append(createChecklist(checkboxes, params));
                const inputs = group.fields.filter(field => field.type !== 'checkbox' && field.type !== 'radio');
                const ranges = create('div', 'pg-filter-inputs');
                inputs.forEach(field => ranges.append(labeledControl(field.label, createInput(field, params))));
                if (inputs.length) body.append(ranges);
                group.searchFields.forEach(field => body.append(createArtistSearch(field, params)));
                if (group.note) body.append(create('p', 'pg-filter-note', group.note));
                details.append(summary, body);
                grid.append(details);
                groups.push({ element: details, badge });
            });
            currentForm.append(grid);
            const empty = create('p', 'pg-filter-empty', 'No matching filters. Try another name.');
            empty.hidden = true;
            currentForm.append(empty);
            search.addEventListener('input', () => {
                const term = search.value.trim().toLocaleLowerCase();
                groups.forEach(group => { group.element.hidden = !group.element.dataset.filterTitle.includes(term); });
                empty.hidden = groups.some(group => !group.element.hidden);
            });
            const actions = create('div', 'pg-filter-actions');
            const count = create('span', 'pg-filter-count');
            count.setAttribute('role', 'status');
            const clear = create('button', 'pg-filter-secondary', 'Clear filters');
            clear.type = 'button';
            clear.addEventListener('click', () => {
                const params = clearParams(new URLSearchParams(window.location.search), source);
                applied(params);
                load(params);
            });
            const close = create('button', 'pg-filter-secondary', 'Cancel');
            close.type = 'button';
            close.addEventListener('click', () => setOpen(false));
            const apply = create('button', 'pg-filter-primary', 'Apply filters');
            apply.type = 'submit';
            actions.append(count, clear, close, apply);
            currentForm.append(actions);
            currentForm.addEventListener('input', updateControls);
            currentForm.addEventListener('change', updateControls);
            currentForm.addEventListener('submit', event => {
                event.preventDefault();
                if (!currentForm.reportValidity()) return;
                applied(serializeFields(readFields(), new URLSearchParams(window.location.search), source));
                setOpen(false);
            });
            editor.replaceChildren(currentForm);
            updateControls();
            updateContext();
        }

        async function load(overrideParams) {
            const token = ++requestId;
            const params = overrideParams instanceof URLSearchParams ? overrideParams : new URLSearchParams(window.location.search);
            const query = new URLSearchParams();
            params.forEach((value, name) => {
                if (!EXPLORER_PARAMS.has(name) && !CATALOG_PARAMS.has(name)) query.append(name, value);
            });
            query.set('filtersOnly', 'true');
            const url = '/' + source + '?' + query.toString();
            status('Loading ' + source + ' filters...');
            try {
                let cached = schemaCache.get(url);
                if (!cached) {
                    const response = await fetch(url, { headers: { 'Accept': 'text/html' } });
                    if (!response.ok) throw new Error('Filter request failed');
                    const parsed = new DOMParser().parseFromString(await response.text(), 'text/html');
                    const form = parsed.querySelector('#filterForm');
                    if (!form) throw new Error('No filter form returned');
                    cached = { schema: extractSchema(form), form };
                    schemaCache.set(url, cached);
                    if (schemaCache.size > 6) schemaCache.delete(schemaCache.keys().next().value);
                }
                if (token !== requestId) return;
                render(cached.schema, cached.form, params);
            } catch (error) {
                if (token === requestId) status('Could not load filters. Your current selection is still applied.', true);
            }
        }

        function setOpen(open) {
            editor.hidden = !open;
            toggle.setAttribute('aria-expanded', String(open));
            if (open) {
                load().then(() => editor.querySelector('.pg-filter-find')?.focus());
            } else {
                requestId++;
                pickers.forEach(picker => picker.close());
                toggle.focus();
            }
        }

        toggle.addEventListener('click', () => setOpen(editor.hidden));
        sourceSelect.addEventListener('change', () => {
            if (!['artists', 'albums', 'songs'].includes(sourceSelect.value)) return;
            source = sourceSelect.value;
            loadedSchema = [];
            const params = clearParams(new URLSearchParams(window.location.search), source, true);
            applied(params);
            currentForm = null;
            if (!editor.hidden) load(params);
        });
        editor.addEventListener('keydown', event => {
            if (event.key === 'Escape') { event.preventDefault(); setOpen(false); }
        });
        window.addEventListener('popstate', () => {
            appliedParams = new URLSearchParams(window.location.search);
            source = ['artists', 'albums', 'songs'].includes(appliedParams.get('source')) ? appliedParams.get('source') : 'songs';
            sourceSelect.value = source;
            loadedSchema = [];
            currentForm = null;
            updateContext();
            if (!editor.hidden) load();
        });
        ['graphs-include-groups', 'graphs-include-featured', 'graphs-item-limit', 'graphs-limit-entity'].forEach(id => {
            document.getElementById(id)?.addEventListener('change', () => {
                appliedParams = new URLSearchParams(window.location.search);
                updateContext();
                if (!editor.hidden) load();
            });
        });
        updateContext();
    }

    document.addEventListener('DOMContentLoaded', initialize);
}());
