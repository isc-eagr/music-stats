// Config page: one section visible at a time, unsaved-change tracking, Esc / Ctrl+S, backup busy state.
(function () {
    const page = document.querySelector('.cfg-page');
    const form = document.getElementById('configForm');
    const foot = document.getElementById('configFoot');
    const status = document.getElementById('configStatus');
    const statusText = document.getElementById('configStatusText');
    const tabs = Array.from(document.querySelectorAll('.cfg-tab'));
    const panels = Array.from(document.querySelectorAll('.cfg-panel'));
    const fields = Array.from(form.querySelectorAll('input, select'));
    const names = tabs.map(tab => tab.dataset.tab);
    const savedOnLoad = status.classList.contains('is-saved');

    const valueOf = input => input.type === 'checkbox' ? input.checked : input.value;
    const initial = new Map(fields.map(input => [input, valueOf(input)]));

    function show(name, updateHash) {
        if (!names.includes(name)) name = names[0];
        tabs.forEach(tab => {
            const active = tab.dataset.tab === name;
            tab.classList.toggle('is-active', active);
            tab.setAttribute('aria-selected', active ? 'true' : 'false');
            tab.tabIndex = active ? 0 : -1;
        });
        panels.forEach(panel => { panel.hidden = panel.dataset.panel !== name; });
        foot.hidden = name === 'database';
        if (updateHash) history.replaceState(null, '', '#' + name);
    }

    function refresh() {
        let changed = 0;
        const dirtyPanels = new Set();
        fields.forEach(input => {
            const isChanged = valueOf(input) !== initial.get(input);
            input.closest('.cfg-row').classList.toggle('is-changed', isChanged);
            if (isChanged) {
                changed++;
                dirtyPanels.add(input.closest('.cfg-panel').dataset.panel);
            }
        });
        tabs.forEach(tab => tab.classList.toggle('has-changes', dirtyPanels.has(tab.dataset.tab)));
        status.classList.toggle('is-dirty', changed > 0);
        status.classList.toggle('is-saved', changed === 0 && savedOnLoad);
        statusText.textContent = changed > 0
            ? changed + (changed === 1 ? ' unsaved change' : ' unsaved changes')
            : (savedOnLoad ? 'Saved' : 'No changes yet');
    }

    tabs.forEach(tab => tab.addEventListener('click', () => show(tab.dataset.tab, true)));

    // Arrow keys move between sections, like a tab list
    document.querySelector('.cfg-nav').addEventListener('keydown', event => {
        const step = { ArrowDown: 1, ArrowRight: 1, ArrowUp: -1, ArrowLeft: -1 }[event.key];
        if (!step) return;
        event.preventDefault();
        const index = tabs.findIndex(tab => tab.classList.contains('is-active'));
        const next = tabs[(index + step + tabs.length) % tabs.length];
        show(next.dataset.tab, true);
        next.focus();
    });

    form.addEventListener('input', refresh);
    form.addEventListener('change', refresh);
    form.addEventListener('reset', () => setTimeout(refresh));

    // A bad value in a hidden section would block saving silently: open that section first
    form.addEventListener('invalid', event => {
        const panel = event.target.closest('.cfg-panel');
        if (panel && panel.hidden) show(panel.dataset.panel, true);
    }, true);

    // The redirect after a POST keeps the request's #section, so the same section reopens
    form.addEventListener('submit', () => { form.action = form.action.split('#')[0] + location.hash; });

    document.addEventListener('keydown', event => {
        if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 's') {
            event.preventDefault();
            form.requestSubmit();
        } else if (event.key === 'Escape' && status.classList.contains('is-dirty')) {
            form.reset();
        }
    });

    const backupForm = document.getElementById('databaseBackupForm');
    backupForm.addEventListener('submit', () => {
        backupForm.action = backupForm.action.split('#')[0] + '#database';
        const button = document.getElementById('databaseBackupButton');
        button.disabled = true;
        button.textContent = 'Backing up...';
        document.getElementById('databaseBackupStatus').textContent = 'Creating the backup. Keep this page open until it finishes.';
    });

    show(page.dataset.initialTab || location.hash.slice(1), false);
    refresh();
})();
