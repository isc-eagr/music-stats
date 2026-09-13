document.addEventListener('DOMContentLoaded', () => {
    const section = document.querySelector('.plays-chart-section');
    if (!section) return;
    const title = section.querySelector('[data-plays-visualization-title]');
    const panes = section.querySelectorAll('[data-plays-visualization]');
    const buttons = section.querySelectorAll('[data-plays-view]');
    const storageKey = 'musicStats.detailPlaysVisualization';

    const show = view => {
        panes.forEach(pane => pane.hidden = pane.dataset.playsVisualization !== view);
        buttons.forEach(button => {
            const active = button.dataset.playsView === view;
            button.classList.toggle('active', active);
            button.setAttribute('aria-selected', String(active));
        });
        if (title) title.textContent = view === 'heatmap' ? 'Daily Play Count' : 'Plays by Year';
        localStorage.setItem(storageKey, view);
        if (view === 'chart') window.dispatchEvent(new Event('resize'));
    };
    buttons.forEach(button => button.addEventListener('click', () => show(button.dataset.playsView)));
    show(localStorage.getItem(storageKey) === 'heatmap' ? 'heatmap' : 'chart');

    section.querySelector('[data-heatmap-year]')?.addEventListener('change', event => {
        const url = new URL(window.location.href);
        url.searchParams.set('tab', 'plays');
        url.searchParams.set('heatmapYear', event.target.value);
        url.searchParams.delete('playsPage');
        window.location.assign(url);
    });
});
