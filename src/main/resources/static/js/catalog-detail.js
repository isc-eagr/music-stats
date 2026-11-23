/* Catalog detail page: switches the Over the years chart and its year pills between plays and time listened. */
(function () {
    'use strict';

    document.addEventListener('DOMContentLoaded', () => {
        const card = document.querySelector('[data-cd-years]');
        if (!card) return;
        const buttons = card.querySelectorAll('[data-cd-measure]');
        buttons.forEach((btn) => btn.addEventListener('click', () => {
            const measure = btn.dataset.cdMeasure;
            buttons.forEach((b) => {
                const on = b === btn;
                b.classList.toggle('is-on', on);
                b.setAttribute('aria-pressed', String(on));
            });
            card.querySelectorAll('[data-cd-stream]').forEach((svg) => { svg.hidden = svg.dataset.cdStream !== measure; });
            card.querySelectorAll('[data-cd-note]').forEach((note) => { note.hidden = note.dataset.cdNote !== measure; });
            card.querySelectorAll('[data-cd-pill]').forEach((pill) => { pill.hidden = pill.dataset.cdPill !== measure; });
        }));
    });
})();
