/* Timeframe detail page: Plays / Time and step toggles of the Across card, and the All / Male / Female filter of the top lists. */
(function () {
    'use strict';

    function press(buttons, active) {
        buttons.forEach((b) => {
            const on = b === active;
            b.classList.toggle('is-on', on);
            b.setAttribute('aria-pressed', String(on));
        });
    }

    function initAcross(card) {
        const measures = card.querySelectorAll('[data-tf-measure]');
        const steps = card.querySelectorAll('[data-tf-step]');
        let measure = 'plays';
        let step = steps.length ? steps[0].dataset.tfStep : (card.querySelector('[data-tf-pills]') || {}).dataset?.tfPills;

        function render() {
            card.querySelectorAll('[data-tf-stream]').forEach((svg) => { svg.hidden = svg.dataset.tfStream !== step + '-' + measure; });
            card.querySelectorAll('[data-tf-pills]').forEach((row) => { row.hidden = row.dataset.tfPills !== step; });
            card.querySelectorAll('[data-tf-pill]').forEach((pill) => { pill.hidden = pill.dataset.tfPill !== measure; });
            if (step) card.querySelectorAll('[data-tf-unit]').forEach((u) => { u.textContent = step.replace(/s$/, ''); });
        }

        measures.forEach((btn) => btn.addEventListener('click', () => {
            measure = btn.dataset.tfMeasure;
            press(measures, btn);
            render();
        }));
        steps.forEach((btn) => btn.addEventListener('click', () => {
            step = btn.dataset.tfStep;
            press(steps, btn);
            render();
        }));
    }

    function initTop(card) {
        const filters = card.querySelectorAll('[data-tf-filter]');
        filters.forEach((btn) => btn.addEventListener('click', () => {
            press(filters, btn);
            card.querySelectorAll('[data-tf-list]').forEach((list) => { list.hidden = list.dataset.tfList !== btn.dataset.tfFilter; });
        }));
    }

    document.addEventListener('DOMContentLoaded', () => {
        document.querySelectorAll('[data-tf-across]').forEach(initAcross);
        document.querySelectorAll('[data-tf-top]').forEach(initTop);
    });
})();
