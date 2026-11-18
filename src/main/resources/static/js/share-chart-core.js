/*
 * Helpers shared by the stacked share charts: Playground > Breakdown over time (breakdown-history.js)
 * and the Share over time panel above the catalog lists (catalog-share-history.js).
 */
window.ShareChartCore = (() => {
    'use strict';

    // Area hit testing uses the actual filled band at the pointer's x coordinate.
    // Nearest-point mode alone can select the adjacent band instead of the hovered one.
    function sectionInteraction(chart, event, options, useFinalPosition) {
        if (!chart.options.scales.y.stacked || chart.config.type === 'bar') {
            return chart.getElementsAtEventForMode(event, 'nearest',
                { axis: 'xy', intersect: chart.config.type === 'bar' }, useFinalPosition).slice(0, 1);
        }
        const { x, y } = Chart.helpers.getRelativePosition(event, chart);
        const area = chart.chartArea;
        if (x < area.left || x > area.right || y < area.top || y > area.bottom) return [];
        const count = chart.data.labels.length;
        const firstX = chart.scales.x.getPixelForValue(0);
        const spacing = count > 1 ? chart.scales.x.getPixelForValue(1) - firstX : 1;
        const position = Math.max(0, Math.min(count - 1, (x - firstX) / spacing));
        const left = Math.floor(position), right = Math.ceil(position), index = Math.round(position);
        let bottom = 0;
        for (let datasetIndex = 0; datasetIndex < chart.data.datasets.length; datasetIndex++) {
            const values = chart.data.datasets[datasetIndex].data;
            if (values[left] === null || values[right] === null) return [];
            const value = values[left] + (values[right] - values[left]) * (position - left);
            const top = bottom + value;
            if (value > 0 && y <= chart.scales.y.getPixelForValue(bottom) && y >= chart.scales.y.getPixelForValue(top)) {
                return [{ element: chart.getDatasetMeta(datasetIndex).data[index], datasetIndex, index }];
            }
            bottom = top;
        }
        return [];
    }

    /** Registers the band hit-testing mode once, as interaction mode 'stackedSection'. */
    function registerSectionMode() {
        if (typeof Chart !== 'undefined') Chart.Interaction.modes.stackedSection = sectionInteraction;
    }

    function niceCeiling(value) {
        if (value <= 0) return 0;
        const step = Math.max(1, 10 ** Math.floor(Math.log10(value)) / 2);
        return Math.ceil(value / step) * step;
    }

    return { registerSectionMode, niceCeiling };
})();
