(() => {
    'use strict';
    const get = name => document.getElementById(`history-${name}`);
    const controls = ['breakdown', 'measure', 'mode', 'interval', 'year', 'chart', 'scale', 'categories'];
    let history = null;
    let chart = null;
    let request = null;
    let allTimePeaks = new Map();
    let clickedTooltip = null;
    const categoryColors = new Map();
    const palette = ['#56b4e9', '#e69f00', '#009e73', '#cc79a7', '#f0e442', '#d55e00',
        '#8b80f9', '#00ced1', '#f27098', '#8da827', '#b38a65', '#3366cc'];
    const INITIAL_IMPORT_DATE = '2005-02-01';
    const displayDate = date => date.split('-').reverse().join('/');
    const number = value => value.toLocaleString(undefined, { maximumFractionDigits: 2 });
    const amount = value => get('measure').value === 'time'
        ? `${Math.floor(value / 3600).toLocaleString()}h ${Math.floor(value % 3600 / 60)}m ${value % 60}s`
        : number(value);
    const share = (value, total) => total > 0 ? `${(100 * value / total).toFixed(2)}%` : 'No data';
    const movementClass = value => value > 0 ? 'increase' : value < 0 ? 'decrease' : 'unchanged';
    const signedAmount = value => `${value > 0 ? '+' : value < 0 ? '-' : ''}${amount(Math.abs(value))}`;
    const signedPercentagePoints = value => `${value > 0 ? '+' : value < 0 ? '-' : ''}${Math.abs(value).toFixed(2)} pp`;

    function color(name) {
        if (get('breakdown').value === 'gender') {
            return name === 'Male' ? '#4a9eff' : name === 'Female' ? '#ff6b9d' : '#85858e';
        }
        if (name === 'Unknown') return '#b1b1ba';
        if (name === 'Remaining categories') return '#73737f';
        const breakdown = get('breakdown').value;
        if (!categoryColors.has(breakdown)) categoryColors.set(breakdown, new Map());
        const assigned = categoryColors.get(breakdown);
        if (!assigned.has(name)) {
            const index = assigned.size;
            assigned.set(name, palette[index] || `hsl(${(index * 137.508) % 360}, ${index % 2 ? 75 : 50}%, ${index % 3 ? 65 : 45}%)`);
        }
        return assigned.get(name);
    }

    function leaderMarkerColor(name) {
        const base = color(name);
        const match = base.match(/^#([\da-f]{2})([\da-f]{2})([\da-f]{2})$/i);
        if (match) {
            const lighten = hex => {
                const value = Number.parseInt(hex, 16);
                return Math.round(value + (255 - value) * 0.35);
            };
            return `rgb(${lighten(match[1])}, ${lighten(match[2])}, ${lighten(match[3])})`;
        }
        const hsl = base.match(/^hsl\(([^,]+),\s*([^,]+),\s*([\d.]+)%\)$/);
        return hsl ? `hsl(${hsl[1]}, ${hsl[2]}, ${Math.min(92, Number(hsl[3]) + 20)}%)` : base;
    }

    const initialImportScaleApplies = () => get('measure').value === 'plays'
        && get('scale').value === 'absolute' && get('mode').value === 'period';

    function findAllTimePeaks(data) {
        const peaks = new Map();
        data.series.forEach(series => {
            data.periods.forEach((period, i) => {
                const total = data.totals[i];
                const value = series.values[i];
                const previous = peaks.get(series.name);
                // Keep the earliest period when the same maximum occurs more than once.
                if (total > 0 && value > 0 && (!previous || value / total > previous.value / previous.total)) {
                    peaks.set(series.name, { end: period.end, value, total });
                }
            });
        });
        return peaks;
    }

    function findPeriodLeaders(data) {
        return data.periods.map((_, i) => {
            const highest = Math.max(0, ...data.series.map(series => series.values[i]));
            return new Set(highest > 0
                ? data.series.filter(series => series.values[i] === highest).map(series => series.name)
                : []);
        });
    }

    function percentageLineMax(values) {
        const highest = Math.max(0, ...values.flat().filter(Number.isFinite));
        return Math.max(5, Math.ceil(highest / 5) * 5);
    }

    function niceCeiling(value) {
        if (value <= 0) return 0;
        const step = Math.max(1, 10 ** Math.floor(Math.log10(value)) / 2);
        return Math.ceil(value / step) * step;
    }

    function initialImportIndex() {
        if (!initialImportScaleApplies()) return -1;
        return history.periods.findIndex(period => period.start <= INITIAL_IMPORT_DATE && period.end >= INITIAL_IMPORT_DATE);
    }

    function capInitialImport(values, stacked) {
        const index = initialImportIndex();
        if (index < 0) return { values, max: undefined, overflow: null };
        const normalTotals = history.totals.filter((_, periodIndex) => periodIndex !== index);
        const totalMax = niceCeiling(Math.max(0, ...normalTotals));
        const totalOverflow = history.totals[index] > totalMax;
        const normalValues = stacked
            ? normalTotals
            : values.flatMap(series => series.filter((_, periodIndex) => periodIndex !== index));
        const max = niceCeiling(Math.max(0, ...normalValues));
        const actual = stacked ? history.totals[index] : Math.max(...values.map(series => series[index]));
        if (!max || (!totalOverflow && actual <= max)) return { values, max: undefined, overflow: null };

        if (stacked) {
            const ratio = max / actual;
            return {
                values: values.map(series => series.map((value, periodIndex) => periodIndex === index ? value * ratio : value)),
                max,
                overflow: { index, type: 'stacked' }
            };
        }
        const datasets = values.flatMap((series, datasetIndex) => series[index] > max ? [datasetIndex] : []);
        return {
            values: values.map(series => series.map((value, periodIndex) => periodIndex === index ? Math.min(value, max) : value)),
            max,
            overflow: { index, type: 'line', datasets }
        };
    }

    function drawOverflowMarker(ctx, x, y, color, offset = 0) {
        const markerX = x + offset;
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.moveTo(markerX, y + 2);
        ctx.lineTo(markerX - 5, y + 11);
        ctx.lineTo(markerX + 5, y + 11);
        ctx.closePath();
        ctx.fill();
        ctx.strokeStyle = '#fff';
        ctx.lineWidth = 1;
        ctx.stroke();
    }

    function expandTooltip(event, activeElements, chart) {
        const selected = activeElements[0]
            || chart.getElementsAtEventForMode(event, 'historySection', { intersect: false }, false)[0];
        if (!selected) return;
        clickedTooltip = { datasetIndex: selected.datasetIndex, index: selected.index };
        const allAtPeriod = chart.data.datasets.flatMap((dataset, datasetIndex) =>
            dataset.data[selected.index] === null ? [] : [{ datasetIndex, index: selected.index }]);
        const position = { x: event.x, y: event.y };
        const showFullTooltip = () => {
            chart.setActiveElements(allAtPeriod);
            chart.tooltip.setActiveElements(allAtPeriod, position);
            chart.update();
        };
        showFullTooltip();
        // Chart.js finishes its native click processing after onClick. Reapply on the next frame.
        if (typeof requestAnimationFrame === 'function') requestAnimationFrame(showFullTooltip);
    }

    function tooltipMovement(context, series) {
        const index = context.dataIndex;
        if (index === 0) return null;
        const total = history.totals[index];
        const previousTotal = history.totals[index - 1];
        const value = series[context.datasetIndex].values[index];
        const previousValue = series[context.datasetIndex].values[index - 1];
        if (total <= 0 || previousTotal <= 0 || !Number.isFinite(value) || !Number.isFinite(previousValue)) return null;
        return {
            amount: value - previousValue,
            percentagePoints: 100 * value / total - 100 * previousValue / previousTotal
        };
    }

    function hideHistoryTooltip() {
        const element = get('tooltip');
        element.hidden = true;
        element.replaceChildren();
    }

    function updateWeeklyIntervalAvailability() {
        const weekly = get('interval-week');
        const hasSelectedRange = Boolean(get('year').value);
        weekly.hidden = !hasSelectedRange;
        weekly.disabled = !hasSelectedRange;
        if (!hasSelectedRange && get('interval').value === 'week') get('interval').value = 'month';
    }

    function renderExternalTooltip({ tooltip }, series, cumulative) {
        const element = get('tooltip');
        if (!tooltip.opacity || !tooltip.dataPoints?.length) {
            hideHistoryTooltip();
            return;
        }
        element.hidden = false;
        element.style.left = `${tooltip.caretX}px`;
        element.style.top = `${tooltip.caretY}px`;
        element.replaceChildren();

        const first = tooltip.dataPoints[0];
        const period = history.periods[first.dataIndex];
        const title = document.createElement('div');
        title.className = 'history-tooltip-title';
        title.textContent = cumulative ? `Through ${displayDate(period.end)}`
            : `${displayDate(period.start)} - ${displayDate(period.end)}`;
        element.append(title);

        tooltip.dataPoints.forEach(context => {
            const row = document.createElement('div');
            const selected = clickedTooltip && context.datasetIndex === clickedTooltip.datasetIndex
                && context.dataIndex === clickedTooltip.index;
            row.className = `history-tooltip-row${selected ? ' is-selected' : ''}`;
            const swatch = document.createElement('i');
            swatch.className = 'history-tooltip-swatch';
            swatch.style.backgroundColor = color(context.dataset.label);
            swatch.style.borderColor = selected ? '#fde047' : color(context.dataset.label);
            const label = document.createElement('span');
            label.className = 'history-tooltip-label';
            const value = series[context.datasetIndex].values[context.dataIndex];
            label.textContent = `${context.dataset.label}: ${amount(value)} (${share(value, history.totals[context.dataIndex])})`;
            row.append(swatch, label);

            if (!clickedTooltip) {
                const movement = tooltipMovement(context, series);
                if (movement) {
                    const change = document.createElement('span');
                    change.className = 'history-tooltip-movement';
                    const amountChange = document.createElement('span');
                    amountChange.className = `history-tooltip-change ${movementClass(movement.amount)}`;
                    amountChange.textContent = signedAmount(movement.amount);
                    const shareChange = document.createElement('span');
                    shareChange.className = `history-tooltip-change ${movementClass(movement.percentagePoints)}`;
                    shareChange.textContent = signedPercentagePoints(movement.percentagePoints);
                    change.append(document.createTextNode('Change: '), amountChange, document.createTextNode(' · '), shareChange);
                    row.append(change);
                }
            }
            element.append(row);
        });
        if (tooltip.dataPoints.length > 1) {
            const total = document.createElement('div');
            total.className = 'history-tooltip-total';
            total.textContent = `Total: ${amount(history.totals[first.dataIndex])}`;
            element.append(total);
        }
    }

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

    function visibleSeries() {
        const series = history.series.filter(series => series.values.some(value => value > 0));
        if (get('breakdown').value === 'gender' || get('categories').value === 'all' || series.length <= 6) return series;
        const score = series => get('mode').value === 'cumulative'
            ? series.values.at(-1) : series.values.reduce((sum, value) => sum + value, 0);
        const ranked = [...series].sort((a, b) => score(b) - score(a) || a.name.localeCompare(b.name));
        const top = ranked.slice(0, 6).sort((a, b) => a.name.localeCompare(b.name));
        const remaining = history.periods.map((_, i) => ranked.slice(6).reduce((sum, series) => sum + series.values[i], 0));
        return [...top, { name: 'Remaining categories', values: remaining }];
    }

    function renderTable(series) {
        const table = get('table');
        table.replaceChildren();
        const caption = document.createElement('caption');
        caption.textContent = `${get('breakdown').selectedOptions[0].text} by ${get('measure').selectedOptions[0].text.toLowerCase()} - ${get('mode').selectedOptions[0].text.toLowerCase()}`;
        table.append(caption);
        const head = table.createTHead().insertRow();
        for (const text of ['Period ending', 'Total', ...series.map(series => series.name)]) {
            const cell = document.createElement('th');
            cell.scope = 'col';
            cell.textContent = text;
            head.append(cell);
        }
        const body = table.createTBody();
        history.periods.forEach((period, i) => {
            const row = body.insertRow();
            const heading = document.createElement('th');
            heading.scope = 'row';
            heading.textContent = displayDate(period.end);
            row.append(heading);
            row.insertCell().textContent = amount(history.totals[i]);
            series.forEach(series => {
                row.insertCell().textContent = `${amount(series.values[i])} (${share(series.values[i], history.totals[i])})`;
            });
        });
        get('data').hidden = false;
    }

    function render() {
        if (!history) return;
        if (chart) { chart.destroy(); chart = null; }
        clickedTooltip = null;
        hideHistoryTooltip();
        get('legend').replaceChildren();
        get('chart-wrap').hidden = true;
        get('data').hidden = true;
        const cumulative = get('mode').value === 'cumulative';
        get('definition').textContent = '';
        get('categories').disabled = get('breakdown').value === 'gender';
        get('peak').hidden = true;
        get('peak').replaceChildren();
        if (!history.periods.length || !history.totals.some(total => total > 0)) {
            get('status').textContent = 'No data for this selection. Try another measure or history range.';
            return;
        }
        history.series.map(series => series.name).sort((a, b) => a.localeCompare(b)).forEach(color);
        const series = visibleSeries();
        const peakEntries = cumulative ? series.flatMap((series, datasetIndex) => {
            const peak = allTimePeaks.get(series.name);
            if (!peak) return [];
            return [{
                ...peak,
                datasetIndex,
                index: history.periods.findIndex(period => period.end === peak.end),
                name: series.name
            }];
        }) : [];
        const periodLeaders = cumulative ? findPeriodLeaders(history) : [];
        const pointMarkers = series.map((category, datasetIndex) => category.values.map((_, index) => ({
            peak: peakEntries.some(entry => entry.datasetIndex === datasetIndex && entry.index === index),
            leader: periodLeaders[index]?.has(category.name) || false
        })));
        if (peakEntries.length) {
            const peakElement = get('peak');
            peakElement.hidden = false;
            peakEntries.forEach(entry => {
                const item = document.createElement('span');
                item.className = 'history-peak-item';
                const swatch = document.createElement('i');
                swatch.style.backgroundColor = color(entry.name);
                swatch.setAttribute('aria-hidden', 'true');
                const outsideRange = entry.index < 0 ? ' (outside range)' : '';
                item.append(swatch, document.createTextNode(`${entry.name}: ${share(entry.value, entry.total)} · ${displayDate(entry.end)}${outsideRange}`));
                peakElement.append(item);
            });
        }
        renderTable(series);
        get('status').textContent = '';
        series.forEach(series => {
            const item = document.createElement('span');
            const swatch = document.createElement('i');
            swatch.style.backgroundColor = color(series.name);
            swatch.setAttribute('aria-hidden', 'true');
            item.append(swatch, document.createTextNode(series.name));
            get('legend').append(item);
        });
        if (typeof Chart === 'undefined') {
            get('status').textContent = 'The chart library could not load. Exact values are available below; reload the page to retry the chart.';
            return;
        }
        const percent = get('scale').value === 'percent';
        const stacked = get('chart').value === 'area';
        const time = get('measure').value === 'time';
        const rawValues = series.map(series => series.values.map((value, i) => percent
            ? (history.totals[i] > 0 ? 100 * value / history.totals[i] : null)
            : time ? value / 3600 : value));
        const importScale = capInitialImport(rawValues, stacked);
        const chartValues = importScale.values;
        if (importScale.overflow) {
            get('definition').textContent = `Initial import: ${amount(history.totals[importScale.overflow.index])}; scale capped at ${amount(importScale.max)}.`;
        }
        const percentMax = percent && !stacked ? percentageLineMax(chartValues) : undefined;
        const yMax = percent ? (stacked ? 100 : percentMax) : importScale.max;
        const textColor = getComputedStyle(document.body).getPropertyValue('--text').trim() || '#ddd';
        const labels = history.periods.map(period => {
            const [year, month, day] = period.start.split('-');
            return get('interval').value === 'week' ? `${month}/${day}/${year}`
                : get('interval').value === 'year' ? year
                : get('interval').value === 'quarter' ? `Q${Math.ceil(Number(month) / 3)} ${year}` : `${month}/${year}`;
        });
        get('chart-wrap').hidden = false;
        Chart.Interaction.modes.historySection = sectionInteraction;
        get('canvas').setAttribute('aria-label', `${get('breakdown').selectedOptions[0].text} breakdown by ${get('measure').selectedOptions[0].text}, ${get('mode').selectedOptions[0].text}, ${get('scale').selectedOptions[0].text}.${importScale.overflow ? ' Initial import exceeds the visible scale.' : ''} Exact values follow below.`);
        chart = new Chart(get('canvas'), {
            type: stacked && history.periods.length === 1 ? 'bar' : 'line',
            plugins: [{
                id: 'history-peaks-and-overflow',
                afterDatasetsDraw(chart) {
                    const ctx = chart.ctx;
                    ctx.save();
                    const visiblePeaks = peakEntries.filter(entry => entry.index >= 0);
                    if (visiblePeaks.length && visiblePeaks.length <= 3) {
                        visiblePeaks.forEach(entry => {
                            const point = chart.getDatasetMeta(entry.datasetIndex).data[entry.index];
                            if (!point) return;
                            ctx.strokeStyle = color(entry.name);
                            ctx.globalAlpha = 0.7;
                            ctx.lineWidth = 1;
                            ctx.setLineDash([4, 4]);
                            ctx.beginPath();
                            ctx.moveTo(point.x, chart.chartArea.top);
                            ctx.lineTo(point.x, chart.chartArea.bottom);
                            ctx.stroke();
                        });
                        ctx.globalAlpha = 1;
                        ctx.setLineDash([]);
                    }
                    if (importScale.overflow) {
                        const x = chart.scales.x.getPixelForValue(importScale.overflow.index);
                        const y = chart.chartArea.top;
                        ctx.strokeStyle = '#f59e0b';
                        ctx.lineWidth = 1;
                        ctx.setLineDash([3, 3]);
                        ctx.beginPath();
                        ctx.moveTo(x, y);
                        ctx.lineTo(x, chart.chartArea.bottom);
                        ctx.stroke();
                        ctx.setLineDash([]);
                        const markerX = Math.max(chart.chartArea.left + 10, Math.min(chart.chartArea.right - 10, x));
                        if (importScale.overflow.type === 'stacked') {
                            drawOverflowMarker(ctx, markerX, y, '#f59e0b');
                        } else {
                            if (importScale.overflow.datasets.length === 0) drawOverflowMarker(ctx, markerX, y, '#f59e0b');
                            importScale.overflow.datasets.forEach((datasetIndex, position) => {
                                drawOverflowMarker(ctx, markerX, y, color(series[datasetIndex].name), (position - (importScale.overflow.datasets.length - 1) / 2) * 12);
                            });
                        }
                    }
                    ctx.restore();
                }
            }],
            data: {
                labels,
                datasets: series.map((series, datasetIndex) => ({
                    label: series.name,
                    data: chartValues[datasetIndex],
                    borderColor: color(series.name),
                    backgroundColor: color(series.name),
                    fill: stacked ? 'stack' : false,
                    borderWidth: stacked ? 1 : 2,
                    pointRadius: series.values.map((_, i) => pointMarkers[datasetIndex][i].peak ? 7
                        : pointMarkers[datasetIndex][i].leader ? 3 : history.periods.length < 3 ? 4 : 0),
                    pointStyle: series.values.map((_, i) => pointMarkers[datasetIndex][i].peak ? 'rectRot' : 'circle'),
                    pointBackgroundColor: series.values.map((_, i) => pointMarkers[datasetIndex][i].leader
                        && !pointMarkers[datasetIndex][i].peak ? leaderMarkerColor(series.name) : color(series.name)),
                    pointBorderColor: series.values.map((_, i) => pointMarkers[datasetIndex][i].peak ? '#fff' : color(series.name)),
                    pointBorderWidth: series.values.map((_, i) => pointMarkers[datasetIndex][i].leader
                        && !pointMarkers[datasetIndex][i].peak ? 1 : 2),
                    pointHitRadius: 10,
                    tension: 0,
                    spanGaps: false
                }))
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                interaction: { mode: 'historySection', intersect: false },
                onHover: event => {
                    if (event.type !== 'click') clickedTooltip = null;
                },
                onClick: expandTooltip,
                scales: {
                    x: { stacked, ticks: { color: textColor, maxTicksLimit: 14, maxRotation: 0 }, grid: { display: false },
                        title: { display: false } },
                    y: { stacked, min: 0, max: yMax,
                        ticks: { color: textColor, stepSize: percent ? 5 : undefined,
                            maxTicksLimit: percent ? Math.floor(yMax / 5) + 1 : 11, autoSkip: !percent,
                            callback: value => percent ? `${value}%` : number(value) },
                        grid: { color: '#88888825' },
                        title: { display: true, color: textColor, text: percent ? 'Share (%)'
                            : time ? 'Play time (hours)' : get('measure').selectedOptions[0].text } }
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        enabled: false,
                        itemSort: (first, second) => {
                            const firstValue = series[first.datasetIndex].values[first.dataIndex];
                            const secondValue = series[second.datasetIndex].values[second.dataIndex];
                            return secondValue - firstValue || first.dataset.label.localeCompare(second.dataset.label);
                        },
                        external: context => renderExternalTooltip(context, series, cumulative),
                        callbacks: {
                        title: items => {
                            const period = history.periods[items[0].dataIndex];
                            return cumulative ? `Through ${displayDate(period.end)}`
                                : `${displayDate(period.start)} - ${displayDate(period.end)}`;
                        },
                        label: context => {
                            const value = series[context.datasetIndex].values[context.dataIndex];
                            return `${context.dataset.label}: ${amount(value)} (${share(value, history.totals[context.dataIndex])})`;
                        },
                        labelColor: context => {
                            const selected = clickedTooltip && context.datasetIndex === clickedTooltip.datasetIndex
                                && context.dataIndex === clickedTooltip.index;
                            return { backgroundColor: color(context.dataset.label), borderColor: selected ? '#fde047' : color(context.dataset.label),
                                borderWidth: selected ? 2 : 0, borderRadius: 2 };
                        },
                        labelTextColor: context => clickedTooltip && context.datasetIndex === clickedTooltip.datasetIndex
                            && context.dataIndex === clickedTooltip.index ? '#fde047' : textColor,
                        footer: items => items.length > 1 ? `Total: ${amount(history.totals[items[0].dataIndex])}` : ''
                    } }
                }
            }
        });
    }

    async function load() {
        if (request) request.abort();
        const current = new AbortController();
        request = current;
        history = null;
        allTimePeaks = new Map();
        clickedTooltip = null;
        hideHistoryTooltip();
        if (chart) { chart.destroy(); chart = null; }
        get('chart-wrap').hidden = true;
        get('data').hidden = true;
        get('peak').hidden = true;
        get('legend').replaceChildren();
        get('retry').hidden = true;
        get('status').textContent = 'Loading history...';
        const params = new URLSearchParams();
        const trackAllTimePeaks = get('mode').value === 'cumulative';
        ['breakdown', 'measure', 'mode', 'interval', 'year'].forEach(name => {
            // Keep all-time peaks available even when viewing a single year.
            if (name === 'year' && trackAllTimePeaks) return;
            if (get(name).value) params.set(name, get(name).value);
        });
        try {
            const response = await fetch(`/api/playground/breakdown-history?${params}`, { signal: current.signal });
            if (!response.ok) throw new Error(response.status === 400
                ? 'Choose a year or a larger interval and try again.' : 'Could not load breakdown history. Please try again.');
            const result = await response.json();
            if (current !== request) return;
            history = result;
            if (trackAllTimePeaks) {
                allTimePeaks = findAllTimePeaks(result);
                if (get('year').value) {
                    const indices = result.periods.flatMap((period, i) => Number(period.start.slice(0, 4)) === Number(get('year').value) ? [i] : []);
                    history = {
                        periods: indices.map(i => result.periods[i]),
                        totals: indices.map(i => result.totals[i]),
                        series: result.series.map(series => ({ name: series.name, values: indices.map(i => series.values[i]) }))
                    };
                }
            }
            render();
        } catch (error) {
            if (current !== request || error.name === 'AbortError') return;
            get('status').textContent = error.message;
            get('retry').hidden = false;
        }
    }

    controls.forEach(name => get(name).addEventListener('change', () => {
        if (name === 'year') updateWeeklyIntervalAvailability();
        if (['chart', 'scale', 'categories'].includes(name)) render();
        else load();
    }));
    get('retry').addEventListener('click', load);
    updateWeeklyIntervalAvailability();
    load();
})();
