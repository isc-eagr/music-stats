(function () {
    var runCache = new Map();

    var runTableConfig = {
        weekly: {
            noTrack: { expand: 0, cover: 1, title: 2, span: 3, peak: 4, atPeak: 5, release: 6, debut: 7, peakDate: 8 },
            track: { expand: 0, cover: 1, track: 2, title: 3, span: 4, peak: 5, atPeak: 6, release: 7, debut: 8, peakDate: 9 }
        },
        recap: {
            noTrack: { expand: 0, cover: 1, title: 2, span: 3, peak: 4, atPeak: 5, debut: 6, peakDate: 7 },
            track: { expand: 0, cover: 1, track: 2, title: 3, span: 4, peak: 5, atPeak: 6, debut: 7, peakDate: 8 }
        }
    };

    function initDetailChartHistory() {
        enhanceRunSections();
        enhancePeriodSections();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initDetailChartHistory);
    } else {
        initDetailChartHistory();
    }

    function enhanceRunSections() {
        var sectionModes = {
            'ch-weekly-section': 'weekly',
            'ch-bb-section': 'bb',
            'ch-pc-section': 'pc',
            'ch-trl-section': 'trl'
        };

        Object.keys(sectionModes).forEach(function (sectionId) {
            var section = document.getElementById(sectionId);
            if (!section) {
                return;
            }

            var mode = sectionModes[sectionId];
            section.querySelectorAll('.table-wrapper > table').forEach(function (table) {
                if (table.dataset.detailChartHistoryEnhanced === '1') {
                    return;
                }
                enhanceRunTable(table, mode);
            });
        });
    }

    function enhancePeriodSections() {
        ['ch-seasonal-section', 'ch-yearly-section'].forEach(function (sectionId) {
            var section = document.getElementById(sectionId);
            if (!section) {
                return;
            }

            section.querySelectorAll('.table-wrapper > table').forEach(function (table) {
                if (table.dataset.detailChartHistoryEnhanced === '1') {
                    return;
                }
                enhancePeriodTable(table);
            });
        });
    }

    function enhanceRunTable(table, mode) {
        table.dataset.detailChartHistoryEnhanced = '1';

        var headerTexts = getHeaderTexts(table);
        var hasTrack = headerTexts.indexOf('#') !== -1;
        var groupKey = mode === 'weekly' ? 'weekly' : 'recap';
        var config = runTableConfig[groupKey][hasTrack ? 'track' : 'noTrack'];
        var isWeekly = mode === 'weekly';

        var wrapper = table.closest('.table-wrapper');
        if (wrapper) {
            wrapper.classList.add(isWeekly ? 'charts-table-wrapper' : 'recap-table-wrap');
        }
        table.classList.add(isWeekly ? 'charts-table' : 'recap-table', 'mobile-compact-table');

        var headRow = table.tHead && table.tHead.rows && table.tHead.rows.length ? table.tHead.rows[0] : null;
        if (headRow) {
            insertMobileHeaderCell(headRow);
            styleRunHeaders(headRow, config);
        }

        var tbody = table.tBodies && table.tBodies.length ? table.tBodies[0] : null;
        if (!tbody) {
            return;
        }

        Array.from(tbody.rows).forEach(function (row) {
            if (isHelperRow(row)) {
                return;
            }

            var info = extractRunRowInfo(row, config, mode);
            row._detailChartHistory = info;
            styleRunRow(row, config, mode, info);
        });
    }

    function enhancePeriodTable(table) {
        table.dataset.detailChartHistoryEnhanced = '1';

        var wrapper = table.closest('.table-wrapper');
        var headRow = table.tHead && table.tHead.rows && table.tHead.rows.length ? table.tHead.rows[0] : null;
        var tbody = table.tBodies && table.tBodies.length ? table.tBodies[0] : null;
        if (!headRow || !tbody) {
            return;
        }

        var hasTitle = headRow.cells.length > 2;
        if (wrapper) {
            wrapper.classList.add(
                'seasonal-table-container',
                'detail-period-history-wrapper',
                hasTitle ? 'detail-period-history-wrapper--list' : 'detail-period-history-wrapper--single'
            );
        }
        table.classList.add('seasonal-table', 'detail-period-history-table');

        var order = hasTitle ? [2, 0, 1] : [1, 0];
        reorderCells(headRow, order);
        Array.from(tbody.rows).forEach(function (row) {
            reorderCells(row, order);
        });

        table.classList.add(hasTitle ? 'detail-period-history-table--list' : 'detail-period-history-table--single');

        if (hasTitle) {
            headRow.cells[0].classList.add('st-pos');
            headRow.cells[1].classList.add('st-title');
            headRow.cells[2].classList.add('st-period');
        } else {
            headRow.cells[0].classList.add('st-pos');
            headRow.cells[1].classList.add('st-period');
        }

        Array.from(tbody.rows).forEach(function (row) {
            if (row.cells.length < 2) {
                return;
            }

            var posCell = row.cells[0];
            posCell.classList.add('st-pos');
            stripHashFromCell(posCell);

            if (hasTitle) {
                row.cells[1].classList.add('st-title');
                decoratePeriodCell(row.cells[2]);
            } else {
                decoratePeriodCell(row.cells[1]);
            }
        });
    }

    function insertMobileHeaderCell(headRow) {
        if (headRow.querySelector('.mobile-expand-col')) {
            return;
        }
        var firstCell = headRow.cells[0];
        if (!firstCell) {
            return;
        }
        var mobileHeadCell = document.createElement('th');
        mobileHeadCell.className = 'mobile-expand-col show-mobile-only';
        mobileHeadCell.setAttribute('data-no-sort', '');
        headRow.insertBefore(mobileHeadCell, firstCell);
    }

    function styleRunHeaders(headRow, config) {
        var headers = Array.from(headRow.cells);
        headers[0].classList.add('mobile-expand-col', 'show-mobile-only');

        addClasses(headers[config.expand + 1], 'col-expand', 'hide-mobile');
        addClasses(headers[config.cover + 1], 'cover-col');
        headers[config.cover + 1].textContent = '';
        addClasses(headers[config.title + 1], 'col-title', 'mobile-name-col');
        addClasses(headers[config.span + 1], 'col-span');
        addClasses(headers[config.peak + 1], 'col-peak');
        addClasses(headers[config.atPeak + 1], 'col-at-peak', 'hide-mobile');

        if (typeof config.track === 'number') {
            addClasses(headers[config.track + 1], 'hide-mobile');
        }
        if (typeof config.release === 'number') {
            addClasses(headers[config.release + 1], 'hide-mobile');
        }
        addClasses(headers[config.debut + 1], 'hide-mobile');
        addClasses(headers[config.peakDate + 1], 'hide-mobile');
    }

    function styleRunRow(row, config, mode, info) {
        var cells = Array.from(row.cells);
        var expandCell = cells[config.expand];
        if (!expandCell) {
            return;
        }

        row.classList.add('detail-chart-history-row');
        if (!row.classList.contains('gender-male') && !row.classList.contains('gender-female') && !row.classList.contains('gender-other')) {
            row.classList.add(info.toneClass);
        }

        var mobileCell = document.createElement('td');
        mobileCell.className = 'mobile-expand-col show-mobile-only';
        var mobileButton = createButton('mobile-row-expand-btn');
        mobileButton.setAttribute('aria-label', 'Show chart details');
        mobileButton.onclick = function () {
            toggleMobileDetails(row, mobileButton);
        };
        mobileCell.appendChild(mobileButton);
        row.insertBefore(mobileCell, expandCell);

        addClasses(expandCell, 'col-expand', 'hide-mobile');
        var desktopButton = ensureButton(expandCell, mode === 'weekly' ? 'expand-btn' : 'recap-expand-btn');
        desktopButton.setAttribute('aria-label', 'Show chart details');
        desktopButton.onclick = function () {
            toggleDesktopDetails(row, desktopButton);
        };

        addClasses(cells[config.cover], 'cover-col');
        addClasses(cells[config.title], 'col-title', 'mobile-name-col');
        addClasses(cells[config.span], 'col-span');
        addClasses(cells[config.peak], 'col-peak');
        addClasses(cells[config.atPeak], 'col-at-peak', 'hide-mobile');

        if (typeof config.track === 'number') {
            addClasses(cells[config.track], 'hide-mobile');
        }
        if (typeof config.release === 'number') {
            addClasses(cells[config.release], 'hide-mobile');
        }
        addClasses(cells[config.debut], 'hide-mobile');
        addClasses(cells[config.peakDate], 'hide-mobile');

        stylePeakCell(cells[config.peak]);
        styleAtPeakCell(cells[config.atPeak]);

        if (!info.hasMobileToggle) {
            mobileCell.innerHTML = '';
        }

        if (!info.hasDesktopToggle) {
            expandCell.innerHTML = '';
        }
    }

    function extractRunRowInfo(row, config, mode) {
        var cells = Array.from(row.cells);
        var fields = [];
        var trackText = typeof config.track === 'number' ? readCellText(cells[config.track]) : '';

        if (trackText && trackText !== '-') {
            fields.push({ label: 'Track', text: trackText, html: escapeHtml(trackText) });
        }

        fields.push(createAtPeakField(mode, cells[config.atPeak]));

        if (mode === 'weekly' && typeof config.release === 'number') {
            fields.push(createCellField('Release', cells[config.release]));
        }

        fields.push(createCellField('Debut', cells[config.debut]));
        fields.push(createCellField('Peak Date', cells[config.peakDate]));

        fields = fields.filter(Boolean);

        return {
            mode: mode,
            runUrl: row.dataset.runUrl || '',
            toneClass: getToneClass(row),
            fields: fields,
            hasDesktopToggle: !!row.dataset.runUrl,
            hasMobileToggle: fields.length > 0 || !!row.dataset.runUrl,
            runData: null,
            runError: null,
            runPromise: null
        };
    }

    function createAtPeakField(mode, cell) {
        if (!cell) {
            return null;
        }
        var label = mode === 'weekly' || mode === 'bb' ? 'Weeks @ Peak' : 'Days @ Peak';
        var value = formatAtPeakText(readCellText(cell));
        return {
            label: label,
            text: value,
            html: escapeHtml(value)
        };
    }

    function createCellField(label, cell) {
        if (!cell) {
            return null;
        }
        var text = readCellText(cell) || '-';
        return {
            label: label,
            text: text,
            html: (cell.innerHTML || '').trim() || escapeHtml(text)
        };
    }

    function toggleDesktopDetails(row, button) {
        var info = row._detailChartHistory;
        if (!info || !info.hasDesktopToggle) {
            return;
        }

        var detailRow = ensureDesktopDetailRow(row, info.mode);
        var isVisible = detailRow.classList.contains('visible');
        detailRow.classList.toggle('visible', !isVisible);
        setExpandedState(button, !isVisible);

        if (isVisible) {
            return;
        }

        renderDesktopDetailRow(detailRow, info);
        if (!info.runUrl || info.runData || info.runError || info.runPromise) {
            return;
        }

        ensureRunData(info)
            .then(function () {
                refreshVisibleDetails(row);
            })
            .catch(function () {
                refreshVisibleDetails(row);
            });
    }

    function toggleMobileDetails(row, button) {
        var info = row._detailChartHistory;
        if (!info || !info.hasMobileToggle) {
            return;
        }

        var detailRow = ensureMobileDetailRow(row);
        var isVisible = detailRow.classList.contains('visible');
        detailRow.classList.toggle('visible', !isVisible);
        setExpandedState(button, !isVisible);

        if (isVisible) {
            return;
        }

        renderMobileDetailRow(detailRow, info);
        if (!info.runUrl || info.runData || info.runError || info.runPromise) {
            return;
        }

        ensureRunData(info)
            .then(function () {
                refreshVisibleDetails(row);
            })
            .catch(function () {
                refreshVisibleDetails(row);
            });
    }

    function ensureDesktopDetailRow(row, mode) {
        var existing = getDesktopDetailRow(row, mode);
        if (existing) {
            return existing;
        }

        var detailRow = document.createElement('tr');
        detailRow.className = mode === 'weekly' ? 'chart-run-row' : 'recap-detail-row';

        var detailCell = document.createElement('td');
        detailCell.className = mode === 'weekly' ? 'chart-run-cell' : 'recap-detail-cell';
        detailCell.colSpan = row.cells.length;
        detailRow.appendChild(detailCell);

        var insertAfter = getMobileDetailRow(row) || row;
        insertAfter.insertAdjacentElement('afterend', detailRow);
        return detailRow;
    }

    function getDesktopDetailRow(row, mode) {
        var next = row.nextElementSibling;
        if (next && next.classList.contains('mobile-table-detail-row')) {
            next = next.nextElementSibling;
        }
        if (next && next.classList.contains(mode === 'weekly' ? 'chart-run-row' : 'recap-detail-row')) {
            return next;
        }
        return null;
    }

    function ensureMobileDetailRow(row) {
        var existing = getMobileDetailRow(row);
        if (existing) {
            if (existing.cells[0]) {
                existing.cells[0].colSpan = getVisibleCellCount(row);
            }
            return existing;
        }

        var detailRow = document.createElement('tr');
        detailRow.className = 'mobile-table-detail-row';

        var detailCell = document.createElement('td');
        detailCell.className = 'mobile-table-detail-cell';
        detailCell.colSpan = getVisibleCellCount(row);
        detailRow.appendChild(detailCell);

        var desktopRow = getDesktopDetailRow(row, row._detailChartHistory ? row._detailChartHistory.mode : 'weekly');
        if (desktopRow) {
            row.parentNode.insertBefore(detailRow, desktopRow);
        } else {
            row.insertAdjacentElement('afterend', detailRow);
        }
        return detailRow;
    }

    function getMobileDetailRow(row) {
        var next = row.nextElementSibling;
        if (next && next.classList.contains('mobile-table-detail-row')) {
            return next;
        }
        return null;
    }

    function getVisibleCellCount(row) {
        var visibleCells = Array.from(row.cells).filter(function (cell) {
            return window.getComputedStyle(cell).display !== 'none';
        }).length;
        return visibleCells || row.cells.length;
    }

    function renderDesktopDetailRow(detailRow, info) {
        var detailCell = detailRow.cells[0];
        if (!detailCell) {
            return;
        }
        detailCell.innerHTML = buildDesktopDetailHtml(info);
    }

    function renderMobileDetailRow(detailRow, info) {
        var detailCell = detailRow.cells[0];
        if (!detailCell) {
            return;
        }
        detailCell.innerHTML = buildMobileDetailHtml(info);
    }

    function buildDesktopDetailHtml(info) {
        var toneClass = getDetailToneClass(info.mode);
        if (info.mode === 'weekly') {
            var weeklyParts = [];
            if (info.runError) {
                weeklyParts.push('<div class="chart-run-error">Failed to load chart run.</div>');
            } else if (info.runData) {
                weeklyParts.push(renderWeeklyRun(info.runData, info.toneClass));
            } else if (info.runUrl) {
                weeklyParts.push('<div class="chart-run-wrap"><div class="chart-run-label">Chart-run</div><div class="chart-run-loading">Loading chart run...</div></div>');
            }
            return '<div class="' + toneClass + ' chart-run-content">' + weeklyParts.join('') + '</div>';
        }

        var recapParts = [];
        if (info.runError) {
            recapParts.push('<div class="recap-run-error">Failed to load chart run.</div>');
        } else if (info.runData) {
            recapParts.push(renderRecapRun(info.runData, info.toneClass, info.mode));
        } else if (info.runUrl) {
            recapParts.push('<div class="recap-run-loading">Loading chart run...</div>');
        }
        return '<div class="' + toneClass + ' recap-detail-content">' + recapParts.join('') + '</div>';
    }

    function buildMobileDetailHtml(info) {
        var mobileFields = info.fields.filter(function (field) {
            return field.label !== 'Debut' && field.label !== 'Peak Date';
        });
        var toneClass = getDetailToneClass(info.mode);

        var fieldsHtml = mobileFields.map(function (field) {
            return '<div class="mobile-table-detail-item">'
                + '<span class="mobile-table-detail-label">' + escapeHtml(field.label) + '</span>'
                + '<span class="mobile-table-detail-value">' + field.html + '</span>'
                + '</div>';
        }).join('');

        var runHtml = '';
        if (info.runError) {
            runHtml = info.mode === 'weekly'
                ? '<div class="chart-run-error">Failed to load chart run.</div>'
                : '<div class="recap-run-error">Failed to load chart run.</div>';
        } else if (info.runData) {
            runHtml = info.mode === 'weekly'
                ? renderWeeklyRun(info.runData, info.toneClass)
                : renderRecapRun(info.runData, info.toneClass, info.mode);
        } else if (info.runUrl) {
            runHtml = info.mode === 'weekly'
                ? '<div class="chart-run-loading">Loading chart run...</div>'
                : '<div class="recap-run-loading">Loading chart run...</div>';
        }

        return '<div class="' + toneClass + '">' 
            + '<div class="mobile-table-detail-grid">' + fieldsHtml + '</div>'
            + (runHtml ? '<div style="margin-top:12px;">' + runHtml + '</div>' : '')
            + '</div>';
    }

    function renderMetaGrid(fields) {
        if (!fields || !fields.length) {
            return '';
        }

        return '<div class="detail-chart-history-meta-grid">' + fields.map(function (field) {
            return '<div class="detail-chart-history-meta-card">'
                + '<div class="detail-chart-history-meta-label">' + escapeHtml(field.label) + '</div>'
                + '<div class="detail-chart-history-meta-value">' + field.html + '</div>'
                + '</div>';
        }).join('') + '</div>';
    }

    function renderWeeklyRun(data, toneClass) {
        var boxesHtml = renderRunBoxes(data && data.weeks ? data.weeks : [], 'weekly', toneClass, 'chart-run-box');
        return '<div class="chart-run-stats">'
            + renderStatCard('Top 1', data && data.weeksAtTop1, toneClass)
            + renderStatCard('Top 5', data && data.weeksAtTop5, toneClass)
            + renderStatCard('Top 10', data && data.weeksAtTop10, toneClass)
            + renderStatCard('Top 20', data && data.weeksAtTop20, toneClass)
            + '</div>'
            + '<div class="chart-run-wrap">'
            + '<div class="chart-run-label">Chart-run</div>'
            + '<div class="chart-run-boxes">' + boxesHtml + '</div>'
            + '</div>';
    }

    function renderRecapRun(data, toneClass, mode) {
        var boxesHtml = renderRunBoxes(data && data.weeks ? data.weeks : [], mode, toneClass, 'recap-run-box');
        return '<div class="recap-run-stats">'
            + renderRecapStatCard('Top 1', data && data.weeksAtTop1, toneClass)
            + renderRecapStatCard('Top 5', data && data.weeksAtTop5, toneClass)
            + renderRecapStatCard('Top 10', data && data.weeksAtTop10, toneClass)
            + '</div>'
            + '<div class="recap-run-label">Chart-run</div>'
            + '<div class="recap-run-boxes">' + boxesHtml + '</div>';
    }

    function getDetailToneClass(mode) {
        return 'detail-chart-tone detail-chart-tone-' + mode;
    }

    function renderStatCard(label, value, toneClass) {
        return '<div class="chart-run-stat ' + escapeHtml(toneClass || '') + '">'
            + '<div class="chart-run-stat-label">' + escapeHtml(label) + '</div>'
            + '<div class="chart-run-stat-value">' + escapeHtml(formatStatValue(value)) + '</div>'
            + '</div>';
    }

    function renderRecapStatCard(label, value, toneClass) {
        return '<div class="recap-run-stat ' + escapeHtml(toneClass || '') + '">'
            + '<div class="recap-run-stat-label">' + escapeHtml(label) + '</div>'
            + '<div class="recap-run-stat-value">' + escapeHtml(formatStatValue(value)) + '</div>'
            + '</div>';
    }

    function renderRunBoxes(weeks, mode, toneClass, boxClass) {
        var consolidated = consolidateAbsentEntries(weeks || []);
        return consolidated.map(function (entry) {
            if (!entry.onChart) {
                return '<span class="' + boxClass + ' absent" title="' + escapeHtml(String(entry.count)) + ' off chart">' + escapeHtml(String(entry.count)) + 'x</span>';
            }

            var classes = [boxClass, toneClass];
            if (Number(entry.position) === 1) {
                classes.push('peak-one');
            }

            var href = buildRunHref(mode, entry.periodKey);
            var title = escapeHtml(entry.dateRange || entry.periodKey || '');
            var label = escapeHtml(String(entry.display != null ? entry.display : entry.position));
            if (!href) {
                return '<span class="' + classes.join(' ') + '" title="' + title + '">' + label + '</span>';
            }
            return '<a href="' + href + '" class="' + classes.join(' ') + '" title="' + title + '">' + label + '</a>';
        }).join('');
    }

    function consolidateAbsentEntries(weeks) {
        var trimmed = trimAbsentEntries(weeks || []);
        var result = [];
        var index = 0;

        while (index < trimmed.length) {
            if (trimmed[index].onChart) {
                result.push(trimmed[index]);
                index++;
                continue;
            }

            var count = 0;
            while (index < trimmed.length && !trimmed[index].onChart) {
                count++;
                index++;
            }
            result.push({ onChart: false, count: count });
        }

        return result;
    }

    function trimAbsentEntries(weeks) {
        var start = 0;
        while (start < weeks.length && !weeks[start].onChart) {
            start++;
        }

        var end = weeks.length - 1;
        while (end >= start && !weeks[end].onChart) {
            end--;
        }

        return start > end ? [] : weeks.slice(start, end + 1);
    }

    function buildRunHref(mode, periodKey) {
        if (!periodKey) {
            return null;
        }
        if (mode === 'weekly') {
            return '/charts/weekly/' + encodeURIComponent(periodKey);
        }
        if (mode === 'bb') {
            return '/misc/billboard-hot-100/recaps?date=' + encodeURIComponent(periodKey);
        }
        if (mode === 'pc') {
            return '/misc/vatos-cuntdown/recaps?date=' + encodeURIComponent(periodKey);
        }
        if (mode === 'trl') {
            return '/misc/trl/recaps?date=' + encodeURIComponent(periodKey);
        }
        return null;
    }

    function ensureRunData(info) {
        if (!info.runUrl) {
            return Promise.resolve(null);
        }
        if (info.runData) {
            return Promise.resolve(info.runData);
        }
        if (info.runError) {
            return Promise.reject(info.runError);
        }
        if (info.runPromise) {
            return info.runPromise;
        }

        if (runCache.has(info.runUrl)) {
            info.runPromise = runCache.get(info.runUrl)
                .then(function (data) {
                    info.runData = data;
                    return data;
                })
                .catch(function (error) {
                    info.runError = error;
                    throw error;
                })
                .finally(function () {
                    info.runPromise = null;
                });
            return info.runPromise;
        }

        var fetchPromise = fetch(info.runUrl)
            .then(function (response) {
                if (!response.ok) {
                    throw new Error('Failed to load chart run');
                }
                return response.json();
            })
            .then(function (data) {
                info.runData = data;
                return data;
            })
            .catch(function (error) {
                runCache.delete(info.runUrl);
                info.runError = error;
                throw error;
            });

        runCache.set(info.runUrl, fetchPromise);
        info.runPromise = fetchPromise.finally(function () {
            info.runPromise = null;
        });
        return info.runPromise;
    }

    function refreshVisibleDetails(row) {
        var info = row._detailChartHistory;
        if (!info) {
            return;
        }

        var mobileRow = getMobileDetailRow(row);
        if (mobileRow && mobileRow.classList.contains('visible')) {
            renderMobileDetailRow(mobileRow, info);
        }

        var desktopRow = getDesktopDetailRow(row, info.mode);
        if (desktopRow && desktopRow.classList.contains('visible')) {
            renderDesktopDetailRow(desktopRow, info);
        }
    }

    function addTrackPill(titleCell, trackText) {
        if (!titleCell || !trackText || trackText === '-' || titleCell.querySelector('.chart-history-track-pill')) {
            return;
        }
        var pill = document.createElement('span');
        pill.className = 'chart-history-track-pill show-mobile-only';
        pill.textContent = trackText;
        titleCell.insertBefore(pill, titleCell.firstChild);
    }

    function stylePeakCell(cell) {
        if (!cell) {
            return;
        }

        var peakText = stripLeadingHash(readCellText(cell));
        var peakSpan = cell.querySelector('span');
        if (!peakSpan) {
            peakSpan = document.createElement('span');
            peakSpan.textContent = peakText || '-';
        } else {
            peakSpan.textContent = peakText || '-';
        }

        peakSpan.classList.add('peak-position');

        var wrapper = cell.querySelector('.peak-cell');
        if (!wrapper) {
            wrapper = document.createElement('div');
            wrapper.className = 'peak-cell';
            cell.innerHTML = '';
            wrapper.appendChild(peakSpan);
            cell.appendChild(wrapper);
        } else if (peakSpan.parentElement !== wrapper) {
            wrapper.innerHTML = '';
            wrapper.appendChild(peakSpan);
        }
    }

    function styleAtPeakCell(cell) {
        if (!cell) {
            return;
        }
        var span = cell.querySelector('span');
        var text = formatAtPeakText(readCellText(cell));
        if (span) {
            span.classList.remove('peak-number-one');
            span.textContent = text;
        } else {
            cell.textContent = text;
        }
        cell.style.textAlign = 'center';
    }

    function decoratePeriodCell(cell) {
        if (!cell) {
            return;
        }

        cell.classList.add('st-period');
        var link = cell.querySelector('a');
        if (link) {
            link.classList.add('chart-history-period-pill');
        } else if (!cell.querySelector('.chart-history-period-pill')) {
            var value = readCellText(cell) || '-';
            cell.innerHTML = '<span class="chart-history-period-pill">' + escapeHtml(value) + '</span>';
        }
    }

    function reorderCells(row, order) {
        var cells = Array.from(row.cells);
        if (cells.length !== order.length) {
            return;
        }
        order.forEach(function (index) {
            row.appendChild(cells[index]);
        });
    }

    function ensureButton(cell, className) {
        var button = cell.querySelector('button');
        if (!button) {
            cell.textContent = '';
            button = document.createElement('button');
            cell.appendChild(button);
        }
        button.type = 'button';
        button.className = className;
        button.textContent = '+';
        button.removeAttribute('onclick');
        button.setAttribute('aria-expanded', 'false');
        return button;
    }

    function createButton(className) {
        var button = document.createElement('button');
        button.type = 'button';
        button.className = className;
        button.textContent = '+';
        button.setAttribute('aria-expanded', 'false');
        return button;
    }

    function setExpandedState(button, expanded) {
        button.classList.toggle('expanded', expanded);
        button.textContent = expanded ? '-' : '+';
        button.setAttribute('aria-expanded', String(expanded));
    }

    function getHeaderTexts(table) {
        var headRow = table.tHead && table.tHead.rows && table.tHead.rows.length ? table.tHead.rows[0] : null;
        if (!headRow) {
            return [];
        }
        return Array.from(headRow.cells).map(function (cell) {
            return readCellText(cell);
        });
    }

    function getToneClass(row) {
        if (row.classList.contains('gender-female') || row.dataset.gender === '1') {
            return 'gender-female';
        }
        if (row.classList.contains('gender-male') || row.dataset.gender === '2') {
            return 'gender-male';
        }
        if (document.body.classList.contains('gender-female')) {
            return 'gender-female';
        }
        if (document.body.classList.contains('gender-male')) {
            return 'gender-male';
        }
        return 'gender-other';
    }

    function formatAtPeakText(text) {
        var cleaned = stripLeadingHash(String(text || '').trim());
        if (!cleaned || cleaned === '-') {
            return '-';
        }
        return cleaned.charAt(0).toLowerCase() === 'x' ? cleaned : 'x' + cleaned;
    }

    function stripHashFromCell(cell) {
        if (!cell) {
            return;
        }
        var span = cell.querySelector('span');
        var cleaned = stripLeadingHash(readCellText(cell));
        if (span) {
            span.textContent = cleaned;
        } else {
            cell.textContent = cleaned;
        }
    }

    function stripLeadingHash(value) {
        return String(value || '').replace(/^#\s*/, '').trim() || '-';
    }

    function readCellText(cell) {
        if (!cell) {
            return '';
        }
        return String(cell.textContent || '').replace(/\s+/g, ' ').trim();
    }

    function formatStatValue(value) {
        return value == null ? '0' : String(value);
    }

    function addClasses(node) {
        if (!node) {
            return;
        }
        Array.prototype.slice.call(arguments, 1).forEach(function (className) {
            if (className) {
                node.classList.add(className);
            }
        });
    }

    function isHelperRow(row) {
        return row.classList.contains('chart-run-row')
            || row.classList.contains('recap-detail-row')
            || row.classList.contains('mobile-table-detail-row');
    }

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }
})();