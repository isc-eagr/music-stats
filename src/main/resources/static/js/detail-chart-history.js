/* Detail pages Chart History tab (fragments/detail-chart-history.html):
   chart tiles switch sections, Songs / Albums switches, run bars loaded lazily from each
   card's run endpoint (click the bars for the run modal, both drawn by chart-run.js), and the Seasonal + Yearly calendar. */
(function () {
    'use strict';

    var MAX_PARALLEL = 4;
    var runCache = new Map();
    var queue = [];
    var running = 0;
    var observer = null;

    function init() {
        document.querySelectorAll('.chh').forEach(function (root) {
            bindTiles(root);
            bindSwitches(root);
            bindCalendar(root);
            restoreState(root);
            bindRuns(root);
        });
    }

    // State kept in the URL (detail-state.js) so a reload or an Include toggle comes back here:
    // chart = tile key, chartView = songs / albums, chartPeriod = calendar cell id
    function record(name, value, defaultValue) {
        if (window.DetailState) {
            window.DetailState.setParam(name, value, defaultValue);
        }
    }

    function restoreState(root) {
        if (!window.DetailState) {
            return;
        }
        var chart = window.DetailState.getParam('chart');
        var tile = chart ? root.querySelector('.chh-tile[data-chh-key="' + cssEscape(chart) + '"]') : null;
        if (tile) {
            activateTile(root, tile);
        }
        var period = window.DetailState.getParam('chartPeriod');
        var cell = period ? root.querySelector('[data-chh-cell="' + cssEscape(period) + '"]') : null;
        var view = window.DetailState.getParam('chartView');
        var section = visibleSection(root);
        if (cell && section && section.contains(cell)) {
            view = cell.closest('[data-chh-view]').dataset.chhView;
        }
        if (view && section) {
            var btn = section.querySelector('[data-chh-view-btn="' + cssEscape(view) + '"]');
            if (btn) {
                activateView(section, btn);
            }
        }
        if (cell) {
            selectCell(cell.closest('.chh-cal'), cell);
        }
    }

    function visibleSection(root) {
        var sections = root.querySelectorAll('[data-chh-section]');
        for (var i = 0; i < sections.length; i++) {
            if (!sections[i].hidden) return sections[i];
        }
        return null;
    }

    function cssEscape(value) {
        return window.CSS && CSS.escape ? CSS.escape(value) : String(value).replace(/["\\]/g, '\\$&');
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // ── Tiles: show the section the tile targets ──
    function bindTiles(root) {
        var tiles = root.querySelectorAll('.chh-tile');
        var firstKey = tiles.length ? tiles[0].dataset.chhKey : null;
        tiles.forEach(function (tile) {
            tile.addEventListener('click', function () {
                activateTile(root, tile);
                record('chart', tile.dataset.chhKey, firstKey);
                record('chartView', null);
            });
        });
    }

    function activateTile(root, tile) {
        root.querySelectorAll('.chh-tile').forEach(function (t) {
            var on = t === tile;
            t.classList.toggle('is-active', on);
            t.setAttribute('aria-pressed', on ? 'true' : 'false');
        });
        var target = tile.dataset.chhTarget;
        root.querySelectorAll('[data-chh-section]').forEach(function (section) {
            section.hidden = section.dataset.chhSection !== target;
        });
    }

    // ── Songs / Albums switch inside a section ──
    function bindSwitches(root) {
        root.querySelectorAll('.chh-switch').forEach(function (group) {
            var section = group.closest('[data-chh-section]');
            group.querySelectorAll('[data-chh-view-btn]').forEach(function (btn) {
                btn.addEventListener('click', function () {
                    activateView(section, btn);
                    record('chartView', btn.dataset.chhViewBtn);
                });
            });
        });
    }

    function activateView(section, btn) {
        var view = btn.dataset.chhViewBtn;
        section.querySelectorAll('[data-chh-view-btn]').forEach(function (b) {
            var on = b === btn;
            b.classList.toggle('is-active', on);
            b.setAttribute('aria-pressed', on ? 'true' : 'false');
        });
        section.querySelectorAll('[data-chh-view]').forEach(function (panel) {
            panel.hidden = panel.dataset.chhView !== view;
        });
        var sub = section.querySelector('.chh-title-sub');
        var text = sub && sub.getAttribute('data-chh-sub-' + view);
        if (text) {
            sub.textContent = text;
        }
    }

    // ── Calendar: a cell shows its period's entries ──
    function bindCalendar(root) {
        root.querySelectorAll('.chh-cal').forEach(function (grid) {
            grid.querySelectorAll('[data-chh-cell]').forEach(function (cell) {
                cell.addEventListener('click', function () {
                    selectCell(grid, cell);
                    record('chartPeriod', cell.dataset.chhCell);
                });
            });
        });
    }

    function selectCell(grid, cell) {
        var id = cell.dataset.chhCell;
        grid.querySelectorAll('[data-chh-cell]').forEach(function (c) {
            var on = c === cell;
            c.classList.toggle('is-selected', on);
            c.setAttribute('aria-pressed', on ? 'true' : 'false');
        });
        grid.querySelectorAll('[data-chh-period]').forEach(function (panel) {
            panel.hidden = panel.dataset.chhPeriod !== id;
        });
    }

    // ── Run bars (drawn by chart-run.js), loaded as the cards scroll into view ──
    function bindRuns(root) {
        var cards = root.querySelectorAll('.chh-hit[data-run-url]');
        if (!cards.length) {
            return;
        }
        if ('IntersectionObserver' in window) {
            if (!observer) {
                observer = new IntersectionObserver(function (entries) {
                    entries.forEach(function (entry) {
                        if (entry.isIntersecting) {
                            observer.unobserve(entry.target);
                            loadBars(entry.target);
                        }
                    });
                }, { rootMargin: '200px 0px' });
            }
            cards.forEach(function (card) { observer.observe(card); });
        } else {
            cards.forEach(loadBars);
        }
        cards.forEach(function (card) {
            var button = card.querySelector('.chh-run');
            if (button) {
                button.addEventListener('click', function () { openRunModal(root, card); });
            }
        });
    }

    function loadBars(card) {
        if (card.dataset.runState) {
            return;
        }
        card.dataset.runState = 'loading';
        var bars = card.querySelector('.chh-run-bars');
        if (bars) {
            bars.classList.add('is-loading');
        }
        fetchRun(card.dataset.runUrl).then(function (data) {
            card.dataset.runState = 'loaded';
            if (bars) {
                bars.classList.remove('is-loading');
                var weeks = (data && data.weeks) || [];
                window.ChartRun.renderBars(bars, window.ChartRun.trim(weeks), window.ChartRun.scaleOf(baseScale(card), weeks), card.dataset.unit, false);
            }
        }).catch(function () {
            card.dataset.runState = 'error';
            if (bars) {
                bars.classList.remove('is-loading');
            }
        });
    }

    function baseScale(card) {
        return parseInt(card.dataset.scale, 10) || 20;
    }

    function fetchRun(url) {
        if (runCache.has(url)) {
            return runCache.get(url);
        }
        var promise = new Promise(function (resolve, reject) {
            queue.push({ url: url, resolve: resolve, reject: reject });
            pump();
        });
        runCache.set(url, promise);
        promise.catch(function () { runCache.delete(url); });
        return promise;
    }

    function pump() {
        while (running < MAX_PARALLEL && queue.length) {
            var job = queue.shift();
            running++;
            fetch(job.url)
                .then(function (r) {
                    if (!r.ok) {
                        throw new Error('HTTP ' + r.status);
                    }
                    return r.json();
                })
                .then(job.resolve, job.reject)
                .finally(function () {
                    running--;
                    pump();
                });
        }
    }

    function openRunModal(root, card) {
        var url = card.dataset.runUrl;
        var dates = card.querySelector('.chh-hit-dates');
        window.ChartRun.openModal({
            host: root,
            head: card.querySelector('.chh-hit-head').cloneNode(true),
            id: url,
            load: function () {
                return fetchRun(url).then(function (data) {
                    return { weeks: (data && data.weeks) || [], scale: baseScale(card) };
                });
            },
            unit: card.dataset.unit,
            periodHref: card.dataset.periodHref,
            foot: dates ? Array.prototype.map.call(dates.children, function (d) { return d.cloneNode(true); }) : []
        });
    }
})();
