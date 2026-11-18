// Fills the Consistency ledger ranks (fragments/consistency.html) from its data-cons-url.
// The rank query scans every play (~1 s), so it runs after the page renders; one request per URL.
(function () {
    const PERIOD_ORDER = ['years', 'months', 'weeks', 'days'];
    const requests = new Map();

    function fetchRanks(url) {
        if (!requests.has(url)) {
            requests.set(url, fetch(url).then(function (response) {
                if (!response.ok) throw new Error('Consistency ranks unavailable');
                return response.json();
            }).catch(function (error) {
                requests.delete(url);
                throw error;
            }));
        }
        return requests.get(url);
    }

    function fill(ledger, data) {
        const genderHead = ledger.querySelector('[data-cons-gender-head]');
        if (genderHead && data.genderName) genderHead.textContent = data.genderName + ' rank';

        let best = null;
        ledger.querySelectorAll('[data-cons-period]').forEach(function (row) {
            const key = row.getAttribute('data-cons-period');
            const period = data.periods ? data.periods[key] : null;
            const rankCell = row.querySelector('[data-cons-rank]');
            const genderCell = row.querySelector('[data-cons-grank]');
            if (rankCell) {
                rankCell.textContent = period ? '#' + period.rank.toLocaleString('en-US') : '-';
                rankCell.classList.toggle('is-top', !!period && period.rank <= 10);
            }
            if (genderCell) {
                genderCell.textContent = period && period.genderRank != null ? '#' + period.genderRank.toLocaleString('en-US') : '-';
            }
            if (period && (!best || period.rank < best.rank
                    || (period.rank === best.rank && PERIOD_ORDER.indexOf(key) < PERIOD_ORDER.indexOf(best.key)))) {
                best = { key: key, rank: period.rank };
            }
        });

        const badge = ledger.querySelector('[data-cons-badge]');
        if (badge && best) {
            badge.textContent = '#' + best.rank.toLocaleString('en-US') + ' by ' + best.key;
            badge.hidden = false;
        }
    }

    function clear(ledger) {
        ledger.querySelectorAll('[data-cons-rank], [data-cons-grank]').forEach(function (cell) {
            cell.textContent = '-';
        });
    }

    // root: a document or element; ledgers already filled are skipped (a card clone keeps the filled state)
    window.loadConsistencyRanks = function (root, selector) {
        (root || document).querySelectorAll(selector || '.cons[data-cons-url]').forEach(function (ledger) {
            if (ledger.dataset.consLoaded === 'true') return;
            fetchRanks(ledger.getAttribute('data-cons-url'))
                .then(function (data) {
                    fill(ledger, data);
                    ledger.dataset.consLoaded = 'true';
                })
                .catch(function () { clear(ledger); });
        });
    };

    // Detail pages load on render; list card ledgers (.cons-compact) load when their More panel opens (list-utils.js)
    document.addEventListener('DOMContentLoaded', function () {
        window.loadConsistencyRanks(document, '.cons[data-cons-url]:not(.cons-compact)');
    });
})();
