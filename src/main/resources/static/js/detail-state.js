/* Detail pages (artist, album, song): keep the page state in the URL so a reload, an Include toggle or a
   save comes back to the same place.
   - switchTab() records the open tab as ?tab= (General is the default and is left out).
   - Other scripts record their own state with DetailState.setParam (Chart History: chart, chartView, chartPeriod).
   - Forms posting to this page (or below it) carry the query along; the controller redirects back with it.
   - The scroll position comes back when the page reloads itself (toggles, saves, pagination). */
(function () {
    'use strict';

    var SCROLL_KEY = 'detailScroll:' + location.pathname;

    function setParam(name, value, defaultValue) {
        var url = new URL(location.href);
        if (value === null || value === undefined || value === '' || value === defaultValue) {
            url.searchParams.delete(name);
        } else {
            url.searchParams.set(name, value);
        }
        var next = url.pathname + url.search + url.hash;
        if (next !== location.pathname + location.search + location.hash) {
            history.replaceState(history.state, '', next);
        }
    }

    function getParam(name) {
        return new URL(location.href).searchParams.get(name);
    }

    window.DetailState = {
        setTab: function (tab) { setParam('tab', tab, 'general'); },
        setParam: setParam,
        getParam: getParam
    };

    // Saves post to /x/{id} or /x/{id}/...: send the page's query with them
    document.addEventListener('submit', function (event) {
        var form = event.target;
        if (!(form instanceof HTMLFormElement) || (form.method || '').toLowerCase() !== 'post' || !location.search) {
            return;
        }
        var action;
        try {
            action = new URL(form.getAttribute('action') || location.href, location.href);
        } catch (e) {
            return;
        }
        if (action.origin !== location.origin) {
            return;
        }
        if (action.pathname === location.pathname || action.pathname.indexOf(location.pathname + '/') === 0) {
            form.setAttribute('action', action.pathname + location.search);
        }
    }, true);

    // Scroll: remembered when leaving, restored when the page comes back to itself
    window.addEventListener('pagehide', function () {
        try {
            sessionStorage.setItem(SCROLL_KEY, String(Math.round(window.scrollY)));
        } catch (e) { /* storage unavailable */ }
    });

    function cameFromSamePage() {
        var nav = performance.getEntriesByType && performance.getEntriesByType('navigation')[0];
        if (nav && nav.type === 'reload') {
            return true;
        }
        try {
            return !!document.referrer && new URL(document.referrer).pathname === location.pathname;
        } catch (e) {
            return false;
        }
    }

    window.addEventListener('load', function () {
        var y = NaN;
        try {
            y = parseInt(sessionStorage.getItem(SCROLL_KEY), 10);
        } catch (e) { /* storage unavailable */ }
        if (y > 0 && cameFromSamePage()) {
            requestAnimationFrame(function () { window.scrollTo(0, y); });
        }
    });
})();
