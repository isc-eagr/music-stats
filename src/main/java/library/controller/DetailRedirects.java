package library.controller;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Redirects back to a detail page after a form save, keeping the page's query string (tab, include toggles,
 * Chart History selection). detail-state.js copies the page's query onto the action of forms that post to it.
 */
final class DetailRedirects {

    private DetailRedirects() {
    }

    static String backTo(String path, HttpServletRequest request) {
        String query = request != null ? request.getQueryString() : null;
        return "redirect:" + path + (query != null && !query.isBlank() ? "?" + query : "");
    }
}
