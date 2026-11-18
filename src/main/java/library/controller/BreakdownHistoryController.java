package library.controller;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import library.service.BreakdownHistoryService;
import library.service.CatalogGraphFilterResolver;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class BreakdownHistoryController {
    private static final List<String> PRIMARY_CATALOGS = List.of("artists", "albums", "songs");
    /** Lookup lists (genres, countries...) only filter by name search, plus parent genre on subgenres. */
    private static final List<String> LOOKUP_FILTERS = List.of("q", "parentGenre");

    private final BreakdownHistoryService service;
    private final CatalogGraphFilterResolver filterResolver;

    public BreakdownHistoryController(BreakdownHistoryService service, CatalogGraphFilterResolver filterResolver) {
        this.service = service;
        this.filterResolver = filterResolver;
    }

    @GetMapping("/api/playground/breakdown-history")
    public BreakdownHistoryService.History history(
            @RequestParam(defaultValue = "gender") String breakdown,
            @RequestParam(defaultValue = "plays") String measure,
            @RequestParam(defaultValue = "cumulative") String mode,
            @RequestParam(defaultValue = "month") String interval,
            @RequestParam(required = false) Integer year) {
        try {
            return service.getHistory(breakdown, measure, mode, interval, year);
        } catch (IllegalArgumentException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exception.getMessage(), exception);
        }
    }

    /**
     * Share over time chart above the catalog lists: one series per listed item (key) plus the rest.
     * filters is the list page's own query string, so "everything" means everything matching its filters.
     */
    @GetMapping("/api/catalog/share-history")
    public BreakdownHistoryService.CatalogHistory catalogHistory(
            HttpServletRequest request,
            @RequestParam String catalog,
            @RequestParam(name = "key", required = false) List<String> keys,
            @RequestParam(defaultValue = "plays") String measure,
            @RequestParam(required = false) String filters) {
        try {
            String scope = scopeJson(request, catalog, keys, parseQuery(filters));
            // The panel always shows cumulative monthly history over everything.
            return service.getCatalogHistory(catalog, keys, measure, "cumulative", "month", null, scope);
        } catch (IllegalArgumentException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exception.getMessage(), exception);
        }
    }

    /** JSON array of the keys inside the current filters, or null when the list is unfiltered. */
    private String scopeJson(HttpServletRequest request, String catalog, List<String> keys,
                             Map<String, String[]> filters) {
        if (filters.isEmpty()) return null;
        if (PRIMARY_CATALOGS.contains(catalog)) {
            // Same complete query the Playground graphs use, so the set matches the list exactly.
            return filterResolver.resolve(new FilterRequest(request, filters), catalog).getMatchedCatalogIdsJson();
        }
        boolean filtered = LOOKUP_FILTERS.stream().anyMatch(name -> filters.containsKey(name)
                && Arrays.stream(filters.get(name)).anyMatch(value -> !value.isBlank()));
        // A searched lookup list charts every listed item, so the listed keys are the whole filtered set.
        return filtered && keys != null ? keys.stream()
                .map(key -> "\"" + key.trim().replace("\\", "\\\\").replace("\"", "\\\"") + "\"")
                .collect(Collectors.joining(",", "[", "]")) : null;
    }

    private static Map<String, String[]> parseQuery(String query) {
        Map<String, List<String>> values = new LinkedHashMap<>();
        if (query != null) {
            for (String pair : query.replaceFirst("^\\?", "").split("&")) {
                if (pair.isBlank()) continue;
                int split = pair.indexOf('=');
                String name = decode(split < 0 ? pair : pair.substring(0, split));
                String value = split < 0 ? "" : decode(pair.substring(split + 1));
                values.computeIfAbsent(name, ignored -> new ArrayList<>()).add(value);
            }
        }
        Map<String, String[]> result = new LinkedHashMap<>();
        values.forEach((name, list) -> result.put(name, list.toArray(String[]::new)));
        return result;
    }

    private static String decode(String text) {
        return URLDecoder.decode(text, StandardCharsets.UTF_8);
    }

    /** Presents the list page's filters as the request parameters the resolver reads. */
    private static final class FilterRequest extends HttpServletRequestWrapper {
        private final Map<String, String[]> parameters;

        FilterRequest(HttpServletRequest request, Map<String, String[]> parameters) {
            super(request);
            this.parameters = Collections.unmodifiableMap(parameters);
        }

        @Override public String getParameter(String name) {
            String[] values = parameters.get(name);
            return values == null || values.length == 0 ? null : values[0];
        }

        @Override public String[] getParameterValues(String name) { return parameters.get(name); }
        @Override public Map<String, String[]> getParameterMap() { return parameters; }
        @Override public Enumeration<String> getParameterNames() { return Collections.enumeration(parameters.keySet()); }
    }
}
