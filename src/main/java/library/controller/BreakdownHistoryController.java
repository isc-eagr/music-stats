package library.controller;

import library.service.BreakdownHistoryService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RestController
public class BreakdownHistoryController {
    /** The Catalogs lists (genres, countries...) only filter by name search, plus parent genre on subgenres. */
    private static final List<String> SEARCH_PARAMS = List.of("q", "parentGenre");

    private final BreakdownHistoryService service;

    public BreakdownHistoryController(BreakdownHistoryService service) {
        this.service = service;
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
     * Share over time chart above the Catalogs list pages: one series per listed item (key) plus the rest.
     * filters is the list page's own query string; when it holds a search, "everything" means the listed items.
     */
    @GetMapping("/api/catalog/share-history")
    public BreakdownHistoryService.CatalogHistory catalogHistory(
            @RequestParam String catalog,
            @RequestParam(name = "key", required = false) List<String> keys,
            @RequestParam(defaultValue = "plays") String measure,
            @RequestParam(required = false) String filters) {
        try {
            // The panel always shows cumulative monthly history over everything.
            return service.getCatalogHistory(catalog, keys, measure, "cumulative", "month", null,
                    searched(filters) && keys != null ? jsonArray(keys) : null);
        } catch (IllegalArgumentException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exception.getMessage(), exception);
        }
    }

    /** True when the list's query string holds a non-blank search; the chart then covers only the listed items. */
    private static boolean searched(String query) {
        if (query == null) return false;
        return Arrays.stream(query.replaceFirst("^\\?", "").split("&")).anyMatch(pair -> {
            int split = pair.indexOf('=');
            return split > 0 && SEARCH_PARAMS.contains(decode(pair.substring(0, split)))
                    && !decode(pair.substring(split + 1)).isBlank();
        });
    }

    private static String jsonArray(List<String> keys) {
        return keys.stream()
                .map(key -> "\"" + key.trim().replace("\\", "\\\\").replace("\"", "\\\"") + "\"")
                .collect(Collectors.joining(",", "[", "]"));
    }

    private static String decode(String text) {
        return URLDecoder.decode(text, StandardCharsets.UTF_8);
    }
}
