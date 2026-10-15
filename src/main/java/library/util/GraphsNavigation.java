package library.util;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Set;

public final class GraphsNavigation {
    private static final Set<String> LAYOUT_PARAMETERS = Set.of(
            "view", "source", "page", "perpage", "sortby", "sortdir", "sortby2", "sortdir2",
            "sortby3", "sortdir3", "randomSeed", "lazyCard", "filtersOnly", "includeExtendedStats");

    private GraphsNavigation() {}

    public static String redirectToPlayground(HttpServletRequest request, String source) {
        var url = UriComponentsBuilder.fromPath("/playground")
                .queryParam("view", "graphs").queryParam("source", source);
        request.getParameterMap().forEach((name, values) -> {
            if (!LAYOUT_PARAMETERS.contains(name)) {
                for (String value : values) {
                    url.queryParam(name, value);
                }
            }
        });
        return "redirect:" + url.build().encode().toUriString();
    }
}
