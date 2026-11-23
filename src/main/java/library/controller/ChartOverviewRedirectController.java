package library.controller;

import org.springframework.stereotype.Controller;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Set;

/**
 * The old per-chart overview URLs (nav links, bookmarks, song detail links) now land on the shared
 * Chart Overview page. Filters and search carry over; the old single-level sort params do not.
 */
@Controller
public class ChartOverviewRedirectController {

    private static final Set<String> DROPPED = Set.of("sort", "dir", "sort2", "dir2", "sort3", "dir3", "page", "size", "overviewTab", "filter");

    @GetMapping("/charts/weekly/overview")
    public String weekly(@RequestParam MultiValueMap<String, String> params) {
        return redirect("weekly", params);
    }

    @GetMapping("/misc/billboard-hot-100")
    public String hot100(@RequestParam MultiValueMap<String, String> params) {
        return redirect("hot100", params);
    }

    @GetMapping("/misc/trl")
    public String trl(@RequestParam MultiValueMap<String, String> params) {
        return redirect("trl", params);
    }

    @GetMapping({"/misc/vatos-cuntdown", "/misc/pc"})
    public String cuntdown(@RequestParam MultiValueMap<String, String> params) {
        return redirect("cuntdown", params);
    }

    static String redirect(String chart, MultiValueMap<String, String> params) {
        UriComponentsBuilder target = UriComponentsBuilder.fromPath("/charts/overview").queryParam("chart", chart);
        String tab = params.getFirst("overviewTab");
        if ("album".equals(tab) || "artist".equals(tab)) {
            target.queryParam("tab", tab);
        }
        params.forEach((key, values) -> {
            if (!DROPPED.contains(key) && !"chart".equals(key) && !"tab".equals(key)) {
                values.forEach(value -> target.queryParam(key, value));
            }
        });
        return "redirect:" + target.encode().build().toUriString();
    }
}
