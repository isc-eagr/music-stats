package library.controller;

import library.service.OverviewFacetService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/overview-filters")
public class OverviewFilterController {

    private final OverviewFacetService overviewFacetService;

    public OverviewFilterController(OverviewFacetService overviewFacetService) {
        this.overviewFacetService = overviewFacetService;
    }

    @GetMapping("/options")
    public Map<String, List<OverviewFacetService.Option>> options() {
        return overviewFacetService.getOptions();
    }
}
