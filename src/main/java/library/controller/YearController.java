package library.controller;

import library.dto.YearCardDTO;
import library.service.YearService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@Controller
public class YearController {

    private final YearService yearService;

    public YearController(YearService yearService) {
        this.yearService = yearService;
    }

    @GetMapping("/listen-years")
    public String listListenYears(
            @RequestParam(required = false, defaultValue = "year") String sortby,
            @RequestParam(required = false, defaultValue = "desc") String sortdir,
            @RequestParam(required = false, defaultValue = "0") int page,
            @RequestParam(required = false, defaultValue = "50") int perpage,
            Model model) {

        List<YearCardDTO> years = yearService.getListenYears(sortby, sortdir, page, perpage);
        long totalCount = yearService.countListenYears();
        int totalPages = (int) Math.ceil((double) totalCount / perpage);

        model.addAttribute("years", years);
        model.addAttribute("yearType", "listen");
        model.addAttribute("pageTitle", "Listen Years");
        model.addAttribute("currentSection", "listen-years");
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        model.addAttribute("defaultSortBy", "year");
        model.addAttribute("currentPage", page);
        model.addAttribute("perPage", perpage);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("startIndex", page * perpage + 1);
        model.addAttribute("endIndex", Math.min((page + 1) * perpage, totalCount));

        return "years/list";
    }

    @GetMapping("/release-years")
    public String listReleaseYears(
            @RequestParam(required = false, defaultValue = "year") String sortby,
            @RequestParam(required = false, defaultValue = "desc") String sortdir,
            @RequestParam(required = false, defaultValue = "0") int page,
            @RequestParam(required = false, defaultValue = "50") int perpage,
            Model model) {

        List<YearCardDTO> years = yearService.getReleaseYears(sortby, sortdir, page, perpage);
        long totalCount = yearService.countReleaseYears();
        int totalPages = (int) Math.ceil((double) totalCount / perpage);

        model.addAttribute("years", years);
        model.addAttribute("yearType", "release");
        model.addAttribute("pageTitle", "Release Years");
        model.addAttribute("currentSection", "release-years");
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        model.addAttribute("defaultSortBy", "year");
        model.addAttribute("currentPage", page);
        model.addAttribute("perPage", perpage);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("startIndex", page * perpage + 1);
        model.addAttribute("endIndex", Math.min((page + 1) * perpage, totalCount));

        return "years/list";
    }
}

