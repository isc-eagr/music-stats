package library.controller;

import jakarta.servlet.http.HttpServletRequest;
import library.dto.ChartFilterDTO;
import library.service.CatalogChartService;
import library.service.CatalogGraphFilterResolver;
import library.service.PlaygroundService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Comparator;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

@Controller
public class PlaygroundController {
    private final PlaygroundService playgroundService;
    private final CatalogChartService catalogChartService;
    private final CatalogGraphFilterResolver catalogGraphFilterResolver;

    @Autowired
    public PlaygroundController(PlaygroundService playgroundService, CatalogChartService catalogChartService,
                                CatalogGraphFilterResolver catalogGraphFilterResolver) {
        this.playgroundService = playgroundService;
        this.catalogChartService = catalogChartService;
        this.catalogGraphFilterResolver = catalogGraphFilterResolver;
    }

    public PlaygroundController(PlaygroundService playgroundService) {
        this(playgroundService, null, null);
    }

    @GetMapping("/playground")
    public String playground(@RequestParam(name = "year", required = false) Integer year,
                             @RequestParam(name = "view", defaultValue = "activity") String view,
                             @RequestParam(name = "reviewType", defaultValue = "year") String reviewType,
                             @RequestParam(name = "season", defaultValue = "winter") String season,
                             @RequestParam(name = "month", required = false) Integer month,
                             @RequestParam(name = "source", defaultValue = "songs") String source,
                             Model model) {
        if (year != null && (year < 1 || year > 9998)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Year must be between 1 and 9998");
        }
        if (!List.of("activity", "flashback", "graphs").contains(view)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown Playground tab");
        }
        model.addAttribute("view", view);
        model.addAttribute("currentSection", "playground");
        if (view.equals("graphs")) {
            validateGraphSource(source);
            model.addAttribute("graphsSource", source);
            return "playground";
        }
        var years = new ArrayList<>(playgroundService.getYears());
        int selectedYear = year == null ? years.getFirst() : year;
        if (!years.contains(selectedYear)) {
            years.add(selectedYear);
            years.sort(Comparator.reverseOrder());
        }
        model.addAttribute("years", years);
        model.addAttribute("selectedYear", selectedYear);
        if (view.equals("activity")) {
            model.addAttribute("heatmap", playgroundService.getHeatmap(selectedYear));
        } else {
            int selectedMonth = month == null
                    ? (selectedYear == LocalDate.now(ZoneId.of("America/Mexico_City")).getYear()
                        ? LocalDate.now(ZoneId.of("America/Mexico_City")).getMonthValue() : 1)
                    : month;
            if (selectedMonth < 1 || selectedMonth > 12) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Month must be between 1 and 12");
            }
            try {
                model.addAttribute("flashback", playgroundService.getFlashback(reviewType,
                        selectedYear, season, selectedMonth));
            } catch (IllegalArgumentException exception) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exception.getMessage());
            }
            model.addAttribute("reviewType", reviewType);
            model.addAttribute("reviewSeason", season);
            model.addAttribute("reviewMonth", selectedMonth);
            model.addAttribute("reviewSeasons", List.of(
                    new SeasonChoice("Winter", "winter"), new SeasonChoice("Spring", "spring"),
                    new SeasonChoice("Summer", "summer"), new SeasonChoice("Fall", "fall")));
            model.addAttribute("monthOptions", java.util.Arrays.stream(Month.values())
                    .map(value -> new MonthChoice(value.getValue(), value.getDisplayName(java.time.format.TextStyle.FULL,
                            java.util.Locale.ENGLISH))).toList());
        }
        return "playground";
    }

    @GetMapping("/playground/api/charts/{tab}")
    @ResponseBody
    public Map<String, Object> charts(@PathVariable String tab,
                                     @RequestParam(defaultValue = "songs") String source,
                                     HttpServletRequest request) {
        validateGraphSource(source);
        if (!List.of("general", "genre", "subgenre", "ethnicity", "language", "country", "releaseYear", "listenYear").contains(tab)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown graph tab");
        }
        ChartFilterDTO filter = catalogGraphFilterResolver.resolve(request, source);
        return switch (tab) {
            case "general" -> catalogChartService.getGeneralChartData(filter);
            case "genre" -> catalogChartService.getGenreChartData(filter);
            case "subgenre" -> catalogChartService.getSubgenreChartData(filter);
            case "ethnicity" -> catalogChartService.getEthnicityChartData(filter);
            case "language" -> catalogChartService.getLanguageChartData(filter);
            case "country" -> catalogChartService.getCountryChartData(filter);
            case "releaseYear" -> catalogChartService.getReleaseYearChartData(filter);
            case "listenYear" -> catalogChartService.getListenYearChartData(filter);
            default -> throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown graph tab");
        };
    }

    private void validateGraphSource(String source) {
        if (!List.of("artists", "albums", "songs").contains(source)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown graph source");
        }
    }

    public String playground(Integer year, String view, String reviewType, String season, Integer month, Model model) {
        return playground(year, view, reviewType, season, month, "songs", model);
    }

    public String playground(Integer year, Model model) {
        return playground(year, "activity", "year", "winter", null, model);
    }

    public record SeasonChoice(String label, String value) {}
    public record MonthChoice(int month, String label) {}
}
