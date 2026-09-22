package library.controller;

import library.service.PlaygroundService;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Comparator;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.List;

@Controller
public class PlaygroundController {
    private final PlaygroundService playgroundService;

    public PlaygroundController(PlaygroundService playgroundService) {
        this.playgroundService = playgroundService;
    }

    @GetMapping("/playground")
    public String playground(@RequestParam(name = "year", required = false) Integer year,
                             @RequestParam(name = "view", defaultValue = "activity") String view,
                             @RequestParam(name = "reviewType", defaultValue = "year") String reviewType,
                             @RequestParam(name = "season", defaultValue = "winter") String season,
                             @RequestParam(name = "month", required = false) Integer month,
                             Model model) {
        if (year != null && (year < 1 || year > 9998)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Year must be between 1 and 9998");
        }
        if (!List.of("activity", "flashback").contains(view)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown Playground tab");
        }
        var years = new ArrayList<>(playgroundService.getYears());
        int selectedYear = year == null ? years.getFirst() : year;
        if (!years.contains(selectedYear)) {
            years.add(selectedYear);
            years.sort(Comparator.reverseOrder());
        }
        model.addAttribute("years", years);
        model.addAttribute("selectedYear", selectedYear);
        model.addAttribute("view", view);
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
        model.addAttribute("currentSection", "playground");
        return "playground";
    }

    public String playground(Integer year, Model model) {
        return playground(year, "activity", "year", "winter", null, model);
    }

    public record SeasonChoice(String label, String value) {}
    public record MonthChoice(int month, String label) {}
}
