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

@Controller
public class PlaygroundController {
    private final PlaygroundService playgroundService;

    public PlaygroundController(PlaygroundService playgroundService) {
        this.playgroundService = playgroundService;
    }

    @GetMapping("/playground")
    public String playground(@RequestParam(required = false) Integer year, Model model) {
        if (year != null && (year < 1 || year > 9998)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Year must be between 1 and 9998");
        }
        var years = new ArrayList<>(playgroundService.getYears());
        int selectedYear = year == null ? years.getFirst() : year;
        if (!years.contains(selectedYear)) {
            years.add(selectedYear);
            years.sort(Comparator.reverseOrder());
        }
        model.addAttribute("years", years);
        model.addAttribute("heatmap", playgroundService.getHeatmap(selectedYear));
        model.addAttribute("currentSection", "playground");
        return "playground";
    }
}
