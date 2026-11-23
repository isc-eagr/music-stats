package library.controller;

import library.service.TimeframeDetailService;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.server.ResponseStatusException;

/**
 * Timeframe detail pages: /days/2025-03-21, /weeks/2025-W11, /months/2025-03, /seasons/2025-Winter, /years/2024,
 * /decades/2010s. Each path is literal so static resources and the other catalogs never match it.
 */
@Controller
public class TimeframeDetailController {

    private final TimeframeDetailService timeframeDetailService;

    public TimeframeDetailController(TimeframeDetailService timeframeDetailService) {
        this.timeframeDetailService = timeframeDetailService;
    }

    @GetMapping("/days/{key}")
    public String day(@PathVariable String key, Model model) {
        return render("days", key, model);
    }

    @GetMapping("/weeks/{key}")
    public String week(@PathVariable String key, Model model) {
        return render("weeks", key, model);
    }

    @GetMapping("/months/{key}")
    public String month(@PathVariable String key, Model model) {
        return render("months", key, model);
    }

    @GetMapping("/seasons/{key}")
    public String season(@PathVariable String key, Model model) {
        return render("seasons", key, model);
    }

    @GetMapping("/years/{key}")
    public String year(@PathVariable String key, Model model) {
        return render("years", key, model);
    }

    @GetMapping("/decades/{key}")
    public String decade(@PathVariable String key, Model model) {
        return render("decades", key, model);
    }

    private String render(String type, String key, Model model) {
        var detail = timeframeDetailService.build(type, key)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Timeframe not found"));
        model.addAttribute("currentSection", type);
        model.addAttribute("d", detail);
        return "timeframes/detail";
    }
}
