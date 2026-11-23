package library.controller;

import jakarta.servlet.http.HttpServletRequest;
import library.dto.ChartFilterDTO;
import library.dto.ReignPageDTO;
import library.service.ReignPageBuilder;
import library.service.TopPlayedTimelineService;
import library.service.CatalogChartService;
import library.service.CatalogGraphFilterResolver;
import library.service.MaleFunFactsService;
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
import java.util.List;
import java.util.Map;

@Controller
public class PlaygroundController {
    private final PlaygroundService playgroundService;
    private final CatalogChartService catalogChartService;
    private final CatalogGraphFilterResolver catalogGraphFilterResolver;
    private final MaleFunFactsService maleFunFactsService;
    private final TopPlayedTimelineService reignService;

    @Autowired
    public PlaygroundController(PlaygroundService playgroundService, CatalogChartService catalogChartService,
                                CatalogGraphFilterResolver catalogGraphFilterResolver, MaleFunFactsService maleFunFactsService,
                                TopPlayedTimelineService reignService) {
        this.playgroundService = playgroundService;
        this.catalogChartService = catalogChartService;
        this.catalogGraphFilterResolver = catalogGraphFilterResolver;
        this.maleFunFactsService = maleFunFactsService;
        this.reignService = reignService;
    }

    public PlaygroundController(PlaygroundService playgroundService, CatalogChartService catalogChartService,
                                CatalogGraphFilterResolver catalogGraphFilterResolver) {
        this(playgroundService, catalogChartService, catalogGraphFilterResolver, null, null);
    }

    public PlaygroundController(PlaygroundService playgroundService, MaleFunFactsService maleFunFactsService) {
        this(playgroundService, null, null, maleFunFactsService, null);
    }

    public PlaygroundController(PlaygroundService playgroundService, TopPlayedTimelineService reignService) {
        this(playgroundService, null, null, null, reignService);
    }

    public PlaygroundController(PlaygroundService playgroundService) {
        this(playgroundService, null, null);
    }

    @GetMapping("/playground")
    public String playground(@RequestParam(name = "year", required = false) Integer year,
                             @RequestParam(name = "view", defaultValue = "activity") String view,
                             @RequestParam(name = "source", defaultValue = "songs") String source,
                             @RequestParam(name = "reign", defaultValue = "artists") String reign,
                             Model model) {
        if (year != null && (year < 1 || year > 9998)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Year must be between 1 and 9998");
        }
        if (!List.of("activity", "graphs", "male", "reigns").contains(view)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown Playground tab");
        }
        model.addAttribute("view", view);
        model.addAttribute("currentSection", "playground");
        if (view.equals("reigns")) {
            model.addAttribute("r", buildReigns(reign));
            return "playground";
        }
        if (view.equals("graphs")) {
            validateGraphSource(source);
            model.addAttribute("graphsSource", source);
            return "playground";
        }
        var years = new ArrayList<>(playgroundService.getYears());
        if (view.equals("male")) {
            // No year means all time for this tab.
            model.addAttribute("years", years);
            model.addAttribute("selectedYear", year);
            model.addAttribute("maleFacts", maleFunFactsService.getFacts(year));
            return "playground";
        }
        int selectedYear = year == null ? years.getFirst() : year;
        if (!years.contains(selectedYear)) {
            years.add(selectedYear);
            years.sort(Comparator.reverseOrder());
        }
        model.addAttribute("years", years);
        model.addAttribute("selectedYear", selectedYear);
        model.addAttribute("heatmap", playgroundService.getHeatmap(selectedYear));
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

    /** Reigns used to live at /reign/artists, /reign/songs and /reign/genres; old links land on the Playground tab. */
    @GetMapping("/reign/{type}")
    public String oldReignPage(@PathVariable String type) {
        if (!List.of("artists", "songs", "genres").contains(type)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown reign type");
        }
        return "redirect:/playground?view=reigns&reign=" + type;
    }

    private ReignPageDTO buildReigns(String reign) {
        LocalDate today = LocalDate.now();
        return switch (reign) {
            case "artists" -> ReignPageBuilder.build("artist", "Artist", reignService.getArtistTimeline(), Map.of(), today);
            case "songs" -> ReignPageBuilder.build("song", "Song", reignService.getSongTimeline(), Map.of(), today);
            case "genres" -> ReignPageBuilder.build("genre", "Genre", reignService.getGenreTimeline(),
                    reignService.getGenreGenderShares(), today);
            default -> throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown reign type");
        };
    }

    public String playground(Integer year, String view, Model model) {
        return playground(year, view, "songs", "artists", model);
    }

    public String reigns(String reign, Model model) {
        return playground(null, "reigns", "songs", reign, model);
    }

    public String playground(Integer year, Model model) {
        return playground(year, "activity", model);
    }
}
