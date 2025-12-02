package library.controller;

import library.repository.LookupRepository;
import library.service.SongService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

/**
 * Controller for the standalone Graphs page.
 * Provides a full-page experience for viewing breakdown charts with filters.
 */
@Controller
public class GraphsController {
    
    private final LookupRepository lookupRepository;
    private final SongService songService;
    
    public GraphsController(LookupRepository lookupRepository, SongService songService) {
        this.lookupRepository = lookupRepository;
        this.songService = songService;
    }
    
    @GetMapping("/graphs")
    public String chartsPage(
            // Entity filter (from clicking on genre/subgenre/ethnicity/language/country/gender cards)
            @RequestParam(required = false) String filterType,
            @RequestParam(required = false) String filterId,
            @RequestParam(required = false) String filterName,
            // Date range filter (from timeframes)
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) String periodLabel,
            // Standard filters (same as song list)
            @RequestParam(required = false) String q,
            @RequestParam(required = false) String artist,
            @RequestParam(required = false) String album,
            @RequestParam(required = false) List<Integer> genre,
            @RequestParam(required = false) String genreMode,
            @RequestParam(required = false) List<Integer> subgenre,
            @RequestParam(required = false) String subgenreMode,
            @RequestParam(required = false) List<Integer> language,
            @RequestParam(required = false) String languageMode,
            @RequestParam(required = false) List<Integer> gender,
            @RequestParam(required = false) String genderMode,
            @RequestParam(required = false) List<Integer> ethnicity,
            @RequestParam(required = false) String ethnicityMode,
            @RequestParam(required = false) List<String> country,
            @RequestParam(required = false) String countryMode,
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String tab,
            Model model) {
        
        // Add lookup data for filter dropdowns
        model.addAttribute("genres", lookupRepository.getAllGenres());
        model.addAttribute("subgenres", lookupRepository.getAllSubGenres());
        model.addAttribute("languages", lookupRepository.getAllLanguages());
        model.addAttribute("genders", lookupRepository.getAllGenders());
        model.addAttribute("ethnicities", lookupRepository.getAllEthnicities());
        model.addAttribute("countries", songService.getCountries());
        
        // Pass through filter params for JavaScript to read and use in API calls
        model.addAttribute("filterType", filterType);
        model.addAttribute("filterId", filterId);
        model.addAttribute("filterName", filterName);
        model.addAttribute("listenedDateFrom", listenedDateFrom);
        model.addAttribute("listenedDateTo", listenedDateTo);
        model.addAttribute("periodLabel", periodLabel);
        
        // Standard filters
        model.addAttribute("searchQuery", q);
        model.addAttribute("selectedArtist", artist);
        model.addAttribute("selectedAlbum", album);
        model.addAttribute("selectedGenres", genre);
        model.addAttribute("genreMode", genreMode != null ? genreMode : "includes");
        model.addAttribute("selectedSubgenres", subgenre);
        model.addAttribute("subgenreMode", subgenreMode != null ? subgenreMode : "includes");
        model.addAttribute("selectedLanguages", language);
        model.addAttribute("languageMode", languageMode != null ? languageMode : "includes");
        model.addAttribute("selectedGenders", gender);
        model.addAttribute("genderMode", genderMode != null ? genderMode : "includes");
        model.addAttribute("selectedEthnicities", ethnicity);
        model.addAttribute("ethnicityMode", ethnicityMode != null ? ethnicityMode : "includes");
        model.addAttribute("selectedCountries", country);
        model.addAttribute("countryMode", countryMode != null ? countryMode : "includes");
        model.addAttribute("releaseDate", releaseDate);
        model.addAttribute("releaseDateFrom", releaseDateFrom);
        model.addAttribute("releaseDateTo", releaseDateTo);
        model.addAttribute("releaseDateMode", releaseDateMode != null ? releaseDateMode : "exact");
        
        // Active tab (default to 'top')
        model.addAttribute("activeTab", tab != null ? tab : "top");
        
        // Navigation marker
        model.addAttribute("currentSection", "graphs");
        
        return "graphs";
    }
}
