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
            // Standard filters (same as song/album/artist list pages)
            @RequestParam(required = false) String q,
            @RequestParam(required = false) String artist,
            @RequestParam(required = false) String album,
            // Account filter
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            // Country filter
            @RequestParam(required = false) List<String> country,
            @RequestParam(required = false) String countryMode,
            // Ethnicity filter
            @RequestParam(required = false) List<Integer> ethnicity,
            @RequestParam(required = false) String ethnicityMode,
            // First Listened Date filter (with entity type: artist, album, song)
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            // Gender filter
            @RequestParam(required = false) List<Integer> gender,
            @RequestParam(required = false) String genderMode,
            // Genre filter
            @RequestParam(required = false) List<Integer> genre,
            @RequestParam(required = false) String genreMode,
            // Has Featured Artists filter
            @RequestParam(required = false) String hasFeaturedArtists,
            // Is Band filter
            @RequestParam(required = false) String isBand,
            // Is Single filter
            @RequestParam(required = false) String isSingle,
            // Language filter
            @RequestParam(required = false) List<Integer> language,
            @RequestParam(required = false) String languageMode,
            // Last Listened Date filter (with entity type: artist, album, song)
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            // Play Count filter (with entity type: artist, album, song)
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            // Release Date filter (with entity type: artist, album, song)
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String releaseDateEntity,
            // Subgenre filter
            @RequestParam(required = false) List<Integer> subgenre,
            @RequestParam(required = false) String subgenreMode,
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
        
        // Standard filters - alphabetically ordered
        model.addAttribute("searchQuery", q);
        model.addAttribute("selectedArtist", artist);
        model.addAttribute("selectedAlbum", album);
        
        // Account filter
        model.addAttribute("selectedAccounts", account);
        model.addAttribute("accountMode", accountMode != null ? accountMode : "includes");
        
        // Country filter
        model.addAttribute("selectedCountries", country);
        model.addAttribute("countryMode", countryMode != null ? countryMode : "includes");
        
        // Ethnicity filter
        model.addAttribute("selectedEthnicities", ethnicity);
        model.addAttribute("ethnicityMode", ethnicityMode != null ? ethnicityMode : "includes");
        
        // First Listened Date filter
        model.addAttribute("firstListenedDate", firstListenedDate);
        model.addAttribute("firstListenedDateFrom", firstListenedDateFrom);
        model.addAttribute("firstListenedDateTo", firstListenedDateTo);
        model.addAttribute("firstListenedDateMode", firstListenedDateMode != null ? firstListenedDateMode : "exact");
        model.addAttribute("firstListenedDateEntity", firstListenedDateEntity != null ? firstListenedDateEntity : "song");
        
        // Gender filter
        model.addAttribute("selectedGenders", gender);
        model.addAttribute("genderMode", genderMode != null ? genderMode : "includes");
        
        // Genre filter
        model.addAttribute("selectedGenres", genre);
        model.addAttribute("genreMode", genreMode != null ? genreMode : "includes");
        
        // Has Featured Artists filter
        model.addAttribute("selectedHasFeaturedArtists", hasFeaturedArtists);
        
        // Is Band filter
        model.addAttribute("selectedIsBand", isBand);
        
        // Is Single filter
        model.addAttribute("selectedIsSingle", isSingle);
        
        // Language filter
        model.addAttribute("selectedLanguages", language);
        model.addAttribute("languageMode", languageMode != null ? languageMode : "includes");
        
        // Last Listened Date filter
        model.addAttribute("lastListenedDate", lastListenedDate);
        model.addAttribute("lastListenedDateFrom", lastListenedDateFrom);
        model.addAttribute("lastListenedDateTo", lastListenedDateTo);
        model.addAttribute("lastListenedDateMode", lastListenedDateMode != null ? lastListenedDateMode : "exact");
        model.addAttribute("lastListenedDateEntity", lastListenedDateEntity != null ? lastListenedDateEntity : "song");
        
        // Play Count filter
        model.addAttribute("playCountMin", playCountMin);
        model.addAttribute("playCountMax", playCountMax);
        model.addAttribute("playCountEntity", playCountEntity != null ? playCountEntity : "song");
        
        // Release Date filter
        model.addAttribute("releaseDate", releaseDate);
        model.addAttribute("releaseDateFrom", releaseDateFrom);
        model.addAttribute("releaseDateTo", releaseDateTo);
        model.addAttribute("releaseDateMode", releaseDateMode != null ? releaseDateMode : "exact");
        model.addAttribute("releaseDateEntity", releaseDateEntity != null ? releaseDateEntity : "album");
        
        // Subgenre filter
        model.addAttribute("selectedSubgenres", subgenre);
        model.addAttribute("subgenreMode", subgenreMode != null ? subgenreMode : "includes");
        
        // Active tab (default to 'top')
        model.addAttribute("activeTab", tab != null ? tab : "top");
        
        // Navigation marker
        model.addAttribute("currentSection", "graphs");
        
        return "graphs";
    }
}
