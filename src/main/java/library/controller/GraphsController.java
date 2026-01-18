package library.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import library.repository.LookupRepository;
import library.service.ArtistService;
import library.service.AlbumService;
import library.service.SongService;
import library.util.DateFormatUtils;
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
    private final ArtistService artistService;
    private final AlbumService albumService;
    private final ObjectMapper objectMapper;
    
    public GraphsController(LookupRepository lookupRepository, SongService songService, 
                           ArtistService artistService, AlbumService albumService, ObjectMapper objectMapper) {
        this.lookupRepository = lookupRepository;
        this.songService = songService;
        this.artistService = artistService;
        this.albumService = albumService;
        this.objectMapper = objectMapper;
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
            @RequestParam(required = false) List<Integer> artist,
            @RequestParam(required = false) List<Integer> album,
            @RequestParam(required = false) List<Integer> song,
            // Age filter
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            // Account filter
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            // Country filter
            @RequestParam(required = false) List<String> country,
            @RequestParam(required = false) String countryMode,
            // Ethnicity filter
            @RequestParam(required = false) List<Integer> ethnicity,
            @RequestParam(required = false) String ethnicityMode,
            // Birth Date filter
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            // Death Date filter
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
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
            // Length filter (with entity type: album, song)
            @RequestParam(required = false) Integer lengthMin,
            @RequestParam(required = false) Integer lengthMax,
            @RequestParam(required = false) String lengthMode,
            @RequestParam(required = false) String lengthEntity,
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
            // Songs chart filters
            @RequestParam(required = false) Integer songsSeasonalChartPeak,
            @RequestParam(required = false) Integer songsSeasonalChartSeasons,
            @RequestParam(required = false) Integer songsWeeklyChartPeak,
            @RequestParam(required = false) Integer songsWeeklyChartWeeks,
            @RequestParam(required = false) Integer songsYearlyChartPeak,
            @RequestParam(required = false) Integer songsYearlyChartYears,
            // Subgenre filter
            @RequestParam(required = false) List<Integer> subgenre,
            @RequestParam(required = false) String subgenreMode,
            // Albums chart filters
            @RequestParam(required = false) Integer albumsSeasonalChartPeak,
            @RequestParam(required = false) Integer albumsSeasonalChartSeasons,
            @RequestParam(required = false) Integer albumsWeeklyChartPeak,
            @RequestParam(required = false) Integer albumsWeeklyChartWeeks,
            @RequestParam(required = false) Integer albumsYearlyChartPeak,
            @RequestParam(required = false) Integer albumsYearlyChartYears,
            // Artist include options (for filtering by artist's groups and featured songs)
            @RequestParam(required = false, defaultValue = "false") boolean includeGroups,
            @RequestParam(required = false, defaultValue = "false") boolean includeFeatured,
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
        
        // Fetch artist/album/song details for initial values (include ID, name, genderId)
        // Pass both the list (for Active Filters display) and JSON (for chip-select components)
        try {
            if (artist != null && !artist.isEmpty()) {
                java.util.List<java.util.Map<String, Object>> artistDetails = artistService.getArtistDetailsForIds(artist);
                model.addAttribute("selectedArtists", artistDetails); // List for display
                model.addAttribute("selectedArtistsJson", objectMapper.writeValueAsString(artistDetails)); // JSON for chip-select
            } else {
                model.addAttribute("selectedArtists", null);
                model.addAttribute("selectedArtistsJson", "null");
            }
            
            if (album != null && !album.isEmpty()) {
                java.util.List<java.util.Map<String, Object>> albumDetails = albumService.getAlbumDetailsForIds(album);
                model.addAttribute("selectedAlbums", albumDetails);
                model.addAttribute("selectedAlbumsJson", objectMapper.writeValueAsString(albumDetails));
            } else {
                model.addAttribute("selectedAlbums", null);
                model.addAttribute("selectedAlbumsJson", "null");
            }
            
            if (song != null && !song.isEmpty()) {
                java.util.List<java.util.Map<String, Object>> songDetails = songService.getSongDetailsForIds(song);
                model.addAttribute("selectedSongs", songDetails);
                model.addAttribute("selectedSongsJson", objectMapper.writeValueAsString(songDetails));
            } else {
                model.addAttribute("selectedSongs", null);
                model.addAttribute("selectedSongsJson", "null");
            }
        } catch (Exception e) {
            // Fallback to null if JSON serialization fails
            model.addAttribute("selectedArtists", null);
            model.addAttribute("selectedArtistsJson", "null");
            model.addAttribute("selectedAlbums", null);
            model.addAttribute("selectedAlbumsJson", "null");
            model.addAttribute("selectedSongs", null);
            model.addAttribute("selectedSongsJson", "null");
        }
        
        // Account filter
        model.addAttribute("selectedAccounts", account);
        model.addAttribute("accountMode", accountMode != null ? accountMode : "includes");
        
        // Country filter
        model.addAttribute("selectedCountries", country);
        model.addAttribute("countryMode", countryMode != null ? countryMode : "includes");
        
        // Ethnicity filter
        model.addAttribute("selectedEthnicities", ethnicity);
        model.addAttribute("ethnicityMode", ethnicityMode != null ? ethnicityMode : "includes");
        
        // Age filter
        model.addAttribute("ageMin", ageMin);
        model.addAttribute("ageMax", ageMax);
        model.addAttribute("ageMode", ageMode);
        model.addAttribute("ageAtReleaseMin", ageAtReleaseMin);
        model.addAttribute("ageAtReleaseMax", ageAtReleaseMax);
        
        // Birth Date filter - convert and add formatted versions
        String birthDateConverted = DateFormatUtils.convertToIsoFormat(birthDate);
        String birthDateFromConverted = DateFormatUtils.convertToIsoFormat(birthDateFrom);
        String birthDateToConverted = DateFormatUtils.convertToIsoFormat(birthDateTo);
        model.addAttribute("birthDate", birthDate);
        model.addAttribute("birthDateFrom", birthDateFrom);
        model.addAttribute("birthDateTo", birthDateTo);
        model.addAttribute("birthDateMode", birthDateMode);
        model.addAttribute("birthDateFormatted", DateFormatUtils.convertToDisplayFormat(birthDate));
        model.addAttribute("birthDateFromFormatted", DateFormatUtils.convertToDisplayFormat(birthDateFrom));
        model.addAttribute("birthDateToFormatted", DateFormatUtils.convertToDisplayFormat(birthDateTo));
        
        // Death Date filter - convert and add formatted versions
        String deathDateConverted = DateFormatUtils.convertToIsoFormat(deathDate);
        String deathDateFromConverted = DateFormatUtils.convertToIsoFormat(deathDateFrom);
        String deathDateToConverted = DateFormatUtils.convertToIsoFormat(deathDateTo);
        model.addAttribute("deathDate", deathDate);
        model.addAttribute("deathDateFrom", deathDateFrom);
        model.addAttribute("deathDateTo", deathDateTo);
        model.addAttribute("deathDateMode", deathDateMode);
        model.addAttribute("deathDateFormatted", DateFormatUtils.convertToDisplayFormat(deathDate));
        model.addAttribute("deathDateFromFormatted", DateFormatUtils.convertToDisplayFormat(deathDateFrom));
        model.addAttribute("deathDateToFormatted", DateFormatUtils.convertToDisplayFormat(deathDateTo));
        
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
        
        // Length filter
        model.addAttribute("lengthMin", lengthMin);
        model.addAttribute("lengthMax", lengthMax);
        model.addAttribute("lengthMode", lengthMode != null ? lengthMode : "range");
        model.addAttribute("lengthEntity", lengthEntity != null ? lengthEntity : "song");
        
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
        
        // Albums chart filters
        model.addAttribute("albumsSeasonalChartPeak", albumsSeasonalChartPeak);
        model.addAttribute("albumsSeasonalChartSeasons", albumsSeasonalChartSeasons);
        model.addAttribute("albumsWeeklyChartPeak", albumsWeeklyChartPeak);
        model.addAttribute("albumsWeeklyChartWeeks", albumsWeeklyChartWeeks);
        model.addAttribute("albumsYearlyChartPeak", albumsYearlyChartPeak);
        model.addAttribute("albumsYearlyChartYears", albumsYearlyChartYears);
        
        // Songs chart filters
        model.addAttribute("songsSeasonalChartPeak", songsSeasonalChartPeak);
        model.addAttribute("songsSeasonalChartSeasons", songsSeasonalChartSeasons);
        model.addAttribute("songsWeeklyChartPeak", songsWeeklyChartPeak);
        model.addAttribute("songsWeeklyChartWeeks", songsWeeklyChartWeeks);
        model.addAttribute("songsYearlyChartPeak", songsYearlyChartPeak);
        model.addAttribute("songsYearlyChartYears", songsYearlyChartYears);
        
        // Artist include options (for filtering by artist's groups and featured songs)
        model.addAttribute("includeGroups", includeGroups);
        model.addAttribute("includeFeatured", includeFeatured);
        
        // Active tab (default to 'top')
        model.addAttribute("activeTab", tab != null ? tab : "top");
        
        // Navigation marker
        model.addAttribute("currentSection", "graphs");
        
        return "graphs";
    }
}
