package library.controller;

import library.dto.ArtistCardDTO;
import library.dto.ArtistSongDTO;
import library.dto.ChartFilterDTO;
import library.dto.FeaturedArtistCardDTO;
import library.dto.GenderCountDTO;
import library.entity.Artist;
import library.repository.LookupRepository;
import library.service.AppConfigService;
import library.service.ArtistService;
import library.service.BillboardHot100Service;
import library.service.CatalogChartService;
import library.service.ChartService;
import library.service.ChartFilterRequestFactory;
import library.service.ItunesService;
import library.service.PcService;
import library.service.ThemeService;
import library.service.TagService;
import library.service.TrlService;
import library.service.DetailAssociationsService;
import library.service.DetailChartHistoryService;
import library.service.DetailPlaysService;
import library.util.ArtistGeneralSummaries;
import library.util.DateFormatUtils;
import library.util.GraphsNavigation;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.multipart.MultipartFile;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller
@RequestMapping("/artists")
public class ArtistController {
    
    private final ArtistService artistService;
    private final ChartService chartService;
    private final LookupRepository lookupRepository;
    private final ItunesService itunesService;
    private final ThemeService themeService;
    private final AppConfigService appConfigService;
    private final BillboardHot100Service billboardHot100Service;
    private final PcService pcService;
    private final TrlService trlService;
    private final CatalogChartService catalogChartService;
    private final ChartFilterRequestFactory chartFilterRequestFactory;
    private final TagService tagService;
    private final DetailPlaysService detailPlaysService;
    private final DetailAssociationsService detailAssociationsService;
    private final DetailChartHistoryService detailChartHistoryService;

    public ArtistController(ArtistService artistService, ChartService chartService, LookupRepository lookupRepository,
                             ItunesService itunesService, ThemeService themeService, AppConfigService appConfigService,
                             BillboardHot100Service billboardHot100Service, PcService pcService, TrlService trlService,
                             CatalogChartService catalogChartService, ChartFilterRequestFactory chartFilterRequestFactory,
                             TagService tagService, DetailPlaysService detailPlaysService,
                             DetailAssociationsService detailAssociationsService,
                             DetailChartHistoryService detailChartHistoryService) {
        this.detailChartHistoryService = detailChartHistoryService;
        this.artistService = artistService;
        this.chartService = chartService;
        this.lookupRepository = lookupRepository;
        this.itunesService = itunesService;
        this.themeService = themeService;
        this.appConfigService = appConfigService;
        this.billboardHot100Service = billboardHot100Service;
        this.pcService = pcService;
        this.trlService = trlService;
        this.catalogChartService = catalogChartService;
        this.chartFilterRequestFactory = chartFilterRequestFactory;
        this.tagService = tagService;
        this.detailPlaysService = detailPlaysService;
        this.detailAssociationsService = detailAssociationsService;
    }
    
    @InitBinder
    public void initBinder(WebDataBinder binder) {
        binder.registerCustomEditor(LocalDate.class, "birthDate", new java.beans.PropertyEditorSupport() {
            private final DateTimeFormatter inputFormat = DateTimeFormatter.ofPattern("dd/MM/yyyy");
            
            @Override
            public void setAsText(String text) {
                if (text == null || text.trim().isEmpty()) {
                    setValue(null);
                } else {
                    try {
                        // Try dd/MM/yyyy format first
                        setValue(LocalDate.parse(text, inputFormat));
                    } catch (Exception e) {
                        try {
                            // Fallback to ISO format (yyyy-MM-dd)
                            setValue(LocalDate.parse(text));
                        } catch (Exception e2) {
                            System.err.println("Failed to parse birth date: " + text);
                            setValue(null);
                        }
                    }
                }
            }
            
            @Override
            public String getAsText() {
                LocalDate date = (LocalDate) getValue();
                return (date != null) ? date.format(inputFormat) : "";
            }
        });
        
        binder.registerCustomEditor(LocalDate.class, "deathDate", new java.beans.PropertyEditorSupport() {
            private final DateTimeFormatter inputFormat = DateTimeFormatter.ofPattern("dd/MM/yyyy");
            
            @Override
            public void setAsText(String text) {
                if (text == null || text.trim().isEmpty()) {
                    setValue(null);
                } else {
                    try {
                        // Try dd/MM/yyyy format first
                        setValue(LocalDate.parse(text, inputFormat));
                    } catch (Exception e) {
                        try {
                            // Fallback to ISO format (yyyy-MM-dd)
                            setValue(LocalDate.parse(text));
                        } catch (Exception e2) {
                            System.err.println("Failed to parse death date: " + text);
                            setValue(null);
                        }
                    }
                }
            }
            
            @Override
            public String getAsText() {
                LocalDate date = (LocalDate) getValue();
                return (date != null) ? date.format(inputFormat) : "";
            }
        });
    }
    
    @GetMapping(params = "view=graphs")
    public String graphs(HttpServletRequest request) {
        return GraphsNavigation.redirectToPlayground(request, "artists");
    }

    @GetMapping
    public String listArtists(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> gender,
            @RequestParam(required = false) String genderMode,
            @RequestParam(required = false) List<Integer> ethnicity,
            @RequestParam(required = false) String ethnicityMode,
            @RequestParam(required = false) List<Integer> genre,
            @RequestParam(required = false) String genreMode,
            @RequestParam(required = false) List<Integer> subgenre,
            @RequestParam(required = false) String subgenreMode,
            @RequestParam(required = false) List<Integer> language,
            @RequestParam(required = false) String languageMode,
            @RequestParam(required = false) List<String> country,
            @RequestParam(required = false) String countryMode,
            @RequestParam(required = false) List<Integer> tag,
            @RequestParam(required = false) String tagMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) String organized,
            @RequestParam(required = false) Integer imageCountMin,
            @RequestParam(required = false) Integer imageCountMax,
            @RequestParam(required = false) Integer imageTheme,
            @RequestParam(required = false) String imageThemeMode,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) Integer albumCountMin,
            @RequestParam(required = false) Integer albumCountMax,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) Integer songCountMin,
            @RequestParam(required = false) Integer songCountMax,
            @RequestParam(required = false) Integer itunesPresenceMin,
            @RequestParam(required = false) Integer itunesPresenceMax,
            @RequestParam(defaultValue = "true") boolean includeMain,
            @RequestParam(defaultValue = "false") boolean includeGroups,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam(defaultValue = "plays") String sortby,
            @RequestParam(defaultValue = "desc") String sortdir,
            @RequestParam(required = false) String sortby2,
            @RequestParam(required = false) String sortdir2,
            @RequestParam(required = false) String sortby3,
            @RequestParam(required = false) String sortdir3,
            @RequestParam(required = false) Integer randomSeed,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(required = false) Integer perpage,
            @RequestParam(defaultValue = "false") boolean includeExtendedStats,
            @RequestParam(defaultValue = "false") boolean lazyCard,
            @RequestParam(defaultValue = "false") boolean filtersOnly,
            Model model) {
        
        // Convert date formats from dd/mm/yyyy to yyyy-MM-dd for database queries
        String firstListenedDateConverted = DateFormatUtils.convertToIsoFormat(firstListenedDate);
        String firstListenedDateFromConverted = DateFormatUtils.convertToIsoFormat(firstListenedDateFrom);
        String firstListenedDateToConverted = DateFormatUtils.convertToIsoFormat(firstListenedDateTo);
        String lastListenedDateConverted = DateFormatUtils.convertToIsoFormat(lastListenedDate);
        String lastListenedDateFromConverted = DateFormatUtils.convertToIsoFormat(lastListenedDateFrom);
        String lastListenedDateToConverted = DateFormatUtils.convertToIsoFormat(lastListenedDateTo);
        String listenedDateFromConverted = DateFormatUtils.convertToIsoFormat(listenedDateFrom);
        String listenedDateToConverted = DateFormatUtils.convertToIsoFormat(listenedDateTo);
        String birthDateConverted = DateFormatUtils.convertToIsoFormat(birthDate);
        String birthDateFromConverted = DateFormatUtils.convertToIsoFormat(birthDateFrom);
        String birthDateToConverted = DateFormatUtils.convertToIsoFormat(birthDateTo);
        String deathDateConverted = DateFormatUtils.convertToIsoFormat(deathDate);
        String deathDateFromConverted = DateFormatUtils.convertToIsoFormat(deathDateFrom);
        String deathDateToConverted = DateFormatUtils.convertToIsoFormat(deathDateTo);
        int effectivePerPage = lazyCard ? 1
                : filtersOnly ? 1
                : appConfigService.normalizePageSize(perpage, appConfigService.getArtistsListPageSize());
        
        // Pre-compute iTunes artist IDs once for all 3 queries
        String itunesIdsJson = filtersOnly ? null : artistService.getItunesArtistIdsJson(inItunes);
        
        // Get filtered and sorted artists
        List<ArtistCardDTO> artists = filtersOnly ? List.of() : artistService.getArtists(
                q, gender, genderMode, ethnicity, ethnicityMode, genre, genreMode, 
                subgenre, subgenreMode, language, languageMode, country, countryMode,
                tag, tagMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                account, accountMode, ageMin, ageMax, ageMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, imageTheme, imageThemeMode, isBand, itunesIdsJson, inItunes,
                playCountMin, playCountMax,
                albumCountMin, albumCountMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                songCountMin, songCountMax,
                itunesPresenceMin, itunesPresenceMax,
                includeMain, includeGroups, includeFeatured,
                sortby, sortdir, sortby2, sortdir2, sortby3, sortdir3, randomSeed, page, effectivePerPage,
                includeExtendedStats
        );
        
        // Get total count for pagination
        long totalCount = filtersOnly || lazyCard ? artists.size() : artistService.countArtists(q, gender, genderMode, ethnicity,
                ethnicityMode, genre, genreMode, subgenre, subgenreMode, language, 
                languageMode, country, countryMode,
                tag, tagMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                account, accountMode, ageMin, ageMax, ageMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, imageTheme, imageThemeMode, isBand, itunesIdsJson, inItunes,
                playCountMin, playCountMax,
                albumCountMin, albumCountMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                songCountMin, songCountMax,
                itunesPresenceMin, itunesPresenceMax,
                includeMain, includeGroups, includeFeatured);
        int totalPages = (int) Math.ceil((double) totalCount / effectivePerPage);
        
        // Get gender counts for the filtered dataset
        GenderCountDTO genderCounts = filtersOnly || lazyCard ? new GenderCountDTO() : artistService.countArtistsByGender(q, gender, genderMode, ethnicity,
                ethnicityMode, genre, genreMode, subgenre, subgenreMode, language, 
                languageMode, country, countryMode,
                tag, tagMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                account, accountMode, ageMin, ageMax, ageMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, imageTheme, imageThemeMode, isBand, itunesIdsJson, inItunes,
                playCountMin, playCountMax,
                albumCountMin, albumCountMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                songCountMin, songCountMax,
                itunesPresenceMin, itunesPresenceMax,
                includeMain, includeGroups, includeFeatured);
        
        // Add data to model
        model.addAttribute("currentSection", "artists");
        model.addAttribute("artists", artists);
        if (lazyCard && includeExtendedStats && !artists.isEmpty()) {
            artistService.fillListenedSongs(artists.getFirst(), includeMain, includeGroups, includeFeatured);
        }
        // Ranks are shown in the More panel, which loads one card at a time (lazyCard), so only rank that card
        model.addAttribute("artistRankings", lazyCard && !artists.isEmpty()
                ? java.util.Map.of(artists.getFirst().getId(), artistService.getAllArtistRankings(artists.getFirst().getId()))
                : java.util.Collections.emptyMap());
        model.addAttribute("genderCounts", genderCounts);
        model.addAttribute("currentPage", page);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("perPage", effectivePerPage);
        model.addAttribute("startIndex", (page * effectivePerPage) + 1);
        model.addAttribute("endIndex", Math.min((page + 1) * effectivePerPage, totalCount));
        
        // Add filter values to maintain state
        model.addAttribute("searchQuery", q);
        model.addAttribute("selectedGenders", gender);
        model.addAttribute("genderMode", genderMode != null ? genderMode : "includes");
        model.addAttribute("selectedEthnicities", ethnicity);
        model.addAttribute("ethnicityMode", ethnicityMode != null ? ethnicityMode : "includes");
        model.addAttribute("selectedGenres", genre);
        model.addAttribute("genreMode", genreMode != null ? genreMode : "includes");
        model.addAttribute("selectedSubgenres", subgenre);
        model.addAttribute("subgenreMode", subgenreMode != null ? subgenreMode : "includes");
        model.addAttribute("selectedLanguages", language);
        model.addAttribute("languageMode", languageMode != null ? languageMode : "includes");
        model.addAttribute("selectedCountries", country);
        model.addAttribute("countryMode", countryMode != null ? countryMode : "includes");
        model.addAttribute("selectedTags", tag);
        model.addAttribute("tagMode", tagMode != null ? tagMode : "includes");
        model.addAttribute("selectedAccounts", account);
        model.addAttribute("accountMode", accountMode != null ? accountMode : "includes");
        model.addAttribute("selectedOrganized", organized);
        model.addAttribute("imageCountMin", imageCountMin);
        model.addAttribute("imageCountMax", imageCountMax);
        model.addAttribute("selectedImageTheme", imageTheme);
        model.addAttribute("imageThemeMode", imageThemeMode != null ? imageThemeMode : "has");
        model.addAttribute("selectedIsBand", isBand);
        model.addAttribute("selectedInItunes", inItunes);
        model.addAttribute("playCountMin", playCountMin);
        model.addAttribute("playCountMax", playCountMax);
        model.addAttribute("albumCountMin", albumCountMin);
        model.addAttribute("albumCountMax", albumCountMax);
        model.addAttribute("songCountMin", songCountMin);
        model.addAttribute("songCountMax", songCountMax);
        model.addAttribute("itunesPresenceMin", itunesPresenceMin);
        model.addAttribute("itunesPresenceMax", itunesPresenceMax);
        model.addAttribute("includeMain", includeMain);
        model.addAttribute("includeGroups", includeGroups);
        model.addAttribute("includeFeatured", includeFeatured);
        model.addAttribute("showPlayBreakdown", selectedArtistPlaySourceCount(includeMain, includeGroups, includeFeatured) > 1);
        
        // Age filter attributes
        model.addAttribute("ageMin", ageMin);
        model.addAttribute("ageMax", ageMax);
        model.addAttribute("ageMode", ageMode);
        
        // Birth date filter attributes
        model.addAttribute("birthDate", birthDate);
        model.addAttribute("birthDateFrom", birthDateFrom);
        model.addAttribute("birthDateTo", birthDateTo);
        model.addAttribute("birthDateMode", birthDateMode);
        model.addAttribute("birthDateFormatted", DateFormatUtils.convertToDisplayFormat(birthDate));
        model.addAttribute("birthDateFromFormatted", DateFormatUtils.convertToDisplayFormat(birthDateFrom));
        model.addAttribute("birthDateToFormatted", DateFormatUtils.convertToDisplayFormat(birthDateTo));
        
        // Death date filter attributes
        model.addAttribute("deathDate", deathDate);
        model.addAttribute("deathDateFrom", deathDateFrom);
        model.addAttribute("deathDateTo", deathDateTo);
        model.addAttribute("deathDateMode", deathDateMode);
        model.addAttribute("deathDateFormatted", DateFormatUtils.convertToDisplayFormat(deathDate));
        model.addAttribute("deathDateFromFormatted", DateFormatUtils.convertToDisplayFormat(deathDateFrom));
        model.addAttribute("deathDateToFormatted", DateFormatUtils.convertToDisplayFormat(deathDateTo));
        
        // First listened date filter attributes
        model.addAttribute("firstListenedDate", firstListenedDate);
        model.addAttribute("firstListenedDateFrom", firstListenedDateFrom);
        model.addAttribute("firstListenedDateTo", firstListenedDateTo);
        model.addAttribute("firstListenedDateMode", firstListenedDateMode != null ? firstListenedDateMode : "exact");
        model.addAttribute("firstListenedDateFormatted", DateFormatUtils.formatIsoDateForDisplay(firstListenedDate));
        model.addAttribute("firstListenedDateFromFormatted", DateFormatUtils.formatIsoDateForDisplay(firstListenedDateFrom));
        model.addAttribute("firstListenedDateToFormatted", DateFormatUtils.formatIsoDateForDisplay(firstListenedDateTo));
        
        // Last listened date filter attributes
        model.addAttribute("lastListenedDate", lastListenedDate);
        model.addAttribute("lastListenedDateFrom", lastListenedDateFrom);
        model.addAttribute("lastListenedDateTo", lastListenedDateTo);
        model.addAttribute("lastListenedDateMode", lastListenedDateMode != null ? lastListenedDateMode : "exact");
        model.addAttribute("lastListenedDateFormatted", DateFormatUtils.formatIsoDateForDisplay(lastListenedDate));
        model.addAttribute("lastListenedDateFromFormatted", DateFormatUtils.formatIsoDateForDisplay(lastListenedDateFrom));
        model.addAttribute("lastListenedDateToFormatted", DateFormatUtils.formatIsoDateForDisplay(lastListenedDateTo));
        
        // Listened date filter attributes (filters by actual play date)
        model.addAttribute("listenedDateFrom", listenedDateFrom);
        // Lists opened from a timeframe or year after the legacy account stopped leave Primary / Legacy out
        model.addAttribute("showLegacy", library.util.LegacyPlays.worthShowingFrom(listenedDateFrom));
        model.addAttribute("listenedDateTo", listenedDateTo);
        model.addAttribute("listenedDateFromFormatted", DateFormatUtils.formatIsoDateForDisplay(listenedDateFrom));
        model.addAttribute("listenedDateToFormatted", DateFormatUtils.formatIsoDateForDisplay(listenedDateTo));
        
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        model.addAttribute("sortBy2", sortby2);
        model.addAttribute("sortDir2", sortdir2 != null ? sortdir2 : "asc");
        model.addAttribute("sortDir2Param", sortby2 != null && !sortby2.isBlank() ? (sortdir2 != null ? sortdir2 : "asc") : null);
        model.addAttribute("sortBy3", sortby3);
        model.addAttribute("sortDir3", sortdir3 != null ? sortdir3 : "asc");
        model.addAttribute("sortDir3Param", sortby3 != null && !sortby3.isBlank() ? (sortdir3 != null ? sortdir3 : "asc") : null);
        model.addAttribute("defaultSortBy", "plays");
        
        // Add filter options
        model.addAttribute("genders", artistService.getGenders());
        model.addAttribute("ethnicities", artistService.getEthnicities());
        model.addAttribute("genres", artistService.getGenres());
        model.addAttribute("subgenres", artistService.getSubGenres());
        model.addAttribute("languages", artistService.getLanguages());
        model.addAttribute("countries", artistService.getCountries());
        model.addAttribute("tags", tagService.getAllTags());
        model.addAttribute("themes", themeService.getAllThemes());
        model.addAttribute("activeTheme", themeService.getActiveTheme().orElse(null));
        
        return filtersOnly ? "artists/list :: #filterForm" : "artists/list";
    }

    @GetMapping("/api")
    @ResponseBody
    public Map<String, Object> listArtistsApi(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> gender,
            @RequestParam(required = false) String genderMode,
            @RequestParam(required = false) List<Integer> ethnicity,
            @RequestParam(required = false) String ethnicityMode,
            @RequestParam(required = false) List<Integer> genre,
            @RequestParam(required = false) String genreMode,
            @RequestParam(required = false) List<Integer> subgenre,
            @RequestParam(required = false) String subgenreMode,
            @RequestParam(required = false) List<Integer> language,
            @RequestParam(required = false) String languageMode,
            @RequestParam(required = false) List<String> country,
            @RequestParam(required = false) String countryMode,
            @RequestParam(required = false) List<Integer> tag,
            @RequestParam(required = false) String tagMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) String organized,
            @RequestParam(required = false) Integer imageCountMin,
            @RequestParam(required = false) Integer imageCountMax,
            @RequestParam(required = false) Integer imageTheme,
            @RequestParam(required = false) String imageThemeMode,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) Integer albumCountMin,
            @RequestParam(required = false) Integer albumCountMax,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) Integer songCountMin,
            @RequestParam(required = false) Integer songCountMax,
            @RequestParam(required = false) Integer itunesPresenceMin,
            @RequestParam(required = false) Integer itunesPresenceMax,
            @RequestParam(defaultValue = "true") boolean includeMain,
            @RequestParam(defaultValue = "false") boolean includeGroups,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam(defaultValue = "plays") String sortby,
            @RequestParam(defaultValue = "desc") String sortdir,
            @RequestParam(required = false) String sortby2,
            @RequestParam(required = false) String sortdir2,
            @RequestParam(required = false) String sortby3,
            @RequestParam(required = false) String sortdir3,
            @RequestParam(required = false) Integer randomSeed,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(required = false) Integer perpage,
            @RequestParam(defaultValue = "false") boolean includeExtendedStats) {

        String firstListenedDateConverted = DateFormatUtils.convertToIsoFormat(firstListenedDate);
        String firstListenedDateFromConverted = DateFormatUtils.convertToIsoFormat(firstListenedDateFrom);
        String firstListenedDateToConverted = DateFormatUtils.convertToIsoFormat(firstListenedDateTo);
        String lastListenedDateConverted = DateFormatUtils.convertToIsoFormat(lastListenedDate);
        String lastListenedDateFromConverted = DateFormatUtils.convertToIsoFormat(lastListenedDateFrom);
        String lastListenedDateToConverted = DateFormatUtils.convertToIsoFormat(lastListenedDateTo);
        String listenedDateFromConverted = DateFormatUtils.convertToIsoFormat(listenedDateFrom);
        String listenedDateToConverted = DateFormatUtils.convertToIsoFormat(listenedDateTo);
        String birthDateConverted = DateFormatUtils.convertToIsoFormat(birthDate);
        String birthDateFromConverted = DateFormatUtils.convertToIsoFormat(birthDateFrom);
        String birthDateToConverted = DateFormatUtils.convertToIsoFormat(birthDateTo);
        String deathDateConverted = DateFormatUtils.convertToIsoFormat(deathDate);
        String deathDateFromConverted = DateFormatUtils.convertToIsoFormat(deathDateFrom);
        String deathDateToConverted = DateFormatUtils.convertToIsoFormat(deathDateTo);
        int effectivePerPage = appConfigService.normalizePageSize(perpage, appConfigService.getArtistsListPageSize());

        String itunesIdsJson = artistService.getItunesArtistIdsJson(inItunes);

        List<ArtistCardDTO> artists = artistService.getArtists(
                q, gender, genderMode, ethnicity, ethnicityMode, genre, genreMode,
                subgenre, subgenreMode, language, languageMode, country, countryMode,
                tag, tagMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                account, accountMode, ageMin, ageMax, ageMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, imageTheme, imageThemeMode, isBand, itunesIdsJson, inItunes,
                playCountMin, playCountMax,
                albumCountMin, albumCountMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                songCountMin, songCountMax,
                itunesPresenceMin, itunesPresenceMax,
                includeMain, includeGroups, includeFeatured,
                sortby, sortdir, sortby2, sortdir2, sortby3, sortdir3, randomSeed, page, effectivePerPage,
                includeExtendedStats
        );

        long totalCount = artistService.countArtists(q, gender, genderMode, ethnicity,
                ethnicityMode, genre, genreMode, subgenre, subgenreMode, language,
                languageMode, country, countryMode,
                tag, tagMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                account, accountMode, ageMin, ageMax, ageMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, imageTheme, imageThemeMode, isBand, itunesIdsJson, inItunes,
                playCountMin, playCountMax,
                albumCountMin, albumCountMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                songCountMin, songCountMax,
                itunesPresenceMin, itunesPresenceMax,
                includeMain, includeGroups, includeFeatured);

        Map<String, Object> result = new HashMap<>();
        result.put("items", artists);
        result.put("totalCount", totalCount);
        result.put("page", page);
        result.put("perPage", effectivePerPage);
        return result;
    }

    @GetMapping("/api/charts/{tab}")
    @ResponseBody
    public Map<String, Object> artistChartsData(
            @PathVariable String tab,
            HttpServletRequest request,
            @RequestParam(defaultValue = "false") boolean includeGroups,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam(defaultValue = "0") int limit,
            @RequestParam(required = false) String limitEntity) {

        ChartFilterDTO filter = chartFilterRequestFactory.build(request, includeGroups, includeFeatured, limit, limitEntity, "artist");

        return switch (tab) {
            case "general" -> catalogChartService.getGeneralChartData(filter);
            case "genre" -> catalogChartService.getGenreChartData(filter);
            case "subgenre" -> catalogChartService.getSubgenreChartData(filter);
            case "ethnicity" -> catalogChartService.getEthnicityChartData(filter);
            case "language" -> catalogChartService.getLanguageChartData(filter);
            case "country" -> catalogChartService.getCountryChartData(filter);
            case "releaseYear" -> catalogChartService.getReleaseYearChartData(filter);
            case "listenYear" -> catalogChartService.getListenYearChartData(filter);
            default -> throw new IllegalArgumentException("Unknown charts tab: " + tab);
        };
    }
    
    /** Plays tab: the latest overall rank moves, loaded when the tab is opened (it replays the whole play history). */
    @GetMapping("/{id}/rank-moves")
    @ResponseBody
    public DetailPlaysService.RankMoves rankMoves(@PathVariable Integer id) {
        return detailPlaysService.rankMoves(DetailPlaysService.Kind.ARTIST, id);
    }

    @GetMapping("/{id}")
    public String viewArtist(@PathVariable Integer id, 
                            @RequestParam(defaultValue = "general") String tab,
                            @RequestParam(defaultValue = "0") int playsPage,
                            @RequestParam(defaultValue = "false") boolean includeGroups,
                            @RequestParam(defaultValue = "false") boolean includeFeatured,
                            @RequestParam(defaultValue = "true") boolean includeMain,
                            Model model) {
        Optional<Artist> artist = artistService.getArtistById(id);
        
        if (artist.isEmpty()) {
            return "redirect:/artists";
        }
        
        // Get the groups this artist belongs to (for aggregation)
        List<Integer> groupIds = artistService.getGroupIdsForArtist(id);
        boolean hasGroups = !groupIds.isEmpty();
        
        // Check if artist has an image (always use raw image on detail page, bypassing theme)
        boolean hasImage = artistService.hasRawArtistImage(id);
        
        model.addAttribute("currentSection", "artists");
        model.addAttribute("artist", artist.get());
        model.addAttribute("hasImage", hasImage);
        model.addAttribute("genders", artistService.getGenders());
        model.addAttribute("ethnicities", artistService.getEthnicities());
        model.addAttribute("genres", artistService.getGenres());
        model.addAttribute("subgenres", artistService.getSubGenres());
        model.addAttribute("languages", artistService.getLanguages());
        // Use ALL countries for edit mode dropdown
        model.addAttribute("countries", artistService.getAllCountries());
        model.addAttribute("tagOptions", tagService.getAllTagOptions());
        model.addAttribute("assignedTags", tagService.getArtistTags(id));
        
        // Add iTunes presence
        Artist artistEntity = artist.get();
        artistEntity.setInItunes(itunesService.artistExistsInItunes(artistEntity.getName()));
        model.addAttribute("artist", artistEntity);
        
        // Add group membership data
        model.addAttribute("hasGroups", hasGroups);
        model.addAttribute("includeGroups", includeGroups);
        
        // Add featured songs data
        boolean hasFeaturedSongs = artistService.hasFeaturedSongs(id);
        model.addAttribute("hasFeaturedSongs", hasFeaturedSongs);
        model.addAttribute("includeFeatured", includeFeatured);
        model.addAttribute("showPlayBreakdown", selectedArtistPlaySourceCount(includeMain, includeGroups, includeFeatured) > 1);
        
        // Add includeMain toggle data (toggle is always shown, default is on)
        model.addAttribute("includeMain", includeMain);
        
        // Groups this artist belongs to (sidebar "Member of" tags and the group editor)
        List<FeaturedArtistCardDTO> groupArtistCards = artistService.getGroupsForArtist(id);
        model.addAttribute("groupArtistCards", groupArtistCards);
        
        // Determine which data sources to use
        // includeGroups works independently from includeMain
        List<Integer> effectiveGroupIds = (includeGroups && hasGroups) ? groupIds : null;
        
        ArtistService.ArtistScopedMetrics scopedMetrics = artistService.getScopedMetricsForArtist(
                id, includeMain, includeGroups, includeFeatured);
        model.addAttribute("albumCount", scopedMetrics.albumCount());
        model.addAttribute("songCount", scopedMetrics.songCount());
        model.addAttribute("artistPlayCount", scopedMetrics.playCount());
        model.addAttribute("artistVatitoPlayCount", scopedMetrics.vatitoPlayCount());
        model.addAttribute("artistRobertloverPlayCount", scopedMetrics.robertloverPlayCount());
        model.addAttribute("artistOwnPlayCount", artistService.getPlayCountForArtist(id));
        model.addAttribute("mainPlayCount", scopedMetrics.mainPlayCount());
        model.addAttribute("groupPlayCount", scopedMetrics.groupPlayCount());
        model.addAttribute("featuredPlayCount", scopedMetrics.featuredPlayCount());
        model.addAttribute("mainVatitoPlayCount", scopedMetrics.mainVatitoPlayCount());
        model.addAttribute("groupVatitoPlayCount", scopedMetrics.groupVatitoPlayCount());
        model.addAttribute("featuredVatitoPlayCount", scopedMetrics.featuredVatitoPlayCount());
        model.addAttribute("mainRobertloverPlayCount", scopedMetrics.mainRobertloverPlayCount());
        model.addAttribute("groupRobertloverPlayCount", scopedMetrics.groupRobertloverPlayCount());
        model.addAttribute("featuredRobertloverPlayCount", scopedMetrics.featuredRobertloverPlayCount());
        model.addAttribute("totalListeningTime", library.util.TimeFormatUtils.formatTime(scopedMetrics.listeningSeconds()));
        model.addAttribute("firstListenedDate", scopedMetrics.firstListened() != null ? DateFormatUtils.formatIsoDateForDisplay(scopedMetrics.firstListened().substring(0, 10)) : null);
        model.addAttribute("lastListenedDate", scopedMetrics.lastListened() != null ? DateFormatUtils.formatIsoDateForDisplay(scopedMetrics.lastListened().substring(0, 10)) : null);
        boolean mainScopeOnly = includeMain && !includeGroups && !includeFeatured;
        model.addAttribute("firstListenedSong", mainScopeOnly ? artistService.getFirstListenedSongForArtist(id) : null);
        model.addAttribute("lastListenedSong", mainScopeOnly ? artistService.getLastListenedSongForArtist(id) : null);
        model.addAttribute("listeningSpan", DateFormatUtils.spanLabel(
                (String) model.getAttribute("firstListenedDate"), (String) model.getAttribute("lastListenedDate")));
        model.addAttribute("averageSongLength", scopedMetrics.songCount() > 0
                ? library.util.TimeFormatUtils.formatTimeHMS(scopedMetrics.totalSongLength() / scopedMetrics.songCount()) : "-");
        model.addAttribute("averagePlaysPerSong", formatAverage(scopedMetrics.averagePlaysPerSong()));
        model.addAttribute("averagePlaysPerAlbum", formatAverage(scopedMetrics.averagePlaysPerAlbum()));
        java.time.LocalDate firstListened = scopedMetrics.firstListened() != null
                ? java.time.LocalDate.parse(scopedMetrics.firstListened().substring(0, 10)) : null;
        model.addAttribute("showLegacy", library.util.LegacyPlays.worthShowing(scopedMetrics.robertloverPlayCount(), firstListened));
        model.addAttribute("consistencyPeriods", library.dto.ConsistencyPeriodDTO.of(scopedMetrics.uniqueDays(),
                scopedMetrics.uniqueWeeks(), scopedMetrics.uniqueMonths(), scopedMetrics.uniqueYears(), firstListened));
        // Ranks compare the artist's own songs, so they only match the counts in the main-only scope
        model.addAttribute("consistencyRankUrl", mainScopeOnly ? "/artists/" + id + "/consistency" : null);

        // Add albums list for the artist
        List<library.dto.ArtistAlbumDTO> albumsList;
        if (includeMain && effectiveGroupIds != null) {
            // Main + Groups
            albumsList = new java.util.ArrayList<>(artistService.getAggregatedAlbumsForArtist(id, effectiveGroupIds));
        } else if (includeMain && effectiveGroupIds == null) {
            // Main only
            albumsList = new java.util.ArrayList<>(artistService.getAlbumsForArtist(id));
        } else if (!includeMain && effectiveGroupIds != null) {
            // Groups only (no main) - pass 0 as ID to exclude main artist from aggregation
            albumsList = new java.util.ArrayList<>(artistService.getAggregatedAlbumsForArtist(0, effectiveGroupIds));
        } else {
            // No main, no groups - no albums to show
            albumsList = new java.util.ArrayList<>();
        }
        // Append cross-artist albums (albums owned by other artists but containing songs by this artist)
        if (includeMain) {
            albumsList.addAll(artistService.getCrossArtistAlbumsForArtist(id));
        }
        model.addAttribute("albums", albumsList);
        
        // Add songs list for the artist
        List<ArtistSongDTO> songs;
        if (includeMain && effectiveGroupIds != null) {
            // Main + Groups
            songs = artistService.getAggregatedSongsForArtist(id, effectiveGroupIds);
        } else if (includeMain && effectiveGroupIds == null) {
            // Main only
            songs = artistService.getSongsForArtist(id);
        } else if (!includeMain && effectiveGroupIds != null) {
            // Groups only (no main) - pass 0 as ID to exclude main artist from aggregation
            songs = artistService.getAggregatedSongsForArtist(0, effectiveGroupIds);
        } else {
            // No main, no groups - only featured
            songs = new java.util.ArrayList<>();
        }
        // Add featured songs if toggle is on
        if (includeFeatured && hasFeaturedSongs) {
            List<ArtistSongDTO> featuredSongs = artistService.getFeaturedSongsForArtist(id);
            songs = new java.util.ArrayList<>(songs);
            songs.addAll(featuredSongs);
            // Re-sort by total plays descending
            songs.sort((a, b) -> Integer.compare(b.getTotalPlays() != null ? b.getTotalPlays() : 0, 
                                                  a.getTotalPlays() != null ? a.getTotalPlays() : 0));
        }
        if (appConfigService.isCombineLinkedSongsEnabled()) {
            songs = artistService.combineLinkedSongs(songs);
        }
        model.addAttribute("songs", songs);
        model.addAttribute("noAlbumSummary", ArtistGeneralSummaries.noAlbumSummary(songs));
        model.addAttribute("playsShare", ArtistGeneralSummaries.playsShare(albumsList, songs));
        model.addAttribute("releases", ArtistGeneralSummaries.releases(albumsList));

        // Tab and plays data
        model.addAttribute("activeTab", tab);
        java.util.Map<Integer, Integer[]> yearRanks = detailPlaysService.yearRanks(DetailPlaysService.Kind.ARTIST, id);
        model.addAttribute("ranksByYear", DetailPlaysService.overallByYear(yearRanks));
        model.addAttribute("playsTab", detailPlaysService.build(
                detailPlaysService.artistScope(id, effectiveGroupIds, includeMain, includeFeatured), yearRanks));
        
        // Artist Associations tab (always pre-loaded): every song the artist is credited on, whatever the toggles
        model.addAttribute("associations", detailAssociationsService.artistTab(id));
        
        // Always load plays data (eager loading for all tabs)
        {
            int pageSize = appConfigService.getArtistDetailPlaysPageSize();
            List<library.dto.PlayDTO> plays;
            
            if (includeMain && effectiveGroupIds != null) {
                // Main + Groups
                plays = new java.util.ArrayList<>(artistService.getAggregatedPlaysForArtist(id, effectiveGroupIds, playsPage, pageSize));
                long totalCount = artistService.getAggregatedPlayCount(id, effectiveGroupIds);
                if (includeFeatured && hasFeaturedSongs) {
                    List<library.dto.PlayDTO> featuredPlays = artistService.getFeaturedPlaysForArtist(id, 0, pageSize);
                    plays.addAll(featuredPlays);
                    plays.sort((a, b) -> b.getPlayDate().compareTo(a.getPlayDate()));
                    if (plays.size() > pageSize) {
                        plays = plays.subList(0, pageSize);
                    }
                    totalCount += artistService.countFeaturedPlaysForArtist(id);
                }
                model.addAttribute("plays", plays);
                model.addAttribute("playsTotalCount", totalCount);
                model.addAttribute("playsPage", playsPage);
                model.addAttribute("playsTotalPages", (int) Math.ceil((double) totalCount / pageSize));
            } else if (includeMain && effectiveGroupIds == null) {
                // Main only
                plays = new java.util.ArrayList<>(artistService.getPlaysForArtist(id, playsPage, pageSize));
                long totalCount = artistService.countPlaysForArtist(id);
                if (includeFeatured && hasFeaturedSongs) {
                    List<library.dto.PlayDTO> featuredPlays = artistService.getFeaturedPlaysForArtist(id, 0, pageSize);
                    plays.addAll(featuredPlays);
                    plays.sort((a, b) -> b.getPlayDate().compareTo(a.getPlayDate()));
                    if (plays.size() > pageSize) {
                        plays = plays.subList(0, pageSize);
                    }
                    totalCount += artistService.countFeaturedPlaysForArtist(id);
                }
                model.addAttribute("plays", plays);
                model.addAttribute("playsTotalCount", totalCount);
                model.addAttribute("playsPage", playsPage);
                model.addAttribute("playsTotalPages", (int) Math.ceil((double) totalCount / pageSize));
            } else if (!includeMain && effectiveGroupIds != null) {
                // Groups only (no main) - pass 0 as ID to exclude main artist from aggregation
                plays = new java.util.ArrayList<>(artistService.getAggregatedPlaysForArtist(0, effectiveGroupIds, playsPage, pageSize));
                long totalCount = artistService.getAggregatedPlayCount(0, effectiveGroupIds);
                if (includeFeatured && hasFeaturedSongs) {
                    List<library.dto.PlayDTO> featuredPlays = artistService.getFeaturedPlaysForArtist(id, 0, pageSize);
                    plays.addAll(featuredPlays);
                    plays.sort((a, b) -> b.getPlayDate().compareTo(a.getPlayDate()));
                    if (plays.size() > pageSize) {
                        plays = plays.subList(0, pageSize);
                    }
                    totalCount += artistService.countFeaturedPlaysForArtist(id);
                }
                model.addAttribute("plays", plays);
                model.addAttribute("playsTotalCount", totalCount);
                model.addAttribute("playsPage", playsPage);
                model.addAttribute("playsTotalPages", (int) Math.ceil((double) totalCount / pageSize));
            } else {
                // No main, no groups - only featured
                if (includeFeatured && hasFeaturedSongs) {
                    plays = artistService.getFeaturedPlaysForArtist(id, playsPage, pageSize);
                    long totalCount = artistService.countFeaturedPlaysForArtist(id);
                    model.addAttribute("plays", plays);
                    model.addAttribute("playsTotalCount", totalCount);
                    model.addAttribute("playsPage", playsPage);
                    model.addAttribute("playsTotalPages", (int) Math.ceil((double) totalCount / pageSize));
                } else {
                    model.addAttribute("plays", java.util.Collections.emptyList());
                    model.addAttribute("playsTotalCount", 0L);
                    model.addAttribute("playsPage", 0);
                    model.addAttribute("playsTotalPages", 0);
                }
            }
        }
        
        // Always load chart history data (eager loading for all tabs)
        {
            List<library.dto.ChartHistoryDTO> songHistory;
            List<library.dto.ChartHistoryDTO> albumHistory;
            List<java.util.Map<String, Object>> seasonalSongHistory;
            List<java.util.Map<String, Object>> yearlySongHistory;
            List<java.util.Map<String, Object>> seasonalAlbumHistory;
            List<java.util.Map<String, Object>> yearlyAlbumHistory;

            if (includeMain && effectiveGroupIds != null) {
                // Main + Groups
                songHistory = chartService.getAggregatedArtistSongChartHistory(id, effectiveGroupIds);
                albumHistory = chartService.getAggregatedArtistAlbumChartHistory(id, effectiveGroupIds);
                seasonalSongHistory = new java.util.ArrayList<>(chartService.getAggregatedArtistChartHistoryByPeriodType(id, effectiveGroupIds, "song", "seasonal"));
                yearlySongHistory = new java.util.ArrayList<>(chartService.getAggregatedArtistChartHistoryByPeriodType(id, effectiveGroupIds, "song", "yearly"));
                seasonalAlbumHistory = chartService.getAggregatedArtistChartHistoryByPeriodType(id, effectiveGroupIds, "album", "seasonal");
                yearlyAlbumHistory = chartService.getAggregatedArtistChartHistoryByPeriodType(id, effectiveGroupIds, "album", "yearly");
                
                if (includeFeatured && hasFeaturedSongs) {
                    List<library.dto.ChartHistoryDTO> featuredSongHistory = chartService.getFeaturedArtistSongChartHistory(id);
                    songHistory = new java.util.ArrayList<>(songHistory);
                    songHistory.addAll(featuredSongHistory);
                    songHistory.sort((a, b) -> {
                        int peakCompare = a.getPeakPosition().compareTo(b.getPeakPosition());
                        if (peakCompare != 0) return peakCompare;
                        return b.getWeeksAtPeak().compareTo(a.getWeeksAtPeak());
                    });
                    seasonalSongHistory.addAll(chartService.getFeaturedArtistChartHistoryByPeriodType(id, "seasonal"));
                    yearlySongHistory.addAll(chartService.getFeaturedArtistChartHistoryByPeriodType(id, "yearly"));
                }
            } else if (includeMain && effectiveGroupIds == null) {
                // Main only
                songHistory = chartService.getArtistSongChartHistory(id);
                albumHistory = chartService.getArtistAlbumChartHistory(id);
                seasonalSongHistory = new java.util.ArrayList<>(chartService.getArtistChartHistoryByPeriodType(id, "song", "seasonal"));
                yearlySongHistory = new java.util.ArrayList<>(chartService.getArtistChartHistoryByPeriodType(id, "song", "yearly"));
                seasonalAlbumHistory = chartService.getArtistChartHistoryByPeriodType(id, "album", "seasonal");
                yearlyAlbumHistory = chartService.getArtistChartHistoryByPeriodType(id, "album", "yearly");
                
                if (includeFeatured && hasFeaturedSongs) {
                    List<library.dto.ChartHistoryDTO> featuredSongHistory = chartService.getFeaturedArtistSongChartHistory(id);
                    songHistory = new java.util.ArrayList<>(songHistory);
                    songHistory.addAll(featuredSongHistory);
                    songHistory.sort((a, b) -> {
                        int peakCompare = a.getPeakPosition().compareTo(b.getPeakPosition());
                        if (peakCompare != 0) return peakCompare;
                        return b.getWeeksAtPeak().compareTo(a.getWeeksAtPeak());
                    });
                    seasonalSongHistory.addAll(chartService.getFeaturedArtistChartHistoryByPeriodType(id, "seasonal"));
                    yearlySongHistory.addAll(chartService.getFeaturedArtistChartHistoryByPeriodType(id, "yearly"));
                }
            } else if (!includeMain && effectiveGroupIds != null) {
                // Groups only (no main) - pass 0 as ID to exclude main artist from aggregation
                songHistory = chartService.getAggregatedArtistSongChartHistory(0, effectiveGroupIds);
                albumHistory = chartService.getAggregatedArtistAlbumChartHistory(0, effectiveGroupIds);
                seasonalSongHistory = new java.util.ArrayList<>(chartService.getAggregatedArtistChartHistoryByPeriodType(0, effectiveGroupIds, "song", "seasonal"));
                yearlySongHistory = new java.util.ArrayList<>(chartService.getAggregatedArtistChartHistoryByPeriodType(0, effectiveGroupIds, "song", "yearly"));
                seasonalAlbumHistory = chartService.getAggregatedArtistChartHistoryByPeriodType(0, effectiveGroupIds, "album", "seasonal");
                yearlyAlbumHistory = chartService.getAggregatedArtistChartHistoryByPeriodType(0, effectiveGroupIds, "album", "yearly");
                
                if (includeFeatured && hasFeaturedSongs) {
                    List<library.dto.ChartHistoryDTO> featuredSongHistory = chartService.getFeaturedArtistSongChartHistory(id);
                    songHistory = new java.util.ArrayList<>(songHistory);
                    songHistory.addAll(featuredSongHistory);
                    songHistory.sort((a, b) -> {
                        int peakCompare = a.getPeakPosition().compareTo(b.getPeakPosition());
                        if (peakCompare != 0) return peakCompare;
                        return b.getWeeksAtPeak().compareTo(a.getWeeksAtPeak());
                    });
                    seasonalSongHistory.addAll(chartService.getFeaturedArtistChartHistoryByPeriodType(id, "seasonal"));
                    yearlySongHistory.addAll(chartService.getFeaturedArtistChartHistoryByPeriodType(id, "yearly"));
                }
            } else {
                // No main, no groups - only featured
                songHistory = new java.util.ArrayList<>();
                albumHistory = java.util.Collections.emptyList();
                seasonalSongHistory = new java.util.ArrayList<>();
                yearlySongHistory = new java.util.ArrayList<>();
                seasonalAlbumHistory = java.util.Collections.emptyList();
                yearlyAlbumHistory = java.util.Collections.emptyList();
                
                if (includeFeatured && hasFeaturedSongs) {
                    songHistory.addAll(chartService.getFeaturedArtistSongChartHistory(id));
                    seasonalSongHistory.addAll(chartService.getFeaturedArtistChartHistoryByPeriodType(id, "seasonal"));
                    yearlySongHistory.addAll(chartService.getFeaturedArtistChartHistoryByPeriodType(id, "yearly"));
                }
            }

            if (appConfigService.isCombineLinkedSongsEnabled()) {
                songHistory = chartService.combineLinkedSongChartHistory(songHistory);
            }

            // Hot 100 / Cuntdown / TRL follow the same Main / Groups / Featured toggles
            List<Integer> ownerIds = new java.util.ArrayList<>();
            if (includeMain) ownerIds.add(id);
            if (effectiveGroupIds != null) ownerIds.addAll(effectiveGroupIds);
            Integer featuredArtistId = includeFeatured && hasFeaturedSongs ? id : null;
            model.addAttribute("chartHistoryView", detailChartHistoryService.forArtist(id, songHistory, albumHistory,
                    seasonalSongHistory, yearlySongHistory, seasonalAlbumHistory, yearlyAlbumHistory,
                    billboardHot100Service.getChartedSongsForArtistScope(ownerIds, featuredArtistId),
                    pcService.getChartedSongsForArtistScope(ownerIds, featuredArtistId),
                    trlService.getChartedSongsForArtistScope(ownerIds, featuredArtistId)));
        }
        
        // Get gender name for bar color
        model.addAttribute("artistGenderName", artistService.getArtistGenderName(id));
        
        // Add ranking chips data - optimized single query
        java.util.Map<String, Integer> rankings = artistService.getAllArtistRankings(id);
        model.addAttribute("rankByGender", rankings.get("gender"));
        model.addAttribute("rankByGenre", rankings.get("genre"));
        model.addAttribute("rankBySubgenre", rankings.get("subgenre"));
        model.addAttribute("rankByEthnicity", rankings.get("ethnicity"));
        model.addAttribute("rankByLanguage", rankings.get("language"));
        model.addAttribute("rankByCountry", rankings.get("country"));
        
        // Add Spanish Rap rank (special combination)
        if (artistService.isArtistSpanishRap(id)) {
            model.addAttribute("rankBySpanishRap", artistService.getArtistSpanishRapRank(id));
            model.addAttribute("rapGenreId", lookupRepository.getGenreIdByName("Rap"));
            model.addAttribute("spanishLanguageId", lookupRepository.getLanguageIdByName("Spanish"));
        }

        // Add new ranking chips
        model.addAttribute("overallPosition", artistService.getArtistOverallPosition(id));
        model.addAttribute("totalArtistCount", artistService.getArtistCount());

        // Extended stats for detail page
        model.addAttribute("soloSongCount", scopedMetrics.soloSongCount());
        model.addAttribute("songsWithFeatCount", scopedMetrics.songsWithFeatCount());
        model.addAttribute("featuredSongCount", artistService.getFeaturedSongCount(id));
        model.addAttribute("standaloneSongCount", scopedMetrics.standaloneSongCount());
        model.addAttribute("avgAlbumLengthFormatted", scopedMetrics.albumCount() > 0
                ? library.util.TimeFormatUtils.formatTimeHMS(scopedMetrics.totalSongLength() / scopedMetrics.albumCount()) : null);
        model.addAttribute("itunesPresenceRatio", scopedMetrics.itunesPresenceRatio());

        return "artists/detail";
    }
    
    @PostMapping("/{id}")
    public String updateArtist(@PathVariable Integer id,
                               @ModelAttribute Artist artist,
                               @RequestParam(required = false) List<Integer> tags,
                               HttpServletRequest request) {
        artist.setId(id);
        artistService.saveArtist(artist);
        tagService.saveArtistTags(id, tags);
        return DetailRedirects.backTo("/artists/" + id, request);
    }
    
    @GetMapping("/{id}/image")
    public ResponseEntity<byte[]> getArtistImage(@PathVariable Integer id,
                                 @RequestParam(required = false, defaultValue = "false") boolean raw,
                                 @RequestParam(required = false, defaultValue = "false") boolean thumbnail,
                                 @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) {
        byte[] image = raw ? artistService.getRawArtistImage(id) : artistService.getArtistImage(id);
        return library.util.ImageResponses.image(image, thumbnail, ifNoneMatch);
    }
    
    @PostMapping("/{id}/image")
    @ResponseBody
    public String uploadArtistImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            
            artistService.updateArtistImage(id, file.getBytes());
            if (artistService.getSecondaryImageCount(id) == 0) {
                themeService.assignDefaultImageToDefaultTheme(id);
            }
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @DeleteMapping("/{id}/image")
    @ResponseBody
    public String deleteArtistImage(@PathVariable Integer id) {
        try {
            artistService.updateArtistImage(id, null);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    // Gallery endpoints for secondary images
    @GetMapping("/{id}/images")
    @ResponseBody
    public List<Map<String, Object>> getArtistImages(@PathVariable Integer id) {
        return artistService.getSecondaryImageMetadata(id);
    }

    @GetMapping("/{id}/images/{imageId}")
    @ResponseBody
    public byte[] getSecondaryImage(@PathVariable Integer id, @PathVariable Integer imageId) {
        return artistService.getSecondaryImage(imageId);
    }

    @PostMapping("/{id}/images")
    @ResponseBody
    public String addSecondaryImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            boolean added = artistService.addSecondaryImage(id, file.getBytes());
            return added ? "success" : "duplicate";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }

    @DeleteMapping("/{id}/images/{imageId}")
    @ResponseBody
    public String deleteSecondaryImage(@PathVariable Integer id, @PathVariable Integer imageId) {
        try {
            artistService.deleteSecondaryImage(imageId);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }

    @PostMapping("/{id}/images/{imageId}/set-default")
    @ResponseBody
    public String setImageAsDefault(@PathVariable Integer id, @PathVariable Integer imageId) {
        try {
            artistService.swapToDefault(id, imageId);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }

    @DeleteMapping("/{id}")
    @ResponseBody
    public String deleteArtist(@PathVariable Integer id) {
        try {
            artistService.deleteArtist(id);
            return "success";
        } catch (IllegalStateException e) {
            return "error:" + e.getMessage();
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @PostMapping("/create")
    @ResponseBody
    public Map<String, Object> createArtist(@RequestBody Artist artist) {
        try {
            Artist created = artistService.createArtist(artist);
            Map<String, Object> response = new java.util.HashMap<>();
            response.put("id", created.getId());
            response.put("name", created.getName());
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create artist");
        }
    }
    
    @GetMapping("/api/artists")
    @ResponseBody
    public List<Map<String, Object>> getAllArtistsForApi(@RequestParam(required = false) String q) {
        if (q != null && !q.trim().isEmpty()) {
            return artistService.searchArtists(q, 20);
        }
        return artistService.getAllArtistsForApi();
    }
    
    @GetMapping("/api/search")
    @ResponseBody
    public List<Map<String, Object>> searchArtistsApi(
            @RequestParam String q,
            @RequestParam(required = false, defaultValue = "0") int limit) {
        return artistService.searchArtists(q, limit);
    }

    @GetMapping("/api/filter-values")
    @ResponseBody
    public List<Map<String, Object>> getArtistFilterValues(
            @RequestParam(name = "id", required = false) List<Integer> artistIds) {
        if (artistIds == null || artistIds.isEmpty()) {
            return List.of();
        }
        return artistIds.stream()
                .distinct()
                .map(artistService::getArtistById)
                .flatMap(Optional::stream)
                .map(artist -> {
                    Map<String, Object> value = new LinkedHashMap<>();
                    value.put("id", artist.getId());
                    value.put("name", artist.getName());
                    value.put("genderId", artist.getGenderId());
                    return value;
                })
                .toList();
    }
    
    /**
     * Save the groups that an artist belongs to
     */
    @PostMapping("/{id}/api/groups")
    @ResponseBody
    public String saveArtistGroups(@PathVariable Integer id, @RequestBody List<Integer> groupIds) {
        try {
            artistService.saveArtistGroups(id, groupIds);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    // -----------------------------------------------------------------------
    // Theme assignment endpoints (called from artist detail page)
    // -----------------------------------------------------------------------

    /**
     * GET /artists/{id}/theme-assignments
     * Returns all theme assignments for this artist.
     */
    @GetMapping("/{id}/theme-assignments")
    @ResponseBody
    public List<Map<String, Object>> getThemeAssignments(@PathVariable Integer id) {
        return themeService.getAssignmentsForArtist(id);
    }

    /**
     * POST /artists/{id}/theme-assignments
     * Body: { themeId, artistImageId (null or -1 => default image) }
     * Returns: { wasReplaced, previousImageId }
     */
    @PostMapping("/{id}/theme-assignments")
    @ResponseBody
    public Map<String, Object> assignImageToTheme(
            @PathVariable Integer id,
            @RequestBody Map<String, Object> body) {
        try {
            Integer themeId = (Integer) body.get("themeId");
            Object imgObj = body.get("artistImageId");
            Integer artistImageId = (imgObj == null || Integer.valueOf(-1).equals(imgObj) || "-1".equals(imgObj.toString()))
                    ? null
                    : (imgObj instanceof Integer ? (Integer) imgObj : Integer.parseInt(imgObj.toString()));
            Map<String, Object> result = themeService.assignImageToTheme(themeId, id, artistImageId);
            result = new java.util.HashMap<>(result);
            result.put("success", true);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return Map.of("success", false, "message", e.getMessage());
        }
    }

    /**
     * DELETE /artists/{id}/theme-assignments/{themeId}
     */
    @DeleteMapping("/{id}/theme-assignments/{themeId}")
    @ResponseBody
    public Map<String, Object> removeThemeAssignment(
            @PathVariable Integer id,
            @PathVariable Integer themeId) {
        try {
            themeService.removeAssignment(themeId, id);
            return Map.of("success", true);
        } catch (Exception e) {
            e.printStackTrace();
            return Map.of("success", false, "message", e.getMessage());
        }
    }

    private String formatAverage(Double value) {
        if (value == null || value == 0) {
            return "-";
        }
        return value == Math.floor(value) ? String.format("%.0f", value) : String.format("%.1f", value);
    }

    private int selectedArtistPlaySourceCount(boolean includeMain, boolean includeGroups, boolean includeFeatured) {
        return (includeMain ? 1 : 0) + (includeGroups ? 1 : 0) + (includeFeatured ? 1 : 0);
    }

}
