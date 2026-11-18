package library.controller;

import library.dto.AlbumCardDTO;
import library.dto.AlbumFullListenStats;
import library.dto.AlbumSongDTO;
import library.dto.ChartFilterDTO;
import library.dto.ChartHistoryDTO;
import library.dto.GenderCountDTO;
import library.entity.Album;
import library.entity.Artist;
import library.repository.LookupRepository;
import library.service.AppConfigService;
import library.service.AlbumService;
import library.service.ArtistService;
import library.service.BillboardHot100Service;
import library.service.CatalogChartService;
import library.service.ChartService;
import library.service.ChartFilterRequestFactory;
import library.service.ItunesService;
import library.service.PcService;
import library.service.TagService;
import library.service.TrlService;
import library.service.DetailAssociationsService;
import library.service.DetailChartHistoryService;
import library.service.DetailPlaysService;
import library.util.AlbumGeneralSummaries;
import library.util.DateFormatUtils;
import library.util.GraphsNavigation;
import library.util.ArtistFilterMode;
import library.util.LegacyPlays;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;

@Controller
@RequestMapping("/albums")
public class AlbumController {
    
    private final AlbumService albumService;
    private final ChartService chartService;
    private final ArtistService artistService;
    private final LookupRepository lookupRepository;
    private final ItunesService itunesService;
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

    public AlbumController(AlbumService albumService, ChartService chartService, ArtistService artistService,
                           LookupRepository lookupRepository, ItunesService itunesService, AppConfigService appConfigService,
                           BillboardHot100Service billboardHot100Service, PcService pcService, TrlService trlService,
                           CatalogChartService catalogChartService, ChartFilterRequestFactory chartFilterRequestFactory,
                           TagService tagService, DetailPlaysService detailPlaysService,
                           DetailAssociationsService detailAssociationsService,
                           DetailChartHistoryService detailChartHistoryService) {
        this.detailAssociationsService = detailAssociationsService;
        this.detailChartHistoryService = detailChartHistoryService;
        this.albumService = albumService;
        this.chartService = chartService;
        this.artistService = artistService;
        this.lookupRepository = lookupRepository;
        this.itunesService = itunesService;
        this.appConfigService = appConfigService;
        this.billboardHot100Service = billboardHot100Service;
        this.pcService = pcService;
        this.trlService = trlService;
        this.catalogChartService = catalogChartService;
        this.chartFilterRequestFactory = chartFilterRequestFactory;
        this.tagService = tagService;
        this.detailPlaysService = detailPlaysService;
    }
    
    @InitBinder
    public void initBinder(WebDataBinder binder) {
        binder.registerCustomEditor(Date.class, new java.beans.PropertyEditorSupport() {
            private final java.text.SimpleDateFormat displayFormat = new java.text.SimpleDateFormat("dd MMM yyyy");
            private final java.text.SimpleDateFormat inputFormat = new java.text.SimpleDateFormat("dd/MM/yyyy");
            
            @Override
            public void setAsText(String text) {
                if (text == null || text.trim().isEmpty()) {
                    setValue(null);
                } else {
                    try {
                        // Try input format first (dd/MM/yyyy)
                        java.util.Date parsed = inputFormat.parse(text);
                        setValue(new Date(parsed.getTime()));
                    } catch (Exception e) {
                        try {
                            // Fallback to display format (dd MMM yyyy)
                            java.util.Date parsed = displayFormat.parse(text);
                            setValue(new Date(parsed.getTime()));
                        } catch (Exception e2) {
                            // Last resort: try yyyy-MM-dd format
                            try {
                                setValue(Date.valueOf(text));
                            } catch (Exception ex) {
                                System.err.println("Failed to parse date: " + text);
                                setValue(null);
                            }
                        }
                    }
                }
            }
            
            @Override
            public String getAsText() {
                Date value = (Date) getValue();
                return value != null ? displayFormat.format(value) : "";
            }
        });
    }
    
    @GetMapping(params = "view=graphs")
    public String graphs(HttpServletRequest request) {
        return GraphsNavigation.redirectToPlayground(request, "albums");
    }

    @GetMapping
    public String listAlbums(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            @RequestParam(required = false) List<Integer> artist,
            @RequestParam(required = false) String artistMode,
            @RequestParam(required = false) List<Integer> featuredArtist,
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
            @RequestParam(required = false) List<Integer> tag,
            @RequestParam(required = false) String tagMode,
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
            @RequestParam(required = false) String organized,
            @RequestParam(required = false) Integer imageCountMin,
            @RequestParam(required = false) Integer imageCountMax,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
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
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) Integer songCountMin,
            @RequestParam(required = false) Integer songCountMax,
            @RequestParam(required = false) Integer lengthMin,
            @RequestParam(required = false) Integer lengthMax,
            @RequestParam(required = false) String lengthMode,
            @RequestParam(required = false) Integer weeklyChartPeak,
            @RequestParam(required = false) String weeklyChartPeakMode,
            @RequestParam(required = false) Integer weeklyChartWeeks,
            @RequestParam(required = false) Integer weeklyChartPeakWeeks,
            @RequestParam(required = false) String weeklyChartPeakWeeksMode,
            @RequestParam(required = false) String weeklyChartDateFrom,
            @RequestParam(required = false) String weeklyChartDateTo,
            @RequestParam(required = false) String weeklyChartSeason,
            @RequestParam(required = false) Integer seasonalChartPeak,
            @RequestParam(required = false) Integer seasonalChartSeasons,
            @RequestParam(required = false) String seasonalChartDateFrom,
            @RequestParam(required = false) String seasonalChartDateTo,
            @RequestParam(required = false) String seasonalChartSeason,
            @RequestParam(required = false) Integer yearlyChartPeak,
            @RequestParam(required = false) Integer yearlyChartYears,
            @RequestParam(required = false) String yearlyChartDateFrom,
            @RequestParam(required = false) String yearlyChartDateTo,
            @RequestParam(required = false) String firstFullListenDate,
            @RequestParam(required = false) String firstFullListenDateFrom,
            @RequestParam(required = false) String firstFullListenDateTo,
            @RequestParam(required = false) String firstFullListenDateMode,
            @RequestParam(required = false) String lastFullListenDate,
            @RequestParam(required = false) String lastFullListenDateFrom,
            @RequestParam(required = false) String lastFullListenDateTo,
            @RequestParam(required = false) String lastFullListenDateMode,
            @RequestParam(required = false) Integer fullAlbumPlaysMin,
            @RequestParam(required = false) Integer fullAlbumPlaysMax,
            @RequestParam(required = false) Integer itunesPresenceMin,
            @RequestParam(required = false) Integer itunesPresenceMax,
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
            HttpServletRequest request,
            Model model) {
        
        // Convert date formats from dd/mm/yyyy to yyyy-MM-dd for database queries
        String releaseDateConverted = DateFormatUtils.convertToIsoFormat(releaseDate);
        String releaseDateFromConverted = DateFormatUtils.convertToIsoFormat(releaseDateFrom);
        String releaseDateToConverted = DateFormatUtils.convertToIsoFormat(releaseDateTo);
        String firstListenedDateConverted = DateFormatUtils.convertToIsoFormat(firstListenedDate);
        String firstListenedDateFromConverted = DateFormatUtils.convertToIsoFormat(firstListenedDateFrom);
        String firstListenedDateToConverted = DateFormatUtils.convertToIsoFormat(firstListenedDateTo);
        String lastListenedDateConverted = DateFormatUtils.convertToIsoFormat(lastListenedDate);
        String lastListenedDateFromConverted = DateFormatUtils.convertToIsoFormat(lastListenedDateFrom);
        String lastListenedDateToConverted = DateFormatUtils.convertToIsoFormat(lastListenedDateTo);
        String firstFullListenDateConverted = DateFormatUtils.convertToIsoFormat(firstFullListenDate);
        String firstFullListenDateFromConverted = DateFormatUtils.convertToIsoFormat(firstFullListenDateFrom);
        String firstFullListenDateToConverted = DateFormatUtils.convertToIsoFormat(firstFullListenDateTo);
        String lastFullListenDateConverted = DateFormatUtils.convertToIsoFormat(lastFullListenDate);
        String lastFullListenDateFromConverted = DateFormatUtils.convertToIsoFormat(lastFullListenDateFrom);
        String lastFullListenDateToConverted = DateFormatUtils.convertToIsoFormat(lastFullListenDateTo);
        String listenedDateFromConverted = DateFormatUtils.convertToIsoFormat(listenedDateFrom);
        String listenedDateToConverted = DateFormatUtils.convertToIsoFormat(listenedDateTo);
        String birthDateConverted = DateFormatUtils.convertToIsoFormat(birthDate);
        String birthDateFromConverted = DateFormatUtils.convertToIsoFormat(birthDateFrom);
        String birthDateToConverted = DateFormatUtils.convertToIsoFormat(birthDateTo);
        String deathDateConverted = DateFormatUtils.convertToIsoFormat(deathDate);
        String deathDateFromConverted = DateFormatUtils.convertToIsoFormat(deathDateFrom);
        String deathDateToConverted = DateFormatUtils.convertToIsoFormat(deathDateTo);
        String weeklyChartDateFromConverted = DateFormatUtils.convertToIsoFormat(weeklyChartDateFrom);
        String weeklyChartDateToConverted = DateFormatUtils.convertToIsoFormat(weeklyChartDateTo);
        String seasonalChartDateFromConverted = DateFormatUtils.convertToIsoFormat(seasonalChartDateFrom);
        String seasonalChartDateToConverted = DateFormatUtils.convertToIsoFormat(seasonalChartDateTo);
        String yearlyChartDateFromConverted = DateFormatUtils.convertToIsoFormat(yearlyChartDateFrom);
        String yearlyChartDateToConverted = DateFormatUtils.convertToIsoFormat(yearlyChartDateTo);
        int effectivePerPage = lazyCard ? 1
                : filtersOnly ? 1
                : appConfigService.normalizePageSize(perpage, appConfigService.getAlbumsListPageSize());
        List<Integer> artistFilter = ArtistFilterMode.encode(artist, artistMode);
        
        // Pre-compute iTunes album IDs once for all 3 queries
        String itunesIdsJson = filtersOnly ? null : albumService.getItunesAlbumIdsJson(inItunes);
        
        // Get filtered and sorted albums
        List<AlbumCardDTO> albums = filtersOnly ? List.of() : albumService.getAlbums(
                q, artistFilter, featuredArtist, genre, genreMode, subgenre, subgenreMode,
                language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
                country, countryMode, tag, tagMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand,
                ageMin, ageMax, ageMode, ageAtReleaseMin, ageAtReleaseMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                itunesIdsJson, inItunes,
                playCountMin, playCountMax, songCountMin, songCountMax,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartPeakMode, weeklyChartWeeks, weeklyChartPeakWeeks, weeklyChartPeakWeeksMode, weeklyChartDateFromConverted, weeklyChartDateToConverted, weeklyChartSeason,
                seasonalChartPeak, seasonalChartSeasons, seasonalChartDateFromConverted, seasonalChartDateToConverted, seasonalChartSeason,
                yearlyChartPeak, yearlyChartYears, yearlyChartDateFromConverted, yearlyChartDateToConverted,
                firstFullListenDateConverted, firstFullListenDateFromConverted, firstFullListenDateToConverted, firstFullListenDateMode,
                lastFullListenDateConverted, lastFullListenDateFromConverted, lastFullListenDateToConverted, lastFullListenDateMode,
                fullAlbumPlaysMin, fullAlbumPlaysMax,
                itunesPresenceMin, itunesPresenceMax,
                false,
                sortby, sortdir, sortby2, sortdir2, sortby3, sortdir3, randomSeed, page, effectivePerPage,
                includeExtendedStats
        );
        
        // Get total count for pagination
        long totalCount = filtersOnly || lazyCard ? albums.size() : albumService.countAlbums(q, artistFilter, featuredArtist, genre,
                genreMode, subgenre, subgenreMode, language, languageMode, gender, 
                genderMode, ethnicity, ethnicityMode, country, countryMode, tag, tagMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand,
                ageMin, ageMax, ageMode, ageAtReleaseMin, ageAtReleaseMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                itunesIdsJson, inItunes,
                playCountMin, playCountMax, songCountMin, songCountMax,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartPeakMode, weeklyChartWeeks, weeklyChartPeakWeeks, weeklyChartPeakWeeksMode, weeklyChartDateFromConverted, weeklyChartDateToConverted, weeklyChartSeason,
                seasonalChartPeak, seasonalChartSeasons, seasonalChartDateFromConverted, seasonalChartDateToConverted, seasonalChartSeason,
                yearlyChartPeak, yearlyChartYears, yearlyChartDateFromConverted, yearlyChartDateToConverted,
                firstFullListenDateConverted, firstFullListenDateFromConverted, firstFullListenDateToConverted, firstFullListenDateMode,
                lastFullListenDateConverted, lastFullListenDateFromConverted, lastFullListenDateToConverted, lastFullListenDateMode,
                fullAlbumPlaysMin, fullAlbumPlaysMax,
                itunesPresenceMin, itunesPresenceMax);
        int totalPages = (int) Math.ceil((double) totalCount / effectivePerPage);
        
        // Get gender counts for the filtered dataset
        GenderCountDTO genderCounts = filtersOnly || lazyCard ? new GenderCountDTO() : albumService.countAlbumsByGender(q, artistFilter, featuredArtist, genre,
                genreMode, subgenre, subgenreMode, language, languageMode, gender, 
                genderMode, ethnicity, ethnicityMode, country, countryMode, tag, tagMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand,
                ageMin, ageMax, ageMode, ageAtReleaseMin, ageAtReleaseMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                itunesIdsJson, inItunes,
                playCountMin, playCountMax, songCountMin, songCountMax,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartPeakMode, weeklyChartWeeks, weeklyChartPeakWeeks, weeklyChartPeakWeeksMode, weeklyChartDateFromConverted, weeklyChartDateToConverted, weeklyChartSeason,
                seasonalChartPeak, seasonalChartSeasons, seasonalChartDateFromConverted, seasonalChartDateToConverted, seasonalChartSeason,
                yearlyChartPeak, yearlyChartYears, yearlyChartDateFromConverted, yearlyChartDateToConverted,
                firstFullListenDateConverted, firstFullListenDateFromConverted, firstFullListenDateToConverted, firstFullListenDateMode,
                lastFullListenDateConverted, lastFullListenDateFromConverted, lastFullListenDateToConverted, lastFullListenDateMode,
                fullAlbumPlaysMin, fullAlbumPlaysMax,
                itunesPresenceMin, itunesPresenceMax);
        
        // Add data to model
        model.addAttribute("currentSection", "albums");
        model.addAttribute("albums", albums);
        if (lazyCard && includeExtendedStats && !albums.isEmpty()) {
            albumService.fillListenedSongs(albums.getFirst());
        }
        // Ranks are shown in the More panel, which loads one card at a time (lazyCard), so only rank that card
        model.addAttribute("albumRankings", lazyCard && !albums.isEmpty()
                ? java.util.Map.of(albums.getFirst().getId(), albumService.getAllAlbumRankings(albums.getFirst().getId()))
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
        model.addAttribute("selectedArtist", artist);
        model.addAttribute("artistMode", "excludes".equalsIgnoreCase(artistMode) ? "excludes" : "includes");
        List<Map<String, Object>> artistDetails = (artist != null && !artist.isEmpty()) ?
            artistService.getArtistDetailsForIds(artist) : List.of();
        model.addAttribute("selectedArtistDetails", artistDetails);
        model.addAttribute("selectedFeaturedArtist", featuredArtist);
        List<Map<String, Object>> featuredArtistDetails = (featuredArtist != null && !featuredArtist.isEmpty()) ?
            artistService.getArtistDetailsForIds(featuredArtist) : List.of();
        model.addAttribute("selectedFeaturedArtistDetails", featuredArtistDetails);
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
        model.addAttribute("selectedTags", tag);
        model.addAttribute("tagMode", tagMode != null ? tagMode : "includes");
        model.addAttribute("selectedAccounts", account);
        model.addAttribute("accountMode", accountMode != null ? accountMode : "includes");
        model.addAttribute("ageMin", ageMin);
        model.addAttribute("ageMax", ageMax);
        model.addAttribute("ageMode", ageMode);
        model.addAttribute("birthDate", birthDate);
        model.addAttribute("birthDateFrom", birthDateFrom);
        model.addAttribute("birthDateTo", birthDateTo);
        model.addAttribute("birthDateMode", birthDateMode);
        model.addAttribute("birthDateFormatted", DateFormatUtils.convertToDisplayFormat(birthDate));
        model.addAttribute("birthDateFromFormatted", DateFormatUtils.convertToDisplayFormat(birthDateFrom));
        model.addAttribute("birthDateToFormatted", DateFormatUtils.convertToDisplayFormat(birthDateTo));
        model.addAttribute("deathDate", deathDate);
        model.addAttribute("deathDateFrom", deathDateFrom);
        model.addAttribute("deathDateTo", deathDateTo);
        model.addAttribute("deathDateMode", deathDateMode);
        model.addAttribute("deathDateFormatted", DateFormatUtils.convertToDisplayFormat(deathDate));
        model.addAttribute("deathDateFromFormatted", DateFormatUtils.convertToDisplayFormat(deathDateFrom));
        model.addAttribute("deathDateToFormatted", DateFormatUtils.convertToDisplayFormat(deathDateTo));
        model.addAttribute("selectedOrganized", organized);
        model.addAttribute("imageCountMin", imageCountMin);
        model.addAttribute("imageCountMax", imageCountMax);
        model.addAttribute("selectedInItunes", inItunes);
        model.addAttribute("selectedHasFeaturedArtists", hasFeaturedArtists);
        model.addAttribute("selectedIsBand", isBand);
        model.addAttribute("playCountMin", playCountMin);
        model.addAttribute("playCountMax", playCountMax);
        model.addAttribute("songCountMin", songCountMin);
        model.addAttribute("songCountMax", songCountMax);
        model.addAttribute("itunesPresenceMin", itunesPresenceMin);
        model.addAttribute("itunesPresenceMax", itunesPresenceMax);
        model.addAttribute("lengthMin", lengthMin);
        model.addAttribute("lengthMax", lengthMax);
        model.addAttribute("lengthMode", lengthMode != null ? lengthMode : "range");
        
        // Chart filter attributes
        model.addAttribute("weeklyChartPeak", weeklyChartPeak);
        model.addAttribute("weeklyChartPeakMode", weeklyChartPeakMode != null ? weeklyChartPeakMode : "top");
        model.addAttribute("weeklyChartWeeks", weeklyChartWeeks);
        model.addAttribute("weeklyChartPeakWeeks", weeklyChartPeakWeeks);
        model.addAttribute("weeklyChartPeakWeeksMode", weeklyChartPeakWeeksMode != null ? weeklyChartPeakWeeksMode : "gte");
        model.addAttribute("weeklyChartDateFrom", weeklyChartDateFrom);
        model.addAttribute("weeklyChartDateTo", weeklyChartDateTo);
        model.addAttribute("weeklyChartSeason", weeklyChartSeason);
        model.addAttribute("seasonalChartPeak", seasonalChartPeak);
        model.addAttribute("seasonalChartSeasons", seasonalChartSeasons);
        model.addAttribute("seasonalChartDateFrom", seasonalChartDateFrom);
        model.addAttribute("seasonalChartDateTo", seasonalChartDateTo);
        model.addAttribute("seasonalChartSeason", seasonalChartSeason);
        model.addAttribute("yearlyChartPeak", yearlyChartPeak);
        model.addAttribute("yearlyChartYears", yearlyChartYears);
        model.addAttribute("yearlyChartDateFrom", yearlyChartDateFrom);
        model.addAttribute("yearlyChartDateTo", yearlyChartDateTo);
        
        // Release date filter attributes
        model.addAttribute("releaseDate", releaseDate);
        model.addAttribute("releaseDateFrom", releaseDateFrom);
        model.addAttribute("releaseDateTo", releaseDateTo);
        model.addAttribute("releaseDateMode", releaseDateMode != null ? releaseDateMode : "exact");
        
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
        
        // Last full listen date filter attributes
        model.addAttribute("firstFullListenDate", firstFullListenDate);
        model.addAttribute("firstFullListenDateFrom", firstFullListenDateFrom);
        model.addAttribute("firstFullListenDateTo", firstFullListenDateTo);
        model.addAttribute("firstFullListenDateMode", firstFullListenDateMode != null ? firstFullListenDateMode : "exact");
        model.addAttribute("firstFullListenDateFormatted", DateFormatUtils.formatIsoDateForDisplay(firstFullListenDate));
        model.addAttribute("firstFullListenDateFromFormatted", DateFormatUtils.formatIsoDateForDisplay(firstFullListenDateFrom));
        model.addAttribute("firstFullListenDateToFormatted", DateFormatUtils.formatIsoDateForDisplay(firstFullListenDateTo));
        model.addAttribute("lastFullListenDate", lastFullListenDate);
        model.addAttribute("lastFullListenDateFrom", lastFullListenDateFrom);
        model.addAttribute("lastFullListenDateTo", lastFullListenDateTo);
        model.addAttribute("lastFullListenDateMode", lastFullListenDateMode != null ? lastFullListenDateMode : "exact");
        model.addAttribute("lastFullListenDateFormatted", DateFormatUtils.formatIsoDateForDisplay(lastFullListenDate));
        model.addAttribute("lastFullListenDateFromFormatted", DateFormatUtils.formatIsoDateForDisplay(lastFullListenDateFrom));
        model.addAttribute("lastFullListenDateToFormatted", DateFormatUtils.formatIsoDateForDisplay(lastFullListenDateTo));
        model.addAttribute("fullAlbumPlaysMin", fullAlbumPlaysMin);
        model.addAttribute("fullAlbumPlaysMax", fullAlbumPlaysMax);
        
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
        model.addAttribute("includeFullListenStats", false);
        model.addAttribute("hasActiveFilters", hasActiveFilters(request));
        
        // Add filter options
        model.addAttribute("genres", albumService.getGenres());
        model.addAttribute("subgenres", albumService.getSubGenres());
        model.addAttribute("languages", albumService.getLanguages());
        model.addAttribute("genders", albumService.getGenders());
        model.addAttribute("ethnicities", albumService.getEthnicities());
        model.addAttribute("countries", albumService.getCountries());
        model.addAttribute("tags", tagService.getAllTags());
        
        return filtersOnly ? "albums/list :: #filterForm" : "albums/list";
    }

    @GetMapping("/api")
    @ResponseBody
    public Map<String, Object> listAlbumsApi(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            @RequestParam(required = false) List<Integer> artist,
            @RequestParam(required = false) String artistMode,
            @RequestParam(required = false) List<Integer> featuredArtist,
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
            @RequestParam(required = false) List<Integer> tag,
            @RequestParam(required = false) String tagMode,
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
            @RequestParam(required = false) String organized,
            @RequestParam(required = false) Integer imageCountMin,
            @RequestParam(required = false) Integer imageCountMax,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
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
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) Integer songCountMin,
            @RequestParam(required = false) Integer songCountMax,
            @RequestParam(required = false) Integer lengthMin,
            @RequestParam(required = false) Integer lengthMax,
            @RequestParam(required = false) String lengthMode,
            @RequestParam(required = false) Integer weeklyChartPeak,
            @RequestParam(required = false) String weeklyChartPeakMode,
            @RequestParam(required = false) Integer weeklyChartWeeks,
            @RequestParam(required = false) Integer weeklyChartPeakWeeks,
            @RequestParam(required = false) String weeklyChartPeakWeeksMode,
            @RequestParam(required = false) String weeklyChartDateFrom,
            @RequestParam(required = false) String weeklyChartDateTo,
            @RequestParam(required = false) String weeklyChartSeason,
            @RequestParam(required = false) Integer seasonalChartPeak,
            @RequestParam(required = false) Integer seasonalChartSeasons,
            @RequestParam(required = false) String seasonalChartDateFrom,
            @RequestParam(required = false) String seasonalChartDateTo,
            @RequestParam(required = false) String seasonalChartSeason,
            @RequestParam(required = false) Integer yearlyChartPeak,
            @RequestParam(required = false) Integer yearlyChartYears,
            @RequestParam(required = false) String yearlyChartDateFrom,
            @RequestParam(required = false) String yearlyChartDateTo,
            @RequestParam(required = false) String firstFullListenDate,
            @RequestParam(required = false) String firstFullListenDateFrom,
            @RequestParam(required = false) String firstFullListenDateTo,
            @RequestParam(required = false) String firstFullListenDateMode,
            @RequestParam(required = false) String lastFullListenDate,
            @RequestParam(required = false) String lastFullListenDateFrom,
            @RequestParam(required = false) String lastFullListenDateTo,
            @RequestParam(required = false) String lastFullListenDateMode,
            @RequestParam(required = false) Integer fullAlbumPlaysMin,
            @RequestParam(required = false) Integer fullAlbumPlaysMax,
            @RequestParam(required = false) Integer itunesPresenceMin,
            @RequestParam(required = false) Integer itunesPresenceMax,
            @RequestParam(defaultValue = "false") boolean includeFullListenStats,
            @RequestParam(defaultValue = "false") boolean includeExtendedStats,
            @RequestParam(defaultValue = "plays") String sortby,
            @RequestParam(defaultValue = "desc") String sortdir,
            @RequestParam(required = false) String sortby2,
            @RequestParam(required = false) String sortdir2,
            @RequestParam(required = false) String sortby3,
            @RequestParam(required = false) String sortdir3,
            @RequestParam(required = false) Integer randomSeed,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(required = false) Integer perpage) {

        String releaseDateConverted = DateFormatUtils.convertToIsoFormat(releaseDate);
        String releaseDateFromConverted = DateFormatUtils.convertToIsoFormat(releaseDateFrom);
        String releaseDateToConverted = DateFormatUtils.convertToIsoFormat(releaseDateTo);
        String firstListenedDateConverted = DateFormatUtils.convertToIsoFormat(firstListenedDate);
        String firstListenedDateFromConverted = DateFormatUtils.convertToIsoFormat(firstListenedDateFrom);
        String firstListenedDateToConverted = DateFormatUtils.convertToIsoFormat(firstListenedDateTo);
        String lastListenedDateConverted = DateFormatUtils.convertToIsoFormat(lastListenedDate);
        String lastListenedDateFromConverted = DateFormatUtils.convertToIsoFormat(lastListenedDateFrom);
        String lastListenedDateToConverted = DateFormatUtils.convertToIsoFormat(lastListenedDateTo);
        String firstFullListenDateConverted = DateFormatUtils.convertToIsoFormat(firstFullListenDate);
        String firstFullListenDateFromConverted = DateFormatUtils.convertToIsoFormat(firstFullListenDateFrom);
        String firstFullListenDateToConverted = DateFormatUtils.convertToIsoFormat(firstFullListenDateTo);
        String lastFullListenDateConverted = DateFormatUtils.convertToIsoFormat(lastFullListenDate);
        String lastFullListenDateFromConverted = DateFormatUtils.convertToIsoFormat(lastFullListenDateFrom);
        String lastFullListenDateToConverted = DateFormatUtils.convertToIsoFormat(lastFullListenDateTo);
        String listenedDateFromConverted = DateFormatUtils.convertToIsoFormat(listenedDateFrom);
        String listenedDateToConverted = DateFormatUtils.convertToIsoFormat(listenedDateTo);
        String birthDateConverted = DateFormatUtils.convertToIsoFormat(birthDate);
        String birthDateFromConverted = DateFormatUtils.convertToIsoFormat(birthDateFrom);
        String birthDateToConverted = DateFormatUtils.convertToIsoFormat(birthDateTo);
        String deathDateConverted = DateFormatUtils.convertToIsoFormat(deathDate);
        String deathDateFromConverted = DateFormatUtils.convertToIsoFormat(deathDateFrom);
        String deathDateToConverted = DateFormatUtils.convertToIsoFormat(deathDateTo);
        String weeklyChartDateFromConverted = DateFormatUtils.convertToIsoFormat(weeklyChartDateFrom);
        String weeklyChartDateToConverted = DateFormatUtils.convertToIsoFormat(weeklyChartDateTo);
        String seasonalChartDateFromConverted = DateFormatUtils.convertToIsoFormat(seasonalChartDateFrom);
        String seasonalChartDateToConverted = DateFormatUtils.convertToIsoFormat(seasonalChartDateTo);
        String yearlyChartDateFromConverted = DateFormatUtils.convertToIsoFormat(yearlyChartDateFrom);
        String yearlyChartDateToConverted = DateFormatUtils.convertToIsoFormat(yearlyChartDateTo);
        int effectivePerPage = appConfigService.normalizePageSize(perpage, appConfigService.getAlbumsListPageSize());
        List<Integer> artistFilter = ArtistFilterMode.encode(artist, artistMode);

        String itunesIdsJson = albumService.getItunesAlbumIdsJson(inItunes);

        List<AlbumCardDTO> albums = albumService.getAlbums(
                q, artistFilter, featuredArtist, genre, genreMode, subgenre, subgenreMode,
                language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
                country, countryMode, tag, tagMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand,
                ageMin, ageMax, ageMode, ageAtReleaseMin, ageAtReleaseMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                itunesIdsJson, inItunes,
                playCountMin, playCountMax, songCountMin, songCountMax,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartPeakMode, weeklyChartWeeks, weeklyChartPeakWeeks, weeklyChartPeakWeeksMode, weeklyChartDateFromConverted, weeklyChartDateToConverted, weeklyChartSeason,
                seasonalChartPeak, seasonalChartSeasons, seasonalChartDateFromConverted, seasonalChartDateToConverted, seasonalChartSeason,
                yearlyChartPeak, yearlyChartYears, yearlyChartDateFromConverted, yearlyChartDateToConverted,
                firstFullListenDateConverted, firstFullListenDateFromConverted, firstFullListenDateToConverted, firstFullListenDateMode,
                lastFullListenDateConverted, lastFullListenDateFromConverted, lastFullListenDateToConverted, lastFullListenDateMode,
                fullAlbumPlaysMin, fullAlbumPlaysMax,
                itunesPresenceMin, itunesPresenceMax,
                includeFullListenStats,
                sortby, sortdir, sortby2, sortdir2, sortby3, sortdir3, randomSeed, page, effectivePerPage,
                includeExtendedStats
        );

        long totalCount = albumService.countAlbums(q, artistFilter, featuredArtist, genre,
                genreMode, subgenre, subgenreMode, language, languageMode, gender,
                genderMode, ethnicity, ethnicityMode, country, countryMode, tag, tagMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand,
                ageMin, ageMax, ageMode, ageAtReleaseMin, ageAtReleaseMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                itunesIdsJson, inItunes,
                playCountMin, playCountMax, songCountMin, songCountMax,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartPeakMode, weeklyChartWeeks, weeklyChartPeakWeeks, weeklyChartPeakWeeksMode, weeklyChartDateFromConverted, weeklyChartDateToConverted, weeklyChartSeason,
                seasonalChartPeak, seasonalChartSeasons, seasonalChartDateFromConverted, seasonalChartDateToConverted, seasonalChartSeason,
                yearlyChartPeak, yearlyChartYears, yearlyChartDateFromConverted, yearlyChartDateToConverted,
                firstFullListenDateConverted, firstFullListenDateFromConverted, firstFullListenDateToConverted, firstFullListenDateMode,
                lastFullListenDateConverted, lastFullListenDateFromConverted, lastFullListenDateToConverted, lastFullListenDateMode,
                fullAlbumPlaysMin, fullAlbumPlaysMax,
                itunesPresenceMin, itunesPresenceMax);

        Map<String, Object> result = new HashMap<>();
        result.put("items", albums);
        result.put("totalCount", totalCount);
        result.put("page", page);
        result.put("perPage", effectivePerPage);
        return result;
    }

    @GetMapping("/api/charts/{tab}")
    @ResponseBody
    public Map<String, Object> albumChartsData(
            @PathVariable String tab,
            HttpServletRequest request,
            @RequestParam(defaultValue = "false") boolean includeGroups,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam(defaultValue = "0") int limit,
            @RequestParam(required = false) String limitEntity) {

        ChartFilterDTO filter = chartFilterRequestFactory.build(request, includeGroups, includeFeatured, limit, limitEntity, "album");

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
        return detailPlaysService.rankMoves(DetailPlaysService.Kind.ALBUM, id);
    }

    @GetMapping("/{id}")
    public String viewAlbum(@PathVariable Integer id, 
                           @RequestParam(defaultValue = "general") String tab,
                           @RequestParam(defaultValue = "0") int playsPage,
                           Model model) {
        Optional<Album> album = albumService.getAlbumById(id);
        
        if (album.isEmpty()) {
            return "redirect:/albums";
        }
        
        // Check if album has an image
        boolean hasImage = albumService.hasAlbumImage(id);
        
        // Get song count
        int songCount = albumService.getSongCount(id);
        
        // Get artist name and gender
        String artistName = albumService.getArtistName(album.get().getArtistId());
        String artistGender = albumService.getArtistGender(album.get().getArtistId());
        String artistCountry = albumService.getArtistCountry(album.get().getArtistId());
        String artistEthnicityName = albumService.getArtistEthnicityName(album.get().getArtistId());
        
        model.addAttribute("currentSection", "albums");
        model.addAttribute("album", album.get());
        model.addAttribute("hasImage", hasImage);
        model.addAttribute("artistName", artistName);
        model.addAttribute("artistGender", artistGender);
        model.addAttribute("artistCountry", artistCountry);
        model.addAttribute("artistEthnicityName", artistEthnicityName);
        
        // Artist entity for the sidebar profile ranks and release age
        Artist artist = artistService.findById(album.get().getArtistId());
        model.addAttribute("artist", artist);
        
        // Add iTunes presence
        Album albumEntity = album.get();
        albumEntity.setInItunes(itunesService.albumExistsInItunes(artistName, albumEntity.getName()));
        model.addAttribute("album", albumEntity);
        
        // Add lookup maps
        Map<Integer, String> genres = albumService.getGenres();
        Map<Integer, String> subgenres = albumService.getSubGenres();
        Map<Integer, String> languages = albumService.getLanguages();
        Map<Integer, String> genders = albumService.getGenders();
        Map<Integer, String> ethnicities = albumService.getEthnicities();
        
        model.addAttribute("genres", genres);
        model.addAttribute("subgenres", subgenres);
        model.addAttribute("languages", languages);
        model.addAttribute("genders", genders);
        model.addAttribute("ethnicities", ethnicities);
        model.addAttribute("tagOptions", tagService.getAllTagOptions());
        model.addAttribute("assignedTags", tagService.getAlbumTags(id));
        
        // Add effective (resolved) value names for display
        Album a = album.get();
        model.addAttribute("effectiveGenreName", a.getEffectiveGenreId() != null ? genres.get(a.getEffectiveGenreId()) : null);
        model.addAttribute("effectiveSubgenreName", a.getEffectiveSubgenreId() != null ? subgenres.get(a.getEffectiveSubgenreId()) : null);
        model.addAttribute("effectiveLanguageName", a.getEffectiveLanguageId() != null ? languages.get(a.getEffectiveLanguageId()) : null);
        
        // Add inherited value names (what would be used if no album override) for dropdown "Inherit" options
        model.addAttribute("inheritedGenreName", a.getArtistGenreId() != null ? genres.get(a.getArtistGenreId()) : null);
        model.addAttribute("inheritedSubgenreName", a.getArtistSubgenreId() != null ? subgenres.get(a.getArtistSubgenreId()) : null);
        model.addAttribute("inheritedLanguageName", a.getArtistLanguageId() != null ? languages.get(a.getArtistLanguageId()) : null);
        
        // NEW: add album play count
        model.addAttribute("albumPlayCount", albumService.getPlayCountForAlbum(id));
        model.addAttribute("albumVatitoPlayCount", albumService.getVatitoPlayCountForAlbum(id));
        int albumLegacyPlays = albumService.getRobertloverPlayCountForAlbum(id);
        model.addAttribute("albumRobertloverPlayCount", albumLegacyPlays);

        // Add album length formatted
        model.addAttribute("albumLengthFormatted", albumService.getAlbumLengthFormatted(id));
        
        // Add average song length and average plays per song for statistics
        model.addAttribute("averageSongLength", albumService.getAverageSongLengthFormatted(id));
        model.addAttribute("averagePlaysPerSong", albumService.getAveragePlaysPerSong(id));
        
        // Add songs list, and how the album plays split across its songs
        List<AlbumSongDTO> songs = albumService.getSongsForAlbum(id);
        model.addAttribute("songs", songs);
        model.addAttribute("songCount", songCount);
        model.addAttribute("playsShare", AlbumGeneralSummaries.playsShare(songs));
        model.addAttribute("albumCatalog", AlbumGeneralSummaries.catalog(songs, a.getArtistId()));
        
        // Add total listening time for the album
        model.addAttribute("totalListeningTime", albumService.getTotalListeningTimeForAlbum(id));
        
        // Add first and last listened dates for the album
        String albumFirstListened = albumService.getFirstListenedDateForAlbum(id);
        model.addAttribute("firstListenedDate", albumFirstListened);
        model.addAttribute("lastListenedDate", albumService.getLastListenedDateForAlbum(id));
        model.addAttribute("showLegacy", LegacyPlays.worthShowing(albumLegacyPlays,
                a.getReleaseDate() != null ? a.getReleaseDate().toLocalDate() : null,
                DateFormatUtils.parseDisplayDate(albumFirstListened)));
        boolean fullListenEligible = songCount > 4;
        AlbumFullListenStats fullListenStats = fullListenEligible
                ? albumService.getFullListenStatsForAlbum(id)
                : new AlbumFullListenStats(null, null, 0);
        model.addAttribute("fullListenEligible", fullListenEligible);
        model.addAttribute("firstFullListenDate", fullListenStats.firstFullListenDate() != null
                ? DateFormatUtils.formatIsoDateForDisplay(fullListenStats.firstFullListenDate()) : null);
        model.addAttribute("lastFullListenDate", fullListenStats.lastFullListenDate() != null
                ? DateFormatUtils.formatIsoDateForDisplay(fullListenStats.lastFullListenDate()) : null);
        model.addAttribute("fullAlbumPlays", fullListenStats.fullAlbumPlays());
        model.addAttribute("listeningSpan", DateFormatUtils.spanLabel(
                (String) model.getAttribute("firstListenedDate"), (String) model.getAttribute("lastListenedDate")));
        model.addAttribute("fullListenSpan", DateFormatUtils.spanLabel(
                (String) model.getAttribute("firstFullListenDate"), (String) model.getAttribute("lastFullListenDate")));
        
        // Consistency ledger: periods with plays out of the periods since the first play; ranks load in the background
        model.addAttribute("consistencyPeriods", library.dto.ConsistencyPeriodDTO.of(
                albumService.getUniqueDaysPlayedForAlbum(id), albumService.getUniqueWeeksPlayedForAlbum(id),
                albumService.getUniqueMonthsPlayedForAlbum(id), albumService.getUniqueYearsPlayedForAlbum(id),
                albumService.getFirstListenedDateAsLocalDateForAlbum(id)));
        model.addAttribute("consistencyRankUrl", "/albums/" + id + "/consistency");
        
        // Tab and plays data
        model.addAttribute("activeTab", tab);
        java.util.Map<Integer, Integer[]> yearRanks = detailPlaysService.yearRanks(DetailPlaysService.Kind.ALBUM, id);
        model.addAttribute("ranksByYear", DetailPlaysService.overallByYear(yearRanks));
        model.addAttribute("playsTab", detailPlaysService.build(detailPlaysService.albumScope(id), yearRanks));
        
        // Seasonal/yearly chart history (General tab "Album charts" and Chart History tab)
        List<Map<String, Object>> seasonalChartHistory = chartService.getChartHistoryForItem(id, "album", "seasonal");
        List<Map<String, Object>> yearlyChartHistory = chartService.getChartHistoryForItem(id, "album", "yearly");
        model.addAttribute("seasonalChartHistory", seasonalChartHistory);
        model.addAttribute("yearlyChartHistory", yearlyChartHistory);
        
        // Featured Artists tab (always pre-loaded); the General tab catalog counts its guests too
        model.addAttribute("albumAssociations", detailAssociationsService.albumTab(id));
        
        // Always load plays data (eager loading for all tabs)
        int pageSize = appConfigService.getAlbumDetailPlaysPageSize();
        model.addAttribute("plays", albumService.getPlaysForAlbum(id, playsPage, pageSize));
        long playsTotalCount = albumService.countPlaysForAlbum(id);
        model.addAttribute("playsTotalCount", playsTotalCount);
        model.addAttribute("playsPage", playsPage);
        model.addAttribute("playsTotalPages", (int) Math.ceil((double) playsTotalCount / pageSize));
        
        // Always load chart history data (eager loading for all tabs)
        List<ChartHistoryDTO> chartHistory = chartService.getAlbumChartHistory(id);
        List<Map<String, Object>> allSongsWeeklyStats = chartService.getAlbumAllSongsWithWeeklyStats(id);
        // Chart History tab: weekly album + songs, seasonal/yearly placements and the Hot 100 / Cuntdown / TRL runs
        model.addAttribute("chartHistoryView", detailChartHistoryService.forAlbum(id, albumEntity.getName(),
                chartHistory, allSongsWeeklyStats, seasonalChartHistory, yearlyChartHistory,
                chartService.getAlbumSongsChartHistoryByPeriodType(id, "seasonal"),
                chartService.getAlbumSongsChartHistoryByPeriodType(id, "yearly"),
                billboardHot100Service.getChartedSongsByAlbumId(id), pcService.getChartedSongsByAlbumId(id),
                trlService.getChartedSongsByAlbumId(id)));
        
        // Sidebar profile ranks by attribute - optimized single query
        java.util.Map<String, Integer> rankings = albumService.getAllAlbumRankings(id);
        model.addAttribute("rankByGender", rankings.get("gender"));
        model.addAttribute("rankByGenre", rankings.get("genre"));
        model.addAttribute("rankBySubgenre", rankings.get("subgenre"));
        model.addAttribute("rankByEthnicity", rankings.get("ethnicity"));
        model.addAttribute("rankByLanguage", rankings.get("language"));
        model.addAttribute("rankByCountry", rankings.get("country"));
        
        // Add Spanish Rap rank (special combination)
        if (albumService.isAlbumSpanishRap(id)) {
            model.addAttribute("rankBySpanishRap", albumService.getAlbumSpanishRapRank(id));
            model.addAttribute("rapGenreId", lookupRepository.getGenreIdByName("Rap"));
            model.addAttribute("spanishLanguageId", lookupRepository.getLanguageIdByName("Spanish"));
        }

        // Weekly chart summary, overall standing, release-year and artist ranks
        model.addAttribute("weeklyChartStats", chartService.getAlbumWeeklyChartStats(id));
        model.addAttribute("overallPosition", albumService.getAlbumOverallPosition(id));
        model.addAttribute("totalAlbumCount", albumService.getAlbumCount());
        model.addAttribute("rankByReleaseYear", albumService.getAlbumRankByReleaseYear(id));
        model.addAttribute("releaseYear", albumService.getAlbumReleaseYear(id));
        model.addAttribute("rankByArtist", albumService.getAlbumRankByArtist(id));

        // Extended stats for detail page
        model.addAttribute("soloSongCount", albumService.getSoloSongCountForAlbum(id));
        model.addAttribute("songsWithFeatCount", albumService.getSongsWithFeatCountForAlbum(id));
        // Age at release
        if (artist != null && artist.getBirthDate() != null && albumEntity.getReleaseDate() != null) {
            long ageAtRelease = java.time.temporal.ChronoUnit.YEARS.between(artist.getBirthDate(), albumEntity.getReleaseDate().toLocalDate());
            model.addAttribute("ageAtRelease", ageAtRelease);
        }

        return "albums/detail";
    }
    
    @PostMapping("/{id}")
    public String updateAlbum(@PathVariable Integer id,
                              @ModelAttribute Album album,
                              @RequestParam(required = false) List<Integer> tags,
                              HttpServletRequest request) {
        album.setId(id);
        albumService.saveAlbum(album);
        tagService.saveAlbumTags(id, tags);
        return DetailRedirects.backTo("/albums/" + id, request);
    }
    
    @GetMapping("/{id}/image")
    public ResponseEntity<byte[]> getAlbumImage(@PathVariable Integer id,
                                @RequestParam(required = false, defaultValue = "false") boolean thumbnail,
                                @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) {
        return library.util.ImageResponses.image(albumService.getAlbumImage(id), thumbnail, ifNoneMatch);
    }
    
    @PostMapping("/{id}/image")
    @ResponseBody
    public String uploadAlbumImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            
            albumService.updateAlbumImage(id, file.getBytes());
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @DeleteMapping("/{id}/image")
    @ResponseBody
    public String deleteAlbumImage(@PathVariable Integer id) {
        try {
            albumService.updateAlbumImage(id, null);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    // Gallery endpoints for secondary images
    @GetMapping("/{id}/images")
    @ResponseBody
    public List<Map<String, Object>> getAlbumImages(@PathVariable Integer id) {
        return albumService.getSecondaryImageMetadata(id);
    }

    @GetMapping("/{id}/images/{imageId}")
    @ResponseBody
    public byte[] getSecondaryImage(@PathVariable Integer id, @PathVariable Integer imageId) {
        return albumService.getSecondaryImage(imageId);
    }

    @PostMapping("/{id}/images")
    @ResponseBody
    public String addSecondaryImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            boolean added = albumService.addSecondaryImage(id, file.getBytes());
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
            albumService.deleteSecondaryImage(imageId);
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
            albumService.swapToDefault(id, imageId);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }

    @DeleteMapping("/{id}")
    @ResponseBody
    public String deleteAlbum(@PathVariable Integer id) {
        try {
            albumService.deleteAlbum(id);
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
    public Map<String, Object> createAlbum(@RequestBody Album album) {
        try {
            Album created = albumService.createAlbum(album);
            Map<String, Object> response = new java.util.HashMap<>();
            response.put("id", created.getId());
            response.put("name", created.getName());
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create album");
        }
    }
    
    @GetMapping("/api/albums")
    @ResponseBody
    public List<Map<String, Object>> getAlbumsByArtist(@RequestParam Integer artistId) {
        return albumService.getAlbumsByArtistForApi(artistId);
    }
    
    @GetMapping("/api/by-artist")
    @ResponseBody
    public List<Map<String, Object>> getAlbumsByArtistAlt(@RequestParam Integer artistId) {
        return albumService.getAlbumsByArtistForApi(artistId);
    }
    
    /**
     * Get album release date by ID (for wizard validation).
     * Returns date in dd/MM/yyyy format to match form inputs.
     */
    @GetMapping("/api/{id}/release-date")
    @ResponseBody
    public Map<String, Object> getAlbumReleaseDate(@PathVariable Integer id) {
        Album album = albumService.findById(id);
        Map<String, Object> response = new java.util.HashMap<>();
        if (album != null && album.getReleaseDate() != null) {
            // Convert java.sql.Date to dd/MM/yyyy format
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("dd/MM/yyyy");
            response.put("releaseDate", sdf.format(album.getReleaseDate()));
        } else {
            response.put("releaseDate", null);
        }
        return response;
    }

    @GetMapping("/api/{id}/last-full-listen")
    @ResponseBody
    public Map<String, Object> getLastFullListenDate(@PathVariable Integer id) {
        Map<String, Object> response = new java.util.HashMap<>();
        boolean fullListenEligible = albumService.isFullListenEligible(id);
        AlbumFullListenStats stats = fullListenEligible
                ? albumService.getFullListenStatsForAlbum(id)
                : new AlbumFullListenStats(null, null, 0);
        response.put("fullListenEligible", fullListenEligible);
        response.put("firstFullListenDate", stats.firstFullListenDate() != null
                ? DateFormatUtils.formatIsoDateForDisplay(stats.firstFullListenDate()) : null);
        response.put("lastFullListenDate", stats.lastFullListenDate() != null
                ? DateFormatUtils.formatIsoDateForDisplay(stats.lastFullListenDate()) : null);
        response.put("fullAlbumPlays", stats.fullAlbumPlays());
        return response;
    }

    @GetMapping("/api/all-albums")
    @ResponseBody
    public List<Map<String, Object>> getAllAlbums() {
        return albumService.getAllAlbumsForApi();
    }

    /**
     * Search albums by name or artist name for the seasonal/yearly chart editor.
     */
    @GetMapping("/api/search")
    @ResponseBody
    public List<Map<String, Object>> searchAlbumsForApi(
            @RequestParam(required = false) String query,
            @RequestParam(required = false, defaultValue = "0") int limit) {
        return albumService.searchAlbums(query, limit);
    }
    
    /**
     * API: Get gender ID for an album (for UI coloring in chart editors).
     */
    @GetMapping("/{albumId}/gender")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getAlbumGender(@PathVariable Integer albumId) {
        Optional<Album> albumOpt = albumService.getAlbumById(albumId);
        if (albumOpt.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        
        Album album = albumOpt.get();
        Integer genderId = null;
        
        if (album.getArtistId() != null) {
            Optional<Artist> artistOpt = artistService.getArtistById(album.getArtistId());
            if (artistOpt.isPresent()) {
                genderId = artistOpt.get().getGenderId();
            }
        }
        
        Map<String, Object> response = new HashMap<>();
        response.put("genderId", genderId);
        return ResponseEntity.ok(response);
    }

    private boolean hasActiveFilters(HttpServletRequest request) {
        return request.getParameterMap().entrySet().stream().anyMatch(entry -> {
            String key = entry.getKey();
            if (java.util.Set.of("sortby", "sortdir", "sortby2", "sortdir2", "sortby3", "sortdir3", "page", "perpage", "view", "tab").contains(key)) {
                return false;
            }
            return java.util.Arrays.stream(entry.getValue()).anyMatch(value -> value != null && !value.isBlank());
        });
    }
}
