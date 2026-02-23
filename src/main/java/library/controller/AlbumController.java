package library.controller;

import library.dto.AlbumCardDTO;
import library.dto.GenderCountDTO;
import library.entity.Album;
import library.entity.Artist;
import library.repository.LookupRepository;
import library.service.AlbumService;
import library.service.ArtistService;
import library.service.ChartService;
import library.service.ItunesService;
import library.util.DateFormatUtils;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller
@RequestMapping("/albums")
public class AlbumController {
    
    private final AlbumService albumService;
    private final ChartService chartService;
    private final ArtistService artistService;
    private final LookupRepository lookupRepository;
    private final ItunesService itunesService;

    public AlbumController(AlbumService albumService, ChartService chartService, ArtistService artistService, LookupRepository lookupRepository, ItunesService itunesService) {
        this.albumService = albumService;
        this.chartService = chartService;
        this.artistService = artistService;
        this.lookupRepository = lookupRepository;
        this.itunesService = itunesService;
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
    
    @GetMapping
    public String listAlbums(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            @RequestParam(required = false) String artist,
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
            @RequestParam(required = false) Integer weeklyChartWeeks,
            @RequestParam(required = false) Integer seasonalChartPeak,
            @RequestParam(required = false) Integer seasonalChartSeasons,
            @RequestParam(required = false) Integer yearlyChartPeak,
            @RequestParam(required = false) Integer yearlyChartYears,
            @RequestParam(defaultValue = "plays") String sortby,
            @RequestParam(defaultValue = "desc") String sortdir,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "100") int perpage,
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
        String listenedDateFromConverted = DateFormatUtils.convertToIsoFormat(listenedDateFrom);
        String listenedDateToConverted = DateFormatUtils.convertToIsoFormat(listenedDateTo);
        String birthDateConverted = DateFormatUtils.convertToIsoFormat(birthDate);
        String birthDateFromConverted = DateFormatUtils.convertToIsoFormat(birthDateFrom);
        String birthDateToConverted = DateFormatUtils.convertToIsoFormat(birthDateTo);
        String deathDateConverted = DateFormatUtils.convertToIsoFormat(deathDate);
        String deathDateFromConverted = DateFormatUtils.convertToIsoFormat(deathDateFrom);
        String deathDateToConverted = DateFormatUtils.convertToIsoFormat(deathDateTo);
        
        // Get filtered and sorted albums
        List<AlbumCardDTO> albums = albumService.getAlbums(
                q, artist, genre, genreMode, subgenre, subgenreMode,
                language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
                country, countryMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand,
                ageMin, ageMax, ageMode, ageAtReleaseMin, ageAtReleaseMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                inItunes,
                playCountMin, playCountMax, songCountMin, songCountMax,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks, seasonalChartPeak, seasonalChartSeasons, yearlyChartPeak, yearlyChartYears,
                sortby, sortdir, page, perpage
        );
        
        // Get total count for pagination
        long totalCount = albumService.countAlbums(q, artist, genre, 
                genreMode, subgenre, subgenreMode, language, languageMode, gender, 
                genderMode, ethnicity, ethnicityMode, country, countryMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand,
                ageMin, ageMax, ageMode, ageAtReleaseMin, ageAtReleaseMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                inItunes,
                playCountMin, playCountMax, songCountMin, songCountMax,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks, seasonalChartPeak, seasonalChartSeasons, yearlyChartPeak, yearlyChartYears);
        int totalPages = (int) Math.ceil((double) totalCount / perpage);
        
        // Get gender counts for the filtered dataset
        GenderCountDTO genderCounts = albumService.countAlbumsByGender(q, artist, genre, 
                genreMode, subgenre, subgenreMode, language, languageMode, gender, 
                genderMode, ethnicity, ethnicityMode, country, countryMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand,
                ageMin, ageMax, ageMode, ageAtReleaseMin, ageAtReleaseMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                inItunes,
                playCountMin, playCountMax, songCountMin, songCountMax,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks, seasonalChartPeak, seasonalChartSeasons, yearlyChartPeak, yearlyChartYears);
        
        // Add data to model
        model.addAttribute("currentSection", "albums");
        model.addAttribute("albums", albums);
        model.addAttribute("genderCounts", genderCounts);
        model.addAttribute("currentPage", page);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("perPage", perpage);
        model.addAttribute("startIndex", (page * perpage) + 1);
        model.addAttribute("endIndex", Math.min((page + 1) * perpage, totalCount));
        
        // Add filter values to maintain state
        model.addAttribute("searchQuery", q);
        model.addAttribute("selectedArtist", artist);
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
        model.addAttribute("selectedAccounts", account);
        model.addAttribute("accountMode", accountMode != null ? accountMode : "includes");
        model.addAttribute("ageMin", ageMin);
        model.addAttribute("ageMax", ageMax);
        model.addAttribute("ageMode", ageMode);
        model.addAttribute("ageAtReleaseMin", ageAtReleaseMin);
        model.addAttribute("ageAtReleaseMax", ageAtReleaseMax);
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
        model.addAttribute("lengthMin", lengthMin);
        model.addAttribute("lengthMax", lengthMax);
        model.addAttribute("lengthMode", lengthMode != null ? lengthMode : "range");
        
        // Chart filter attributes
        model.addAttribute("weeklyChartPeak", weeklyChartPeak);
        model.addAttribute("weeklyChartWeeks", weeklyChartWeeks);
        model.addAttribute("seasonalChartPeak", seasonalChartPeak);
        model.addAttribute("seasonalChartSeasons", seasonalChartSeasons);
        model.addAttribute("yearlyChartPeak", yearlyChartPeak);
        model.addAttribute("yearlyChartYears", yearlyChartYears);
        
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
        model.addAttribute("firstListenedDateFormatted", formatDateForDisplay(firstListenedDate));
        model.addAttribute("firstListenedDateFromFormatted", formatDateForDisplay(firstListenedDateFrom));
        model.addAttribute("firstListenedDateToFormatted", formatDateForDisplay(firstListenedDateTo));
        
        // Last listened date filter attributes
        model.addAttribute("lastListenedDate", lastListenedDate);
        model.addAttribute("lastListenedDateFrom", lastListenedDateFrom);
        model.addAttribute("lastListenedDateTo", lastListenedDateTo);
        model.addAttribute("lastListenedDateMode", lastListenedDateMode != null ? lastListenedDateMode : "exact");
        model.addAttribute("lastListenedDateFormatted", formatDateForDisplay(lastListenedDate));
        model.addAttribute("lastListenedDateFromFormatted", formatDateForDisplay(lastListenedDateFrom));
        model.addAttribute("lastListenedDateToFormatted", formatDateForDisplay(lastListenedDateTo));
        
        // Listened date filter attributes (filters by actual play date)
        model.addAttribute("listenedDateFrom", listenedDateFrom);
        model.addAttribute("listenedDateTo", listenedDateTo);
        model.addAttribute("listenedDateFromFormatted", formatDateForDisplay(listenedDateFrom));
        model.addAttribute("listenedDateToFormatted", formatDateForDisplay(listenedDateTo));
        
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        model.addAttribute("defaultSortBy", "plays");
        
        // Add filter options
        model.addAttribute("genres", albumService.getGenres());
        model.addAttribute("subgenres", albumService.getSubGenres());
        model.addAttribute("languages", albumService.getLanguages());
        model.addAttribute("genders", albumService.getGenders());
        model.addAttribute("ethnicities", albumService.getEthnicities());
        model.addAttribute("countries", albumService.getCountries());
        
        return "albums/list";
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
        byte[] image = albumService.getAlbumImage(id);
        boolean hasImage = (image != null && image.length > 0);
        
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
        
        // Add artist entity for ranking chips
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
        model.addAttribute("albumRobertloverPlayCount", albumService.getRobertloverPlayCountForAlbum(id));
        // Add per-account breakdown string for tooltip
        model.addAttribute("albumPlaysByAccount", albumService.getPlaysByAccountForAlbum(id));
        
        // Add album length formatted
        model.addAttribute("albumLengthFormatted", albumService.getAlbumLengthFormatted(id));
        
        // Add average song length and average plays per song for statistics
        model.addAttribute("averageSongLength", albumService.getAverageSongLengthFormatted(id));
        model.addAttribute("averagePlaysPerSong", albumService.getAveragePlaysPerSong(id));
        
        // Add songs list
        model.addAttribute("songs", albumService.getSongsForAlbum(id));
        model.addAttribute("songCount", songCount);
        
        // Add total listening time for the album
        model.addAttribute("totalListeningTime", albumService.getTotalListeningTimeForAlbum(id));
        
        // Add first and last listened dates for the album
        model.addAttribute("firstListenedDate", albumService.getFirstListenedDateForAlbum(id));
        model.addAttribute("lastListenedDate", albumService.getLastListenedDateForAlbum(id));
        
        // Add unique period stats for the album
        model.addAttribute("uniqueDaysPlayed", albumService.getUniqueDaysPlayedForAlbum(id));
        model.addAttribute("uniqueWeeksPlayed", albumService.getUniqueWeeksPlayedForAlbum(id));
        model.addAttribute("uniqueMonthsPlayed", albumService.getUniqueMonthsPlayedForAlbum(id));
        model.addAttribute("uniqueYearsPlayed", albumService.getUniqueYearsPlayedForAlbum(id));
        
        // Calculate totals based on first listened date
        java.time.LocalDate firstListened = albumService.getFirstListenedDateAsLocalDateForAlbum(id);
        if (firstListened != null) {
            java.time.LocalDate now = java.time.LocalDate.now();
            
            // Calendar days: actual days between dates
            long daysSinceFirst = java.time.temporal.ChronoUnit.DAYS.between(firstListened, now) + 1;
            
            // Calendar weeks: count week numbers from first to now
            java.time.temporal.WeekFields weekFields = java.time.temporal.WeekFields.of(java.util.Locale.getDefault());
            int firstWeek = firstListened.get(weekFields.weekOfWeekBasedYear());
            int firstWeekYear = firstListened.get(weekFields.weekBasedYear());
            int nowWeek = now.get(weekFields.weekOfWeekBasedYear());
            int nowWeekYear = now.get(weekFields.weekBasedYear());
            // Approximate: weeks in between years + week difference in current year
            long weeksSinceFirst = ((nowWeekYear - firstWeekYear) * 52L) + (nowWeek - firstWeek) + 1;
            
            // Calendar months: count month numbers from first to now
            long monthsSinceFirst = ((now.getYear() - firstListened.getYear()) * 12L) + (now.getMonthValue() - firstListened.getMonthValue()) + 1;
            
            // Calendar years: count year numbers from first to now
            long yearsSinceFirst = (now.getYear() - firstListened.getYear()) + 1;
            
            model.addAttribute("totalDaysSinceFirstPlay", daysSinceFirst);
            model.addAttribute("totalWeeksSinceFirstPlay", weeksSinceFirst);
            model.addAttribute("totalMonthsSinceFirstPlay", monthsSinceFirst);
            model.addAttribute("totalYearsSinceFirstPlay", yearsSinceFirst);
        } else {
            model.addAttribute("totalDaysSinceFirstPlay", 0);
            model.addAttribute("totalWeeksSinceFirstPlay", 0);
            model.addAttribute("totalMonthsSinceFirstPlay", 0);
            model.addAttribute("totalYearsSinceFirstPlay", 0);
        }
        
        // Tab and plays data
        model.addAttribute("activeTab", tab);
        
        // Add seasonal/yearly chart history for chips display (always needed)
        model.addAttribute("seasonalChartHistory", chartService.getChartHistoryForItem(id, "album", "seasonal"));
        model.addAttribute("yearlyChartHistory", chartService.getChartHistoryForItem(id, "album", "yearly"));
        
        // Add featured artist cards (always pre-loaded for Featured Artists tab)
        model.addAttribute("featuredArtistCards", albumService.getFeaturedArtistCardsForAlbum(id));
        
        // Always load plays data (eager loading for all tabs)
        int pageSize = 100;
        model.addAttribute("plays", albumService.getPlaysForAlbum(id, playsPage, pageSize));
        model.addAttribute("playsTotalCount", albumService.countPlaysForAlbum(id));
        model.addAttribute("playsPage", playsPage);
        model.addAttribute("playsPageSize", pageSize);
        model.addAttribute("playsTotalPages", (int) Math.ceil((double) albumService.countPlaysForAlbum(id) / pageSize));
        model.addAttribute("playsByYear", albumService.getPlaysByYearForAlbum(id));
        model.addAttribute("playsByMonth", albumService.getPlaysByMonthForAlbum(id));
        
        // Always load chart history data (eager loading for all tabs)
        model.addAttribute("chartHistory", chartService.getAlbumChartHistory(id));
        model.addAttribute("songChartHistory", chartService.getAlbumSongChartHistory(id));
        // Songs' seasonal/yearly chart history
        model.addAttribute("seasonalSongChartHistory", chartService.getAlbumSongsChartHistoryByPeriodType(id, "seasonal"));
        model.addAttribute("yearlySongChartHistory", chartService.getAlbumSongsChartHistoryByPeriodType(id, "yearly"));
        
        // Add ranking chips data - optimized single query
        java.util.Map<String, Integer> rankings = albumService.getAllAlbumRankings(id);
        model.addAttribute("rankByGender", rankings.get("gender"));
        model.addAttribute("rankByGenre", rankings.get("genre"));
        model.addAttribute("rankBySubgenre", rankings.get("subgenre"));
        model.addAttribute("rankByEthnicity", rankings.get("ethnicity"));
        model.addAttribute("rankByLanguage", rankings.get("language"));
        model.addAttribute("rankByCountry", rankings.get("country"));
        model.addAttribute("ranksByYear", albumService.getAlbumRanksByYear(id));
        
        // Add Spanish Rap rank (special combination)
        if (albumService.isAlbumSpanishRap(id)) {
            model.addAttribute("rankBySpanishRap", albumService.getAlbumSpanishRapRank(id));
            model.addAttribute("rapGenreId", lookupRepository.getGenreIdByName("Rap"));
            model.addAttribute("spanishLanguageId", lookupRepository.getLanguageIdByName("Spanish"));
        }

        // Add new ranking chips
        model.addAttribute("weeklyChartStats", chartService.getAlbumWeeklyChartStats(id));
        model.addAttribute("overallPosition", albumService.getAlbumOverallPosition(id));
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
    public String updateAlbum(@PathVariable Integer id, @ModelAttribute Album album) {
        album.setId(id);
        albumService.saveAlbum(album);
        return "redirect:/albums/" + id;
    }
    
    @GetMapping("/{id}/image")
    @ResponseBody
    public byte[] getAlbumImage(@PathVariable Integer id) {
        return albumService.getAlbumImage(id);
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
        var images = albumService.getSecondaryImages(id);
        return images.stream()
                .map(img -> Map.<String, Object>of(
                        "id", img.getId(),
                        "displayOrder", img.getDisplayOrder() != null ? img.getDisplayOrder() : 0
                ))
                .toList();
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
    
    // Helper method to format date strings for display (yyyy-MM-dd -> dd-MMM-yyyy)
    private String formatDateForDisplay(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }
        try {
            String[] parts = dateStr.split("-");
            if (parts.length == 3) {
                int year = Integer.parseInt(parts[0]);
                int month = Integer.parseInt(parts[1]);
                int day = Integer.parseInt(parts[2]);
                String[] monthNames = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
                return String.format("%02d-%s-%d", day, monthNames[month - 1], year);
            }
        } catch (Exception e) {
            // If parsing fails, return original
        }
        return dateStr;
    }
    
}