package library.controller;

import library.dto.ChartFilterDTO;
import library.dto.FeaturedArtistDTO;
import library.dto.GenderCountDTO;
import library.dto.SongCardDTO;
import library.entity.Album;
import library.entity.Artist;
import library.entity.Song;
import library.repository.LookupRepository;
import library.service.AlbumService;
import library.service.ArtistService;
import library.service.ChartService;
import library.service.SongService;
import library.service.ItunesService;
import library.service.TrlService;
import library.service.PcService;
import library.service.BillboardHot100Service;
import library.util.DateFormatUtils;
import library.util.StringNormalizer;
import library.service.iTunesLibraryService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Controller
@RequestMapping("/songs")
public class SongController {
    
    private final SongService songService;
    private final ChartService chartService;
    private final ArtistService artistService;
    private final AlbumService albumService;
    private final iTunesLibraryService iTunesLibraryService;
    private final LookupRepository lookupRepository;
    private final ItunesService itunesService;
    private final TrlService trlService;
    private final PcService pcService;
    private final BillboardHot100Service billboardHot100Service;
    private final JdbcTemplate jdbcTemplate;
    private static final Pattern PARENTHETICAL_PATTERN = Pattern.compile("\\(([^)]*)\\)");
    private static final Pattern BRACKET_PATTERN = Pattern.compile("\\[([^]]*)\\]");

    public SongController(SongService songService, ChartService chartService, ArtistService artistService,
                         AlbumService albumService, iTunesLibraryService iTunesLibraryService, LookupRepository lookupRepository,
                         ItunesService itunesService, TrlService trlService, PcService pcService,
                         BillboardHot100Service billboardHot100Service, JdbcTemplate jdbcTemplate) {
        this.songService = songService;
        this.chartService = chartService;
        this.artistService = artistService;
        this.albumService = albumService;
        this.iTunesLibraryService = iTunesLibraryService;
        this.lookupRepository = lookupRepository;
        this.itunesService = itunesService;
        this.trlService = trlService;
        this.pcService = pcService;
        this.billboardHot100Service = billboardHot100Service;
        this.jdbcTemplate = jdbcTemplate;
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
    public String listSongs(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> artist,
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
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
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
            @RequestParam(required = false) String organized,
            @RequestParam(required = false) Integer imageCountMin,
            @RequestParam(required = false) Integer imageCountMax,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) Integer trackNumber,
            @RequestParam(required = false) String trackNumberMode,
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
        
        // Pre-compute iTunes song IDs once for all 3 queries (getSongs, countSongs, countSongsByGender)
        String itunesIdsJson = songService.getItunesSongIdsJson(inItunes);
        
        // Get filtered and sorted songs
        List<SongCardDTO> songs = songService.getSongs(
                q, artist, album, genre, genreMode, 
                subgenre, subgenreMode, language, languageMode, gender, genderMode,
                ethnicity, ethnicityMode, country, countryMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand, isSingle,
                ageMin, ageMax, ageMode, ageAtReleaseMin, ageAtReleaseMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                itunesIdsJson, inItunes,
                playCountMin, playCountMax,
                trackNumber, trackNumberMode,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks, seasonalChartPeak, seasonalChartSeasons, yearlyChartPeak, yearlyChartYears,
                sortby, sortdir, page, perpage
        );
        
        // Get total count for pagination
        long totalCount = songService.countSongs(q, artist, album, 
                genre, genreMode, subgenre, subgenreMode, language, languageMode,
                gender, genderMode, ethnicity, ethnicityMode, country, countryMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand, isSingle,
                ageMin, ageMax, ageMode, ageAtReleaseMin, ageAtReleaseMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                itunesIdsJson, inItunes,
                playCountMin, playCountMax,
                trackNumber, trackNumberMode,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks, seasonalChartPeak, seasonalChartSeasons, yearlyChartPeak, yearlyChartYears);
        int totalPages = (int) Math.ceil((double) totalCount / perpage);
        
        // Get gender counts for the filtered dataset
        GenderCountDTO genderCounts = songService.countSongsByGender(q, artist, album,
                genre, genreMode, subgenre, subgenreMode, language, languageMode,
                gender, genderMode, ethnicity, ethnicityMode, country, countryMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand, isSingle,
                ageMin, ageMax, ageMode, ageAtReleaseMin, ageAtReleaseMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                itunesIdsJson, inItunes,
                playCountMin, playCountMax,
                trackNumber, trackNumberMode,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks, seasonalChartPeak, seasonalChartSeasons, yearlyChartPeak, yearlyChartYears);
        
        // Add data to model
        model.addAttribute("currentSection", "songs");
        model.addAttribute("songs", songs);
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
        List<Map<String, Object>> artistDetails = (artist != null && !artist.isEmpty()) ?
            artistService.getArtistDetailsForIds(artist) : List.of();
        model.addAttribute("selectedArtistDetails", artistDetails);
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
        model.addAttribute("selectedHasFeaturedArtists", hasFeaturedArtists);
        model.addAttribute("selectedIsBand", isBand);
        model.addAttribute("selectedIsSingle", isSingle);
        model.addAttribute("selectedInItunes", inItunes);
        model.addAttribute("playCountMin", playCountMin);
        model.addAttribute("playCountMax", playCountMax);
        model.addAttribute("trackNumber", trackNumber);
        model.addAttribute("trackNumberMode", trackNumberMode != null ? trackNumberMode : "exact");
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
        
        model.addAttribute("releaseDate", releaseDate);
        model.addAttribute("releaseDateFrom", releaseDateFrom);
        model.addAttribute("releaseDateTo", releaseDateTo);
        model.addAttribute("releaseDateMode", releaseDateMode != null ? releaseDateMode : "exact");
        // Add formatted dates for display
        model.addAttribute("releaseDateFormatted", formatDateForDisplay(releaseDate));
        model.addAttribute("releaseDateFromFormatted", formatDateForDisplay(releaseDateFrom));
        model.addAttribute("releaseDateToFormatted", formatDateForDisplay(releaseDateTo));
        
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
        model.addAttribute("genres", songService.getGenres());
        model.addAttribute("subgenres", songService.getSubGenres());
        model.addAttribute("languages", songService.getLanguages());
        model.addAttribute("genders", songService.getGenders());
        model.addAttribute("ethnicities", songService.getEthnicities());
        model.addAttribute("countries", songService.getCountries());
        
        return "songs/list";
    }
    
    @GetMapping("/{id}")
    public String viewSong(@PathVariable Integer id, 
                          @RequestParam(defaultValue = "general") String tab,
                          @RequestParam(defaultValue = "0") int playsPage,
                          Model model) {
        Optional<Song> song = songService.getSongById(id);
        
        if (song.isEmpty()) {
            return "redirect:/songs";
        }
        
        // Check if song has its own image (not falling back to album)
        byte[] ownImage = songService.getSongOwnImage(id);
        boolean hasImage = (ownImage != null && ownImage.length > 0);
        
        // Get artist and album names
        String artistName = songService.getArtistName(song.get().getArtistId());
        String artistGender = songService.getArtistGender(song.get().getArtistId());
        String artistCountry = songService.getArtistCountry(song.get().getArtistId());
        String albumName = song.get().getAlbumId() != null ? 
                          songService.getAlbumName(song.get().getAlbumId()) : null;
        
        model.addAttribute("currentSection", "songs");
        model.addAttribute("song", song.get());
        model.addAttribute("hasImage", hasImage);
        model.addAttribute("artistName", artistName);
        model.addAttribute("artistGender", artistGender);
        model.addAttribute("artistCountry", artistCountry);
        model.addAttribute("albumName", albumName);
        
        // Add artist and album entities for ranking chips
        Artist artist = artistService.findById(song.get().getArtistId());
        model.addAttribute("artist", artist);
        Album album = song.get().getAlbumId() != null ? 
                      albumService.getAlbumById(song.get().getAlbumId()).orElse(null) : null;
        model.addAttribute("album", album);
        
        // Add iTunes presence
        Song songEntity = song.get();
        songEntity.setInItunes(itunesService.songExistsInItunes(artistName, albumName, songEntity.getName()));
        model.addAttribute("song", songEntity);
        
        // Add album release date for inheritance display
        String albumReleaseDate = (album != null && album.getReleaseDateFormatted() != null) ? 
                                  album.getReleaseDateFormatted() : null;
        model.addAttribute("albumReleaseDate", albumReleaseDate);
        
        // NEW: add song play count
        model.addAttribute("songPlayCount", songService.getPlayCountForSong(id));
        model.addAttribute("songVatitoPlayCount", songService.getVatitoPlayCountForSong(id));
        model.addAttribute("songRobertloverPlayCount", songService.getRobertloverPlayCountForSong(id));
        // Add per-account breakdown string for tooltip
        model.addAttribute("songPlaysByAccount", songService.getPlaysByAccountForSong(id));
        
        // Add statistics for the song
        model.addAttribute("totalListeningTime", songService.getTotalListeningTimeForSong(id));
        model.addAttribute("firstListenedDate", songService.getFirstListenedDateForSong(id));
        model.addAttribute("lastListenedDate", songService.getLastListenedDateForSong(id));
        
        // Add unique period stats for the song
        model.addAttribute("uniqueDaysPlayed", songService.getUniqueDaysPlayedForSong(id));
        model.addAttribute("uniqueWeeksPlayed", songService.getUniqueWeeksPlayedForSong(id));
        model.addAttribute("uniqueMonthsPlayed", songService.getUniqueMonthsPlayedForSong(id));
        model.addAttribute("uniqueYearsPlayed", songService.getUniqueYearsPlayedForSong(id));
        
        // Calculate totals based on first listened date
        java.time.LocalDate firstListened = songService.getFirstListenedDateAsLocalDateForSong(id);
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
        
        // Add lookup maps for ranking chips
        Map<Integer, String> genres = songService.getGenres();
        Map<Integer, String> subgenres = songService.getSubGenres();
        Map<Integer, String> languages = songService.getLanguages();
        Map<Integer, String> genders = songService.getGenders();
        Map<Integer, String> ethnicities = songService.getEthnicities();
        
        model.addAttribute("genres", genres);
        model.addAttribute("subgenres", subgenres);
        model.addAttribute("languages", languages);
        model.addAttribute("genders", genders);
        model.addAttribute("ethnicities", ethnicities);
        
        // Add effective (resolved) value names for display
        Song s = song.get();
        model.addAttribute("effectiveGenreName", s.getEffectiveGenreId() != null ? genres.get(s.getEffectiveGenreId()) : null);
        model.addAttribute("effectiveSubgenreName", s.getEffectiveSubgenreId() != null ? subgenres.get(s.getEffectiveSubgenreId()) : null);
        model.addAttribute("effectiveLanguageName", s.getEffectiveLanguageId() != null ? languages.get(s.getEffectiveLanguageId()) : null);
        model.addAttribute("effectiveGenderName", s.getEffectiveGenderId() != null ? genders.get(s.getEffectiveGenderId()) : null);
        model.addAttribute("effectiveEthnicityName", s.getEffectiveEthnicityId() != null ? ethnicities.get(s.getEffectiveEthnicityId()) : null);
        
        // Add inherited value names (what would be used if no song override) for dropdown "Inherit" options
        Integer inheritedGenreId = s.getAlbumGenreId() != null ? s.getAlbumGenreId() : s.getArtistGenreId();
        Integer inheritedSubgenreId = s.getAlbumSubgenreId() != null ? s.getAlbumSubgenreId() : s.getArtistSubgenreId();
        Integer inheritedLanguageId = s.getAlbumLanguageId() != null ? s.getAlbumLanguageId() : s.getArtistLanguageId();
        Integer inheritedGenderId = s.getArtistGenderId();
        Integer inheritedEthnicityId = s.getArtistEthnicityId();
        
        model.addAttribute("inheritedGenreName", inheritedGenreId != null ? genres.get(inheritedGenreId) : null);
        model.addAttribute("inheritedSubgenreName", inheritedSubgenreId != null ? subgenres.get(inheritedSubgenreId) : null);
        model.addAttribute("inheritedLanguageName", inheritedLanguageId != null ? languages.get(inheritedLanguageId) : null);
        model.addAttribute("inheritedGenderName", inheritedGenderId != null ? genders.get(inheritedGenderId) : null);
        model.addAttribute("inheritedEthnicityName", inheritedEthnicityId != null ? ethnicities.get(inheritedEthnicityId) : null);
        
        // Tab and plays data
        model.addAttribute("activeTab", tab);
        
        // Add featured artists for this song (for editing)
        model.addAttribute("featuredArtists", songService.getFeaturedArtistsForSong(id));
        
        // Add featured artist cards (always pre-loaded for Featured Artists tab)
        model.addAttribute("featuredArtistCards", songService.getFeaturedArtistCardsForSong(id));
        
        // Always load plays data (eager loading for all tabs)
        int pageSize = 100;
        model.addAttribute("plays", songService.getPlaysForSong(id, playsPage, pageSize));
        model.addAttribute("playsTotalCount", songService.countPlaysForSong(id));
        model.addAttribute("playsPage", playsPage);
        model.addAttribute("playsPageSize", pageSize);
        model.addAttribute("playsTotalPages", (int) Math.ceil((double) songService.countPlaysForSong(id) / pageSize));
        model.addAttribute("playsByYear", songService.getPlaysByYearForSong(id));
        model.addAttribute("playsByMonth", songService.getPlaysByMonthForSong(id));
        
        // Always load seasonal/yearly chart history for sidebar chips
        model.addAttribute("seasonalChartHistory", chartService.getChartHistoryForItem(id, "song", "seasonal"));
        model.addAttribute("yearlyChartHistory", chartService.getChartHistoryForItem(id, "song", "yearly"));
        
        // Always load weekly chart history (eager loading for all tabs)
        model.addAttribute("chartHistory", chartService.getSongChartHistory(id));
        
        // Add ranking chips data - optimized single query
        java.util.Map<String, Integer> rankings = songService.getAllSongRankings(id);
        model.addAttribute("rankByGender", rankings.get("gender"));
        model.addAttribute("rankByGenre", rankings.get("genre"));
        model.addAttribute("rankBySubgenre", rankings.get("subgenre"));
        model.addAttribute("rankByEthnicity", rankings.get("ethnicity"));
        model.addAttribute("rankByLanguage", rankings.get("language"));
        model.addAttribute("rankByCountry", rankings.get("country"));
        model.addAttribute("ranksByYear", songService.getSongRanksByYear(id));
        
        // Add Spanish Rap rank (special combination)
        if (songService.isSongSpanishRap(id)) {
            model.addAttribute("rankBySpanishRap", songService.getSongSpanishRapRank(id));
            model.addAttribute("rapGenreId", lookupRepository.getGenreIdByName("Rap"));
            model.addAttribute("spanishLanguageId", lookupRepository.getLanguageIdByName("Spanish"));
        }

        // Add new ranking chips
        model.addAttribute("weeklyChartStats", chartService.getSongWeeklyChartStats(id));
        model.addAttribute("overallPosition", songService.getSongOverallPosition(id));
        model.addAttribute("rankByReleaseYear", songService.getSongRankByReleaseYear(id));
        model.addAttribute("releaseYear", songService.getSongReleaseYear(id));
        model.addAttribute("rankByArtist", songService.getSongRankByArtist(id));
        model.addAttribute("rankByAlbum", songService.getSongRankByAlbum(id));

        // TRL chip
        model.addAttribute("trlDays", trlService.getDaysOnTrlBySongId(id));
        model.addAttribute("trlStats", trlService.getTrlStatsBySongId(id));

        // Personal Cuntdown chip
        model.addAttribute("pcStats", pcService.getPcStatsBySongId(id));

        // Billboard Hot 100 chip
        model.addAttribute("billboardHot100Stats", billboardHot100Service.getStatsBySongId(id));

        // Extended stats for detail page - age at release
        if (artist != null && artist.getBirthDate() != null) {
            java.time.LocalDate effectiveReleaseDate = null;
            if (song.get().getReleaseDate() != null) {
                effectiveReleaseDate = song.get().getReleaseDate().toLocalDate();
            } else if (album != null && album.getReleaseDate() != null) {
                effectiveReleaseDate = album.getReleaseDate().toLocalDate();
            }
            if (effectiveReleaseDate != null) {
                long ageAtRelease = java.time.temporal.ChronoUnit.YEARS.between(artist.getBirthDate(), effectiveReleaseDate);
                model.addAttribute("ageAtRelease", ageAtRelease);
            }
        }

        return "songs/detail";
    }
    
    @PostMapping("/{id}")
    public String updateSong(@PathVariable Integer id, @ModelAttribute Song song) {
        song.setId(id);
        songService.saveSong(song);
        return "redirect:/songs/" + id;
    }
    
    @GetMapping("/{id}/image")
    @ResponseBody
    public byte[] getSongImage(@PathVariable Integer id,
                               @RequestParam(required = false, defaultValue = "false") boolean thumbnail) {
        byte[] image = songService.getSongImage(id);
        return thumbnail ? library.util.ImageUtil.resizeThumbnail(image, 600) : image;
    }
    
    @PostMapping("/{id}/image")
    @ResponseBody
    public String uploadSongImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            
            songService.updateSongImage(id, file.getBytes());
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @DeleteMapping("/{id}/image")
    @ResponseBody
    public String deleteSongImage(@PathVariable Integer id) {
        try {
            songService.updateSongImage(id, null);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    // Gallery endpoints for secondary images
    @GetMapping("/{id}/images")
    @ResponseBody
    public List<Map<String, Object>> getSongImages(@PathVariable Integer id) {
        var images = songService.getSecondaryImages(id);
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
        return songService.getSecondaryImage(imageId);
    }

    @PostMapping("/{id}/images")
    @ResponseBody
    public String addSecondaryImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            boolean added = songService.addSecondaryImage(id, file.getBytes());
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
            songService.deleteSecondaryImage(imageId);
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
            songService.swapToDefault(id, imageId);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }

    @DeleteMapping("/{id}")
    @ResponseBody
    public String deleteSong(@PathVariable Integer id) {
        try {
            songService.deleteSong(id);
            return "success";
        } catch (IllegalStateException e) {
            return "error:" + e.getMessage();
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    /**
     * Fetch sampling information from WhoSampled for a specific song.
     * Returns HTML content if found, or error status if not.
     */
    @GetMapping("/{id}/whosampled")
    @ResponseBody
    public Map<String, Object> fetchWhoSampledData(@PathVariable Integer id) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            Optional<Song> songOpt = songService.getSongById(id);
            if (!songOpt.isPresent()) {
                response.put("success", false);
                response.put("message", "Song not found");
                return response;
            }
            
            Song song = songOpt.get();
            String artistName = songService.getArtistName(song.getArtistId());
            String songName = song.getName();
            
            if (artistName == null || songName == null) {
                response.put("success", false);
                response.put("message", "Missing artist or song name");
                return response;
            }
            
            // Build WhoSampled URL: https://www.whosampled.com/Artist-Name/Song-Name/
            String urlPath = buildWhoSampledUrl(artistName, songName);
            String fullUrl = "https://www.whosampled.com" + urlPath;
            String searchUrl = buildWhoSampledSearchUrl(artistName, songName);

            response.put("url", fullUrl);
            response.put("searchUrl", searchUrl);
            
            System.out.println("Attempting WhoSampled URL: " + fullUrl);
            
            // Try to fetch the page
            try {
                org.jsoup.nodes.Document doc = fetchWhoSampledDocument(fullUrl);
                
                System.out.println("Successfully fetched WhoSampled page!");
                
                org.jsoup.nodes.Element mainContent = extractWhoSampledContent(doc, fullUrl);
                
                if (mainContent != null) {
                    response.put("success", true);
                    response.put("html", mainContent.outerHtml());
                } else {
                    response.put("success", false);
                    response.put("parseFailed", true);
                    response.put("message", "WhoSampled loaded, but the expected song content could not be parsed.");
                }
            } catch (org.jsoup.HttpStatusException e) {
                int statusCode = e.getStatusCode();
                System.out.println("WhoSampled request failed (status " + statusCode + "): " + fullUrl);
                response.put("success", false);
                response.put("statusCode", statusCode);

                if (statusCode == 404) {
                    response.put("notFound", true);
                    response.put("message", "WhoSampled does not appear to have a direct page for this song.");
                } else if (statusCode == 403) {
                    response.put("blocked", true);
                    response.put("message", "WhoSampled blocked the server-side fetch for this page.");
                } else {
                    response.put("message", "WhoSampled returned HTTP " + statusCode + " while fetching this page.");
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            response.put("success", false);
            response.put("message", "Error fetching WhoSampled data: " + e.getMessage());
        }
        
        return response;
    }
    
    /**
     * Build WhoSampled URL path from artist and song name.
     * Format: /Artist-Name/Song-Name/
     * WhoSampled keeps most punctuation but URL-encodes it.
     */
    private String buildWhoSampledUrl(String artist, String song) {
        // Remove featuring artists (e.g., "feat. Someone")
        artist = artist.replaceAll("(?i)\\s+feat\\..*$", "");
        artist = artist.replaceAll("(?i)\\s+ft\\..*$", "");
        artist = artist.replaceAll("(?i)\\s+featuring.*$", "");

        song = normalizeWhoSampledSongTitle(song);
        
        // Replace spaces with hyphens
        artist = artist.replaceAll("\\s+", "-");
        song = song.replaceAll("\\s+", "-");
        
        // URL encode the segments to handle special characters properly
        try {
            artist = java.net.URLEncoder.encode(artist, "UTF-8")
                    .replace("+", "-")  // URLEncoder uses + for spaces, we want -
                    .replace("%2D", "-");  // Keep hyphens as-is
            song = java.net.URLEncoder.encode(song, "UTF-8")
                    .replace("+", "-")
                    .replace("%2D", "-");
        } catch (Exception e) {
            // Fallback: just remove problematic characters
            artist = artist.replaceAll("[^a-zA-Z0-9\\s-]", "");
            song = song.replaceAll("[^a-zA-Z0-9\\s-]", "");
        }
        
        return "/" + artist + "/" + song + "/";
    }

    private String normalizeWhoSampledSongTitle(String song) {
        if (song == null || song.isBlank()) {
            return "";
        }

        String normalizedSong = stripFeaturingDelimitedClause(song, PARENTHETICAL_PATTERN, '(', ')');
        normalizedSong = stripFeaturingDelimitedClause(normalizedSong, BRACKET_PATTERN, '[', ']');

        normalizedSong = stripFeaturingClause(normalizedSong)
                .replaceAll("\\s+", " ")
            .replaceAll("\\s+\\)", ")")
            .replaceAll("\\s+\\]", "]")
            .replaceAll("\\(\\s+", "(")
            .replaceAll("\\[\\s+", "[")
                .trim();

        return normalizedSong;
    }

    private String stripFeaturingDelimitedClause(String value, Pattern pattern, char openChar, char closeChar) {
        Matcher matcher = pattern.matcher(value);
        StringBuffer normalized = new StringBuffer();
        while (matcher.find()) {
            String cleaned = stripFeaturingClause(matcher.group(1));
            String replacement = cleaned.isBlank() ? "" : openChar + cleaned + closeChar;
            matcher.appendReplacement(normalized, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(normalized);
        return normalized.toString();
    }

    private String stripFeaturingClause(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }

        return value
                .replaceAll("(?i)\\s*(?:[,/&+-]|and)?\\s*(feat\\.?|ft\\.?|featuring)\\s+.*$", "")
                .replaceAll("\\s+", " ")
                .trim();
    }

    private String buildWhoSampledSearchUrl(String artistName, String songName) throws java.io.UnsupportedEncodingException {
        return "https://www.whosampled.com/search/?q=" + java.net.URLEncoder.encode(artistName + " " + songName, "UTF-8");
    }

    private org.jsoup.nodes.Element extractWhoSampledContent(org.jsoup.nodes.Document doc, String fullUrl) throws IOException {
        java.util.List<org.jsoup.nodes.Element> sections = extractSongConnectionSections(doc);
        if (sections.isEmpty()) {
            return null;
        }

        Map<String, Integer> artistCache = new HashMap<>();
        Map<String, Integer> songCache = new HashMap<>();
        org.jsoup.nodes.Element container = new org.jsoup.nodes.Element("div");
        container.addClass("whosampled-song-connections");
        container.appendElement("h2").text("Song Connections");

        for (org.jsoup.nodes.Element section : sections) {
            org.jsoup.nodes.Element preparedSection = expandAndPrepareConnectionSection(section, fullUrl, artistCache, songCache);
            if (preparedSection != null) {
                container.appendChild(preparedSection);
            }
        }

        return container.children().isEmpty() ? null : container;
    }

    private java.util.List<org.jsoup.nodes.Element> extractSongConnectionSections(org.jsoup.nodes.Document doc) {
        java.util.List<org.jsoup.nodes.Element> sections = new ArrayList<>();
        org.jsoup.nodes.Element article = doc.selectFirst("main article.leftContent, main article");
        if (article == null) {
            return sections;
        }

        org.jsoup.nodes.Element songConnectionsHeading = article.selectFirst("h2:matchesOwn(^\\s*Song Connections\\s*$)");
        if (songConnectionsHeading == null) {
            return sections;
        }

        for (org.jsoup.nodes.Element sibling = songConnectionsHeading.nextElementSibling(); sibling != null; sibling = sibling.nextElementSibling()) {
            if ("h2".equalsIgnoreCase(sibling.tagName())) {
                break;
            }

            if (sibling.selectFirst("h3") != null) {
                sections.add(sibling.clone());
            }
        }

        return sections;
    }

    private org.jsoup.nodes.Element expandAndPrepareConnectionSection(org.jsoup.nodes.Element section,
                                                                     String fullUrl,
                                                                     Map<String, Integer> artistCache,
                                                                     Map<String, Integer> songCache) throws IOException {
        org.jsoup.nodes.Element workingSection = section.clone();
        String headingText = getConnectionSectionHeading(workingSection);
        if (headingText.startsWith("remixed in")) {
            return null;
        }
        String seeAllUrl = findSeeAllUrl(workingSection);

        if (seeAllUrl != null && (headingText.startsWith("contains samples") || headingText.startsWith("sampled in"))) {
            try {
                org.jsoup.nodes.Document detailDoc = fetchWhoSampledDocument(seeAllUrl);
                org.jsoup.nodes.Element fullSection = extractDetailedConnectionSection(detailDoc);
                if (fullSection != null) {
                    workingSection = fullSection;
                }
            } catch (Exception e) {
                System.out.println("Failed to expand WhoSampled section from " + seeAllUrl + ": " + e.getMessage());
            }
        }

        normalizeConnectionSection(workingSection, fullUrl, artistCache, songCache);
        return workingSection;
    }

    private org.jsoup.nodes.Element extractDetailedConnectionSection(org.jsoup.nodes.Document doc) {
        org.jsoup.nodes.Element article = doc.selectFirst("main article.leftContent, main article");
        if (article == null) {
            return null;
        }

        org.jsoup.nodes.Element heading = article.selectFirst("h2");
        if (heading == null) {
            return null;
        }

        org.jsoup.nodes.Element section = new org.jsoup.nodes.Element("div");
        section.addClass("ws-connection-section");
        org.jsoup.nodes.Element headingHeader = heading.parent();
        if (headingHeader != null && "header".equalsIgnoreCase(headingHeader.tagName())) {
            section.appendChild(headingHeader.clone());
            for (org.jsoup.nodes.Element sibling = headingHeader.nextElementSibling(); sibling != null; sibling = sibling.nextElementSibling()) {
                if ("header".equalsIgnoreCase(sibling.tagName())) {
                    break;
                }

                if ("h1".equalsIgnoreCase(sibling.tagName()) || "h2".equalsIgnoreCase(sibling.tagName())) {
                    break;
                }

                if ("nav".equalsIgnoreCase(sibling.tagName()) || "table".equalsIgnoreCase(sibling.tagName()) || sibling.selectFirst("table") != null) {
                    section.appendChild(sibling.clone());
                }
            }
            return section;
        }

        section.appendChild(heading.clone());

        for (org.jsoup.nodes.Element sibling = heading.nextElementSibling(); sibling != null; sibling = sibling.nextElementSibling()) {
            if ("h2".equalsIgnoreCase(sibling.tagName())) {
                break;
            }

            if ("h1".equalsIgnoreCase(sibling.tagName())) {
                break;
            }

            if (sibling.selectFirst("table") != null || "nav".equalsIgnoreCase(sibling.tagName())) {
                section.appendChild(sibling.clone());
            }
        }

        return section;
    }

    private void normalizeConnectionSection(org.jsoup.nodes.Element section,
                                            String baseUrl,
                                            Map<String, Integer> artistCache,
                                            Map<String, Integer> songCache) {
        section.select("script, style, .ad, .ads, .ad-wrapper, .comment, .comments, .comment-form, nav").remove();
        removeSeeAllLinks(section);

        section.select("img[src]").forEach(image -> image.attr("src", image.absUrl("src")));
        section.select("img[srcset]").forEach(image -> image.attr("srcset", absolutizeSrcSet(image.attr("srcset"), baseUrl)));

        for (org.jsoup.nodes.Element row : section.select("tr")) {
            if (!row.select("th").isEmpty()) {
                continue;
            }

            org.jsoup.select.Elements cells = row.select("> td");
            if (cells.size() < 3) {
                continue;
            }

            String songTitle = cells.get(1).text().trim();
            String primaryArtist = extractPrimaryArtistName(cells.get(2));
            Integer localSongId = findLocalSongId(primaryArtist, songTitle, songCache);

            rewriteSongLinks(cells.get(0), localSongId);
            rewriteSongLinks(cells.get(1), localSongId);
            rewriteArtistLinks(cells.get(2), artistCache);
        }

        section.select("a[href]").forEach(link -> {
            String href = link.attr("href");
            if (!href.startsWith("/songs/") && !href.startsWith("/artists/")) {
                link.unwrap();
            }
        });
    }

    private String getConnectionSectionHeading(org.jsoup.nodes.Element section) {
        org.jsoup.nodes.Element heading = section.selectFirst("h3, h2");
        return heading != null ? heading.text().toLowerCase().trim() : "";
    }

    private String findSeeAllUrl(org.jsoup.nodes.Element section) {
        org.jsoup.nodes.Element seeAllLink = findSeeAllLink(section);
        return seeAllLink != null ? seeAllLink.absUrl("href") : null;
    }

    private org.jsoup.nodes.Element findSeeAllLink(org.jsoup.nodes.Element section) {
        for (org.jsoup.nodes.Element link : section.select("a[href]")) {
            if ("see all".equalsIgnoreCase(link.text().trim())) {
                return link;
            }
        }
        return null;
    }

    private void removeSeeAllLinks(org.jsoup.nodes.Element section) {
        for (org.jsoup.nodes.Element link : section.select("a[href]")) {
            if ("see all".equalsIgnoreCase(link.text().trim())) {
                link.remove();
            }
        }
    }

    private String extractPrimaryArtistName(org.jsoup.nodes.Element artistCell) {
        org.jsoup.nodes.Element firstArtistLink = artistCell.selectFirst("a");
        if (firstArtistLink != null) {
            return firstArtistLink.text().trim();
        }

        String artistText = artistCell.text().trim();
        if (artistText.isEmpty()) {
            return artistText;
        }

        return artistText.split("(?i)\\s+(feat\\.?|featuring|and|&)\\s+")[0].trim();
    }

    private void rewriteSongLinks(org.jsoup.nodes.Element cell, Integer localSongId) {
        for (org.jsoup.nodes.Element link : cell.select("a[href]")) {
            if (localSongId != null) {
                link.attr("href", "/songs/" + localSongId);
            } else {
                link.unwrap();
            }
        }
    }

    private void rewriteArtistLinks(org.jsoup.nodes.Element cell, Map<String, Integer> artistCache) {
        for (org.jsoup.nodes.Element link : cell.select("a[href]")) {
            Integer localArtistId = findLocalArtistId(link.text().trim(), artistCache);
            if (localArtistId != null) {
                link.attr("href", "/artists/" + localArtistId);
            } else {
                link.unwrap();
            }
        }
    }

    private Integer findLocalArtistId(String artistName, Map<String, Integer> artistCache) {
        if (artistName == null || artistName.isBlank()) {
            return null;
        }

        String cacheKey = normalizeForLookup(artistName);
        if (artistCache.containsKey(cacheKey)) {
            return artistCache.get(cacheKey);
        }

        String sql = "SELECT id FROM Artist WHERE " + StringNormalizer.sqlNormalizeColumn("name") + " = ? LIMIT 1";
        List<Integer> results = jdbcTemplate.query(sql, (rs, rowNum) -> rs.getInt("id"), cacheKey);
        Integer artistId = results.isEmpty() ? null : results.get(0);
        artistCache.put(cacheKey, artistId);
        return artistId;
    }

    private Integer findLocalSongId(String artistName, String songTitle, Map<String, Integer> songCache) {
        if (artistName == null || artistName.isBlank() || songTitle == null || songTitle.isBlank()) {
            return null;
        }

        String normalizedArtist = normalizeForLookup(artistName);
        String normalizedSong = normalizeForLookup(songTitle);
        String normalizedWhoSampledSong = normalizeForWhoSampledLookup(songTitle);
        String cacheKey = normalizedArtist + "||" + normalizedWhoSampledSong;
        if (songCache.containsKey(cacheKey)) {
            return songCache.get(cacheKey);
        }

        String sql = "SELECT s.id, s.name FROM Song s JOIN Artist a ON s.artist_id = a.id " +
                "WHERE " + StringNormalizer.sqlNormalizeColumn("a.name") + " = ?";
        List<Map<String, Object>> results = jdbcTemplate.queryForList(sql, normalizedArtist);

        Integer songId = null;
        for (Map<String, Object> result : results) {
            String candidateTitle = result.get("name") != null ? result.get("name").toString() : "";
            String candidateNormalized = normalizeForLookup(candidateTitle);
            String candidateWhoSampledNormalized = normalizeForWhoSampledLookup(candidateTitle);

            if (candidateNormalized.equals(normalizedSong)
                    || candidateNormalized.equals(normalizedWhoSampledSong)
                    || candidateWhoSampledNormalized.equals(normalizedSong)
                    || candidateWhoSampledNormalized.equals(normalizedWhoSampledSong)) {
                songId = ((Number) result.get("id")).intValue();
                break;
            }
        }

        // If no match found, try stripping title variants like "Single Version", "Radio Edit", "Album Version"
        if (songId == null) {
            String strippedSong = stripTitleVariants(songTitle);
            if (!strippedSong.equals(songTitle)) {
                String strippedNormalized = normalizeForLookup(strippedSong);
                String strippedWhoSampled = normalizeForWhoSampledLookup(strippedSong);
                for (Map<String, Object> result : results) {
                    String candidateTitle = result.get("name") != null ? result.get("name").toString() : "";
                    String candidateNormalized = normalizeForLookup(candidateTitle);
                    String candidateWhoSampledNormalized = normalizeForWhoSampledLookup(candidateTitle);

                    if (candidateNormalized.equals(strippedNormalized)
                            || candidateNormalized.equals(strippedWhoSampled)
                            || candidateWhoSampledNormalized.equals(strippedNormalized)
                            || candidateWhoSampledNormalized.equals(strippedWhoSampled)) {
                        songId = ((Number) result.get("id")).intValue();
                        break;
                    }
                }
            }
        }

        songCache.put(cacheKey, songId);
        return songId;
    }

    private String stripTitleVariants(String title) {
        if (title == null) return title;
        // Strip common title variants like "Single Version", "Radio Edit", "Album Version", etc.
        return title
                .replaceAll("(?i)\\s*[-–]\\s*(Single|Album|Extended|Clean|Explicit|Radio|Acoustic|Remix|Remaster|Version|Edit|Mix).*$", "")
                .replaceAll("(?i)\\s*\\(\s*(Single|Album|Extended|Clean|Explicit|Radio|Acoustic|Remix|Remaster|Version|Edit|Mix|Feat\\..*?)\\s*\\).*$", "")
                .replaceAll("(?i)\\s*\\[\\s*(Single|Album|Extended|Clean|Explicit|Radio|Acoustic|Remix|Remaster|Version|Edit|Mix).*?\\].*$", "")
                .trim();
    }

    private String normalizeForLookup(String value) {
        if (value == null) {
            return "";
        }

        return StringNormalizer.stripAccents(value.toLowerCase().trim()).replace("'", "");
    }

    private String normalizeForWhoSampledLookup(String value) {
        return normalizeForLookup(normalizeWhoSampledSongTitle(value))
                .replace("[", "")
                .replace("]", "");
    }

    private String absolutizeSrcSet(String srcSet, String baseUri) {
        String[] entries = srcSet.split(",");
        java.util.List<String> rewrittenEntries = new java.util.ArrayList<>();

        for (String entry : entries) {
            String trimmedEntry = entry.trim();
            if (trimmedEntry.isEmpty()) {
                continue;
            }

            String[] parts = trimmedEntry.split("\\s+", 2);
            String absoluteUrl = org.jsoup.internal.StringUtil.resolve(baseUri, parts[0]);
            if (parts.length == 2) {
                rewrittenEntries.add(absoluteUrl + " " + parts[1]);
            } else {
                rewrittenEntries.add(absoluteUrl);
            }
        }

        return String.join(", ", rewrittenEntries);
    }

    private org.jsoup.nodes.Document fetchWhoSampledDocument(String fullUrl) throws IOException {
        org.jsoup.nodes.Document curlDocument = fetchWhoSampledDocumentWithCurl(fullUrl);
        if (curlDocument != null) {
            return curlDocument;
        }

        return org.jsoup.Jsoup.connect(fullUrl)
            .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")
            .referrer("https://www.google.com/")
            .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
            .header("Accept-Language", "en-US,en;q=0.9")
            .header("Cache-Control", "no-cache")
            .header("Pragma", "no-cache")
            .header("Sec-Fetch-Site", "none")
            .header("Sec-Fetch-Mode", "navigate")
            .header("Sec-Fetch-User", "?1")
            .header("Sec-Fetch-Dest", "document")
            .timeout(10000)
            .get();
    }

    private org.jsoup.nodes.Document fetchWhoSampledDocumentWithCurl(String fullUrl) {
        File tempFile = null;
        List<String> command = new ArrayList<>();
        command.add("curl.exe");
        command.add("-L");
        command.add("--silent");
        command.add("--show-error");
        command.add("--compressed");
        command.add("--max-time");
        command.add("20");
        command.add(fullUrl);
        command.add("-H");
        command.add("User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36");
        command.add("-H");
        command.add("Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8");
        command.add("-H");
        command.add("Accept-Language: en-US,en;q=0.9");
        command.add("-H");
        command.add("Cache-Control: no-cache");
        command.add("-H");
        command.add("Pragma: no-cache");
        command.add("-H");
        command.add("Sec-Fetch-Site: none");
        command.add("-H");
        command.add("Sec-Fetch-Mode: navigate");
        command.add("-H");
        command.add("Sec-Fetch-User: ?1");
        command.add("-H");
        command.add("Sec-Fetch-Dest: document");

        try {
            tempFile = Files.createTempFile("whosampled-", ".html").toFile();
            command.add("--output");
            command.add(tempFile.getAbsolutePath());
        } catch (IOException e) {
            System.out.println("Unable to create temp file for WhoSampled curl fetch: " + e.getMessage());
            return null;
        }

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true);

        try {
            Process process = processBuilder.start();
            boolean finished = process.waitFor(20, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                System.out.println("curl.exe timed out while fetching WhoSampled page.");
                return null;
            }

            String stderrOutput;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder builder = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    builder.append(line).append('\n');
                }
                stderrOutput = builder.toString();
            }

            if (process.exitValue() != 0) {
                System.out.println("curl.exe did not return usable WhoSampled HTML. Exit code: " + process.exitValue());
                if (!stderrOutput.isBlank()) {
                    System.out.println(stderrOutput);
                }
                return null;
            }

            String output = Files.readString(tempFile.toPath(), StandardCharsets.UTF_8);
            if (output.isBlank()) {
                System.out.println("curl.exe completed but returned an empty WhoSampled response.");
                return null;
            }

            if (output.contains("Song Connections") || output.contains("Rain on Me by Ashanti") || output.contains("WhoSampled")) {
                return org.jsoup.Jsoup.parse(output, fullUrl);
            }

            System.out.println("curl.exe output did not look like a WhoSampled song page.");
        } catch (IOException e) {
            System.out.println("curl.exe is not available for WhoSampled fetch: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("curl.exe fetch was interrupted.");
        } finally {
            if (tempFile != null && tempFile.exists() && !tempFile.delete()) {
                tempFile.deleteOnExit();
            }
        }

        return null;
    }
    
    /**
     * Fetch release date and song length from iTunes XML for a specific song.
     * Returns JSON with releaseDate (YYYY-MM-DD) and lengthSeconds.
     */
    @PostMapping("/{id}/fetch-itunes-data")
    @ResponseBody
    public Map<String, Object> fetchItunesData(@PathVariable Integer id) {
        Map<String, Object> response = new java.util.HashMap<>();
        
        try {
            // Get the song details
            Optional<Song> songOpt = songService.getSongById(id);
            if (!songOpt.isPresent()) {
                response.put("success", false);
                response.put("message", "Song not found");
                return response;
            }
            
            Song song = songOpt.get();
            
            // Get artist and album names
            String artistName = null;
            String albumName = null;
            
            if (song.getArtistId() != null) {
                Optional<Artist> artistOpt = artistService.getArtistById(song.getArtistId());
                if (artistOpt.isPresent()) {
                    artistName = artistOpt.get().getName();
                }
            }
            
            if (song.getAlbumId() != null) {
                Optional<Album> albumOpt = albumService.getAlbumById(song.getAlbumId());
                if (albumOpt.isPresent()) {
                    albumName = albumOpt.get().getName();
                }
            }
            
            // Look up in iTunes XML
            iTunesLibraryService.iTunesTrackData trackData = 
                iTunesLibraryService.findTrackData(song.getName(), artistName, albumName);
            
            if (trackData != null) {
                response.put("success", true);
                response.put("releaseDate", trackData.releaseDate);
                response.put("lengthSeconds", trackData.lengthSeconds);
                response.put("trackNumber", trackData.trackNumber);
                response.put("matchType", trackData.matchType);
                
                // Format length as mm:ss for display
                if (trackData.lengthSeconds != null) {
                    int minutes = trackData.lengthSeconds / 60;
                    int seconds = trackData.lengthSeconds % 60;
                    response.put("lengthFormatted", String.format("%d:%02d", minutes, seconds));
                }
            } else {
                response.put("success", false);
                response.put("message", "Song not found in iTunes library");
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            response.put("success", false);
            response.put("message", "Error: " + e.getMessage());
        }
        
        return response;
    }

    /**
     * Fetch release date and song length from iTunes XML by artist/album/song names.
     * Accepts JSON body: { "songName": "...", "artistName": "...", "albumName": "..." }
     */
    @PostMapping("/fetch-itunes-by-fields")
    @ResponseBody
    public Map<String, Object> fetchItunesByFields(@RequestBody Map<String, String> payload) {
        Map<String, Object> response = new java.util.HashMap<>();

        try {
            String songName = payload.get("songName");
            String artistName = payload.get("artistName");
            String albumName = payload.get("albumName");

            if ((songName == null || songName.trim().isEmpty()) && (artistName == null || artistName.trim().isEmpty())) {
                response.put("success", false);
                response.put("message", "Missing song or artist name");
                return response;
            }

            iTunesLibraryService.iTunesTrackData trackData =
                    iTunesLibraryService.findTrackData(songName, artistName, albumName);

            if (trackData != null) {
                // Determine whether we should populate the song's release date
                String albumReleaseDate = null;
                if (albumName != null && artistName != null && !albumName.trim().isEmpty() && !artistName.trim().isEmpty()) {
                    albumReleaseDate = albumService.findAlbumReleaseDateByNameAndArtist(albumName, artistName);
                }

                boolean populateReleaseDate = false;
                if (albumReleaseDate == null || albumReleaseDate.trim().isEmpty()) {
                    // No album release date -> safe to populate
                    populateReleaseDate = true;
                } else if (trackData.releaseDate != null && !trackData.releaseDate.equals(albumReleaseDate)) {
                    // Only populate if different from album release date
                    populateReleaseDate = true;
                }

                response.put("success", true);
                response.put("releaseDate", trackData.releaseDate);
                response.put("lengthSeconds", trackData.lengthSeconds);
                response.put("trackNumber", trackData.trackNumber);
                response.put("matchType", trackData.matchType);
                response.put("populateReleaseDate", populateReleaseDate);
                if (trackData.lengthSeconds != null) {
                    int minutes = trackData.lengthSeconds / 60;
                    int seconds = trackData.lengthSeconds % 60;
                    response.put("lengthFormatted", String.format("%d:%02d", minutes, seconds));
                }
            } else {
                response.put("success", false);
                response.put("message", "Song not found in iTunes library");
            }

        } catch (Exception e) {
            e.printStackTrace();
            response.put("success", false);
            response.put("message", "Error: " + e.getMessage());
        }

        return response;
    }
    
    @PostMapping("/create")
    @ResponseBody
    public Map<String, Object> createSong(@RequestBody Song song) {
        try {
            Song created = songService.createSong(song);
            Map<String, Object> response = new java.util.HashMap<>();
            response.put("id", created.getId());
            response.put("name", created.getName());
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create song");
        }
    }
    
    /**
     * Helper method to build ChartFilterDTO from request parameters.
     * Centralizes all filter parameter handling for chart endpoints.
     */
    private ChartFilterDTO buildChartFilter(
            String q, List<Integer> artist, List<Integer> album, List<Integer> song,
            List<Integer> genre, String genreMode,
            List<Integer> subgenre, String subgenreMode,
            List<Integer> language, String languageMode,
            List<Integer> gender, String genderMode,
            List<Integer> ethnicity, String ethnicityMode,
            List<String> country, String countryMode,
            List<String> account, String accountMode,
            String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
            String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode, String firstListenedDateEntity,
            String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode, String lastListenedDateEntity,
            String lastFullListenDate, String lastFullListenDateFrom, String lastFullListenDateTo, String lastFullListenDateMode,
            String listenedDateFrom, String listenedDateTo,
            Integer playCountMin, Integer playCountMax, String playCountEntity,
            String hasFeaturedArtists, String isBand, String isSingle, String inItunes,
            Integer itunesPresenceMin, Integer itunesPresenceMax,
            Integer limit,
            // Albums chart filters
            Integer albumsWeeklyChartPeak, Integer albumsWeeklyChartWeeks,
            Integer albumsSeasonalChartPeak, Integer albumsSeasonalChartSeasons,
            Integer albumsYearlyChartPeak, Integer albumsYearlyChartYears,
            // Songs chart filters
            Integer songsWeeklyChartPeak, Integer songsWeeklyChartWeeks,
            Integer songsSeasonalChartPeak, Integer songsSeasonalChartSeasons,
            Integer songsYearlyChartPeak, Integer songsYearlyChartYears,
            // Age/date filters
            Integer ageMin, Integer ageMax, String ageMode,
            Integer ageAtReleaseMin, Integer ageAtReleaseMax,
            String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
            String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode) {
        
        return ChartFilterDTO.builder()
            .setName(q)
            .setArtistIds(artist)
            .setAlbumIds(album)
            .setSongIds(song)
            .setGenreIds(genre)
            .setGenreMode(genreMode)
            .setSubgenreIds(subgenre)
            .setSubgenreMode(subgenreMode)
            .setLanguageIds(language)
            .setLanguageMode(languageMode)
            .setGenderIds(gender)
            .setGenderMode(genderMode)
            .setEthnicityIds(ethnicity)
            .setEthnicityMode(ethnicityMode)
            .setCountries(country)
            .setCountryMode(countryMode)
            .setAccounts(account)
            .setAccountMode(accountMode)
            .setReleaseDate(DateFormatUtils.convertToIsoFormat(releaseDate))
            .setReleaseDateFrom(DateFormatUtils.convertToIsoFormat(releaseDateFrom))
            .setReleaseDateTo(DateFormatUtils.convertToIsoFormat(releaseDateTo))
            .setReleaseDateMode(releaseDateMode)
            .setFirstListenedDate(DateFormatUtils.convertToIsoFormat(firstListenedDate))
            .setFirstListenedDateFrom(DateFormatUtils.convertToIsoFormat(firstListenedDateFrom))
            .setFirstListenedDateTo(DateFormatUtils.convertToIsoFormat(firstListenedDateTo))
            .setFirstListenedDateMode(firstListenedDateMode)
            .setFirstListenedDateEntity(firstListenedDateEntity)
            .setLastListenedDate(DateFormatUtils.convertToIsoFormat(lastListenedDate))
            .setLastListenedDateFrom(DateFormatUtils.convertToIsoFormat(lastListenedDateFrom))
            .setLastListenedDateTo(DateFormatUtils.convertToIsoFormat(lastListenedDateTo))
            .setLastListenedDateMode(lastListenedDateMode)
            .setLastListenedDateEntity(lastListenedDateEntity)
            .setListenedDateFrom(DateFormatUtils.convertToIsoFormat(listenedDateFrom))
            .setListenedDateTo(DateFormatUtils.convertToIsoFormat(listenedDateTo))
            .setPlayCountMin(playCountMin)
            .setPlayCountMax(playCountMax)
            .setPlayCountEntity(playCountEntity)
            .setHasFeaturedArtists(hasFeaturedArtists)
            .setIsBand(isBand)
            .setIsSingle(isSingle)
            .setInItunes(inItunes)
            .setItunesPresenceMin(itunesPresenceMin)
            .setItunesPresenceMax(itunesPresenceMax)
            .setTopLimit(limit)
            // Albums chart filters
            .setAlbumsWeeklyChartPeak(albumsWeeklyChartPeak)
            .setAlbumsWeeklyChartWeeks(albumsWeeklyChartWeeks)
            .setAlbumsSeasonalChartPeak(albumsSeasonalChartPeak)
            .setAlbumsSeasonalChartSeasons(albumsSeasonalChartSeasons)
            .setAlbumsYearlyChartPeak(albumsYearlyChartPeak)
            .setAlbumsYearlyChartYears(albumsYearlyChartYears)
            // Songs chart filters
            .setSongsWeeklyChartPeak(songsWeeklyChartPeak)
            .setSongsWeeklyChartWeeks(songsWeeklyChartWeeks)
            .setSongsSeasonalChartPeak(songsSeasonalChartPeak)
            .setSongsSeasonalChartSeasons(songsSeasonalChartSeasons)
            .setSongsYearlyChartPeak(songsYearlyChartPeak)
            .setSongsYearlyChartYears(songsYearlyChartYears)
            // Age/date filters
            .setAgeMin(ageMin)
            .setAgeMax(ageMax)
            .setAgeMode(ageMode)
            .setAgeAtReleaseMin(ageAtReleaseMin)
            .setAgeAtReleaseMax(ageAtReleaseMax)
            .setBirthDate(DateFormatUtils.convertToIsoFormat(birthDate))
            .setBirthDateFrom(DateFormatUtils.convertToIsoFormat(birthDateFrom))
            .setBirthDateTo(DateFormatUtils.convertToIsoFormat(birthDateTo))
            .setBirthDateMode(birthDateMode)
            .setDeathDate(DateFormatUtils.convertToIsoFormat(deathDate))
            .setDeathDateFrom(DateFormatUtils.convertToIsoFormat(deathDateFrom))
            .setDeathDateTo(DateFormatUtils.convertToIsoFormat(deathDateTo))
            .setDeathDateMode(deathDateMode);
    }
    
    // API endpoint for General tab chart data (pie charts)
    @GetMapping("/api/charts/general")
    @ResponseBody
    public Map<String, Object> getGeneralChartData(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> artist,
            @RequestParam(required = false) List<Integer> album,
            @RequestParam(required = false) List<Integer> song,
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String lastFullListenDate,
            @RequestParam(required = false) String lastFullListenDateFrom,
            @RequestParam(required = false) String lastFullListenDateTo,
            @RequestParam(required = false) String lastFullListenDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) Integer albumsWeeklyChartPeak,
            @RequestParam(required = false) Integer albumsWeeklyChartWeeks,
            @RequestParam(required = false) Integer albumsSeasonalChartPeak,
            @RequestParam(required = false) Integer albumsSeasonalChartSeasons,
            @RequestParam(required = false) Integer albumsYearlyChartPeak,
            @RequestParam(required = false) Integer albumsYearlyChartYears,
            @RequestParam(required = false) Integer songsWeeklyChartPeak,
            @RequestParam(required = false) Integer songsWeeklyChartWeeks,
            @RequestParam(required = false) Integer songsSeasonalChartPeak,
            @RequestParam(required = false) Integer songsSeasonalChartSeasons,
            @RequestParam(required = false) Integer songsYearlyChartPeak,
            @RequestParam(required = false) Integer songsYearlyChartYears,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
            @RequestParam(defaultValue = "false") boolean includeGroups,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam(defaultValue = "0") int limit,
            @RequestParam(required = false) String limitEntity) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, song, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            lastFullListenDate, lastFullListenDateFrom, lastFullListenDateTo, lastFullListenDateMode,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, inItunes, null, null, limit,
            albumsWeeklyChartPeak, albumsWeeklyChartWeeks,
            albumsSeasonalChartPeak, albumsSeasonalChartSeasons,
            albumsYearlyChartPeak, albumsYearlyChartYears,
            songsWeeklyChartPeak, songsWeeklyChartWeeks,
            songsSeasonalChartPeak, songsSeasonalChartSeasons,
            songsYearlyChartPeak, songsYearlyChartYears,
            ageMin, ageMax, ageMode,
            ageAtReleaseMin, ageAtReleaseMax,
            birthDate, birthDateFrom, birthDateTo, birthDateMode,
            deathDate, deathDateFrom, deathDateTo, deathDateMode);

        filter.setIncludeGroups(includeGroups);
        filter.setIncludeFeatured(includeFeatured);
        // For General tab, the limit should apply to the selected entity for play-derived metrics
        filter.setLimitEntity(limitEntity);

        return songService.getGeneralChartData(filter);
    }
    
    // API endpoint for Genre tab chart data (bar charts)
    @GetMapping("/api/charts/genre")
    @ResponseBody
    public Map<String, Object> getGenreChartData(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> artist,
            @RequestParam(required = false) List<Integer> album,
            @RequestParam(required = false) List<Integer> song,
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String lastFullListenDate,
            @RequestParam(required = false) String lastFullListenDateFrom,
            @RequestParam(required = false) String lastFullListenDateTo,
            @RequestParam(required = false) String lastFullListenDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) Integer albumsWeeklyChartPeak,
            @RequestParam(required = false) Integer albumsWeeklyChartWeeks,
            @RequestParam(required = false) Integer albumsSeasonalChartPeak,
            @RequestParam(required = false) Integer albumsSeasonalChartSeasons,
            @RequestParam(required = false) Integer albumsYearlyChartPeak,
            @RequestParam(required = false) Integer albumsYearlyChartYears,
            @RequestParam(required = false) Integer songsWeeklyChartPeak,
            @RequestParam(required = false) Integer songsWeeklyChartWeeks,
            @RequestParam(required = false) Integer songsSeasonalChartPeak,
            @RequestParam(required = false) Integer songsSeasonalChartSeasons,
            @RequestParam(required = false) Integer songsYearlyChartPeak,
            @RequestParam(required = false) Integer songsYearlyChartYears,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
            @RequestParam(defaultValue = "false") boolean includeGroups,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam(defaultValue = "0") int limit) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, song, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            lastFullListenDate, lastFullListenDateFrom, lastFullListenDateTo, lastFullListenDateMode,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, inItunes, null, null, limit,
            albumsWeeklyChartPeak, albumsWeeklyChartWeeks,
            albumsSeasonalChartPeak, albumsSeasonalChartSeasons,
            albumsYearlyChartPeak, albumsYearlyChartYears,
            songsWeeklyChartPeak, songsWeeklyChartWeeks,
            songsSeasonalChartPeak, songsSeasonalChartSeasons,
            songsYearlyChartPeak, songsYearlyChartYears,
            ageMin, ageMax, ageMode,
            ageAtReleaseMin, ageAtReleaseMax,
            birthDate, birthDateFrom, birthDateTo, birthDateMode,
            deathDate, deathDateFrom, deathDateTo, deathDateMode);

        filter.setIncludeGroups(includeGroups);
        filter.setIncludeFeatured(includeFeatured);
        return songService.getGenreChartData(filter);
    }
    
    // API endpoint for Subgenre tab chart data (bar charts)
    @GetMapping("/api/charts/subgenre")
    @ResponseBody
    public Map<String, Object> getSubgenreChartData(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> artist,
            @RequestParam(required = false) List<Integer> album,
            @RequestParam(required = false) List<Integer> song,
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String lastFullListenDate,
            @RequestParam(required = false) String lastFullListenDateFrom,
            @RequestParam(required = false) String lastFullListenDateTo,
            @RequestParam(required = false) String lastFullListenDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) Integer albumsWeeklyChartPeak,
            @RequestParam(required = false) Integer albumsWeeklyChartWeeks,
            @RequestParam(required = false) Integer albumsSeasonalChartPeak,
            @RequestParam(required = false) Integer albumsSeasonalChartSeasons,
            @RequestParam(required = false) Integer albumsYearlyChartPeak,
            @RequestParam(required = false) Integer albumsYearlyChartYears,
            @RequestParam(required = false) Integer songsWeeklyChartPeak,
            @RequestParam(required = false) Integer songsWeeklyChartWeeks,
            @RequestParam(required = false) Integer songsSeasonalChartPeak,
            @RequestParam(required = false) Integer songsSeasonalChartSeasons,
            @RequestParam(required = false) Integer songsYearlyChartPeak,
            @RequestParam(required = false) Integer songsYearlyChartYears,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
            @RequestParam(defaultValue = "false") boolean includeGroups,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam(defaultValue = "0") int limit) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, song, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            lastFullListenDate, lastFullListenDateFrom, lastFullListenDateTo, lastFullListenDateMode,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, inItunes, null, null, limit,
            albumsWeeklyChartPeak, albumsWeeklyChartWeeks,
            albumsSeasonalChartPeak, albumsSeasonalChartSeasons,
            albumsYearlyChartPeak, albumsYearlyChartYears,
            songsWeeklyChartPeak, songsWeeklyChartWeeks,
            songsSeasonalChartPeak, songsSeasonalChartSeasons,
            songsYearlyChartPeak, songsYearlyChartYears,
            ageMin, ageMax, ageMode,
            ageAtReleaseMin, ageAtReleaseMax,
            birthDate, birthDateFrom, birthDateTo, birthDateMode,
            deathDate, deathDateFrom, deathDateTo, deathDateMode);

        filter.setIncludeGroups(includeGroups);
        filter.setIncludeFeatured(includeFeatured);
        return songService.getSubgenreChartData(filter);
    }
    
    // API endpoint for Ethnicity tab chart data (bar charts)
    @GetMapping("/api/charts/ethnicity")
    @ResponseBody
    public Map<String, Object> getEthnicityChartData(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> artist,
            @RequestParam(required = false) List<Integer> album,
            @RequestParam(required = false) List<Integer> song,
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String lastFullListenDate,
            @RequestParam(required = false) String lastFullListenDateFrom,
            @RequestParam(required = false) String lastFullListenDateTo,
            @RequestParam(required = false) String lastFullListenDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) Integer albumsWeeklyChartPeak,
            @RequestParam(required = false) Integer albumsWeeklyChartWeeks,
            @RequestParam(required = false) Integer albumsSeasonalChartPeak,
            @RequestParam(required = false) Integer albumsSeasonalChartSeasons,
            @RequestParam(required = false) Integer albumsYearlyChartPeak,
            @RequestParam(required = false) Integer albumsYearlyChartYears,
            @RequestParam(required = false) Integer songsWeeklyChartPeak,
            @RequestParam(required = false) Integer songsWeeklyChartWeeks,
            @RequestParam(required = false) Integer songsSeasonalChartPeak,
            @RequestParam(required = false) Integer songsSeasonalChartSeasons,
            @RequestParam(required = false) Integer songsYearlyChartPeak,
            @RequestParam(required = false) Integer songsYearlyChartYears,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
            @RequestParam(defaultValue = "false") boolean includeGroups,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam(defaultValue = "0") int limit) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, song, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            lastFullListenDate, lastFullListenDateFrom, lastFullListenDateTo, lastFullListenDateMode,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, inItunes, null, null, limit,
            albumsWeeklyChartPeak, albumsWeeklyChartWeeks,
            albumsSeasonalChartPeak, albumsSeasonalChartSeasons,
            albumsYearlyChartPeak, albumsYearlyChartYears,
            songsWeeklyChartPeak, songsWeeklyChartWeeks,
            songsSeasonalChartPeak, songsSeasonalChartSeasons,
            songsYearlyChartPeak, songsYearlyChartYears,
            ageMin, ageMax, ageMode,
            ageAtReleaseMin, ageAtReleaseMax,
            birthDate, birthDateFrom, birthDateTo, birthDateMode,
            deathDate, deathDateFrom, deathDateTo, deathDateMode);

        filter.setIncludeGroups(includeGroups);
        filter.setIncludeFeatured(includeFeatured);
        return songService.getEthnicityChartData(filter);
    }
    
    // API endpoint for Language tab chart data (bar charts)
    @GetMapping("/api/charts/language")
    @ResponseBody
    public Map<String, Object> getLanguageChartData(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> artist,
            @RequestParam(required = false) List<Integer> album,
            @RequestParam(required = false) List<Integer> song,
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String lastFullListenDate,
            @RequestParam(required = false) String lastFullListenDateFrom,
            @RequestParam(required = false) String lastFullListenDateTo,
            @RequestParam(required = false) String lastFullListenDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) Integer albumsWeeklyChartPeak,
            @RequestParam(required = false) Integer albumsWeeklyChartWeeks,
            @RequestParam(required = false) Integer albumsSeasonalChartPeak,
            @RequestParam(required = false) Integer albumsSeasonalChartSeasons,
            @RequestParam(required = false) Integer albumsYearlyChartPeak,
            @RequestParam(required = false) Integer albumsYearlyChartYears,
            @RequestParam(required = false) Integer songsWeeklyChartPeak,
            @RequestParam(required = false) Integer songsWeeklyChartWeeks,
            @RequestParam(required = false) Integer songsSeasonalChartPeak,
            @RequestParam(required = false) Integer songsSeasonalChartSeasons,
            @RequestParam(required = false) Integer songsYearlyChartPeak,
            @RequestParam(required = false) Integer songsYearlyChartYears,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
            @RequestParam(defaultValue = "false") boolean includeGroups,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam(defaultValue = "0") int limit) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, song, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            lastFullListenDate, lastFullListenDateFrom, lastFullListenDateTo, lastFullListenDateMode,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, inItunes, null, null, limit,
            albumsWeeklyChartPeak, albumsWeeklyChartWeeks,
            albumsSeasonalChartPeak, albumsSeasonalChartSeasons,
            albumsYearlyChartPeak, albumsYearlyChartYears,
            songsWeeklyChartPeak, songsWeeklyChartWeeks,
            songsSeasonalChartPeak, songsSeasonalChartSeasons,
            songsYearlyChartPeak, songsYearlyChartYears,
            ageMin, ageMax, ageMode,
            ageAtReleaseMin, ageAtReleaseMax,
            birthDate, birthDateFrom, birthDateTo, birthDateMode,
            deathDate, deathDateFrom, deathDateTo, deathDateMode);

        filter.setIncludeGroups(includeGroups);
        filter.setIncludeFeatured(includeFeatured);
        return songService.getLanguageChartData(filter);
    }
    
    // API endpoint for Country tab chart data (bar charts)
    @GetMapping("/api/charts/country")
    @ResponseBody
    public Map<String, Object> getCountryChartData(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> artist,
            @RequestParam(required = false) List<Integer> album,
            @RequestParam(required = false) List<Integer> song,
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String lastFullListenDate,
            @RequestParam(required = false) String lastFullListenDateFrom,
            @RequestParam(required = false) String lastFullListenDateTo,
            @RequestParam(required = false) String lastFullListenDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) Integer albumsWeeklyChartPeak,
            @RequestParam(required = false) Integer albumsWeeklyChartWeeks,
            @RequestParam(required = false) Integer albumsSeasonalChartPeak,
            @RequestParam(required = false) Integer albumsSeasonalChartSeasons,
            @RequestParam(required = false) Integer albumsYearlyChartPeak,
            @RequestParam(required = false) Integer albumsYearlyChartYears,
            @RequestParam(required = false) Integer songsWeeklyChartPeak,
            @RequestParam(required = false) Integer songsWeeklyChartWeeks,
            @RequestParam(required = false) Integer songsSeasonalChartPeak,
            @RequestParam(required = false) Integer songsSeasonalChartSeasons,
            @RequestParam(required = false) Integer songsYearlyChartPeak,
            @RequestParam(required = false) Integer songsYearlyChartYears,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
            @RequestParam(defaultValue = "false") boolean includeGroups,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam(defaultValue = "0") int limit) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, song, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            lastFullListenDate, lastFullListenDateFrom, lastFullListenDateTo, lastFullListenDateMode,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, inItunes, null, null, limit,
            albumsWeeklyChartPeak, albumsWeeklyChartWeeks,
            albumsSeasonalChartPeak, albumsSeasonalChartSeasons,
            albumsYearlyChartPeak, albumsYearlyChartYears,
            songsWeeklyChartPeak, songsWeeklyChartWeeks,
            songsSeasonalChartPeak, songsSeasonalChartSeasons,
            songsYearlyChartPeak, songsYearlyChartYears,
            ageMin, ageMax, ageMode,
            ageAtReleaseMin, ageAtReleaseMax,
            birthDate, birthDateFrom, birthDateTo, birthDateMode,
            deathDate, deathDateFrom, deathDateTo, deathDateMode);

        filter.setIncludeGroups(includeGroups);
        filter.setIncludeFeatured(includeFeatured);
        return songService.getCountryChartData(filter);
    }
    
    // API endpoint for Release Year tab chart data (bar charts)
    @GetMapping("/api/charts/releaseYear")
    @ResponseBody
    public Map<String, Object> getReleaseYearChartData(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> artist,
            @RequestParam(required = false) List<Integer> album,
            @RequestParam(required = false) List<Integer> song,
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String lastFullListenDate,
            @RequestParam(required = false) String lastFullListenDateFrom,
            @RequestParam(required = false) String lastFullListenDateTo,
            @RequestParam(required = false) String lastFullListenDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) Integer albumsWeeklyChartPeak,
            @RequestParam(required = false) Integer albumsWeeklyChartWeeks,
            @RequestParam(required = false) Integer albumsSeasonalChartPeak,
            @RequestParam(required = false) Integer albumsSeasonalChartSeasons,
            @RequestParam(required = false) Integer albumsYearlyChartPeak,
            @RequestParam(required = false) Integer albumsYearlyChartYears,
            @RequestParam(required = false) Integer songsWeeklyChartPeak,
            @RequestParam(required = false) Integer songsWeeklyChartWeeks,
            @RequestParam(required = false) Integer songsSeasonalChartPeak,
            @RequestParam(required = false) Integer songsSeasonalChartSeasons,
            @RequestParam(required = false) Integer songsYearlyChartPeak,
            @RequestParam(required = false) Integer songsYearlyChartYears,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
            @RequestParam(defaultValue = "false") boolean includeGroups,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam(defaultValue = "0") int limit) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, song, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            lastFullListenDate, lastFullListenDateFrom, lastFullListenDateTo, lastFullListenDateMode,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, inItunes, null, null, limit,
            albumsWeeklyChartPeak, albumsWeeklyChartWeeks,
            albumsSeasonalChartPeak, albumsSeasonalChartSeasons,
            albumsYearlyChartPeak, albumsYearlyChartYears,
            songsWeeklyChartPeak, songsWeeklyChartWeeks,
            songsSeasonalChartPeak, songsSeasonalChartSeasons,
            songsYearlyChartPeak, songsYearlyChartYears,
            ageMin, ageMax, ageMode,
            ageAtReleaseMin, ageAtReleaseMax,
            birthDate, birthDateFrom, birthDateTo, birthDateMode,
            deathDate, deathDateFrom, deathDateTo, deathDateMode);

        filter.setIncludeGroups(includeGroups);
        filter.setIncludeFeatured(includeFeatured);
        return songService.getReleaseYearChartData(filter);
    }
    
    // API endpoint for Listen Year tab chart data (bar charts)
    @GetMapping("/api/charts/listenYear")
    @ResponseBody
    public Map<String, Object> getListenYearChartData(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> artist,
            @RequestParam(required = false) List<Integer> album,
            @RequestParam(required = false) List<Integer> song,
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String lastFullListenDate,
            @RequestParam(required = false) String lastFullListenDateFrom,
            @RequestParam(required = false) String lastFullListenDateTo,
            @RequestParam(required = false) String lastFullListenDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) Integer albumsWeeklyChartPeak,
            @RequestParam(required = false) Integer albumsWeeklyChartWeeks,
            @RequestParam(required = false) Integer albumsSeasonalChartPeak,
            @RequestParam(required = false) Integer albumsSeasonalChartSeasons,
            @RequestParam(required = false) Integer albumsYearlyChartPeak,
            @RequestParam(required = false) Integer albumsYearlyChartYears,
            @RequestParam(required = false) Integer songsWeeklyChartPeak,
            @RequestParam(required = false) Integer songsWeeklyChartWeeks,
            @RequestParam(required = false) Integer songsSeasonalChartPeak,
            @RequestParam(required = false) Integer songsSeasonalChartSeasons,
            @RequestParam(required = false) Integer songsYearlyChartPeak,
            @RequestParam(required = false) Integer songsYearlyChartYears,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
            @RequestParam(defaultValue = "false") boolean includeGroups,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam(defaultValue = "0") int limit) {

        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, song, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            lastFullListenDate, lastFullListenDateFrom, lastFullListenDateTo, lastFullListenDateMode,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, inItunes, null, null, limit,
            albumsWeeklyChartPeak, albumsWeeklyChartWeeks,
            albumsSeasonalChartPeak, albumsSeasonalChartSeasons,
            albumsYearlyChartPeak, albumsYearlyChartYears,
            songsWeeklyChartPeak, songsWeeklyChartWeeks,
            songsSeasonalChartPeak, songsSeasonalChartSeasons,
            songsYearlyChartPeak, songsYearlyChartYears,
            ageMin, ageMax, ageMode,
            ageAtReleaseMin, ageAtReleaseMax,
            birthDate, birthDateFrom, birthDateTo, birthDateMode,
            deathDate, deathDateFrom, deathDateTo, deathDateMode);

        filter.setIncludeGroups(includeGroups);
        filter.setIncludeFeatured(includeFeatured);
        return songService.getListenYearChartData(filter);
    }
    
    // API endpoint for Top tab data (artists, albums, songs tables)
    @GetMapping("/api/charts/top")
    @ResponseBody
    public Map<String, Object> getTopChartData(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> artist,
            @RequestParam(required = false) List<Integer> album,
            @RequestParam(required = false) List<Integer> song,
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String lastFullListenDate,
            @RequestParam(required = false) String lastFullListenDateFrom,
            @RequestParam(required = false) String lastFullListenDateTo,
            @RequestParam(required = false) String lastFullListenDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) Integer itunesPresenceMin,
            @RequestParam(required = false) Integer itunesPresenceMax,
            @RequestParam(required = false) Integer albumsWeeklyChartPeak,
            @RequestParam(required = false) Integer albumsWeeklyChartWeeks,
            @RequestParam(required = false) Integer albumsSeasonalChartPeak,
            @RequestParam(required = false) Integer albumsSeasonalChartSeasons,
            @RequestParam(required = false) Integer albumsYearlyChartPeak,
            @RequestParam(required = false) Integer albumsYearlyChartYears,
            @RequestParam(required = false) Integer songsWeeklyChartPeak,
            @RequestParam(required = false) Integer songsWeeklyChartWeeks,
            @RequestParam(required = false) Integer songsSeasonalChartPeak,
            @RequestParam(required = false) Integer songsSeasonalChartSeasons,
            @RequestParam(required = false) Integer songsYearlyChartPeak,
            @RequestParam(required = false) Integer songsYearlyChartYears,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode,
            @RequestParam(defaultValue = "false") boolean includeGroups,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            @RequestParam(defaultValue = "10") int limit) {
        
        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, song, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            lastFullListenDate, lastFullListenDateFrom, lastFullListenDateTo, lastFullListenDateMode,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, inItunes, itunesPresenceMin, itunesPresenceMax, limit,
            albumsWeeklyChartPeak, albumsWeeklyChartWeeks,
            albumsSeasonalChartPeak, albumsSeasonalChartSeasons,
            albumsYearlyChartPeak, albumsYearlyChartYears,
            songsWeeklyChartPeak, songsWeeklyChartWeeks,
            songsSeasonalChartPeak, songsSeasonalChartSeasons,
            songsYearlyChartPeak, songsYearlyChartYears,
            ageMin, ageMax, ageMode,
            ageAtReleaseMin, ageAtReleaseMax,
            birthDate, birthDateFrom, birthDateTo, birthDateMode,
            deathDate, deathDateFrom, deathDateTo, deathDateMode);
        
        // Set include toggles for top artists
        filter.setIncludeGroups(includeGroups);
        filter.setIncludeFeatured(includeFeatured);
        
        return songService.getTopChartData(filter);
    }
    
    // Legacy API endpoint for filtered gender breakdown chart data (kept for backwards compatibility)
    @GetMapping("/api/charts/gender")
    @ResponseBody
    public Map<String, Object> getFilteredGenderChartData(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> artist,
            @RequestParam(required = false) List<Integer> album,
            @RequestParam(required = false) List<Integer> song,
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
            @RequestParam(required = false) String releaseDate,
            @RequestParam(required = false) String releaseDateFrom,
            @RequestParam(required = false) String releaseDateTo,
            @RequestParam(required = false) String releaseDateMode,
            @RequestParam(required = false) String firstListenedDate,
            @RequestParam(required = false) String firstListenedDateFrom,
            @RequestParam(required = false) String firstListenedDateTo,
            @RequestParam(required = false) String firstListenedDateMode,
            @RequestParam(required = false) String firstListenedDateEntity,
            @RequestParam(required = false) String lastListenedDate,
            @RequestParam(required = false) String lastListenedDateFrom,
            @RequestParam(required = false) String lastListenedDateTo,
            @RequestParam(required = false) String lastListenedDateMode,
            @RequestParam(required = false) String lastListenedDateEntity,
            @RequestParam(required = false) String lastFullListenDate,
            @RequestParam(required = false) String lastFullListenDateFrom,
            @RequestParam(required = false) String lastFullListenDateTo,
            @RequestParam(required = false) String lastFullListenDateMode,
            @RequestParam(required = false) String listenedDateFrom,
            @RequestParam(required = false) String listenedDateTo,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) String playCountEntity,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(required = false) String inItunes,
            @RequestParam(required = false) Integer albumsWeeklyChartPeak,
            @RequestParam(required = false) Integer albumsWeeklyChartWeeks,
            @RequestParam(required = false) Integer albumsSeasonalChartPeak,
            @RequestParam(required = false) Integer albumsSeasonalChartSeasons,
            @RequestParam(required = false) Integer albumsYearlyChartPeak,
            @RequestParam(required = false) Integer albumsYearlyChartYears,
            @RequestParam(required = false) Integer songsWeeklyChartPeak,
            @RequestParam(required = false) Integer songsWeeklyChartWeeks,
            @RequestParam(required = false) Integer songsSeasonalChartPeak,
            @RequestParam(required = false) Integer songsSeasonalChartSeasons,
            @RequestParam(required = false) Integer songsYearlyChartPeak,
            @RequestParam(required = false) Integer songsYearlyChartYears,
            @RequestParam(required = false) Integer ageMin,
            @RequestParam(required = false) Integer ageMax,
            @RequestParam(required = false) String ageMode,
            @RequestParam(required = false) Integer ageAtReleaseMin,
            @RequestParam(required = false) Integer ageAtReleaseMax,
            @RequestParam(required = false) String birthDate,
            @RequestParam(required = false) String birthDateFrom,
            @RequestParam(required = false) String birthDateTo,
            @RequestParam(required = false) String birthDateMode,
            @RequestParam(required = false) String deathDate,
            @RequestParam(required = false) String deathDateFrom,
            @RequestParam(required = false) String deathDateTo,
            @RequestParam(required = false) String deathDateMode) {
        
        ChartFilterDTO filter = buildChartFilter(
            q, artist, album, song, genre, genreMode, subgenre, subgenreMode,
            language, languageMode, gender, genderMode, ethnicity, ethnicityMode,
            country, countryMode, account, accountMode,
            releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
            firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode, firstListenedDateEntity,
            lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode, lastListenedDateEntity,
            lastFullListenDate, lastFullListenDateFrom, lastFullListenDateTo, lastFullListenDateMode,
            listenedDateFrom, listenedDateTo, playCountMin, playCountMax, playCountEntity,
            hasFeaturedArtists, isBand, isSingle, inItunes, null, null, 0,
            albumsWeeklyChartPeak, albumsWeeklyChartWeeks,
            albumsSeasonalChartPeak, albumsSeasonalChartSeasons,
            albumsYearlyChartPeak, albumsYearlyChartYears,
            songsWeeklyChartPeak, songsWeeklyChartWeeks,
            songsSeasonalChartPeak, songsSeasonalChartSeasons,
            songsYearlyChartPeak, songsYearlyChartYears,
            ageMin, ageMax, ageMode,
            ageAtReleaseMin, ageAtReleaseMax,
            birthDate, birthDateFrom, birthDateTo, birthDateMode,
            deathDate, deathDateFrom, deathDateTo, deathDateMode);
        
        return songService.getFilteredChartData(filter);
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
    

    
    // ============================================
    // Featured Artists API Endpoints
    // ============================================
    
    /**
     * Search artists by name for the featured artists autocomplete
     */
    @GetMapping("/api/artists/search")
    @ResponseBody
    public List<FeaturedArtistDTO> searchArtists(@RequestParam String q) {
        return songService.searchArtists(q, 10);
    }
    
    /**
     * Get featured artists for a song
     */
    @GetMapping("/{id}/featured-artists")
    @ResponseBody
    public List<FeaturedArtistDTO> getFeaturedArtists(@PathVariable Integer id) {
        return songService.getFeaturedArtistsForSong(id);
    }
    
    /**
     * Save featured artists for a song
     */
    @PostMapping("/{id}/featured-artists")
    @ResponseBody
    public String saveFeaturedArtists(@PathVariable Integer id, @RequestBody List<Integer> artistIds) {
        try {
            songService.saveFeaturedArtists(id, artistIds);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    /**
     * Search songs by name for the unmatched plays assignment
     */
    @GetMapping("/api/search")
    @ResponseBody
    public List<Map<String, Object>> searchSongsForApi(
            @RequestParam(required = false) String artist,
            @RequestParam(required = false) String song,
            @RequestParam(required = false, defaultValue = "0") int limit) {
        return songService.searchSongs(artist, song, limit);
    }
    
    /**
     * Get all songs matching the current filters for playlist export (no pagination)
     * Returns minimal data needed for iTunes validation: id, name, artist, album
     */
    @GetMapping("/api/export")
    @ResponseBody
    public List<Map<String, Object>> getFilteredSongsForExport(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) List<Integer> artist,
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
            @RequestParam(required = false) List<String> account,
            @RequestParam(required = false) String accountMode,
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
            @RequestParam(required = false) String organized,
            @RequestParam(required = false) Integer imageCountMin,
            @RequestParam(required = false) Integer imageCountMax,
            @RequestParam(required = false) String hasFeaturedArtists,
            @RequestParam(required = false) String isBand,
            @RequestParam(required = false) String isSingle,
            @RequestParam(required = false) Integer playCountMin,
            @RequestParam(required = false) Integer playCountMax,
            @RequestParam(required = false) Integer trackNumber,
            @RequestParam(required = false) String trackNumberMode,
            @RequestParam(defaultValue = "plays") String sortby,
            @RequestParam(defaultValue = "desc") String sortdir,
            @RequestParam(defaultValue = "10000") int limit) {
        
        // Convert date formats
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
        
        // Get all songs matching filters (using a large limit instead of pagination)
        List<SongCardDTO> songs = songService.getSongs(
                q, artist, album, genre, genreMode, 
                subgenre, subgenreMode, language, languageMode, gender, genderMode,
                ethnicity, ethnicityMode, country, countryMode, account, accountMode,
                releaseDateConverted, releaseDateFromConverted, releaseDateToConverted, releaseDateMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand, isSingle,
                null, null, null,           // ageMin, ageMax, ageMode (not used in export)
                null, null,                 // ageAtReleaseMin, ageAtReleaseMax (not used in export)
                null, null, null, null,     // birthDate, birthDateFrom, birthDateTo, birthDateMode (not used in export)
                null, null, null, null,     // deathDate, deathDateFrom, deathDateTo, deathDateMode (not used in export)
                null, null,                 // itunesIdsJson, inItunes (not used in export)
                playCountMin, playCountMax,
                trackNumber, trackNumberMode,
                null, null, null,           // lengthMin, lengthMax, lengthMode (not used in export)
                null, null,                 // weeklyChartPeak, weeklyChartWeeks (not used in export)
                null, null,                 // seasonalChartPeak, seasonalChartSeasons (not used in export)
                null, null,                 // yearlyChartPeak, yearlyChartYears (not used in export)
                sortby, sortdir, 0, limit
        );
        
        // Convert to minimal export format
        return songs.stream().map(song -> {
            Map<String, Object> map = new java.util.HashMap<>();
            map.put("id", song.getId());
            map.put("name", song.getName());
            map.put("artist", song.getArtistName());
            map.put("album", song.getAlbumName());
            return map;
        }).toList();
    }

    /**
     * API: Get gender ID for a song (for UI coloring in chart editors).
     */
    @GetMapping("/{songId}/gender")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getSongGender(@PathVariable Integer songId) {
        Optional<Song> songOpt = songService.getSongById(songId);
        if (songOpt.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        
        Song song = songOpt.get();
        Integer genderId = null;
        
        if (song.getArtistId() != null) {
            Optional<Artist> artistOpt = artistService.getArtistById(song.getArtistId());
            if (artistOpt.isPresent()) {
                genderId = artistOpt.get().getGenderId();
            }
        }
        
        Map<String, Object> response = new HashMap<>();
        response.put("genderId", genderId);
        return ResponseEntity.ok(response);
    }
}


