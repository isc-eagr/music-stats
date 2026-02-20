package library.controller;

import library.dto.ArtistCardDTO;
import library.dto.ArtistSongDTO;
import library.dto.FeaturedArtistCardDTO;
import library.dto.GenderCountDTO;
import library.entity.Artist;
import library.repository.LookupRepository;
import library.service.ArtistService;
import library.service.ChartService;
import library.service.ItunesService;
import library.service.ThemeService;
import library.util.DateFormatUtils;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.multipart.MultipartFile;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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

    public ArtistController(ArtistService artistService, ChartService chartService, LookupRepository lookupRepository, ItunesService itunesService, ThemeService themeService) {
        this.artistService = artistService;
        this.chartService = chartService;
        this.lookupRepository = lookupRepository;
        this.itunesService = itunesService;
        this.themeService = themeService;
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
            @RequestParam(defaultValue = "plays") String sortby,
            @RequestParam(defaultValue = "desc") String sortdir,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "100") int perpage,
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
        
        // Get filtered and sorted artists
        List<ArtistCardDTO> artists = artistService.getArtists(
                q, gender, genderMode, ethnicity, ethnicityMode, genre, genreMode, 
                subgenre, subgenreMode, language, languageMode, country, countryMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                account, accountMode, ageMin, ageMax, ageMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, imageTheme, imageThemeMode, isBand, inItunes,
                playCountMin, playCountMax,
                albumCountMin, albumCountMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                songCountMin, songCountMax,
                sortby, sortdir, page, perpage
        );
        
        // Get total count for pagination
        long totalCount = artistService.countArtists(q, gender, genderMode, ethnicity, 
                ethnicityMode, genre, genreMode, subgenre, subgenreMode, language, 
                languageMode, country, countryMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                account, accountMode, ageMin, ageMax, ageMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, imageTheme, imageThemeMode, isBand, inItunes,
                playCountMin, playCountMax,
                albumCountMin, albumCountMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                songCountMin, songCountMax);
        int totalPages = (int) Math.ceil((double) totalCount / perpage);
        
        // Get gender counts for the filtered dataset
        GenderCountDTO genderCounts = artistService.countArtistsByGender(q, gender, genderMode, ethnicity, 
                ethnicityMode, genre, genreMode, subgenre, subgenreMode, language, 
                languageMode, country, countryMode,
                deathDateConverted, deathDateFromConverted, deathDateToConverted, deathDateMode,
                account, accountMode, ageMin, ageMax, ageMode,
                firstListenedDateConverted, firstListenedDateFromConverted, firstListenedDateToConverted, firstListenedDateMode,
                lastListenedDateConverted, lastListenedDateFromConverted, lastListenedDateToConverted, lastListenedDateMode,
                listenedDateFromConverted, listenedDateToConverted,
                organized, imageCountMin, imageCountMax, imageTheme, imageThemeMode, isBand, inItunes,
                playCountMin, playCountMax,
                albumCountMin, albumCountMax,
                birthDateConverted, birthDateFromConverted, birthDateToConverted, birthDateMode,
                songCountMin, songCountMax);
        
        // Add data to model
        model.addAttribute("currentSection", "artists");
        model.addAttribute("artists", artists);
        model.addAttribute("genderCounts", genderCounts);
        model.addAttribute("currentPage", page);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("perPage", perpage);
        model.addAttribute("startIndex", (page * perpage) + 1);
        model.addAttribute("endIndex", Math.min((page + 1) * perpage, totalCount));
        
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
        
        // Add filter options
        model.addAttribute("genders", artistService.getGenders());
        model.addAttribute("ethnicities", artistService.getEthnicities());
        model.addAttribute("genres", artistService.getGenres());
        model.addAttribute("subgenres", artistService.getSubGenres());
        model.addAttribute("languages", artistService.getLanguages());
        model.addAttribute("countries", artistService.getCountries());
        model.addAttribute("themes", themeService.getAllThemes());
        
        return "artists/list";
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
        byte[] image = artistService.getRawArtistImage(id);
        boolean hasImage = (image != null && image.length > 0);
        
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
        
        // Add includeMain toggle data (toggle is always shown, default is on)
        model.addAttribute("includeMain", includeMain);
        
        // Get group/member cards for Artist Associations tab
        List<FeaturedArtistCardDTO> groupArtistCards = artistService.getGroupsForArtist(id);
        List<FeaturedArtistCardDTO> memberArtistCards = artistService.getMembersForArtist(id);
        boolean hasMembers = !memberArtistCards.isEmpty();
        
        model.addAttribute("groupArtistCards", groupArtistCards);
        model.addAttribute("memberArtistCards", memberArtistCards);
        model.addAttribute("hasMembers", hasMembers);
        
        // Determine which data sources to use
        // includeGroups works independently from includeMain
        List<Integer> effectiveGroupIds = (includeGroups && hasGroups) ? groupIds : null;
        
        // Calculate featured additions (when includeFeatured is true)
        int featuredSongCountAddition = (includeFeatured && hasFeaturedSongs) ? artistService.getFeaturedSongCount(id) : 0;
        int featuredPlayCountAddition = (includeFeatured && hasFeaturedSongs) ? artistService.getFeaturedPlayCount(id) : 0;
        int featuredVatitoPlayAddition = (includeFeatured && hasFeaturedSongs) ? artistService.getFeaturedVatitoPlayCount(id) : 0;
        int featuredRobertloverPlayAddition = (includeFeatured && hasFeaturedSongs) ? artistService.getFeaturedRobertloverPlayCount(id) : 0;
        
        // Add album and song counts for quick stats
        if (includeMain && effectiveGroupIds != null) {
            // Main + Groups
            model.addAttribute("albumCount", artistService.getAggregatedAlbumCount(id, effectiveGroupIds));
            model.addAttribute("songCount", artistService.getAggregatedSongCount(id, effectiveGroupIds) + featuredSongCountAddition);
        } else if (includeMain && effectiveGroupIds == null) {
            // Main only
            int[] counts = artistService.getAlbumAndSongCounts(id);
            model.addAttribute("albumCount", counts[0]);
            model.addAttribute("songCount", counts[1] + featuredSongCountAddition);
        } else if (!includeMain && effectiveGroupIds != null) {
            // Groups only (no main) - pass 0 as ID to exclude main artist from aggregation
            model.addAttribute("albumCount", artistService.getAggregatedAlbumCount(0, effectiveGroupIds));
            model.addAttribute("songCount", artistService.getAggregatedSongCount(0, effectiveGroupIds) + featuredSongCountAddition);
        } else {
            // No main, no groups - only featured
            model.addAttribute("albumCount", 0);
            model.addAttribute("songCount", featuredSongCountAddition);
        }
        
        // Add play count for artist
        if (includeMain && effectiveGroupIds != null) {
            // Main + Groups
            model.addAttribute("artistPlayCount", artistService.getAggregatedPlayCount(id, effectiveGroupIds) + featuredPlayCountAddition);
            model.addAttribute("artistVatitoPlayCount", artistService.getAggregatedVatitoPlayCount(id, effectiveGroupIds) + featuredVatitoPlayAddition);
            model.addAttribute("artistRobertloverPlayCount", artistService.getAggregatedRobertloverPlayCount(id, effectiveGroupIds) + featuredRobertloverPlayAddition);
        } else if (includeMain && effectiveGroupIds == null) {
            // Main only
            model.addAttribute("artistPlayCount", artistService.getPlayCountForArtist(id) + featuredPlayCountAddition);
            model.addAttribute("artistVatitoPlayCount", artistService.getVatitoPlayCountForArtist(id) + featuredVatitoPlayAddition);
            model.addAttribute("artistRobertloverPlayCount", artistService.getRobertloverPlayCountForArtist(id) + featuredRobertloverPlayAddition);
        } else if (!includeMain && effectiveGroupIds != null) {
            // Groups only (no main) - pass 0 as ID to exclude main artist from aggregation
            model.addAttribute("artistPlayCount", artistService.getAggregatedPlayCount(0, effectiveGroupIds) + featuredPlayCountAddition);
            model.addAttribute("artistVatitoPlayCount", artistService.getAggregatedVatitoPlayCount(0, effectiveGroupIds) + featuredVatitoPlayAddition);
            model.addAttribute("artistRobertloverPlayCount", artistService.getAggregatedRobertloverPlayCount(0, effectiveGroupIds) + featuredRobertloverPlayAddition);
        } else {
            // No main, no groups - only featured
            model.addAttribute("artistPlayCount", featuredPlayCountAddition);
            model.addAttribute("artistVatitoPlayCount", featuredVatitoPlayAddition);
            model.addAttribute("artistRobertloverPlayCount", featuredRobertloverPlayAddition);
        }
        // Add per-account breakdown string for tooltip
        model.addAttribute("artistPlaysByAccount", artistService.getPlaysByAccountForArtist(id));
        
        // Add statistics for the artist
        if (includeMain && effectiveGroupIds != null) {
            // Main + Groups
            model.addAttribute("totalListeningTime", artistService.getAggregatedListeningTime(id, effectiveGroupIds));
            model.addAttribute("firstListenedDate", artistService.getAggregatedFirstListenedDate(id, effectiveGroupIds));
            model.addAttribute("lastListenedDate", artistService.getAggregatedLastListenedDate(id, effectiveGroupIds));
        } else if (includeMain && effectiveGroupIds == null) {
            // Main only
            model.addAttribute("totalListeningTime", artistService.getTotalListeningTimeForArtist(id));
            model.addAttribute("firstListenedDate", artistService.getFirstListenedDateForArtist(id));
            model.addAttribute("lastListenedDate", artistService.getLastListenedDateForArtist(id));
        } else if (!includeMain && effectiveGroupIds != null) {
            // Groups only (no main) - pass 0 as ID to exclude main artist from aggregation
            model.addAttribute("totalListeningTime", artistService.getAggregatedListeningTime(0, effectiveGroupIds));
            model.addAttribute("firstListenedDate", artistService.getAggregatedFirstListenedDate(0, effectiveGroupIds));
            model.addAttribute("lastListenedDate", artistService.getAggregatedLastListenedDate(0, effectiveGroupIds));
        } else {
            // No main, no groups - only featured
            if (includeFeatured && hasFeaturedSongs) {
                model.addAttribute("totalListeningTime", artistService.getFeaturedListeningTime(id));
                model.addAttribute("firstListenedDate", artistService.getFeaturedFirstListenedDate(id));
                model.addAttribute("lastListenedDate", artistService.getFeaturedLastListenedDate(id));
            } else {
                model.addAttribute("totalListeningTime", "0:00");
                model.addAttribute("firstListenedDate", null);
                model.addAttribute("lastListenedDate", null);
            }
        }
        
        // Add average song length and average plays per song statistics
        model.addAttribute("averageSongLength", artistService.getAverageSongLengthFormatted(id));
        model.addAttribute("averagePlaysPerSong", artistService.getAveragePlaysPerSong(id));
        
        // Add unique period stats for the artist (main artist only for now)
        model.addAttribute("uniqueDaysPlayed", artistService.getUniqueDaysPlayedForArtist(id));
        model.addAttribute("uniqueWeeksPlayed", artistService.getUniqueWeeksPlayedForArtist(id));
        model.addAttribute("uniqueMonthsPlayed", artistService.getUniqueMonthsPlayedForArtist(id));
        model.addAttribute("uniqueYearsPlayed", artistService.getUniqueYearsPlayedForArtist(id));
        
        // Calculate totals based on first listened date
        java.time.LocalDate firstListened = artistService.getFirstListenedDateAsLocalDateForArtist(id);
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
        
        // Add albums list for the artist
        if (includeMain && effectiveGroupIds != null) {
            // Main + Groups
            model.addAttribute("albums", artistService.getAggregatedAlbumsForArtist(id, effectiveGroupIds));
        } else if (includeMain && effectiveGroupIds == null) {
            // Main only
            model.addAttribute("albums", artistService.getAlbumsForArtist(id));
        } else if (!includeMain && effectiveGroupIds != null) {
            // Groups only (no main) - pass 0 as ID to exclude main artist from aggregation
            model.addAttribute("albums", artistService.getAggregatedAlbumsForArtist(0, effectiveGroupIds));
        } else {
            // No main, no groups - no albums to show
            model.addAttribute("albums", java.util.Collections.emptyList());
        }
        
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
        model.addAttribute("songs", songs);
        
        // Tab and plays data
        model.addAttribute("activeTab", tab);
        
        // Add collaborated artist cards (always pre-loaded for Artist Associations tab)
        if (includeMain && effectiveGroupIds != null) {
            // Main + Groups
            model.addAttribute("collaboratedArtistCards", artistService.getAggregatedCollaboratedArtists(id, effectiveGroupIds));
        } else if (includeMain && effectiveGroupIds == null) {
            // Main only
            model.addAttribute("collaboratedArtistCards", artistService.getCollaboratedArtistsForArtist(id));
        } else if (!includeMain && effectiveGroupIds != null) {
            // Groups only (no main) - pass 0 as ID to exclude main artist from aggregation
            model.addAttribute("collaboratedArtistCards", artistService.getAggregatedCollaboratedArtists(0, effectiveGroupIds));
        } else {
            // No main, no groups - only featured collaborators
            if (includeFeatured && hasFeaturedSongs) {
                model.addAttribute("collaboratedArtistCards", artistService.getFeaturedCollaboratedArtists(id));
            } else {
                model.addAttribute("collaboratedArtistCards", java.util.Collections.emptyList());
            }
        }
        
        // Always load plays data (eager loading for all tabs)
        {
            int pageSize = 100;
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
                model.addAttribute("playsPageSize", pageSize);
                model.addAttribute("playsTotalPages", (int) Math.ceil((double) totalCount / pageSize));
                model.addAttribute("playsByYear", artistService.getAggregatedPlaysByYear(id, effectiveGroupIds));
                model.addAttribute("playsByMonth", artistService.getAggregatedPlaysByMonth(id, effectiveGroupIds));
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
                model.addAttribute("playsPageSize", pageSize);
                model.addAttribute("playsTotalPages", (int) Math.ceil((double) totalCount / pageSize));
                model.addAttribute("playsByYear", artistService.getPlaysByYearForArtist(id));
                model.addAttribute("playsByMonth", artistService.getPlaysByMonthForArtist(id));
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
                model.addAttribute("playsPageSize", pageSize);
                model.addAttribute("playsTotalPages", (int) Math.ceil((double) totalCount / pageSize));
                model.addAttribute("playsByYear", artistService.getAggregatedPlaysByYear(0, effectiveGroupIds));
                model.addAttribute("playsByMonth", artistService.getAggregatedPlaysByMonth(0, effectiveGroupIds));
            } else {
                // No main, no groups - only featured
                if (includeFeatured && hasFeaturedSongs) {
                    plays = artistService.getFeaturedPlaysForArtist(id, playsPage, pageSize);
                    long totalCount = artistService.countFeaturedPlaysForArtist(id);
                    model.addAttribute("plays", plays);
                    model.addAttribute("playsTotalCount", totalCount);
                    model.addAttribute("playsPage", playsPage);
                    model.addAttribute("playsPageSize", pageSize);
                    model.addAttribute("playsTotalPages", (int) Math.ceil((double) totalCount / pageSize));
                    model.addAttribute("playsByYear", artistService.getFeaturedPlaysByYear(id));
                    model.addAttribute("playsByMonth", java.util.Collections.emptyList());
                } else {
                    model.addAttribute("plays", java.util.Collections.emptyList());
                    model.addAttribute("playsTotalCount", 0L);
                    model.addAttribute("playsPage", 0);
                    model.addAttribute("playsPageSize", pageSize);
                    model.addAttribute("playsTotalPages", 0);
                    model.addAttribute("playsByYear", java.util.Collections.emptyMap());
                    model.addAttribute("playsByMonth", java.util.Collections.emptyList());
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

            model.addAttribute("songChartHistory", songHistory);
            model.addAttribute("albumChartHistory", albumHistory);
            model.addAttribute("seasonalSongChartHistory", seasonalSongHistory);
            model.addAttribute("yearlySongChartHistory", yearlySongHistory);
            model.addAttribute("seasonalAlbumChartHistory", seasonalAlbumHistory);
            model.addAttribute("yearlyAlbumChartHistory", yearlyAlbumHistory);
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
        model.addAttribute("ranksByYear", artistService.getArtistRanksByYear(id));
        
        // Add Spanish Rap rank (special combination)
        if (artistService.isArtistSpanishRap(id)) {
            model.addAttribute("rankBySpanishRap", artistService.getArtistSpanishRapRank(id));
            model.addAttribute("rapGenreId", lookupRepository.getGenreIdByName("Rap"));
            model.addAttribute("spanishLanguageId", lookupRepository.getLanguageIdByName("Spanish"));
        }

        // Add new ranking chips
        model.addAttribute("overallPosition", artistService.getArtistOverallPosition(id));
        
        // Add #1 song achievement chips
        Integer numberOneSongsCount = chartService.getNumberOneSongsCount(id);
        model.addAttribute("numberOneSongsCount", numberOneSongsCount);
        model.addAttribute("numberOneWeeksCount", chartService.getNumberOneWeeksCount(id));
        
        // Get list of #1 songs for tooltip
        if (numberOneSongsCount != null && numberOneSongsCount > 0) {
            model.addAttribute("numberOneSongsList", chartService.getNumberOneSongNames(id));
        }
        
        // Add #1 album achievement chips
        Integer numberOneAlbumsCount = chartService.getNumberOneAlbumsCount(id);
        model.addAttribute("numberOneAlbumsCount", numberOneAlbumsCount);
        model.addAttribute("numberOneAlbumWeeksCount", chartService.getNumberOneAlbumWeeksCount(id));
        
        // Get list of #1 albums for tooltip
        if (numberOneAlbumsCount != null && numberOneAlbumsCount > 0) {
            model.addAttribute("numberOneAlbumsList", chartService.getNumberOneAlbumNames(id));
        }
        
        // Add #1 featured song achievement chips
        Integer numberOneFeaturedSongsCount = chartService.getNumberOneFeaturedSongsCount(id);
        model.addAttribute("numberOneFeaturedSongsCount", numberOneFeaturedSongsCount);
        model.addAttribute("numberOneFeaturedWeeksCount", chartService.getNumberOneFeaturedWeeksCount(id));
        
        // Get list of #1 featured songs for tooltip
        if (numberOneFeaturedSongsCount != null && numberOneFeaturedSongsCount > 0) {
            model.addAttribute("numberOneFeaturedSongsList", chartService.getNumberOneFeaturedSongNames(id));
        }
        
        // Reverse the year rankings order for artist detail page (most recent first)
        Map<Integer, Integer> yearRanks = artistService.getArtistRanksByYear(id);
        if (yearRanks != null && !yearRanks.isEmpty()) {
            // Convert to list, reverse it, and create a new LinkedHashMap in reversed order
            List<Map.Entry<Integer, Integer>> yearList = new java.util.ArrayList<>(yearRanks.entrySet());
            java.util.Collections.reverse(yearList);
            Map<Integer, Integer> reversedYearRanks = new java.util.LinkedHashMap<>();
            for (Map.Entry<Integer, Integer> entry : yearList) {
                reversedYearRanks.put(entry.getKey(), entry.getValue());
            }
            model.addAttribute("ranksByYear", reversedYearRanks);
        } else {
            model.addAttribute("ranksByYear", yearRanks);
        }

        return "artists/detail";
    }
    
    @PostMapping("/{id}")
    public String updateArtist(@PathVariable Integer id, @ModelAttribute Artist artist) {
        artist.setId(id);
        artistService.saveArtist(artist);
        return "redirect:/artists/" + id;
    }
    
    @GetMapping("/{id}/image")
    @ResponseBody
    public byte[] getArtistImage(@PathVariable Integer id,
                                 @RequestParam(required = false, defaultValue = "false") boolean raw) {
        return raw ? artistService.getRawArtistImage(id) : artistService.getArtistImage(id);
    }
    
    @PostMapping("/{id}/image")
    @ResponseBody
    public String uploadArtistImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            
            artistService.updateArtistImage(id, file.getBytes());
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
        var images = artistService.getSecondaryImages(id);
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
            @RequestParam(required = false, defaultValue = "20") int limit) {
        return artistService.searchArtists(q, limit);
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
    
    /**
     * Get the groups that an artist belongs to (for initial form population)
     */
    @GetMapping("/{id}/groups")
    @ResponseBody
    public List<Integer> getArtistGroups(@PathVariable Integer id) {
        return artistService.getGroupIdsForArtist(id);
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