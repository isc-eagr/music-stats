package library.controller;

import library.dto.TimeframeCardDTO;
import library.repository.LookupRepository;
import library.service.ChartService;
import library.service.TimeframeService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Controller
public class TimeframeController {
    
    private static final Set<String> VALID_PERIOD_TYPES = Set.of("days", "weeks", "months", "seasons", "years", "decades");
    
    private final TimeframeService timeframeService;
    private final ChartService chartService;
    private final LookupRepository lookupRepository;
    private final JdbcTemplate jdbcTemplate;
    
    public TimeframeController(TimeframeService timeframeService, ChartService chartService, LookupRepository lookupRepository, JdbcTemplate jdbcTemplate) {
        this.timeframeService = timeframeService;
        this.chartService = chartService;
        this.lookupRepository = lookupRepository;
        this.jdbcTemplate = jdbcTemplate;
    }
    
    @GetMapping("/{periodType}")
    public String listTimeframes(
            @PathVariable String periodType,
            // Winning attribute filters
            @RequestParam(required = false) List<Integer> winningGender,
            @RequestParam(required = false) String winningGenderMode,
            @RequestParam(required = false) List<Integer> winningGenre,
            @RequestParam(required = false) String winningGenreMode,
            @RequestParam(required = false) List<Integer> winningEthnicity,
            @RequestParam(required = false) String winningEthnicityMode,
            @RequestParam(required = false) List<Integer> winningLanguage,
            @RequestParam(required = false) String winningLanguageMode,
            @RequestParam(required = false) List<String> winningCountry,
            @RequestParam(required = false) String winningCountryMode,
            // Count range filters
            @RequestParam(required = false) Integer artistCountMin,
            @RequestParam(required = false) Integer artistCountMax,
            @RequestParam(required = false) Integer albumCountMin,
            @RequestParam(required = false) Integer albumCountMax,
            @RequestParam(required = false) Integer songCountMin,
            @RequestParam(required = false) Integer songCountMax,
            @RequestParam(required = false) Integer playsMin,
            @RequestParam(required = false) Integer playsMax,
            @RequestParam(required = false) Long timeMin,
            @RequestParam(required = false) Long timeMax,
            // Male percentage range filters
            @RequestParam(required = false) Double maleArtistPctMin,
            @RequestParam(required = false) Double maleArtistPctMax,
            @RequestParam(required = false) Double maleAlbumPctMin,
            @RequestParam(required = false) Double maleAlbumPctMax,
            @RequestParam(required = false) Double maleSongPctMin,
            @RequestParam(required = false) Double maleSongPctMax,
            @RequestParam(required = false) Double malePlayPctMin,
            @RequestParam(required = false) Double malePlayPctMax,
            @RequestParam(required = false) Double maleTimePctMin,
            @RequestParam(required = false) Double maleTimePctMax,
            // Perfect male filter (100% male across all metrics)
            @RequestParam(required = false) Boolean perfectMale,
            // Sorting and pagination
            @RequestParam(defaultValue = "period") String sortby,
            @RequestParam(defaultValue = "desc") String sortdir,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int perpage,
            HttpServletRequest request,
            Model model) {
        
        // Validate period type
        if (!VALID_PERIOD_TYPES.contains(periodType)) {
            return "redirect:/years";
        }
        
        // If perfectMale filter is enabled, set all male percentage mins to 100
        Double effectiveMaleArtistPctMin = maleArtistPctMin;
        Double effectiveMaleAlbumPctMin = maleAlbumPctMin;
        Double effectiveMaleSongPctMin = maleSongPctMin;
        Double effectiveMalePlayPctMin = malePlayPctMin;
        Double effectiveMaleTimePctMin = maleTimePctMin;
        
        if (Boolean.TRUE.equals(perfectMale)) {
            effectiveMaleArtistPctMin = 100.0;
            effectiveMaleAlbumPctMin = 100.0;
            effectiveMaleSongPctMin = 100.0;
            effectiveMalePlayPctMin = 100.0;
            effectiveMaleTimePctMin = 100.0;
        }
        
        // Get timeframe cards
        List<TimeframeCardDTO> timeframes = timeframeService.getTimeframeCards(
            periodType,
            winningGender, winningGenderMode,
            winningGenre, winningGenreMode,
            winningEthnicity, winningEthnicityMode,
            winningLanguage, winningLanguageMode,
            winningCountry, winningCountryMode,
            artistCountMin, artistCountMax,
            albumCountMin, albumCountMax,
            songCountMin, songCountMax,
            playsMin, playsMax,
            timeMin, timeMax,
            effectiveMaleArtistPctMin, maleArtistPctMax,
            effectiveMaleAlbumPctMin, maleAlbumPctMax,
            effectiveMaleSongPctMin, maleSongPctMax,
            effectiveMalePlayPctMin, malePlayPctMax,
            effectiveMaleTimePctMin, maleTimePctMax,
            sortby, sortdir, page, perpage
        );
        
        // Get total count for pagination
        long totalCount = timeframeService.countTimeframes(
            periodType,
            winningGender, winningGenderMode,
            winningGenre, winningGenreMode,
            winningEthnicity, winningEthnicityMode,
            winningLanguage, winningLanguageMode,
            winningCountry, winningCountryMode,
            artistCountMin, artistCountMax,
            albumCountMin, albumCountMax,
            songCountMin, songCountMax,
            playsMin, playsMax,
            timeMin, timeMax,
            effectiveMaleArtistPctMin, maleArtistPctMax,
            effectiveMaleAlbumPctMin, maleAlbumPctMax,
            effectiveMaleSongPctMin, maleSongPctMax,
            effectiveMalePlayPctMin, malePlayPctMax,
            effectiveMaleTimePctMin, maleTimePctMax
        );
        int totalPages = (int) Math.ceil((double) totalCount / perpage);
        
        // Add core data to model
        model.addAttribute("currentSection", periodType);
        model.addAttribute("periodType", periodType);
        model.addAttribute("periodTypeDisplay", formatPeriodTypeDisplay(periodType));
        model.addAttribute("timeframes", timeframes);
        
        // For weeks, populate hasChart status and weekComplete status
        if ("weeks".equals(periodType)) {
            Set<String> weeksWithCharts = chartService.getExistingChartPeriodKeys("song");
            for (TimeframeCardDTO tf : timeframes) {
                tf.setHasChart(weeksWithCharts.contains(tf.getPeriodKey()));
                tf.setWeekComplete(chartService.isWeekComplete(tf.getPeriodKey()));
                // Get #1 song/album if chart exists
                if (Boolean.TRUE.equals(tf.getHasChart())) {
                    Map<String, String> topEntries = chartService.getWeeklyChartTopEntries(tf.getPeriodKey());
                    tf.setNumberOneSongName(topEntries.get("song"));
                    tf.setNumberOneAlbumName(topEntries.get("album"));
                }
            }
        }
        
        // For seasons, populate chart status
        if ("seasons".equals(periodType)) {
            Set<String> seasonsWithFinalizedCharts = chartService.getFinalizedChartPeriodKeys("seasonal");
            Set<String> seasonsWithAnyCharts = chartService.getAllChartPeriodKeys("seasonal");
            for (TimeframeCardDTO tf : timeframes) {
                boolean hasChart = seasonsWithAnyCharts.contains(tf.getPeriodKey());
                boolean finalized = seasonsWithFinalizedCharts.contains(tf.getPeriodKey());
                tf.setHasSeasonalChart(hasChart);
                tf.setSeasonalChartFinalized(finalized);
                // Get #1 song/album if chart is finalized
                if (finalized) {
                    tf.setNumberOneSongName(chartService.getNumberOneSongName("seasonal", tf.getPeriodKey()));
                    tf.setNumberOneAlbumName(chartService.getNumberOneAlbumName("seasonal", tf.getPeriodKey()));
                }
            }
        }
        
        // For years, populate chart status
        if ("years".equals(periodType)) {
            Set<String> yearsWithFinalizedCharts = chartService.getFinalizedChartPeriodKeys("yearly");
            Set<String> yearsWithAnyCharts = chartService.getAllChartPeriodKeys("yearly");
            for (TimeframeCardDTO tf : timeframes) {
                boolean hasChart = yearsWithAnyCharts.contains(tf.getPeriodKey());
                boolean finalized = yearsWithFinalizedCharts.contains(tf.getPeriodKey());
                tf.setHasYearlyChart(hasChart);
                tf.setYearlyChartFinalized(finalized);
                // Get #1 song/album if chart is finalized
                if (finalized) {
                    tf.setNumberOneSongName(chartService.getNumberOneSongName("yearly", tf.getPeriodKey()));
                    tf.setNumberOneAlbumName(chartService.getNumberOneAlbumName("yearly", tf.getPeriodKey()));
                }
            }
        }
        
        // Pagination
        model.addAttribute("currentPage", page);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("perPage", perpage);
        model.addAttribute("startIndex", totalCount > 0 ? (page * perpage) + 1 : 0);
        model.addAttribute("endIndex", Math.min((page + 1) * perpage, totalCount));
        
        // Sorting
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        
        // Lookup data for filters
        model.addAttribute("genders", lookupRepository.getAllGenders());
        model.addAttribute("genres", lookupRepository.getAllGenres());
        model.addAttribute("ethnicities", lookupRepository.getAllEthnicities());
        model.addAttribute("languages", lookupRepository.getAllLanguages());
        model.addAttribute("countries", getAllCountries());
        
        // Current filter values (for maintaining state)
        model.addAttribute("winningGender", winningGender);
        model.addAttribute("winningGenderMode", winningGenderMode);
        model.addAttribute("winningGenre", winningGenre);
        model.addAttribute("winningGenreMode", winningGenreMode);
        model.addAttribute("winningEthnicity", winningEthnicity);
        model.addAttribute("winningEthnicityMode", winningEthnicityMode);
        model.addAttribute("winningLanguage", winningLanguage);
        model.addAttribute("winningLanguageMode", winningLanguageMode);
        model.addAttribute("winningCountry", winningCountry);
        model.addAttribute("winningCountryMode", winningCountryMode);
        model.addAttribute("artistCountMin", artistCountMin);
        model.addAttribute("artistCountMax", artistCountMax);
        model.addAttribute("albumCountMin", albumCountMin);
        model.addAttribute("albumCountMax", albumCountMax);
        model.addAttribute("songCountMin", songCountMin);
        model.addAttribute("songCountMax", songCountMax);
        model.addAttribute("playsMin", playsMin);
        model.addAttribute("playsMax", playsMax);
        model.addAttribute("timeMin", timeMin);
        model.addAttribute("timeMax", timeMax);
        model.addAttribute("maleArtistPctMin", maleArtistPctMin);
        model.addAttribute("maleArtistPctMax", maleArtistPctMax);
        model.addAttribute("maleAlbumPctMin", maleAlbumPctMin);
        model.addAttribute("maleAlbumPctMax", maleAlbumPctMax);
        model.addAttribute("maleSongPctMin", maleSongPctMin);
        model.addAttribute("maleSongPctMax", maleSongPctMax);
        model.addAttribute("malePlayPctMin", malePlayPctMin);
        model.addAttribute("malePlayPctMax", malePlayPctMax);
        model.addAttribute("maleTimePctMin", maleTimePctMin);
        model.addAttribute("maleTimePctMax", maleTimePctMax);
        model.addAttribute("perfectMale", perfectMale);
        
        // Query string for pagination links (without page param)
        String queryString = request.getQueryString();
        if (queryString != null) {
            queryString = queryString.replaceAll("&?page=\\d+", "").replaceAll("^&", "");
        }
        model.addAttribute("queryString", queryString != null && !queryString.isEmpty() ? queryString : "");
        
        return "timeframes/list";
    }
    
    /**
     * Get all unique countries from artists
     */
    private List<String> getAllCountries() {
        String sql = "SELECT DISTINCT country FROM Artist WHERE country IS NOT NULL ORDER BY country";
        return jdbcTemplate.queryForList(sql, String.class);
    }
    
    /**
     * Format period type for display (e.g., "years" -> "Years")
     */
    private String formatPeriodTypeDisplay(String periodType) {
        if (periodType == null || periodType.isEmpty()) {
            return "";
        }
        return periodType.substring(0, 1).toUpperCase() + periodType.substring(1);
    }
}
