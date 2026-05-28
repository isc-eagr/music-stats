package library.controller;

import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.PcOverviewRowDTO;
import library.service.AppConfigService;
import library.service.PcService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping({"/misc/vatos-cuntdown", "/misc/pc"})
public class PcController {

    private final AppConfigService appConfigService;
    private final PcService pcService;

    public PcController(AppConfigService appConfigService, PcService pcService) {
        this.appConfigService = appConfigService;
        this.pcService = pcService;
    }

    @GetMapping
    public String pcList(
            @RequestParam(defaultValue = "song") String overviewTab,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(required = false) Integer size,
            @RequestParam(required = false) String sort,
            @RequestParam(required = false) String dir,
            @RequestParam(defaultValue = "false") boolean includeFeatured,
            Model model) {
        String normalizedOverviewTab = normalizeOverviewTab(overviewTab);
        int safePage = Math.max(1, page);
        int safeSize = normalizeSize(size);
        String normalizedSort = normalizeSort(normalizedOverviewTab, sort);
        String normalizedDir = normalizeDir(normalizedOverviewTab, dir);

        List<PcOverviewRowDTO> entries = List.of();
        List<ChartAlbumOverviewRowDTO> albumOverviewRows = List.of();
        List<ChartArtistOverviewRowDTO> artistOverviewRows = List.of();
        int activeTotalCount;

        if ("album".equals(normalizedOverviewTab)) {
            List<ChartAlbumOverviewRowDTO> allAlbumRows = sortAlbumRows(pcService.getAlbumOverviewRows(), normalizedSort, normalizedDir);
            activeTotalCount = allAlbumRows.size();
            albumOverviewRows = paginateRows(allAlbumRows, safePage, safeSize);
        } else if ("artist".equals(normalizedOverviewTab)) {
            List<ChartArtistOverviewRowDTO> allArtistRows = sortArtistRows(pcService.getArtistOverviewRows(includeFeatured), normalizedSort, normalizedDir);
            activeTotalCount = allArtistRows.size();
            artistOverviewRows = paginateRows(allArtistRows, safePage, safeSize);
        } else {
            List<PcOverviewRowDTO> allEntries = sortSongRows(pcService.getOverviewRows(), normalizedSort, normalizedDir);
            activeTotalCount = allEntries.size();
            entries = paginateRows(allEntries, safePage, safeSize);
        }

        model.addAttribute("entries", entries);
        model.addAttribute("albumOverviewRows", albumOverviewRows);
        model.addAttribute("artistOverviewRows", artistOverviewRows);
        model.addAttribute("overviewTab", normalizedOverviewTab);
        model.addAttribute("pageSize", safeSize);
        model.addAttribute("activeTotalCount", activeTotalCount);
        model.addAttribute("selectedSort", normalizedSort);
        model.addAttribute("selectedDir", normalizedDir);
        model.addAttribute("selectedIncludeFeatured", includeFeatured);
        model.addAttribute("currentSection", "pc");
        return "misc/pc";
    }

    @GetMapping("/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> pcData(
            @RequestParam(defaultValue = "song") String overviewTab,
            @RequestParam(defaultValue = "2") int page,
            @RequestParam(required = false) Integer size,
            @RequestParam(required = false) String sort,
            @RequestParam(required = false) String dir,
            @RequestParam(defaultValue = "false") boolean includeFeatured) {
        String normalizedOverviewTab = normalizeOverviewTab(overviewTab);
        int safePage = Math.max(1, page);
        int safeSize = normalizeSize(size);
        String normalizedSort = normalizeSort(normalizedOverviewTab, sort);
        String normalizedDir = normalizeDir(normalizedOverviewTab, dir);
        Map<String, Object> result = new java.util.LinkedHashMap<>();

        if ("album".equals(normalizedOverviewTab)) {
            List<ChartAlbumOverviewRowDTO> allAlbumRows = sortAlbumRows(pcService.getAlbumOverviewRows(), normalizedSort, normalizedDir);
            result.put("entries", paginateRows(allAlbumRows, safePage, safeSize));
            result.put("totalCount", allAlbumRows.size());
            result.put("hasMore", (long) safePage * safeSize < allAlbumRows.size());
        } else if ("artist".equals(normalizedOverviewTab)) {
            List<ChartArtistOverviewRowDTO> allArtistRows = sortArtistRows(pcService.getArtistOverviewRows(includeFeatured), normalizedSort, normalizedDir);
            result.put("entries", paginateRows(allArtistRows, safePage, safeSize));
            result.put("totalCount", allArtistRows.size());
            result.put("hasMore", (long) safePage * safeSize < allArtistRows.size());
        } else {
            List<PcOverviewRowDTO> allEntries = sortSongRows(pcService.getOverviewRows(), normalizedSort, normalizedDir);
            int totalCount = allEntries.size();
            result.put("entries", paginateRows(allEntries, safePage, safeSize));
            result.put("totalCount", totalCount);
            result.put("hasMore", (long) safePage * safeSize < totalCount);
        }

        result.put("nextPage", safePage + 1);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/merge")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> mergeEntries(
            @RequestParam String sourceArtist,
            @RequestParam String sourceSong,
            @RequestParam String targetArtist,
            @RequestParam String targetSong) {
        int updated = pcService.mergeEntries(sourceArtist, sourceSong, targetArtist, targetSong);
        return ResponseEntity.ok(Map.of("ok", true, "updated", updated));
    }

    /** Search songs by title/artist for the match modal. */
    @GetMapping("/songs/search")
    @ResponseBody
    public List<Map<String, Object>> searchSongs(
            @RequestParam(defaultValue = "") String q) {
        return pcService.searchSongs(q, 30);
    }

    /** Auto-match unlinked PC rows to songs in the library by exact name. */
    @PostMapping("/auto-link")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> autoLink() {
        int linked = pcService.autoLinkExactMatches();
        return ResponseEntity.ok(Map.of("ok", true, "linked", linked));
    }

    @PostMapping("/normalize-case")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> normalizeCase() {
        int normalized = pcService.normalizeCaseDifferences();
        return ResponseEntity.ok(Map.of("ok", true, "normalized", normalized));
    }

    /** Link a raw PC artist/title group to a library song. */
    @PostMapping("/match-group")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> matchRawGroup(
            @RequestParam String rawArtist,
            @RequestParam String rawSong,
            @RequestParam Integer songId) {
        return ResponseEntity.ok(pcService.matchRawGroup(rawArtist, rawSong, songId));
    }

    // ---- PC Recaps page ----

    @GetMapping("/recaps")
    public String recaps(@RequestParam(required = false) String date, Model model) {
        List<String> availableDates = pcService.getAvailableChartDates();
        model.addAttribute("availableDates", availableDates);

        String effectiveDate = date;
        if ((effectiveDate == null || effectiveDate.isBlank()) && !availableDates.isEmpty()) {
            effectiveDate = availableDates.get(availableDates.size() - 1);
        } else if (effectiveDate != null && !effectiveDate.isBlank()) {
            effectiveDate = pcService.findClosestChartDate(effectiveDate);
        }
        if (effectiveDate != null) {
            List<Map<String, Object>> countdown = pcService.getCountdownForDate(effectiveDate);
            model.addAttribute("countdown", countdown);
            model.addAttribute("fallOffs", pcService.getFallOffsForDate(effectiveDate, countdown));
            model.addAttribute("selectedDate", effectiveDate);
        }
        model.addAttribute("currentSection", "pc-recaps");
        return "misc/pc-recaps";
    }

    @GetMapping("/recaps/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> recapsData(@RequestParam String date) {
        String effectiveDate = pcService.findClosestChartDate(date);
        if (effectiveDate == null) {
            return ResponseEntity.ok(Map.of("entries", List.of()));
        }
        List<Map<String, Object>> countdown = pcService.getCountdownForDate(effectiveDate);
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put("date", effectiveDate);
        result.put("entries", countdown);
        result.put("fallOffs", pcService.getFallOffsForDate(effectiveDate, countdown));
        result.put("prevDate", pcService.getPrevChartDate(effectiveDate));
        result.put("nextDate", pcService.getNextChartDate(effectiveDate));
        return ResponseEntity.ok(result);
    }

    @GetMapping("/chart-run")
    @ResponseBody
    public List<Map<String, Object>> getChartRun(
            @RequestParam(required = false) Integer songId,
            @RequestParam(required = false) String artist,
            @RequestParam(required = false) String song) {
        if (songId != null) {
            return pcService.getChartRunBySongId(songId);
        }
        if (artist != null && song != null) {
            return pcService.getChartRunByNames(artist, song);
        }
        return List.of();
    }

    private String normalizeOverviewTab(String overviewTab) {
        if ("album".equalsIgnoreCase(overviewTab)) {
            return "album";
        }
        if ("artist".equalsIgnoreCase(overviewTab)) {
            return "artist";
        }
        return "song";
    }

    private String normalizeSort(String overviewTab, String sort) {
        if ("album".equals(overviewTab)) {
            return switch (sort == null ? "" : sort) {
                case "artist", "album", "songs", "days", "peak", "numberOnes", "atNumberOne", "firstDebut", "lastAppearance" -> sort;
                default -> "days";
            };
        }
        if ("artist".equals(overviewTab)) {
            return switch (sort == null ? "" : sort) {
                case "artist", "songs", "days", "peak", "numberOnes", "atNumberOne", "firstDebut", "lastAppearance" -> sort;
                default -> "days";
            };
        }
        return switch (sort == null ? "" : sort) {
            case "days", "peak", "atPeak", "debutDate", "debutPosition", "peakDate", "lastDate", "song", "artist" -> sort;
            default -> "debutDate";
        };
    }

    private String normalizeDir(String overviewTab, String dir) {
        if (dir == null || dir.isBlank()) {
            if ("song".equals(overviewTab)) {
                return "asc";
            }
            return "desc";
        }
        return "asc".equalsIgnoreCase(dir) ? "asc" : "desc";
    }

    private List<PcOverviewRowDTO> sortSongRows(List<PcOverviewRowDTO> rows, String sort, String dir) {
        List<PcOverviewRowDTO> sortedRows = new ArrayList<>(rows);
        sortedRows.sort(buildSongComparator(sort, dir));
        return sortedRows;
    }

    private List<ChartAlbumOverviewRowDTO> sortAlbumRows(List<ChartAlbumOverviewRowDTO> rows, String sort, String dir) {
        List<ChartAlbumOverviewRowDTO> sortedRows = new ArrayList<>(rows);
        sortedRows.sort(buildAlbumComparator(sort, dir));
        return sortedRows;
    }

    private List<ChartArtistOverviewRowDTO> sortArtistRows(List<ChartArtistOverviewRowDTO> rows, String sort, String dir) {
        List<ChartArtistOverviewRowDTO> sortedRows = new ArrayList<>(rows);
        sortedRows.sort(buildArtistComparator(sort, dir));
        return sortedRows;
    }

    private Comparator<PcOverviewRowDTO> buildSongComparator(String sort, String dir) {
        Comparator<PcOverviewRowDTO> comparator = switch (sort) {
            case "days" -> Comparator.comparingInt(PcOverviewRowDTO::getDaysOnCountdown);
            case "peak" -> Comparator.comparingInt(PcOverviewRowDTO::getPeakPosition);
            case "atPeak" -> Comparator.comparingInt(PcOverviewRowDTO::getDaysAtPeak);
            case "debutPosition" -> Comparator.comparing(PcOverviewRowDTO::getDebutPosition, Comparator.nullsLast(Integer::compareTo));
            case "peakDate" -> Comparator.comparing(PcOverviewRowDTO::getPeakWeek, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "lastDate" -> Comparator.comparing(PcOverviewRowDTO::getLastWeek, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "song" -> Comparator.comparing(PcOverviewRowDTO::getSongTitle, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "artist" -> Comparator.comparing(PcOverviewRowDTO::getArtistName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            default -> Comparator.comparing(PcOverviewRowDTO::getFirstWeek, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
        };
        comparator = comparator
            .thenComparing(PcOverviewRowDTO::getFirstWeek, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER))
            .thenComparing(PcOverviewRowDTO::getArtistName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER))
            .thenComparing(PcOverviewRowDTO::getSongTitle, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
        return "asc".equals(dir) ? comparator : comparator.reversed();
    }

    private Comparator<ChartAlbumOverviewRowDTO> buildAlbumComparator(String sort, String dir) {
        Comparator<ChartAlbumOverviewRowDTO> comparator = switch (sort) {
            case "artist" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getArtistName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "album" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getAlbumName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "songs" -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getChartedSongsCount);
            case "peak" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getHighestPeak, Comparator.nullsLast(Integer::compareTo));
            case "numberOnes" -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getNumberOneSongsCount);
            case "atNumberOne" -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getTotalSpanAtNumberOne);
            case "firstDebut" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getFirstDebutDate, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "lastAppearance" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getLastAppearanceDate, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            default -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getTotalChartSpan);
        };
        comparator = comparator
            .thenComparing(ChartAlbumOverviewRowDTO::getArtistName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER))
            .thenComparing(ChartAlbumOverviewRowDTO::getAlbumName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
        return "asc".equals(dir) ? comparator : comparator.reversed();
    }

    private Comparator<ChartArtistOverviewRowDTO> buildArtistComparator(String sort, String dir) {
        Comparator<ChartArtistOverviewRowDTO> comparator = switch (sort) {
            case "artist" -> Comparator.comparing(ChartArtistOverviewRowDTO::getArtistName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "songs" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getChartedSongsCount);
            case "peak" -> Comparator.comparing(ChartArtistOverviewRowDTO::getHighestPeak, Comparator.nullsLast(Integer::compareTo));
            case "numberOnes" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getNumberOneSongsCount);
            case "atNumberOne" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getTotalSpanAtNumberOne);
            case "firstDebut" -> Comparator.comparing(ChartArtistOverviewRowDTO::getFirstDebutDate, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "lastAppearance" -> Comparator.comparing(ChartArtistOverviewRowDTO::getLastAppearanceDate, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            default -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getTotalChartSpan);
        };
        comparator = comparator.thenComparing(ChartArtistOverviewRowDTO::getArtistName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
        return "asc".equals(dir) ? comparator : comparator.reversed();
    }

    private int normalizeSize(Integer size) {
        return appConfigService.normalizePageSize(size, appConfigService.getPcOverviewPageSize(), 25, 200);
    }

    private <T> List<T> paginateRows(List<T> rows, int page, int size) {
        int fromIndex = Math.max(0, (page - 1) * size);
        if (fromIndex >= rows.size()) {
            return List.of();
        }
        int toIndex = Math.min(rows.size(), fromIndex + size);
        return rows.subList(fromIndex, toIndex);
    }
}
