package library.controller;

import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.TrlChartEntryGroupDTO;
import library.entity.TrlDebut;
import library.service.TrlService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/misc/trl")
public class TrlController {

    private static final int OVERVIEW_PAGE_SIZE = 100;

    private final TrlService trlService;

    public TrlController(TrlService trlService) {
        this.trlService = trlService;
    }

    @GetMapping
    public String trlList(
            @RequestParam(defaultValue = "song") String overviewTab,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "100") int size,
            @RequestParam(required = false) String sort,
            @RequestParam(required = false) String dir,
            Model model) {
        String normalizedOverviewTab = normalizeOverviewTab(overviewTab);
        int safePage = Math.max(1, page);
        int safeSize = normalizeSize(size);
        String normalizedSort = normalizeSort(normalizedOverviewTab, sort);
        String normalizedDir = normalizeDir(normalizedOverviewTab, dir);

        List<TrlDebut> debuts = List.of();
        List<ChartAlbumOverviewRowDTO> albumOverviewRows = List.of();
        List<ChartArtistOverviewRowDTO> artistOverviewRows = List.of();
        int activeTotalCount;

        if ("album".equals(normalizedOverviewTab)) {
            List<ChartAlbumOverviewRowDTO> allAlbumRows = sortAlbumRows(trlService.getAlbumOverviewRows(), normalizedSort, normalizedDir);
            activeTotalCount = allAlbumRows.size();
            albumOverviewRows = paginateRows(allAlbumRows, safePage, safeSize);
        } else if ("artist".equals(normalizedOverviewTab)) {
            List<ChartArtistOverviewRowDTO> allArtistRows = sortArtistRows(trlService.getArtistOverviewRows(), normalizedSort, normalizedDir);
            activeTotalCount = allArtistRows.size();
            artistOverviewRows = paginateRows(allArtistRows, safePage, safeSize);
        } else {
            List<TrlDebut> allDebuts = sortSongRows(trlService.getAllDebuts(), normalizedSort, normalizedDir);
            activeTotalCount = allDebuts.size();
            debuts = paginateRows(allDebuts, safePage, safeSize);
        }

        model.addAttribute("debuts", debuts);
        model.addAttribute("albumOverviewRows", albumOverviewRows);
        model.addAttribute("artistOverviewRows", artistOverviewRows);
        model.addAttribute("overviewTab", normalizedOverviewTab);
        model.addAttribute("pageSize", safeSize);
        model.addAttribute("activeTotalCount", activeTotalCount);
        model.addAttribute("selectedSort", normalizedSort);
        model.addAttribute("selectedDir", normalizedDir);
        model.addAttribute("summary", trlService.getSummary());
        model.addAttribute("currentSection", "trl");
        return "misc/trl";
    }

    @GetMapping("/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> trlData(
            @RequestParam(defaultValue = "song") String overviewTab,
            @RequestParam(defaultValue = "2") int page,
            @RequestParam(defaultValue = "100") int size,
            @RequestParam(required = false) String sort,
            @RequestParam(required = false) String dir) {
        String normalizedOverviewTab = normalizeOverviewTab(overviewTab);
        int safePage = Math.max(1, page);
        int safeSize = normalizeSize(size);
        String normalizedSort = normalizeSort(normalizedOverviewTab, sort);
        String normalizedDir = normalizeDir(normalizedOverviewTab, dir);
        Map<String, Object> result = new java.util.LinkedHashMap<>();

        if ("album".equals(normalizedOverviewTab)) {
            List<ChartAlbumOverviewRowDTO> allAlbumRows = sortAlbumRows(trlService.getAlbumOverviewRows(), normalizedSort, normalizedDir);
            result.put("entries", paginateRows(allAlbumRows, safePage, safeSize));
            result.put("totalCount", allAlbumRows.size());
            result.put("hasMore", (long) safePage * safeSize < allAlbumRows.size());
        } else if ("artist".equals(normalizedOverviewTab)) {
            List<ChartArtistOverviewRowDTO> allArtistRows = sortArtistRows(trlService.getArtistOverviewRows(), normalizedSort, normalizedDir);
            result.put("entries", paginateRows(allArtistRows, safePage, safeSize));
            result.put("totalCount", allArtistRows.size());
            result.put("hasMore", (long) safePage * safeSize < allArtistRows.size());
        } else {
            List<TrlDebut> allDebuts = sortSongRows(trlService.getAllDebuts(), normalizedSort, normalizedDir);
            result.put("entries", paginateRows(allDebuts, safePage, safeSize));
            result.put("totalCount", allDebuts.size());
            result.put("hasMore", (long) safePage * safeSize < allDebuts.size());
        }

        result.put("nextPage", safePage + 1);
        return ResponseEntity.ok(result);
    }

    /** Search songs by title/artist for the match modal. */
    @GetMapping("/songs/search")
    @ResponseBody
    public List<Map<String, Object>> searchSongs(
            @RequestParam(defaultValue = "") String q) {
        return trlService.searchSongs(q);
    }

    /** Link a TRL entry to a song in the library. */
    @PostMapping("/{id}/match")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> matchSong(
            @PathVariable Integer id,
            @RequestParam Integer songId) {
        return ResponseEntity.ok(trlService.matchSong(id, songId));
    }

    /** Remove the song link from a TRL entry. */
    @PostMapping("/{id}/unmatch")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> unmatchSong(@PathVariable Integer id) {
        trlService.unmatchSong(id);
        return ResponseEntity.ok(Map.of("ok", true));
    }

    /** Retroactively normalize all linked debuts + their chart entries to library names. */
    @PostMapping("/normalize-linked")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> normalizeLinked() {
        int updated = trlService.retroactiveNormalizeLinked();
        return ResponseEntity.ok(Map.of("ok", true, "updated", updated));
    }

    // ---- Chart Entries merge page ----

    @GetMapping("/chart-entries")
    public String chartEntries(Model model) {
        model.addAttribute("entries", trlService.getDistinctChartEntries());
        model.addAttribute("currentSection", "trl");
        return "misc/trl-chart-entries";
    }

    @PostMapping("/chart-entries/merge")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> mergeChartEntries(
            @RequestParam String sourceArtist,
            @RequestParam String sourceSong,
            @RequestParam String targetArtist,
            @RequestParam String targetSong) {
        int updated = trlService.mergeChartEntries(sourceArtist, sourceSong, targetArtist, targetSong);
        return ResponseEntity.ok(Map.of("ok", true, "updated", updated));
    }

    // ---- Chart link on TRL debut ----

    /** Search chart entries for the chart-link modal. */
    @GetMapping("/chart-entries/search")
    @ResponseBody
    public List<TrlChartEntryGroupDTO> searchChartEntries(
            @RequestParam(defaultValue = "") String q) {
        return trlService.searchChartEntries(q);
    }

    /** Link a TRL debut to a specific chart entry combo. */
    @PostMapping("/{id}/link-chart")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> linkChart(
            @PathVariable Integer id,
            @RequestParam String chartArtist,
            @RequestParam String chartSong) {
        trlService.linkChart(id, chartArtist, chartSong);
        return ResponseEntity.ok(Map.of("ok", true));
    }

    /** Remove the chart link from a TRL debut. */
    @PostMapping("/{id}/unlink-chart")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> unlinkChart(@PathVariable Integer id) {
        trlService.unlinkChart(id);
        return ResponseEntity.ok(Map.of("ok", true));
    }

    /** Auto-link all debuts that have an exact match in chart entries. */
    @PostMapping("/auto-link")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> autoLink() {
        int linked = trlService.autoLinkExactMatches();
        return ResponseEntity.ok(Map.of("ok", true, "linked", linked));
    }

    // ---- TRL Recaps page ----

    @GetMapping("/recaps")
    public String recaps(@RequestParam(required = false) String date, Model model) {
        List<String> availableDates = trlService.getAvailableChartDates();
        model.addAttribute("availableDates", availableDates);

        String effectiveDate = null;
        if (date != null && !date.isBlank()) {
            effectiveDate = trlService.findClosestChartDate(date);
        }
        if (effectiveDate != null) {
            model.addAttribute("countdown", trlService.getCountdownForDate(effectiveDate));
            model.addAttribute("fallOffs", trlService.getFallOffsForDate(effectiveDate));
            model.addAttribute("selectedDate", effectiveDate);
        }
        model.addAttribute("currentSection", "trl-recaps");
        return "misc/trl-recaps";
    }

    @GetMapping("/recaps/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> recapsData(@RequestParam String date) {
        String effectiveDate = trlService.findClosestChartDate(date);
        if (effectiveDate == null) {
            return ResponseEntity.ok(Map.of("entries", List.of()));
        }
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put("date", effectiveDate);
        result.put("entries", trlService.getCountdownForDate(effectiveDate));
        result.put("fallOffs", trlService.getFallOffsForDate(effectiveDate));
        result.put("prevDate", trlService.getPrevChartDate(effectiveDate));
        result.put("nextDate", trlService.getNextChartDate(effectiveDate));
        return ResponseEntity.ok(result);
    }

    @GetMapping("/{id}/chart-run")
    @ResponseBody
    public List<Map<String, Object>> getChartRun(@PathVariable int id) {
        return trlService.getChartRunForDebut(id);
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
            case "days", "actualDays", "peak", "atPeak", "debutDate", "debutPosition", "peakDate", "lastDate", "song", "artist" -> sort;
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

    private List<TrlDebut> sortSongRows(List<TrlDebut> rows, String sort, String dir) {
        List<TrlDebut> sortedRows = new ArrayList<>(rows);
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

    private Comparator<TrlDebut> buildSongComparator(String sort, String dir) {
        Comparator<TrlDebut> comparator = switch (sort) {
            case "days" -> Comparator.comparing(TrlDebut::getDaysOnCountdown, Comparator.nullsLast(Integer::compareTo));
            case "actualDays" -> Comparator.comparing(TrlDebut::getActualDays, Comparator.nullsLast(Integer::compareTo));
            case "peak" -> Comparator.comparing(TrlDebut::getPeakPosition, Comparator.nullsLast(Integer::compareTo));
            case "atPeak" -> Comparator.comparing(TrlDebut::getDaysAtPeak, Comparator.nullsLast(Integer::compareTo));
            case "debutPosition" -> Comparator.comparing(TrlDebut::getDebutPosition, Comparator.nullsLast(Integer::compareTo));
            case "peakDate" -> Comparator.comparing(TrlDebut::getPeakDate, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "lastDate" -> Comparator.comparing(TrlDebut::getLastAppearanceDate, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "song" -> Comparator.comparing(TrlDebut::getSongTitle, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "artist" -> Comparator.comparing(TrlDebut::getArtistName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            default -> Comparator.comparing(TrlDebut::getDebutDate, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
        };
        comparator = comparator
            .thenComparing(TrlDebut::getDebutDate, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER))
            .thenComparing(TrlDebut::getArtistName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER))
            .thenComparing(TrlDebut::getSongTitle, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
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

    private int normalizeSize(int size) {
        return Math.min(Math.max(size, 25), 200);
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
