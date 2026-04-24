package library.controller;

import library.dto.TrlChartEntryGroupDTO;
import library.entity.TrlDebut;
import library.service.TrlService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/misc/trl")
public class TrlController {

    private final TrlService trlService;

    public TrlController(TrlService trlService) {
        this.trlService = trlService;
    }

    @GetMapping
    public String trlList(Model model) {
        List<TrlDebut> debuts = trlService.getAllDebuts();
        model.addAttribute("debuts", debuts);
        model.addAttribute("summary", trlService.getSummary());
        model.addAttribute("currentSection", "trl");
        return "misc/trl";
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
        result.put("prevDate", trlService.getPrevChartDate(effectiveDate));
        result.put("nextDate", trlService.getNextChartDate(effectiveDate));
        return ResponseEntity.ok(result);
    }
}
