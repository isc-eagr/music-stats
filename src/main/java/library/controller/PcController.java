package library.controller;

import library.service.ChartOverviewService;
import library.service.PcService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Controller
@RequestMapping({"/misc/vatos-cuntdown", "/misc/pc"})
public class PcController {

    private final PcService pcService;

    public PcController(PcService pcService) {
        this.pcService = pcService;
    }

    @PostMapping("/merge")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> mergeEntries(
            @RequestParam String sourceArtist,
            @RequestParam String sourceSong,
            @RequestParam String targetArtist,
            @RequestParam String targetSong) {
        return ResponseEntity.ok(pcService.mergeEntries(sourceArtist, sourceSong, targetArtist, targetSong));
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
        ChartRecapPage.populate(model, ChartOverviewService.Chart.CUNTDOWN, "/misc/vatos-cuntdown", availableDates, effectiveDate);
        model.addAttribute("currentSection", "pc-recaps");
        return ChartRecapPage.VIEW;
    }

    @GetMapping("/recaps/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> recapsData(@RequestParam String date) {
        String effectiveDate = pcService.findClosestChartDate(date);
        if (effectiveDate == null) {
            return ResponseEntity.ok(Map.of("entries", List.of()));
        }
        List<Map<String, Object>> countdown = pcService.getCountdownForDate(effectiveDate);
        pcService.applyBreakdownAttributes(countdown);
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
}
