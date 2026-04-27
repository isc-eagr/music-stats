package library.controller;

import library.service.PcService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/misc/pc")
public class PcController {

    private final PcService pcService;

    public PcController(PcService pcService) {
        this.pcService = pcService;
    }

    @GetMapping
    public String pcList(Model model) {
        model.addAttribute("entries", pcService.getOverviewRows());
        model.addAttribute("summary", pcService.getSummary());
        model.addAttribute("currentSection", "pc");
        return "misc/pc";
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

        String effectiveDate = null;
        if (date != null && !date.isBlank()) {
            effectiveDate = pcService.findClosestChartDate(date);
        }
        if (effectiveDate != null) {
            model.addAttribute("countdown", pcService.getCountdownForDate(effectiveDate));
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
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put("date", effectiveDate);
        result.put("entries", pcService.getCountdownForDate(effectiveDate));
        result.put("prevDate", pcService.getPrevChartDate(effectiveDate));
        result.put("nextDate", pcService.getNextChartDate(effectiveDate));
        return ResponseEntity.ok(result);
    }
}
