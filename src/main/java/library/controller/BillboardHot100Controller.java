package library.controller;

import library.service.BillboardHot100Service;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/misc/billboard-hot-100")
public class BillboardHot100Controller {

    private final BillboardHot100Service billboardHot100Service;

    public BillboardHot100Controller(BillboardHot100Service billboardHot100Service) {
        this.billboardHot100Service = billboardHot100Service;
    }

    @GetMapping
    public String overview(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "250") int size,
            @RequestParam(defaultValue = "firstWeek") String sort,
            @RequestParam(defaultValue = "desc") String dir,
            @RequestParam(required = false) String q,
            Model model) {
        int safeSize = normalizeSize(size);
        String safeSort = normalizeSort(sort);
        String safeDir = normalizeDir(dir);
        Map<String, Object> summary = billboardHot100Service.getSummary();
        int resultTotal = billboardHot100Service.countOverviewRows(q);
        int totalPages = Math.max(1, (int) Math.ceil(resultTotal / (double) safeSize));
        int safePage = Math.max(1, Math.min(page, totalPages));

        model.addAttribute("entries", billboardHot100Service.getOverviewRows(safePage, safeSize, safeSort, safeDir, q));
        model.addAttribute("summary", summary);
        model.addAttribute("nameIssueReport", billboardHot100Service.getNameIssueReport(20));
        model.addAttribute("page", safePage);
        model.addAttribute("size", safeSize);
        model.addAttribute("sort", safeSort);
        model.addAttribute("dir", safeDir);
        model.addAttribute("q", q);
        model.addAttribute("resultTotal", resultTotal);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("hasPrev", safePage > 1);
        model.addAttribute("hasNext", safePage < totalPages);
        model.addAttribute("currentSection", "billboard-hot100");
        return "misc/billboard-hot100";
    }

    @PostMapping("/import")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> importCharts() {
        return ResponseEntity.ok(billboardHot100Service.importIncrementalCharts());
    }

    @PostMapping("/auto-link")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> autoLink() {
        return ResponseEntity.ok(billboardHot100Service.autoLinkExactMatches());
    }

    @PostMapping("/normalize-case")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> normalizeCase() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ok", true);
        result.put("updatedRows", billboardHot100Service.normalizeCaseDifferences());
        return ResponseEntity.ok(result);
    }

    @PostMapping("/match")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> matchRawGroup(
            @RequestParam String rawArtist,
            @RequestParam String rawSong,
            @RequestParam Integer songId) {
        return ResponseEntity.ok(billboardHot100Service.matchRawGroup(rawArtist, rawSong, songId));
    }

    @GetMapping("/recaps")
    public String recaps(@RequestParam(required = false) String date, Model model) {
        List<String> availableDates = billboardHot100Service.getAvailableChartDates();

        String effectiveDate = date;
        if ((effectiveDate == null || effectiveDate.isBlank()) && !availableDates.isEmpty()) {
            effectiveDate = availableDates.get(availableDates.size() - 1);
        } else if (effectiveDate != null && !effectiveDate.isBlank()) {
            effectiveDate = billboardHot100Service.findClosestChartDate(effectiveDate);
        }

        if (effectiveDate != null) {
            model.addAttribute("countdown", billboardHot100Service.getCountdownForDate(effectiveDate));
        }

        model.addAttribute("availableDates", availableDates);
        model.addAttribute("selectedDate", effectiveDate);
        model.addAttribute("currentSection", "billboard-hot100-recaps");
        return "misc/billboard-hot100-recaps";
    }

    @GetMapping("/recaps/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> recapsData(@RequestParam String date) {
        String effectiveDate = billboardHot100Service.findClosestChartDate(date);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("date", effectiveDate);
        result.put("prevDate", effectiveDate != null ? billboardHot100Service.getPrevChartDate(effectiveDate) : null);
        result.put("nextDate", effectiveDate != null ? billboardHot100Service.getNextChartDate(effectiveDate) : null);
        result.put("entries", effectiveDate != null ? billboardHot100Service.getCountdownForDate(effectiveDate) : List.of());
        return ResponseEntity.ok(result);
    }

    private int normalizeSize(int size) {
        if (size <= 100) return 100;
        if (size <= 250) return 250;
        return 500;
    }

    private String normalizeSort(String sort) {
        return switch (sort) {
            case "weeks", "peak", "weeksAtPeak", "peakWeek", "firstWeek", "lastWeek", "song", "artist" -> sort;
            default -> "firstWeek";
        };
    }

    private String normalizeDir(String dir) {
        return "asc".equalsIgnoreCase(dir) ? "asc" : "desc";
    }
}