package library.controller;

import library.service.ChartOverviewService;
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

    @PostMapping("/import")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> importCharts() {
        try {
            return ResponseEntity.ok(billboardHot100Service.importIncrementalCharts());
        } catch (Exception e) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("ok", false);
            result.put("error", e.getMessage() != null ? e.getMessage() : "Billboard import failed");
            return ResponseEntity.internalServerError().body(result);
        }
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

        ChartRecapPage.populate(model, ChartOverviewService.Chart.HOT100, "/misc/billboard-hot-100", availableDates, effectiveDate);
        model.addAttribute("currentSection", "billboard-hot100-recaps");
        return ChartRecapPage.VIEW;
    }

    @GetMapping("/recaps/data")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> recapsData(@RequestParam String date) {
        String effectiveDate = billboardHot100Service.findClosestChartDate(date);
        Map<String, Object> result = new LinkedHashMap<>();
        List<Map<String, Object>> countdown = effectiveDate != null ? billboardHot100Service.getCountdownForDate(effectiveDate) : List.of();
        billboardHot100Service.applyBreakdownAttributes(countdown);
        result.put("date", effectiveDate);
        result.put("prevDate", effectiveDate != null ? billboardHot100Service.getPrevChartDate(effectiveDate) : null);
        result.put("nextDate", effectiveDate != null ? billboardHot100Service.getNextChartDate(effectiveDate) : null);
        result.put("entries", countdown);
        result.put("fallOffs", effectiveDate != null ? billboardHot100Service.getFallOffsForDate(effectiveDate, countdown) : List.of());
        return ResponseEntity.ok(result);
    }

    @GetMapping("/chart-run")
    @ResponseBody
    public List<Map<String, Object>> getChartRun(
            @RequestParam(required = false) Integer songId,
            @RequestParam(required = false) String artist,
            @RequestParam(required = false) String song) {
        if (songId != null) {
            return billboardHot100Service.getChartRunBySongId(songId);
        }
        if (artist != null && song != null) {
            return billboardHot100Service.getChartRunByNames(artist, song);
        }
        return List.of();
    }
}
