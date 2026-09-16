package library.controller;

import library.service.BreakdownHistoryService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class BreakdownHistoryController {
    private final BreakdownHistoryService service;

    public BreakdownHistoryController(BreakdownHistoryService service) {
        this.service = service;
    }

    @GetMapping("/api/playground/breakdown-history")
    public BreakdownHistoryService.History history(
            @RequestParam(defaultValue = "gender") String breakdown,
            @RequestParam(defaultValue = "plays") String measure,
            @RequestParam(defaultValue = "cumulative") String mode,
            @RequestParam(defaultValue = "month") String interval,
            @RequestParam(required = false) Integer year) {
        try {
            return service.getHistory(breakdown, measure, mode, interval, year);
        } catch (IllegalArgumentException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exception.getMessage(), exception);
        }
    }
}
