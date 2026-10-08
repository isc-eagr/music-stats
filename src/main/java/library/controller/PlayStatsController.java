package library.controller;

import library.dto.TodayMalePlayStatsDTO;
import library.service.PlayStatsService;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/plays/api")
public class PlayStatsController {

    private final PlayStatsService playStatsService;

    public PlayStatsController(PlayStatsService playStatsService) {
        this.playStatsService = playStatsService;
    }

    @CrossOrigin
    @GetMapping(value = "/today/male-percentage", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TodayMalePlayStatsDTO> getTodayMalePlayStats() {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(playStatsService.getTodayMalePlayStats());
    }
}
