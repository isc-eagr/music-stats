package library.controller;

import library.dto.TopPlayedTimelineEntryDTO;
import library.service.TopPlayedTimelineService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

/**
 * Controller for the Top Played Timeline feature.
 * Shows the history of #1 most-played artists, songs, and genres over time.
 */
@Controller
@RequestMapping("/top-played-timeline")
public class TopPlayedTimelineController {

    private final TopPlayedTimelineService timelineService;

    public TopPlayedTimelineController(TopPlayedTimelineService timelineService) {
        this.timelineService = timelineService;
    }

    @GetMapping("/artists")
    public String artistTimeline(Model model) {
        List<TopPlayedTimelineEntryDTO> timeline = timelineService.getArtistTimeline();
        model.addAttribute("timeline", timeline);
        model.addAttribute("timelineType", "artist");
        model.addAttribute("timelineTitle", "Artist");
        model.addAttribute("currentSection", "top-played-timeline");
        return "top-played-timeline";
    }

    @GetMapping("/songs")
    public String songTimeline(Model model) {
        List<TopPlayedTimelineEntryDTO> timeline = timelineService.getSongTimeline();
        model.addAttribute("timeline", timeline);
        model.addAttribute("timelineType", "song");
        model.addAttribute("timelineTitle", "Song");
        model.addAttribute("currentSection", "top-played-timeline");
        return "top-played-timeline";
    }

    @GetMapping("/genres")
    public String genreTimeline(Model model) {
        List<TopPlayedTimelineEntryDTO> timeline = timelineService.getGenreTimeline();
        model.addAttribute("timeline", timeline);
        model.addAttribute("timelineType", "genre");
        model.addAttribute("timelineTitle", "Genre");
        model.addAttribute("currentSection", "top-played-timeline");
        return "top-played-timeline";
    }
}
