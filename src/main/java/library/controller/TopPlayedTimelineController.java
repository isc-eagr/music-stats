package library.controller;

import library.dto.TopPlayedHistoryCardDTO;
import library.dto.TopPlayedSnapshotDTO;
import library.dto.TopPlayedSnapshotItemDTO;
import library.service.TopPlayedTimelineService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Controller for the Top Played Reigns feature.
 * Shows the history of top-3 snapshot configurations for artists, songs, and genres.
 */
@Controller
@RequestMapping("/reign")
public class TopPlayedTimelineController {

    private final TopPlayedTimelineService timelineService;

    public TopPlayedTimelineController(TopPlayedTimelineService timelineService) {
        this.timelineService = timelineService;
    }

    @GetMapping("/artists")
    public String artistTimeline(Model model) {
        List<TopPlayedSnapshotDTO> timeline = timelineService.getArtistTimeline();
        populateCommonModel(model, timeline, "artist", "Artist");
        return "top-played-timeline";
    }

    @GetMapping("/songs")
    public String songTimeline(Model model) {
        List<TopPlayedSnapshotDTO> timeline = timelineService.getSongTimeline();
        populateCommonModel(model, timeline, "song", "Song");
        return "top-played-timeline";
    }

    @GetMapping("/genres")
    public String genreTimeline(Model model) {
        List<TopPlayedSnapshotDTO> timeline = timelineService.getGenreTimeline();
        populateCommonModel(model, timeline, "genre", "Genre");
        return "top-played-timeline";
    }

    private void populateCommonModel(Model model, List<TopPlayedSnapshotDTO> timeline, String timelineType, String timelineTitle) {
        model.addAttribute("timeline", timeline);
        model.addAttribute("timelineType", timelineType);
        model.addAttribute("timelineTitle", timelineTitle);
        model.addAttribute("top3History", buildTop3History(timeline));
        model.addAttribute("currentSection", "reign");
    }

    private List<TopPlayedHistoryCardDTO> buildTop3History(List<TopPlayedSnapshotDTO> timeline) {
        Map<Integer, TopPlayedHistoryCardDTO> cardsByItem = new LinkedHashMap<>();

        for (TopPlayedSnapshotDTO snapshot : timeline) {
            if (snapshot.getItems() == null) {
                continue;
            }

            for (TopPlayedSnapshotItemDTO item : snapshot.getItems()) {
                if (item == null || item.getItemId() == null) {
                    continue;
                }

                TopPlayedHistoryCardDTO card = cardsByItem.get(item.getItemId());
                if (card == null) {
                    card = new TopPlayedHistoryCardDTO();
                    card.setItemId(item.getItemId());
                    card.setItemName(item.getItemName());
                    card.setSecondaryName(item.getSecondaryName());
                    card.setGenderId(item.getGenderId());
                    card.setGenderName(item.getGenderName());
                    card.setFirstAppearanceRank(snapshot.getRank());
                    cardsByItem.put(item.getItemId(), card);
                }

                // These values are cumulative in the timeline; keep latest seen snapshot values.
                card.setDaysAtPos1(item.getDaysAtPos1());
                card.setDaysAtPos2(item.getDaysAtPos2());
                card.setDaysAtPos3(item.getDaysAtPos3());
            }
        }

        return new ArrayList<>(cardsByItem.values());
    }
}
