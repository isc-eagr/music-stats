package library.controller;

import library.service.ItunesService;
import library.service.ItunesService.ItunesSong;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Controller for iTunes library comparison functionality.
 * Shows songs that exist in iTunes but not in the Music Stats database.
 * Uses the same iTunes Library.xml as the song detail page's "Fetch from iTunes" feature.
 */
@Controller
@RequestMapping("/itunes")
public class ItunesController {

    private final ItunesService itunesService;

    public ItunesController(ItunesService itunesService) {
        this.itunesService = itunesService;
    }

    /**
     * Shows the iTunes Only page - songs in iTunes but not in the database.
     * Uses the default Library.xml path configured in iTunesLibraryService.
     */
    @GetMapping("/unmatched")
    public String showUnmatchedItunes(Model model) {
        String libraryPath = itunesService.getDefaultLibraryPath();
        
        try {
            if (itunesService.libraryExists()) {
                List<ItunesSong> unmatchedSongs = itunesService.findUnmatchedItunesSongs();
                model.addAttribute("unmatchedSongs", unmatchedSongs);
                model.addAttribute("totalCount", unmatchedSongs.size());
                model.addAttribute("libraryPath", libraryPath);
            } else {
                model.addAttribute("unmatchedSongs", List.of());
                model.addAttribute("totalCount", 0);
                model.addAttribute("libraryPath", libraryPath);
                model.addAttribute("errorMessage", "iTunes Library.xml not found at: " + libraryPath);
            }
        } catch (Exception e) {
            model.addAttribute("unmatchedSongs", List.of());
            model.addAttribute("totalCount", 0);
            model.addAttribute("libraryPath", libraryPath);
            model.addAttribute("errorMessage", "Error parsing Library.xml: " + e.getMessage());
        }
        return "itunesOnly";
    }
}
