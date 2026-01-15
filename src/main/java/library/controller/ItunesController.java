package library.controller;

import library.dto.ItunesChangesResultDTO;
import library.service.ItunesChangesService;
import library.service.ItunesService;
import library.service.ItunesService.ItunesSong;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.time.format.DateTimeFormatter;
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
    private final ItunesChangesService itunesChangesService;
    
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd MMM yyyy HH:mm");

    public ItunesController(ItunesService itunesService, ItunesChangesService itunesChangesService) {
        this.itunesService = itunesService;
        this.itunesChangesService = itunesChangesService;
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

    /**
     * Shows the iTunes Changes page - songs that changed since the last snapshot.
     */
    @GetMapping("/changes")
    public String showItunesChanges(Model model) {
        String libraryPath = itunesService.getDefaultLibraryPath();
        model.addAttribute("libraryPath", libraryPath);
        model.addAttribute("currentSection", "itunes-changes");
        
        try {
            if (!itunesService.libraryExists()) {
                model.addAttribute("errorMessage", "iTunes Library.xml not found at: " + libraryPath);
                model.addAttribute("hasSnapshot", false);
                return "itunesChanges";
            }

            ItunesChangesResultDTO changes = itunesChangesService.detectChanges();
            
            model.addAttribute("hasSnapshot", changes.isHasSnapshot());
            model.addAttribute("lastSnapshotDate", changes.getLastSnapshotDate() != null 
                ? changes.getLastSnapshotDate().format(DATE_FORMATTER) : null);
            model.addAttribute("snapshotCount", itunesChangesService.getSnapshotCount());
            
            model.addAttribute("changedSongs", changes.getChangedSongs());
            model.addAttribute("addedSongs", changes.getAddedSongs());
            model.addAttribute("removedSongs", changes.getRemovedSongs());
            
            model.addAttribute("changedCount", changes.getChangedCount());
            model.addAttribute("addedCount", changes.getAddedCount());
            model.addAttribute("removedCount", changes.getRemovedCount());
            model.addAttribute("changedNotFoundCount", changes.getChangedNotFoundCount());
            model.addAttribute("hasChanges", changes.hasChanges());
            
        } catch (Exception e) {
            model.addAttribute("errorMessage", "Error detecting changes: " + e.getMessage());
            model.addAttribute("hasSnapshot", false);
        }
        
        return "itunesChanges";
    }

    /**
     * Save the current iTunes state as a snapshot.
     */
    @PostMapping("/changes/save-snapshot")
    public String saveSnapshot(RedirectAttributes redirectAttributes) {
        try {
            int count = itunesChangesService.saveSnapshot();
            redirectAttributes.addFlashAttribute("successMessage", 
                "Snapshot saved successfully! " + count + " songs recorded.");
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("errorMessage", 
                "Error saving snapshot: " + e.getMessage());
        }
        return "redirect:/itunes/changes";
    }
}
