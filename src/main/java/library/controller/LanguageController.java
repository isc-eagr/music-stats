package library.controller;

import library.dto.LanguageCardDTO;
import library.entity.Language;
import library.service.LanguageService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Controller
@RequestMapping("/languages")
public class LanguageController {
    
    private final LanguageService languageService;
    
    public LanguageController(LanguageService languageService) {
        this.languageService = languageService;
    }
    
    @GetMapping
    public String listLanguages(
            @RequestParam(required = false) String q,
            @RequestParam(defaultValue = "name") String sortby,
            @RequestParam(defaultValue = "asc") String sortdir,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "40") int perpage,
            Model model) {
        
        // Get filtered and sorted languages
        List<LanguageCardDTO> languages = languageService.getLanguages(q, sortby, sortdir, page, perpage);
        
        // Get total count for pagination
        long totalCount = languageService.countLanguages(q);
        int totalPages = (int) Math.ceil((double) totalCount / perpage);
        
        // Add data to model
        model.addAttribute("currentSection", "languages");
        model.addAttribute("languages", languages);
        model.addAttribute("currentPage", page);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("perPage", perpage);
        model.addAttribute("startIndex", (page * perpage) + 1);
        model.addAttribute("endIndex", Math.min((page + 1) * perpage, totalCount));
        
        // Add filter values to maintain state
        model.addAttribute("searchQuery", q);
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        
        return "languages/list";
    }
    
    @GetMapping("/{id}")
    public String viewLanguage(@PathVariable Integer id, Model model) {
        Optional<Language> language = languageService.getLanguageById(id);
        
        if (language.isEmpty()) {
            return "redirect:/languages";
        }
        
        // Check if language has an image
        byte[] image = languageService.getLanguageImage(id);
        boolean hasImage = (image != null && image.length > 0);
        
        model.addAttribute("currentSection", "languages");
        model.addAttribute("language", language.get());
        model.addAttribute("hasImage", hasImage);
        
        // Add statistics for the language
        Map<String, Object> stats = languageService.getLanguageStats(id);
        model.addAttribute("stats", stats);
        
        // Add top 50 artists, albums, and songs
        model.addAttribute("topArtists", languageService.getTopArtistsForLanguage(id));
        model.addAttribute("topAlbums", languageService.getTopAlbumsForLanguage(id));
        model.addAttribute("topSongs", languageService.getTopSongsForLanguage(id));
        
        return "languages/detail";
    }
    
    @GetMapping("/{id}/image")
    @ResponseBody
    public byte[] getLanguageImage(@PathVariable Integer id) {
        byte[] image = languageService.getLanguageImage(id);
        return image != null ? image : new byte[0];
    }
    
    @PostMapping("/{id}/image")
    @ResponseBody
    public String uploadLanguageImage(@PathVariable Integer id, @RequestParam("image") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "error";
            }
            
            languageService.updateLanguageImage(id, file.getBytes());
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @DeleteMapping("/{id}/image")
    @ResponseBody
    public String deleteLanguageImage(@PathVariable Integer id) {
        try {
            languageService.updateLanguageImage(id, null);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        }
    }
    
    @PostMapping("/create")
    @ResponseBody
    public Map<String, Object> createLanguage(@RequestBody Map<String, String> payload) {
        try {
            Language language = new Language();
            language.setName(payload.get("name"));
            Language created = languageService.createLanguage(language);
            Map<String, Object> response = new java.util.HashMap<>();
            response.put("id", created.getId());
            response.put("name", created.getName());
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create language");
        }
    }
}
