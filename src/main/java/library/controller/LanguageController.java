package library.controller;

import library.dto.LanguageCardDTO;
import library.entity.Language;
import library.service.LanguageService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

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
            @RequestParam(required = false) Integer randomSeed,
            Model model) {
        
        // Get filtered and sorted languages
        List<LanguageCardDTO> languages = languageService.getLanguages(q, sortby, sortdir, randomSeed);

        long totalCount = languageService.countLanguages(q);
        
        // Add data to model
        model.addAttribute("currentSection", "languages");
        model.addAttribute("languages", languages);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("startIndex", totalCount > 0 ? 1 : 0);
        model.addAttribute("endIndex", totalCount);
        
        // Add filter values to maintain state
        model.addAttribute("searchQuery", q);
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        model.addAttribute("defaultSortBy", "name");
        
        return "languages/list";
    }

    @GetMapping("/{id}/image")
    @ResponseBody
    public byte[] getLanguageImage(@PathVariable Integer id) {
        byte[] image = languageService.getLanguageImage(id);
        return image != null ? image : new byte[0];
    }

    /**
     * API endpoint to get all languages for dropdowns.
     */
    @GetMapping("/api/list")
    @ResponseBody
    public java.util.List<java.util.Map<String, Object>> listLanguagesApi() {
        return languageService.getAllLanguagesSimple();
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
