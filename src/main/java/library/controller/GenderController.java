package library.controller;

import library.dto.GenderCardDTO;
import library.service.GenderService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Controller
@RequestMapping("/genders")
public class GenderController {
    
    private final GenderService genderService;
    
    public GenderController(GenderService genderService) {
        this.genderService = genderService;
    }
    
    @GetMapping
    public String listGenders(
            @RequestParam(required = false) String q,
            @RequestParam(defaultValue = "name") String sortby,
            @RequestParam(defaultValue = "asc") String sortdir,
            Model model) {
        
        // Get filtered and sorted genders
        List<GenderCardDTO> genders = genderService.getGenders(q, sortby, sortdir);

        long totalCount = genderService.countGenders(q);
        
        // Add data to model
        model.addAttribute("currentSection", "genders");
        model.addAttribute("genders", genders);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("startIndex", totalCount > 0 ? 1 : 0);
        model.addAttribute("endIndex", totalCount);
        
        // Add filter values to maintain state
        model.addAttribute("searchQuery", q);
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        model.addAttribute("defaultSortBy", "name");
        
        return "genders/list";
    }
    
    /**
     * API endpoint to get all genders for dropdowns.
     */
    @GetMapping("/api/list")
    @ResponseBody
    public ResponseEntity<java.util.List<java.util.Map<String, Object>>> listGendersApi() {
        java.util.List<java.util.Map<String, Object>> genders = genderService.getAllGendersSimple();
        return ResponseEntity.ok(genders);
    }
}
