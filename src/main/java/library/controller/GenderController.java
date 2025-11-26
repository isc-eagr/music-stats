package library.controller;

import library.dto.GenderCardDTO;
import library.service.GenderService;
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
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "40") int perpage,
            Model model) {
        
        // Get filtered and sorted genders
        List<GenderCardDTO> genders = genderService.getGenders(q, sortby, sortdir, page, perpage);
        
        // Get total count for pagination
        long totalCount = genderService.countGenders(q);
        int totalPages = (int) Math.ceil((double) totalCount / perpage);
        
        // Add data to model
        model.addAttribute("currentSection", "genders");
        model.addAttribute("genders", genders);
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
        
        return "genders/list";
    }
}
