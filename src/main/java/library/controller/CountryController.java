package library.controller;

import library.dto.CountryCardDTO;
import library.service.CountryService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Controller
@RequestMapping("/countries")
public class CountryController {
    
    private final CountryService countryService;
    
    public CountryController(CountryService countryService) {
        this.countryService = countryService;
    }
    
    @GetMapping
    public String listCountries(
            @RequestParam(required = false) String q,
            @RequestParam(defaultValue = "name") String sortby,
            @RequestParam(defaultValue = "asc") String sortdir,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "40") int perpage,
            Model model) {
        
        // Get filtered and sorted countries
        List<CountryCardDTO> countries = countryService.getCountries(q, sortby, sortdir, page, perpage);
        
        // Get total count for pagination
        long totalCount = countryService.countCountries(q);
        int totalPages = (int) Math.ceil((double) totalCount / perpage);
        
        // Add data to model
        model.addAttribute("currentSection", "countries");
        model.addAttribute("countries", countries);
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
        
        return "countries/list";
    }
}
