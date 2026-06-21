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
            @RequestParam(required = false) Integer randomSeed,
            Model model) {
        
        // Get filtered and sorted countries
        List<CountryCardDTO> countries = countryService.getCountries(q, sortby, sortdir, randomSeed);

        long totalCount = countryService.countCountries(q);
        
        // Add data to model
        model.addAttribute("currentSection", "countries");
        model.addAttribute("countries", countries);
        model.addAttribute("totalCount", totalCount);
        model.addAttribute("startIndex", totalCount > 0 ? 1 : 0);
        model.addAttribute("endIndex", totalCount);
        
        // Add filter values to maintain state
        model.addAttribute("searchQuery", q);
        model.addAttribute("sortBy", sortby);
        model.addAttribute("sortDir", sortdir);
        model.addAttribute("randomSeed", randomSeed);
        model.addAttribute("defaultSortBy", "name");
        
        return "countries/list";
    }
    
    /**
     * API endpoint to get all distinct countries for dropdowns.
     */
    @GetMapping("/api/list")
    @ResponseBody
    public java.util.List<String> listCountriesApi() {
        return countryService.getAllCountriesSimple();
    }
}
