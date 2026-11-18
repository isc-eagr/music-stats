package library.controller;

import library.service.CatalogDetailService;
import library.service.CatalogDetailService.Dimension;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.server.ResponseStatusException;

/** Detail pages of the secondary catalogs: /genres/{id}, /subgenres/{id}, /languages/{id}, /ethnicities/{id}, /countries/{name}. */
@Controller
public class CatalogDetailController {

    private final CatalogDetailService catalogDetailService;

    public CatalogDetailController(CatalogDetailService catalogDetailService) {
        this.catalogDetailService = catalogDetailService;
    }

    @GetMapping("/genres/{id}")
    public String genre(@PathVariable String id, Model model) {
        return render(Dimension.GENRE, id, model);
    }

    @GetMapping("/subgenres/{id}")
    public String subgenre(@PathVariable String id, Model model) {
        return render(Dimension.SUBGENRE, id, model);
    }

    @GetMapping("/languages/{id}")
    public String language(@PathVariable String id, Model model) {
        return render(Dimension.LANGUAGE, id, model);
    }

    @GetMapping("/ethnicities/{id}")
    public String ethnicity(@PathVariable String id, Model model) {
        return render(Dimension.ETHNICITY, id, model);
    }

    @GetMapping("/countries/{name}")
    public String country(@PathVariable String name, Model model) {
        return render(Dimension.COUNTRY, name, model);
    }

    private String render(Dimension dimension, String key, Model model) {
        var detail = catalogDetailService.build(dimension, key)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, dimension.label + " not found"));
        model.addAttribute("currentSection", dimension.path);
        model.addAttribute("d", detail);
        return "catalog/detail";
    }
}
