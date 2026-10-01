package library.controller;

import library.dto.TagCardDTO;
import library.service.TagService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.List;

@Controller
@RequestMapping("/tags")
public class TagController {
    private final TagService tagService;

    public TagController(TagService tagService) {
        this.tagService = tagService;
    }

    @GetMapping
    public String listTags(Model model) {
        List<TagCardDTO> tags = tagService.getTagCards();
        model.addAttribute("currentSection", "tags");
        model.addAttribute("tags", tags);
        model.addAttribute("tagCount", tags.size());
        return "tags/list";
    }

    @PostMapping
    public String createTag(@RequestParam("name") String name, RedirectAttributes redirectAttributes) {
        try {
            tagService.createTag(name);
            redirectAttributes.addFlashAttribute("successMessage", "Tag created");
        } catch (IllegalArgumentException ex) {
            redirectAttributes.addFlashAttribute("errorMessage", ex.getMessage());
        }
        return "redirect:/tags";
    }

    @PostMapping("/{id}/delete")
    public String deleteTag(@PathVariable Integer id, RedirectAttributes redirectAttributes) {
        tagService.deleteTag(id);
        redirectAttributes.addFlashAttribute("successMessage", "Tag deleted");
        return "redirect:/tags";
    }
}
