package library.controller;

import library.dto.SavedFilterDTO;
import library.service.SavedFilterService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/saved-filters")
public class SavedFilterController {

    private final SavedFilterService savedFilterService;

    public SavedFilterController(SavedFilterService savedFilterService) {
        this.savedFilterService = savedFilterService;
    }

    @GetMapping
    public List<SavedFilterDTO> list(@RequestParam String pageKey) {
        return savedFilterService.list(pageKey);
    }

    @PostMapping
    public SavedFilterDTO save(@RequestBody SavedFilterDTO request) {
        return savedFilterService.save(request);
    }

    @DeleteMapping
    public ResponseEntity<Map<String, Object>> delete(@RequestParam String pageKey, @RequestParam String name) {
        boolean deleted = savedFilterService.delete(pageKey, name);
        return ResponseEntity.ok(Map.of("deleted", deleted));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
