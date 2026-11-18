package library.controller;

import library.service.ConsistencyRankService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/** Consistency ranks for the detail pages and the list More cards, fetched in the background by consistency.js. */
@RestController
public class ConsistencyController {

    private final ConsistencyRankService consistencyRankService;

    public ConsistencyController(ConsistencyRankService consistencyRankService) {
        this.consistencyRankService = consistencyRankService;
    }

    @GetMapping("/{catalog:artists|albums|songs}/{id}/consistency")
    public ResponseEntity<ConsistencyRankService.ConsistencyRanks> ranks(@PathVariable String catalog, @PathVariable int id) {
        return ConsistencyRankService.Catalog.fromPath(catalog)
                .flatMap(c -> consistencyRankService.findRanks(c, id))
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
