package library.controller;

import library.service.RankNeighborsService;
import library.service.RankNeighborsService.RankNeighbors;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** JSON behind the detail sidebar rank tooltips: the items ranked around the current one in an attribute. */
@RestController
public class RankNeighborsController {

    private final RankNeighborsService rankNeighborsService;

    public RankNeighborsController(RankNeighborsService rankNeighborsService) {
        this.rankNeighborsService = rankNeighborsService;
    }

    @GetMapping("/{catalog}/{id}/rank-neighbors")
    public ResponseEntity<RankNeighbors> neighbors(@PathVariable String catalog, @PathVariable int id,
                                                   @RequestParam String attr) {
        RankNeighbors result = switch (catalog) {
            case "artists" -> rankNeighborsService.forArtist(id, attr);
            case "albums" -> rankNeighborsService.forAlbum(id, attr);
            case "songs" -> rankNeighborsService.forSong(id, attr);
            default -> null;
        };
        return result == null ? ResponseEntity.badRequest().build() : ResponseEntity.ok(result);
    }
}
