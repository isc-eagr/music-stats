package library.repository;

import library.dto.ArtistStatsQuery;
import library.dto.ArtistStatsRow;

import java.util.List;
import java.util.Map;

public interface ArtistRepositoryCustom {
    List<ArtistStatsRow> findArtistsWithStats(ArtistStatsQuery query);

    Long countArtistsWithFilters(ArtistStatsQuery query);

    Map<Integer, Long> countArtistsByGenderWithFilters(ArtistStatsQuery query);
}
