package library.util;

import java.util.List;

/**
 * Encodes the artist filter mode without widening the catalog service method signatures.
 * Artist IDs are positive in the database, so negative values safely represent exclusion.
 */
public final class ArtistFilterMode {

    private ArtistFilterMode() {
    }

    public static List<Integer> encode(List<Integer> artistIds, String mode) {
        if (artistIds == null || artistIds.isEmpty() || !"excludes".equalsIgnoreCase(mode)) {
            return artistIds;
        }

        return artistIds.stream()
                .filter(id -> id != null && id > 0)
                .map(id -> -id)
                .toList();
    }

    public static void appendSqlFilter(StringBuilder sql, List<Object> params,
                                       String artistIdExpression, List<Integer> encodedArtistIds) {
        if (encodedArtistIds == null || encodedArtistIds.isEmpty()) {
            return;
        }

        boolean excludes = encodedArtistIds.stream().allMatch(id -> id != null && id < 0);
        List<Integer> artistIds = encodedArtistIds.stream()
                .filter(id -> id != null && id != 0)
                .map(Math::abs)
                .toList();
        if (artistIds.isEmpty()) {
            return;
        }

        String placeholders = String.join(",", artistIds.stream().map(id -> "?").toList());
        sql.append(" AND ").append(artistIdExpression)
                .append(excludes ? " NOT IN (" : " IN (")
                .append(placeholders).append(")");
        params.addAll(artistIds);
    }
}
