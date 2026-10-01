package library.dto;

import java.util.List;

/**
 * Request body for saving externally fetched artwork (Apple Music, Deezer) to an album or song.
 *
 * @param entityType "album" or "song"
 */
public record SaveImagesRequest(List<String> imageUrls, Long entityId, String entityType, boolean hasExistingImage) {
}
