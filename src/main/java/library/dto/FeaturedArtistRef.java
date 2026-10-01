package library.dto;

/**
 * Lightweight featured-artist reference attached to chart rows (Billboard, Vato's Cuntdown, TRL).
 */
public record FeaturedArtistRef(Integer artistId, String artistName, String genderClass, boolean hasImage) {
}
