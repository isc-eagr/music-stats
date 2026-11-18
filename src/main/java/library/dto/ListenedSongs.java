package library.dto;

/** Names of the songs behind the first and last play of an artist or album (either may be null). */
public record ListenedSongs(String first, String last) {
}
