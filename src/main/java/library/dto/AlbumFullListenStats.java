package library.dto;

public record AlbumFullListenStats(
        String firstFullListenDate,
        String lastFullListenDate,
        int fullAlbumPlays
) {
}
