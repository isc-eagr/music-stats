package library.util;

import library.dto.AlbumSongDTO;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Summaries shown on the album detail General tab, derived from the song list already loaded
 * for the page: where the plays come from and what the album is made of.
 */
public final class AlbumGeneralSummaries {

    private AlbumGeneralSummaries() {
    }

    /**
     * One song in the "where the plays come from" bar.
     *
     * @param percent share of the album plays, 0-100
     * @param shade   color strength for the slice, strongest first
     */
    public record SongShareSlice(Integer songId, String name, int plays, double percent, double shade) {
    }

    /**
     * Every song on the album, biggest first (ties by name), so the legend reads as a ranking.
     * Shades fade evenly from the top song to the last one. Empty when the album has no plays.
     */
    public static List<SongShareSlice> playsShare(List<AlbumSongDTO> songs) {
        List<AlbumSongDTO> ordered = new ArrayList<>(songs == null ? List.of() : songs);
        int total = ordered.stream().mapToInt(song -> valueOf(song.getTotalPlays())).sum();
        List<SongShareSlice> slices = new ArrayList<>();
        if (total <= 0) {
            return slices;
        }
        ordered.sort(Comparator.comparingInt((AlbumSongDTO song) -> valueOf(song.getTotalPlays())).reversed()
                .thenComparing(song -> song.getName() != null ? song.getName().toLowerCase() : ""));
        double step = ordered.size() > 1 ? 0.75 / (ordered.size() - 1) : 0;
        for (int rank = 0; rank < ordered.size(); rank++) {
            AlbumSongDTO song = ordered.get(rank);
            int plays = valueOf(song.getTotalPlays());
            slices.add(new SongShareSlice(song.getId(), song.getName(), plays, plays * 100.0 / total,
                    0.95 - rank * step));
        }
        return slices;
    }

    /**
     * What the album is made of, for the Catalog bars: iTunes presence (counted only where it is known)
     * and songs credited to another artist than the album's.
     */
    public record AlbumCatalog(int songs, int inItunes, int itunesKnown, int byOthers) {
    }

    public static AlbumCatalog catalog(List<AlbumSongDTO> songs, Integer albumArtistId) {
        int count = 0;
        int inItunes = 0;
        int itunesKnown = 0;
        int byOthers = 0;
        for (AlbumSongDTO song : songs == null ? List.<AlbumSongDTO>of() : songs) {
            count++;
            if (song.getInItunes() != null) {
                itunesKnown++;
                if (song.getInItunes()) {
                    inItunes++;
                }
            }
            if (song.getArtistId() != null && albumArtistId != null && !song.getArtistId().equals(albumArtistId)) {
                byOthers++;
            }
        }
        return new AlbumCatalog(count, inItunes, itunesKnown, byOthers);
    }

    private static int valueOf(Integer value) {
        return value != null ? value : 0;
    }
}
