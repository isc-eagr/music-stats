package library.util;

import library.dto.LinkedSongDTO;
import library.entity.Song;
import library.util.ArtistGeneralSummaries.PlaysShareSlice;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Summaries shown on the song detail General tab: where each profile value comes from (song override,
 * album or artist) and how the plays split across linked versions.
 */
public final class SongGeneralSummaries {

    public static final String OVERRIDE = "override";
    public static final String ALBUM = "album";
    public static final String ARTIST = "artist";

    private SongGeneralSummaries() {
    }

    /**
     * Source of each overridable attribute (gender, ethnicity, genre, subgenre, language):
     * "override" when the song sets it, "album" or "artist" when inherited. Attributes with no value are left out.
     */
    public static Map<String, String> attributeSources(Song song) {
        Map<String, String> sources = new LinkedHashMap<>();
        if (song == null) {
            return sources;
        }
        put(sources, "gender", source(song.getOverrideGenderId(), null, song.getArtistGenderId()));
        put(sources, "ethnicity", source(song.getOverrideEthnicityId(), null, song.getArtistEthnicityId()));
        put(sources, "genre", source(song.getOverrideGenreId(), song.getAlbumGenreId(), song.getArtistGenreId()));
        put(sources, "subgenre", source(song.getOverrideSubgenreId(), song.getAlbumSubgenreId(), song.getArtistSubgenreId()));
        put(sources, "language", source(song.getOverrideLanguageId(), song.getAlbumLanguageId(), song.getArtistLanguageId()));
        return sources;
    }

    /**
     * Source of the release date: "override" when the song has its own date and the album has one too,
     * "album" when the song inherits the album date, null when the song's own date is the only one (or none).
     */
    public static String releaseDateSource(boolean songHasDate, boolean albumHasDate) {
        if (songHasDate) {
            return albumHasDate ? OVERRIDE : null;
        }
        return albumHasDate ? ALBUM : null;
    }

    /**
     * How the plays split across the linked versions of a song, biggest first ("current" marks this song,
     * "linked" the other versions). Empty unless at least two versions have plays.
     */
    public static List<PlaysShareSlice> linkedPlaysShare(List<LinkedSongDTO> linkedSongs, Integer currentSongId) {
        List<LinkedSongDTO> played = new ArrayList<>();
        for (LinkedSongDTO song : linkedSongs == null ? List.<LinkedSongDTO>of() : linkedSongs) {
            if (song.getPlayCount() != null && song.getPlayCount() > 0) {
                played.add(song);
            }
        }
        List<PlaysShareSlice> slices = new ArrayList<>();
        if (played.size() < 2) {
            return slices;
        }
        played.sort(Comparator.comparingInt(LinkedSongDTO::getPlayCount).reversed());
        int total = played.stream().mapToInt(LinkedSongDTO::getPlayCount).sum();
        double step = 0.75 / (played.size() - 1);
        for (int rank = 0; rank < played.size(); rank++) {
            LinkedSongDTO song = played.get(rank);
            String name = song.getAlbumName() != null ? song.getName() + " · " + song.getAlbumName() : song.getName();
            slices.add(new PlaysShareSlice(name, song.getPlayCount(), song.getPlayCount() * 100.0 / total,
                    Objects.equals(song.getId(), currentSongId) ? "current" : "linked", 0.95 - rank * step, song.getId()));
        }
        return slices;
    }

    private static String source(Integer override, Integer fromAlbum, Integer fromArtist) {
        if (override != null) {
            return OVERRIDE;
        }
        if (fromAlbum != null) {
            return ALBUM;
        }
        return fromArtist != null ? ARTIST : null;
    }

    private static void put(Map<String, String> sources, String key, String value) {
        if (value != null) {
            sources.put(key, value);
        }
    }
}
