package library.util;

import library.dto.ArtistAlbumDTO;
import library.dto.ArtistSongDTO;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Summaries shown on the artist detail General tab that are derived from the album and song lists
 * already loaded for the page: where the plays come from, and the "No album songs" totals.
 */
public final class ArtistGeneralSummaries {

    private ArtistGeneralSummaries() {
    }

    /**
     * One slice of the "where the plays come from" bar.
     *
     * @param kind    album, no-album or featured (artist page); current or linked (song page linked versions)
     * @param percent share of all listed plays, 0-100
     * @param shade   color strength for the slice, strongest first
     * @param id      album id for album slices (artist page) or song id (song page linked versions); null otherwise
     */
    public record PlaysShareSlice(String name, int plays, double percent, String kind, double shade, Integer id) {
    }

    /** Totals for the songs that are not on any album (featured-on songs excluded). */
    public record NoAlbumSummary(int songCount, int primaryPlays, int legacyPlays, int totalPlays,
                                 long listeningSeconds, String listeningTime,
                                 String firstListened, String lastListened) {
    }

    public static NoAlbumSummary noAlbumSummary(List<ArtistSongDTO> songs) {
        int count = 0;
        int primary = 0;
        int legacy = 0;
        int total = 0;
        long seconds = 0;
        LocalDate first = null;
        LocalDate last = null;
        String firstLabel = null;
        String lastLabel = null;
        for (ArtistSongDTO song : songs == null ? List.<ArtistSongDTO>of() : songs) {
            if (song.getAlbumId() != null || song.isFeaturedOn()) {
                continue;
            }
            count++;
            primary += valueOf(song.getVatitoPlays());
            legacy += valueOf(song.getRobertloverPlays());
            total += valueOf(song.getTotalPlays());
            seconds += valueOf(song.getTotalListeningTimeSeconds());
            LocalDate songFirst = DateFormatUtils.parseDisplayDate(song.getFirstListenedDate());
            if (songFirst != null && (first == null || songFirst.isBefore(first))) {
                first = songFirst;
                firstLabel = song.getFirstListenedDate();
            }
            LocalDate songLast = DateFormatUtils.parseDisplayDate(song.getLastListenedDate());
            if (songLast != null && (last == null || songLast.isAfter(last))) {
                last = songLast;
                lastLabel = song.getLastListenedDate();
            }
        }
        if (count == 0) {
            return null;
        }
        return new NoAlbumSummary(count, primary, legacy, total, seconds,
                seconds > 0 ? TimeFormatUtils.formatTime(seconds) : null, firstLabel, lastLabel);
    }

    /**
     * Every played album, the songs without an album, and the songs where the artist is only featured,
     * biggest first. Empty slices are left out; shades fade evenly from the top slice to the last one.
     */
    public static List<PlaysShareSlice> playsShare(List<ArtistAlbumDTO> albums, List<ArtistSongDTO> songs) {
        record Part(String name, int plays, String kind, Integer id) {
        }
        List<Part> parts = new ArrayList<>();
        for (ArtistAlbumDTO album : albums == null ? List.<ArtistAlbumDTO>of() : albums) {
            if (valueOf(album.getTotalPlays()) > 0) {
                parts.add(new Part(album.getName(), valueOf(album.getTotalPlays()), "album", album.getId()));
            }
        }
        NoAlbumSummary noAlbum = noAlbumSummary(songs);
        if (noAlbum != null && noAlbum.totalPlays() > 0) {
            parts.add(new Part("No album songs", noAlbum.totalPlays(), "no-album", null));
        }
        int featured = 0;
        for (ArtistSongDTO song : songs == null ? List.<ArtistSongDTO>of() : songs) {
            if (song.isFeaturedOn()) {
                featured += valueOf(song.getTotalPlays());
            }
        }
        if (featured > 0) {
            parts.add(new Part("Featured on other artists", featured, "featured", null));
        }

        int total = parts.stream().mapToInt(Part::plays).sum();
        List<PlaysShareSlice> slices = new ArrayList<>();
        if (total <= 0) {
            return slices;
        }
        parts.sort(Comparator.comparingInt(Part::plays).reversed());
        double step = parts.size() > 1 ? 0.75 / (parts.size() - 1) : 0;
        for (int rank = 0; rank < parts.size(); rank++) {
            Part part = parts.get(rank);
            slices.add(new PlaysShareSlice(part.name(), part.plays(), part.plays() * 100.0 / total,
                    part.kind(), 0.95 - rank * step, part.id()));
        }
        return slices;
    }

    /**
     * The artist's album releases: how many, the first and latest by release date, and the span between them.
     *
     * @param first  earliest dated album, null when no album has a release date
     * @param latest latest dated album
     * @param span   "8 years, 2 months" between first and latest release, null when under a day apart
     */
    public record ReleasesSummary(int count, ArtistAlbumDTO first, ArtistAlbumDTO latest, String span) {
    }

    /**
     * Albums listed on the page minus the ones that belong to another artist (the artist only appears on them).
     * Null when nothing is left to count.
     */
    public static ReleasesSummary releases(List<ArtistAlbumDTO> albums) {
        int count = 0;
        ArtistAlbumDTO first = null;
        ArtistAlbumDTO latest = null;
        LocalDate firstDate = null;
        LocalDate latestDate = null;
        for (ArtistAlbumDTO album : albums == null ? List.<ArtistAlbumDTO>of() : albums) {
            if (album.isFromCrossArtist()) {
                continue;
            }
            count++;
            LocalDate released = DateFormatUtils.parseDisplayDate(album.getReleaseDate());
            if (released == null) {
                continue;
            }
            if (firstDate == null || released.isBefore(firstDate)) {
                firstDate = released;
                first = album;
            }
            if (latestDate == null || released.isAfter(latestDate)) {
                latestDate = released;
                latest = album;
            }
        }
        if (count == 0) {
            return null;
        }
        String span = first != null ? DateFormatUtils.spanLabel(first.getReleaseDate(), latest.getReleaseDate()) : null;
        return new ReleasesSummary(count, first, latest, span);
    }

    private static int valueOf(Integer value) {
        return value != null ? value : 0;
    }
}
