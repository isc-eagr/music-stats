package library.dto;

import java.sql.ResultSet;
import java.sql.SQLException;

public record ArtistStatsRow(
        Integer id,
        String name,
        Integer genderId,
        String genderName,
        Integer ethnicityId,
        String ethnicityName,
        Integer genreId,
        String genreName,
        Integer subgenreId,
        String subgenreName,
        Integer languageId,
        String languageName,
        String country,
        int songCount,
        int albumCount,
        boolean hasImage,
        int playCount,
        int vatitoPlayCount,
        int robertloverPlayCount,
        long timeListened,
        String firstListened,
        String lastListened,
        int daysListened,
        int weeksListened,
        int monthsListened,
        int yearsListened,
        boolean organized,
        int featuredSongCount,
        String birthDate,
        String deathDate,
        int imageCount,
        long totalSongLength,
        int featuredArtistCount,
        int soloSongCount,
        int songsWithFeatCount,
        int standaloneSongCount,
        boolean hasThemeImage,
        Double itunesPresenceRatio
) {
    public static ArtistStatsRow from(ResultSet rs) throws SQLException {
        return new ArtistStatsRow(
                getInteger(rs, "id"),
                rs.getString("name"),
                getInteger(rs, "gender_id"),
                rs.getString("gender_name"),
                getInteger(rs, "ethnicity_id"),
                rs.getString("ethnicity_name"),
                getInteger(rs, "genre_id"),
                rs.getString("genre_name"),
                getInteger(rs, "subgenre_id"),
                rs.getString("subgenre_name"),
                getInteger(rs, "language_id"),
                rs.getString("language_name"),
                rs.getString("country"),
                rs.getInt("song_count"),
                rs.getInt("album_count"),
                rs.getInt("has_image") == 1,
                rs.getInt("play_count"),
                rs.getInt("vatito_play_count"),
                rs.getInt("robertlover_play_count"),
                rs.getLong("time_listened"),
                rs.getString("first_listened"),
                rs.getString("last_listened"),
                rs.getInt("days_listened"),
                rs.getInt("weeks_listened"),
                rs.getInt("months_listened"),
                rs.getInt("years_listened"),
                getBooleanInt(rs, "organized"),
                rs.getInt("featured_song_count"),
                rs.getString("birth_date"),
                rs.getString("death_date"),
                rs.getInt("image_count"),
                rs.getLong("total_song_length"),
                rs.getInt("featured_artist_count_stat"),
                rs.getInt("solo_song_count"),
                rs.getInt("songs_with_feat_count"),
                rs.getInt("standalone_song_count"),
                rs.getInt("has_theme_image") == 1,
                getDouble(rs, "itunes_presence_ratio")
        );
    }

    private static Integer getInteger(ResultSet rs, String column) throws SQLException {
        Object value = rs.getObject(column);
        return value instanceof Number number ? number.intValue() : null;
    }

    private static Double getDouble(ResultSet rs, String column) throws SQLException {
        Object value = rs.getObject(column);
        return value instanceof Number number ? number.doubleValue() : null;
    }

    private static boolean getBooleanInt(ResultSet rs, String column) throws SQLException {
        Object value = rs.getObject(column);
        return value instanceof Number number && number.intValue() == 1;
    }
}
