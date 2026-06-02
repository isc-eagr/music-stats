package library.dto;

import java.sql.ResultSet;
import java.sql.SQLException;

public record AlbumStatsRow(
        Integer id,
        String name,
        String artistName,
        Integer artistId,
        Integer genreId,
        String genreName,
        Integer subgenreId,
        String subgenreName,
        Integer languageId,
        String languageName,
        Integer ethnicityId,
        String ethnicityName,
        String releaseYear,
        String releaseDate,
        int songCount,
        long albumLength,
        boolean hasImage,
        String genderName,
        Integer genderId,
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
        String country,
        boolean organized,
        String birthDate,
        String deathDate,
        int imageCount,
        Integer seasonalChartPeak,
        Integer weeklyChartPeak,
        Integer weeklyChartWeeks,
        Integer yearlyChartPeak,
        String weeklyChartPeakStartDate,
        String seasonalChartPeakPeriod,
        String yearlyChartPeakPeriod,
        String seasonalChartPeakStartDate,
        int featuredArtistCount,
        int soloSongCount,
        int songsWithFeatCount,
        Integer ageAtRelease,
        Integer weeklyChartPeakWeekCount,
        Integer seasonalChartPeakSeasonCount,
        Integer yearlyChartPeakYearCount,
        String lastFullListenDate,
        Double itunesPresenceRatio
) {
    public static AlbumStatsRow from(ResultSet rs) throws SQLException {
        return new AlbumStatsRow(
                getInteger(rs, "id"),
                rs.getString("name"),
                rs.getString("artist_name"),
                getInteger(rs, "artist_id"),
                getInteger(rs, "genre_id"),
                rs.getString("genre_name"),
                getInteger(rs, "subgenre_id"),
                rs.getString("subgenre_name"),
                getInteger(rs, "language_id"),
                rs.getString("language_name"),
                getInteger(rs, "ethnicity_id"),
                rs.getString("ethnicity_name"),
                rs.getString("release_year"),
                rs.getString("release_date"),
                rs.getInt("song_count"),
                rs.getLong("album_length"),
                rs.getInt("has_image") == 1,
                rs.getString("gender_name"),
                getInteger(rs, "gender_id"),
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
                rs.getString("country"),
                getBooleanInt(rs, "organized"),
                rs.getString("birth_date"),
                rs.getString("death_date"),
                rs.getInt("image_count"),
                getInteger(rs, "seasonal_chart_peak"),
                getInteger(rs, "weekly_chart_peak"),
                getInteger(rs, "weekly_chart_weeks"),
                getInteger(rs, "yearly_chart_peak"),
                rs.getString("weekly_chart_peak_start_date"),
                rs.getString("seasonal_chart_peak_period"),
                rs.getString("yearly_chart_peak_period"),
                rs.getString("seasonal_chart_peak_start_date"),
                rs.getInt("featured_artist_count"),
                rs.getInt("solo_song_count"),
                rs.getInt("songs_with_feat_count"),
                getInteger(rs, "age_at_release"),
                getInteger(rs, "weekly_chart_peak_weeks"),
                getInteger(rs, "seasonal_chart_peak_seasons"),
                getInteger(rs, "yearly_chart_peak_years"),
                rs.getString("last_full_listen_date"),
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
