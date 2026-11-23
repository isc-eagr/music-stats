package library;

import library.controller.BillboardHot100Controller;
import library.controller.PcController;
import library.controller.TrlController;
import library.service.BillboardHot100Service;
import library.service.PcService;
import library.service.TrlService;
import library.util.ChartRowAttributes;
import org.junit.jupiter.api.Test;
import org.springframework.ui.ExtendedModelMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Billboard Hot 100, TRL and Vato's Cuntdown recaps: one shared page and the breakdown tile attributes. */
class ChartRecapPageTest {

    private static Map<String, Object> row(Integer songId, Integer artistId) {
        Map<String, Object> row = new HashMap<>();
        row.put("songId", songId);
        row.put("artistId", artistId);
        return row;
    }

    @Test
    void recapRowsUseTheSongThenTheArtistThenNothing() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            // Song 2 overrides genre (Dance) and language (English) over Selena's Pop / Spanish
            List<Map<String, Object>> rows = new ArrayList<>(List.of(row(2, 1), row(null, 3), row(null, null)));

            ChartRowAttributes.applyToRecapRows(db.jdbcTemplate, rows);

            assertThat(rows.get(0)).containsEntry("genreName", "Dance").containsEntry("languageName", "English")
                    .containsEntry("ethnicityName", "Latina").containsEntry("countryName", "Mexico");
            // Not linked to a song but matched to The Static Hearts: the artist's own values
            assertThat(rows.get(1)).containsEntry("genreName", "Rock").containsEntry("languageName", "English")
                    .containsEntry("ethnicityName", "White").containsEntry("countryName", "United States");
            assertThat(rows.get(2)).containsEntry("genreName", null).containsEntry("countryName", null);
        }
    }

    @Test
    void chartsWithOnlyUnmatchedRowsBesideSongsStillLoad() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            // No artist-only rows: the artist lookup is empty, and the unmatched row must not crash the date
            List<Map<String, Object>> rows = new ArrayList<>(List.of(row(1, 1), row(null, null)));

            ChartRowAttributes.applyToRecapRows(db.jdbcTemplate, rows);

            // Song 1 has no genre of its own; its album (Amor Prohibido) overrides Selena's Pop to Rock
            assertThat(rows.get(0)).containsEntry("genreName", "Rock");
            assertThat(rows.get(1)).containsEntry("genreName", null).containsEntry("ethnicityName", null);
        }
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            List<Map<String, Object>> unmatchedOnly = new ArrayList<>(List.of(row(null, null), row(null, null)));

            ChartRowAttributes.applyToRecapRows(db.jdbcTemplate, unmatchedOnly);

            assertThat(unmatchedOnly).allSatisfy(r -> assertThat(r).containsEntry("countryName", null));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void allThreeRecapsRenderTheSharedPageWithTheirChartConfig() {
        BillboardHot100Service billboard = mock(BillboardHot100Service.class);
        TrlService trl = mock(TrlService.class);
        PcService pc = mock(PcService.class);
        when(billboard.getAvailableChartDates()).thenReturn(List.of("2010-05-29", "2010-06-05"));
        when(trl.getAvailableChartDates()).thenReturn(List.of("2005-03-31"));
        when(pc.getAvailableChartDates()).thenReturn(List.of("2008-07-16"));

        ExtendedModelMap hot100 = new ExtendedModelMap();
        ExtendedModelMap trlModel = new ExtendedModelMap();
        ExtendedModelMap pcModel = new ExtendedModelMap();
        assertThat(new BillboardHot100Controller(billboard).recaps(null, hot100)).isEqualTo("charts/recap");
        assertThat(new TrlController(trl).recaps(null, trlModel)).isEqualTo("charts/recap");
        assertThat(new PcController(pc).recaps(null, pcModel)).isEqualTo("charts/recap");

        Map<String, Object> hot100Config = (Map<String, Object>) hot100.get("recapConfig");
        assertThat(hot100Config).containsEntry("key", "hot100").containsEntry("unit", "week")
                .containsEntry("base", "/misc/billboard-hot-100").containsEntry("selectedDate", "2010-06-05")
                .containsEntry("tiers", List.of(1, 5, 10, 20, 50, 100));
        assertThat((Map<String, Object>) trlModel.get("recapConfig")).containsEntry("unit", "day").containsEntry("base", "/misc/trl");
        assertThat((Map<String, Object>) pcModel.get("recapConfig")).containsEntry("key", "cuntdown").containsEntry("base", "/misc/vatos-cuntdown");
        // The page loads its rows from /recaps/data, so the page request no longer builds the countdown
        verify(billboard, never()).getCountdownForDate("2010-06-05");
    }

    @Test
    void recapDataCarriesTheBreakdownAttributes() {
        BillboardHot100Service billboard = mock(BillboardHot100Service.class);
        List<Map<String, Object>> countdown = new ArrayList<>(List.of(row(1, 1)));
        when(billboard.findClosestChartDate("2010-06-05")).thenReturn("2010-06-05");
        when(billboard.getCountdownForDate("2010-06-05")).thenReturn(countdown);

        new BillboardHot100Controller(billboard).recapsData("2010-06-05");

        verify(billboard).applyBreakdownAttributes(countdown);
    }
}
