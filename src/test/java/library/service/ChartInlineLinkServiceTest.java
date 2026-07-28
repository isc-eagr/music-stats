package library.service;

import library.dto.PcOverviewRowDTO;
import library.entity.TrlDebut;
import library.repository.TrlDebutRepository;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class ChartInlineLinkServiceTest {

    @Test
    void pcMatchReturnsTheFreshOverviewRowForInlineReplacement() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        PcService service = spy(new PcService(jdbcTemplate));
        PcOverviewRowDTO updatedRow = new PcOverviewRowDTO();
        updatedRow.setSongId(42);
        updatedRow.setSongTitle("Canonical Song");
        updatedRow.setArtistName("Canonical Artist");

        when(jdbcTemplate.queryForMap(anyString(), any(Object[].class))).thenReturn(Map.of(
            "song_name", "Canonical Song",
            "artist_name", "Canonical Artist"
        ));
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(3);
        doReturn(List.of(updatedRow)).when(service).getOverviewRows();

        Map<String, Object> result = service.matchRawGroup("raw artist", "raw song", 42);

        assertThat(result)
            .containsEntry("ok", true)
            .containsEntry("updatedEntries", 3)
            .containsEntry("row", updatedRow);
    }

    @Test
    void pcMergeReturnsTheConsolidatedOverviewRowForInlineReplacement() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        PcService service = spy(new PcService(jdbcTemplate));
        PcOverviewRowDTO updatedRow = new PcOverviewRowDTO();
        updatedRow.setSongId(42);
        updatedRow.setSongTitle("Source Song");
        updatedRow.setArtistName("Source Artist");

        when(jdbcTemplate.query(
            anyString(),
            any(ResultSetExtractor.class),
            any(Object[].class)
        )).thenReturn(42);
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(4);
        doReturn(List.of(updatedRow)).when(service).getOverviewRows();

        Map<String, Object> result = service.mergeEntries(
            "Source Artist",
            "Source Song",
            "Target Artist",
            "Target Song"
        );

        assertThat(result)
            .containsEntry("ok", true)
            .containsEntry("updated", 4)
            .containsEntry("row", updatedRow);
    }

    @Test
    void trlMatchReturnsTheFreshDebutRowForInlineReplacement() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        TrlService service = spy(new TrlService(mock(TrlDebutRepository.class), jdbcTemplate));
        TrlDebut updatedRow = new TrlDebut();
        updatedRow.setId(7);
        updatedRow.setSongId(42);
        updatedRow.setSongTitle("Canonical Song");
        updatedRow.setArtistName("Canonical Artist");

        when(jdbcTemplate.queryForMap(anyString(), any(Object[].class))).thenReturn(Map.of(
            "song_name", "Canonical Song",
            "artist_name", "Canonical Artist"
        ));
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);
        doReturn(List.of(updatedRow)).when(service).getAllDebuts();

        Map<String, Object> result = service.matchSong(7, 42);

        assertThat(result)
            .containsEntry("ok", true)
            .containsEntry("row", updatedRow);
    }
}
