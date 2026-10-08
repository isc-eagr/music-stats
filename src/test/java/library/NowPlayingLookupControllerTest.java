package library;

import library.controller.NowPlayingLookupController;
import library.service.BillboardHot100Service;
import library.service.ChartService;
import library.service.PcService;
import library.service.SongService;
import library.service.TrlService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.net.URI;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class NowPlayingLookupControllerTest {

    private TestDatabaseSupport db;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        db = TestDatabaseSupport.create();
        db.jdbcTemplate.update("INSERT INTO Artist (id, name, gender_id) VALUES (50, '2 Chainz', 1)");
        db.jdbcTemplate.update("INSERT INTO Album (id, artist_id, name) VALUES (50, 50, 'B.O.A.T.S. II #METIME')");
        db.jdbcTemplate.update("""
                INSERT INTO Song (id, artist_id, album_id, name, track_number)
                VALUES (50, 50, 50, 'Fork', 1),
                       (51, 50, 50, 'Feds Watching (feat. Pharrell)', 3),
                       (52, 50, 50, 'Used 2', 6)
                """);
        db.jdbcTemplate.update("""
                INSERT INTO Play (id, artist, album, song, play_date, song_id, account)
                VALUES (100, '2 Chainz', 'B.O.A.T.S. II #METIME', 'Feds Watching (feat. Pharrell)', '2024-06-01 10:00:00', 51, 'vatito'),
                       (101, '2 Chainz', 'B.O.A.T.S. II #METIME', 'Feds Watching (feat. Pharrell)', '2024-06-02 10:00:00', 51, 'vatito')
                """);
        mockMvc = MockMvcBuilders.standaloneSetup(new NowPlayingLookupController(db.jdbcTemplate,
                mock(SongService.class), mock(ChartService.class), mock(BillboardHot100Service.class),
                mock(PcService.class), mock(TrlService.class))).build();
    }

    @AfterEach
    void tearDown() {
        db.close();
    }

    @Test
    void getWithHashInAlbumLosesTheSongButResolvesTheAlbumAndOffersItsTracklist() throws Exception {
        // What the Shortcut builds: the '#' starts a URL fragment, so the song never reaches the server
        mockMvc.perform(get(URI.create("/api/lookup?artist=2%20Chainz&album=B.O.A.T.S.%20II%20#METIME&song=Feds%20Watching")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.songId").value(nullValue()))
                .andExpect(jsonPath("$.albumId").value(50))
                .andExpect(jsonPath("$.artistId").value(50))
                .andExpect(jsonPath("$.albumPlays").value(2))
                .andExpect(jsonPath("$.candidates[*].id", contains(50, 51, 52)))
                .andExpect(jsonPath("$.html", containsString("B.O.A.T.S. II #METIME")))
                .andExpect(jsonPath("$.html", containsString("/api/lookup/card?songId=51")));
    }

    @Test
    void getWithHashInAlbumStillMatchesWhenTheAlbumIsTheLastParameter() throws Exception {
        mockMvc.perform(get(URI.create("/api/lookup?artist=2%20Chainz&song=Feds%20Watching%20(feat.%20Pharrell)&album=B.O.A.T.S.%20II%20#METIME")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.songId").value(51))
                .andExpect(jsonPath("$.albumId").value(50))
                .andExpect(jsonPath("$.songPlays").value(2))
                .andExpect(jsonPath("$.candidates").doesNotExist());
    }

    @Test
    void formPostKeepsTheHashInTheAlbumIntact() throws Exception {
        mockMvc.perform(post("/api/lookup")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                        .content("artist=2+Chainz&album=B.O.A.T.S.+II+%23METIME&song=Feds+Watching+%28feat.+Pharrell%29"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.songId").value(51))
                .andExpect(jsonPath("$.albumQuery").value("B.O.A.T.S. II #METIME"));
    }

    @Test
    void jsonPostKeepsTheHashInTheAlbumIntact() throws Exception {
        mockMvc.perform(post("/api/lookup")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"artist\":\"2 Chainz\",\"album\":\"B.O.A.T.S. II #METIME\",\"song\":\"Feds Watching (feat. Pharrell)\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.songId").value(51))
                .andExpect(jsonPath("$.albumId").value(50));
    }

    @Test
    void cardEndpointRendersThePickedTrackAsHtml() throws Exception {
        mockMvc.perform(get("/api/lookup/card").param("songId", "51"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.TEXT_HTML))
                .andExpect(content().string(containsString("Feds Watching (feat. Pharrell)")))
                .andExpect(content().string(containsString("B.O.A.T.S. II #METIME")));
    }
}
