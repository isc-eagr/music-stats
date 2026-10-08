package library.controller;

import library.dto.TodayMalePlayStatsDTO;
import library.service.PlayStatsService;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class PlayStatsControllerTest {

    @ParameterizedTest
    @CsvSource({"83.33,65 male out of 78", "100.00,1 male out of 1", "0.00,0 male out of 0"})
    void returnsPercentageAndTextAsJson(String percentage, String summary) throws Exception {
        PlayStatsService service = mock(PlayStatsService.class);
        when(service.getTodayMalePlayStats())
                .thenReturn(new TodayMalePlayStatsDTO(new BigDecimal(percentage), summary));

        MockMvcBuilders.standaloneSetup(new PlayStatsController(service)).build()
                .perform(get("/plays/api/today/male-percentage")
                        .header("Origin", "https://another-app.example"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(content().string("{\"percentage\":" + percentage + ",\"text\":\"" + summary + "\"}"))
                .andExpect(header().string("Cache-Control", "no-store"))
                .andExpect(header().string("Access-Control-Allow-Origin", "*"));
    }
}
