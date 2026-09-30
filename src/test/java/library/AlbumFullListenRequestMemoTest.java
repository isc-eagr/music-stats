package library;

import library.service.AlbumFullListenCalculator;
import library.service.AppConfigService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AlbumFullListenRequestMemoTest {

    @AfterEach
    void clearRequest() {
        RequestContextHolder.resetRequestAttributes();
    }

    @Test
    void sharesAllAlbumStatsWithinOneRequestOnly() {
        try (TestDatabaseSupport db = TestDatabaseSupport.create()) {
            AppConfigService config = mock(AppConfigService.class);
            when(config.getAlbumFullListenConfig()).thenReturn(new AppConfigService.AlbumFullListenConfig(0, 2, 3, 3, 4, 5));
            AlbumFullListenCalculator calculator = new AlbumFullListenCalculator(db.jdbcTemplate, config);

            String outsideRequest = calculator.calculateAllAsJson();
            assertThat(calculator.calculateAllAsJson()).isEqualTo(outsideRequest).isNotSameAs(outsideRequest);

            RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(new MockHttpServletRequest()));
            String firstInRequest = calculator.calculateAllAsJson();
            assertThat(calculator.calculateAllAsJson()).isSameAs(firstInRequest);
            assertThat(firstInRequest).isEqualTo(outsideRequest);

            // A different full-listen configuration is never served from the memo.
            when(config.getAlbumFullListenConfig()).thenReturn(new AppConfigService.AlbumFullListenConfig(0, 0, 0, 0, 0, 0));
            assertThat(calculator.calculateAllAsJson()).isNotSameAs(firstInRequest);

            RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(new MockHttpServletRequest()));
            when(config.getAlbumFullListenConfig()).thenReturn(new AppConfigService.AlbumFullListenConfig(0, 2, 3, 3, 4, 5));
            assertThat(calculator.calculateAllAsJson()).isEqualTo(firstInRequest).isNotSameAs(firstInRequest);
        }
    }
}
