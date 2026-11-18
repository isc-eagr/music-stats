package library.controller;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;

class DetailRedirectsTest {

    @Test
    void keepsThePageQueryAfterASave() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/artists/196");
        request.setQueryString("tab=chart-history&includeGroups=true&chart=bb");
        assertThat(DetailRedirects.backTo("/artists/196", request))
                .isEqualTo("redirect:/artists/196?tab=chart-history&includeGroups=true&chart=bb");
    }

    @Test
    void plainRedirectWithoutAQuery() {
        assertThat(DetailRedirects.backTo("/songs/5", new MockHttpServletRequest("POST", "/songs/5")))
                .isEqualTo("redirect:/songs/5");
        assertThat(DetailRedirects.backTo("/songs/5", null)).isEqualTo("redirect:/songs/5");
    }
}
