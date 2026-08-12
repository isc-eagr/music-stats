package library.controller;

import library.service.PlayAutomationStateService;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PlayAutomationControllerTest {

    @Test
    void returnsTheCurrentUnmatchedBannerForInPlaceRefreshes() {
        PlayAutomationStateService stateService = mock(PlayAutomationStateService.class);
        PlayAutomationStateService.UnmatchedBanner banner =
                new PlayAutomationStateService.UnmatchedBanner(12, "/plays/unmatched?account=vatito");
        when(stateService.getBannerState())
                .thenReturn(new PlayAutomationStateService.BannerState(banner, null, null));

        PlayAutomationController controller = new PlayAutomationController(stateService);
        Map<String, Object> response = controller.getUnmatchedBanner().getBody();

        assertThat(response).containsEntry("success", true);
        assertThat(response).containsEntry("unmatchedBanner", banner);
        assertThat(((PlayAutomationStateService.UnmatchedBanner) response.get("unmatchedBanner")).getCount())
                .isEqualTo(12);
    }

    @Test
    void returnsNullWhenTheCurrentUnmatchedBannerHasCleared() {
        PlayAutomationStateService stateService = mock(PlayAutomationStateService.class);
        when(stateService.getBannerState())
                .thenReturn(new PlayAutomationStateService.BannerState(null, null, null));

        PlayAutomationController controller = new PlayAutomationController(stateService);
        Map<String, Object> response = controller.getUnmatchedBanner().getBody();

        assertThat(response).containsEntry("success", true);
        assertThat(response).containsKey("unmatchedBanner").containsEntry("unmatchedBanner", null);
    }
}
