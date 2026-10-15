package library;

import org.jsoup.Jsoup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SongCreationConfirmationTemplateTest {
    @ParameterizedTest
    @CsvSource({"songs/list,createSongModal", "fragments/unmatched-plays-modal,createWizardModal"})
    void rendersConfirmationInsideTheCreationModal(String template, String modalId) {
        var resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(resolver);

        var document = Jsoup.parse(engine.process(template, Set.of("#" + modalId), new Context()));
        var panel = document.select("#" + modalId + " [data-song-duplicate-confirmation]");
        assertThat(panel).hasSize(1);
        assertThat(panel.getFirst().hasAttr("hidden")).isTrue();
        assertThat(panel.select("[data-cancel-duplicate]").text()).isEqualTo("Go back");
        assertThat(panel.select("[data-confirm-duplicate]").text()).isEqualTo("Create anyway");
    }
}
