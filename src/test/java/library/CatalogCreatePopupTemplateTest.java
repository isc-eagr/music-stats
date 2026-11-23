package library;

import org.jsoup.Jsoup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Create and Bulk tags popups on the catalog lists use the shared popup look (catalog-modal.css). */
class CatalogCreatePopupTemplateTest {
    @ParameterizedTest
    @CsvSource({
            "artists/list,createArtistModal,Create,true",
            "albums/list,createAlbumModal,Create,true",
            "songs/list,createSongModal,Create,true",
            "songs/list,bulkTagModal,Apply Tags,false",
            "genres/list,createModal,Create,false",
            "subgenres/list,createModal,Create,false",
            "languages/list,createModal,Create,false",
            "ethnicities/list,createModal,Create,false"})
    void popupUsesTheSharedLook(String template, String modalId, String primaryLabel, boolean wide) {
        var resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(resolver);

        var popup = Jsoup.parse(engine.process(template, Set.of("#" + modalId), new Context())).getElementById(modalId);
        assertThat(popup).isNotNull();
        assertThat(popup.select(".modal-header button.modal-close[aria-label=Close] svg")).hasSize(1);
        assertThat(popup.select(".modal-content").hasClass("cm-wide")).isEqualTo(wide);

        var footer = popup.select(".modal-footer .cm-btn");
        assertThat(footer.select(".is-primary").text()).isEqualTo(primaryLabel);
        // Primary action last, Cancel right before it
        assertThat(footer.last().hasClass("is-primary")).isTrue();
        assertThat(footer.get(footer.size() - 2).text()).isEqualTo("Cancel");
        assertThat(popup.select(".modal-footer .btn-action, .modal-footer .btn-secondary")).isEmpty();
    }
}
