package library;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletContext;
import org.thymeleaf.context.WebContext;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import org.thymeleaf.web.servlet.JakartaServletWebApplication;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Renders one part of a real template (and the fragments it pulls in) without Spring or the navigation bar. */
final class DetailTemplateSupport {

    private DetailTemplateSupport() {}

    static Document render(String template, String selector, Map<String, Object> model) {
        var resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        resolver.setCharacterEncoding("UTF-8");
        var engine = new SpringTemplateEngine();
        engine.setTemplateResolver(resolver);
        var servletContext = new MockServletContext();
        var exchange = JakartaServletWebApplication.buildApplication(servletContext)
                .buildExchange(new MockHttpServletRequest(servletContext), new MockHttpServletResponse());
        return Jsoup.parse(engine.process(template, Set.of(selector), new WebContext(exchange, Locale.ENGLISH, model)));
    }
}
