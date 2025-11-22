package library.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    @Override
    public void configurePathMatch(PathMatchConfigurer configurer) {
        // Enable trailing slash matching (e.g. /songs and /songs/ both match)
        configurer.setUseTrailingSlashMatch(true);
        // Using AntPathMatcher is configured via application.properties
    }
}
