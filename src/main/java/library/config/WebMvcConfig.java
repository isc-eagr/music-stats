package library.config;

import library.controller.AutomationBannerInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    private final AutomationBannerInterceptor automationBannerInterceptor;

    public WebMvcConfig(AutomationBannerInterceptor automationBannerInterceptor) {
        this.automationBannerInterceptor = automationBannerInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(automationBannerInterceptor);
    }
}
