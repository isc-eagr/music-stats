package library.controller;

import library.service.PlayAutomationStateService;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;

@ControllerAdvice
public class AutomationBannerControllerAdvice {

    private final PlayAutomationStateService automationStateService;

    public AutomationBannerControllerAdvice(PlayAutomationStateService automationStateService) {
        this.automationStateService = automationStateService;
    }

    @ModelAttribute
    public void addAutomationBannerState(Model model) {
        model.addAttribute("playAutomationBannerState", automationStateService.getBannerState());
    }
}