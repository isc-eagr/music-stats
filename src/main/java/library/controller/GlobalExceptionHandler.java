package library.controller;

import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.multipart.MultipartException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler({MaxUploadSizeExceededException.class, MultipartException.class})
    public String handleMaxSizeException(Exception ex, RedirectAttributes redirectAttributes) {
        // Use a flash attribute so the GET /scrobbles/upload can show the message after redirect
        redirectAttributes.addFlashAttribute("error", "Uploaded file is too large. Maximum allowed size is 100MB.");
        return "redirect:/scrobbles/upload";
    }
}
