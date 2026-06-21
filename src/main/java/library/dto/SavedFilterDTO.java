package library.dto;

public record SavedFilterDTO(
        String pageKey,
        String name,
        String query
) {
}
