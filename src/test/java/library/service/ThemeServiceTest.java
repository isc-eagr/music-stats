package library.service;

import library.entity.ArtistImageTheme;
import library.entity.ArtistTheme;
import library.repository.ArtistImageThemeRepository;
import library.repository.ArtistThemeRepository;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ThemeServiceTest {

    @Test
    void assignsAnArtistMainImageToTheDefaultTheme() {
        ArtistThemeRepository themeRepository = mock(ArtistThemeRepository.class);
        ArtistImageThemeRepository imageThemeRepository = mock(ArtistImageThemeRepository.class);
        ThemeService service = new ThemeService(themeRepository, imageThemeRepository, mock(JdbcTemplate.class));
        ArtistTheme defaultTheme = new ArtistTheme();
        defaultTheme.setId(1);

        when(themeRepository.findFirstByNameIgnoreCase("Default")).thenReturn(Optional.of(defaultTheme));
        when(imageThemeRepository.findByThemeIdAndArtistId(1, 42)).thenReturn(Optional.empty());
        when(imageThemeRepository.save(any(ArtistImageTheme.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        service.assignDefaultImageToDefaultTheme(42);

        org.mockito.ArgumentCaptor<ArtistImageTheme> assignment = org.mockito.ArgumentCaptor.forClass(ArtistImageTheme.class);
        verify(imageThemeRepository).save(assignment.capture());
        assertThat(assignment.getValue().getThemeId()).isEqualTo(1);
        assertThat(assignment.getValue().getArtistId()).isEqualTo(42);
        assertThat(assignment.getValue().getArtistImageId()).isNull();
    }
}
