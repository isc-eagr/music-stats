package library.util;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ThumbnailCacheTest {

    @Test
    void resizesEachDistinctImageOnlyOnce() {
        AtomicInteger resizes = new AtomicInteger();
        ThumbnailCache cache = new ThumbnailCache(1024, (bytes, max) -> {
            resizes.incrementAndGet();
            return new byte[]{bytes[0], (byte) max.intValue()};
        });

        byte[] first = cache.thumbnail(new byte[]{1, 2, 3}, 600);
        byte[] again = cache.thumbnail(new byte[]{1, 2, 3}, 600);
        byte[] other = cache.thumbnail(new byte[]{9, 2, 3}, 600);

        assertThat(again).isSameAs(first);
        assertThat(other).containsExactly(9, (byte) 600);
        assertThat(resizes).hasValue(2);
    }

    @Test
    void evictsLeastRecentlyUsedEntriesWhenOverBudget() {
        ThumbnailCache cache = new ThumbnailCache(20, (bytes, max) -> new byte[10]);

        cache.thumbnail(new byte[]{1}, 600);
        cache.thumbnail(new byte[]{2}, 600);
        cache.thumbnail(new byte[]{1}, 600); // touch 1 so 2 becomes the eldest
        cache.thumbnail(new byte[]{3}, 600);

        assertThat(cache.size()).isEqualTo(2);
    }

    @Test
    void notModifiedWhenTheBrowserAlreadyHasThisImage() throws Exception {
        byte[] jpeg = jpeg(40, 30);
        ResponseEntity<byte[]> first = ImageResponses.image(jpeg, false, null);
        String etag = first.getHeaders().getETag();

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getHeaders().getContentType()).isEqualTo(MediaType.IMAGE_JPEG);
        assertThat(first.getHeaders().getCacheControl()).isEqualTo("no-cache");
        assertThat(etag).isNotBlank();

        ResponseEntity<byte[]> revalidated = ImageResponses.image(jpeg, false, "W/" + etag + ", \"other\"");
        assertThat(revalidated.getStatusCode()).isEqualTo(HttpStatus.NOT_MODIFIED);
        assertThat(revalidated.getBody()).isNull();
    }

    @Test
    void replacedImageGetsANewEtag() throws Exception {
        String before = ImageResponses.image(jpeg(40, 30), false, null).getHeaders().getETag();
        ResponseEntity<byte[]> after = ImageResponses.image(jpeg(41, 30), false, before);

        assertThat(after.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(after.getHeaders().getETag()).isNotEqualTo(before);
    }

    @Test
    void thumbnailsAreResizedAndTaggedSeparatelyFromTheFullImage() throws Exception {
        byte[] large = jpeg(1200, 800);

        ResponseEntity<byte[]> full = ImageResponses.image(large, false, null);
        ResponseEntity<byte[]> thumb = ImageResponses.image(large, true, null);

        assertThat(thumb.getHeaders().getETag()).isNotEqualTo(full.getHeaders().getETag());
        BufferedImage decoded = ImageIO.read(new ByteArrayInputStream(thumb.getBody()));
        assertThat(decoded.getWidth()).isEqualTo(600);
        assertThat(decoded.getHeight()).isEqualTo(400);
        assertThat(ImageResponses.image(large, true, null).getBody()).isSameAs(thumb.getBody());
    }

    @Test
    void missingImageStaysAnEmptyOkResponse() {
        ResponseEntity<byte[]> response = ImageResponses.image(null, true, "\"anything\"");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNull();
    }

    @Test
    void detectsCommonImageTypes() {
        assertThat(ImageResponses.mediaType(new byte[]{(byte) 0x89, 'P', 'N', 'G'})).isEqualTo(MediaType.IMAGE_PNG);
        assertThat(ImageResponses.mediaType(new byte[]{'G', 'I', 'F', '8'})).isEqualTo(MediaType.IMAGE_GIF);
        assertThat(ImageResponses.mediaType(new byte[]{'R', 'I', 'F', 'F', 0, 0, 0, 0, 'W', 'E', 'B', 'P'}).toString())
                .isEqualTo("image/webp");
        assertThat(ImageResponses.mediaType(new byte[]{1, 2, 3})).isEqualTo(MediaType.APPLICATION_OCTET_STREAM);
    }

    private static byte[] jpeg(int width, int height) throws Exception {
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        image.setRGB(0, 0, 0xFF0000);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ImageIO.write(image, "jpg", out);
        return out.toByteArray();
    }
}
