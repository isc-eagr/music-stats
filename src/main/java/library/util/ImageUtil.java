package library.util;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Iterator;

public class ImageUtil {

    private ImageUtil() {}

    /**
     * Resizes the given image bytes so the longest side is at most maxDimension pixels.
     * Uses multi-step progressive downscaling (halving repeatedly) for high quality.
     * Detects the source format and preserves it (PNG stays PNG, JPG stays JPG).
     * Returns the original bytes unchanged if the image is already small enough or cannot be decoded.
     */
    public static byte[] resizeThumbnail(byte[] input, int maxDimension) {
        if (input == null || input.length == 0) return input;
        try {
            BufferedImage original = ImageIO.read(new ByteArrayInputStream(input));
            if (original == null) return input;

            int w = original.getWidth();
            int h = original.getHeight();
            if (w <= maxDimension && h <= maxDimension) return input;

            // Detect source format
            String format = detectFormat(input);
            boolean hasAlpha = format.equals("png");

            // Multi-step progressive downscale: halve repeatedly until within 2x of target
            double targetScale = (double) maxDimension / Math.max(w, h);
            int targetW = Math.max(1, (int) Math.round(w * targetScale));
            int targetH = Math.max(1, (int) Math.round(h * targetScale));

            BufferedImage current = original;
            int curW = w;
            int curH = h;

            // Halve dimensions progressively until we're within 2x of target
            while (curW / 2 > targetW && curH / 2 > targetH) {
                int halfW = curW / 2;
                int halfH = curH / 2;
                int imageType = hasAlpha ? BufferedImage.TYPE_INT_ARGB : BufferedImage.TYPE_INT_RGB;
                BufferedImage half = new BufferedImage(halfW, halfH, imageType);
                Graphics2D g = half.createGraphics();
                g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
                g.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
                g.drawImage(current, 0, 0, halfW, halfH, null);
                g.dispose();
                current = half;
                curW = halfW;
                curH = halfH;
            }

            // Final resize to exact target dimensions with bicubic
            int imageType = hasAlpha ? BufferedImage.TYPE_INT_ARGB : BufferedImage.TYPE_INT_RGB;
            BufferedImage resized = new BufferedImage(targetW, targetH, imageType);
            Graphics2D g2d = resized.createGraphics();
            g2d.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
            g2d.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
            g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            g2d.drawImage(current, 0, 0, targetW, targetH, null);
            g2d.dispose();

            // Write in source format with high quality
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            if (format.equals("png")) {
                ImageIO.write(resized, "png", baos);
            } else {
                Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpg");
                if (!writers.hasNext()) return input;
                ImageWriter writer = writers.next();
                ImageWriteParam param = writer.getDefaultWriteParam();
                param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
                param.setCompressionQuality(0.95f);
                try (ImageOutputStream ios = ImageIO.createImageOutputStream(baos)) {
                    writer.setOutput(ios);
                    writer.write(null, new IIOImage(resized, null, null), param);
                } finally {
                    writer.dispose();
                }
            }
            return baos.toByteArray();
        } catch (Exception e) {
            return input;
        }
    }

    /**
     * Detects image format from the first bytes (magic numbers).
     */
    private static String detectFormat(byte[] data) {
        if (data.length >= 8
                && data[0] == (byte) 0x89
                && data[1] == (byte) 0x50  // P
                && data[2] == (byte) 0x4E  // N
                && data[3] == (byte) 0x47) { // G
            return "png";
        }
        return "jpg";
    }
}


