package com.redpoc.processor.inference.preprocess

import java.awt.Color
import java.awt.Image
import java.awt.RenderingHints
import java.awt.image.BufferedImage
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO
import kotlin.math.min
import kotlin.math.round

/**
 * JPEG bytes → 640×640 CHW float32 tensor normalized to [0,1].
 *
 * Layout matches what YOLOv8/YOLO26n expects at its `images` input:
 *   shape [1, 3, 640, 640], channel order RGB.
 *
 * ## Letterbox (aspect-preserving resize)
 *
 * The Ultralytics training pipeline letterboxes inputs: scale by
 * `min(H/h, W/w)`, pad the shorter side with gray (114, 114, 114). If we
 * instead stretched a 16:9 source (1920×1080 or 640×360) to 1:1, cars
 * would become 1.78× taller and often get classified as buses/trucks
 * /trains — exactly the regression observed when enabling the SD edge
 * transcoder. Letterboxing here matches training-time preprocessing and
 * restores accuracy at every source resolution.
 *
 * The returned `scale`, `padX`, `padY` let downstream code map model-space
 * bounding boxes back to source-image coordinates without distortion.
 */
data class PreprocessedImage(
    val tensor: FloatArray,      // 3 * 640 * 640 = 1_228_800 floats
    val originalWidth: Int,
    val originalHeight: Int,
    /** Scale factor applied to the source before padding. */
    val scale: Double,
    /** Pixels of gray padding added on each side of the x-axis. */
    val padX: Int,
    /** Pixels of gray padding added on each side of the y-axis. */
    val padY: Int,
)

class ImagePreprocessor(
    private val targetSize: Int = 640,
    /** YOLO convention: gray (114, 114, 114) for letterbox fill. */
    private val padRgb: Int = 114,
) {
    fun preprocess(jpegBytes: ByteArray): PreprocessedImage {
        val src = ImageIO.read(ByteArrayInputStream(jpegBytes))
            ?: error("not a decodable image (${jpegBytes.size} bytes)")

        val w = src.width
        val h = src.height
        val scale = min(targetSize.toDouble() / w, targetSize.toDouble() / h)
        val newW = round(w * scale).toInt().coerceAtLeast(1)
        val newH = round(h * scale).toInt().coerceAtLeast(1)
        val padX = (targetSize - newW) / 2
        val padY = (targetSize - newH) / 2

        val canvas = BufferedImage(targetSize, targetSize, BufferedImage.TYPE_INT_RGB)
        val g = canvas.createGraphics()
        try {
            // Fill the whole canvas with the letterbox color first.
            g.color = Color(padRgb, padRgb, padRgb)
            g.fillRect(0, 0, targetSize, targetSize)
            // High-quality downscale; matches what Ultralytics does with
            // cv2.INTER_LINEAR closely enough for detection accuracy.
            g.setRenderingHint(
                RenderingHints.KEY_INTERPOLATION,
                RenderingHints.VALUE_INTERPOLATION_BILINEAR,
            )
            g.drawImage(src.getScaledInstance(newW, newH, Image.SCALE_AREA_AVERAGING), padX, padY, null)
        } finally {
            g.dispose()
        }

        // CHW layout: plane 0 = R, plane 1 = G, plane 2 = B.
        val planeSize = targetSize * targetSize
        val out = FloatArray(3 * planeSize)
        for (y in 0 until targetSize) {
            val row = y * targetSize
            for (x in 0 until targetSize) {
                val rgb = canvas.getRGB(x, y)
                val r = (rgb ushr 16) and 0xFF
                val g2 = (rgb ushr 8) and 0xFF
                val b = rgb and 0xFF
                val idx = row + x
                out[idx] = r / 255f
                out[planeSize + idx] = g2 / 255f
                out[2 * planeSize + idx] = b / 255f
            }
        }
        return PreprocessedImage(
            tensor = out,
            originalWidth = w,
            originalHeight = h,
            scale = scale,
            padX = padX,
            padY = padY,
        )
    }
}
