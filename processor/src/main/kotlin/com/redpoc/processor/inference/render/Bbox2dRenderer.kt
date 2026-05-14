package com.redpoc.processor.inference.render

import com.redpoc.processor.model.Detection
import java.awt.BasicStroke
import java.awt.Color
import java.awt.Font
import java.awt.RenderingHints
import java.awt.image.BufferedImage
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import javax.imageio.IIOImage
import javax.imageio.ImageIO
import javax.imageio.ImageWriteParam
import javax.imageio.stream.MemoryCacheImageOutputStream

/**
 * Draws detection bounding boxes + class/confidence labels onto the
 * original-resolution JPEG. Pure Java AWT — no native deps.
 *
 * Coordinate system: `Detection.bbox` is already in **source-image
 * pixel coordinates** after `TritonInferencer` un-letterboxes the model
 * output. This renderer therefore just clips to the image bounds and
 * draws — no scaling, no letterbox math here. Older callers who wrote
 * bboxes in 640-space must upgrade the producer side.
 *
 * Color selection is deterministic per class via a small hash so that the
 * "bus" bounding box is always the same color across frames — useful
 * when scrubbing a sequence of annotated frames.
 */
class Bbox2dRenderer(
    private val lineWidth: Float = 3f,
    private val fontSize: Float = 18f,
) : Renderer {

    override fun render(
        jpegBytes: ByteArray,
        detections: List<Detection>,
        jpegQuality: Int,
        overlay: List<String>,
    ): ByteArray {
        val src = ImageIO.read(ByteArrayInputStream(jpegBytes))
            ?: error("not a decodable JPEG (${jpegBytes.size} bytes)")

        // Always work on a fresh RGB buffer so we can draw opaque shapes.
        val canvas = BufferedImage(src.width, src.height, BufferedImage.TYPE_INT_RGB)
        canvas.createGraphics().use { g ->
            g.drawImage(src, 0, 0, null)
        }

        canvas.createGraphics().use { g ->
            g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
            g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON)
            g.stroke = BasicStroke(lineWidth)
            g.font = Font("SansSerif", Font.BOLD, fontSize.toInt())
            val fm = g.fontMetrics

            for (d in detections) {
                val bb = d.bbox ?: continue
                if (bb.size != 4) continue
                val color = colorFor(d.className)

                val x1 = bb[0].toInt().coerceAtLeast(0)
                val y1 = bb[1].toInt().coerceAtLeast(0)
                val x2 = bb[2].toInt().coerceAtMost(src.width - 1)
                val y2 = bb[3].toInt().coerceAtMost(src.height - 1)

                // bounding box
                g.color = color
                g.drawRect(x1, y1, x2 - x1, y2 - y1)

                // label background + text
                val label = "${d.className} ${"%.2f".format(d.confidence)}"
                val textW = fm.stringWidth(label)
                val textH = fm.height
                val labelY = if (y1 - textH >= 0) y1 - textH else y1
                g.color = color
                g.fillRect(x1, labelY, textW + 8, textH)
                g.color = contrastTextColor(color)
                g.drawString(label, x1 + 4, labelY + fm.ascent)
            }

            // ── top-right overlay (GPS, timestamp, etc.) ─────────────────
            if (overlay.isNotEmpty()) {
                val padding = 8
                val lineH = fm.height
                val maxLineW = overlay.maxOf { fm.stringWidth(it) }
                val boxW = maxLineW + padding * 2
                val boxH = lineH * overlay.size + padding * 2
                val boxX = src.width - boxW - padding
                val boxY = padding
                // Semi-transparent black rectangle
                g.color = Color(0, 0, 0, 170)
                g.fillRect(boxX, boxY, boxW, boxH)
                g.color = Color.WHITE
                for ((i, line) in overlay.withIndex()) {
                    g.drawString(line, boxX + padding, boxY + padding + fm.ascent + lineH * i)
                }
            }
        }

        return encodeJpeg(canvas, jpegQuality)
    }

    /** Stable color per class name (hash-based). */
    private fun colorFor(className: String): Color {
        val h = className.hashCode()
        val hue = (h and 0xFFFF) / 65535f
        // Saturation 0.75, Value 0.9 for bright readable colors.
        return Color(Color.HSBtoRGB(hue, 0.75f, 0.9f))
    }

    private fun contrastTextColor(bg: Color): Color {
        // Simple perceived-luminance test.
        val y = 0.299 * bg.red + 0.587 * bg.green + 0.114 * bg.blue
        return if (y > 140) Color.BLACK else Color.WHITE
    }

    private fun encodeJpeg(img: BufferedImage, quality: Int): ByteArray {
        val out = ByteArrayOutputStream()
        val writer = ImageIO.getImageWritersByFormatName("jpg").next()
        val param = writer.defaultWriteParam.apply {
            compressionMode = ImageWriteParam.MODE_EXPLICIT
            compressionQuality = (quality / 100f).coerceIn(0.1f, 1.0f)
        }
        MemoryCacheImageOutputStream(out).use { mcos ->
            writer.output = mcos
            writer.write(null, IIOImage(img, null, null), param)
        }
        writer.dispose()
        return out.toByteArray()
    }
}

/** Small inline helper so we don't pull in kotlinx.io. */
private inline fun <T> java.awt.Graphics2D.use(block: (java.awt.Graphics2D) -> T): T =
    try { block(this) } finally { dispose() }
