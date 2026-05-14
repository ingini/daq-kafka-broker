package com.redpoc.processor.inference.annotate

import com.redpoc.processor.inference.fetch.ImageFetcher
import com.redpoc.processor.inference.fetch.ImageUploader
import com.redpoc.processor.inference.render.Renderer
import com.redpoc.processor.metrics.Metrics
import com.redpoc.processor.model.Detection
import org.slf4j.LoggerFactory

/**
 * Composes fetch → render → upload into a single "annotate" operation.
 *
 * Key derivation:
 *   - `keySuffix` replaces the trailing `.jpg` (case-insensitive).
 *   - When `subdir` is non-empty it is inserted between the source's
 *     parent directory and the filename. This keeps annotated images
 *     grouped under e.g. `cam0/result/` so the raw `cam0/` listing
 *     stays clean.
 *
 * Examples:
 *   src = `2026/05/11/09/VID_001/cam0/20260511T091634_ns.jpg`
 *     subdir=""        + suffix=".annotated.jpg"
 *       → `2026/05/11/09/VID_001/cam0/20260511T091634_ns.annotated.jpg`
 *     subdir="result"  + suffix="_annotated.jpg"
 *       → `2026/05/11/09/VID_001/cam0/result/20260511T091634_ns_annotated.jpg`
 */
class JpegAnnotator(
    private val fetcher: ImageFetcher,
    private val renderer: Renderer,
    private val uploader: ImageUploader,
    private val jpegQuality: Int = 85,
    /** Replaces the trailing `.jpg` if present; otherwise appended. */
    private val keySuffix: String = ".annotated.jpg",
    /** Optional subdirectory to insert before the filename. */
    private val subdir: String = "",
) : Annotator {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun annotate(
        sourceUri: String,
        detections: List<Detection>,
        overlay: List<String>,
    ): String? {
        if (detections.isEmpty() && overlay.isEmpty()) return null
        val (bucket, key) = parse(sourceUri) ?: return null

        val srcBytes = try {
            fetcher.fetch(sourceUri)
        } catch (e: Exception) {
            Metrics.annotationErrors.labels("fetch").inc()
            log.warn("annotate fetch failed {}", sourceUri, e)
            return null
        }

        val annotated = try {
            renderer.render(srcBytes, detections, jpegQuality, overlay)
        } catch (e: Exception) {
            Metrics.annotationErrors.labels("render").inc()
            log.warn("annotate render failed {}", sourceUri, e)
            return null
        }

        val annotatedKey = deriveKey(key)
        return try {
            val uri = uploader.put(bucket, annotatedKey, annotated, "image/jpeg")
            Metrics.annotationsProduced.inc()
            uri
        } catch (e: Exception) {
            Metrics.annotationErrors.labels("upload").inc()
            log.warn("annotate upload failed {}/{}", bucket, annotatedKey, e)
            null
        }
    }

    private fun deriveKey(key: String): String {
        // Step 1 — strip the final `.jpg` (case-insensitive).
        val idx = key.lowercase().lastIndexOf(".jpg")
        val base = if (idx > 0) key.substring(0, idx) else key

        // Step 2 — split base into <dir>/<filename> (last slash).
        // Step 3 — if subdir is set, insert it between dir and filename.
        if (subdir.isEmpty()) return base + keySuffix

        val lastSlash = base.lastIndexOf('/')
        return if (lastSlash >= 0) {
            // <dir>/<subdir>/<filename><keySuffix>
            base.substring(0, lastSlash) + "/" + subdir + "/" +
                    base.substring(lastSlash + 1) + keySuffix
        } else {
            // <subdir>/<base><keySuffix>  (key has no directory part)
            "$subdir/$base$keySuffix"
        }
    }

    private fun parse(uri: String): Pair<String, String>? {
        if (!uri.startsWith("s3://")) return null
        val rest = uri.removePrefix("s3://")
        val bucket = rest.substringBefore('/')
        val key = rest.substringAfter('/')
        return bucket to key
    }
}
