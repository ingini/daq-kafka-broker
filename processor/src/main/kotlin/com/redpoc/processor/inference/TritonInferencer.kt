package com.redpoc.processor.inference

import com.redpoc.processor.inference.client.TritonHttpClient
import com.redpoc.processor.inference.fetch.ImageFetcher
import com.redpoc.processor.inference.preprocess.ImagePreprocessor
import com.redpoc.processor.inference.preprocess.PreprocessedImage
import com.redpoc.processor.metrics.Metrics
import com.redpoc.processor.model.Detection
import com.redpoc.processor.model.FrameBundleMeta
import org.slf4j.LoggerFactory

/**
 * Real Triton-backed inferencer. Composition root wires in the fetcher,
 * preprocessor and HTTP client; this class holds none of the transport
 * details itself (DIP).
 *
 * Instrumentation:
 *   - Metrics.inferenceErrors labeled by stage (fetch/preprocess/infer/parse)
 *   - Metrics.detectionsProduced labeled by class_name (incremented before
 *     threshold-filtering for downstream Grafana heatmap)
 *
 * Flow:
 *   1. For each sensor's decoded JPEG URI:
 *      a. ImageFetcher.fetch → bytes
 *      b. ImagePreprocessor → 3×640×640 FP32 tensor
 *      c. TritonHttpClient.infer → output [1,300,6] (NMS already baked in)
 *      d. parse → confidence filter → Detection list
 *   2. Merge detections across sensors (first pass: concatenate)
 */
class TritonInferencer(
    private val fetcher: ImageFetcher,
    private val preprocessor: ImagePreprocessor,
    private val client: TritonHttpClient,
    private val classNames: List<String>,
    private val confidenceThreshold: Double,
    /**
     * If non-empty, only detections whose className is in this set are
     * kept. Case-insensitive match.
     */
    private val allowedClasses: Set<String> = emptySet(),
) : Inferencer {

    private val log = LoggerFactory.getLogger(javaClass)
    private val allowedLower: Set<String> = allowedClasses.map { it.lowercase() }.toSet()

    override fun infer(bundle: FrameBundleMeta): List<Detection> {
        val images = bundle.inferenceImages()
        if (bundle.imagesDecoded.isNullOrEmpty()) return emptyList()

        val all = mutableListOf<Detection>()
        for ((sensor, uri) in images) {
            val jpeg = try {
                fetcher.fetch(uri)
            } catch (e: Exception) {
                Metrics.inferenceErrors.labels("fetch").inc()
                log.warn("fetch failed sensor={} bundle={} uri={}", sensor, bundle.bundleId, uri, e)
                continue
            }
            val pre = try {
                preprocessor.preprocess(jpeg)
            } catch (e: Exception) {
                Metrics.inferenceErrors.labels("preprocess").inc()
                log.warn("preprocess failed sensor={} bundle={}", sensor, bundle.bundleId, e)
                continue
            }
            val out = try {
                client.infer(pre.tensor, intArrayOf(1, 3, 640, 640))
            } catch (e: Exception) {
                Metrics.inferenceErrors.labels("infer").inc()
                log.warn("infer failed sensor={} bundle={}", sensor, bundle.bundleId, e)
                continue
            }
            val detections = try {
                parseYoloEndToEndOutput(out, pre).map { it.copy(sensor = sensor) }
            } catch (e: Exception) {
                Metrics.inferenceErrors.labels("parse").inc()
                log.warn("parse failed sensor={} bundle={}", sensor, bundle.bundleId, e)
                continue
            }
            for (d in detections) Metrics.detectionsProduced.labels(d.className).inc()
            all += detections
        }
        return all
    }

    override fun close() = fetcher.close()

    /**
     * Ultralytics end-to-end export: `[1, 300, 6]` where each row is
     *   [x1, y1, x2, y2, confidence, class_id]
     * in the 640×640 letterboxed model-input space. We un-letterbox here
     * so that every `Detection.bbox` leaves this class in **source-image
     * coordinates** (pixels on the original fetched JPEG), independent
     * of whether that JPEG was 1920×1080 or 640×360.
     *
     * Un-letterbox formula:
     *   x_src = (x_640 - padX) / scale
     *   y_src = (y_640 - padY) / scale
     * clipped to [0, W-1] × [0, H-1].
     *
     * Rows with confidence 0 (padding) or below threshold are dropped.
     * Class ids outside the names table are labelled "class_<id>".
     */
    private fun parseYoloEndToEndOutput(out: FloatArray, pre: PreprocessedImage): List<Detection> {
        require(out.size == 300 * 6) { "unexpected output length ${out.size}" }
        val result = mutableListOf<Detection>()
        val wMax = (pre.originalWidth - 1).toDouble()
        val hMax = (pre.originalHeight - 1).toDouble()
        val invScale = 1.0 / pre.scale
        for (i in 0 until 300) {
            val base = i * 6
            val conf = out[base + 4].toDouble()
            if (conf < confidenceThreshold) continue
            val cls = out[base + 5].toInt()
            val name = classNames.getOrNull(cls) ?: "class_$cls"
            if (allowedLower.isNotEmpty() && name.lowercase() !in allowedLower) continue

            val x1 = ((out[base].toDouble() - pre.padX) * invScale).coerceIn(0.0, wMax)
            val y1 = ((out[base + 1].toDouble() - pre.padY) * invScale).coerceIn(0.0, hMax)
            val x2 = ((out[base + 2].toDouble() - pre.padX) * invScale).coerceIn(0.0, wMax)
            val y2 = ((out[base + 3].toDouble() - pre.padY) * invScale).coerceIn(0.0, hMax)
            if (x2 <= x1 || y2 <= y1) continue  // degenerate after clipping

            result += Detection(
                classId = cls,
                className = name,
                confidence = conf,
                bbox = listOf(x1, y1, x2, y2),
            )
        }
        return result
    }
}
