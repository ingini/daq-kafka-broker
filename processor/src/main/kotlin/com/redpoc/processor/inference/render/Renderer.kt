package com.redpoc.processor.inference.render

import com.redpoc.processor.model.Detection

/**
 * Draw detections onto a source JPEG and return annotated JPEG bytes.
 *
 * Implementations are expected to be pure — same input always produces
 * byte-identical output (no timestamps, no random colors) so that Grafana
 * image snapshot comparisons are stable.
 */
interface Renderer {
    fun render(
        jpegBytes: ByteArray,
        detections: List<Detection>,
        jpegQuality: Int = 85,
        /** Optional multi-line overlay drawn in the top-right corner. */
        overlay: List<String> = emptyList(),
    ): ByteArray
}
