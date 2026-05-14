package com.redpoc.processor.inference.annotate

import com.redpoc.processor.model.Detection

/**
 * Given a source image URI and a detection list, produce an annotated
 * image and return its URI. Returns null when annotation isn't meaningful
 * (empty detection list, fetch/render failure) so the caller can simply
 * omit the annotated URI from the event.
 */
interface Annotator {
    fun annotate(
        sourceUri: String,
        detections: List<Detection>,
        /** Optional top-right overlay lines (GPS, timestamp, etc.). */
        overlay: List<String> = emptyList(),
    ): String?
    fun close() {}
}
