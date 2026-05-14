package com.redpoc.processor.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

@JsonIgnoreProperties(ignoreUnknown = true)
data class FrameBundleMeta(
    @JsonProperty("bundle_id") val bundleId: String,
    @JsonProperty("vehicle_id") val vehicleId: String,
    @JsonProperty("capture_ts_ns") val captureTsNs: Long,
    val images: Map<String, String>,
    @JsonProperty("images_decoded") val imagesDecoded: Map<String, String>? = null,
) {
    /** Prefer decoded JPEGs when the transcoder has produced them. */
    fun inferenceImages(): Map<String, String> = imagesDecoded ?: images
}

data class Detection(
    @JsonProperty("class_id") val classId: Int,
    @JsonProperty("class_name") val className: String,
    val confidence: Double,
    val bbox: List<Double>? = null,
    /** Which sensor produced this detection. null = unknown (e.g. mock). */
    val sensor: String? = null,
)

data class GpsFix(
    val lat: Double,
    val lon: Double,
    val alt: Double,
    /** Nanoseconds between the bundle capture and the matched GPS fix. */
    @JsonProperty("time_offset_ns") val timeOffsetNs: Long,
)

data class DetectionEvent(
    @JsonProperty("event_id") val eventId: String,
    @JsonProperty("bundle_id") val bundleId: String,
    @JsonProperty("vehicle_id") val vehicleId: String,
    @JsonProperty("capture_ts_ns") val captureTsNs: Long,
    val detections: List<Detection>,
    @JsonProperty("image_uris") val imageUris: Map<String, String>,
    /** Annotated JPEG URIs (with bbox drawn). null when visualize disabled. */
    @JsonProperty("annotated_uris") val annotatedUris: Map<String, String>? = null,
    /** Time-correlated GPS fix. null when no GPS source or fix out of tolerance. */
    val gps: GpsFix? = null,
)
