package com.redpoc.notifier.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

@JsonIgnoreProperties(ignoreUnknown = true)
data class Detection(
    @JsonProperty("class_id") val classId: Int,
    @JsonProperty("class_name") val className: String,
    val confidence: Double,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class GpsFix(
    val lat: Double,
    val lon: Double,
    val alt: Double,
    @JsonProperty("time_offset_ns") val timeOffsetNs: Long,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class DetectionEvent(
    @JsonProperty("event_id") val eventId: String,
    @JsonProperty("bundle_id") val bundleId: String,
    @JsonProperty("vehicle_id") val vehicleId: String,
    @JsonProperty("capture_ts_ns") val captureTsNs: Long,
    val detections: List<Detection>,
    @JsonProperty("image_uris") val imageUris: Map<String, String>,
    @JsonProperty("annotated_uris") val annotatedUris: Map<String, String>? = null,
    val gps: GpsFix? = null,
)
