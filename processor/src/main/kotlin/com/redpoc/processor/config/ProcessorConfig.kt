package com.redpoc.processor.config

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.nio.file.Files
import java.nio.file.Paths

data class KafkaConfig(
    @JsonProperty("bootstrap_servers") val bootstrapServers: String,
    @JsonProperty("application_id") val applicationId: String,
    @JsonProperty("input_topic") val inputTopic: String,
    @JsonProperty("output_topic") val outputTopic: String,
    @JsonProperty("dlq_topic") val dlqTopic: String,
)

data class TritonConfig(
    val url: String,
    @JsonProperty("model_name") val modelName: String,
    @JsonProperty("confidence_threshold") val confidenceThreshold: Double,
    @JsonProperty("timeout_ms") val timeoutMs: Long = 5000,
)

data class InferenceConfig(
    val kind: String,
    val triton: TritonConfig?,
    /**
     * If non-empty, only detections whose className is in this list are
     * kept. Empty / null = accept all classes (standard COCO 80).
     */
    @JsonProperty("allowed_classes") val allowedClasses: List<String>? = null,
)

data class VisualizeConfig(
    val enabled: Boolean = false,
    /** JPEG output quality 1–100 for the annotated image. */
    @JsonProperty("jpeg_quality") val jpegQuality: Int = 85,
    /**
     * Key suffix appended after the base (without `.jpg`) name.
     *   `.annotated.jpg`  → `foo.annotated.jpg`        (flat, default legacy)
     *   `_annotated.jpg`  → `foo_annotated.jpg`        (paired with `subdir`)
     */
    @JsonProperty("key_suffix") val keySuffix: String = ".annotated.jpg",
    /**
     * Optional subdirectory inserted before the filename in the annotated
     * object key. Empty = same directory as source (legacy behavior).
     *
     * Examples (source = `2026/05/11/09/VID_001/cam0/20260511T091634_ns.jpg`):
     *   subdir=""        + suffix=".annotated.jpg"  →  .../cam0/20260511T091634_ns.annotated.jpg
     *   subdir="result"  + suffix="_annotated.jpg"  →  .../cam0/result/20260511T091634_ns_annotated.jpg
     */
    val subdir: String = "",
)

data class S3Config(
    val endpoint: String,
    @JsonProperty("access_key") val accessKey: String,
    @JsonProperty("secret_key") val secretKey: String,
    val region: String,
)

data class MockConfig(
    @JsonProperty("emit_every_n") val emitEveryN: Int,
    @JsonProperty("detection_class") val detectionClass: String,
    val confidence: Double,
)

data class MetricsConfig(val port: Int)

data class GpsConfig(
    /**
     * Source implementation. "file" (default) uses the offline JSONL
     * produced by scripts/extract-imu-gps.py. "kafka" consumes live
     * samples off the `raw.gps` topic populated by mqtt-kafka-bridge.
     * Empty or null disables GPS enrichment entirely.
     */
    val kind: String = "file",

    /** Path (container-local) to JSONL produced by scripts/extract-imu-gps.py. */
    @JsonProperty("file_path") val filePath: String? = null,

    /** Max ms between (wrapped) query ts and nearest GPS fix. */
    @JsonProperty("tolerance_ms") val toleranceMs: Long = 60_000,

    /**
     * POC replay flag. When true, FileGpsSource wraps query ts into the
     * data span (modulo) before matching — essential when edge-agent
     * stamps bundles with live wall-clock while the pcap GPS records
     * carry their original capture date. Ignored for kind=kafka.
     */
    @JsonProperty("replay_wrap") val replayWrap: Boolean = false,

    /**
     * Live consumer knobs — only read when kind=kafka.
     */
    val topic: String = "raw.gps",
    @JsonProperty("group_id") val groupId: String = "processor-gps",
    @JsonProperty("max_fixes") val maxFixes: Int = 18_000,
)

data class ProcessorConfig(
    val kafka: KafkaConfig,
    val inference: InferenceConfig,
    val s3: S3Config?,
    val mock: MockConfig,
    val metrics: MetricsConfig?,
    val visualize: VisualizeConfig = VisualizeConfig(),
    val gps: GpsConfig? = null,
    @JsonProperty("class_names") val classNames: List<String>?,
) {
    companion object {
        fun load(path: String): ProcessorConfig {
            val yamlFactory = YAMLFactory()
            val mapper: ObjectMapper = jacksonObjectMapper().apply {
                configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            }
            val tree = ObjectMapper(yamlFactory).readTree(Files.newBufferedReader(Paths.get(path)))
            return mapper.treeToValue(tree, ProcessorConfig::class.java)
        }
    }
}
