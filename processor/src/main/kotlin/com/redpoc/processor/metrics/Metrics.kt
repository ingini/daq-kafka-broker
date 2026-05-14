package com.redpoc.processor.metrics

import io.prometheus.client.Counter
import io.prometheus.client.Histogram

/**
 * Centralised Prometheus collectors. Feature packages import the fields
 * directly — they don't need to know about the registry.
 *
 * Keep label cardinality low: `class_name` and `stage` are both bounded
 * sets (≤80 COCO classes, ≤4 pipeline stages).
 */
object Metrics {
    val bundlesConsumed: Counter = Counter.build()
        .name("processor_bundles_consumed_total")
        .help("FrameBundle messages consumed from input topic.")
        .register()

    val inferenceDuration: Histogram = Histogram.build()
        .name("processor_inference_duration_seconds")
        .help("End-to-end time for one bundle's inference (all sensors).")
        .buckets(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
        .register()

    val inferenceErrors: Counter = Counter.build()
        .name("processor_inference_errors_total")
        .labelNames("stage")
        .help("Inference pipeline errors by stage (fetch / preprocess / infer / parse).")
        .register()

    val detectionsProduced: Counter = Counter.build()
        .name("processor_detections_produced_total")
        .labelNames("class_name")
        .help("Detections emitted to events.detection, by class.")
        .register()

    val eventsPublished: Counter = Counter.build()
        .name("processor_events_published_total")
        .help("DetectionEvent messages pushed to output topic.")
        .register()

    val annotationsProduced: Counter = Counter.build()
        .name("processor_annotations_produced_total")
        .help("Annotated (bbox-drawn) JPEG objects uploaded to MinIO.")
        .register()

    val annotationErrors: Counter = Counter.build()
        .name("processor_annotation_errors_total")
        .labelNames("stage")  // fetch | render | upload
        .help("Annotation pipeline errors by stage.")
        .register()
}
