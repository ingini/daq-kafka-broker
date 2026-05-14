package com.redpoc.notifier.metrics

import io.prometheus.client.Counter
import io.prometheus.client.Histogram

/**
 * Prometheus collectors shared across consumer / notifier / repo layers.
 * Kept as a top-level object so feature packages import the variables
 * directly rather than a registry instance.
 */
object Metrics {
    val eventsReceived: Counter = Counter.build()
        .name("notifier_events_received_total")
        .help("DetectionEvent messages consumed from Kafka.")
        .register()

    val eventsPersisted: Counter = Counter.build()
        .name("notifier_events_persisted_total")
        .labelNames("result")  // "inserted" | "duplicate"
        .help("Outcome of ON CONFLICT INSERT into events table.")
        .register()

    val notifyDuration: Histogram = Histogram.build()
        .name("notifier_notify_duration_seconds")
        .labelNames("channel")
        .help("Time spent inside Notifier.notify per channel.")
        .buckets(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0)
        .register()

    val sentMessagesPersisted: Counter = Counter.build()
        .name("notifier_sent_messages_persisted_total")
        .labelNames("channel", "status")  // SENT | FAILED
        .help("sent_messages rows inserted, grouped by channel + delivery status.")
        .register()

    val pipelineErrors: Counter = Counter.build()
        .name("notifier_pipeline_errors_total")
        .labelNames("stage")  // deser | db_insert | notify | mark_notified | sent_messages
        .help("Consumer loop errors by stage.")
        .register()
}
