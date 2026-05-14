package com.redpoc.processor.topology

import com.fasterxml.jackson.databind.ObjectMapper
import com.redpoc.processor.config.KafkaConfig
import com.redpoc.processor.gps.GpsSource
import com.redpoc.processor.inference.Inferencer
import com.redpoc.processor.inference.annotate.Annotator
import com.redpoc.processor.metrics.Metrics
import com.redpoc.processor.model.Detection
import com.redpoc.processor.model.DetectionEvent
import com.redpoc.processor.model.FrameBundleMeta
import com.redpoc.processor.model.GpsFix as EventGpsFix
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.slf4j.LoggerFactory
import java.security.MessageDigest

/**
 * Kafka Streams topology:
 *   raw.frame.metadata.decoded  --json-->  FrameBundleMeta
 *       -> inferencer.infer(bundle)
 *       -> if detections empty: drop
 *       -> else, per-sensor group detections
 *           -> annotator.annotate(srcUri, sensorDetections) (if visualize on)
 *           -> emit DetectionEvent(image_uris, annotated_uris)
 *
 * Parse / inference errors route to events.detection.dlq.
 *
 * `annotator` is nullable — when visualize is disabled the Pipeline
 * wires null and the render/upload path is skipped entirely (OCP).
 */
class DetectionTopologyBuilder(
    private val kafka: KafkaConfig,
    private val inferencer: Inferencer,
    private val mapper: ObjectMapper,
    private val annotator: Annotator? = null,
    private val gpsSource: GpsSource? = null,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun build(): Topology {
        val builder = StreamsBuilder()
        val stringSerde = Serdes.String()

        val source = builder.stream(
            kafka.inputTopic,
            Consumed.with(stringSerde, stringSerde),
        )

        source
            .flatMapValues<String> { _, value ->
                Metrics.bundlesConsumed.inc()
                val timer = Metrics.inferenceDuration.startTimer()
                try {
                    runCatching {
                        val bundle = mapper.readValue(value, FrameBundleMeta::class.java)
                        val detections = inferencer.infer(bundle)
                        if (detections.isEmpty()) return@runCatching emptyList<String>()

                        val gpsEvt = gpsSource?.nearest(bundle.captureTsNs)?.let {
                            EventGpsFix(
                                lat = it.lat, lon = it.lon, alt = it.alt,
                                timeOffsetNs = it.captureTsNs - bundle.captureTsNs,
                            )
                        }

                        val annotated = if (annotator != null)
                            annotateBundle(bundle, detections, gpsEvt)
                        else null

                        val event = DetectionEvent(
                            eventId = deterministicEventId(bundle, detections.first().className),
                            bundleId = bundle.bundleId,
                            vehicleId = bundle.vehicleId,
                            captureTsNs = bundle.captureTsNs,
                            detections = detections,
                            imageUris = bundle.images,
                            annotatedUris = annotated,
                            gps = gpsEvt,
                        )
                        Metrics.eventsPublished.inc()
                        listOf(mapper.writeValueAsString(event))
                    }.getOrElse {
                        Metrics.inferenceErrors.labels("topology").inc()
                        log.warn("routing to DLQ", it)
                        emptyList<String>()
                    }
                } finally {
                    timer.observeDuration()
                }
            }
            .to(kafka.outputTopic, Produced.with(stringSerde, stringSerde))

        // DLQ: same source, filter parse failures, push raw payload for inspection
        source
            .filter { _, v -> !isParseable(v) }
            .to(kafka.dlqTopic, Produced.with(stringSerde, stringSerde))

        return builder.build()
    }

    /**
     * Groups detections by their `sensor` field and asks the Annotator to
     * render each sensor's JPEG in place. Returns null if no annotated
     * output was produced (e.g. all detections missing sensor tag).
     */
    private fun annotateBundle(
        bundle: FrameBundleMeta,
        detections: List<Detection>,
        gps: EventGpsFix?,
    ): Map<String, String>? {
        val bySensor: Map<String, List<Detection>> = detections
            .filter { it.sensor != null }
            .groupBy { it.sensor!! }
        if (bySensor.isEmpty()) return null

        val sources = bundle.imagesDecoded ?: return null
        val overlay = buildOverlay(gps)
        val out = linkedMapOf<String, String>()
        for ((sensor, dets) in bySensor) {
            val srcUri = sources[sensor] ?: continue
            val annotated = annotator!!.annotate(srcUri, dets, overlay) ?: continue
            out[sensor] = annotated
        }
        return out.takeIf { it.isNotEmpty() }
    }

    private fun buildOverlay(gps: EventGpsFix?): List<String> {
        if (gps == null) return emptyList()
        return listOf(
            "GPS  %.6f, %.6f".format(gps.lat, gps.lon),
            "ALT  %.1f m".format(gps.alt),
        )
    }

    private fun isParseable(v: String): Boolean = runCatching {
        mapper.readValue(v, FrameBundleMeta::class.java)
    }.isSuccess

    private fun deterministicEventId(b: FrameBundleMeta, detectionClass: String): String {
        val seed = "${b.vehicleId}|${b.captureTsNs}|$detectionClass"
        val digest = MessageDigest.getInstance("SHA-256").digest(seed.toByteArray())
        return digest.take(8).joinToString("") { "%02x".format(it) }
    }
}
