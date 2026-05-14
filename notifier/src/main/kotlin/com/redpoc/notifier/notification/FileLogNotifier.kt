package com.redpoc.notifier.notification

import com.fasterxml.jackson.databind.ObjectMapper
import com.redpoc.notifier.model.DetectionEvent
import org.slf4j.LoggerFactory
import java.io.BufferedWriter
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicLong

/**
 * Appends one JSON-line per detection event to a file that is bind-
 * mounted from the host, so the operator can run
 *     tail -f logs/notifier.jsonl | jq .
 * without opening the container.
 *
 * Each line is a single JSON object; this keeps the log
 *   - machine-parseable (jq / splunk / elk friendly)
 *   - human-scannable (every line starts with an ISO timestamp)
 *   - atomic (the POSIX guarantee for append-writes < PIPE_BUF bytes
 *     holds for our typical event size of ~500 B).
 *
 * Thread-safe: all writes are serialised through a single synchronized
 * writer. For POC single-consumer-thread this is effectively lock-free.
 */
class FileLogNotifier(
    private val path: String,
    private val mapper: ObjectMapper,
) : Notifier {
    private val log = LoggerFactory.getLogger(javaClass)
    private val lineCount = AtomicLong(0)
    private val writer: BufferedWriter

    init {
        val parent = Paths.get(path).parent
        if (parent != null) Files.createDirectories(parent)
        writer = BufferedWriter(FileWriter(path, /* append = */ true))
        log.info("file_log notifier writing to {}", path)
    }

    override fun notify(event: DetectionEvent): NotificationResult {
        val sentAtIso = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
        val captureTimeIso = Instant.ofEpochSecond(0, event.captureTsNs)
            .atZone(ZoneId.of("Asia/Seoul"))
            .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

        val record = linkedMapOf<String, Any?>(
            "sent_at" to sentAtIso,
            "event_id" to event.eventId,
            "vehicle_id" to event.vehicleId,
            "bundle_id" to event.bundleId,
            "capture_ts_ns" to event.captureTsNs,
            "capture_time" to captureTimeIso,
            "detections" to event.detections.map {
                mapOf(
                    "class_id" to it.classId,
                    "class_name" to it.className,
                    "confidence" to it.confidence,
                )
            },
            "image_uris" to event.imageUris,
            "annotated_uris" to event.annotatedUris,
            "gps" to event.gps?.let {
                mapOf(
                    "lat" to it.lat,
                    "lon" to it.lon,
                    "alt" to it.alt,
                    "time_offset_ms" to (it.timeOffsetNs / 1_000_000),
                )
            },
            "text" to buildHumanText(event, sentAtIso, captureTimeIso),
        )
        val json = mapper.writeValueAsString(record)

        val lineNo = synchronized(writer) {
            writer.write(json)
            writer.newLine()
            writer.flush()
            lineCount.incrementAndGet()
        }

        return NotificationResult(
            channel = "file_log",
            messageText = record["text"] as String,
            messageJson = json,
            providerRef = "$path:$lineNo",
            status = "SENT",
        )
    }

    override fun close() {
        synchronized(writer) { writer.close() }
    }

    private fun buildHumanText(event: DetectionEvent, sentAt: String, captureTime: String): String {
        val top = event.detections.firstOrNull()
        val cls = top?.className ?: "(no detections)"
        val conf = top?.confidence?.let { "%.2f".format(it) } ?: "-"
        val nDet = event.detections.size
        val gpsStr = event.gps?.let { " gps=(%.6f, %.6f, %.1fm)".format(it.lat, it.lon, it.alt) } ?: ""
        return buildString {
            append("[sent $sentAt] vehicle=${event.vehicleId} class=$cls conf=$conf")
            append(" detections=$nDet$gpsStr")
            append(" capture=$captureTime event=${event.eventId}")
        }
    }
}
