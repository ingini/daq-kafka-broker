package com.redpoc.notifier.notification

import com.redpoc.notifier.model.DetectionEvent
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.format.DateTimeFormatter

class NoOpNotifier : Notifier {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun notify(event: DetectionEvent): NotificationResult {
        val top = event.detections.firstOrNull()
        val text = "noop notify event_id=${event.eventId} vehicle=${event.vehicleId} " +
            "class=${top?.className} confidence=${top?.confidence}"
        log.info(text)
        return NotificationResult(
            channel = "noop",
            messageText = text,
            providerRef = "noop-${DateTimeFormatter.ISO_INSTANT.format(Instant.now())}",
            status = "SENT",
        )
    }
}
