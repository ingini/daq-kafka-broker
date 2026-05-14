package com.redpoc.notifier.notification

import com.redpoc.notifier.model.DetectionEvent

/**
 * Outbound alerting channel abstraction. Adding email, webhook, or a new
 * chat provider is a new class; the consumer loop stays untouched.
 *
 * Returning a structured [NotificationResult] lets the consumer persist
 * the exact body sent (for audit / post-hoc analysis) without the
 * notifier needing to know about a database.
 */
interface Notifier {
    fun notify(event: DetectionEvent): NotificationResult

    fun close() {}
}
