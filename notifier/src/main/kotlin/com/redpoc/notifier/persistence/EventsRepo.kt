package com.redpoc.notifier.persistence

import com.redpoc.notifier.model.DetectionEvent

/**
 * Abstraction over event metadata storage. A test stub or a different
 * backend (Spanner, Dynamo, etc.) is plugged in without touching the
 * Kafka consumer code (DIP).
 */
interface EventsRepo {
    /**
     * Insert the event; returns true if a row was actually inserted,
     * false if the event_id already existed (idempotent).
     */
    fun insertIfAbsent(event: DetectionEvent): Boolean

    fun markNotified(eventId: String, slackTs: String?)

    fun close() {}
}
