package com.redpoc.notifier.persistence

import com.redpoc.notifier.notification.NotificationResult

/**
 * Abstraction over the sent_messages audit table. A test stub backed by
 * a List<*> lets consumer-loop tests assert outbound calls without
 * standing up Postgres.
 */
interface SentMessagesRepo {
    fun record(eventId: String, result: NotificationResult)

    fun close() {}
}
