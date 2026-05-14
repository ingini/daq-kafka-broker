package com.redpoc.notifier.notification

/**
 * Outcome of a single notify attempt. Carries enough information that the
 * consumer can both (a) mark the event delivered and (b) persist an audit
 * record in sent_messages without re-deriving the message body.
 *
 * A failed attempt is expressed as `providerRef = null, status = FAILED`;
 * the consumer still records it so the operator can later inspect why.
 */
data class NotificationResult(
    /** Free-form channel name persisted alongside the record. */
    val channel: String,
    /** Human-readable body as actually sent / logged. */
    val messageText: String,
    /** Optional structured form (e.g. Slack Block Kit, raw JSON). */
    val messageJson: String? = null,
    /** Provider-assigned reference (Slack ts, file path, …). null on failure. */
    val providerRef: String? = null,
    /** "SENT" or "FAILED". */
    val status: String = "SENT",
) {
    val succeeded: Boolean get() = status == "SENT" && providerRef != null
}
