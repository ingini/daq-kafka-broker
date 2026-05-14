package com.redpoc.notifier.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.redpoc.notifier.config.KafkaConfig
import com.redpoc.notifier.metrics.Metrics
import com.redpoc.notifier.model.DetectionEvent
import com.redpoc.notifier.notification.Notifier
import com.redpoc.notifier.persistence.EventsRepo
import com.redpoc.notifier.persistence.SentMessagesRepo
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties

/**
 * Kafka consumer loop. Depends only on interfaces (EventsRepo, Notifier,
 * SentMessagesRepo) — the concrete storage / channel are chosen at
 * wiring time (DIP).
 *
 * Per-event flow:
 *   1. Insert into events (idempotent on event_id)
 *   2. If this was a fresh insert, call Notifier.notify
 *   3. Record the exact NotificationResult into sent_messages
 *      (success *or* failure — useful for audit)
 *   4. On SENT, update events.notify_status to 'SENT'
 */
class EventsConsumer(
    private val kafka: KafkaConfig,
    private val events: EventsRepo,
    private val messages: SentMessagesRepo,
    private val notifier: Notifier,
    private val mapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun run() {
        val consumer = KafkaConsumer<String, String>(consumerProps())
        consumer.subscribe(listOf(kafka.inputTopic))
        log.info("consumer subscribed topic={} group={}", kafka.inputTopic, kafka.groupId)

        Runtime.getRuntime().addShutdownHook(Thread {
            log.info("shutdown; closing consumer")
            consumer.wakeup()
        })

        try {
            while (true) {
                val records = consumer.poll(Duration.ofMillis(500))
                for (r in records) {
                    handle(r.value())
                }
                if (!records.isEmpty) consumer.commitAsync()
            }
        } catch (_: org.apache.kafka.common.errors.WakeupException) {
            log.info("consumer woken up")
        } finally {
            consumer.close(Duration.ofSeconds(5))
            notifier.close()
            events.close()
            messages.close()
        }
    }

    private fun handle(value: String) {
        Metrics.eventsReceived.inc()
        val event = runCatching { mapper.readValue(value, DetectionEvent::class.java) }
            .getOrElse {
                Metrics.pipelineErrors.labels("deser").inc()
                log.warn("malformed event payload; skipping", it)
                return
            }
        val inserted = try {
            events.insertIfAbsent(event)
        } catch (e: Exception) {
            Metrics.pipelineErrors.labels("db_insert").inc()
            log.warn("insert failed event_id={}", event.eventId, e)
            return
        }
        Metrics.eventsPersisted.labels(if (inserted) "inserted" else "duplicate").inc()
        if (!inserted) {
            log.debug("event_id={} already present; skipping notify", event.eventId)
            return
        }

        val timer = Metrics.notifyDuration.labels(channelFor(notifier)).startTimer()
        val result = try {
            runCatching { notifier.notify(event) }
                .getOrElse {
                    Metrics.pipelineErrors.labels("notify").inc()
                    log.warn("notify failed event_id={}", event.eventId, it)
                    return
                }
        } finally {
            timer.observeDuration()
        }

        Metrics.sentMessagesPersisted.labels(result.channel, result.status).inc()
        runCatching { messages.record(event.eventId, result) }
            .onFailure {
                Metrics.pipelineErrors.labels("sent_messages").inc()
                log.warn("sent_messages insert failed event_id={}", event.eventId, it)
            }

        if (result.succeeded) {
            runCatching { events.markNotified(event.eventId, result.providerRef) }
                .onFailure {
                    Metrics.pipelineErrors.labels("mark_notified").inc()
                    log.warn("markNotified failed event_id={}", event.eventId, it)
                }
        }
    }

    // Label cardinality stays bounded: {noop, file_log, slack}.
    private fun channelFor(n: Notifier): String = n.javaClass.simpleName
        .removeSuffix("Notifier")
        .lowercase()

    private fun consumerProps(): Properties = Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
        put(ConsumerConfig.GROUP_ID_CONFIG, kafka.groupId)
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50)
    }
}
