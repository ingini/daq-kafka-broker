package com.redpoc.notifier.pipeline

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.redpoc.notifier.config.NotifierConfig
import com.redpoc.notifier.consumer.EventsConsumer
import com.redpoc.notifier.notification.BlockKitBuilder
import com.redpoc.notifier.notification.FileLogNotifier
import com.redpoc.notifier.notification.NoOpNotifier
import com.redpoc.notifier.notification.Notifier
import com.redpoc.notifier.notification.PresignedUrlResolver
import com.redpoc.notifier.notification.SlackNotifier
import com.redpoc.notifier.persistence.EventsRepo
import com.redpoc.notifier.persistence.PostgresEventsRepo
import com.redpoc.notifier.persistence.PostgresSentMessagesRepo
import com.redpoc.notifier.persistence.SentMessagesRepo
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import org.slf4j.LoggerFactory
import java.time.Duration

class Pipeline(private val cfg: NotifierConfig) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val mapper: ObjectMapper = jacksonObjectMapper()

    fun run() {
        val metrics = startMetrics()
        try {
            val events = buildEventsRepo()
            val messages = buildMessagesRepo()
            val notifier = buildNotifier()
            EventsConsumer(cfg.kafka, events, messages, notifier, mapper).run()
        } finally {
            metrics?.close()
        }
    }

    private fun startMetrics(): HTTPServer? {
        val port = cfg.metrics?.port ?: return null
        DefaultExports.initialize()
        // Touch Metrics singleton so counters register before scraping.
        com.redpoc.notifier.metrics.Metrics.eventsReceived
        val server = HTTPServer.Builder().withPort(port).build()
        log.info("metrics on :{}", port)
        return server
    }

    private fun buildEventsRepo(): EventsRepo = PostgresEventsRepo(
        url = cfg.postgres.url,
        user = cfg.postgres.user,
        password = cfg.postgres.password,
        mapper = mapper,
    )

    private fun buildMessagesRepo(): SentMessagesRepo = PostgresSentMessagesRepo(
        url = cfg.postgres.url,
        user = cfg.postgres.user,
        password = cfg.postgres.password,
    )

    private fun buildNotifier(): Notifier = when (cfg.notification.kind) {
        "", "noop" -> NoOpNotifier()
        "file_log" -> {
            val f = cfg.notification.fileLog
                ?: error("notification.file_log section required for kind=file_log")
            FileLogNotifier(path = f.path, mapper = mapper)
        }
        "slack" -> {
            val s = cfg.notification.slack ?: error("slack section required for kind=slack")
            require(s.token.isNotBlank()) { "slack.token is empty" }
            val resolver = cfg.s3?.let {
                PresignedUrlResolver(
                    endpoint = it.endpoint,
                    accessKey = it.accessKey,
                    secretKey = it.secretKey,
                    region = it.region,
                    ttl = Duration.ofHours(it.presignTtlHours),
                )
            }
            SlackNotifier(
                token = s.token,
                channel = s.channel,
                mapper = mapper,
                blockKit = BlockKitBuilder(resolver),
                resolver = resolver,
            )
        }
        else -> error("unknown notification kind: ${cfg.notification.kind}")
    }
}
