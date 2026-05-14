package com.redpoc.notifier.notification

import com.fasterxml.jackson.databind.ObjectMapper
import com.redpoc.notifier.model.DetectionEvent
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

/**
 * Posts to Slack chat.postMessage with Block Kit payload. Retained so
 * flipping `notification.kind: slack` in config remains a one-line
 * change. Returns a NotificationResult whether or not the post succeeded.
 */
class SlackNotifier(
    private val token: String,
    private val channel: String,
    private val mapper: ObjectMapper,
    private val blockKit: BlockKitBuilder,
    private val resolver: PresignedUrlResolver? = null,
) : Notifier {
    private val log = LoggerFactory.getLogger(javaClass)
    private val http = OkHttpClient.Builder()
        .connectTimeout(Duration.ofSeconds(5))
        .readTimeout(Duration.ofSeconds(10))
        .build()

    override fun notify(event: DetectionEvent): NotificationResult {
        val fallback = fallbackText(event)
        val payload = mapOf(
            "channel" to channel,
            "text" to fallback,
            "blocks" to blockKit.build(event),
        )
        val bodyJson = mapper.writeValueAsString(payload)
        val req = Request.Builder()
            .url("https://slack.com/api/chat.postMessage")
            .header("Authorization", "Bearer $token")
            .post(bodyJson.toRequestBody("application/json; charset=utf-8".toMediaType()))
            .build()
        http.newCall(req).execute().use { resp ->
            val respBody = resp.body?.string().orEmpty()
            if (!resp.isSuccessful) {
                log.warn("slack post failed http={} body={}", resp.code, respBody)
                return NotificationResult("slack", fallback, bodyJson, null, "FAILED")
            }
            val tree = mapper.readTree(respBody)
            if (tree?.get("ok")?.asBoolean() != true) {
                log.warn("slack API error: {}", tree?.get("error")?.asText())
                return NotificationResult("slack", fallback, bodyJson, null, "FAILED")
            }
            val ts = tree.get("ts")?.asText()
            return NotificationResult("slack", fallback, bodyJson, ts, "SENT")
        }
    }

    override fun close() { resolver?.close() }

    private fun fallbackText(event: DetectionEvent): String {
        val dt = Instant.ofEpochSecond(0, event.captureTsNs)
            .atZone(ZoneId.of("Asia/Seoul"))
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        val det = event.detections.firstOrNull()
        val cls = det?.className ?: "(unknown)"
        val conf = det?.confidence?.let { "%.2f".format(it) } ?: "-"
        return "[$cls] conf=$conf · $dt · ${event.vehicleId}"
    }
}
