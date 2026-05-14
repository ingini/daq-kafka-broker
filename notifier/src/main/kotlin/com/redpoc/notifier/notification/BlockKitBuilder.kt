package com.redpoc.notifier.notification

import com.redpoc.notifier.model.DetectionEvent
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

/**
 * Builds a Slack Block Kit payload for a detection event.
 * Output shape follows Slack Web API chat.postMessage `blocks` contract.
 *
 * env `SLACK_INCLUDE_IMAGES` 로 image block 토글 가능.
 *   false / no / 0 → image block skip (POC 단계 self-signed cert 환경)
 *   기본 true → image block 포함 (운영 cert 환경)
 *
 * self-signed cert + 외부 도달 불가 환경에서 Slack 의 image_url validation
 * 이 실패 (`invalid_blocks`) → false 로 설정하면 text + link 만 발송.
 */
class BlockKitBuilder(
    private val resolver: PresignedUrlResolver?,
) {
    private val includeImages: Boolean =
        System.getenv("SLACK_INCLUDE_IMAGES")?.lowercase()
            ?.let { it !in setOf("false", "no", "0") } ?: true

    fun build(event: DetectionEvent): List<Map<String, Any>> {
        val det = event.detections.firstOrNull()
        val cls = det?.className ?: "(unknown)"
        val conf = det?.confidence?.let { "%.2f".format(it) } ?: "-"
        val when_ = Instant.ofEpochSecond(0, event.captureTsNs)
            .atZone(ZoneId.of("Asia/Seoul"))
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z"))

        val header = mapOf(
            "type" to "header",
            "text" to mapOf(
                "type" to "plain_text",
                "text" to "🚧 $cls (conf=$conf)",
                "emoji" to true,
            )
        )
        val context = mapOf(
            "type" to "section",
            "fields" to listOf(
                mdField("*차량*", event.vehicleId),
                mdField("*시각*", when_),
                mdField("*이벤트 ID*", "`${event.eventId}`"),
                mdField("*번들 ID*", "`${event.bundleId}`"),
            )
        )

        if (!includeImages) {
            // POC 단계: image block 제외. 대신 URL 만 text 로 추가 (사용자 클릭).
            val urls = event.imageUris.entries.joinToString("\n") { (sensor, uri) ->
                val url = resolver?.presign(uri) ?: uri
                "*$sensor* — <$url|view>"
            }
            val urlSection = mapOf(
                "type" to "section",
                "text" to mapOf("type" to "mrkdwn", "text" to urls),
            )
            return listOf(header, context, urlSection)
        }

        val imageBlocks = event.imageUris.entries.map { (sensor, uri) ->
            mapOf(
                "type" to "image",
                "title" to mapOf("type" to "plain_text", "text" to sensor),
                "image_url" to (resolver?.presign(uri) ?: uri),
                "alt_text" to sensor,
            )
        }
        return listOf(header, context) + imageBlocks
    }

    private fun mdField(label: String, value: String) = mapOf(
        "type" to "mrkdwn",
        "text" to "$label\n$value",
    )
}
