package com.redpoc.notifier.persistence

import com.fasterxml.jackson.databind.ObjectMapper
import com.redpoc.notifier.model.DetectionEvent
import java.sql.DriverManager
import java.sql.SQLException
import javax.sql.DataSource

class PostgresEventsRepo(
    private val url: String,
    private val user: String,
    private val password: String,
    private val mapper: ObjectMapper,
) : EventsRepo {

    override fun insertIfAbsent(event: DetectionEvent): Boolean {
        val sql = """
            INSERT INTO events (
                event_id, vehicle_id, bundle_id, capture_ts_ns,
                detection_class, confidence, image_uris, notify_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, 'PENDING')
            ON CONFLICT (event_id) DO NOTHING
        """.trimIndent()
        connect().use { conn ->
            conn.prepareStatement(sql).use { ps ->
                val first = event.detections.firstOrNull()
                ps.setString(1, event.eventId)
                ps.setString(2, event.vehicleId)
                ps.setString(3, event.bundleId)
                ps.setLong(4, event.captureTsNs)
                ps.setString(5, first?.className)
                if (first != null) ps.setDouble(6, first.confidence) else ps.setNull(6, java.sql.Types.DOUBLE)
                ps.setString(7, mapper.writeValueAsString(event.imageUris))
                return ps.executeUpdate() == 1
            }
        }
    }

    override fun markNotified(eventId: String, slackTs: String?) {
        val sql = """
            UPDATE events
               SET notify_status = 'SENT',
                   slack_message_ts = ?,
                   updated_at = now()
             WHERE event_id = ?
        """.trimIndent()
        connect().use { conn ->
            conn.prepareStatement(sql).use { ps ->
                ps.setString(1, slackTs)
                ps.setString(2, eventId)
                ps.executeUpdate()
            }
        }
    }

    private fun connect() = try {
        DriverManager.getConnection(url, user, password)
    } catch (e: SQLException) {
        throw IllegalStateException("postgres connect failed: ${e.message}", e)
    }
}
