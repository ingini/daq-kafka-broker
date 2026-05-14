package com.redpoc.notifier.persistence

import com.redpoc.notifier.notification.NotificationResult
import java.sql.DriverManager
import java.sql.SQLException

class PostgresSentMessagesRepo(
    private val url: String,
    private val user: String,
    private val password: String,
) : SentMessagesRepo {

    override fun record(eventId: String, result: NotificationResult) {
        val sql = """
            INSERT INTO sent_messages (
                event_id, channel, message_text, message_json, provider_ref, status
            )
            VALUES (?, ?, ?, ?::jsonb, ?, ?)
        """.trimIndent()
        connect().use { conn ->
            conn.prepareStatement(sql).use { ps ->
                ps.setString(1, eventId)
                ps.setString(2, result.channel)
                ps.setString(3, result.messageText)
                if (result.messageJson != null) {
                    ps.setString(4, result.messageJson)
                } else {
                    ps.setNull(4, java.sql.Types.OTHER)
                }
                if (result.providerRef != null) {
                    ps.setString(5, result.providerRef)
                } else {
                    ps.setNull(5, java.sql.Types.VARCHAR)
                }
                ps.setString(6, result.status)
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
