package com.redpoc.processor.gps

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.thread
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Live GPS source that subscribes to the `raw.gps` Kafka topic populated
 * by mqtt-kafka-bridge. The edge-agent publishes one JSON message per
 * BynavX1 INSPVA frame to `vehicle/<id>/gps`; the bridge forwards those
 * onto Kafka, and this consumer loads them into a bounded ring buffer
 * that supports O(log n) nearest-ts lookup.
 *
 * Wire format (one JSON object per message):
 *   {"capture_ts_ns": <int64>, "lat": <double>, "lon": <double>, "alt": <double>}
 *
 * The ring buffer is bounded by [maxFixes]; when full, the oldest fix
 * is evicted. At 10 Hz × 30 min window, 18 000 fixes comfortably fits
 * in a few MB.
 *
 * Thread safety: [nearest] may be called from any Streams thread. The
 * consumer thread is owned by this class and joined on [close].
 */
class KafkaGpsSource(
    bootstrapServers: String,
    topic: String,
    groupId: String,
    /** Max fixes retained. 18 000 ≈ 30 minutes at 10 Hz. */
    private val maxFixes: Int = 18_000,
    /**
     * Max age between (query ts, nearest fix ts) before we consider the
     * fix stale. Mirrors the FileGpsSource knob of the same name.
     */
    private val toleranceMs: Long = 500,
) : GpsSource {

    private val log = LoggerFactory.getLogger(javaClass)
    private val mapper: ObjectMapper = jacksonObjectMapper()
    private val running = AtomicBoolean(true)
    private val lock = ReentrantReadWriteLock()

    // Two parallel arrays kept sorted by captureTsNs. ArrayList shifts
    // on head-eviction but at 10 Hz and 18 K entries the amortised cost
    // is negligible compared with Kafka poll latency.
    private val timestamps = ArrayList<Long>(maxFixes)
    private val fixes = ArrayList<GpsFix>(maxFixes)

    private val consumer: KafkaConsumer<String, String>
    private val pollThread: Thread

    init {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            // Start from the newest events on the first run. GPS age >
            // toleranceMs is rejected by nearest() anyway, so replaying
            // weeks of history would be pure CPU waste on a fresh boot.
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)
            put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10_000)
        }
        consumer = KafkaConsumer(props)
        consumer.subscribe(listOf(topic))

        pollThread = thread(name = "kafka-gps-consumer", isDaemon = true) { pollLoop() }
        log.info("KafkaGpsSource subscribed: topic={} group={} max_fixes={}", topic, groupId, maxFixes)
    }

    private fun pollLoop() {
        try {
            while (running.get()) {
                val records = consumer.poll(Duration.ofMillis(500))
                if (records.isEmpty) continue
                for (rec in records) {
                    val v = rec.value() ?: continue
                    try {
                        val row: Map<String, Any?> = mapper.readValue(v)
                        val ts = (row["capture_ts_ns"] as? Number)?.toLong() ?: continue
                        val lat = (row["lat"] as? Number)?.toDouble() ?: continue
                        val lon = (row["lon"] as? Number)?.toDouble() ?: continue
                        val alt = (row["alt"] as? Number)?.toDouble() ?: continue
                        append(GpsFix(ts, lat, lon, alt))
                    } catch (e: Exception) {
                        log.warn("drop malformed gps row: {}", e.message)
                    }
                }
            }
        } catch (e: Exception) {
            if (running.get()) log.error("kafka-gps-consumer loop exited", e)
        } finally {
            try { consumer.close() } catch (_: Exception) {}
        }
    }

    /**
     * Append a fix, evicting the oldest when the ring is full. GPS
     * samples generally arrive in order, but the partition key is
     * vehicle_id so interleavings between vehicles are possible; we
     * handle out-of-order insertion by binary-searching the right slot.
     */
    private fun append(fix: GpsFix) {
        lock.write {
            val idx = java.util.Collections.binarySearch(timestamps, fix.captureTsNs)
            val ins = if (idx >= 0) idx else -idx - 1
            timestamps.add(ins, fix.captureTsNs)
            fixes.add(ins, fix)
            while (timestamps.size > maxFixes) {
                timestamps.removeAt(0)
                fixes.removeAt(0)
            }
        }
    }

    override fun nearest(captureTsNs: Long): GpsFix? {
        lock.read {
            if (timestamps.isEmpty()) return null
            val idx = java.util.Collections.binarySearch(timestamps, captureTsNs)
            val candidate = when {
                idx >= 0 -> fixes[idx]
                else -> {
                    val ins = -idx - 1
                    val left = if (ins > 0) fixes[ins - 1] else null
                    val right = if (ins < fixes.size) fixes[ins] else null
                    when {
                        left == null -> right!!
                        right == null -> left
                        (captureTsNs - left.captureTsNs) <= (right.captureTsNs - captureTsNs) -> left
                        else -> right
                    }
                }
            }
            val diffMs = kotlin.math.abs(candidate.captureTsNs - captureTsNs) / 1_000_000
            return if (diffMs <= toleranceMs) candidate else null
        }
    }

    override fun close() {
        running.set(false)
        try {
            // Wake the consumer so poll() returns promptly.
            consumer.wakeup()
        } catch (_: Exception) {}
        try {
            pollThread.join(2_000)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }
}
