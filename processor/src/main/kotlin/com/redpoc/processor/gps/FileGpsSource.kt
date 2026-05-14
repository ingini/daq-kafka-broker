package com.redpoc.processor.gps

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Loads GPS fixes from a JSONL file (one JSON object per line) produced
 * by `scripts/extract-imu-gps.py`. The file is loaded in full at
 * construction time — 30 K rows × ~120 B = ~4 MB, a trivial footprint.
 *
 * Lookup is O(log n) via binary search on a sorted timestamp array.
 *
 * `toleranceMs` caps how far the nearest fix can be from the requested
 * timestamp before we treat it as "no fix known". Prevents accidentally
 * attaching a day-old GPS record to a freshly replayed bundle when the
 * pcap was captured on a different day.
 */
class FileGpsSource(
    path: String,
    private val toleranceMs: Long = 60_000,
    /**
     * POC replay mode: when `true`, a query timestamp that falls outside
     * the data span is first wrapped modulo the span so each replay loop
     * maps onto the same GPS sequence. Avoids having to synthesise fake
     * GPS that tracks the wall-clock edge-agent uses.
     */
    private val replayWrap: Boolean = false,
) : GpsSource {

    private val log = LoggerFactory.getLogger(javaClass)
    private val timestamps: LongArray
    private val fixes: Array<GpsFix>

    init {
        val mapper: ObjectMapper = jacksonObjectMapper()
        val list = mutableListOf<GpsFix>()
        val p = Paths.get(path)
        if (Files.exists(p)) {
            Files.newBufferedReader(p).use { br ->
                br.lineSequence().forEach { line ->
                    if (line.isBlank()) return@forEach
                    val row: Map<String, Any?> = mapper.readValue(line)
                    val ts = (row["capture_ts_ns"] as Number).toLong()
                    val lat = (row["lat"] as Number).toDouble()
                    val lon = (row["lon"] as Number).toDouble()
                    val alt = (row["alt"] as Number).toDouble()
                    list += GpsFix(ts, lat, lon, alt)
                }
            }
        } else {
            log.warn("gps file not found: {} — all lookups will return null", path)
        }
        list.sortBy { it.captureTsNs }
        timestamps = LongArray(list.size) { list[it].captureTsNs }
        fixes = list.toTypedArray()
        log.info("loaded {} GPS fixes from {}", fixes.size, path)
    }

    override fun nearest(captureTsNs: Long): GpsFix? {
        if (fixes.isEmpty()) return null
        // Map far-future/past timestamps back into the GPS data span
        // when running in replay mode. Outside replay, keep the strict
        // tolerance-based matching.
        val query = if (replayWrap) wrapIntoSpan(captureTsNs) else captureTsNs

        val idx = java.util.Arrays.binarySearch(timestamps, query)
        val candidate = when {
            idx >= 0 -> fixes[idx]
            else -> {
                val ins = -idx - 1
                val left = if (ins > 0) fixes[ins - 1] else null
                val right = if (ins < fixes.size) fixes[ins] else null
                when {
                    left == null -> right!!
                    right == null -> left
                    (query - left.captureTsNs) <= (right.captureTsNs - query) -> left
                    else -> right
                }
            }
        }
        val diffMs = kotlin.math.abs(candidate.captureTsNs - query) / 1_000_000
        return if (diffMs <= toleranceMs) candidate else null
    }

    private fun wrapIntoSpan(ts: Long): Long {
        val lo = timestamps.first()
        val hi = timestamps.last()
        val span = hi - lo
        if (span <= 0) return ts
        val offset = ((ts - lo) % span + span) % span
        return lo + offset
    }
}
