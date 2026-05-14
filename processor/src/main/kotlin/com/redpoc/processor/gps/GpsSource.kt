package com.redpoc.processor.gps

/**
 * One GPS fix with a nanosecond-precision capture timestamp so it can be
 * time-correlated with camera bundles without lossy conversions.
 */
data class GpsFix(
    val captureTsNs: Long,
    val lat: Double,
    val lon: Double,
    val alt: Double,
)

/**
 * Look-up source for GPS fixes. Swap in a real-time TCP parser or a
 * Kafka-backed source later without touching anything downstream.
 */
interface GpsSource {
    /**
     * Returns the fix closest in time to `captureTsNs`. null when the
     * source is empty or the requested timestamp lies beyond the
     * configured tolerance from any known fix.
     */
    fun nearest(captureTsNs: Long): GpsFix?
    fun close() {}
}
