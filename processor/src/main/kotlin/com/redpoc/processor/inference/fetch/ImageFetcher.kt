package com.redpoc.processor.inference.fetch

/**
 * Read/Write abstractions over S3-like image storage. Split into two
 * narrow interfaces (§1.3 ISP) so read-only consumers (TritonInferencer)
 * don't gain upload privileges and vice-versa.
 */
interface ImageFetcher {
    fun fetch(uri: String): ByteArray
    fun close() {}
}

interface ImageUploader {
    /** Stores `bytes` at `bucket`/`key` and returns the `s3://bucket/key` URI. */
    fun put(bucket: String, key: String, bytes: ByteArray, contentType: String = "image/jpeg"): String
    fun close() {}
}
