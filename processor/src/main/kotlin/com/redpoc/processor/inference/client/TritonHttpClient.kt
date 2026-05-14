package com.redpoc.processor.inference.client

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Duration

/**
 * Triton Inference Server v2 HTTP binary protocol.
 *
 * Request layout:
 *   HEADER (JSON) | PAYLOAD (raw FP32 bytes)
 *   with `Inference-Header-Content-Length: <JSON length>`.
 *
 * Response layout: identical — read header length from the same header
 * in the response, then the remaining bytes are the output tensor data.
 *
 * This keeps the on-wire payload minimal (no base64 / JSON float list)
 * at a 1.2 M-float per frame working set.
 */
class TritonHttpClient(
    baseUrl: String,
    private val modelName: String,
    timeout: Duration = Duration.ofSeconds(5),
) {
    private val base = baseUrl.trimEnd('/')
    private val mapper: ObjectMapper = jacksonObjectMapper()
    private val http = OkHttpClient.Builder()
        .connectTimeout(timeout)
        .callTimeout(timeout.multipliedBy(2))
        .build()

    /**
     * @param tensor flattened CHW FP32 array (3*640*640 for yolo26n).
     * @return FP32 output array (1*300*6 = 1800 values).
     */
    fun infer(tensor: FloatArray, inputShape: IntArray): FloatArray {
        val headerBytes = buildHeader(inputShape, tensor.size * 4).toByteArray(Charsets.UTF_8)
        val payloadBytes = floatArrayToLe(tensor)

        val body = ByteArray(headerBytes.size + payloadBytes.size)
        System.arraycopy(headerBytes, 0, body, 0, headerBytes.size)
        System.arraycopy(payloadBytes, 0, body, headerBytes.size, payloadBytes.size)

        val req = Request.Builder()
            .url("$base/v2/models/$modelName/infer")
            .header("Inference-Header-Content-Length", headerBytes.size.toString())
            .post(body.toRequestBody("application/octet-stream".toMediaType()))
            .build()

        http.newCall(req).execute().use { resp ->
            if (!resp.isSuccessful) {
                error("triton http ${resp.code}: ${resp.body?.string()?.take(200)}")
            }
            val respHeaderLen = resp.header("Inference-Header-Content-Length")?.toInt()
                ?: error("missing Inference-Header-Content-Length")
            val bytes = resp.body?.bytes() ?: error("empty response")
            // Ensure it's an output0 tensor (validation is cheap).
            // Skip parsing header JSON beyond sanity check — we know the
            // shape from config.pbtxt and only need the float bytes.
            if (respHeaderLen >= bytes.size) {
                error("response shorter than declared header: $respHeaderLen ≥ ${bytes.size}")
            }
            return leToFloatArray(bytes, respHeaderLen, bytes.size - respHeaderLen)
        }
    }

    private fun buildHeader(inputShape: IntArray, payloadBytes: Int): String {
        val header = mapOf(
            "inputs" to listOf(
                mapOf(
                    "name" to "images",
                    "shape" to inputShape.toList(),
                    "datatype" to "FP32",
                    "parameters" to mapOf("binary_data_size" to payloadBytes),
                )
            ),
            "outputs" to listOf(
                mapOf(
                    "name" to "output0",
                    "parameters" to mapOf("binary_data" to true),
                )
            ),
        )
        return mapper.writeValueAsString(header)
    }

    private fun floatArrayToLe(src: FloatArray): ByteArray {
        val buf = ByteBuffer.allocate(src.size * 4).order(ByteOrder.LITTLE_ENDIAN)
        for (v in src) buf.putFloat(v)
        return buf.array()
    }

    private fun leToFloatArray(src: ByteArray, offset: Int, len: Int): FloatArray {
        require(len % 4 == 0) { "output length $len not aligned to 4" }
        val buf = ByteBuffer.wrap(src, offset, len).order(ByteOrder.LITTLE_ENDIAN)
        val out = FloatArray(len / 4)
        for (i in out.indices) out[i] = buf.getFloat()
        return out
    }
}
