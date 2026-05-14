package com.redpoc.processor.inference

import com.redpoc.processor.model.Detection
import com.redpoc.processor.model.FrameBundleMeta
import java.util.concurrent.atomic.AtomicLong

/**
 * Emits a synthetic Detection every `emitEveryN` bundles so the downstream
 * notifier path can be exercised before the real Triton model is ready.
 */
class MockInferencer(
    private val emitEveryN: Int,
    private val className: String,
    private val confidence: Double,
) : Inferencer {
    private val seen = AtomicLong(0)

    override fun infer(bundle: FrameBundleMeta): List<Detection> {
        val n = seen.incrementAndGet()
        if (emitEveryN <= 0 || n % emitEveryN != 0L) return emptyList()
        return listOf(Detection(classId = 11, className = className, confidence = confidence))
    }
}
