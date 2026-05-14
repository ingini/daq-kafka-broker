package com.redpoc.processor.inference

import com.redpoc.processor.model.Detection
import com.redpoc.processor.model.FrameBundleMeta

/**
 * Sole abstraction the topology depends on. A new backend (Triton, local
 * ONNX, a remote HTTP model service) is added as a new class implementing
 * this interface; nothing in the topology changes (OCP + DIP).
 */
interface Inferencer {
    /**
     * Inspect one frame bundle and return zero or more detections.
     * Returning an empty list means "no event"; the topology drops the
     * message. Implementations must be thread-safe.
     */
    fun infer(bundle: FrameBundleMeta): List<Detection>

    fun close() {}
}
