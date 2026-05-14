package com.redpoc.processor.pipeline

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.redpoc.processor.config.ProcessorConfig
import com.redpoc.processor.gps.FileGpsSource
import com.redpoc.processor.gps.GpsSource
import com.redpoc.processor.gps.KafkaGpsSource
import com.redpoc.processor.inference.Inferencer
import com.redpoc.processor.inference.MockInferencer
import com.redpoc.processor.inference.TritonInferencer
import com.redpoc.processor.inference.annotate.Annotator
import com.redpoc.processor.inference.annotate.JpegAnnotator
import com.redpoc.processor.inference.client.TritonHttpClient
import com.redpoc.processor.inference.fetch.S3ImageFetcher
import com.redpoc.processor.inference.preprocess.ImagePreprocessor
import com.redpoc.processor.inference.render.Bbox2dRenderer
import com.redpoc.processor.topology.DetectionTopologyBuilder
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.concurrent.CountDownLatch

/**
 * Composition root. All concrete classes are chosen here; feature modules
 * never import each other's concrete types (DIP).
 */
class Pipeline(private val cfg: ProcessorConfig) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val mapper: ObjectMapper = jacksonObjectMapper()

    fun run() {
        val metrics = startMetrics()
        val inferencer = buildInferencer()
        val annotator = buildAnnotator()
        val gps = buildGpsSource()
        val topology = DetectionTopologyBuilder(
            cfg.kafka, inferencer, mapper, annotator, gps,
        ).build()
        val streams = KafkaStreams(topology, streamsProps())
        val latch = CountDownLatch(1)

        Runtime.getRuntime().addShutdownHook(Thread {
            log.info("shutdown signal; closing streams")
            streams.close()
            inferencer.close()
            annotator?.close()
            gps?.close()
            metrics?.close()
            latch.countDown()
        })

        streams.setUncaughtExceptionHandler { e ->
            log.error("streams uncaught exception", e)
            StreamsUncaughtExceptionHandlerResponse.SHUTDOWN_CLIENT
        }
        streams.start()
        log.info(
            "processor started: {} -> {} (visualize={}, allowed_classes={})",
            cfg.kafka.inputTopic, cfg.kafka.outputTopic,
            cfg.visualize.enabled, cfg.inference.allowedClasses.orEmpty().ifEmpty { "ALL" },
        )
        latch.await()
    }

    private fun startMetrics(): HTTPServer? {
        val port = cfg.metrics?.port ?: return null
        DefaultExports.initialize()
        com.redpoc.processor.metrics.Metrics.bundlesConsumed
        val server = HTTPServer.Builder().withPort(port).build()
        log.info("metrics on :{}", port)
        return server
    }

    private fun buildInferencer(): Inferencer = when (cfg.inference.kind) {
        "", "mock" -> MockInferencer(
            emitEveryN = cfg.mock.emitEveryN,
            className = cfg.mock.detectionClass,
            confidence = cfg.mock.confidence,
        )
        "triton" -> {
            val t = cfg.inference.triton
                ?: error("inference.triton section is required for kind=triton")
            val s3 = cfg.s3 ?: error("s3 section is required for kind=triton")
            val names = cfg.classNames ?: CocoClassNames.COCO80
            val fetcher = S3ImageFetcher(
                endpoint = s3.endpoint,
                accessKey = s3.accessKey,
                secretKey = s3.secretKey,
                region = s3.region,
            )
            val client = TritonHttpClient(
                baseUrl = t.url,
                modelName = t.modelName,
                timeout = java.time.Duration.ofMillis(t.timeoutMs),
            )
            TritonInferencer(
                fetcher = fetcher,
                preprocessor = ImagePreprocessor(),
                client = client,
                classNames = names,
                confidenceThreshold = t.confidenceThreshold,
                allowedClasses = (cfg.inference.allowedClasses ?: emptyList()).toSet(),
            )
        }
        else -> error("unknown inference kind: ${cfg.inference.kind}")
    }

    private fun buildGpsSource(): GpsSource? {
        val gps = cfg.gps ?: return null
        return when (gps.kind.lowercase()) {
            "", "file" -> {
                val path = gps.filePath ?: return null
                FileGpsSource(
                    path = path,
                    toleranceMs = gps.toleranceMs,
                    replayWrap = gps.replayWrap,
                )
            }
            "kafka" -> KafkaGpsSource(
                bootstrapServers = cfg.kafka.bootstrapServers,
                topic = gps.topic,
                groupId = gps.groupId,
                maxFixes = gps.maxFixes,
                toleranceMs = gps.toleranceMs,
            )
            "none" -> null
            else -> error("unknown gps.kind: ${gps.kind}")
        }
    }

    /**
     * Visualization is optional and orthogonal to inference.kind. When
     * disabled or S3 is not configured, return null so the topology skips
     * the render/upload path entirely.
     */
    private fun buildAnnotator(): Annotator? {
        if (!cfg.visualize.enabled) return null
        val s3 = cfg.s3 ?: return null
        val io = S3ImageFetcher(
            endpoint = s3.endpoint,
            accessKey = s3.accessKey,
            secretKey = s3.secretKey,
            region = s3.region,
        )
        return JpegAnnotator(
            fetcher = io,
            renderer = Bbox2dRenderer(),
            uploader = io,
            jpegQuality = cfg.visualize.jpegQuality,
            keySuffix = cfg.visualize.keySuffix,
            subdir = cfg.visualize.subdir,
        )
    }

    private fun streamsProps(): Properties = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, cfg.kafka.applicationId)
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.kafka.bootstrapServers)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            org.apache.kafka.common.serialization.Serdes.String().javaClass.name)
        put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            org.apache.kafka.common.serialization.Serdes.String().javaClass.name)
        put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2)
        put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE)
    }
}

private typealias StreamsUncaughtExceptionHandlerResponse =
    org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse
