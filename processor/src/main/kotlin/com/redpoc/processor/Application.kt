package com.redpoc.processor

import com.redpoc.processor.config.ProcessorConfig
import com.redpoc.processor.pipeline.Pipeline
import org.slf4j.LoggerFactory

fun main(args: Array<String>) {
    val log = LoggerFactory.getLogger("processor")
    val cfgPath = args.firstOrNull() ?: "/app/application.yml"
    log.info("loading config from {}", cfgPath)
    val cfg = ProcessorConfig.load(cfgPath)
    Pipeline(cfg).run()
}
