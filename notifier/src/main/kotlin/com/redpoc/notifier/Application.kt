package com.redpoc.notifier

import com.redpoc.notifier.config.NotifierConfig
import com.redpoc.notifier.pipeline.Pipeline
import org.slf4j.LoggerFactory

fun main(args: Array<String>) {
    val log = LoggerFactory.getLogger("notifier")
    val cfgPath = args.firstOrNull() ?: "/app/application.yml"
    log.info("loading config from {}", cfgPath)
    val cfg = NotifierConfig.load(cfgPath)
    Pipeline(cfg).run()
}
