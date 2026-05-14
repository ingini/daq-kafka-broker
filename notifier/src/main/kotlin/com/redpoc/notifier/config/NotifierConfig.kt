package com.redpoc.notifier.config

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.nio.file.Files
import java.nio.file.Paths

data class KafkaConfig(
    @JsonProperty("bootstrap_servers") val bootstrapServers: String,
    @JsonProperty("group_id") val groupId: String,
    @JsonProperty("input_topic") val inputTopic: String,
    @JsonProperty("dlq_topic") val dlqTopic: String,
)

data class PostgresConfig(val url: String, val user: String, val password: String)

data class SlackConfig(val token: String, val channel: String)

data class FileLogConfig(val path: String)

data class NotificationConfig(
    val kind: String,
    val slack: SlackConfig?,
    @JsonProperty("file_log") val fileLog: FileLogConfig?,
)

data class S3Config(
    val endpoint: String,
    @JsonProperty("access_key") val accessKey: String,
    @JsonProperty("secret_key") val secretKey: String,
    val region: String,
    @JsonProperty("presign_ttl_hours") val presignTtlHours: Long,
)

data class MetricsConfig(val port: Int)

data class NotifierConfig(
    val kafka: KafkaConfig,
    val postgres: PostgresConfig,
    val notification: NotificationConfig,
    val s3: S3Config?,
    val metrics: MetricsConfig?,
) {
    companion object {
        fun load(path: String): NotifierConfig {
            val mapper: ObjectMapper = jacksonObjectMapper().apply {
                configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            }
            val tree = ObjectMapper(YAMLFactory()).readTree(Files.newBufferedReader(Paths.get(path)))
            return mapper.treeToValue(tree, NotifierConfig::class.java)
        }
    }
}
