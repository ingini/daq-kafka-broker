package com.redpoc.notifier.notification

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest
import java.net.URI
import java.time.Duration

/**
 * Resolves s3:// URIs to time-limited presigned HTTPS URLs so Slack users
 * can load images without having bucket credentials.
 */
class PresignedUrlResolver(
    endpoint: String,
    accessKey: String,
    secretKey: String,
    region: String = "us-east-1",
    private val ttl: Duration = Duration.ofHours(24),
) : AutoCloseable {
    private val presigner: S3Presigner = S3Presigner.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(region))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKey, secretKey)
        ))
        .serviceConfiguration(
            software.amazon.awssdk.services.s3.S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .build()
        )
        .build()

    fun presign(s3Uri: String): String {
        val (bucket, key) = parse(s3Uri)
        val get = GetObjectRequest.builder().bucket(bucket).key(key).build()
        val req = GetObjectPresignRequest.builder()
            .signatureDuration(ttl)
            .getObjectRequest(get)
            .build()
        return presigner.presignGetObject(req).url().toString()
    }

    private fun parse(uri: String): Pair<String, String> {
        require(uri.startsWith("s3://")) { "not an s3 uri: $uri" }
        val rest = uri.removePrefix("s3://")
        val bucket = rest.substringBefore("/")
        val key = rest.substringAfter("/")
        return bucket to key
    }

    override fun close() = presigner.close()
}
