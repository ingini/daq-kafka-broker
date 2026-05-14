package com.redpoc.processor.inference.fetch

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.net.URI

/**
 * S3-compatible client implementing both fetch and upload against a
 * MinIO endpoint with path-style addressing. One client instance backs
 * both interfaces so the SDK connection pool is shared.
 */
class S3ImageFetcher(
    endpoint: String,
    accessKey: String,
    secretKey: String,
    region: String = "us-east-1",
) : ImageFetcher, ImageUploader {

    private val client: S3Client = S3Client.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(region))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
        )
        .serviceConfiguration(
            S3Configuration.builder().pathStyleAccessEnabled(true).build()
        )
        .build()

    override fun fetch(uri: String): ByteArray {
        val (bucket, key) = parse(uri)
        val req = GetObjectRequest.builder().bucket(bucket).key(key).build()
        client.getObject(req).use { return it.readBytes() }
    }

    override fun put(bucket: String, key: String, bytes: ByteArray, contentType: String): String {
        val req = PutObjectRequest.builder()
            .bucket(bucket).key(key).contentType(contentType).build()
        client.putObject(req, RequestBody.fromBytes(bytes))
        return "s3://$bucket/$key"
    }

    override fun close() = client.close()

    private fun parse(uri: String): Pair<String, String> {
        require(uri.startsWith("s3://")) { "not an s3 uri: $uri" }
        val rest = uri.removePrefix("s3://")
        val bucket = rest.substringBefore('/')
        val key = rest.substringAfter('/')
        return bucket to key
    }
}
