package com.example.kafkabackup;

import java.nio.file.Path;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Component
public class S3Archive {

    private final S3Client s3;
    private final String bucket;

    public S3Archive(S3Client s3, BackupProperties properties) {
        this.s3 = s3;
        this.bucket = properties.s3().bucket();
    }

    /** S3 rejects the object if the SHA-256 it computes does not match ours, so no HEAD verify. */
    public void put(String key, Path file) {
        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .checksumAlgorithm(ChecksumAlgorithm.SHA256)
                        .build(),
                file);
    }
}
