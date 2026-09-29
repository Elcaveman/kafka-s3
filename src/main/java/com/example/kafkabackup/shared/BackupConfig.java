package com.example.kafkabackup.shared;

import java.net.URI;
import java.time.Clock;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

@Configuration
class BackupConfig {

    @Bean
    Clock clock(BackupProperties properties) {
        return Clock.system(properties.zoneId());
    }

    /** Explicit factory so the consumer loop gets concrete {@code <String, byte[]>} generics. */
    @Bean
    ConsumerFactory<String, byte[]> backupConsumerFactory(KafkaProperties properties) {
        return new DefaultKafkaConsumerFactory<>(properties.buildConsumerProperties(null));
    }

    /** Credentials normally come from Vault (properties), otherwise the default AWS chain. */
    @Bean
    S3Client s3Client(BackupProperties properties, Environment env) {
        S3ClientBuilder builder = S3Client.builder().region(Region.of(properties.s3().region()));

        String accessKey = env.getProperty("aws.access-key-id");
        String secretKey = env.getProperty("aws.secret-access-key");
        if (accessKey != null && secretKey != null) {
            builder.credentialsProvider(
                    StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
        }

        String endpoint = env.getProperty("aws.endpoint");
        if (endpoint != null) {
            builder.endpointOverride(URI.create(endpoint)).forcePathStyle(true);
        }
        return builder.build();
    }
}
