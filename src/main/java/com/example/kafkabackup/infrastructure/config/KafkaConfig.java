package com.example.kafkabackup.infrastructure.config;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
class KafkaConfig {

    /** Explicit factory so the consumer loop gets concrete {@code <String, byte[]>} generics. */
    @Bean
    ConsumerFactory<String, byte[]> backupConsumerFactory(KafkaProperties properties) {
        return new DefaultKafkaConsumerFactory<>(properties.buildConsumerProperties(null));
    }
}
