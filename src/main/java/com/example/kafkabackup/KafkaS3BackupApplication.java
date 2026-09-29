package com.example.kafkabackup;

import com.example.kafkabackup.shared.BackupProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(BackupProperties.class)
public class KafkaS3BackupApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaS3BackupApplication.class, args);
    }
}
