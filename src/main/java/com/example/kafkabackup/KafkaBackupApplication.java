package com.example.kafkabackup;

import java.time.Clock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class KafkaBackupApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaBackupApplication.class, args);
    }

    /** Injected so tests can control the time windows. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
