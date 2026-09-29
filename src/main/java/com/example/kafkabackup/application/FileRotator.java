package com.example.kafkabackup.application;

import com.example.kafkabackup.domain.BackupWindow;
import com.example.kafkabackup.domain.WindowCalculator;
import com.example.kafkabackup.infrastructure.config.BackupProperties;
import java.time.Clock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class FileRotator {

    private final BatchWriter batchWriter;
    private final BackupProperties properties;
    private final Clock clock;

    public FileRotator(BatchWriter batchWriter, BackupProperties properties, Clock clock) {
        this.batchWriter = batchWriter;
        this.properties = properties;
        this.clock = clock;
    }

    @Scheduled(cron = "${backup.rotate-cron}", zone = "${backup.zone}")
    public void rotate() {
        BackupWindow window = WindowCalculator.sealedAt(clock.instant(), properties.zoneId());
        properties.topics().forEach(topic -> batchWriter.seal(topic, window));
    }
}
