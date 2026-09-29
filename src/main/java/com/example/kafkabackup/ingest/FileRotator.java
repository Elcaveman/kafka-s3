package com.example.kafkabackup.ingest;

import com.example.kafkabackup.shared.BackupProperties;
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
        BackupWindow window = BackupWindow.sealedAt(clock.instant(), properties.zoneId());
        properties.topics().forEach(topic -> batchWriter.seal(topic, window));
    }
}
