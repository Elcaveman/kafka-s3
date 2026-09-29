package com.example.kafkabackup;

import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

@ConfigurationProperties(prefix = "backup")
public record BackupProperties(
        List<String> topics,
        String zone,
        String workDir,
        String applicationName,
        boolean gapCheck,
        Batch batch,
        Disk disk,
        S3 s3) {

    public ZoneId zoneId() {
        return ZoneId.of(zone);
    }

    public record Batch(int maxRecords, DataSize maxBytes, Duration pollTimeout) {
    }

    public record Disk(double pauseAbove, double resumeBelow) {
    }

    public record S3(String bucket, String region) {
    }
}
