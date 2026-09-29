package com.example.kafkabackup.datafile;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.kafkabackup.shared.BackupProperties;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

class DataFileUploaderTest {

    private final DataFileUploader uploader = new DataFileUploader(
            null,
            new BackupProperties(
                    List.of("orders"),
                    "UTC",
                    "/tmp/backup",
                    "my-app",
                    true,
                    new BackupProperties.Batch(500, DataSize.ofMegabytes(4), Duration.ofSeconds(1)),
                    new BackupProperties.Disk(0.8, 0.7),
                    new BackupProperties.S3("my-backup", "eu-west-3")));

    @Test
    void dataKeyFollowsApplicationDateTopicUidLayout() {
        assertThat(uploader.key("orders", "2026-09-28", "a1b2c3d4"))
                .isEqualTo("my-app/2026-09-28/orders-a1b2c3d4.log");
    }
}
