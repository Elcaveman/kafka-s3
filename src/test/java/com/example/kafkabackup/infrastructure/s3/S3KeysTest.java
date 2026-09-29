package com.example.kafkabackup.infrastructure.s3;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.kafkabackup.infrastructure.config.BackupProperties;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

class S3KeysTest {

    private final S3Keys keys = new S3Keys(new BackupProperties(
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
        assertThat(keys.data("orders", "2026-09-28", "a1b2c3d4"))
                .isEqualTo("my-app/2026-09-28/orders-a1b2c3d4.log");
    }

    @Test
    void manifestKeySitsNextToItsDataFile() {
        assertThat(S3Keys.manifestFor("my-app/2026-09-28/orders-a1b2c3d4.log"))
                .isEqualTo("my-app/2026-09-28/orders-a1b2c3d4.manifest.json");
    }
}
