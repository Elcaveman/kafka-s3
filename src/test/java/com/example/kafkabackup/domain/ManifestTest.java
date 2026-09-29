package com.example.kafkabackup.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ManifestTest {

    @Test
    void nextOffsetsIsMaxPlusOnePerPartition() {
        Manifest manifest = new Manifest(
                "orders",
                "2026-09-28T23",
                "kafka-s3-backup/2026-09-28/orders-abc.log",
                5,
                Map.of(0, List.of(100L, 101L, 102L), 1, List.of(40L, 41L)));

        assertThat(manifest.nextOffsets()).containsExactlyInAnyOrderEntriesOf(Map.of(0, 103L, 1, 42L));
    }
}
