package com.example.kafkabackup;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class ManifestTest {

    @Test
    void commitOffsetsAreMaxPlusOnePerPartition() {
        Manifest manifest = new Manifest(
                "orders",
                "2026-09-28T23",
                "kafka-s3-backup/2026-09-28/orders-abc.log",
                5,
                Map.of(0, List.of(100L, 101L, 102L), 1, List.of(40L, 41L)));

        assertThat(manifest.commitOffsets())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        new TopicPartition("orders", 0), new OffsetAndMetadata(103L),
                        new TopicPartition("orders", 1), new OffsetAndMetadata(42L)));
    }
}
