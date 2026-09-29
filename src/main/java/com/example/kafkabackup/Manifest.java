package com.example.kafkabackup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Proof that a data file is complete. Uploaded after the data file, so its presence in S3 means
 * the data is there. {@code offsets} lists every offset in the file per partition, in line order.
 */
public record Manifest(
        String topic,
        String window,
        String file,
        int records,
        Map<Integer, List<Long>> offsets) {

    /** What to commit once this file is in S3: highest offset seen per partition, plus one. */
    public Map<TopicPartition, OffsetAndMetadata> commitOffsets() {
        Map<TopicPartition, OffsetAndMetadata> commit = new HashMap<>();
        offsets.forEach((partition, list) -> commit.put(
                new TopicPartition(topic, partition),
                new OffsetAndMetadata(list.stream().mapToLong(Long::longValue).max().orElseThrow() + 1)));
        return commit;
    }
}
