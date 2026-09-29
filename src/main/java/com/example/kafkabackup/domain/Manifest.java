package com.example.kafkabackup.domain;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    /** Offsets to commit to Kafka: highest offset seen per partition, plus one. */
    public Map<Integer, Long> nextOffsets() {
        Map<Integer, Long> next = new HashMap<>();
        offsets.forEach((partition, list) -> next.put(
                partition, list.stream().mapToLong(Long::longValue).max().orElseThrow() + 1));
        return next;
    }
}
