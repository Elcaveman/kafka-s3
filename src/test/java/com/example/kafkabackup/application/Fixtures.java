package com.example.kafkabackup.application;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.example.kafkabackup.domain.BackupWindow;
import com.example.kafkabackup.domain.WindowCalculator;
import com.example.kafkabackup.infrastructure.config.BackupProperties;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.util.unit.DataSize;

final class Fixtures {

    static final BackupWindow WINDOW = WindowCalculator.sealedAt(Instant.parse("2026-09-29T01:00:05Z"), ZoneOffset.UTC);
    static final BackupWindow NEXT_WINDOW = WindowCalculator.sealedAt(Instant.parse("2026-09-29T02:00:05Z"), ZoneOffset.UTC);

    private Fixtures() {
    }

    static BackupProperties properties(Path workDir) {
        return properties(workDir, true, List.of("orders"));
    }

    static BackupProperties properties(Path workDir, boolean gapCheck, List<String> topics) {
        return new BackupProperties(
                topics,
                "UTC",
                workDir.toString(),
                "my-app",
                gapCheck,
                new BackupProperties.Batch(500, DataSize.ofMegabytes(4), Duration.ofMillis(100)),
                new BackupProperties.Disk(0.8, 0.7),
                new BackupProperties.S3("my-backup", "eu-west-3"));
    }

    /** The value encodes where the record came from, so file contents can be checked line by line. */
    static String value(String topic, int partition, long offset) {
        return "%s-%d-%d".formatted(topic, partition, offset);
    }

    static List<ConsumerRecord<String, byte[]>> batch(String topic, int partition, long... offsets) {
        return Arrays.stream(offsets)
                .mapToObj(o -> new ConsumerRecord<String, byte[]>(
                        topic, partition, o, null, value(topic, partition, o).getBytes(UTF_8)))
                .toList();
    }

    @SafeVarargs
    static List<ConsumerRecord<String, byte[]>> concat(List<ConsumerRecord<String, byte[]>>... batches) {
        List<ConsumerRecord<String, byte[]>> all = new ArrayList<>();
        Arrays.stream(batches).forEach(all::addAll);
        return all;
    }

    static List<String> lines(Path file) {
        try {
            return Files.exists(file) ? Files.readAllLines(file, UTF_8) : List.of();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
