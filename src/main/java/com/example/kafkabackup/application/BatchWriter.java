package com.example.kafkabackup.application;

import com.example.kafkabackup.domain.BackupWindow;
import com.example.kafkabackup.domain.Manifest;
import com.example.kafkabackup.domain.SealedFile;
import com.example.kafkabackup.infrastructure.config.BackupProperties;
import com.example.kafkabackup.infrastructure.file.ActiveFile;
import com.example.kafkabackup.infrastructure.file.WorkDir;
import com.example.kafkabackup.infrastructure.metrics.BackupMetrics;
import com.example.kafkabackup.infrastructure.s3.S3Keys;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Owns the active files. One lock guards them so a rotation can never interleave with an append
 * (I3). The uploader only reads {@code sealed/} and never takes this lock.
 */
@Component
public class BatchWriter {

    private static final Logger log = LoggerFactory.getLogger(BatchWriter.class);

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, ActiveFile> active = new HashMap<>();
    private final Map<TopicPartition, Long> lastOffset = new HashMap<>();
    private final Map<String, Map<Integer, Acknowledgment>> latestAcks = new HashMap<>();
    /** Read by the uploader without the lock. */
    private final Map<Path, List<Acknowledgment>> sealedAcks = new ConcurrentHashMap<>();

    private final WorkDir workDir;
    private final S3Keys s3Keys;
    private final BackupProperties properties;
    private final BackupMetrics metrics;
    private final ObjectMapper json;

    private volatile boolean gapDetected;

    public BatchWriter(
            WorkDir workDir,
            S3Keys s3Keys,
            BackupProperties properties,
            BackupMetrics metrics,
            ObjectMapper json) {
        this.workDir = workDir;
        this.s3Keys = s3Keys;
        this.properties = properties;
        this.metrics = metrics;
        this.json = json;
    }

    /** Called on a listener container thread; each container consumes one topic. */
    public void append(List<ConsumerRecord<String, byte[]>> records, Acknowledgment ack) {
        if (records.isEmpty()) {
            return;
        }
        String topic = records.get(0).topic();
        if (records.stream().anyMatch(r -> !r.topic().equals(topic))) {
            throw new IllegalStateException("A batch must hold a single topic: its ack commits all of it");
        }
        lock.lock();
        try {
            ActiveFile file = activeFor(topic);
            for (ConsumerRecord<String, byte[]> record : records) {
                checkGap(record);
                file.append(record.value(), record.partition(), record.offset());
            }
            file.fsync();
            rememberAck(topic, records, ack);
            metrics.recordsConsumed(topic, records.size());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.unlock();
        }
    }

    /** Closes the active file, moves it to {@code sealed/} with its manifest, opens a fresh one. */
    public Optional<SealedFile> seal(String topic, BackupWindow window) {
        lock.lock();
        try {
            ActiveFile file = active.remove(topic);
            Map<Integer, Acknowledgment> acks = latestAcks.remove(topic);
            if (file == null || file.records() == 0) {
                closeAndDropEmpty(file);
                return Optional.empty();
            }

            String uid = UUID.randomUUID().toString().replace("-", "");
            Manifest manifest = new Manifest(
                    topic,
                    window.label(),
                    s3Keys.data(topic, window.date(), uid),
                    file.records(),
                    file.offsets());
            file.close();

            Path data = workDir.sealedData(topic, window.id(), uid);
            Files.move(file.path(), data, StandardCopyOption.ATOMIC_MOVE);
            // Registered before the manifest exists, since the manifest is what the uploader looks for.
            sealedAcks.put(data, List.copyOf(new LinkedHashSet<>(acks.values())));

            Path manifestPath = workDir.sealedManifest(topic, window.id(), uid);
            json.writeValue(manifestPath.toFile(), manifest);

            log.info("Sealed {} records for {} window {}", manifest.records(), topic, window.label());
            return Optional.of(new SealedFile(data, manifestPath));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Asks Kafka to commit a sealed file's offsets. Returns false for a file sealed before a restart:
     * its acks are gone, and Kafka replays whatever was not committed.
     */
    public boolean acknowledge(SealedFile sealed) {
        List<Acknowledgment> acks = sealedAcks.remove(sealed.data());
        if (acks == null) {
            return false;
        }
        acks.forEach(Acknowledgment::acknowledge);
        return true;
    }

    public boolean gapDetected() {
        return gapDetected;
    }

    /**
     * An ack commits only the partitions present in its own batch, so a file needs the latest ack of
     * every partition it holds. Re-inserting keeps the map in arrival order, so acks are replayed
     * oldest first and a commit never moves backwards. Only these batches stay in memory.
     */
    private void rememberAck(String topic, List<ConsumerRecord<String, byte[]>> records, Acknowledgment ack) {
        Map<Integer, Acknowledgment> acks = latestAcks.computeIfAbsent(topic, t -> new LinkedHashMap<>());
        records.stream().map(ConsumerRecord::partition).distinct().forEach(partition -> {
            acks.remove(partition);
            acks.put(partition, ack);
        });
    }

    private ActiveFile activeFor(String topic) throws IOException {
        ActiveFile file = active.get(topic);
        if (file == null) {
            file = new ActiveFile(workDir.active(topic));
            active.put(topic, file);
        }
        return file;
    }

    private void checkGap(ConsumerRecord<String, byte[]> record) {
        if (!properties.gapCheck()) {
            return;
        }
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        Long previous = lastOffset.put(partition, record.offset());
        if (previous != null && record.offset() != previous + 1) {
            gapDetected = true;
            metrics.offsetGap(record.topic());
            log.error("Offset gap on {}: {} -> {}", partition, previous, record.offset());
        }
    }

    private void closeAndDropEmpty(ActiveFile file) throws IOException {
        if (file != null) {
            file.close();
            file.deleteIfEmpty();
        }
    }
}
