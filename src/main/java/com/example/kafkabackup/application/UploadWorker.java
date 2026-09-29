package com.example.kafkabackup.application;

import com.example.kafkabackup.domain.Manifest;
import com.example.kafkabackup.domain.SealedFile;
import com.example.kafkabackup.infrastructure.file.WorkDir;
import com.example.kafkabackup.infrastructure.metrics.BackupMetrics;
import com.example.kafkabackup.infrastructure.s3.S3Archive;
import com.example.kafkabackup.infrastructure.s3.S3Keys;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Data file, then manifest, then offset commit, then local delete. Never a different order (I1). */
@Component
public class UploadWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(UploadWorker.class);
    private static final Duration IDLE = Duration.ofSeconds(5);
    private static final Duration MAX_BACKOFF = Duration.ofMinutes(5);
    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(30);

    private final WorkDir workDir;
    private final S3Archive s3;
    private final CommitQueue commitQueue;
    private final BackupMetrics metrics;
    private final ObjectMapper json;

    private final ExecutorService executor =
            Executors.newSingleThreadExecutor(r -> new Thread(r, "uploader"));
    private volatile boolean running;

    public UploadWorker(
            WorkDir workDir,
            S3Archive s3,
            CommitQueue commitQueue,
            BackupMetrics metrics,
            ObjectMapper json) {
        this.workDir = workDir;
        this.s3 = s3;
        this.commitQueue = commitQueue;
        this.metrics = metrics;
        this.json = json;
    }

    public void start() {
        running = true;
        executor.submit(this::run);
    }

    private void run() {
        Duration backoff = IDLE;
        while (running) {
            try {
                boolean uploaded = uploadPending();
                backoff = IDLE;
                if (!uploaded) {
                    Thread.sleep(IDLE.toMillis());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("Upload failed, retrying in {}", backoff, e);
                sleep(backoff);
                backoff = nextBackoff(backoff);
            }
        }
    }

    /** Uploads everything currently in {@code sealed/}; returns false when there was nothing. */
    public boolean uploadPending() throws Exception {
        List<SealedFile> pending = workDir.pendingSealed();
        for (SealedFile sealed : pending) {
            upload(sealed);
        }
        return !pending.isEmpty();
    }

    private void upload(SealedFile sealed) throws Exception {
        Manifest manifest = json.readValue(sealed.manifest().toFile(), Manifest.class);
        long start = System.nanoTime();
        try {
            s3.put(manifest.file(), sealed.data());
            s3.put(S3Keys.manifestFor(manifest.file()), sealed.manifest());
        } catch (RuntimeException e) {
            metrics.uploadFailure(manifest.topic());
            throw e;
        }

        commitQueue.submit(offsetsOf(manifest)).get(COMMIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        long bytes = Files.size(sealed.data());
        Files.delete(sealed.data());
        Files.delete(sealed.manifest());
        metrics.uploadSuccess(manifest.topic(), bytes, System.nanoTime() - start);
        log.info("Uploaded {} ({} records)", manifest.file(), manifest.records());
    }

    private static Map<TopicPartition, OffsetAndMetadata> offsetsOf(Manifest manifest) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        manifest.nextOffsets().forEach((partition, next) ->
                offsets.put(new TopicPartition(manifest.topic(), partition), new OffsetAndMetadata(next)));
        return offsets;
    }

    private static Duration nextBackoff(Duration current) {
        Duration doubled = current.multipliedBy(2);
        return doubled.compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : doubled;
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        executor.shutdownNow();
    }
}
