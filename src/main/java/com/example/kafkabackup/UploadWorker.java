package com.example.kafkabackup;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Data file, then manifest, then offset commit, then local delete. Never a different order (I1). */
@Component
public class UploadWorker {

    private static final Logger log = LoggerFactory.getLogger(UploadWorker.class);
    private static final long COMMIT_TIMEOUT_SECONDS = 30;

    private final WorkDir workDir;
    private final S3Archive s3;
    private final ConsumerLoop consumerLoop;
    private final BackupMetrics metrics;
    private final ObjectMapper json;

    public UploadWorker(
            WorkDir workDir,
            S3Archive s3,
            ConsumerLoop consumerLoop,
            BackupMetrics metrics,
            ObjectMapper json) {
        this.workDir = workDir;
        this.s3 = s3;
        this.consumerLoop = consumerLoop;
        this.metrics = metrics;
        this.json = json;
    }

    /** A file that fails stays in {@code sealed/} and blocks the rest until the next tick. */
    @Scheduled(initialDelay = 5, fixedDelay = 5, timeUnit = TimeUnit.SECONDS)
    public void uploadPending() {
        for (SealedFile sealed : workDir.pendingSealed()) {
            try {
                upload(sealed);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("Upload failed for {}, retrying later", sealed.data(), e);
                return;
            }
        }
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

        consumerLoop.commit(manifest.commitOffsets()).get(COMMIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        long bytes = Files.size(sealed.data());
        Files.delete(sealed.data());
        Files.delete(sealed.manifest());
        metrics.uploadSuccess(manifest.topic(), bytes, System.nanoTime() - start);
        log.info("Uploaded {} ({} records)", manifest.file(), manifest.records());
    }
}
