package com.example.kafkabackup.upload;

import com.example.kafkabackup.datafile.DataFileUploader;
import com.example.kafkabackup.ingest.ConsumerLoop;
import com.example.kafkabackup.manifest.Manifest;
import com.example.kafkabackup.manifest.ManifestStore;
import com.example.kafkabackup.manifest.ManifestUploader;
import com.example.kafkabackup.shared.BackupMetrics;
import com.example.kafkabackup.shared.SealedFile;
import com.example.kafkabackup.shared.WorkDir;
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
    private final DataFileUploader dataFiles;
    private final ManifestUploader manifestUploads;
    private final ManifestStore manifests;
    private final ConsumerLoop consumerLoop;
    private final BackupMetrics metrics;

    public UploadWorker(
            WorkDir workDir,
            DataFileUploader dataFiles,
            ManifestUploader manifestUploads,
            ManifestStore manifests,
            ConsumerLoop consumerLoop,
            BackupMetrics metrics) {
        this.workDir = workDir;
        this.dataFiles = dataFiles;
        this.manifestUploads = manifestUploads;
        this.manifests = manifests;
        this.consumerLoop = consumerLoop;
        this.metrics = metrics;
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
        Manifest manifest = manifests.read(sealed.manifest());
        long start = System.nanoTime();
        try {
            dataFiles.upload(manifest.file(), sealed.data());
            manifestUploads.upload(manifest, sealed.manifest());
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
