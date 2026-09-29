package com.example.kafkabackup.application;

import com.example.kafkabackup.infrastructure.file.WorkDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class RecoveryService {

    private static final Logger log = LoggerFactory.getLogger(RecoveryService.class);

    private final WorkDir workDir;

    public RecoveryService(WorkDir workDir) {
        this.workDir = workDir;
    }

    /**
     * Active files were never sealed and their offsets were never committed, so Kafka still has
     * the records: drop them and let the consumer replay. Sealed files are left for the uploader, which
     * uploads them without committing (their acks died with the old process), so Kafka replays those
     * records too: duplicates in S3, never a loss.
     */
    public void run() {
        workDir.discardActiveFiles();
        int pending = workDir.pendingCount();
        if (pending > 0) {
            log.warn("Recovery: {} sealed file(s) still to upload and commit", pending);
        }
    }
}
