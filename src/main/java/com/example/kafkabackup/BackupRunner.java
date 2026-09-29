package com.example.kafkabackup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class BackupRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(BackupRunner.class);

    private final WorkDir workDir;
    private final ConsumerLoop consumerLoop;

    public BackupRunner(WorkDir workDir, ConsumerLoop consumerLoop) {
        this.workDir = workDir;
        this.consumerLoop = consumerLoop;
    }

    @Override
    public void run(ApplicationArguments args) {
        // Active files were never sealed and their offsets never committed, so Kafka still has them.
        workDir.discardActiveFiles();
        int pending = workDir.pendingCount();
        if (pending > 0) {
            log.warn("Recovery: {} sealed file(s) still to upload and commit", pending);
        }
        consumerLoop.start();
    }
}
