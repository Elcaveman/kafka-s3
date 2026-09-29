package com.example.kafkabackup.application;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class BackupRunner implements ApplicationRunner {

    private final RecoveryService recovery;
    private final ConsumerLoop consumerLoop;
    private final UploadWorker uploadWorker;

    public BackupRunner(RecoveryService recovery, ConsumerLoop consumerLoop, UploadWorker uploadWorker) {
        this.recovery = recovery;
        this.consumerLoop = consumerLoop;
        this.uploadWorker = uploadWorker;
    }

    @Override
    public void run(ApplicationArguments args) {
        recovery.run();
        consumerLoop.start();
        uploadWorker.start();
    }
}
