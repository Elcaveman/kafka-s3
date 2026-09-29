package com.example.kafkabackup.application;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class BackupRunner implements ApplicationRunner {

    private final RecoveryService recovery;
    private final TopicConsumers consumers;
    private final UploadWorker uploadWorker;

    public BackupRunner(RecoveryService recovery, TopicConsumers consumers, UploadWorker uploadWorker) {
        this.recovery = recovery;
        this.consumers = consumers;
        this.uploadWorker = uploadWorker;
    }

    @Override
    public void run(ApplicationArguments args) {
        recovery.run();
        consumers.start();
        uploadWorker.start();
    }
}
