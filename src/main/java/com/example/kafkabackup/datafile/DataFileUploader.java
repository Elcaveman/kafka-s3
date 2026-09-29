package com.example.kafkabackup.datafile;

import com.example.kafkabackup.shared.BackupProperties;
import com.example.kafkabackup.shared.S3Archive;
import java.nio.file.Path;
import org.springframework.stereotype.Component;

/** Owns the raw message file: its S3 key layout and its upload. */
@Component
public class DataFileUploader {

    public static final String SUFFIX = ".log";

    private final S3Archive s3;
    private final String applicationName;

    public DataFileUploader(S3Archive s3, BackupProperties properties) {
        this.s3 = s3;
        this.applicationName = properties.applicationName();
    }

    /** Built once at seal time and stored in the manifest, so a retry overwrites the same key (I4). */
    public String key(String topic, String date, String uid) {
        return "%s/%s/%s-%s%s".formatted(applicationName, date, topic, uid, SUFFIX);
    }

    public void upload(String key, Path file) {
        s3.put(key, file);
    }
}
