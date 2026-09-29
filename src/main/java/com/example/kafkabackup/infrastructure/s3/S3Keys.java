package com.example.kafkabackup.infrastructure.s3;

import com.example.kafkabackup.infrastructure.config.BackupProperties;
import org.springframework.stereotype.Component;

/**
 * Keys are derived from the sealed file's UID, which is generated once at seal time and kept in
 * the local file name. A retried upload therefore reuses the same key and simply overwrites.
 */
@Component
public class S3Keys {

    private static final String LOG_SUFFIX = ".log";
    private static final String MANIFEST_SUFFIX = ".manifest.json";

    private final String applicationName;

    public S3Keys(BackupProperties properties) {
        this.applicationName = properties.applicationName();
    }

    public String data(String topic, String date, String uid) {
        return "%s/%s/%s-%s%s".formatted(applicationName, date, topic, uid, LOG_SUFFIX);
    }

    public static String manifestFor(String dataKey) {
        return dataKey.substring(0, dataKey.length() - LOG_SUFFIX.length()) + MANIFEST_SUFFIX;
    }
}
