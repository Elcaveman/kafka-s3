package com.example.kafkabackup.manifest;

import com.example.kafkabackup.datafile.DataFileUploader;
import com.example.kafkabackup.shared.S3Archive;
import java.nio.file.Path;
import org.springframework.stereotype.Component;

/** Owns the manifest's S3 key and its upload; the key sits next to the data file it describes. */
@Component
public class ManifestUploader {

    private static final String SUFFIX = ".manifest.json";

    private final S3Archive s3;

    public ManifestUploader(S3Archive s3) {
        this.s3 = s3;
    }

    public void upload(Manifest manifest, Path file) {
        s3.put(keyFor(manifest.file()), file);
    }

    static String keyFor(String dataKey) {
        return dataKey.substring(0, dataKey.length() - DataFileUploader.SUFFIX.length()) + SUFFIX;
    }
}
