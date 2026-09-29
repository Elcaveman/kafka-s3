package com.example.kafkabackup;

import java.nio.file.Path;

/** A data file and its manifest waiting in {@code sealed/}: not yet uploaded and committed. */
public record SealedFile(Path data, Path manifest) {
}
