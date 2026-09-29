package com.example.kafkabackup.manifest;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ManifestUploaderTest {

    @Test
    void manifestKeySitsNextToItsDataFile() {
        assertThat(ManifestUploader.keyFor("my-app/2026-09-28/orders-a1b2c3d4.log"))
                .isEqualTo("my-app/2026-09-28/orders-a1b2c3d4.manifest.json");
    }
}
