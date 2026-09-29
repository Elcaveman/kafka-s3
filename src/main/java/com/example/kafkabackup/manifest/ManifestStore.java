package com.example.kafkabackup.manifest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import org.springframework.stereotype.Component;

/** The manifest's on-disk JSON form, written at seal time and read back by the uploader. */
@Component
public class ManifestStore {

    private final ObjectMapper json;

    public ManifestStore(ObjectMapper json) {
        this.json = json;
    }

    public Manifest read(Path file) throws IOException {
        return json.readValue(file.toFile(), Manifest.class);
    }

    public void write(Manifest manifest, Path file) throws IOException {
        json.writeValue(file.toFile(), manifest);
    }
}
