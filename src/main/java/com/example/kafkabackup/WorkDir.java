package com.example.kafkabackup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.springframework.stereotype.Component;

/**
 * <pre>
 * work-dir/
 *   active/&lt;topic&gt;.log
 *   sealed/&lt;topic&gt;_&lt;yyyyMMddHH&gt;_&lt;uid&gt;.log (+ .manifest.json)
 * </pre>
 */
@Component
public class WorkDir {

    private static final String MANIFEST_SUFFIX = ".manifest.json";

    private final Path root;
    private final Path activeDir;
    private final Path sealedDir;

    public WorkDir(BackupProperties properties) throws IOException {
        this.root = Path.of(properties.workDir());
        this.activeDir = root.resolve("active");
        this.sealedDir = root.resolve("sealed");
        Files.createDirectories(activeDir);
        Files.createDirectories(sealedDir);
    }

    public Path active(String topic) {
        return activeDir.resolve(topic + ".log");
    }

    public Path sealedData(String topic, String windowId, String uid) {
        return sealedDir.resolve("%s_%s_%s.log".formatted(topic, windowId, uid));
    }

    public Path sealedManifest(String topic, String windowId, String uid) {
        return sealedDir.resolve("%s_%s_%s%s".formatted(topic, windowId, uid, MANIFEST_SUFFIX));
    }

    /** Only pairs with a manifest are complete; a lone data file means the seal never finished. */
    public List<SealedFile> pendingSealed() {
        try (Stream<Path> files = Files.list(sealedDir)) {
            List<SealedFile> pending = new ArrayList<>();
            files.filter(p -> p.getFileName().toString().endsWith(MANIFEST_SUFFIX))
                    .sorted(Comparator.comparing(Path::getFileName))
                    .forEach(manifest -> pending.add(new SealedFile(dataFor(manifest), manifest)));
            return pending;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public int pendingCount() {
        return pendingSealed().size();
    }

    /** Anything in active/ was never sealed, so it is replayed from Kafka after a crash. */
    public void discardActiveFiles() {
        try (Stream<Path> files = Files.list(activeDir)) {
            files.forEach(WorkDir::deleteQuietly);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public double diskUsedRatio() {
        try {
            FileStore store = Files.getFileStore(root);
            long total = store.getTotalSpace();
            return total == 0 ? 0 : 1.0 - ((double) store.getUsableSpace() / total);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path dataFor(Path manifest) {
        String name = manifest.getFileName().toString();
        return sealedDir.resolve(name.substring(0, name.length() - MANIFEST_SUFFIX.length()) + ".log");
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
