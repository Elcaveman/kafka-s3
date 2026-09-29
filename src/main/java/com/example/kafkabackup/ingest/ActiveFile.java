package com.example.kafkabackup.ingest;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The one file currently being appended to for a topic, plus the offsets it already contains. */
class ActiveFile implements Closeable {

    private static final byte[] NEWLINE = {'\n'};

    private final Path path;
    private final FileChannel channel;
    private final Map<Integer, List<Long>> offsets = new LinkedHashMap<>();
    private int records;

    ActiveFile(Path path) throws IOException {
        this.path = path;
        this.channel = FileChannel.open(
                path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    /** Writes exactly one line per record so line N of a partition matches its N-th offset. */
    void append(byte[] value, int partition, long offset) throws IOException {
        write(value == null ? new byte[0] : value);
        write(NEWLINE);
        offsets.computeIfAbsent(partition, p -> new ArrayList<>()).add(offset);
        records++;
    }

    void fsync() throws IOException {
        channel.force(false);
    }

    Path path() {
        return path;
    }

    int records() {
        return records;
    }

    Map<Integer, List<Long>> offsets() {
        return Map.copyOf(offsets);
    }

    @Override
    public void close() throws IOException {
        channel.force(true);
        channel.close();
    }

    void deleteIfEmpty() throws IOException {
        if (records == 0) {
            Files.deleteIfExists(path);
        }
    }

    private void write(byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }
}
