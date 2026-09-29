package com.example.kafkabackup.application;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * A KafkaConsumer may only be used from its own thread, so the uploader hands commits here and
 * the consumer loop performs them between polls.
 */
@Component
public class CommitQueue {

    public record PendingCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets, CompletableFuture<Void> done) {
    }

    private final ConcurrentLinkedQueue<PendingCommit> queue = new ConcurrentLinkedQueue<>();

    public CompletableFuture<Void> submit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        queue.add(new PendingCommit(offsets, done));
        return done;
    }

    public PendingCommit poll() {
        return queue.poll();
    }

    public void failAll(Throwable cause) {
        PendingCommit pending;
        while ((pending = queue.poll()) != null) {
            pending.done().completeExceptionally(cause);
        }
    }
}
