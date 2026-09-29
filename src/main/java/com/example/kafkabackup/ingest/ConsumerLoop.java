package com.example.kafkabackup.ingest;

import com.example.kafkabackup.shared.BackupMetrics;
import com.example.kafkabackup.shared.BackupProperties;
import com.example.kafkabackup.shared.WorkDir;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

/** Polls Kafka on a dedicated thread, appends every batch to disk and commits on request. */
@Component
public class ConsumerLoop implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerLoop.class);

    private record PendingCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets, CompletableFuture<Void> done) {
    }

    private final ConsumerFactory<String, byte[]> consumerFactory;
    private final BatchWriter batchWriter;
    private final BackupProperties properties;
    private final WorkDir workDir;
    private final BackupMetrics metrics;

    private final Queue<PendingCommit> commits = new ConcurrentLinkedQueue<>();
    private final ExecutorService executor =
            Executors.newSingleThreadExecutor(r -> new Thread(r, "kafka-consumer"));

    private volatile Consumer<String, byte[]> consumer;
    private volatile boolean running;
    private boolean paused;

    public ConsumerLoop(
            ConsumerFactory<String, byte[]> consumerFactory,
            BatchWriter batchWriter,
            BackupProperties properties,
            WorkDir workDir,
            BackupMetrics metrics) {
        this.consumerFactory = consumerFactory;
        this.batchWriter = batchWriter;
        this.properties = properties;
        this.workDir = workDir;
        this.metrics = metrics;
    }

    public void start() {
        running = true;
        executor.submit(this::run);
    }

    /** A KafkaConsumer may only be used from its own thread, so the uploader asks for commits here. */
    public CompletableFuture<Void> commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        commits.add(new PendingCommit(offsets, done));
        return done;
    }

    private void run() {
        consumer = consumerFactory.createConsumer(null, null, null, batchOverrides());
        consumer.subscribe(properties.topics());
        log.info("Consuming {}", properties.topics());
        try {
            while (running) {
                batchWriter.append(consumer.poll(properties.batch().pollTimeout()));
                drainCommits();
                applyBackpressure();
            }
        } catch (WakeupException e) {
            log.info("Consumer woken up for shutdown");
        } catch (Exception e) {
            log.error("Consumer loop stopped", e);
            failPendingCommits(e);
        } finally {
            consumer.close();
        }
    }

    private void drainCommits() {
        PendingCommit pending;
        while ((pending = commits.poll()) != null) {
            try {
                consumer.commitSync(pending.offsets());
                pending.done().complete(null);
            } catch (Exception e) {
                pending.offsets().keySet().forEach(tp -> metrics.commitFailure(tp.topic()));
                pending.done().completeExceptionally(e);
            }
        }
    }

    private void failPendingCommits(Throwable cause) {
        PendingCommit pending;
        while ((pending = commits.poll()) != null) {
            pending.done().completeExceptionally(cause);
        }
    }

    /** Stop consuming when the disk fills up or an offset gap was seen; never drop data. */
    private void applyBackpressure() {
        boolean shouldPause = batchWriter.gapDetected()
                || workDir.diskUsedRatio() > properties.disk().pauseAbove();
        boolean canResume = !batchWriter.gapDetected()
                && workDir.diskUsedRatio() < properties.disk().resumeBelow();

        Collection<TopicPartition> assignment = consumer.assignment();
        if (!paused && shouldPause) {
            consumer.pause(assignment);
            paused = true;
            log.warn("Consumer paused (disk {}, gap {})", workDir.diskUsedRatio(), batchWriter.gapDetected());
        } else if (paused && canResume) {
            consumer.resume(assignment);
            paused = false;
            log.info("Consumer resumed");
        }
    }

    private Properties batchOverrides() {
        Properties overrides = new Properties();
        overrides.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.batch().maxRecords());
        overrides.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, (int) properties.batch().maxBytes().toBytes());
        return overrides;
    }

    @Override
    public void close() {
        running = false;
        if (consumer != null) {
            consumer.wakeup();
        }
        executor.shutdown();
    }
}
