package com.example.kafkabackup.application;

import static com.example.kafkabackup.application.Fixtures.WINDOW;
import static com.example.kafkabackup.application.Fixtures.batch;
import static com.example.kafkabackup.application.Fixtures.concat;
import static com.example.kafkabackup.application.Fixtures.lines;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.example.kafkabackup.application.MockConsumerFactory.RecordingConsumer;
import com.example.kafkabackup.domain.SealedFile;
import com.example.kafkabackup.infrastructure.config.BackupProperties;
import com.example.kafkabackup.infrastructure.file.WorkDir;
import com.example.kafkabackup.infrastructure.metrics.BackupMetrics;
import com.example.kafkabackup.infrastructure.s3.S3Keys;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopicConsumersTest {

    private static final TopicPartition P0 = new TopicPartition("orders", 0);
    private static final TopicPartition P1 = new TopicPartition("orders", 1);

    @TempDir
    Path dir;

    private final MockConsumerFactory factory = new MockConsumerFactory();
    private final WorkDir disk = mock(WorkDir.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private BackupProperties properties;
    private WorkDir workDir;
    private BatchWriter writer;
    private TopicConsumers consumers;

    @BeforeEach
    void setUp() throws IOException {
        properties = Fixtures.properties(dir);
        workDir = new WorkDir(properties);
        writer = new BatchWriter(
                workDir, new S3Keys(properties), properties, new BackupMetrics(registry, workDir), new ObjectMapper());
        when(disk.diskUsedRatio()).thenReturn(0.5);
    }

    @AfterEach
    void tearDown() {
        if (consumers != null) {
            consumers.close();
        }
    }

    private void start(BatchWriter batchWriter, BackupProperties props) {
        consumers = new TopicConsumers(factory, batchWriter, props, disk, new BackupMetrics(registry, workDir));
        consumers.start();
    }

    private RecordingConsumer consumer(String instanceId) {
        return await().until(() -> factory.consumers.get(instanceId), Objects::nonNull);
    }

    /** Assigns the records' partitions and delivers the records, on the consumer's own thread. */
    private void deliverFirst(RecordingConsumer consumer, List<ConsumerRecord<String, byte[]>> records) {
        Set<TopicPartition> partitions = records.stream()
                .map(r -> new TopicPartition(r.topic(), r.partition()))
                .collect(Collectors.toSet());
        consumer.schedulePollTask(() -> {
            consumer.updateBeginningOffsets(partitions.stream().collect(Collectors.toMap(Function.identity(), p -> 0L)));
            consumer.rebalance(partitions);
            records.forEach(consumer::addRecord);
        });
    }

    private void deliverMore(RecordingConsumer consumer, List<ConsumerRecord<String, byte[]>> records) {
        consumer.schedulePollTask(() -> records.forEach(consumer::addRecord));
    }

    private int activeLines() {
        return lines(workDir.active("orders")).size();
    }

    @Test
    void offsetsAreCommittedOnlyAfterTheUploaderAcknowledges() {
        start(writer, properties);
        RecordingConsumer consumer = consumer("local-orders");

        deliverFirst(consumer, concat(batch("orders", 0, 0, 1, 2), batch("orders", 1, 0, 1)));
        await().until(() -> activeLines() == 5);
        // A second batch that only holds partition 0: partition 1 must still get committed.
        deliverMore(consumer, batch("orders", 0, 3));
        await().until(() -> activeLines() == 6);

        SealedFile sealed = writer.seal("orders", WINDOW).orElseThrow();
        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                .until(() -> consumer.commits.isEmpty());

        writer.acknowledge(sealed);

        await().untilAsserted(() -> assertThat(consumer.commits)
                .containsExactlyInAnyOrderEntriesOf(Map.of(P0, 4L, P1, 2L)));
    }

    @Test
    void recordsOfAnUnsealedFileAreNeverCommitted() {
        start(writer, properties);
        RecordingConsumer consumer = consumer("local-orders");

        deliverFirst(consumer, batch("orders", 0, 0, 1));
        await().until(() -> activeLines() == 2);
        SealedFile sealed = writer.seal("orders", WINDOW).orElseThrow();
        deliverMore(consumer, batch("orders", 0, 2, 3));
        await().until(() -> activeLines() == 2); // new active file holds offsets 2 and 3

        writer.acknowledge(sealed);

        await().untilAsserted(() -> assertThat(consumer.commits).containsExactlyEntriesOf(Map.of(P0, 2L)));
        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                .until(() -> consumer.commits.get(P0) == 2L);
    }

    @Test
    void eachTopicGetsItsOwnConsumerAndStaticMemberId() {
        start(writer, Fixtures.properties(dir, true, List.of("orders", "payments")));

        RecordingConsumer orders = consumer("local-orders");
        RecordingConsumer payments = consumer("local-payments");

        await().untilAsserted(() -> {
            assertThat(orders.subscription()).containsExactly("orders");
            assertThat(payments.subscription()).containsExactly("payments");
        });
        Map<String, Object> config = factory.configs.get("local-orders");
        assertThat(config).containsEntry(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        assertThat(config).containsEntry(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 4 * 1024 * 1024);
    }

    @Test
    void aFullDiskPausesConsumptionUntilItDropsBelowTheResumeMark() {
        start(writer, properties);
        RecordingConsumer consumer = consumer("local-orders");
        deliverFirst(consumer, batch("orders", 0, 0));
        await().until(() -> consumer.assignment().contains(P0));

        when(disk.diskUsedRatio()).thenReturn(0.9);
        consumers.applyBackpressure();
        await().until(() -> consumer.paused().contains(P0));

        when(disk.diskUsedRatio()).thenReturn(0.75); // between resume (0.7) and pause (0.8)
        consumers.applyBackpressure();
        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                .until(() -> consumer.paused().contains(P0));

        when(disk.diskUsedRatio()).thenReturn(0.5);
        consumers.applyBackpressure();
        await().until(() -> consumer.paused().isEmpty());
    }

    @Test
    void anOffsetGapPausesConsumptionEvenWithFreeDisk() {
        start(writer, properties);
        RecordingConsumer consumer = consumer("local-orders");

        deliverFirst(consumer, batch("orders", 0, 0, 1, 3));
        await().until(writer::gapDetected);

        consumers.applyBackpressure();
        await().until(() -> consumer.paused().contains(P0));

        consumers.applyBackpressure();
        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                .until(() -> consumer.paused().contains(P0));
    }

    @Test
    void aBatchThatCannotBeWrittenStopsTheConsumerAndCommitsNothing() {
        BatchWriter failing = spy(writer);
        doThrow(new UncheckedIOException(new IOException("disk failure"))).when(failing).append(anyList(), any());
        start(failing, properties);
        RecordingConsumer consumer = consumer("local-orders");

        deliverFirst(consumer, batch("orders", 0, 0, 1));

        await().until(consumer::closed);
        assertThat(consumer.commits).isEmpty();
    }
}
