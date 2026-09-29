package com.example.kafkabackup.application;

import static com.example.kafkabackup.application.Fixtures.NEXT_WINDOW;
import static com.example.kafkabackup.application.Fixtures.WINDOW;
import static com.example.kafkabackup.application.Fixtures.batch;
import static com.example.kafkabackup.application.Fixtures.concat;
import static com.example.kafkabackup.application.Fixtures.lines;
import static com.example.kafkabackup.application.Fixtures.value;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.example.kafkabackup.domain.Manifest;
import com.example.kafkabackup.domain.SealedFile;
import com.example.kafkabackup.infrastructure.config.BackupProperties;
import com.example.kafkabackup.infrastructure.file.WorkDir;
import com.example.kafkabackup.infrastructure.metrics.BackupMetrics;
import com.example.kafkabackup.infrastructure.s3.S3Keys;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;
import org.springframework.kafka.support.Acknowledgment;

class BatchWriterTest {

    @TempDir
    Path dir;

    private final ObjectMapper json = new ObjectMapper();
    private final Acknowledgment ack1 = mock(Acknowledgment.class);
    private final Acknowledgment ack2 = mock(Acknowledgment.class);
    private final Acknowledgment ack3 = mock(Acknowledgment.class);

    private WorkDir workDir;
    private BatchWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        writer = newWriter(true);
    }

    /** A new instance on the same work dir behaves like the process after a restart. */
    private BatchWriter newWriter(boolean gapCheck) throws IOException {
        BackupProperties properties = Fixtures.properties(dir, gapCheck, List.of("orders"));
        workDir = new WorkDir(properties);
        return new BatchWriter(
                workDir,
                new S3Keys(properties),
                properties,
                new BackupMetrics(new SimpleMeterRegistry(), workDir),
                json);
    }

    @Test
    void sealedFileHoldsEveryRecordAndItsManifestListsEveryOffset() throws IOException {
        writer.append(concat(batch("orders", 0, 0, 1, 2), batch("orders", 1, 5, 6)), ack1);

        SealedFile sealed = writer.seal("orders", WINDOW).orElseThrow();

        assertThat(lines(sealed.data())).containsExactly(
                value("orders", 0, 0), value("orders", 0, 1), value("orders", 0, 2),
                value("orders", 1, 5), value("orders", 1, 6));
        Manifest manifest = json.readValue(sealed.manifest().toFile(), Manifest.class);
        assertThat(manifest.topic()).isEqualTo("orders");
        assertThat(manifest.window()).isEqualTo(WINDOW.label());
        assertThat(manifest.records()).isEqualTo(5);
        assertThat(manifest.offsets()).containsExactlyInAnyOrderEntriesOf(
                Map.of(0, List.of(0L, 1L, 2L), 1, List.of(5L, 6L)));
        assertThat(manifest.file()).startsWith("my-app/2026-09-29/orders-").endsWith(".log");
        assertThat(workDir.active("orders")).doesNotExist();
    }

    @Test
    void recordsAppendedAfterASealGoToTheNextFile() {
        writer.append(batch("orders", 0, 0, 1), ack1);
        SealedFile first = writer.seal("orders", WINDOW).orElseThrow();
        writer.append(batch("orders", 0, 2), ack2);
        SealedFile second = writer.seal("orders", NEXT_WINDOW).orElseThrow();

        assertThat(lines(first.data())).containsExactly(value("orders", 0, 0), value("orders", 0, 1));
        assertThat(lines(second.data())).containsExactly(value("orders", 0, 2));
    }

    @Test
    void sealingWithNothingAppendedProducesNoFile() {
        assertThat(writer.seal("orders", WINDOW)).isEmpty();
        assertThat(workDir.pendingSealed()).isEmpty();
    }

    @Test
    void nothingIsAcknowledgedUntilTheUploaderAsks() {
        writer.append(batch("orders", 0, 0, 1), ack1);
        writer.seal("orders", WINDOW);

        verifyNoInteractions(ack1);
    }

    @Test
    void acknowledgeAcksTheLatestBatchOfEveryPartitionOldestFirst() {
        writer.append(concat(batch("orders", 0, 0), batch("orders", 1, 0)), ack1);
        writer.append(batch("orders", 0, 1), ack2);
        SealedFile sealed = writer.seal("orders", WINDOW).orElseThrow();

        assertThat(writer.acknowledge(sealed)).isTrue();

        // ack1 is still the latest batch for partition 1: skipping it would leave partition 1 uncommitted.
        InOrder order = inOrder(ack1, ack2);
        order.verify(ack1).acknowledge();
        order.verify(ack2).acknowledge();
        verifyNoMoreInteractions(ack1, ack2);
    }

    @Test
    void aBatchSupersededOnEveryPartitionIsNotAcked() {
        writer.append(batch("orders", 0, 0), ack1);
        writer.append(batch("orders", 0, 1), ack2);
        SealedFile sealed = writer.seal("orders", WINDOW).orElseThrow();

        writer.acknowledge(sealed);

        verify(ack1, never()).acknowledge();
        verify(ack2).acknowledge();
    }

    @Test
    void eachFileAcksOnlyItsOwnBatches() {
        writer.append(batch("orders", 0, 0), ack1);
        SealedFile first = writer.seal("orders", WINDOW).orElseThrow();
        writer.append(batch("orders", 0, 1), ack2);
        SealedFile second = writer.seal("orders", NEXT_WINDOW).orElseThrow();

        writer.acknowledge(first);
        verify(ack1).acknowledge();
        verifyNoInteractions(ack2);

        writer.acknowledge(second);
        verify(ack2).acknowledge();
    }

    @Test
    void aNewerFileUploadedFirstWaitsForTheOlderOne() {
        // Two seals in the same hour: upload order follows the random uid, not the seal order.
        writer.append(batch("orders", 0, 0), ack1);
        SealedFile older = writer.seal("orders", WINDOW).orElseThrow();
        writer.append(batch("orders", 0, 1), ack2);
        SealedFile newer = writer.seal("orders", WINDOW).orElseThrow();

        assertThat(writer.acknowledge(newer)).isTrue();
        verifyNoInteractions(ack1, ack2); // committing now would skip past the older file's records

        assertThat(writer.acknowledge(older)).isTrue();
        InOrder order = inOrder(ack1, ack2);
        order.verify(ack1).acknowledge();
        order.verify(ack2).acknowledge();
    }

    @Test
    void topicsDoNotWaitForEachOther() {
        writer.append(batch("orders", 0, 0), ack1);
        writer.seal("orders", WINDOW);
        writer.append(batch("payments", 0, 0), ack2);
        SealedFile payments = writer.seal("payments", WINDOW).orElseThrow();

        writer.acknowledge(payments);

        verify(ack2).acknowledge();
        verifyNoInteractions(ack1);
    }

    @Test
    void acknowledgeAcksAFileOnlyOnce() {
        writer.append(batch("orders", 0, 0), ack1);
        SealedFile sealed = writer.seal("orders", WINDOW).orElseThrow();

        assertThat(writer.acknowledge(sealed)).isTrue();
        assertThat(writer.acknowledge(sealed)).isFalse();

        verify(ack1).acknowledge();
    }

    @Test
    void aFileSealedBeforeARestartHasNoAcks() throws IOException {
        writer.append(batch("orders", 0, 0), ack1);
        writer.seal("orders", WINDOW);

        BatchWriter restarted = newWriter(true);
        SealedFile leftover = workDir.pendingSealed().get(0);

        assertThat(restarted.acknowledge(leftover)).isFalse();
        verifyNoInteractions(ack1);
    }

    @Test
    void aBatchMixingTopicsIsRejectedBeforeAnythingIsWritten() {
        var mixed = concat(batch("orders", 0, 0), batch("payments", 0, 0));

        assertThatThrownBy(() -> writer.append(mixed, ack1)).isInstanceOf(IllegalStateException.class);

        assertThat(writer.seal("orders", WINDOW)).isEmpty();
        assertThat(writer.seal("payments", WINDOW)).isEmpty();
        verifyNoInteractions(ack1);
    }

    @Test
    void anOffsetGapWithinABatchIsDetected() {
        writer.append(batch("orders", 0, 0, 1, 3), ack1);

        assertThat(writer.gapDetected()).isTrue();
    }

    @Test
    void anOffsetGapAcrossBatchesIsDetected() {
        writer.append(batch("orders", 0, 0, 1), ack1);
        assertThat(writer.gapDetected()).isFalse();

        writer.append(batch("orders", 0, 3), ack2);
        assertThat(writer.gapDetected()).isTrue();
    }

    @Test
    void offsetsAreTrackedPerPartition() {
        writer.append(concat(batch("orders", 0, 0, 1), batch("orders", 1, 0, 1)), ack1);
        writer.append(concat(batch("orders", 1, 2), batch("orders", 0, 2)), ack2);

        assertThat(writer.gapDetected()).isFalse();
    }

    @Test
    void gapCheckCanBeTurnedOffForCompactedTopics() throws IOException {
        BatchWriter compacted = newWriter(false);

        compacted.append(batch("orders", 0, 0, 5, 9), ack1);

        assertThat(compacted.gapDetected()).isFalse();
    }
}
