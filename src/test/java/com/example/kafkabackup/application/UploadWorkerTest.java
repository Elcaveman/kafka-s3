package com.example.kafkabackup.application;

import static com.example.kafkabackup.application.Fixtures.NEXT_WINDOW;
import static com.example.kafkabackup.application.Fixtures.WINDOW;
import static com.example.kafkabackup.application.Fixtures.batch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.example.kafkabackup.domain.Manifest;
import com.example.kafkabackup.domain.SealedFile;
import com.example.kafkabackup.infrastructure.config.BackupProperties;
import com.example.kafkabackup.infrastructure.file.WorkDir;
import com.example.kafkabackup.infrastructure.metrics.BackupMetrics;
import com.example.kafkabackup.infrastructure.s3.S3Archive;
import com.example.kafkabackup.infrastructure.s3.S3Keys;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;
import org.springframework.kafka.support.Acknowledgment;

class UploadWorkerTest {

    @TempDir
    Path dir;

    private final ObjectMapper json = new ObjectMapper();
    private final S3Archive s3 = mock(S3Archive.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private BackupProperties properties;
    private WorkDir workDir;
    private BatchWriter writer;
    private UploadWorker uploader;

    @BeforeEach
    void setUp() throws IOException {
        properties = Fixtures.properties(dir);
        workDir = new WorkDir(properties);
        writer = newWriter();
        uploader = new UploadWorker(workDir, s3, writer, new BackupMetrics(registry, workDir), json);
    }

    private BatchWriter newWriter() {
        return new BatchWriter(
                workDir, new S3Keys(properties), properties, new BackupMetrics(registry, workDir), json);
    }

    private SealedFile sealOneFile() {
        writer.append(batch("orders", 0, 0, 1, 2), ack);
        return writer.seal("orders", WINDOW).orElseThrow();
    }

    private Manifest manifestOf(SealedFile sealed) throws IOException {
        return json.readValue(sealed.manifest().toFile(), Manifest.class);
    }

    @Test
    void uploadsDataThenManifestThenAcksThenDeletesTheLocalFiles() throws Exception {
        SealedFile sealed = sealOneFile();
        Manifest manifest = manifestOf(sealed);
        doAnswer(invocation -> {
            assertThat(sealed.data()).as("local copy must outlive the commit request").exists();
            assertThat(sealed.manifest()).exists();
            return null;
        }).when(ack).acknowledge();

        assertThat(uploader.uploadPending()).isTrue();

        InOrder order = inOrder(s3, ack);
        order.verify(s3).put(manifest.file(), sealed.data());
        order.verify(s3).put(S3Keys.manifestFor(manifest.file()), sealed.manifest());
        order.verify(ack).acknowledge();
        assertThat(sealed.data()).doesNotExist();
        assertThat(sealed.manifest()).doesNotExist();
        assertThat(registry.get("kb_upload_success_total").tag("topic", "orders").counter().count()).isEqualTo(1);
    }

    @Test
    void aFailedDataUploadCommitsNothingAndKeepsTheFile() throws Exception {
        SealedFile sealed = sealOneFile();
        Manifest manifest = manifestOf(sealed);
        doThrow(new RuntimeException("S3 down")).when(s3).put(eq(manifest.file()), any());

        assertThatThrownBy(uploader::uploadPending).hasMessage("S3 down");

        verify(s3, never()).put(eq(S3Keys.manifestFor(manifest.file())), any());
        verifyNoInteractions(ack);
        assertThat(sealed.data()).exists();
        assertThat(sealed.manifest()).exists();
        assertThat(registry.get("kb_upload_failure_total").tag("topic", "orders").counter().count()).isEqualTo(1);
    }

    @Test
    void aFailedManifestUploadCommitsNothingAndKeepsTheFile() throws Exception {
        SealedFile sealed = sealOneFile();
        Manifest manifest = manifestOf(sealed);
        doThrow(new RuntimeException("S3 down")).when(s3).put(eq(S3Keys.manifestFor(manifest.file())), any());

        assertThatThrownBy(uploader::uploadPending).hasMessage("S3 down");

        verifyNoInteractions(ack);
        assertThat(sealed.data()).exists();
        assertThat(sealed.manifest()).exists();
    }

    @Test
    void aRetryReusesTheSameKeysAndAcksOnce() throws Exception {
        SealedFile sealed = sealOneFile();
        Manifest manifest = manifestOf(sealed);
        doThrow(new RuntimeException("S3 down")).doNothing().when(s3).put(eq(manifest.file()), any());

        assertThatThrownBy(uploader::uploadPending).hasMessage("S3 down");
        assertThat(uploader.uploadPending()).isTrue();

        verify(s3, times(2)).put(manifest.file(), sealed.data());
        verify(ack).acknowledge();
        assertThat(sealed.data()).doesNotExist();
    }

    @Test
    void aFileSealedBeforeARestartIsUploadedAndDeletedWithoutACommit() throws Exception {
        SealedFile sealed = sealOneFile();
        Manifest manifest = manifestOf(sealed);

        BatchWriter restartedWriter = newWriter();
        UploadWorker restarted = new UploadWorker(
                workDir, s3, restartedWriter, new BackupMetrics(registry, workDir), json);

        assertThat(restarted.uploadPending()).isTrue();

        verify(s3).put(manifest.file(), sealed.data());
        verify(s3).put(S3Keys.manifestFor(manifest.file()), sealed.manifest());
        verifyNoInteractions(ack);
        assertThat(sealed.data()).doesNotExist();
    }

    @Test
    void olderWindowsAreUploadedAndAckedFirst() throws Exception {
        Acknowledgment later = mock(Acknowledgment.class);
        writer.append(batch("orders", 0, 0), ack);
        writer.seal("orders", WINDOW);
        writer.append(batch("orders", 0, 1), later);
        writer.seal("orders", NEXT_WINDOW);

        uploader.uploadPending();

        InOrder order = inOrder(ack, later);
        order.verify(ack).acknowledge();
        order.verify(later).acknowledge();
    }

    @Test
    void aFailureStopsTheRoundSoLaterFilesAreNotCommittedFirst() throws Exception {
        Acknowledgment later = mock(Acknowledgment.class);
        writer.append(batch("orders", 0, 0), ack);
        SealedFile first = writer.seal("orders", WINDOW).orElseThrow();
        writer.append(batch("orders", 0, 1), later);
        SealedFile second = writer.seal("orders", NEXT_WINDOW).orElseThrow();
        doThrow(new RuntimeException("S3 down")).when(s3).put(eq(manifestOf(first).file()), any());

        assertThatThrownBy(uploader::uploadPending).hasMessage("S3 down");

        verify(s3, never()).put(eq(manifestOf(second).file()), any());
        verifyNoInteractions(ack, later);
    }

    @Test
    void reportsWhenThereIsNothingToUpload() throws Exception {
        assertThat(uploader.uploadPending()).isFalse();
        verifyNoInteractions(s3);
    }

    @Test
    void aDataFileWithoutItsManifestIsNotUploaded() throws Exception {
        // A crash between the move to sealed/ and the manifest write leaves a lone data file.
        doNothing().when(s3).put(any(), any());
        java.nio.file.Files.writeString(workDir.sealedData("orders", WINDOW.id(), "abc"), "orders-0-0\n");

        assertThat(uploader.uploadPending()).isFalse();
        verifyNoInteractions(s3);
    }
}
