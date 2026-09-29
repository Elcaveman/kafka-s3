package com.example.kafkabackup;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.kafkabackup.application.FileRotator;
import com.example.kafkabackup.domain.Manifest;
import com.example.kafkabackup.infrastructure.config.BackupProperties;
import com.example.kafkabackup.infrastructure.s3.S3Archive;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * End-to-end data-loss checks against a real Kafka broker and S3 (LocalStack). Needs Docker.
 *
 * <p>Named {@code *IT} so a plain {@code mvn test} skips it. Run it with {@code mvn test -Dtest=DataLossIT}.
 *
 * <p>Every scenario produces uniquely valued messages, pushes them through the real application, and
 * then checks:
 * <ul>
 *   <li>every produced record is in S3, byte for byte, in a file whose manifest lists its offset;</li>
 *   <li>at no point is an offset committed whose record is not yet in S3 (invariant I1);</li>
 *   <li>the committed offsets finally reach the end of every partition.</li>
 * </ul>
 * Duplicates are allowed after a crash (at-least-once), loss never is.
 *
 * <p>Crashes are simulated by closing the application context. That is not a {@code kill -9}, but it
 * leaves the same on-disk and Kafka state: an unacked batch is never committed on close, and active
 * and sealed files are left exactly where they were.
 */
@Testcontainers
class DataLossIT {

    private static final int PARTITIONS = 3;

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

    @Container
    static final LocalStackContainer localstack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices(LocalStackContainer.Service.S3);

    private static S3Client s3;
    private static AdminClient admin;
    private static KafkaProducer<String, String> producer;

    private final ObjectMapper json = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /** Every record produced so far, by where it landed in Kafka. */
    private final Map<Position, String> produced = new ConcurrentHashMap<>();

    @TempDir
    Path workDir;

    private String topic;
    private String group;
    private String bucket;
    private ConfigurableApplicationContext app;

    record Position(int partition, long offset) {
    }

    @BeforeAll
    static void clients() {
        s3 = S3Client.builder()
                .endpointOverride(localstack.getEndpoint())
                .forcePathStyle(true)
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();
        admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer = new KafkaProducer<>(props);
    }

    @AfterAll
    static void closeClients() {
        producer.close();
        admin.close();
        s3.close();
    }

    @BeforeEach
    void freshTopicGroupAndBucket() throws Exception {
        String id = UUID.randomUUID().toString().substring(0, 8);
        topic = "orders-" + id;
        group = "backup-" + id;
        bucket = "backup-" + id;
        admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1))).all().get();
    }

    @AfterEach
    void stopApp() {
        if (app != null) {
            app.close();
        }
    }

    // ---------------------------------------------------------------- reception

    @Test
    void nothingIsLostOrDuplicatedWhileConsumingAndRotatingConcurrently() throws Exception {
        createBucket();
        app = startApp();

        CompletableFuture<Void> producing = CompletableFuture.runAsync(() -> produce(5_000));
        while (!producing.isDone()) {
            rotator().rotate();
            Thread.sleep(50);
        }
        producing.get();

        awaitEverythingBackedUp(false);
    }

    @Test
    void aCrashBeforeTheFileIsSealedReplaysItsRecords() throws Exception {
        createBucket();
        app = startApp();
        produce(500);
        await().atMost(Duration.ofSeconds(30)).until(() -> activeLines() == 500);

        crash();
        assertThat(committedOffsets()).as("nothing sealed, so nothing committed").isEmpty();

        app = startApp();
        produce(500);

        awaitEverythingBackedUp(true);
    }

    // ---------------------------------------------------------------- sending

    @Test
    void anS3OutageKeepsTheFilesAndCommitsNothingUntilItRecovers() throws Exception {
        app = startApp(); // the bucket does not exist yet: every upload fails
        produce(500);
        rotateUntilSealed();
        await().atMost(Duration.ofSeconds(30)).until(() -> uploadFailures() > 0);

        assertThat(sealedFiles()).isNotEmpty();
        assertThat(committedOffsets()).isEmpty();

        createBucket();
        awaitEverythingBackedUp(false);
        assertThat(sealedFiles()).isEmpty();
    }

    @Test
    void aCrashDuringAnS3OutageUploadsTheLeftoverFilesAfterRestart() throws Exception {
        app = startApp();
        produce(500);
        rotateUntilSealed();
        await().atMost(Duration.ofSeconds(30)).until(() -> uploadFailures() > 0);

        crash();
        assertThat(sealedFiles()).isNotEmpty();
        assertThat(committedOffsets()).isEmpty();

        createBucket();
        app = startApp();
        produce(500);

        awaitEverythingBackedUp(true);
        assertThat(sealedFiles()).isEmpty();
    }

    // ---------------------------------------------------------------- after sending

    @Test
    void aCrashAfterTheUploadButBeforeTheCommitReplaysTheRecords() throws Exception {
        createBucket();
        CrashAfterUpload.crashed = new CountDownLatch(1);
        app = startApp(CrashAfterUpload.class);
        produce(500);
        rotateUntilSealed();
        assertThat(CrashAfterUpload.crashed.await(30, TimeUnit.SECONDS)).isTrue();

        crash();
        assertThat(manifestsInS3()).as("the upload itself went through").isNotEmpty();
        assertThat(sealedFiles()).as("the local copy was not deleted").isNotEmpty();
        assertThat(committedOffsets()).as("the commit never happened").isEmpty();

        app = startApp();
        produce(500);

        awaitEverythingBackedUp(true);
        assertThat(sealedFiles()).isEmpty();
    }

    @Test
    void uploadedFilesMatchWhatWasProducedAcrossManyFiles() throws Exception {
        createBucket();
        app = startApp();
        for (int round = 0; round < 5; round++) {
            produce(300);
            rotator().rotate();
        }

        awaitEverythingBackedUp(false);
        assertThat(manifestsInS3()).hasSizeGreaterThan(1);
    }

    // ---------------------------------------------------------------- application

    /** Uploads normally, then dies right after the manifest is in S3: before the ack and the delete. */
    public static class CrashAfterUpload extends S3Archive {

        static volatile CountDownLatch crashed;

        public CrashAfterUpload(S3Client s3, BackupProperties properties) {
            super(s3, properties);
        }

        @Override
        public void put(String key, Path file) {
            super.put(key, file);
            if (key.endsWith(".manifest.json")) {
                crashed.countDown();
                throw new SimulatedCrash();
            }
        }
    }

    /** An Error, so the uploader's retry loop does not catch it and its thread dies. */
    static class SimulatedCrash extends Error {
    }

    private ConfigurableApplicationContext startApp(Class<?>... primaryOverrides) {
        return new SpringApplicationBuilder(KafkaBackupApplication.class)
                .initializers(ctx -> Arrays.stream(primaryOverrides).forEach(type ->
                        ((GenericApplicationContext) ctx).registerBean(type, bd -> bd.setPrimary(true))))
                .run(
                        "--spring.config.on-not-found=ignore",
                        "--spring.cloud.vault.enabled=false",
                        "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                        "--spring.kafka.consumer.group-id=" + group,
                        "--backup.topics=" + topic,
                        "--backup.work-dir=" + workDir,
                        "--backup.application-name=it",
                        "--backup.rotate-cron=-",
                        "--backup.batch.max-records=50",
                        "--backup.batch.poll-timeout=200ms",
                        "--backup.disk.pause-above=1.0",
                        "--backup.disk.resume-below=1.0",
                        "--backup.s3.bucket=" + bucket,
                        "--backup.s3.region=" + localstack.getRegion(),
                        "--aws.endpoint=" + localstack.getEndpoint(),
                        "--aws.access-key-id=" + localstack.getAccessKey(),
                        "--aws.secret-access-key=" + localstack.getSecretKey());
    }

    private void crash() {
        app.close();
        app = null;
    }

    private FileRotator rotator() {
        return app.getBean(FileRotator.class);
    }

    private double uploadFailures() {
        Counter counter = app.getBean(MeterRegistry.class).find("kb_upload_failure_total").counter();
        return counter == null ? 0 : counter.count();
    }

    // ---------------------------------------------------------------- Kafka

    /** Spreads messages over every partition and waits until the broker has them all. */
    private void produce(int count) {
        try {
            List<Future<RecordMetadata>> sends = new ArrayList<>();
            List<String> values = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                String value = "msg-" + UUID.randomUUID();
                values.add(value);
                sends.add(producer.send(new ProducerRecord<>(topic, i % PARTITIONS, null, value)));
            }
            producer.flush();
            for (int i = 0; i < count; i++) {
                RecordMetadata metadata = sends.get(i).get();
                produced.put(new Position(metadata.partition(), metadata.offset()), values.get(i));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> committedOffsets() throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets =
                admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
        offsets.values().removeIf(java.util.Objects::isNull);
        return offsets;
    }

    // ---------------------------------------------------------------- local files

    private long activeLines() throws Exception {
        Path active = workDir.resolve("active").resolve(topic + ".log");
        if (!Files.exists(active)) {
            return 0;
        }
        try (Stream<String> lines = Files.lines(active, UTF_8)) {
            return lines.count();
        }
    }

    private List<Path> sealedFiles() throws Exception {
        try (Stream<Path> files = Files.list(workDir.resolve("sealed"))) {
            return files.toList();
        }
    }

    private void rotateUntilSealed() {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).until(() -> {
            rotator().rotate();
            return !sealedFiles().isEmpty() || !manifestsInS3().isEmpty();
        });
    }

    // ---------------------------------------------------------------- S3

    private void createBucket() {
        s3.createBucket(b -> b.bucket(bucket));
    }

    private boolean bucketExists() {
        return s3.listBuckets().buckets().stream().anyMatch(b -> b.name().equals(bucket));
    }

    private List<String> manifestsInS3() {
        if (!bucketExists()) {
            return List.of();
        }
        return s3.listObjectsV2Paginator(b -> b.bucket(bucket)).contents().stream()
                .map(S3Object::key)
                .filter(key -> key.endsWith(".manifest.json"))
                .toList();
    }

    private String download(String key) {
        return s3.getObjectAsBytes(b -> b.bucket(bucket).key(key)).asString(UTF_8);
    }

    /** How many times each record is in S3, after checking every file against its manifest. */
    private Map<Position, Integer> recordsInS3() throws Exception {
        Map<Position, Integer> seen = new HashMap<>();
        for (String key : manifestsInS3()) {
            Manifest manifest = json.readValue(download(key), Manifest.class);
            List<String> lines = download(manifest.file()).lines().toList();
            assertThat(lines).as("%s line count", manifest.file()).hasSize(manifest.records());

            List<String> expected = new ArrayList<>();
            manifest.offsets().forEach((partition, offsets) -> offsets.forEach(offset -> {
                Position position = new Position(partition, offset);
                assertThat(produced).as("%s lists an offset nobody produced", key).containsKey(position);
                expected.add(produced.get(position));
                seen.merge(position, 1, Integer::sum);
            }));
            assertThat(lines).as("%s content", manifest.file()).containsExactlyInAnyOrderElementsOf(expected);
        }
        return seen;
    }

    // ---------------------------------------------------------------- the checks

    /** I1: every offset below a committed offset must already be in S3. Commits are read first. */
    private void assertNothingCommittedBeyondS3() throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = committedOffsets();
        Set<Position> inS3 = recordsInS3().keySet();
        committed.forEach((partition, offset) -> {
            Set<Position> committedButMissing = produced.keySet().stream()
                    .filter(p -> p.partition() == partition.partition() && p.offset() < offset.offset())
                    .filter(p -> !inS3.contains(p))
                    .collect(Collectors.toSet());
            assertThat(committedButMissing).as("committed on %s but not in S3", partition).isEmpty();
        });
    }

    private void assertEverythingBackedUp(boolean allowDuplicates) throws Exception {
        Map<Position, Integer> inS3 = recordsInS3();
        Set<Position> missing = produced.keySet().stream()
                .filter(p -> !inS3.containsKey(p))
                .collect(Collectors.toSet());
        assertThat(missing).as("records lost").isEmpty();
        if (!allowDuplicates) {
            assertThat(inS3).as("records duplicated").allSatisfy((p, count) -> assertThat(count).isEqualTo(1));
        }

        Map<Integer, Long> endOffsets = new HashMap<>();
        produced.keySet().forEach(p -> endOffsets.merge(p.partition(), p.offset() + 1, Math::max));
        Map<Integer, Long> committed = committedOffsets().entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().partition(), e -> e.getValue().offset()));
        assertThat(committed).as("committed offsets").isEqualTo(endOffsets);
    }

    private void awaitEverythingBackedUp(boolean allowDuplicates) {
        await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            rotator().rotate();
            assertNothingCommittedBeyondS3();
            assertEverythingBackedUp(allowDuplicates);
        });
    }
}
