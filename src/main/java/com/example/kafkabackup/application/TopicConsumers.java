package com.example.kafkabackup.application;

import com.example.kafkabackup.infrastructure.config.BackupProperties;
import com.example.kafkabackup.infrastructure.file.WorkDir;
import com.example.kafkabackup.infrastructure.metrics.BackupMetrics;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * One Spring Kafka listener container per topic, so a batch, and therefore its ack, never spans two
 * topics' files. The uploader acks batches from its own thread; Spring queues those acks and commits
 * them on the consumer thread.
 */
@Component
public class TopicConsumers implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TopicConsumers.class);

    private final ConsumerFactory<String, byte[]> consumerFactory;
    private final BatchWriter batchWriter;
    private final BackupProperties properties;
    private final WorkDir workDir;
    private final BackupMetrics metrics;

    private final List<MessageListenerContainer> containers = new CopyOnWriteArrayList<>();
    private boolean paused;

    public TopicConsumers(
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
        for (String topic : properties.topics()) {
            KafkaMessageListenerContainer<String, byte[]> container =
                    new KafkaMessageListenerContainer<>(consumerFactory, containerProperties(topic));
            // A batch that cannot be written to disk must not be skipped and committed: stop instead.
            container.setCommonErrorHandler(new CommonContainerStoppingErrorHandler());
            container.start();
            containers.add(container);
        }
        log.info("Consuming {}", properties.topics());
    }

    private ContainerProperties containerProperties(String topic) {
        ContainerProperties container = new ContainerProperties(topic);
        container.setAckMode(ContainerProperties.AckMode.MANUAL);
        container.setPollTimeout(properties.batch().pollTimeout().toMillis());
        container.setKafkaConsumerProperties(consumerOverrides(topic));
        container.setSyncCommits(false);
        container.setCommitCallback((offsets, e) -> {
            if (e != null) {
                log.error("Commit failed for {}", offsets.keySet(), e);
                offsets.keySet().forEach(tp -> metrics.commitFailure(tp.topic()));
            }
        });
        container.setMessageListener((BatchAcknowledgingMessageListener<String, byte[]>) batchWriter::append);
        return container;
    }

    /** Each topic has its own consumer, so each needs its own static member id. */
    private Properties consumerOverrides(String topic) {
        Properties overrides = new Properties();
        overrides.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.batch().maxRecords());
        overrides.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, (int) properties.batch().maxBytes().toBytes());
        Object instanceId = consumerFactory.getConfigurationProperties().get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
        if (instanceId != null) {
            overrides.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId + "-" + topic);
        }
        return overrides;
    }

    /** Stop consuming when the disk fills up or an offset gap was seen; never drop data. */
    @Scheduled(fixedDelay = 1000)
    public void applyBackpressure() {
        if (containers.isEmpty()) {
            return;
        }
        boolean shouldPause = batchWriter.gapDetected()
                || workDir.diskUsedRatio() > properties.disk().pauseAbove();
        boolean canResume = !batchWriter.gapDetected()
                && workDir.diskUsedRatio() < properties.disk().resumeBelow();

        if (!paused && shouldPause) {
            containers.forEach(MessageListenerContainer::pause);
            paused = true;
            log.warn("Consumers paused (disk {}, gap {})", workDir.diskUsedRatio(), batchWriter.gapDetected());
        } else if (paused && canResume) {
            containers.forEach(MessageListenerContainer::resume);
            paused = false;
            log.info("Consumers resumed");
        }
    }

    @Override
    public void close() {
        containers.forEach(MessageListenerContainer::stop);
    }
}
