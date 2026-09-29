package com.example.kafkabackup.application;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

/**
 * A real Spring consumer factory (so container overrides are merged as in production) that hands out
 * {@link MockConsumer}s instead of connecting to a broker. Consumers are keyed by group.instance.id.
 */
class MockConsumerFactory extends DefaultKafkaConsumerFactory<String, byte[]> {

    final Map<String, RecordingConsumer> consumers = new ConcurrentHashMap<>();
    final Map<String, Map<String, Object>> configs = new ConcurrentHashMap<>();

    MockConsumerFactory() {
        super(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092",
                ConsumerConfig.GROUP_ID_CONFIG, "kafka-s3-backup",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                // As in application.yml. With "latest", Spring would commit the position on assignment.
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "local"));
    }

    @Override
    protected Consumer<String, byte[]> createRawConsumer(Map<String, Object> configProps) {
        String instanceId = (String) configProps.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
        RecordingConsumer consumer = new RecordingConsumer();
        configs.put(instanceId, Map.copyOf(configProps));
        consumers.put(instanceId, consumer);
        return consumer;
    }

    /** Remembers the last committed offset per partition, readable even after the consumer is closed. */
    static class RecordingConsumer extends MockConsumer<String, byte[]> {

        final Map<TopicPartition, Long> commits = new ConcurrentHashMap<>();

        RecordingConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        @Override
        public synchronized void commitAsync(
                Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
            record(offsets);
            super.commitAsync(offsets, callback);
        }

        @Override
        public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            record(offsets);
            super.commitSync(offsets);
        }

        @Override
        public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
            record(offsets);
            super.commitSync(offsets, timeout);
        }

        private void record(Map<TopicPartition, OffsetAndMetadata> offsets) {
            offsets.forEach((partition, offset) -> commits.put(partition, offset.offset()));
        }
    }
}
