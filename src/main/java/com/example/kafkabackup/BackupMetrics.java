package com.example.kafkabackup;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

@Component
public class BackupMetrics {

    private final MeterRegistry registry;
    private final AtomicLong lastSuccessEpoch = new AtomicLong();

    public BackupMetrics(MeterRegistry registry, WorkDir workDir) {
        this.registry = registry;
        registry.gauge("kb_sealed_files_pending", workDir, WorkDir::pendingCount);
        registry.gauge("kb_disk_used_ratio", workDir, WorkDir::diskUsedRatio);
        registry.gauge("kb_last_success_epoch", lastSuccessEpoch, AtomicLong::get);
    }

    public void recordsConsumed(String topic, long count) {
        counter("kb_records_consumed_total", topic).increment(count);
    }

    public void offsetGap(String topic) {
        counter("kb_offset_gap_total", topic).increment();
    }

    public void commitFailure(String topic) {
        counter("kb_commit_failure_total", topic).increment();
    }

    public void uploadSuccess(String topic, long bytes, long nanos) {
        counter("kb_upload_success_total", topic).increment();
        counter("kb_upload_bytes_total", topic).increment(bytes);
        Timer.builder("kb_upload_duration").tag("topic", topic).register(registry)
                .record(nanos, TimeUnit.NANOSECONDS);
        lastSuccessEpoch.set(System.currentTimeMillis() / 1000);
    }

    public void uploadFailure(String topic) {
        counter("kb_upload_failure_total", topic).increment();
    }

    private Counter counter(String name, String topic) {
        return Counter.builder(name).tag("topic", topic).register(registry);
    }
}
