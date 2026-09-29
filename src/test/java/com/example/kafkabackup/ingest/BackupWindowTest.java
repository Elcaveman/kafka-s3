package com.example.kafkabackup.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class BackupWindowTest {

    private static final ZoneId UTC = ZoneId.of("UTC");

    @Test
    void tickAtMidnightSealsPreviousDayHour23() {
        BackupWindow window = BackupWindow.sealedAt(Instant.parse("2026-09-29T00:00:05Z"), UTC);

        assertThat(window.date()).isEqualTo("2026-09-28");
        assertThat(window.label()).isEqualTo("2026-09-28T23");
        assertThat(window.id()).isEqualTo("2026092823");
    }

    @Test
    void tickAtOneAmSealsSameDayHour00() {
        BackupWindow window = BackupWindow.sealedAt(Instant.parse("2026-09-29T01:00:05Z"), UTC);

        assertThat(window.date()).isEqualTo("2026-09-29");
        assertThat(window.label()).isEqualTo("2026-09-29T00");
    }

    @Test
    void windowStartsAtTheFullHourBeforeTheTick() {
        BackupWindow window = BackupWindow.sealedAt(Instant.parse("2026-09-29T14:00:05Z"), UTC);

        assertThat(window.start().toInstant()).isEqualTo(Instant.parse("2026-09-29T13:00:00Z"));
    }
}
