package com.example.kafkabackup.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class WindowCalculatorTest {

    private static final ZoneId UTC = ZoneId.of("UTC");

    @Test
    void tickAtMidnightSealsPreviousDayHour23() {
        BackupWindow window = WindowCalculator.sealedAt(Instant.parse("2026-09-29T00:00:05Z"), UTC);

        assertThat(window.date()).isEqualTo("2026-09-28");
        assertThat(window.hour()).isEqualTo("23");
        assertThat(window.label()).isEqualTo("2026-09-28T23");
        assertThat(window.id()).isEqualTo("2026092823");
    }

    @Test
    void tickAtOneAmSealsSameDayHour00() {
        BackupWindow window = WindowCalculator.sealedAt(Instant.parse("2026-09-29T01:00:05Z"), UTC);

        assertThat(window.date()).isEqualTo("2026-09-29");
        assertThat(window.hour()).isEqualTo("00");
    }

    @Test
    void windowIsTheFullHourBeforeTheTick() {
        BackupWindow window = WindowCalculator.sealedAt(Instant.parse("2026-09-29T14:00:05Z"), UTC);

        assertThat(window.start().toInstant()).isEqualTo(Instant.parse("2026-09-29T13:00:00Z"));
        assertThat(window.end().toInstant()).isEqualTo(Instant.parse("2026-09-29T14:00:00Z"));
    }
}
