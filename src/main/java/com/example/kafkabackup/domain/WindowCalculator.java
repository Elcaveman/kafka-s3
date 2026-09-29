package com.example.kafkabackup.domain;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public final class WindowCalculator {

    private WindowCalculator() {
    }

    /** A tick at {@code HH:00:05} seals the previous full hour: {@code [T-1h, T)}. */
    public static BackupWindow sealedAt(Instant tick, ZoneId zone) {
        ZonedDateTime end = tick.atZone(zone).truncatedTo(ChronoUnit.HOURS);
        return new BackupWindow(end.minusHours(1), end);
    }
}
