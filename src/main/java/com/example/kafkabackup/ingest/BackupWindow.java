package com.example.kafkabackup.ingest;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/** The full hour a rotation tick seals: {@code [start, start + 1h)}. */
public record BackupWindow(ZonedDateTime start) {

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter ID = DateTimeFormatter.ofPattern("yyyyMMddHH");
    private static final DateTimeFormatter LABEL = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH");

    /** A tick at {@code HH:00:05} seals the previous full hour. */
    public static BackupWindow sealedAt(Instant tick, ZoneId zone) {
        return new BackupWindow(tick.atZone(zone).truncatedTo(ChronoUnit.HOURS).minusHours(1));
    }

    /** {@code 2026-09-28}, used in the S3 key. */
    public String date() {
        return start.format(DATE);
    }

    /** {@code 2026092823}, used in local sealed file names. */
    public String id() {
        return start.format(ID);
    }

    /** {@code 2026-09-28T23}, used in the manifest. */
    public String label() {
        return start.format(LABEL);
    }
}
