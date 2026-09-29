package com.example.kafkabackup.domain;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/** Closed-open window {@code [start, end)}. Its date/hour labels always come from {@code start}. */
public record BackupWindow(ZonedDateTime start, ZonedDateTime end) {

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter HOUR = DateTimeFormatter.ofPattern("HH");
    private static final DateTimeFormatter ID = DateTimeFormatter.ofPattern("yyyyMMddHH");

    /** {@code 2026-09-28}, used in the S3 key. */
    public String date() {
        return start.format(DATE);
    }

    /** {@code 23} */
    public String hour() {
        return start.format(HOUR);
    }

    /** {@code 2026092823}, used in local sealed file names. */
    public String id() {
        return start.format(ID);
    }

    /** {@code 2026-09-28T23}, used in the manifest. */
    public String label() {
        return date() + "T" + hour();
    }
}
