package com.example.anonymizer.common.io;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Output partition naming: dt=YYYY-MM-DD/hr=HH in UTC. */
public final class PartitionPaths {
    private static final DateTimeFormatter HOURLY =
            DateTimeFormatter.ofPattern("'dt='yyyy-MM-dd'/hr='HH").withZone(ZoneOffset.UTC);

    private PartitionPaths() {
    }

    public static String hourly(Instant instant) {
        return HOURLY.format(instant);
    }
}
