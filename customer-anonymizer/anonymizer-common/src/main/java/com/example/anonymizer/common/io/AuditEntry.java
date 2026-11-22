package com.example.anonymizer.common.io;

import java.util.Map;

/** One immutable audit record per release (hourly partition commit or profile publish/rejection). */
public record AuditEntry(
        String releaseType,
        String releaseId,
        String status,
        String partition,
        long recordCount,
        long quarantinedCount,
        long dlqCount,
        long suppressedCount,
        String profileVersion,
        String keyVersion,
        String schemaVersion,
        String jobVersion,
        String committedAt,
        Map<String, Object> details) {
}
