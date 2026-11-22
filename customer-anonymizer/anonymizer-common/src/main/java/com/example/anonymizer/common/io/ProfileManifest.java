package com.example.anonymizer.common.io;

import java.util.Map;

/** Metadata for one published profile version. */
public record ProfileManifest(
        String version,
        String keyVersion,
        String schemaVersion,
        int k,
        int l,
        double suppressionLimit,
        Map<String, Integer> levels,
        double highestProsecutorRisk,
        double averageProsecutorRisk,
        long recordCount,
        long suppressedCount,
        boolean pinnedLevels,
        String createdAt,
        String jobVersion) {
}
