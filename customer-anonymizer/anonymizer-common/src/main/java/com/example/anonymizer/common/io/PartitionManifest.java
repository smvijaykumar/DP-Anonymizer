package com.example.anonymizer.common.io;

import java.util.List;

/** Written beside each committed batch of files. Readers treat a batch as visible only once this exists. */
public record PartitionManifest(
        String kind,
        String partition,
        long batchId,
        long recordCount,
        long suppressedCount,
        String profileVersion,
        String keyVersion,
        String schemaVersion,
        String jobVersion,
        String committedAt,
        List<String> files) {
}
