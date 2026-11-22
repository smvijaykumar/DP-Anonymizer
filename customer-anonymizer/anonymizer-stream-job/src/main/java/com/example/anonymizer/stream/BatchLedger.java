package com.example.anonymizer.stream;

import com.example.anonymizer.common.io.HadoopFiles;
import com.example.anonymizer.common.io.Json;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Remembers which hourly partition a micro-batch was assigned to. A replay of the same batch id after
 * a crash reuses that partition, even if the clock has moved to the next hour, so replays stay
 * idempotent.
 */
final class BatchLedger {
    private final Path dir;
    private final FileSystem fs;

    BatchLedger(Path dir, Configuration conf) throws IOException {
        this.dir = dir;
        this.fs = dir.getFileSystem(conf);
    }

    String partitionFor(long batchId, Supplier<String> fresh) throws IOException {
        Path entry = new Path(dir, "b" + batchId + ".json");
        Optional<byte[]> existing = HadoopFiles.readIfExists(fs, entry);
        if (existing.isPresent()) {
            JsonNode node = Json.MAPPER.readTree(existing.get());
            return node.get("partition").asText();
        }
        String partition = fresh.get();
        HadoopFiles.writeAtomically(fs, entry, Json.bytes(Map.of("batchId", batchId, "partition", partition)));
        return partition;
    }
}
