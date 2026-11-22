package com.example.anonymizer.common.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * Append-only audit log. Object stores do not support appends, so each entry is its own immutable
 * file under {@code audit/dt=YYYY-MM-DD/}. Entries are also emitted as structured log lines.
 */
public final class AuditLog {
    private static final Logger AUDIT = LoggerFactory.getLogger("audit");
    private final Path root;
    private final FileSystem fs;

    public AuditLog(Path root, Configuration conf) throws IOException {
        this.root = root;
        this.fs = root.getFileSystem(conf);
    }

    public void append(AuditEntry e) throws IOException {
        String day = e.committedAt().substring(0, 10);
        String file = e.releaseType().toLowerCase(Locale.ROOT) + "-" + e.releaseId().replaceAll("[^A-Za-z0-9_.-]", "_") + ".json";
        HadoopFiles.writeAtomically(fs, new Path(root, "dt=" + day + "/" + file), Json.bytes(e));
        AUDIT.info("release recorded",
                kv("releaseType", e.releaseType()), kv("releaseId", e.releaseId()), kv("status", e.status()),
                kv("partition", e.partition()), kv("recordCount", e.recordCount()),
                kv("quarantinedCount", e.quarantinedCount()), kv("dlqCount", e.dlqCount()),
                kv("suppressedCount", e.suppressedCount()), kv("profileVersion", e.profileVersion()),
                kv("keyVersion", e.keyVersion()), kv("jobVersion", e.jobVersion()));
    }
}
