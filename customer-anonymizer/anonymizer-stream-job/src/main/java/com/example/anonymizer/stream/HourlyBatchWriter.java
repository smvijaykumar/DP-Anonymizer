package com.example.anonymizer.stream;

import com.example.anonymizer.common.config.ColumnClass;
import com.example.anonymizer.common.config.ConfigValidator;
import com.example.anonymizer.common.config.PipelineConfig;
import com.example.anonymizer.common.config.SchemaClassification;
import com.example.anonymizer.common.guard.OutputSchemaGuard;
import com.example.anonymizer.common.io.AtomicPartitionCommitter;
import com.example.anonymizer.common.io.AuditEntry;
import com.example.anonymizer.common.io.AuditLog;
import com.example.anonymizer.common.io.Json;
import com.example.anonymizer.common.io.PartitionManifest;
import com.example.anonymizer.common.io.PartitionPaths;
import com.example.anonymizer.common.io.ProfileRepository;
import com.example.anonymizer.common.metrics.PipelineMetrics;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static net.logstash.logback.argument.StructuredArguments.kv;
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 * Handles one hourly micro-batch on the driver:
 * <ol>
 *   <li>assign (or recall, on replay) the dt=/hr= partition for this batch id;</li>
 *   <li>skip if this batch's output manifest already exists (replay after commit);</li>
 *   <li>join parsed events to the latest profile version by token;</li>
 *   <li>commit unmatched events to quarantine, publish DLQ rows, then commit matched output last.</li>
 * </ol>
 * Spark records the batch's Kafka offsets as committed only after this method returns, so offsets
 * advance only once files are durable.
 */
public final class HourlyBatchWriter implements VoidFunction2<Dataset<Row>, Long> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HourlyBatchWriter.class);

    private final PipelineConfig cfg;
    private final transient PipelineMetrics metrics;
    private final transient Clock clock;
    private transient ProfileCache profileCache;

    public HourlyBatchWriter(PipelineConfig driverConfig, PipelineMetrics metrics, Clock clock) {
        this.cfg = driverConfig;
        this.metrics = metrics;
        this.clock = clock;
        Config c = driverConfig.raw();
        ConfigValidator.requireNonBlank(c, "stream.output-path", "stream.quarantine-path", "stream.dlq-topic",
                "profile.output-path", "audit-path");
    }

    @Override
    public void call(Dataset<Row> batch, Long batchId) throws Exception {
        long started = System.nanoTime();
        MDC.put("batchId", String.valueOf(batchId));
        try {
            process(batch, batchId);
        } finally {
            metrics.batchDuration(Duration.ofNanos(System.nanoTime() - started));
            MDC.remove("batchId");
        }
    }

    private void process(Dataset<Row> batch, long batchId) throws IOException {
        SparkSession spark = batch.sparkSession();
        Configuration hadoop = spark.sparkContext().hadoopConfiguration();
        Config c = cfg.raw();
        SchemaClassification sc = cfg.classification();
        Path outputRoot = new Path(c.getString("stream.output-path"));
        Path quarantineRoot = new Path(c.getString("stream.quarantine-path"));

        String partition = new BatchLedger(new Path(outputRoot, "_batches"), hadoop)
                .partitionFor(batchId, () -> PartitionPaths.hourly(clock.instant()));
        MDC.put("partition", partition);
        AtomicPartitionCommitter outputCommitter = new AtomicPartitionCommitter(outputRoot, hadoop);
        if (outputCommitter.isCommitted(new Path(outputRoot, partition), batchId)) {
            LOG.info("Batch already committed; skipping replay");
            return;
        }

        batch.persist(StorageLevel.MEMORY_AND_DISK());
        Dataset<Row> joined = null;
        try {
            long recordsIn = batch.count();
            Dataset<Row> ok = batch.filter(col("status").equalTo(EventParser.STATUS_OK));
            Dataset<Row> dlq = batch.filter(col("status").equalTo(EventParser.STATUS_DLQ));

            ProfileCache.Loaded profile = profileCache(hadoop, sc).latest(spark);
            if (!profile.manifest().keyVersion().equals(cfg.activeKeyVersion())) {
                throw new IllegalStateException("Profile version " + profile.manifest().version() + " uses key "
                        + profile.manifest().keyVersion() + " but the active key is " + cfg.activeKeyVersion()
                        + "; run the profile job with the active key before streaming");
            }
            Dataset<Row> prof = profile.data()
                    .withColumnRenamed("customer_token", "p_token")
                    .withColumnRenamed("key_version", "p_key_version")
                    .withColumnRenamed("profile_version", "p_profile_version");
            Dataset<Row> right = c.getBoolean("stream.broadcast-profile") ? broadcast(prof) : prof;
            joined = ok.join(right, ok.col("customer_token").equalTo(right.col("p_token")), "left")
                    .persist(StorageLevel.MEMORY_AND_DISK());

            Dataset<Row> output = joined.filter(col("p_token").isNotNull()).select(outputColumns(sc));
            Dataset<Row> quarantine = joined.filter(col("p_token").isNull()).select(quarantineColumns(sc));
            OutputSchemaGuard.verify(output.schema(), sc);
            OutputSchemaGuard.verify(quarantine.schema(), sc);

            String profileVersion = profile.manifest().version();
            long quarantined = writeAndCommit(quarantine, quarantineRoot, partition, batchId, "quarantine",
                    0L, profileVersion, sc, hadoop);
            Map<String, Long> dlqByReason = new DlqPublisher(cfg).publish(dlq, batchId);
            long dlqCount = dlqByReason.values().stream().mapToLong(Long::longValue).sum();
            long suppressed = output.filter(col("suppressed")).count();
            long written = writeAndCommit(output, outputRoot, partition, batchId, "output",
                    suppressed, profileVersion, sc, hadoop);

            metrics.recordsIn(recordsIn);
            metrics.recordsOut(written);
            metrics.quarantined(quarantined);
            metrics.suppressed(suppressed);
            dlqByReason.forEach(metrics::dlq);

            Map<String, Object> details = new LinkedHashMap<>();
            details.put("recordsIn", recordsIn);
            details.put("dlqByReason", dlqByReason);
            new AuditLog(new Path(c.getString("audit-path")), hadoop).append(new AuditEntry(
                    "HOURLY", partition.replace('/', '_') + "_b" + batchId, "COMMITTED", partition, written,
                    quarantined, dlqCount, suppressed, profileVersion, cfg.activeKeyVersion(), sc.fingerprint(),
                    cfg.jobVersion(), clock.instant().toString(), details));
            LOG.info("Hourly batch committed", kv("recordsIn", recordsIn), kv("recordsOut", written),
                    kv("quarantined", quarantined), kv("dlq", dlqCount), kv("suppressed", suppressed),
                    kv("profileVersion", profileVersion));
        } finally {
            if (joined != null) {
                joined.unpersist();
            }
            batch.unpersist();
            MDC.remove("partition");
        }
    }

    private long writeAndCommit(Dataset<Row> df, Path root, String partition, long batchId, String kind,
                                long suppressed, String profileVersion, SchemaClassification sc,
                                Configuration hadoop) throws IOException {
        AtomicPartitionCommitter committer = new AtomicPartitionCommitter(root, hadoop);
        Path target = new Path(root, partition);
        long count = df.count();
        if (committer.isCommitted(target, batchId)) {
            return count; // committed by an earlier attempt of this batch
        }
        Path staging = new Path(root, "_staging/b" + batchId);
        committer.deleteIfExists(staging);
        df.write().mode(SaveMode.Overwrite)
                .option("compression", cfg.raw().getString("output.compression"))
                .parquet(staging.toString());
        committer.commit(staging, target, batchId, files -> Json.bytes(new PartitionManifest(
                kind, partition, batchId, count, suppressed, profileVersion, cfg.activeKeyVersion(),
                sc.fingerprint(), cfg.jobVersion(), clock.instant().toString(), files)));
        return count;
    }

    private static Column[] outputColumns(SchemaClassification sc) {
        List<Column> cols = new ArrayList<>();
        cols.add(col("customer_token"));
        cols.add(col("key_version"));
        cols.add(col("p_profile_version").as("profile_version"));
        for (String q : sc.names(ColumnClass.QUASI_IDENTIFIER)) {
            cols.add(col(q + OutputSchemaGuard.GENERALIZED_SUFFIX));
        }
        for (String s : sc.names(ColumnClass.SENSITIVE)) {
            cols.add(col(s));
        }
        cols.add(col("suppressed"));
        for (String i : sc.names(ColumnClass.INSENSITIVE)) {
            cols.add(col(i));
        }
        return cols.toArray(new Column[0]);
    }

    private static Column[] quarantineColumns(SchemaClassification sc) {
        List<Column> cols = new ArrayList<>();
        cols.add(col("customer_token"));
        cols.add(col("key_version"));
        cols.add(col("kafka_partition"));
        cols.add(col("kafka_offset"));
        cols.add(col("kafka_timestamp"));
        for (String i : sc.names(ColumnClass.INSENSITIVE)) {
            cols.add(col(i));
        }
        cols.add(lit("NO_PROFILE").as("reason"));
        return cols.toArray(new Column[0]);
    }

    private ProfileCache profileCache(Configuration hadoop, SchemaClassification sc) throws IOException {
        if (profileCache == null) {
            profileCache = new ProfileCache(
                    new ProfileRepository(new Path(cfg.raw().getString("profile.output-path")), hadoop),
                    sc.fingerprint());
        }
        return profileCache;
    }
}
