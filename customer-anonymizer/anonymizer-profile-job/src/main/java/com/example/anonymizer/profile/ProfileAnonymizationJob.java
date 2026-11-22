package com.example.anonymizer.profile;

import com.example.anonymizer.common.config.ColumnClass;
import com.example.anonymizer.common.config.ConfigValidator;
import com.example.anonymizer.common.config.PipelineConfig;
import com.example.anonymizer.common.config.SchemaClassification;
import com.example.anonymizer.common.crypto.Tokenizer;
import com.example.anonymizer.common.crypto.TokenizerHolder;
import com.example.anonymizer.common.guard.OutputSchemaGuard;
import com.example.anonymizer.common.io.AuditEntry;
import com.example.anonymizer.common.io.AuditLog;
import com.example.anonymizer.common.io.ProfileManifest;
import com.example.anonymizer.common.io.ProfileRepository;
import com.example.anonymizer.common.metrics.PipelineMetrics;
import com.example.anonymizer.profile.arx.ArxAnonymizer;
import com.example.anonymizer.profile.hierarchy.HierarchySpec;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static net.logstash.logback.argument.StructuredArguments.kv;
import static org.apache.spark.sql.functions.col;

/**
 * Daily batch job: snapshot to tokenized, k-anonymous, l-diverse profile table (customer_token to
 * generalized attributes). Publishes a new immutable version only if the re-identification risk gate
 * passes. Exit codes: 0 published, 2 rejected by the risk gate, 1 error.
 */
public final class ProfileAnonymizationJob {
    public static final int EXIT_OK = 0;
    public static final int EXIT_ERROR = 1;
    public static final int EXIT_RISK_REJECTED = 2;

    private static final Logger LOG = LoggerFactory.getLogger(ProfileAnonymizationJob.class);
    private static final DateTimeFormatter VERSION_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
    private static final String UNKNOWN_SENSITIVE = "UNKNOWN";

    private final PipelineConfig cfg;
    private final PipelineMetrics metrics;
    private final Clock clock;

    public ProfileAnonymizationJob(PipelineConfig cfg, PipelineMetrics metrics, Clock clock) {
        this.cfg = cfg;
        this.metrics = metrics;
        this.clock = clock;
    }

    public static void main(String[] args) {
        int code;
        try {
            PipelineConfig cfg = PipelineConfig.load();
            cfg.classification(); // fail fast on unclassified or inconsistent fields
            try (PipelineMetrics metrics = PipelineMetrics.start(cfg, "profile");
                 SparkSession spark = SparkSession.builder().appName("profile-anonymization")
                         .config("spark.sql.session.timeZone", "UTC").getOrCreate()) {
                code = new ProfileAnonymizationJob(cfg, metrics, Clock.systemUTC()).run(spark);
            }
        } catch (Exception e) {
            LOG.error("Profile anonymization failed", e);
            code = EXIT_ERROR;
        }
        System.exit(code);
    }

    public int run(SparkSession spark) throws IOException {
        long started = System.nanoTime();
        Config c = cfg.raw();
        ConfigValidator.requireNonBlank(c, "profile.output-path", "audit-path");
        SchemaClassification sc = cfg.classification();
        Instant now = clock.instant();
        String version = VERSION_FORMAT.format(now);
        MDC.put("profileVersion", version);
        try {
            List<String> qi = sc.names(ColumnClass.QUASI_IDENTIFIER);
            List<String> sensitive = sc.names(ColumnClass.SENSITIVE);
            String idField = sc.idField();
            Map<String, HierarchySpec> specs = new LinkedHashMap<>();
            for (String q : qi) {
                specs.put(q, HierarchySpec.fromConfig(q, c.getConfig(ConfigUtil.joinPath("hierarchies", q))));
            }
            Tokenizer tokenizer = TokenizerHolder.get(cfg);
            Configuration hadoop = spark.sparkContext().hadoopConfiguration();
            ProfileRepository repo = new ProfileRepository(new Path(c.getString("profile.output-path")), hadoop);
            AuditLog audit = new AuditLog(new Path(c.getString("audit-path")), hadoop);

            Dataset<Row> snapshot = selectColumns(SnapshotReader.read(spark, c.getConfig("profile.source")),
                    idField, qi, sensitive);
            snapshot = handleDuplicates(snapshot, idField, c.getString("profile.duplicate-policy"));

            // Collect to the driver: ARX runs in one JVM over the whole population.
            List<String> tokens = new ArrayList<>();
            List<String[]> rows = new ArrayList<>();
            Map<String, Set<String>> leaves = new LinkedHashMap<>();
            for (String q : qi) {
                leaves.put(q, new HashSet<>());
            }
            long missingId = 0;
            Iterator<Row> it = snapshot.toLocalIterator();
            while (it.hasNext()) {
                Row r = it.next();
                Object id = r.getAs(idField);
                if (id == null || id.toString().isBlank()) {
                    missingId++;
                    continue;
                }
                String[] values = new String[qi.size() + sensitive.size()];
                for (int i = 0; i < qi.size(); i++) {
                    String leaf = specs.get(qi.get(i)).leaf(r.getAs(qi.get(i)));
                    values[i] = leaf;
                    leaves.get(qi.get(i)).add(leaf);
                }
                for (int j = 0; j < sensitive.size(); j++) {
                    Object v = r.getAs(sensitive.get(j));
                    values[qi.size() + j] = v == null || v.toString().isBlank() ? UNKNOWN_SENSITIVE : v.toString().trim();
                }
                tokens.add(tokenizer.tokenize(id.toString()));
                rows.add(values);
            }
            LOG.info("Snapshot collected", kv("customers", rows.size()), kv("missingId", missingId));
            metrics.recordsIn(rows.size());

            Map<String, Hierarchy> hierarchies = new LinkedHashMap<>();
            for (String q : qi) {
                hierarchies.put(q, specs.get(q).build(leaves.get(q)));
            }

            Map<String, Integer> pinned = null;
            if (c.getBoolean("profile.pin-previous-levels")) {
                pinned = previousLevels(repo, tokenizer.keyVersion(), sc.fingerprint());
            }

            ArxAnonymizer.Params params = new ArxAnonymizer.Params(c.getInt("profile.k"), c.getInt("profile.l"),
                    c.getDouble("profile.suppression-limit"));
            ArxAnonymizer.Outcome outcome = new ArxAnonymizer().anonymize(qi, sensitive, rows, hierarchies, params, pinned);
            metrics.suppressed(outcome.suppressedCount());
            LOG.info("ARX finished", kv("levels", outcome.levels()), kv("pinnedLevels", outcome.usedPinnedLevels()),
                    kv("suppressed", outcome.suppressedCount()), kv("highestRisk", outcome.highestRisk()),
                    kv("averageRisk", outcome.averageRisk()));

            double maxHighest = c.getDouble("profile.max-prosecutor-risk");
            double maxAverage = c.getDouble("profile.max-average-risk");
            if (outcome.highestRisk() > maxHighest || outcome.averageRisk() > maxAverage) {
                LOG.error("Risk gate failed; profile version not published",
                        kv("highestRisk", outcome.highestRisk()), kv("maxHighest", maxHighest),
                        kv("averageRisk", outcome.averageRisk()), kv("maxAverage", maxAverage));
                audit.append(new AuditEntry("PROFILE", version, "REJECTED_RISK", null, rows.size(), 0, 0,
                        outcome.suppressedCount(), version, tokenizer.keyVersion(), sc.fingerprint(), cfg.jobVersion(),
                        clock.instant().toString(), riskDetails(outcome, maxHighest, maxAverage)));
                return EXIT_RISK_REJECTED;
            }

            boolean exclude = "EXCLUDE".equals(c.getString("profile.suppressed-handling"));
            StructType schema = profileSchema(qi, sensitive);
            List<Row> out = new ArrayList<>(rows.size());
            for (int i = 0; i < rows.size(); i++) {
                if (exclude && outcome.suppressed()[i]) {
                    continue;
                }
                String[] anon = outcome.rows().get(i);
                Object[] v = new Object[schema.size()];
                v[0] = tokens.get(i);
                v[1] = tokenizer.keyVersion();
                v[2] = version;
                System.arraycopy(anon, 0, v, 3, anon.length);
                v[v.length - 1] = outcome.suppressed()[i];
                out.add(RowFactory.create(v));
            }

            Dataset<Row> profile = spark.createDataFrame(out, schema);
            OutputSchemaGuard.verify(profile.schema(), sc);
            Path staging = repo.stagingPath(version);
            profile.write().mode(SaveMode.Overwrite)
                    .option("compression", c.getString("output.compression"))
                    .parquet(staging.toString());

            ProfileManifest manifest = new ProfileManifest(version, tokenizer.keyVersion(), sc.fingerprint(),
                    params.k(), params.l(), params.suppressionLimit(), outcome.levels(), outcome.highestRisk(),
                    outcome.averageRisk(), out.size(), outcome.suppressedCount(), outcome.usedPinnedLevels(),
                    clock.instant().toString(), cfg.jobVersion());
            repo.publish(staging, manifest);
            metrics.recordsOut(out.size());
            audit.append(new AuditEntry("PROFILE", version, "PUBLISHED", repo.versionPath(version).getName(), out.size(),
                    0, 0, outcome.suppressedCount(), version, tokenizer.keyVersion(), sc.fingerprint(), cfg.jobVersion(),
                    clock.instant().toString(), riskDetails(outcome, maxHighest, maxAverage)));
            LOG.info("Profile version published", kv("records", out.size()));
            return EXIT_OK;
        } finally {
            metrics.batchDuration(Duration.ofNanos(System.nanoTime() - started));
            MDC.remove("profileVersion");
        }
    }

    private static Dataset<Row> selectColumns(Dataset<Row> df, String id, List<String> qi, List<String> sensitive) {
        List<Column> cols = new ArrayList<>();
        cols.add(col(id));
        for (String q : qi) {
            cols.add(col(q));
        }
        for (String s : sensitive) {
            cols.add(col(s));
        }
        return df.select(cols.toArray(new Column[0]));
    }

    private static Dataset<Row> handleDuplicates(Dataset<Row> df, String id, String policy) {
        long total = df.count();
        long distinct = df.select(id).distinct().count();
        if (total == distinct) {
            return df;
        }
        LOG.warn("Duplicate customer ids in snapshot", kv("rows", total), kv("distinctIds", distinct));
        if ("fail".equals(policy)) {
            throw new IllegalStateException("Snapshot has " + (total - distinct)
                    + " duplicate customer rows; fix the source or set profile.duplicate-policy = first");
        }
        return df.dropDuplicates(id);
    }

    private static Map<String, Integer> previousLevels(ProfileRepository repo, String keyVersion, String schemaVersion)
            throws IOException {
        var latest = repo.latestVersion();
        if (latest.isEmpty()) {
            return null;
        }
        var manifest = repo.manifest(latest.get());
        if (manifest.isEmpty() || !manifest.get().keyVersion().equals(keyVersion)
                || !manifest.get().schemaVersion().equals(schemaVersion)) {
            return null;
        }
        return manifest.get().levels();
    }

    static StructType profileSchema(List<String> qi, List<String> sensitive) {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("customer_token", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("key_version", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("profile_version", DataTypes.StringType, false));
        for (String q : qi) {
            fields.add(DataTypes.createStructField(q + OutputSchemaGuard.GENERALIZED_SUFFIX, DataTypes.StringType, true));
        }
        for (String s : sensitive) {
            fields.add(DataTypes.createStructField(s, DataTypes.StringType, true));
        }
        fields.add(DataTypes.createStructField("suppressed", DataTypes.BooleanType, false));
        return DataTypes.createStructType(fields);
    }

    private static Map<String, Object> riskDetails(ArxAnonymizer.Outcome o, double maxHighest, double maxAverage) {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("levels", o.levels());
        d.put("pinnedLevels", o.usedPinnedLevels());
        d.put("highestProsecutorRisk", o.highestRisk());
        d.put("averageProsecutorRisk", o.averageRisk());
        d.put("maxProsecutorRisk", maxHighest);
        d.put("maxAverageRisk", maxAverage);
        return d;
    }
}
