package com.example.anonymizer.it;

import com.example.anonymizer.common.config.PipelineConfig;
import com.example.anonymizer.common.crypto.TokenizerHolder;
import com.example.anonymizer.common.guard.OutputSchemaGuard;
import com.example.anonymizer.common.io.Json;
import com.example.anonymizer.common.metrics.PipelineMetrics;
import com.example.anonymizer.profile.ProfileAnonymizationJob;
import com.example.anonymizer.stream.StreamJob;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.countDistinct;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full hourly cycle against a real Kafka broker: profile job to produce events to stream job, then
 * verify outputs, quarantine, DLQ, manifests, audit, idempotent re-run and that no raw identifier
 * reached any output file.
 */
@Testcontainers
class EndToEndHourlyCycleIT {
    private static final String SOURCE_TOPIC = "customer-events";
    private static final String DLQ_TOPIC = "customer-events-dlq";
    private static final int CUSTOMERS = 400;
    private static final int EVENTS_PER_CUSTOMER = 3;
    private static final Instant NOW = Instant.parse("2026-11-12T10:15:00Z");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @TempDir
    static Path tmp;
    static SparkSession spark;

    @BeforeAll
    static void setUp() {
        byte[] key = "integration-test-key-material-32b!!".getBytes(StandardCharsets.UTF_8);
        System.setProperty("ANON_HMAC_KEY_V1", Base64.getEncoder().encodeToString(key));
        TokenizerHolder.clear();
        spark = SparkSession.builder().master("local[2]").appName("anonymizer-it")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
        System.clearProperty("ANON_HMAC_KEY_V1");
    }

    @Test
    void fullHourlyCycle() throws Exception {
        PipelineConfig cfg = PipelineConfig.parse(config());
        List<SyntheticData.Customer> customers = SyntheticData.customers(CUSTOMERS, 42);

        writeSnapshot(customers);
        int exit = new ProfileAnonymizationJob(cfg, PipelineMetrics.noop("profile"), Clock.fixed(NOW, ZoneOffset.UTC)).run(spark);
        assertThat(exit).isEqualTo(ProfileAnonymizationJob.EXIT_OK);

        createTopics();
        produceEvents(customers);

        PipelineMetrics metrics = PipelineMetrics.noop("stream");
        runStreamToCompletion(cfg, metrics);

        String outRoot = uri("output");
        Dataset<Row> out = spark.read().option("basePath", outRoot).parquet(outRoot + "/dt=*/hr=*");
        assertThat(out.count()).isEqualTo((long) CUSTOMERS * EVENTS_PER_CUSTOMER);
        OutputSchemaGuard.verify(out.schema(), cfg.classification());

        String qRoot = uri("quarantine");
        assertThat(spark.read().option("basePath", qRoot).parquet(qRoot + "/dt=*/hr=*").count()).isEqualTo(5);

        // Every record for a customer carries the same anonymized values.
        long inconsistent = out.groupBy("customer_token")
                .agg(countDistinct(concat_ws("|", col("dob_gen"), col("zip_gen"), col("gender_gen"),
                        col("occupation_gen"), col("incomeBand"))).as("variants"))
                .filter(col("variants").gt(1)).count();
        assertThat(inconsistent).isZero();
        assertThat(out.select("customer_token").distinct().count()).isEqualTo(CUSTOMERS);

        // No raw direct identifier or full date of birth appears anywhere in the output.
        Set<String> raw = new HashSet<>();
        for (SyntheticData.Customer c : customers) {
            raw.addAll(List.of(c.id(), c.firstName(), c.lastName(), c.email(), c.phone(), c.dob().toString()));
        }
        for (Row r : out.collectAsList()) {
            for (int i = 0; i < r.length(); i++) {
                assertThat(raw).doesNotContain(String.valueOf(r.get(i)));
            }
        }

        // DLQ: reasons only, no payload.
        List<String> dlq = consumeDlq(3);
        Set<String> reasons = dlq.stream().map(EndToEndHourlyCycleIT::reason).collect(Collectors.toSet());
        assertThat(reasons).containsExactlyInAnyOrder("MALFORMED_JSON", "NOT_A_JSON_OBJECT", "UNCLASSIFIED_FIELD");
        assertThat(dlq).allSatisfy(m -> assertThat(m).doesNotContain("123-45-6789").doesNotContain("C00001"));

        // Manifests, _SUCCESS and audit.
        Path partition = tmp.resolve("output/dt=2026-11-12/hr=10");
        assertThat(partition.resolve("_SUCCESS")).exists();
        long manifestRecords = 0;
        try (Stream<Path> files = Files.list(partition)) {
            for (Path m : files.filter(p -> p.getFileName().toString().startsWith("_manifest-b")).collect(Collectors.toList())) {
                JsonNode node = Json.MAPPER.readTree(Files.readAllBytes(m));
                manifestRecords += node.get("recordCount").asLong();
                assertThat(node.get("keyVersion").asText()).isEqualTo("v1");
            }
        }
        assertThat(manifestRecords).isEqualTo((long) CUSTOMERS * EVENTS_PER_CUSTOMER);
        try (Stream<Path> audit = Files.walk(tmp.resolve("audit"))) {
            assertThat(audit.filter(p -> p.getFileName().toString().startsWith("hourly-")).count()).isGreaterThanOrEqualTo(1);
        }

        // Metrics.
        assertThat(metrics.count("anon.records.out")).isEqualTo(CUSTOMERS * EVENTS_PER_CUSTOMER);
        assertThat(metrics.count("anon.records.quarantined")).isEqualTo(5);

        // Re-running from the same checkpoint adds nothing.
        runStreamToCompletion(cfg, PipelineMetrics.noop("stream"));
        assertThat(spark.read().option("basePath", outRoot).parquet(outRoot + "/dt=*/hr=*").count())
                .isEqualTo((long) CUSTOMERS * EVENTS_PER_CUSTOMER);
    }

    private void runStreamToCompletion(PipelineConfig cfg, PipelineMetrics metrics) throws Exception {
        StreamingQuery q = StreamJob.start(spark, cfg, metrics, Clock.fixed(NOW, ZoneOffset.UTC));
        q.awaitTermination();
        assertThat(q.exception().isDefined()).as("stream query error").isFalse();
    }

    private void writeSnapshot(List<SyntheticData.Customer> customers) {
        StructType schema = new StructType()
                .add("customerId", DataTypes.StringType).add("firstName", DataTypes.StringType)
                .add("lastName", DataTypes.StringType).add("email", DataTypes.StringType)
                .add("phone", DataTypes.StringType).add("dob", DataTypes.DateType)
                .add("gender", DataTypes.StringType).add("zip", DataTypes.StringType)
                .add("occupation", DataTypes.StringType).add("incomeBand", DataTypes.StringType);
        List<Row> rows = new ArrayList<>();
        for (SyntheticData.Customer c : customers) {
            rows.add(RowFactory.create(c.id(), c.firstName(), c.lastName(), c.email(), c.phone(), Date.valueOf(c.dob()),
                    c.gender(), c.zip(), c.occupation(), c.incomeBand()));
        }
        spark.createDataFrame(rows, schema).write().parquet(uri("snapshot"));
    }

    private void createTopics() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(SOURCE_TOPIC, 2, (short) 1), new NewTopic(DLQ_TOPIC, 1, (short) 1))).all().get();
        }
    }

    private void produceEvents(List<SyntheticData.Customer> customers) throws Exception {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            for (SyntheticData.Customer c : customers) {
                for (int e = 0; e < EVENTS_PER_CUSTOMER; e++) {
                    producer.send(new ProducerRecord<>(SOURCE_TOPIC, SyntheticData.eventJson(c, "EVENT_" + e, "2026-11-12T09:0" + e + ":00Z")));
                }
            }
            List<SyntheticData.Customer> unknown = SyntheticData.customers(5, 99).stream()
                    .map(c -> new SyntheticData.Customer("X" + c.id(), c.firstName(), c.lastName(), c.email(), c.phone(),
                            c.dob(), c.gender(), c.zip(), c.occupation(), c.incomeBand()))
                    .collect(Collectors.toList());
            for (SyntheticData.Customer c : unknown) {
                producer.send(new ProducerRecord<>(SOURCE_TOPIC, SyntheticData.eventJson(c, "EVENT_X", "2026-11-12T09:10:00Z")));
            }
            producer.send(new ProducerRecord<>(SOURCE_TOPIC, "{\"customerId\":\"C00001\", not json"));
            producer.send(new ProducerRecord<>(SOURCE_TOPIC, "[\"C00001\"]"));
            producer.send(new ProducerRecord<>(SOURCE_TOPIC, SyntheticData.eventJson(customers.get(1), "EVENT_0", "2026-11-12T09:00:00Z")
                    .replace("}", ",\"ssn\":\"123-45-6789\"}")));
            producer.flush();
        }
    }

    private List<String> consumeDlq(int expected) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-dlq-reader");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        List<String> values = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(DLQ_TOPIC));
            long deadline = System.currentTimeMillis() + 30_000;
            while (values.size() < expected && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> rec : consumer.poll(Duration.ofMillis(500))) {
                    values.add(rec.value());
                }
            }
        }
        return values;
    }

    private static String reason(String json) {
        try {
            return Json.MAPPER.readTree(json).get("reason").asText();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String uri(String dir) {
        return tmp.resolve(dir).toUri().toString().replaceAll("/$", "");
    }

    private static String config() {
        return String.join("\n",
                "anonymizer {",
                "  schema {",
                "    id-field = customerId",
                "    fields = [",
                "      {name = customerId, type = string}, {name = firstName, type = string},",
                "      {name = lastName, type = string}, {name = email, type = string}, {name = phone, type = string},",
                "      {name = dob, type = date}, {name = gender, type = string}, {name = zip, type = string},",
                "      {name = occupation, type = string}, {name = incomeBand, type = string},",
                "      {name = eventType, type = string}, {name = eventTs, type = timestamp}",
                "    ]",
                "  }",
                "  classification {",
                "    DIRECT_IDENTIFIER = [customerId, firstName, lastName, email, phone]",
                "    QUASI_IDENTIFIER = [dob, gender, zip, occupation]",
                "    SENSITIVE = [incomeBand]",
                "    INSENSITIVE = [eventType, eventTs]",
                "  }",
                "  hierarchies {",
                "    dob { type = birth-year-bands, widths = [5, 10, 20] }",
                "    zip { type = zip-mask, keep-digits = [5, 3] }",
                "    occupation { type = category-map, file = \"classpath:occupation-categories-test.csv\", unmapped-category = OTHER }",
                "    gender { type = value-star }",
                "  }",
                "  crypto { provider = env, active-version = v1, env.allow-system-properties = true }",
                "  profile {",
                "    source { format = parquet, path = \"" + uri("snapshot") + "\" }",
                "    output-path = \"" + uri("profile") + "\"",
                "    k = 5, l = 2, suppression-limit = 0.05",
                "    max-prosecutor-risk = 0.2, max-average-risk = 0.2",
                "  }",
                "  stream {",
                "    kafka { bootstrap-servers = \"" + KAFKA.getBootstrapServers() + "\", topic = " + SOURCE_TOPIC + ", max-offsets-per-trigger = 500 }",
                "    trigger = available-now",
                "    checkpoint-path = \"" + uri("checkpoint") + "\"",
                "    output-path = \"" + uri("output") + "\"",
                "    quarantine-path = \"" + uri("quarantine") + "\"",
                "    dlq-topic = " + DLQ_TOPIC,
                "  }",
                "  audit-path = \"" + uri("audit") + "\"",
                "  metrics.enabled = false",
                "}");
    }
}
