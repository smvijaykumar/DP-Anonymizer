package com.example.anonymizer.stream;

import com.example.anonymizer.common.config.ConfigValidator;
import com.example.anonymizer.common.config.PipelineConfig;
import com.example.anonymizer.common.config.SchemaClassification;
import com.example.anonymizer.common.metrics.PipelineMetrics;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Hourly streaming job: Kafka to tokenized events to join with the latest profile to hourly Parquet.
 *
 * <p>Trigger: processing-time, every hour. Each micro-batch is committed whole to the partition of
 * the hour it ran in, so partitions are immutable once their manifest exists; late events land in the
 * next hour's partition and keep their original eventTs.
 */
public final class StreamJob {
    private static final Logger LOG = LoggerFactory.getLogger(StreamJob.class);

    private StreamJob() {
    }

    public static void main(String[] args) throws Exception {
        PipelineConfig cfg = PipelineConfig.load();
        cfg.classification(); // fail fast
        try (PipelineMetrics metrics = PipelineMetrics.start(cfg, "stream")) {
            SparkSession spark = SparkSession.builder().appName("hourly-anonymization-stream")
                    .config("spark.sql.session.timeZone", "UTC").getOrCreate();
            StreamingQuery query = start(spark, cfg, metrics, Clock.systemUTC());
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    LOG.info("Stopping streaming query");
                    query.stop();
                } catch (Exception e) {
                    LOG.warn("Error while stopping streaming query", e);
                }
            }, "stream-shutdown"));
            query.awaitTermination();
        }
    }

    public static StreamingQuery start(SparkSession spark, PipelineConfig cfg, PipelineMetrics metrics, Clock clock)
            throws TimeoutException {
        Config c = cfg.raw();
        ConfigValidator.requireNonBlank(c, "stream.kafka.bootstrap-servers", "stream.kafka.topic",
                "stream.checkpoint-path");
        SchemaClassification sc = cfg.classification();
        spark.streams().addListener(new KafkaLagListener(metrics));

        DataStreamReader reader = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", c.getString("stream.kafka.bootstrap-servers"))
                .option("subscribe", c.getString("stream.kafka.topic"))
                .option("startingOffsets", c.getString("stream.kafka.starting-offsets"))
                .option("failOnDataLoss", "true")
                .option("maxOffsetsPerTrigger", c.getLong("stream.kafka.max-offsets-per-trigger"));
        if (!c.getString("stream.kafka.group-id").isBlank()) {
            reader = reader.option("kafka.group.id", c.getString("stream.kafka.group-id"));
        }
        for (Map.Entry<String, String> e : KafkaOptions.clientOptions(cfg).entrySet()) {
            reader = reader.option(e.getKey(), e.getValue());
        }

        Dataset<Row> raw = reader.load().select("value", "partition", "offset", "timestamp");
        Dataset<Row> parsed = raw.mapPartitions(new EventParser(cfg.forExecutors()),
                Encoders.row(EventParser.outputSchema(sc)));

        String t = c.getString("stream.trigger");
        Trigger trigger = "available-now".equalsIgnoreCase(t) ? Trigger.AvailableNow() : Trigger.ProcessingTime(t);
        return parsed.writeStream()
                .queryName("hourly-anonymization")
                .option("checkpointLocation", c.getString("stream.checkpoint-path"))
                .trigger(trigger)
                .foreachBatch(new HourlyBatchWriter(cfg, metrics, clock))
                .start();
    }
}
