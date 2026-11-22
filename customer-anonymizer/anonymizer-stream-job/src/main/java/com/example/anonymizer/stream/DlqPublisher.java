package com.example.anonymizer.stream;

import com.example.anonymizer.common.config.PipelineConfig;
import com.typesafe.config.Config;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;

/**
 * Publishes DLQ rows to the dead-letter topic. Each message holds the reason code and the source
 * partition/offset so an authorized operator can find the original inside the security boundary;
 * no payload is copied. Delivery is at-least-once: a replayed batch may republish.
 */
final class DlqPublisher {
    private final PipelineConfig cfg;

    DlqPublisher(PipelineConfig driverConfig) {
        this.cfg = driverConfig;
    }

    /** @return message count per reason code */
    Map<String, Long> publish(Dataset<Row> dlq, long batchId) {
        Map<String, Long> byReason = new TreeMap<>();
        List<Row> counts = dlq.groupBy("reason").count().collectAsList();
        for (Row r : counts) {
            byReason.put(r.getString(0), r.getLong(1));
        }
        if (byReason.isEmpty()) {
            return byReason;
        }
        Config c = cfg.raw();
        Dataset<Row> messages = dlq.select(to_json(struct(
                col("reason"),
                lit(c.getString("stream.kafka.topic")).as("sourceTopic"),
                col("kafka_partition").as("sourcePartition"),
                col("kafka_offset").as("sourceOffset"),
                col("kafka_timestamp").as("sourceTimestamp"),
                lit(batchId).as("batchId"),
                lit(cfg.jobVersion()).as("jobVersion"))).as("value"));
        DataFrameWriter<Row> writer = messages.write().format("kafka")
                .option("kafka.bootstrap.servers", c.getString("stream.kafka.bootstrap-servers"))
                .option("topic", c.getString("stream.dlq-topic"));
        for (Map.Entry<String, String> e : KafkaOptions.clientOptions(cfg).entrySet()) {
            writer = writer.option(e.getKey(), e.getValue());
        }
        writer.save();
        return byReason;
    }
}
