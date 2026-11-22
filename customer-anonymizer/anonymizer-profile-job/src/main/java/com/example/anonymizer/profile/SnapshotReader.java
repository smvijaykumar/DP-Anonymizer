package com.example.anonymizer.profile;

import com.typesafe.config.Config;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/** Reads the full customer demographic snapshot from a catalog table or a path. */
final class SnapshotReader {
    private SnapshotReader() {
    }

    static Dataset<Row> read(SparkSession spark, Config source) {
        if (source.hasPath("table") && !source.getString("table").isBlank()) {
            return spark.table(source.getString("table"));
        }
        DataFrameReader reader = spark.read().format(source.getString("format"));
        for (Map.Entry<String, Object> e : source.getObject("options").unwrapped().entrySet()) {
            reader = reader.option(e.getKey(), String.valueOf(e.getValue()));
        }
        return reader.load(source.getString("path"));
    }
}
