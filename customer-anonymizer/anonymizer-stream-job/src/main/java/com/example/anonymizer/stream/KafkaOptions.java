package com.example.anonymizer.stream;

import com.example.anonymizer.common.config.PipelineConfig;

import java.util.LinkedHashMap;
import java.util.Map;

/** Kafka client options from stream.kafka.options, prefixed with "kafka." as Spark expects. */
final class KafkaOptions {
    private KafkaOptions() {
    }

    static Map<String, String> clientOptions(PipelineConfig cfg) {
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : cfg.stringMap("stream.kafka.options").entrySet()) {
            String key = e.getKey().startsWith("kafka.") ? e.getKey() : "kafka." + e.getKey();
            out.put(key, e.getValue());
        }
        return out;
    }
}
