package com.example.anonymizer.stream;

import com.example.anonymizer.common.metrics.PipelineMetrics;
import org.apache.spark.sql.streaming.SourceProgress;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** Publishes consumer lag from the Kafka source's progress metrics (maxOffsetsBehindLatest). */
final class KafkaLagListener extends StreamingQueryListener {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaLagListener.class);
    private final PipelineMetrics metrics;

    KafkaLagListener(PipelineMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public void onQueryStarted(QueryStartedEvent event) {
        LOG.info("Streaming query started");
    }

    @Override
    public void onQueryProgress(QueryProgressEvent event) {
        for (SourceProgress source : event.progress().sources()) {
            Map<String, String> m = source.metrics();
            if (m != null && m.containsKey("maxOffsetsBehindLatest")) {
                try {
                    metrics.consumerLag(Long.parseLong(m.get("maxOffsetsBehindLatest")));
                } catch (NumberFormatException ignored) {
                    // metric not yet available
                }
            }
        }
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {
        if (event.exception().isDefined()) {
            LOG.error("Streaming query terminated with an error: {}", event.exception().get());
        } else {
            LOG.info("Streaming query terminated");
        }
    }
}
