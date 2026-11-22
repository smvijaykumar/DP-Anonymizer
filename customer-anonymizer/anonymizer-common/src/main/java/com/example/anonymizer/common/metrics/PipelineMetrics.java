package com.example.anonymizer.common.metrics;

import com.example.anonymizer.common.config.PipelineConfig;
import com.sun.net.httpserver.HttpServer;
import com.typesafe.config.Config;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Prometheus metrics, served at {@code :<port>/metrics} on the driver. Names (Prometheus form):
 * anon_records_in_total, anon_records_out_total, anon_records_quarantined_total,
 * anon_records_suppressed_total, anon_records_dlq_total{reason}, anon_consumer_lag,
 * anon_batch_duration_seconds.
 */
public final class PipelineMetrics implements AutoCloseable {
    private final PrometheusMeterRegistry registry;
    private final HttpServer server;
    private final Counter in;
    private final Counter out;
    private final Counter quarantined;
    private final Counter suppressed;
    private final AtomicLong lag = new AtomicLong();
    private final Timer batchDuration;

    private PipelineMetrics(String job, int port) {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.config().commonTags("job", job);
        in = Counter.builder("anon.records.in").description("Records consumed").register(registry);
        out = Counter.builder("anon.records.out").description("Anonymized records written").register(registry);
        quarantined = Counter.builder("anon.records.quarantined").description("Records with no profile match").register(registry);
        suppressed = Counter.builder("anon.records.suppressed").description("Records whose quasi-identifiers ARX suppressed").register(registry);
        Gauge.builder("anon.consumer.lag", lag, AtomicLong::doubleValue)
                .description("Max offsets behind latest across partitions").register(registry);
        batchDuration = Timer.builder("anon.batch.duration").description("Hourly batch or profile run duration").register(registry);

        if (port > 0) {
            try {
                server = HttpServer.create(new InetSocketAddress(port), 0);
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot start metrics endpoint on port " + port, e);
            }
            server.createContext("/metrics", ex -> {
                byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
                ex.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                ex.sendResponseHeaders(200, body.length);
                try (OutputStream os = ex.getResponseBody()) {
                    os.write(body);
                }
            });
            server.createContext("/health", ex -> {
                byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
                ex.sendResponseHeaders(200, body.length);
                try (OutputStream os = ex.getResponseBody()) {
                    os.write(body);
                }
            });
            server.start();
        } else {
            server = null;
        }
    }

    public static PipelineMetrics start(PipelineConfig cfg, String job) {
        Config c = cfg.raw();
        return new PipelineMetrics(job, c.getBoolean("metrics.enabled") ? c.getInt("metrics.port") : -1);
    }

    /** Registry without an HTTP endpoint (tests). */
    public static PipelineMetrics noop(String job) {
        return new PipelineMetrics(job, -1);
    }

    public void recordsIn(long n) { in.increment(n); }

    public void recordsOut(long n) { out.increment(n); }

    public void quarantined(long n) { quarantined.increment(n); }

    public void suppressed(long n) { suppressed.increment(n); }

    public void dlq(String reason, long n) {
        Counter.builder("anon.records.dlq").description("Records sent to the dead-letter topic")
                .tag("reason", reason).register(registry).increment(n);
    }

    public void consumerLag(long offsetsBehind) { lag.set(offsetsBehind); }

    public void batchDuration(Duration d) { batchDuration.record(d); }

    public double count(String name) {
        Counter c = registry.find(name).counter();
        return c == null ? 0.0 : c.count();
    }

    public String scrape() {
        return registry.scrape();
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
        }
        registry.close();
    }
}
