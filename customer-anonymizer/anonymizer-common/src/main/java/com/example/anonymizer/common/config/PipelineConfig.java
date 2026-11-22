package com.example.anonymizer.common.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable view of the {@code anonymizer} configuration subtree.
 *
 * <p>Configuration never contains the HMAC key itself, only where to fetch it. Paths that may carry
 * credentials (Kafka SASL settings) are stripped by {@link #forExecutors()} before the config is
 * serialized into Spark tasks.
 */
public final class PipelineConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final String ROOT = "anonymizer";
    private static final List<String> DRIVER_ONLY_PATHS = List.of("stream.kafka.options");

    private final String rendered;
    private transient Config config;

    private PipelineConfig(Config subtree) {
        this.config = subtree;
        this.rendered = subtree.root().render(ConfigRenderOptions.concise());
    }

    /** Loads application config (honours -Dconfig.file) layered over reference.conf. */
    public static PipelineConfig load() {
        return fromConfig(ConfigFactory.load());
    }

    public static PipelineConfig fromConfig(Config full) {
        return new PipelineConfig(full.resolve().getConfig(ROOT));
    }

    /** Parses HOCON containing an {@code anonymizer { ... }} block, with reference.conf as fallback. */
    public static PipelineConfig parse(String hocon) {
        Config c = ConfigFactory.parseString(hocon).withFallback(ConfigFactory.defaultReference()).resolve();
        return fromConfig(c);
    }

    public Config raw() {
        if (config == null) {
            config = ConfigFactory.parseString(rendered);
        }
        return config;
    }

    /** Copy safe to ship inside Spark tasks. */
    public PipelineConfig forExecutors() {
        Config c = raw();
        for (String p : DRIVER_ONLY_PATHS) {
            c = c.withoutPath(p);
        }
        return new PipelineConfig(c);
    }

    public SchemaClassification classification() {
        return SchemaClassification.fromConfig(raw());
    }

    public String jobVersion() {
        return raw().getString("job-version");
    }

    public String activeKeyVersion() {
        return raw().getString("crypto.active-version");
    }

    /** Top-level string entries of an object; keys keep their dots (e.g. "security.protocol"). */
    public Map<String, String> stringMap(String path) {
        Map<String, String> out = new LinkedHashMap<>();
        if (!raw().hasPath(path)) {
            return out;
        }
        for (Map.Entry<String, Object> e : raw().getObject(path).unwrapped().entrySet()) {
            if (e.getValue() != null) {
                out.put(e.getKey(), String.valueOf(e.getValue()));
            }
        }
        return out;
    }
}
