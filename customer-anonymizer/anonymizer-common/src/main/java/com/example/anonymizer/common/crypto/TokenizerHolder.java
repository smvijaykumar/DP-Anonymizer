package com.example.anonymizer.common.crypto;

import com.example.anonymizer.common.config.PipelineConfig;
import com.typesafe.config.Config;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One {@link Tokenizer} per JVM per key version. Executors fetch the key themselves on first use, so
 * key material is never serialized into Spark tasks or broadcast variables.
 */
public final class TokenizerHolder {
    private static final ConcurrentHashMap<String, Tokenizer> CACHE = new ConcurrentHashMap<>();

    private TokenizerHolder() {
    }

    public static Tokenizer get(PipelineConfig cfg) {
        Config c = cfg.raw();
        String version = c.getString("crypto.active-version");
        String context = c.getString("crypto.token-context");
        String cacheKey = c.getString("crypto.provider") + '|' + version + '|' + context;
        return CACHE.computeIfAbsent(cacheKey, k -> {
            byte[] key = KeyProviders.create(c).keyBytes(version);
            try {
                return new Tokenizer(version, key, context);
            } finally {
                Arrays.fill(key, (byte) 0);
            }
        });
    }

    /** Test hook. */
    public static void clear() {
        CACHE.clear();
    }
}
