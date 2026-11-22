package com.example.anonymizer.common.crypto;

import java.util.Locale;

/**
 * Reads base64 keys from environment variables named {@code <prefix><VERSION>}, e.g.
 * {@code ANON_HMAC_KEY_V1}. For local development and Kubernetes secrets exposed as env vars.
 * System properties are consulted only when explicitly allowed (tests).
 */
public final class EnvKeyProvider implements KeyProvider {
    private final String prefix;
    private final boolean allowSystemProperties;

    public EnvKeyProvider(String prefix, boolean allowSystemProperties) {
        this.prefix = prefix;
        this.allowSystemProperties = allowSystemProperties;
    }

    @Override
    public byte[] keyBytes(String version) {
        String name = prefix + version.toUpperCase(Locale.ROOT);
        String value = System.getenv(name);
        if ((value == null || value.isBlank()) && allowSystemProperties) {
            value = System.getProperty(name);
        }
        if (value == null || value.isBlank()) {
            throw new KeyUnavailableException("HMAC key version " + version + " not found (expected variable " + name + ")");
        }
        return KeyProviders.decode(value, version);
    }
}
