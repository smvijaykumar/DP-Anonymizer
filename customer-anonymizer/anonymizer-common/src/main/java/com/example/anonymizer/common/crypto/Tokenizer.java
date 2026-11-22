package com.example.anonymizer.common.crypto;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Deterministic keyed tokenization: token = hex(HMAC-SHA256(key, context || 0x1F || strip(id))).
 *
 * <p>The same id, key version and context always give the same 64-character token, in any JVM and
 * on any run. Without the key, tokens cannot be reversed or rebuilt by dictionary attack. Thread-safe.
 */
public final class Tokenizer {
    private static final String ALGORITHM = "HmacSHA256";
    private static final byte SEPARATOR = 0x1F;

    private final String keyVersion;
    private final byte[] context;
    private final ThreadLocal<Mac> mac;

    public Tokenizer(String keyVersion, byte[] keyBytes, String context) {
        this.keyVersion = Objects.requireNonNull(keyVersion, "keyVersion");
        Objects.requireNonNull(keyBytes, "keyBytes");
        if (keyBytes.length < KeyProviders.MIN_KEY_BYTES) {
            throw new IllegalArgumentException("HMAC key must be at least " + KeyProviders.MIN_KEY_BYTES + " bytes");
        }
        this.context = Objects.requireNonNull(context, "context").getBytes(StandardCharsets.UTF_8);
        SecretKeySpec key = new SecretKeySpec(keyBytes, ALGORITHM); // copies the bytes
        this.mac = ThreadLocal.withInitial(() -> {
            try {
                Mac m = Mac.getInstance(ALGORITHM);
                m.init(key);
                return m;
            } catch (GeneralSecurityException e) {
                throw new IllegalStateException("Cannot initialise " + ALGORITHM, e);
            }
        });
    }

    public String tokenize(String rawId) {
        if (rawId == null) {
            throw new IllegalArgumentException("identifier is null");
        }
        String normalized = rawId.strip();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("identifier is blank");
        }
        Mac m = mac.get();
        m.reset();
        m.update(context);
        m.update(SEPARATOR);
        return HexFormat.of().formatHex(m.doFinal(normalized.getBytes(StandardCharsets.UTF_8)));
    }

    public String keyVersion() {
        return keyVersion;
    }

    @Override
    public String toString() {
        return "Tokenizer[keyVersion=" + keyVersion + ", key=<redacted>]";
    }
}
