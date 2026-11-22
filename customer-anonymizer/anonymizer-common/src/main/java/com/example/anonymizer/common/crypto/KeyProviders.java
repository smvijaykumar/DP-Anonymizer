package com.example.anonymizer.common.crypto;

import com.typesafe.config.Config;

import java.util.Base64;

/** Factory for the configured key provider. */
public final class KeyProviders {
    public static final int MIN_KEY_BYTES = 32;

    private KeyProviders() {
    }

    public static KeyProvider create(Config c) {
        String type = c.getString("crypto.provider");
        switch (type) {
            case "env":
                return new EnvKeyProvider(c.getString("crypto.env.prefix"),
                        c.getBoolean("crypto.env.allow-system-properties"));
            case "vault":
                return new VaultKeyProvider(c.getConfig("crypto.vault"));
            case "aws-secrets-manager":
                return new AwsSecretsManagerKeyProvider(c.getConfig("crypto.aws"));
            default:
                throw new IllegalArgumentException("unknown crypto.provider: " + type);
        }
    }

    /** Decodes a base64 key and enforces a minimum length of 256 bits. */
    static byte[] decode(String base64, String version) {
        byte[] key;
        try {
            key = Base64.getDecoder().decode(base64.trim());
        } catch (IllegalArgumentException e) {
            throw new KeyUnavailableException("HMAC key version " + version + " is not valid base64");
        }
        if (key.length < MIN_KEY_BYTES) {
            throw new KeyUnavailableException("HMAC key version " + version + " must be at least "
                    + MIN_KEY_BYTES + " bytes");
        }
        return key;
    }
}
