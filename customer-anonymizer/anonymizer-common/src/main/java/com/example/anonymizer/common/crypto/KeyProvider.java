package com.example.anonymizer.common.crypto;

/** Supplies HMAC key material by version. Implementations must never log key bytes. */
public interface KeyProvider {
    /** Returns a fresh copy of the key bytes for {@code version}; callers should zero it after use. */
    byte[] keyBytes(String version);
}
