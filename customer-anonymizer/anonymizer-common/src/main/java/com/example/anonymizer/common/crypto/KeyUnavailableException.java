package com.example.anonymizer.common.crypto;

/** The HMAC key could not be loaded. Messages never include key material. */
public class KeyUnavailableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public KeyUnavailableException(String message) {
        super(message);
    }

    public KeyUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
