package com.example.anonymizer.common.config;

import java.util.List;

/** Thrown at startup when configuration is incomplete or inconsistent. */
public class ConfigValidationException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final List<String> errors;

    public ConfigValidationException(List<String> errors) {
        super("Invalid anonymizer configuration:\n  - " + String.join("\n  - ", errors));
        this.errors = List.copyOf(errors);
    }

    public List<String> errors() {
        return errors;
    }
}
