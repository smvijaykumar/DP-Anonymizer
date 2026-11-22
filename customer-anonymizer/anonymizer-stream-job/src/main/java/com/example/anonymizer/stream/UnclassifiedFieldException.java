package com.example.anonymizer.stream;

/** Raised under unknown-field-policy = fail. The message never names the field or its value. */
public class UnclassifiedFieldException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public UnclassifiedFieldException() {
        super("Input message contains a field that is not declared in schema.fields; stopping (unknown-field-policy = fail)");
    }
}
