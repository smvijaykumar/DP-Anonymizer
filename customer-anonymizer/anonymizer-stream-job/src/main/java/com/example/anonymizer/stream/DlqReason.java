package com.example.anonymizer.stream;

/** Reason codes for dead-lettered messages. Codes describe the problem, never the content. */
public final class DlqReason {
    public static final String EMPTY_VALUE = "EMPTY_VALUE";
    public static final String MALFORMED_JSON = "MALFORMED_JSON";
    public static final String NOT_A_JSON_OBJECT = "NOT_A_JSON_OBJECT";
    public static final String UNCLASSIFIED_FIELD = "UNCLASSIFIED_FIELD";
    public static final String MISSING_ID = "MISSING_ID";
    public static final String INVALID_ID = "INVALID_ID";
    public static final String INVALID_FIELD_PREFIX = "INVALID_FIELD:";

    private DlqReason() {
    }

    public static String invalidField(String fieldName) {
        return INVALID_FIELD_PREFIX + fieldName;
    }
}
