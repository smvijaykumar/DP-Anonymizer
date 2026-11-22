package com.example.anonymizer.common.config;

/** How a field is treated by the pipeline. Assigned in configuration, never in code. */
public enum ColumnClass {
    /** Identifies a person on its own. The id field is tokenized; all others are dropped. */
    DIRECT_IDENTIFIER,
    /** Identifies a person in combination (dob, zip, ...). Generalized by ARX. */
    QUASI_IDENTIFIER,
    /** Protected by l-diversity; released only alongside generalized quasi-identifiers. */
    SENSITIVE,
    /** Passed through unchanged. */
    INSENSITIVE
}
