package com.example.anonymizer.common.config;

import java.io.Serializable;

/** One input field with its type and privacy classification. */
public record FieldSpec(String name, FieldType type, ColumnClass columnClass) implements Serializable {
}
