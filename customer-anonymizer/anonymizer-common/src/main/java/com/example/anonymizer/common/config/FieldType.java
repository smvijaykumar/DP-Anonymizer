package com.example.anonymizer.common.config;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.Locale;

/** Supported input field types. */
public enum FieldType {
    STRING, INT, LONG, DOUBLE, BOOLEAN, DATE, TIMESTAMP;

    public static FieldType parse(String value) {
        try {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IllegalArgumentException("unsupported type '" + value + "'");
        }
    }

    public DataType sparkType() {
        switch (this) {
            case STRING: return DataTypes.StringType;
            case INT: return DataTypes.IntegerType;
            case LONG: return DataTypes.LongType;
            case DOUBLE: return DataTypes.DoubleType;
            case BOOLEAN: return DataTypes.BooleanType;
            case DATE: return DataTypes.DateType;
            case TIMESTAMP: return DataTypes.TimestampType;
            default: throw new IllegalStateException("unhandled type " + this);
        }
    }
}
