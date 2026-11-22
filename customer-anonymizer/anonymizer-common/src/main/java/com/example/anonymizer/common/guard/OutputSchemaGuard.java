package com.example.anonymizer.common.guard;

import com.example.anonymizer.common.config.ColumnClass;
import com.example.anonymizer.common.config.SchemaClassification;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Last line of defence before any write: refuses a schema that contains a raw direct-identifier or
 * raw quasi-identifier column. Generalized values are written under different names (e.g. dob_gen).
 */
public final class OutputSchemaGuard {
    public static final String GENERALIZED_SUFFIX = "_gen";

    private OutputSchemaGuard() {
    }

    public static void verify(StructType schema, SchemaClassification sc) {
        Set<String> forbidden = new HashSet<>();
        for (String n : sc.names(ColumnClass.DIRECT_IDENTIFIER)) {
            forbidden.add(n.toLowerCase(Locale.ROOT));
        }
        for (String n : sc.names(ColumnClass.QUASI_IDENTIFIER)) {
            forbidden.add(n.toLowerCase(Locale.ROOT));
        }
        for (StructField f : schema.fields()) {
            if (forbidden.contains(f.name().toLowerCase(Locale.ROOT))) {
                throw new IllegalStateException("Refusing to write: output schema contains raw identifier column '" + f.name() + "'");
            }
        }
    }
}
