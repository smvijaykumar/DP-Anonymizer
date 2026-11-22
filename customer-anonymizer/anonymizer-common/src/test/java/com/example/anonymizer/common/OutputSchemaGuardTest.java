package com.example.anonymizer.common;

import com.example.anonymizer.common.config.SchemaClassification;
import com.example.anonymizer.common.guard.OutputSchemaGuard;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutputSchemaGuardTest {
    private static final SchemaClassification SC = SchemaClassification.fromConfig(ConfigFactory.parseString(
            "schema { id-field = customerId, fields = [{name = customerId, type = string}, {name = email, type = string},"
                    + " {name = zip, type = string}, {name = eventType, type = string}] }\n"
                    + "classification { DIRECT_IDENTIFIER = [customerId, email], QUASI_IDENTIFIER = [zip], INSENSITIVE = [eventType] }\n"
                    + "hierarchies { zip { type = zip-mask, keep-digits = [5, 3] } }\nprofile { l = 1 }\n"));

    private static StructType schema(String... names) {
        StructType s = new StructType();
        for (String n : names) {
            s = s.add(n, DataTypes.StringType);
        }
        return s;
    }

    @Test
    void allowsTokensGeneralizedAndInsensitiveColumns() {
        assertThatCode(() -> OutputSchemaGuard.verify(schema("customer_token", "zip_gen", "eventType"), SC))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsRawDirectIdentifier() {
        assertThatThrownBy(() -> OutputSchemaGuard.verify(schema("customer_token", "email"), SC))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("email");
    }

    @Test
    void rejectsRawQuasiIdentifier() {
        assertThatThrownBy(() -> OutputSchemaGuard.verify(schema("zip"), SC)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void comparisonIsCaseInsensitive() {
        assertThatThrownBy(() -> OutputSchemaGuard.verify(schema("CUSTOMERID"), SC)).isInstanceOf(IllegalStateException.class);
    }
}
