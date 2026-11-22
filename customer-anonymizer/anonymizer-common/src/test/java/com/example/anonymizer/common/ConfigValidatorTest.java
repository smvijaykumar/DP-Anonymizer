package com.example.anonymizer.common;

import com.example.anonymizer.common.config.ConfigValidationException;
import com.example.anonymizer.common.config.ConfigValidator;
import com.example.anonymizer.common.config.SchemaClassification;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigValidatorTest {
    private static final String FIELDS = "schema { id-field = customerId\n fields = [\n"
            + "{name = customerId, type = string}, {name = dob, type = date},\n"
            + "{name = incomeBand, type = string}, {name = eventTs, type = timestamp} %s ] }\n";
    private static final String CLASSES = "classification {\n DIRECT_IDENTIFIER = [customerId %s]\n"
            + " QUASI_IDENTIFIER = [dob]\n SENSITIVE = [incomeBand]\n INSENSITIVE = [eventTs] }\n";
    private static final String REST = "hierarchies { dob { type = birth-year-bands, widths = [5, 10] } }\n"
            + "profile { k = 5, l = 2, suppression-limit = 0.02 }\nunknown-field-policy = dlq\n";

    private static Config config(String extraField, String extraDirect, String rest) {
        return ConfigFactory.parseString(String.format(FIELDS, extraField) + String.format(CLASSES, extraDirect) + rest);
    }

    @Test
    void validConfigurationPasses() {
        assertThat(ConfigValidator.validate(config("", "", REST))).isEmpty();
        assertThat(SchemaClassification.fromConfig(config("", "", REST)).fingerprint()).hasSize(16);
    }

    @Test
    void unclassifiedFieldFailsFast() {
        Config c = config(", {name = email, type = string}", "", REST);
        assertThat(ConfigValidator.validate(c)).contains("field email is unclassified");
        assertThatThrownBy(() -> SchemaClassification.fromConfig(c))
                .isInstanceOf(ConfigValidationException.class)
                .hasMessageContaining("email is unclassified");
    }

    @Test
    void fieldClassifiedTwiceFails() {
        Config c = config("", ", dob", REST);
        assertThat(ConfigValidator.validate(c)).anyMatch(e -> e.contains("dob is classified twice"));
    }

    @Test
    void classifiedFieldMissingFromSchemaFails() {
        Config c = config("", ", phone", REST);
        assertThat(ConfigValidator.validate(c)).contains("classified field phone is not declared in schema.fields");
    }

    @Test
    void idFieldMustBeDirectIdentifier() {
        Config c = config("", "", REST).withValue("schema.id-field", ConfigFactory.parseString("x = eventTs").getValue("x"));
        assertThat(ConfigValidator.validate(c)).contains("schema.id-field eventTs must be classified DIRECT_IDENTIFIER");
    }

    @Test
    void quasiIdentifierWithoutHierarchyFails() {
        Config c = config("", "", "profile { k = 5, l = 2 }\n");
        assertThat(ConfigValidator.validate(c)).contains("quasi-identifier dob has no entry under hierarchies");
    }

    @Test
    void reservedFieldNameFails() {
        Config c = config(", {name = customer_token, type = string}", ", customer_token", REST);
        assertThat(ConfigValidator.validate(c)).contains("field name is reserved by the pipeline: customer_token");
    }

    @Test
    void lDiversityNeedsSensitiveField() {
        String noSensitive = "schema { id-field = customerId, fields = [{name = customerId, type = string}] }\n"
                + "classification { DIRECT_IDENTIFIER = [customerId] }\nprofile { l = 3 }\n";
        assertThat(ConfigValidator.validate(ConfigFactory.parseString(noSensitive)))
                .contains("profile.l > 1 requires at least one SENSITIVE field");
    }

    @Test
    void unsupportedTypeFails() {
        Config c = config(", {name = blob, type = binary}", ", blob", REST);
        assertThat(ConfigValidator.validate(c)).anyMatch(e -> e.contains("unsupported type 'binary'"));
    }
}
