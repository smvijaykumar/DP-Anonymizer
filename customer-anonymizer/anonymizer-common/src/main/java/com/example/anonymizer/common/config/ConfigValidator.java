package com.example.anonymizer.common.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Validates the "anonymizer" configuration subtree. Every rule exists so that the job fails at
 * startup instead of silently writing a field it does not know how to protect.
 */
public final class ConfigValidator {

    /** Column names the pipeline creates itself; input fields may not use them. */
    public static final Set<String> RESERVED_COLUMNS = Set.of(
            "status", "reason", "customer_token", "key_version", "profile_version", "suppressed",
            "kafka_partition", "kafka_offset", "kafka_timestamp", "p_token", "p_key_version",
            "p_profile_version", "dt", "hr");

    private static final Set<String> UNKNOWN_FIELD_POLICIES = Set.of("dlq", "fail");
    private static final Set<String> SUPPRESSED_HANDLING = Set.of("STAR", "EXCLUDE");

    private ConfigValidator() {
    }

    public static List<String> validate(Config c) {
        List<String> errors = new ArrayList<>();

        if (!c.hasPath("schema.fields")) {
            errors.add("schema.fields is missing");
            return errors;
        }
        Set<String> schemaFields = new LinkedHashSet<>();
        for (Config f : c.getConfigList("schema.fields")) {
            if (!f.hasPath("name") || !f.hasPath("type")) {
                errors.add("every entry in schema.fields needs a name and a type");
                continue;
            }
            String name = f.getString("name");
            if (!schemaFields.add(name)) {
                errors.add("duplicate schema field: " + name);
            }
            try {
                FieldType.parse(f.getString("type"));
            } catch (IllegalArgumentException e) {
                errors.add("field " + name + ": " + e.getMessage());
            }
            if (RESERVED_COLUMNS.contains(name.toLowerCase(Locale.ROOT))) {
                errors.add("field name is reserved by the pipeline: " + name);
            }
        }

        Map<String, ColumnClass> classified = new HashMap<>();
        for (ColumnClass cc : ColumnClass.values()) {
            String path = "classification." + cc.name();
            if (!c.hasPath(path)) {
                continue;
            }
            for (String name : c.getStringList(path)) {
                ColumnClass previous = classified.put(name, cc);
                if (previous != null) {
                    errors.add("field " + name + " is classified twice: " + previous + " and " + cc);
                }
                if (!schemaFields.contains(name)) {
                    errors.add("classified field " + name + " is not declared in schema.fields");
                }
            }
        }
        for (String name : schemaFields) {
            if (!classified.containsKey(name)) {
                errors.add("field " + name + " is unclassified");
            }
        }

        if (!c.hasPath("schema.id-field")) {
            errors.add("schema.id-field is missing");
        } else {
            String id = c.getString("schema.id-field");
            if (classified.get(id) != ColumnClass.DIRECT_IDENTIFIER) {
                errors.add("schema.id-field " + id + " must be classified DIRECT_IDENTIFIER");
            }
        }

        boolean hasSensitive = false;
        for (Map.Entry<String, ColumnClass> e : classified.entrySet()) {
            if (e.getValue() == ColumnClass.SENSITIVE) {
                hasSensitive = true;
            }
            if (e.getValue() == ColumnClass.QUASI_IDENTIFIER
                    && !c.hasPath(ConfigUtil.joinPath("hierarchies", e.getKey()))) {
                errors.add("quasi-identifier " + e.getKey() + " has no entry under hierarchies");
            }
        }

        if (c.hasPath("profile.k") && c.getInt("profile.k") < 2) {
            errors.add("profile.k must be at least 2");
        }
        if (c.hasPath("profile.l") && c.getInt("profile.l") > 1 && !hasSensitive) {
            errors.add("profile.l > 1 requires at least one SENSITIVE field");
        }
        if (c.hasPath("profile.suppression-limit")) {
            double s = c.getDouble("profile.suppression-limit");
            if (s < 0.0 || s > 0.2) {
                errors.add("profile.suppression-limit must be between 0 and 0.2");
            }
        }
        if (c.hasPath("profile.suppressed-handling")
                && !SUPPRESSED_HANDLING.contains(c.getString("profile.suppressed-handling"))) {
            errors.add("profile.suppressed-handling must be one of " + SUPPRESSED_HANDLING);
        }
        if (c.hasPath("unknown-field-policy")
                && !UNKNOWN_FIELD_POLICIES.contains(c.getString("unknown-field-policy"))) {
            errors.add("unknown-field-policy must be one of " + UNKNOWN_FIELD_POLICIES);
        }
        return errors;
    }

    public static void validateOrThrow(Config c) {
        List<String> errors = validate(c);
        if (!errors.isEmpty()) {
            throw new ConfigValidationException(errors);
        }
    }

    /** Job-specific check that settings without a sensible default have been provided. */
    public static void requireNonBlank(Config c, String... paths) {
        List<String> errors = new ArrayList<>();
        for (String p : paths) {
            if (!c.hasPath(p) || c.getString(p).isBlank()) {
                errors.add(p + " must be set");
            }
        }
        if (!errors.isEmpty()) {
            throw new ConfigValidationException(errors);
        }
    }
}
