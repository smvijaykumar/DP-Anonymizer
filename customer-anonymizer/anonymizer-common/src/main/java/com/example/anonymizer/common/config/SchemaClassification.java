package com.example.anonymizer.common.config;

import com.typesafe.config.Config;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Validated, ordered view of the input schema and each field's classification. */
public final class SchemaClassification implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String idField;
    private final List<FieldSpec> fields;
    private final Map<String, FieldSpec> byName;

    private SchemaClassification(String idField, List<FieldSpec> fields) {
        this.idField = idField;
        this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
        Map<String, FieldSpec> m = new LinkedHashMap<>();
        for (FieldSpec f : fields) {
            m.put(f.name(), f);
        }
        this.byName = Collections.unmodifiableMap(m);
    }

    /** Builds the classification, failing fast if the configuration is invalid. */
    public static SchemaClassification fromConfig(Config c) {
        ConfigValidator.validateOrThrow(c);
        Map<String, ColumnClass> classOf = new HashMap<>();
        for (ColumnClass cc : ColumnClass.values()) {
            String path = "classification." + cc.name();
            if (c.hasPath(path)) {
                for (String n : c.getStringList(path)) {
                    classOf.put(n, cc);
                }
            }
        }
        List<FieldSpec> specs = new ArrayList<>();
        for (Config f : c.getConfigList("schema.fields")) {
            String name = f.getString("name");
            specs.add(new FieldSpec(name, FieldType.parse(f.getString("type")), classOf.get(name)));
        }
        return new SchemaClassification(c.getString("schema.id-field"), specs);
    }

    public String idField() {
        return idField;
    }

    public List<FieldSpec> fields() {
        return fields;
    }

    public List<FieldSpec> fields(ColumnClass cc) {
        List<FieldSpec> out = new ArrayList<>();
        for (FieldSpec f : fields) {
            if (f.columnClass() == cc) {
                out.add(f);
            }
        }
        return out;
    }

    public List<String> names(ColumnClass cc) {
        List<String> out = new ArrayList<>();
        for (FieldSpec f : fields(cc)) {
            out.add(f.name());
        }
        return out;
    }

    public boolean hasField(String name) {
        return byName.containsKey(name);
    }

    public FieldSpec field(String name) {
        return byName.get(name);
    }

    /** Short, stable hash of schema + classification, recorded as schema_version in outputs. */
    public String fingerprint() {
        StringBuilder sb = new StringBuilder("id=").append(idField).append(';');
        for (FieldSpec f : fields) {
            sb.append(f.name()).append(':').append(f.type()).append(':').append(f.columnClass()).append(';');
        }
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(d).substring(0, 16);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
