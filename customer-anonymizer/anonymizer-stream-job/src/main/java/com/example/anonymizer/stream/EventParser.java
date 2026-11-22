package com.example.anonymizer.stream;

import com.example.anonymizer.common.config.ColumnClass;
import com.example.anonymizer.common.config.FieldSpec;
import com.example.anonymizer.common.config.FieldType;
import com.example.anonymizer.common.config.PipelineConfig;
import com.example.anonymizer.common.config.SchemaClassification;
import com.example.anonymizer.common.crypto.Tokenizer;
import com.example.anonymizer.common.crypto.TokenizerHolder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * First and only place raw messages are touched. Each Kafka record becomes one row that contains the
 * customer token, Kafka coordinates and INSENSITIVE fields only. The raw id, other direct identifiers,
 * quasi-identifiers and sensitive values in the event are discarded here; generalized values come
 * from the profile table. Errors become DLQ rows carrying a reason code, never message content.
 */
public final class EventParser implements MapPartitionsFunction<Row, Row> {
    private static final long serialVersionUID = 1L;
    public static final String STATUS_OK = "OK";
    public static final String STATUS_DLQ = "DLQ";
    static final int META_COLUMNS = 7;

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);

    private final PipelineConfig cfg;

    /** @param executorConfig a config from {@link PipelineConfig#forExecutors()} */
    public EventParser(PipelineConfig executorConfig) {
        this.cfg = executorConfig;
    }

    public static StructType outputSchema(SchemaClassification sc) {
        List<StructField> f = new ArrayList<>();
        f.add(DataTypes.createStructField("status", DataTypes.StringType, false));
        f.add(DataTypes.createStructField("reason", DataTypes.StringType, true));
        f.add(DataTypes.createStructField("customer_token", DataTypes.StringType, true));
        f.add(DataTypes.createStructField("key_version", DataTypes.StringType, true));
        f.add(DataTypes.createStructField("kafka_partition", DataTypes.IntegerType, false));
        f.add(DataTypes.createStructField("kafka_offset", DataTypes.LongType, false));
        f.add(DataTypes.createStructField("kafka_timestamp", DataTypes.TimestampType, true));
        for (FieldSpec s : sc.fields(ColumnClass.INSENSITIVE)) {
            f.add(DataTypes.createStructField(s.name(), s.type().sparkType(), true));
        }
        return DataTypes.createStructType(f);
    }

    @Override
    public Iterator<Row> call(Iterator<Row> input) {
        SchemaClassification sc = cfg.classification();
        Tokenizer tokenizer = TokenizerHolder.get(cfg);
        boolean failOnUnknown = "fail".equals(cfg.raw().getString("unknown-field-policy"));
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public Row next() {
                Row r = input.next();
                byte[] value = r.getAs("value");
                int partition = r.getAs("partition");
                long offset = r.getAs("offset");
                Timestamp ts = r.getAs("timestamp");
                return parse(value, partition, offset, ts, sc, tokenizer, failOnUnknown);
            }
        };
    }

    static Row parse(byte[] value, int partition, long offset, Timestamp ts, SchemaClassification sc,
                     Tokenizer tokenizer, boolean failOnUnknown) {
        List<FieldSpec> insensitive = sc.fields(ColumnClass.INSENSITIVE);
        if (value == null || value.length == 0) {
            return dlq(DlqReason.EMPTY_VALUE, partition, offset, ts, insensitive.size());
        }
        JsonNode root;
        try {
            root = MAPPER.readTree(value);
        } catch (IOException e) {
            return dlq(DlqReason.MALFORMED_JSON, partition, offset, ts, insensitive.size());
        }
        if (root == null || !root.isObject()) {
            return dlq(DlqReason.NOT_A_JSON_OBJECT, partition, offset, ts, insensitive.size());
        }
        Iterator<String> names = root.fieldNames();
        while (names.hasNext()) {
            if (!sc.hasField(names.next())) {
                if (failOnUnknown) {
                    throw new UnclassifiedFieldException();
                }
                return dlq(DlqReason.UNCLASSIFIED_FIELD, partition, offset, ts, insensitive.size());
            }
        }

        JsonNode id = root.get(sc.idField());
        if (id == null || id.isNull()) {
            return dlq(DlqReason.MISSING_ID, partition, offset, ts, insensitive.size());
        }
        if (!(id.isTextual() || id.isIntegralNumber())) {
            return dlq(DlqReason.INVALID_ID, partition, offset, ts, insensitive.size());
        }
        String idText = id.asText();
        if (idText.isBlank()) {
            return dlq(DlqReason.MISSING_ID, partition, offset, ts, insensitive.size());
        }

        Object[] v = new Object[META_COLUMNS + insensitive.size()];
        v[0] = STATUS_OK;
        v[1] = null;
        v[2] = tokenizer.tokenize(idText);
        v[3] = tokenizer.keyVersion();
        v[4] = partition;
        v[5] = offset;
        v[6] = ts;
        for (int i = 0; i < insensitive.size(); i++) {
            FieldSpec f = insensitive.get(i);
            try {
                v[META_COLUMNS + i] = convert(root.get(f.name()), f.type());
            } catch (RuntimeException e) {
                return dlq(DlqReason.invalidField(f.name()), partition, offset, ts, insensitive.size());
            }
        }
        return RowFactory.create(v);
    }

    private static Row dlq(String reason, int partition, long offset, Timestamp ts, int insensitiveCount) {
        Object[] v = new Object[META_COLUMNS + insensitiveCount];
        v[0] = STATUS_DLQ;
        v[1] = reason;
        v[4] = partition;
        v[5] = offset;
        v[6] = ts;
        return RowFactory.create(v);
    }

    static Object convert(JsonNode n, FieldType type) {
        if (n == null || n.isNull()) {
            return null;
        }
        switch (type) {
            case STRING:
                return n.isValueNode() ? n.asText() : n.toString();
            case INT:
                if (n.isTextual()) {
                    return Integer.valueOf(n.asText().trim());
                }
                requireIntegral(n);
                if (!n.canConvertToInt()) {
                    throw new IllegalArgumentException("out of int range");
                }
                return n.intValue();
            case LONG:
                if (n.isTextual()) {
                    return Long.valueOf(n.asText().trim());
                }
                requireIntegral(n);
                if (!n.canConvertToLong()) {
                    throw new IllegalArgumentException("out of long range");
                }
                return n.longValue();
            case DOUBLE:
                if (n.isTextual()) {
                    return Double.valueOf(n.asText().trim());
                }
                if (!n.isNumber()) {
                    throw new IllegalArgumentException("not a number");
                }
                return n.doubleValue();
            case BOOLEAN:
                if (n.isBoolean()) {
                    return n.booleanValue();
                }
                String b = n.asText().trim();
                if ("true".equalsIgnoreCase(b) || "false".equalsIgnoreCase(b)) {
                    return Boolean.valueOf(b);
                }
                throw new IllegalArgumentException("not a boolean");
            case DATE:
                return Date.valueOf(LocalDate.parse(n.asText().trim()));
            case TIMESTAMP:
                if (n.isIntegralNumber()) {
                    return new Timestamp(n.longValue()); // epoch millis
                }
                return Timestamp.from(parseInstant(n.asText().trim()));
            default:
                throw new IllegalStateException("unhandled type " + type);
        }
    }

    private static void requireIntegral(JsonNode n) {
        if (!n.isIntegralNumber()) {
            throw new IllegalArgumentException("not an integer");
        }
    }

    private static Instant parseInstant(String s) {
        try {
            return Instant.parse(s);
        } catch (DateTimeParseException e1) {
            try {
                return OffsetDateTime.parse(s).toInstant();
            } catch (DateTimeParseException e2) {
                return LocalDateTime.parse(s).toInstant(ZoneOffset.UTC);
            }
        }
    }
}
