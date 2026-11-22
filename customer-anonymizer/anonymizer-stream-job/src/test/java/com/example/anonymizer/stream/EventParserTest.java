package com.example.anonymizer.stream;

import com.example.anonymizer.common.config.SchemaClassification;
import com.example.anonymizer.common.crypto.Tokenizer;
import com.example.anonymizer.common.guard.OutputSchemaGuard;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Proves no raw identifier, quasi-identifier or sensitive value survives parsing. */
class EventParserTest {
    private static final SchemaClassification SC = SchemaClassification.fromConfig(ConfigFactory.parseString(
            "schema { id-field = customerId, fields = [\n"
                    + "{name = customerId, type = string}, {name = firstName, type = string}, {name = email, type = string},\n"
                    + "{name = dob, type = date}, {name = zip, type = string}, {name = incomeBand, type = string},\n"
                    + "{name = eventType, type = string}, {name = eventTs, type = timestamp}, {name = amount, type = double} ] }\n"
                    + "classification { DIRECT_IDENTIFIER = [customerId, firstName, email], QUASI_IDENTIFIER = [dob, zip],\n"
                    + " SENSITIVE = [incomeBand], INSENSITIVE = [eventType, eventTs, amount] }\n"
                    + "hierarchies { dob { type = birth-year-bands, widths = [5] }, zip { type = zip-mask, keep-digits = [5, 3] } }\n"
                    + "profile { l = 2 }\n"));
    private static final Tokenizer TOKENIZER = new Tokenizer("v1",
            "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8), "customer-token");
    private static final Timestamp TS = Timestamp.valueOf("2026-11-12 10:00:00");

    private static final List<String> CANARIES = List.of("CANARY-ID-123", "CANARY_FIRST", "canary@example.com",
            "1961-02-03", "99999", "CANARY_INCOME");
    private static final String VALID = "{\"customerId\":\"CANARY-ID-123\",\"firstName\":\"CANARY_FIRST\","
            + "\"email\":\"canary@example.com\",\"dob\":\"1961-02-03\",\"zip\":\"99999\",\"incomeBand\":\"CANARY_INCOME\","
            + "\"eventType\":\"LOGIN\",\"eventTs\":\"2026-11-12T09:30:00Z\",\"amount\":12.5}";

    private static Row parse(String json, boolean failOnUnknown) {
        byte[] bytes = json == null ? null : json.getBytes(StandardCharsets.UTF_8);
        return EventParser.parse(bytes, 3, 42L, TS, SC, TOKENIZER, failOnUnknown);
    }

    private static void assertNoCanaries(Row row) {
        for (int i = 0; i < row.length(); i++) {
            String value = String.valueOf(row.get(i));
            for (String canary : CANARIES) {
                assertThat(value).as("column %d", i).doesNotContain(canary);
            }
        }
    }

    @Test
    void validEventKeepsOnlyTokenAndInsensitiveFields() {
        Row r = parse(VALID, false);
        assertThat(r.getString(0)).isEqualTo(EventParser.STATUS_OK);
        assertThat(r.getString(2)).isEqualTo(TOKENIZER.tokenize("CANARY-ID-123"));
        assertThat(r.getString(3)).isEqualTo("v1");
        assertThat(r.length()).isEqualTo(EventParser.META_COLUMNS + 3);
        assertThat(r.getString(7)).isEqualTo("LOGIN");
        assertThat(r.getTimestamp(8).toInstant().toString()).isEqualTo("2026-11-12T09:30:00Z");
        assertThat(r.getDouble(9)).isEqualTo(12.5);
        assertNoCanaries(r);
    }

    @Test
    void sameCustomerAlwaysGetsSameToken() {
        assertThat(parse(VALID, false).getString(2)).isEqualTo(parse(VALID.replace("LOGIN", "LOGOUT"), false).getString(2));
    }

    @Test
    void parserOutputSchemaPassesTheGuard() {
        assertThatCode(() -> OutputSchemaGuard.verify(EventParser.outputSchema(SC), SC)).doesNotThrowAnyException();
    }

    @Test
    void malformedJsonGoesToDlqWithoutContent() {
        Row r = parse("{\"customerId\":\"CANARY-ID-123\", broken", false);
        assertThat(r.getString(0)).isEqualTo(EventParser.STATUS_DLQ);
        assertThat(r.getString(1)).isEqualTo(DlqReason.MALFORMED_JSON);
        assertThat(r.getInt(4)).isEqualTo(3);
        assertThat(r.getLong(5)).isEqualTo(42L);
        assertNoCanaries(r);
    }

    @Test
    void nonObjectAndEmptyMessagesGoToDlq() {
        assertThat(parse("[1,2,3]", false).getString(1)).isEqualTo(DlqReason.NOT_A_JSON_OBJECT);
        assertThat(parse(null, false).getString(1)).isEqualTo(DlqReason.EMPTY_VALUE);
    }

    @Test
    void unknownFieldGoesToDlqUnderDlqPolicy() {
        Row r = parse(VALID.replace("}", ",\"ssn\":\"123-45-6789\"}"), false);
        assertThat(r.getString(1)).isEqualTo(DlqReason.UNCLASSIFIED_FIELD);
        for (int i = 0; i < r.length(); i++) {
            assertThat(String.valueOf(r.get(i))).doesNotContain("123-45-6789");
        }
        assertNoCanaries(r);
    }

    @Test
    void unknownFieldStopsTheJobUnderFailPolicy() {
        assertThatThrownBy(() -> parse(VALID.replace("}", ",\"ssn\":\"123-45-6789\"}"), true))
                .isInstanceOf(UnclassifiedFieldException.class)
                .hasMessageNotContaining("ssn")
                .hasMessageNotContaining("123-45-6789");
    }

    @Test
    void missingOrInvalidIdGoesToDlq() {
        assertThat(parse(VALID.replace("\"customerId\":\"CANARY-ID-123\",", ""), false).getString(1)).isEqualTo(DlqReason.MISSING_ID);
        assertThat(parse(VALID.replace("\"CANARY-ID-123\"", "{\"x\":1}"), false).getString(1)).isEqualTo(DlqReason.INVALID_ID);
        assertThat(parse(VALID.replace("\"CANARY-ID-123\"", "\"  \""), false).getString(1)).isEqualTo(DlqReason.MISSING_ID);
    }

    @Test
    void invalidInsensitiveValueGoesToDlqWithFieldName() {
        Row r = parse(VALID.replace("2026-11-12T09:30:00Z", "yesterday"), false);
        assertThat(r.getString(1)).isEqualTo(DlqReason.invalidField("eventTs"));
        assertNoCanaries(r);
    }

    @Test
    void duplicateKeysAreRejected() {
        Row r = parse("{\"customerId\":\"A\",\"customerId\":\"B\"}", false);
        assertThat(r.getString(1)).isEqualTo(DlqReason.MALFORMED_JSON);
    }
}
