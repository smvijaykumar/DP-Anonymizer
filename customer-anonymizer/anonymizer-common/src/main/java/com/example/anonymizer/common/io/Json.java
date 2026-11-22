package com.example.anonymizer.common.io;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/** Shared JSON mapper for manifests and audit entries (never for customer records). */
public final class Json {
    public static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private Json() {
    }

    public static byte[] bytes(Object value) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize " + value.getClass().getSimpleName(), e);
        }
    }
}
