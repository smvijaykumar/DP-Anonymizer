package com.example.anonymizer.common;

import com.example.anonymizer.common.crypto.Tokenizer;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TokenizerTest {
    private static final byte[] KEY = "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);
    private static final byte[] OTHER_KEY = "fedcba9876543210fedcba9876543210".getBytes(StandardCharsets.UTF_8);

    @Test
    void sameIdGivesSameTokenAcrossInstances() {
        Tokenizer a = new Tokenizer("v1", KEY, "customer-token");
        Tokenizer b = new Tokenizer("v1", KEY.clone(), "customer-token");
        assertThat(a.tokenize("C-000123")).isEqualTo(b.tokenize("C-000123"));
    }

    @Test
    void tokenIs64LowercaseHexCharacters() {
        assertThat(new Tokenizer("v1", KEY, "customer-token").tokenize("C-1")).matches("[0-9a-f]{64}");
    }

    @Test
    void surroundingWhitespaceIsIgnored() {
        Tokenizer t = new Tokenizer("v1", KEY, "customer-token");
        assertThat(t.tokenize("  C-1\t")).isEqualTo(t.tokenize("C-1"));
    }

    @Test
    void differentKeysGiveDifferentTokens() {
        assertThat(new Tokenizer("v1", KEY, "customer-token").tokenize("C-1"))
                .isNotEqualTo(new Tokenizer("v2", OTHER_KEY, "customer-token").tokenize("C-1"));
    }

    @Test
    void differentContextsGiveDifferentTokens() {
        assertThat(new Tokenizer("v1", KEY, "customer-token").tokenize("C-1"))
                .isNotEqualTo(new Tokenizer("v1", KEY, "account-token").tokenize("C-1"));
    }

    @Test
    void tokenMatchesIndependentHmacComputation() throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(KEY, "HmacSHA256"));
        mac.update("customer-token".getBytes(StandardCharsets.UTF_8));
        mac.update((byte) 0x1F);
        String expected = HexFormat.of().formatHex(mac.doFinal("C-42".getBytes(StandardCharsets.UTF_8)));
        assertThat(new Tokenizer("v1", KEY, "customer-token").tokenize("C-42")).isEqualTo(expected);
    }

    @Test
    void tokenDoesNotContainTheIdentifier() {
        String id = "CUSTOMER-987654";
        assertThat(new Tokenizer("v1", KEY, "customer-token").tokenize(id)).doesNotContain(id).doesNotContain("987654");
    }

    @Test
    void blankOrNullIdentifiersAreRejected() {
        Tokenizer t = new Tokenizer("v1", KEY, "customer-token");
        assertThatThrownBy(() -> t.tokenize("   ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> t.tokenize(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shortKeysAreRejected() {
        assertThatThrownBy(() -> new Tokenizer("v1", new byte[16], "customer-token"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void isThreadSafe() {
        Tokenizer t = new Tokenizer("v1", KEY, "customer-token");
        List<String> sequential = IntStream.range(0, 5_000).mapToObj(i -> t.tokenize("C-" + i)).collect(Collectors.toList());
        List<String> parallel = IntStream.range(0, 5_000).parallel().mapToObj(i -> t.tokenize("C-" + i)).collect(Collectors.toList());
        assertThat(parallel).isEqualTo(sequential);
    }

    @Test
    void toStringNeverRevealsKey() {
        assertThat(new Tokenizer("v1", KEY, "customer-token").toString()).doesNotContain("0123456789abcdef").contains("redacted");
    }
}
