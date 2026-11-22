package com.example.anonymizer.common.crypto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Reads keys from a HashiCorp Vault KV v2 secret whose fields are key versions, e.g.
 * {@code {"v1": "<base64>", "v2": "<base64>"}}. The Vault token comes from an environment variable or
 * from a file written by the Vault Agent sidecar.
 */
public final class VaultKeyProvider implements KeyProvider {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String address;
    private final String mount;
    private final String secretPath;
    private final String namespace;
    private final String tokenEnv;
    private final String tokenFile;
    private final Duration timeout;
    private final HttpClient http;

    public VaultKeyProvider(Config c) {
        this.address = stripTrailingSlash(c.getString("address"));
        this.mount = c.getString("mount");
        this.secretPath = c.getString("path");
        this.namespace = c.getString("namespace");
        this.tokenEnv = c.getString("token-env");
        this.tokenFile = c.getString("token-file");
        this.timeout = c.getDuration("timeout");
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
        if (address.isBlank()) {
            throw new KeyUnavailableException("crypto.vault.address is not set");
        }
    }

    @Override
    public byte[] keyBytes(String version) {
        URI uri = URI.create(address + "/v1/" + mount + "/data/" + secretPath);
        HttpRequest.Builder req = HttpRequest.newBuilder(uri).timeout(timeout).header("X-Vault-Token", token()).GET();
        if (!namespace.isBlank()) {
            req.header("X-Vault-Namespace", namespace);
        }
        try {
            HttpResponse<String> resp = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new KeyUnavailableException("Vault returned HTTP " + resp.statusCode() + " for the HMAC key secret");
            }
            JsonNode value = MAPPER.readTree(resp.body()).path("data").path("data").path(version);
            if (value.isMissingNode() || value.asText().isBlank()) {
                throw new KeyUnavailableException("HMAC key version " + version + " is not present in the Vault secret");
            }
            return KeyProviders.decode(value.asText(), version);
        } catch (IOException e) {
            throw new KeyUnavailableException("Could not read HMAC key from Vault", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KeyUnavailableException("Interrupted while reading HMAC key from Vault", e);
        }
    }

    private String token() {
        String t = System.getenv(tokenEnv);
        if (t != null && !t.isBlank()) {
            return t.trim();
        }
        Path p = Path.of(tokenFile);
        if (Files.isReadable(p)) {
            try {
                return Files.readString(p).trim();
            } catch (IOException e) {
                throw new KeyUnavailableException("Could not read Vault token file", e);
            }
        }
        throw new KeyUnavailableException("No Vault token: set " + tokenEnv + " or provide " + tokenFile);
    }

    private static String stripTrailingSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }
}
